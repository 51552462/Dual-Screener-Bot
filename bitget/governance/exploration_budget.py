"""
Dynamic Exploration Budget (7일 롤링 MAB) — 챔피언(LIVE) vs 탐험(OBSERVING/CANDIDATE)
그룹 간 자본 배분을 실력 기반으로 자율 조절하고, 국면 전환 시 즉시 안전선으로
강제 리셋하는 블랙스완 방어 엔진.

설계 원칙:
- 고정 비율 폐기: 탐험 자본 캡을 7일 롤링 실현 수익률 비교로 매일 재산정한다.
- 국면 전환 방어: CURRENT_REGIME_KEY 가 바뀌는 순간 과거 7일 우위를 즉시 무효화하고
  안전선(10%)으로 강제 리셋 — 새 국면에서 7일간 실력을 다시 증명해야 예산이 늘어난다.
- 비침습: 기존 Kelly 파이프라인(auto_pilot/meta_governor/meta_consumer) 뒤에 곱해지는
  '역할 스케일러'로만 개입한다. 상태는 system_config 의 EXPLORATION_BUDGET_STATE 단일
  키에 저장 — 실패 시 스케일러는 항상 1.0(무변경)로 안전 폴백.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

from bitget.infra.bounded_reads import forward_exploration_budget_closed_sql
from bitget.infra.clock import parse_utc_iso, utc_date_days_ago_str, utc_now, utc_now_iso
from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.governance.exploration_budget")

STATE_KEY = "EXPLORATION_BUDGET_STATE"

ROLLING_WINDOW_DAYS = 7
REGIME_SHIFT_PROTECTION_DAYS = 7
MIN_RECOMPUTE_INTERVAL_HOURS = 6.0
MIN_TRADES_PER_BUCKET = 3

EXPLORE_DEFAULT_PCT = 0.10
EXPLORE_CEILING_PCT = 0.50
EXPLORE_UNDERPERFORM_PCT = 0.08
CHAMPION_DEFAULT_PCT = 1.0 - EXPLORE_DEFAULT_PCT
OUTPERFORM_SPREAD_PP = 10.0

_ROLE_CACHE: Dict[str, Any] = {"ts": 0.0, "map": {}}
_ROLE_CACHE_TTL_SEC = 120.0


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _default_state() -> Dict[str, Any]:
    return {
        "explore_pct": EXPLORE_DEFAULT_PCT,
        "champion_pct": CHAMPION_DEFAULT_PCT,
        "mode": "INIT",
        "last_regime_key": None,
        "regime_shift_at": None,
        "prev_regime_key": None,
        "champion_7d_ret_pct": None,
        "exploration_7d_ret_pct": None,
        "champion_n": 0,
        "exploration_n": 0,
        "updated_at": utc_now_iso(),
    }


def load_exploration_budget_state() -> Dict[str, Any]:
    try:
        from bitget.infra import config_manager

        st = config_manager.get_config_value(STATE_KEY, None)
        if isinstance(st, dict) and st:
            return st
    except Exception as ex:
        logger.warning("exploration_budget: load state failed: %s", ex)
    return _default_state()


def _save_state(state: Dict[str, Any]) -> None:
    try:
        from bitget.infra import config_manager

        config_manager.set_config_value(STATE_KEY, state)
    except Exception as ex:
        logger.warning("exploration_budget: save state failed: %s", ex)


def _classify_role(state: str) -> str:
    s = str(state or "").strip().upper()
    if s == "LIVE":
        return "CHAMPION"
    if s in ("OBSERVING", "CANDIDATE"):
        return "EXPLORATION"
    return "NEUTRAL"


def _load_registry_role_map(*, force: bool = False) -> Dict[str, str]:
    """group_key → CHAMPION/EXPLORATION/NEUTRAL, TTL 캐시(120s)."""
    now = time.time()
    if not force and (now - float(_ROLE_CACHE.get("ts", 0.0))) < _ROLE_CACHE_TTL_SEC:
        return dict(_ROLE_CACHE.get("map") or {})

    out: Dict[str, str] = {}
    try:
        from strategy_registry_store import load_registry_rows

        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.market_keys import is_bitget_registry_market

        # 코인 전용 strategy_registry는 Bitget 자체 DB에 격리 저장된다(주식 DB 미참조).
        rows = load_registry_rows(market_data_db_path())
        for r in rows:
            mkt = str(r.get("market") or "").strip()
            if not is_bitget_registry_market(mkt):
                continue
            gk = str(r.get("group_key") or "").strip()
            if not gk:
                continue
            role = _classify_role(str(r.get("state") or ""))
            # 여러 마켓(SPOT/FUT/레거시 BG)에 동일 group_key 가 있을 수 있으므로
            # CHAMPION 을 우선 유지(더 정보가 많은 상태를 신뢰).
            if gk not in out or role == "CHAMPION":
                out[gk] = role
    except Exception as ex:
        logger.warning("exploration_budget: registry role map load failed: %s", ex)

    _ROLE_CACHE["ts"] = now
    _ROLE_CACHE["map"] = out
    return dict(out)


def compute_rolling_bucket_returns(days: int = ROLLING_WINDOW_DAYS) -> Dict[str, Any]:
    """최근 N일 청산분을 CHAMPION/EXPLORATION 버킷으로 나눠 자본가중 실현수익률(%) 산출."""
    from bitget.forward.gates import _extract_core_group
    from bitget.infra.data_paths import market_data_db_path
    from bitget.infra.shared_db_connector import get_connection

    role_map = _load_registry_role_map()
    cutoff = utc_date_days_ago_str(days)

    buckets: Dict[str, Dict[str, float]] = {
        "CHAMPION": {"pnl": 0.0, "capital": 0.0, "n": 0},
        "EXPLORATION": {"pnl": 0.0, "capital": 0.0, "n": 0},
    }

    conn = None
    try:
        conn = get_connection(market_data_db_path(), read_only=True)
        q, params = forward_exploration_budget_closed_sql(since_date=cutoff)
        cur = conn.execute(q, params)
        for sig_type, final_ret, sim_kelly_invest in cur.fetchall():
            sig_s = str(sig_type or "")
            if "[INCUBATOR_" in sig_s.upper():
                continue
            gk = _extract_core_group(sig_s)
            role = role_map.get(gk, "NEUTRAL")
            if role not in ("CHAMPION", "EXPLORATION"):
                continue
            invest = float(sim_kelly_invest or 0.0)
            ret = float(final_ret or 0.0)
            buckets[role]["pnl"] += invest * ret / 100.0
            buckets[role]["capital"] += invest
            buckets[role]["n"] += 1
    except Exception as ex:
        logger.warning("exploration_budget: rolling bucket query failed: %s", ex)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _ret_pct(b: Dict[str, float]) -> float:
        cap = b["capital"]
        return (b["pnl"] / cap * 100.0) if cap > 0 else 0.0

    return {
        "champion_7d_ret_pct": _ret_pct(buckets["CHAMPION"]),
        "champion_n": int(buckets["CHAMPION"]["n"]),
        "champion_pnl_usdt": buckets["CHAMPION"]["pnl"],
        "exploration_7d_ret_pct": _ret_pct(buckets["EXPLORATION"]),
        "exploration_n": int(buckets["EXPLORATION"]["n"]),
        "exploration_pnl_usdt": buckets["EXPLORATION"]["pnl"],
    }


def trigger_regime_shift_reset(
    *, previous_regime: str, new_regime: str, notify: bool = True
) -> Dict[str, Any]:
    """[블랙스완 방어] 국면 전환 즉시 탐험 예산을 안전선(10%)으로 강제 리셋."""
    state = _default_state()
    state.update(
        {
            "explore_pct": EXPLORE_DEFAULT_PCT,
            "champion_pct": CHAMPION_DEFAULT_PCT,
            "mode": "REGIME_SHIFT_DEFENSE",
            "last_regime_key": new_regime,
            "prev_regime_key": previous_regime,
            "regime_shift_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }
    )
    _save_state(state)
    logger.warning(
        "exploration_budget: REGIME SHIFT %s -> %s — exploration budget hard-reset to %.0f%%",
        previous_regime,
        new_regime,
        EXPLORE_DEFAULT_PCT * 100.0,
    )
    if notify:
        try:
            from bitget.forward.shared import send_telegram_msg

            send_telegram_msg(
                "🦢 <b>[블랙스완 방어 — 국면 전환 감지]</b>\n"
                f"국면 전환: <b>{previous_regime}</b> → <b>{new_regime}</b>\n"
                f"탐험 자본 캡 즉시 <b>{EXPLORE_DEFAULT_PCT*100:.0f}%</b>로 강제 리셋 "
                f"(챔피언 <b>{CHAMPION_DEFAULT_PCT*100:.0f}%</b> 보장).\n"
                f"신규 국면에서 <b>{REGIME_SHIFT_PROTECTION_DAYS}일</b>간 다시 실력을 "
                f"증명해야 탐험 예산이 확장됩니다."
            )
        except Exception:
            pass
    return state


def refresh_exploration_budget_state(*, force: bool = False) -> Dict[str, Any]:
    """
    7일 롤링 평가 갱신 진입점.
      1) 국면 변경을 놓쳤을 경우의 안전망(meta_sync 훅이 1차 방어선).
      2) 국면 방어 윈도우(7일) 내면 안전선 고정.
      3) 그 외엔 챔피언 vs 탐험 7일 실현수익률 비교로 배분 재산정.
    """
    from bitget.governance.meta_sync import normalize_regime_key

    try:
        from bitget.infra import config_manager

        current_regime = normalize_regime_key(config_manager.get_config_value("CURRENT_REGIME_KEY"))
    except Exception:
        current_regime = "UNKNOWN"

    state = load_exploration_budget_state()
    last_regime = state.get("last_regime_key")

    if not last_regime:
        state = _default_state()
        state["last_regime_key"] = current_regime
        _save_state(state)
        return state

    if last_regime != current_regime:
        # 훅이 놓친 국면전환을 여기서 잡아 즉시 방어(안전망).
        return trigger_regime_shift_reset(previous_regime=last_regime, new_regime=current_regime)

    regime_shift_at = parse_utc_iso(state.get("regime_shift_at"))
    if regime_shift_at is not None:
        days_since = (utc_now() - regime_shift_at).total_seconds() / 86400.0
        if days_since < REGIME_SHIFT_PROTECTION_DAYS:
            state["mode"] = "REGIME_SHIFT_DEFENSE"
            state["explore_pct"] = EXPLORE_DEFAULT_PCT
            state["champion_pct"] = CHAMPION_DEFAULT_PCT
            state["defense_days_remaining"] = round(REGIME_SHIFT_PROTECTION_DAYS - days_since, 2)
            state["updated_at"] = utc_now_iso()
            _save_state(state)
            return state

    updated_at = parse_utc_iso(state.get("updated_at"))
    if not force and updated_at is not None:
        hours_since = (utc_now() - updated_at).total_seconds() / 3600.0
        if hours_since < MIN_RECOMPUTE_INTERVAL_HOURS and state.get("mode") not in ("INIT",):
            return state

    perf = compute_rolling_bucket_returns(ROLLING_WINDOW_DAYS)
    champ_ret = perf["champion_7d_ret_pct"]
    expl_ret = perf["exploration_7d_ret_pct"]
    champ_n = perf["champion_n"]
    expl_n = perf["exploration_n"]

    if champ_n < MIN_TRADES_PER_BUCKET or expl_n < MIN_TRADES_PER_BUCKET:
        explore_pct = EXPLORE_DEFAULT_PCT
        mode = "INSUFFICIENT_SAMPLE"
    elif expl_ret > champ_ret:
        edge_pp = expl_ret - champ_ret
        ramp = _clamp(edge_pp / OUTPERFORM_SPREAD_PP, 0.0, 1.0)
        explore_pct = EXPLORE_DEFAULT_PCT + (EXPLORE_CEILING_PCT - EXPLORE_DEFAULT_PCT) * ramp
        mode = "EXPANDED"
    else:
        explore_pct = EXPLORE_UNDERPERFORM_PCT
        mode = "CONTRACTED"

    state.update(
        {
            "explore_pct": explore_pct,
            "champion_pct": 1.0 - explore_pct,
            "mode": mode,
            "last_regime_key": current_regime,
            "champion_7d_ret_pct": champ_ret,
            "exploration_7d_ret_pct": expl_ret,
            "champion_n": champ_n,
            "exploration_n": expl_n,
            "defense_days_remaining": 0,
            "updated_at": utc_now_iso(),
        }
    )
    _save_state(state)
    return state


def get_exploration_role_scaler(sys_config: Dict[str, Any], group_key: str) -> Tuple[float, str]:
    """
    Kelly 사이징 핫패스용 — 추가 DB 왕복 없이 이미 로드된 sys_config 의
    EXPLORATION_BUDGET_STATE 를 읽고, group_key 의 역할(캐시된 레지스트리)만 조회한다.
    실패 시 항상 (1.0, 'NEUTRAL') 로 안전 폴백 — 기존 Kelly 동작 무변경 보장.
    """
    try:
        state = sys_config.get(STATE_KEY) if isinstance(sys_config, dict) else None
        if not isinstance(state, dict) or not state:
            return 1.0, "NEUTRAL"
        explore_pct = float(state.get("explore_pct", EXPLORE_DEFAULT_PCT))
        champion_pct = float(state.get("champion_pct", CHAMPION_DEFAULT_PCT))
        role_map = _load_registry_role_map()
        role = role_map.get(str(group_key or "").strip(), "NEUTRAL")
        if role == "CHAMPION":
            scaler = champion_pct / CHAMPION_DEFAULT_PCT if CHAMPION_DEFAULT_PCT > 0 else 1.0
            return max(0.0, scaler), role
        if role == "EXPLORATION":
            scaler = explore_pct / EXPLORE_DEFAULT_PCT if EXPLORE_DEFAULT_PCT > 0 else 1.0
            return max(0.0, scaler), role
        return 1.0, "NEUTRAL"
    except Exception as ex:
        logger.warning("exploration_budget: role scaler failed, fallback 1.0: %s", ex)
        return 1.0, "NEUTRAL"


def format_exploration_budget_panel_html(state: Optional[Dict[str, Any]] = None) -> str:
    """데일리 리포트용 '📊 [자본 배분]' 패널."""
    st = state if isinstance(state, dict) and state else load_exploration_budget_state()
    explore_pct = float(st.get("explore_pct", EXPLORE_DEFAULT_PCT)) * 100.0
    champion_pct = float(st.get("champion_pct", CHAMPION_DEFAULT_PCT)) * 100.0
    mode = str(st.get("mode", "INIT"))
    mode_label = {
        "INIT": "초기화",
        "INSUFFICIENT_SAMPLE": "표본부족(기본값 유지)",
        "EXPANDED": "탐험 우위 확장",
        "CONTRACTED": "탐험 부진 수축",
        "REGIME_SHIFT_DEFENSE": "국면전환 방어중",
    }.get(mode, mode)

    champ_ret = st.get("champion_7d_ret_pct")
    expl_ret = st.get("exploration_7d_ret_pct")
    champ_ret_s = f"{float(champ_ret):+.2f}%" if champ_ret is not None else "—"
    expl_ret_s = f"{float(expl_ret):+.2f}%" if expl_ret is not None else "—"

    lines = [
        f"📊 <b>[자본 배분]</b> 탐험 <b>{explore_pct:.0f}%</b> / 챔피언 "
        f"<b>{champion_pct:.0f}%</b> (최근 7일 성과 및 국면 방어 적용)",
        f"▪️ 모드: <b>{mode_label}</b> · 챔피언(LIVE) 7일 {champ_ret_s} "
        f"(n={st.get('champion_n', 0)}) vs 탐험(OBS/CAND) 7일 {expl_ret_s} "
        f"(n={st.get('exploration_n', 0)})",
    ]
    if mode == "REGIME_SHIFT_DEFENSE":
        remain = st.get("defense_days_remaining")
        prev_rk = st.get("prev_regime_key")
        new_rk = st.get("last_regime_key")
        remain_s = f"{float(remain):.1f}일" if remain is not None else "—"
        lines.append(
            f"🦢 국면전환 방어중: {prev_rk} → {new_rk} · 잔여 {remain_s} "
            f"(안전선 {EXPLORE_DEFAULT_PCT*100:.0f}% 강제 고정)"
        )
    return "\n".join(lines) + "\n"
