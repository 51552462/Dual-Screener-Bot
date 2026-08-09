"""
MetaGovernor 전략 무기고 — Discovery · LIVE Hard Gate · Whipsaw(일별) · Alpha TTL.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from strategy_lifecycle_config import load_strategy_lifecycle_config, market_params
from strategy_registry_store import (
    consecutive_below_live_days,
    load_registry_rows,
    merge_registry_sources,
    record_quality_daily,
    upsert_registry_rows,
    _kst_today,
)

logger = logging.getLogger(__name__)


def stable_strategy_id(market: str, group_key: str) -> str:
    raw = f"{str(market or 'KR').upper()}|{str(group_key or '').strip()}"
    return "strat:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def resolve_live_ev_verification_tolerance(
    market: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    regime_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    불사조 LIVE 실전 EV 검증 — ATR14/Price × META_REGIME_KEY 가중 동적 오차율.
    re_evolution_ev_rampup 청산 평가부 SSOT 진입점.
    """
    from re_evolution_dynamic_tolerance import compute_dynamic_ev_tolerance_pct

    return compute_dynamic_ev_tolerance_pct(
        market,
        meta=meta,
        sys_config=sys_config,
        regime_key=regime_key,
    )


def evaluate_live_ev_performance_verification(
    live_rets: List[float],
    warm_record: Optional[Dict[str, Any]],
    *,
    market: str = "KR",
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    불사조 LIVE 실전 성과 검증 — ATR tolerance OR Z-Score (-1.5σ) 통합 게이트.
    re_evolution_ev_rampup.process_warm_start_live_closure 와 동일 SSOT.
    """
    from re_evolution_dynamic_tolerance import enrich_ev_ramp_config_with_dynamic_tolerance
    from re_evolution_ev_rampup import ev_rampup_config
    from re_evolution_zscore_ev import (
        enrich_ev_ramp_config_with_zscore,
        evaluate_combined_live_ev_verification,
        resolve_shadow_ev_distribution,
    )

    base_cfg = ev_rampup_config(sys_config)
    cfg = enrich_ev_ramp_config_with_dynamic_tolerance(
        base_cfg, market, meta=meta, sys_config=sys_config
    )
    cfg = enrich_ev_ramp_config_with_zscore(cfg, sys_config=sys_config)
    dist = resolve_shadow_ev_distribution(warm_record)
    matched, detail = evaluate_combined_live_ev_verification(
        live_rets,
        dist.get("mean_pct"),
        dist.get("std_pct"),
        cfg,
    )
    return {
        "match": matched,
        "detail": detail,
        "shadow_distribution": dist,
        "market": str(market or "KR").upper(),
    }


def parse_health_key(key: str) -> Tuple[str, str]:
    s = str(key or "").strip()
    if "|" in s:
        mp, _, gk = s.partition("|")
        return mp.upper().strip() or "KR", gk.strip()
    return "KR", s


def profit_factor_from_returns(rets: List[float]) -> float:
    from reports.forward_report_scalar import profit_factor_from_returns as _pf_ssot

    return _pf_ssot(rets)


def _health_for_row(health: Dict[str, Any], market: str, group_key: str) -> Optional[Dict[str, Any]]:
    gk = str(group_key or "").strip()
    mp = str(market or "KR").upper()
    if not gk:
        return None
    full = f"{mp}|{gk}"
    hv = health.get(full)
    if isinstance(hv, dict):
        return hv
    for hk, v in health.items():
        if hk == "__meta__" or not isinstance(v, dict):
            continue
        m2, g2 = parse_health_key(hk)
        if g2 == gk and m2 == mp:
            return v
    return None


def passes_candidate_gate(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    n = int(hv.get("n", 0) or 0)
    if n < int(mp.get("candidate_min_trades", 15)):
        return False
    wr = float(hv.get("rolling_wr", 0) or 0)
    pf = float(hv.get("rolling_pf", 0) or 0)
    mdd = float(hv.get("mdd_pct", 0) or 0)
    if wr < float(mp.get("candidate_min_wr", 0.45)):
        return False
    if pf < float(mp.get("candidate_min_pf", 1.2)):
        return False
    if mdd < float(mp.get("candidate_max_mdd_pct", -28)):
        return False
    mult = float(hv.get("mult", 1.0) or 1.0)
    if mult <= 0.0:
        return False
    return True


def passes_live_hard_gate(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    """LIVE 승격 Hard Gate — 슬리피지 버퍼 반영."""
    n = int(hv.get("n", 0) or 0)
    if n < int(mp.get("promote_min_trades", 15)):
        return False
    wr = float(hv.get("rolling_wr", 0) or 0)
    pf = float(hv.get("rolling_pf", 0) or 0)
    mult = float(hv.get("mult", 1.0) or 1.0)
    if mult <= 0.0:
        return False

    live_wr = float(mp.get("live_min_wr", 0.50))
    mid_min = float(mp.get("live_wr_mid_min", 0.45))
    mid_max = float(mp.get("live_wr_mid_max", 0.499))
    mid_pf = float(mp.get("live_mid_min_pf", 1.50))
    pf_if_ok = float(mp.get("live_min_pf_if_wr_ok", 1.35))

    if wr >= live_wr:
        return pf >= pf_if_ok
    if mid_min <= wr <= mid_max:
        return pf >= mid_pf
    return False


def wf_warn_tag_enabled() -> bool:
    env = os.environ.get("WF_WARN_TAG_ENABLED")
    if env is not None:
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    return True


def wf_warn_telegram_enabled() -> bool:
    env = os.environ.get("WF_WARN_TELEGRAM_ENABLED")
    if env is not None:
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    return True


def walk_forward_promotion_block_enabled() -> bool:
    """
    V-2 scaffold — CANDIDATE→LIVE WF/OOS 차단. **기본 OFF** (4주 관측 후 디렉터 활성화).
    """
    env = os.environ.get("WALK_FORWARD_PROMOTION_BLOCK_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    return False


def should_block_live_promotion(
    row: Dict[str, Any],
    *,
    forward_db_path: Optional[str] = None,
    min_total_trades: int = 30,
) -> bool:
    """V-2 — wf_warn / WF OOS fail 시 LIVE 승격 스킵 (block ON 일 때만)."""
    if not walk_forward_promotion_block_enabled():
        return False
    meta = row.get("meta")
    if isinstance(meta, dict) and meta.get("wf_warn"):
        return True
    return evaluate_wf_oos_warn_for_group(
        str(row.get("market") or "KR"),
        str(row.get("group_key") or row.get("display_name") or ""),
        forward_db_path=forward_db_path,
        min_total_trades=min_total_trades,
    )


def annotate_wf_promotion_observation(
    row: Dict[str, Any],
    *,
    forward_db_path: Optional[str] = None,
    min_total_trades: int = 30,
) -> None:
    """관측용 meta — block OFF 일 때도 would_block 기록."""
    meta = row.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    would = evaluate_wf_oos_warn_for_group(
        str(row.get("market") or "KR"),
        str(row.get("group_key") or row.get("display_name") or ""),
        forward_db_path=forward_db_path,
        min_total_trades=min_total_trades,
    )
    meta["wf_warn"] = bool(meta.get("wf_warn")) or would
    meta["wf_would_block"] = would
    meta["wf_block_active"] = bool(
        walk_forward_promotion_block_enabled() and should_block_live_promotion(
            row, forward_db_path=forward_db_path, min_total_trades=min_total_trades
        )
    )
    row["meta"] = meta


def try_skip_live_promotion_for_wf_block(
    row: Dict[str, Any],
    stats: Dict[str, Any],
    *,
    forward_db_path: Optional[str] = None,
    min_total_trades: int = 30,
) -> bool:
    """True → caller must not promote to LIVE."""
    annotate_wf_promotion_observation(
        row, forward_db_path=forward_db_path, min_total_trades=min_total_trades
    )
    if not should_block_live_promotion(
        row, forward_db_path=forward_db_path, min_total_trades=min_total_trades
    ):
        return False
    meta = row.get("meta")
    if isinstance(meta, dict):
        meta["wf_promotion_skipped"] = True
    stats["wf_promotion_blocked"] = int(stats.get("wf_promotion_blocked") or 0) + 1
    return True


def _sig_to_group_key(sig: str) -> str:
    """meta_governor._ledger_group_key 와 동일 — import 순환 방지."""
    raw = str(sig or "")
    if "[INCUBATOR_" in raw.upper():
        m = re.search(r"\[INCUBATOR_([^\]]+)\]", raw, flags=re.I)
        if m:
            return f"INCUBATOR_{m.group(1).strip()}"
    s = raw.replace("💀[기각/관찰용] ", "").replace("💀[기각] ", "")
    s = re.sub(r"^\[.*?\]\s*", "", s)
    return (s.split(" [")[0].strip() or "UNKNOWN")


def fetch_group_closed_returns_decimal(
    market: str,
    group_key: str,
    db_path: Optional[str] = None,
) -> List[float]:
    """forward_trades CLOSED — chronological decimal returns for WF OOS."""
    mkt = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return []
    path = db_path
    if not path:
        try:
            from market_db_paths import market_db_read_path

            path = market_db_read_path()
        except Exception:
            return []
    if not path or not os.path.isfile(path):
        return []
    out: List[float] = []
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            cur = conn.execute(
                """
                SELECT sig_type, final_ret
                FROM forward_trades
                WHERE status LIKE 'CLOSED%'
                  AND UPPER(IFNULL(market,'KR')) = ?
                  AND final_ret IS NOT NULL
                ORDER BY IFNULL(exit_date,''), rowid
                """,
                (mkt,),
            )
            for sig, ret in cur.fetchall():
                if _sig_to_group_key(str(sig or "")) != gk:
                    continue
                try:
                    out.append(float(ret) / 100.0)
                except (TypeError, ValueError):
                    continue
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("fetch_group_closed_returns_decimal %s|%s: %s", mkt, gk, ex)
    return out


def evaluate_wf_oos_warn_for_group(
    market: str,
    group_key: str,
    *,
    forward_db_path: Optional[str] = None,
    min_total_trades: int = 30,
) -> bool:
    """V-1 — WF OOS fail → True (WARN tag only, no promotion block)."""
    from validation.walk_forward import evaluate_oos_pass_from_returns

    rets = fetch_group_closed_returns_decimal(market, group_key, forward_db_path)
    if len(rets) < int(min_total_trades):
        return False
    ev = evaluate_oos_pass_from_returns(
        rets,
        min_total_trades=int(min_total_trades),
    )
    return bool(not ev.get("pass") and ev.get("reason") == "oos_fail")


def apply_registry_meta_wf_warn(
    row: Dict[str, Any],
    *,
    forward_db_path: Optional[str] = None,
    min_total_trades: int = 30,
) -> bool:
    """registry row meta.wf_warn — 승격 판정 비접촉."""
    meta = row.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    warn = evaluate_wf_oos_warn_for_group(
        str(row.get("market") or "KR"),
        str(row.get("group_key") or row.get("display_name") or ""),
        forward_db_path=forward_db_path,
        min_total_trades=min_total_trades,
    )
    meta["wf_warn"] = bool(warn)
    row["meta"] = meta
    return warn


def stamp_registry_wf_warn_meta(
    rows: List[Dict[str, Any]],
    *,
    forward_db_path: Optional[str] = None,
    min_total_trades: int = 30,
) -> List[str]:
    """All registry rows — meta.wf_warn tags. Returns warned group keys."""
    if not wf_warn_tag_enabled():
        for row in rows:
            meta = row.get("meta")
            if not isinstance(meta, dict):
                meta = {}
            meta["wf_warn"] = False
            row["meta"] = meta
        return []
    warned: List[str] = []
    for row in rows:
        if apply_registry_meta_wf_warn(
            row,
            forward_db_path=forward_db_path,
            min_total_trades=min_total_trades,
        ):
            mk = str(row.get("market") or "KR").upper()
            gk = str(row.get("group_key") or "").strip()
            warned.append(f"{mk}|{gk}" if gk else mk)
    return warned


def notify_wf_warn_telegram(warned_groups: List[str]) -> bool:
    """V-1 — WF/OOS fail WARN telegram (no block)."""
    if not wf_warn_telegram_enabled() or not warned_groups:
        return False
    try:
        from deploy_watch import send_deploy_watch_telegram

        lines = [
            "🟡 <b>[WF_WARN]</b> V-1 promotion meta wf_warn (no LIVE block)",
        ]
        for g in warned_groups[:12]:
            lines.append(f"· <code>{g}</code>")
        if len(warned_groups) > 12:
            lines.append(f"· … +{len(warned_groups) - 12} more")
        return bool(send_deploy_watch_telegram("\n".join(lines)))
    except Exception as ex:
        logger.warning("wf_warn telegram skip: %s", ex)
        return False


_FAST_TRACK_PREFIXES = ("INCUBATOR_", "ACE_", "MUTANT_", "PLAYBOOK_", "HIDDEN_THEME_")


def is_fast_track_group(group_key: str) -> bool:
    """인큐베이터·ACE·뮤턴트 등 하이퍼-패스트트랙 대상 그룹."""
    gk = str(group_key or "").strip().upper()
    if not gk:
        return False
    if any(gk.startswith(p) for p in _FAST_TRACK_PREFIXES):
        return True
    if "ACE" in gk and ("PLAYBOOK" in gk or "EVOLUTION" in gk):
        return True
    return False


def passes_hard_threshold_auto_promotion(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    """
    Hard-Threshold Auto-Promotion — PF≥2.0 · 표본≥N 이면 WR 게이트 생략 LIVE.
    """
    if not bool(mp.get("fast_track_enabled", True)):
        return False
    n = int(hv.get("n", 0) or 0)
    n_min = int(mp.get("fast_track_min_trades", mp.get("promote_min_trades", 10)))
    pf_min = float(mp.get("fast_track_min_pf", 2.0))
    pf = float(hv.get("rolling_pf", 0) or 0)
    mult = float(hv.get("mult", 1.0) or 1.0)
    if mult <= 0.0:
        return False
    return n >= n_min and pf >= pf_min


def is_group_live_in_registry(
    meta: Optional[Dict[str, Any]],
    market: str,
    group_key: str,
) -> bool:
    """META_STRATEGY_REGISTRY 에 LIVE 로 등재된 그룹인지."""
    if not isinstance(meta, dict):
        return False
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return False
    reg = meta.get("META_STRATEGY_REGISTRY")
    if not isinstance(reg, list):
        return False
    for row in reg:
        if not isinstance(row, dict):
            continue
        if str(row.get("state") or "").upper() != "LIVE":
            continue
        if str(row.get("market") or "").upper() != mk:
            continue
        rg = str(row.get("group_key") or row.get("display_name") or "").strip()
        if rg == gk or gk in rg or rg in gk:
            return True
    return False


def is_below_live_threshold(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    """Whipsaw: rolling_wr 또는 rolling_pf 가 LIVE 최소 기준 미만."""
    wr = float(hv.get("rolling_wr", 0) or 0)
    pf = float(hv.get("rolling_pf", 0) or 0)
    live_wr = float(mp.get("live_min_wr", 0.50))
    mid_min = float(mp.get("live_wr_mid_min", 0.45))
    pf_if_ok = float(mp.get("live_min_pf_if_wr_ok", 1.35))
    mid_pf = float(mp.get("live_mid_min_pf", 1.50))

    if wr < mid_min:
        return True
    if wr >= live_wr:
        return pf < pf_if_ok
    if mid_min <= wr < live_wr:
        return pf < mid_pf
    return True


def _parse_iso_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _days_since(iso_val: Any, now: datetime) -> Optional[int]:
    dt = _parse_iso_dt(iso_val)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now_a = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return max(0, (now_a - dt.astimezone(timezone.utc)).days)


def _load_validated_promoted(path: Optional[str]) -> List[Dict[str, Any]]:
    import json
    import os

    p = path or ""
    if not p or not os.path.isfile(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        prom = raw.get("promoted") if isinstance(raw, dict) else None
        return [x for x in prom if isinstance(x, dict)] if isinstance(prom, list) else []
    except Exception as e:
        logger.warning("validated_live_mutants read failed: %s", e)
        return []


def _stable_mutant_id(name: str) -> str:
    h = hashlib.sha256(str(name).encode("utf-8")).hexdigest()[:14]
    return f"mutant:{h}"


def run_registry_lifecycle(
    *,
    prior_registry: List[Dict[str, Any]],
    health: Dict[str, Any],
    system_cfg: Optional[Dict[str, Any]] = None,
    validated_mutants_path: Optional[str] = None,
    forward_db_path: Optional[str] = None,
    meta_working: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Treasury 헬스 → Discovery → 일별 품질 기록 → 승격/강등/은퇴.
    반환: (registry 리스트, META_REGISTRY_CYCLE_STATS)
    """
    now = now or datetime.now(timezone.utc)
    now_iso = now.isoformat()
    lc_cfg = load_strategy_lifecycle_config(system_cfg)
    today_kst = _kst_today()

    db_rows = load_registry_rows(forward_db_path)
    reg = merge_registry_sources(db_rows, prior_registry)
    by_sid: Dict[str, Dict[str, Any]] = {
        str(r["strategy_id"]): dict(r) for r in reg if r.get("strategy_id")
    }

    stats: Dict[str, Any] = {
        "discovery_new": 0,
        "promoted_live": 0,
        "fast_track_promoted": 0,
        "demoted_cooled": 0,
        "retired": 0,
        "promoted_live_by_market": {"KR": 0, "US": 0, "BG": 0},
        "demoted_7d": 0,
        "re_evolution_redemption_promoted": 0,
        "re_evolution_warm_start_promoted": 0,
        "re_evolution_ev_full_ramp": 0,
        "re_evolution_ev_shadow_recall": 0,
        "lifecycle_observe_redemption_promoted": 0,
        "today_kst": today_kst,
    }

    # --- mutant CANDIDATE 입성 ---
    for prom in _load_validated_promoted(validated_mutants_path):
        name = str(prom.get("name") or "").strip()
        if not name:
            continue
        sid = _stable_mutant_id(name)
        if sid in by_sid:
            continue
        gk = f"INCUBATOR_{name}"
        row = {
            "strategy_id": sid,
            "market": "KR",
            "display_name": name,
            "group_key": gk,
            "state": "CANDIDATE",
            "capital_mult": 0.0,
            "source": "validated_live_mutants",
            "updated_at": now_iso,
            "oos_win_rate": prom.get("oos_win_rate"),
            "oos_avg_return": prom.get("oos_avg_return"),
            "regime_tag": prom.get("regime_tag"),
        }
        by_sid[sid] = row
        stats["discovery_new"] += 1

    # --- Discovery: health → OBSERVING / CANDIDATE ---
    if isinstance(health, dict):
        for hk, hv in health.items():
            if hk == "__meta__" or not isinstance(hv, dict):
                continue
            mkt, gk = parse_health_key(hk)
            if not gk:
                continue
            mp = market_params(lc_cfg, mkt)
            sid = stable_strategy_id(mkt, gk)
            if sid in by_sid:
                row = by_sid[sid]
                row["market"] = mkt
                row["group_key"] = gk
                row["rolling_wr"] = hv.get("rolling_wr")
                row["rolling_pf"] = hv.get("rolling_pf")
                row["n_closed"] = hv.get("n")
            else:
                if not passes_candidate_gate(hv, mp):
                    continue
                row = {
                    "strategy_id": sid,
                    "market": mkt,
                    "group_key": gk,
                    "display_name": gk,
                    "state": "OBSERVING",
                    "capital_mult": 0.0,
                    "source": "health_discovery",
                    "rolling_wr": hv.get("rolling_wr"),
                    "rolling_pf": hv.get("rolling_pf"),
                    "n_closed": hv.get("n"),
                    "updated_at": now_iso,
                }
                by_sid[sid] = row
                stats["discovery_new"] += 1

            st = str(row.get("state") or "").upper()
            try:
                from re_evolution_redemption_gate import is_re_evolution_observing_row

                _re_evol_obs = is_re_evolution_observing_row(row)
            except Exception:
                _re_evol_obs = False
            if (
                st in ("OBSERVING", "")
                and passes_candidate_gate(hv, mp)
                and not _re_evol_obs
            ):
                row["state"] = "CANDIDATE"
                row["updated_at"] = now_iso
                if not row.get("promote_reason"):
                    row["promote_reason"] = "discovery_candidate"

    # --- 일별 품질 스냅샷 + LIVE 처리 ---
    demote_cutoff = now - timedelta(days=7)
    for sid, row in list(by_sid.items()):
        mkt = str(row.get("market") or "KR").upper()
        gk = str(row.get("group_key") or row.get("display_name") or "").strip()
        if not gk and "|" in sid:
            continue
        mp = market_params(lc_cfg, mkt)
        hv = _health_for_row(health if isinstance(health, dict) else {}, mkt, gk)
        if hv:
            row["rolling_wr"] = hv.get("rolling_wr")
            row["rolling_pf"] = hv.get("rolling_pf")
            row["n_closed"] = hv.get("n")
            below = is_below_live_threshold(hv, mp)
            record_quality_daily(
                sid,
                mkt,
                rolling_wr=float(hv.get("rolling_wr") or 0),
                rolling_pf=float(hv.get("rolling_pf") or 0),
                below_live_threshold=below,
                trade_date=today_kst,
            )
            row["health_miss_streak"] = consecutive_below_live_days(sid)

        st = str(row.get("state") or "").upper()

        try:
            from lifecycle_observe_only import ensure_lifecycle_observe_only_stamp

            ensure_lifecycle_observe_only_stamp(row, now_iso)
        except Exception:
            pass

        # --- Re-Evolution Phase 3: 섀도우 부활전 → LIVE ---
        if st == "OBSERVING":
            try:
                from re_evolution_redemption_gate import try_promote_re_evolution_redemption

                promoted, _rev = try_promote_re_evolution_redemption(
                    row,
                    meta=meta_working,
                    sys_config=system_cfg,
                    forward_db_path=forward_db_path,
                    now=now,
                )
                if promoted:
                    if try_skip_live_promotion_for_wf_block(
                        row, stats, forward_db_path=forward_db_path
                    ):
                        row["state"] = "OBSERVING"
                        row["capital_mult"] = 0.0
                        continue
                    stats["re_evolution_redemption_promoted"] += 1
                    if _rev.get("warm_start_applied"):
                        stats["re_evolution_warm_start_promoted"] = (
                            int(stats.get("re_evolution_warm_start_promoted") or 0) + 1
                        )
                        _dyn_tol = None
                        if isinstance(meta_working, dict):
                            try:
                                _dyn_tol = resolve_live_ev_verification_tolerance(
                                    mkt,
                                    meta=meta_working,
                                    sys_config=system_cfg,
                                )
                                meta_working["META_RE_EVOLUTION_LAST_DYNAMIC_TOLERANCE"] = _dyn_tol
                            except Exception:
                                pass
                        if isinstance(_dyn_tol, dict):
                            stats.setdefault("re_evolution_dynamic_tolerance_last", _dyn_tol)
                    stats["promoted_live"] += 1
                    mk_stat = mkt if mkt in stats["promoted_live_by_market"] else "KR"
                    stats["promoted_live_by_market"][mk_stat] = (
                        stats["promoted_live_by_market"].get(mk_stat, 0) + 1
                    )
                    continue
            except Exception as ex:
                logger.warning("re_evolution redemption skip %s: %s", gk, ex)

        # --- Hard-Threshold Auto-Promotion (인큐베이터·ACE) ---
        if (
            hv
            and is_fast_track_group(gk)
            and st in ("OBSERVING", "CANDIDATE", "COOLED", "")
            and passes_hard_threshold_auto_promotion(hv, mp)
        ):
            if try_skip_live_promotion_for_wf_block(
                row, stats, forward_db_path=forward_db_path
            ):
                continue
            row["state"] = "LIVE"
            row["capital_mult"] = 1.0
            row["promoted_at"] = row.get("promoted_at") or now_iso
            row["last_promoted_at"] = now_iso
            row["promote_reason"] = "fast_track_pf2"
            row["updated_at"] = now_iso
            row["demote_reason"] = None
            row["observe_only_released"] = True
            stats["promoted_live"] += 1
            stats["fast_track_promoted"] += 1
            mk_stat = mkt if mkt in stats["promoted_live_by_market"] else "KR"
            stats["promoted_live_by_market"][mk_stat] = (
                stats["promoted_live_by_market"].get(mk_stat, 0) + 1
            )
            continue

        if st == "LIVE" and hv:
            # Alpha TTL
            age = _days_since(row.get("promoted_at") or row.get("last_promoted_at"), now)
            half_life = int(mp.get("alpha_half_life_days", 10))
            whipsaw_need = int(mp.get("whipsaw_below_days", 2))
            streak = int(row.get("health_miss_streak", 0) or 0)
            hard_mult = float(hv.get("mult", 1.0) or 1.0) <= 0.0

            demote = False
            reason = ""
            if hard_mult:
                demote = True
                reason = "treasury_hard_cut"
            elif age is not None and age > half_life:
                demote = True
                reason = "alpha_half_life"
            elif streak >= whipsaw_need:
                demote = True
                reason = f"whipsaw_below_live_{streak}d"

            if demote:
                row["state"] = "COOLED"
                row["capital_mult"] = 0.0
                row["last_demoted_at"] = now_iso
                row["demote_reason"] = reason
                row["updated_at"] = now_iso
                stats["demoted_cooled"] += 1
                dt = _parse_iso_dt(row.get("last_demoted_at"))
                if dt and dt >= demote_cutoff.replace(tzinfo=timezone.utc):
                    stats["demoted_7d"] += 1

        elif st == "CANDIDATE" and hv and passes_live_hard_gate(hv, mp):
            if try_skip_live_promotion_for_wf_block(
                row, stats, forward_db_path=forward_db_path
            ):
                continue
            row["state"] = "LIVE"
            row["capital_mult"] = 1.0
            row["promoted_at"] = row.get("promoted_at") or now_iso
            row["last_promoted_at"] = now_iso
            row["promote_reason"] = "live_hard_gate"
            row["updated_at"] = now_iso
            row["demote_reason"] = None
            stats["promoted_live"] += 1
            mk = mkt if mkt in stats["promoted_live_by_market"] else "KR"
            stats["promoted_live_by_market"][mk] = stats["promoted_live_by_market"].get(mk, 0) + 1

        elif st == "COOLED":
            try:
                from lifecycle_observe_only import try_promote_lifecycle_observe_only_redemption

                _lo_promoted, _lo_rev = try_promote_lifecycle_observe_only_redemption(
                    row,
                    meta=meta_working,
                    sys_config=system_cfg,
                    forward_db_path=forward_db_path,
                    now=now,
                    now_iso=now_iso,
                )
                if _lo_promoted:
                    stats["lifecycle_observe_redemption_promoted"] += 1
                    continue
            except Exception as ex:
                logger.warning("lifecycle observe redemption skip %s: %s", gk, ex)

            cooloff = int(mp.get("cooloff_days", 3))
            since_dem = _days_since(row.get("last_demoted_at"), now)
            if since_dem is not None and since_dem >= cooloff:
                if hv and passes_live_hard_gate(hv, mp):
                    if try_skip_live_promotion_for_wf_block(
                        row, stats, forward_db_path=forward_db_path
                    ):
                        continue
                    row["state"] = "CANDIDATE"
                    row["capital_mult"] = 0.0
                    row["promote_reason"] = "recovery_reobserve"
                    row["updated_at"] = now_iso
                else:
                    row["state"] = "RETIRED"
                    row["capital_mult"] = 0.0
                    row["demote_reason"] = "cooloff_expired"
                    row["updated_at"] = now_iso
                    stats["retired"] += 1

        elif st == "RETIRED":
            try:
                from lifecycle_observe_only import try_promote_lifecycle_observe_only_redemption

                _lo_promoted, _lo_rev = try_promote_lifecycle_observe_only_redemption(
                    row,
                    meta=meta_working,
                    sys_config=system_cfg,
                    forward_db_path=forward_db_path,
                    now=now,
                    now_iso=now_iso,
                )
                if _lo_promoted:
                    stats["lifecycle_observe_redemption_promoted"] += 1
                    continue
            except Exception as ex:
                logger.warning("lifecycle observe redemption skip %s: %s", gk, ex)

    out = list(by_sid.values())

    wf_warned = stamp_registry_wf_warn_meta(
        out,
        forward_db_path=forward_db_path,
        min_total_trades=30,
    )
    stats["wf_warn_count"] = len(wf_warned)
    if wf_warned:
        stats["wf_warn_groups"] = wf_warned
        stats["wf_warn_telegram_sent"] = notify_wf_warn_telegram(wf_warned)
    else:
        stats["wf_warn_telegram_sent"] = False

    upsert_registry_rows(out, forward_db_path)

    # 집계
    counts = {"OBSERVING": 0, "CANDIDATE": 0, "LIVE": 0, "COOLED": 0, "RETIRED": 0}
    by_market: Dict[str, Dict[str, int]] = {}
    for r in out:
        st = str(r.get("state") or "").upper()
        counts[st] = counts.get(st, 0) + 1
        mk = str(r.get("market") or "KR").upper()
        if mk not in by_market:
            by_market[mk] = {"LIVE": 0, "CANDIDATE": 0, "COOLED": 0, "OBSERVING": 0, "RETIRED": 0}
        if st in by_market[mk]:
            by_market[mk][st] += 1

    stats["counts"] = counts
    stats["by_market"] = by_market
    stats["n_registry"] = len(out)
    return out, stats
