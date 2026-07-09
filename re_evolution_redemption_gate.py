"""
Re-Evolution Phase 3 — Redemption & Promotion (패자 부활전 게이트).

OBSERVING(3-Strike 섀도우) 로직이 동적 섀도우 검증 윈도우에서 벤치마크 대비 알파 +
승률을 회복하면 LIVE 승격 및 Kelly/자본 복원.

평가 SSOT:
  · forward_trades CLOSED + RE_EVOL_SHADOW / OBSERVE_ONLY 태그
  · 시장별 alpha_half_life 기반 Base Window (70~100%)
  · META_REGIME_KEY 국면별 Time Dilation (HIGH_VOL/BEAR_PANIC ×0.5, SIDEWAYS ×1.0)
  · regime_logic_crossmatrix MIN_SAMPLES 통계적 유의성 허들 (이중 게이트)
  · rolling win rate · PF · 누적 수익 vs SPY/^KS11
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from re_evolution_strike_guard import (
    _load_shadow_set,
    _load_strike_map,
    _strike_bucket_key,
    extract_core_group_name,
)
from strategy_lifecycle_config import compute_dynamic_shadow_verification_window

logger = logging.getLogger(__name__)

_SHADOW_TAG = "RE_EVOL_SHADOW"


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def re_evolution_redemption_config(
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    block = cfg.get("RE_EVOLUTION_REDEMPTION") or {}
    base = block if isinstance(block, dict) else cfg

    def _i(key: str, default: int) -> int:
        try:
            return int(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "enabled": _cfg_bool(cfg, "ENABLE_RE_EVOLUTION_REDEMPTION", True),
        "min_trades": _i("RE_EVOLUTION_REDEMPTION_MIN_TRADES", 5),
        "min_wr": _f("RE_EVOLUTION_REDEMPTION_MIN_WR", 0.52),
        "min_pf": _f("RE_EVOLUTION_REDEMPTION_MIN_PF", 1.25),
        "min_alpha_pct": _f("RE_EVOLUTION_REDEMPTION_MIN_ALPHA_PCT", 1.0),
        ** _redemption_sample_thresholds(sys_config),
    }


def _redemption_sample_thresholds(
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """섀도우 표본 허들 — redemption min_trades × crossmatrix MIN_SAMPLES 이중 게이트."""
    try:
        from evolution.regime_logic_crossmatrix import resolve_regime_crossmatrix_min_samples

        regime_min = resolve_regime_crossmatrix_min_samples(sys_config)
    except Exception:
        regime_min = 15

    cfg = sys_config if isinstance(sys_config, dict) else {}
    block = cfg.get("RE_EVOLUTION_REDEMPTION") or {}
    base = block if isinstance(block, dict) else cfg
    try:
        redeem_min = int(
            base.get(
                "RE_EVOLUTION_REDEMPTION_MIN_TRADES",
                cfg.get("RE_EVOLUTION_REDEMPTION_MIN_TRADES", 5),
            )
        )
    except (TypeError, ValueError):
        redeem_min = 5

    effective = max(redeem_min, regime_min)
    return {
        "min_samples_regime": int(regime_min),
        "effective_min_trades": int(effective),
    }


def resolve_shadow_demoted_at(
    *,
    market: str,
    group_key: str,
    row: Optional[Mapping[str, Any]] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> Optional[str]:
    """섀도우 강등 시각 — registry row → META_RE_EVOLUTION_STRIKES 순."""
    if isinstance(row, Mapping):
        for key in ("last_demoted_at", "demoted_at", "re_evolution_demoted_at"):
            val = row.get(key)
            if val:
                return str(val)

    if isinstance(meta, Mapping):
        mk = str(market or "KR").upper()
        gk = str(group_key or "").strip()
        bk = _strike_bucket_key(mk, gk)
        rec = _load_strike_map(meta).get(bk)
        if isinstance(rec, dict):
            for key in ("demoted_at", "last_loss_at"):
                val = rec.get(key)
                if val:
                    return str(val)
        for item in meta.get("META_RE_EVOLUTION_DEMOTED") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("group_key") or "").strip() != gk:
                continue
            if str(item.get("market") or "").upper() != mk:
                continue
            val = item.get("demoted_at")
            if val:
                return str(val)
    return None


def resolve_dynamic_shadow_verification_window(
    market: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    row: Optional[Mapping[str, Any]] = None,
    meta: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """alpha_half_life Base Window + META_REGIME_KEY 국면 Time Dilation."""
    mk = str(market or "KR").upper()
    gk = str((row or {}).get("group_key") or (row or {}).get("display_name") or "").strip()
    demoted_at = resolve_shadow_demoted_at(market=mk, group_key=gk, row=row, meta=meta)
    window = compute_dynamic_shadow_verification_window(
        mk,
        system_cfg=sys_config,
        demoted_at_iso=demoted_at,
        now=now,
        meta=meta,
    )
    window["demoted_at"] = demoted_at
    return window


def is_re_evolution_observing_row(row: Mapping[str, Any]) -> bool:
    """3-Strike 강등 OBSERVING 행 여부."""
    st = str(row.get("state") or "").upper()
    if st != "OBSERVING":
        return False
    demote = str(row.get("demote_reason") or "")
    src = str(row.get("source") or "")
    if demote.startswith("re_evolution_3_strike"):
        return True
    if src == "re_evolution_strike":
        return True
    return False


def fetch_benchmark_return_pct(
    market: str,
    *,
    lookback_days: int = 7,
) -> Optional[float]:
    """SPY / ^KS11 최근 N거래일 누적 수익률(%)."""
    sym = "^KS11" if str(market or "").upper() == "KR" else "SPY"
    days = max(2, int(lookback_days))
    try:
        import yfinance as yf

        hist = yf.Ticker(sym).history(period="2mo", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        c = hist["Close"].astype(float).dropna()
        if len(c) < 2:
            return None
        tail = c.iloc[-days:] if len(c) >= days else c
        ret = (float(tail.iloc[-1]) / float(tail.iloc[0]) - 1.0) * 100.0
        return float(ret)
    except Exception:
        return None


def _default_db_path() -> Optional[str]:
    try:
        from market_db_paths import market_db_read_path

        return market_db_read_path()
    except Exception:
        try:
            from market_db_paths import MARKET_DATA_DB_PATH

            return MARKET_DATA_DB_PATH
        except Exception:
            return None


def fetch_shadow_closed_rows(
    market: str,
    group_key: str,
    *,
    lookback_days: int = 7,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """섀도우(RE_EVOL_SHADOW) 청산 — exit_date 기준 N일 롤링."""
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return []

    path = db_path or _default_db_path()
    if not path or not os.path.isfile(path):
        return []

    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    like_g = f"%{gk}%"
    like_shadow = f"%{_SHADOW_TAG}%"
    like_observe = "%OBSERVE_ONLY%"

    try:
        conn = sqlite3.connect(path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                """
                SELECT sig_type, final_ret, exit_date, entry_date,
                       invest_amount, sim_kelly_invest
                FROM forward_trades
                WHERE UPPER(TRIM(market)) = ?
                  AND status LIKE 'CLOSED%%'
                  AND final_ret IS NOT NULL
                  AND (
                    IFNULL(sig_type,'') LIKE ?
                    OR (IFNULL(sig_type,'') LIKE ? AND IFNULL(sig_type,'') LIKE ?)
                  )
                  AND COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) >= ?
                ORDER BY rowid DESC
                """,
                (mk, like_shadow, like_observe, like_g, cutoff),
            )
            rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as ex:
        logger.warning("fetch_shadow_closed_rows failed: %s", ex)
        return []

    out: List[Dict[str, Any]] = []
    for row in rows:
        sig = str(row.get("sig_type") or "")
        core = extract_core_group_name(sig)
        if gk not in core and core != gk and gk not in sig:
            continue
        out.append(row)
    return out


def compute_shadow_stats(closed_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """섀도우 청산 집계 — WR, PF, equal-weight avg return."""
    from strategy_promotion_engine import profit_factor_from_returns

    rets: List[float] = []
    for row in closed_rows:
        try:
            rets.append(float(row.get("final_ret") or 0.0))
        except (TypeError, ValueError):
            continue

    n = len(rets)
    if n == 0:
        return {
            "n_closed": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_ret_pct": 0.0,
            "cum_ret_pct": 0.0,
        }

    wins = sum(1 for r in rets if r > 0.0)
    wr = wins / n
    pf = profit_factor_from_returns(rets)
    avg = sum(rets) / n
    cum = sum(rets)

    return {
        "n_closed": n,
        "win_rate": round(wr, 4),
        "profit_factor": round(float(pf), 4),
        "avg_ret_pct": round(avg, 4),
        "cum_ret_pct": round(cum, 4),
    }


def passes_redemption_gate(
    shadow_stats: Mapping[str, Any],
    benchmark_ret_pct: Optional[float],
    cfg: Mapping[str, Any],
    *,
    verification_window: Optional[Mapping[str, Any]] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    섀도우 부활전 최종 게이트 — 이중 허들 + 품질 + 알파.

    허들 순서:
      1) 동적 검증 윈도우 내 표본 수 ≥ effective_min_trades
         (max(redemption_min_trades, regime_logic_crossmatrix MIN_SAMPLES))
      2) win rate · profit factor
      3) benchmark 대비 alpha excess
    """
    n = int(shadow_stats.get("n_closed") or 0)
    wr = float(shadow_stats.get("win_rate") or 0.0)
    pf = float(shadow_stats.get("profit_factor") or 0.0)
    strat_ret = float(shadow_stats.get("avg_ret_pct") or 0.0)

    min_trades_soft = int(cfg.get("min_trades") or 5)
    min_samples_regime = int(cfg.get("min_samples_regime") or 15)
    effective_min_n = int(cfg.get("effective_min_trades") or max(min_trades_soft, min_samples_regime))
    min_wr = float(cfg.get("min_wr") or 0.52)
    min_pf = float(cfg.get("min_pf") or 1.25)
    min_alpha = float(cfg.get("min_alpha_pct") or 1.0)

    bench = float(benchmark_ret_pct) if benchmark_ret_pct is not None else None
    alpha = (strat_ret - bench) if bench is not None else None

    vw = verification_window if isinstance(verification_window, Mapping) else {}
    detail = {
        "n_closed": n,
        "win_rate": wr,
        "profit_factor": pf,
        "strategy_avg_ret_pct": strat_ret,
        "benchmark_ret_pct": bench,
        "alpha_excess_pct": alpha,
        "min_trades": min_trades_soft,
        "min_samples_regime": min_samples_regime,
        "effective_min_trades": effective_min_n,
        "min_wr": min_wr,
        "min_pf": min_pf,
        "min_alpha_pct": min_alpha,
        "verification_window_days": vw.get("verification_window_days"),
        "base_window_days": vw.get("base_window_days"),
        "dilation_mode": vw.get("dilation_mode"),
        "regime_key": vw.get("regime_key"),
    }

    if n < min_samples_regime:
        detail["fail"] = "regime_min_samples"
        detail["fail_detail"] = (
            f"shadow_n={n} < crossmatrix_MIN_SAMPLES={min_samples_regime}"
        )
        return False, detail
    if n < min_trades_soft:
        detail["fail"] = "insufficient_trades"
        detail["fail_detail"] = f"shadow_n={n} < redemption_min_trades={min_trades_soft}"
        return False, detail
    if n < effective_min_n:
        detail["fail"] = "insufficient_effective_trades"
        detail["fail_detail"] = f"shadow_n={n} < effective_min={effective_min_n}"
        return False, detail
    if wr < min_wr:
        detail["fail"] = "win_rate"
        return False, detail
    if pf < min_pf:
        detail["fail"] = "profit_factor"
        return False, detail
    if bench is None:
        detail["fail"] = "benchmark_unavailable"
        return False, detail
    if alpha is None or alpha < min_alpha:
        detail["fail"] = "alpha_excess"
        return False, detail

    detail["pass"] = True
    detail["gates_passed"] = [
        "dynamic_window_sample_size",
        "regime_min_samples",
        "win_rate",
        "profit_factor",
        "alpha_excess",
    ]
    return True, detail


def restore_redemption_capital_overlay(meta: Dict[str, Any], group_key: str) -> None:
    """Kelly overlay 0 해제 — health 기반 배수 복원."""
    gk = str(group_key or "").strip()
    if not gk:
        return

    re_overlay = dict(meta.get("META_RE_EVOLUTION_KELLY_OVERLAY") or {})
    re_overlay.pop(gk, None)
    meta["META_RE_EVOLUTION_KELLY_OVERLAY"] = re_overlay

    dm = dict(meta.get("META_DEATHMATCH_KELLY_OVERLAY") or {})
    if gk in dm and float(dm.get(gk) or 1.0) <= 0.0:
        dm.pop(gk, None)
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = dm

    try:
        from evolution.deathmatch_allocation import (
            health_to_group_mult,
            merge_group_kelly_from_overlay,
        )

        health_mult = health_to_group_mult(meta.get("META_STRATEGY_HEALTH") or {})
        cap = 1.5
        meta["META_GROUP_KELLY_MULT"] = merge_group_kelly_from_overlay(
            health_mult, dm, max_mult=cap
        )
    except Exception as ex:
        logger.warning("redemption kelly restore skip: %s", ex)


def apply_redemption_meta_updates(
    meta: Dict[str, Any],
    *,
    market: str,
    group_key: str,
    strategy_id: str,
    gate_detail: Mapping[str, Any],
    now_iso: Optional[str] = None,
) -> None:
    """섀도우 집합·strike·demoted 로그 갱신 (in-place)."""
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    sid = str(strategy_id or "").strip()
    now_iso = now_iso or datetime.now(timezone.utc).isoformat()

    shadow = _load_shadow_set(meta)
    if gk in shadow:
        meta["META_RE_EVOLUTION_SHADOW_GROUPS"] = sorted(shadow - {gk})

    strikes_map = _load_strike_map(meta)
    bk = _strike_bucket_key(mk, gk)
    rec = dict(strikes_map.get(bk) or {})
    rec.update(
        {
            "strategy_id": sid,
            "market": mk,
            "group_key": gk,
            "demoted": False,
            "redeemed_at": now_iso,
            "consecutive_strikes": 0,
        }
    )
    strikes_map[bk] = rec
    meta["META_RE_EVOLUTION_STRIKES"] = strikes_map

    demoted: List[Dict[str, Any]] = list(meta.get("META_RE_EVOLUTION_DEMOTED") or [])
    for item in demoted:
        if not isinstance(item, dict):
            continue
        if str(item.get("group_key") or "").strip() != gk:
            continue
        if str(item.get("market") or "").upper() != mk:
            continue
        item["redeemed_at"] = now_iso
        item["redemption_gate"] = dict(gate_detail)
    meta["META_RE_EVOLUTION_DEMOTED"] = demoted

    log: List[Dict[str, Any]] = list(meta.get("META_RE_EVOLUTION_REDEMPTION_LOG") or [])
    log.append(
        {
            "strategy_id": sid,
            "market": mk,
            "group_key": gk,
            "redeemed_at": now_iso,
            "gate": dict(gate_detail),
        }
    )
    meta["META_RE_EVOLUTION_REDEMPTION_LOG"] = log[-50:]
    meta["META_RE_EVOLUTION_LAST_REDEMPTION_AT"] = now_iso

    restore_redemption_capital_overlay(meta, gk)


def evaluate_shadow_redemption(
    *,
    market: str,
    group_key: str,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
    row: Optional[Mapping[str, Any]] = None,
    meta: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """단일 그룹 섀도우 부활전 평가 (부작용 없음)."""
    cfg = re_evolution_redemption_config(sys_config)
    if not cfg["enabled"]:
        return {"eligible": False, "reason": "disabled"}

    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    eval_row = dict(row or {})
    if gk and not eval_row.get("group_key"):
        eval_row["group_key"] = gk

    window = resolve_dynamic_shadow_verification_window(
        mk,
        sys_config=sys_config,
        row=eval_row,
        meta=meta,
        now=now,
    )
    lookback = int(
        window.get("verification_window_days")
        or window.get("final_window_days")
        or window["base_window_days"]
    )

    rows = fetch_shadow_closed_rows(
        mk, gk, lookback_days=lookback, db_path=forward_db_path
    )
    stats = compute_shadow_stats(rows)
    bench = fetch_benchmark_return_pct(mk, lookback_days=lookback)
    ok, detail = passes_redemption_gate(
        stats, bench, cfg, verification_window=window
    )
    detail["verification_window"] = window

    return {
        "eligible": True,
        "market": mk,
        "group_key": gk,
        "passes": ok,
        "shadow_stats": stats,
        "gate_detail": detail,
        "lookback_days": lookback,
        "verification_window": window,
    }


def try_promote_re_evolution_redemption(
    row: Dict[str, Any],
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
    now: Optional[datetime] = None,
    now_iso: Optional[str] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    OBSERVING re_evolution 행 → LIVE 승격 시도.
    성공 시 registry row·meta in-place 갱신.
    """
    if not is_re_evolution_observing_row(row):
        return False, {"reason": "not_re_evolution_observing"}

    cfg = re_evolution_redemption_config(sys_config)
    if not cfg["enabled"]:
        return False, {"reason": "disabled"}

    now_dt = now
    if now_dt is None and now_iso:
        try:
            s = str(now_iso).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            now_dt = datetime.fromisoformat(s)
            if now_dt.tzinfo is None:
                now_dt = now_dt.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            now_dt = None
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()

    mk = str(row.get("market") or "KR").upper()
    gk = str(row.get("group_key") or row.get("display_name") or "").strip()
    sid = str(row.get("strategy_id") or "").strip()

    ev = evaluate_shadow_redemption(
        market=mk,
        group_key=gk,
        sys_config=sys_config,
        forward_db_path=forward_db_path,
        row=row,
        meta=meta,
        now=now_dt,
    )
    if not ev.get("passes"):
        return False, ev

    row["state"] = "LIVE"
    row["capital_mult"] = 1.0
    row["promoted_at"] = row.get("promoted_at") or now_iso
    row["last_promoted_at"] = now_iso
    row["promote_reason"] = "re_evolution_redemption"
    row["demote_reason"] = None
    row["observe_only_released"] = True
    row["re_evolution_redeemed_at"] = now_iso
    row["updated_at"] = now_iso

    if isinstance(meta, dict):
        apply_redemption_meta_updates(
            meta,
            market=mk,
            group_key=gk,
            strategy_id=sid,
            gate_detail=ev.get("gate_detail") or {},
            now_iso=now_iso,
        )

    logger.info(
        "Re-Evolution redemption LIVE: %s %s (n=%s wr=%.2f alpha=%s)",
        mk,
        gk,
        (ev.get("gate_detail") or {}).get("n_closed"),
        float((ev.get("gate_detail") or {}).get("win_rate") or 0.0),
        (ev.get("gate_detail") or {}).get("alpha_excess_pct"),
    )
    return True, ev
