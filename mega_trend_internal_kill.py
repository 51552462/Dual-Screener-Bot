"""
Mega-Trend Internal Momentum Kill-Switch (내부 1번 실행).

internal_diagnostics 에서 momentum_lost 자가진단 시:
  · Correlation Forgiveness 즉시 박탈
  · defensive_exit 유체 청산
  · MEGA_TREND 언락 해제

연동: mega_trend_internal_monitor · mega_trend_toxic_kill · mega_trend_climax
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional

from exit_dynamics import MEGA_TREND_INTERNAL_DIAG_KEY
from mega_trend_climax import (
    MEGA_TREND_CLIMAX_EXIT_TAG,
    _deactivate_mega_trend_state,
    liquidate_mega_trend_sector_positions,
)
from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    load_mega_trend_state,
    mega_trend_unlock_enabled,
)
from mega_trend_toxic_kill import (
    FORGIVENESS_REVOKED_KEY,
    resolve_defensive_exit_fraction,
    revoke_mega_trend_correlation_forgiveness,
)


def internal_momentum_kill_enabled() -> bool:
    raw = os.environ.get("ENABLE_MEGA_TREND_INTERNAL_KILL", "1")
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def evaluate_mega_trend_internal_momentum_kill(
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    [1번 실행] internal_diagnostics 기반 동력 상실 킬 판정.
    """
    state = load_mega_trend_state(config)
    if not state.get("active"):
        return {"kill": False, "reason": "mega_trend_inactive"}

    diag = state.get(MEGA_TREND_INTERNAL_DIAG_KEY)
    if not isinstance(diag, Mapping):
        return {"kill": False, "reason": "no_internal_diagnostics"}

    if not diag.get("any_momentum_lost"):
        return {"kill": False, "reason": "internal_momentum_ok"}

    lost_sectors = [str(s) for s in (diag.get("momentum_lost_sectors") or []) if s]
    if not lost_sectors:
        return {"kill": False, "reason": "no_momentum_lost_sectors"}

    sector_diags = diag.get("sectors") if isinstance(diag.get("sectors"), Mapping) else {}
    primary = state.get("primary_sector")
    kill_sector: Optional[str] = None
    if primary and str(primary) in lost_sectors:
        kill_sector = str(primary)
    else:
        kill_sector = lost_sectors[0]

    sector_verdicts: List[Dict[str, Any]] = []
    for sec in lost_sectors:
        sec_diag = sector_diags.get(sec) if isinstance(sector_diags, Mapping) else {}
        if not isinstance(sec_diag, Mapping):
            sec_diag = {}
        sector_verdicts.append(
            {
                "sector": sec,
                "kill": True,
                "momentum_lost": True,
                "triggers": list(sec_diag.get("triggers") or []),
                "metrics": sec_diag.get("metrics"),
                "reason": sec_diag.get("reason") or "internal_momentum_lost",
            }
        )

    primary_diag = sector_diags.get(kill_sector) if isinstance(sector_diags, Mapping) else {}
    if not isinstance(primary_diag, Mapping):
        primary_diag = {}
    reason = str(
        primary_diag.get("reason")
        or f"internal_momentum_lost: {lost_sectors}"
    )

    return {
        "kill": True,
        "exit_mode": "defensive_exit",
        "sector": kill_sector,
        "sectors": lost_sectors,
        "reason": reason,
        "sector_verdicts": sector_verdicts,
        "diagnostics": dict(diag),
    }


def refresh_mega_trend_internal_momentum_kill(
    config: Dict[str, Any],
    *,
    save_config_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """
    [1번 실행] 내부 동력 상실 킬스위치 — 면죄부 박탈 + defensive_exit + 언락 해제.
    """
    if not mega_trend_unlock_enabled() or not internal_momentum_kill_enabled():
        return {"kill": False, "reason": "disabled"}

    state = load_mega_trend_state(config)
    if not state.get("active"):
        return {"kill": False, "reason": "mega_trend_inactive"}

    verdict = evaluate_mega_trend_internal_momentum_kill(config)
    if not verdict.get("kill"):
        return verdict

    own_conn = False
    c = conn
    if c is None:
        try:
            import auto_forward_tester as aft

            c = sqlite3.connect(aft.DB_PATH, timeout=30)
            own_conn = True
        except Exception:
            c = None

    try:
        state = dict(load_mega_trend_state(config))
        state = revoke_mega_trend_correlation_forgiveness(
            state, reason=str(verdict.get("reason") or "internal_momentum_kill")
        )
        now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        kill_ignited_at = state.get("ignited_at")
        state["internal_momentum_kill_at"] = now_s
        state["internal_momentum_kill_reason"] = verdict.get("reason")
        state = _deactivate_mega_trend_state(state, verdict)
        config[MEGA_TREND_CONFIG_KEY] = state

        frac = resolve_defensive_exit_fraction()
        liq: Dict[str, Any] = {"liquidated": 0, "scaled": 0}
        if c is not None:
            liq = liquidate_mega_trend_sector_positions(
                c,
                verdict.get("sectors") or [verdict.get("sector")],
                exit_mode="defensive_exit",
                exit_reason=(
                    f"{MEGA_TREND_CLIMAX_EXIT_TAG}_INTERNAL: {verdict.get('reason', '')}"
                ),
            )
            liq["defensive_exit_fraction"] = frac

        if save_config_fn:
            save_config_fn(config)

        try:
            from mega_trend_kill_rl import record_mega_trend_kill_event

            record_mega_trend_kill_event(
                config,
                sector=str(verdict.get("sector") or ""),
                kill_type="internal_momentum",
                reason=str(verdict.get("reason") or ""),
                exit_mode=str(verdict.get("exit_mode") or "defensive_exit"),
                ignited_at=str(kill_ignited_at or "") or None,
                snapshot={
                    "sectors": verdict.get("sectors"),
                    "sector_verdicts": verdict.get("sector_verdicts"),
                    "liquidation": liq,
                    "ignited_at": kill_ignited_at,
                },
            )
            if save_config_fn:
                save_config_fn(config)
        except Exception:
            pass

        print(
            f"🧠☠️ [Mega-Trend Internal Kill] {verdict.get('sector')} — "
            f"{verdict.get('reason')} | "
            f"면죄부박탈 · defensive_exit scale={frac:.0%} · "
            f"청산={liq.get('liquidated', 0)} scaled={liq.get('scaled', 0)}"
        )
        verdict["liquidation"] = liq
        verdict["state"] = state
        verdict[FORGIVENESS_REVOKED_KEY] = True
        return verdict
    finally:
        if own_conn and c is not None:
            try:
                c.close()
            except Exception:
                pass
