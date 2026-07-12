"""Bitget doomsday γ evolution — bitget_forward_trades + bitget_system_config SSOT."""
from __future__ import annotations

import math
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from doomsday_dampener import (
    EVAL_WINDOW_DAYS,
    ETA,
    GAMMA_KEY,
    GRADIENT_SCALE,
    HISTORY_MAX,
    STATE_KEY,
    _clamp_gamma,
    resolve_gamma,
)

from bitget.forward.shared import DB_PATH
from bitget.infra.clock import utc_date_days_ago_str, utc_date_key, utc_now


def _bitget_forward_net_pnl_pct(db_path: str, start_date: str, end_date: str) -> Tuple[float, int]:
    if not db_path or not os.path.isfile(db_path):
        return 0.0, 0
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(final_ret), 0.0), COUNT(*)
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL
              AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
              AND COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) >= ?
              AND COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) <= ?
            """,
            (start_date, end_date),
        ).fetchone()
        return float(row[0] or 0.0), int(row[1] or 0)
    except sqlite3.Error:
        return 0.0, 0
    finally:
        conn.close()


def evolve_bitget_gamma(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
    db_path: Optional[str] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """주간 γ 경사하강 — Bitget forward DB·config만 사용(주식 SSOT 오염 방지)."""
    now = now or utc_now()
    if sys_config is None:
        try:
            from bitget.infra.config_manager import load_system_config

            sys_config = load_system_config()
        except Exception:
            sys_config = {}
    path = db_path or DB_PATH

    gamma = resolve_gamma(sys_config=sys_config)
    state = dict(sys_config.get(STATE_KEY) or {})
    log: List[Dict[str, Any]] = [e for e in (state.get("brake_log") or []) if isinstance(e, dict)]

    win_start = utc_date_days_ago_str(EVAL_WINDOW_DAYS, anchor=now)
    win_end = utc_date_key(anchor=now)
    braked = [
        e
        for e in log
        if e.get("date")
        and win_start <= str(e["date"]) <= win_end
        and isinstance(e.get("mult"), (int, float))
        and float(e["mult"]) < 1.0
    ]

    avoided = missed = 0.0
    gradient = 0.0
    fwd_pnl, n = (0.0, 0)
    reason = "no_brake_last_week"
    gamma_new = gamma

    if braked:
        brake_intensity = sum(1.0 - float(e["mult"]) for e in braked) / len(braked)
        fwd_pnl, n = _bitget_forward_net_pnl_pct(path or "", win_start, win_end)
        if fwd_pnl < 0:
            avoided = brake_intensity * abs(fwd_pnl)
            reason = "defense_success"
        elif fwd_pnl > 0:
            missed = brake_intensity * fwd_pnl
            reason = "opportunity_cost"
        else:
            reason = "flat"
        gradient = avoided - missed
        gamma_new = _clamp_gamma(gamma + ETA * math.tanh(gradient / GRADIENT_SCALE))

    hist: List[Dict[str, Any]] = [h for h in (state.get("history") or []) if isinstance(h, dict)]
    hist.append(
        {
            "date": win_end,
            "gamma_before": round(gamma, 4),
            "gamma_after": round(gamma_new, 4),
            "brake_days": len(braked),
            "avoided_loss": round(avoided, 4),
            "missed_profit": round(missed, 4),
            "gradient": round(gradient, 4),
            "fwd_pnl_pct": round(fwd_pnl, 4),
            "n_trades": n,
            "reason": reason,
        }
    )
    if len(hist) > HISTORY_MAX:
        hist = hist[-HISTORY_MAX:]
    state["history"] = hist
    state["brake_log"] = [e for e in log if str(e.get("date") or "") >= win_end]
    state["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

    summary: Dict[str, Any] = {
        "gamma_before": round(gamma, 4),
        "gamma_after": round(gamma_new, 4),
        "brake_days": len(braked),
        "avoided_loss": round(avoided, 4),
        "missed_profit": round(missed, 4),
        "gradient": round(gradient, 4),
        "fwd_pnl_pct": round(fwd_pnl, 4),
        "n_trades": n,
        "reason": reason,
    }

    if persist:
        try:
            from bitget.infra.config_manager import update_system_config

            update_system_config({GAMMA_KEY: round(gamma_new, 4), STATE_KEY: state})
        except Exception as ex:
            summary["persist_error"] = str(ex)

    return summary
