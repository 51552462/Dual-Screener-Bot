"""Bitget exit ratchet κ RL — bitget_forward_trades free_runner SSOT."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import exit_dynamics as xd

from bitget.forward.shared import DB_PATH


def _read_runner_trades(db_path: str, cutoff: str):
    uri = str(db_path).replace("\\", "/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
    try:
        return conn.execute(
            """
            SELECT mfe, final_ret, exit_type, bars_held
            FROM bitget_forward_trades
            WHERE (free_runner=1 OR scaled_out_frac > 0)
              AND status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL AND mfe IS NOT NULL
              AND substr(IFNULL(exit_date, entry_date),1,10) >= ?
            """,
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()


def compute_runner_rates(rows) -> Dict[str, Any]:
    from exit_ratchet_rl import compute_runner_rates as _stock_rates

    return _stock_rates(rows)


def evolve_bitget_ratchet_kappa(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    lookback_days: int = 7,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    own_cfg = cfg is None
    if own_cfg:
        try:
            from bitget.infra.config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}
    path = db_path or DB_PATH
    now = now or datetime.utcnow()
    cutoff = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    rates: Dict[str, Any] = {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}
    if path:
        try:
            rates = compute_runner_rates(_read_runner_trades(path, cutoff))
        except Exception as ex:
            rates["error"] = str(ex)

    old_state = xd.load_ratchet_state(cfg)
    if rates.get("n", 0) < 3:
        return {"updated": False, "reason": "insufficient_runner_sample", "rates": rates, "state": old_state}

    new_state = xd.update_ratchet_kappa_rl(
        old_state,
        whipsaw_rate=float(rates["whipsaw_rate"]),
        giveback_rate=float(rates["giveback_rate"]),
    )
    new_state["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    cfg = dict(cfg or {})
    cfg[xd.RATCHET_STATE_KEY] = new_state

    result: Dict[str, Any] = {
        "updated": True,
        "rates": rates,
        "old_state": old_state,
        "state": new_state,
    }
    if persist:
        try:
            from bitget.infra.config_manager import update_system_config

            update_system_config({xd.RATCHET_STATE_KEY: new_state})
        except Exception as ex:
            result["persist_error"] = str(ex)

    return result


def build_ratchet_brief(result: Dict[str, Any]) -> str:
    from exit_ratchet_rl import build_ratchet_brief as _stock_brief

    return _stock_brief(result)
