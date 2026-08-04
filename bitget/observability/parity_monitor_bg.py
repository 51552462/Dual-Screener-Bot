"""
D-3b — paper vs real parity monitor (scaffold only).

``compute_paper_vs_real_parity_bg`` is defined for unit tests and future P2-5 wiring.
No weekly_evolution/cron hook — ``PARITY_MONITOR_ENABLED`` defaults false.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional

from bitget.infra.clock import utc_hours_ago_iso

logger = logging.getLogger(__name__)

_FORWARD_TABLE = "bitget_forward_trades"
_REAL_TABLE = "bitget_real_execution"


def parity_monitor_enabled() -> bool:
    env = os.environ.get("PARITY_MONITOR_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("PARITY_MONITOR_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import PARITY_MONITOR_ENABLED

    return bool(PARITY_MONITOR_ENABLED)


def _resolve_window_days(window_days: Optional[int] = None) -> int:
    if window_days is not None:
        return max(1, int(window_days))
    return 7


def _forward_db_path() -> str:
    from bitget.forward.shared import DB_PATH

    return str(DB_PATH or "")


def _paper_pnl_usd(
    *,
    sim_kelly_invest: float,
    final_ret: Optional[float],
) -> float:
    invest = float(sim_kelly_invest or 0.0)
    ret_pct = float(final_ret or 0.0)
    return round(invest * ret_pct / 100.0, 6)


def _load_paper_closed_rows(
    *,
    since_iso: str,
    forward_db_path: Optional[str] = None,
) -> Dict[int, Dict[str, Any]]:
    path = forward_db_path or _forward_db_path()
    if not path or not os.path.isfile(path):
        return {}
    since_cmp = since_iso[:19]
    out: Dict[int, Dict[str, Any]] = {}
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_FORWARD_TABLE,),
            ).fetchone()
            if not table:
                return {}
            cur = conn.execute(
                f"""
                SELECT id, exit_date, final_ret, sim_kelly_invest, symbol, market_type
                FROM {_FORWARD_TABLE}
                WHERE status LIKE 'CLOSED%'
                  AND exit_date IS NOT NULL
                  AND datetime(exit_date) >= datetime(?)
                ORDER BY id ASC
                """,
                (since_cmp,),
            )
            for trade_id, exit_date, final_ret, invest, symbol, market_type in cur.fetchall():
                tid = int(trade_id)
                invest_f = float(invest or 0.0)
                out[tid] = {
                    "virtual_trade_id": tid,
                    "exit_date": exit_date,
                    "final_ret_pct": float(final_ret or 0.0),
                    "paper_pnl_usd": _paper_pnl_usd(
                        sim_kelly_invest=invest_f,
                        final_ret=final_ret,
                    ),
                    "symbol": str(symbol or ""),
                    "market_type": str(market_type or ""),
                }
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("parity monitor load paper rows failed: %s", ex)
        return {}
    return out


def _load_real_rows(
    *,
    since_iso: str,
    forward_db_path: Optional[str] = None,
) -> Dict[int, Dict[str, Any]]:
    """Read-only CAT-N interface — ``bitget_real_execution`` only, not invoked from pipelines."""
    path = forward_db_path or _forward_db_path()
    if not path or not os.path.isfile(path):
        return {}
    since_cmp = since_iso[:19]
    out: Dict[int, Dict[str, Any]] = {}
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_REAL_TABLE,),
            ).fetchone()
            if not table:
                return {}
            cur = conn.execute(
                f"""
                SELECT virtual_trade_id, created_at, realized_pnl_usdt, realized_ret_pct,
                       symbol, market_type, exec_ok
                FROM {_REAL_TABLE}
                WHERE virtual_trade_id > 0
                  AND datetime(created_at) >= datetime(?)
                ORDER BY id ASC
                """,
                (since_cmp,),
            )
            for vid, created_at, pnl, ret_pct, symbol, market_type, exec_ok in cur.fetchall():
                vtid = int(vid)
                out[vtid] = {
                    "virtual_trade_id": vtid,
                    "created_at": created_at,
                    "real_pnl_usd": round(float(pnl or 0.0), 6),
                    "realized_ret_pct": float(ret_pct or 0.0),
                    "symbol": str(symbol or ""),
                    "market_type": str(market_type or ""),
                    "exec_ok": int(exec_ok or 0),
                }
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("parity monitor load real rows failed: %s", ex)
        return {}
    return out


def compute_paper_vs_real_parity_bg(
    window_days: int = 7,
    *,
    forward_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    days = _resolve_window_days(window_days)
    since_iso = utc_hours_ago_iso(float(days) * 24.0)
    paper_by_id = _load_paper_closed_rows(since_iso=since_iso, forward_db_path=forward_db_path)
    real_by_id = _load_real_rows(since_iso=since_iso, forward_db_path=forward_db_path)

    matches: List[Dict[str, Any]] = []
    paper_ids = set(paper_by_id)
    real_ids = set(real_by_id)
    matched_ids = paper_ids & real_ids

    total_paper = 0.0
    total_real = 0.0
    for vid in sorted(matched_ids):
        paper = paper_by_id[vid]
        real = real_by_id[vid]
        paper_pnl = float(paper["paper_pnl_usd"])
        real_pnl = float(real["real_pnl_usd"])
        diff = round(real_pnl - paper_pnl, 6)
        total_paper += paper_pnl
        total_real += real_pnl
        matches.append(
            {
                "virtual_trade_id": vid,
                "symbol": paper.get("symbol") or real.get("symbol"),
                "market_type": paper.get("market_type") or real.get("market_type"),
                "paper_pnl_usd": paper_pnl,
                "real_pnl_usd": real_pnl,
                "diff_usd": diff,
                "paper_final_ret_pct": paper.get("final_ret_pct"),
                "realized_ret_pct": real.get("realized_ret_pct"),
            }
        )

    unmatched_paper = sorted(paper_ids - real_ids)
    unmatched_real = sorted(real_ids - paper_ids)
    total_diff = round(total_real - total_paper, 6)

    return {
        "window_days": days,
        "parity_monitor_enabled": parity_monitor_enabled(),
        "matched_count": len(matches),
        "unmatched_paper_count": len(unmatched_paper),
        "unmatched_real_count": len(unmatched_real),
        "unmatched_paper_ids": unmatched_paper[:50],
        "unmatched_real_ids": unmatched_real[:50],
        "total_paper_pnl_usd": round(total_paper, 6),
        "total_real_pnl_usd": round(total_real, 6),
        "total_parity_diff_usd": total_diff,
        "matches": matches,
    }
