"""
B-1 — Bitget strategy_registry market key normalize (post lifecycle / read paths).
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Optional

from bitget.evolution.market_key_normalize import (
    deathmatch_key_normalize_enabled,
    normalize_registry_rows,
)

logger = logging.getLogger(__name__)


def build_group_market_hints_from_forward_db(db_path: str) -> dict[str, str]:
    """
    Majority market_type per sig_type prefix (group proxy) from forward_trades.
  Returns group_key → ``spot``|``futures`` hint for BG resolve.
    """
    hints: dict[str, str] = {}
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            cur = conn.execute(
                """
                SELECT sig_type, market_type, COUNT(*) AS n
                FROM bitget_forward_trades
                WHERE sig_type IS NOT NULL AND TRIM(sig_type) != ''
                GROUP BY sig_type, market_type
                """
            )
            by_sig: dict[str, dict[str, int]] = {}
            for sig_type, market_type, n in cur.fetchall():
                st = str(sig_type or "").strip()
                if not st:
                    continue
                mt = str(market_type or "spot").strip().lower()
                by_sig.setdefault(st, {})[mt] = int(n or 0)
            for sig, counts in by_sig.items():
                if not counts:
                    continue
                winner = max(counts.items(), key=lambda x: x[1])[0]
                hints[sig] = winner
                try:
                    from forward.ledger import ledger_group_key

                    gk = ledger_group_key(sig)
                    if gk:
                        hints[gk] = winner
                except Exception:
                    pass
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("build_group_market_hints_from_forward_db skip: %s", ex)
    return hints


def write_through_registry_markets(
    rows: list[dict[str, Any]],
    *,
    db_path: str,
) -> int:
    if not rows:
        return 0
    from strategy_registry_store import upsert_registry_rows

    upsert_registry_rows(rows, db_path)
    return len(rows)


def normalize_bitget_registry_after_lifecycle(
    *,
    db_path: Optional[str] = None,
    meta_registry: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Post ``meta_governor._step_lifecycle`` hook — read-time BG resolve + write-through.
    """
    if not deathmatch_key_normalize_enabled():
        return {"enabled": False, "changed": 0}

    from bitget.infra.data_paths import market_data_db_path
    from strategy_registry_store import load_registry_rows

    path = db_path or market_data_db_path()
    hints = build_group_market_hints_from_forward_db(path)
    rows = load_registry_rows(path)
    if not rows and meta_registry:
        rows = [dict(r) for r in meta_registry if isinstance(r, dict)]

    normalized, n_changed = normalize_registry_rows(rows, hints=hints)
    if n_changed:
        write_through_registry_markets(
            [r for r in normalized if isinstance(r, dict)],
            db_path=path,
        )
        logger.info("registry market_key B-1 write-through: %s rows", n_changed)

    return {
        "enabled": True,
        "changed": n_changed,
        "hints": len(hints),
        "db_path": path,
    }


def load_registry_rows_normalized(db_path: Optional[str] = None) -> list[dict[str, Any]]:
    """Read path for deathmatch — resolve BG + optional write-through."""
    from strategy_registry_store import load_registry_rows

    from bitget.infra.data_paths import market_data_db_path

    path = db_path or market_data_db_path()
    rows = load_registry_rows(path)
    if not deathmatch_key_normalize_enabled():
        return rows
    hints = build_group_market_hints_from_forward_db(path)
    normalized, n_changed = normalize_registry_rows(rows, hints=hints)
    if n_changed:
        write_through_registry_markets(normalized, db_path=path)
    return normalized
