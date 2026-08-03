"""
B-1 — Bitget strategy_registry market key normalize (post lifecycle / read paths).
B-4 — lifecycle state counts → MAB explore budget (config log only, no consumer).
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Dict, Optional

from bitget.evolution.market_key_normalize import (
    deathmatch_key_normalize_enabled,
    normalize_market_key,
    normalize_registry_rows,
)

logger = logging.getLogger(__name__)

MAB_EXPLORE_BUDGET_KV = "MAB_EXPLORE_BUDGET_CURRENT"
LIFECYCLE_EXPLORE_BUDGET_KV = "LIFECYCLE_EXPLORE_BUDGET_ENABLED"

_LIFECYCLE_STATES = ("OBSERVING", "CANDIDATE", "LIVE", "COOLED", "RETIRED")
_STATE_RANK = {"LIVE": 5, "CANDIDATE": 4, "OBSERVING": 3, "COOLED": 2, "RETIRED": 1}


def lifecycle_explore_budget_enabled() -> bool:
    env = os.environ.get(LIFECYCLE_EXPLORE_BUDGET_KV)
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value(LIFECYCLE_EXPLORE_BUDGET_KV, None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import LIFECYCLE_EXPLORE_BUDGET_ENABLED

    return bool(LIFECYCLE_EXPLORE_BUDGET_ENABLED)


def count_lifecycle_states_bg(bitget_db_path: str) -> Dict[str, int]:
    """
    Count registry lifecycle states from Bitget DB (read-only).

    Dedupes by ``(normalize_market_key(market), group_key)`` — raw BG keys never
    double-count the same logical group. Canonical state = highest rank when merged.
    """
    counts: Dict[str, int] = {s: 0 for s in _LIFECYCLE_STATES}
    if not bitget_db_path:
        return counts

    try:
        from strategy_registry_store import ensure_strategy_registry_schema, load_registry_rows

        ensure_strategy_registry_schema(bitget_db_path)
        rows = load_registry_rows(bitget_db_path)
    except Exception as ex:
        logger.warning("count_lifecycle_states_bg skip: %s", ex)
        return counts

    buckets: Dict[tuple[str, str], list[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        gk = str(row.get("group_key") or "").strip()
        if not gk:
            continue
        mk = normalize_market_key(str(row.get("market") or "BG"))
        st = str(row.get("state") or "OBSERVING").strip().upper() or "OBSERVING"
        buckets.setdefault((mk, gk), []).append(st)

    for _key, state_list in buckets.items():
        canonical = max(state_list, key=lambda s: _STATE_RANK.get(s, 0))
        if canonical not in counts:
            counts[canonical] = 0
        counts[canonical] += 1

    return counts


def compute_explore_budget_bg(
    *,
    retired: int = 0,
    cooled: int = 0,
    live: int = 0,
    candidate: int = 0,
    observing: int = 0,
    base: Optional[float] = None,
    ceiling: Optional[float] = None,
    per_retired: Optional[float] = None,
    per_cooled: Optional[float] = None,
) -> float:
    """
    Pure fn — explore budget ratio from deduped lifecycle counts (0..ceiling).

    No churn (retired+cooled=0) → 0.0 (matches ``MAB_EXPLORE_BUDGET_CURRENT`` default).
    Consumer wiring is a separate sub-phase.
    """
    from bitget.infra.memory_policy import (
        MAB_EXPLORE_BUDGET_BASE,
        MAB_EXPLORE_BUDGET_CEILING,
        MAB_EXPLORE_BUDGET_DEFAULT,
        MAB_EXPLORE_PER_COOLED,
        MAB_EXPLORE_PER_RETIRED,
    )

    r = max(0, int(retired))
    c = max(0, int(cooled))
    if r + c <= 0:
        return float(MAB_EXPLORE_BUDGET_DEFAULT)

    b = float(MAB_EXPLORE_BUDGET_BASE if base is None else base)
    cap = float(MAB_EXPLORE_BUDGET_CEILING if ceiling is None else ceiling)
    pr = float(MAB_EXPLORE_PER_RETIRED if per_retired is None else per_retired)
    pc = float(MAB_EXPLORE_PER_COOLED if per_cooled is None else per_cooled)

    active = max(1, int(live) + int(candidate) + int(observing))
    churn_boost = (r * pr + c * pc) / float(active)
    ratio = b + churn_boost
    return max(0.0, min(cap, ratio))


def persist_mab_explore_budget_current(value: float) -> None:
    """CAT-K — config_manager.set_config_value only (no raw SQL)."""
    from bitget.infra import config_manager

    config_manager.set_config_value(MAB_EXPLORE_BUDGET_KV, float(value))


def refresh_lifecycle_explore_budget_bg(
    bitget_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    B-4 post-lifecycle hook — count, compute, persist ``MAB_EXPLORE_BUDGET_CURRENT``.

    Does not touch registry state, INCUBATOR, Kelly, or shadow tables.
    """
    if not lifecycle_explore_budget_enabled():
        return {"enabled": False, "written": False}

    from bitget.infra.data_paths import market_data_db_path

    path = bitget_db_path or market_data_db_path()
    counts = count_lifecycle_states_bg(path)
    ratio = compute_explore_budget_bg(
        retired=counts.get("RETIRED", 0),
        cooled=counts.get("COOLED", 0),
        live=counts.get("LIVE", 0),
        candidate=counts.get("CANDIDATE", 0),
        observing=counts.get("OBSERVING", 0),
    )
    persist_mab_explore_budget_current(ratio)
    logger.info(
        "lifecycle explore budget B-4: ratio=%.4f RETIRED=%d COOLED=%d LIVE=%d",
        ratio,
        counts.get("RETIRED", 0),
        counts.get("COOLED", 0),
        counts.get("LIVE", 0),
    )
    return {
        "enabled": True,
        "written": True,
        "ratio": float(ratio),
        "counts": counts,
        "db_path": path,
    }


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
    Post ``meta_governor._step_lifecycle`` hook — B-1 BG resolve + B-4 explore budget log.
    """
    from bitget.infra.data_paths import market_data_db_path

    path = db_path or market_data_db_path()
    out: dict[str, Any] = {"db_path": path, "b1": {}, "b4": {}}

    if deathmatch_key_normalize_enabled():
        from strategy_registry_store import load_registry_rows

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

        out["b1"] = {
            "enabled": True,
            "changed": n_changed,
            "hints": len(hints),
        }
    else:
        out["b1"] = {"enabled": False, "changed": 0}

    try:
        out["b4"] = refresh_lifecycle_explore_budget_bg(path)
    except Exception as ex:
        logger.warning("lifecycle explore budget B-4 skip: %s", ex)
        out["b4"] = {"enabled": False, "error": str(ex)}

    out["enabled"] = bool(out["b1"].get("enabled"))
    out["changed"] = int(out["b1"].get("changed") or 0)
    return out


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

