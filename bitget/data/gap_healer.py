"""
REST backfill when WebSocket lag or disconnect is detected.
"""
from __future__ import annotations

import os
import time
from typing import Iterable, Optional

from bitget.data.stream_buffer import get_stream_buffer
from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.data.gap_healer")

DEFAULT_MAX_AGE_SEC = float(os.environ.get("BITGET_GAP_HEAL_MAX_AGE_SEC", "120"))


def _normalize_symbols(symbols: Optional[Iterable[str]]) -> tuple[str, ...]:
    if not symbols:
        return ()
    out: list[str] = []
    for s in symbols:
        sym = str(s or "").replace("_", "").upper().strip()
        if sym:
            out.append(sym)
    return tuple(dict.fromkeys(out))


def assess_buffer_health(
    *,
    symbols: Optional[Iterable[str]] = None,
    max_age_sec: float = DEFAULT_MAX_AGE_SEC,
    inst_types: tuple[str, ...] = ("SPOT", "USDT-FUTURES"),
) -> dict:
    """
    Inspect global + per-symbol stream buffer freshness (ticker + orderbook).
    """
    buf = get_stream_buffer()
    stats = buf.stats()
    global_age = float(stats.get("last_update_age_sec") or 9999.0)
    ob_age = float(stats.get("last_orderbook_age_sec") or 9999.0)
    stale_symbols: list[str] = []
    sym_list = _normalize_symbols(symbols)

    for sym in sym_list:
        for inst_type in inst_types:
            age = buf.age_sec(sym, inst_type)
            if age is None or age > float(max_age_sec):
                stale_symbols.append(f"{inst_type}:{sym}")

    global_stale = global_age >= float(max_age_sec)
    if stats.get("orderbooks", 0) and ob_age >= float(max_age_sec):
        global_stale = True
    return {
        "buffer_age_sec": global_age,
        "orderbook_age_sec": ob_age,
        "global_stale": global_stale,
        "stale_symbols": stale_symbols,
        "stats": stats,
    }


def heal_if_stale(
    *,
    max_age_sec: float = DEFAULT_MAX_AGE_SEC,
    symbols: Optional[Iterable[str]] = None,
    force: bool = False,
) -> dict:
    """
    If stream buffer is stale (global or watched symbols), run incremental MTF REST update.
    """
    health = assess_buffer_health(symbols=symbols, max_age_sec=max_age_sec)
    age = float(health["buffer_age_sec"])
    out = {
        "buffer_age_sec": age,
        "orderbook_age_sec": health["orderbook_age_sec"],
        "healed": False,
        "reason": "ok",
        "stale_symbols": health["stale_symbols"],
    }

    needs_heal = force or health["global_stale"] or bool(health["stale_symbols"])
    if not needs_heal:
        return out

    reason = "global_stale" if health["global_stale"] else "symbol_stale"
    logger.warning(
        "stream buffer stale (global=%.1fs ob=%.1fs symbols=%s) — running REST mtf update",
        age,
        health["orderbook_age_sec"],
        health["stale_symbols"][:5],
    )
    try:
        from bitget.mtf_data_updater import run_mtf_update

        run_mtf_update()
        out["healed"] = True
        out["reason"] = f"mtf_update_ok:{reason}"
    except Exception as e:
        out["reason"] = f"mtf_update_failed:{reason}:{e}"
        logger.exception("gap heal failed")
    out["elapsed_sec"] = age
    return out


def run_scheduled_gap_heal() -> None:
    """Pipeline/cron step wrapper."""
    symbols_raw = os.environ.get("BITGET_WS_SYMBOLS", "")
    symbols = _normalize_symbols(symbols_raw.split(",")) if symbols_raw else None
    result = heal_if_stale(symbols=symbols)
    try:
        from bitget.infra import ops_logger

        ops_logger.record_gauge_snapshot("bitget.gap_healer", result)
    except Exception:
        pass
    print(f"[gap_healer] {result}")
