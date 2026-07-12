"""
REST backfill when WebSocket lag or disconnect is detected.

Tier-1 연동: stream_buffer freshness → throttled REST heal (서버 다운 방지).
"""
from __future__ import annotations

import os
import time
from typing import Iterable, Optional

import memory_bounds

from bitget.data.stream_buffer import get_stream_buffer
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import (
    GAP_HEAL_MAX_AGE_SEC,
    GAP_HEAL_MAX_SYMBOLS_SCAN,
    GAP_HEAL_MIN_INTERVAL_SEC,
)

logger = get_logger("bitget.data.gap_healer")

DEFAULT_MAX_AGE_SEC = float(os.environ.get("BITGET_GAP_HEAL_MAX_AGE_SEC", str(GAP_HEAL_MAX_AGE_SEC)))

_heal_gate = memory_bounds.ThrottledCallback(interval_sec=GAP_HEAL_MIN_INTERVAL_SEC)
_stale_symbols_buf: list[str] = []
_norm_symbols_buf: list[str] = []


def _normalize_symbols(symbols: Optional[Iterable[str]]) -> tuple[str, ...]:
    buf = _norm_symbols_buf
    buf.clear()
    if not symbols:
        return ()
    seen: set[str] = set()
    for s in symbols:
        sym = str(s or "").replace("_", "").upper().strip()
        if sym and sym not in seen:
            seen.add(sym)
            buf.append(sym)
            if len(buf) >= GAP_HEAL_MAX_SYMBOLS_SCAN:
                break
    return tuple(buf)


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
    stale = _stale_symbols_buf
    stale.clear()
    sym_list = _normalize_symbols(symbols)

    for sym in sym_list:
        for inst_type in inst_types:
            age = buf.age_sec(sym, inst_type)
            if age is None or age > float(max_age_sec):
                stale.append(f"{inst_type}:{sym}")

    global_stale = global_age >= float(max_age_sec)
    if stats.get("orderbooks", 0) and ob_age >= float(max_age_sec):
        global_stale = True
    return {
        "buffer_age_sec": global_age,
        "orderbook_age_sec": ob_age,
        "global_stale": global_stale,
        "stale_symbols": list(stale),
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
    Throttled by GAP_HEAL_MIN_INTERVAL_SEC unless force=True.
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

    if not force and not _heal_gate.due():
        out["reason"] = "throttled"
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
