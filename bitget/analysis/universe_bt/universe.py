"""Universe snapshot: load_dynamic_universe ∩ OHLCV tables (§1 SSOT)."""
from __future__ import annotations

import os
from typing import List, Optional, Set

from bitget.infra.data_paths import market_data_db_path, market_db_read_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.shared_db_connector import get_connection

logger = get_logger("bitget.analysis.universe_bt.universe")


def _table_prefix(market_type: str) -> str:
    mt = str(market_type or "").strip().lower()
    if mt in ("futures", "fut", "linear"):
        return "BITGET_FUT_"
    return "BITGET_SPOT_"


def list_ohlcv_symbols(market_type: str, *, db_path: Optional[str] = None) -> List[str]:
    """Symbols that have at least one OHLCV table for market_type."""
    prefix = _table_prefix(market_type)
    path = db_path or market_db_read_path()
    if not os.path.isfile(path):
        path = db_path or market_data_db_path()
    if not os.path.isfile(path):
        return []
    conn = get_connection(path, read_only=True)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"{prefix}%",),
        ).fetchall()
    finally:
        conn.close()
    syms: Set[str] = set()
    for (name,) in rows:
        parts = str(name).split("_")
        if len(parts) < 4:
            continue
        # BITGET_SPOT_BTC_USDT_1D → BTC_USDT
        syms.add("_".join(parts[2:-1]))
    return sorted(syms)


def load_live_universe_symbols(market_type: str) -> Optional[List[str]]:
    """Best-effort live universe; None if exchange unavailable."""
    if (os.environ.get("BITGET_UNIVERSE_BT_OHLCV_ONLY") or "").strip() in (
        "1",
        "true",
        "TRUE",
        "yes",
    ):
        return None
    try:
        from bitget.config_hub import load_config
        from bitget.mtf_data_updater import create_exchange, load_dynamic_universe

        cfg = load_config()
        uni_cfg = cfg.get("universe") if isinstance(cfg.get("universe"), dict) else {}
        mt_key = (
            "futures"
            if str(market_type).lower() in ("futures", "fut", "linear")
            else "spot"
        )
        mcfg = uni_cfg.get("linear" if mt_key == "futures" else "spot", {})
        if not isinstance(mcfg, dict):
            mcfg = {}
        min_qv = float(mcfg.get("min_quote_volume_usdt", uni_cfg.get("min_quote_volume_usdt", 500_000)) or 500_000)
        default_quote = str(cfg.get("default_quote", uni_cfg.get("default_quote", "USDT")) or "USDT")
        ex_type = "swap" if mt_key == "futures" else "spot"
        load_mt = "linear" if mt_key == "futures" else "spot"
        ex = create_exchange(ex_type)
        pairs = load_dynamic_universe(ex, load_mt, min_qv, default_quote)
        out: List[str] = []
        for item in pairs or []:
            sym = item[0] if isinstance(item, (list, tuple)) else item
            table_symbol = str(sym).replace("/", "_").split(":")[0]
            out.append(table_symbol)
        return sorted(set(out))
    except Exception as ex:
        logger.warning("load_live_universe_symbols skip: %s", ex)
        return None


def resolve_universe_snapshot(
    market_type: str, *, db_path: Optional[str] = None
) -> List[str]:
    """U = live_universe ∩ OHLCV; if live unavailable → OHLCV-only (logged)."""
    ohlcv = set(list_ohlcv_symbols(market_type, db_path=db_path))
    live = load_live_universe_symbols(market_type)
    if live is None:
        logger.info(
            "universe snapshot OHLCV-only n=%s market=%s (live filter unavailable)",
            len(ohlcv),
            market_type,
        )
        return sorted(ohlcv)
    inter = sorted(ohlcv & set(live))
    logger.info(
        "universe snapshot live∩ohlcv n=%s (live=%s ohlcv=%s) market=%s",
        len(inter),
        len(live),
        len(ohlcv),
        market_type,
    )
    return inter
