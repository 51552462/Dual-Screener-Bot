"""Universe snapshot: load_dynamic_universe ∩ OHLCV tables (§1 SSOT)."""
from __future__ import annotations

import os
from typing import List, Optional, Set

from bitget.infra.data_paths import market_data_db_path, market_db_read_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.shared_db_connector import get_connection

logger = get_logger("bitget.analysis.universe_bt.universe")

# When MAX_SYMBOLS caps a run, prefer liquid majors first (alpha-order starves L0
# with new meme listings that fail U1 min-bars=240).
_PREFERRED_RUN_SYMBOLS = (
    "BTC_USDT",
    "ETH_USDT",
    "SOL_USDT",
    "XRP_USDT",
    "BNB_USDT",
    "DOGE_USDT",
    "ADA_USDT",
    "AVAX_USDT",
    "LINK_USDT",
    "DOT_USDT",
    "LTC_USDT",
    "BCH_USDT",
    "NEAR_USDT",
    "ATOM_USDT",
    "UNI_USDT",
)


def _table_prefix(market_type: str) -> str:
    mt = str(market_type or "").strip().lower()
    if mt in ("futures", "fut", "linear"):
        return "BITGET_FUT_"
    return "BITGET_SPOT_"


def _1d_bar_count(conn, market_type: str, symbol: str) -> int:
    tbl = f"{_table_prefix(market_type)}{symbol}_1D"
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (tbl,),
        ).fetchone()
        if not row:
            return 0
        return int(conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0])
    except Exception:
        return 0


def select_run_symbols(
    market_type: str,
    symbols: List[str],
    *,
    max_symbols: Optional[int] = None,
    min_bars: int = 240,
    db_path: Optional[str] = None,
) -> List[str]:
    """Cap run symbols by 1D depth — skip thin listings that yield 0 windows.

    Alphabetical ``symbols[:N]`` picks brand-new tickers first; futures L0 then
    writes 0 rows even when FUT_1D coverage is healthy. Prefer majors, then any
    symbol with ``COUNT(1D) >= min_bars`` (default = U1 ``_U1_MIN_BARS``).
    """
    syms = list(symbols)
    if max_symbols is None:
        return syms
    cap = max(0, int(max_symbols))
    if cap == 0:
        return []

    path = db_path or market_db_read_path()
    if not os.path.isfile(path):
        path = db_path or market_data_db_path()
    if not os.path.isfile(path):
        return syms[:cap]

    preferred = [s for s in _PREFERRED_RUN_SYMBOLS if s in syms]
    rest = [s for s in syms if s not in set(preferred)]
    ordered = preferred + rest

    out: List[str] = []
    skipped_thin = 0
    conn = get_connection(path, read_only=True)
    try:
        for s in ordered:
            n = _1d_bar_count(conn, market_type, s)
            if n < int(min_bars):
                skipped_thin += 1
                continue
            out.append(s)
            if len(out) >= cap:
                break
    finally:
        conn.close()

    logger.info(
        "select_run_symbols market=%s cap=%s eligible=%s skipped_thin(<%s)=%s sample=%s",
        market_type,
        cap,
        len(out),
        min_bars,
        skipped_thin,
        out[:8],
    )
    return out

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
