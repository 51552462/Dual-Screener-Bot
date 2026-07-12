import json
import os
import sqlite3
from typing import Any, Dict, Optional

from bitget.infra.bounded_reads import macro_daily_last_row_sql
from bitget.infra.clock import utc_date_key
from bitget.infra.data_paths import alt_data_db_path
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.network_retry import http_get
from bitget.infra.shared_db_connector import get_connection

logger = get_logger("bitget.alt_data_miner")


def init_alt_db():
    path = alt_data_db_path()
    conn = get_connection(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_daily (
            date TEXT PRIMARY KEY,
            btc_dominance REAL,
            eth_btc_ratio REAL,
            total_market_cap_usd REAL,
            market_cap_change_24h REAL,
            btc_price_usd REAL,
            eth_price_usd REAL
        )
        """
    )
    conn.commit()
    conn.close()


def _fetch_global():
    res = http_get(
        "https://api.coingecko.com/api/v3/global",
        op="alt.coingecko.global",
        throttle_key="http.coingecko.global",
        throttle_interval_sec=0.35,
        timeout=15.0,
        swallow=False,
    )
    g = res.json().get("data", {})
    btc_dom = float(g.get("market_cap_percentage", {}).get("btc", 0.0) or 0.0)
    total_mc = float(g.get("total_market_cap", {}).get("usd", 0.0) or 0.0)
    mc_24h = float(g.get("market_cap_change_percentage_24h_usd", 0.0) or 0.0)
    return btc_dom, total_mc, mc_24h


def _fetch_prices():
    res = http_get(
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
        op="alt.coingecko.simple_price",
        throttle_key="http.coingecko.simple_price",
        throttle_interval_sec=0.35,
        timeout=15.0,
        swallow=False,
    )
    p = res.json()
    btc = float(p.get("bitcoin", {}).get("usd", 0.0) or 0.0)
    eth = float(p.get("ethereum", {}).get("usd", 0.0) or 0.0)
    ratio = (eth / btc) if btc > 0 else 0.0
    return btc, eth, ratio


def _load_last_row() -> Optional[Dict[str, Any]]:
    path = alt_data_db_path()
    if not os.path.isfile(path):
        return None
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(macro_daily_last_row_sql()).fetchone()
        return {k: row[k] for k in row.keys()} if row else None
    finally:
        conn.close()


def run_once() -> Optional[Dict[str, Any]]:
    """Live fetch + DB upsert. 실패 시 None (hydrate가 lookback 처리)."""
    init_alt_db()
    today = utc_date_key()
    try:
        btc_dom, total_mc, mc_24h = _fetch_global()
        btc_px, eth_px, eth_btc = _fetch_prices()
    except Exception as e:
        log_exception(logger, "alt-data live fetch failed: %s", e)
        return None

    path = alt_data_db_path()
    conn = get_connection(path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO macro_daily
        (date, btc_dominance, eth_btc_ratio, total_market_cap_usd, market_cap_change_24h, btc_price_usd, eth_price_usd)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (today, btc_dom, eth_btc, total_mc, mc_24h, btc_px, eth_px),
    )
    conn.commit()
    conn.close()
    return {
        "date": today,
        "btc_dominance": btc_dom,
        "eth_btc_ratio": eth_btc,
        "total_market_cap_usd": total_mc,
        "market_cap_change_24h": mc_24h,
        "btc_price_usd": btc_px,
        "eth_price_usd": eth_px,
    }


def run_alternative_data_mining():
    logger.info("[alt-data] crypto macro alternative data fetch")
    row = run_once()
    if not row:
        logger.warning("external alt-data fetch failed")
        return {"ok": False}
    logger.info(
        "alt-data saved: BTC.D=%.2f%% ETH/BTC=%.5f MC24h=%+.2f%%",
        row["btc_dominance"],
        row["eth_btc_ratio"],
        row["market_cap_change_24h"],
    )
    return {"ok": True, "row": row}


if __name__ == "__main__":
    run_alternative_data_mining()
