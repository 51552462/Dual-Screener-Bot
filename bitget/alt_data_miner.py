import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

import requests

from bitget.rate_limit_guard import throttle

from bitget.infra.data_paths import alt_data_db_path
from bitget.infra.shared_db_connector import get_connection


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
    throttle("http.coingecko.global", 0.35)
    g = requests.get("https://api.coingecko.com/api/v3/global", timeout=15).json().get("data", {})
    btc_dom = float(g.get("market_cap_percentage", {}).get("btc", 0.0) or 0.0)
    total_mc = float(g.get("total_market_cap", {}).get("usd", 0.0) or 0.0)
    mc_24h = float(g.get("market_cap_change_percentage_24h_usd", 0.0) or 0.0)
    return btc_dom, total_mc, mc_24h


def _fetch_prices():
    throttle("http.coingecko.simple_price", 0.35)
    p = requests.get(
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
        timeout=15,
    ).json()
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
        row = conn.execute("SELECT * FROM macro_daily ORDER BY date DESC LIMIT 1").fetchone()
        return {k: row[k] for k in row.keys()} if row else None
    finally:
        conn.close()


def run_once() -> Optional[Dict[str, Any]]:
    """Live fetch + DB upsert. 실패 시 None (hydrate가 lookback 처리)."""
    init_alt_db()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        btc_dom, total_mc, mc_24h = _fetch_global()
        btc_px, eth_px, eth_btc = _fetch_prices()
    except Exception:
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
    print("📡 [Bitget Alt Data Miner] 크립토 거시 대체데이터 수집...")
    row = run_once()
    if not row:
        print("⚠️ 외부 데이터 수집 실패")
        return {"ok": False}
    print(
        f"✅ 저장 완료: BTC.D={row['btc_dominance']:.2f}% | "
        f"ETH/BTC={row['eth_btc_ratio']:.5f} | MC24h={row['market_cap_change_24h']:+.2f}%"
    )
    return {"ok": True, "row": row}


if __name__ == "__main__":
    run_alternative_data_mining()
