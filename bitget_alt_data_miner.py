import os
import sqlite3
from datetime import datetime

import requests

from bitget_rate_limit_guard import throttle

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ALT_DB_PATH = os.path.join(BASE_DIR, "alt_data.sqlite")


def init_alt_db():
    conn = sqlite3.connect(ALT_DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
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


def run_alternative_data_mining():
    print("📡 [Bitget Alt Data Miner] 크립토 거시 대체데이터 수집...")
    init_alt_db()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        btc_dom, total_mc, mc_24h = _fetch_global()
        btc_px, eth_px, eth_btc = _fetch_prices()
    except Exception as e:
        print(f"⚠️ 외부 데이터 수집 실패: {e}")
        return

    conn = sqlite3.connect(ALT_DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
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
    print(f"✅ 저장 완료: BTC.D={btc_dom:.2f}% | ETH/BTC={eth_btc:.5f} | MC24h={mc_24h:+.2f}%")


if __name__ == "__main__":
    run_alternative_data_mining()
