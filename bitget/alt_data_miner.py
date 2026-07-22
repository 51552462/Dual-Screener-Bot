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


def _mine_zombie_coins_to_blacklist():
    """
    [아키텍트 수술] 코인 팩토리 전용 능동형 '좀비 코인' 색출기
    로컬 DB를 스캔하여 최근 3일간 거래량이 '0'이거나, 
    가격 변동이 완전히 멈춰버린(러그풀/상폐) 코인들을 찾아내어 관제탑 블랙리스트에 추가합니다.
    """
    from bitget.config_hub import load_config, save_config_atomic
    import pandas as pd
    
    try:
        conn = get_connection(market_data_db_path(), read_only=True)
        # DB에 존재하는 모든 코인 테이블 목록 조회
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_%'").fetchall()]
        
        zombies = set()
        for tbl in tables:
            try:
                # 최근 3일 치 데이터만 빠르게 조회하여 생사(Health) 여부 판별
                df = pd.read_sql(f'SELECT Close, Volume FROM "{tbl}" ORDER BY Date DESC LIMIT 3', conn)
                if df.empty or len(df) < 3:
                    continue
                
                vol_sum = df['Volume'].astype(float).sum()
                close_std = df['Close'].astype(float).std()
                
                # 거래량 합이 0에 수렴하거나, 3일 내내 가격이 1원도 안 변했다면(거래정지/상폐) 좀비로 판정
                if vol_sum <= 1e-9 or close_std <= 1e-9:
                    # BITGET_SPOT_BTC_USDT_1D -> BTC_USDT 추출
                    symbol = "_".join(tbl.split("_")[2:-1])
                    zombies.add(symbol)
            except Exception:
                continue
        conn.close()

        # 색출된 좀비 코인 목록을 관제탑(SYS_CONFIG)에 원자적으로 업데이트
        if zombies:
            cfg = load_config()
            existing_zombies = set(cfg.get("DYNAMIC_ZOMBIE_BLACKLIST", []))
            new_zombies = existing_zombies.union(zombies)
            
            cfg["DYNAMIC_ZOMBIE_BLACKLIST"] = list(new_zombies)
            save_config_atomic(cfg)
            logger.info(f"🧟 [Zombie Miner] 상폐/유동성 고갈 코인 {len(zombies)}개 색출 및 블랙리스트 등록 완료 (누적: {len(new_zombies)}개)")
            
        return len(zombies)
    except Exception as e:
        log_exception(logger, "zombie coin mining failed: %s", e)
        return 0


def run_alternative_data_mining():
    logger.info("[alt-data] crypto macro alternative data fetch")
    row = run_once()
    
    # [아키텍트 수술] 매크로 데이터 수집 후, 즉각적으로 생태계 내부의 좀비 코인 색출 작업 병행
    zombie_count = _mine_zombie_coins_to_blacklist()
    
    if not row:
        logger.warning("external alt-data fetch failed")
        return {"ok": False, "zombies_mined": zombie_count}
        
    logger.info(
        "alt-data saved: BTC.D=%.2f%% ETH/BTC=%.5f MC24h=%+.2f%% | 🧟 Zombies: %d",
        row["btc_dominance"],
        row["eth_btc_ratio"],
        row["market_cap_change_24h"],
        zombie_count
    )
    return {"ok": True, "row": row, "zombies_mined": zombie_count}


if __name__ == "__main__":
    run_alternative_data_mining()
