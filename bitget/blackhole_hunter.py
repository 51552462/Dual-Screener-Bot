import json
import os
import sqlite3

import pandas as pd

from bitget.config_hub import load_config, save_config
from bitget.infra.bounded_reads import forward_blackhole_recent_closed_sql
from bitget.infra.clock import utc_date_days_ago_str, utc_hm_key
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
logger = get_logger("bitget.blackhole_hunter")


def scan_blackhole_targets():
    logger.info("[blackhole] toxic DNA cluster scan")
    cfg = load_config()
    anti = cfg.get("ANTI_PATTERNS", {})
    if not isinstance(anti, (dict, list)) or len(anti) == 0:
        cfg["BLACKHOLE_TOXIC_COUNT"] = {"count": 0, "symbols": [], "updated_at": utc_hm_key()}
        cfg["BLACKHOLE_SWITCH_SIGNAL"] = {"active": False, "action": "NONE"}
        save_config(cfg)
        logger.info("no anti-patterns registered — switch none")
        return

    conn = get_connection(DB_PATH, read_only=True)
    since = utc_date_days_ago_str(14)
    q, params = forward_blackhole_recent_closed_sql(since_date=since)
    df = pd.read_sql(q, conn, params=params)
    conn.close()
    if df.empty:
        logger.warning("insufficient CLOSED trades in last 14d")
        return

    df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0)
    toxic_df = df[df["final_ret"] <= -8.0].copy()
    toxic_symbols = toxic_df["symbol"].astype(str).unique().tolist()
    toxic_count = int(len(toxic_symbols))
    toxic_ratio = float(toxic_count / max(len(df["symbol"].unique()), 1))

    # 참사 군집이 일정 비율 이상이면 BTC 숏 방어 전환
    switch_on = (toxic_count >= 8) or (toxic_ratio >= 0.25)
    action = "BTC_SHORT" if switch_on else "NONE"
    msg = "⚠️ 시장 전반 Toxic 확산: BTC 숏 스위치 ON" if switch_on else "🟢 독성 군집 미약: 일반 모드 유지"

    cfg["BLACKHOLE_TOXIC_COUNT"] = {
        "count": toxic_count,
        "symbols": toxic_symbols[:50],
        "ratio": round(toxic_ratio, 4),
        "updated_at": utc_hm_key(),
    }
    cfg["BLACKHOLE_SWITCH_SIGNAL"] = {
        "active": bool(switch_on),
        "action": action,
        "target_symbol": "BTC_USDT",
        "position_side": "SHORT" if switch_on else "NONE",
        "reason": msg,
        "updated_at": utc_hm_key(),
    }
    save_config(cfg)
    logger.info(
        "blackhole done: toxic_count=%s toxic_ratio=%.2f%% action=%s",
        toxic_count,
        toxic_ratio * 100.0,
        action,
    )


if __name__ == "__main__":
    scan_blackhole_targets()
