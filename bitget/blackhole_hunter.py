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
    logger.info("[blackhole] 코인 전용 숏 스퀴즈 참사 DNA 자율 학습 가동")
    cfg = load_config()
    
    conn = get_connection(DB_PATH, read_only=True)
    # 최근 14일 동안의 데이터로 룩백(Lookback)
    since = utc_date_days_ago_str(14)
    
    # [아키텍트 수술 2] 내 장부에서 '숏(SHORT)'을 쳤다가 수익률 -10% 이하로 개박살 난 타점들만 집중 수집합니다.
    q = """
        SELECT dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret, symbol 
        FROM bitget_forward_trades 
        WHERE status LIKE 'CLOSED%' 
          AND position_side = 'SHORT' 
          AND final_ret <= -10.0 
          AND exit_date >= ?
    """
    df = pd.read_sql(q, conn, params=(since,))
    conn.close()
    
    if df.empty or len(df) < 3:
        logger.info("최근 14일간 유의미한 숏 스퀴즈 참사 표본 없음 (학습 대기)")
        return

    # 참사가 발생했던 타점들의 DNA 평균을 계산
    sqz_cpv = df["dyn_cpv"].astype(float).mean()
    sqz_tb = df["dyn_tb"].astype(float).mean()
    sqz_bbe = df["v_energy"].astype(float).mean()
    sqz_rs = df["dyn_rs"].astype(float).mean()
    
    anti = cfg.get("ANTI_PATTERNS", {})
    if not isinstance(anti, dict):
        anti = {}
        
    # 새로운 '숏 스퀴즈 함정(Trap)' DNA Bounding Box를 생성 (오차범위 ±15%)
    trap_name = f"SHORT_SQUEEZE_TRAP_{utc_hm_key()}"
    anti[trap_name] = {
        "source": "BLACKHOLE_HUNTER_BG",
        "label": "SHORT_SQUEEZE_FATAL",
        "dyn_cpv_min": round(max(0, sqz_cpv - 0.15), 4),
        "dyn_cpv_max": round(min(1, sqz_cpv + 0.15), 4),
        "dyn_tb_min": round(max(0, sqz_tb * 0.85), 4),
        "dyn_tb_max": round(sqz_tb * 1.15, 4),
        "v_energy_min": round(max(0, sqz_bbe * 0.85), 4),
        "v_energy_max": round(sqz_bbe * 1.15, 4),
        "dyn_rs_min": round(max(0, sqz_rs - 1.0), 4),
        "dyn_rs_max": round(sqz_rs + 1.0, 4),
        "created_at": utc_hm_key()
    }
    
    cfg["ANTI_PATTERNS"] = anti
    
    # [아키텍트 수술] NameError 크래시 버그 제거 및 코인 전용 도플갱어 TRAP 연동
    # 스캐너의 도플갱어 엔진(_doppelganger_adjustment)이 이 치명적인 숏 스퀴즈 DNA를 읽고
    # 즉각적으로 스코어를 깎을 수 있도록 CRYPTO_DNA_TRAP_RANK1에 4D 벡터를 꽂아 넣습니다.
    cfg["CRYPTO_DNA_TRAP_RANK1"] = {
        "name": "COIN_SHORT_SQUEEZE_FATAL",
        "cpv": round(sqz_cpv, 4),
        "tb": round(sqz_tb, 4),
        "bbe": round(sqz_bbe, 4),
        "rs": round(sqz_rs, 4),
        "updated_at": utc_hm_key()
    }

    toxic_symbols = df["symbol"].astype(str).unique().tolist()
    toxic_count = int(len(toxic_symbols))
    # 전체 시장 유니버스 추정치(약 150~200개) 대비 참사 비율 계산
    toxic_ratio = float(toxic_count / 150.0) 

    # 참사 군집이 일정 비율 이상이면 시스템 방어 모드 발동
    switch_on = (toxic_count >= 5) or (toxic_ratio >= 0.10)
    action = "BTC_SHORT_HEDGE" if switch_on else "NONE"
    msg = "⚠️ 숏 스퀴즈 군집 발생: 방어 모드 전환" if switch_on else "🟢 독성 군집 미약: 일반 모드 유지"

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
    logger.info(f"🚨 [블랙홀] 숏 스퀴즈 참사 DNA 학습 및 도플갱어 TRAP망 결속 완료 (표본: {len(df)}건)")

if __name__ == "__main__":
    scan_blackhole_targets()
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
