import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
import pandas as pd

from bitget.infra.data_paths import market_data_db_path
from bitget.infra.shared_db_connector import get_connection

def catastrophic_day_thresholds(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    return {
        "min_closed": float(cfg.get("CATASTROPHIC_DAY_MIN_CLOSED", 5)),
        "wr_threshold_pct": float(cfg.get("CATASTROPHIC_DAY_WR_THRESHOLD_PCT", 15.0)),
        "block_wr_pct": float(cfg.get("CATASTROPHIC_DAY_BLOCK_WR_PCT", 0.0)),
        "block_min_closed": float(cfg.get("CATASTROPHIC_DAY_BLOCK_MIN_CLOSED", 8)),
    }

def evaluate_rolling_catastrophic_clutch(
    market_type: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    [아키텍트 수술] 코인 24/7 환경에 맞춘 롤링 윈도우(최근 24시간) 승률 붕괴 감지기
    자정을 기준으로 끊지 않고, 현재 시간부터 정확히 24시간 전까지의 청산 내역을 긁어와
    연패(Drawdown)의 심각도를 실시간으로 평가합니다.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not cfg.get("ENABLE_CATASTROPHIC_DAY_CLUTCH", True):
        return {"active": False, "block_entry": False, "reason": "disabled"}

    th = catastrophic_day_thresholds(cfg)
    now = datetime.now(timezone.utc)
    cutoff_24h_ago = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        conn = get_connection(market_data_db_path(), read_only=True)
        query = """
            SELECT COUNT(*) AS n, 
                   SUM(CASE WHEN CAST(final_ret AS REAL) > 0 THEN 1 ELSE 0 END) AS wins
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%' 
              AND market_type = ? 
              AND exit_date >= ?
        """
        row = conn.execute(query, (str(market_type).lower(), cutoff_24h_ago)).fetchone()
        conn.close()
    except Exception as e:
        return {"active": False, "block_entry": False, "reason": f"db_error: {e}"}

    n = int(row[0] if row else 0)
    wins = int(row[1] if row else 0)
    
    if n < int(th["min_closed"]):
        return {"active": False, "block_entry": False, "reason": f"insufficient_sample(n={n})"}

    wr = (wins / n) * 100.0
    block = bool(cfg.get("ENABLE_CATASTROPHIC_DAY_BLOCK_ENTRIES", True)) and (wr <= th["block_wr_pct"] and n >= int(th["block_min_closed"]))

    return {
        "active": True,
        "block_entry": block,
        "win_rate_pct": round(wr, 1),
        "n_closed": n,
        "reason": f"catastrophic_rolling_24h:wr={wr:.1f}%/n={n}" + ("/BLOCK" if block else "")
    }