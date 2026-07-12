"""
Bitget 그림자 장부 성과 추적:
- bitget_blocked_trade_history 차단 기록이 실제로 손실 방어에 기여했는지 계산
- Long/Short/레버리지 반영 방어율을 SHADOW_PERFORMANCE에 저장
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict

import pandas as pd

from bitget.config_hub import load_config, save_config
from bitget.infra.bounded_reads import shadow_blocked_history_sql, forward_shadow_defended_match_sql
from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
logger = get_logger("bitget.shadow_performance_tracker")


def _load_config():
    return load_config()


def _save_config(cfg):
    return bool(save_config(cfg))


def _calc_ret(entry: float, close: float, side: str) -> float:
    if entry <= 0 or close <= 0:
        return 0.0
    if str(side).upper() == "SHORT":
        return ((entry - close) / entry) * 100.0
    return ((close - entry) / entry) * 100.0


def run_shadow_performance_evaluation(blocked_limit: int = 500) -> Dict[str, Any]:
    conn = get_connection(DB_PATH)
    try:
        q, params = shadow_blocked_history_sql(limit=blocked_limit)
        blocked = pd.read_sql(q, conn, params=params)
    except Exception:
        blocked = pd.DataFrame()

    if blocked.empty:
        payload = {"updated_at": utc_datetime_str(), "blocked": {}, "notes": "no blocked rows"}
        cfg = _load_config()
        cfg["SHADOW_PERFORMANCE"] = payload
        _save_config(cfg)
        conn.close()
        return payload

    blocked["blocked_date"] = blocked["blocked_at"].astype(str).str[:10]
    reasons = {}
    for reason, b in blocked.groupby(blocked["reason"].astype(str)):
        n_eval = 0
        sum_signed = 0.0
        n_positive = 0
        for _, r in b.iterrows():
            q = forward_shadow_defended_match_sql()
            sub = pd.read_sql(
                q,
                conn,
                params=(str(r.get("market_type", "spot")).lower(), str(r.get("symbol", "")), str(r.get("blocked_date", ""))),
            )
            if sub.empty:
                continue
            fr = float(pd.to_numeric(sub["final_ret"], errors="coerce").fillna(0.0).iloc[0])
            lev = float(pd.to_numeric(sub["leverage"], errors="coerce").fillna(1.0).iloc[0] or 1.0)
            side = str(sub["position_side"].iloc[0] if "position_side" in sub.columns else r.get("position_side", "LONG")).upper()
            # 차단했을 때 방어한 잠재손익(롱/숏 레버리지 반영): 실제 미래 결과를 반대로 해석
            defended_pct = -fr
            defended_roe = defended_pct * lev
            sum_signed += defended_roe
            n_eval += 1
            if defended_roe > 0:
                n_positive += 1
        reasons[reason] = {
            "n_evaluated": int(n_eval),
            "sum_signed_defense_roe_pct": round(float(sum_signed), 4),
            "avg_signed_defense_roe_pct": round(float(sum_signed / n_eval), 4) if n_eval > 0 else 0.0,
            "n_positive_defense": int(n_positive),
        }

    payload = {
        "updated_at": utc_datetime_str(),
        "blocked": {"by_reason": reasons, "rows_loaded": int(len(blocked))},
        "notes": "defense metric uses blocked-trade counterfactual with leverage-adjusted ROE",
    }
    cfg = _load_config()
    cfg["SHADOW_PERFORMANCE"] = payload
    _save_config(cfg)
    conn.close()
    return payload


def run_shadow_snapshot_report() -> str:
    try:
        p = run_shadow_performance_evaluation()
        return json.dumps(p, ensure_ascii=False, indent=2)[:8000]
    except Exception as e:
        return f"[BitgetShadowTracker] 오류: {e}"


if __name__ == "__main__":
    logger.info("%s", run_shadow_snapshot_report())
