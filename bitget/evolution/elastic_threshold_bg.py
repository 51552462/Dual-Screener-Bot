"""Bitget ElasticThreshold — scan/entry 기아 지수 (forward_trades SSOT)."""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from bitget.forward.shared import DB_PATH
from bitget.infra.proprietary_friction_store_bg import normalize_friction_market


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


@dataclass(frozen=True)
class BitgetElasticThresholdState:
    cos_cutoff: float
    ml_cutoff: float
    starvation_index: float
    vol_proxy: float


class BitgetElasticThreshold:
    """주식 ElasticThreshold 의 코인 대응 — bitget_forward_trades 사용."""

    def __init__(self, cfg: Dict[str, Any], market_type: str):
        self.cfg = cfg if isinstance(cfg, dict) else {}
        self.market_db = str(market_type or "spot").lower()
        self.market_label = normalize_friction_market(market_type)

    def compute_starvation_index(self, *, lookback_days: int = 7) -> float:
        target_entries = int(self.cfg.get("ELASTIC_TARGET_ENTRIES_PER_WEEK", 12) or 12)
        target_closed = int(self.cfg.get("ELASTIC_TARGET_CLOSED_PER_WEEK", 6) or 6)
        since = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        db = DB_PATH
        if not db or not os.path.isfile(db):
            return 0.85
        try:
            conn = sqlite3.connect(db, timeout=20)
            try:
                ent = conn.execute(
                    """
                    SELECT COUNT(*) FROM bitget_forward_trades
                    WHERE market_type=? AND substr(entry_date,1,10) >= ?
                      AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                    """,
                    (self.market_db, since),
                ).fetchone()[0]
                closed = conn.execute(
                    """
                    SELECT COUNT(*) FROM bitget_forward_trades
                    WHERE market_type=? AND status LIKE 'CLOSED%'
                      AND substr(IFNULL(NULLIF(TRIM(exit_date),''), entry_date),1,10) >= ?
                      AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                    """,
                    (self.market_db, since),
                ).fetchone()[0]
                n_open = conn.execute(
                    "SELECT COUNT(*) FROM bitget_forward_trades WHERE market_type=? AND status='OPEN'",
                    (self.market_db,),
                ).fetchone()[0]
            finally:
                conn.close()
        except sqlite3.Error:
            return 0.75

        ent_gap = 1.0 - _clip(int(ent or 0) / max(1, target_entries), 0.0, 1.0)
        cl_gap = 1.0 - _clip(int(closed or 0) / max(1, target_closed), 0.0, 1.0)
        stagnation = 0.35 if (int(n_open or 0) > 0 and int(ent or 0) == 0) else 0.0
        return _clip(0.45 * ent_gap + 0.45 * cl_gap + stagnation, 0.0, 1.0)

    def volatility_proxy(self) -> float:
        return internal_ledger_volatility_proxy(self.market_db)


def internal_ledger_volatility_proxy(market_type: str, *, lookback_days: int = 20) -> float:
    """OPEN MFE 분산 + 청산 σ 프록시 — 1.0=중립."""
    import numpy as np

    mk = str(market_type or "spot").lower()
    since = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    db = DB_PATH
    if not db or not os.path.isfile(db):
        return 1.0
    try:
        conn = sqlite3.connect(db, timeout=20)
        try:
            open_rows = conn.execute(
                """
                SELECT entry_price, max_high, min_low, position_side
                FROM bitget_forward_trades
                WHERE market_type=? AND status='OPEN' AND entry_price > 0
                """,
                (mk,),
            ).fetchall()
            closed_rets = conn.execute(
                """
                SELECT final_ret FROM bitget_forward_trades
                WHERE market_type=? AND status LIKE 'CLOSED%'
                  AND substr(IFNULL(NULLIF(TRIM(exit_date),''), entry_date),1,10) >= ?
                  AND final_ret IS NOT NULL
                """,
                (mk, since),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return 1.0

    mfes: list[float] = []
    for ep, mh, ml, side in open_rows:
        ep_f, mh_f, ml_f = float(ep), float(mh or ep), float(ml or ep)
        if ep_f <= 0:
            continue
        if str(side or "LONG").upper() == "SHORT":
            mfes.append(((ep_f - ml_f) / ep_f) * 100.0)
        else:
            mfes.append(((mh_f - ep_f) / ep_f) * 100.0)
    rets = [float(r[0]) for r in closed_rets if r[0] is not None]
    vol_parts: list[float] = []
    if len(mfes) >= 2:
        vol_parts.append(float(np.std(mfes, ddof=0)) / 5.0)
    if len(rets) >= 2:
        vol_parts.append(float(np.std(rets, ddof=0)) / 4.0)
    if not vol_parts:
        return 1.0
    raw = float(np.mean(vol_parts))
    return _clip(0.85 + raw * 0.25, 0.7, 1.5)
