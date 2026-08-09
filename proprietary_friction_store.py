"""
Proprietary Friction Simulator — 실매매 전환을 위한 극강의 가상 마찰/슬리피지 엔진.

단순한 가상매매 수익률의 환상을 깨부수고, 시가총액, 거래대금, 변동성(ATR),
그리고 실제 투입되는 자본(Kelly Invest)의 크기에 비례하여
진입 단가는 비싸게, 청산 단가는 싸게 후려치는 '가혹한 현실 물리 법칙'을 주입합니다.
이 마찰을 견디고 우상향하는 로직만이 실매매(LIVE) 승격 심사를 받을 자격이 주어집니다.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# 기존 스키마 유지 (무음 적재용)
_DDL = """
CREATE TABLE IF NOT EXISTS scan_funnel_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    market TEXT NOT NULL,
    universe_size INTEGER NOT NULL DEFAULT 0,
    survivors INTEGER NOT NULL DEFAULT 0,
    pass_rate_pct REAL NOT NULL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_scan_funnel_ts ON scan_funnel_snapshot(ts DESC);
CREATE INDEX IF NOT EXISTS idx_scan_funnel_mkt_ts ON scan_funnel_snapshot(market, ts DESC);

CREATE TABLE IF NOT EXISTS regime_friction_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    market TEXT NOT NULL,
    event_type TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_regime_friction_date ON regime_friction_event(date DESC, market);

CREATE TABLE IF NOT EXISTS scan_funnel_drop_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    market TEXT NOT NULL,
    scanner TEXT NOT NULL,
    code TEXT,
    reason TEXT NOT NULL,
    final_score REAL,
    eff_cos_cutoff REAL,
    eff_ml_cutoff REAL,
    regime_key TEXT,
    rank_in_slot INTEGER
);
CREATE INDEX IF NOT EXISTS idx_scan_funnel_drop_ts ON scan_funnel_drop_event(ts DESC);
CREATE INDEX IF NOT EXISTS idx_scan_funnel_drop_mkt ON scan_funnel_drop_event(market, ts DESC);
"""

def _db_path() -> str:
    try:
        from market_db_paths import MARKET_DATA_DB_PATH
        return MARKET_DATA_DB_PATH
    except Exception:
        from forward.shared import DB_PATH
        return DB_PATH

def ensure_proprietary_friction_schema(
    cursor: Optional[sqlite3.Cursor] = None,
    *,
    db_path: Optional[str] = None,
) -> None:
    if cursor is not None:
        cursor.executescript(_DDL)
        return
    path = db_path or _db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.executescript(_DDL)
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("proprietary_friction schema skip: %s", ex)


def _migrate_scan_funnel_snapshot_columns(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(scan_funnel_snapshot)")}
    if "scanner" not in cols:
        conn.execute("ALTER TABLE scan_funnel_snapshot ADD COLUMN scanner TEXT")
    if "drops_json" not in cols:
        conn.execute("ALTER TABLE scan_funnel_snapshot ADD COLUMN drops_json TEXT")


def insert_scan_funnel_snapshot(
    *,
    ts: str,
    market: str,
    universe_size: int,
    survivors: int,
    pass_rate_pct: float,
    scanner: Optional[str] = None,
    drops_json: Optional[str] = None,
    db_path: Optional[str] = None,
) -> None:
    """스캔 퍼널 집계 스냅샷 — PRI·weekly regime 입력."""
    path = db_path or _db_path()
    if not path:
        return
    try:
        ensure_proprietary_friction_schema(db_path=path)
        conn = sqlite3.connect(path, timeout=15)
        try:
            _migrate_scan_funnel_snapshot_columns(conn)
            conn.execute(
                """
                INSERT INTO scan_funnel_snapshot
                    (ts, market, universe_size, survivors, pass_rate_pct, scanner, drops_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(ts)[:19],
                    str(market or "").upper()[:8],
                    int(universe_size),
                    int(survivors),
                    round(float(pass_rate_pct), 6),
                    str(scanner)[:64] if scanner else None,
                    drops_json,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("scan_funnel_snapshot insert failed: %s", ex)


def insert_scan_funnel_drop_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    db_path: Optional[str] = None,
) -> None:
    """Near-miss 탈락 이벤트 배치 insert."""
    if not rows:
        return
    path = db_path or _db_path()
    if not path:
        return
    payload = []
    for row in rows:
        payload.append(
            (
                str(row.get("ts") or "")[:32],
                str(row.get("market") or "").upper()[:8],
                str(row.get("scanner") or "")[:64],
                (str(row["code"])[:32] if row.get("code") is not None else None),
                str(row.get("reason") or "")[:64],
                row.get("final_score"),
                row.get("eff_cos_cutoff"),
                row.get("eff_ml_cutoff"),
                (str(row["regime_key"])[:32] if row.get("regime_key") else None),
                row.get("rank_in_slot"),
            )
        )
    try:
        ensure_proprietary_friction_schema(db_path=path)
        conn = sqlite3.connect(path, timeout=15)
        try:
            conn.executemany(
                """
                INSERT INTO scan_funnel_drop_event
                    (ts, market, scanner, code, reason, final_score,
                     eff_cos_cutoff, eff_ml_cutoff, regime_key, rank_in_slot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("scan_funnel_drop_event insert failed: %s", ex)


def insert_regime_friction_event(
    *,
    date: str,
    market: str,
    event_type: str,
    db_path: Optional[str] = None,
) -> None:
    """일별 마찰 이벤트 — DM-A 등."""
    path = db_path or _db_path()
    if not path:
        return
    d = str(date or "")[:10]
    if len(d) != 10:
        return
    et = str(event_type or "").strip().upper()[:64]
    if not et:
        return
    try:
        ensure_proprietary_friction_schema(db_path=path)
        conn = sqlite3.connect(path, timeout=15)
        try:
            conn.execute(
                """
                INSERT INTO regime_friction_event (date, market, event_type)
                VALUES (?, ?, ?)
                """,
                (d, str(market or "").upper()[:8], et),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        logger.warning("regime_friction_event insert failed: %s", ex)

# ===========================================================================
# 👑 [가혹한 현실] 다이나믹 슬리피지(Slippage) & 마켓 임팩트(Market Impact) 산출 엔진
# ===========================================================================
def calculate_dynamic_friction_penalty(
    ep: float, 
    invest_amount: float, 
    marcap_eok: float, 
    tb: float, 
    market: str
) -> Tuple[float, str]:
    """
    진입 단가(ep)에 강제 부과할 슬리피지 패널티(초과 비용)를 계산합니다.
    
    Args:
        ep: 원래 진입하려던 현재가
        invest_amount: 쏘아올릴 켈리 베팅 자금 (원/달러)
        marcap_eok: 시가총액 (억 원 기준)
        tb: 거래대금 터짐 정도 (유동성 대용 지표)
        market: KR / US
        
    Returns:
        (조정된 비싼 진입 단가, 패널티 부여 사유 문자열)
    """
    if ep <= 0 or invest_amount <= 0:
        return ep, "zero_input"

    penalty_rate = 0.005 # 기본 0.5% 슬리피지 (세금 및 기본 호가 스프레드 가정)
    reason_parts = []

    # 1. 시가총액(유동성) 기반 마찰 (소형주 폭격 패널티)
    # 한국장 기준 시총 1000억 미만 잡주는 호가가 얇아 무조건 비싸게 사야 함
    if market == "KR" and marcap_eok > 0:
        if marcap_eok < 500:
            penalty_rate += 0.020 # 초소형주: +2.0% 강제 마찰
            reason_parts.append("MicroCap_Slippage")
        elif marcap_eok < 1000:
            penalty_rate += 0.010 # 소형주: +1.0% 강제 마찰
            reason_parts.append("SmallCap_Slippage")

    # 2. 시장 충격(Market Impact) 패널티 (시드 비대화 억제)
    # 내 투자금이 1억인데 거래가 마른 종목이면 호가를 위로 다 잡아먹게 됨
    if invest_amount > 50000000: # 5천만원 이상 투입 시
        impact_ratio = invest_amount / 50000000
        # 돈이 클수록, 거래대금(tb)이 마를수록 패널티 기하급수 증가
        safe_tb = max(0.1, float(tb))
        impact_penalty = (impact_ratio * 0.002) / safe_tb 
        
        # 최대 3%까지만 충격 적용
        impact_penalty = min(0.03, impact_penalty)
        if impact_penalty > 0.005:
            penalty_rate += impact_penalty
            reason_parts.append("MarketImpact_Heavy")

    # 3. 미국장 보정 (US는 유동성이 풍부하므로 마찰을 줄여줌)
    if market == "US":
        penalty_rate = penalty_rate * 0.4

    # 최종 패널티 캡 (어떤 최악의 상황이라도 최대 5% 위에서 샀다고 가정)
    final_penalty_rate = min(0.05, penalty_rate)
    
    # 👑 가혹한 팩트: 진입 단가를 강제로 비싸게 올림 (매수할 때 불리하게)
    adjusted_ep = ep * (1.0 + final_penalty_rate)
    
    reason_str = "Friction_[" + "|".join(reason_parts) + f"]_({final_penalty_rate*100:.1f}%)" if reason_parts else f"Base_Friction_({final_penalty_rate*100:.1f}%)"
    
    return round(adjusted_ep, 4), reason_str