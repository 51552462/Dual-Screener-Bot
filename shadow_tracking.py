"""
그림자 장부(Shadow Tracking): 차단·가상매매 보조 테이블 및 위성 태그 스냅샷.
"""
from __future__ import annotations

import os
import random
import sqlite3
import time
from datetime import datetime

DB_PATH = os.path.join(
    os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite"
)


def init_shadow_tables(cursor) -> None:
    """forward_trades 초기화와 동일 커넥션에서 호출: 차단/그림자 테이블 생성."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blocked_trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            name TEXT,
            reason TEXT,
            entry_price REAL,
            blocked_at TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS virtual_trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT,
            code TEXT,
            name TEXT,
            entry_price REAL,
            sig_type TEXT,
            satellite_tags TEXT,
            logged_at TEXT
        )
        """
    )
    try:
        cursor.execute("ALTER TABLE virtual_trade_history ADD COLUMN satellite_tags TEXT")
    except sqlite3.OperationalError:
        pass


def build_satellite_tags(config: dict) -> str:
    """system_config 스냅샷을 콤마 구분 문자열로 압축."""
    if not isinstance(config, dict):
        return ""
    parts = []
    dd = config.get("DOOMSDAY_DEFCON") or {}
    if isinstance(dd, dict):
        try:
            parts.append(f"DEFCON={int(dd.get('level', 5))}")
        except (TypeError, ValueError):
            parts.append("DEFCON=?")
    rad = config.get("SMART_MONEY_RADAR") or {}
    picks = {}
    if isinstance(rad, dict):
        picks = rad.get("picks") or {}
    parts.append(f"SMART_MONEY_ACTIVE={'yes' if isinstance(picks, dict) and len(picks) > 0 else 'no'}")
    parts.append(f"GLOBAL_CIRCUIT_BREAKER={config.get('GLOBAL_CIRCUIT_BREAKER', 'OFF')}")
    bh = config.get("BLACKHOLE_TOXIC_COUNT")
    if isinstance(bh, dict):
        try:
            parts.append(f"BLACKHOLE_CNT={int(bh.get('count', 0) or 0)}")
        except (TypeError, ValueError):
            parts.append("BLACKHOLE_CNT=?")
    else:
        parts.append("BLACKHOLE_CNT=0")
    tox_ml = config.get("TOXIC_ML_ANTIPATTERNS")
    if isinstance(tox_ml, dict) and len(tox_ml) > 0:
        parts.append("TOXIC_ML_RULES=yes")
    else:
        parts.append("TOXIC_ML_RULES=no")
    return ",".join(parts)


def record_blocked_trade(
    code,
    name,
    reason,
    entry_price,
    *,
    max_retries: int = 5,
) -> bool:
    """매수 포기(차단) 1건 기록. 장갑차 재시도."""
    blocked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=60)
            conn.execute("PRAGMA journal_mode=WAL;")
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO blocked_trade_history (code, name, reason, entry_price, blocked_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(code)[:32],
                    str(name)[:200],
                    str(reason)[:500],
                    float(entry_price) if entry_price is not None else 0.0,
                    blocked_at,
                ),
            )
            conn.commit()
            conn.close()
            return True
        except sqlite3.OperationalError:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                return False
    return False


def insert_virtual_trade_row(
    cursor,
    market: str,
    code_str: str,
    name: str,
    entry_price: float,
    sig_type: str,
    satellite_tags: str,
    logged_at: str,
) -> None:
    """try_add_virtual_position 트랜잭션 안에서만 호출 (동일 cursor)."""
    cursor.execute(
        """
        INSERT INTO virtual_trade_history (market, code, name, entry_price, sig_type, satellite_tags, logged_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            market,
            str(code_str)[:32],
            str(name)[:200],
            float(entry_price),
            str(sig_type)[:800],
            str(satellite_tags)[:2000] if satellite_tags else "",
            logged_at,
        ),
    )
