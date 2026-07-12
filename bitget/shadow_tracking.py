"""
그림자 장부(Shadow Tracking): Bitget 차단·가상매매 보조 테이블 및 위성 태그 스냅샷.
"""
from __future__ import annotations

import os
import random
import sqlite3
import time

from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.shared_db_connector import get_connection

import memory_bounds

DB_PATH = market_data_db_path()


def _toxic_ml_antipatterns_rule_map(ml_obj: object) -> dict:
    if not isinstance(ml_obj, dict):
        return {}
    inner = ml_obj.get("rules")
    if isinstance(inner, dict):
        return inner
    return {k: v for k, v in ml_obj.items() if k != "_metadata"}


def init_shadow_tables(cursor) -> None:
    """bitget_forward_trades 초기화와 동일 커넥션에서 호출: 차단/그림자 테이블 생성."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bitget_blocked_trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_type TEXT,
            symbol TEXT,
            name TEXT,
            reason TEXT,
            position_side TEXT,
            timeframe TEXT,
            entry_price REAL,
            blocked_at TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bitget_virtual_trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_type TEXT,
            symbol TEXT,
            name TEXT,
            entry_price REAL,
            sig_type TEXT,
            position_side TEXT,
            timeframe TEXT,
            satellite_tags TEXT,
            logged_at TEXT
        )
        """
    )
    try:
        cursor.execute("ALTER TABLE bitget_virtual_trade_history ADD COLUMN satellite_tags TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE bitget_virtual_trade_history ADD COLUMN position_side TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE bitget_virtual_trade_history ADD COLUMN timeframe TEXT")
    except sqlite3.OperationalError:
        pass


def build_satellite_tags(config: dict) -> str:
    """bitget_system_config 스냅샷을 콤마 구분 문자열로 압축."""
    if not isinstance(config, dict):
        return ""
    parts = []
    dd = config.get("DOOMSDAY_DEFCON") or {}
    if isinstance(dd, dict):
        try:
            parts.append(f"DEFCON={int(dd.get('level', 5))}")
        except (TypeError, ValueError):
            parts.append("DEFCON=?")
    parts.append(f"GLOBAL_CIRCUIT_BREAKER={config.get('GLOBAL_CIRCUIT_BREAKER', 'OFF')}")
    parts.append(f"BREADTH={config.get('CRYPTO_BREADTH_STATUS', 'UNKNOWN')}")
    tox_rules = _toxic_ml_antipatterns_rule_map(config.get("TOXIC_ML_ANTIPATTERNS"))
    if isinstance(tox_rules, dict) and len(tox_rules) > 0:
        parts.append("TOXIC_ML_RULES=yes")
    else:
        parts.append("TOXIC_ML_RULES=no")
    gmm = config.get("BITGET_GMM_DNA_TEMPLATES")
    if isinstance(gmm, dict) and len(gmm) > 0:
        parts.append("GMM_DNA=yes")
    else:
        parts.append("GMM_DNA=no")
    return ",".join(parts)


def record_blocked_trade(
    symbol,
    reason,
    entry_price,
    market_type="spot",
    *,
    name="",
    position_side="LONG",
    timeframe="1D",
    max_retries: int = 5,
) -> bool:
    """매수/매도 포기(차단) 1건 기록. 장갑차 재시도."""
    blocked_at = utc_datetime_str()
    for attempt in range(max_retries):
        try:
            conn = get_connection(DB_PATH)
            cur = conn.cursor()
            init_shadow_tables(cur)
            cur.execute(
                """
                INSERT INTO bitget_blocked_trade_history
                (market_type, symbol, name, reason, position_side, timeframe, entry_price, blocked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(market_type).lower(),
                    str(symbol)[:64],
                    str(name)[:200],
                    str(reason)[:500],
                    str(position_side).upper()[:16],
                    str(timeframe).upper()[:16],
                    float(entry_price) if entry_price is not None else 0.0,
                    blocked_at,
                ),
            )
            conn.commit()
            conn.close()
            try:
                from bitget.infra.memory_retention import maybe_run_bitget_retention_after_write

                maybe_run_bitget_retention_after_write()
            except Exception:
                pass
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
    position_side: str = "LONG",
    timeframe: str = "1D",
) -> None:
    """try_add_virtual_position 트랜잭션 안에서만 호출 (동일 cursor)."""
    cursor.execute(
        """
        INSERT INTO bitget_virtual_trade_history
        (market_type, symbol, name, entry_price, sig_type, position_side, timeframe, satellite_tags, logged_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(market).lower(),
            str(code_str)[:64],
            str(name)[:200],
            float(entry_price),
            str(sig_type)[:800],
            str(position_side).upper()[:16],
            str(timeframe).upper()[:16],
            str(satellite_tags)[:2000] if satellite_tags else "",
            logged_at,
        ),
    )
