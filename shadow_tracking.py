"""
그림자 장부(Shadow Tracking): 차단·가상매매 보조 테이블 및 위성 태그 스냅샷.
운영 시계열 `ops_snapshot`: 국고·테일·롱/숏 오픈 명목 (관제탑 1분 주기 INSERT).
"""
from __future__ import annotations

import os
import random
import sqlite3
import time
from datetime import datetime

import low_ram_sqlite_pragmas

try:
    from market_db_paths import MARKET_DATA_DB_PATH as DB_PATH
except ImportError:
    DB_PATH = os.path.join(
        os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite"
    )

_OPS_DB_PATH = DB_PATH


def _toxic_ml_antipatterns_rule_map(ml_obj: object) -> dict:
    if not isinstance(ml_obj, dict):
        return {}
    inner = ml_obj.get("rules")
    if isinstance(inner, dict):
        return inner
    return {k: v for k, v in ml_obj.items() if k != "_metadata"}


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
    init_ops_snapshot_table(cursor)


def init_ops_snapshot_table(cursor) -> None:
    """관제용 시계열: 국고·테일·OPEN 롱/숏 명목 (market_data.sqlite, CQRS/설정 DB와 분리)."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            treasury_kr REAL NOT NULL,
            treasury_us REAL NOT NULL,
            tail_fund_kr REAL NOT NULL,
            tail_fund_us REAL NOT NULL,
            long_notional REAL NOT NULL,
            short_notional REAL NOT NULL
        )
        """
    )


def insert_ops_snapshot_row(
    cursor,
    *,
    treasury_kr: float,
    treasury_us: float,
    tail_fund_kr: float,
    tail_fund_us: float,
    long_notional: float,
    short_notional: float,
    timestamp: str | None = None,
) -> None:
    """동일 트랜잭션 cursor에서 1행 INSERT."""
    ts = timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        """
        INSERT INTO ops_snapshot (
            timestamp, treasury_kr, treasury_us,
            tail_fund_kr, tail_fund_us, long_notional, short_notional
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            float(treasury_kr),
            float(treasury_us),
            float(tail_fund_kr),
            float(tail_fund_us),
            float(long_notional),
            float(short_notional),
        ),
    )


def record_ops_snapshot_from_live_state(*, max_retries: int = 5) -> bool:
    """
    config_manager(KV)에서 국고·테일, forward_trades에서 OPEN 롱/숏 명목을 읽어 ops_snapshot 1행 기록.
    쓰기는 항상 메인 market_data.sqlite (스냅샷 복제본과 분리).
    """
    from config_manager import get_config_value

    def _f(x) -> float:
        try:
            return float(x or 0.0)
        except (TypeError, ValueError):
            return 0.0

    treasury_kr = _f(get_config_value("CENTRAL_TREASURY_KR", 0.0))
    treasury_us = _f(get_config_value("CENTRAL_TREASURY_US", 0.0))
    tail_fund_kr = _f(get_config_value("TAIL_RISK_FUND_KR", 0.0))
    tail_fund_us = _f(get_config_value("TAIL_RISK_FUND_US", 0.0))

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    inv_pat = "[INVERSE_ETF]"

    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(_OPS_DB_PATH, timeout=60)
            conn.execute("PRAGMA journal_mode=WAL;")
            low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
            cur = conn.cursor()
            init_ops_snapshot_table(cur)
            row = cur.execute(
                """
                SELECT
                  COALESCE(SUM(CASE
                    WHEN UPPER(IFNULL(status,'')) = 'OPEN'
                     AND IFNULL(sig_type,'') NOT LIKE '%' || ? || '%'
                    THEN COALESCE(invest_amount, sim_kelly_invest, 0)
                    ELSE 0 END), 0),
                  COALESCE(SUM(CASE
                    WHEN UPPER(IFNULL(status,'')) = 'OPEN'
                     AND IFNULL(sig_type,'') LIKE '%' || ? || '%'
                    THEN COALESCE(invest_amount, sim_kelly_invest, 0)
                    ELSE 0 END), 0)
                FROM forward_trades
                """,
                (inv_pat, inv_pat),
            ).fetchone()
            long_n = float(row[0] or 0.0) if row else 0.0
            short_n = float(row[1] or 0.0) if row else 0.0
            insert_ops_snapshot_row(
                cur,
                treasury_kr=treasury_kr,
                treasury_us=treasury_us,
                tail_fund_kr=tail_fund_kr,
                tail_fund_us=tail_fund_us,
                long_notional=long_n,
                short_notional=short_n,
                timestamp=ts,
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
    tox_rules = _toxic_ml_antipatterns_rule_map(config.get("TOXIC_ML_ANTIPATTERNS"))
    if isinstance(tox_rules, dict) and len(tox_rules) > 0:
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
            init_shadow_tables(cur)
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
