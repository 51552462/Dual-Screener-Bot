"""
4GB RAM급 서버 OOM 완화용 SQLite PRAGMA — 기존 연결 흐름에 `connect` 직후 호출만 덧붙인다.

추가(2026-06-29): `database is locked` 원천 차단을 위한 `busy_timeout` 통일 유틸.
  - 모든 SQLite 연결은 락 경합 시 즉시 실패하지 말고 `busy_timeout` 까지 질서 있게 대기해야 한다.
  - 코인측은 `bitget.infra.shared_db_connector`(busy_timeout=60000) 가 이미 SSOT.
    주식측 raw `sqlite3.connect` 들은 이 헬퍼로 동일 정책을 부여한다.
  - 정책 상수는 코인 SSOT(`BUSY_TIMEOUT_MS=60_000`)와 의도적으로 동일하게 맞춘다.
"""
from __future__ import annotations

import sqlite3

# 락 경합 대기(ms). 코인 shared_db_connector.BUSY_TIMEOUT_MS 와 동일하게 통일.
DEFAULT_BUSY_TIMEOUT_MS = 60_000


def apply_busy_timeout(conn: sqlite3.Connection, ms: int = DEFAULT_BUSY_TIMEOUT_MS) -> None:
    """락 경합 시 즉시 'database is locked' 를 던지지 말고 ms 밀리초까지 대기.

    실패해도 호출부 흐름을 막지 않도록 방어적으로 무시(연결 자체는 유효).
    """
    try:
        conn.execute(f"PRAGMA busy_timeout={int(ms)};")
    except Exception:
        pass


def apply_oom_safe_pragmas(conn: sqlite3.Connection) -> None:
    """저 RAM 서버용 OOM 완화 PRAGMA + busy_timeout(락 대기) 동시 적용.

    busy_timeout 을 함께 적용해, 이 헬퍼를 이미 호출하던 경로(market_data_fetcher 등)는
    자동으로 `database is locked` 내성을 얻는다(순수 추가 — 기존 동작 불변).
    """
    try:
        conn.execute("PRAGMA cache_size = -8000")
        conn.execute("PRAGMA mmap_size = 0")
        conn.execute("PRAGMA wal_autocheckpoint = 400")
        conn.execute("PRAGMA journal_size_limit = 67108864")
    except Exception:
        pass
    # busy_timeout 은 별도 try 로 분리 — 위 PRAGMA 중 하나가 실패해도 락 대기는 반드시 적용.
    apply_busy_timeout(conn)


def apply_lock_safe_pragmas(
    conn: sqlite3.Connection,
    *,
    wal: bool = False,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
) -> None:
    """주식측 raw 연결용 '락 안전' 공용 유틸.

    - busy_timeout 강제(필수).
    - wal=True 면 journal_mode=WAL + synchronous=NORMAL 도 적용(쓰기 연결용).
    호출부는 `sqlite3.connect(...)` 직후 한 줄만 추가하면 된다.
    """
    apply_busy_timeout(conn, busy_timeout_ms)
    if wal:
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
