"""
Global Data Access Layer (DAL) — 표준 SQLite 커넥터 (SSOT).

[문제] 모듈마다 sqlite3.connect 타임아웃/PRAGMA 가 제각각(7s~120s)이라
동시 writer 경합 시 `database is locked` 가 빈발했다.

[해결] 시스템의 모든 SQLite 접속을 이 팩토리 하나로 통일한다.
Traffic Rule(강제, 예외 없음):
  - timeout = 60s  (sqlite3 busy handler)
  - PRAGMA journal_mode = WAL       → 무제한 동시 READ + 단일 WRITE
  - PRAGMA synchronous = NORMAL     → WAL 안전성 유지하며 fsync 병목 제거
  - PRAGMA busy_timeout = 60000(ms) → 락 경합 시 60s 까지 질서 있게 대기

사용 예:
    from bitget.infra.shared_db_connector import get_connection, connect

    # 1) 함수형 (직접 close 책임)
    conn = get_connection(db_path)
    ...
    conn.close()

    # 2) 컨텍스트 매니저 (자동 commit/rollback/close)
    with connect(db_path) as conn:
        conn.execute("INSERT ...")

    # 3) 읽기 전용 (mode=ro + query_only — writer 락 비차단)
    with connect(db_path, read_only=True) as conn:
        rows = conn.execute("SELECT ...").fetchall()
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

# --- Traffic Rule 상수 (변경 금지 — 전 시스템 일관성의 핵심) ---
DEFAULT_TIMEOUT_SEC: float = 60.0
BUSY_TIMEOUT_MS: int = 60_000


def _normalize_path(db_path: str) -> str:
    return os.path.abspath(os.path.expanduser(str(db_path)))


def _ro_uri(path: str) -> str:
    # Windows 경로 호환 — 역슬래시를 슬래시로.
    return f"file:{path.replace(os.sep, '/')}?mode=ro"


def _apply_traffic_rules(conn: sqlite3.Connection, *, read_only: bool) -> None:
    """모든 커넥션에 강제되는 PRAGMA 3종 세트."""
    # busy_timeout 은 read/write 모두 — 락 대기 60s.
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")
    if read_only:
        # 읽기 전용 연결은 절대 쓰지 않음을 SQLite 에 보장 → writer 락 비차단.
        conn.execute("PRAGMA query_only=ON;")
        return
    # WAL/synchronous 는 DB 파일 단위 영속 설정. writer 연결에서만 보장.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")


def get_connection(
    db_path: str,
    *,
    read_only: bool = False,
    timeout: float = DEFAULT_TIMEOUT_SEC,
    row_factory: Optional[Callable[[sqlite3.Cursor, tuple], Any]] = None,
    check_same_thread: bool = True,
    isolation_level: Any = "",
) -> sqlite3.Connection:
    """
    표준화된 SQLite 커넥션을 생성한다. (Traffic Rule 강제)

    Args:
        db_path: SQLite 파일 경로.
        read_only: True 면 file:...?mode=ro + query_only (writer 비차단).
        timeout: busy handler 대기(초). 기본 60s — 일괄 적용. 60 미만으로 낮추지 말 것.
        row_factory: 예) sqlite3.Row.
        check_same_thread: 멀티스레드 공유 시 False.
        isolation_level: sqlite3 트랜잭션 모드. 기본 "" (Python 기본).
            BEGIN EXCLUSIVE 등 수동 트랜잭션이 필요하면 None(autocommit) 전달.
    """
    # timeout 은 절대 60s 미만이 되지 않도록 하한 강제 (Traffic Rule).
    eff_timeout = max(float(DEFAULT_TIMEOUT_SEC), float(timeout))
    path = _normalize_path(db_path)

    if read_only:
        conn = sqlite3.connect(
            _ro_uri(path),
            uri=True,
            timeout=eff_timeout,
            check_same_thread=check_same_thread,
            isolation_level=isolation_level,
        )
    else:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        conn = sqlite3.connect(
            path,
            timeout=eff_timeout,
            check_same_thread=check_same_thread,
            isolation_level=isolation_level,
        )

    if row_factory is not None:
        conn.row_factory = row_factory
    _apply_traffic_rules(conn, read_only=read_only)
    return conn


@contextmanager
def connect(
    db_path: str,
    *,
    read_only: bool = False,
    timeout: float = DEFAULT_TIMEOUT_SEC,
    row_factory: Optional[Callable[[sqlite3.Cursor, tuple], Any]] = None,
    check_same_thread: bool = True,
    isolation_level: Any = "",
) -> Iterator[sqlite3.Connection]:
    """
    컨텍스트 매니저. 정상 종료 시 commit, 예외 시 rollback, 항상 close.
    read_only=True 면 commit/rollback 을 건너뛴다.
    """
    conn = get_connection(
        db_path,
        read_only=read_only,
        timeout=timeout,
        row_factory=row_factory,
        check_same_thread=check_same_thread,
        isolation_level=isolation_level,
    )
    try:
        yield conn
        if not read_only:
            conn.commit()
    except Exception:
        if not read_only:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def apply_traffic_rules(conn: sqlite3.Connection, *, read_only: bool = False) -> sqlite3.Connection:
    """
    이미 존재하는 외부 커넥션(레거시)에 동일 Traffic Rule 을 사후 적용한다.
    점진적 마이그레이션용 — 신규 코드는 get_connection/connect 사용 권장.
    """
    _apply_traffic_rules(conn, read_only=read_only)
    return conn
