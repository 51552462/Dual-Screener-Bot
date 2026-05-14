"""
4GB RAM급 서버 OOM 완화용 SQLite PRAGMA — 기존 연결 흐름에 `connect` 직후 호출만 덧붙인다.
"""
from __future__ import annotations

import sqlite3


def apply_oom_safe_pragmas(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA cache_size = -8000")
        conn.execute("PRAGMA mmap_size = 0")
        conn.execute("PRAGMA wal_autocheckpoint = 400")
        conn.execute("PRAGMA journal_size_limit = 67108864")
    except Exception:
        pass
