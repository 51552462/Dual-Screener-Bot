"""
SQLite 스키마 방어: `CREATE TABLE IF NOT EXISTS` 이후 누락 컬럼만 `ALTER TABLE ... ADD COLUMN` 으로 보강.
DROP 금지. 기존 행 100% 유지.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

# 테이블별 (컬럼명, ADD COLUMN 뒤에 붙는 SQL 조각). 필요 시 여기만 확장.
KNOWN_COLUMN_MIGRATIONS: dict[str, list[tuple[str, str]]] = {
    "ops_events": [],
    "msg_queue": [],
    "config_kv": [],
}


def apply_column_migrations(
    conn: sqlite3.Connection,
    table: str,
    columns: Iterable[tuple[str, str]] | None = None,
) -> None:
    """테이블이 존재한다는 전제에서, 없는 컬럼만 ADD COLUMN."""
    cols = list(columns) if columns is not None else list(KNOWN_COLUMN_MIGRATIONS.get(table, []))
    if not cols:
        return
    tq = table.replace('"', "")
    try:
        cur = conn.execute(f'PRAGMA table_info("{tq}")')
        existing = {str(row[1]) for row in cur.fetchall()}
    except Exception:
        return
    for name, ddl in cols:
        name = name.strip()
        if not name or name in existing:
            continue
        try:
            conn.execute(f'ALTER TABLE "{tq}" ADD COLUMN {name} {ddl}')
        except sqlite3.OperationalError:
            pass
        else:
            existing.add(name)
