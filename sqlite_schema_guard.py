"""
SQLite 스키마 방어: `CREATE TABLE IF NOT EXISTS` 이후 누락 컬럼만 `ALTER TABLE ... ADD COLUMN` 으로 보강.
DROP 금지. 기존 행 100% 유지.

market_data.sqlite 핵심 테이블(`forward_trades` 등)이 유실된 경우 Self-healing DDL을 적용한다.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Iterable, Sequence

logger = logging.getLogger(__name__)

KNOWN_COLUMN_MIGRATIONS: dict[str, list[tuple[str, str]]] = {
    "ops_events": [],
    "msg_queue": [],
    "config_kv": [],
}

REQUIRED_MARKET_CORE_TABLES: tuple[str, ...] = ("forward_trades",)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    tq = table.replace('"', "")
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (tq,),
    ).fetchone()
    return row is not None


def missing_required_tables(
    db_path: str,
    tables: Sequence[str] = REQUIRED_MARKET_CORE_TABLES,
) -> list[str]:
    if not db_path or not os.path.isfile(db_path):
        return list(tables)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        return [t for t in tables if not table_exists(conn, t)]
    finally:
        conn.close()


def check_market_db_schema(
    db_path: str,
    tables: Sequence[str] = REQUIRED_MARKET_CORE_TABLES,
) -> dict:
    missing = missing_required_tables(db_path, tables)
    return {
        "path": db_path,
        "exists": bool(db_path and os.path.isfile(db_path)),
        "ok": not missing,
        "missing": missing,
    }


def apply_column_migrations(
    conn: sqlite3.Connection,
    table: str,
    columns: Iterable[tuple[str, str]] | None = None,
) -> None:
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


def _heal_forward_trades_at(db_path: str) -> None:
    from forward.shared import init_forward_db

    init_forward_db(db_path)


def ensure_market_db_core_schema(
    *,
    heal: bool = True,
    heal_snapshot: bool = True,
) -> dict:
    from market_db_paths import MARKET_DATA_DB_PATH, MARKET_DATA_SNAPSHOT_PATH

    result: dict = {"healed_paths": []}
    paths: list[tuple[str, str]] = [("main", MARKET_DATA_DB_PATH)]
    if heal_snapshot:
        paths.append(("snapshot", MARKET_DATA_SNAPSHOT_PATH))

    for label, path in paths:
        if label == "snapshot" and not os.path.isfile(path):
            result[label] = {"path": path, "exists": False, "ok": True, "missing": [], "skipped": True}
            continue

        check = check_market_db_schema(path)
        if not check["ok"] and heal:
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                _heal_forward_trades_at(path)
                result["healed_paths"].append(path)
                logger.warning(
                    "sqlite_schema_guard: healed missing tables %s on %s",
                    check["missing"],
                    path,
                )
            except Exception as exc:
                logger.exception("sqlite_schema_guard: heal failed for %s: %s", path, exc)
                check["heal_error"] = str(exc)
            check = check_market_db_schema(path)

        result[label] = check
        if not check.get("ok"):
            result.setdefault("error", "schema_incomplete")
            result["failed_label"] = label

    return result
