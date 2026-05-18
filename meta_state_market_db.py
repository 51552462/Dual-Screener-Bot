"""
MetaGovernor 스냅샷 — market_data.sqlite SSOT (불사조 레이어).

git clean 으로 JSON·config 만 유실돼도 market_data 백업에 메타 뇌가 남는다.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from market_db_paths import MARKET_DATA_DB_PATH

logger = logging.getLogger(__name__)

META_STATE_LOG_TABLE = "meta_state_log"
_DEFAULT_SCOPE = "GLOBAL"


def ensure_meta_state_log_schema(db_path: Optional[str] = None) -> None:
    p = db_path or MARKET_DATA_DB_PATH
    conn = sqlite3.connect(p, timeout=60)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {META_STATE_LOG_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL DEFAULT '{_DEFAULT_SCOPE}',
                state_json TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                governor_status TEXT,
                regime_key TEXT,
                schema_version TEXT
            )
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_meta_state_log_scope_time
            ON {META_STATE_LOG_TABLE}(scope, id DESC)
            """
        )
        conn.commit()
    finally:
        conn.close()


def save_meta_state_to_market_db(
    state: Dict[str, Any],
    *,
    scope: str = _DEFAULT_SCOPE,
    db_path: Optional[str] = None,
    keep_last: int = 48,
) -> None:
    """Governor 완료 스냅샷 append + 오래된 행 정리."""
    if not isinstance(state, dict):
        return
    p = db_path or MARKET_DATA_DB_PATH
    ensure_meta_state_log_schema(p)
    payload = json.dumps(state, ensure_ascii=False)
    now = datetime.now(timezone.utc).isoformat()
    status = str(state.get("META_GOVERNOR_LAST_RUN_STATUS") or "")
    regime = str(state.get("META_REGIME_KEY") or "")
    schema = str(state.get("META_SCHEMA_VERSION") or "")

    conn = sqlite3.connect(p, timeout=60)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            f"""
            INSERT INTO {META_STATE_LOG_TABLE}
            (scope, state_json, updated_at_utc, governor_status, regime_key, schema_version)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (scope, payload, now, status, regime, schema),
        )
        if keep_last > 0:
            conn.execute(
                f"""
                DELETE FROM {META_STATE_LOG_TABLE}
                WHERE scope = ? AND id NOT IN (
                    SELECT id FROM {META_STATE_LOG_TABLE}
                    WHERE scope = ?
                    ORDER BY id DESC
                    LIMIT ?
                )
                """,
                (scope, scope, int(keep_last)),
            )
        conn.commit()
    finally:
        conn.close()


def load_meta_state_from_market_db(
    *,
    scope: str = _DEFAULT_SCOPE,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """최신 스냅샷 1건."""
    p = db_path or MARKET_DATA_DB_PATH
    if not __import__("os").path.isfile(p):
        return None
    try:
        ensure_meta_state_log_schema(p)
    except Exception as e:
        logger.warning("meta_state_market_db: schema init failed: %s", e)
        return None

    conn = sqlite3.connect(p, timeout=60)
    try:
        row = conn.execute(
            f"""
            SELECT state_json FROM {META_STATE_LOG_TABLE}
            WHERE scope = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (scope,),
        ).fetchone()
    except sqlite3.Error as e:
        logger.warning("meta_state_market_db: load failed: %s", e)
        return None
    finally:
        conn.close()

    if not row or not row[0]:
        return None
    try:
        raw = json.loads(str(row[0]))
        return raw if isinstance(raw, dict) else None
    except json.JSONDecodeError:
        return None
