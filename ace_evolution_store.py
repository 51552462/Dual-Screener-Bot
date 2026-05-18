"""
AceEvolution SSOT 저장 — config_kv + market_data.sqlite ace_evolution_log.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional

from ace_evolution_schema import config_key_for_market, default_playbook
from ace_evolution_ttl import apply_ttl_to_playbook, is_playbook_expired

logger = logging.getLogger(__name__)

_LOG_DDL = """
CREATE TABLE IF NOT EXISTS ace_evolution_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of_kst TEXT NOT NULL,
    market TEXT NOT NULL,
    logic_core TEXT,
    playbook_json TEXT NOT NULL,
    confidence REAL,
    observe_only INTEGER,
    n_ace INTEGER,
    validator_notes TEXT,
    created_at TEXT DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_ace_evol_log_market_date
    ON ace_evolution_log(market, as_of_kst);
"""


def _market_db_path() -> str:
    from market_db_paths import MARKET_DATA_DB_PATH

    return MARKET_DATA_DB_PATH


def ensure_ace_evolution_log_table(db_path: Optional[str] = None) -> None:
    path = db_path or _market_db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.executescript(_LOG_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("ace_evolution_log DDL skip: %s", ex)


def load_playbook(market: str, sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    m = str(market).upper()
    key = config_key_for_market(m)
    cfg = sys_config if isinstance(sys_config, dict) else {}
    raw = cfg.get(key)
    if isinstance(raw, dict) and raw.get("logic_core"):
        pb = dict(raw)
        if is_playbook_expired(pb, cfg):
            pb["_expired"] = True
        return pb
    try:
        from config_manager import get_config_value

        disk = get_config_value(key)
        if isinstance(disk, dict) and disk.get("logic_core"):
            pb = dict(disk)
            if is_playbook_expired(pb, cfg):
                pb["_expired"] = True
            return pb
    except Exception:
        pass
    return default_playbook(m)


def save_playbook(
    playbook: Dict[str, Any],
    *,
    validator_notes: str = "",
    persist_log: bool = True,
) -> bool:
    if not isinstance(playbook, dict):
        return False
    market = str(playbook.get("market") or "KR").upper()
    pb = apply_ttl_to_playbook(playbook, market=market)

    key = config_key_for_market(market)
    try:
        from config_manager import load_system_config, save_system_config, set_config_value

        set_config_value(key, pb)
        cfg = load_system_config() or {}
        cfg[key] = pb
        save_system_config(cfg)
    except Exception as ex:
        logger.error("AceEvolution config save failed: %s", ex)
        return False

    if persist_log:
        append_log(pb, validator_notes=validator_notes)
    return True


def append_log(playbook: Dict[str, Any], *, validator_notes: str = "") -> None:
    path = _market_db_path()
    if not path:
        return
    ensure_ace_evolution_log_table(path)
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.execute(
                """
                INSERT INTO ace_evolution_log
                (as_of_kst, market, logic_core, playbook_json, confidence, observe_only, n_ace, validator_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(playbook.get("as_of_kst") or "")[:10],
                    str(playbook.get("market") or ""),
                    str(playbook.get("logic_core") or ""),
                    json.dumps(playbook, ensure_ascii=False, default=str),
                    float(playbook.get("confidence") or 0),
                    1 if playbook.get("observe_only", True) else 0,
                    int(playbook.get("n_ace") or 0),
                    str(validator_notes or "")[:500],
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("ace_evolution_log append failed: %s", ex)


def revoke_playbook(market: str, *, reason: str = "") -> None:
    """Fast-decay / 만료 시 활성 playbook 무효화."""
    m = str(market).upper()
    pb = default_playbook(m)
    pb["revoked_reason"] = str(reason)[:200]
    pb["observe_only"] = True
    save_playbook(pb, validator_notes=f"revoked:{reason}", persist_log=True)


def load_recent_logs(market: str, limit: int = 5) -> List[Dict[str, Any]]:
    path = _market_db_path()
    if not path or not os.path.isfile(path):
        return []
    ensure_ace_evolution_log_table(path)
    out: List[Dict[str, Any]] = []
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            rows = conn.execute(
                """
                SELECT as_of_kst, market, logic_core, playbook_json, confidence, observe_only, n_ace, validator_notes
                FROM ace_evolution_log
                WHERE market = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (str(market).upper(), int(limit)),
            ).fetchall()
        finally:
            conn.close()
        for row in rows:
            try:
                pb = json.loads(row[3])
            except (json.JSONDecodeError, TypeError):
                pb = {}
            out.append(
                {
                    "as_of_kst": row[0],
                    "market": row[1],
                    "logic_core": row[2],
                    "playbook": pb,
                    "confidence": row[4],
                    "observe_only": bool(row[5]),
                    "n_ace": row[6],
                    "validator_notes": row[7],
                }
            )
    except (OSError, sqlite3.Error):
        pass
    return out
