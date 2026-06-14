<<<<<<< HEAD
"""
Bitget 시스템 설정: SQLite KV + 낙관적 동시성(OCC).

- DB: `bitget_system_config.sqlite` (market_data와 분리)
- 레거시: `bitget_system_config.json` (DB 비어 있을 때 읽기 전용 bootstrap)
- 비밀(API 키·토큰)은 `.env` / `bitget/env.py` 만 사용
"""
from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Optional

import low_ram_sqlite_pragmas
import sqlite_schema_guard

from bitget.infra.data_paths import (
    bitget_data_dir,
    system_config_db_path,
    system_config_json_path,
)

_SENSITIVE_KEY_RE = re.compile(
    r"(TOKEN|SECRET|PASSPHRASE|PASSWORD|PRIVATE[_-]?KEY|API[_-]?KEY|CREDENTIAL|AUTHORIZATION|WEBHOOK)",
    re.I,
)

CONFIG_DIR = bitget_data_dir()
CONFIG_PATH = system_config_json_path()
CONFIG_DB_PATH = system_config_db_path()
CONFIG_SNAPSHOTS_DIR = os.path.join(CONFIG_DIR, "bitget_config_snapshots")
_MAX_CONFIG_SNAPSHOT_FILES = 365
LOCK_PATH = os.path.join(CONFIG_DIR, ".bitget_config_kv.lock")


class ConfigConcurrencyError(RuntimeError):
    """update_config_value OCC 실패."""


ModifierFunc = Callable[[Any], Any]


def _is_sensitive_config_key(key: str) -> bool:
    return bool(_SENSITIVE_KEY_RE.search(str(key)))


def strip_sensitive_from_config_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if _is_sensitive_config_key(ks):
                continue
            out[ks] = strip_sensitive_from_config_obj(v)
        return out
    if isinstance(obj, list):
        return [strip_sensitive_from_config_obj(x) for x in obj]
    return obj


def _ensure_config_dir() -> None:
    if CONFIG_DIR:
        os.makedirs(CONFIG_DIR, exist_ok=True)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS config_kv (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            version INTEGER NOT NULL
        )
        """
    )
    sqlite_schema_guard.apply_column_migrations(conn, "config_kv")
    conn.commit()


def _connect() -> sqlite3.Connection:
    _ensure_config_dir()
    conn = sqlite3.connect(CONFIG_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
    _ensure_schema(conn)
    return conn


def _retry_on_locked(fn: Callable[[], Any], *, max_retries: int = 5) -> Any:
    last: Optional[BaseException] = None
    for attempt in range(max_retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last = e
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            if attempt < max_retries - 1:
                time.sleep(0.05 + random.uniform(0, 0.15))
    assert last is not None
    raise last


def _read_json_file(path: str, max_retries: int = 5) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    for attempt in range(max_retries):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [bitget.config_manager] JSON 읽기 실패: {path} — {e}")
                return {}
    return {}


def _load_legacy_json_view(max_retries: int = 5) -> dict[str, Any]:
    legacy = _read_json_file(CONFIG_PATH, max_retries=max_retries)
    pkg_legacy = os.path.join(os.path.dirname(os.path.dirname(__file__)), "bitget_system_config.json")
    if not legacy and os.path.isfile(pkg_legacy) and os.path.abspath(pkg_legacy) != os.path.abspath(CONFIG_PATH):
        legacy = _read_json_file(pkg_legacy, max_retries=max_retries)
    return legacy


def config_persisted() -> bool:
    if os.path.isfile(CONFIG_PATH):
        return True
    if not os.path.isfile(CONFIG_DB_PATH):
        return False
    try:
        conn = sqlite3.connect(CONFIG_DB_PATH, timeout=30.0)
        try:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='config_kv' LIMIT 1"
            )
            if cur.fetchone() is None:
                return False
            cur = conn.execute("SELECT 1 FROM config_kv LIMIT 1")
            return cur.fetchone() is not None
        finally:
            conn.close()
    except OSError:
        return False


def _sqlite_row_count(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT COUNT(*) AS c FROM config_kv")
    row = cur.fetchone()
    return int(row["c"]) if row else 0


def _decode_json(text: str) -> Any:
    return json.loads(text)


def _encode_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def get_config_value(key: str, default_value: Any = None) -> Any:
    if not key:
        return default_value

    def _read() -> Any:
        conn = _connect()
        try:
            cur = conn.execute(
                "SELECT value_json, version FROM config_kv WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                return default_value
            return _decode_json(str(row["value_json"]))
        finally:
            conn.close()

    try:
        return _retry_on_locked(_read)
    except (json.JSONDecodeError, OSError, sqlite3.Error) as e:
        print(f"⚠️ [bitget.config_manager] get_config_value({key!r}) 실패: {e}")
        return default_value


def set_config_value(key: str, value: Any) -> None:
    if not key:
        raise ValueError("config key must be non-empty")
    if _is_sensitive_config_key(key):
        raise ValueError(
            f"config key {key!r} looks like a secret; use .env (BITGET_* ) instead"
        )

    payload = _encode_json(
        strip_sensitive_from_config_obj(value) if isinstance(value, (dict, list)) else value
    )

    def _write() -> None:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute("SELECT version FROM config_kv WHERE key = ?", (key,))
            row = cur.fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (key, payload),
                )
            else:
                conn.execute(
                    """
                    UPDATE config_kv SET value_json = ?, version = version + 1 WHERE key = ?
                    """,
                    (payload, key),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    _retry_on_locked(_write)
    invalidate_runtime_system_config_cache()


def update_config_value(
    key: str,
    modifier: ModifierFunc,
    *,
    max_retries: int = 8,
) -> Any:
    if not key:
        raise ValueError("config key must be non-empty")
    if _is_sensitive_config_key(key):
        raise ValueError(f"config key {key!r} looks like a secret")

    for attempt in range(max_retries):
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "SELECT value_json, version FROM config_kv WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                current = None
                version = 0
            else:
                current = _decode_json(str(row["value_json"]))
                version = int(row["version"])
            new_val = modifier(current)
            payload = _encode_json(
                strip_sensitive_from_config_obj(new_val)
                if isinstance(new_val, (dict, list))
                else new_val
            )
            if row is None:
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (key, payload),
                )
            else:
                updated = conn.execute(
                    """
                    UPDATE config_kv SET value_json = ?, version = version + 1
                    WHERE key = ? AND version = ?
                    """,
                    (payload, key, version),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    continue
            conn.commit()
            invalidate_runtime_system_config_cache()
            return new_val
        except ConfigConcurrencyError:
            raise
        except sqlite3.OperationalError as e:
            conn.rollback()
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(0.05 + random.uniform(0, 0.1) * (attempt + 1))
                continue
            raise
        finally:
            conn.close()
    raise ConfigConcurrencyError(f"OCC failed for key={key!r} after {max_retries} retries")


def load_system_config(max_retries: int = 5) -> dict[str, Any]:
    def _load_sqlite() -> dict[str, Any]:
        conn = _connect()
        try:
            if _sqlite_row_count(conn) == 0:
                return {}
            cur = conn.execute("SELECT key, value_json FROM config_kv")
            out: dict[str, Any] = {}
            for r in cur.fetchall():
                k = str(r["key"])
                try:
                    out[k] = _decode_json(str(r["value_json"]))
                except json.JSONDecodeError:
                    print(f"⚠️ [bitget.config_manager] 손상된 JSON 건너뜀: key={k!r}")
            return out
        finally:
            conn.close()

    try:
        blob = _retry_on_locked(_load_sqlite, max_retries=max_retries)
    except (OSError, sqlite3.Error) as e:
        print(f"⚠️ [bitget.config_manager] SQLite 로드 실패, JSON 시도: {e}")
        blob = {}

    if blob:
        return strip_sensitive_from_config_obj(blob)
    return strip_sensitive_from_config_obj(_load_legacy_json_view(max_retries=max_retries))


_RUNTIME_CFG_LOCK = threading.Lock()
_RUNTIME_CFG_TS: float = 0.0
_RUNTIME_CFG_DATA: Optional[dict[str, Any]] = None


def invalidate_runtime_system_config_cache() -> None:
    global _RUNTIME_CFG_TS, _RUNTIME_CFG_DATA
    with _RUNTIME_CFG_LOCK:
        _RUNTIME_CFG_DATA = None
        _RUNTIME_CFG_TS = 0.0


def load_runtime_system_config(ttl_seconds: float = 60.0, *, max_retries: int = 5) -> dict[str, Any]:
    global _RUNTIME_CFG_TS, _RUNTIME_CFG_DATA
    if ttl_seconds <= 0:
        return load_system_config(max_retries=max_retries)
    now = time.monotonic()
    with _RUNTIME_CFG_LOCK:
        if _RUNTIME_CFG_DATA is not None and (now - _RUNTIME_CFG_TS) < float(ttl_seconds):
            return _RUNTIME_CFG_DATA
    fresh = load_system_config(max_retries=max_retries)
    with _RUNTIME_CFG_LOCK:
        _RUNTIME_CFG_DATA = fresh
        _RUNTIME_CFG_TS = time.monotonic()
    return fresh


def save_system_config(config_data: Mapping[str, Any], max_retries: int = 5) -> bool:
    if not isinstance(config_data, dict):
        return False
    config_data = strip_sensitive_from_config_obj(dict(config_data))

    def _save() -> None:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM config_kv")
            for k, v in config_data.items():
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (str(k), _encode_json(v)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        _retry_on_locked(_save, max_retries=max_retries)
        invalidate_runtime_system_config_cache()
        return True
    except Exception as e:
        print(f"🚨 [bitget.config_manager] save_system_config 실패: {e}")
        return False


def update_system_config(updates_dict: Mapping[str, Any], max_retries: int = 5) -> bool:
    if not isinstance(updates_dict, dict) or not updates_dict:
        return True
    merged = load_system_config(max_retries=max_retries)
    merged.update(strip_sensitive_from_config_obj(dict(updates_dict)))
    return save_system_config(merged, max_retries=max_retries)


def load_config(max_retries: int = 5) -> dict[str, Any]:
    return load_system_config(max_retries=max_retries)


def save_config(config_data: Mapping[str, Any], max_retries: int = 5) -> bool:
    return save_system_config(config_data, max_retries=max_retries)


def bootstrap_from_json_if_empty(*, max_retries: int = 5) -> bool:
    """SQLite가 비어 있고 JSON이 있으면 1회 import."""

    def _count_rows() -> int:
        conn = _connect()
        try:
            return _sqlite_row_count(conn)
        finally:
            conn.close()

    try:
        if _retry_on_locked(_count_rows, max_retries=max_retries) > 0:
            return False
    except (OSError, sqlite3.Error):
        pass
    legacy = _load_legacy_json_view(max_retries=max_retries)
    if not legacy:
        return False
    ok = save_system_config(legacy, max_retries=max_retries)
    if ok:
        print(f"📦 [bitget.config_manager] JSON → SQLite bootstrap: {CONFIG_PATH}")
    return ok
=======
"""
Bitget 시스템 설정: SQLite KV + 낙관적 동시성(OCC).

- DB: `bitget_system_config.sqlite` (market_data와 분리)
- 레거시: `bitget_system_config.json` (DB 비어 있을 때 읽기 전용 bootstrap)
- 비밀(API 키·토큰)은 `.env` / `bitget/env.py` 만 사용
"""
from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Optional

import low_ram_sqlite_pragmas
import sqlite_schema_guard

from bitget.infra.data_paths import (
    bitget_data_dir,
    system_config_db_path,
    system_config_json_path,
)

_SENSITIVE_KEY_RE = re.compile(
    r"(TOKEN|SECRET|PASSPHRASE|PASSWORD|PRIVATE[_-]?KEY|API[_-]?KEY|CREDENTIAL|AUTHORIZATION|WEBHOOK)",
    re.I,
)

CONFIG_DIR = bitget_data_dir()
CONFIG_PATH = system_config_json_path()
CONFIG_DB_PATH = system_config_db_path()
CONFIG_SNAPSHOTS_DIR = os.path.join(CONFIG_DIR, "bitget_config_snapshots")
_MAX_CONFIG_SNAPSHOT_FILES = 365
LOCK_PATH = os.path.join(CONFIG_DIR, ".bitget_config_kv.lock")


class ConfigConcurrencyError(RuntimeError):
    """update_config_value OCC 실패."""


ModifierFunc = Callable[[Any], Any]


def _is_sensitive_config_key(key: str) -> bool:
    return bool(_SENSITIVE_KEY_RE.search(str(key)))


def strip_sensitive_from_config_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if _is_sensitive_config_key(ks):
                continue
            out[ks] = strip_sensitive_from_config_obj(v)
        return out
    if isinstance(obj, list):
        return [strip_sensitive_from_config_obj(x) for x in obj]
    return obj


def _ensure_config_dir() -> None:
    if CONFIG_DIR:
        os.makedirs(CONFIG_DIR, exist_ok=True)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS config_kv (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            version INTEGER NOT NULL
        )
        """
    )
    sqlite_schema_guard.apply_column_migrations(conn, "config_kv")
    conn.commit()


def _connect() -> sqlite3.Connection:
    _ensure_config_dir()
    conn = sqlite3.connect(CONFIG_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
    _ensure_schema(conn)
    return conn


def _retry_on_locked(fn: Callable[[], Any], *, max_retries: int = 5) -> Any:
    last: Optional[BaseException] = None
    for attempt in range(max_retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last = e
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            if attempt < max_retries - 1:
                time.sleep(0.05 + random.uniform(0, 0.15))
    assert last is not None
    raise last


def _read_json_file(path: str, max_retries: int = 5) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    for attempt in range(max_retries):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [bitget.config_manager] JSON 읽기 실패: {path} — {e}")
                return {}
    return {}


def _load_legacy_json_view(max_retries: int = 5) -> dict[str, Any]:
    legacy = _read_json_file(CONFIG_PATH, max_retries=max_retries)
    pkg_legacy = os.path.join(os.path.dirname(os.path.dirname(__file__)), "bitget_system_config.json")
    if not legacy and os.path.isfile(pkg_legacy) and os.path.abspath(pkg_legacy) != os.path.abspath(CONFIG_PATH):
        legacy = _read_json_file(pkg_legacy, max_retries=max_retries)
    return legacy


def config_persisted() -> bool:
    if os.path.isfile(CONFIG_PATH):
        return True
    if not os.path.isfile(CONFIG_DB_PATH):
        return False
    try:
        conn = sqlite3.connect(CONFIG_DB_PATH, timeout=30.0)
        try:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='config_kv' LIMIT 1"
            )
            if cur.fetchone() is None:
                return False
            cur = conn.execute("SELECT 1 FROM config_kv LIMIT 1")
            return cur.fetchone() is not None
        finally:
            conn.close()
    except OSError:
        return False


def _sqlite_row_count(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT COUNT(*) AS c FROM config_kv")
    row = cur.fetchone()
    return int(row["c"]) if row else 0


def _decode_json(text: str) -> Any:
    return json.loads(text)


def _encode_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def get_config_value(key: str, default_value: Any = None) -> Any:
    if not key:
        return default_value

    def _read() -> Any:
        conn = _connect()
        try:
            cur = conn.execute(
                "SELECT value_json, version FROM config_kv WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                return default_value
            return _decode_json(str(row["value_json"]))
        finally:
            conn.close()

    try:
        return _retry_on_locked(_read)
    except (json.JSONDecodeError, OSError, sqlite3.Error) as e:
        print(f"⚠️ [bitget.config_manager] get_config_value({key!r}) 실패: {e}")
        return default_value


def set_config_value(key: str, value: Any) -> None:
    if not key:
        raise ValueError("config key must be non-empty")
    if _is_sensitive_config_key(key):
        raise ValueError(
            f"config key {key!r} looks like a secret; use .env (BITGET_* ) instead"
        )

    payload = _encode_json(
        strip_sensitive_from_config_obj(value) if isinstance(value, (dict, list)) else value
    )

    def _write() -> None:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute("SELECT version FROM config_kv WHERE key = ?", (key,))
            row = cur.fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (key, payload),
                )
            else:
                conn.execute(
                    """
                    UPDATE config_kv SET value_json = ?, version = version + 1 WHERE key = ?
                    """,
                    (payload, key),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    _retry_on_locked(_write)
    invalidate_runtime_system_config_cache()


def update_config_value(
    key: str,
    modifier: ModifierFunc,
    *,
    max_retries: int = 8,
) -> Any:
    if not key:
        raise ValueError("config key must be non-empty")
    if _is_sensitive_config_key(key):
        raise ValueError(f"config key {key!r} looks like a secret")

    for attempt in range(max_retries):
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "SELECT value_json, version FROM config_kv WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                current = None
                version = 0
            else:
                current = _decode_json(str(row["value_json"]))
                version = int(row["version"])
            new_val = modifier(current)
            payload = _encode_json(
                strip_sensitive_from_config_obj(new_val)
                if isinstance(new_val, (dict, list))
                else new_val
            )
            if row is None:
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (key, payload),
                )
            else:
                updated = conn.execute(
                    """
                    UPDATE config_kv SET value_json = ?, version = version + 1
                    WHERE key = ? AND version = ?
                    """,
                    (payload, key, version),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    continue
            conn.commit()
            invalidate_runtime_system_config_cache()
            return new_val
        except ConfigConcurrencyError:
            raise
        except sqlite3.OperationalError as e:
            conn.rollback()
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(0.05 + random.uniform(0, 0.1) * (attempt + 1))
                continue
            raise
        finally:
            conn.close()
    raise ConfigConcurrencyError(f"OCC failed for key={key!r} after {max_retries} retries")


def load_system_config(max_retries: int = 5) -> dict[str, Any]:
    def _load_sqlite() -> dict[str, Any]:
        conn = _connect()
        try:
            if _sqlite_row_count(conn) == 0:
                return {}
            cur = conn.execute("SELECT key, value_json FROM config_kv")
            out: dict[str, Any] = {}
            for r in cur.fetchall():
                k = str(r["key"])
                try:
                    out[k] = _decode_json(str(r["value_json"]))
                except json.JSONDecodeError:
                    print(f"⚠️ [bitget.config_manager] 손상된 JSON 건너뜀: key={k!r}")
            return out
        finally:
            conn.close()

    try:
        blob = _retry_on_locked(_load_sqlite, max_retries=max_retries)
    except (OSError, sqlite3.Error) as e:
        print(f"⚠️ [bitget.config_manager] SQLite 로드 실패, JSON 시도: {e}")
        blob = {}

    if blob:
        return strip_sensitive_from_config_obj(blob)
    return strip_sensitive_from_config_obj(_load_legacy_json_view(max_retries=max_retries))


_RUNTIME_CFG_LOCK = threading.Lock()
_RUNTIME_CFG_TS: float = 0.0
_RUNTIME_CFG_DATA: Optional[dict[str, Any]] = None


def invalidate_runtime_system_config_cache() -> None:
    global _RUNTIME_CFG_TS, _RUNTIME_CFG_DATA
    with _RUNTIME_CFG_LOCK:
        _RUNTIME_CFG_DATA = None
        _RUNTIME_CFG_TS = 0.0


def load_runtime_system_config(ttl_seconds: float = 60.0, *, max_retries: int = 5) -> dict[str, Any]:
    global _RUNTIME_CFG_TS, _RUNTIME_CFG_DATA
    if ttl_seconds <= 0:
        return load_system_config(max_retries=max_retries)
    now = time.monotonic()
    with _RUNTIME_CFG_LOCK:
        if _RUNTIME_CFG_DATA is not None and (now - _RUNTIME_CFG_TS) < float(ttl_seconds):
            return _RUNTIME_CFG_DATA
    fresh = load_system_config(max_retries=max_retries)
    with _RUNTIME_CFG_LOCK:
        _RUNTIME_CFG_DATA = fresh
        _RUNTIME_CFG_TS = time.monotonic()
    return fresh


def save_system_config(config_data: Mapping[str, Any], max_retries: int = 5) -> bool:
    if not isinstance(config_data, dict):
        return False
    config_data = strip_sensitive_from_config_obj(dict(config_data))

    def _save() -> None:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM config_kv")
            for k, v in config_data.items():
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (str(k), _encode_json(v)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        _retry_on_locked(_save, max_retries=max_retries)
        invalidate_runtime_system_config_cache()
        return True
    except Exception as e:
        print(f"🚨 [bitget.config_manager] save_system_config 실패: {e}")
        return False


def update_system_config(updates_dict: Mapping[str, Any], max_retries: int = 5) -> bool:
    if not isinstance(updates_dict, dict) or not updates_dict:
        return True
    merged = load_system_config(max_retries=max_retries)
    merged.update(strip_sensitive_from_config_obj(dict(updates_dict)))
    return save_system_config(merged, max_retries=max_retries)


def load_config(max_retries: int = 5) -> dict[str, Any]:
    return load_system_config(max_retries=max_retries)


def save_config(config_data: Mapping[str, Any], max_retries: int = 5) -> bool:
    return save_system_config(config_data, max_retries=max_retries)


def bootstrap_from_json_if_empty(*, max_retries: int = 5) -> bool:
    """SQLite가 비어 있고 JSON이 있으면 1회 import."""

    def _count_rows() -> int:
        conn = _connect()
        try:
            return _sqlite_row_count(conn)
        finally:
            conn.close()

    try:
        if _retry_on_locked(_count_rows, max_retries=max_retries) > 0:
            return False
    except (OSError, sqlite3.Error):
        pass
    legacy = _load_legacy_json_view(max_retries=max_retries)
    if not legacy:
        return False
    ok = save_system_config(legacy, max_retries=max_retries)
    if ok:
        print(f"📦 [bitget.config_manager] JSON → SQLite bootstrap: {CONFIG_PATH}")
    return ok
>>>>>>> a6f17ca59385c6492c35a2f0368a732550fef092
