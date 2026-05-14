"""
시스템 설정: SQLite KV + 낙관적 동시성 제어(OCC).

- DB: `system_config.sqlite` (market_data.sqlite와 분리)
- 연결: timeout=30, PRAGMA journal_mode=WAL
- 테이블: config_kv (key PRIMARY KEY, value_json, version)

고수준 API: get_config_value / set_config_value / update_config_value(OCC)
브릿지 API: load_config / save_config (전체 dict 스냅샷)

하위 호환: system_config_atomic.py 가 기대하는 CONFIG_PATH·샤드 경로·
load_system_config / save_system_config / update_system_config 유지.
DB가 비어 있으면 기존 JSON(legacy + 샤드 파일)을 읽기 전용으로 병합해 반환한다.

비밀(API 키·토큰·비밀번호 등)은 .env 만 사용한다. SQLite/JSON 에 실수로 들어간
민감 키 이름은 저장·로드 시 `strip_sensitive_from_config_obj` 로 제거한다.
"""
from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import time
from typing import Any, Callable, Mapping, Optional

import low_ram_sqlite_pragmas
import sqlite_schema_guard
from factory_data_paths import factory_data_dir

# 비밀·토큰은 .env 만 사용. JSON/SQLite 설정에 실수로 들어온 키는 저장·로드 시 제거한다.
_SENSITIVE_KEY_RE = re.compile(
    r"(TOKEN|SECRET|PASSPHRASE|PASSWORD|PRIVATE[_-]?KEY|API[_-]?KEY|CREDENTIAL|AUTHORIZATION|WEBHOOK)",
    re.I,
)


def _is_sensitive_config_key(key: str) -> bool:
    return bool(_SENSITIVE_KEY_RE.search(str(key)))


def strip_sensitive_from_config_obj(obj: Any) -> Any:
    """dict/list 재귀: 민감해 보이는 키는 제거. 스칼라는 그대로."""
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


# ---------------------------------------------------------------------------
# 경로 (기존 system_config.json과 동일 디렉터리 = factory_data_dir())
# ---------------------------------------------------------------------------
CONFIG_DIR = factory_data_dir()
CONFIG_PATH = os.path.join(CONFIG_DIR, "system_config.json")
CONFIG_DB_PATH = os.path.join(CONFIG_DIR, "system_config.sqlite")
CONFIG_SNAPSHOTS_DIR = os.path.join(CONFIG_DIR, "config_snapshots")
_MAX_CONFIG_SNAPSHOT_FILES = 365

PATH_TRADE = os.path.join(CONFIG_DIR, "config_trade.json")
PATH_MACRO = os.path.join(CONFIG_DIR, "config_macro.json")
PATH_ML = os.path.join(CONFIG_DIR, "config_ml.json")
PATH_SHADOW = os.path.join(CONFIG_DIR, "config_shadow.json")

SHARD_PATHS: dict[str, str] = {
    "trade": PATH_TRADE,
    "macro": PATH_MACRO,
    "ml": PATH_ML,
    "shadow": PATH_SHADOW,
}

# 레거시 파일 락 경로(호환용). SQLite 사용 시 쓰기 경로에서는 사용하지 않음.
LOCK_PATH = os.path.join(CONFIG_DIR, ".config_kv.lock")


class ConfigConcurrencyError(RuntimeError):
    """update_config_value 가 max_retries 안에 OCC 성공하지 못했을 때."""


ModifierFunc = Callable[[Any], Any]


# ---------------------------------------------------------------------------
# DB 연결 / 스키마
# ---------------------------------------------------------------------------
def _ensure_config_dir() -> None:
    if CONFIG_DIR:
        os.makedirs(CONFIG_DIR, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_config_dir()
    conn = sqlite3.connect(CONFIG_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
    _ensure_schema(conn)
    return conn


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


# ---------------------------------------------------------------------------
# 레거시 JSON (DB 비어 있을 때 읽기 전용 병합)
# ---------------------------------------------------------------------------
def _read_json_file(path: str, max_retries: int = 5) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    for attempt in range(max_retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [config_manager] JSON 읽기 실패: {path} — {e}")
                return {}
    return {}


def _load_legacy_merged_view(max_retries: int = 5) -> dict[str, Any]:
    legacy = _read_json_file(CONFIG_PATH, max_retries=max_retries)
    trade = _read_json_file(PATH_TRADE, max_retries=max_retries)
    macro = _read_json_file(PATH_MACRO, max_retries=max_retries)
    ml = _read_json_file(PATH_ML, max_retries=max_retries)
    shadow = _read_json_file(PATH_SHADOW, max_retries=max_retries)
    out: dict[str, Any] = {}
    out.update(legacy)
    out.update(trade)
    out.update(macro)
    out.update(ml)
    out.update(shadow)
    return out


def config_persisted() -> bool:
    """legacy JSON·샤드·또는 설정 DB(config_kv에 1행 이상) 중 하나라면 True."""
    if os.path.isfile(CONFIG_PATH):
        return True
    if any(os.path.isfile(p) for p in SHARD_PATHS.values()):
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


# ---------------------------------------------------------------------------
# 단일 키 API
# ---------------------------------------------------------------------------
def _decode_json(text: str) -> Any:
    return json.loads(text)


def _encode_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _archive_daily_config_snapshot_after_save(*, max_retries: int = 5) -> None:
    """
    하루에 최초 1회만: 현재 load_system_config() 병합 뷰를
    config_snapshots/system_config_YYYYMMDD.json 으로 원자 저장.
    365개 초과 시 날짜순 가장 오래된 파일부터 삭제.
    """
    from datetime import datetime

    try:
        _ensure_config_dir()
        os.makedirs(CONFIG_SNAPSHOTS_DIR, exist_ok=True)
        ymd = datetime.now().strftime("%Y%m%d")
        dest = os.path.join(CONFIG_SNAPSHOTS_DIR, f"system_config_{ymd}.json")
        if os.path.isfile(dest):
            return
        blob = load_system_config(max_retries=max_retries)
        tmp = dest + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dict(blob), f, indent=2, ensure_ascii=False, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, dest)
        print(f"📦 [config_snapshots] 일별 블랙박스 저장: {dest}")
        _prune_old_config_snapshots()
    except Exception as e:
        print(f"⚠️ [config_snapshots] 일별 스냅샷 생략: {e}")


def _prune_old_config_snapshots(max_keep: int = _MAX_CONFIG_SNAPSHOT_FILES) -> None:
    if not os.path.isdir(CONFIG_SNAPSHOTS_DIR):
        return
    dated: list[tuple[str, str]] = []
    for name in os.listdir(CONFIG_SNAPSHOTS_DIR):
        if not name.startswith("system_config_") or not name.endswith(".json"):
            continue
        tag = name[len("system_config_") : -len(".json")]
        if len(tag) != 8 or not tag.isdigit():
            continue
        path = os.path.join(CONFIG_SNAPSHOTS_DIR, name)
        dated.append((tag, path))
    dated.sort(key=lambda x: x[0])
    while len(dated) > max_keep:
        _, oldest = dated.pop(0)
        try:
            os.remove(oldest)
            print(f"🗑️ [config_snapshots] 보관 한도({max_keep}일) 초과로 삭제: {oldest}")
        except OSError as e:
            print(f"⚠️ [config_snapshots] 삭제 실패: {oldest} — {e}")


def find_latest_config_snapshot_on_or_before(end_iso: str) -> Optional[str]:
    """
    end_iso: 'YYYY-MM-DD' (레짐 종료일 등).
    해당일 이하 날짜 중 가장 최근의 system_config_YYYYMMDD.json 절대 경로, 없으면 None.
    """
    from datetime import datetime

    if not os.path.isdir(CONFIG_SNAPSHOTS_DIR):
        return None
    try:
        end_d = datetime.strptime(str(end_iso)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    best_d = None
    best_path: Optional[str] = None
    for name in os.listdir(CONFIG_SNAPSHOTS_DIR):
        if not name.startswith("system_config_") or not name.endswith(".json"):
            continue
        tag = name[len("system_config_") : -len(".json")]
        if len(tag) != 8 or not tag.isdigit():
            continue
        try:
            d = datetime.strptime(tag, "%Y%m%d").date()
        except ValueError:
            continue
        if d <= end_d and (best_d is None or d > best_d):
            best_d = d
            best_path = os.path.join(CONFIG_SNAPSHOTS_DIR, name)
    return best_path


def get_config_value(key: str, default_value: Any = None) -> Any:
    """
    key 에 해당하는 값을 JSON 디코딩해 반환한다.
    OCC용 version 은 내부적으로만 사용(update_config_value).
    """
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
            _ = row["version"]  # 조회만 (디버깅·추후 확장 여지)
            return _decode_json(str(row["value_json"]))
        finally:
            conn.close()

    try:
        return _retry_on_locked(_read)
    except (json.JSONDecodeError, OSError, sqlite3.Error) as e:
        print(f"⚠️ [config_manager] get_config_value({key!r}) 실패: {e}")
        return default_value


def set_config_value(key: str, value: Any) -> None:
    """단순 덮어쓰기. 존재하면 version + 1, 없으면 version = 1 로 INSERT."""
    if not key:
        raise ValueError("config key must be non-empty")
    if _is_sensitive_config_key(key):
        raise ValueError(
            f"config key {key!r} looks like a secret; use .env (telegram_env / BITGET_* ) instead"
        )

    payload = _encode_json(strip_sensitive_from_config_obj(value) if isinstance(value, (dict, list)) else value)

    def _write() -> None:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "SELECT version FROM config_kv WHERE key = ?", (key,)
            )
            row = cur.fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (key, payload),
                )
            else:
                conn.execute(
                    """
                    UPDATE config_kv
                    SET value_json = ?, version = version + 1
                    WHERE key = ?
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


def update_config_value(
    key: str,
    modifier_func: ModifierFunc,
    *,
    max_retries: int = 10,
) -> Any:
    """
    낙관적 락: (value, version) 읽기 → modifier_func 로 새 값 계산
    → UPDATE ... WHERE key=? AND version=? 가 1행이면 성공, 아니면 재시도.
    키가 없으면 modifier_func(None) 결과로 INSERT(version=1).
    """
    if not key:
        raise ValueError("config key must be non-empty")

    for attempt in range(max_retries):
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "SELECT value_json, version FROM config_kv WHERE key = ?", (key,)
            )
            row = cur.fetchone()

            if row is None:
                new_val = modifier_func(None)
                if isinstance(new_val, (dict, list)):
                    new_val = strip_sensitive_from_config_obj(new_val)
                new_json = _encode_json(new_val)
                try:
                    conn.execute(
                        "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                        (key, new_json),
                    )
                    conn.commit()
                    return new_val
                except sqlite3.IntegrityError:
                    conn.rollback()
                    # 동시에 다른 스레드가 INSERT — 재시도
                    continue
                except Exception:
                    conn.rollback()
                    raise
            else:
                cur_ver = int(row["version"])
                old_val = _decode_json(str(row["value_json"]))
                new_val = modifier_func(old_val)
                if isinstance(new_val, (dict, list)):
                    new_val = strip_sensitive_from_config_obj(new_val)
                new_json = _encode_json(new_val)
                cur2 = conn.execute(
                    """
                    UPDATE config_kv
                    SET value_json = ?, version = version + 1
                    WHERE key = ? AND version = ?
                    """,
                    (new_json, key, cur_ver),
                )
                if cur2.rowcount == 1:
                    conn.commit()
                    return new_val
                conn.rollback()
                time.sleep(0.01 + random.uniform(0, 0.04))
        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except Exception:
                pass
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(0.05 + random.uniform(0, 0.1))
                continue
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

    raise ConfigConcurrencyError(
        f"update_config_value({key!r}) failed after {max_retries} OCC retries"
    )


# ---------------------------------------------------------------------------
# 전체 dict 브릿지
# ---------------------------------------------------------------------------
def load_system_config(max_retries: int = 5) -> dict[str, Any]:
    """
    config_kv 의 모든 행을 하나의 dict 로 병합해 반환.
    테이블이 비어 있으면 레거시 JSON(병합 뷰)을 읽기 전용으로 반환.
    """

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
                    print(f"⚠️ [config_manager] 손상된 JSON 건너뜀: key={k!r}")
            return out
        finally:
            conn.close()

    try:
        blob = _retry_on_locked(_load_sqlite, max_retries=max_retries)
    except (OSError, sqlite3.Error) as e:
        print(f"⚠️ [config_manager] SQLite 로드 실패, 레거시 JSON 시도: {e}")
        blob = {}

    if blob:
        return strip_sensitive_from_config_obj(blob)
    return strip_sensitive_from_config_obj(_load_legacy_merged_view(max_retries=max_retries))


def save_system_config(config_data: Mapping[str, Any], max_retries: int = 5) -> bool:
    """
    dict 전체를 DB에 통째로 반영: 기존 행 전부 삭제 후 키마다 INSERT(version=1).
    (점진적으로 update_config_value 로 옮길 때까지의 브릿지)
    """
    if not isinstance(config_data, dict):
        return False
    config_data = strip_sensitive_from_config_obj(dict(config_data))

    def _save() -> None:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM config_kv")
            for k, v in config_data.items():
                ks = str(k)
                conn.execute(
                    "INSERT INTO config_kv (key, value_json, version) VALUES (?, ?, 1)",
                    (ks, _encode_json(v)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    try:
        _retry_on_locked(_save, max_retries=max_retries)
        _archive_daily_config_snapshot_after_save(max_retries=max_retries)
        return True
    except Exception as e:
        print(f"🚨 [config_manager] save_system_config 실패: {e}")
        return False


def update_system_config(updates_dict: Mapping[str, Any], max_retries: int = 5) -> bool:
    """
    최신 전체 뷰를 읽은 뒤 updates_dict 를 얕게 병합하고 save_system_config 로 저장.
    """
    if not isinstance(updates_dict, dict) or not updates_dict:
        return True
    merged = load_system_config(max_retries=max_retries)
    merged.update(strip_sensitive_from_config_obj(dict(updates_dict)))
    return save_system_config(merged, max_retries=max_retries)


# ---------------------------------------------------------------------------
# 기존 모듈 호환 별칭
# ---------------------------------------------------------------------------
def load_config(max_retries: int = 5) -> dict[str, Any]:
    return load_system_config(max_retries=max_retries)


def save_config(config_data: Mapping[str, Any], max_retries: int = 5) -> bool:
    return save_system_config(config_data, max_retries=max_retries)


def update_config(updates_dict: Mapping[str, Any], max_retries: int = 5) -> bool:
    return update_system_config(updates_dict, max_retries=max_retries)
