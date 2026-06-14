<<<<<<< HEAD
"""
Bitget 팩토리 데이터·설정 경로 단일 정의 (SSOT).

우선순위:
  1. 환경 변수 `BITGET_DB_STORAGE_PATH`
  2. `bitget_system_config.json` 키 `BITGET_DB_STORAGE_PATH`
  3. 레거시 `bitget/` 패키지 디렉터리 (기존 SQLite 위치)

주식 `factory_data_paths.py`와 `DB_STORAGE_PATH` 키를 공유하지 않는다.
"""
from __future__ import annotations

import json
import os
import time


def _bitget_pkg_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _legacy_bitget_data_dir() -> str:
    return _bitget_pkg_dir()


def _read_storage_from_json(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return ""
        v = data.get("BITGET_DB_STORAGE_PATH") or data.get("DB_STORAGE_PATH")
        return str(v).strip() if v is not None else ""
    except Exception:
        return ""


def bitget_data_dir() -> str:
    raw = (os.environ.get("BITGET_DB_STORAGE_PATH") or "").strip()
    if raw:
        p = os.path.abspath(os.path.expanduser(raw))
        os.makedirs(p, exist_ok=True)
        return p

    cfg_candidates = [
        os.path.join(_legacy_bitget_data_dir(), "bitget_system_config.json"),
    ]
    sc = (os.environ.get("BITGET_SYSTEM_CONFIG_PATH") or "").strip()
    if sc:
        cfg_candidates.insert(0, os.path.abspath(os.path.expanduser(sc)))

    for cp in cfg_candidates:
        sub = _read_storage_from_json(cp)
        if sub:
            p = os.path.abspath(os.path.expanduser(sub))
            os.makedirs(p, exist_ok=True)
            return p

    legacy = _legacy_bitget_data_dir()
    os.makedirs(legacy, exist_ok=True)
    return legacy


def bitget_install_root() -> str:
    """저장소 루트 (코드·deploy 스크립트 기준)."""
    raw = (os.environ.get("BITGET_INSTALL_ROOT") or os.environ.get("INSTALL_ROOT") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return os.path.dirname(_bitget_pkg_dir())


def bitget_pkg_dir() -> str:
    return _bitget_pkg_dir()


def market_data_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_market_data.sqlite")


def market_data_snapshot_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_market_data_snapshot.sqlite")


def _snapshot_max_stale_seconds() -> float:
    raw = (os.environ.get("BITGET_SNAPSHOT_MAX_STALE_SEC") or "").strip()
    if not raw:
        return 1800.0
    try:
        return float(raw)
    except ValueError:
        return 1800.0


def market_db_read_path() -> str:
    """
    CQRS read path: snapshot when fresh, else main DB.
    Env BITGET_SNAPSHOT_MAX_STALE_SEC (default 1800). 0 = always prefer snapshot.
    """
    main = market_data_db_path()
    snap = market_data_snapshot_db_path()
    if not os.path.isfile(snap):
        return main
    max_age = _snapshot_max_stale_seconds()
    if max_age <= 0:
        return snap
    try:
        age = time.time() - os.path.getmtime(snap)
    except OSError:
        return main
    if age > max_age:
        return main
    return snap


def report_db_read_path() -> str:
    """Reports always prefer main DB to avoid stale watermark reads."""
    force = str(os.environ.get("BITGET_REPORT_FORCE_MAIN_DB", "1")).strip().lower()
    if force in ("1", "true", "yes", "on"):
        return market_data_db_path()
    path = market_db_read_path()
    main = market_data_db_path()
    snap = market_data_snapshot_db_path()
    if path == snap and os.path.isfile(main):
        return main
    return path


def system_config_json_path() -> str:
    sc = (os.environ.get("BITGET_SYSTEM_CONFIG_PATH") or "").strip()
    if sc:
        return os.path.abspath(os.path.expanduser(sc))
    return os.path.join(bitget_data_dir(), "bitget_system_config.json")


def system_config_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_system_config.sqlite")


def ops_events_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_ops_events.sqlite")


def message_queue_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_message_queue.sqlite")


def news_data_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_news_data.sqlite")


def alt_data_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_alt_data.sqlite")


def charts_dir() -> str:
    p = os.path.join(bitget_data_dir(), "charts")
    os.makedirs(p, exist_ok=True)
    return p


def schedule_lock_state_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_schedule_lock_state.json")


def runtime_lock_path() -> str:
    return os.path.join(bitget_data_dir(), ".bitget_runtime.lock")


def logs_dir() -> str:
    raw = (os.environ.get("BITGET_LOG_DIR") or "").strip()
    if raw:
        p = os.path.abspath(os.path.expanduser(raw))
    else:
        p = os.path.join(_bitget_pkg_dir(), "logs")
    os.makedirs(p, exist_ok=True)
    return p


def meta_governor_state_path() -> str:
    env = (os.environ.get("BITGET_META_GOVERNOR_STATE_PATH") or "").strip()
    if env:
        return os.path.abspath(os.path.expanduser(env))
    return os.path.join(bitget_install_root(), "bitget_meta_governor_state.json")


def flow_csv_path() -> str:
    return os.path.join(bitget_data_dir(), "Supernova_Flow_Tracking_Master.csv")


def validation_state_dir() -> str:
    p = os.path.join(bitget_data_dir(), "validation")
    os.makedirs(p, exist_ok=True)
    return p


def dashboard_port() -> int:
    raw = (os.environ.get("BITGET_DASHBOARD_PORT") or "8511").strip()
    try:
        return max(1024, min(65535, int(raw)))
    except ValueError:
        return 8511


def heatmap_port() -> int:
    raw = (os.environ.get("BITGET_HEATMAP_PORT") or "8512").strip()
    try:
        return max(1024, min(65535, int(raw)))
    except ValueError:
        return 8512


def watchdog_state_dir() -> str:
    raw = (os.environ.get("BITGET_WATCHDOG_STATE_DIR") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return os.path.join(bitget_data_dir(), "watchdog_state")
=======
"""
Bitget 팩토리 데이터·설정 경로 단일 정의 (SSOT).

우선순위:
  1. 환경 변수 `BITGET_DB_STORAGE_PATH`
  2. `bitget_system_config.json` 키 `BITGET_DB_STORAGE_PATH`
  3. 레거시 `bitget/` 패키지 디렉터리 (기존 SQLite 위치)

주식 `factory_data_paths.py`와 `DB_STORAGE_PATH` 키를 공유하지 않는다.
"""
from __future__ import annotations

import json
import os
import time


def _bitget_pkg_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _legacy_bitget_data_dir() -> str:
    return _bitget_pkg_dir()


def _read_storage_from_json(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return ""
        v = data.get("BITGET_DB_STORAGE_PATH") or data.get("DB_STORAGE_PATH")
        return str(v).strip() if v is not None else ""
    except Exception:
        return ""


def bitget_data_dir() -> str:
    raw = (os.environ.get("BITGET_DB_STORAGE_PATH") or "").strip()
    if raw:
        p = os.path.abspath(os.path.expanduser(raw))
        os.makedirs(p, exist_ok=True)
        return p

    cfg_candidates = [
        os.path.join(_legacy_bitget_data_dir(), "bitget_system_config.json"),
    ]
    sc = (os.environ.get("BITGET_SYSTEM_CONFIG_PATH") or "").strip()
    if sc:
        cfg_candidates.insert(0, os.path.abspath(os.path.expanduser(sc)))

    for cp in cfg_candidates:
        sub = _read_storage_from_json(cp)
        if sub:
            p = os.path.abspath(os.path.expanduser(sub))
            os.makedirs(p, exist_ok=True)
            return p

    legacy = _legacy_bitget_data_dir()
    os.makedirs(legacy, exist_ok=True)
    return legacy


def bitget_install_root() -> str:
    """저장소 루트 (코드·deploy 스크립트 기준)."""
    raw = (os.environ.get("BITGET_INSTALL_ROOT") or os.environ.get("INSTALL_ROOT") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return os.path.dirname(_bitget_pkg_dir())


def bitget_pkg_dir() -> str:
    return _bitget_pkg_dir()


def market_data_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_market_data.sqlite")


def market_data_snapshot_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_market_data_snapshot.sqlite")


def _snapshot_max_stale_seconds() -> float:
    raw = (os.environ.get("BITGET_SNAPSHOT_MAX_STALE_SEC") or "").strip()
    if not raw:
        return 1800.0
    try:
        return float(raw)
    except ValueError:
        return 1800.0


def market_db_read_path() -> str:
    """
    CQRS read path: snapshot when fresh, else main DB.
    Env BITGET_SNAPSHOT_MAX_STALE_SEC (default 1800). 0 = always prefer snapshot.
    """
    main = market_data_db_path()
    snap = market_data_snapshot_db_path()
    if not os.path.isfile(snap):
        return main
    max_age = _snapshot_max_stale_seconds()
    if max_age <= 0:
        return snap
    try:
        age = time.time() - os.path.getmtime(snap)
    except OSError:
        return main
    if age > max_age:
        return main
    return snap


def report_db_read_path() -> str:
    """Reports always prefer main DB to avoid stale watermark reads."""
    force = str(os.environ.get("BITGET_REPORT_FORCE_MAIN_DB", "1")).strip().lower()
    if force in ("1", "true", "yes", "on"):
        return market_data_db_path()
    path = market_db_read_path()
    main = market_data_db_path()
    snap = market_data_snapshot_db_path()
    if path == snap and os.path.isfile(main):
        return main
    return path


def system_config_json_path() -> str:
    sc = (os.environ.get("BITGET_SYSTEM_CONFIG_PATH") or "").strip()
    if sc:
        return os.path.abspath(os.path.expanduser(sc))
    return os.path.join(bitget_data_dir(), "bitget_system_config.json")


def system_config_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_system_config.sqlite")


def ops_events_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_ops_events.sqlite")


def message_queue_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_message_queue.sqlite")


def news_data_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_news_data.sqlite")


def alt_data_db_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_alt_data.sqlite")


def charts_dir() -> str:
    p = os.path.join(bitget_data_dir(), "charts")
    os.makedirs(p, exist_ok=True)
    return p


def schedule_lock_state_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_schedule_lock_state.json")


def runtime_lock_path() -> str:
    return os.path.join(bitget_data_dir(), ".bitget_runtime.lock")


def logs_dir() -> str:
    raw = (os.environ.get("BITGET_LOG_DIR") or "").strip()
    if raw:
        p = os.path.abspath(os.path.expanduser(raw))
    else:
        p = os.path.join(_bitget_pkg_dir(), "logs")
    os.makedirs(p, exist_ok=True)
    return p


def meta_governor_state_path() -> str:
    env = (os.environ.get("BITGET_META_GOVERNOR_STATE_PATH") or "").strip()
    if env:
        return os.path.abspath(os.path.expanduser(env))
    return os.path.join(bitget_install_root(), "bitget_meta_governor_state.json")


def flow_csv_path() -> str:
    return os.path.join(bitget_data_dir(), "Supernova_Flow_Tracking_Master.csv")


def validation_state_dir() -> str:
    p = os.path.join(bitget_data_dir(), "validation")
    os.makedirs(p, exist_ok=True)
    return p


def dashboard_port() -> int:
    raw = (os.environ.get("BITGET_DASHBOARD_PORT") or "8511").strip()
    try:
        return max(1024, min(65535, int(raw)))
    except ValueError:
        return 8511


def heatmap_port() -> int:
    raw = (os.environ.get("BITGET_HEATMAP_PORT") or "8512").strip()
    try:
        return max(1024, min(65535, int(raw)))
    except ValueError:
        return 8512


def watchdog_state_dir() -> str:
    raw = (os.environ.get("BITGET_WATCHDOG_STATE_DIR") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return os.path.join(bitget_data_dir(), "watchdog_state")
>>>>>>> a6f17ca59385c6492c35a2f0368a732550fef092
