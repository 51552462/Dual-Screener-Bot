import json
import os
from datetime import datetime, timezone


from bitget.infra.data_paths import (
    bitget_pkg_dir,
    system_config_json_path,
)

SYSTEM_CONFIG_PATH = system_config_json_path()
LEGACY_CONFIG_PATH = os.path.join(bitget_pkg_dir(), "bitget_config.json")
LEGACY_DEPRECATED = os.path.join(bitget_pkg_dir(), "bitget_config.json.deprecated")


def _read_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _deprecate_legacy_file():
    if not os.path.exists(LEGACY_CONFIG_PATH):
        return
    try:
        os.replace(LEGACY_CONFIG_PATH, LEGACY_DEPRECATED)
    except Exception:
        try:
            os.remove(LEGACY_CONFIG_PATH)
        except Exception:
            pass


def load_config():
    cfg = _read_json(SYSTEM_CONFIG_PATH)
    if cfg:
        return cfg
    legacy = _read_json(LEGACY_CONFIG_PATH)
    if legacy:
        save_config_atomic({**legacy, "IMPORTED_FROM_BITGET_CONFIG_JSON_DEPRECATING": True})
        _deprecate_legacy_file()
        return legacy
    return cfg


def save_config_atomic(cfg):
    os.makedirs(os.path.dirname(SYSTEM_CONFIG_PATH) or ".", exist_ok=True)
    payload = dict(cfg or {})
    payload["UPDATED_AT_UTC"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    temp_path = f"{SYSTEM_CONFIG_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, SYSTEM_CONFIG_PATH)
