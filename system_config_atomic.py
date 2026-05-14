"""
하위 호환: 기존 `from system_config_atomic import load_config, save_config, update_config, CONFIG_PATH`
는 도메인 샤딩 구현체 `config_manager`로 위임된다.

새 코드에서는 `config_manager.load_system_config` / `save_system_config` / `update_system_config` 직접 사용을 권장한다.
"""
from __future__ import annotations

from config_manager import (  # noqa: F401
    CONFIG_DIR,
    CONFIG_PATH,
    LOCK_PATH,
    PATH_MACRO,
    PATH_ML,
    PATH_SHADOW,
    PATH_TRADE,
    config_persisted,
    load_config,
    load_system_config,
    save_config,
    save_system_config,
    update_config,
    update_system_config,
)

__all__ = [
    "CONFIG_DIR",
    "CONFIG_PATH",
    "LOCK_PATH",
    "PATH_TRADE",
    "PATH_MACRO",
    "PATH_ML",
    "PATH_SHADOW",
    "config_persisted",
    "load_config",
    "save_config",
    "update_config",
    "load_system_config",
    "save_system_config",
    "update_system_config",
]
