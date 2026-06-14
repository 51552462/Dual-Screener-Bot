"""
Bitget config hub — thin facade over `bitget.infra.config_manager` (SQLite SSOT).

JSON 직접 read/write 제거. bootstrap·legacy import는 config_manager가 담당.
위성·스캐너 모듈은 이 모듈만 import (load_config / save_config).
"""
from __future__ import annotations

from bitget.infra import config_manager


def load_config():
    return config_manager.load_system_config() or {}


def load_system_config():
    """`signal_engines` 등 호환 alias."""
    return load_config()


def save_config_atomic(cfg):
    return config_manager.save_system_config(cfg or {})


save_config = save_config_atomic
