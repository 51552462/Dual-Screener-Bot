"""FULL-BT isolated paths — paper / config_kv never write targets."""
from __future__ import annotations

import os

from bitget.infra.data_paths import bitget_data_dir


def full_bt_db_path() -> str:
    """Physical FULL-BT book — under BITGET_DB_STORAGE_PATH (no new env invent)."""
    override = (os.environ.get("BITGET_FULL_BT_DB") or "").strip()
    if override:
        p = os.path.abspath(os.path.expanduser(override))
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        return p
    return os.path.join(bitget_data_dir(), "bitget_full_bt.sqlite")
