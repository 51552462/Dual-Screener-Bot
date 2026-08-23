"""Isolated paths for UNIVERSE-BT — never paper / market_data write targets."""
from __future__ import annotations

import os

from bitget.infra.data_paths import bitget_data_dir


def universe_bt_db_path() -> str:
    """Results SSOT — physical file separate from market_data / forward paper."""
    override = (os.environ.get("BITGET_UNIVERSE_BT_DB") or "").strip()
    if override:
        p = os.path.abspath(os.path.expanduser(override))
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        return p
    return os.path.join(bitget_data_dir(), "bitget_universe_bt.sqlite")


def universe_bt_scratch_forward_path() -> str:
    """Ephemeral forward schema for dry try_add — never the live paper DB."""
    override = (os.environ.get("BITGET_UNIVERSE_BT_SCRATCH_FORWARD") or "").strip()
    if override:
        p = os.path.abspath(os.path.expanduser(override))
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        return p
    return os.path.join(bitget_data_dir(), "bitget_universe_bt_scratch_forward.sqlite")
