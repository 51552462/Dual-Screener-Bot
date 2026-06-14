"""One-shot: copy forward_tester.py -> forward/_core.py with shared imports."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
src = ROOT / "forward_tester.py"
dst = ROOT / "forward" / "_core.py"

text = src.read_text(encoding="utf-8")
lines = text.splitlines()
start = 0
for i, line in enumerate(lines):
    if line.startswith("def _cached_funding_snapshot"):
        start = i
        break
body = "\n".join(lines[start:])

header = '''"""Bitget forward ledger core (internal — import via bitget.forward package)."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

from bitget.funding_fetcher import fetch_funding_snapshot
from bitget.forward.shared import (
    DB_PATH,
    DEFAULT_MAX_OPEN_POSITIONS,
    _ensure_col,
    load_system_config,
    save_system_config,
    send_telegram_msg,
)
from meta_governor_consumer import (
    apply_meta_kelly_merge,
    effective_max_position_pct,
    load_meta_state_resolved,
)
from reports.forward_report_scalar import (
    col_series,
    prepare_forward_trades_df,
    row_scalar,
    scalar_float,
    series_mean,
)
from reports.report_state_binder import build_macro_treasury_block, format_macro_treasury_section_html

_DEFAULT_BITGET_MAX_OPEN_POSITIONS = DEFAULT_MAX_OPEN_POSITIONS
_FUNDING_SNAP_CACHE = {}

'''

body = body.replace(
    '"SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE \'CLOSED%\' ORDER BY id DESC LIMIT 120",',
    '"SELECT * FROM bitget_forward_trades WHERE status LIKE \'CLOSED%\' ORDER BY id DESC LIMIT 120",',
)
body = body.replace(
    "            params=(str(market_type).lower(),),\n",
    "",
)

dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(header + body, encoding="utf-8")
print(f"wrote {dst} ({len((header + body).splitlines())} lines)")
