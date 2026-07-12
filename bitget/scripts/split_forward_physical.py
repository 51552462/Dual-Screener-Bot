"""Extract bitget/forward/_core.py into physical submodules (Phase 3)."""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path

from bitget.infra.logging_setup import get_logger

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "forward" / "_core.py"
logger = get_logger("bitget.scripts.split_forward_physical")

# Functions to skip (duplicates / moved to shared already)
SKIP = {
    "_telegram_plain_from_html",
    "send_telegram_msg",
    "load_system_config",
    "save_system_config",
    "_ensure_col",
}

# First send_comprehensive_daily_report is legacy; keep V104.1 only
SKIP_LEGACY_REPORT = True

MODULE_MAP: dict[str, str] = {
    "_cached_funding_snapshot": "shared",
    "init_forward_db": "shared",
    "_deathmatch_min_n_cfg": "shared",
    "_fmt_deathmatch_ret": "shared",
    "_deathmatch_ab_verdict": "shared",
    "_extract_practitioner_key": "execution_bridge",
    "log_real_execution": "execution_bridge",
    "sync_real_leaderboard_with_virtual": "execution_bridge",
    "build_practitioner_reality_leaderboard": "execution_bridge",
    "_tf_weight": "gates",
    "_extract_core_group": "gates",
    "_thompson_ns_prefix": "gates",
    "_apply_thompson_kelly_multiplier": "gates",
    "_table_name": "gates",
    "_load_bench_close": "gates",
    "_calc_market_breadth": "gates",
    "_cosine_similarity": "gates",
    "_extract_4d_dna_from_facts": "gates",
    "_is_blocked_by_anti_patterns": "gates",
    "_load_hist": "gates",
    "_calc_atr14": "gates",
    "evaluate_evolved_alpha_formula": "gates",
    "compute_evolved_alpha_bonus_score": "gates",
    "_facts_cos_scalar_01": "gates",
    "try_add_virtual_position": "ledger",
    "_get_latest_bar": "ledger",
    "_floating_pnl_usdt_open_row": "ledger",
    "_aggregate_global_open_loss_usdt": "ledger",
    "_finalize_global_circuit_breaker_track": "ledger",
    "_days_since_entry_date": "ledger",
    "_force_close_zombie_delist_or_halt": "ledger",
    "track_daily_positions": "ledger",
    "_pf": "mutant",
    "_calculate_metrics": "mutant",
    "_coin_asset_group": "mutant",
    "_gaussian_gene_mutate": "mutant",
    "_merge_incubator_templates": "mutant",
    "generate_mutant_strategies": "mutant",
    "_auto_tune_brain_from_closed_df": "mutant",
    "send_group_practitioner_reports": "reports",
    "run_deep_dive_analysis": "reports",
    "send_comprehensive_daily_report": "reports",
}

HEADERS: dict[str, str] = {
    "shared": '''"""Shared DB paths, config, Telegram, schema init for Bitget forward ledger."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
from bitget.funding_fetcher import fetch_funding_snapshot
from bitget.infra.data_paths import market_data_db_path, system_config_json_path

DB_PATH = market_data_db_path()
CONFIG_PATH = system_config_json_path()
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()

DEFAULT_MAX_OPEN_POSITIONS = 20
_FUNDING_SNAP_CACHE: dict = {}

''',
    "gates": '''"""Entry gates, DNA similarity, alpha scoring."""
from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from bitget.forward.shared import DB_PATH, load_system_config

''',
    "execution_bridge": '''"""Real execution logging and practitioner leaderboard."""
from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd

from bitget.forward.shared import DB_PATH, init_forward_db, load_system_config, save_system_config

''',
    "ledger": '''"""Virtual position entry and exit engine."""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from bitget.forward.gates import (
    _apply_thompson_kelly_multiplier,
    _calc_atr14,
    _calc_market_breadth,
    _extract_4d_dna_from_facts,
    _facts_cos_scalar_01,
    _is_blocked_by_anti_patterns,
    _load_bench_close,
    _load_hist,
    _table_name,
    _tf_weight,
    compute_evolved_alpha_bonus_score,
    evaluate_evolved_alpha_formula,
)
from bitget.forward.shared import (
    DB_PATH,
    DEFAULT_MAX_OPEN_POSITIONS,
    _cached_funding_snapshot,
    load_system_config,
    save_system_config,
    send_telegram_msg,
)
from meta_governor_consumer import (
    apply_meta_kelly_merge,
    effective_max_position_pct,
    load_meta_state_resolved,
)

_DEFAULT_BITGET_MAX_OPEN_POSITIONS = DEFAULT_MAX_OPEN_POSITIONS

''',
    "mutant": '''"""Incubator mutant strategy generation and auto-tune brain."""
from __future__ import annotations

import random

import numpy as np
import pandas as pd

from bitget.forward.shared import load_system_config, save_system_config

''',
    "reports": '''"""Daily reports and deep dive."""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from bitget.forward.execution_bridge import (
    build_practitioner_reality_leaderboard,
    sync_real_leaderboard_with_virtual,
)
from bitget.forward.gates import _extract_core_group
from bitget.forward.mutant import _auto_tune_brain_from_closed_df, _coin_asset_group, _pf
from bitget.forward.shared import DB_PATH, init_forward_db, load_system_config, save_system_config, send_telegram_msg
from meta_governor_consumer import load_meta_state_resolved
from reports.forward_report_scalar import (
    col_series,
    prepare_forward_trades_df,
    row_scalar,
    scalar_float,
)
from reports.report_state_binder import build_macro_treasury_block, format_macro_treasury_section_html

''',
}


def _function_blocks(source: str) -> list[tuple[str, str, int]]:
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)
    blocks: list[tuple[str, str, int]] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            start = node.lineno - 1
            end = node.end_lineno
            name = node.name
            blocks.append((name, "".join(lines[start:end]), start))
    return blocks


def main() -> None:
    source = CORE.read_text(encoding="utf-8")
    blocks = _function_blocks(source)
    by_module: dict[str, list[str]] = {k: [] for k in HEADERS}
    seen_report = False

    for name, body, _lineno in blocks:
        if name in SKIP:
            continue
        if name == "send_comprehensive_daily_report":
            if seen_report and SKIP_LEGACY_REPORT:
                continue
            seen_report = True
        mod = MODULE_MAP.get(name)
        if mod is None:
            raise SystemExit(f"unmapped function: {name}")
        by_module[mod].append(body.rstrip() + "\n\n")

    for mod, chunks in by_module.items():
        if mod == "shared":
            # Preserve existing shared helpers at top, append extracted funcs
            existing = (ROOT / "forward" / "shared.py").read_text(encoding="utf-8")
            # Strip circular init_forward_db stub
            existing = existing.replace(
                "\n\ndef init_forward_db():\n"
                "    from bitget.forward._core import init_forward_db as _init_forward_db\n\n"
                "    return _init_forward_db()\n",
                "\n",
            )
            # Remove duplicate DEFAULT if we're rebuilding
            out = existing.rstrip() + "\n\n" + "".join(chunks)
        else:
            out = HEADERS[mod] + "".join(chunks)
        path = ROOT / "forward" / f"{mod}.py"
        path.write_text(out, encoding="utf-8")
        logger.info("wrote %s (%s lines)", path, len(out.splitlines()))

    facade = '''"""Backward-compat aggregate — prefer `bitget.forward.*` submodules."""
from bitget.forward.execution_bridge import (
    build_practitioner_reality_leaderboard,
    log_real_execution,
    sync_real_leaderboard_with_virtual,
)
from bitget.forward.gates import (
    compute_evolved_alpha_bonus_score,
    evaluate_evolved_alpha_formula,
)
from bitget.forward.ledger import track_daily_positions, try_add_virtual_position
from bitget.forward.mutant import generate_mutant_strategies
from bitget.forward.reports import (
    run_deep_dive_analysis,
    send_comprehensive_daily_report,
    send_group_practitioner_reports,
)
from bitget.forward.shared import init_forward_db, send_telegram_msg

__all__ = [
    "init_forward_db",
    "send_telegram_msg",
    "try_add_virtual_position",
    "track_daily_positions",
    "log_real_execution",
    "sync_real_leaderboard_with_virtual",
    "build_practitioner_reality_leaderboard",
    "generate_mutant_strategies",
    "send_group_practitioner_reports",
    "send_comprehensive_daily_report",
    "run_deep_dive_analysis",
    "compute_evolved_alpha_bonus_score",
    "evaluate_evolved_alpha_formula",
]
'''
    CORE.write_text(facade, encoding="utf-8")
    logger.info("wrote facade %s", CORE)


if __name__ == "__main__":
    main()
