"""UNIVERSE-BT-U1 read-only replay harness (C3: crash_window metric deferred).

L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지

C3 policy (director 2026-08-23):
  - Compute hit / gate_pass / virtual_entry / side asymmetry inputs.
  - exit_trigger always NULL (crash_window_forced_exit_rate deferred to U1.1+).
  - resolve_historical_regime returns UNKNOWN (no fake HIGH_VOL / live stamp).
"""

from bitget.analysis.universe_bt.replay import (
    replay_symbol_window,
    run_universe_bt_u1,
)
from bitget.analysis.universe_bt.regime import resolve_historical_regime
from bitget.analysis.universe_bt.store import write_bt_results
from bitget.analysis.universe_bt.u2 import run_universe_bt_u2
from bitget.analysis.universe_bt.u3_report import (
    generate_universe_bt_u3_report,
    render_u3_report_md,
)

__all__ = [
    "run_universe_bt_u1",
    "run_universe_bt_u2",
    "replay_symbol_window",
    "resolve_historical_regime",
    "write_bt_results",
    "generate_universe_bt_u3_report",
    "render_u3_report_md",
]
