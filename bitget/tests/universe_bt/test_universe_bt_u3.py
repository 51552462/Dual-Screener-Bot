"""UNIVERSE-BT-U3 report tests — L0 banner, null denom, SPOT asymmetry, metric4 N/A."""
from __future__ import annotations

import sqlite3

from bitget.analysis.universe_bt.store import ensure_results_schema, write_bt_results
from bitget.analysis.universe_bt.u3_report import (
    L0_BANNER,
    METRIC4_NA,
    build_u3_side_by_side_report,
    generate_universe_bt_u3_report,
    render_u3_report_md,
    write_u3_report_file,
)


def _seed(db: str, run_id: str, rows: list[dict]) -> None:
    ensure_results_schema(db)
    write_bt_results(rows, db_path=db)


def test_denominator_zero_yields_null(tmp_path):
    db = str(tmp_path / "ubt.sqlite")
    _seed(
        db,
        "r0",
        [
            {
                "run_id": "r0",
                "market_type": "futures",
                "symbol": "ETH_USDT",
                "bar_ts": 1,
                "regime_label": "UNKNOWN",
                "candidate_generated": 0,
                "gate_passed": 0,
                "virtual_entry": 0,
                "side": None,
                "exit_trigger": None,
            }
        ],
    )
    rep = generate_universe_bt_u3_report("futures", "r0", db_path=db)
    assert rep["metrics"]["hit_rate"] == 0.0  # 0/1
    assert rep["metrics"]["gate_pass_rate"] is None  # 0/0 candidates
    assert rep["metrics"]["virtual_entry_rate"] is None


def test_spot_side_asymmetry_null(tmp_path):
    db = str(tmp_path / "ubt.sqlite")
    _seed(
        db,
        "r1",
        [
            {
                "run_id": "r1",
                "market_type": "spot",
                "symbol": "ETH_USDT",
                "bar_ts": 1,
                "regime_label": "UNKNOWN",
                "candidate_generated": 1,
                "gate_passed": 1,
                "virtual_entry": 1,
                "side": "LONG",
                "exit_trigger": None,
            }
        ],
    )
    rep = generate_universe_bt_u3_report("spot", "r1", db_path=db)
    assert rep["metrics"]["side_asymmetry_ratio"] is None


def test_banner_and_metric4_na_in_md(tmp_path):
    db = str(tmp_path / "ubt.sqlite")
    _seed(
        db,
        "r2",
        [
            {
                "run_id": "r2",
                "market_type": "futures",
                "symbol": "BTC_USDT",
                "bar_ts": 1,
                "regime_label": "UNKNOWN",
                "candidate_generated": 1,
                "gate_passed": 1,
                "virtual_entry": 1,
                "side": "LONG",
                "exit_trigger": None,
            },
            {
                "run_id": "r2",
                "market_type": "futures",
                "symbol": "BTC_USDT",
                "bar_ts": 2,
                "regime_label": "UNKNOWN",
                "candidate_generated": 1,
                "gate_passed": 1,
                "virtual_entry": 1,
                "side": "SHORT",
                "exit_trigger": None,
            },
        ],
    )
    dual = build_u3_side_by_side_report("r2", db_path=db)
    md = render_u3_report_md(dual)
    assert md.startswith(L0_BANNER)
    assert METRIC4_NA in md
    assert "### SPOT" in md and "### FUTURES" in md
    assert dual["markets"]["futures"]["metrics"]["crash_window_forced_exit_rate"] == METRIC4_NA
    assert dual["markets"]["futures"]["metrics"]["side_asymmetry_ratio"]["UNKNOWN"] == 1.0

    out = write_u3_report_file(dual, reports_dir=str(tmp_path / "reports"))
    assert out.endswith("u3_r2.md")
    text = open(out, encoding="utf-8").read()
    assert L0_BANNER in text
    assert METRIC4_NA in text


def test_metric4_never_numeric(tmp_path):
    db = str(tmp_path / "ubt.sqlite")
    _seed(
        db,
        "r3",
        [
            {
                "run_id": "r3",
                "market_type": "spot",
                "symbol": "X",
                "bar_ts": 1,
                "regime_label": "UNKNOWN",
                "candidate_generated": 1,
                "gate_passed": 0,
                "virtual_entry": 0,
                "side": "LONG",
                "exit_trigger": None,
            }
        ],
    )
    rep = generate_universe_bt_u3_report("spot", "r3", db_path=db)
    assert rep["metrics"]["crash_window_forced_exit_rate"] == METRIC4_NA
    assert not isinstance(rep["metrics"]["crash_window_forced_exit_rate"], float)
