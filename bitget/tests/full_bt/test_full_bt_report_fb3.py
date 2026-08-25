"""FULL-BT-3 tests — §2 quant keys · banner/Kill · SPOT/FUT split · incomplete warn."""
from __future__ import annotations

from bitget.forward.shared import _init_forward_db_schema
from bitget.full_bt.checkpoint import ensure_checkpoint_schema, save_full_bt_checkpoint
from bitget.full_bt.paths import full_bt_db_path
from bitget.full_bt.report import (
    B1_REFERENCE_BAND,
    CLUE_KEYS,
    FULL_BT_DB_PATH_MODE,
    INCOMPLETE_WARN,
    L1_BANNER,
    QUANT_KEYS,
    UNMEASURED_FOOTNOTE,
    build_full_bt_l1_side_by_side,
    generate_full_bt_l1_report,
    render_full_bt_l1_report_md,
)
from bitget.infra.clock import utc_date_str
from bitget.infra.shared_db_connector import get_connection


def _init_full(path: str) -> None:
    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _insert_trade(
    path: str,
    *,
    symbol: str,
    market_type: str,
    side: str = "LONG",
    status: str = "CLOSED_WIN",
    entry_date: str | None = None,
    exit_date: str | None = None,
    final_ret: float = 1.0,
) -> None:
    day = entry_date or utc_date_str()
    xday = exit_date or day
    conn = get_connection(path)
    try:
        conn.execute(
            """
            INSERT INTO bitget_forward_trades (
                symbol, market_type, status, entry_date, exit_date,
                final_ret, position_side, timeframe, sig_type
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                symbol,
                market_type,
                status,
                day,
                xday,
                final_ret,
                side,
                "1D",
                "FULL_BT_TEST",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_checkpoint_at(
    path: str,
    run_id: str,
    market_type: str,
    symbol: str,
    batch_idx: int,
    updated_at: str,
) -> None:
    ensure_checkpoint_schema(path)
    conn = get_connection(path)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO bitget_full_bt_checkpoint (
                run_id, market_type, shard_index, completed_symbol,
                completed_batch_idx, updated_at
            ) VALUES (?,?,?,?,?,?)
            """,
            (run_id, market_type, 0, symbol, batch_idx, updated_at),
        )
        conn.commit()
    finally:
        conn.close()


def test_quant_keys_and_banner_kill(tmp_path):
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    save_full_bt_checkpoint("run-a", "spot", "BTC_USDT", 0, db_path=full)
    _insert_trade(full, symbol="BTC_USDT", market_type="spot", final_ret=2.0)

    rep = generate_full_bt_l1_report("spot", "run-a", db_path=full)
    assert rep["banner"] == L1_BANNER
    q = rep["quantitative"]
    for k in QUANT_KEYS:
        assert k in q
    assert q["b1_reference_band"] == B1_REFERENCE_BAND
    for k in CLUE_KEYS:
        assert k in rep["clues"]
    assert rep["clues"]["gate_bottleneck_by_step"]["step11_execution_safety"] == "N/A"
    assert rep["clues"]["side_asymmetry"] is None  # SPOT U3 null

    md = render_full_bt_l1_report_md(rep)
    assert md.startswith(L1_BANNER)
    assert B1_REFERENCE_BAND in md
    # Kill: no CAGR / win-rate / annualized narrative injected
    assert "CAGR" not in md
    assert "승률" not in md
    assert "연복리" not in md


def test_spot_fut_side_by_side_no_sum(tmp_path):
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    save_full_bt_checkpoint("run-b", "spot", "BTC_USDT", 0, db_path=full)
    save_full_bt_checkpoint("run-b", "futures", "BTC_USDT", 0, db_path=full)
    _insert_trade(full, symbol="BTC_USDT", market_type="spot", final_ret=1.0)
    _insert_trade(
        full,
        symbol="BTC_USDT",
        market_type="futures",
        side="LONG",
        final_ret=3.0,
    )
    _insert_trade(
        full,
        symbol="BTC_USDT",
        market_type="futures",
        side="SHORT",
        final_ret=-1.0,
    )

    dual = build_full_bt_l1_side_by_side("run-b", db_path=full)
    spot = dual["markets"]["spot"]
    fut = dual["markets"]["futures"]
    assert spot["quantitative"]["trade_count"] == 1
    assert fut["quantitative"]["trade_count"] == 2
    # no pooled sum key
    assert "trade_count" not in dual
    assert spot["clues"]["side_asymmetry"] is None
    sa = fut["clues"]["side_asymmetry"]
    assert sa["long_entered"] == 1 and sa["short_entered"] == 1

    md = render_full_bt_l1_report_md(dual)
    assert md.startswith(L1_BANNER)
    assert "### SPOT" in md and "### FUTURES" in md
    # counts appear separately — not a single summed trade_count row at root
    assert spot["quantitative"]["trade_count"] + fut["quantitative"]["trade_count"] == 3


def test_incomplete_run_partial_warning(tmp_path):
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)

    # Only batch 0 checkpointed; expected window batches mock → 0,1,2 → incomplete
    save_full_bt_checkpoint("run-partial", "spot", "ETH_USDT", 0, db_path=full)
    _insert_trade(full, symbol="ETH_USDT", market_type="spot", final_ret=0.5)

    from unittest import mock

    with mock.patch(
        "bitget.full_bt.batch.get_full_bt_window_batches",
        return_value=[(1, 2), (2, 3), (3, 4)],
    ):
        rep = generate_full_bt_l1_report("spot", "run-partial", db_path=full)
    assert INCOMPLETE_WARN in rep["warnings"]
    assert rep["quantitative"]["trade_count"] == 1  # completed-symbol filter still applies
    md = render_full_bt_l1_report_md(rep)
    assert INCOMPLETE_WARN in md


def test_no_checkpoint_incomplete_and_zero_trades(tmp_path):
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    _insert_trade(full, symbol="BTC_USDT", market_type="spot", final_ret=9.0)
    rep = generate_full_bt_l1_report("spot", "never-run", db_path=full)
    assert INCOMPLETE_WARN in rep["warnings"]
    # no completed checkpoint → filter yields zero trades
    assert rep["quantitative"]["trade_count"] == 0
    assert rep["quantitative"]["b1_reference_band"] == B1_REFERENCE_BAND


def test_shared_db_path_mode_is_shared():
    """(A) paths.py = 공유 — HIST-1 candle 축으로 updated_at 일자 창은 report Adapter 비활성."""
    assert FULL_BT_DB_PATH_MODE == "공유"
    assert "run_id" not in full_bt_db_path().lower()
    assert full_bt_db_path().endswith("bitget_full_bt.sqlite")
    from bitget.full_bt.report import CANDLE_ENTRY_AXIS

    assert CANDLE_ENTRY_AXIS is True


def test_unmeasured_footnote_in_render(tmp_path):
    """(B) gate_bottleneck / top_rejected 옆 미측정 각주."""
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)
    save_full_bt_checkpoint("run-note", "spot", "BTC_USDT", 0, db_path=full)
    _insert_trade(full, symbol="BTC_USDT", market_type="spot", final_ret=0.0)
    md = render_full_bt_l1_report_md(
        generate_full_bt_l1_report("spot", "run-note", db_path=full)
    )
    assert UNMEASURED_FOOTNOTE in md
    assert "gate_bottleneck_by_step" in md
    assert f"top_rejected: {UNMEASURED_FOOTNOTE}" in md
