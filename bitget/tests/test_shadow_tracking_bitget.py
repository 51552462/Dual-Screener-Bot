"""bitget shadow ledger — blocked/virtual trade Clock SSOT."""
from __future__ import annotations

import sqlite3
from unittest import mock


def test_shadow_tracking_module_uses_clock_ssot():
    import inspect

    from bitget import shadow_tracking as st

    src = inspect.getsource(st)
    assert "datetime.now()" not in src
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src


def test_record_blocked_trade_stamps_utc(tmp_path, monkeypatch):
    from bitget import shadow_tracking as st

    db_path = tmp_path / "market.sqlite"
    monkeypatch.setattr(st, "DB_PATH", str(db_path))
    with mock.patch("bitget.shadow_tracking.utc_datetime_str", return_value="2026-07-11 04:15:00"), mock.patch(
        "bitget.infra.memory_retention.maybe_run_bitget_retention_after_write"
    ):
        ok = st.record_blocked_trade(
            "SOL_USDT",
            "GATE_TOXIC",
            100.0,
            market_type="spot",
            name="SOL",
        )
    assert ok is True
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT blocked_at FROM bitget_blocked_trade_history").fetchone()
    conn.close()
    assert row[0] == "2026-07-11 04:15:00"


def test_shadow_performance_tracker_module_uses_clock_ssot():
    import inspect

    from bitget import shadow_performance_tracker as spt

    src = inspect.getsource(spt)
    assert "datetime.utcnow()" not in src
    assert "from datetime import" not in src
    assert "utc_datetime_str" in src
    assert "shadow_blocked_history_sql" in src


def test_shadow_performance_empty_payload_utc(tmp_path, monkeypatch):
    from bitget import shadow_performance_tracker as spt

    db_path = tmp_path / "market.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE bitget_blocked_trade_history (
            id INTEGER PRIMARY KEY, market_type TEXT, symbol TEXT, reason TEXT,
            position_side TEXT, timeframe TEXT, entry_price REAL, blocked_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(spt, "DB_PATH", str(db_path))
    with mock.patch("bitget.shadow_performance_tracker.load_config", return_value={}), mock.patch(
        "bitget.shadow_performance_tracker.save_config", return_value=True
    ), mock.patch(
        "bitget.shadow_performance_tracker.utc_datetime_str", return_value="2026-07-11 04:20:00"
    ):
        out = spt.run_shadow_performance_evaluation()
    assert out["updated_at"] == "2026-07-11 04:20:00"
    assert out["notes"] == "no blocked rows"
