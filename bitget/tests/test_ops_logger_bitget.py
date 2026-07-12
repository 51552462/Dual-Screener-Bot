"""Bitget ops_logger — append/query smoke tests."""
from __future__ import annotations

import sqlite3

from bitget.infra import ops_logger


def test_insert_and_fetch_recent_rows(tmp_path, monkeypatch):
    db = tmp_path / "ops.sqlite"
    monkeypatch.setattr(ops_logger, "OPS_EVENTS_DB_PATH", str(db))
    monkeypatch.setattr(ops_logger, "OPS_HEALTH_DB_PATH", str(db))
    monkeypatch.setattr(ops_logger, "_BOT_DIR", str(tmp_path))

    ok = ops_logger.insert_ops_event(
        component="test",
        severity="INFO",
        event="gauge.snapshot",
        payload={"n": 1},
    )
    assert ok is True

    rows = ops_logger.fetch_recent_rows(hours=1.0, limit=10)
    assert len(rows) == 1
    assert rows[0]["component"] == "test"
    assert rows[0]["event"] == "gauge.snapshot"
    assert rows[0]["payload"]["n"] == 1
    assert "T" in rows[0]["ts_utc"] or "+" in rows[0]["ts_utc"]

    conn = sqlite3.connect(str(db))
    n = conn.execute("SELECT COUNT(*) FROM ops_events").fetchone()[0]
    conn.close()
    assert int(n) == 1
