"""sqlite_schema_guard — 필수 테이블 검문·Self-healing."""
from __future__ import annotations

import os
import sqlite3
import tempfile

from sqlite_schema_guard import (
    check_market_db_schema,
    ensure_market_db_core_schema,
    missing_required_tables,
)


def test_missing_forward_trades_detected():
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "empty.sqlite")
        sqlite3.connect(path).close()
        assert missing_required_tables(path) == ["forward_trades"]


def test_heal_creates_forward_trades():
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "heal.sqlite")
        sqlite3.connect(path).close()
        from forward.shared import init_forward_db

        init_forward_db(path)
        check = check_market_db_schema(path)
        assert check["ok"]
        assert check["missing"] == []


def test_ensure_market_db_core_schema_monkeypatched_paths(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        main = os.path.join(td, "market_data.sqlite")
        snap = os.path.join(td, "market_data_snapshot.sqlite")
        sqlite3.connect(main).close()
        sqlite3.connect(snap).close()

        import market_db_paths

        monkeypatch.setattr(market_db_paths, "MARKET_DATA_DB_PATH", main)
        monkeypatch.setattr(market_db_paths, "MARKET_DATA_SNAPSHOT_PATH", snap)

        out = ensure_market_db_core_schema(heal=True, heal_snapshot=True)
        assert out["main"]["ok"]
        assert out["snapshot"]["ok"]
