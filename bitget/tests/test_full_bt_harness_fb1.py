"""FULL-BT-1 smoke — isolated DB grows; paper forward_trades unchanged."""
from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest import mock


def _count_trades(path: str) -> int:
    if not os.path.isfile(path):
        return 0
    conn = sqlite3.connect(path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_forward_trades'"
        ).fetchone()
        if not row:
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM bitget_forward_trades").fetchone()[0])
    finally:
        conn.close()


def test_full_bt_smoke_isolated_grows_paper_invariant(tmp_path):
    from bitget.full_bt.harness import count_forward_trades, report_tf_and_funding, run_replay
    from bitget.forward.shared import _init_forward_db_schema
    from bitget.infra.shared_db_connector import get_connection

    paper = str(tmp_path / "paper.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    # seed paper with 2 rows (must stay 2)
    conn = get_connection(paper)
    try:
        _init_forward_db_schema(conn)
        conn.execute(
            "INSERT INTO bitget_forward_trades (symbol, market_type, status, entry_date) "
            "VALUES ('AAA_USDT','spot','OPEN','2026-01-01')"
        )
        conn.execute(
            "INSERT INTO bitget_forward_trades (symbol, market_type, status, entry_date) "
            "VALUES ('BBB_USDT','futures','CLOSED_WIN','2026-01-02')"
        )
        conn.commit()
    finally:
        conn.close()
    assert _count_trades(paper) == 2

    before_full = count_forward_trades(full)
    # Point live paper path away from real machine paper
    with mock.patch("bitget.forward.shared.DB_PATH", paper), mock.patch(
        "bitget.forward.ledger.DB_PATH", paper
    ):
        paper_before = _count_trades(paper)
        run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            date(2026, 1, 1),
            date(2026, 1, 2),
            full,
        )
        paper_after = _count_trades(paper)

    after_full = count_forward_trades(full)
    assert paper_after == paper_before == 2
    assert after_full > before_full

    rep = report_tf_and_funding()
    assert "재사용 TF:" in rep["tf"]
    assert "funding:" in rep["funding"]
