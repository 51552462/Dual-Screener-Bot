"""NS-BOOK-COUNT-01 — CLOSED_* statuses count as closed."""

from __future__ import annotations

import sqlite3

import dual_north_star_ledger as ledger


def test_forward_book_counts_closed_status_family(tmp_path, monkeypatch):
    db = tmp_path / "market_data.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE forward_trades (market TEXT, status TEXT)")
        conn.executemany(
            "INSERT INTO forward_trades (market, status) VALUES (?, ?)",
            [
                ("KR", "CLOSED_WIN"),
                ("KR", "CLOSED_LOSS"),
                ("KR", "CLOSED_ZOMBIE"),
                ("US", "CLOSED_AUTO"),
                ("US", "CLOSED_LOSS"),
                ("KR", "OPEN"),
                ("US", "ACTIVE"),
            ],
        )

    monkeypatch.setattr("factory_data_paths.market_data_db_path", lambda: str(db))
    book = ledger._forward_book_counts_a()

    assert book["error"] is None
    assert book["closed_total"] == 5
    assert book["closed_by_market"] == {"KR": 3, "US": 2}
    assert book["open_total"] == 2
    assert book["open_by_market"] == {"KR": 1, "US": 1}
    assert book["ok"] is True
