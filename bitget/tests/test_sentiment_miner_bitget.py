"""bitget.sentiment_miner — daily sentiment Clock SSOT."""
from __future__ import annotations

import sqlite3
from unittest import mock


def test_sentiment_miner_module_uses_clock_ssot():
    import inspect

    from bitget import sentiment_miner as sm

    src = inspect.getsource(sm)
    assert "datetime.utcnow()" not in src
    assert "datetime.now()" not in src
    assert "from datetime import" not in src
    assert "utc_date_key" in src
    assert "print(" not in src
    assert "log_exception" in src


def test_run_sentiment_mining_stamps_utc_date(tmp_path, monkeypatch):
    from bitget import sentiment_miner as sm

    db_path = tmp_path / "news.sqlite"
    monkeypatch.setattr(sm, "NEWS_DB_PATH", str(db_path))
    with mock.patch("bitget.sentiment_miner.utc_date_key", return_value="2026-07-11"), mock.patch(
        "bitget.sentiment_miner.fetch_crypto_news_titles", return_value=["Bitcoin rally continues today"]
    ), mock.patch("bitget.sentiment_miner.fetch_fear_greed", return_value=(72.0, "Greed")):
        sm.run_sentiment_mining()
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT date FROM daily_sentiment").fetchone()
    conn.close()
    assert row[0] == "2026-07-11"
