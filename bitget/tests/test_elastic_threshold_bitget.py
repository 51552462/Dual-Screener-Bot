"""Elastic threshold — bounded vol proxy reads + clock SSOT audit."""
from __future__ import annotations

import inspect
import sqlite3

from bitget.evolution import elastic_threshold_bg as et
from bitget.infra.bounded_reads import elastic_vol_closed_rets_sql, forward_pri_open_metrics_sql
from bitget.infra.memory_policy import ELASTIC_VOL_CLOSED_LIMIT, ELASTIC_VOL_OPEN_LIMIT, FORWARD_OPEN_MAX_SAFETY


def test_elastic_threshold_module_uses_bounded_reads_ssot():
    src = inspect.getsource(et)
    assert "forward_pri_open_metrics_sql" in src
    assert "elastic_vol_closed_rets_sql" in src
    assert "forward_open_count_sql" in src
    assert "SELECT entry_price, max_high" not in src
    assert "SELECT final_ret FROM bitget_forward_trades" not in src


def test_forward_pri_open_metrics_sql_bounded_default():
    sql, params = forward_pri_open_metrics_sql(market_type="spot")
    assert "LIMIT ?" in sql
    assert "ORDER BY id DESC" in sql
    assert params == ("spot", FORWARD_OPEN_MAX_SAFETY)

    sql2, params2 = forward_pri_open_metrics_sql(market_type="futures", limit=100)
    assert params2 == ("futures", 100)


def test_elastic_vol_closed_rets_sql_bounded():
    sql, params = elastic_vol_closed_rets_sql(
        market_type="spot",
        since_date="2026-07-01",
        limit=120,
    )
    assert "SELECT *" not in sql
    assert "final_ret" in sql
    assert "LIMIT ?" in sql
    assert params == ("spot", "2026-07-01", 120)

    _, params_def = elastic_vol_closed_rets_sql(
        market_type="spot",
        since_date="2026-07-01T12:00:00+00:00",
    )
    assert params_def == ("spot", "2026-07-01", ELASTIC_VOL_CLOSED_LIMIT)


def test_internal_ledger_volatility_proxy_respects_open_cap(tmp_path, monkeypatch):
    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            entry_date TEXT, exit_date TEXT,
            market_type TEXT, status TEXT, sig_type TEXT,
            entry_price REAL, max_high REAL, min_low REAL,
            position_side TEXT, final_ret REAL
        )
        """
    )
    for i in range(600):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (entry_date, exit_date, market_type, status, sig_type,
             entry_price, max_high, min_low, position_side, final_ret)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-07-01",
                "2026-07-01",
                "spot",
                "OPEN",
                "STANDARD",
                100.0,
                110.0,
                95.0,
                "LONG",
                None,
            ),
        )
    for i in range(50):
        conn.execute(
            """
            INSERT INTO bitget_forward_trades
            (entry_date, exit_date, market_type, status, sig_type,
             entry_price, max_high, min_low, position_side, final_ret)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-07-05",
                "2026-07-08",
                "spot",
                "CLOSED_WIN",
                "STANDARD",
                100.0,
                110.0,
                95.0,
                "LONG",
                2.0 + (i % 3),
            ),
        )
    conn.commit()
    conn.close()
    monkeypatch.setattr(et, "DB_PATH", str(db))

    open_q, open_params = forward_pri_open_metrics_sql(
        market_type="spot",
        limit=ELASTIC_VOL_OPEN_LIMIT,
    )
    conn2 = sqlite3.connect(db)
    open_rows = conn2.execute(open_q, open_params).fetchall()
    conn2.close()
    assert len(open_rows) == ELASTIC_VOL_OPEN_LIMIT

    vol = et.internal_ledger_volatility_proxy("spot", lookback_days=20)
    assert 0.7 <= vol <= 1.5
