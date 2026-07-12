"""bitget.ai_overseer — KST audit + Tier-2 bounded reads."""
from __future__ import annotations

import sqlite3
from unittest import mock

import pandas as pd


def test_ai_overseer_module_uses_bounded_reads_and_kst_audit():
    import inspect

    from bitget import ai_overseer as ao

    src = inspect.getsource(ao)
    assert "overseer_daily_closed_sql" in src
    assert "overseer_rnd_day_count_sql" in src
    assert "_kst_today_str" in src
    assert "datetime.now(timezone.utc)" not in src
    assert "_csv_status_row_count" in src
    assert "pd.read_csv" not in src


def test_kst_today_str_uses_seoul_tz():
    from bitget import ai_overseer as ao

    tz_kr = __import__("pytz").timezone("Asia/Seoul")
    with mock.patch("bitget.ai_overseer.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2026-07-11"
        assert ao._kst_today_str() == "2026-07-11"
        mock_dt.now.assert_called_once_with(tz_kr)


def test_csv_status_row_count_caps_without_dataframe():
    from bitget import ai_overseer as ao

    lines = "h\n" + "\n".join(f"r{i}" for i in range(5))
    with mock.patch("builtins.open", mock.mock_open(read_data=lines)), mock.patch(
        "bitget.ai_overseer.OVERSEER_CSV_STATUS_ROW_CAP", 3
    ):
        n, truncated = ao._csv_status_row_count("/fake.csv")
    assert n == 3
    assert truncated is True


def test_gather_daily_system_facts_uses_count_and_bounded_closed(tmp_path):
    from bitget import ai_overseer as ao

    db = tmp_path / "m.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            entry_date TEXT,
            exit_date TEXT,
            final_ret REAL,
            position_side TEXT,
            sig_type TEXT,
            status TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO bitget_forward_trades VALUES (?,?,?,?,?,?,?)",
        [
            (1, "2026-07-11", "2026-07-11", 2.0, "LONG", "S1", "CLOSED"),
            (2, "2026-07-11", "2026-07-11", -1.0, "SHORT", "[R&D_A]", "CLOSED"),
        ],
    )
    conn.commit()
    conn.close()

    with mock.patch("bitget.ai_overseer.DB_PATH", str(db)), mock.patch(
        "bitget.ai_overseer._kst_today_str", return_value="2026-07-11"
    ), mock.patch("bitget.ai_overseer.load_config", return_value={}), mock.patch(
        "bitget.ai_overseer.load_meta_state_resolved", return_value={}
    ), mock.patch("bitget.ai_overseer.os.path.exists", return_value=False):
        facts = ao.gather_daily_system_facts()

    assert facts["date"] == "2026-07-11"
    assert facts["rnd_data_count"] == 1
    assert facts["trades"]["total_closed"] == 2
    assert facts["trades"]["wins"] == 1


def test_kst_exit_date_union_in_bounded_reads():
    from bitget.infra.bounded_reads import _kst_exit_date_union

    assert _kst_exit_date_union("2026-07-11") == ("2026-07-10", "2026-07-11")


def test_overseer_rnd_day_count_sql_is_scalar_count():
    from bitget.infra.bounded_reads import overseer_rnd_day_count_sql

    sql, params = overseer_rnd_day_count_sql(today="2026-07-11")
    assert "COUNT(*)" in sql
    assert "LIMIT" not in sql
    assert params == ("2026-07-11",)
