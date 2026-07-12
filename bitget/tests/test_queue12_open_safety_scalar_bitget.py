"""Queue 12 — OPEN safety alert + ledger/weekly scalar SSOT."""
from __future__ import annotations

import inspect
import sqlite3
from unittest import mock


def test_open_safety_alert_constant():
    from bitget.infra.memory_policy import OPEN_SAFETY_ALERT_MIN_INTERVAL_SEC

    assert float(OPEN_SAFETY_ALERT_MIN_INTERVAL_SEC) >= 60.0


def test_warn_if_open_exceeds_safety_logs_and_throttles_alert(tmp_path):
    from bitget.infra import bounded_reads as br

    db = tmp_path / "fwd.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE bitget_forward_trades (id INTEGER PRIMARY KEY, market_type TEXT, status TEXT)"
    )
    for i in range(5):
        conn.execute(
            "INSERT INTO bitget_forward_trades (market_type, status) VALUES ('spot','OPEN')"
        )
    conn.commit()

    alerts: list[tuple] = []

    def _alert(title, body, *, prefix="CRITICAL"):
        alerts.append((title, body, prefix))
        return True

    br._OPEN_SAFETY_ALERT_MONO = 0.0
    with mock.patch(
        "bitget.governance.meta_alerts.send_meta_critical_alert", side_effect=_alert
    ), mock.patch("bitget.infra.ops_logger.record_gauge_snapshot"):
        n1 = br.warn_if_open_exceeds_safety(conn, max_open=2, alert=True)
        n2 = br.warn_if_open_exceeds_safety(conn, max_open=2, alert=True)
    conn.close()

    assert n1 == 5
    assert n2 == 5
    assert len(alerts) == 1
    assert alerts[0][2] == "OPEN_SAFETY"


def test_ledger_scalar_helpers_ssot():
    from bitget.infra.bounded_reads import (
        forward_group_closed_pnl_sum_sql,
        forward_group_open_margin_sum_sql,
        forward_open_dup_id_sql,
        forward_open_exposure_sum_sql,
        forward_weekly_market_pnl_sum_sql,
    )

    assert "LIMIT 1" in forward_open_dup_id_sql()
    assert "SUM((sim_kelly_invest * final_ret)" in forward_group_closed_pnl_sum_sql()
    assert "SUM(margin_used)" in forward_group_open_margin_sum_sql()
    q, p = forward_open_exposure_sum_sql(market_type="futures")
    assert "COALESCE(SUM(sim_kelly_invest)" in q
    assert p == ("futures",)
    wq, wp = forward_weekly_market_pnl_sum_sql(market_type="spot", since_date="2026-07-04")
    assert "INCUBATOR" in wq
    assert wp == ("spot", "2026-07-04")


def test_ledger_module_uses_scalar_ssot():
    from bitget.forward import ledger as lg

    src = inspect.getsource(lg)
    assert "forward_open_dup_id_sql" in src
    assert "forward_group_closed_pnl_sum_sql" in src
    assert "forward_group_open_margin_sum_sql" in src
    assert "forward_open_exposure_sum_sql" in src
    assert (
        "SELECT id FROM bitget_forward_trades WHERE symbol=? AND timeframe=? AND market_type=? AND position_side=? AND status='OPEN'"
        not in src
    )


def test_auto_pilot_and_pipelines_use_weekly_pnl_ssot():
    from bitget import auto_pilot as ap
    from bitget.pipelines import bitget_pipelines as bp

    assert "forward_weekly_market_pnl_sum_sql" in inspect.getsource(ap)
    assert "forward_weekly_market_pnl_sum_sql" in inspect.getsource(bp)


def test_exploration_budget_uses_logging_ssot():
    from bitget.governance import exploration_budget as eb

    src = inspect.getsource(eb)
    assert "get_logger" in src
    assert "logging.getLogger" not in src
