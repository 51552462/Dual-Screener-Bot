"""bitget.forward.gates — OHLCV LIMIT SSOT + entry gate helpers."""
from __future__ import annotations

from unittest import mock


def test_gates_module_uses_ohlcv_limit_ssot():
    import inspect

    from bitget.forward import gates as g

    src = inspect.getsource(g)
    assert "memory_bounds.ohlcv_limit_sql" in src
    assert "OHLCV_SIGNAL_BAR_LIMIT" in src
    assert "GATES_BREADTH_BENCH_BAR_LIMIT" in src
    assert "ORDER BY Date DESC LIMIT {int(limit)}" not in src
    assert "print(" not in src
    assert "log_exception" in src


def test_load_hist_uses_bounded_ohlcv_sql():
    from bitget.forward import gates as g

    captured: dict = {}

    def _fake_read_sql(sql, conn, params=()):
        captured["sql"] = sql
        import pandas as pd

        return pd.DataFrame()

    with mock.patch("bitget.forward.gates.pd.read_sql", side_effect=_fake_read_sql):
        g._load_hist(mock.Mock(), "spot", "BTC_USDT", "1D")

    assert "LIMIT 300" in captured["sql"]
    assert "Open" in captured["sql"]


def test_thompson_ns_prefix_supernova():
    from bitget.forward.gates import _thompson_ns_prefix

    assert _thompson_ns_prefix("4H", "[SUPERNOVA] test") == "4H_SUPERNOVA_MASTER"


def test_compute_evolved_alpha_bonus_score_zero_when_below_threshold():
    from bitget.forward.gates import compute_evolved_alpha_bonus_score
    import pandas as pd

    df = pd.DataFrame(
        {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [1.0]}
    )
    cfg = {"BITGET_EVOLVED_ALPHA_FACTORS": {"A1": "C"}, "BITGET_EVOLVED_ALPHA_THRESHOLD": 999.0}
    assert compute_evolved_alpha_bonus_score(cfg, df) == 0.0
