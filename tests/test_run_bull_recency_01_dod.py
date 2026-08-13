"""BULL-RECENCY-01 DoD compare — regime_name key regression tests."""
from __future__ import annotations

from scripts.run_bull_recency_01_rp1 import compare_dod, _period_map


def _period(name: str, verdict: str, *, ret: float = 0.0, n: int = 100) -> dict:
    return {
        "regime_name": name,
        "verdict": verdict,
        "period_return_pct": ret,
        "total_trades": n,
        "mdd_pct_tier": 5.0,
    }


def test_period_map_uses_regime_name():
    stage1 = {"periods": [_period("BULL_03_최근상승", "FAIL")]}
    assert "BULL_03_최근상승" in _period_map(stage1)


def test_compare_dod_not_vacuous_pass():
    baseline = {
        "periods": [
            _period("BULL_03_최근상승", "FAIL"),
            _period("BULL_05_글로벌리플레이", "FAIL"),
            _period("BULL_01_유동성초강세", "PASS"),
        ]
    }
    patched = {
        "periods": [
            _period("BULL_03_최근상승", "NEAR_MISS"),
            _period("BULL_05_글로벌리플레이", "FAIL"),
            _period("BULL_01_유동성초강세", "PASS"),
        ],
        "mdd_crosscheck": {"badge": "MDD_OK"},
    }
    dod = compare_dod(baseline, patched)
    assert len(dod["bull_targets"]) == 2
    assert dod["dod_1_bull_05_near_miss_plus"] is False
    assert dod["dod_2_bull_03_verdict_unchanged"] is True
    assert dod["all_pass"] is False


def test_compare_dod_iter3_pass_shape():
    baseline = {
        "periods": [
            _period("BULL_03_최근상승", "FAIL"),
            _period("BULL_05_글로벌리플레이", "FAIL"),
            _period("SIDE_01_2023횡보", "PASS"),
        ]
    }
    patched = {
        "periods": [
            _period("BULL_03_최근상승", "NEAR_MISS"),
            _period("BULL_05_글로벌리플레이", "NEAR_MISS", ret=1.0),
            _period("SIDE_01_2023횡보", "PASS"),
        ],
        "mdd_crosscheck": {"badge": "MDD_OK"},
    }
    dod = compare_dod(baseline, patched)
    assert dod["dod_1_bull_05_near_miss_plus"] is True
    assert dod["dod_2_bull_03_verdict_unchanged"] is True
    assert dod["dod_3_other_verdict_unchanged"] is True
