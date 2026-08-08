"""RP-1 + C-1 regime panel — unit tests (no live fdr)."""
from __future__ import annotations

import os

import pytest

from regime_panel_rp1 import (
    apply_c1_sector_boost,
    build_stage1_report,
    compute_period_portfolio_metrics,
    decide_stage2_c1,
    judge_period_verdict,
    mdd_crosscheck_badge,
    replay_tier_overlay_on_returns,
    run_regime_panel_rp1,
    tag_fail_cause,
    trades_to_daily_returns,
)
from regime_panel_rp1_runner import (
    resolve_rp1_chunk_size,
    resolve_rp1_max_workers,
    resolve_rp1_use_parallel,
)
from time_machine_backtester import REGIME_PERIODS


class TestRp1RunnerTuning:
    def test_parallel_default_on(self, monkeypatch):
        monkeypatch.delenv("RP1_SEQUENTIAL", raising=False)
        monkeypatch.delenv("RP1_PARALLEL", raising=False)
        assert resolve_rp1_use_parallel() is True

    def test_sequential_flag(self, monkeypatch):
        monkeypatch.setenv("RP1_SEQUENTIAL", "1")
        assert resolve_rp1_use_parallel() is False

    def test_max_workers_default_two(self, monkeypatch):
        monkeypatch.delenv("RP1_MAX_WORKERS", raising=False)
        monkeypatch.delenv("MAX_WORKERS", raising=False)
        assert resolve_rp1_max_workers() == 2

    def test_chunk_size_default(self):
        assert resolve_rp1_chunk_size() == 25


class TestRegimePeriodsExpanded:
    def test_fifteen_periods_three_buckets(self):
        counts = {"BULL": 0, "SIDEWAYS": 0, "BEAR": 0}
        for meta in REGIME_PERIODS.values():
            b = meta.get("bucket")
            assert b in counts
            counts[b] += 1
            assert "backup" in meta
        assert sum(counts.values()) == 15
        assert counts == {"BULL": 5, "SIDEWAYS": 5, "BEAR": 5}

    def test_regime_periods_dates_ssot_snapshot(self):
        """스펙 대조용 — 15구간 primary 날짜 원문 (Claude MASTER 검증)."""
        expected = {
            "BULL_01_유동성초강세": ("2020-10-01", "2021-11-30", "BULL"),
            "BULL_02_US_AI랠리": ("2023-01-01", "2023-07-31", "BULL"),
            "BULL_03_최근상승": ("2024-10-01", "2025-03-31", "BULL"),
            "BULL_04_KR코스피랠리": ("2017-01-01", "2018-01-31", "BULL"),
            "BULL_05_글로벌리플레이": ("2016-06-01", "2016-11-30", "BULL"),
            "SIDE_01_2023횡보": ("2023-05-01", "2023-08-31", "SIDEWAYS"),
            "SIDE_02_2015횡보": ("2015-06-01", "2016-06-30", "SIDEWAYS"),
            "SIDE_03_2021-22혼조": ("2021-12-01", "2022-12-31", "SIDEWAYS"),
            "SIDE_04_2024여름횡보": ("2024-04-01", "2024-08-31", "SIDEWAYS"),
            "SIDE_05_2019횡보": ("2019-04-01", "2019-12-31", "SIDEWAYS"),
            "BEAR_01_서브프라임GFC": ("2008-09-01", "2009-03-31", "BEAR"),
            "BEAR_02_COVID폭락": ("2020-02-01", "2020-05-31", "BEAR"),
            "BEAR_03_글로벌금리인상": ("2022-01-01", "2022-06-30", "BEAR"),
            "BEAR_04_미중무역분쟁": ("2018-09-01", "2018-12-31", "BEAR"),
            "BEAR_05_미국신용등급강등": ("2011-08-01", "2011-10-31", "BEAR"),
        }
        assert set(REGIME_PERIODS.keys()) == set(expected.keys())
        for name, (start, end, bucket) in expected.items():
            meta = REGIME_PERIODS[name]
            assert meta["start"] == start
            assert meta["end"] == end
            assert meta["bucket"] == bucket


class TestRp1NoConfigKvWrite:
    def test_rp1_no_config_kv_write(self, tmp_path, monkeypatch):
        """Phase A tier replay — config_kv / sync_performance_budget 쓰기 금지."""
        writes: list = []

        def _trap_write(*args, **kwargs):
            writes.append((args, kwargs))
            raise AssertionError("config_kv write must not occur during RP-1")

        monkeypatch.setattr(
            "performance_budget_governor.sync_performance_budget_to_config_kv",
            _trap_write,
            raising=False,
        )
        monkeypatch.setattr(
            "performance_budget_governor.set_config_value",
            _trap_write,
            raising=False,
        )
        monkeypatch.setattr(
            "regime_panel_rp1_runner.load_rp1_brain_cached",
            lambda **kwargs: {
                "ENABLE_PERFORMANCE_BUDGET_GOVERNOR": True,
                "LIVE_CLUSTER_TEMPLATES": {"t1": {}},
            },
        )

        def mock_bt(_name, _stocks, start, end):
            return {"trades": [{"final_ret": 1.0, "date": start, "code": "005930"}] * 22}

        run_regime_panel_rp1(
            ["005930"],
            run_backtest_fn=mock_bt,
            output_dir=str(tmp_path),
            run_stage2=False,
        )
        assert writes == []


class TestTierOverlay:
    def test_lockdown_zeros_next_trade(self):
        returns = [5.0, 5.0, -15.0, 2.0]
        adj, log = replay_tier_overlay_on_returns(returns, mdd_cap_pct=10.0)
        assert len(adj) == 4
        assert any(entry["band"] == "LOCKDOWN" for entry in log) or any(a == 0.0 for a in adj)


class TestPortfolioMetricsV2:
    def test_daily_equal_weight_caps_cagr_explosion(self):
        trades = [
            {"date": "2020-10-01", "final_ret": 1.0, "code": f"{i:04d}"}
            for i in range(200)
        ]
        m = compute_period_portfolio_metrics(trades, "2020-10-01", "2020-10-31")
        assert m["metrics_method"] == "daily_equal_weight_v2"
        assert m["cagr_pct"] < 200.0
        assert m["trades_per_day"] == 200.0

    def test_same_day_trades_averaged_to_one_daily_return(self):
        _, daily = trades_to_daily_returns(
            [
                {"date": "2020-10-01", "final_ret": 2.0, "code": "a"},
                {"date": "2020-10-01", "final_ret": 4.0, "code": "b"},
            ]
        )
        assert daily == [3.0]

    def test_mdd_raw_and_tier_divergence_possible(self):
        trades = [
            {"date": f"2020-10-{d:02d}", "final_ret": -2.0, "code": "x"}
            for d in range(1, 11)
        ]
        m = compute_period_portfolio_metrics(trades, "2020-10-01", "2020-10-31")
        assert "mdd_pct_raw" in m
        assert "mdd_pct_tier" in m
        assert m["trading_days"] == 10


class TestVerdictAndCause:
    def test_low_n_skips_auto_verdict(self):
        v = judge_period_verdict(
            bucket="BULL", total_trades=5, cagr_pct=50.0, mdd_pct=5.0, pf=2.0
        )
        assert v == "SKIP_LOW_N"

    def test_fail_cause_a_zero_trades(self):
        assert tag_fail_cause(total_trades=0, mdd_pct=5.0, cagr_pct=10.0, bucket="BULL") == "A"

    def test_fail_cause_c_mdd(self):
        assert tag_fail_cause(total_trades=30, mdd_pct=12.0, cagr_pct=10.0, bucket="BEAR") == "C"

    def test_all_skip_low_n_is_inconclusive(self):
        rows = [
            {
                "regime_name": f"p{i}",
                "bucket": "BULL" if i < 5 else ("SIDEWAYS" if i < 10 else "BEAR"),
                "verdict": "SKIP_LOW_N",
                "mdd_pct": 5.0,
                "cagr_pct": 0.0,
                "total_trades": 0,
            }
            for i in range(15)
        ]
        s1 = build_stage1_report(rows)
        assert s1["overall_verdict"] == "INCONCLUSIVE"


class TestStage2Branching:
    """Stage2 5분기 mock 커버: A / C / B / Near-miss / Pass."""

    def _period(self, name: str, verdict: str, cause=None):
        return {
            "regime_name": name,
            "verdict": verdict,
            "fail_cause": cause,
            "mdd_pct": 8.0,
            "bucket": "BULL",
        }

    def test_stage2_branch_fail_cause_a_skip(self):
        s1 = build_stage1_report([self._period("x", "FAIL", "A")])
        s1["overall_verdict"] = "FAIL"
        plan = decide_stage2_c1(s1)
        assert plan["action"] == "SKIP"
        assert "원인 A" in plan["reason"]

    def test_stage2_branch_fail_cause_c_skip(self):
        s1 = {"overall_verdict": "FAIL", "periods": [self._period("x", "FAIL", "C")]}
        plan = decide_stage2_c1(s1)
        assert plan["action"] == "SKIP"
        assert "원인 C" in plan["reason"]

    def test_stage2_branch_fail_cause_b_reduced_ab(self):
        s1 = {
            "overall_verdict": "FAIL",
            "periods": [self._period("BULL_01", "FAIL", "B")],
        }
        plan = decide_stage2_c1(s1)
        assert plan["action"] == "REDUCED_AB"
        assert "BULL_01" in plan["regime_filter"]

    def test_stage2_branch_near_miss_full_ab(self):
        s1 = {"overall_verdict": "NEAR_MISS", "periods": [self._period("x", "NEAR_MISS")]}
        plan = decide_stage2_c1(s1)
        assert plan["action"] == "FULL_AB"

    def test_stage2_branch_pass_optional_skip(self):
        s1 = {"overall_verdict": "PASS", "periods": []}
        plan = decide_stage2_c1(s1)
        assert plan["action"] == "OPTIONAL_SKIP"


class TestMddCrosscheck:
    def test_violation_demote_badge(self):
        rows = [{"regime_name": "a", "mdd_pct": 11.0}, {"regime_name": "b", "mdd_pct": 5.0}]
        badge = mdd_crosscheck_badge(rows)
        assert badge["mdd_cap_violation"] is True
        assert badge["badge"] == "NEAR_MISS_DEMOTE"


class TestC1Boost:
    def test_sector_boost_applied(self):
        trades = [{"final_ret": 10.0, "sector_boost_eligible": True}]
        out = apply_c1_sector_boost(trades, boost_pct=5.0)
        assert out[0]["final_ret"] == pytest.approx(10.5)
        assert out[0]["c1_boost_applied"] is True


class TestRunPanelMock:
    def test_run_regime_panel_rp1_mock(self, tmp_path):
        def mock_bt(_name, _stocks, start, end):
            n = 25 if "BULL" in _name else 22
            return {
                "trades": [{"final_ret": 1.5, "date": start, "code": "005930"} for _ in range(n)],
            }

        report = run_regime_panel_rp1(
            ["005930"],
            run_backtest_fn=mock_bt,
            output_dir=str(tmp_path),
            run_stage2=False,
        )
        assert len(report["stage1"]["periods"]) == 15
        assert report["stage1"]["schema"] == "regime_panel_rp1.v2"
        assert report["stage1"]["metrics_method"] == "daily_equal_weight_v2"
        p0 = report["stage1"]["periods"][0]
        assert "mdd_pct_raw" in p0
        assert "trades_per_day" in p0
        assert "output_path" in report
