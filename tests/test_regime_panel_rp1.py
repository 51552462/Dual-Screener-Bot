"""RP-1 + C-1 regime panel — unit tests (no live fdr)."""
from __future__ import annotations

import os

import pytest

from regime_panel_rp1 import (
    RP1_CAGR_MEASUREMENT_FLOOR_PCT,
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
    _sample_tier_log,
)
from regime_panel_rp1_runner import (
    resolve_rp1_chunk_size,
    resolve_rp1_matrix_reuse,
    resolve_rp1_max_workers,
    resolve_rp1_use_matrix_cache,
    resolve_rp1_use_parallel,
)
from time_machine_backtester import (
    REGIME_PERIODS,
    collect_rp1_ohlcv_windows,
    compute_rp1_global_ohlcv_bounds,
)


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

    def test_matrix_cache_default_on(self, monkeypatch):
        monkeypatch.delenv("RP1_MATRIX", raising=False)
        monkeypatch.delenv("RP1_MATRIX_DISABLE", raising=False)
        assert resolve_rp1_use_matrix_cache() is True

    def test_matrix_cache_disable_flag(self, monkeypatch):
        monkeypatch.setenv("RP1_MATRIX", "0")
        assert resolve_rp1_use_matrix_cache() is False

    def test_matrix_reuse_flag(self, monkeypatch):
        monkeypatch.delenv("RP1_MATRIX_REUSE", raising=False)
        monkeypatch.delenv("RP1_MATRIX_SNAPSHOT", raising=False)
        assert resolve_rp1_matrix_reuse() is False
        monkeypatch.setenv("RP1_MATRIX_REUSE", "1")
        assert resolve_rp1_matrix_reuse() is True


class TestRp1OhlcvMatrixHelpers:
    def test_global_bounds_cover_gfc_backup(self):
        fetch_start, global_end = compute_rp1_global_ohlcv_bounds(REGIME_PERIODS)
        assert fetch_start <= "2008-08-01"
        assert global_end >= "2025-03-31"

    def test_collect_windows_includes_primary_and_backup(self):
        windows = collect_rp1_ohlcv_windows(REGIME_PERIODS)
        assert ("2008-09-01", "2009-03-31") in windows
        assert ("2020-10-01", "2021-11-30") in windows
        assert len(windows) >= 15


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
        assert m["metrics_method"] == "daily_equal_weight_v2.2_trade_tier"
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
        assert m.get("tier_replay_unit") == "trade"

    def test_daily_cap_uses_chronological_not_worst_returns(self):
        """Regression: sorted[:20] was picking worst returns → CAGR -99% artifact."""
        trades = [
            {"date": "2020-10-01", "final_ret": 10.0, "code": f"a{i:02d}"}
            for i in range(15)
        ] + [
            {"date": "2020-10-01", "final_ret": -50.0, "code": f"b{i:02d}"}
            for i in range(15)
        ]
        _, daily = trades_to_daily_returns(trades, max_positions_per_day=20)
        assert len(daily) == 1
        assert daily[0] == pytest.approx(-5.0)

    def test_mdd_pct_field_uses_tier_for_verdict(self):
        trades = [
            {"date": f"2020-10-{d:02d}", "final_ret": 2.0, "code": f"x{d}"}
            for d in range(1, 26)
        ]
        m = compute_period_portfolio_metrics(trades, "2020-10-01", "2020-10-31")
        assert m["mdd_pct"] == m["mdd_pct_tier"]
        assert m["mdd_pct"] <= 10.0
        badge = mdd_crosscheck_badge([{"regime_name": "x", "mdd_pct": m["mdd_pct"]}])
        assert badge["mdd_cap_violation"] is False

    def test_positive_trades_yield_non_catastrophic_cagr(self):
        trades = [
            {"date": f"2020-10-{d:02d}", "final_ret": 0.5, "code": f"t{d}"}
            for d in range(1, 22)
        ]
        m = compute_period_portfolio_metrics(trades, "2020-10-01", "2020-10-31")
        assert m["cagr_pct"] > RP1_CAGR_MEASUREMENT_FLOOR_PCT
        assert m["cagr_pct_raw"] > RP1_CAGR_MEASUREMENT_FLOOR_PCT

    def test_tier_log_sample_spreads_indices(self):
        log = [{"band": f"b{i}", "nav_after": float(i)} for i in range(100)]
        sample = _sample_tier_log(log, samples=5)
        idxs = [s["trade_idx"] for s in sample]
        assert len(set(idxs)) == 5
        assert idxs[0] == 0
        assert idxs[-1] == 99

    def test_tier_mdd_varies_by_trade_sequence(self):
        short = [
            {"date": "2020-10-01", "final_ret": -1.0, "code": "a"},
            {"date": "2020-10-02", "final_ret": 5.0, "code": "b"},
        ]
        long_loss = [
            {"date": f"2020-10-{d:02d}", "final_ret": -3.5, "code": f"c{d}"}
            for d in range(1, 21)
        ]
        m_short = compute_period_portfolio_metrics(short, "2020-10-01", "2020-10-31")
        m_long = compute_period_portfolio_metrics(long_loss, "2020-10-01", "2020-10-31")
        assert m_short["mdd_pct_tier"] != m_long["mdd_pct_tier"]
        assert m_short["tier_log_sample"] != m_long["tier_log_sample"]

    def test_uniform_tier_mdd_triggers_inconclusive(self):
        rows = [
            {
                "regime_name": f"p{i}",
                "bucket": "BULL",
                "verdict": "FAIL",
                "mdd_pct": 50.0,
                "mdd_pct_tier": 9.2015,
                "tier_events": 100 + i,
                "cagr_pct": -10.0,
            }
            for i in range(5)
        ]
        report = build_stage1_report(rows)
        assert report["tier_mdd_uniform_suspect"] is True
        assert report["overall_verdict"] == "INCONCLUSIVE"


class TestVerdictAndCause:
    def test_low_n_skips_auto_verdict(self):
        v = judge_period_verdict(
            bucket="BULL", total_trades=5, cagr_pct=50.0, mdd_pct=5.0, pf=2.0
        )
        assert v == "SKIP_LOW_N"

    def test_bull_cagr_floor_triggers_inconclusive(self):
        v = judge_period_verdict(
            bucket="BULL",
            total_trades=30,
            cagr_pct=RP1_CAGR_MEASUREMENT_FLOOR_PCT - 1.0,
            mdd_pct=5.0,
            pf=2.0,
        )
        assert v == "INCONCLUSIVE"

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
        assert report["stage1"]["schema"] == "regime_panel_rp1.v2.2"
        assert report["stage1"]["metrics_method"] == "daily_equal_weight_v2.2_trade_tier"
        p0 = report["stage1"]["periods"][0]
        assert "mdd_pct_raw" in p0
        assert "trades_per_day" in p0
        assert "output_path" in report
