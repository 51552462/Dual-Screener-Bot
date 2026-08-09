"""RP-1 L0 OHLCV cache + FDR timeout tests."""
from __future__ import annotations

import time
import pandas as pd
import pytest

import rp1_ohlcv_cache as cache


class TestFetchOhlcvCached:
    def test_timeout_skips_ticker(self, tmp_path, monkeypatch):
        monkeypatch.setenv("RP1_OHLCV_CACHE_DIR", str(tmp_path))

        def _slow_fetch(_ticker, _start, _end):
            time.sleep(2.0)
            return pd.DataFrame()

        monkeypatch.setattr(cache, "_fetch_fdr", _slow_fetch)
        df, gate = cache.fetch_ohlcv_cached("SLOW", "2020-01-01", "2020-12-31", timeout_sec=1)
        assert df is None
        assert gate == "timeout"

    def test_cache_hit_avoids_refetch(self, tmp_path, monkeypatch):
        monkeypatch.setenv("RP1_OHLCV_CACHE_DIR", str(tmp_path))
        idx = pd.date_range("2020-01-01", periods=30, freq="D")
        frame = pd.DataFrame({"Close": range(30)}, index=idx)

        calls = {"n": 0}

        def _fetch_once(_ticker, _start, _end):
            calls["n"] += 1
            return frame

        monkeypatch.setattr(cache, "_fetch_fdr", _fetch_once)

        df1, gate1 = cache.fetch_ohlcv_cached("005930", "2020-01-01", "2020-01-28")
        assert gate1 == "fetch_ok"
        assert calls["n"] == 1

        df2, gate2 = cache.fetch_ohlcv_cached("005930", "2020-01-01", "2020-01-28")
        assert gate2 == "cache_hit"
        assert calls["n"] == 1
        assert len(df2) == len(df1)

    def test_matrix_prime_continues_after_timeout(self, monkeypatch):
        """One hung ticker must not block remaining tickers in matrix batch."""
        from regime_panel_rp1_runner import _run_matrix_ticker_batch

        windows = [("2020-10-01", "2020-10-31")]

        def _fake_multi(code, *_args, **_kwargs):
            if code == "HANG":
                return {"by_window": {}, "fetch_gate": "timeout"}
            wkey = "2020-10-01|2020-10-31"
            return {
                "by_window": {
                    wkey: {"trades": [{"date": "2020-10-01", "final_ret": 1.0, "code": code}], "gate": "success"}
                },
                "fetch_gate": "success",
            }

        monkeypatch.setattr(
            "regime_panel_rp1_runner.backtest_ticker_rp1_multi_window",
            _fake_multi,
        )

        merged = _run_matrix_ticker_batch(
            ["HANG", "OK1", "OK2"],
            "2020-09-01",
            "2020-10-31",
            windows,
            {"t1": {}},
            {},
            use_pool=False,
        )
        wkey = "2020-10-01|2020-10-31"
        assert len(merged[wkey]["trades"]) == 2

    def test_corrupt_cache_refetches(self, tmp_path, monkeypatch):
        monkeypatch.setenv("RP1_OHLCV_CACHE_DIR", str(tmp_path))
        bad = tmp_path / "BAD.parquet"
        bad.write_text("not parquet", encoding="utf-8")

        idx = pd.date_range("2020-01-01", periods=5, freq="D")
        frame = pd.DataFrame({"Close": [1, 2, 3, 4, 5]}, index=idx)

        monkeypatch.setattr(cache, "_fetch_fdr", lambda *_a, **_k: frame)
        df, gate = cache.fetch_ohlcv_cached("BAD", "2020-01-01", "2020-01-05")
        assert gate == "fetch_ok"
        assert df is not None and not df.empty
