"""Chapter 3 — MTF / funding public REST wired through network_retry SSOT."""
from __future__ import annotations

import inspect
from unittest import mock

import pytest

from bitget import funding_fetcher as ff
from bitget import mtf_data_updater as mtf


def test_funding_fetcher_uses_network_retry_ssot():
    src = inspect.getsource(ff)
    assert "from bitget.infra.network_retry import call_with_retry" in src
    assert 'op="funding.fetch_funding_rate"' in src
    assert 'op="funding.load_markets"' in src
    assert "backoff_sleep(" not in src


def test_mtf_data_updater_uses_network_retry_ssot():
    src = inspect.getsource(mtf)
    assert "from bitget.infra.network_retry import call_with_retry" in src
    assert 'op="mtf.load_markets"' in src
    assert 'op="mtf.fetch_tickers"' in src
    assert 'op="mtf.fetch_ohlcv"' in src
    assert "backoff_sleep(" not in src
    assert "throttle(" not in src


def test_fetch_funding_snapshot_retries_then_ok(monkeypatch):
    calls = {"n": 0}

    class _Ex:
        markets = {}

        def load_markets(self):
            return True

        def fetch_funding_rate(self, symbol):
            calls["n"] += 1
            if calls["n"] < 2:
                raise TimeoutError("funding slow")
            return {
                "fundingRate": 0.0001,
                "nextFundingTimestamp": 1_720_000_000_000,
                "symbol": symbol,
            }

    monkeypatch.setattr(ff, "_pub_ex", None)
    monkeypatch.setattr(ff, "ccxt", mock.MagicMock(bitget=lambda cfg: _Ex()))
    with mock.patch("bitget.infra.network_retry.time.sleep"):
        out = ff.fetch_funding_snapshot("BTC_USDT")
    assert out is not None
    assert out["funding_rate"] == pytest.approx(0.0001)
    assert calls["n"] == 2


def test_fetch_funding_snapshot_exhausted_returns_none(monkeypatch):
    class _Ex:
        def load_markets(self):
            return True

        def fetch_funding_rate(self, symbol):
            raise ConnectionError("down")

    monkeypatch.setattr(ff, "_pub_ex", None)
    monkeypatch.setattr(ff, "ccxt", mock.MagicMock(bitget=lambda cfg: _Ex()))
    with mock.patch("bitget.infra.network_retry.time.sleep"):
        assert ff.fetch_funding_snapshot("ETH_USDT") is None


def test_load_dynamic_universe_empty_on_ticker_failure():
    class _Ex:
        markets = {"BTC/USDT": {"active": True, "quote": "USDT", "type": "spot"}}

        def fetch_tickers(self):
            raise TimeoutError("tickers down")

    with mock.patch("bitget.infra.network_retry.time.sleep"):
        out = mtf.load_dynamic_universe(_Ex(), "spot", 1.0, "USDT")
    assert out == []


def test_fetch_symbol_ohlcv_payload_retries_then_stores():
    calls = {"n": 0}

    class _Ex:
        def fetch_ohlcv(self, symbol, timeframe, limit):
            calls["n"] += 1
            if calls["n"] < 2:
                raise TimeoutError("ohlcv slow")
            return [[1, 1.0, 2.0, 0.5, 1.5, 10.0]]

    with mock.patch("bitget.infra.network_retry.time.sleep"), mock.patch(
        "bitget.mtf_data_updater.time.sleep"
    ), mock.patch("bitget.mtf_data_updater.flush_gc"):
        sym, payloads = mtf.fetch_symbol_ohlcv_payload(
            _Ex(), "spot", "BTC/USDT", ["1h"], 10
        )
    assert sym == "BTC/USDT"
    assert len(payloads) == 1
    assert payloads[0][1] == "1h"
    assert calls["n"] == 2
