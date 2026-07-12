"""Chapter 4 — satellite HTTP / canary public REST via network_retry SSOT."""
from __future__ import annotations

import inspect
from unittest import mock

import pytest
import requests

from bitget.infra import network_retry as nr


def test_http_get_retries_on_429_then_ok():
    calls = {"n": 0}

    class _Resp:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True}

    class _Resp429:
        status_code = 429
        headers = {"Retry-After": "1"}

        def raise_for_status(self):
            e = requests.HTTPError("429")
            e.response = self
            raise e

    def _fake_get(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Resp429()
        return _Resp()

    with mock.patch("requests.get", side_effect=_fake_get), mock.patch(
        "bitget.infra.network_retry.time.sleep"
    ):
        out = nr.http_get("https://example.test/x", op="unit.http", swallow=True)
    assert out is not None
    assert out.json()["ok"] is True
    assert calls["n"] == 2


def test_sentiment_doomsday_alt_use_http_get_ssot():
    from bitget import alt_data_miner as alt
    from bitget import doomsday_bot as doom
    from bitget import sentiment_miner as sent

    for mod in (sent, doom, alt):
        src = inspect.getsource(mod)
        assert "from bitget.infra.network_retry import http_get" in src
        assert "requests.get(" not in src
        assert "backoff_sleep(" not in src


def test_canary_exporter_uses_call_with_retry_ssot():
    from bitget import canary_exporter as canary

    src = inspect.getsource(canary)
    assert "from bitget.infra.network_retry import call_with_retry" in src
    assert 'op="canary.load_markets"' in src
    assert 'op="canary.fetch_tickers' in src
    assert 'op="canary.fetch_open_interest"' in src
    assert 'op="canary.fetch_ohlcv.btc"' in src
    assert "rate_limit_guard" not in src


def test_sentiment_fear_greed_defaults_when_http_fails():
    from bitget import sentiment_miner as sent

    with mock.patch("bitget.sentiment_miner.http_get", return_value=None):
        val, label = sent.fetch_fear_greed()
    assert val == pytest.approx(50.0)
    assert label == "Neutral"


def test_alt_fetch_global_raises_when_exhausted():
    from bitget import alt_data_miner as alt

    with mock.patch(
        "bitget.alt_data_miner.http_get",
        side_effect=nr.NetworkRetryExhausted("down"),
    ), pytest.raises(nr.NetworkRetryExhausted):
        alt._fetch_global()
