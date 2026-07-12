"""Chapter 1 — bitget.infra.network_retry SSOT."""
from __future__ import annotations

from unittest import mock

import pytest
import requests

from bitget.infra import network_retry as nr
from bitget.infra.memory_policy import (
    NETWORK_429_BACKOFF_BASE_SEC,
    NETWORK_BACKOFF_BASE_SEC,
    NETWORK_BACKOFF_CAP_SEC,
    NETWORK_RETRY_MAX_ATTEMPTS,
)


def test_memory_policy_network_retry_constants():
    assert NETWORK_RETRY_MAX_ATTEMPTS == 3
    assert NETWORK_BACKOFF_BASE_SEC == 2.0
    assert NETWORK_BACKOFF_CAP_SEC == 8.0
    assert NETWORK_429_BACKOFF_BASE_SEC == 4.0


def test_classify_connection_timeout_429_fatal():
    c = nr.classify_network_error(ConnectionError("reset by peer"))
    assert c.kind == nr.NetworkErrorKind.CONNECTION
    assert c.retryable is True

    t = nr.classify_network_error(TimeoutError("timed out"))
    assert t.kind == nr.NetworkErrorKind.TIMEOUT
    assert t.retryable is True

    class _Resp:
        status_code = 429
        headers = {"Retry-After": "5"}

    http429 = requests.HTTPError("429")
    http429.response = _Resp()  # type: ignore[attr-defined]
    r = nr.classify_network_error(http429)
    assert r.kind == nr.NetworkErrorKind.RATE_LIMIT_429
    assert r.retryable is True
    assert r.retry_after_sec == 5.0

    class _Resp400:
        status_code = 400
        headers = {}

    http400 = requests.HTTPError("bad request")
    http400.response = _Resp400()  # type: ignore[attr-defined]
    f = nr.classify_network_error(http400)
    assert f.kind == nr.NetworkErrorKind.FATAL
    assert f.retryable is False


def test_backoff_schedule_2_4_8_and_429_floor():
    with mock.patch("bitget.infra.network_retry.random.uniform", return_value=0.0):
        assert nr.compute_backoff_sec(0, nr.NetworkErrorKind.TIMEOUT) == pytest.approx(2.0)
        assert nr.compute_backoff_sec(1, nr.NetworkErrorKind.CONNECTION) == pytest.approx(4.0)
        assert nr.compute_backoff_sec(2, nr.NetworkErrorKind.TRANSIENT) == pytest.approx(8.0)
        # capped
        assert nr.compute_backoff_sec(5, nr.NetworkErrorKind.TIMEOUT) == pytest.approx(8.0)
        # 429 longer base; Retry-After wins when larger
        assert nr.compute_backoff_sec(0, nr.NetworkErrorKind.RATE_LIMIT_429) == pytest.approx(4.0)
        assert nr.compute_backoff_sec(
            0, nr.NetworkErrorKind.RATE_LIMIT_429, retry_after_sec=12.0
        ) == pytest.approx(12.0)


def test_call_with_retry_recovers_after_timeouts():
    calls = {"n": 0}

    def _flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("slow")
        return "ok"

    with mock.patch("bitget.infra.network_retry.time.sleep") as slept:
        out = nr.call_with_retry(_flaky, op="unit", max_attempts=3, swallow=True)
    assert out == "ok"
    assert calls["n"] == 3
    assert slept.call_count == 2


def test_call_with_retry_429_then_ok():
    calls = {"n": 0}

    class _Resp:
        status_code = 429
        headers = {"Retry-After": "1"}

    def _flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            e = requests.HTTPError("rate")
            e.response = _Resp()  # type: ignore[attr-defined]
            raise e
        return 42

    with mock.patch("bitget.infra.network_retry.time.sleep") as slept:
        out = nr.call_with_retry(_flaky, op="unit429", max_attempts=3)
    assert out == 42
    assert slept.call_count == 1


def test_call_with_retry_fatal_swallowed_returns_default():
    def _boom():
        class _Resp:
            status_code = 401
            headers = {}

        e = requests.HTTPError("unauthorized")
        e.response = _Resp()  # type: ignore[attr-defined]
        raise e

    with mock.patch("bitget.infra.network_retry.time.sleep") as slept:
        out = nr.call_with_retry(_boom, op="auth", default="safe", swallow=True)
    assert out == "safe"
    assert slept.call_count == 0


def test_call_with_retry_exhausted_raises_when_not_swallow():
    def _always():
        raise ConnectionError("down")

    with mock.patch("bitget.infra.network_retry.time.sleep"), pytest.raises(
        nr.NetworkRetryExhausted
    ):
        nr.call_with_retry(_always, op="down", max_attempts=2, swallow=False)
