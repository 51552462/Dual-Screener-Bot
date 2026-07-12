"""Chapter 2 — trading / OMS REST wired through network_retry SSOT."""
from __future__ import annotations

import inspect
from unittest import mock

import pytest

from bitget.infra import network_retry as nr


def _assert_call_with_retry_ssot(mod) -> None:
    src = inspect.getsource(mod)
    assert "from bitget.infra.network_retry import" in src
    assert "call_with_retry" in src
    # Client spacing stays inside call_with_retry; no ad-hoc retry loops.
    assert "_transient_error" not in src
    assert "backoff_sleep(" not in src


def test_oms_core_uses_network_retry_ssot():
    from bitget.trading import oms_core as m

    _assert_call_with_retry_ssot(m)
    src = inspect.getsource(m)
    assert 'op="oms.load_markets"' in src
    assert 'op="oms.create_order"' in src


def test_leverage_position_recon_executor_use_network_retry_ssot():
    from bitget import executor as ex_mod
    from bitget.trading import leverage_manager as lev
    from bitget.trading import position_manager as pos
    from bitget.trading import reconciliation as recon

    for m in (lev, pos, recon):
        _assert_call_with_retry_ssot(m)

    pos_src = inspect.getsource(pos)
    assert 'op="oms.fetch_positions"' in pos_src

    recon_src = inspect.getsource(recon)
    assert 'op="oms.fetch_my_trades"' in recon_src
    assert "list_open_orders" in recon_src
    assert "fetch_order_snapshot" in recon_src

    from bitget.trading import order_snapshot as osnap

    osnap_src = inspect.getsource(osnap)
    assert 'op="oms.fetch_open_orders"' in osnap_src
    assert 'op="oms.fetch_order"' in osnap_src

    ex_src = inspect.getsource(ex_mod)
    assert 'op="oms.fetch_ticker"' not in ex_src or "fetch_ref_price" in ex_src
    assert "fetch_ref_price" in ex_src
    assert "fetch_usdt_balance" in ex_src

    from bitget.trading import account_snapshot as asnap
    from bitget.trading import market_price_snapshot as mps

    asnap_src = inspect.getsource(asnap)
    assert 'op="oms.fetch_balance"' in asnap_src
    assert "call_with_retry" in asnap_src
    assert "try_private_ws_usdt_total" in asnap_src

    mps_src = inspect.getsource(mps)
    assert 'op="oms.fetch_ticker"' in mps_src
    assert "try_public_ws_ref_price" in mps_src
    _assert_call_with_retry_ssot(mps)
    _assert_call_with_retry_ssot(asnap)


def test_build_open_position_index_retries_then_ok():
    from bitget.trading.position_manager import build_open_position_index

    calls = {"n": 0}

    class _Ex:
        def fetch_positions(self):
            calls["n"] += 1
            if calls["n"] < 2:
                raise TimeoutError("slow positions")
            return [
                {"symbol": "BTC/USDT:USDT", "side": "long", "contracts": 1.5},
                {"symbol": "ETH/USDT:USDT", "side": "short", "contracts": 2.0},
            ]

    with mock.patch("bitget.infra.network_retry.time.sleep"):
        out = build_open_position_index(_Ex())
    assert calls["n"] == 2
    assert out[("BTC/USDT:USDT", "LONG")] == pytest.approx(1.5)
    assert out[("ETH/USDT:USDT", "SHORT")] == pytest.approx(2.0)


def test_build_open_position_index_exhausted_returns_empty():
    from bitget.trading.position_manager import build_open_position_index

    class _Ex:
        def fetch_positions(self):
            raise ConnectionError("down")

    with mock.patch("bitget.infra.network_retry.time.sleep"):
        out = build_open_position_index(_Ex())
    assert out == {}


def test_oms_place_market_order_retries_transient_then_fills():
    from bitget.trading.oms_core import oms_place_market_order

    calls = {"n": 0}

    class _Ex:
        def create_order(self, *a, **k):
            calls["n"] += 1
            if calls["n"] < 2:
                raise TimeoutError("create slow")
            return {
                "id": "oid-1",
                "filled": 1.0,
                "remaining": 0.0,
                "status": "closed",
            }

    with mock.patch("bitget.trading.oms_core.meta_kill_switch_active", return_value=False), mock.patch(
        "bitget.trading.oms_core.oms_defense_block_reason", return_value=None
    ), mock.patch("bitget.infra.network_retry.time.sleep"):
        out = oms_place_market_order(_Ex(), "BTC/USDT:USDT", "buy", 1.0, max_attempts=3)

    assert out["ok"] is True
    assert out["order_id"] == "oid-1"
    assert calls["n"] == 2


def test_oms_place_market_order_exhausted_returns_network_kind():
    from bitget.trading.oms_core import oms_place_market_order

    class _Ex:
        def create_order(self, *a, **k):
            raise ConnectionError("peer reset")

    with mock.patch("bitget.trading.oms_core.meta_kill_switch_active", return_value=False), mock.patch(
        "bitget.trading.oms_core.oms_defense_block_reason", return_value=None
    ), mock.patch("bitget.infra.network_retry.time.sleep"):
        out = oms_place_market_order(_Ex(), "BTC/USDT:USDT", "buy", 1.0, max_attempts=2)

    assert out["ok"] is False
    assert out["status"] == "error"
    assert out.get("network_kind") == nr.NetworkErrorKind.CONNECTION.value
    assert out.get("client_order_id")


def test_apply_futures_leverage_swallows_exhausted():
    from bitget.trading.leverage_manager import apply_futures_leverage

    class _Ex:
        def set_leverage(self, *a, **k):
            raise TimeoutError("lev timeout")

    with mock.patch("bitget.infra.network_retry.time.sleep"):
        ok = apply_futures_leverage(_Ex(), "BTC/USDT:USDT", 3.0)
    assert ok is False
