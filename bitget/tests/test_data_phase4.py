"""Phase 4 / 4b data layer unit tests (no live WS)."""
from __future__ import annotations

import unittest


class TestStreamBuffer(unittest.TestCase):
    def test_orderbook_spread_bps(self):
        from bitget.data.stream_buffer import StreamBuffer

        buf = StreamBuffer()
        buf.update_orderbook(
            "BTCUSDT",
            bids=[["100.0", "1"]],
            asks=[["101.0", "1"]],
            inst_type="SPOT",
        )
        spread = buf.orderbook_spread_bps("BTCUSDT", "SPOT")
        self.assertIsNotNone(spread)
        assert spread is not None
        self.assertGreater(spread, 90.0)

    def test_spread_prefers_orderbook(self):
        from bitget.data.stream_buffer import StreamBuffer

        buf = StreamBuffer()
        buf.update_ticker("ETHUSDT", last=100.0, bid=99.0, ask=100.0, inst_type="SPOT")
        buf.update_orderbook(
            "ETHUSDT",
            bids=[["100.0", "1"]],
            asks=[["100.2", "1"]],
            inst_type="SPOT",
        )
        spread = buf.spread_bps("ETHUSDT", "SPOT")
        self.assertIsNotNone(spread)
        assert spread is not None
        self.assertLess(spread, 25.0)


class TestSlippagePreTrade(unittest.TestCase):
    def test_blocks_wide_orderbook_spread(self):
        from bitget.data.stream_buffer import get_stream_buffer
        from bitget.trading.slippage_guard import estimate_slippage_bps

        buf = get_stream_buffer()
        buf.update_orderbook(
            "BTCUSDT",
            bids=[["100.0", "1"]],
            asks=[["102.0", "1"]],
            inst_type="SPOT",
        )
        ok, spread, reason = estimate_slippage_bps(
            "BTCUSDT",
            market_type="spot",
            max_spread_bps=30.0,
            max_stale_sec=60.0,
        )
        self.assertFalse(ok)
        self.assertIsNotNone(spread)
        self.assertIn("orderbook_spread", reason)

    def test_passes_tight_spread(self):
        from bitget.data.stream_buffer import get_stream_buffer
        from bitget.trading.slippage_guard import run_pre_trade_gate

        buf = get_stream_buffer()
        buf.update_orderbook(
            "SOLUSDT",
            bids=[["50.0", "1"]],
            asks=[["50.01", "1"]],
            inst_type="USDT-FUTURES",
        )
        ok, meta = run_pre_trade_gate("SOL_USDT", "futures", {"ENABLE_SLIPPAGE_GUARD": True})
        self.assertTrue(ok)
        self.assertIn("ok", meta.get("slippage_reason", ""))


class TestGapHealer(unittest.TestCase):
    def test_assess_fresh_buffer(self):
        from bitget.data.gap_healer import assess_buffer_health
        from bitget.data.stream_buffer import get_stream_buffer

        buf = get_stream_buffer()
        buf.update_ticker("BTCUSDT", last=1.0, bid=1.0, ask=1.0, inst_type="SPOT")
        health = assess_buffer_health(symbols=["BTCUSDT"], max_age_sec=120.0)
        self.assertFalse(health["global_stale"])


if __name__ == "__main__":
    unittest.main()
