"""Dynamic order routing integration tests (no live exchange)."""
from __future__ import annotations

import unittest


class TestOrderRouting(unittest.TestCase):
    def test_safe_evaluate_routing_fallback_on_bad_atr(self):
        from bitget.trading.order_routing import safe_evaluate_routing

        d = safe_evaluate_routing(
            signal_score=5.0,
            atr_pct=0.0,
            order_size_usd=10_000.0,
        )
        self.assertEqual(d.route, "HYBRID_TWAP")
        self.assertEqual(d.maker_ratio, 0.5)
        self.assertEqual(d.taker_ratio, 0.5)

    def test_signal_score_normalizes_0_100_scale(self):
        from bitget.trading.order_routing import safe_evaluate_routing

        d = safe_evaluate_routing(
            signal_score=95.0,
            atr_pct=1.5,
            order_size_usd=50_000.0,
        )
        self.assertIn(d.route, ("TAKER_IMMEDIATE", "MAKER_PASSIVE", "HYBRID_TWAP"))

    def test_orderbook_imbalance_from_buffer(self):
        from bitget.data.stream_buffer import StreamBuffer
        from bitget.trading.order_routing import compute_orderbook_imbalance

        buf = StreamBuffer()
        buf.update_orderbook(
            "BTCUSDT",
            bids=[["100.0", "3"], ["99.9", "1"]],
            asks=[["100.1", "1"]],
            inst_type="USDT-FUTURES",
        )
        # Patch global buffer for test
        import bitget.data.stream_buffer as sb

        orig = sb._GLOBAL_BUFFER
        sb._GLOBAL_BUFFER = buf
        try:
            imb = compute_orderbook_imbalance("BTC_USDT", market_type="futures")
        finally:
            sb._GLOBAL_BUFFER = orig
        self.assertGreater(imb, 0.0)
        self.assertLessEqual(imb, 1.0)

    def test_routing_decision_to_meta_keys(self):
        from bitget.trading.order_routing import routing_decision_to_meta
        from dynamic_order_router import RoutingDecision

        meta = routing_decision_to_meta(
            RoutingDecision("HYBRID_TWAP", 0.5, 0.5, 0.6, 0.4, "test")
        )
        self.assertEqual(meta["routing_route"], "HYBRID_TWAP")
        self.assertEqual(meta["routing_maker_ratio"], 0.6)


class TestDynamicOrderRouter(unittest.TestCase):
    def test_router_self_test(self):
        from dynamic_order_router import test_dynamic_order_router

        test_dynamic_order_router()


if __name__ == "__main__":
    unittest.main()
