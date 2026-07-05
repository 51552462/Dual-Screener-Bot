"""bitget.evolution.coin_regime_vector — 4차원 코인 국면 벡터 산출·롤링 히스토리."""
from __future__ import annotations

import unittest
from unittest import mock

from bitget.evolution import coin_regime_vector as crv


class TestBuildCurrentCoinRegimeVector(unittest.TestCase):
    def test_uses_crypto_regime_detail_when_present(self):
        cfg = {
            "CRYPTO_REGIME_DETAIL": {
                "dist_from_ema200_pct": 10.0,
                "ema200_slope_pct": 1.0,
                "atr_pct": 5.0,
                "eth_btc_breadth": 1.1,
            }
        }
        built = crv.build_current_coin_regime_vector(cfg)
        self.assertEqual(len(built["vector"]), crv.N_DIMS)
        self.assertAlmostEqual(built["vector_map"]["dist_ema200_z"], 1.0)
        self.assertAlmostEqual(built["vector_map"]["ema200_slope_z"], 0.5)
        self.assertAlmostEqual(built["vector_map"]["atr_z"], 1.0)
        self.assertAlmostEqual(built["vector_map"]["breadth_z"], 1.0)
        self.assertEqual(built["data_completeness"], 1.0)

    def test_empty_cfg_falls_back_to_neutral_defaults(self):
        built = crv.build_current_coin_regime_vector({})
        self.assertEqual(built["vector"], [0.0, 0.0, 0.0, 0.0])
        self.assertEqual(built["data_completeness"], 0.0)

    def test_regime_index_direction(self):
        bull_vec = [1.0, 1.0, 0.0, 0.0]
        bear_vec = [-1.0, -1.0, 0.0, 0.0]
        self.assertGreater(crv.regime_index(bull_vec), crv.regime_index(bear_vec))

    def test_regime_index_handles_bad_input(self):
        self.assertEqual(crv.regime_index([1.0]), 0.0)
        self.assertEqual(crv.regime_index("not-a-list"), 0.0)


class TestVectorHistoryRoundtrip(unittest.TestCase):
    def test_append_and_load(self):
        store: dict = {}

        def _fake_update(key, modifier):
            store[key] = modifier(store.get(key))

        cfg = {
            "CRYPTO_REGIME_DETAIL": {
                "dist_from_ema200_pct": 5.0,
                "ema200_slope_pct": 0.5,
                "atr_pct": 3.0,
                "eth_btc_breadth": 1.0,
            }
        }
        with mock.patch(
            "bitget.infra.config_manager.update_config_value", side_effect=_fake_update
        ):
            crv.append_coin_regime_vector_history(cfg)

        cfg[crv.VECTOR_HISTORY_KEY] = store[crv.VECTOR_HISTORY_KEY]
        history = crv.load_vector_history(cfg)
        self.assertEqual(len(history), 1)
        arrays = crv.load_vector_history_arrays(cfg)
        self.assertEqual(len(arrays), 1)
        self.assertEqual(len(arrays[0]), crv.N_DIMS)

    def test_history_cap_enforced(self):
        store: dict = {crv.VECTOR_HISTORY_KEY: [{"ts": "2020-01-01", "vector": [0.0] * crv.N_DIMS}] * (crv.HISTORY_CAP + 5)}

        def _fake_update(key, modifier):
            store[key] = modifier(store.get(key))

        with mock.patch(
            "bitget.infra.config_manager.update_config_value", side_effect=_fake_update
        ):
            crv.append_coin_regime_vector_history({})

        self.assertEqual(len(store[crv.VECTOR_HISTORY_KEY]), crv.HISTORY_CAP)


if __name__ == "__main__":
    unittest.main()
