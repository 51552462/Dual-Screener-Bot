"""GMM → CRYPTO_DNA_ALPHA_RANK sync (I-GMM-DNA-01)."""
from __future__ import annotations

import unittest


class TestGmmDnaAlphaSync(unittest.TestCase):
    def _sample_gmm(self):
        return {
            "TF_1D": {
                "templates": {
                    "GMM_CLUSTER_1": {
                        "dyn_cpv_min": 0.1,
                        "dyn_cpv_max": 0.3,
                        "dyn_tb_min": 5.0,
                        "dyn_tb_max": 15.0,
                        "v_energy_min": 100.0,
                        "v_energy_max": 500.0,
                        "dyn_rs_min": -2.0,
                        "dyn_rs_max": 8.0,
                        "mean_mfe": 12.5,
                        "sample_size": 40,
                        "shape": [0.1 * i for i in range(20)],
                    },
                    "GMM_CLUSTER_2": {
                        "dyn_cpv_min": 0.2,
                        "dyn_cpv_max": 0.4,
                        "dyn_tb_min": 4.0,
                        "dyn_tb_max": 10.0,
                        "v_energy_min": 80.0,
                        "v_energy_max": 400.0,
                        "dyn_rs_min": 0.0,
                        "dyn_rs_max": 5.0,
                        "mean_mfe": 8.0,
                        "sample_size": 25,
                    },
                }
            }
        }

    def test_gmm_cluster_to_dna_midpoints(self):
        from bitget.evolution.gmm_dna_alpha_sync import gmm_cluster_to_dna_template

        cluster = self._sample_gmm()["TF_1D"]["templates"]["GMM_CLUSTER_1"]
        dna = gmm_cluster_to_dna_template(cluster, name="TF_1D/GMM_CLUSTER_1")
        self.assertIsNotNone(dna)
        assert dna is not None
        self.assertAlmostEqual(dna["cpv"], 0.2)
        self.assertAlmostEqual(dna["tb"], 10.0)
        self.assertAlmostEqual(dna["bbe"], 300.0)
        self.assertAlmostEqual(dna["rs"], 3.0)
        self.assertEqual(len(dna["shape"]), 20)

    def test_rank_gmm_clusters_by_mean_mfe(self):
        from bitget.evolution.gmm_dna_alpha_sync import rank_gmm_clusters

        ranked = rank_gmm_clusters(self._sample_gmm(), top_n=2)
        self.assertEqual(len(ranked), 2)
        self.assertEqual(ranked[0][1], "GMM_CLUSTER_1")
        self.assertGreater(ranked[0][3], ranked[1][3])

    def test_sync_writes_crypto_dna_ranks(self):
        from bitget.evolution.gmm_dna_alpha_sync import sync_gmm_to_crypto_dna_alpha

        cfg = {
            "BITGET_GMM_DNA_TEMPLATES": self._sample_gmm(),
            "BITGET_GMM_DNA_UPDATED_AT": "2026-08-12T00:00:00Z",
        }
        res = sync_gmm_to_crypto_dna_alpha(cfg, force=True)
        self.assertTrue(res.get("ok"))
        self.assertTrue(res.get("updated"))
        self.assertIn("CRYPTO_DNA_ALPHA_RANK1", cfg)
        self.assertEqual(cfg["CRYPTO_DNA_ALPHA_RANK1"]["source"], "BITGET_GMM")
        self.assertIn("CRYPTO_DNA_ALPHA_SYNCED_AT", cfg)

    def test_manual_rank_not_overwritten(self):
        from bitget.evolution.gmm_dna_alpha_sync import sync_gmm_to_crypto_dna_alpha

        cfg = {
            "BITGET_GMM_DNA_TEMPLATES": self._sample_gmm(),
            "BITGET_GMM_DNA_UPDATED_AT": "2026-08-12T00:00:00Z",
            "CRYPTO_DNA_ALPHA_RANK1": {"source": "manual", "name": "keep"},
        }
        sync_gmm_to_crypto_dna_alpha(cfg, force=False)
        self.assertEqual(cfg["CRYPTO_DNA_ALPHA_RANK1"]["name"], "keep")

    def test_facts_cos_falls_back_when_sn_score_zero_paper(self):
        from bitget.forward.gates import _facts_cos_scalar_01

        cos = _facts_cos_scalar_01({"sn_score": 0.0}, 72.0, sys_config={"ENABLE_REAL_EXECUTION": False})
        self.assertAlmostEqual(cos, 0.72)

    def test_facts_cos_fail_closed_when_sn_score_zero_live(self):
        from bitget.forward.gates import _facts_cos_scalar_01

        cos = _facts_cos_scalar_01({"sn_score": 0.0}, 72.0, sys_config={"ENABLE_REAL_EXECUTION": True})
        self.assertAlmostEqual(cos, 0.0)

    def test_sync_tags_shape_source(self):
        from bitget.evolution.gmm_dna_alpha_sync import sync_gmm_to_crypto_dna_alpha

        cfg = {
            "BITGET_GMM_DNA_TEMPLATES": self._sample_gmm(),
            "BITGET_GMM_DNA_UPDATED_AT": "2026-08-12T00:00:00Z",
        }
        sync_gmm_to_crypto_dna_alpha(cfg, force=True)
        self.assertIn("shape_source", cfg["CRYPTO_DNA_ALPHA_RANK1"])

    def test_compute_shape20_from_closes(self):
        from bitget.evolution.gmm_dna_alpha_sync import compute_shape20_from_closes
        import numpy as np

        closes = np.linspace(100.0, 200.0, 80)
        shape = compute_shape20_from_closes(closes)
        self.assertIsNotNone(shape)
        assert shape is not None
        self.assertEqual(len(shape), 20)


if __name__ == "__main__":
    unittest.main()
