"""BULL-RECENCY-01 CLUSTER_1 bounds tightening — unit tests."""
from __future__ import annotations

import copy

import pytest

from bull_recency_01_bounds import (
    apply_bull_recency_01_brain_patch,
    is_cluster_1_explosive_template,
    mirror_bounds_for_time_machine,
    tighten_axis_range,
    tighten_template_bounds,
)


class TestCluster1NameMatch:
    def test_explosive_korean(self):
        assert is_cluster_1_explosive_template("CLUSTER_1_강응축_폭발형_250811")

    def test_cluster_2_rejected(self):
        assert not is_cluster_1_explosive_template("CLUSTER_2_강응축_폭발형")

    def test_stealth_rejected(self):
        assert not is_cluster_1_explosive_template("CLUSTER_1_매집봉_스텔스형")


class TestTightenBounds:
    def test_shrink_narrows_box(self):
        lo, hi = tighten_axis_range(0.2, 0.8, shrink=0.20)
        assert lo > 0.2
        assert hi < 0.8
        assert abs((lo + hi) / 2 - 0.5) < 1e-9

    def test_floor_lift_raises_min(self):
        lo_plain, _ = tighten_axis_range(1.0, 5.0, shrink=0.20, floor_lift=0.0)
        lo_lift, _ = tighten_axis_range(1.0, 5.0, shrink=0.20, floor_lift=0.15)
        assert lo_lift > lo_plain

    def test_dyn_and_legacy_keys(self):
        bounds = {
            "dyn_cpv_min": 0.1,
            "dyn_cpv_max": 0.9,
            "tb_min": 2.0,
            "tb_max": 20.0,
            "bbe_min": 5.0,
            "bbe_max": 30.0,
        }
        out = tighten_template_bounds(bounds, shrink=0.20)
        assert out["dyn_cpv_min"] > 0.1
        assert out["tb_min"] > 2.0
        assert out["bbe_min"] > 5.0


class TestPrepareSimTemplates:
    def test_scope_then_reorder(self):
        from bull_recency_01_bounds import prepare_live_templates_for_br01_sim

        ml = {
            "CLUSTER_2_강응축_폭발형_260628": {"dyn_cpv_min": 0.0},
            "CLUSTER_1_혼조세_돌연변이형_260719": {"dyn_cpv_min": 0.0},
            "CLUSTER_1_강응축_폭발형_260802": {"dyn_cpv_min": 0.1},
            "CLUSTER_1_강응축_폭발형_260628": {"dyn_cpv_min": 0.2},
        }
        ordered, audit = prepare_live_templates_for_br01_sim(ml)
        assert audit["ready"] is True
        assert list(ordered) == [
            "CLUSTER_1_강응축_폭발형_260628",
            "CLUSTER_1_강응축_폭발형_260802",
        ]
        assert audit["scope"]["live_out"] == 2
        assert audit["scope"]["live_in"] == 4


class TestBr01SmokeValidate:
    def test_rejects_fallthrough(self):
        from bull_recency_01_bounds import validate_br01_smoke_trades

        trades = [{"template": "CLUSTER_2_x"}] * 3000
        ok, msg = validate_br01_smoke_trades(trades)
        assert ok is False
        assert "fallthrough" in msg

    def test_accepts_cluster_1_only(self):
        from bull_recency_01_bounds import validate_br01_smoke_trades

        trades = [{"template": "CLUSTER_1_강응축_폭발형_260628"}] * 5000
        ok, msg = validate_br01_smoke_trades(trades)
        assert ok is True


class TestBr01SsotBounds:
    def test_ssot_overlay_before_patch(self):
        from bull_recency_01_bounds import (
            apply_bull_recency_01_brain_patch,
            apply_br01_ssot_bounds_to_brain,
        )

        brain = {
            "LIVE_CLUSTER_TEMPLATES": {
                "CLUSTER_1_강응축_폭발형_260628": {"dyn_cpv_min": -99.0},
            }
        }
        overlaid, audit = apply_br01_ssot_bounds_to_brain(brain, force=True)
        assert audit["applied"] is True
        b = overlaid["LIVE_CLUSTER_TEMPLATES"]["CLUSTER_1_강응축_폭발형_260628"]
        assert b["dyn_cpv_min"] == -0.51
        assert b["v_energy_min"] == 98.1
        _, patch_audit = apply_bull_recency_01_brain_patch(overlaid, shrink=0.45)
        before = patch_audit["patched"][0]["bounds_before"]
        after = patch_audit["patched"][0]["bounds_after"]
        assert before["dyn_cpv_min"] == -0.51
        assert after["dyn_cpv_min"] == -0.1703
        assert after["dyn_cpv_min"] != before["dyn_cpv_min"]

    def test_mirror_dyn_wins_when_both_aliases_present(self):
        from bull_recency_01_bounds import mirror_bounds_for_time_machine

        bounds = {"cpv_min": -0.51, "cpv_max": 1.0, "dyn_cpv_min": -0.1703, "dyn_cpv_max": 0.6603}
        out = mirror_bounds_for_time_machine(bounds)
        assert out["cpv_min"] == -0.1703
        assert out["dyn_cpv_min"] == -0.1703


class TestBrainPatch:
    def test_only_cluster_1_explosive_touched(self):
        brain = {
            "LIVE_CLUSTER_TEMPLATES": {
                "CLUSTER_1_강응축_폭발형_250811": {
                    "dyn_cpv_min": 0.1,
                    "dyn_cpv_max": 0.9,
                    "dyn_tb_min": 2.0,
                    "dyn_tb_max": 20.0,
                    "v_energy_min": 5.0,
                    "v_energy_max": 30.0,
                },
                "CLUSTER_2_혼조세_돌연변이형": {
                    "dyn_cpv_min": 0.0,
                    "dyn_cpv_max": 1.0,
                    "dyn_tb_min": 1.0,
                    "dyn_tb_max": 15.0,
                    "v_energy_min": 3.0,
                    "v_energy_max": 25.0,
                },
            }
        }
        original = copy.deepcopy(brain)
        patched, audit = apply_bull_recency_01_brain_patch(brain, shrink=0.20)
        assert brain == original
        assert audit["templates_patched"] == 1
        c1 = patched["LIVE_CLUSTER_TEMPLATES"]["CLUSTER_1_강응축_폭발형_250811"]
        c2 = patched["LIVE_CLUSTER_TEMPLATES"]["CLUSTER_2_혼조세_돌연변이형"]
        assert c1["dyn_cpv_min"] > 0.1
        assert c2["dyn_cpv_min"] == 0.0

    def test_no_match_returns_empty_audit(self):
        brain = {"LIVE_CLUSTER_TEMPLATES": {"CLUSTER_2_x": {"dyn_cpv_min": 0.1, "dyn_cpv_max": 0.5}}}
        _, audit = apply_bull_recency_01_brain_patch(brain)
        assert audit["templates_patched"] == 0

    def test_mirror_legacy_keys_for_time_machine(self):
        bounds = {
            "cpv_min": 0.1,
            "cpv_max": 0.9,
            "tb_min": 2.0,
            "tb_max": 20.0,
            "bbe_min": 5.0,
            "bbe_max": 30.0,
        }
        out = mirror_bounds_for_time_machine(bounds)
        assert out["dyn_cpv_min"] == 0.1
        assert out["dyn_cpv_max"] == 0.9
        assert out["dyn_tb_min"] == 2.0
        assert out["v_energy_min"] == 5.0

    def test_patch_writes_dyn_keys_for_legacy_templates(self):
        brain = {
            "LIVE_CLUSTER_TEMPLATES": {
                "CLUSTER_1_강응축_폭발형_260628": {
                    "cpv_min": 0.0,
                    "cpv_max": 1.0,
                    "tb_min": 1.0,
                    "tb_max": 10.0,
                    "bbe_min": 3.0,
                    "bbe_max": 25.0,
                },
            }
        }
        patched, audit = apply_bull_recency_01_brain_patch(brain, shrink=0.20)
        ba = patched["LIVE_CLUSTER_TEMPLATES"]["CLUSTER_1_강응축_폭발형_260628"]
        assert "dyn_cpv_min" in ba and "v_energy_min" in ba
        assert audit["patched"][0]["keys_mirrored_for_time_machine"] is True


class TestKrRsLever:
    def test_is_kr_ticker(self):
        from bull_recency_01_bounds import is_kr_ticker

        assert is_kr_ticker("005930")
        assert not is_kr_ticker("AAPL")

    def test_kr_lever_tags_binding_template(self):
        from bull_recency_01_bounds import apply_kr_rs_lever_to_brain

        brain = {
            "LIVE_CLUSTER_TEMPLATES": {
                "CLUSTER_1_강응축_폭발형_260628": {"dyn_cpv_min": 0.0},
                "CLUSTER_1_강응축_폭발형_260802": {"dyn_cpv_min": 0.0},
            }
        }
        patched, audit = apply_kr_rs_lever_to_brain(brain, kr_dyn_rs_min=7.5)
        assert (
            patched["LIVE_CLUSTER_TEMPLATES"]["CLUSTER_1_강응축_폭발형_260628"][
                "_bull_recency_01_kr_dyn_rs_min"
            ]
            == 7.5
        )
        assert "_bull_recency_01_kr_dyn_rs_min" not in patched["LIVE_CLUSTER_TEMPLATES"][
            "CLUSTER_1_강응축_폭발형_260802"
        ]
        assert audit["enabled"] is True
