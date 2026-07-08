"""Regime Specialization Item 3 — regime_tag MAB/Kelly quarantine tests."""
from __future__ import annotations

from evolution.regime_logic_crossmatrix import (
    apply_regime_tag_quarantine_to_kelly,
    evaluate_regime_tag_quarantine,
    lookup_regime_tag_from_incubator_template,
    regime_tag_compatible,
    regime_tag_mab_group_mult,
    regime_tag_quarantine_kelly_mult,
    resolve_regime_tag_for_signal,
)


def test_bull_only_compatible_in_bull():
    assert regime_tag_compatible("BULL", "BULL_ONLY") is True
    ev = evaluate_regime_tag_quarantine("BULL", "BULL_ONLY")
    assert ev["quarantined"] is False
    assert ev["kelly_mult"] == 1.0


def test_bull_only_quarantined_in_bear():
    assert regime_tag_compatible("BEAR", "BULL_ONLY") is False
    ev = evaluate_regime_tag_quarantine("BEAR", "BULL_ONLY")
    assert ev["quarantined"] is True
    assert ev["kelly_mult"] == 0.0
    assert ev["reject_entry"] is False


def test_bear_only_allowed_in_high_vol():
    assert regime_tag_compatible("HIGH_VOL", "BEAR_ONLY") is True


def test_bear_only_quarantined_in_bull():
    ev = evaluate_regime_tag_quarantine("BULL", "BEAR_ONLY")
    assert ev["quarantined"] is True


def test_all_weather_never_quarantined():
    for meta in ("BULL", "BEAR", "SIDEWAYS", "HIGH_VOL"):
        assert regime_tag_compatible(meta, "ALL_WEATHER") is True


def test_apply_kelly_quarantine_zeros():
    out = apply_regime_tag_quarantine_to_kelly("BEAR", "BULL_ONLY", 0.025)
    assert out["kelly_risk_pct"] == 0.0
    assert out["quarantined"] is True


def test_reject_mode():
    cfg = {"REGIME_TAG_QUARANTINE_MODE": "reject"}
    ev = evaluate_regime_tag_quarantine("BEAR", "BULL_ONLY", sys_config=cfg)
    assert ev["reject_entry"] is True


def test_mab_group_mult_overlay():
    mult, ev = regime_tag_mab_group_mult("BEAR", "BULL_ONLY", 1.35)
    assert mult == 0.0
    assert ev["quarantined"] is True


def test_lookup_from_incubator_template():
    cfg = {
        "INCUBATOR_TEMPLATES": {
            "GP_MUT_alpha": {"regime_tag": "BULL_ONLY", "mutant_oos_expr": "x>y"},
        }
    }
    assert lookup_regime_tag_from_incubator_template(cfg, "GP_MUT_alpha") == "BULL_ONLY"
    assert lookup_regime_tag_from_incubator_template(cfg, "alpha") == "BULL_ONLY"


def test_resolve_from_sig_type():
    cfg = {
        "INCUBATOR_TEMPLATES": {
            "GP_MUT_beta": {"regime_tag": "BEAR_ONLY"},
        }
    }
    tag = resolve_regime_tag_for_signal(
        cfg,
        sig_type="[INCUBATOR_GP_MUT_beta] RANK_A",
        incubator_template_name="GP_MUT_beta",
    )
    assert tag == "BEAR_ONLY"


def test_resolve_from_registry():
    meta = {
        "META_STRATEGY_REGISTRY": [
            {"group_key": "INCUBATOR_gamma", "regime_tag": "ALL_WEATHER"},
        ]
    }
    tag = resolve_regime_tag_for_signal({}, group_key="INCUBATOR_gamma", meta_state=meta)
    assert tag == "ALL_WEATHER"
