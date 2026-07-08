"""mutant_oos_validator — Regime Specialization (Item 1 Hard Block + Item 2 Tagging) tests."""
from __future__ import annotations

import numpy as np
import pandas as pd

from mutant_oos_validator import (
    REGIME_HARD_BLOCK_ENABLED,
    apply_regime_hard_block,
    attach_regime_tag_fields,
    classify_regime_specialization_tag,
    evaluate_synthetic_regime_stress_audit,
    legacy_regime_hard_block_reason,
    normalize_regime_tag,
    regime_hard_block_enabled,
    resolve_oos_promotion_pass,
)


def _audit(
    *,
    bull_excess=0.0,
    bull_wr=0.5,
    bull_n=20,
    bear_excess=0.0,
    bear_wr=0.5,
    bear_n=20,
    bear_avg=0.0,
    side_excess=0.0,
    side_wr=0.5,
    side_n=20,
):
    return {
        "BULL": {
            "n_signals": bull_n,
            "excess_alpha": bull_excess,
            "win_rate": bull_wr,
            "avg_return": bull_excess,
            "mdd_pct": -5.0,
            "audit_only": True,
        },
        "BEAR": {
            "n_signals": bear_n,
            "excess_alpha": bear_excess,
            "win_rate": bear_wr,
            "avg_return": bear_avg,
            "mdd_pct": -12.0,
            "audit_only": True,
        },
        "SIDEWAYS": {
            "n_signals": side_n,
            "excess_alpha": side_excess,
            "win_rate": side_wr,
            "avg_return": side_excess,
            "mdd_pct": -3.0,
            "audit_only": True,
        },
    }


def test_hard_block_ssot_disabled():
    assert REGIME_HARD_BLOCK_ENABLED is False
    assert regime_hard_block_enabled() is False


def test_apply_regime_hard_block_never_blocks():
    bad_audit = {
        "BEAR": {
            "n_signals": 100,
            "excess_alpha": -0.05,
            "mdd_pct": -40.0,
            "audit_only": True,
        },
        "_legacy_would_block": "legacy_bear_excess_alpha_fail",
    }
    blocked, reason = apply_regime_hard_block(bad_audit)
    assert blocked is False
    assert reason == ""


def test_legacy_reason_reported_but_not_used_for_pass():
    audit = {
        "BEAR": {"n_signals": 50, "excess_alpha": -0.02, "mdd_pct": -30.0},
    }
    legacy = legacy_regime_hard_block_reason(audit)
    assert legacy == "legacy_bear_excess_alpha_fail"

    passed, reason = resolve_oos_promotion_pass(
        excess_alpha=0.001,
        oos_wr=0.62,
        n_sig=40,
        stress_audit=audit,
    )
    assert passed is True
    assert reason == ""


def test_real_panel_pass_unaffected_by_bear_weakness():
    audit = _audit(
        bull_excess=0.003,
        bull_wr=0.58,
        bear_excess=-0.015,
        bear_wr=0.42,
        bear_n=60,
    )
    passed, _ = resolve_oos_promotion_pass(
        excess_alpha=0.0002,
        oos_wr=0.55,
        n_sig=35,
        stress_audit=audit,
    )
    assert passed is True
    assert legacy_regime_hard_block_reason(audit) != ""


def test_real_panel_threshold_still_applies():
    audit = {"BEAR": {"n_signals": 0}}
    passed, reason = resolve_oos_promotion_pass(
        excess_alpha=0.00001,
        oos_wr=0.40,
        n_sig=35,
        stress_audit=audit,
    )
    assert passed is False
    assert reason == "real_panel_threshold"


def test_tag_bull_only():
    audit = _audit(
        bull_excess=0.0004,
        bull_wr=0.55,
        bear_excess=-0.0002,
        bear_wr=0.45,
    )
    tag, meta = classify_regime_specialization_tag(audit)
    assert tag == "BULL_ONLY"
    assert meta["reason"] == "bull_strong_bear_not"


def test_tag_bear_only():
    audit = _audit(
        bull_excess=-0.0002,
        bull_wr=0.46,
        bear_excess=0.0003,
        bear_wr=0.54,
        bear_avg=0.0002,
    )
    tag, meta = classify_regime_specialization_tag(audit)
    assert tag == "BEAR_ONLY"
    assert meta["reason"] == "bear_strong_bull_not"


def test_tag_all_weather():
    audit = _audit(
        bull_excess=0.0004,
        bull_wr=0.56,
        bear_excess=0.00025,
        bear_wr=0.53,
        bear_avg=0.00015,
    )
    tag, _ = classify_regime_specialization_tag(audit)
    assert tag == "ALL_WEATHER"


def test_tag_unclassified_insufficient_samples():
    audit = _audit(bull_n=5, bear_n=3, side_n=2)
    tag, meta = classify_regime_specialization_tag(audit)
    assert tag == "UNCLASSIFIED"
    assert meta["bull_profile"] == "insufficient"


def test_attach_regime_tag_fields_on_promoted_row():
    audit = _audit(bull_excess=0.0004, bull_wr=0.55, bear_excess=-0.0002, bear_wr=0.45)
    tag, meta = classify_regime_specialization_tag(audit)
    audit["_regime_tag"] = tag
    audit["_regime_tag_meta"] = meta
    rec: dict = {"name": "m1", "expr": "close > ma20"}
    attach_regime_tag_fields(rec, audit)
    assert rec["regime_tag"] == "BULL_ONLY"
    assert rec["regime_tag_meta"]["tag"] == "BULL_ONLY"


def test_normalize_regime_tag():
    assert normalize_regime_tag("bull_only") == "BULL_ONLY"
    assert normalize_regime_tag("INVALID") == "UNCLASSIFIED"


def _mk_synthetic_frames(n: int = 60) -> dict[str, dict[str, pd.DataFrame]]:
    dates = pd.date_range("2024-01-01", periods=n, freq="B")
    bull_close = 100 * np.cumprod(1 + np.random.default_rng(1).normal(0.002, 0.01, n))
    bear_close = 100 * np.cumprod(1 + np.random.default_rng(2).normal(-0.002, 0.015, n))

    def _df(close: np.ndarray) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Date": dates,
                "Open": close * 0.99,
                "High": close * 1.01,
                "Low": close * 0.98,
                "Close": close,
                "Volume": np.full(n, 1_000_000.0),
            }
        )

    return {
        "BULL": {"SYN_001": _df(bull_close)},
        "BEAR": {"SYN_002": _df(bear_close)},
        "SIDEWAYS": {"SYN_003": _df(np.full(n, 100.0))},
    }


def test_synthetic_stress_audit_includes_regime_tag():
    expr = "close > ma20"
    audit = evaluate_synthetic_regime_stress_audit(expr, regime_frames=_mk_synthetic_frames())
    assert audit["_hard_block_disabled"] is True
    assert audit["_regime_tag"] in ("BULL_ONLY", "BEAR_ONLY", "ALL_WEATHER", "UNCLASSIFIED")
    assert isinstance(audit["_regime_tag_meta"], dict)
    for bucket in ("BULL", "BEAR", "SIDEWAYS"):
        assert audit[bucket].get("audit_only") is True
