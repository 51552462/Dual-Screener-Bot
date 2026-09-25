"""US-COSINE-AXIS-01 live helpers — KV roundtrip, z on RANK 3D only."""
from __future__ import annotations

import numpy as np

from cosine_axis_stats import (
    COSINE_AXIS_STATS_KEY,
    MIN_HIST_N,
    append_market_day,
    apply_z,
    best_rank3_cosine,
    empty_market_slot,
    fit_mu_sd,
    hist_rows_from_slot,
    percentile_p90,
    stats_ready,
    z_templates_3d,
    _norm_blob,
)


def test_fit_and_same_z_on_template():
    rows = np.array([[0.5, 8.0, 16.0], [0.7, 12.0, 24.0]], dtype=float)
    mu, sd = fit_mu_sd(rows)
    tpl = np.array([0.6, 10.0, 20.0])
    z_n = apply_z(rows[0], mu, sd)
    z_t = apply_z(tpl, mu, sd)
    assert z_n.shape == (3,)
    assert z_t.shape == (3,)


def test_p90_needs_ten_scores():
    assert percentile_p90([0.1] * 9) is None
    p90 = percentile_p90(list(np.linspace(0.0, 1.0, 20)))
    assert p90 is not None
    assert 0.7 < p90 < 1.0


def test_append_ring_caps_window():
    blob = _norm_blob({})
    for i in range(7):
        blob = append_market_day(
            blob,
            market="US",
            as_of=f"2026-09-{10+i:02d}",
            rows=[[0.6, 10.0, 20.0]] * 10,
            p90_z=0.9,
        )
    assert len(blob["US"]["days"]) == 5
    assert blob["KR"]["n"] == 0
    assert blob["US"]["n"] == 50
    assert stats_ready(blob["US"]) is True


def test_z_templates_skip_multi_24d():
    mu = np.zeros(3)
    sd = np.ones(3)
    tpls = {
        "US_RANK_A_장기매집": np.array([0.7, 10.5, 25.0]),
        "MULTI": np.zeros(24),
    }
    z = z_templates_3d(tpls, mu, sd)
    assert "US_RANK_A_장기매집" in z
    assert "MULTI" not in z
    sim = best_rank3_cosine(np.array([0.7, 10.5, 25.0]), z)
    assert sim > 0.99


def test_key_constant():
    assert COSINE_AXIS_STATS_KEY == "COSINE_AXIS_STATS"
    assert MIN_HIST_N == 30
    assert empty_market_slot()["window_sessions"] == 5
    assert hist_rows_from_slot(empty_market_slot()).shape[0] == 0
