"""Institutional Regime Analog Engine — 마할라노비스+DTW 유사도, 선취매 게이트, 타임머신 모핑."""
import numpy as np

import regime_analog_engine as rae


# ---------------------------------------------------------------------------
# 거리 함수
# ---------------------------------------------------------------------------
def test_mahalanobis_identity_equals_euclidean():
    x = np.array([0.0, 0.0])
    mu = np.array([3.0, 4.0])
    inv = np.eye(2)
    assert abs(rae.mahalanobis_distance(x, mu, inv) - 5.0) < 1e-9


def test_dtw_identical_is_zero():
    a = np.array([1.0, 2.0, 3.0, 4.0])
    assert rae.dtw_distance(a, a) < 1e-9


def test_dtw_v_recovery_closer_to_v_than_crash():
    cur = np.array([-2.0, -1.5, -0.5, 0.5, 1.5, 2.0])  # V자 형상
    v_traj = np.array(rae.HISTORICAL_EPISODES["V_RECOVERY"]["trajectory"])
    crash_traj = np.array(rae.HISTORICAL_EPISODES["EXTREME_CRASH"]["trajectory"])
    assert rae.dtw_distance(cur, v_traj) < rae.dtw_distance(cur, crash_traj)


# ---------------------------------------------------------------------------
# 국면 벡터 구성
# ---------------------------------------------------------------------------
def _cfg_with_indices(close_ma_dist, vix, range5d):
    ma20 = 100.0
    close = ma20 * (1.0 + close_ma_dist)
    return {
        "REGIME_ANALYSIS": {
            "vix_close": vix,
            "indices": {
                "GSPC": {"ok": True, "close": close, "ma20": ma20, "range5d_pct": range5d},
                "KOSPI": {"ok": True, "close": close, "ma20": ma20, "range5d_pct": range5d},
            },
        }
    }


def test_build_vector_dims_and_values():
    cfg = _cfg_with_indices(-0.12, 38.0, 8.75)
    built = rae.build_current_regime_vector(cfg, pri_blend_z=-1.5, macro_z=-1.4)
    vec = built["vector"]
    assert len(vec) == rae.N_DIMS
    vm = built["vector_map"]
    assert abs(vm["spx_ma20_dist"] - (-0.12)) < 1e-6
    assert abs(vm["vix_z"] - 1.8) < 1e-6
    assert abs(vm["range5d_norm"] - 1.5) < 1e-6
    assert vm["pri_z"] == -1.5
    assert vm["macro_z"] == -1.4
    assert built["data_completeness"] == 1.0


# ---------------------------------------------------------------------------
# 앙상블 유사도 산출
# ---------------------------------------------------------------------------
def test_compute_analog_picks_crash_for_crash_vector():
    cfg = _cfg_with_indices(-0.12, 38.0, 8.75)
    out = rae.compute_regime_analog(cfg, persist=False, pri_blend_z=-1.5, macro_z=-1.4)
    assert out["best_episode"] == "EXTREME_CRASH"
    assert out["front_run_favorable"] is False
    assert 0.0 <= out["score"] <= 1.0


def test_compute_analog_picks_bull_for_bull_vector():
    cfg = _cfg_with_indices(0.05, 14.0, 3.85)
    out = rae.compute_regime_analog(cfg, persist=False, pri_blend_z=1.2, macro_z=1.3)
    assert out["best_episode"] == "MASSIVE_BULL"
    assert out["front_run_favorable"] is True
    assert out["score"] > 0.9  # 센트로이드와 거의 일치


def test_compute_analog_structure():
    cfg = _cfg_with_indices(0.0, 20.0, 3.5)
    out = rae.compute_regime_analog(cfg, persist=False, pri_blend_z=0.0, macro_z=0.0)
    assert set(out["per_episode"].keys()) == set(rae.HISTORICAL_EPISODES.keys())
    assert out["covariance_mode"] == "euclidean_fallback"  # 히스토리 없음
    for ep in out["per_episode"].values():
        assert 0.0 <= ep["ensemble_sim"] <= 1.0


# ---------------------------------------------------------------------------
# Mission 2: 선취매 게이트
# ---------------------------------------------------------------------------
def test_gate_disabled_allows():
    allowed, info = rae.frontrun_gate({"REGIME_ANALOG_GATE_ENABLED": False})
    assert allowed is True
    assert info["reason"] == "gate_disabled"


def test_gate_no_data_fail_open_default():
    allowed, info = rae.frontrun_gate({})
    assert allowed is True
    assert info["reason"] == "no_analog_data_fail_open"


def test_gate_no_data_fail_closed():
    allowed, info = rae.frontrun_gate({"REGIME_ANALOG_GATE_FAIL_OPEN": False})
    assert allowed is False
    assert info["reason"] == "no_analog_data_blocked"


def test_gate_favorable_high_score_allows():
    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.91,
            "front_run_favorable": True,
            "best_episode": "V_RECOVERY",
            "best_regime": "UP",
        }
    }
    allowed, info = rae.frontrun_gate(cfg)
    assert allowed is True
    assert info["reason"] == "analog_match"


def test_gate_low_score_blocks():
    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.50,
            "front_run_favorable": True,
            "best_episode": "V_RECOVERY",
        }
    }
    allowed, info = rae.frontrun_gate(cfg)
    assert allowed is False
    assert info["reason"] == "low_analog_score"


def test_gate_unfavorable_blocks():
    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.99,
            "front_run_favorable": False,
            "best_episode": "EXTREME_CRASH",
        }
    }
    allowed, info = rae.frontrun_gate(cfg)
    assert allowed is False
    assert info["reason"] == "unfavorable_regime"


# ---------------------------------------------------------------------------
# Mission 3: 타임머신 모핑 타깃
# ---------------------------------------------------------------------------
def test_morph_target_returns_archetype_on_high_favorable():
    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.90,
            "front_run_favorable": True,
            "best_episode": "V_RECOVERY",
            "best_regime": "UP",
        }
    }
    tgt = rae.resolve_morph_target_dna(cfg, "KR")
    assert tgt is not None
    assert tgt["episode"] == "V_RECOVERY"
    assert len(tgt["dna"]) == 3


def test_morph_target_none_on_low_score():
    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.50,
            "front_run_favorable": True,
            "best_episode": "V_RECOVERY",
        }
    }
    assert rae.resolve_morph_target_dna(cfg, "KR") is None


def test_morph_target_none_on_unfavorable():
    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.99,
            "front_run_favorable": False,
            "best_episode": "EXTREME_CRASH",
        }
    }
    assert rae.resolve_morph_target_dna(cfg, "US") is None


# ---------------------------------------------------------------------------
# Mission 3 통합: template_evolution.morph_templates 가 타임머신 타깃으로 전환
# ---------------------------------------------------------------------------
def test_morph_templates_switches_to_timemachine_target():
    import template_evolution as te

    cfg = {
        "REGIME_ANALOG_SCORE": {
            "score": 0.92,
            "front_run_favorable": True,
            "best_episode": "V_RECOVERY",
            "best_regime": "UP",
        },
        "REGIME_ANALOG_MORPH_ALPHA": 0.3,
    }
    logs = te.morph_templates(cfg, None, "KR")
    assert logs, "타임머신 모핑 로그가 있어야 한다"
    assert "🕰️" in logs[0]
    # 베이스 템플릿 cpv 가 V_RECOVERY 아카이브 DNA(0.72) 쪽으로 이동해야 한다.
    base = cfg["DNA_BASE_TEMPLATES"]["KR"]
    seed_cpv = te.DEFAULT_BASE_TEMPLATES["KR"]["RANK_A_장기매집"][0]
    moved_cpv = base["RANK_A_장기매집"][0]
    assert abs(moved_cpv - 0.72) < abs(seed_cpv - 0.72)
