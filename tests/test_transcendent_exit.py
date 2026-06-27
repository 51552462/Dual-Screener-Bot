"""초월적 비대칭 청산 4미션 단위테스트 (순수 수식·RL·블렌딩)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import exit_dynamics as xd
import exit_ratchet_rl as rl


# ---- M1: 유동 부분익절 -----------------------------------------------------
def test_m1_defensive_sells_more():
    f_bear = xd.fluid_scale_out_fraction("BEAR", volatility_pct=8.0, edge_score=1.0)
    f_bull = xd.fluid_scale_out_fraction("BULL", volatility_pct=8.0, edge_score=1.0)
    assert f_bear >= 0.70, f_bear           # 방어국면 70%+ 매도
    assert f_bull <= 0.25, f_bull           # 상승국면 소량 매도
    assert f_bear > f_bull
    print("[M1] F_out bear", round(f_bear, 3), "bull", round(f_bull, 3))


def test_m1_high_edge_keeps_runner():
    f_lo = xd.fluid_scale_out_fraction("BULL", 5.0, edge_score=1.0)
    f_hi = xd.fluid_scale_out_fraction("BULL", 5.0, edge_score=3.0)
    assert f_hi <= f_lo                      # 엣지 강할수록 덜 판다(러너 보존)
    assert 0.10 <= f_hi <= 0.20
    print("[M1] edge low", round(f_lo, 3), "high", round(f_hi, 3))


def test_m1_blend_asymmetric():
    # F_out=0.2 @ TP=10 → partial=2.0; 러너 80%가 +50% → 2.0 + 0.8*50 = 42.0
    blended = xd.blend_final_return(realized_partial_ret=2.0, scaled_out_frac=0.2, runner_ret_pct=50.0)
    assert abs(blended - 42.0) < 1e-6
    print("[M1] blended", blended)


# ---- M2: 볼록 래칫 ---------------------------------------------------------
def test_m2_kappa_tightens_with_profit():
    st = dict(xd.DEFAULT_RATCHET_STATE)
    k_early = xd.convex_ratchet_kappa(2.0, st)
    k_late = xd.convex_ratchet_kappa(40.0, st)
    assert k_early > k_late                  # 초반 넓게 → 팽창 후 조임
    assert xd.trail_stop_price(100.0, 0.1) == 90.0
    print("[M2] kappa early", round(k_early, 4), "late", round(k_late, 4))


def test_m2_rl_widens_on_whipsaw():
    st = dict(xd.DEFAULT_RATCHET_STATE)
    # 조기청산 과다 → κ 확대 + 볼록화
    new = xd.update_ratchet_kappa_rl(st, whipsaw_rate=0.8, giveback_rate=0.1)
    assert new["kappa_max"] > st["kappa_max"]
    assert new["convexity"] > st["convexity"]
    # 이익반납 과다 → κ 축소
    new2 = xd.update_ratchet_kappa_rl(st, whipsaw_rate=0.1, giveback_rate=0.8)
    assert new2["kappa_max"] < st["kappa_max"]
    print("[M2] rl whipsaw->", new["kappa_max"], "giveback->", new2["kappa_max"])


def test_m2_runner_rates():
    rows = [
        (50.0, 42.0, "RUNNER_TRAIL", 10),   # giveback 16%
        (8.0, 2.0, "RUNNER_TRAIL", 3),      # 저 mfe 트레일 → whipsaw 후보
        (60.0, 55.0, "RUNNER_TRAIL", 12),
        (5.0, 1.0, "RUNNER_TRAIL", 2),      # whipsaw 후보
    ]
    out = rl.compute_runner_rates(rows)
    assert out["n"] == 4
    assert 0.0 <= out["whipsaw_rate"] <= 1.0
    assert 0.0 <= out["giveback_rate"] <= 1.0
    print("[M2] rates", out)


# ---- M3: 우측꼬리 메타튜닝 -------------------------------------------------
def test_m3_percentile_meta():
    assert xd.target_percentile("BULL", 2.0) >= 88        # 상승장 90 근방
    assert xd.target_percentile("CHOP", 2.0) == 70        # 평장 70
    assert xd.target_percentile("BEAR", 2.0) == 60        # 방어 60
    # fat-tail 클수록 상향
    assert xd.target_percentile("CHOP", 5.0) > xd.target_percentile("CHOP", 2.0)
    assert xd.target_percentile("BULL", 10.0) <= 95
    print("[M3] bull", xd.target_percentile("BULL", 2.0), "chop", xd.target_percentile("CHOP", 2.0))


def test_m3_no_median_anchor():
    # 50퍼센타일 고정이 아님을 보장(평장에서도 70p)
    assert xd.target_percentile("CHOP", 1.0) != 50
    print("[M3] median anchor removed OK")


# ---- M4: 자가 증식 피라미딩 -----------------------------------------------
def test_m4_pyramid_fires_on_edge():
    d = xd.pyramid_decision(edge_score=2.5, regime="BULL", idle_cash=1_000_000, nav=10_000_000,
                            pyramid_adds_done=0, free_runner=True)
    assert d["do"] is True
    assert 0 < d["add_notional"] <= 10_000_000 * xd.PYRAMID_NAV_CAP_FRAC
    print("[M4] pyramid", d)


def test_m4_pyramid_blocked_cases():
    assert not xd.pyramid_decision(edge_score=2.5, regime="BEAR", idle_cash=1e6, nav=1e7,
                                   pyramid_adds_done=0, free_runner=True)["do"]
    assert not xd.pyramid_decision(edge_score=1.0, regime="BULL", idle_cash=1e6, nav=1e7,
                                   pyramid_adds_done=0, free_runner=True)["do"]
    assert not xd.pyramid_decision(edge_score=2.5, regime="BULL", idle_cash=1e6, nav=1e7,
                                   pyramid_adds_done=3, free_runner=True)["do"]
    assert not xd.pyramid_decision(edge_score=2.5, regime="BULL", idle_cash=0, nav=1e7,
                                   pyramid_adds_done=0, free_runner=True)["do"]
    assert not xd.pyramid_decision(edge_score=2.5, regime="BULL", idle_cash=1e6, nav=1e7,
                                   pyramid_adds_done=0, free_runner=False)["do"]
    print("[M4] block cases OK")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
    print(f"\n✅ ALL {len(fns)} EXIT TESTS PASSED")
