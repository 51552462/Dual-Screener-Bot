"""초월적 진화 4미션 단위테스트 (DB/네트워크 비의존 — 주입형)."""
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import template_bandit as tb
import template_evolution as te


def _mk_df(rows):
    return pd.DataFrame(rows)


def _winner(market, sig, ret, cpv, tbv, bbe, exit_date="2026-06-25"):
    return {
        "market": market, "sig_type": sig, "status": "CLOSED_WIN",
        "final_ret": ret, "dyn_cpv": cpv, "dyn_tb": tbv, "v_energy": bbe,
        "mfe": ret, "exit_date": exit_date, "entry_date": exit_date,
    }


def test_m1_morphing_moves_toward_real():
    cfg = {}
    base0 = list(te.seed_base_templates(cfg, "KR")["RANK_A_장기매집"])  # copy [0.75,11.8,27.15]
    df = _mk_df([
        _winner("KR", "[SUPERNOVA] RANK_A_장기매집", 12.0, 1.75, 21.8, 37.15),
        _winner("KR", "[SUPERNOVA] RANK_A_장기매집", 8.0, 1.75, 21.8, 37.15),
        _winner("KR", "[SUPERNOVA] RANK_A_장기매집", 9.0, 1.75, 21.8, 37.15),
    ])
    logs = te.morph_templates(cfg, df, "KR")
    new = cfg["DNA_BASE_TEMPLATES"]["KR"]["RANK_A_장기매집"]
    assert logs, "모핑 로그가 있어야 함"
    # EMA α=0.2 → 0.75*0.8 + 1.75*0.2 = 0.95
    assert abs(new[0] - (base0[0] * 0.8 + 1.75 * 0.2)) < 1e-6
    assert new[0] > base0[0] and new[2] > base0[2]
    print("[M1] morph", base0, "->", new)


def test_m1_min_n_guard():
    cfg = {}
    df = _mk_df([_winner("KR", "[SUPERNOVA] RANK_B_중기스윙", 12.0, 9.9, 99.0, 99.0)])
    logs = te.morph_templates(cfg, df, "KR")  # n=1 < min_n=3
    assert not logs
    assert cfg["DNA_BASE_TEMPLATES"]["KR"]["RANK_B_중기스윙"] == [0.75, 10.0, 27.35]
    print("[M1] min_n guard OK")


def test_m2_graduation_when_shadow_beats_live():
    cfg = {}
    # 실전 14일: 패배 위주(저승률), 표본 12
    rows = []
    for i in range(12):
        ret = 5.0 if i < 3 else -4.0  # WR 25%
        rows.append(_winner("KR", "[SUPERNOVA] RANK_C_단기테마", ret, 0.6, 8.0, 20.0) if ret > 0
                    else {**_winner("KR", "[SUPERNOVA] RANK_C_단기테마", ret, 0.6, 8.0, 20.0),
                          "status": "CLOSED_LOSS"})
    df = _mk_df(rows)
    v = te.maybe_graduate_forensics(cfg, df, "KR", shadow_wins=11, shadow_losses=1)
    assert v["graduated"] is True, v
    assert te.GRADUATED_TEMPLATE_NAME in cfg["DNA_SUPERNOVA_KR_MULTI"]
    assert tb.BANDIT_KEY in cfg and te.GRADUATED_TEMPLATE_NAME in cfg[tb.BANDIT_KEY]
    print("[M2] graduated:", v["shadow_wilson_lb"], ">", v["live_wr"])


def test_m2_no_graduation_when_no_edge():
    cfg = {}
    rows = [_winner("KR", "[SUPERNOVA] RANK_C_단기테마", 5.0, 0.6, 8.0, 20.0) for _ in range(12)]
    df = _mk_df(rows)  # 실전 WR 100%
    v = te.maybe_graduate_forensics(cfg, df, "KR", shadow_wins=6, shadow_losses=4)
    assert v["graduated"] is False
    assert v["reason"] == "no_statistical_edge"
    print("[M2] no-edge hold OK")


def test_m2_insufficient_sample():
    cfg = {}
    df = _mk_df([_winner("KR", "[SUPERNOVA] RANK_C_단기테마", 5.0, 0.6, 8.0, 20.0)])
    v = te.maybe_graduate_forensics(cfg, df, "KR", shadow_wins=2, shadow_losses=0)
    assert v["graduated"] is False and v["reason"] == "insufficient_sample"
    print("[M2] insufficient sample OK")


def test_m3_bandit_valve():
    cfg = {}
    rec = tb.init_bandit(cfg, te.GRADUATED_TEMPLATE_NAME, shadow_wins=8, shadow_losses=2)
    assert tb.MULT_MIN <= rec["mult"] <= tb.MULT_MAX
    sig = f"[SUPERNOVA] {te.GRADUATED_TEMPLATE_NAME} [forensics]"
    base_mult = tb.resolve_template_multiplier(cfg, sig)
    assert base_mult == rec["mult"]
    # 연승 → 배수 상승
    for _ in range(20):
        tb.update_bandit(cfg, sig, won=True)
    hi = tb.resolve_template_multiplier(cfg, sig)
    # 연패 → 배수 하락
    for _ in range(60):
        tb.update_bandit(cfg, sig, won=False)
    lo = tb.resolve_template_multiplier(cfg, sig)
    assert hi > lo, (hi, lo)
    assert tb.resolve_template_multiplier(cfg, "[SUPERNOVA] RANK_A_장기매집") == 1.0
    print("[M3] valve hi", hi, "lo", lo)


def test_m3_shadow_eval_injected():
    # forensics 섀도우 채점 — 가격조회 주입(네트워크/DB 없이 검증은 함수 시그니처/로직만)
    wins, losses = te.evaluate_forensics_shadow(
        "KR", price_fetcher=lambda c, m: 100.0, db_path="___nonexistent___.db"
    )
    assert (wins, losses) == (0, 0)  # DB 없음 → 안전 0
    print("[M3] shadow eval safe-zero OK")


def test_m4_no_treasury_mutation_in_culling():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "system_auto_pilot.py"), encoding="utf-8") as f:
        src = f.read()
    assert "current_config[treasury_key] = current_treasury" not in src
    assert "current_treasury += final_balance" not in src
    print("[M4] legacy treasury mutation removed OK")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
    print(f"\n✅ ALL {len(fns)} TESTS PASSED")
