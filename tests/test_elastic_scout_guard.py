"""Elastic Scout Guard — conditional live cross-validation tests."""
from elastic_scout_guard import (
    SCOUT_SHADOW_BLOCK_REASON,
    SCOUT_SHADOW_STRATEGY_ID,
    build_scout_shadow_observe_facts,
    count_scout_cross_validation_weapons,
    evaluate_scout_conditional_live,
    format_scout_bear_cv_live_tag,
    format_scout_bear_shadow_sig_type,
    route_fluid_scout_bear_shadow,
)


def test_no_weapons():
    n, w = count_scout_cross_validation_weapons()
    assert n == 0
    assert w == ()


def test_flow_bonus_weapon():
    n, w = count_scout_cross_validation_weapons(flow_bonus=2.5)
    assert n == 1
    assert w == ("flow",)


def test_flow_divergence_weapon():
    n, w = count_scout_cross_validation_weapons(flow_divergence=0.45)
    assert n == 1
    assert w == ("flow_div",)


def test_short_fund_dart_weapons():
    n, w = count_scout_cross_validation_weapons(
        short_net=1.2, fund_net=0.8, dart_net=1.0
    )
    assert n == 3
    assert set(w) == {"short_sq", "fund", "dart"}


def test_smart_money_tag_weapon():
    n, w = count_scout_cross_validation_weapons(
        sig_type="SUPERNOVA [🕵️세력매집_교차검증]"
    )
    assert n == 1
    assert w == ("smart_money",)


def test_bull_regime_skips_gate():
    v = evaluate_scout_conditional_live(
        regime_key="BULL",
        fluid_scout=True,
        flow_bonus=0.0,
    )
    assert not v.needs_gate
    assert v.live_allowed


def test_bear_scout_no_cv_rejected():
    v = evaluate_scout_conditional_live(
        regime_key="BEAR",
        fluid_scout=True,
    )
    assert v.needs_gate
    assert not v.live_allowed
    assert v.weapon_count == 0


def test_high_vol_scout_with_cv_allowed():
    v = evaluate_scout_conditional_live(
        regime_key="HIGH_VOL",
        fluid_scout=True,
        flow_bonus=1.5,
        dart_net=0.5,
    )
    assert v.needs_gate
    assert v.live_allowed
    assert v.weapon_count == 2
    assert set(v.weapons) == {"flow", "dart"}


def test_non_scout_never_gated():
    v = evaluate_scout_conditional_live(regime_key="BEAR", fluid_scout=False)
    assert not v.needs_gate
    assert v.live_allowed


def test_format_live_tag():
    assert " #ScoutBearCVLive(flow+short_sq)" == format_scout_bear_cv_live_tag(
        ("flow", "short_sq")
    )


def test_format_shadow_sig_type():
    sig = format_scout_bear_shadow_sig_type("SUPERNOVA RANK_A", "BEAR")
    assert "[🔭SCOUT]" in sig
    assert "#ScoutBearShadow(BEAR)" in sig


def test_build_shadow_observe_facts():
    facts = build_scout_shadow_observe_facts(
        {"dyn_cpv": 1.2, "v_energy": 3.0},
        regime_key="HIGH_VOL",
        flow_bonus=0.0,
        short_net=-0.5,
    )
    assert facts["entry_regime"] == "HIGH_VOL"
    assert facts["_fluid_scout"] is True
    assert facts["_scout_shadow_routed"] is True
    assert facts["short_net"] == -0.5
    assert facts["dyn_cpv"] == 1.2


def test_route_shadow_observe_success(monkeypatch):
    calls = {"observe": 0, "blocked": 0, "virtual": 0}

    def _fake_observe(**kwargs):
        calls["observe"] += 1
        assert kwargs["strategy_id"] == SCOUT_SHADOW_STRATEGY_ID
        assert kwargs["facts"]["_scout_shadow_routed"] is True
        assert "#ScoutBearShadow(BEAR)" in kwargs["sig_type"]
        return True, "관측 장부 등재: TEST"

    def _fake_blocked(code, name, reason, ep, **kwargs):
        calls["blocked"] += 1
        assert reason == SCOUT_SHADOW_BLOCK_REASON

    def _fake_virtual(cur, market, code, name, ep, sig, tags, logged_at):
        calls["virtual"] += 1

    monkeypatch.setattr(
        "forward_observe_bridge.try_add_observe_forward_trade", _fake_observe
    )
    monkeypatch.setattr("shadow_tracking.record_blocked_trade", _fake_blocked)
    monkeypatch.setattr("shadow_tracking.insert_virtual_trade_row", _fake_virtual)
    monkeypatch.setattr("shadow_tracking.init_shadow_tables", lambda cur: None)
    monkeypatch.setattr("shadow_tracking.DB_PATH", ":memory:")

    ok, msg = route_fluid_scout_bear_shadow(
        market="KR",
        code="005930",
        name="삼성전자",
        sig_type="SUPERNOVA",
        score=72.0,
        ep=70000.0,
        regime_key="BEAR",
        facts={"dyn_cpv": 0.9},
    )
    assert ok is True
    assert "OBSERVE_ONLY" in msg
    assert calls["observe"] == 1
    assert calls["blocked"] == 1
    assert calls["virtual"] == 1


def test_route_shadow_observe_failure(monkeypatch):
    monkeypatch.setattr(
        "forward_observe_bridge.try_add_observe_forward_trade",
        lambda **kwargs: (False, "중복 OPEN 관측"),
    )
    monkeypatch.setattr("shadow_tracking.record_blocked_trade", lambda *a, **k: True)
    monkeypatch.setattr("shadow_tracking.insert_virtual_trade_row", lambda *a, **k: None)
    monkeypatch.setattr("shadow_tracking.init_shadow_tables", lambda cur: None)
    monkeypatch.setattr("shadow_tracking.DB_PATH", ":memory:")

    ok, msg = route_fluid_scout_bear_shadow(
        market="KR",
        code="000660",
        name="SK하이닉스",
        sig_type="SUPERNOVA",
        score=70.0,
        ep=120000.0,
        regime_key="BEAR",
    )
    assert ok is False
    assert "OBSERVE 실패" in msg
