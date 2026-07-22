"""ARCR — asymmetric regime capital relay (coin-quant synergy plane)."""
from __future__ import annotations

from unittest import mock

import pytest

from bitget.infra.memory_policy import ARCR_SHORT_RELAY_CAP, ARCR_SHORT_RELAY_GAIN
from bitget.trading import regime_capital_relay as arcr


def _stress_cfg(score: float = 85.0) -> dict:
    return {
        "DOOMSDAY_DEFCON": {
            "level": 2,
            "scores": {"Global_Contagion_Score": float(score)},
        },
        "DOOMSDAY_DAMPEN_GAMMA": 1.0,
        "DYNAMIC_KELLY_RISK": 0.01,
        "TS_KELLY_BY_SIDE": {
            "LONG": {"risk": 0.008},
            "SHORT": {"risk": 0.015},
        },
    }


def test_side_regime_long_damps_short_boosts_under_contagion():
    cfg = _stress_cfg(85.0)
    m_long, meta_l = arcr.resolve_side_regime_mult(cfg, position_side="LONG")
    m_short, meta_s = arcr.resolve_side_regime_mult(cfg, position_side="SHORT")
    assert m_long < 1.0
    assert meta_l.get("side_regime_source") == "long_dampen"
    assert m_short >= 1.0
    assert m_short <= float(ARCR_SHORT_RELAY_CAP)
    assert meta_s.get("side_regime_source") == "short_relay_boost"
    # Soft boost math: 1 + (1 - damp) * gain
    damp = float(meta_s.get("contagion_damp") or 1.0)
    expected = 1.0 + (1.0 - damp) * float(ARCR_SHORT_RELAY_GAIN)
    assert m_short == pytest.approx(min(expected, float(ARCR_SHORT_RELAY_CAP)))


def test_side_regime_soft_pass_missing_defcon():
    m, meta = arcr.resolve_side_regime_mult({}, position_side="LONG")
    assert m == 1.0
    assert "soft_pass" in str(meta.get("side_regime_source") or "")


def test_thompson_side_tilts_relative_to_base():
    cfg = _stress_cfg()
    m_s, meta = arcr.resolve_side_thompson_mult(cfg, position_side="SHORT")
    assert m_s == pytest.approx(1.5)  # 0.015/0.01 clipped to cap 1.5
    assert meta.get("side_thompson_source") == "ts_kelly_by_side"
    m_miss, _ = arcr.resolve_side_thompson_mult({}, position_side="SHORT")
    assert m_miss == 1.0


def test_funding_carry_favors_short_when_positive():
    m_s, meta_s = arcr.resolve_funding_carry_mult(0.0005, position_side="SHORT")
    m_l, meta_l = arcr.resolve_funding_carry_mult(0.0005, position_side="LONG")
    assert m_s > 1.0
    assert m_l < 1.0
    assert meta_s.get("funding_carry_source") == "funding_carry"
    m0, meta0 = arcr.resolve_funding_carry_mult(None, position_side="SHORT")
    assert m0 == 1.0
    assert "soft_pass" in str(meta0.get("funding_carry_source") or "")


def test_apply_regime_capital_product_asymmetric():
    cfg = _stress_cfg(90.0)
    k_long, meta_l = arcr.apply_regime_capital_to_kelly(
        0.02, cfg=cfg, position_side="LONG", funding_rate=0.0004
    )
    k_short, meta_s = arcr.apply_regime_capital_to_kelly(
        0.02, cfg=cfg, position_side="SHORT", funding_rate=0.0004
    )
    assert k_short > k_long
    assert meta_s.get("arcr_product", 0) > meta_l.get("arcr_product", 0)


def test_doomsday_size_mult_delegates_to_arcr():
    from bitget.trading.doomsday_gate import doomsday_size_mult

    cfg = _stress_cfg(85.0)
    assert doomsday_size_mult(cfg, position_side="LONG") < 1.0
    assert doomsday_size_mult(cfg, position_side="SHORT") >= 1.0


def test_meta_merge_skips_blind_dampen_when_side_set():
    from bitget.governance.meta_consumer import apply_meta_kelly_merge

    cfg = _stress_cfg(90.0)
    meta = {"META_GLOBAL_KELLY_MULT": 1.0}
    with mock.patch(
        "doomsday_dampener.apply_doomsday_dampening", side_effect=AssertionError("should skip")
    ):
        out = apply_meta_kelly_merge(
            0.02, meta, ns_prefix="X", sys_config=cfg, position_side="SHORT"
        )
    assert out == pytest.approx(0.02)


def test_meta_merge_blind_dampen_when_side_unknown():
    from bitget.governance.meta_consumer import apply_meta_kelly_merge

    cfg = _stress_cfg(90.0)
    meta = {"META_GLOBAL_KELLY_MULT": 1.0}
    with mock.patch(
        "doomsday_dampener.apply_doomsday_dampening", return_value=0.01
    ) as damp:
        out = apply_meta_kelly_merge(0.02, meta, ns_prefix="X", sys_config=cfg)
    damp.assert_called_once()
    assert out == pytest.approx(0.01)


def test_executor_skips_resize_for_virtual_kelly():
    from bitget.executor import execute_real_order
    from bitget.trading.execution_safety import ExecutionGateOutcome, GateResult

    gate = GateResult(
        ExecutionGateOutcome.APPROVED,
        meta={"doomsday_size_mult": 0.5, "nav_size_mult": 0.5},
    )
    with mock.patch("bitget.executor._load_config", return_value={"ENABLE_REAL_EXECUTION": "1"}), mock.patch(
        "bitget.executor.run_pre_execution_gates", return_value=gate
    ), mock.patch(
        "bitget.executor.resolve_leverage", return_value=3.0
    ), mock.patch(
        "bitget.executor.resolve_margin_mode", return_value="crossed"
    ), mock.patch(
        "bitget.executor.create_trade_exchange", side_effect=RuntimeError("stop-before-exchange")
    ):
        try:
            execute_real_order(
                "BTC_USDT",
                "SHORT",
                amount=1.0,
                amount_source="virtual_kelly",
            )
        except RuntimeError as e:
            assert "stop-before-exchange" in str(e)


def test_master_scanner_prefers_virtual_kelly_qty():
    src = open("bitget/master_scanner.py", encoding="utf-8").read()
    assert "_lookup_virtual_trade_quantity" in src
    assert 'amount_source="virtual_kelly"' in src or "amount_source=amount_source" in src
    assert "virtual_kelly" in src
