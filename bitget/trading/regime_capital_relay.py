"""
Asymmetric Regime Capital Relay (ARCR) — Bitget coin-quant capital plane.

Synergy invariant:
  Under contagion / DEFCON stress, LONG capital shrinks (or blocks via doomsday_gate);
  SHORT / hedge capital holds or soft-boosts so crash-edge can compound into
  deathmatch / exploration / promotion — instead of being side-blind dampened.

Invariants:
  - Never auto-flatten open inventory
  - Soft-pass → mult 1.0 when DEFCON / TS / funding missing
  - Paper ≈ live: same resolve_* functions; live skips re-apply when amount_source=virtual_kelly
  - Hard caps on boosts — never invent unbounded size
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.memory_policy import (
    ARCR_FUNDING_CARRY_CAP,
    ARCR_FUNDING_CARRY_FLOOR,
    ARCR_FUNDING_SCALE,
    ARCR_SHORT_RELAY_CAP,
    ARCR_SHORT_RELAY_GAIN,
    ARCR_TS_SIDE_CAP,
    ARCR_TS_SIDE_FLOOR,
)


def _clip(x: float, lo: float, hi: float) -> float:
    return float(min(hi, max(lo, x)))


def resolve_side_regime_mult(
    cfg: Optional[dict],
    *,
    position_side: str,
    meta_state: Optional[dict] = None,
) -> tuple[float, dict[str, Any]]:
    """
    LONG → shared doomsday dampener (≤1.0).
    SHORT → hold 1.0, or soft-boost when damp would have shrunk LONG:
            1 + (1 − damp) × GAIN, clamped to [1.0, SHORT_RELAY_CAP].
    """
    side = str(position_side or "LONG").upper()
    meta: dict[str, Any] = {
        "arcr_side": side,
        "side_regime_mult": 1.0,
        "side_regime_source": "soft_pass",
    }
    try:
        from doomsday_dampener import (
            dampening_multiplier,
            global_score_from_config,
            resolve_gamma,
        )

        score = global_score_from_config(cfg)
        if score is None:
            meta["side_regime_source"] = "soft_pass_missing_score"
            return 1.0, meta
        gamma = resolve_gamma(meta_state, cfg)
        damp = float(dampening_multiplier(score, gamma))
        meta["contagion_damp"] = damp
        meta["contagion_score"] = score
    except Exception as e:
        meta["side_regime_source"] = "soft_pass_dampener_error"
        meta["side_regime_error"] = str(e)[:120]
        return 1.0, meta

    if side == "SHORT":
        if damp >= 1.0 - 1e-12:
            meta["side_regime_source"] = "short_hold_no_stress"
            meta["side_regime_mult"] = 1.0
            return 1.0, meta
        gain = float(ARCR_SHORT_RELAY_GAIN)
        cap = float(ARCR_SHORT_RELAY_CAP)
        boost = 1.0 + (1.0 - damp) * gain
        mult = _clip(boost, 1.0, cap)
        meta["side_regime_mult"] = mult
        meta["side_regime_source"] = "short_relay_boost"
        return mult, meta

    # LONG (default): classic dampen
    mult = _clip(damp, 0.0, 1.0)
    meta["side_regime_mult"] = mult
    meta["side_regime_source"] = "long_dampen"
    return mult, meta


def resolve_side_thompson_mult(
    cfg: Optional[dict],
    *,
    position_side: str,
) -> tuple[float, dict[str, Any]]:
    """
    TS_KELLY_BY_SIDE[side].risk / DYNAMIC_KELLY_RISK → relative capital tilt.
    Missing / stale → 1.0 soft-pass.
    """
    side = str(position_side or "LONG").upper()
    meta: dict[str, Any] = {
        "arcr_ts_side": side,
        "side_thompson_mult": 1.0,
        "side_thompson_source": "soft_pass",
    }
    ts = (cfg or {}).get("TS_KELLY_BY_SIDE")
    if not isinstance(ts, dict) or not ts:
        meta["side_thompson_source"] = "soft_pass_missing_ts"
        return 1.0, meta
    row = ts.get(side)
    if not isinstance(row, dict):
        meta["side_thompson_source"] = "soft_pass_side_absent"
        return 1.0, meta
    try:
        side_risk = float(row.get("risk"))
    except (TypeError, ValueError):
        meta["side_thompson_source"] = "soft_pass_bad_risk"
        return 1.0, meta
    if side_risk <= 0:
        meta["side_thompson_source"] = "soft_pass_nonpositive_risk"
        return 1.0, meta
    try:
        base = float((cfg or {}).get("DYNAMIC_KELLY_RISK", 0.01) or 0.01)
    except (TypeError, ValueError):
        base = 0.01
    if base <= 0:
        meta["side_thompson_source"] = "soft_pass_bad_base"
        return 1.0, meta
    raw = side_risk / base
    mult = _clip(raw, float(ARCR_TS_SIDE_FLOOR), float(ARCR_TS_SIDE_CAP))
    meta["side_thompson_mult"] = mult
    meta["side_thompson_source"] = "ts_kelly_by_side"
    meta["ts_side_risk"] = side_risk
    meta["ts_base_risk"] = base
    return mult, meta


def resolve_funding_carry_mult(
    funding_rate: Optional[float],
    *,
    position_side: str,
    cfg: Optional[dict] = None,
) -> tuple[float, dict[str, Any]]:
    """
    Ex-ante soft tilt from perpetual funding (Bitget: +rate → longs pay shorts).
    SHORT gains when rate > 0; LONG gains when rate < 0.
    Missing rate → 1.0. Never hard-blocks.
    """
    side = str(position_side or "LONG").upper()
    meta: dict[str, Any] = {
        "arcr_funding_side": side,
        "funding_carry_mult": 1.0,
        "funding_carry_source": "soft_pass",
    }
    if funding_rate is None:
        meta["funding_carry_source"] = "soft_pass_missing_rate"
        return 1.0, meta
    try:
        fr = float(funding_rate)
    except (TypeError, ValueError):
        meta["funding_carry_source"] = "soft_pass_bad_rate"
        return 1.0, meta

    # Optional kill via cfg ≤0 scale
    try:
        scale = float((cfg or {}).get("ARCR_FUNDING_SCALE", ARCR_FUNDING_SCALE))
    except (TypeError, ValueError):
        scale = float(ARCR_FUNDING_SCALE)
    if scale <= 0:
        meta["funding_carry_source"] = "disabled"
        return 1.0, meta

    edge = fr if side == "SHORT" else -fr
    delta = edge * scale
    lo = float(ARCR_FUNDING_CARRY_FLOOR)
    hi = float(ARCR_FUNDING_CARRY_CAP)
    mult = _clip(1.0 + delta, lo, hi)
    meta["funding_rate"] = fr
    meta["funding_carry_mult"] = mult
    meta["funding_carry_source"] = "funding_carry"
    return mult, meta


def apply_regime_capital_to_kelly(
    kelly_risk_pct: float,
    *,
    cfg: Optional[dict],
    position_side: str,
    meta_state: Optional[dict] = None,
    funding_rate: Optional[float] = None,
) -> tuple[float, dict[str, Any]]:
    """
    Paper Kelly path SSOT — product of side-regime × side-thompson × funding-carry.
    """
    side = str(position_side or "LONG").upper()
    out = float(kelly_risk_pct)
    merged: dict[str, Any] = {"arcr_applied": True, "arcr_side": side}

    m_reg, meta_reg = resolve_side_regime_mult(
        cfg, position_side=side, meta_state=meta_state
    )
    out *= m_reg
    merged.update(meta_reg)

    m_ts, meta_ts = resolve_side_thompson_mult(cfg, position_side=side)
    out *= m_ts
    merged.update(meta_ts)

    m_fund, meta_fund = resolve_funding_carry_mult(
        funding_rate, position_side=side, cfg=cfg
    )
    out *= m_fund
    merged.update(meta_fund)

    product = float(m_reg) * float(m_ts) * float(m_fund)
    merged["arcr_product"] = product
    merged["kelly_after_arcr"] = max(0.0, float(out))
    return max(0.0, float(out)), merged
