"""
Transcendent Asymmetric Exit — 비대칭 수익 극대화 청산 수식 엔진 (순수/무 I/O, 테스트 가능).

하드코딩된 분할비율·트레일폭·목표가를 전면 폐기하고, 모든 값이 국면(Regime)·변동성·
수급 엣지(EdgeScore)·우측 꼬리 팽창도에 맞춰 0~100% 사이를 유동적으로 자율 진화한다.

  M1 유동 부분익절:  F_out = f(Regime, Volatility, EdgeScore)
  M2 볼록 트레일 래칫: TrailStop = MaxHigh × (1 - κ(run_ret)), κ 곡선은 RL 자가학습
  M3 우측꼬리 메타튜닝: TargetPercentile = g(Regime, FatTailRatio)  (50% 중앙값 앵커 폐기)
  M4 자가증식 피라미딩: edge_score 임계 돌파 시 유휴 NAV로 불타기 추가매수
"""
from __future__ import annotations

from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# 국면 분류
# ---------------------------------------------------------------------------
DEFENSIVE_REGIMES = {"BEAR", "HIGH_VOL", "DEFENSE", "RISK_OFF"}
BULLISH_REGIMES = {"BULL", "RISK_ON", "TREND_UP"}


def _norm_regime(regime: Any) -> str:
    return str(regime or "UNKNOWN").strip().upper()


def is_defensive(regime: Any) -> bool:
    return _norm_regime(regime) in DEFENSIVE_REGIMES


def is_bullish(regime: Any) -> bool:
    return _norm_regime(regime) in BULLISH_REGIMES


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


# ===========================================================================
# Mission 1 — 유동적 상태 기반 부분 익절 (Fluid Scale-Out)
# ===========================================================================
def fluid_scale_out_fraction(
    regime: Any,
    volatility_pct: float,
    edge_score: float,
) -> float:
    """
    1차 목표가 도달 시 매도할 비율 F_out ∈ [0,1].

      · 방어적 국면(BEAR/HIGH_VOL)일수록 ↑ (원금 방어, 70~80% 매도)
      · BULL + 높은 edge_score 일수록 ↓ (프리러너 극대화, 10~20% 매도)
      · 변동성↑ → 약간 ↑ (이익 보호)
    """
    reg = _norm_regime(regime)
    if reg in DEFENSIVE_REGIMES:
        base_f = 0.78
    elif reg in BULLISH_REGIMES:
        base_f = 0.18
    else:  # CHOP / UNKNOWN
        base_f = 0.45

    # 엣지가 강할수록 더 적게 팔아 러너 보존 (최대 -0.08 → BULL 하한 ≈10%)
    edge_adj = -_clamp((float(edge_score) - 1.0) * 0.05, 0.0, 0.08)
    # 변동성이 클수록 더 많이 팔아 방어 (최대 +0.12)
    vol_adj = _clamp((float(volatility_pct) - 5.0) / 100.0, 0.0, 0.12)

    return round(_clamp(base_f + edge_adj + vol_adj, 0.0, 1.0), 4)


# ===========================================================================
# Mission 2 — 진화형 볼록성 트레일링 래칫 (Evolutionary Convex Ratchet)
# ===========================================================================
RATCHET_STATE_KEY = "EXIT_RATCHET_STATE"
DEFAULT_RATCHET_STATE: Dict[str, Any] = {
    "kappa_max": 0.12,   # 초기(러너 진입 직후) 트레일 폭 — 넓게 숨통
    "kappa_min": 0.05,   # 수익 팽창 후 최소 트레일 폭 — 이익 보호로 조임
    "anchor_ret": 40.0,  # κ_min 으로 수렴하는 수익률 기준점(%)
    "convexity": 1.0,    # 1.0=선형, >1=볼록(초반 더 넓게), <1=오목
    "curve": "linear",
}


def load_ratchet_state(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = dict(DEFAULT_RATCHET_STATE)
    if isinstance(cfg, dict):
        st = cfg.get(RATCHET_STATE_KEY)
        if isinstance(st, dict):
            base.update({k: st[k] for k in st if k in DEFAULT_RATCHET_STATE})
    return base


def convex_ratchet_kappa(run_ret_pct: float, state: Optional[Dict[str, Any]] = None) -> float:
    """
    러너 수익률(run_ret_pct, 진입 대비 고점 수익 %)에 따른 트레일 계수 κ.
    초반엔 κ_max(넓게 숨통) → anchor_ret 로 갈수록 κ_min(이익 보호로 조임).
    convexity>1 이면 초반을 더 넓게 유지(볼록).
    """
    st = state or DEFAULT_RATCHET_STATE
    k_max = float(st.get("kappa_max", 0.12))
    k_min = float(st.get("kappa_min", 0.05))
    anchor = max(1.0, float(st.get("anchor_ret", 40.0)))
    p = max(0.1, float(st.get("convexity", 1.0)))

    prog = _clamp(float(run_ret_pct) / anchor, 0.0, 1.0)
    shape = prog ** p  # convex(p>1): 초반 작게 → κ가 천천히 줄어 더 오래 넓다
    kappa = k_max - (k_max - k_min) * shape
    return _clamp(kappa, min(k_min, k_max), max(k_min, k_max))


def trail_stop_price(max_high_price: float, kappa: float) -> float:
    """TrailStop = MaxHigh × (1 - κ)."""
    return float(max_high_price) * (1.0 - _clamp(kappa, 0.0, 0.95))


def update_ratchet_kappa_rl(
    state: Dict[str, Any],
    *,
    whipsaw_rate: float,
    giveback_rate: float,
    eta: float = 0.04,
) -> Dict[str, Any]:
    """
    주간 RL 업데이트.
      · whipsaw_rate (조기 청산 비율) 높음 → 트레일이 너무 빡빡 → κ 확대 + 곡선 볼록화
      · giveback_rate (이익 반납 비율) 높음 → 트레일이 너무 헐거움 → κ 축소 + 곡선 선형/오목화
    그래디언트: Δ = eta · (whipsaw_rate − giveback_rate)
    """
    st = dict(DEFAULT_RATCHET_STATE)
    if isinstance(state, dict):
        st.update({k: state[k] for k in state if k in DEFAULT_RATCHET_STATE})

    w = _clamp(whipsaw_rate, 0.0, 1.0)
    g = _clamp(giveback_rate, 0.0, 1.0)
    delta = float(eta) * (w - g)

    st["kappa_max"] = round(_clamp(float(st["kappa_max"]) + delta, 0.04, 0.30), 4)
    st["kappa_min"] = round(_clamp(float(st["kappa_min"]) + delta * 0.5, 0.02, float(st["kappa_max"])), 4)

    # 곡선 형태 진화: 초기 조기청산이 지배적이면 볼록(초반 더 넓게), 반대면 선형화
    conv = float(st["convexity"])
    if w - g > 0.15:
        conv = _clamp(conv + 0.20, 0.5, 3.0)
    elif g - w > 0.15:
        conv = _clamp(conv - 0.20, 0.5, 3.0)
    st["convexity"] = round(conv, 3)
    st["curve"] = "convex" if conv > 1.05 else ("concave" if conv < 0.95 else "linear")
    return st


# ===========================================================================
# Mission 3 — 우측 꼬리 자가 확장 메타튜닝 (Right-Tail Meta-Tuning)
# ===========================================================================
def target_percentile(regime: Any, fat_tail_ratio: float) -> float:
    """
    목표가 추적 퍼센타일. 50% 중앙값 앵커를 폐기하고 우측 꼬리로 끌어올린다.
      · 평장(CHOP) 70 · 대세상승(BULL) 90 · 방어(BEAR/HIGH_VOL) 60
      · fat_tail_ratio(=p90/p50 등) 가 클수록 추가 상향.
    """
    reg = _norm_regime(regime)
    if reg in BULLISH_REGIMES:
        base = 90.0
    elif reg in DEFENSIVE_REGIMES:
        base = 60.0
    else:
        base = 70.0
    fat_bonus = _clamp((float(fat_tail_ratio) - 2.0) * 5.0, 0.0, 8.0)
    return _clamp(base + fat_bonus, 55.0, 95.0)


def fat_tail_ratio(p_hi: float, p_mid: float) -> float:
    """우측 꼬리 팽창도 = 상위분위 MFE / 중앙 MFE (안전 가드)."""
    try:
        mid = float(p_mid)
        if mid <= 1e-6:
            return 1.0
        return max(1.0, float(p_hi) / mid)
    except (TypeError, ValueError, ZeroDivisionError):
        return 1.0


# ===========================================================================
# Mission 4 — 엣지 스코어 연동 자가 증식 (Autonomous Pyramiding)
# ===========================================================================
PYRAMID_EDGE_THRESHOLD = 1.5
PYRAMID_MAX_ADDS = 3
PYRAMID_NAV_CAP_FRAC = 0.10  # 1회 추가매수는 NAV 의 최대 10%


def pyramid_decision(
    *,
    edge_score: float,
    regime: Any,
    idle_cash: float,
    nav: float,
    pyramid_adds_done: int,
    free_runner: bool,
    edge_threshold: float = PYRAMID_EDGE_THRESHOLD,
    max_adds: int = PYRAMID_MAX_ADDS,
) -> Dict[str, Any]:
    """
    프리러너 상태에서 수급·수익속도가 동반 폭발(edge>임계)하면 유휴 NAV로 불타기.
    반환: {"do": bool, "add_notional": float, "f_add": float, "reason": str}
    """
    out = {"do": False, "add_notional": 0.0, "f_add": 0.0, "reason": ""}
    if not free_runner:
        out["reason"] = "not_free_runner"
        return out
    if int(pyramid_adds_done) >= int(max_adds):
        out["reason"] = "max_adds_reached"
        return out
    if is_defensive(regime) or not is_bullish(regime):
        out["reason"] = "regime_not_bullish"
        return out
    if float(edge_score) < float(edge_threshold):
        out["reason"] = "edge_below_threshold"
        return out
    if float(idle_cash) <= 0 or float(nav) <= 0:
        out["reason"] = "no_idle_cash"
        return out

    # 엣지 초과분에 비례한 유휴현금 투입 비율 5~30%
    f_add = _clamp((float(edge_score) - float(edge_threshold)) * 0.10 + 0.05, 0.05, 0.30)
    add_notional = float(idle_cash) * f_add
    cap = float(nav) * PYRAMID_NAV_CAP_FRAC
    add_notional = min(add_notional, cap)
    if add_notional <= 0:
        out["reason"] = "zero_notional"
        return out

    out.update({"do": True, "add_notional": round(add_notional, 2), "f_add": round(f_add, 4), "reason": "pyramid"})
    return out


def blend_final_return(realized_partial_ret: float, scaled_out_frac: float, runner_ret_pct: float) -> float:
    """
    최종 실현 수익률 = 부분익절 실현분 + 잔여 러너 실현분.
      final = realized_partial_ret + (1 - scaled_out_frac) × runner_ret
    realized_partial_ret 는 이미 (F_out × 부분청산수익)으로 적립된 값.
    """
    rem = _clamp(1.0 - float(scaled_out_frac), 0.0, 1.0)
    return round(float(realized_partial_ret) + rem * float(runner_ret_pct), 4)
