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

from typing import Any, Dict, List, Mapping, Optional, Sequence

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


# ===========================================================================
# Mission 5 — Mega-Trend 내재적 PnL·승률 자가 진단 (Internal Kill-Switch 1번)
# ===========================================================================
import os as _os

MEGA_TREND_INTERNAL_DIAG_KEY = "internal_diagnostics"

_BOUNCE_EXIT_TYPES = frozenset(
    {"STAT_MAE", "STAT_ATR", "HYBRID_ATR", "ZOMBIE_FORCE_CLOSE", "MEGA_CLIMAX_RUNNER"}
)
_BOUNCE_REASON_MARKERS = ("본절", "손절", "MAE", "bounce", "칼손절", "이탈")


def _mega_trend_internal_thresholds_base() -> Dict[str, Any]:
    """RL 적용 전 기본 임계치."""
    def _f(key: str, default: float) -> float:
        try:
            return float(_os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(_os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    return {
        "window_n": _i("MEGA_TREND_INTERNAL_WINDOW_N", 8),
        "window_n_min": _i("MEGA_TREND_INTERNAL_WINDOW_MIN", 5),
        "win_rate_min": _f("MEGA_TREND_INTERNAL_WIN_RATE_MIN", 0.40),
        "mfe_reach_min": _f("MEGA_TREND_INTERNAL_MFE_REACH_MIN", 0.35),
        "bounce_stop_max_rate": _f("MEGA_TREND_INTERNAL_BOUNCE_RATE_MAX", 0.45),
        "pnl_accel_drop_min": _f("MEGA_TREND_INTERNAL_PNL_ACCEL_DROP", 0.15),
        "mfe_target_pct": _f("MEGA_TREND_INTERNAL_MFE_TARGET_PCT", 5.0),
        "breakeven_band_pct": _f("MEGA_TREND_INTERNAL_BE_BAND", 1.5),
        "gave_back_mfe_ratio": _f("MEGA_TREND_INTERNAL_GAVE_BACK_RATIO", 0.35),
    }


def mega_trend_internal_thresholds(sector: Optional[str] = None) -> Dict[str, Any]:
    """내재적 킬스위치 1번 임계치 — env + Kill RL delta (P5: sector overlay)."""
    base = _mega_trend_internal_thresholds_base()
    try:
        from mega_trend_kill_rl import apply_kill_rl_threshold_adjustments, load_kill_rl_state

        return apply_kill_rl_threshold_adjustments(
            base, rl_state=load_kill_rl_state(), sector=sector
        )
    except Exception:
        return base


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        x = float(v)  # type: ignore[arg-type]
        if x != x:  # NaN
            return default
        return x
    except (TypeError, ValueError):
        return default


def classify_mega_trend_trade_outcome(
    trade: Mapping[str, Any],
    *,
    breakeven_band_pct: float = 1.5,
    mfe_target_pct: float = 5.0,
    gave_back_ratio: float = 0.35,
) -> str:
    """
    단일 체결 결과 분류.
    win | loss | bounce_stop | open_live
    """
    status = str(trade.get("status") or "OPEN").upper()
    final_ret = trade.get("final_ret")
    sim_ret = _safe_float(trade.get("sim_stat_ret"))
    mfe = _safe_float(trade.get("mfe"))
    exit_type = str(trade.get("exit_type") or "").strip().upper()
    exit_reason = str(trade.get("exit_reason") or "")

    if status == "OPEN":
        if sim_ret > breakeven_band_pct:
            return "open_live"
        if sim_ret <= -breakeven_band_pct:
            return "loss"
        return "open_live"

    ret = _safe_float(final_ret, sim_ret)
    band = float(breakeven_band_pct)

    if exit_type in _BOUNCE_EXIT_TYPES:
        return "bounce_stop"
    if any(m in exit_reason for m in _BOUNCE_REASON_MARKERS):
        return "bounce_stop"
    if -band <= ret <= band:
        return "bounce_stop"
    if mfe >= float(mfe_target_pct) and ret < mfe * float(gave_back_ratio):
        return "bounce_stop"
    if ret > band:
        return "win"
    return "loss"


def is_mfe_target_reached(
    trade: Mapping[str, Any],
    *,
    mfe_target_pct: float = 5.0,
) -> bool:
    """MFE(최대 허용 수익) 목표 도달 여부."""
    mfe = _safe_float(trade.get("mfe"))
    if mfe >= float(mfe_target_pct):
        return True
    if str(trade.get("status") or "").upper() == "OPEN":
        entry = _safe_float(trade.get("entry_price"))
        max_high = _safe_float(trade.get("max_high"))
        if entry > 0 and max_high > 0:
            run_mfe = ((max_high - entry) / entry) * 100.0
            return run_mfe >= float(mfe_target_pct)
    return False


def compute_internal_trade_metrics(
    trades: Sequence[Mapping[str, Any]],
    *,
    thresholds: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """최근 N회 체결 — 승률·MFE 도달률·본절/손절 튕김 빈도."""
    thr = dict(mega_trend_internal_thresholds())
    if isinstance(thresholds, Mapping):
        thr.update({k: thresholds[k] for k in thresholds if k in thr})

    closed_wins = 0
    closed_losses = 0
    bounce_stops = 0
    mfe_hits = 0
    rets: List[float] = []
    outcomes: List[str] = []

    for t in trades or []:
        oc = classify_mega_trend_trade_outcome(
            t,
            breakeven_band_pct=float(thr["breakeven_band_pct"]),
            mfe_target_pct=float(thr["mfe_target_pct"]),
            gave_back_ratio=float(thr["gave_back_mfe_ratio"]),
        )
        outcomes.append(oc)
        if is_mfe_target_reached(t, mfe_target_pct=float(thr["mfe_target_pct"])):
            mfe_hits += 1

        if oc == "win":
            closed_wins += 1
            rets.append(_safe_float(t.get("final_ret"), _safe_float(t.get("sim_stat_ret"))))
        elif oc == "loss":
            closed_losses += 1
            rets.append(_safe_float(t.get("final_ret"), _safe_float(t.get("sim_stat_ret"))))
        elif oc == "bounce_stop":
            bounce_stops += 1
            rets.append(_safe_float(t.get("final_ret"), _safe_float(t.get("sim_stat_ret"))))

    n = len(trades or [])
    closed_n = closed_wins + closed_losses + bounce_stops
    win_rate = (closed_wins / closed_n) if closed_n > 0 else None
    mfe_reach_rate = (mfe_hits / n) if n > 0 else None
    bounce_rate = (bounce_stops / closed_n) if closed_n > 0 else None
    avg_ret = (sum(rets) / len(rets)) if rets else None

    return {
        "n_trades": n,
        "n_closed": closed_n,
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "mfe_reach_rate": round(mfe_reach_rate, 4) if mfe_reach_rate is not None else None,
        "bounce_stop_rate": round(bounce_rate, 4) if bounce_rate is not None else None,
        "avg_ret_pct": round(avg_ret, 4) if avg_ret is not None else None,
        "wins": closed_wins,
        "losses": closed_losses,
        "bounce_stops": bounce_stops,
        "mfe_hits": mfe_hits,
        "outcomes": outcomes,
    }


def compute_pnl_acceleration(
    trades: Sequence[Mapping[str, Any]],
    *,
    thresholds: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    PnL 가속도 — 최근 절반 vs 이전 절반 승률·평균수익 변화.
    음수 가속 = 내부 동력 둔화.
    """
    items = list(trades or [])
    n = len(items)
    if n < 4:
        return {
            "accel_win_rate": None,
            "accel_avg_ret": None,
            "recent_metrics": None,
            "prior_metrics": None,
            "reason": "insufficient_trades_for_accel",
        }

    mid = n // 2
    prior = items[:mid]
    recent = items[mid:]
    recent_m = compute_internal_trade_metrics(recent, thresholds=thresholds)
    prior_m = compute_internal_trade_metrics(prior, thresholds=thresholds)

    accel_wr = None
    if recent_m.get("win_rate") is not None and prior_m.get("win_rate") is not None:
        accel_wr = float(recent_m["win_rate"]) - float(prior_m["win_rate"])

    accel_ret = None
    if recent_m.get("avg_ret_pct") is not None and prior_m.get("avg_ret_pct") is not None:
        accel_ret = float(recent_m["avg_ret_pct"]) - float(prior_m["avg_ret_pct"])

    return {
        "accel_win_rate": round(accel_wr, 4) if accel_wr is not None else None,
        "accel_avg_ret": round(accel_ret, 4) if accel_ret is not None else None,
        "recent_metrics": recent_m,
        "prior_metrics": prior_m,
        "reason": "computed",
    }


def evaluate_internal_momentum_loss(
    trades: Sequence[Mapping[str, Any]],
    *,
    thresholds: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    [1번] 내재적 자가 진단 — 외부 가격/수급 없이 장부만으로 동력 상실 판정.

    트리거 (OR):
      · 승률 ≤ win_rate_min (기본 40%)
      · MFE 도달률 ≤ mfe_reach_min
      · 본절/손절 튕김 비율 ≥ bounce_stop_max_rate
      · 승률 가속도 ≤ -pnl_accel_drop_min
    """
    thr = dict(mega_trend_internal_thresholds())
    if isinstance(thresholds, Mapping):
        thr.update({k: thresholds[k] for k in thresholds if k in thr})

    min_n = int(thr["window_n_min"])
    metrics = compute_internal_trade_metrics(trades, thresholds=thr)
    accel = compute_pnl_acceleration(trades, thresholds=thr)

    out: Dict[str, Any] = {
        "momentum_lost": False,
        "self_diagnosis": False,
        "triggers": [],
        "metrics": metrics,
        "acceleration": accel,
        "reason": "neutral",
    }

    if int(metrics.get("n_trades") or 0) < min_n:
        out["reason"] = f"insufficient_sample n={metrics.get('n_trades')}<{min_n}"
        return out

    triggers: List[str] = []
    wr = metrics.get("win_rate")
    if wr is not None and float(wr) <= float(thr["win_rate_min"]):
        triggers.append(f"win_rate_collapse_{float(wr):.2f}<={thr['win_rate_min']}")

    mfe_r = metrics.get("mfe_reach_rate")
    if mfe_r is not None and float(mfe_r) <= float(thr["mfe_reach_min"]):
        triggers.append(f"mfe_reach_collapse_{float(mfe_r):.2f}<={thr['mfe_reach_min']}")

    bounce_r = metrics.get("bounce_stop_rate")
    if bounce_r is not None and float(bounce_r) >= float(thr["bounce_stop_max_rate"]):
        triggers.append(f"bounce_stop_spike_{float(bounce_r):.2f}>={thr['bounce_stop_max_rate']}")

    accel_wr = accel.get("accel_win_rate")
    if accel_wr is not None and float(accel_wr) <= -float(thr["pnl_accel_drop_min"]):
        triggers.append(f"pnl_accel_win_rate_{float(accel_wr):.2f}")

    if triggers:
        out.update(
            {
                "momentum_lost": True,
                "self_diagnosis": True,
                "triggers": triggers,
                "reason": "internal_momentum_lost: " + " | ".join(triggers),
            }
        )
    else:
        out["reason"] = "internal_momentum_ok"

    return out

