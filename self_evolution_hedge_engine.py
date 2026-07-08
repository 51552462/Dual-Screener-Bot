"""
Self-Evolution Hedge Engine — Axis 1: 시스템 내재적 패닉 스케일링.

Rule-based 외부 VIX 단독 헤지를 거부하고, predictive_regime_ensemble 5팩터 앙상블
(score · short/long_trend · vix · breadth · pri) + crisis/regime 을 SSOT 로
BEAR_GRIND / BEAR_ACCEL / BEAR_PANIC Base Cap 을 산출한다.

외부 yfinance VIX/벤치마크는 REGIME_ENSEMBLE 부재 시에만 최후 폴백.

Axis 2 — RL Hedge:
  live_nav_manager.inverse_sleeve rolling log + forward_trades [INVERSE_ETF] 실현 PnL.
  수익(+) → Base Cap 100% · 휩쏘(−) → min(Base×50%, 15%).

Axis 3 — V-Recovery Kill Switch:
  BEAR/HIGH_VOL → BULL(또는 analog V_RECOVERY / ensemble 반등) 전환 시
  OPEN 인버스 전량 청산 + RL rolling log Reset (INVERSE_MODE ON 이어도 비대칭 발동).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from dynamic_hedge_cap import (
    CAP_BY_BEAR_PHASE,
    HEDGE_EFFICACY_LOSS_MULT,
    HEDGE_EFFICACY_MIN_CLOSES,
    HEDGE_EFFICACY_PROFIT_MULT,
    HEDGE_EFFICACY_WHIPSAW_MAX_CAP,
    HEDGE_EFFICACY_LOOKBACK_DAYS,
    HEDGE_EFFICACY_EPS,
    ACCEL_SCORE_THRESHOLD,
    cap_pct_for_bear_phase,
    classify_bear_stress_phase,
    fetch_inverse_sleeve_realized_stats,
    _defcon_level_from_config,
    _fetch_benchmark_20d_return_pct,
    _fetch_vix_level,
)

# ensemble vix 팩터 상태 → 패닉 (≈ raw VIX 28+ 와 정합: -tanh((28-18)/8) ≈ -0.85)
INTRINSIC_VIX_FACTOR_PANIC = -0.85
INTRINSIC_SHORT_TREND_ACCEL = -0.45
INTRINSIC_LONG_TREND_ACCEL = -0.30
INTRINSIC_BREADTH_COLLAPSE = -0.55

DOWN_THRESHOLD = -0.18


@dataclass
class InverseSleeveRLContext:
    """Axis 2 — 인버스 슬리브 실현 PnL RL 입력."""

    market: str
    n_closed: int = 0
    weighted_ret_pct: float = 0.0
    total_invest: float = 0.0
    total_net_pnl_abs: float = 0.0
    verdict: str = "insufficient"
    lookback_days: int = HEDGE_EFFICACY_LOOKBACK_DAYS
    sources: Tuple[str, ...] = ()


def fetch_inverse_sleeve_rl_context(
    market: str,
    *,
    lookback_days: int = HEDGE_EFFICACY_LOOKBACK_DAYS,
    db_path: Optional[str] = None,
) -> InverseSleeveRLContext:
    """
    Axis 2 SSOT — forward_trades(DB) + live_nav_manager rolling log 병합.

    DB 우선(장부 정본); NAV log 는 DB 부재·지연 시 보조 + absolute PnL enrich.
    """
    mkt = str(market or "US").upper()
    db_stats = fetch_inverse_sleeve_realized_stats(
        mkt, lookback_days=lookback_days, db_path=db_path
    )
    nav_stats: Dict[str, Any] = {}
    try:
        from live_nav_manager import get_inverse_sleeve_rl_stats

        nav_stats = get_inverse_sleeve_rl_stats(mkt, lookback_days=lookback_days)
    except Exception:
        nav_stats = {}

    sources: list[str] = []
    n_closed = 0
    wret = 0.0
    invest = 0.0
    pnl_abs = 0.0
    verdict = "insufficient"

    db_n = int(db_stats.get("n_closed") or 0)
    nav_n = int(nav_stats.get("n_closed") or 0)

    if db_n >= HEDGE_EFFICACY_MIN_CLOSES:
        sources.append("forward_trades")
        n_closed = db_n
        wret = float(db_stats.get("weighted_ret_pct") or 0.0)
        invest = float(db_stats.get("total_invest") or 0.0)
        verdict = str(db_stats.get("verdict") or "insufficient")
        pnl_abs = float(nav_stats.get("total_net_pnl_abs") or 0.0)
        if pnl_abs == 0.0 and invest > 0:
            pnl_abs = invest * wret / 100.0
    elif nav_n >= HEDGE_EFFICACY_MIN_CLOSES:
        sources.append("live_nav_manager")
        n_closed = nav_n
        wret = float(nav_stats.get("weighted_ret_pct") or 0.0)
        invest = float(nav_stats.get("total_invest") or 0.0)
        pnl_abs = float(nav_stats.get("total_net_pnl_abs") or 0.0)
        verdict = str(nav_stats.get("verdict") or "insufficient")
    else:
        if db_n > 0:
            sources.append("forward_trades_partial")
        if nav_n > 0:
            sources.append("live_nav_manager_partial")

    return InverseSleeveRLContext(
        market=mkt,
        n_closed=n_closed,
        weighted_ret_pct=round(wret, 4),
        total_invest=round(invest, 2),
        total_net_pnl_abs=round(pnl_abs, 2),
        verdict=verdict,
        lookback_days=int(lookback_days),
        sources=tuple(sources),
    )


def apply_rl_hedge_cap(
    base_cap_pct: float,
    ctx: InverseSleeveRLContext,
) -> Tuple[float, Dict[str, Any]]:
    """
    Axis 2 — 실전 PnL RL 피드백.

    profitable  → base × 100%
    whipsaw     → min(base × 50%, 15%)  — 헤지 미작동 장세 자율 수축
    insufficient → base 유지
    """
    try:
        base = float(base_cap_pct)
    except (TypeError, ValueError):
        base = CAP_BY_BEAR_PHASE["NEUTRAL"]

    verdict = str(ctx.verdict or "insufficient")
    wret = float(ctx.weighted_ret_pct)

    if ctx.n_closed >= HEDGE_EFFICACY_MIN_CLOSES and verdict == "profitable" and wret > HEDGE_EFFICACY_EPS:
        mult = HEDGE_EFFICACY_PROFIT_MULT
        final = base * mult
        mode = "rl_hedge_full"
        rl_note = "헤지유효_실전PnL"
    elif ctx.n_closed >= HEDGE_EFFICACY_MIN_CLOSES and verdict == "whipsaw":
        mult = HEDGE_EFFICACY_LOSS_MULT
        final = min(base * mult, HEDGE_EFFICACY_WHIPSAW_MAX_CAP)
        mode = "rl_hedge_whipsaw_shrink"
        rl_note = "휩쏘_헤지무효"
    else:
        mult = 1.0
        final = base
        mode = "rl_hedge_neutral"
        rl_note = "실전표본부족"

    audit = {
        "axis": "rl_hedge",
        "rl_mode": mode,
        "rl_verdict": verdict,
        "rl_mult": round(mult, 4),
        "base_cap_pct": round(base, 4),
        "final_cap_pct": round(final, 4),
        "inverse_sleeve_n_closed": ctx.n_closed,
        "inverse_sleeve_5d_ret_pct": round(wret, 4),
        "inverse_sleeve_net_pnl_abs": ctx.total_net_pnl_abs,
        "rl_sources": list(ctx.sources),
        "lookback_days": ctx.lookback_days,
        "rl_note": rl_note,
        # dynamic_hedge_cap 호환 alias
        "efficacy_mode": mode.replace("rl_hedge", "efficacy"),
        "efficacy_verdict": verdict,
        "efficacy_mult": round(mult, 4),
        "macro_cap_pct": round(base, 4),
    }
    return final, audit


def resolve_self_evolution_hedge_cap_pct(
    market: str,
    sys_config: Optional[dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
) -> Tuple[float, Dict[str, Any]]:
    """Axis 1 Base Cap × Axis 2 RL Hedge — Self-Evolution Hedge Engine 최종 캡."""
    base_cap, base_meta = resolve_intrinsic_base_cap_pct(market, sys_config)
    rl_ctx = fetch_inverse_sleeve_rl_context(market, db_path=db_path)
    final_cap, rl_meta = apply_rl_hedge_cap(base_cap, rl_ctx)

    meta = {
        **base_meta,
        **rl_meta,
        "engine": "self_evolution_hedge",
        "cap_pct": round(final_cap, 4),
        "summary": (
            f"[내재]{base_meta.get('bear_phase', '?')} base={base_cap * 100:.0f}% "
            f"→ final={final_cap * 100:.0f}% "
            f"({rl_meta.get('rl_note', 'rl')}: 5d={rl_ctx.weighted_ret_pct:+.2f}%, "
            f"n={rl_ctx.n_closed}, src={'+'.join(rl_ctx.sources) or 'none'})"
        ),
    }
    return final_cap, meta


@dataclass
class EnsembleIntrinsicContext:
    """predictive_regime_ensemble 스냅샷 — 읽기 전용."""

    market: str
    score: float = 0.0
    regime: str = "UNKNOWN"
    raw_regime: str = "UNKNOWN"
    crisis: bool = False
    crisis_synced: bool = False
    factor_states: Dict[str, Optional[float]] = field(default_factory=dict)
    probs: Dict[str, float] = field(default_factory=dict)
    source: str = "missing"  # config | state | external_fallback


def _float_or_none(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_market_block(blk: Any) -> Dict[str, Any]:
    if not isinstance(blk, dict):
        return {}
    fs_raw = blk.get("factor_states") if isinstance(blk.get("factor_states"), dict) else {}
    factor_states: Dict[str, Optional[float]] = {}
    for key in ("short_trend", "long_trend", "vix", "breadth", "pri"):
        factor_states[key] = _float_or_none(fs_raw.get(key))

    probs_raw = blk.get("probs") if isinstance(blk.get("probs"), dict) else {}
    probs = {}
    for k, v in probs_raw.items():
        fv = _float_or_none(v)
        if fv is not None:
            probs[str(k).upper()] = fv

    return {
        "score": _float_or_none(blk.get("score")) or 0.0,
        "regime": str(blk.get("regime") or "UNKNOWN").upper(),
        "raw_regime": str(blk.get("raw_regime") or blk.get("regime") or "UNKNOWN").upper(),
        "crisis": bool(blk.get("crisis", False)),
        "crisis_synced": bool(blk.get("crisis_synced", False)),
        "factor_states": factor_states,
        "probs": probs,
    }


def load_ensemble_intrinsic_context(
    sys_config: Optional[dict[str, Any]],
    market: str,
) -> EnsembleIntrinsicContext:
    """
    REGIME_ENSEMBLE (config SSOT) → 5팩터 내재 컨텍스트.
    시장 블록 없으면 US/KR 교차·state 파일 순으로 방어적 조회.
    """
    mkt = str(market or "US").upper()
    cfg = sys_config if isinstance(sys_config, dict) else {}
    re = cfg.get("REGIME_ENSEMBLE")
    mk_map = re.get("markets") if isinstance(re, dict) else None

    if isinstance(mk_map, dict):
        blk = mk_map.get(mkt) or (mk_map.get("US") if mkt == "KR" else None)
        parsed = _parse_market_block(blk)
        if parsed:
            return EnsembleIntrinsicContext(
                market=mkt,
                source="config",
                **parsed,
            )

    # config 미동기화 시 ensemble state 파일에서 읽기(쓰기·락 없음)
    try:
        from predictive_regime_ensemble import load_state

        st = load_state()
        hyst = st.get("hysteresis") if isinstance(st.get("hysteresis"), dict) else {}
        rec = hyst.get(mkt) if isinstance(hyst.get(mkt), dict) else {}
        if rec.get("current"):
            # state 파일엔 factor_states 가 없을 수 있음 — config 재시도 후 fallback
            pass
    except Exception:
        pass

    return EnsembleIntrinsicContext(market=mkt, source="missing")


def classify_intrinsic_bear_phase(
    ctx: EnsembleIntrinsicContext,
    *,
    defcon_level: int = 5,
) -> Tuple[str, str]:
    """
    5팩터 앙상블 내재 신호로 BEAR 서브타입 분류.

    PANIC > ACCEL > GRIND > NEUTRAL
    외부 raw VIX 는 사용하지 않음 — ensemble vix factor + crisis + regime SSOT.
    """
    score = float(ctx.score)
    fs = ctx.factor_states or {}
    vix_f = _float_or_none(fs.get("vix"))
    short_f = _float_or_none(fs.get("short_trend"))
    long_f = _float_or_none(fs.get("long_trend"))
    breadth_f = _float_or_none(fs.get("breadth"))
    bear_prob = float(ctx.probs.get("BEAR", 0.0) or 0.0)

    try:
        defcon = int(defcon_level)
    except (TypeError, ValueError):
        defcon = 5

    # BEAR_PANIC — 내재 crisis / DEFCON / 극단 vix·score
    if defcon <= 2:
        return "BEAR_PANIC", "defcon_panic"
    if ctx.crisis or ctx.crisis_synced:
        return "BEAR_PANIC", "ensemble_crisis"
    if ctx.regime == "HIGH_VOL" and score <= DOWN_THRESHOLD:
        return "BEAR_PANIC", "high_vol_bear"
    if vix_f is not None and vix_f <= INTRINSIC_VIX_FACTOR_PANIC and score <= DOWN_THRESHOLD:
        return "BEAR_PANIC", "ensemble_vix_factor_panic"
    if score <= ACCEL_SCORE_THRESHOLD and bear_prob >= 0.45 and vix_f is not None and vix_f <= -0.65:
        return "BEAR_PANIC", "bear_prob_vix_stress"

    # BEAR_ACCEL — score · trend · breadth 붕괴
    if score <= ACCEL_SCORE_THRESHOLD:
        return "BEAR_ACCEL", "ensemble_score_accel"
    if (
        short_f is not None
        and long_f is not None
        and short_f <= INTRINSIC_SHORT_TREND_ACCEL
        and long_f <= INTRINSIC_LONG_TREND_ACCEL
    ):
        return "BEAR_ACCEL", "ensemble_trend_accel"
    if breadth_f is not None and breadth_f <= INTRINSIC_BREADTH_COLLAPSE:
        return "BEAR_ACCEL", "ensemble_breadth_collapse"

    # BEAR_GRIND — 완만 하락
    if score <= DOWN_THRESHOLD or ctx.raw_regime == "BEAR" or ctx.regime == "BEAR":
        return "BEAR_GRIND", "ensemble_grind"

    return "NEUTRAL", "ensemble_neutral"


def resolve_intrinsic_base_cap_pct(
    market: str,
    sys_config: Optional[dict[str, Any]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    Axis 1 — 시스템 내재 Base Cap (% of tail fund).

    predictive_regime_ensemble 5팩터 → BEAR_GRIND 20% / ACCEL 35% / PANIC 50%.
    """
    from config_manager import load_system_config

    cfg = sys_config if isinstance(sys_config, dict) else (load_system_config() or {})
    mkt = str(market or "US").upper()
    ctx = load_ensemble_intrinsic_context(cfg, mkt)
    defcon = _defcon_level_from_config(cfg)

    if ctx.source != "missing":
        phase, phase_reason = classify_intrinsic_bear_phase(ctx, defcon_level=defcon)
        cap = cap_pct_for_bear_phase(phase)
        fs = ctx.factor_states or {}
        meta = {
            "market": mkt,
            "engine": "self_evolution_hedge",
            "axis": "intrinsic_panic_scaling",
            "bear_phase": phase,
            "phase_reason": phase_reason,
            "base_cap_pct": round(cap, 4),
            "macro_cap_pct": round(cap, 4),
            "ensemble_score": round(float(ctx.score), 4),
            "ensemble_regime": ctx.regime,
            "ensemble_raw_regime": ctx.raw_regime,
            "ensemble_crisis": ctx.crisis,
            "ensemble_source": ctx.source,
            "factor_short_trend": fs.get("short_trend"),
            "factor_long_trend": fs.get("long_trend"),
            "factor_vix": fs.get("vix"),
            "factor_breadth": fs.get("breadth"),
            "factor_pri": fs.get("pri"),
            "bear_prob": round(float(ctx.probs.get("BEAR", 0.0) or 0.0), 4),
            "defcon_level": defcon,
            "intrinsic": True,
            "summary": (
                f"[내재]{phase} base={cap * 100:.0f}% "
                f"({phase_reason}; score={ctx.score:.2f}, regime={ctx.regime})"
            ),
        }
        return cap, meta

    # 최후 폴백 — ensemble 미동기화 시에만 외부 VIX/DD (rule-based legacy)
    score = ctx.score
    vix = _fetch_vix_level(market=mkt) or _fetch_vix_level(market="US") or 18.0
    dd_20d = _fetch_benchmark_20d_return_pct(mkt)
    phase = classify_bear_stress_phase(score, vix, dd_20d_pct=dd_20d, defcon_level=defcon)
    cap = cap_pct_for_bear_phase(phase)
    meta = {
        "market": mkt,
        "engine": "self_evolution_hedge",
        "axis": "intrinsic_panic_scaling",
        "bear_phase": phase,
        "phase_reason": "external_fallback_no_ensemble",
        "base_cap_pct": round(cap, 4),
        "macro_cap_pct": round(cap, 4),
        "ensemble_score": round(float(score), 4),
        "ensemble_source": "external_fallback",
        "vix_level": round(float(vix), 2),
        "dd_20d_pct": round(float(dd_20d), 2) if dd_20d is not None else None,
        "defcon_level": defcon,
        "intrinsic": False,
        "summary": (
            f"[폴백]{phase} base={cap * 100:.0f}% "
            f"(ensemble 부재; VIX={vix:.1f})"
        ),
    }
    return cap, meta


# ---------------------------------------------------------------------------
# Axis 3 — Asymmetric V-Recovery Kill Switch
# ---------------------------------------------------------------------------
HEDGE_VRECOVERY_LAST_REGIME_KEY = "HEDGE_VRECOVERY_LAST_REGIME"
HEDGE_VRECOVERY_LAST_EVENT_KEY = "HEDGE_VRECOVERY_LAST_EVENT"
HEDGE_RL_RESET_AT_KEY = "HEDGE_RL_RESET_AT"
VRECOVERY_DEFENSIVE_REGIMES = frozenset({"BEAR", "HIGH_VOL"})
ENSEMBLE_BULL_SCORE_THRESHOLD = 0.18
ENSEMBLE_REBOUND_SHORT_TREND = 0.55


def _normalize_regime_key(regime_key: Any) -> str:
    try:
        from meta_state_store import normalize_regime_key

        return normalize_regime_key(regime_key)
    except Exception:
        return str(regime_key or "UNKNOWN").strip().upper()


def resolve_current_meta_regime_key(sys_config: Optional[dict[str, Any]] = None) -> str:
    """META_REGIME_KEY SSOT."""
    try:
        from bear_defense_booster_guard import resolve_meta_regime_key

        return _normalize_regime_key(resolve_meta_regime_key(sys_config))
    except Exception:
        cfg = sys_config if isinstance(sys_config, dict) else {}
        return _normalize_regime_key(cfg.get("CURRENT_REGIME_KEY", "UNKNOWN"))


def load_previous_hedge_regime(sys_config: Optional[dict[str, Any]] = None) -> str:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    return _normalize_regime_key(cfg.get(HEDGE_VRECOVERY_LAST_REGIME_KEY, "UNKNOWN"))


def detect_v_recovery_transition(
    *,
    previous_regime: str,
    current_regime: str,
    sys_config: Optional[dict[str, Any]] = None,
    ensemble_ctx: Optional[EnsembleIntrinsicContext] = None,
) -> Tuple[bool, str]:
    """
    BEAR/HIGH_VOL → BULL(또는 명확한 반등 시그널) 전환 감지.

    트리거 (prev 방어국면 필수):
      1) META_REGIME_KEY → BULL
      2) regime_analog V_RECOVERY unlock
      3) ensemble score ≥ 0.18 (BULL threshold cross)
      4) ensemble short_trend ≥ 0.55 + current 비-BEAR/HIGH_VOL
    """
    prev = _normalize_regime_key(previous_regime)
    cur = _normalize_regime_key(current_regime)
    if prev not in VRECOVERY_DEFENSIVE_REGIMES:
        return False, "prev_not_defensive"

    if cur == "BULL":
        return True, "meta_bear_to_bull"

    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        from bear_defense_booster_guard import is_analog_v_recovery_unlock

        if is_analog_v_recovery_unlock(cfg):
            return True, "analog_v_recovery"
    except Exception:
        pass

    ctx = ensemble_ctx if ensemble_ctx is not None else load_ensemble_intrinsic_context(cfg, "US")
    score = float(ctx.score)
    if score >= ENSEMBLE_BULL_SCORE_THRESHOLD:
        return True, "ensemble_bull_score_cross"

    short_f = _float_or_none((ctx.factor_states or {}).get("short_trend"))
    if (
        short_f is not None
        and short_f >= ENSEMBLE_REBOUND_SHORT_TREND
        and cur not in VRECOVERY_DEFENSIVE_REGIMES
        and cur not in ("", "UNKNOWN")
    ):
        return True, "ensemble_short_trend_rebound"

    return False, "no_v_recovery_signal"


def evaluate_v_recovery_kill_switch(
    sys_config: Optional[dict[str, Any]] = None,
    *,
    market: str = "US",
) -> Dict[str, Any]:
    """Axis 3 평가 — 청산/RL reset 필요 여부 (읽기 전용)."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    prev = load_previous_hedge_regime(cfg)
    cur = resolve_current_meta_regime_key(cfg)
    ctx = load_ensemble_intrinsic_context(cfg, market)
    triggered, reason = detect_v_recovery_transition(
        previous_regime=prev,
        current_regime=cur,
        sys_config=cfg,
        ensemble_ctx=ctx,
    )
    return {
        "triggered": bool(triggered),
        "reason": reason,
        "previous_regime": prev,
        "current_regime": cur,
        "ensemble_score": round(float(ctx.score), 4),
        "ensemble_regime": ctx.regime,
    }


def reset_hedge_rl_after_v_recovery(
    *,
    reason: str = "",
    markets: Tuple[str, ...] = ("KR", "US"),
) -> Dict[str, Any]:
    """V-Recovery 순간 RL amnesia — 과거 인버스 rolling log 전량 삭제."""
    from datetime import datetime

    out: Dict[str, Any] = {"reason": reason, "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    try:
        from live_nav_manager import reset_inverse_sleeve_rl

        out["sleeve_reset"] = reset_inverse_sleeve_rl()
    except Exception as ex:
        out["sleeve_reset_error"] = str(ex)

    try:
        from config_manager import set_config_value

        event = {
            "at": out["at"],
            "reason": str(reason or ""),
            "axis": "v_recovery_kill_switch",
        }
        set_config_value(HEDGE_RL_RESET_AT_KEY, out["at"])
        set_config_value(HEDGE_VRECOVERY_LAST_EVENT_KEY, event)
        out["config_persisted"] = True
    except Exception as ex:
        out["config_persisted"] = False
        out["config_error"] = str(ex)
    _ = markets
    return out


def persist_hedge_regime_snapshot(
    sys_config: Optional[dict[str, Any]] = None,
    *,
    current_regime: Optional[str] = None,
) -> str:
    """사이클 말미 — HEDGE_VRECOVERY_LAST_REGIME 갱신(다음 전환 감지용)."""
    cur = _normalize_regime_key(
        current_regime if current_regime is not None else resolve_current_meta_regime_key(sys_config)
    )
    try:
        from config_manager import set_config_value

        set_config_value(HEDGE_VRECOVERY_LAST_REGIME_KEY, cur)
    except Exception:
        pass
    return cur
