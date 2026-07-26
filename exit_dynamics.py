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
    hit_and_run_index: float = 0.0,
) -> float:
    """
    1차 목표가 도달 시 매도할 비율 F_out ∈ [0.0, 0.99].

    기존 3개 인자 호출은 그대로 동작한다.
    hit_and_run_index가 전달되지 않으면 0.0을 사용한다.

    하드코딩된 국면별 매도비율 대신 다음 상태를 연속 곡선으로 결합한다.

    - Hit-and-Run 지수가 높을수록 더 많이 매도
    - 방어·횡보 국면일수록 더 많이 매도
    - 변동성이 높을수록 더 많이 매도
    - 엣지가 강할수록 러너 보존을 위해 덜 매도
    """
    import math

    def _finite_float(value: Any, default: float) -> float:
        """NaN, 무한대, 잘못된 입력을 안전한 기본값으로 변환한다."""
        try:
            parsed = float(value)
            return parsed if math.isfinite(parsed) else float(default)
        except (TypeError, ValueError):
            return float(default)

    reg = _norm_regime(regime)

    volatility = max(
        0.0,
        _finite_float(volatility_pct, 0.0),
    )
    edge = _finite_float(edge_score, 1.0)
    hri = _clamp(
        _finite_float(hit_and_run_index, 0.0),
        0.0,
        1.0,
    )

    # 국면을 특정 매도비율로 직접 연결하지 않고
    # -1.0~1.0 사이의 연속 계산 입력값으로 변환한다.
    defensive_signal = float(reg in DEFENSIVE_REGIMES)
    bullish_signal = float(reg in BULLISH_REGIMES)
    choppy_signal = float(
        ("CHOP" in reg) or ("SIDEWAYS" in reg)
    )

    regime_pressure = _clamp(
        0.85 * defensive_signal
        + 1.00 * choppy_signal
        - 0.70 * bullish_signal,
        -1.0,
        1.0,
    )

    # tanh를 사용해 극단적인 숫자가 들어와도 계산값이 폭주하지 않게 한다.
    volatility_signal = math.tanh(
        (volatility - 5.0) / 8.0
    )
    edge_signal = math.tanh(
        (edge - 1.0) / 0.75
    )

    # hit_and_run_index의 0~1 범위를 -1~1 범위로 변환한다.
    hit_and_run_signal = 2.0 * hri - 1.0

    # 상태 벡터를 하나의 연속 점수로 결합한다.
    features = (
        1.0,
        hit_and_run_signal,
        regime_pressure,
        volatility_signal,
        edge_signal,
    )
    weights = (
        -0.15,  # 중립 절편
        1.50,   # 이익 반납 학습값
        1.20,   # 방어·횡보 국면 압력
        0.85,   # 변동성 압력
        -0.95,  # 엣지가 강하면 매도비율 감소
    )

    logit = sum(
        weight * feature
        for weight, feature in zip(weights, features)
    )

    # 안정적인 Sigmoid 연속 곡선.
    # 결과는 0에 가까운 값부터 1에 가까운 값까지 부드럽게 움직인다.
    fraction = 0.5 * (
        1.0 + math.tanh(logit / 2.0)
    )

    return round(
        _clamp(fraction, 0.0, 0.99),
        4,
    )

def dynamic_mfe_target_multiplier(
    hit_and_run_index: float,
    *,
    min_multiplier: float = 0.25,
    max_multiplier: float = 1.0,
) -> float:
    """
    Hit-and-Run 지수에 반비례하여 1차 MFE 목표가를 압축한다.

    기본값 기준:
        hit_and_run_index = 0.0 → 승수 1.00
        hit_and_run_index = 1.0 → 승수 0.25

    실제 적용 예:
        adjusted_target = original_target * multiplier
    """
    import math

    def _finite_float(value: Any, default: float) -> float:
        """NaN, 무한대, 잘못된 입력을 안전한 기본값으로 변환한다."""
        try:
            parsed = float(value)
            return parsed if math.isfinite(parsed) else float(default)
        except (TypeError, ValueError):
            return float(default)

    hri = _clamp(
        _finite_float(hit_and_run_index, 0.0),
        0.0,
        1.0,
    )

    parsed_min = _finite_float(
        min_multiplier,
        0.25,
    )
    parsed_max = _finite_float(
        max_multiplier,
        1.0,
    )

    lower = _clamp(
        min(parsed_min, parsed_max),
        0.0,
        1.0,
    )
    upper = _clamp(
        max(parsed_min, parsed_max),
        lower,
        1.0,
    )

    # Smoothstep 연속 곡선:
    # hri가 0과 1 부근에서 목표가가 갑자기 튀지 않고 부드럽게 움직인다.
    compression = (
        hri * hri * (3.0 - 2.0 * hri)
    )

    multiplier = (
        upper
        - (upper - lower) * compression
    )

    return round(
        _clamp(multiplier, lower, upper),
        4,
    )

# ===========================================================================
# Mission 2 — 진화형 볼록성 트레일링 래칫 (Evolutionary Convex Ratchet)
# ===========================================================================
RATCHET_STATE_KEY = "EXIT_RATCHET_STATE"
DEFAULT_RATCHET_STATE: Dict[str, Any] = {
    "kappa_max": 0.12,   # 초기(러너 진입 직후) 트레일 폭 — 넓게 숨통
    "kappa_min": 0.05,   # 수익 팽창 후 최소 트레일 폭 — 이익 보호로 조임
    "anchor_ret": 20.0,  # 20% 수익부터 이익 보호 극대화
    "convexity": 1.0,    # 1.0=선형, >1=볼록, <1=오목
    "curve": "linear",
    "hit_and_run_index": 0.0,
}


def load_ratchet_state(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = dict(DEFAULT_RATCHET_STATE)
    if isinstance(cfg, dict):
        st = cfg.get(RATCHET_STATE_KEY)
        if isinstance(st, dict):
            base.update({k: st[k] for k in st if k in DEFAULT_RATCHET_STATE})

    regime = str(cfg.get("CURRENT_REGIME_KEY", "")).upper() if isinstance(cfg, dict) else "UNKNOWN"

    if "CHOP" in regime or "SIDEWAYS" in regime:
        base["kappa_max"] = min(float(base.get("kappa_max", 0.12)), 0.05)
        base["kappa_min"] = min(float(base.get("kappa_min", 0.05)), 0.02)
        base["anchor_ret"] = min(float(base.get("anchor_ret", 20.0)), 5.0)
    elif "BEAR" in regime or "HIGH_VOL" in regime:
        base["kappa_max"] = min(float(base.get("kappa_max", 0.12)), 0.08)
        base["anchor_ret"] = min(float(base.get("anchor_ret", 20.0)), 10.0)

    return base


def nonlinear_trailing_ratchet_kappa(
    run_ret_pct: float,
    entry_atr: float,
    entry_price: float,
    mfe_atr_mult: float,
    trail_atr_mult: float,
) -> float:
    """
    ATR 유전자와 목표 수익 길이에 따라 비선형 트레일 계수 κ를 계산한다.

    수식
    ----
    anchor_ret = (entry_atr * mfe_atr_mult / entry_price) * 100

    progress = clip(run_ret_pct / anchor_ret, 0, 1)

    장기 추세 유전자일수록 convexity p가 증가:
        u = clip((mfe_atr_mult - 1.5) / (15.0 - 1.5), 0, 1)
        p = 1 + 3 * smoothstep(u)       # p ∈ [1, 4]

    트레일 폭:
        trail_pct = entry_atr * trail_atr_mult / entry_price
        kappa_max = clip(1.5 * trail_pct, 0.02, 0.30)
        kappa_min = clip(0.5 * trail_pct, 0.01, kappa_max)

    최종 래칫:
        κ = kappa_max - (kappa_max - kappa_min) * progress**p

    p > 1이면 초반 progress**p가 작으므로 κ가 천천히 감소하고,
    목표 수익률 부근에서 빠르게 kappa_min으로 수렴한다.
    """
    import math

    values = (
        run_ret_pct,
        entry_atr,
        entry_price,
        mfe_atr_mult,
        trail_atr_mult,
    )

    try:
        values = tuple(float(value) for value in values)
    except (TypeError, ValueError, OverflowError):
        return 0.05

    if not all(math.isfinite(value) for value in values):
        return 0.05

    run_ret, atr, price, mfe_mult, trail_mult = values

    if atr <= 0.0 or price <= 0.0:
        return 0.05

    # 유전자 허용 범위를 벗어난 값은 안전하게 제한한다.
    mfe_mult = _clamp(mfe_mult, 1.5, 15.0)
    trail_mult = _clamp(trail_mult, 0.5, 5.0)
    run_ret = max(0.0, run_ret)

    anchor_ret = (atr * mfe_mult / price) * 100.0
    if anchor_ret <= 1e-12:
        return 0.05

    progress = _clamp(run_ret / anchor_ret, 0.0, 1.0)

    # 장기 목표 유전자일수록 강한 볼록성을 부여한다.
    normalized_mfe = (mfe_mult - 1.5) / (15.0 - 1.5)
    smooth_mfe = normalized_mfe**2 * (3.0 - 2.0 * normalized_mfe)
    convexity = 1.0 + 3.0 * smooth_mfe

    trail_pct = atr * trail_mult / price

    kappa_max = _clamp(1.5 * trail_pct, 0.02, 0.30)
    kappa_min = _clamp(0.5 * trail_pct, 0.01, kappa_max)

    shape = progress**convexity
    kappa = kappa_max - (kappa_max - kappa_min) * shape

    return round(_clamp(kappa, 0.01, 0.30), 6)


def convex_ratchet_kappa(run_ret_pct: float, state: Optional[Dict[str, Any]] = None) -> float:
    """
    러너 수익률(run_ret_pct, 진입 대비 고점 수익 %)에 따른 트레일 계수 κ.
    초반엔 κ_max(넓게 숨통) → anchor_ret 로 갈수록 κ_min(이익 보호로 조임).
    convexity>1 이면 초반을 더 넓게 유지(볼록).
    """
    st = state or DEFAULT_RATCHET_STATE

    e_atr = st.get("entry_atr")
    e_price = st.get("entry_price")
    mfe_mult = st.get("mfe_atr_mult")
    trail_mult = st.get("trail_atr_mult")
    if (
        e_atr is not None
        and e_price is not None
        and mfe_mult is not None
        and trail_mult is not None
    ):
        try:
            e_atr_f = float(e_atr)
            e_price_f = float(e_price)
            mfe_mult_f = float(mfe_mult)
            trail_mult_f = float(trail_mult)
        except (TypeError, ValueError, OverflowError):
            pass
        else:
            import math

            if (
                math.isfinite(e_atr_f)
                and math.isfinite(e_price_f)
                and math.isfinite(mfe_mult_f)
                and math.isfinite(trail_mult_f)
                and e_atr_f > 0.0
                and e_price_f > 0.0
                and mfe_mult_f > 0.0
                and trail_mult_f > 0.0
            ):
                return nonlinear_trailing_ratchet_kappa(
                    run_ret_pct,
                    e_atr_f,
                    e_price_f,
                    mfe_mult_f,
                    trail_mult_f,
                )
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
    주간 RL 상태 업데이트.

    - whipsaw_rate가 높으면 조기 청산이 많으므로 트레일 폭을 넓힌다.
    - giveback_rate가 높으면 이익 반납이 많으므로 트레일 폭을 좁힌다.
    - 두 지표를 이용해 hit_and_run_index를 0.0~1.0 범위로 학습한다.
    """
    import math

    def _finite_float(value: Any, default: float) -> float:
        """NaN, 무한대, 잘못된 입력을 안전한 기본값으로 변환한다."""
        try:
            parsed = float(value)
            return parsed if math.isfinite(parsed) else float(default)
        except (TypeError, ValueError):
            return float(default)

    # 기존 state의 추가 데이터는 보존하고,
    # 누락된 기본 래칫 항목만 기본값으로 채운다.
    st = dict(state) if isinstance(state, dict) else {}

    for key, default_value in DEFAULT_RATCHET_STATE.items():
        st.setdefault(key, default_value)

    w = _clamp(
        _finite_float(whipsaw_rate, 0.0),
        0.0,
        1.0,
    )
    g = _clamp(
        _finite_float(giveback_rate, 0.0),
        0.0,
        1.0,
    )
    learning_rate = _clamp(
        _finite_float(eta, 0.04),
        0.0,
        1.0,
    )

    kappa_max = _finite_float(
        st.get("kappa_max"),
        DEFAULT_RATCHET_STATE["kappa_max"],
    )
    kappa_min = _finite_float(
        st.get("kappa_min"),
        DEFAULT_RATCHET_STATE["kappa_min"],
    )
    convexity = _finite_float(
        st.get("convexity"),
        DEFAULT_RATCHET_STATE["convexity"],
    )

    # 양수이면 조기 청산이 더 많고,
    # 음수이면 이익 반납이 더 많다.
    imbalance = w - g
    delta = learning_rate * imbalance

    kappa_max = _clamp(
        kappa_max + delta,
        0.04,
        0.30,
    )
    kappa_min = _clamp(
        kappa_min + delta * 0.5,
        0.02,
        kappa_max,
    )

    st["kappa_max"] = round(kappa_max, 4)
    st["kappa_min"] = round(kappa_min, 4)

    # 기존의 if/elif 임계 구간 대신 tanh 연속 곡선을 사용한다.
    # 주간 변화량은 최대 ±0.20 범위에서 부드럽게 움직인다.
    convexity_step = (
        0.20 * math.tanh(imbalance / 0.15)
    )
    convexity = _clamp(
        convexity + convexity_step,
        0.5,
        3.0,
    )

    st["convexity"] = round(convexity, 3)

    # curve는 실제 계산 분기가 아니라 사람이 확인하기 위한 상태 이름이다.
    curve_labels = (
        "concave",
        "linear",
        "convex",
    )
    curve_index = (
        int(convexity >= 0.95)
        + int(convexity > 1.05)
    )
    st["curve"] = curve_labels[curve_index]

    # -----------------------------------------------------------------------
    # Hit-and-Run Index 계산
    # -----------------------------------------------------------------------
    # 이익 반납률이 커질수록 비선형적으로 1에 가까워진다.
    giveback_pressure = g ** 1.5

    # 조기 청산 비율이 높다면 이미 청산이 과민할 가능성이 있으므로
    # Hit-and-Run 압력을 조금 완화한다.
    whipsaw_relief = (1.0 - w) ** 1.2

    numerator = giveback_pressure * (
        0.80 + 0.20 * whipsaw_relief
    )
    denominator = (
        numerator + (1.0 - g) ** 1.5
    )

    observed_hit_and_run = _clamp(
        numerator / max(denominator, 1e-12),
        0.0,
        1.0,
    )

    # 이전에 학습된 값을 읽는다.
    previous_hit_and_run = _clamp(
        _finite_float(
            st.get("hit_and_run_index"),
            observed_hit_and_run,
        ),
        0.0,
        1.0,
    )

    # EMA 형태로 갱신해 주간 데이터가 한 번 튀었다고
    # 지수가 갑자기 크게 변하지 않도록 한다.
    adaptation_rate = (
        1.0 - math.exp(-8.0 * learning_rate)
    )

    hit_and_run_index = (
        previous_hit_and_run
        + adaptation_rate
        * (
            observed_hit_and_run
            - previous_hit_and_run
        )
    )

    st["hit_and_run_index"] = round(
        _clamp(hit_and_run_index, 0.0, 1.0),
        6,
    )
    st["whipsaw_rate"] = round(w, 6)
    st["giveback_rate"] = round(g, 6)

    return st

def apply_elastic_dna_to_ratchet(state: Dict[str, Any], trade: Mapping[str, Any], dna_pack: Dict[str, Any]) -> Dict[str, Any]:
    """
    👑 [초월적 진화] DNA에 각인된 고무줄 유전자(trail_atr_mult, mfe_atr_mult)를 
    실제 진입 변동성(entry_atr)에 곱해 래칫 상태(익절 목표 및 손절 조임)를 덮어씌웁니다.
    """
    out = dict(state)
    e_atr = _safe_float(trade.get("entry_atr"))
    e_price = _safe_float(trade.get("entry_price"))
    
    if e_atr <= 0 or e_price <= 0:
        return out
        
    mfe_mult = float(dna_pack.get("mfe_atr_mult", 0.0))
    trail_mult = float(dna_pack.get("trail_atr_mult", 0.0))

    out["entry_atr"] = e_atr
    out["entry_price"] = e_price
    out["mfe_atr_mult"] = mfe_mult
    out["trail_atr_mult"] = trail_mult
    
    # 1. 고무줄 목표가 재설정
    if mfe_mult > 0:
        out["anchor_ret"] = round(((e_atr * mfe_mult) / e_price) * 100.0, 2)
        
    # 2. 고무줄 래칫 강도(트레일링 폭) 재설정
    if trail_mult > 0:
        trail_pct = ((e_atr * trail_mult) / e_price)
        out["kappa_min"] = round(max(0.02, trail_pct * 0.5), 4)
        out["kappa_max"] = round(min(0.25, trail_pct * 1.5), 4)
        
    return out


# ===========================================================================
# Mission 3 — 우측 꼬리 자가 확장 메타튜닝 (Right-Tail Meta-Tuning)
# ===========================================================================
FAT_TAIL_RATIO_MIN = 1.0
FAT_TAIL_RATIO_MAX = 3.0
FAT_TAIL_MIN_SAMPLES = 8
FAT_TAIL_FULL_CONFIDENCE_N = 40
TARGET_PERCENTILE_MAX = 99.0


def _linear_quantile(sorted_values: Sequence[float], probability: float) -> float:
    """외부 통계 패키지 없이 선형 보간 백분위수를 계산한다."""
    import math

    values = list(sorted_values)
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])

    q = _clamp(float(probability), 0.0, 1.0)
    position = (len(values) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))

    if lower == upper:
        return float(values[lower])

    weight = position - lower
    return float(
        values[lower] * (1.0 - weight)
        + values[upper] * weight
    )


def calculate_dynamic_fat_tail_ratio(mfe_list: Sequence[Any]) -> float:
    """
    최근 청산 거래의 MFE(%)로 우측 꼬리 팽창도를 1.0~3.0으로 계산한다.

    강건 통계 구성:
      1) 음수 MFE는 0으로 치환하고 NaN/Inf/비수치 값은 제거한다.
      2) 상위 2.5%에서 winsorize하여 단일 극단값의 영향력을 제한한다.
      3) P90·P95 혼합 꼬리비율, Bowley skewness, Moors kurtosis를 결합한다.
      4) 표본이 40건보다 적으면 1.0 쪽으로 수축(shrinkage)한다.

    Returns
    -------
    float
        1.0 = 꼬리 팽창 근거 없음
        3.0 = 강한 우측 fat-tail
    """
    import math

    clean: List[float] = []
    for raw in mfe_list or []:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        clean.append(max(0.0, value))

    n = len(clean)
    if n < FAT_TAIL_MIN_SAMPLES:
        return FAT_TAIL_RATIO_MIN

    clean.sort()

    # 단일 초대형 MFE가 P95와 첨도 지표를 지배하지 못하도록 상위 2.5% 윈저라이징.
    upper_cap = _linear_quantile(clean, 0.975)
    robust = sorted(min(value, upper_cap) for value in clean)

    p125 = _linear_quantile(robust, 0.125)
    p25 = _linear_quantile(robust, 0.25)
    p375 = _linear_quantile(robust, 0.375)
    p50 = _linear_quantile(robust, 0.50)
    p625 = _linear_quantile(robust, 0.625)
    p75 = _linear_quantile(robust, 0.75)
    p875 = _linear_quantile(robust, 0.875)
    p90 = _linear_quantile(robust, 0.90)
    p95 = _linear_quantile(robust, 0.95)

    iqr = max(0.0, p75 - p25)
    eps = 1e-12

    # -----------------------------------------------------------------------
    # 1. 우측 꼬리 백분위 비율
    # -----------------------------------------------------------------------
    # P95 하나에만 의존하지 않고 P90 60% + P95 40%로 꼬리 앵커를 만든다.
    tail_anchor = 0.60 * p90 + 0.40 * p95

    # 중앙값이 0에 가까운 MFE 분포에서도 분모가 폭발하지 않도록 강건 바닥을 둔다.
    median_floor = max(p50, 0.25 * p75, 0.25)
    percentile_ratio = max(1.0, tail_anchor / median_floor)

    # ratio 1.5 이하는 평범, 8 이상은 극단 꼬리로 포화시키는 로그 스케일.
    log_low = math.log(1.5)
    log_high = math.log(8.0)
    percentile_score = _clamp(
        (math.log(percentile_ratio) - log_low) / (log_high - log_low),
        0.0,
        1.0,
    )

    # -----------------------------------------------------------------------
    # 2. Bowley skewness — 사분위수 기반 강건 비대칭도
    # -----------------------------------------------------------------------
    # (Q3 + Q1 - 2Q2) / (Q3 - Q1), 이론 범위 [-1, 1].
    if iqr <= eps:
        bowley_skewness = 0.0
    else:
        bowley_skewness = (
            p75 + p25 - 2.0 * p50
        ) / iqr
    skew_score = _clamp(bowley_skewness, 0.0, 1.0)

    # -----------------------------------------------------------------------
    # 3. Moors kurtosis — 팔분위수 기반 강건 첨도
    # -----------------------------------------------------------------------
    # 고전적 4차 모멘트 첨도보다 극단값에 훨씬 덜 민감하다.
    if iqr <= eps:
        moors_kurtosis = 1.0
    else:
        moors_kurtosis = (
            (p875 - p625) + (p375 - p125)
        ) / iqr

    # 정규형 분포의 Moors 값(약 1.23) 부근은 0,
    # 2.8 이상이면 강한 첨도로 포화한다.
    kurtosis_score = _clamp(
        (moors_kurtosis - 1.23) / (2.80 - 1.23),
        0.0,
        1.0,
    )

    # 우측 꼬리 자체를 가장 크게 반영하고, 비대칭도와 첨도를 보조 신호로 사용한다.
    raw_energy = _clamp(
        0.60 * percentile_score
        + 0.25 * skew_score
        + 0.15 * kurtosis_score,
        0.0,
        1.0,
    )

    # Smoothstep: 중간 구간은 민감하되 0과 1 근처에서는 변화율을 완만하게 한다.
    smooth_energy = raw_energy * raw_energy * (3.0 - 2.0 * raw_energy)

    # 작은 표본에서 나온 극단적 모양을 전적으로 믿지 않고 1.0 방향으로 수축한다.
    sample_confidence = min(
        1.0,
        math.sqrt(n / float(FAT_TAIL_FULL_CONFIDENCE_N)),
    )

    ratio = FAT_TAIL_RATIO_MIN + (
        FAT_TAIL_RATIO_MAX - FAT_TAIL_RATIO_MIN
    ) * smooth_energy * sample_confidence

    return round(
        _clamp(ratio, FAT_TAIL_RATIO_MIN, FAT_TAIL_RATIO_MAX),
        6,
    )


def update_target_percentile(
    base_percentile: float,
    fat_tail_ratio_value: float,
    *,
    max_percentile: float = TARGET_PERCENTILE_MAX,
) -> float:
    """
    Base Percentile을 fat-tail 강도에 따라 최대 99%까지 부드럽게 확장한다.

        u = clip((FatTailRatio - 1) / 2, 0, 1)
        s(u) = u²(3 - 2u)
        Target = Base + (Max - Base) × s(u)

    FatTailRatio=1이면 Base 유지, 3이면 Max에 도달한다.
    """
    import math

    try:
        base = float(base_percentile)
    except (TypeError, ValueError):
        base = 70.0
    if not math.isfinite(base):
        base = 70.0

    try:
        ratio = float(fat_tail_ratio_value)
    except (TypeError, ValueError):
        ratio = FAT_TAIL_RATIO_MIN
    if not math.isfinite(ratio):
        ratio = FAT_TAIL_RATIO_MIN

    maximum = _clamp(float(max_percentile), 50.0, 99.0)
    base = _clamp(base, 0.0, maximum)
    ratio = _clamp(ratio, FAT_TAIL_RATIO_MIN, FAT_TAIL_RATIO_MAX)

    normalized = (ratio - FAT_TAIL_RATIO_MIN) / (
        FAT_TAIL_RATIO_MAX - FAT_TAIL_RATIO_MIN
    )
    smooth = normalized * normalized * (3.0 - 2.0 * normalized)

    target = base + (maximum - base) * smooth
    return round(_clamp(target, base, maximum), 4)


def target_percentile(regime: Any, fat_tail_ratio: float) -> float:
    """
    국면별 Base Percentile에 강건 우측꼬리 비율을 적용한다.

    기존 호출 형식 target_percentile(regime, fat_tail_ratio)은 그대로 유지된다.
    """
    reg = _norm_regime(regime)
    if reg in BULLISH_REGIMES:
        base = 90.0
    elif reg in DEFENSIVE_REGIMES:
        base = 60.0
    elif "CHOP" in reg or "SIDEWAYS" in reg:
        base = 40.0
    else:
        base = 70.0

    return update_target_percentile(
        base,
        fat_tail_ratio,
        max_percentile=TARGET_PERCENTILE_MAX,
    )


def target_percentile_from_mfe(regime: Any, mfe_list: Sequence[Any]) -> float:
    """MFE 리스트에서 fat-tail 비율 계산과 목표 퍼센타일 갱신을 한 번에 수행한다."""
    ratio = calculate_dynamic_fat_tail_ratio(mfe_list)
    return target_percentile(regime, ratio)


def fat_tail_ratio(p_hi: float, p_mid: float) -> float:
    """
    레거시 호환 함수.

    기존 호출부가 p_hi / p_mid를 직접 전달하는 경우를 깨뜨리지 않되,
    반환값은 새 모델과 동일한 안전 범위 1.0~3.0으로 제한한다.
    신규 코드는 calculate_dynamic_fat_tail_ratio(mfe_list)를 사용한다.
    """
    import math

    try:
        hi = max(0.0, float(p_hi))
        mid = max(0.0, float(p_mid))
    except (TypeError, ValueError):
        return FAT_TAIL_RATIO_MIN

    if not (math.isfinite(hi) and math.isfinite(mid)):
        return FAT_TAIL_RATIO_MIN
    if mid <= 1e-6:
        return FAT_TAIL_RATIO_MIN

    return round(
        _clamp(hi / mid, FAT_TAIL_RATIO_MIN, FAT_TAIL_RATIO_MAX),
        6,
    )


# ===========================================================================
# Mission 4 — 엣지 스코어 연동 자가 증식 (Autonomous Pyramiding)
# ===========================================================================
PYRAMID_EDGE_THRESHOLD = 1.5
PYRAMID_MAX_ADDS = 3
PYRAMID_NAV_CAP_FRAC = 0.04  # 👑 [수술 완료] 0.10 -> 0.04 (1회 추가매수는 NAV 의 최대 4%로 리스크 통제)

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


def mega_trend_internal_thresholds(sector: Optional[str] = None, regime: Optional[str] = None) -> Dict[str, Any]:
    """내재적 킬스위치 1번 임계치 — env + Kill RL delta (P5: sector overlay)."""
    base = _mega_trend_internal_thresholds_base()
    
    # ===========================================================================
    # 👑 [핑퐁 프로토콜 3] 횡보장 이익 반납 강제 차단 (Tight Breakeven)
    # 횡보장(CHOP)에서는 본절(Breakeven Band)의 여유를 주지 않습니다.
    # 1.5% 수익이 났다가 0.5%로 떨어지면 "본절이네" 하고 기다리는 게 아니라,
    # 바로 쳐내고 빠져나옵니다. MFE 도달 목표치도 5.0%에서 2.5%로 압착합니다.
    # ===========================================================================
    reg = str(regime or "").strip().upper()
    if "CHOP" in reg or "SIDEWAYS" in reg:
        base["breakeven_band_pct"] = 0.5   # 본절 밴드를 1.5%에서 0.5%로 극단적 타이트
        base["mfe_target_pct"] = 2.5       # 메가트렌드 목표치를 5.0%에서 2.5%로 압착
    # ===========================================================================

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


def _extract_close_volume_series(
    trade: Mapping[str, Any],
) -> tuple[Optional[List[float]], Optional[List[float]]]:
    """trade 딕셔너리·봉 시퀀스에서 종가/거래량 리스트를 추출한다."""
    for close_key, volume_key in (
        ("close_series", "volume_series"),
        ("closes", "volumes"),
        ("bar_closes", "bar_volumes"),
        ("close_prices", "volume_prices"),
    ):
        raw_close = trade.get(close_key)
        raw_volume = trade.get(volume_key)
        if raw_close is None or raw_volume is None:
            continue
        try:
            close_series = [float(v) for v in raw_close]
            volume_series = [float(v) for v in raw_volume]
        except (TypeError, ValueError):
            continue
        if close_series and volume_series and len(close_series) == len(volume_series):
            return close_series, volume_series

    for bars_key in ("bars", "ohlcv_bars", "bar_history", "recent_bars"):
        bars = trade.get(bars_key)
        if not isinstance(bars, (list, tuple)) or not bars:
            continue

        close_series: List[float] = []
        volume_series: List[float] = []
        for bar in bars:
            if not isinstance(bar, Mapping):
                continue
            close_val = bar.get("close")
            if close_val is None:
                close_val = bar.get("Close")
            if close_val is None:
                close_val = bar.get("c")
            volume_val = bar.get("volume")
            if volume_val is None:
                volume_val = bar.get("Volume")
            if volume_val is None:
                volume_val = bar.get("v")
            if close_val is None or volume_val is None:
                continue
            try:
                close_series.append(float(close_val))
                volume_series.append(float(volume_val))
            except (TypeError, ValueError):
                continue

        if close_series and volume_series and len(close_series) == len(volume_series):
            return close_series, volume_series

    return None, None


def _resolve_volume_divergence_from_trade(
    trade: Mapping[str, Any],
    *,
    lookback: int = 10,
) -> float:
    """종가/거래량 시계열이 부족하거나 계산 실패 시 0.0으로 폴백한다."""
    try:
        close_series, volume_series = _extract_close_volume_series(trade)
        if not close_series or not volume_series:
            return 0.0
        if len(close_series) < lookback or len(volume_series) < lookback:
            return 0.0
        return compute_volume_divergence(
            close_series,
            volume_series,
            lookback=lookback,
        )
    except Exception:
        return 0.0


def _classify_via_dynamic_exit_models(
    trade: Mapping[str, Any],
    *,
    ret: float,
    breakeven_band_pct: float,
) -> Optional[str]:
    """
    Mission 6 — ATR 동적 손절 / 알파 붕괴 익절로 조기 분류.
    필수 키(entry_price, entry_atr 등)가 없거나 계산 실패 시 None → 정적 로직 폴백.
    """
    try:
        entry_price = _safe_float(trade.get("entry_price"))
        entry_atr = _safe_float(trade.get("entry_atr"))
        if entry_price <= 0.0 or entry_atr <= 0.0:
            return None

        bars_raw = trade.get("bars_held")
        try:
            bars_held = int(bars_raw) if bars_raw is not None else 0
        except (TypeError, ValueError):
            bars_held = 0
        if bars_held < 0:
            bars_held = 0

        regime = str(
            trade.get("regime")
            or trade.get("entry_regime")
            or trade.get("sector_regime")
            or "UNKNOWN"
        )
        volume_divergence = _resolve_volume_divergence_from_trade(trade, lookback=10)
        mfe = _safe_float(trade.get("mfe"))
        band = float(breakeven_band_pct)

        current_price = _safe_float(
            trade.get("current_price") or trade.get("exit_price")
        )
        if current_price <= 0.0:
            current_price = entry_price * (1.0 + ret / 100.0)
        if current_price <= 0.0:
            return None

        side = str(trade.get("side") or "LONG").strip().upper()
        if side not in {"LONG", "SHORT"}:
            side = "LONG"

        sl = AtrDynamicStopLoss(entry_price=entry_price, side=side)  # type: ignore[arg-type]

        max_high = _safe_float(trade.get("max_high"))
        max_low = _safe_float(trade.get("max_low"))
        peak_bar = max(0, bars_held - 1) if bars_held > 0 else 0

        if side == "LONG" and max_high > entry_price:
            sl.update_and_check(max_high, entry_atr, peak_bar, regime)
        elif side == "SHORT" and max_low > 0.0 and max_low < entry_price:
            sl.update_and_check(max_low, entry_atr, peak_bar, regime)

        _, should_stop = sl.update_and_check(
            current_price, entry_atr, bars_held, regime
        )

        alpha = AlphaDecayTakeProfit()
        _, should_take_profit = alpha.evaluate_decay(
            peak_mfe=mfe,
            current_profit=ret,
            volume_divergence=volume_divergence,
        )

        if should_stop:
            if -band <= ret <= band:
                return "bounce_stop"
            return "loss"

        if should_take_profit:
            if -band <= ret <= band:
                return "bounce_stop"
            if ret > band:
                return "win"
            if ret > 0.0:
                return "win"
            return "bounce_stop"

    except Exception:
        return None

    return None


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

    band = float(breakeven_band_pct)
    ret = sim_ret if status == "OPEN" else _safe_float(final_ret, sim_ret)

    dynamic_outcome = _classify_via_dynamic_exit_models(
        trade,
        ret=ret,
        breakeven_band_pct=band,
    )
    if dynamic_outcome is not None:
        if status == "OPEN":
            if dynamic_outcome == "loss":
                return "loss"
            return "open_live"
        return dynamic_outcome

    if status == "OPEN":
        if sim_ret > breakeven_band_pct:
            return "open_live"
        if sim_ret <= -breakeven_band_pct:
            return "loss"
        return "open_live"

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
    dna_pack: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    MFE(최대 허용 수익) 목표 도달 여부. 
    👑 고정된 %가 아닌, 템플릿의 고무줄 유전자(ATR 배수)가 있다면 그것을 우선하여 환산합니다.
    """
    target = float(mfe_target_pct)
    
    # DNA팩이 존재하고 변동성 배수 유전자가 발현되어 있다면 % 타겟을 덮어씁니다.
    if dna_pack and float(dna_pack.get("mfe_atr_mult", 0.0)) > 0:
        e_atr = _safe_float(trade.get("entry_atr"))
        e_price = _safe_float(trade.get("entry_price"))
        if e_atr > 0 and e_price > 0:
            target = ((e_atr * float(dna_pack["mfe_atr_mult"])) / e_price) * 100.0

    mfe = _safe_float(trade.get("mfe"))
    if mfe >= target:
        return True
        
    if str(trade.get("status") or "").upper() == "OPEN":
        entry = _safe_float(trade.get("entry_price"))
        max_high = _safe_float(trade.get("max_high"))
        if entry > 0 and max_high > 0:
            run_mfe = ((max_high - entry) / entry) * 100.0
            return run_mfe >= target
            
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



# ===========================================================================
# [초월적 방어] 국면 연동형 유동적 타임 스탑 (Fluid EOD Close)
# ===========================================================================
def resolve_eod_exit_time(bear_stress_phase: str) -> str:
    """
    dynamic_hedge_cap.py에서 분류된 하락장 강도(PANIC, ACCEL, GRIND) 및 
    총사령관의 범용 국면(BEAR)에 맞춰 오버나이트 리스크를 차단합니다.
    """
    phase = str(bear_stress_phase).strip().upper()

    # 1. 패닉장: 오후 투매 폭포수가 쏟아지기 전, 가장 빠르게 전량 현금화 (14:00)
    if phase == "BEAR_PANIC":
        return "14:00:00"
        
    # 2. 가속 하락장 및 범용 하락장(BEAR): 기관의 포트폴리오 조정 매물이 나오기 직전 회수 (14:30)
    elif phase in ["BEAR_ACCEL", "BEAR"]:
        return "14:30:00"
        
    # 3. 완만 하락장 / 고변동성장: 장중 알파를 최대한 취한 뒤 동시호가 전 회수 (15:15)
    elif phase in ["BEAR_GRIND", "HIGH_VOL", "DEFENSE"]:
        return "15:15:00"
        
    # 4. 중립 / 상승장 (BULL): EOD 강제 청산을 발동하지 않거나, 폴백(Fallback) 시간 적용
    else:
        return "15:20:00"


# ===========================================================================
# Mission 6 — 실시간 ATR·시간감쇠 동적 손절 / 알파붕괴 익절
# ===========================================================================
import math as _math
from dataclasses import dataclass as _dataclass, field as _field
from typing import Literal as _Literal, Tuple as _Tuple


_PositionSide = _Literal["LONG", "SHORT"]


def _finite_float(name: str, value: float) -> float:
    """실시간 리스크 계산에 NaN/inf가 침투하지 않도록 유한 실수만 허용한다."""
    x = float(value)
    if not _math.isfinite(x):
        raise ValueError(f"{name} must be finite, got {value!r}")
    return x


@_dataclass(slots=True)
class AtrDynamicStopLoss:
    """
    단일 Tick/Bar 단위 ATR + Time-Decay 동적 손절 엔진.

    기본 손절 거리:
        D_t = ATR_t × m_regime × exp(-lambda_regime × bars_held)

    단, 시간이 매우 길어졌을 때 D_t -> 0으로 붕괴하여 미세 노이즈에도
    즉시 청산되는 것을 막기 위해 ATR 배수 하한 m_min을 적용한다.

        effective_mult_t = max(m_min, m_regime × exp(-lambda_regime × bars_held))
        D_t = ATR_t × effective_mult_t

    Regime 조정:
        HIGH_VOL: m_regime = base_multiplier × 1.5
        CHOP:     lambda_regime = base_lambda × 2.0

    LONG:
        best_t = max(best_{t-1}, current_price)
        candidate_stop_t = best_t - D_t
        stop_t = max(stop_{t-1}, candidate_stop_t)  # 절대 완화하지 않는 래칫

    SHORT:
        best_t = min(best_{t-1}, current_price)
        candidate_stop_t = best_t + D_t
        stop_t = min(stop_{t-1}, candidate_stop_t)
    """

    entry_price: float
    side: _PositionSide = "LONG"
    atr_multiplier: float = 3.0
    decay_lambda: float = 0.03
    min_atr_multiplier: float = 0.50
    high_vol_multiplier: float = 1.50
    chop_decay_multiplier: float = 2.00

    _best_price: float = _field(init=False, repr=False)
    _stop_price: float | None = _field(default=None, init=False, repr=False)
    _last_bars_held: int = _field(default=-1, init=False, repr=False)

    def __post_init__(self) -> None:
        self.entry_price = _finite_float("entry_price", self.entry_price)
        if self.entry_price <= 0.0:
            raise ValueError("entry_price must be > 0")

        normalized_side = str(self.side).strip().upper()
        if normalized_side not in {"LONG", "SHORT"}:
            raise ValueError("side must be 'LONG' or 'SHORT'")
        self.side = normalized_side  # type: ignore[assignment]

        self.atr_multiplier = _finite_float("atr_multiplier", self.atr_multiplier)
        self.decay_lambda = _finite_float("decay_lambda", self.decay_lambda)
        self.min_atr_multiplier = _finite_float(
            "min_atr_multiplier", self.min_atr_multiplier
        )
        self.high_vol_multiplier = _finite_float(
            "high_vol_multiplier", self.high_vol_multiplier
        )
        self.chop_decay_multiplier = _finite_float(
            "chop_decay_multiplier", self.chop_decay_multiplier
        )

        if self.atr_multiplier <= 0.0:
            raise ValueError("atr_multiplier must be > 0")
        if self.decay_lambda < 0.0:
            raise ValueError("decay_lambda must be >= 0")
        if not 0.0 < self.min_atr_multiplier <= self.atr_multiplier:
            raise ValueError(
                "min_atr_multiplier must be > 0 and <= atr_multiplier"
            )
        if self.high_vol_multiplier < 1.0:
            raise ValueError("high_vol_multiplier must be >= 1")
        if self.chop_decay_multiplier < 1.0:
            raise ValueError("chop_decay_multiplier must be >= 1")

        self._best_price = self.entry_price

    @property
    def stop_price(self) -> float | None:
        """아직 update 전이면 None, 이후에는 현재 래칫 손절가를 반환한다."""
        return self._stop_price

    @property
    def best_price(self) -> float:
        """LONG은 보유 중 최고가, SHORT는 보유 중 최저가."""
        return self._best_price

    def reset(self, entry_price: float | None = None) -> None:
        """동일 객체를 새 포지션에 재사용할 때 내부 상태를 초기화한다."""
        if entry_price is not None:
            new_entry = _finite_float("entry_price", entry_price)
            if new_entry <= 0.0:
                raise ValueError("entry_price must be > 0")
            self.entry_price = new_entry

        self._best_price = self.entry_price
        self._stop_price = None
        self._last_bars_held = -1

    def _distance(self, atr: float, bars_held: int, regime: str) -> float:
        reg = str(regime or "UNKNOWN").strip().upper()

        regime_multiplier = self.atr_multiplier
        if "HIGH_VOL" in reg:
            regime_multiplier *= self.high_vol_multiplier

        regime_lambda = self.decay_lambda
        if "CHOP" in reg:
            regime_lambda *= self.chop_decay_multiplier

        decayed_multiplier = regime_multiplier * _math.exp(
            -regime_lambda * float(bars_held)
        )
        effective_multiplier = max(self.min_atr_multiplier, decayed_multiplier)
        return atr * effective_multiplier

    def update_and_check(
        self,
        current_price: float,
        atr: float,
        bars_held: int,
        regime: str,
    ) -> _Tuple[float, bool]:
        """
        현재 Tick/Bar를 반영하고 ``(stop_price, should_exit)``를 반환한다.

        ``bars_held``는 동일 포지션 안에서 단조 증가해야 한다. 과거 Bar가 뒤늦게
        들어와 손절폭이 다시 넓어지는 데이터 순서 오류를 조기에 차단한다.
        """
        price = _finite_float("current_price", current_price)
        atr_value = _finite_float("atr", atr)

        if price <= 0.0:
            raise ValueError("current_price must be > 0")
        if atr_value <= 0.0:
            raise ValueError("atr must be > 0")
        if isinstance(bars_held, bool) or not isinstance(bars_held, int):
            raise TypeError("bars_held must be an int")
        if bars_held < 0:
            raise ValueError("bars_held must be >= 0")
        if bars_held < self._last_bars_held:
            raise ValueError(
                "bars_held cannot decrease within the same position; call reset()"
            )
        self._last_bars_held = bars_held

        distance = self._distance(atr_value, bars_held, regime)

        if self.side == "LONG":
            self._best_price = max(self._best_price, price)
            candidate_stop = self._best_price - distance
            self._stop_price = (
                candidate_stop
                if self._stop_price is None
                else max(self._stop_price, candidate_stop)
            )
            should_exit = price <= self._stop_price
        else:
            self._best_price = min(self._best_price, price)
            candidate_stop = self._best_price + distance
            self._stop_price = (
                candidate_stop
                if self._stop_price is None
                else min(self._stop_price, candidate_stop)
            )
            should_exit = price >= self._stop_price

        return float(self._stop_price), bool(should_exit)


@_dataclass(slots=True)
class AlphaDecayTakeProfit:
    """
    MFE 이익 반납률과 거래량 약화 지수를 융합한 알파 붕괴 확률 모델.

    1) MFE 대비 이익 반납률:

        giveback = clip((peak_mfe - current_profit) / peak_mfe, 0, max_giveback)

    2) 특징 결합:

        x = w_mfe × giveback + w_volume × volume_divergence

    3) Logistic 확률:

        P(decay) = 1 / (1 + exp(-k × (x - x0)))

       P(decay) >= trigger_probability이면 익절/청산 신호가 True다.
    """

    k: float = 10.0
    x0: float = 0.50
    mfe_weight: float = 0.70
    volume_weight: float = 0.30
    trigger_probability: float = 0.80
    min_peak_mfe: float = 1e-6
    max_giveback_ratio: float = 2.0

    def __post_init__(self) -> None:
        self.k = _finite_float("k", self.k)
        self.x0 = _finite_float("x0", self.x0)
        self.mfe_weight = _finite_float("mfe_weight", self.mfe_weight)
        self.volume_weight = _finite_float("volume_weight", self.volume_weight)
        self.trigger_probability = _finite_float(
            "trigger_probability", self.trigger_probability
        )
        self.min_peak_mfe = _finite_float("min_peak_mfe", self.min_peak_mfe)
        self.max_giveback_ratio = _finite_float(
            "max_giveback_ratio", self.max_giveback_ratio
        )

        if self.k <= 0.0:
            raise ValueError("k must be > 0")
        if self.mfe_weight < 0.0 or self.volume_weight < 0.0:
            raise ValueError("feature weights must be >= 0")
        if self.mfe_weight + self.volume_weight <= 0.0:
            raise ValueError("at least one feature weight must be > 0")
        if not 0.0 < self.trigger_probability < 1.0:
            raise ValueError("trigger_probability must be in (0, 1)")
        if self.min_peak_mfe <= 0.0:
            raise ValueError("min_peak_mfe must be > 0")
        if self.max_giveback_ratio <= 0.0:
            raise ValueError("max_giveback_ratio must be > 0")

    @staticmethod
    def _stable_logistic(z: float) -> float:
        """큰 |z|에서도 exp overflow가 발생하지 않는 수치 안정형 sigmoid."""
        if z >= 0.0:
            return 1.0 / (1.0 + _math.exp(-z))
        exp_z = _math.exp(z)
        return exp_z / (1.0 + exp_z)

    def evaluate_decay(
        self,
        peak_mfe: float,
        current_profit: float,
        volume_divergence: float,
    ) -> _Tuple[float, bool]:
        """``(decay_probability, should_take_profit)``를 반환한다."""
        peak = _finite_float("peak_mfe", peak_mfe)
        current = _finite_float("current_profit", current_profit)
        volume = _finite_float("volume_divergence", volume_divergence)

        if peak <= self.min_peak_mfe:
            giveback_ratio = 0.0
        else:
            raw_giveback = (peak - current) / peak
            giveback_ratio = max(
                0.0, min(self.max_giveback_ratio, raw_giveback)
            )

        volume_score = max(0.0, min(1.0, volume))

        weight_sum = self.mfe_weight + self.volume_weight
        x = (
            self.mfe_weight * giveback_ratio
            + self.volume_weight * volume_score
        ) / weight_sum

        z = self.k * (x - self.x0)
        probability = self._stable_logistic(z)
        should_take_profit = probability >= self.trigger_probability
        return float(probability), bool(should_take_profit)


# ===========================================================================
# Mission 6b — 거래량 약화 지표 (Volume Divergence, 순수 math · 무 I/O)
# ===========================================================================
def _smoothstep(u: float) -> float:
    """[0, 1] smoothstep: u²(3 - 2u)."""
    import math

    x = max(0.0, min(1.0, float(u)))
    if not math.isfinite(x):
        return 0.0
    return x * x * (3.0 - 2.0 * x)


def _stable_price_position(
    close_series: Sequence[float],
    lookback: int,
) -> float:
    """
    lookback 구간에서 마지막 종가의 상대 위치 [0, 1].
    0 = 구간 최저, 1 = 구간 최고.
    """
    import math

    if lookback < 2:
        return 0.0

    window = list(close_series)[-lookback:]
    clean: List[float] = []
    for value in window:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed):
            clean.append(parsed)

    if len(clean) < 2:
        return 0.0

    lo = min(clean)
    hi = max(clean)
    last = clean[-1]
    span = hi - lo
    eps = 1e-12
    if span <= eps:
        return 0.5

    return max(0.0, min(1.0, (last - lo) / span))


def _stable_volume_ratio(
    volume_series: Sequence[float],
    lookback: int,
) -> float:
    """
    최근 봉 거래량 / 직전 lookback-1 평균 거래량.
    수치 안정성을 위해 분모에 epsilon을 둔다.
    """
    import math

    if lookback < 2:
        return 0.0

    window = list(volume_series)[-lookback:]
    clean: List[float] = []
    for value in window:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed) and parsed >= 0.0:
            clean.append(parsed)

    if len(clean) < 2:
        return 0.0

    recent = clean[-1]
    prior = clean[:-1]
    avg_prior = sum(prior) / len(prior)
    eps = 1e-12
    if avg_prior <= eps:
        return 1.0 if recent > eps else 0.0

    ratio = recent / avg_prior
    if not math.isfinite(ratio):
        return 0.0
    return max(0.0, ratio)


def compute_volume_divergence(
    close_series: Sequence[float],
    volume_series: Sequence[float],
    lookback: int = 10,
) -> float:
    """
    가격 고점 대비 거래량 약화(negative volume divergence) 지수 [0, 1].

    가격이 lookback 구간 상단에 위치하는데 거래량이 평균 대비 약하면
    높은 divergence score를 반환한다.
    """
    if lookback < 2:
        return 0.0

    closes = list(close_series) if close_series is not None else []
    volumes = list(volume_series) if volume_series is not None else []
    if len(closes) < lookback or len(volumes) < lookback:
        return 0.0

    price_pos = _stable_price_position(closes, lookback)
    volume_ratio = _stable_volume_ratio(volumes, lookback)

    # ratio=1.0 -> 중립, ratio>=2.0 -> 강한 거래량, ratio<=0.5 -> 약화.
    volume_strength = _smoothstep(min(1.0, volume_ratio / 2.0))
    raw_divergence = price_pos * (1.0 - volume_strength)
    return _smoothstep(raw_divergence)