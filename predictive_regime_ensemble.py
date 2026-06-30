"""
Predictive Auto-Evolving Regime Ensemble — 예측형 자율 진화 앙상블 국면 엔진.

파편화된 3개 장세 판별기(결정트리 ①·메타거버너 ②·자율조율 ③)를 하나의 **스코어링 앙상블**로 통합한다.
인간이 가중치를 고정하지 않는다. 모든 지표(팩터)가 투표권을 갖고, 기계가 '미래 예측력'을 매일
채점해 가중치를 자가 진화시키되, 거시 지표를 망각하지 않도록 안전망(Macro Anchor Floor)을 건다.

핵심 설계
- [Mission 1] 팩터: short_trend(단기추세) · long_trend(장기추세) · vix(변동성) · breadth(시장폭) · pri(내부선행).
  각 팩터는 상태값 s_f ∈ [-1,+1] 을 내고, 가중합 Σ w_f·s_f → 확률(softmax) → BULL/SIDEWAYS/BEAR.
- [Mission 2] 가중치 강화학습: '팩터의 과거 국면 판정' × '실제 미래 5일 PnL' 보상으로 skill EMA 갱신 →
  softmax 로 가중치 재산출. **Macro Anchor Floor:** (long_trend+vix) 합계 ≥ 15% 락. **PRI Cap:** ≤ 85%.
- [Mission 3] PRI 라이브 편입: 외부지수 횡보라도 내부 PRI 가 끓으면 PRI 가중치가 최대 85%까지 올라
  '선제적 BULL' 선언 가능.
- [Mission 4] KR/US 독립 산출 + 위기 시(미국 VIX 패닉) 한국 강제 동기화(비대칭 오버라이드) + N일 히스테리시스.

상태 SSOT: factory_data_dir()/predictive_regime_ensemble_state.json (원자적 저장).
환경변수 REGIME_ENSEMBLE_STATE_PATH 로 경로 override 가능(테스트).
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from factory_data_paths import factory_data_dir
except Exception:  # pragma: no cover
    def factory_data_dir() -> str:  # type: ignore[misc]
        d = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")
        os.makedirs(d, exist_ok=True)
        return d


logger = logging.getLogger("predictive_regime_ensemble")


STATE_FILENAME = "predictive_regime_ensemble_state.json"

# ── 팩터 정의 ──────────────────────────────────────────────────────────────
FACTORS: Tuple[str, ...] = ("short_trend", "long_trend", "vix", "breadth", "pri")
MACRO_ANCHOR_FACTORS: Tuple[str, ...] = ("long_trend", "vix")  # 전통 거시 닻
MACRO_ANCHOR_FLOOR = 0.15   # (long_trend+vix) 가중치 합 하한 (파멸적 망각 차단)
PRI_WEIGHT_CAP = 0.85       # PRI 단일 가중치 상한 (선제적 BULL 허용 + 폭주 방지)

# ── 판정 임계 ──────────────────────────────────────────────────────────────
UP_THRESHOLD = 0.18
DOWN_THRESHOLD = -0.18
SOFTMAX_BETA = 6.0          # 확률 첨예도

# ── VIX ────────────────────────────────────────────────────────────────────
VIX_MID = 18.0
VIX_SCALE = 8.0
VIX_PANIC_ABS = 30.0        # 절대 패닉(위기 오버라이드 트리거)

# ── 코인 선행 레이더(canary) 소프트 흡수 ────────────────────────────────────
#   코인(24×7) 유동성/전염 신호를 VIX 팩터 상태에만 소프트 가중. 하드 위기
#   오버라이드(is_vix_crisis)는 절대 건드리지 않는다(원본 vix만 사용).
#   기본 ACTIVE: CRYPTO_CANARY_PENALTY_ENABLED=1 → 게이트 통과 시 실제 차감(롤백 0).
CRYPTO_STRESS_GATE = 0.8          # 이 스트레스 이상 + 전염 True 일 때만 발동
CRYPTO_VIX_PENALTY_MAX = 0.25     # st["vix"] 에 더할 하방 압력 상한
CANARY_MAX_STALE_SEC = 5400.0     # 90분 초과 노후 canary 는 무시(코인 다운 오염 차단)

# ── 진화/히스테리시스 ───────────────────────────────────────────────────────
FWD_EVAL_DAYS = 5           # 미래 PnL 관측 창(보상)
SKILL_EMA_BETA = 0.20       # skill EMA 학습률
SOFTMAX_WEIGHT_TEMP = 2.2   # skill→weight softmax 온도
PNL_REWARD_SCALE = 4.0      # |5일 PnL%| 정규화 스케일
DEFAULT_HYSTERESIS_DAYS = 2 # 초기 히스테리시스(콜드스타트). 이후 RL 로 자가 진화.

# ── [Mission 1] RL 동적 히스테리시스 ────────────────────────────────────────
#   전환 직후 FWD_EVAL_DAYS 의 실측 PnL 로 EMA 갱신. 손실(Whipsaw)→증가(방어),
#   이익(전환이 옳고 늦었을 수 있음)→감소(민첩). 범위 1~5일에 클램프.
HYSTERESIS_MIN_DAYS = 1
HYSTERESIS_MAX_DAYS = 5
HYSTERESIS_EMA_LR = 0.5     # 1회 평가당 최대 ±0.5일 이동(EMA-step)
WHIPSAW_PNL_SCALE = 4.0     # 전환 후 PnL% → 보정방향 tanh 정규화 스케일

# ── [Mission 2] 켈리 클러치 (앙상블 확신도 기반 관망) ───────────────────────
#   1위 국면 확률 < 임계치면 변곡점(혼조세)으로 보고 글로벌 켈리를 기하급수 축소.
CLUTCH_PROB_THRESHOLD = 0.60  # 1위 softmax 확률 임계
CLUTCH_MULT_CEIL = 0.30       # 임계 직하(약한 불확실)일 때 상한
CLUTCH_MULT_FLOOR = 0.10      # 완전 혼조(최대 불확실)일 때 하한
_PROB_BASE = 1.0 / 3.0        # 3국면 균등 확률(최대 불확실 기준점)

_LOCK = threading.RLock()


# ===========================================================================
# 상태 입출력
# ===========================================================================
def state_path() -> str:
    env = os.environ.get("REGIME_ENSEMBLE_STATE_PATH")
    if env:
        return env
    return os.path.join(factory_data_dir(), STATE_FILENAME)


def _default_state() -> Dict[str, Any]:
    eq = round(1.0 / len(FACTORS), 6)
    return {
        "schema": "predictive_regime_ensemble.v2",
        "weights": {f: eq for f in FACTORS},
        "skill": {f: 0.0 for f in FACTORS},
        "hysteresis_days": DEFAULT_HYSTERESIS_DAYS,        # 정수(소비/표시용)
        "hysteresis_days_f": float(DEFAULT_HYSTERESIS_DAYS),  # RL 연속값(SSOT)
        "pending_obs": [],
        "history": [],
        "hysteresis": {},          # market -> {current, candidate, streak}
        "transitions_pending": [],  # 전환 이벤트(미성숙) — RL 히스테리시스 평가 대기
        "transitions_history": [],  # 전환 평가 로그
        "clutch": {"active": False, "mult": 1.0, "top_prob": None, "as_of": None},
        "updated_at": None,
    }


def load_state() -> Dict[str, Any]:
    p = state_path()
    if not os.path.isfile(p):
        return _default_state()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state()
        base = _default_state()
        base.update({k: v for k, v in data.items() if v is not None})
        if not isinstance(base.get("weights"), dict):
            base["weights"] = _default_state()["weights"]
        if not isinstance(base.get("skill"), dict):
            base["skill"] = _default_state()["skill"]
        for f in FACTORS:
            base["weights"].setdefault(f, 0.0)
            base["skill"].setdefault(f, 0.0)
        for key in ("pending_obs", "history", "transitions_pending", "transitions_history"):
            if not isinstance(base.get(key), list):
                base[key] = []
        if not isinstance(base.get("hysteresis"), dict):
            base["hysteresis"] = {}
        if not isinstance(base.get("clutch"), dict):
            base["clutch"] = {"active": False, "mult": 1.0, "top_prob": None, "as_of": None}
        # 동적 히스테리시스 연속값 보정(구버전 상태 호환) + 1~5일 클램프
        try:
            hf = float(base.get("hysteresis_days_f", base.get("hysteresis_days", DEFAULT_HYSTERESIS_DAYS)))
        except (TypeError, ValueError):
            hf = float(DEFAULT_HYSTERESIS_DAYS)
        hf = min(float(HYSTERESIS_MAX_DAYS), max(float(HYSTERESIS_MIN_DAYS), hf))
        base["hysteresis_days_f"] = round(hf, 4)
        base["hysteresis_days"] = int(round(hf))
        base["weights"] = project_weights(base["weights"])
        return base
    except (OSError, json.JSONDecodeError, ValueError):
        return _default_state()


def save_state(state: Dict[str, Any]) -> bool:
    p = state_path()
    state = dict(state)
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with _LOCK:
            os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".pre_", suffix=".json", dir=os.path.dirname(p) or ".")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                os.replace(tmp, p)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
        return True
    except OSError:
        return False


# ===========================================================================
# 가중치 제약 투영 (Macro Anchor Floor + PRI Cap)
# ===========================================================================
def project_weights(raw: Dict[str, float]) -> Dict[str, float]:
    """비음수·합=1 정규화 후 PRI≤0.85, (long_trend+vix)≥0.15 제약을 반복 투영."""
    w = {f: max(0.0, float(raw.get(f, 0.0))) for f in FACTORS}
    s = sum(w.values())
    if s <= 0:
        w = {f: 1.0 / len(FACTORS) for f in FACTORS}
    else:
        w = {f: v / s for f, v in w.items()}

    for _ in range(8):
        # PRI 상한
        if w["pri"] > PRI_WEIGHT_CAP:
            excess = w["pri"] - PRI_WEIGHT_CAP
            w["pri"] = PRI_WEIGHT_CAP
            others = [f for f in FACTORS if f != "pri"]
            so = sum(w[f] for f in others)
            if so > 0:
                for f in others:
                    w[f] += excess * (w[f] / so)
            else:
                for f in others:
                    w[f] += excess / len(others)
        # 매크로 닻 하한
        macro = w["long_trend"] + w["vix"]
        if macro < MACRO_ANCHOR_FLOOR - 1e-9:
            deficit = MACRO_ANCHOR_FLOOR - macro
            non = [f for f in FACTORS if f not in MACRO_ANCHOR_FACTORS]
            sn = sum(w[f] for f in non)
            if sn > 0:
                scale = max(0.0, (sn - deficit) / sn)
                for f in non:
                    w[f] *= scale
            if macro > 0:
                for f in MACRO_ANCHOR_FACTORS:
                    w[f] += deficit * (w[f] / macro)
            else:
                for f in MACRO_ANCHOR_FACTORS:
                    w[f] += deficit / len(MACRO_ANCHOR_FACTORS)
        # 재정규화
        s = sum(w.values()) or 1.0
        w = {f: v / s for f, v in w.items()}
    return {f: round(w[f], 6) for f in FACTORS}


def _softmax(vals: Dict[str, float], temp: float) -> Dict[str, float]:
    if not vals:
        return {}
    mx = max(vals.values())
    exps = {k: math.exp((v - mx) / max(1e-6, temp)) for k, v in vals.items()}
    z = sum(exps.values()) or 1.0
    return {k: v / z for k, v in exps.items()}


def weights_from_skill(skill: Dict[str, float]) -> Dict[str, float]:
    """skill EMA → softmax → 제약 투영. 콜드스타트(전부 0)면 균등에서 출발."""
    raw = _softmax({f: float(skill.get(f, 0.0)) for f in FACTORS}, SOFTMAX_WEIGHT_TEMP)
    return project_weights(raw)


# ===========================================================================
# 팩터 상태 스코어링 (Mission 1)
# ===========================================================================
@dataclass
class FactorSnapshot:
    """한 시장의 팩터 원자료. 결측은 None (해당 팩터는 투표에서 자동 제외)."""
    close: Optional[float] = None
    ma20: Optional[float] = None
    ma200: Optional[float] = None
    vix: Optional[float] = None
    vix_p90: Optional[float] = None
    breadth_ratio: Optional[float] = None  # (RSP/SPY) / 50d avg, 시장 폭
    pri_z: Optional[float] = None          # 내부 PRI composite_z (~[-1,1])
    crypto_liquidity_stress: float = 0.0   # [canary] 0.0~1.0 (결측/노후 시 0)
    macro_contagion_risk: bool = False     # [canary] BTC·VIX 동기화 위험회피


def _t(x: float) -> float:
    return float(math.tanh(x))


def _canary_penalty_enabled() -> bool:
    """ACTIVE(기본 1): 이중 게이트 통과 시 VIX 팩터에 실제 페널티 차감.
    롤백이 필요하면 CRYPTO_CANARY_PENALTY_ENABLED=0 으로 SHADOW(로그만)로 되돌린다."""
    return str(os.environ.get("CRYPTO_CANARY_PENALTY_ENABLED", "1")).strip().lower() in (
        "1", "true", "yes", "on",
    )


def _crypto_vix_penalty(snap: "FactorSnapshot") -> float:
    """이중 게이트(stress≥GATE AND contagion) 통과 시 0.8→1.0 선형 램프 × 상한.

    게이트 미충족이면 0.0. enable 여부와 무관하게 '계산'은 항상 수행(shadow 로그용).
    """
    try:
        stress = float(snap.crypto_liquidity_stress or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if not snap.macro_contagion_risk or stress < CRYPTO_STRESS_GATE:
        return 0.0
    ramp = min(1.0, (stress - CRYPTO_STRESS_GATE) / max(1e-9, (1.0 - CRYPTO_STRESS_GATE)))
    return float(CRYPTO_VIX_PENALTY_MAX * ramp)


def _load_crypto_canary() -> Tuple[float, bool]:
    """코인 canary JSON 을 비동기·방어적으로 읽는다(주식 DB 무접촉).

    실패/노후(90분 초과)면 (0.0, False) → 국면 로직 영향 0(graceful).
    """
    try:
        path = (os.environ.get("BITGET_CANARY_STATE_PATH") or "").strip()
        if not path:
            try:
                from bitget.infra.data_paths import canary_state_path

                path = canary_state_path()
            except Exception:
                return 0.0, False
        if not path or not os.path.isfile(path):
            return 0.0, False
        if (time.time() - os.path.getmtime(path)) > CANARY_MAX_STALE_SEC:
            return 0.0, False
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        if not isinstance(d, dict):
            return 0.0, False
        stress = float(d.get("crypto_liquidity_stress") or 0.0)
        contagion = bool(d.get("macro_contagion_risk"))
        return max(0.0, min(1.0, stress)), contagion
    except Exception:
        return 0.0, False


def compute_factor_states(snap: FactorSnapshot) -> Dict[str, Optional[float]]:
    st: Dict[str, Optional[float]] = {f: None for f in FACTORS}
    if snap.close and snap.ma20 and snap.ma20 > 0:
        st["short_trend"] = _t(((snap.close / snap.ma20) - 1.0) / 0.03)
    if snap.close and snap.ma200 and snap.ma200 > 0:
        st["long_trend"] = _t(((snap.close / snap.ma200) - 1.0) / 0.08)
    if snap.vix is not None:
        st["vix"] = -_t((float(snap.vix) - VIX_MID) / VIX_SCALE)  # 높을수록 약세(-)
        # [코인 선행 레이더] 이중 게이트 통과 시 VIX 팩터를 더 약세(-)로 소프트 가중.
        #   기본 ACTIVE(ENABLED=1): 실제 차감. 0 으로 두면 SHADOW(로그만, run_regime_ensemble).
        pen = _crypto_vix_penalty(snap)
        if pen > 0.0 and _canary_penalty_enabled():
            st["vix"] = max(-1.0, st["vix"] - pen)
    if snap.breadth_ratio is not None:
        st["breadth"] = _t((float(snap.breadth_ratio) - 1.0) / 0.03)
    if snap.pri_z is not None:
        st["pri"] = float(max(-1.0, min(1.0, float(snap.pri_z))))
    return st


def is_vix_crisis(snap: FactorSnapshot) -> bool:
    if snap.vix is None:
        return False
    v = float(snap.vix)
    if v >= VIX_PANIC_ABS:
        return True
    if snap.vix_p90 is not None and v > float(snap.vix_p90) and v >= 20.0:
        return True
    return False


def _regime_from_score(score: float) -> str:
    if score >= UP_THRESHOLD:
        return "BULL"
    if score <= DOWN_THRESHOLD:
        return "BEAR"
    return "SIDEWAYS"


def _regime_probs(score: float) -> Dict[str, float]:
    anchors = {"BULL": 1.0, "SIDEWAYS": 0.0, "BEAR": -1.0}
    logits = {k: -SOFTMAX_BETA * (score - a) ** 2 for k, a in anchors.items()}
    return {k: round(v, 4) for k, v in _softmax(logits, 1.0).items()}


@dataclass
class MarketRegimeDecision:
    market: str
    regime: str                       # 히스테리시스 적용 후 최종
    raw_regime: str                   # 점수 기반(전환 전)
    score: float
    probs: Dict[str, float]
    factor_states: Dict[str, Optional[float]]
    weights_used: Dict[str, float]
    crisis: bool = False
    crisis_synced: bool = False
    hysteresis: Dict[str, Any] = field(default_factory=dict)


def _weighted_score(states: Dict[str, Optional[float]], weights: Dict[str, float]) -> float:
    """결측 팩터는 제외하고 사용 가중치만 재정규화해 가중합."""
    num = 0.0
    wsum = 0.0
    for f in FACTORS:
        s = states.get(f)
        if s is None:
            continue
        w = float(weights.get(f, 0.0))
        num += w * float(s)
        wsum += w
    if wsum <= 0:
        return 0.0
    return float(num / wsum)


# ===========================================================================
# 히스테리시스 (Mission 4)
# ===========================================================================
def _apply_hysteresis(
    hyst: Dict[str, Any],
    market: str,
    target: str,
    n_days: int,
    *,
    force_immediate: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    rec = hyst.get(market) if isinstance(hyst.get(market), dict) else {}
    current = str(rec.get("current") or target)
    candidate = str(rec.get("candidate") or current)
    streak = int(rec.get("streak") or 0)

    if force_immediate:
        new = {"current": target, "candidate": target, "streak": 0}
        hyst[market] = new
        return target, new

    if target == current:
        new = {"current": current, "candidate": current, "streak": 0}
        hyst[market] = new
        return current, new

    if target == candidate:
        streak += 1
    else:
        candidate = target
        streak = 1

    if streak >= max(1, int(n_days)):
        new = {"current": target, "candidate": target, "streak": 0}
        hyst[market] = new
        return target, new

    new = {"current": current, "candidate": candidate, "streak": streak}
    hyst[market] = new
    return current, new


# ===========================================================================
# 메인: 시장별 국면 산출 + 위기 동기화 + 히스테리시스
# ===========================================================================
def _stress_merge(*keys: str) -> str:
    s = {str(k or "").upper() for k in keys}
    for pri in ("HIGH_VOL", "BEAR", "SIDEWAYS", "BULL"):
        if pri in s:
            return pri
    return "UNKNOWN"


def dynamic_hysteresis_days(state: Dict[str, Any]) -> int:
    """상태의 연속 히스테리시스값 → 1~5일 정수."""
    try:
        hf = float(state.get("hysteresis_days_f", state.get("hysteresis_days", DEFAULT_HYSTERESIS_DAYS)))
    except (TypeError, ValueError):
        hf = float(DEFAULT_HYSTERESIS_DAYS)
    hf = min(float(HYSTERESIS_MAX_DAYS), max(float(HYSTERESIS_MIN_DAYS), hf))
    return int(round(hf))


def _clutch_mult(top_prob: Optional[float]) -> Tuple[float, bool]:
    """
    1위 국면 확률 → (켈리 클러치 배수, 불확실 여부).
    임계 이상이면 (1.0, False). 미만이면 임계직하 0.30 → 완전혼조 0.10 기하급수 보간.
    """
    if top_prob is None:
        return 1.0, False
    tp = float(top_prob)
    if tp >= CLUTCH_PROB_THRESHOLD:
        return 1.0, False
    span = max(1e-6, CLUTCH_PROB_THRESHOLD - _PROB_BASE)
    gap = min(1.0, max(0.0, (CLUTCH_PROB_THRESHOLD - tp) / span))
    mult = CLUTCH_MULT_CEIL * (CLUTCH_MULT_FLOOR / CLUTCH_MULT_CEIL) ** gap  # 기하급수
    return round(float(mult), 4), True


def run_regime_ensemble(
    snapshots: Dict[str, FactorSnapshot],
    *,
    persist: bool = True,
    record_obs: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    KR/US 등 시장별 FactorSnapshot → 앙상블 국면.
    위기(미국 VIX 패닉) 시 KR 을 강제 HIGH_VOL 동기화(비대칭). 전환은 히스테리시스로 완충.
    """
    now = now or datetime.now()
    today = now.strftime("%Y-%m-%d")
    st = load_state()
    weights = project_weights(st.get("weights") or {})
    n_days = dynamic_hysteresis_days(st)   # [Mission 1] RL 동적 히스테리시스(1~5일)
    hyst = st.get("hysteresis") if isinstance(st.get("hysteresis"), dict) else {}

    # 1) 시장별 raw 판정
    decisions: Dict[str, MarketRegimeDecision] = {}
    for mk, snap in snapshots.items():
        states = compute_factor_states(snap)
        # [코인 선행 레이더 — SHADOW 로그] 점수 반영 여부와 무관하게 항상 관측 기록.
        pen = _crypto_vix_penalty(snap)
        if snap.macro_contagion_risk or float(snap.crypto_liquidity_stress or 0.0) > 0.0 or pen > 0.0:
            enabled = _canary_penalty_enabled()
            vix_w = float(weights.get("vix", 0.0))
            logger.info(
                "[CANARY %s] market=%s stress=%.3f contagion=%s gate=%.2f "
                "penalty=%.3f applied=%s vix_state=%s vix_w=%.3f est_score_delta=%.4f",
                "ACTIVE" if enabled else "SHADOW", mk,
                float(snap.crypto_liquidity_stress or 0.0), snap.macro_contagion_risk,
                CRYPTO_STRESS_GATE, pen, enabled, states.get("vix"), vix_w,
                (-vix_w * pen if enabled else 0.0),
            )
        score = _weighted_score(states, weights)
        raw_regime = _regime_from_score(score)
        crisis = is_vix_crisis(snap)
        decisions[mk] = MarketRegimeDecision(
            market=mk, regime=raw_regime, raw_regime=raw_regime, score=round(score, 4),
            probs=_regime_probs(score), factor_states=states, weights_used=weights, crisis=crisis,
        )

    # 2) 비대칭 위기 오버라이드: 미국 위기 → 한국 강제 동기화
    us = decisions.get("US")
    us_crisis = bool(us and us.crisis)
    target_regime: Dict[str, str] = {}
    for mk, d in decisions.items():
        tgt = "HIGH_VOL" if d.crisis else d.raw_regime
        if mk != "US" and us_crisis:
            # 미국 패닉이면 한국을 최소 HIGH_VOL(또는 미국이 BEAR면 BEAR 이상)로 강제
            us_sev = "HIGH_VOL"
            tgt = _stress_merge(tgt, us_sev)
            d.crisis_synced = True
        target_regime[mk] = tgt

    # 3) 히스테리시스(위기/동기화는 즉시 전환) + 전환 이벤트 기록(RL 평가 대기)
    committed_transitions: List[Dict[str, Any]] = []
    for mk, d in decisions.items():
        prev_rec = hyst.get(mk) if isinstance(hyst.get(mk), dict) else {}
        prev_current = str(prev_rec.get("current") or "")
        force = bool(d.crisis or d.crisis_synced)
        final, rec = _apply_hysteresis(hyst, mk, target_regime[mk], n_days, force_immediate=force)
        d.regime = final
        d.hysteresis = rec
        if prev_current and final != prev_current:
            committed_transitions.append({
                "date": today, "market": mk,
                "from": prev_current, "to": final,
                "forced": bool(force), "hysteresis_days": n_days,
            })

    if committed_transitions:
        existing_tx = {
            (t.get("date"), t.get("market"))
            for t in st["transitions_pending"] if isinstance(t, dict)
        }
        for ev in committed_transitions:
            if (ev["date"], ev["market"]) not in existing_tx:
                st["transitions_pending"].append(ev)

    # 3b) 켈리 클러치: 가장 불확실한 시장의 1위 확률로 글로벌 켈리 축소 배수 산출
    top_probs = [max(d.probs.values()) for d in decisions.values() if d.probs]
    min_top_prob = min(top_probs) if top_probs else None
    clutch_mult, transition_uncertain = _clutch_mult(min_top_prob)
    clutch_block = {
        "active": bool(transition_uncertain),
        "mult": clutch_mult,
        "top_prob": round(float(min_top_prob), 4) if min_top_prob is not None else None,
        "threshold": CLUTCH_PROB_THRESHOLD,
        "as_of": today,
    }
    st["clutch"] = clutch_block

    # 4) 일자별 관측 기록(진화용) — 시장×날짜 dedup
    if record_obs:
        existing = {(o.get("date"), o.get("market")) for o in st["pending_obs"] if isinstance(o, dict)}
        hist_keys = {(h.get("date"), h.get("market")) for h in st["history"] if isinstance(h, dict)}
        for mk, d in decisions.items():
            key = (today, mk)
            if key in existing or key in hist_keys:
                continue
            st["pending_obs"].append({
                "date": today,
                "market": mk,
                "states": {f: d.factor_states.get(f) for f in FACTORS},
                "score": d.score,
            })

    st["hysteresis"] = hyst
    st["weights"] = weights
    if persist:
        save_state(st)

    blended = _stress_merge(*[d.regime for d in decisions.values()]) if decisions else "UNKNOWN"
    return {
        "as_of": today,
        "blended_regime": blended,
        "weights": weights,
        "macro_anchor_sum": round(weights["long_trend"] + weights["vix"], 4),
        "pri_weight": weights["pri"],
        "hysteresis_days": n_days,
        "clutch": clutch_block,
        "transition_uncertainty": bool(transition_uncertain),
        "transitions_committed": committed_transitions,
        "markets": {
            mk: {
                "regime": d.regime,
                "raw_regime": d.raw_regime,
                "score": d.score,
                "probs": d.probs,
                "crisis": d.crisis,
                "crisis_synced": d.crisis_synced,
                "factor_states": {f: (round(v, 4) if v is not None else None) for f, v in d.factor_states.items()},
                "hysteresis": d.hysteresis,
            }
            for mk, d in decisions.items()
        },
    }


# ===========================================================================
# 진화: 미래 5일 PnL 보상으로 가중치 자가 갱신 (Mission 2)
# ===========================================================================
def _forward_market_pnl(conn: sqlite3.Connection, market: str, start: str, end: str) -> Tuple[float, int]:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(final_ret), 0.0), COUNT(*)
        FROM forward_trades
        WHERE UPPER(TRIM(market))=? AND status LIKE 'CLOSED%'
          AND final_ret IS NOT NULL
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
          AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) >= ?
          AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) <= ?
        """,
        (str(market).upper(), start, end),
    ).fetchone()
    try:
        return float(row[0] or 0.0), int(row[1] or 0)
    except (TypeError, ValueError):
        return 0.0, 0


def evolve_weights(
    *,
    db_path: Optional[str] = None,
    now: Optional[datetime] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """
    5일 경과한 관측을 채점: reward_f = state_f · tanh(fwd_pnl%/scale).
    팩터가 옳은 방향을 가리켰고 시장이 그쪽으로 갔으면 +보상 → skill EMA↑ → 가중치↑.
    제약(Macro Anchor Floor 15%, PRI Cap 85%)은 project_weights 가 매번 강제.
    """
    now = now or datetime.now()
    if db_path is None:
        try:
            from market_db_paths import report_db_read_path

            db_path = report_db_read_path()
        except Exception:
            db_path = None

    with _LOCK:
        st = load_state()
        skill = {f: float(st["skill"].get(f, 0.0)) for f in FACTORS}
        evaluated = 0
        rewards_acc: Dict[str, List[float]] = {f: [] for f in FACTORS}

        if db_path and os.path.isfile(db_path):
            conn = sqlite3.connect(db_path, timeout=60)
            try:
                still: List[Dict[str, Any]] = []
                for ob in st["pending_obs"]:
                    if not isinstance(ob, dict) or not ob.get("date"):
                        continue
                    try:
                        d0 = datetime.strptime(str(ob["date"])[:10], "%Y-%m-%d")
                    except ValueError:
                        continue
                    mature = d0 + timedelta(days=FWD_EVAL_DAYS)
                    if now < mature:
                        still.append(ob)
                        continue
                    mk = str(ob.get("market") or "")
                    fwd, n = _forward_market_pnl(conn, mk, ob["date"], mature.strftime("%Y-%m-%d"))
                    pnl_norm = _t(float(fwd) / PNL_REWARD_SCALE)
                    states = ob.get("states") or {}
                    rec_rewards: Dict[str, float] = {}
                    for f in FACTORS:
                        sv = states.get(f)
                        if sv is None:
                            continue
                        r = float(sv) * pnl_norm
                        rewards_acc[f].append(r)
                        rec_rewards[f] = round(r, 5)
                    st["history"].append({
                        "date": ob["date"], "market": mk, "eval_end": mature.strftime("%Y-%m-%d"),
                        "fwd_pnl_pct": round(fwd, 4), "n_trades": n, "rewards": rec_rewards,
                    })
                    evaluated += 1
                st["pending_obs"] = still
            finally:
                conn.close()

        # skill EMA 갱신
        for f in FACTORS:
            if rewards_acc[f]:
                avg_r = sum(rewards_acc[f]) / len(rewards_acc[f])
                skill[f] = (1.0 - SKILL_EMA_BETA) * skill[f] + SKILL_EMA_BETA * avg_r

        st["skill"] = {f: round(skill[f], 6) for f in FACTORS}
        st["weights"] = weights_from_skill(skill)
        if len(st["history"]) > 400:
            st["history"] = st["history"][-400:]
        if persist:
            save_state(st)

        return {
            "events_evaluated": evaluated,
            "pending": len(st["pending_obs"]),
            "skill": st["skill"],
            "weights": st["weights"],
            "macro_anchor_sum": round(st["weights"]["long_trend"] + st["weights"]["vix"], 4),
            "pri_weight": st["weights"]["pri"],
        }


# ===========================================================================
# [Mission 1] RL 동적 히스테리시스 — 전환 후 실측 PnL 피드백 (evolve_weights 와 독립)
# ===========================================================================
def evolve_hysteresis(
    *,
    db_path: Optional[str] = None,
    now: Optional[datetime] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """
    성숙(전환 후 FWD_EVAL_DAYS 경과)한 전환 이벤트를 채점해 히스테리시스를 자가 진화.

      fwd_pnl < 0 (Whipsaw)  → dynamic_hysteresis ↑ (방어적: 다음엔 더 확실할 때만 전환)
      fwd_pnl > 0 (전환 적중) → dynamic_hysteresis ↓ (민첩: 더 빨리 잡았어도 됐다)
      delta = HYSTERESIS_EMA_LR · (-tanh(fwd_pnl% / WHIPSAW_PNL_SCALE)), 1~5일 클램프.

    5-Factor 가중치 진화(evolve_weights)와 상태 키가 분리되어 충돌하지 않는다.
    """
    now = now or datetime.now()
    if db_path is None:
        try:
            from market_db_paths import report_db_read_path

            db_path = report_db_read_path()
        except Exception:
            db_path = None

    with _LOCK:
        st = load_state()
        hf = float(st.get("hysteresis_days_f", DEFAULT_HYSTERESIS_DAYS))
        evaluated = 0
        adjustments: List[Dict[str, Any]] = []

        if db_path and os.path.isfile(db_path):
            conn = sqlite3.connect(db_path, timeout=60)
            try:
                still: List[Dict[str, Any]] = []
                for tx in st.get("transitions_pending", []):
                    if not isinstance(tx, dict) or not tx.get("date"):
                        continue
                    try:
                        d0 = datetime.strptime(str(tx["date"])[:10], "%Y-%m-%d")
                    except ValueError:
                        continue
                    mature = d0 + timedelta(days=FWD_EVAL_DAYS)
                    if now < mature:
                        still.append(tx)
                        continue
                    mk = str(tx.get("market") or "")
                    fwd, n = _forward_market_pnl(conn, mk, tx["date"], mature.strftime("%Y-%m-%d"))
                    delta_dir = -_t(float(fwd) / WHIPSAW_PNL_SCALE)
                    delta = HYSTERESIS_EMA_LR * delta_dir
                    prev_hf = hf
                    hf = min(float(HYSTERESIS_MAX_DAYS),
                             max(float(HYSTERESIS_MIN_DAYS), hf + delta))
                    evaluated += 1
                    rec = {
                        "date": tx["date"], "market": mk,
                        "from": tx.get("from"), "to": tx.get("to"),
                        "eval_end": mature.strftime("%Y-%m-%d"),
                        "fwd_pnl_pct": round(float(fwd), 4), "n_trades": n,
                        "whipsaw": bool(fwd < 0),
                        "hyst_before": round(prev_hf, 4), "hyst_after": round(hf, 4),
                        "delta": round(delta, 4),
                    }
                    adjustments.append(rec)
                    st["transitions_history"].append(rec)
                st["transitions_pending"] = still
            finally:
                conn.close()

        st["hysteresis_days_f"] = round(hf, 4)
        st["hysteresis_days"] = int(round(hf))
        if len(st.get("transitions_history", [])) > 400:
            st["transitions_history"] = st["transitions_history"][-400:]
        if persist:
            save_state(st)

        return {
            "events_evaluated": evaluated,
            "pending_transitions": len(st.get("transitions_pending", [])),
            "hysteresis_days": st["hysteresis_days"],
            "hysteresis_days_f": st["hysteresis_days_f"],
            "adjustments": adjustments,
        }


# ===========================================================================
# 라이브 데이터 수집 (yfinance) → 스냅샷
# ===========================================================================
def _pri_z_by_market() -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {"KR": None, "US": None}
    try:
        from weekly_proprietary_regime import load_weekly_shadow_pri

        data = load_weekly_shadow_pri()
        mkts = data.get("markets") if isinstance(data, dict) else {}
        if isinstance(mkts, dict):
            for mk in ("KR", "US"):
                blk = mkts.get(mk)
                if isinstance(blk, dict) and blk.get("composite_z") is not None:
                    out[mk] = float(blk.get("composite_z"))
    except Exception:
        pass
    return out


def _fetch_vkospi() -> Tuple[Optional[float], Optional[float]]:
    """[국지화] KR 전용 변동성(VKOSPI) 최근값 + 60d p90. 수집 실패 시 (None, None) →
    호출부가 글로벌 VIX 로 폴백(기존 동작, 무회귀). 여러 소스를 방어적으로 시도한다.

    VKOSPI 는 VIX 와 동일 스케일대(대략 15~30)라 기존 VIX_MID/SCALE 변환을 그대로 재사용한다.
    """
    # 1) pykrx — VKOSPI 지수(코드 '1009': 코스피 변동성지수). 환경 부재/오류 시 조용히 폴백.
    #    pykrx 내부의 stdout 에러 출력(로그인 실패 등)을 삼켜 일일 로그 오염을 막는다.
    try:
        import contextlib
        import io
        from pykrx import stock as _krx  # type: ignore
        import numpy as _np

        _to = datetime.now().strftime("%Y%m%d")
        _from = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            df = _krx.get_index_ohlcv(_from, _to, "1009")
        if df is not None and not df.empty:
            col = "종가" if "종가" in df.columns else df.columns[-1]
            ser = df[col].astype(float).dropna()
            if len(ser) >= 10:
                last = float(ser.iloc[-1])
                p90 = float(_np.quantile(ser.tail(60).to_numpy(), 0.90))
                if last > 0:
                    return last, p90
    except Exception:
        pass

    # 2) FinanceDataReader 폴백(심볼 가용 시).
    try:
        import FinanceDataReader as _fdr  # type: ignore
        import numpy as _np

        for sym in ("VKOSPI", "KRX:VKOSPI"):
            try:
                df = _fdr.DataReader(sym)
            except Exception:
                df = None
            if df is not None and not df.empty and "Close" in df.columns:
                ser = df["Close"].astype(float).dropna()
                if len(ser) >= 10:
                    last = float(ser.iloc[-1])
                    p90 = float(_np.quantile(ser.tail(60).to_numpy(), 0.90))
                    if last > 0:
                        return last, p90
    except Exception:
        pass

    return None, None


def _compute_kr_breadth(max_sample: int = 600) -> Optional[float]:
    """[국지화] KR 시장폭 — 표본 종목의 'MA20 상회 비율' → 1.0 중심 breadth_ratio 로 환산.

    market_data.sqlite 의 KR_<code> 일봉을 **읽기 전용**(busy_timeout)으로 표본 조사한다.
    매핑: ratio = 1.0 + (pct_above_ma20 - 0.5) * 0.2  (50% 상회 → 1.0 중립; 기존 breadth
    스코어 공식 _t((ratio-1)/0.03) 와 스케일 정합). 데이터 부족/오류 시 None → KR breadth 미투표.
    """
    try:
        from market_db_paths import MARKET_DATA_DB_PATH as _DB
    except Exception:
        return None
    if not _DB:
        return None

    conn = None
    try:
        conn = sqlite3.connect(_DB, timeout=60)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            conn.execute("PRAGMA busy_timeout=60000;")
        except Exception:
            pass

        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'KR\\_%' ESCAPE '\\'"
        ).fetchall()
        tables = [
            r[0] for r in rows
            if r and r[0] and not str(r[0]).endswith("_IDX")
        ]
        if len(tables) < 30:
            return None
        # 표본 상한 — 균등 샘플링(전 구간 대표성 유지하며 비용 제한).
        if len(tables) > max_sample:
            step = len(tables) / float(max_sample)
            tables = [tables[int(i * step)] for i in range(max_sample)]

        above = 0
        total = 0
        for tbl in tables:
            try:
                px = conn.execute(
                    f'SELECT Close FROM "{tbl}" ORDER BY Date DESC LIMIT 20'
                ).fetchall()
            except Exception:
                continue
            if not px or len(px) < 20:
                continue
            try:
                closes = [float(p[0]) for p in px if p and p[0] is not None]
            except (TypeError, ValueError):
                continue
            if len(closes) < 20:
                continue
            last = closes[0]
            ma20 = sum(closes) / len(closes)
            if ma20 <= 0:
                continue
            total += 1
            if last > ma20:
                above += 1

        if total < 30:
            return None
        pct_above = above / float(total)
        return 1.0 + (pct_above - 0.5) * 0.2
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def collect_live_snapshots() -> Dict[str, FactorSnapshot]:
    """yfinance 로 US(SPY/RSP/^VIX)·KR(^KS11) + 내부 PRI 를 모아 스냅샷 구성."""
    snaps: Dict[str, FactorSnapshot] = {}
    pri = _pri_z_by_market()
    crypto_stress, crypto_contagion = _load_crypto_canary()
    vix_last: Optional[float] = None
    vix_p90: Optional[float] = None
    try:
        import yfinance as yf
        import numpy as np
        import pandas as pd

        def _close(sym: str, period: str = "1y") -> "pd.Series":
            try:
                h = yf.Ticker(sym).history(period=period, auto_adjust=True)
                if h is None or h.empty or "Close" not in h.columns:
                    return pd.Series(dtype=float)
                return h["Close"].astype(float).dropna()
            except Exception:
                return pd.Series(dtype=float)

        vix = _close("^VIX", "6mo")
        if not vix.empty:
            vix_last = float(vix.iloc[-1])
            tail = vix.tail(60)
            if len(tail) >= 10:
                vix_p90 = float(np.quantile(tail.to_numpy(), 0.90))

        spy = _close("SPY")
        rsp = _close("RSP")
        breadth = None
        if not spy.empty and not rsp.empty and len(spy) >= 50 and len(rsp) >= 50:
            try:
                ratio_now = float(rsp.iloc[-1]) / float(spy.iloc[-1])
                ratio_avg = float(rsp.rolling(50).mean().iloc[-1]) / float(spy.rolling(50).mean().iloc[-1])
                breadth = ratio_now / ratio_avg if ratio_avg else None
            except Exception:
                breadth = None
        if not spy.empty:
            snaps["US"] = FactorSnapshot(
                close=float(spy.iloc[-1]),
                ma20=float(spy.rolling(20, min_periods=10).mean().iloc[-1]),
                ma200=float(spy.ewm(span=200, adjust=False).mean().iloc[-1]),
                vix=vix_last, vix_p90=vix_p90, breadth_ratio=breadth, pri_z=pri.get("US"),
                crypto_liquidity_stress=crypto_stress, macro_contagion_risk=crypto_contagion,
            )

        ks = _close("^KS11")
        if not ks.empty:
            # [국지화] 미국 VIX 의존도 ↓ — VKOSPI(한국 변동성) 우선, 실패 시 글로벌 VIX 폴백.
            kr_vix, kr_vix_p90 = _fetch_vkospi()
            if kr_vix is None:
                kr_vix, kr_vix_p90 = vix_last, vix_p90
            # [국지화] 미투표였던 KR 시장폭(breadth) 활성화 — 로컬 일봉 표본 MA20 상회 비율.
            kr_breadth = _compute_kr_breadth()
            snaps["KR"] = FactorSnapshot(
                close=float(ks.iloc[-1]),
                ma20=float(ks.rolling(20, min_periods=10).mean().iloc[-1]),
                ma200=float(ks.ewm(span=200, adjust=False).mean().iloc[-1]),
                vix=kr_vix, vix_p90=kr_vix_p90,
                breadth_ratio=kr_breadth, pri_z=pri.get("KR"),
                crypto_liquidity_stress=crypto_stress, macro_contagion_risk=crypto_contagion,
            )
    except Exception:
        pass
    return snaps


def run_and_evolve(*, persist: bool = True, db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    [일일 훅] 라이브 스냅샷 수집 → 앙상블 판정(관측 기록) → 5일 경과분 채점·가중치 진화.
    config 에 KR/US/blended 국면 키를 반영(다른 모듈이 즉시 소비).
    """
    snaps = collect_live_snapshots()
    ens = run_regime_ensemble(snaps, persist=persist) if snaps else {"markets": {}, "blended_regime": "UNKNOWN"}
    evo = evolve_weights(db_path=db_path, persist=persist)
    # [Mission 1] 전환 후 실측 PnL 로 동적 히스테리시스 자가 진화(가중치 진화와 독립)
    hevo = evolve_hysteresis(db_path=db_path, persist=persist)

    clutch = ens.get("clutch") if isinstance(ens.get("clutch"), dict) else {
        "active": False, "mult": 1.0, "top_prob": None,
    }

    if persist:
        try:
            from config_manager import set_config_value, invalidate_runtime_system_config_cache

            mk_map = ens.get("markets") or {}
            kr = (mk_map.get("KR") or {}).get("regime")
            us = (mk_map.get("US") or {}).get("regime")
            if kr:
                set_config_value("KR_REGIME_KEY", kr)
            if us:
                set_config_value("US_REGIME_KEY", us)
            if ens.get("blended_regime") and ens["blended_regime"] != "UNKNOWN":
                set_config_value("CURRENT_REGIME_KEY", ens["blended_regime"])
            # [Mission 2] 켈리 클러치 — 자본 배분 소비자(meta_governor_consumer)가 읽는 키
            set_config_value("REGIME_TRANSITION_CLUTCH", {
                "active": bool(clutch.get("active")),
                "mult": float(clutch.get("mult", 1.0) or 1.0),
                "top_prob": clutch.get("top_prob"),
                "threshold": clutch.get("threshold", CLUTCH_PROB_THRESHOLD),
                "as_of": ens.get("as_of"),
            })
            set_config_value("REGIME_ENSEMBLE", {
                "as_of": ens.get("as_of"),
                "blended_regime": ens.get("blended_regime"),
                "weights": ens.get("weights"),
                "macro_anchor_sum": ens.get("macro_anchor_sum"),
                "pri_weight": ens.get("pri_weight"),
                "markets": ens.get("markets"),
                "hysteresis_days": hevo.get("hysteresis_days"),
                "clutch": clutch,
                "evolution": {
                    "events_evaluated": evo.get("events_evaluated"),
                    "pending": evo.get("pending"),
                    "hysteresis_events": hevo.get("events_evaluated"),
                    "hysteresis_days_f": hevo.get("hysteresis_days_f"),
                },
            })
            invalidate_runtime_system_config_cache()
        except Exception:
            pass

    return {"ensemble": ens, "evolution": evo, "hysteresis_evolution": hevo}


if __name__ == "__main__":
    out = run_and_evolve(persist=True)
    e = out["ensemble"]
    print(f"blended={e.get('blended_regime')} weights={e.get('weights')}")
    print(f"macro_anchor_sum={e.get('macro_anchor_sum')} pri_weight={e.get('pri_weight')}")
    for mk, blk in (e.get("markets") or {}).items():
        print(f"  {mk}: {blk['regime']} (raw {blk['raw_regime']}, score {blk['score']}, crisis {blk['crisis']})")
    cl = e.get("clutch") or {}
    print(f"clutch: active={cl.get('active')} mult={cl.get('mult')} top_prob={cl.get('top_prob')} "
          f"hysteresis_days={e.get('hysteresis_days')}")
    print("evolution:", out["evolution"].get("events_evaluated"), "factor-evaluated ·",
          out["hysteresis_evolution"].get("events_evaluated"), "hysteresis-evaluated")
