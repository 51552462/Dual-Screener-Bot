"""
[P4] 초신성(SUPERNOVA) 전용 유동 자본 배분 — 연속 켈리 스케일러 + 유동적 자본 캡.

펀드매니저 철학: 초신성 로직의 자본 배분을 고정 상수(하드코딩 15% 등)에 가두지 않고,
① 오늘 후보가 과거 실전 초신성 DNA와 얼마나 닮았는지(score, 코사인 부합도)와
② 국면(regime) + 최근 실현 승률이라는 두 개의 살아있는 신호로 매 순간 재계산한다.

기존 켈리·MAX_POSITION_PCT 파이프라인(forward/shared.py::try_add_virtual_position)은
그대로 두고, sig_type 에 "SUPERNOVA" 가 포함된 거래에 한해서만 배수(scaler/cap)를
얹는 방식이라 다른 전략(S1/S4/눌림 등)에는 어떤 영향도 주지 않는다. 모든 함수는
데이터 부족·예외 시 무조건 중립(1.0 배수 / 기존 정적 캡 폴백)으로 수렴해 안전하다.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pytz

# ---------------------------------------------------------------------------
# 1. 연속 켈리 스케일러 — "부합도가 높을수록 자연스럽게 더 많이 싣는다"
# ---------------------------------------------------------------------------
SCALER_CLIP_LO = 0.8
SCALER_CLIP_HI = 1.5
_DEFAULT_COS_CUTOFF = 0.50


def supernova_continuous_kelly_scaler(
    score: Any,
    sys_config: Optional[Dict[str, Any]],
    sig_type: Any,
) -> Tuple[float, float]:
    """
    scaler = clip(score/100 / cutoff, 0.8, 1.5)

    - score: try_add_virtual_position 에 넘어온 final_score (코사인 유사도×100 + 보너스).
    - cutoff: 이 후보가 실제로 통과한 컷오프. MLBOX 트랙이면 DYNAMIC_ML_BOX_CUTOFF,
      그 외(COSINE/RANK/MFE 등)는 DYNAMIC_SUPERNOVA_CUTOFF — 실제 게이트에 쓰인
      허들과 동일 분모를 써야 "커트라인 대비 얼마나 압도적으로 닮았는가"가 정확해진다.
    - 데이터 이상/누락 시 (1.0, 0.0) 중립 반환 → 호출부에서 무영향 처리.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        score_frac = float(score) / 100.0
    except (TypeError, ValueError):
        return 1.0, 0.0
    if score_frac <= 0:
        return 1.0, 0.0

    cutoff_key = (
        "DYNAMIC_ML_BOX_CUTOFF" if "MLBOX" in str(sig_type).upper() else "DYNAMIC_SUPERNOVA_CUTOFF"
    )
    try:
        cutoff = float(cfg.get(cutoff_key, _DEFAULT_COS_CUTOFF) or _DEFAULT_COS_CUTOFF)
    except (TypeError, ValueError):
        cutoff = _DEFAULT_COS_CUTOFF
    if cutoff <= 0:
        cutoff = _DEFAULT_COS_CUTOFF

    raw = score_frac / cutoff
    scaler = max(SCALER_CLIP_LO, min(SCALER_CLIP_HI, raw))
    return float(scaler), float(cutoff)


# ---------------------------------------------------------------------------
# 2. 유동적 자본 캡 — "고정 15%가 아니라 국면 × 최근 성과로 숨쉬는 한도"
# ---------------------------------------------------------------------------

# 국면별 베이스 캡(그룹 시드 대비 비중) — 이건 "시작점"일 뿐, 실제 적용 캡은
# 아래 승률 피드백까지 곱해진 뒤에야 확정되는 유동값이다. system_config 의
# "SUPERNOVA_FLUID_CAP_REGIME_BASE" 로 오버라이드 가능(기관 튜닝/향후 자동학습 여지).
_DEFAULT_REGIME_BASE_CAP: Dict[str, float] = {
    "BULL": 0.30,
    "GOLDILOCKS": 0.28,
    "EXPANSION": 0.28,
    "RISK_ON": 0.28,
    "NEUTRAL": 0.20,
    "SIDEWAYS": 0.20,
    "BEAR": 0.12,
    "RISK_OFF": 0.12,
    "HIGH_VOL": 0.10,
    "HIGH_VOLATILITY": 0.10,
    "CRISIS": 0.07,
    "DEFENSE": 0.08,
    "STANDBY": 0.10,
}
_FALLBACK_BASE_CAP = 0.20

_WR_EXPAND_HURDLE = 0.60
_WR_CONTRACT_HURDLE = 0.40
_WR_EXPAND_MULT = 1.2
_WR_CONTRACT_MULT = 0.5
_WR_MIN_SAMPLE = 5
_WR_LOOKBACK_DAYS = 5

# 절대 안전핀 — 국면×승률 조합이 극단으로 튀어도 이 밴드를 벗어나지 못한다.
# (완전 폭주/완전 제로 방지. "고정 캡 파괴"가 "무한 레버리지"를 뜻하진 않는다.)
_ABS_FLOOR = 0.03
_ABS_CEIL = 0.45


def _regime_base_cap(sys_config: Optional[Dict[str, Any]], regime_key: str) -> float:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    table = cfg.get("SUPERNOVA_FLUID_CAP_REGIME_BASE")
    if not isinstance(table, dict) or not table:
        table = _DEFAULT_REGIME_BASE_CAP
    rk = str(regime_key or "").upper()
    if rk in table:
        try:
            return float(table[rk])
        except (TypeError, ValueError):
            pass
    for key, val in table.items():
        if key and str(key).upper() in rk:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return _FALLBACK_BASE_CAP


def _recent_supernova_win_rate(
    cursor: sqlite3.Cursor,
    market: str,
    *,
    lookback_days: int = _WR_LOOKBACK_DAYS,
) -> Tuple[Optional[float], int]:
    """최근 N일 청산된 SUPERNOVA 거래의 승률. 표본 부족/오류 시 (None, n)."""
    try:
        tz = pytz.timezone("Asia/Seoul") if str(market).upper() == "KR" else pytz.timezone("America/New_York")
        cutoff_str = (datetime.now(tz) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        cursor.execute(
            "SELECT COUNT(*), SUM(CASE WHEN final_ret > 0 THEN 1 ELSE 0 END) "
            "FROM forward_trades WHERE market=? AND sig_type LIKE '%SUPERNOVA%' "
            "AND status LIKE 'CLOSED%' AND exit_date >= ?",
            (market, cutoff_str),
        )
        row = cursor.fetchone()
        n_closed = int(row[0] or 0) if row else 0
        n_wins = int(row[1] or 0) if row else 0
    except Exception:
        return None, 0
    if n_closed < _WR_MIN_SAMPLE:
        return None, n_closed
    return (n_wins / n_closed), n_closed


def supernova_fluid_max_cap_mult(
    sys_config: Optional[Dict[str, Any]],
    cursor: sqlite3.Cursor,
    market: str,
    regime_key: str,
) -> Tuple[float, str]:
    """
    반환: (fluid_cap_pct, reason).

    fluid_cap_pct 는 "그룹 시드 대비 최대 투입 비중"(예: 0.24 = 24%) 그 자체다.
    forward/shared.py 쪽에서 이 값을 effective_max_position_pct() 대신 SUPERNOVA
    거래에만 사용한다(비-SUPERNOVA 전략은 기존 정적 로직 100% 그대로 유지).

    실패 시 예외를 던지지 않고 호출부가 자체적으로 정적 캡 폴백을 쓰도록
    비어있지 않은 값만 반환(내부 방어는 하되, 최종 안전판은 호출부 try/except).
    """
    base_cap = _regime_base_cap(sys_config, regime_key)
    wr, n = _recent_supernova_win_rate(cursor, market)

    wr_mult = 1.0
    wr_note = "표본부족(중립)" if wr is None else f"WR{wr*100:.0f}%(n={n})"
    if wr is not None:
        if wr >= _WR_EXPAND_HURDLE:
            wr_mult = _WR_EXPAND_MULT
        elif wr < _WR_CONTRACT_HURDLE:
            wr_mult = _WR_CONTRACT_MULT

    fluid_cap = base_cap * wr_mult
    fluid_cap = max(_ABS_FLOOR, min(_ABS_CEIL, fluid_cap))

    reason = (
        f"국면={regime_key or 'UNKNOWN'}(기준{base_cap*100:.0f}%)×{wr_note}"
        f"({wr_mult:g}x)⇒{fluid_cap*100:.1f}%"
    )
    return float(fluid_cap), reason
