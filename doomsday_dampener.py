"""
Evolutionary Shape-Shifting Doomsday Dampener — 둠스데이 위험점수 → 켈리 감쇠.

고정 수식(선형 vs 기하급수) 하드코딩을 배제하고, 감쇠 곡선의 '형상(Shape)'을 결정하는
멱지수 감마(γ)를 시스템이 매주 스스로 조율한다.

    Multiplier = 1.0 − ( max(0, GlobalScore − 40) / 60 ) ** γ      (clamp [0,1])

    γ = 1.0  → 선형(Linear)
    γ > 1    → 볼록(Convex, 급제동)
    γ < 1    → 오목(Concave, 완만)

자율 진화(주말):  방어 성공(Avoided Loss) > 기회비용(Missed Profit) 이면 γ↑(브레이크 예민),
반대면 γ↓(브레이크 완화). 경사하강 형태:

    γ(t+1) = clip( γ(t) + η · tanh( (AvoidedLoss − MissedProfit) / scale ),  0.5, 3.0 )

상태 SSOT: system_config (DOOMSDAY_DEFCON 등과 동일 위치).
- DOOMSDAY_DAMPEN_GAMMA: 현재 γ
- DOOMSDAY_DAMPEN_STATE: { brake_log:[{date,score,gamma,mult}], history:[...], updated_at }
"""
from __future__ import annotations

import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

GAMMA_KEY = "DOOMSDAY_DAMPEN_GAMMA"
STATE_KEY = "DOOMSDAY_DAMPEN_STATE"

GAMMA_INIT = 1.5
GAMMA_MIN = 0.5
GAMMA_MAX = 3.0

SCORE_FLOOR = 40.0     # 이 점수 이하는 무제동(Multiplier=1)
SCORE_SPAN = 60.0      # 40→100 구간 정규화 분모
MULT_FLOOR = 0.0       # 수식 그대로(점수 100이면 0). DEFCON1 전면중단과 정합.

ETA = 0.15             # 학습률(주간 경사 스텝)
GRADIENT_SCALE = 5.0   # |AvoidedLoss−MissedProfit| 5%p면 tanh≈0.76
EVAL_WINDOW_DAYS = 7
BRAKE_LOG_MAX = 180
HISTORY_MAX = 120


# ---------------------------------------------------------------------------
# 파라미터·점수 해석
# ---------------------------------------------------------------------------
def _clamp_gamma(g: float) -> float:
    return float(min(GAMMA_MAX, max(GAMMA_MIN, g)))


def resolve_gamma(
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> float:
    """meta_governor_state → system_config → 초기값(1.5) 순으로 γ 동적 로드."""
    for src in (meta, sys_config):
        if isinstance(src, dict) and src.get(GAMMA_KEY) is not None:
            try:
                return _clamp_gamma(float(src.get(GAMMA_KEY)))
            except (TypeError, ValueError):
                continue
    return GAMMA_INIT


def global_score_from_config(sys_config: Optional[Dict[str, Any]]) -> Optional[float]:
    """DOOMSDAY_RADAR_SSOT → DOOMSDAY_DEFCON 에서 Global_Contagion_Score 추출."""
    if not isinstance(sys_config, dict):
        return None
    for key in ("DOOMSDAY_RADAR_SSOT", "DOOMSDAY_DEFCON"):
        blk = sys_config.get(key)
        if isinstance(blk, dict):
            scores = blk.get("scores")
            if isinstance(scores, dict) and scores.get("Global_Contagion_Score") is not None:
                try:
                    return float(scores.get("Global_Contagion_Score"))
                except (TypeError, ValueError):
                    pass
    return None


# ---------------------------------------------------------------------------
# 핵심 감쇠 수식 (순수 함수)
# ---------------------------------------------------------------------------
def dampening_multiplier(global_score: Optional[float], gamma: float) -> float:
    """Multiplier = 1 − (max(0, score−40)/60)**γ , clamp [MULT_FLOOR, 1]."""
    if global_score is None:
        return 1.0
    try:
        s = float(global_score)
    except (TypeError, ValueError):
        return 1.0
    excess = max(0.0, s - SCORE_FLOOR)
    if excess <= 0.0:
        return 1.0
    base = min(1.0, excess / SCORE_SPAN)
    g = _clamp_gamma(gamma)
    mult = 1.0 - (base ** g)
    return float(min(1.0, max(MULT_FLOOR, mult)))


def apply_doomsday_dampening(
    kelly_risk_pct: float,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    """켈리 비중에 동적 γ 감쇠를 곱한다(consumer 훅용 · 순수/무 I/O)."""
    score = global_score_from_config(sys_config)
    if score is None or score <= SCORE_FLOOR:
        return float(kelly_risk_pct)
    gamma = resolve_gamma(meta, sys_config)
    return float(kelly_risk_pct) * dampening_multiplier(score, gamma)


# ---------------------------------------------------------------------------
# 일일 브레이크 상태 로깅 (doomsday_bridge 동기화 경로에서 호출)
# ---------------------------------------------------------------------------
def record_brake_event_into(
    cfg: Dict[str, Any],
    *,
    global_score: Optional[float],
    when: Optional[datetime] = None,
) -> Dict[str, Any]:
    """cfg(딕셔너리)에 오늘의 브레이크 스냅샷을 dedup 기록하고 STATE 블록을 반환."""
    when = when or datetime.now()
    today = when.strftime("%Y-%m-%d")
    gamma = resolve_gamma(sys_config=cfg)
    mult = dampening_multiplier(global_score, gamma)

    state = dict(cfg.get(STATE_KEY) or {})
    log: List[Dict[str, Any]] = list(state.get("brake_log") or [])
    log = [e for e in log if isinstance(e, dict) and e.get("date") != today]
    log.append(
        {
            "date": today,
            "score": (round(float(global_score), 2) if global_score is not None else None),
            "gamma": round(gamma, 4),
            "mult": round(mult, 4),
        }
    )
    if len(log) > BRAKE_LOG_MAX:
        log = log[-BRAKE_LOG_MAX:]
    state["brake_log"] = log
    state.setdefault("history", state.get("history") or [])
    state["last_brake_at"] = today
    return state


# ---------------------------------------------------------------------------
# 성과 평가용 forward PnL
# ---------------------------------------------------------------------------
def _forward_net_pnl_pct(db_path: str, start_date: str, end_date: str) -> Tuple[float, int]:
    if not db_path or not os.path.isfile(db_path):
        return 0.0, 0
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(final_ret),0.0), COUNT(*)
            FROM forward_trades
            WHERE status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL
              AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
              AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) >= ?
              AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) <= ?
            """,
            (start_date, end_date),
        ).fetchone()
        return float(row[0] or 0.0), int(row[1] or 0)
    except sqlite3.Error:
        return 0.0, 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Mission 2: 주간 자율 진화 (경사하강 γ 갱신)
# ---------------------------------------------------------------------------
def evolve_gamma(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
    db_path: Optional[str] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """
    지난주 브레이크 발동 구간의 반사실 성과로 γ 를 경사하강 갱신.
      AvoidedLoss / MissedProfit = brake_intensity × |realized PnL%|
      γ(t+1) = clip( γ + η·tanh((Avoided−Missed)/scale), 0.5, 3.0 )
    """
    now = now or datetime.now()
    if sys_config is None:
        try:
            from config_manager import load_system_config

            sys_config = load_system_config()
        except Exception:
            sys_config = {}
    if db_path is None:
        try:
            from market_db_paths import report_db_read_path

            db_path = report_db_read_path()
        except Exception:
            db_path = None

    gamma = resolve_gamma(sys_config=sys_config)
    state = dict(sys_config.get(STATE_KEY) or {})
    log: List[Dict[str, Any]] = [e for e in (state.get("brake_log") or []) if isinstance(e, dict)]

    win_start = (now - timedelta(days=EVAL_WINDOW_DAYS)).strftime("%Y-%m-%d")
    win_end = now.strftime("%Y-%m-%d")
    braked = [
        e for e in log
        if e.get("date") and win_start <= str(e["date"]) <= win_end
        and isinstance(e.get("mult"), (int, float)) and float(e["mult"]) < 1.0
    ]

    avoided = missed = 0.0
    gradient = 0.0
    fwd_pnl, n = (0.0, 0)
    reason = "no_brake_last_week"
    gamma_new = gamma

    if braked:
        brake_intensity = sum(1.0 - float(e["mult"]) for e in braked) / len(braked)  # 0..1
        fwd_pnl, n = _forward_net_pnl_pct(db_path or "", win_start, win_end)
        if fwd_pnl < 0:
            avoided = brake_intensity * abs(fwd_pnl)   # 하락장에서 제동 = 방어 성공
            reason = "defense_success"
        elif fwd_pnl > 0:
            missed = brake_intensity * fwd_pnl         # 상승장에서 제동 = 기회비용
            reason = "opportunity_cost"
        else:
            reason = "flat"
        gradient = avoided - missed
        gamma_new = _clamp_gamma(gamma + ETA * math.tanh(gradient / GRADIENT_SCALE))

    hist: List[Dict[str, Any]] = [h for h in (state.get("history") or []) if isinstance(h, dict)]
    hist.append(
        {
            "date": win_end,
            "gamma_before": round(gamma, 4),
            "gamma_after": round(gamma_new, 4),
            "brake_days": len(braked),
            "avoided_loss": round(avoided, 4),
            "missed_profit": round(missed, 4),
            "gradient": round(gradient, 4),
            "fwd_pnl_pct": round(fwd_pnl, 4),
            "n_trades": n,
            "reason": reason,
        }
    )
    if len(hist) > HISTORY_MAX:
        hist = hist[-HISTORY_MAX:]
    state["history"] = hist
    # 평가 끝난 윈도우의 로그는 정리(오늘자 신규 스냅샷만 보존 → 다음 주 중복평가 방지)
    state["brake_log"] = [e for e in log if str(e.get("date") or "") >= win_end]
    state["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

    summary = {
        "gamma_before": round(gamma, 4),
        "gamma_after": round(gamma_new, 4),
        "brake_days": len(braked),
        "avoided_loss": round(avoided, 4),
        "missed_profit": round(missed, 4),
        "gradient": round(gradient, 4),
        "fwd_pnl_pct": round(fwd_pnl, 4),
        "reason": reason,
    }

    if persist:
        try:
            from config_manager import update_system_config

            update_system_config({GAMMA_KEY: round(gamma_new, 4), STATE_KEY: state})
        except Exception as ex:
            summary["persist_error"] = str(ex)

    return summary


# ---------------------------------------------------------------------------
# Mission 3: 메타 인지 브리핑
# ---------------------------------------------------------------------------
def _disposition(gamma: float) -> Tuple[str, str]:
    if gamma >= 1.8:
        return "볼록(급제동)", "고위험 구간에서 자본을 급격히 거둬들이는 보수적 성향"
    if gamma >= 1.15:
        return "볼록(완만한 급제동)", "위험 가속 시 점증적으로 제동을 강화하는 성향"
    if gamma > 0.85:
        return "선형에 근접", "위험 점수에 비례해 균형 있게 제동하는 성향"
    return "오목(완만)", "초기 제동을 부드럽게 풀어 기회를 살리는 공격적 성향"


def build_doomsday_dampening_brief(
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    """[1/9]/둠스데이 브리핑 한 줄 — 현재 브레이크 성향 투명 공개."""
    if sys_config is None:
        try:
            from config_manager import load_system_config

            sys_config = load_system_config()
        except Exception:
            sys_config = {}
    gamma = resolve_gamma(meta, sys_config)
    score = global_score_from_config(sys_config)
    mult = dampening_multiplier(score, gamma)
    shape, _desc = _disposition(gamma)

    state = sys_config.get(STATE_KEY) or {}
    hist = [h for h in (state.get("history") or []) if isinstance(h, dict)]
    trend = ""
    if hist:
        last = hist[-1]
        r = str(last.get("reason") or "")
        if r == "opportunity_cost" and last.get("gamma_after", 0) < last.get("gamma_before", 0):
            trend = "최근 잦은 휩쏘로 기회비용 발생 → 브레이크를 완화"
        elif r == "defense_success" and last.get("gamma_after", 0) > last.get("gamma_before", 0):
            trend = "최근 제동이 손실을 방어 → 브레이크를 더 예민하게 조정"
        elif r == "no_brake_last_week":
            trend = "지난주 제동 미발동 → 성향 유지"
        else:
            trend = "성향 미세 조정"

    score_txt = f"{score:.0f}" if score is not None else "—"
    cut_txt = f"켈리 ×{mult:.2f}" if (score is not None and score > SCORE_FLOOR) else "무제동"
    head = f"🧠 <b>진화형 레이더:</b> {trend + ' · ' if trend else ''}"
    return (
        f"{head}브레이크 성향 <b>{shape}</b> "
        f"(감쇠 멱수 γ=<b>{gamma:.2f}</b> · GlobalScore {score_txt} → {cut_txt})"
    )


if __name__ == "__main__":
    print(evolve_gamma(persist=False))
    print(build_doomsday_dampening_brief())
