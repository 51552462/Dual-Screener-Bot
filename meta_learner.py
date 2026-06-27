"""
Evolutionary Meta-Learning Engine — 내부 PRI ↔ 외부 매크로 REGIME '신뢰 매트릭스'.

인간이 정한 고정 가중치를 폐기하고, 두 지표의 '의견 발산(Divergence)' 구간에서 실제 7일
가상 PnL 이 어느 방향을 보상했는지로 승패를 채점하여, 신뢰 가중치를 스스로 강화학습한다.

    W_internal(t+1) = clip( W_internal(t) + α · sign(winner)·tanh(|ΔPnL|/scale),  0.20, 0.80 )
    W_external      = 1 − W_internal

상태는 factory_data_dir()/meta_trust_matrix.json 단일 SSOT 로 보관(원자적 저장).
- 내부 PRI 방향: weekly_proprietary_regime shadow JSON 의 blended.composite_z (UP/DOWN/NEUTRAL).
- 외부 REGIME 방향: META_REGIME_KEY (BULL→UP, BEAR/HIGH_VOL→DOWN, 그 외 NEUTRAL).
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import tempfile
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    from factory_data_paths import factory_data_dir
except Exception:  # pragma: no cover
    def factory_data_dir() -> str:  # type: ignore[misc]
        d = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")
        os.makedirs(d, exist_ok=True)
        return d


TRUST_MATRIX_FILENAME = "meta_trust_matrix.json"

W_INIT = 0.50
W_MIN = 0.20   # 과적합 방지 하드 캡(하한)
W_MAX = 0.80   # 과적합 방지 하드 캡(상한)
DEFAULT_ALPHA = 0.05          # 학습률
PNL_SCALE_PCT = 5.0           # 7일 |Net PnL%| 5%면 tanh≈0.76 (보상 크기 정규화)
DIVERGENCE_EVAL_DAYS = 7      # 발산 후 성과 관측 창
PRI_Z_UP = 0.45
PRI_Z_DOWN = -0.45
RECENT_VERDICTS_FOR_REPORT = 3

_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# 상태 입출력
# ---------------------------------------------------------------------------
def trust_matrix_path() -> str:
    return os.path.join(factory_data_dir(), TRUST_MATRIX_FILENAME)


def _default_matrix() -> Dict[str, Any]:
    return {
        "schema": "meta_trust_matrix.v1",
        "w_internal": W_INIT,
        "w_external": W_INIT,
        "alpha": DEFAULT_ALPHA,
        "updated_at": None,
        "pending_events": [],   # 아직 7일 미경과(미채점)
        "history": [],          # 채점 완료 이벤트
    }


def load_trust_matrix() -> Dict[str, Any]:
    path = trust_matrix_path()
    if not os.path.isfile(path):
        return _default_matrix()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_matrix()
        base = _default_matrix()
        base.update({k: v for k, v in data.items() if v is not None})
        # 가중치 정합성 보정
        wi = float(base.get("w_internal", W_INIT) or W_INIT)
        wi = min(W_MAX, max(W_MIN, wi))
        base["w_internal"] = wi
        base["w_external"] = round(1.0 - wi, 6)
        if not isinstance(base.get("pending_events"), list):
            base["pending_events"] = []
        if not isinstance(base.get("history"), list):
            base["history"] = []
        return base
    except (OSError, json.JSONDecodeError, ValueError):
        return _default_matrix()


def save_trust_matrix(state: Dict[str, Any]) -> bool:
    path = trust_matrix_path()
    state = dict(state)
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with _LOCK:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".trust_", suffix=".json", dir=os.path.dirname(path))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
        return True
    except OSError:
        return False


def get_trust_weights() -> Tuple[float, float]:
    m = load_trust_matrix()
    wi = min(W_MAX, max(W_MIN, float(m.get("w_internal", W_INIT) or W_INIT)))
    return wi, round(1.0 - wi, 6)


# ---------------------------------------------------------------------------
# 방향 해석
# ---------------------------------------------------------------------------
def _dir_from_z(z: float) -> str:
    if z >= PRI_Z_UP:
        return "UP"
    if z <= PRI_Z_DOWN:
        return "DOWN"
    return "NEUTRAL"


def internal_direction() -> Tuple[str, Optional[float]]:
    """내부 PRI 방향(UP/DOWN/NEUTRAL) + composite_z. shadow JSON 부재 시 (NEUTRAL, None)."""
    try:
        from weekly_proprietary_regime import load_weekly_shadow_pri

        data = load_weekly_shadow_pri()
        blended = data.get("blended") if isinstance(data, dict) else None
        if isinstance(blended, dict):
            z = float(blended.get("composite_z") or 0.0)
            reg = str(blended.get("regime") or "").upper()
            if reg in ("UP", "DOWN", "SIDEWAYS"):
                return ("NEUTRAL" if reg == "SIDEWAYS" else reg), z
            return _dir_from_z(z), z
    except Exception:
        pass
    return "NEUTRAL", None


_EXTERNAL_UP = {"BULL", "RISK_ON", "BULL_TREND", "UP"}
_EXTERNAL_DOWN = {"BEAR", "HIGH_VOL", "RISK_OFF", "DOWN"}


def external_direction(
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """외부 매크로 REGIME 방향(UP/DOWN/NEUTRAL) + 원본 키."""
    rk = ""
    if isinstance(meta, dict):
        rk = str(meta.get("META_REGIME_KEY") or "").upper()
    if not rk:
        try:
            from meta_governor_consumer import load_meta_state_resolved

            m = load_meta_state_resolved()
            rk = str(m.get("META_REGIME_KEY") or "").upper()
        except Exception:
            rk = ""
    if not rk and isinstance(sys_config, dict):
        ra = sys_config.get("REGIME_ANALYSIS")
        if isinstance(ra, dict):
            rk = str(ra.get("regime_key") or "").upper()
    if rk in _EXTERNAL_UP:
        return "UP", rk
    if rk in _EXTERNAL_DOWN:
        return "DOWN", rk
    return "NEUTRAL", (rk or "UNKNOWN")


def _is_divergence(internal_dir: str, external_dir: str) -> bool:
    """방향이 정반대(UP↔DOWN)일 때만 발산으로 본다(중립은 제외)."""
    return {internal_dir, external_dir} == {"UP", "DOWN"}


# ---------------------------------------------------------------------------
# 성과 평가 (7일 가상 PnL)
# ---------------------------------------------------------------------------
def _forward_net_pnl_pct(conn: sqlite3.Connection, start_date: str, end_date: str) -> Tuple[float, int]:
    """[start, end] 청산행 Net PnL%(=Σ final_ret) 와 건수. INCUBATOR 제외, KR·US 합산."""
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT COALESCE(SUM(final_ret), 0.0), COUNT(*)
        FROM forward_trades
        WHERE status LIKE 'CLOSED%'
          AND final_ret IS NOT NULL
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
          AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) >= ?
          AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) <= ?
        """,
        (start_date, end_date),
    ).fetchone()
    try:
        return float(row[0] or 0.0), int(row[1] or 0)
    except (TypeError, ValueError):
        return 0.0, 0


def _winner_of(internal_dir: str, external_dir: str, fwd_pnl_pct: float) -> Optional[str]:
    """7일 PnL 부호로 승자 판정. PnL>0 → UP 예측자 승, PnL<0 → DOWN 예측자 승."""
    if fwd_pnl_pct > 0:
        realized = "UP"
    elif fwd_pnl_pct < 0:
        realized = "DOWN"
    else:
        return None
    if internal_dir == realized:
        return "internal"
    if external_dir == realized:
        return "external"
    return None


def _apply_weight_update(w_internal: float, winner: str, fwd_pnl_pct: float, alpha: float) -> float:
    """강화학습 갱신 + 하드 캡 [0.20, 0.80]."""
    magnitude = math.tanh(abs(float(fwd_pnl_pct)) / PNL_SCALE_PCT)
    step = alpha * magnitude
    if winner == "internal":
        w_internal += step
    elif winner == "external":
        w_internal -= step
    return float(min(W_MAX, max(W_MIN, w_internal)))


# ---------------------------------------------------------------------------
# 메인 사이클
# ---------------------------------------------------------------------------
def run_meta_learning_cycle(
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    주간 자율 조율 훅: (1) 오늘 발산이면 이벤트 기록, (2) 7일 경과 이벤트 채점·가중치 갱신.
    """
    now = now or datetime.now()
    today = now.strftime("%Y-%m-%d")

    if db_path is None:
        try:
            from market_db_paths import report_db_read_path

            db_path = report_db_read_path()
        except Exception:
            db_path = None

    with _LOCK:
        m = load_trust_matrix()
        alpha = float(m.get("alpha", DEFAULT_ALPHA) or DEFAULT_ALPHA)

        # (1) 오늘 발산 기록 (하루 1건 dedup)
        idir, iz = internal_direction()
        edir, ekey = external_direction(meta, sys_config)
        recorded = False
        if _is_divergence(idir, edir):
            already = any(
                isinstance(e, dict) and e.get("date") == today for e in m["pending_events"]
            ) or any(isinstance(h, dict) and h.get("date") == today for h in m["history"])
            if not already:
                m["pending_events"].append(
                    {
                        "date": today,
                        "internal_dir": idir,
                        "external_dir": edir,
                        "external_key": ekey,
                        "internal_z": iz,
                    }
                )
                recorded = True

        # (2) 7일 경과 이벤트 채점
        evaluated = 0
        if db_path and os.path.isfile(db_path):
            conn = sqlite3.connect(db_path, timeout=60)
            try:
                still_pending: List[Dict[str, Any]] = []
                for ev in m["pending_events"]:
                    if not isinstance(ev, dict) or not ev.get("date"):
                        continue
                    try:
                        ev_date = datetime.strptime(str(ev["date"])[:10], "%Y-%m-%d")
                    except ValueError:
                        continue
                    mature_on = ev_date + timedelta(days=DIVERGENCE_EVAL_DAYS)
                    if now < mature_on:
                        still_pending.append(ev)
                        continue
                    start = ev["date"]
                    end = mature_on.strftime("%Y-%m-%d")
                    fwd_pnl, n = _forward_net_pnl_pct(conn, start, end)
                    winner = _winner_of(ev.get("internal_dir", ""), ev.get("external_dir", ""), fwd_pnl)
                    w_before = float(m["w_internal"])
                    if winner is not None:
                        m["w_internal"] = _apply_weight_update(w_before, winner, fwd_pnl, alpha)
                    m["history"].append(
                        {
                            "date": ev["date"],
                            "eval_end": end,
                            "internal_dir": ev.get("internal_dir"),
                            "external_dir": ev.get("external_dir"),
                            "fwd_pnl_pct": round(fwd_pnl, 4),
                            "n_trades": n,
                            "winner": winner or "tie",
                            "w_internal_before": round(w_before, 4),
                            "w_internal_after": round(float(m["w_internal"]), 4),
                        }
                    )
                    evaluated += 1
                m["pending_events"] = still_pending
            finally:
                conn.close()

        m["w_internal"] = round(min(W_MAX, max(W_MIN, float(m["w_internal"]))), 6)
        m["w_external"] = round(1.0 - m["w_internal"], 6)
        # history 비대 방지(최근 200건)
        if len(m["history"]) > 200:
            m["history"] = m["history"][-200:]
        save_trust_matrix(m)

        return {
            "today": today,
            "internal_dir": idir,
            "external_dir": edir,
            "divergence_recorded": recorded,
            "events_evaluated": evaluated,
            "w_internal": m["w_internal"],
            "w_external": m["w_external"],
            "pending": len(m["pending_events"]),
        }


# ---------------------------------------------------------------------------
# Mission 3: 메타 인지 리포팅 라인
# ---------------------------------------------------------------------------
def build_meta_cognition_line() -> str:
    """[1/9] '진화 철학' 한 줄. 학습 표본 없으면 균형 신뢰 안내."""
    m = load_trust_matrix()
    wi = min(W_MAX, max(W_MIN, float(m.get("w_internal", W_INIT) or W_INIT)))
    history = [h for h in m.get("history", []) if isinstance(h, dict)]
    recent = history[-RECENT_VERDICTS_FOR_REPORT:]

    if not recent:
        return (
            "🧠 <b>진화 철학:</b> 발산 학습 표본 축적 중 — "
            f"내부 PRI {wi*100:.0f}% : 외부 매크로 {(1-wi)*100:.0f}% 균형 신뢰로 국면 판정 중"
        )

    int_wins = sum(1 for h in recent if h.get("winner") == "internal")
    ext_wins = sum(1 for h in recent if h.get("winner") == "external")
    k = len(recent)
    if int_wins > ext_wins:
        verdict = f"내부 PRI 승({int_wins}/{k})"
        leaning = "내부 지표"
        lean_pct = wi * 100.0
    elif ext_wins > int_wins:
        verdict = f"외부 매크로 승({ext_wins}/{k})"
        leaning = "외부 매크로"
        lean_pct = (1 - wi) * 100.0
    else:
        verdict = f"호각({int_wins}:{ext_wins})"
        leaning = "내부 지표" if wi >= 0.5 else "외부 매크로"
        lean_pct = max(wi, 1 - wi) * 100.0

    return (
        f"🧠 <b>진화 철학:</b> 최근 {k}회 발산 구간에서 {verdict} → "
        f"현재 시스템은 {leaning}를 <b>{lean_pct:.0f}%</b> 신뢰하여 국면 판정 중"
    )


if __name__ == "__main__":
    print(run_meta_learning_cycle())
    print(build_meta_cognition_line())
