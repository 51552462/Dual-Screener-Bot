"""
[M2] 진화형 볼록 래칫 κ 의 주간 강화학습(RL) 업데이트.

지난주 프리러너(부분익절 후 잔여 추적) 청산 결과에서
  · whipsaw_rate (조기 청산 비율): 트레일이 너무 빡빡해 일찍 털린 비율
  · giveback_rate (이익 반납 비율): 고점 대비 반납한 평균 비율
을 측정하여 exit_dynamics.update_ratchet_kappa_rl 로 κ 곡선을 자가 진화시킨다.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import exit_dynamics as xd


def _read_runner_trades(db_path: str, cutoff: str):
    uri = str(db_path).replace("\\", "/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
    try:
        cur = conn.execute(
            """
            SELECT mfe, final_ret, exit_type, bars_held
            FROM forward_trades
            WHERE (free_runner=1 OR scaled_out_frac > 0)
              AND status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL AND mfe IS NOT NULL
              AND substr(IFNULL(exit_date, entry_date),1,10) >= ?
            """,
            (cutoff,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def compute_runner_rates(rows) -> Dict[str, Any]:
    """rows: [(mfe, final_ret, exit_type, bars_held), ...] → whipsaw/giveback 비율."""
    n = len(rows)
    if n == 0:
        return {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}

    mfes = []
    givebacks = []
    parsed = []
    for mfe, fr, et, bars in rows:
        try:
            mfe_f = float(mfe)
            fr_f = float(fr)
        except (TypeError, ValueError):
            continue
        gb = (mfe_f - fr_f) / max(mfe_f, 1.0)
        gb = max(0.0, min(1.0, gb))
        givebacks.append(gb)
        mfes.append(mfe_f)
        parsed.append((mfe_f, fr_f, str(et or ""), gb))

    if not parsed:
        return {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}

    giveback_rate = sum(givebacks) / len(givebacks)

    # ===========================================================================
    # 👑 [변동성 생존 모드 2] 조기 청산(Whipsaw) 학습 예민도 극대화
    # 변동성 장세에서 기계가 "내가 너무 빨리 털렸구나"라고 느끼는 기준을
    # 하위 40% 고점에서 하위 60% 고점(p60_mfe)으로 대폭 올립니다.
    # 이렇게 하면 whipsaw_rate가 급등하여, 다음 주에는 기계가 스스로 트레일 폭(Kappa)을
    # 크게 넓혀버립니다 (노이즈에 털리지 않도록 숨통을 틔움).
    # ===========================================================================
    srt = sorted(mfes)
    idx = max(0, int(0.60 * (len(srt) - 1))) # 👑 0.40 -> 0.60 (더 예민하게 Whipsaw를 감지)
    p60_mfe = srt[idx]
    whips = sum(
        1 for (mfe_f, fr_f, et, gb) in parsed
        if et == "RUNNER_TRAIL" and mfe_f <= p60_mfe
    )
    whipsaw_rate = whips / len(parsed)
    # ===========================================================================

    return {
        "n": len(parsed),
        "whipsaw_rate": round(whipsaw_rate, 4),
        "giveback_rate": round(giveback_rate, 4),
        "p60_mfe": round(p60_mfe, 2), # 👑 반환키 변경
    }


def evolve_ratchet_kappa(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    lookback_days: int = 7,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """주간 κ RL 1사이클. cfg 미지정 시 system_config 로드/저장."""
    own_cfg = cfg is None
    if own_cfg:
        try:
            from config_manager import load_system_config
            cfg = load_system_config()
        except Exception:
            cfg = {}
    if db_path is None:
        try:
            from market_db_paths import market_db_read_path
            db_path = market_db_read_path()
        except Exception:
            db_path = None

    now = now or datetime.now()
    cutoff = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    rates = {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}
    if db_path:
        try:
            rows = _read_runner_trades(db_path, cutoff)
            rates = compute_runner_rates(rows)
        except Exception as ex:
            rates["error"] = str(ex)

    old_state = xd.load_ratchet_state(cfg)
    if rates["n"] < 3:
        return {"updated": False, "reason": "insufficient_runner_sample", "rates": rates, "state": old_state}

    new_state = xd.update_ratchet_kappa_rl(
        old_state,
        whipsaw_rate=float(rates["whipsaw_rate"]),
        giveback_rate=float(rates["giveback_rate"]),
    )
    new_state["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    cfg[xd.RATCHET_STATE_KEY] = new_state

    if persist and own_cfg:
        try:
            from config_manager import update_system_config
            update_system_config({xd.RATCHET_STATE_KEY: new_state})
        except Exception:
            pass

    return {"updated": True, "rates": rates, "old_state": old_state, "state": new_state}


def evolve_mega_trend_kill_sensitivity(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    lookback_days: int = 90,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    [Mega-Trend 3번] 주말 킬스위치 민감도 RL 1사이클.
    exit_ratchet_rl 주간 진화 루프와 동일 cron 경로에서 호출.
    """
    try:
        from mega_trend_kill_rl import evolve_mega_trend_kill_sensitivity as _evolve

        return _evolve(cfg, db_path=db_path, persist=persist, now=now)
    except Exception as ex:
        return {"updated": False, "reason": str(ex), "state": {}}


def build_mega_trend_kill_rl_brief(result: Dict[str, Any]) -> str:
    try:
        from mega_trend_kill_rl import build_kill_rl_brief

        return build_kill_rl_brief(result)
    except Exception:
        return f"[Mega-Trend Kill RL] {result}"


def build_ratchet_brief(result: Dict[str, Any]) -> str:
    st = result.get("state", {})
    rates = result.get("rates", {})
    if not result.get("updated"):
        return (
            f"🪝 <b>[래칫 κ RL]</b> 표본 부족({rates.get('n',0)}건) — "
            f"κ_max {st.get('kappa_max','—')} 유지"
        )
    return (
        f"🪝 <b>[래칫 κ RL]</b> 조기청산 {rates.get('whipsaw_rate',0)*100:.0f}% / "
        f"이익반납 {rates.get('giveback_rate',0)*100:.0f}% → "
        f"κ_max {st.get('kappa_max')} · κ_min {st.get('kappa_min')} · "
        f"곡선 {st.get('curve')}(c={st.get('convexity')})"
    )
