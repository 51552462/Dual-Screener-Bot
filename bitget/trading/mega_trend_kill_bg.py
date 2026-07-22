import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from bitget.infra.data_paths import market_data_db_path
from bitget.infra.shared_db_connector import get_connection

def _get_dynamic_kill_sensitivity(cfg: Dict[str, Any]) -> float:
    """
    [자율 진화(Self-Evolving) 수용체]
    auto_pilot이 매일 밤 평가한 킬스위치 민감도(Sensitivity)를 로드합니다.
    기본값은 1.0이며, 최근 롱 타점이 계속 터지면 시스템이 이 값을 낮춰서(예: 0.8) 
    조금만 과열돼도 바로 진입을 차단하게 스스로 학습합니다.
    """
    return float(cfg.get("DYNAMIC_KILL_SENSITIVITY", 1.0))

def evaluate_crypto_climax_kill_switch(
    sys_config: Dict[str, Any],
    position_side: str = "LONG",
) -> Dict[str, Any]:
    """
    코인 전용 메가 트렌드 킬스위치 (펀딩비 + 유동성 스트레스 기반)
    """
    side = str(position_side).upper()
    
    # 숏(SHORT)은 탐욕의 끝자락에서 오히려 유리하므로 킬스위치를 면제합니다.
    if side != "LONG":
        return {"kill_active": False, "reason": "short_exempt"}

    try:
        from bitget.reports.canary_panel_bg import load_canary_state
        canary = load_canary_state()
        stress = float(canary.get("crypto_liquidity_stress") or 0.0)
        avg_funding = float(canary.get("components", {}).get("avg_funding_rate") or 0.0)
    except Exception:
        return {"kill_active": False, "reason": "canary_blind"}

    # 자율 진화된 민감도 적용
    sensitivity = _get_dynamic_kill_sensitivity(sys_config)
    
    # [코인 물리법칙] 펀딩비가 극단적 양수(롱 과열)이면서 유동성 스트레스가 높을 때가 '청산 빔'의 전조입니다.
    # 민감도(sensitivity)가 낮아질수록 더 얇은 펀딩비에서도 킬스위치가 예민하게 작동합니다.
    funding_threshold = 0.0008 * sensitivity
    stress_threshold = 0.65 * sensitivity

    if avg_funding >= funding_threshold and stress >= stress_threshold:
        return {
            "kill_active": True,
            "reason": f"Crypto Climax 감지: 펀딩비 폭발({avg_funding:.5f}) & 스트레스({stress:.2f}). 롱 뚝배기 빔 위험으로 진입 차단.",
            "metrics": {"funding": avg_funding, "stress": stress, "sensitivity": sensitivity}
        }

    return {"kill_active": False, "reason": "safe"}

def evolve_mega_trend_kill_sensitivity(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    [아키텍트 수술: 강화학습(RL) 피드백 루프]
    매일 밤(UTC 00:00) auto_pilot이 호출하여 킬스위치 민감도를 자율 갱신합니다.
    최근 48시간 동안 롱(Long) 타점의 승률이 박살 났다면, "지금은 탐욕의 끝자락이구나"라고
    스스로 깨닫고 민감도를 대폭 조입니다.
    """
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
    
    conn = get_connection(market_data_db_path(), read_only=True)
    try:
        query = """
            SELECT COUNT(*) AS n, 
                   SUM(CASE WHEN CAST(final_ret AS REAL) > 0 THEN 1 ELSE 0 END) AS wins
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%' 
              AND position_side = 'LONG'
              AND exit_date >= ?
        """
        row = conn.execute(query, (cutoff,)).fetchone()
    finally:
        conn.close()

    n = int(row[0] if row else 0)
    wins = int(row[1] if row else 0)
    
    current_sensitivity = float(cfg.get("DYNAMIC_KILL_SENSITIVITY", 1.0))
    
    if n < 5:
        return {"updated": False, "sensitivity": current_sensitivity}

    win_rate = wins / n
    
    # 롱 승률이 30% 이하라면 탐욕의 끝자락에서 다 터져나가고 있다는 뜻 -> 민감도를 예민하게(0.8) 조임
    if win_rate <= 0.30:
        new_sensitivity = max(0.5, current_sensitivity - 0.1)
    # 롱 승률이 60% 이상이면 아직 달리는 말 -> 민감도를 둔감하게(1.2) 풀어서 수익 극대화
    elif win_rate >= 0.60:
        new_sensitivity = min(1.5, current_sensitivity + 0.1)
    else:
        new_sensitivity = current_sensitivity

    cfg["DYNAMIC_KILL_SENSITIVITY"] = round(new_sensitivity, 2)
    return {"updated": True, "sensitivity": round(new_sensitivity, 2), "win_rate": round(win_rate, 2)}