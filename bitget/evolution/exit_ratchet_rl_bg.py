"""Bitget exit ratchet κ RL — bitget_forward_trades free_runner SSOT."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

import exit_dynamics as xd

from bitget.forward.shared import DB_PATH
from bitget.infra.bounded_reads import exit_ratchet_runner_trades_sql
from bitget.infra.clock import utc_date_days_ago_str, utc_now


def _read_runner_trades(db_path: str, cutoff: str):
    uri = str(db_path).replace("\\", "/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
    try:
        sql, params = exit_ratchet_runner_trades_sql(cutoff=cutoff)
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def compute_runner_rates(rows) -> Dict[str, Any]:
    from exit_ratchet_rl import compute_runner_rates as _stock_rates

    return _stock_rates(rows)


def evolve_bitget_ratchet_kappa(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    # [아키텍트 수술] 코인 초단기 변동성 반영. 
    # 기본 7일 룩백을 유지하되, 코인의 물리법칙에 맞게 3일(72시간)로 단축하여 RL 민감도를 극대화.
    lookback_days: int = 3,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    own_cfg = cfg is None
    if own_cfg:
        try:
            from bitget.infra.config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}
    path = db_path or DB_PATH
    now = now or utc_now()
    cutoff = utc_date_days_ago_str(lookback_days, anchor=now)

    rates: Dict[str, Any] = {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}
    if path:
        try:
            rates = compute_runner_rates(_read_runner_trades(path, cutoff))
        except Exception as ex:
            rates["error"] = str(ex)

    old_state = xd.load_ratchet_state(cfg)
    if rates.get("n", 0) < 3:
        return {"updated": False, "reason": "insufficient_runner_sample", "rates": rates, "state": old_state}

   # [아키텍트 수술] 코인 휩쏘(청산 빔) 가속기 주입
    # Canary 센서를 읽어 시장에 펀딩비/유동성 스트레스가 감지되면
    # RL 엔진이 인식하는 휩쏘 확률을 1.5배로 부풀려, 방어막(Ratchet)을 훨씬 빠르고 팽팽하게 조입니다.
    try:
        from bitget.reports.canary_panel_bg import load_canary_state
        stress = float(load_canary_state().get("crypto_liquidity_stress") or 0.0)
        stress_multiplier = 1.5 if stress >= 0.5 else 1.0
    except Exception:
        stress_multiplier = 1.0

    new_state = xd.update_ratchet_kappa_rl(
        old_state,
        whipsaw_rate=float(rates["whipsaw_rate"]) * stress_multiplier,
        giveback_rate=float(rates["giveback_rate"]),
    )
    new_state["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    cfg = dict(cfg or {})
    cfg[xd.RATCHET_STATE_KEY] = new_state

    result: Dict[str, Any] = {
        "updated": True,
        "rates": rates,
        "old_state": old_state,
        "state": new_state,
    }
    if persist:
        try:
            from bitget.infra.config_manager import update_system_config

            update_system_config({xd.RATCHET_STATE_KEY: new_state})
        except Exception as ex:
            result["persist_error"] = str(ex)

    return result


def build_ratchet_brief(result: Dict[str, Any]) -> str:
    from exit_ratchet_rl import build_ratchet_brief as _stock_brief

    return _stock_brief(result)
