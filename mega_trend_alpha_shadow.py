"""MEGATREND-SCOPE-DEFINE-01 — RP-1 대체 독립 신호, 섀도우 only.

기존 S1/S4/RANK_A/코사인·CorrKelly 언락 부스터와 분리.
실발동 게이트(AXIS 이후 RANK_B/D n≥20 AND PF>0.5)는 이번 Handoff에서 열지 않음.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Tuple

from mega_trend_ignition import MEGA_TREND_CONFIG_KEY
from re_evolution_strike_guard import apply_shadow_entry_zero_notional

MEGA_TREND_ALPHA_TAG = "MEGA_TREND_ALPHA"
EXIT_TYPE_MEGA_TREND_ALPHA_SHADOW = "MEGA_TREND_ALPHA_SHADOW"
# 장부 프록시 — 0 notional·OBSERVE_ONLY. 스캐너 유니버스 아님.
_SHADOW_PROXY_CODE = "005930"
_SHADOW_PROXY_NAME = "MEGA_TREND_ALPHA_SHADOW"


def mega_trend_alpha_live_fire_allowed() -> bool:
    """실자본 발동. 이번 Handoff 고정 False.

    게이트(미개방): AXIS-01 이후 RANK_B/D 신규 CLOSED n≥20 AND PF>0.5
    → 충족 시 Claude 재검토 + 별도 Handoff.
    """
    return False


def is_mega_trend_alpha_sig(sig_type: object) -> bool:
    return MEGA_TREND_ALPHA_TAG in str(sig_type or "")


def format_mega_trend_alpha_shadow_sig() -> str:
    """OBSERVE_ONLY 로 좀비 치유 스킵. RE_EVOL_SHADOW 네임스페이스 미사용."""
    return f"[OBSERVE_ONLY] {MEGA_TREND_ALPHA_TAG}"


def shadow_zero_notional() -> Tuple[str, int, float, float]:
    """apply_shadow_entry_zero_notional 재사용 — 수량·투자금만 취하고 태그는 교체."""
    _tagged, shares, invest, kelly = apply_shadow_entry_zero_notional(
        MEGA_TREND_ALPHA_TAG, strategy_id=MEGA_TREND_ALPHA_TAG
    )
    return format_mega_trend_alpha_shadow_sig(), int(shares or 0), float(invest or 0), float(kelly or 0)


def climax_should_close_alpha(state: Optional[Mapping[str, Any]]) -> bool:
    if not isinstance(state, Mapping):
        return False
    if str(state.get("climax_kill_at") or "").strip():
        return True
    verdict = state.get("climax_verdict")
    if isinstance(verdict, Mapping) and (
        verdict.get("kill") or str(verdict.get("action") or "").lower() in ("kill", "full", "exit")
    ):
        return True
    return False


def _blank_insert_row(*, today: str, sector: str, sig_type: str) -> Dict[str, Any]:
    from forward.shared import _FORWARD_TRADE_INSERT_COLS

    row: Dict[str, Any] = {c: 0 for c in _FORWARD_TRADE_INSERT_COLS}
    row.update(
        {
            "entry_date": today,
            "market": "KR",
            "code": _SHADOW_PROXY_CODE,
            "name": _SHADOW_PROXY_NAME,
            "sector": sector or "",
            "sig_type": sig_type,
            "tier": MEGA_TREND_ALPHA_TAG,
            "entry_price": 1.0,
            "max_high": 1.0,
            "min_low": 1.0,
            "invest_amount": 0.0,
            "shares": 0,
            "sim_kelly_invest": 0.0,
            "entry_regime": "SIDEWAYS",
            "entry_dtw_score": 99.0,
        }
    )
    return row


def open_mega_trend_alpha_shadow(
    conn: Any,
    *,
    config: Optional[Mapping[str, Any]] = None,
    today: Optional[str] = None,
) -> Dict[str, Any]:
    """점화 판정 → 독립 섀도우 OPEN. live fire 게이트는 닫힘."""
    out: Dict[str, Any] = {"opened": 0, "skipped": "ok", "trade_id": None}
    if mega_trend_alpha_live_fire_allowed():
        out["skipped"] = "live_fire_not_implemented"
        return out
    block = (config or {}).get(MEGA_TREND_CONFIG_KEY) if isinstance(config, Mapping) else None
    if not isinstance(block, Mapping) or not block.get("active"):
        out["skipped"] = "not_ignited"
        return out
    n_open = conn.execute(
        """
        SELECT COUNT(*) FROM forward_trades
        WHERE status='OPEN' AND IFNULL(sig_type,'') LIKE ?
        """,
        (f"%{MEGA_TREND_ALPHA_TAG}%",),
    ).fetchone()[0]
    if int(n_open or 0) > 0:
        out["skipped"] = "already_open"
        return out
    today_s = today or datetime.now().strftime("%Y-%m-%d")
    sig, shares, invest, kelly = shadow_zero_notional()
    if shares or invest or kelly:
        out["skipped"] = "nonzero_notional_abort"
        return out
    sector = str(block.get("primary_sector") or "")
    row = _blank_insert_row(today=today_s, sector=sector, sig_type=sig)
    from forward.shared import _insert_forward_trade_row

    cur = conn.cursor()
    _insert_forward_trade_row(cur, row)
    tid = cur.lastrowid
    conn.commit()
    out.update({"opened": 1, "skipped": "", "trade_id": tid})
    return out


def close_mega_trend_alpha_shadow(
    conn: Any,
    *,
    final_ret_pct: float = 0.0,
    today: Optional[str] = None,
    call_record_closure: bool = True,
) -> Dict[str, Any]:
    """클라이맥스 판정 → 청산 기록. 실현손익 0 → NAV 불변. exit_type 명시."""
    today_s = today or datetime.now().strftime("%Y-%m-%d")
    ret = 0.0 if final_ret_pct is None else float(final_ret_pct)
    # 섀도우 RP-1: 손익 0 강제 (실자본 오염 금지)
    ret = 0.0
    ids = [
        int(r[0])
        for r in conn.execute(
            """
            SELECT id FROM forward_trades
            WHERE status='OPEN' AND IFNULL(sig_type,'') LIKE ?
            """,
            (f"%{MEGA_TREND_ALPHA_TAG}%",),
        ).fetchall()
        if r and r[0] is not None
    ]
    closed = 0
    for tid in ids:
        conn.execute(
            """
            UPDATE forward_trades
            SET status='CLOSED', exit_date=?, exit_reason=?,
                exit_type=?, final_ret=?
            WHERE id=?
            """,
            (
                today_s,
                f"{MEGA_TREND_ALPHA_TAG}_CLIMAX_SHADOW",
                EXIT_TYPE_MEGA_TREND_ALPHA_SHADOW,
                ret,
                tid,
            ),
        )
        closed += 1
    if ids:
        conn.commit()
    nav_hook = None
    if call_record_closure and closed:
        from live_nav_manager import record_closure

        nav_hook = record_closure("KR", final_ret_pct=0.0, kelly_pct=None, exit_date=today_s)
    return {"closed": closed, "ids": ids, "nav_hook": nav_hook}


def sync_mega_trend_alpha_shadow(
    config: Dict[str, Any],
    *,
    conn: Optional[Any] = None,
) -> Dict[str, Any]:
    """점화→섀도우 진입, 클라이맥스→섀도우 청산. factory 일일 잡 말미."""
    out: Dict[str, Any] = {"entry": None, "exit": None}
    own_conn = conn is None
    if own_conn:
        from forward.shared import DB_PATH, init_forward_db

        init_forward_db()
        import sqlite3

        conn = sqlite3.connect(DB_PATH, timeout=60)
    try:
        block = config.get(MEGA_TREND_CONFIG_KEY) if isinstance(config, dict) else None
        if climax_should_close_alpha(block if isinstance(block, Mapping) else None):
            out["exit"] = close_mega_trend_alpha_shadow(conn)
        elif isinstance(block, Mapping) and block.get("active"):
            out["entry"] = open_mega_trend_alpha_shadow(conn, config=config)
        else:
            out["entry"] = {"opened": 0, "skipped": "not_ignited"}
    finally:
        if own_conn and conn is not None:
            conn.close()
    return out
