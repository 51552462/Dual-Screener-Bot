"""
통합 리포트 컨텍스트 수집 — 시장별 [0]·[0b]·[Δ] HTML 조각.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

import pandas as pd

from evolution_digest import (
    build_evolution_digest_html,
    build_global_evolution_digest_html,
)
from inverse_etf_sniper import (
    INVERSE_CANDIDATES,
    INVERSE_SIG_MARKER,
    _fetch_hedge_5d_return_pct,
    _numeric_tail_balance,
    _tail_fund_key,
)
from market_db_paths import market_db_read_path
from reports.report_formatter import format_doomsday_banner_html, format_short_sleeve_html

DB_PATH = market_db_read_path()


def _df_long_only(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    sig = df["sig_type"].astype(str) if "sig_type" in df.columns else pd.Series("", index=df.index)
    # regex=False — '[INVERSE_ETF]' 는 정규식 character class 로 오인되어 'RANK_C' 등이 탈락함
    mask = ~sig.str.contains("INCUBATOR", na=False, regex=False) & ~sig.str.contains(
        INVERSE_SIG_MARKER, na=False, regex=False
    )
    return df.loc[mask].copy()


def _inverse_open_for_market(conn: sqlite3.Connection, market: str) -> list[dict[str, Any]]:
    mkt = market.upper()
    cur = conn.execute(
        """
        SELECT code, name, sim_kelly_invest, invest_amount, status
        FROM forward_trades
        WHERE market=? AND status='OPEN'
          AND IFNULL(sig_type,'') LIKE ?
        """,
        (mkt, f"%{INVERSE_SIG_MARKER}%"),
    )
    rows = []
    for r in cur.fetchall():
        inv = float(r[2] or 0) or float(r[3] or 0)
        rows.append({"code": r[0], "name": r[1], "invest": inv, "status": r[4]})
    return rows


def collect_short_sleeve_context(
    market: str,
    sys_config: dict[str, Any],
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> dict[str, Any]:
    mkt = market.upper()
    icon = "🇰🇷" if mkt == "KR" else "🇺🇸"
    mode = bool(sys_config.get("INVERSE_MODE_ACTIVE", False))
    tail_key = _tail_fund_key(mkt)
    tail = _numeric_tail_balance(tail_key)

    triggers: list[dict[str, Any]] = []
    for cand in INVERSE_CANDIDATES:
        if cand["market"].upper() != mkt:
            continue
        thr = float(cand["trigger_ret_5d"])
        r5 = _fetch_hedge_5d_return_pct(mkt, cand["hedge"])
        met = r5 is not None and r5 <= thr
        triggers.append(
            {
                "code": cand["code"],
                "hedge": cand["hedge"],
                "hedge_5d_ret": r5,
                "threshold": thr,
                "trigger_met": met,
            }
        )

    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        opens = _inverse_open_for_market(conn, mkt)
    finally:
        if own_conn and conn:
            conn.close()

    last = sys_config.get("INVERSE_LAST_CYCLE_SUMMARY") or {}
    if not isinstance(last, dict):
        last = {}
    cycle = last.get("cycle") if isinstance(last.get("cycle"), dict) else last
    ex: dict[str, Any] = {}
    if isinstance(cycle, dict):
        ex["entered"] = cycle.get("entered")
        ex["skipped"] = cycle.get("skipped")
        ex["kill_closed"] = cycle.get("kill_closed", 0)
    if mode and any(t.get("trigger_met") for t in triggers) and not opens:
        if ex.get("entered"):
            ex["summary_line"] = "트리거 충족 → 스나이퍼 진입 실행됨"
        elif ex.get("skipped"):
            ex["summary_line"] = f"트리거 충족·진입 대기/거부 ({ex.get('skipped')})"
        else:
            ex["summary_line"] = "트리거 충족 — Autopilot/MetaGovernor 인버스 모드 ON (분 배치)"
    elif not mode:
        ex["summary_line"] = "인버스 모드 OFF — 롱 비중 유지(테일만 대기)"
    elif opens:
        ex["summary_line"] = f"OPEN {len(opens)}건 유지 — 신규 숏 진입 차단"
    else:
        ex["summary_line"] = "트리거 미충족 — 관망"

    return {
        "market": mkt,
        "market_icon": icon,
        "inverse_mode_active": mode,
        "tail_balance": tail,
        "triggers": triggers,
        "open_inverse": opens,
        "execution": ex,
    }


def build_market_report_opening(
    market: str,
    sys_config: dict[str, Any],
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> str:
    icon = "🇰🇷" if market.upper() == "KR" else "🇺🇸"
    dd = sys_config.get("DOOMSDAY_DEFCON") or {}
    if not isinstance(dd, dict):
        dd = {}
    banner = format_doomsday_banner_html(
        market_icon=icon,
        defcon_block=dd,
        regime=str(dd.get("regime") or ""),
    )
    short_ctx = collect_short_sleeve_context(market, sys_config, conn=conn)
    short_html = format_short_sleeve_html(short_ctx)
    return banner + short_html


def build_market_evolution_digest(
    market: str,
    meta: dict[str, Any],
) -> str:
    """레거시 — 시장 인자 무시, 글로벌 [Δ]와 동일."""
    _ = market
    return build_global_evolution_digest_html(meta)
