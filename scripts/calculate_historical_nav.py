#!/usr/bin/env python3
"""
Historical NAV Replay — 과거 모든 청산을 시간순으로 복리 적용해 '진짜 누적 자산(True NAV)'을 역산.

KR 3억 원 / US 30만 달러에서 출발하여, 각 거래 당시의 유효 켈리(sim_kelly_risk_pct)를 적용:

    E_t = E_{t-1} × (1 + f · R_t),   R_t = final_ret/100

최종 시장별 NAV / HWM(최고 자산) / MDD(최대 낙폭%)를 treasury_state.json 에 확정 기록한다.

사용:
    python3 scripts/calculate_historical_nav.py            # 복원 + 저장
    python3 scripts/calculate_historical_nav.py --dry-run  # 계산만(미저장)
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Dict, List, Optional, Tuple

# 디렉토리 위치와 무관하게 시스템 루트(상위 폴더)의 모듈을 임포트할 수 있게 경로 주입.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import live_nav_manager as nav_mgr


def _fetch_closed_rows(conn: sqlite3.Connection, market: str) -> List[Tuple[str, float, float]]:
    """(exit_date, final_ret%, kelly_pct) 시간순. INCUBATOR 섀도우 제외."""
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) AS ed,
            final_ret,
            COALESCE(NULLIF(sim_kelly_risk_pct, 0), ?) AS kelly_pct
        FROM forward_trades
        WHERE market = ?
          AND status LIKE 'CLOSED%'
          AND final_ret IS NOT NULL
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
        ORDER BY ed ASC, id ASC
        """,
        (nav_mgr.DEFAULT_EFFECTIVE_KELLY, market),
    ).fetchall()
    out: List[Tuple[str, float, float]] = []
    for ed, ret, k in rows:
        try:
            r = float(ret)
        except (TypeError, ValueError):
            continue
        try:
            kp = float(k)
        except (TypeError, ValueError):
            kp = nav_mgr.DEFAULT_EFFECTIVE_KELLY
        # sim_kelly_risk_pct 는 비율(0.02) 저장이 기본. 혹시 %(2.0)로 저장된 경우 보정.
        if kp > 1.0:
            kp = kp / 100.0
        kp = min(nav_mgr.MAX_EFFECTIVE_KELLY, max(nav_mgr.MIN_EFFECTIVE_KELLY, kp))
        out.append((str(ed)[:10], r, kp))
    return out


def replay_market(conn: sqlite3.Connection, market: str) -> Dict[str, float]:
    base = nav_mgr.base_capital_for(market)
    rows = _fetch_closed_rows(conn, market)

    nav = base
    hwm = base
    mdd_pct = 0.0
    last_exit: Optional[str] = None
    for ed, ret, kelly in rows:
        growth = 1.0 + kelly * (ret / 100.0)
        if growth < 0.0:
            growth = 0.0  # -100% 미만(자본 음수) 방어
        nav = max(0.0, nav * growth)
        hwm = max(hwm, nav)
        if hwm > 0:
            dd = (hwm - nav) / hwm * 100.0
            if dd > mdd_pct:
                mdd_pct = dd
        last_exit = ed

    return {
        "base": base,
        "nav": nav,
        "hwm": hwm,
        "mdd_pct": mdd_pct,
        "n_closed": len(rows),
        "last_exit_date": last_exit,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Historical NAV Replay → treasury_state.json")
    ap.add_argument("--dry-run", action="store_true", help="계산만 하고 저장하지 않음")
    args = ap.parse_args()

    from market_db_paths import report_db_read_path

    db = report_db_read_path()
    print(f"DB={db}")
    conn = sqlite3.connect(db, timeout=60)
    try:
        results: Dict[str, Dict[str, float]] = {}
        for mkt in ("KR", "US"):
            res = replay_market(conn, mkt)
            results[mkt] = res
            cur = nav_mgr.currency_for(mkt)
            growth_pct = (res["nav"] / res["base"] - 1.0) * 100.0 if res["base"] else 0.0
            print(
                f"\n=== {mkt} ({cur['code']}) ===\n"
                f"  청산건수      : {int(res['n_closed'])}\n"
                f"  기준자본      : {nav_mgr.format_currency(mkt, res['base'])}\n"
                f"  복원 NAV      : {nav_mgr.format_currency(mkt, res['nav'])} "
                f"({growth_pct:+.2f}%)\n"
                f"  HWM(최고자산) : {nav_mgr.format_currency(mkt, res['hwm'])}\n"
                f"  MDD(최대낙폭) : -{res['mdd_pct']:.2f}%\n"
                f"  마지막 청산일 : {res['last_exit_date']}"
            )
    finally:
        conn.close()

    if args.dry_run:
        print("\n[dry-run] treasury_state.json 미저장.")
        return 0

    for mkt in ("KR", "US"):
        res = results[mkt]
        nav_mgr.overwrite_market_state(
            mkt,
            nav=res["nav"],
            hwm=res["hwm"],
            mdd_pct=res["mdd_pct"],
            n_closed=int(res["n_closed"]),
            last_exit_date=res["last_exit_date"],
        )
    print(f"\n✅ treasury_state.json 확정 기록 완료 → {nav_mgr.treasury_state_path()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
