"""
Mega-Trend Internal Monitor (Kill-Switch 1번) — 장부 기반 PnL·승률 자가 진단.

외부 가격/수급 없이 forward_trades 실측 데이터만으로
MEGA_TREND_SECTOR 내부 동력(Win Rate · MFE · 본절튕김) 추적.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from exit_dynamics import (
    MEGA_TREND_INTERNAL_DIAG_KEY,
    evaluate_internal_momentum_loss,
    mega_trend_internal_thresholds,
)
from mega_trend_ignition import MEGA_TREND_CONFIG_KEY, load_mega_trend_state
from mega_trend_trade_filter import is_mega_trend_unlock_trade


def _connect(conn: Optional[sqlite3.Connection] = None) -> Optional[sqlite3.Connection]:
    if conn is not None:
        return conn
    try:
        import auto_forward_tester as aft

        return sqlite3.connect(aft.DB_PATH, timeout=30)
    except Exception:
        return None


def _normalize_sector_row(
    code: object,
    sector_raw: object,
) -> str:
    from mega_trend_ignition import resolve_kr_code_sector

    return resolve_kr_code_sector(code, sector_raw)


def fetch_mega_trend_sector_trades(
    conn: sqlite3.Connection,
    sector: str,
    *,
    ignited_at: Optional[str] = None,
    window_n: Optional[int] = None,
    include_open: bool = True,
) -> List[Dict[str, Any]]:
    """
    MEGA_TREND 언락 구간 체결 — 섹터 일치 + (MegaTrend 태그 또는 ignited_at 이후).
    최신 N건, 시간순 오래된→최신 (가속도 계산용).
    """
    thr = mega_trend_internal_thresholds(sector=sector)
    n_lim = int(window_n or thr["window_n"])
    since = str(ignited_at or "")[:10] if ignited_at else ""

    status_clause = "status LIKE 'CLOSED%' OR status='OPEN'" if include_open else "status LIKE 'CLOSED%'"

    try:
        rows = conn.execute(
            f"""
            SELECT id, code, sector, sig_type, status, entry_date, exit_date,
                   final_ret, mfe, max_high, entry_price, sim_stat_ret,
                   exit_type, exit_reason, invest_amount, sim_kelly_invest
            FROM forward_trades
            WHERE market='KR' AND ({status_clause})
            ORDER BY COALESCE(exit_date, entry_date) DESC, id DESC
            LIMIT ?
            """,
            (max(n_lim * 4, 40),),
        ).fetchall()
    except Exception:
        return []

    target = str(sector)
    picked: List[Dict[str, Any]] = []
    for row in rows or []:
        (
            rid,
            code,
            sec_raw,
            sig,
            status,
            entry_date,
            exit_date,
            final_ret,
            mfe,
            max_high,
            entry_price,
            sim_stat_ret,
            exit_type,
            exit_reason,
            invest_amt,
            sk_inv,
        ) = row
        sec = _normalize_sector_row(code, sec_raw)
        if sec != target:
            continue
        sig_s = str(sig or "")
        ed = str(entry_date or "")[:10]
        if since and ed and ed < since:
            continue
        if not is_mega_trend_unlock_trade(
            sig_type=sig_s, entry_date=ed, ignited_at=since or None
        ):
            continue
        picked.append(
            {
                "id": rid,
                "code": str(code),
                "sector": sec,
                "sig_type": sig_s,
                "status": str(status),
                "entry_date": ed,
                "exit_date": str(exit_date or "")[:10],
                "final_ret": final_ret,
                "mfe": mfe,
                "max_high": max_high,
                "entry_price": entry_price,
                "sim_stat_ret": sim_stat_ret,
                "exit_type": exit_type,
                "exit_reason": exit_reason,
                "invest_amount": invest_amt,
                "sim_kelly_invest": sk_inv,
            }
        )
        if len(picked) >= n_lim:
            break

    picked.reverse()
    return picked


def evaluate_sector_internal_diagnosis(
    sector: str,
    conn: Optional[sqlite3.Connection] = None,
    *,
    ignited_at: Optional[str] = None,
    thresholds: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """단일 섹터 내재적 자가 진단."""
    c = _connect(conn)
    if c is None:
        return {
            "sector": sector,
            "momentum_lost": False,
            "reason": "db_unavailable",
        }
    own = conn is None
    try:
        trades = fetch_mega_trend_sector_trades(
            c, sector, ignited_at=ignited_at
        )
        diag = evaluate_internal_momentum_loss(
            trades,
            thresholds=thresholds or mega_trend_internal_thresholds(sector=sector),
        )
        diag["sector"] = sector
        diag["n_trades_fetched"] = len(trades)
        diag["trades_sample"] = [
            {
                "id": t.get("id"),
                "code": t.get("code"),
                "status": t.get("status"),
                "final_ret": t.get("final_ret"),
                "mfe": t.get("mfe"),
            }
            for t in trades[-5:]
        ]
        return diag
    finally:
        if own and c is not None:
            try:
                c.close()
            except Exception:
                pass


def refresh_mega_trend_internal_diagnostics(
    config: Dict[str, Any],
    *,
    save_config_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """
    [1번] 활성 MEGA_TREND_SECTOR — 장부 기반 PnL·승률 모니터링 갱신.
    config[MEGA_TREND_SECTOR][internal_diagnostics] 영속화.
    """
    state = load_mega_trend_state(config)
    if not state.get("active"):
        return {"updated": False, "reason": "mega_trend_inactive"}

    sectors = list(state.get("sectors") or [])
    primary = state.get("primary_sector")
    if primary and str(primary) not in [str(s) for s in sectors]:
        sectors.insert(0, str(primary))
    if not sectors:
        return {"updated": False, "reason": "no_sectors"}

    ignited_at = state.get("ignited_at")
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sector_diags: Dict[str, Any] = {}
    any_lost = False
    lost_sectors: List[str] = []

    own = conn is None
    c = _connect(conn)
    try:
        for sec in sectors:
            diag = evaluate_sector_internal_diagnosis(
                str(sec), c, ignited_at=ignited_at
            )
            sector_diags[str(sec)] = diag
            if diag.get("momentum_lost"):
                any_lost = True
                lost_sectors.append(str(sec))
    finally:
        if own and c is not None:
            try:
                c.close()
            except Exception:
                pass

    block = dict(state)
    block[MEGA_TREND_INTERNAL_DIAG_KEY] = {
        "updated_at": now_s,
        "ignited_at": ignited_at,
        "sectors": sector_diags,
        "any_momentum_lost": any_lost,
        "momentum_lost_sectors": lost_sectors,
        "primary_momentum_lost": bool(
            sector_diags.get(str(primary or ""), {}).get("momentum_lost")
        ),
        "thresholds": mega_trend_internal_thresholds(sector=str(primary or "") or None),
    }
    config[MEGA_TREND_CONFIG_KEY] = block

    if save_config_fn:
        save_config_fn(config)

    if any_lost:
        print(
            f"🧠 [Mega-Trend Internal] 내부 동력 상실 자가진단 — "
            f"섹터 {lost_sectors} | "
            f"{sector_diags.get(lost_sectors[0], {}).get('reason', '')}"
        )
    else:
        primary_diag = sector_diags.get(str(primary or ""), {})
        wr = (primary_diag.get("metrics") or {}).get("win_rate")
        mfe_r = (primary_diag.get("metrics") or {}).get("mfe_reach_rate")
        print(
            f"🧠 [Mega-Trend Internal] 동력 양호 "
            f"(primary={primary} WR={wr} MFE={mfe_r})"
        )

    return {
        "updated": True,
        "any_momentum_lost": any_lost,
        "momentum_lost_sectors": lost_sectors,
        "diagnostics": block[MEGA_TREND_INTERNAL_DIAG_KEY],
    }


def load_internal_diagnostics(
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """config 에서 internal_diagnostics 블록 읽기."""
    state = load_mega_trend_state(config)
    diag = state.get(MEGA_TREND_INTERNAL_DIAG_KEY)
    return dict(diag) if isinstance(diag, dict) else {}


def is_internal_momentum_lost(
    config: Optional[Mapping[str, Any]] = None,
    sector: Optional[str] = None,
) -> bool:
    """[1번] 내재적 동력 상실 여부 — internal_momentum_kill 판정 소비."""
    diag = load_internal_diagnostics(config)
    if not diag:
        return False
    if diag.get("any_momentum_lost"):
        if not sector:
            return True
        lost = diag.get("momentum_lost_sectors") or []
        return str(sector) in [str(s) for s in lost]
    return False
