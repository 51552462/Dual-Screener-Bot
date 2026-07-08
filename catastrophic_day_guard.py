"""
당일 승률 붕괴(CATASTROPHIC_LOSS_DAY) 실시간 켈리 클러치 SSOT.

AI 감사관은 사후 리포트만 생성했고, 진입 경로에는 당일 연패 브레이크가 없어
0% 승률 날에도 effective_kelly 가 그대로 유지되는 구조적 공백을 메운다.

설계:
  · 당일 exit_date 기준 CLOSED 표본이 min_n 이상이고 승률 ≤ wr_threshold 이면 활성
  · mult = 1 − severity×(1−min_mult), severity = 1 − wr/threshold (0%일 때 최소 mult)
  · block 모드: wr ≤ block_wr AND n ≥ block_min_n → 진입 거부(기본 off, wr=0·n≥8 시 on)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pytz


def _cfg_f(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(cfg.get(key, default))
    except (TypeError, ValueError):
        return default


def _cfg_i(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        return int(cfg.get(key, default))
    except (TypeError, ValueError):
        return default


def catastrophic_day_thresholds(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else cfg
    return {
        "min_closed": float(_cfg_i(base, "CATASTROPHIC_DAY_MIN_CLOSED", 5)),
        "wr_threshold_pct": _cfg_f(base, "CATASTROPHIC_DAY_WR_THRESHOLD_PCT", 5.0),
        "min_mult": _cfg_f(base, "CATASTROPHIC_DAY_MIN_MULT", 0.15),
        "block_wr_pct": _cfg_f(base, "CATASTROPHIC_DAY_BLOCK_WR_PCT", 0.0),
        "block_min_closed": float(_cfg_i(base, "CATASTROPHIC_DAY_BLOCK_MIN_CLOSED", 8)),
    }


def _sql_date_normalized(col: str) -> str:
    return (
        f"CASE WHEN {col} IS NULL OR TRIM({col}) = '' THEN NULL "
        f"ELSE date(substr(replace(replace({col}, 'T', ' '), '/', '-'), 1, 10)) END"
    )


def query_today_closed_stats(
    conn: sqlite3.Connection,
    market: str,
    today_str: str,
) -> Dict[str, Any]:
    """당일 청산( exit_date ) 통계 — market 스코프."""
    exit_d = _sql_date_normalized("exit_date")
    mkt = str(market or "").upper()
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN CAST(final_ret AS REAL) > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CAST(sim_kelly_invest AS REAL) * CAST(final_ret AS REAL) / 100.0) AS pnl_est
        FROM forward_trades
        WHERE status LIKE 'CLOSED%%'
          AND UPPER(market) = ?
          AND {exit_d} = date(?)
        """,
        (mkt, today_str),
    ).fetchone()
    n = int(row[0] or 0) if row else 0
    wins = int(row[1] or 0) if row else 0
    pnl = float(row[2] or 0.0) if row else 0.0
    wr = (wins / n * 100.0) if n > 0 else None
    return {
        "n_closed": n,
        "wins": wins,
        "win_rate_pct": wr,
        "realized_pnl_est": pnl,
        "market": mkt,
        "today": today_str,
    }


def evaluate_catastrophic_day_clutch(
    conn: sqlite3.Connection,
    market: str,
    today_str: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    당일 승률 붕괴 클러치 평가.

    Returns:
      active, kelly_mult, block_entry, reason, stats{}
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not cfg.get("ENABLE_CATASTROPHIC_DAY_CLUTCH", True):
        return {
            "active": False,
            "kelly_mult": 1.0,
            "block_entry": False,
            "reason": "disabled",
            "stats": {},
        }

    th = catastrophic_day_thresholds(cfg)
    stats = query_today_closed_stats(conn, market, today_str)
    n = int(stats["n_closed"])
    wr = stats["win_rate_pct"]

    base_out = {
        "active": False,
        "kelly_mult": 1.0,
        "block_entry": False,
        "reason": "neutral",
        "stats": stats,
        "thresholds": th,
    }

    if n < int(th["min_closed"]) or wr is None:
        base_out["reason"] = f"insufficient_sample(n={n})"
        return base_out

    if wr > th["wr_threshold_pct"]:
        base_out["reason"] = f"wr_ok({wr:.1f}%>{th['wr_threshold_pct']:.0f}%)"
        return base_out

    threshold = max(th["wr_threshold_pct"], 0.01)
    severity = max(0.0, min(1.0, 1.0 - (wr / threshold)))
    min_mult = max(0.0, min(1.0, th["min_mult"]))
    mult = 1.0 - severity * (1.0 - min_mult)
    mult = max(min_mult, min(1.0, mult))

    block = bool(cfg.get("ENABLE_CATASTROPHIC_DAY_BLOCK_ENTRIES", True))
    block = block and (
        wr <= th["block_wr_pct"] and n >= int(th["block_min_closed"])
    )

    reason = (
        f"catastrophic_day:wr={wr:.1f}%/n={n}/mult={mult:.3f}"
    )
    if block:
        reason += "/BLOCK"

    return {
        "active": True,
        "kelly_mult": mult,
        "block_entry": block,
        "reason": reason,
        "stats": stats,
        "thresholds": th,
        "severity": severity,
    }


def resolve_today_kst(market: str) -> str:
    tz = pytz.timezone("Asia/Seoul") if str(market).upper() == "KR" else pytz.timezone(
        "America/New_York"
    )
    return datetime.now(tz).strftime("%Y-%m-%d")


def evaluate_catastrophic_day_clutch_for_market(
    conn: sqlite3.Connection,
    market: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    today_str: Optional[str] = None,
) -> Dict[str, Any]:
    ts = today_str or resolve_today_kst(market)
    return evaluate_catastrophic_day_clutch(conn, market, ts, sys_config=sys_config)
