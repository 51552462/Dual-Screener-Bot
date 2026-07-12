"""
bitget_forward_trades OPEN 장부 ↔ 리포트 정합 (주식 forward_book_integrity 이식).
"""
from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import pandas as pd

from bitget.forward.shared import DB_PATH
from bitget.infra.bounded_reads import (
    forward_integrity_closed_window_count_sql,
    forward_open_integrity_open_sql,
    warn_if_open_exceeds_safety,
)
from bitget.infra.clock import utc_date_days_ago_str, utc_date_str
from bitget.infra.memory_policy import FORWARD_INTEGRITY_CLOSED_WINDOW_DAYS
from bitget.infra.shared_db_connector import get_connection


@dataclass(frozen=True)
class OpenBookStats:
    market: str
    session_anchor: str
    open_raw: int
    open_valid: int
    open_ghost: int
    open_today_raw: int
    open_today_valid: int
    closed_window: int
    integrity_note: str

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _open_status_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty or "status" not in df.columns:
        return pd.Series(dtype=bool)
    u = df["status"].astype(str).str.strip().str.upper()
    return u.isin(["OPEN", "ACTIVE"])


def _sig_excluded_from_holdings(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    sig = df["sig_type"].astype(str) if "sig_type" in df.columns else pd.Series("", index=df.index)
    return sig.str.contains("INCUBATOR|OBSERVE_ONLY|기각/관찰용", na=False, regex=True)


def _qty_numeric(df: pd.DataFrame) -> pd.Series:
    parts = []
    if "quantity" in df.columns:
        parts.append(pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0))
    if "shares" in df.columns:
        parts.append(pd.to_numeric(df["shares"], errors="coerce").fillna(0.0))
    if not parts:
        return pd.Series(0.0, index=df.index, dtype=float)
    out = parts[0].copy()
    for p in parts[1:]:
        out = pd.concat([out, p], axis=1).max(axis=1)
    return out.astype(float)


def reporter_notional_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    qty = _qty_numeric(df) > 0
    sk = (
        pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0)
        if "sim_kelly_invest" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    inv = (
        pd.to_numeric(df["margin_used"], errors="coerce").fillna(0.0)
        if "margin_used" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    ep = (
        pd.to_numeric(df["entry_price"], errors="coerce").fillna(0.0)
        if "entry_price" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    return qty | (sk > 0) | (inv > 0) | (ep > 0)


def reporter_valid_holding_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty or "status" not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [], dtype=bool)
    live = _open_status_mask(df)
    return live & reporter_notional_mask(df) & ~_sig_excluded_from_holdings(df)


def _normalize_market_filter(market_type: str) -> tuple[str, str]:
    m = str(market_type or "spot").strip().lower()
    label = "FUT" if m in ("futures", "fut", "future") else "SPOT"
    return label, m


def compute_open_book_stats(
    df_market: pd.DataFrame,
    *,
    market_type: str,
    session_anchor: str,
    valid_mask_fn=None,
    closed_window: int | None = None,
) -> OpenBookStats:
    label, _ = _normalize_market_filter(market_type)
    anchor = str(session_anchor or "")[:10]
    mask_fn = valid_mask_fn or reporter_valid_holding_mask

    if df_market is None or df_market.empty:
        return OpenBookStats(
            market=label,
            session_anchor=anchor,
            open_raw=0,
            open_valid=0,
            open_ghost=0,
            open_today_raw=0,
            open_today_valid=0,
            closed_window=0,
            integrity_note="no_rows",
        )

    raw_m = _open_status_mask(df_market)
    valid_m = mask_fn(df_market)
    ghost_m = raw_m & ~valid_m
    ent = (
        df_market["entry_date"].astype(str).str[:10]
        if "entry_date" in df_market.columns
        else pd.Series("", index=df_market.index)
    )
    today_raw = raw_m & (ent == anchor)
    today_valid = valid_m & (ent == anchor)
    if closed_window is not None:
        closed_n = int(closed_window)
    else:
        st = df_market["status"].astype(str).str.upper()
        closed_n = int(st.str.contains("CLOSED", na=False).sum())

    note = "ok"
    n_raw = int(raw_m.sum())
    n_val = int(valid_m.sum())
    if n_raw > 0 and n_val == 0:
        note = "OPEN_RAW_BUT_ZERO_VALID — quantity·sim_kelly·margin 점검 또는 zombie 정리 확인"
    elif n_raw == 0 and today_valid.sum() == 0 and anchor:
        note = "OPEN_EMPTY — 스캔→try_add·track_daily 청산 경로 점검"

    return OpenBookStats(
        market=label,
        session_anchor=anchor,
        open_raw=n_raw,
        open_valid=n_val,
        open_ghost=int(ghost_m.sum()),
        open_today_raw=int(today_raw.sum()),
        open_today_valid=int(today_valid.sum()),
        closed_window=closed_n,
        integrity_note=note,
    )


def format_open_book_integrity_html(stats: OpenBookStats) -> str:
    if stats.integrity_note == "ok" and stats.open_valid > 0:
        return ""
    import html as _html

    parts = [
        f"📎 장부정합 OPEN원시 <b>{stats.open_raw}</b> · 유효 <b>{stats.open_valid}</b>"
        f" · 유령 <b>{stats.open_ghost}</b> · 당일진입 <b>{stats.open_today_valid}</b>"
    ]
    if stats.integrity_note not in ("ok", "no_rows"):
        parts.append(f" · <i>{_html.escape(stats.integrity_note, quote=False)}</i>")
    return " ".join(parts) + "\n"


def diagnose_open_book_from_db(
    market_type: str,
    *,
    db_path: Optional[str] = None,
    session_anchor: Optional[str] = None,
) -> OpenBookStats:
    _, mkt_raw = _normalize_market_filter(market_type)
    path = db_path or DB_PATH
    anchor = str(session_anchor or utc_date_str())[:10]
    conn = get_connection(path, read_only=True)
    try:
        warn_if_open_exceeds_safety(conn, market_type=mkt_raw)
        open_q, open_params = forward_open_integrity_open_sql(market_type=mkt_raw)
        df = pd.read_sql(open_q, conn, params=open_params)

        since = utc_date_days_ago_str(int(FORWARD_INTEGRITY_CLOSED_WINDOW_DAYS))
        closed_q, closed_params = forward_integrity_closed_window_count_sql(
            market_type=mkt_raw,
            since_date=since,
        )
        closed_row = conn.execute(closed_q, closed_params).fetchone()
        closed_window = int(closed_row[0] if closed_row else 0)
    finally:
        conn.close()
    return compute_open_book_stats(
        df,
        market_type=market_type,
        session_anchor=anchor,
        closed_window=closed_window,
    )
