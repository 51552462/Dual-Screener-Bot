"""
forward_trades OPEN 장부 ↔ 리포트 정합 — 가상매매 notional 기준 SSOT.
"""
from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import pandas as pd

from market_db_paths import MARKET_DATA_DB_PATH


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
    return (
        sig.str.contains("INCUBATOR", na=False, regex=False)
        | sig.str.contains("OBSERVE_ONLY", na=False, regex=False)
        | sig.str.contains("기각/관찰용", na=False, regex=False)
    )


def _qty_numeric(df: pd.DataFrame) -> pd.Series:
    parts = []
    if "current_qty" in df.columns:
        parts.append(pd.to_numeric(df["current_qty"], errors="coerce").fillna(0.0))
    if "shares" in df.columns:
        parts.append(pd.to_numeric(df["shares"], errors="coerce").fillna(0.0))
    if not parts:
        return pd.Series(0.0, index=df.index, dtype=float)
    out = parts[0].copy()
    for p in parts[1:]:
        out = pd.concat([out, p], axis=1).max(axis=1)
    return out.astype(float)


def reporter_notional_mask(df: pd.DataFrame) -> pd.Series:
    """가상매매 유효 명목: shares/qty 또는 sim_kelly/invest > 0."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    qty = _qty_numeric(df) > 0
    sk = (
        pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0)
        if "sim_kelly_invest" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    inv = (
        pd.to_numeric(df["invest_amount"], errors="coerce").fillna(0.0)
        if "invest_amount" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    ep = (
        pd.to_numeric(df["entry_price"], errors="coerce").fillna(0.0)
        if "entry_price" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    return qty | (sk > 0) | (inv > 0) | (ep > 0)


def reporter_valid_holding_mask(df: pd.DataFrame) -> pd.Series:
    """리포트·리더보드·쿼터 — OPEN + 명목 + 비관측."""
    if df is None or df.empty or "status" not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [], dtype=bool)
    u = df["status"].astype(str).str.strip().str.upper()
    live = u.isin(["OPEN", "ACTIVE"])
    return live & reporter_notional_mask(df) & ~_sig_excluded_from_holdings(df)


def compute_open_book_stats(
    df_market: pd.DataFrame,
    *,
    market: str,
    session_anchor: str,
    valid_mask_fn=None,
) -> OpenBookStats:
    mk = str(market).upper()
    anchor = str(session_anchor or "")[:10]
    mask_fn = valid_mask_fn or reporter_valid_holding_mask

    if df_market is None or df_market.empty:
        return OpenBookStats(
            market=mk,
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

    st = df_market["status"].astype(str).str.upper()
    closed_n = int(st.str.contains("CLOSED", na=False).sum())

    note = "ok"
    n_raw = int(raw_m.sum())
    n_val = int(valid_m.sum())
    if n_raw > 0 and n_val == 0:
        note = "OPEN_RAW_BUT_ZERO_VALID — shares·sim_kelly·invest 점검 또는 zombie 정리 확인"
    elif n_raw == 0 and today_valid.sum() == 0 and anchor:
        note = "OPEN_EMPTY — 스캔→try_add_virtual_position·SessionDedup·track_daily 청산 경로 점검"

    return OpenBookStats(
        market=mk,
        session_anchor=anchor,
        open_raw=n_raw,
        open_valid=n_val,
        open_ghost=int(ghost_m.sum()),
        open_today_raw=int(today_raw.sum()),
        open_today_valid=int(today_valid.sum()),
        closed_window=closed_n,
        integrity_note=note,
    )


def sql_open_counts(
    conn: sqlite3.Connection,
    market: str,
    *,
    session_anchor: Optional[str] = None,
) -> Dict[str, int]:
    mk = str(market).upper()
    anchor = str(session_anchor or "")[:10]
    raw = conn.execute(
        """
        SELECT COUNT(*) FROM forward_trades
        WHERE UPPER(TRIM(COALESCE(market,''))) = ?
          AND UPPER(TRIM(COALESCE(status,''))) = 'OPEN'
        """,
        (mk,),
    ).fetchone()[0]
    today = 0
    if len(anchor) == 10:
        today = conn.execute(
            """
            SELECT COUNT(*) FROM forward_trades
            WHERE UPPER(TRIM(COALESCE(market,''))) = ?
              AND UPPER(TRIM(COALESCE(status,''))) = 'OPEN'
              AND substr(CAST(entry_date AS TEXT),1,10) = ?
            """,
            (mk, anchor),
        ).fetchone()[0]
    return {"open_raw_sql": int(raw or 0), "open_today_sql": int(today or 0)}


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
    market: str,
    *,
    db_path: Optional[str] = None,
    session_anchor: Optional[str] = None,
) -> OpenBookStats:
    path = db_path or MARKET_DATA_DB_PATH
    mk = str(market).upper()
    conn = sqlite3.connect(path, timeout=60)
    try:
        df = pd.read_sql(
            """
            SELECT id, market, code, name, status, entry_date, shares,
                   sim_kelly_invest, invest_amount, entry_price, sig_type
            FROM forward_trades
            WHERE UPPER(TRIM(COALESCE(market,''))) = ?
            """,
            conn,
            params=(mk,),
        )
        sql = sql_open_counts(conn, mk, session_anchor=session_anchor)
    finally:
        conn.close()
    stats = compute_open_book_stats(
        df,
        market=mk,
        session_anchor=session_anchor or "",
    )
    if stats.open_raw != sql.get("open_raw_sql", stats.open_raw):
        return OpenBookStats(
            **{
                **stats.as_dict(),
                "integrity_note": (
                    f"{stats.integrity_note}; sql_open_raw={sql.get('open_raw_sql')}"
                ),
            }
        )
    return stats
