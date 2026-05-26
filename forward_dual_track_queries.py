"""
듀얼 트랙 리포팅 — LIVE_TODAY / HIST_BASELINE / CHAMPION_ROLLING DB 쿼리 분리.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Optional, Tuple

import pandas as pd
import pytz

from report_timekeeper import ReportTimekeeper, kr_session_anchor_date

if TYPE_CHECKING:
    pass

_KR_TZ = pytz.timezone("Asia/Seoul")

_LIVE_EXCLUDE_SIG = (
    "IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'",
    "IFNULL(sig_type,'') NOT LIKE '%[R&D_%'",
)


def kst_today() -> date:
    return datetime.now(_KR_TZ).date()


def kst_today_str() -> str:
    return kst_today().strftime("%Y-%m-%d")


def recent_business_day_kst(*, ref: Optional[date] = None) -> date:
    """토·일이면 직전 금요일, 그 외 당일 (KR 레거시 호환)."""
    return kr_session_anchor_date(ref=ref)


def trade_date_column_sql() -> str:
    """SQLite: trade_date 컬럼이 있으면 우선, 없으면 exit_date → entry_date."""
    return (
        "COALESCE("
        "NULLIF(TRIM(CAST(trade_date AS TEXT)), ''), "
        "NULLIF(TRIM(CAST(exit_date AS TEXT)), ''), "
        "NULLIF(TRIM(CAST(entry_date AS TEXT)), '')"
        ")"
    )


def normalize_trade_dates(df: pd.DataFrame) -> pd.Series:
    """DataFrame용 trade_date (YYYY-MM-DD)."""
    if df is None or df.empty:
        return pd.Series(dtype=str)
    if "trade_date" in df.columns:
        base = df["trade_date"].astype(str).str[:10]
    elif "exit_date" in df.columns:
        base = df["exit_date"].astype(str).str[:10]
    elif "entry_date" in df.columns:
        base = df["entry_date"].astype(str).str[:10]
    else:
        return pd.Series([""] * len(df), index=df.index)
    return base.replace({"nan": "", "None": "", "NaT": ""})


def _live_where_clause(has_trade_date_col: bool) -> str:
    td = "trade_date" if has_trade_date_col else None
    if td:
        date_pred = (
            f"(substr(TRIM(CAST({td} AS TEXT)),1,10) = ? "
            f"OR substr(TRIM(CAST(exit_date AS TEXT)),1,10) = ?)"
        )
    else:
        date_pred = "substr(TRIM(CAST(exit_date AS TEXT)),1,10) = ?"
    parts = [
        "market = ?",
        "status LIKE 'CLOSED%'",
        date_pred,
        *_LIVE_EXCLUDE_SIG,
    ]
    return " AND ".join(parts)


def _hist_where_clause(has_trade_date_col: bool) -> str:
    td_expr = trade_date_column_sql() if has_trade_date_col else "substr(TRIM(CAST(exit_date AS TEXT)),1,10)"
    parts = [
        "market = ?",
        "status LIKE 'CLOSED%'",
        f"{td_expr} >= ?",
        f"{td_expr} < ?",
        *_LIVE_EXCLUDE_SIG,
    ]
    return " AND ".join(parts)


def _champion_rolling_where_clause(has_trade_date_col: bool) -> str:
    """롤링 윈도우 [cutoff, session_anchor] 양끝 포함 — 최우수 성적표 전용."""
    td_expr = trade_date_column_sql() if has_trade_date_col else "substr(TRIM(CAST(exit_date AS TEXT)),1,10)"
    parts = [
        "market = ?",
        "status LIKE 'CLOSED%'",
        f"{td_expr} >= ?",
        f"{td_expr} <= ?",
        *_LIVE_EXCLUDE_SIG,
    ]
    return " AND ".join(parts)


def _table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(str(r[1]) == col for r in cur.fetchall())


@dataclass(frozen=True)
class DualTrackQueryMeta:
    market: str
    calendar_today: str
    anchor_business_day: str
    rolling_cutoff: str
    live_row_count: int
    hist_row_count: int
    latest_closed_trade_date: Optional[str]


@dataclass(frozen=True)
class LiveStalenessVerdict:
    is_stale: bool
    banner_html: str
    reason: str


def query_latest_closed_trade_date(
    conn: sqlite3.Connection, market: str
) -> Optional[str]:
    has_td = _table_has_column(conn, "forward_trades", "trade_date")
    td_expr = trade_date_column_sql() if has_td else "substr(TRIM(CAST(exit_date AS TEXT)),1,10)"
    row = conn.execute(
        f"""
        SELECT MAX({td_expr}) FROM forward_trades
        WHERE market=? AND status LIKE 'CLOSED%'
          AND {_LIVE_EXCLUDE_SIG[0]}
          AND {_LIVE_EXCLUDE_SIG[1]}
        """,
        (market,),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])[:10]


def fetch_live_today_closed(
    conn: sqlite3.Connection,
    market: str,
    anchor_day: str,
) -> pd.DataFrame:
    has_td = _table_has_column(conn, "forward_trades", "trade_date")
    where = _live_where_clause(has_td)
    if has_td:
        params: Tuple[object, ...] = (market, anchor_day, anchor_day)
    else:
        params = (market, anchor_day)
    return pd.read_sql(
        f"SELECT * FROM forward_trades WHERE {where} ORDER BY exit_date DESC",
        conn,
        params=params,
    )


def fetch_hist_baseline_closed(
    conn: sqlite3.Connection,
    market: str,
    anchor_day: str,
    rolling_cutoff: str,
) -> pd.DataFrame:
    has_td = _table_has_column(conn, "forward_trades", "trade_date")
    where = _hist_where_clause(has_td)
    return pd.read_sql(
        f"SELECT * FROM forward_trades WHERE {where} ORDER BY exit_date DESC",
        conn,
        params=(market, rolling_cutoff, anchor_day),
    )


def fetch_champion_rolling_closed(
    conn: sqlite3.Connection,
    market: str,
    anchor_day: str,
    rolling_cutoff: str,
) -> pd.DataFrame:
    """최우수 성적표: 세션 앵커일 청산 포함 롤링."""
    has_td = _table_has_column(conn, "forward_trades", "trade_date")
    where = _champion_rolling_where_clause(has_td)
    return pd.read_sql(
        f"SELECT * FROM forward_trades WHERE {where} ORDER BY exit_date DESC",
        conn,
        params=(market, rolling_cutoff, anchor_day),
    )


def load_dual_track_frames(
    conn: sqlite3.Connection,
    market: str,
    *,
    timekeeper: Optional[ReportTimekeeper] = None,
    rolling_days: int = 90,
    anchor_day: Optional[str] = None,
    calendar_today: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, DualTrackQueryMeta]:
    if timekeeper is not None:
        cal = timekeeper.calendar_today_kst
        anchor = timekeeper.session_anchor
        cutoff = timekeeper.rolling_cutoff
    else:
        cal = calendar_today or kst_today_str()
        anchor = anchor_day or recent_business_day_kst().strftime("%Y-%m-%d")
        cutoff = (
            datetime.strptime(anchor, "%Y-%m-%d").date() - timedelta(days=int(rolling_days))
        ).strftime("%Y-%m-%d")

    df_live = fetch_live_today_closed(conn, market, anchor)
    df_hist = fetch_hist_baseline_closed(conn, market, anchor, cutoff)
    df_champion = fetch_champion_rolling_closed(conn, market, anchor, cutoff)
    latest = query_latest_closed_trade_date(conn, market)

    meta = DualTrackQueryMeta(
        market=str(market).upper(),
        calendar_today=cal,
        anchor_business_day=anchor,
        rolling_cutoff=cutoff,
        live_row_count=len(df_live),
        hist_row_count=len(df_hist),
        latest_closed_trade_date=latest,
    )
    return df_live, df_hist, df_champion, meta


def assess_live_staleness(meta: DualTrackQueryMeta) -> LiveStalenessVerdict:
    """
    당일 실전 0건이거나 DB 최신 청산일이 앵커 영업일보다 이전이면 stale.
  과거 롤링 데이터를 오늘 실전처럼 표시하지 않도록 방어.
    """
    anchor = meta.anchor_business_day
    latest = meta.latest_closed_trade_date
    stale_reasons: list[str] = []

    if meta.live_row_count <= 0:
        stale_reasons.append("당일 실전 청산 0건")

    if latest and latest < anchor:
        stale_reasons.append(
            f"최신 청산일 {latest}이(가) 기준 영업일 {anchor}보다 이전"
        )

    if not stale_reasons:
        return LiveStalenessVerdict(is_stale=False, banner_html="", reason="")

    detail = " · ".join(stale_reasons)
    banner = (
        f"⚠️ <b>[당일 실전 데이터 0건 또는 갱신 지연]</b> "
        f"({html_escape(detail)}) — 아래 🟢 당일 실전 줄은 비어 있을 수 있으며, "
        f"🏛️ 과거 기준(Sim)만 참고하십시오."
    )
    return LiveStalenessVerdict(is_stale=True, banner_html=banner, reason=detail)


def html_escape(s: str) -> str:
    import html

    return html.escape(str(s), quote=False)
