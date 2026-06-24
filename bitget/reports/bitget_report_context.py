"""
Bitget 일일 리포트 — SPOT/FUTURES 시간 앵커 SSOT (주식 DailyReportContext 패턴).

코인 24/7: session_anchor = UTC 달력일, rolling_cutoff = anchor - window_days.
"""
from __future__ import annotations

import html
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from bitget.forward.forward_book_integrity import reporter_valid_holding_mask
from bitget.infra.data_paths import report_db_read_path


def _market_label(market_type: str) -> str:
    m = str(market_type or "spot").strip().lower()
    return "FUT" if m in ("futures", "fut", "future") else "SPOT"


@dataclass(frozen=True)
class BitgetReportTimekeeper:
    market: str
    session_anchor: str
    rolling_cutoff: str
    rolling_days: int
    db_watermark_exit: Optional[str]

    @classmethod
    def for_market(
        cls,
        market_type: str,
        *,
        rolling_days: int = 90,
        db_watermark_exit: Optional[str] = None,
    ) -> "BitgetReportTimekeeper":
        mk = _market_label(market_type)
        now = datetime.now(timezone.utc)
        anchor = now.strftime("%Y-%m-%d")
        rd = 90 if rolling_days not in (90, 180) else int(rolling_days)
        cutoff = (now.date() - timedelta(days=rd)).strftime("%Y-%m-%d")
        return cls(
            market=mk,
            session_anchor=anchor,
            rolling_cutoff=cutoff,
            rolling_days=rd,
            db_watermark_exit=db_watermark_exit,
        )


@dataclass(frozen=True)
class BitgetReportMarketSlice:
    market: str
    df_window: pd.DataFrame
    df_real: pd.DataFrame
    df_closed: pd.DataFrame
    df_open: pd.DataFrame
    n_closed_window: int
    n_open_valid: int


@dataclass(frozen=True)
class BitgetReportContext:
    tk_spot: BitgetReportTimekeeper
    tk_futures: BitgetReportTimekeeper
    db_read_path: str
    window_days: int
    calendar_today_utc: str

    @classmethod
    def build(cls, *, rolling_days: Optional[int] = None) -> "BitgetReportContext":
        try:
            from bitget.infra import config_manager

            cfg = config_manager.load_system_config() or {}
            try:
                rd = int(cfg.get("FORWARD_DEEP_DIVE_EXIT_WINDOW_DAYS", 90))
            except (TypeError, ValueError):
                rd = 90
            wd = int(rolling_days if rolling_days is not None else rd)
        except Exception:
            wd = int(rolling_days or 90)

        path = report_db_read_path()
        wm_spot: Optional[str] = None
        wm_fut: Optional[str] = None
        try:
            uri = path.replace("\\", "/")
            conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=60)
            for mtype, slot in (("spot", "wm_spot"), ("futures", "wm_fut")):
                row = conn.execute(
                    """
                    SELECT MAX(substr(CAST(exit_date AS TEXT),1,10))
                    FROM bitget_forward_trades
                    WHERE LOWER(IFNULL(market_type,'')) = ?
                      AND status LIKE 'CLOSED%'
                      AND exit_date IS NOT NULL AND TRIM(CAST(exit_date AS TEXT)) != ''
                    """,
                    (mtype,),
                ).fetchone()
                val = row[0] if row else None
                if mtype == "spot":
                    wm_spot = val
                else:
                    wm_fut = val
            conn.close()
        except Exception:
            pass

        return cls(
            tk_spot=BitgetReportTimekeeper.for_market(
                "spot", rolling_days=wd, db_watermark_exit=wm_spot
            ),
            tk_futures=BitgetReportTimekeeper.for_market(
                "futures", rolling_days=wd, db_watermark_exit=wm_fut
            ),
            db_read_path=path,
            window_days=wd,
            calendar_today_utc=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        )

    def timekeeper_for(self, market_type: str) -> BitgetReportTimekeeper:
        mk = _market_label(market_type)
        return self.tk_futures if mk == "FUT" else self.tk_spot

    def lag_for(self, market_type: str) -> int:
        tk = self.timekeeper_for(market_type)
        wm = tk.db_watermark_exit
        if not wm or len(str(wm)) < 10:
            return 999
        try:
            a = datetime.strptime(tk.session_anchor, "%Y-%m-%d").date()
            w = datetime.strptime(str(wm)[:10], "%Y-%m-%d").date()
            return max(0, (a - w).days)
        except ValueError:
            return 999

    def market_window_header_html(
        self,
        market_type: str,
        *,
        n_real: int,
        n_closed: int,
        n_open: int,
    ) -> str:
        tk = self.timekeeper_for(market_type)
        mk = tk.market
        wl = html.escape(f"{tk.rolling_cutoff}~{tk.session_anchor}")
        wm = html.escape(str(tk.db_watermark_exit or "—"))
        lag = self.lag_for(market_type)
        return (
            f"<i>윈도우 <b>{wl}</b> UTC · {mk} 청산 <b>{n_closed}</b> · "
            f"표본 <b>{n_real}</b> · OPEN <b>{n_open}</b> · "
            f"워터마크 <b>{wm}</b> · lag <b>{lag}</b>d</i>\n"
        )

    def slice_for_market(
        self,
        df_all: pd.DataFrame,
        market_type: str,
    ) -> BitgetReportMarketSlice:
        mk = _market_label(market_type)
        mkt_raw = str(market_type or "spot").strip().lower()
        if df_all is None or df_all.empty:
            empty = pd.DataFrame()
            return BitgetReportMarketSlice(mk, empty, empty, empty, empty, 0, 0)

        if "market_type" in df_all.columns:
            work = df_all[df_all["market_type"].astype(str).str.lower() == mkt_raw].copy()
        else:
            work = df_all.copy()

        tk = self.timekeeper_for(market_type)
        ent = (
            work["entry_date"].astype(str).str[:10]
            if "entry_date" in work.columns
            else pd.Series("", index=work.index)
        )
        win = work.loc[
            (ent >= tk.rolling_cutoff) & (ent <= tk.session_anchor)
        ].copy()

        st = work["status"].astype(str).str.upper() if "status" in work.columns else pd.Series("", index=work.index)
        closed = win[st.str.contains("CLOSED", na=False)].copy()
        open_df = work[st.isin(["OPEN", "ACTIVE"])].copy()
        valid_open = open_df[reporter_valid_holding_mask(open_df)] if not open_df.empty else open_df

        return BitgetReportMarketSlice(
            market=mk,
            df_window=win,
            df_real=win,
            df_closed=closed,
            df_open=open_df,
            n_closed_window=int(len(closed)),
            n_open_valid=int(len(valid_open)),
        )
