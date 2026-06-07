"""
일일 통합 9분할 리포트 — ReportTimekeeper SSOT · 시장별 데이터 슬라이스.
"""
from __future__ import annotations

import html
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
import pytz

from forward_dual_track_queries import query_latest_closed_trade_date
from market_db_paths import report_db_read_path, report_read_source_label
from report_date_utils import closed_event_dates, in_date_window, normalize_date_series
from reports.report_timekeeper import ReadSource, ReportTimekeeper, business_lag_days

_KR_TZ = pytz.timezone("Asia/Seoul")


@dataclass(frozen=True)
class DailyReportMarketSlice:
    """시장 1회분 리포트용 정규화 DataFrame."""

    market: str
    df_window: pd.DataFrame
    df_real: pd.DataFrame
    df_closed: pd.DataFrame
    df_open: pd.DataFrame
    n_closed_window: int
    n_open_valid: int


@dataclass(frozen=True)
class DailyReportContext:
    tk_kr: ReportTimekeeper
    tk_us: ReportTimekeeper
    db_read_path: str
    read_source_label: str
    window_days: int

    @classmethod
    def build(
        cls,
        *,
        ref_kst: Optional[datetime] = None,
        rolling_days: Optional[int] = None,
    ) -> "DailyReportContext":
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
            try:
                rd = int(cfg.get("FORWARD_DEEP_DIVE_EXIT_WINDOW_DAYS", 90))
            except (TypeError, ValueError):
                rd = 90
            wd = int(rolling_days if rolling_days is not None else rd)
            if wd not in (90, 180):
                wd = 90
        except Exception:
            wd = int(rolling_days or 90)

        path = report_db_read_path()
        src = report_read_source_label(path)
        read_src: ReadSource = "MAIN" if src == "MAIN" else "SNAPSHOT"
        wm_kr: Optional[str] = None
        wm_us: Optional[str] = None
        try:
            uri = path.replace("\\", "/")
            conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=60)
            wm_kr = query_latest_closed_trade_date(conn, "KR")
            wm_us = query_latest_closed_trade_date(conn, "US")
            conn.close()
        except Exception:
            pass

        tk_kr = ReportTimekeeper.for_market(
            "KR",
            rolling_days=wd,
            ref_kst=ref_kst,
            db_watermark_exit=wm_kr,
            read_source=read_src,
        )
        tk_us = ReportTimekeeper.for_market(
            "US",
            rolling_days=wd,
            ref_kst=ref_kst,
            db_watermark_exit=wm_us,
            read_source=read_src,
        )
        return cls(
            tk_kr=tk_kr,
            tk_us=tk_us,
            db_read_path=path,
            read_source_label=src,
            window_days=wd,
        )

    @property
    def calendar_today_kst(self) -> str:
        return self.tk_kr.calendar_today_kst

    def timekeeper_for(self, market: str) -> ReportTimekeeper:
        return self.tk_us if str(market).upper() == "US" else self.tk_kr

    def anchor_for(self, market: str) -> str:
        return self.timekeeper_for(market).session_anchor

    def rolling_cutoff_for(self, market: str) -> str:
        return self.timekeeper_for(market).rolling_cutoff

    def lag_for(self, market: str) -> int:
        tk = self.timekeeper_for(market)
        return business_lag_days(
            tk.db_watermark_exit, tk.session_anchor, market=tk.market
        )

    def global_header_html(self) -> str:
        wm_kr = self.tk_kr.db_watermark_exit or "—"
        wm_us = self.tk_us.db_watermark_exit or "—"
        lag_kr = self.lag_for("KR")
        lag_us = self.lag_for("US")
        return (
            f"📎 리포트일 KST <b>{html.escape(self.calendar_today_kst)}</b> · "
            f"KR앵커 <b>{html.escape(self.tk_kr.session_anchor)}</b> · "
            f"US앵커(ET) <b>{html.escape(self.tk_us.session_anchor)}</b> · "
            f"DB워터마크 KR <b>{html.escape(str(wm_kr))}</b> · "
            f"US <b>{html.escape(str(wm_us))}</b> · "
            f"lag KR <b>{lag_kr}</b> · US <b>{lag_us}</b> · "
            f"롤링 <b>{self.window_days}</b>일 · "
            f"읽기 <b>{html.escape(self.read_source_label)}</b>\n"
        )

    def market_window_header_html(
        self,
        market: str,
        *,
        n_real: int,
        n_closed: int,
        n_open: int,
    ) -> str:
        mk = str(market).upper()
        tk = self.timekeeper_for(mk)
        wm = tk.db_watermark_exit or "—"
        lag = self.lag_for(mk)
        return (
            f"◽ <i>{mk} 윈도우 <b>{html.escape(tk.rolling_cutoff)}</b>~"
            f"<b>{html.escape(tk.session_anchor)}</b> · "
            f"표본 실거래 <b>{n_real}</b> · 청산 <b>{n_closed}</b> · "
            f"유효OPEN <b>{n_open}</b> · 워터마크 <b>{html.escape(str(wm))}</b> · "
            f"lag <b>{lag}</b></i>\n"
        )

    def load_market_slice(
        self,
        conn: sqlite3.Connection,
        market: str,
        *,
        df_long_only_fn: Callable[[pd.DataFrame], pd.DataFrame],
        normalize_market_fn: Callable[[pd.DataFrame, str], pd.DataFrame],
        valid_open_mask_fn: Callable[[pd.DataFrame], pd.Series],
    ) -> DailyReportMarketSlice:
        """
        OPEN(전체) + CLOSED(exit_date ∈ [rolling_cutoff, session_anchor]).
        INCUBATOR 제외는 df_long_only_fn에서 처리.
        """
        mkt = str(market).upper()
        tk = self.timekeeper_for(mkt)
        # SQL 단에서 exit_date만 강제하면 timezone/포맷 차이(T/공백, TZ suffix)나
        # exit_date 누락(legacy row) 때문에 CLOSED 표본이 0건으로 누락될 수 있다.
        # market+INCUBATOR만 1차 필터 후, 세션 윈도우는 Pandas에서 안전하게 재평가한다.
        df_raw = pd.read_sql(
            """
            SELECT * FROM forward_trades
            WHERE market = ?
              AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
            """,
            conn,
            params=(mkt,),
        )
        df_norm = normalize_market_fn(df_raw, mkt)
        df_all_real = df_long_only_fn(df_norm)
        if "exit_date" in df_all_real.columns:
            df_all_real["exit_date"] = normalize_date_series(df_all_real["exit_date"])
        if "entry_date" in df_all_real.columns:
            df_all_real["entry_date"] = normalize_date_series(df_all_real["entry_date"])

        status_s = df_all_real["status"].astype(str).str.upper().str.strip()
        closed_mask = status_s.str.contains("CLOSED", na=False)
        open_mask = (status_s == "OPEN") | (status_s == "ACTIVE")

        closed_day = closed_event_dates(df_all_real)
        window_closed_mask = closed_mask & in_date_window(
            closed_day, tk.rolling_cutoff, tk.session_anchor
        )

        open_subset = df_all_real.loc[open_mask].copy()
        valid_open = valid_open_mask_fn(open_subset)
        df_open = open_subset.loc[valid_open].copy()
        df_closed = df_all_real.loc[window_closed_mask].copy()
        df_window = pd.concat([df_open, df_closed], axis=0).drop_duplicates(
            subset=["id"] if "id" in df_all_real.columns else None
        )

        return DailyReportMarketSlice(
            market=mkt,
            df_window=df_window,
            df_real=df_all_real,
            df_closed=df_closed,
            df_open=df_open,
            n_closed_window=int(len(df_closed)),
            n_open_valid=int(len(df_open)),
        )
