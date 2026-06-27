"""
콜로세움·에이스 로직 심층 부검 — ReportTimekeeper SSOT.
"""
from __future__ import annotations

import html
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pytz

from forward_dual_track_queries import query_latest_closed_trade_date
from market_db_paths import report_db_read_path, report_read_source_label
from reports.report_timekeeper import ReadSource, ReportTimekeeper, business_lag_days

_KR_TZ = pytz.timezone("Asia/Seoul")


@dataclass(frozen=True)
class ColosseumReportContext:
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
        rolling_days: int = 90,
    ) -> "ColosseumReportContext":
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
            rolling_days=rolling_days,
            ref_kst=ref_kst,
            db_watermark_exit=wm_kr,
            read_source=read_src,
        )
        tk_us = ReportTimekeeper.for_market(
            "US",
            rolling_days=rolling_days,
            ref_kst=ref_kst,
            db_watermark_exit=wm_us,
            read_source=read_src,
        )
        return cls(
            tk_kr=tk_kr,
            tk_us=tk_us,
            db_read_path=path,
            read_source_label=src,
            window_days=int(rolling_days),
        )

    def timekeeper_for(self, league: str) -> ReportTimekeeper:
        return self.tk_us if str(league).upper() == "US" else self.tk_kr

    def anchor_for_league(self, league: str) -> str:
        return self.timekeeper_for(league).session_anchor

    def rolling_cutoff_for_league(self, league: str) -> str:
        return self.timekeeper_for(league).rolling_cutoff

    def lag_for_league(self, league: str) -> int:
        tk = self.timekeeper_for(league)
        return business_lag_days(
            tk.db_watermark_exit, tk.session_anchor, market=tk.market
        )

    def global_header_html(self) -> str:
        wm_kr = self.tk_kr.db_watermark_exit or "—"
        wm_us = self.tk_us.db_watermark_exit or "—"
        lag_kr = self.lag_for_league("KR")
        lag_us = self.lag_for_league("US")
        return (
            f"📎 리포트일 KST <b>{html.escape(self.tk_kr.calendar_today_kst)}</b> · "
            f"KR앵커 <b>{html.escape(self.tk_kr.session_anchor)}</b> · "
            f"US앵커(ET) <b>{html.escape(self.tk_us.session_anchor)}</b> · "
            f"DB워터마크 KR <b>{html.escape(str(wm_kr))}</b> · "
            f"US <b>{html.escape(str(wm_us))}</b> · "
            f"lag KR <b>{lag_kr}</b> · US <b>{lag_us}</b> · "
            f"롤링 <b>{self.window_days}</b>일 · "
            f"읽기 <b>{html.escape(self.read_source_label)}</b>\n"
        )
