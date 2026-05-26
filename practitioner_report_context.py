"""
PIL 실무자 리포트 — ReportTimekeeper SSOT · 활성 그룹 · 헤더/스테일니스.
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
from report_staleness_gate import evaluate_staleness
from report_timekeeper import ReadSource, ReportTimekeeper, business_lag_days

_KR_TZ = pytz.timezone("Asia/Seoul")


@dataclass(frozen=True)
class PractitionerReportContext:
    """실무자 리포트 전역 시계·DB 워터마크 SSOT."""

    tk_kr: ReportTimekeeper
    tk_us: ReportTimekeeper
    db_read_path: str
    read_source_label: str

    @classmethod
    def build(
        cls,
        *,
        ref_kst: Optional[datetime] = None,
        rolling_days: int = 90,
    ) -> "PractitionerReportContext":
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
        )

    def timekeeper_for(self, market: str) -> ReportTimekeeper:
        return self.tk_us if str(market).upper() == "US" else self.tk_kr

    def session_anchor_str(self, market: str) -> str:
        return self.timekeeper_for(market).session_anchor

    def rolling_cutoff_str(self, market: str) -> str:
        return self.timekeeper_for(market).rolling_cutoff

    def lag_business_days(self, market: str) -> int:
        tk = self.timekeeper_for(market)
        return business_lag_days(
            tk.db_watermark_exit, tk.session_anchor, market=tk.market
        )

    def is_row_active(self, market: str, status: object, exit_cal: str) -> bool:
        """OPEN 또는 [rolling_cutoff, session_anchor] 청산."""
        st = str(status or "").strip().upper()
        if st == "OPEN":
            return True
        ex = str(exit_cal or "").strip()[:10]
        if not ex or len(ex) < 10:
            return False
        tk = self.timekeeper_for(market)
        return tk.rolling_cutoff <= ex <= tk.session_anchor

    def global_timekeeper_header_html(self) -> str:
        wm_kr = self.tk_kr.db_watermark_exit or "—"
        wm_us = self.tk_us.db_watermark_exit or "—"
        lag_kr = self.lag_business_days("KR")
        lag_us = self.lag_business_days("US")
        return (
            f"📎 리포트일 KST <b>{html.escape(self.tk_kr.calendar_today_kst)}</b> · "
            f"KR앵커 <b>{html.escape(self.tk_kr.session_anchor)}</b> · "
            f"US앵커(ET) <b>{html.escape(self.tk_us.session_anchor)}</b> · "
            f"DB워터마크 KR <b>{html.escape(str(wm_kr))}</b> · "
            f"US <b>{html.escape(str(wm_us))}</b> · "
            f"lag KR <b>{lag_kr}</b> · US <b>{lag_us}</b> · "
            f"읽기 <b>{html.escape(self.read_source_label)}</b>\n"
        )

    def staleness_banner_html(self, market: str, *, live_row_count: int) -> str:
        tk = self.timekeeper_for(market)
        verdict = evaluate_staleness(tk, live_row_count=live_row_count)
        if verdict.grade == "GREEN":
            return ""
        return verdict.banner_html


def format_practitioner_fail_card(
    *,
    market: str,
    group_key: str,
    sample_sig: str,
    error: BaseException,
    ctx: Optional[PractitionerReportContext] = None,
) -> str:
    """Per-group Fail-safe — 다른 스캐너 송출은 계속."""
    mk = str(market).upper()
    icon = "🇰🇷" if mk == "KR" else "🇺🇸"
    err_s = html.escape(str(error)[:240], quote=False)
    grp = html.escape(str(group_key), quote=False)
    sig = html.escape(str(sample_sig)[:80], quote=False)
    anchor = ""
    if ctx is not None:
        anchor = (
            f" · 앵커 <b>{html.escape(ctx.session_anchor_str(mk))}</b>"
        )
    return (
        f"{icon} <b>⚠️ [실무자 PIL · 저 죽었습니다]</b> "
        f"<code>{mk}</code> / {grp}{anchor}\n"
        f"<i>{sig}</i>\n"
        f"❌ {err_s}\n"
    )
