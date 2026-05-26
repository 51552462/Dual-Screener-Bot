"""
3단계 Staleness Gate — GREEN / YELLOW / RED + Fail-safe 텔레그램 카드.
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Literal, Optional

from report_timekeeper import ReportTimekeeper, business_lag_days

StalenessGrade = Literal["GREEN", "YELLOW", "RED"]


@dataclass(frozen=True)
class StalenessVerdict:
    grade: StalenessGrade
    lag_business_days: int
    live_row_count: int
    reason: str
    banner_html: str
    fail_safe_html: str
    allow_tier_champion: bool
    allow_micro_dna: bool


def evaluate_staleness(
    tk: ReportTimekeeper,
    *,
    live_row_count: int,
) -> StalenessVerdict:
    lag = business_lag_days(tk.db_watermark_exit, tk.session_anchor, market=tk.market)
    reasons: list[str] = []

    if lag >= 2:
        grade: StalenessGrade = "RED"
        reasons.append(f"DB청산 워터마크 {tk.db_watermark_exit or '—'} · 앵커 {tk.session_anchor} · 영업일 지연 {lag}일")
    elif lag == 1 or live_row_count <= 0:
        grade = "YELLOW"
        if lag == 1:
            reasons.append(f"최신 청산이 앵커 직전 영업일 수준 (lag {lag}일)")
        if live_row_count <= 0:
            reasons.append("당일 실전(LIVE) 청산 0건")
    else:
        grade = "GREEN"

    reason = " · ".join(reasons) if reasons else "정상"

    if grade == "RED":
        banner = (
            f"⛔ <b>[{html.escape(tk.market, quote=False)} · 데이터 정체 RED]</b> "
            f"{html.escape(reason, quote=False)}"
        )
        fail = _fail_safe_card(tk, grade, lag, live_row_count, reason)
        return StalenessVerdict(
            grade=grade,
            lag_business_days=lag,
            live_row_count=live_row_count,
            reason=reason,
            banner_html=banner,
            fail_safe_html=fail,
            allow_tier_champion=False,
            allow_micro_dna=True,
        )

    if grade == "YELLOW":
        banner = (
            f"⚠️ <b>[{html.escape(tk.market, quote=False)} · 갱신 지연 YELLOW]</b> "
            f"{html.escape(reason, quote=False)}"
        )
        return StalenessVerdict(
            grade=grade,
            lag_business_days=lag,
            live_row_count=live_row_count,
            reason=reason,
            banner_html=banner,
            fail_safe_html="",
            allow_tier_champion=True,
            allow_micro_dna=True,
        )

    return StalenessVerdict(
        grade=grade,
        lag_business_days=lag,
        live_row_count=live_row_count,
        reason=reason,
        banner_html="",
        fail_safe_html="",
        allow_tier_champion=True,
        allow_micro_dna=True,
    )


def _fail_safe_card(
    tk: ReportTimekeeper,
    grade: StalenessGrade,
    lag: int,
    live_n: int,
    reason: str,
) -> str:
    return (
        f"⛔ <b>[{html.escape(tk.market, quote=False)} · Fail-safe · {grade}]</b>\n"
        f"· {tk.header_watermark_line()}\n"
        f"· 영업일 지연: <b>{lag}</b> · LIVE 청산: <b>{live_n}</b>건\n"
        f"· 사유: {html.escape(reason, quote=False)}\n"
        f"· <b>최우수 성적표 요약 생략</b> — track_daily_positions · 메인 DB WAL · "
        f"dante-snapshot 백업 주기를 확인하십시오.\n\n"
    )


def persist_staleness_to_config(
    tk: ReportTimekeeper,
    verdict: StalenessVerdict,
    *,
    save_fn,
    load_fn,
) -> None:
    """P2: system_config + ops_events에 staleness 스냅샷 기록."""
    payload = {
        "grade": verdict.grade,
        "lag_business_days": verdict.lag_business_days,
        "session_anchor": tk.session_anchor,
        "db_watermark": tk.db_watermark_exit,
        "calendar_today_kst": tk.calendar_today_kst,
        "live_row_count": verdict.live_row_count,
        "reason": verdict.reason,
        "read_source": tk.read_source,
    }
    try:
        cfg = load_fn()
        if not isinstance(cfg, dict):
            cfg = {}
        cfg[f"LAST_REPORT_STALENESS_{tk.market}"] = payload
        save_fn(cfg)
    except Exception:
        pass
    try:
        import ops_logger

        ops_logger.insert_ops_event(
            component="report_staleness_gate",
            severity="WARN" if verdict.grade != "GREEN" else "INFO",
            event="report.staleness",
            payload=payload,
        )
    except Exception:
        pass
