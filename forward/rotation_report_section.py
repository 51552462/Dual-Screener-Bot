"""
[7/9] 섹터 순환매 궤적 및 스필오버 — DailyReportContext · Junk Hard Block · Tier R Fallback.
"""
from __future__ import annotations

import html
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from reports.daily_report_context import DailyReportContext, DailyReportMarketSlice
from rotation_sector_filter import (
    dominant_sector_for_series,
    filter_eligible_daily_series,
    is_rotation_eligible_sector,
)


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def _min_rotation_days(sys_config: Optional[dict]) -> int:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        return max(1, int(cfg.get("ROTATION_MIN_DOMINANT_DAYS", 3)))
    except (TypeError, ValueError):
        return 3


def _tier_r_a(ctx: DailyReportContext, market: str, *, n_real: int, n_open: int) -> str:
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    wm = tk.db_watermark_exit or "—"
    lag = ctx.lag_for(mk)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>⚠️ [7/9] {mk} 순환매 — 롤링 윈도우 (<b>{wl}</b>) 내 진입 표본 <b>0</b>건.</i>\n"
        f"표본 실거래 <b>{n_real}</b> · 유효OPEN <b>{n_open}</b> · "
        f"DB청산 워터마크 <b>{_esc(wm)}</b> · lag <b>{lag}</b>일 · 롤링 <b>{ctx.window_days}</b>일.\n"
        f"<i>→ 앵커 구간에 entry_date가 있는 거래가 없어 순환매 궤적을 산출할 수 없습니다.</i>\n"
    )


def _tier_r_b(ctx: DailyReportContext, market: str, *, n_entries: int) -> str:
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 진입 <b>{n_entries}</b>건 · 윈도우 <b>{wl}</b> — "
        f"유효 표준 섹터 0건 (기타/혼합·US/EQUITY·폴백 라벨 제외).</i>\n"
        f"<i>sector_normalize·스캐너 섹터 태깅 품질을 점검하세요.</i>\n"
    )


def _tier_r_c(
    ctx: DailyReportContext,
    market: str,
    *,
    n_eligible_days: int,
    min_days: int,
) -> str:
    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 유효 주도 일수 <b>{n_eligible_days}</b>일 "
        f"(최소 <b>{min_days}</b>일 권장) · 윈도우 <b>{wl}</b> — 누적 중.</i>\n"
    )


def _build_inline_rotation_body(
    rot_df: pd.DataFrame,
    market: str,
    sys_config: dict,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """인라인 groupby — 유효 섹터만. (body_html, stats)."""
    mk = str(market).upper()
    stats: Dict[str, Any] = {"n_entries": len(rot_df), "n_eligible_days": 0}

    if rot_df.empty or "sector" not in rot_df.columns:
        return None, stats

    work = rot_df.copy()
    if "entry_date" in work.columns:
        work["entry_date"] = work["entry_date"].astype(str).str[:10]

    def _dominant(group: pd.Series) -> Optional[str]:
        return dominant_sector_for_series(group, market=mk)

    daily_dom = (
        work.groupby("entry_date")["sector"]
        .agg(_dominant)
        .dropna()
    )
    stats["n_eligible_days"] = int(len(daily_dom))

    if daily_dom.empty:
        return None, stats

    streaks: Dict[str, List[int]] = {}
    transitions: Dict[str, int] = {}
    current_sec: Optional[str] = None
    current_streak = 0

    for _date, sec in daily_dom.items():
        if not is_rotation_eligible_sector(sec, market=mk):
            continue
        if sec == current_sec:
            current_streak += 1
        else:
            if current_sec is not None:
                streaks.setdefault(current_sec, []).append(current_streak)
                t_key = f"{current_sec[:15]}➔{str(sec)[:15]}"
                transitions[t_key] = transitions.get(t_key, 0) + 1
            current_sec = sec
            current_streak = 1
    if current_sec is not None:
        streaks.setdefault(current_sec, []).append(current_streak)

    if current_sec is None:
        return None, stats

    lines: List[str] = []
    lines.append(
        f"🔥 <b>현재 주도 섹터:</b> {_esc(current_sec)} ({current_streak}일째 체류 중)\n"
    )
    try:
        from sector_spillover_refresh import resolve_predicted_sector_display

        pred = resolve_predicted_sector_display(sys_config, mk)
    except Exception:
        pred = str(sys_config.get(f"PREDICTED_NEXT_SECTOR_{mk}") or "데이터 없음")
    lines.append(f"🔮 <b>다음 예측 섹터:</b> {_esc(pred)}\n")
    adv = "🔥활성화(200%)" if sys_config.get("ROTATION_ADVANTAGE_ACTIVE") else "정상(100%)"
    lines.append(f"⚡ <b>베팅 어드밴티지:</b> {adv}\n\n")

    if streaks:
        lines.append("▪️ <b>섹터별 자금 체류 시간(수명):</b>\n")
        for s, lengths in streaks.items():
            if not is_rotation_eligible_sector(s, market=mk):
                continue
            lines.append(
                f" - {_esc(s[:15])}: 평균 {sum(lengths) / len(lengths):.1f}일\n"
            )

    sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:2]
    if sorted_trans:
        lines.append("\n▪️ <b>빈번한 자금 이동 궤적:</b>\n")
        for p, c in sorted_trans:
            lines.append(f" - {_esc(p)} ({c}회 관측)\n")

    return "".join(lines), stats


def build_rotation_spillover_section(
    ctx: DailyReportContext,
    market: str,
    mkt_slice: DailyReportMarketSlice,
    *,
    sys_config: dict,
    market_icon: str,
) -> str:
    """[7/9] 전문 — ctx 윈도우 SSOT · DB·인라인 동일 잣대."""
    from sector_rotation_store import (
        format_rotation_telegram_block,
        ingest_sector_daily_leaders,
        _load_daily_series,
    )

    mk = str(market).upper()
    tk = ctx.timekeeper_for(mk)
    cutoff = tk.rolling_cutoff
    anchor = tk.session_anchor
    min_days = _min_rotation_days(sys_config)

    n_real = int(len(mkt_slice.df_real))
    n_open = int(mkt_slice.n_open_valid)

    msg = f"{market_icon} <b>[7/9] 섹터 순환매 궤적 및 스필오버</b>\n"
    msg += ctx.market_window_header_html(
        mk, n_real=n_real, n_closed=mkt_slice.n_closed_window, n_open=n_open
    )

    rot_df = mkt_slice.df_real.copy()
    if "entry_date" in rot_df.columns:
        rot_df = rot_df[
            rot_df["entry_date"].astype(str).str[:10] >= cutoff
        ].copy()
        rot_df = rot_df[rot_df["entry_date"].astype(str).str[:10] <= anchor]

    if rot_df.empty:
        msg += _tier_r_a(ctx, mk, n_real=n_real, n_open=n_open)
    else:
        try:
            ingest_sector_daily_leaders(
                mk,
                rolling_cutoff=cutoff,
                session_anchor=anchor,
                db_path=ctx.db_read_path,
            )
        except Exception as _ing_ex:
            print(f"⚠️ [7/9] rotation ingest 스킵: {_ing_ex}")

        series = _load_daily_series(
            mk,
            rolling_cutoff=cutoff,
            session_anchor=anchor,
            db_path=ctx.db_read_path,
        )
        series = filter_eligible_daily_series(series, market=mk)
        body_db = ""
        if len(series) >= min_days:
            try:
                body_db = format_rotation_telegram_block(
                    mk,
                    sys_config,
                    rolling_cutoff=cutoff,
                    session_anchor=anchor,
                    db_path=ctx.db_read_path,
                    prefiltered_series=series,
                )
            except Exception as _db_fmt:
                print(f"⚠️ [7/9] rotation DB format 스킵: {_db_fmt}")

        if body_db:
            msg += body_db
        else:
            inline_body, stats = _build_inline_rotation_body(rot_df, mk, sys_config)
            n_elig = int(stats.get("n_eligible_days", 0))
            if inline_body:
                if n_elig < min_days:
                    msg += _tier_r_c(ctx, mk, n_eligible_days=n_elig, min_days=min_days)
                msg += inline_body
            elif len(rot_df) > 0:
                msg += _tier_r_b(ctx, mk, n_entries=len(rot_df))
            else:
                msg += _tier_r_a(ctx, mk, n_real=n_real, n_open=n_open)

    if mk == "KR":
        try:
            from cross_market_ssot import format_kr_spillover_telegram_line

            msg += format_kr_spillover_telegram_line(sys_config)
        except Exception as _cm_ex:
            try:
                from sector_spillover_refresh import resolve_us_spillover_display

                actual_spillover = resolve_us_spillover_display(sys_config)
            except Exception:
                actual_spillover = "—"
            msg += (
                f"\n🌐 <b>한미 스필오버 연동:</b> 🇺🇸 [{_esc(actual_spillover)}] "
                f"➔ 🇰🇷 선취매 (fallback · {_esc(str(_cm_ex)[:48])})\n"
            )

    return msg
