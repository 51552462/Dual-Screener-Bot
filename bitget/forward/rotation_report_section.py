"""
[7/9] 타임프레임 순환매 궤적 — BitgetReportContext (주식 rotation_report_section 코인 적응).

섹터 대신 timeframe(1D/4H/2H/1H)을 주도 축으로 사용. KR→US 스필오버 없음.
"""
from __future__ import annotations

import html
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from bitget.infra.market_keys import to_deathmatch_key as _market_key
from bitget.reports.bitget_report_context import BitgetReportContext, BitgetReportMarketSlice


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def _min_rotation_days(sys_config: Optional[dict]) -> int:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        return max(1, int(cfg.get("BITGET_ROTATION_MIN_DOMINANT_DAYS", cfg.get("ROTATION_MIN_DOMINANT_DAYS", 3))))
    except (TypeError, ValueError):
        return 3


def _eligible_tf(tf: Any) -> bool:
    s = str(tf or "").strip().upper()
    return s in ("1D", "4H", "2H", "1H", "15M", "30M")


def _dominant_tf(series: pd.Series) -> Optional[str]:
    if series is None or series.empty:
        return None
    counts: Dict[str, int] = {}
    for v in series:
        t = str(v or "").strip().upper()
        if not _eligible_tf(t):
            continue
        counts[t] = counts.get(t, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda x: (x[1], x[0]))[0]


def _tier_r_a(
    ctx: BitgetReportContext,
    market_type: str,
    *,
    n_real: int,
    n_open: int,
) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    wm = tk.db_watermark_exit or "—"
    lag = ctx.lag_for(market_type)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>⚠️ [7/9] {mk} TF 순환매 — 롤링 윈도우 (<b>{wl}</b>) 내 진입 표본 <b>0</b>건.</i>\n"
        f"표본 실거래 <b>{n_real}</b> · 유효OPEN <b>{n_open}</b> · "
        f"DB청산 워터마크 <b>{_esc(wm)}</b> · lag <b>{lag}</b>d · 롤링 <b>{ctx.window_days}</b>일.\n"
    )


def _tier_r_b(ctx: BitgetReportContext, market_type: str, *, n_entries: int) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 진입 <b>{n_entries}</b>건 · 윈도우 <b>{wl}</b> — "
        f"유효 timeframe(1D/4H/2H/1H) 태그 0건.</i>\n"
    )


def _tier_r_c(
    ctx: BitgetReportContext,
    market_type: str,
    *,
    n_eligible_days: int,
    min_days: int,
) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    wl = _esc(f"{tk.rolling_cutoff}~{tk.session_anchor}")
    return (
        f"<i>ℹ️ {mk} 유효 주도 일수 <b>{n_eligible_days}</b>일 "
        f"(최소 <b>{min_days}</b>일 권장) · 윈도우 <b>{wl}</b> — 누적 중.</i>\n"
    )


def _build_inline_rotation_body(
    rot_df: pd.DataFrame,
    market_type: str,
    sys_config: dict,
) -> Tuple[Optional[str], Dict[str, Any]]:
    mk = _market_key(market_type)
    stats: Dict[str, Any] = {"n_entries": len(rot_df), "n_eligible_days": 0}

    if rot_df.empty or "timeframe" not in rot_df.columns:
        return None, stats

    work = rot_df.copy()
    if "entry_date" in work.columns:
        work["entry_date"] = work["entry_date"].astype(str).str[:10]

    daily_dom = (
        work.groupby("entry_date")["timeframe"]
        .agg(_dominant_tf)
        .dropna()
    )
    stats["n_eligible_days"] = int(len(daily_dom))

    if daily_dom.empty:
        return None, stats

    streaks: Dict[str, List[int]] = {}
    transitions: Dict[str, int] = {}
    current_tf: Optional[str] = None
    current_streak = 0

    for _date, tf in daily_dom.items():
        if not _eligible_tf(tf):
            continue
        if tf == current_tf:
            current_streak += 1
        else:
            if current_tf is not None:
                streaks.setdefault(current_tf, []).append(current_streak)
                t_key = f"{current_tf}➔{str(tf)}"
                transitions[t_key] = transitions.get(t_key, 0) + 1
            current_tf = tf
            current_streak = 1
    if current_tf is not None:
        streaks.setdefault(current_tf, []).append(current_streak)

    if current_tf is None:
        return None, stats

    lines: List[str] = []
    lines.append(
        f"🔥 <b>현재 주도 TF:</b> {_esc(current_tf)} ({current_streak}일째 체류)\n"
    )
    pred_key = f"BITGET_PREDICTED_NEXT_TF_{mk}"
    pred = str(sys_config.get(pred_key) or sys_config.get(f"PREDICTED_NEXT_TF_{mk}") or "데이터 없음")
    lines.append(f"🔮 <b>다음 예측 TF:</b> {_esc(pred)}\n")
    adv = "🔥활성화(200%)" if sys_config.get("BITGET_ROTATION_ADVANTAGE_ACTIVE") else "정상(100%)"
    lines.append(f"⚡ <b>베팅 어드밴티지:</b> {adv}\n\n")

    if streaks:
        lines.append("▪️ <b>TF별 자금 체류 시간(수명):</b>\n")
        for tf, lengths in streaks.items():
            lines.append(f" - {_esc(tf)}: 평균 {sum(lengths) / len(lengths):.1f}일\n")

    sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:2]
    if sorted_trans:
        lines.append("\n▪️ <b>빈번한 TF 이동 궤적:</b>\n")
        for p, c in sorted_trans:
            lines.append(f" - {_esc(p)} ({c}회 관측)\n")

    return "".join(lines), stats


def build_rotation_spillover_section(
    ctx: BitgetReportContext,
    market_type: str,
    mkt_slice: BitgetReportMarketSlice,
    *,
    sys_config: dict,
    market_icon: str,
) -> str:
    mk = _market_key(market_type)
    tk = ctx.timekeeper_for(market_type)
    cutoff = tk.rolling_cutoff
    anchor = tk.session_anchor
    min_days = _min_rotation_days(sys_config)

    n_real = int(len(mkt_slice.df_real))
    n_open = int(mkt_slice.n_open_valid)

    msg = f"{market_icon} <b>[7/9] 타임프레임 순환매 궤적</b>\n"
    msg += ctx.market_window_header_html(
        market_type, n_real=n_real, n_closed=mkt_slice.n_closed_window, n_open=n_open
    )

    rot_df = mkt_slice.df_real.copy()
    if not rot_df.empty and "entry_date" in rot_df.columns:
        ent = rot_df["entry_date"].astype(str).str[:10]
        rot_df = rot_df.loc[(ent >= cutoff) & (ent <= anchor)].copy()

    if rot_df.empty:
        msg += _tier_r_a(ctx, market_type, n_real=n_real, n_open=n_open)
    else:
        inline_body, stats = _build_inline_rotation_body(rot_df, market_type, sys_config)
        n_elig = int(stats.get("n_eligible_days", 0))
        if inline_body:
            if n_elig < min_days:
                msg += _tier_r_c(ctx, market_type, n_eligible_days=n_elig, min_days=min_days)
            msg += inline_body
        elif len(rot_df) > 0:
            msg += _tier_r_b(ctx, market_type, n_entries=len(rot_df))
        else:
            msg += _tier_r_a(ctx, market_type, n_real=n_real, n_open=n_open)

    return msg
