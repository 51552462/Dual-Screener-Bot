"""
주간 Flow 마스터 리포트 — WeeklyFlowSnapshot SSOT + ReportStateBinder 연동.
"""
from __future__ import annotations

import html
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import pytz

from report_state_binder import (
    LifecycleReportBlock,
    MacroTreasuryReportBlock,
    build_lifecycle_report_block,
    build_macro_treasury_block,
)
from weekly_flow_pnl import (
    add_realized_pnl_column,
    zero_invest_fallback_for_market,
)
from weekly_flow_rollup import (
    WeeklyDnaRollup,
    WeeklyFlowTagRollup,
    build_weekly_dna_rollup,
    build_weekly_flow_tag_rollup,
    format_weekly_dna_rollup_html,
    format_weekly_flow_tag_rollup_html,
)
from weekly_action_plan import (
    WeeklyActionPlan,
    build_weekly_action_plan,
    persist_weekly_baseline,
)


@dataclass(frozen=True)
class WeeklyDayPnlRow:
    exit_date: str
    pnl: float
    wins: int
    total: int


@dataclass(frozen=True)
class WeeklySectorLeader:
    sector: str
    pnl: float
    n_trades: int
    win_rate_pct: float


@dataclass(frozen=True)
class WeeklyMvpEngine:
    sig_core: str
    pnl: float
    n_trades: int
    top_ticker: str
    top_name: str
    top_contrib: float


@dataclass(frozen=True)
class WeeklyFlowSnapshot:
    market: str
    week_start: str
    week_end: str
    daily_timeline: Tuple[WeeklyDayPnlRow, ...]
    week_pnl: float
    week_wr_pct: float
    week_n_closed: int
    top_sectors: Tuple[WeeklySectorLeader, ...]
    sector_rotation_path: str
    mvp_engines: Tuple[WeeklyMvpEngine, ...]


@dataclass(frozen=True)
class WeeklyFlowMasterSnapshot:
    """
    주간 Flow 마스터 SSOT — 시장 스냅샷(KR/US) + 롤업(DNA·태그) + 관제탑 + Action Plan.
    """

    week_start: str
    week_end: str
    as_of_kst: str
    kr: Optional[WeeklyFlowSnapshot]
    us: Optional[WeeklyFlowSnapshot]
    macro_kr: MacroTreasuryReportBlock
    macro_us: MacroTreasuryReportBlock
    lifecycle: LifecycleReportBlock
    sys_config: Dict[str, Any]
    dna_kr: Optional[WeeklyDnaRollup] = None
    dna_us: Optional[WeeklyDnaRollup] = None
    tags_kr: Optional[WeeklyFlowTagRollup] = None
    tags_us: Optional[WeeklyFlowTagRollup] = None
    action_plan: Optional[WeeklyActionPlan] = None


# 하위 호환 별칭
WeeklyFlowReportBundle = WeeklyFlowMasterSnapshot


def _sig_core(sig: Any) -> str:
    s = str(sig or "").strip()
    clean = re.sub(r"\[.*?\]", "", s).strip()
    return clean if clean else s.replace("[", "").replace("]", "").strip() or "UNKNOWN"


def _load_week_closed_df(
    conn: sqlite3.Connection,
    market: str,
    week_start: str,
) -> pd.DataFrame:
    df = pd.read_sql(
        """
        SELECT * FROM forward_trades
        WHERE market=? AND status LIKE 'CLOSED%%' AND exit_date >= ?
          AND IFNULL(sig_type, '') NOT LIKE '%%INCUBATOR%%'
          AND final_ret IS NOT NULL
        ORDER BY exit_date ASC
        """,
        conn,
        params=(market, week_start),
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
    return df.dropna(subset=["final_ret"])


def build_weekly_market_snapshot(
    df: pd.DataFrame,
    *,
    market: str,
    week_start: str,
    week_end: str,
) -> Optional[WeeklyFlowSnapshot]:
    if df is None or df.empty:
        return None

    fb = zero_invest_fallback_for_market(market)
    wdf = add_realized_pnl_column(df, market=market, zero_fallback=fb)
    pnl_col = "_realized_pnl"

    daily_rows: List[WeeklyDayPnlRow] = []
    for exit_date, g in wdf.groupby("exit_date", sort=True):
        d_pnl = float(g[pnl_col].sum())
        wins = int((g["final_ret"] > 0).sum())
        total = int(len(g))
        daily_rows.append(
            WeeklyDayPnlRow(
                exit_date=str(exit_date),
                pnl=d_pnl,
                wins=wins,
                total=total,
            )
        )

    week_pnl = float(wdf[pnl_col].sum())
    week_n = int(len(wdf))
    week_wins = int((wdf["final_ret"] > 0).sum())
    week_wr = (week_wins / week_n * 100.0) if week_n else 0.0

    sector_leaders: List[WeeklySectorLeader] = []
    if "sector" in wdf.columns:
        sec = wdf["sector"].astype(str).str.strip()
        wdf_sec = wdf.loc[sec.ne("") & sec.ne("nan") & sec.notna()].copy()
        if not wdf_sec.empty:
            agg = wdf_sec.groupby("sector", as_index=False).agg(
                pnl=(pnl_col, "sum"),
                n=("final_ret", "count"),
                wins=("final_ret", lambda s: int((s > 0).sum())),
            )
            agg = agg.sort_values("pnl", ascending=False).head(2)
            for _, r in agg.iterrows():
                n = int(r["n"])
                w = int(r["wins"])
                sector_leaders.append(
                    WeeklySectorLeader(
                        sector=str(r["sector"]),
                        pnl=float(r["pnl"]),
                        n_trades=n,
                        win_rate_pct=(w / n * 100.0) if n else 0.0,
                    )
                )

    rotation_path = ""
    if "entry_date" in df.columns and "sector" in df.columns:
        rot = df[["entry_date", "sector"]].copy()
        rot["sector"] = rot["sector"].astype(str).str.strip()
        rot = rot.loc[rot["sector"].ne("") & rot["sector"].ne("nan")]
        if not rot.empty:
            daily_dom = rot.groupby("entry_date")["sector"].agg(
                lambda x: x.mode().iloc[0] if not x.mode().empty else None
            )
            parts = [
                f"{str(s)[:8]}({str(d)[5:]})"
                for d, s in daily_dom.dropna().items()
            ]
            rotation_path = " ➔ ".join(parts[:10])

    mvp_list: List[WeeklyMvpEngine] = []
    wdf = wdf.copy()
    wdf["_sig_core"] = wdf["sig_type"].apply(_sig_core)
    sig_agg = (
        wdf.groupby("_sig_core", as_index=False)
        .agg(pnl=(pnl_col, "sum"), n=("final_ret", "count"))
        .sort_values("pnl", ascending=False)
        .head(3)
    )
    for _, row in sig_agg.iterrows():
        sig = str(row["_sig_core"])
        g_sig = wdf[wdf["_sig_core"] == sig]
        stock_agg = (
            g_sig.groupby(["code", "name"], as_index=False)[pnl_col]
            .sum()
            .sort_values(pnl_col, ascending=False)
        )
        if stock_agg.empty:
            top_ticker, top_name, top_c = "—", "—", 0.0
        else:
            top = stock_agg.iloc[0]
            top_ticker = str(top["code"])
            top_name = str(top["name"])
            top_c = float(top[pnl_col])
        mvp_list.append(
            WeeklyMvpEngine(
                sig_core=sig,
                pnl=float(row["pnl"]),
                n_trades=int(row["n"]),
                top_ticker=top_ticker,
                top_name=top_name,
                top_contrib=top_c,
            )
        )

    return WeeklyFlowSnapshot(
        market=market,
        week_start=week_start,
        week_end=week_end,
        daily_timeline=tuple(daily_rows),
        week_pnl=week_pnl,
        week_wr_pct=week_wr,
        week_n_closed=week_n,
        top_sectors=tuple(sector_leaders),
        sector_rotation_path=rotation_path,
        mvp_engines=tuple(mvp_list),
    )


def build_weekly_flow_snapshot(
    *,
    db_path: str,
    sys_config: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    week_days: int = 7,
) -> WeeklyFlowMasterSnapshot:
    """주간 마스터 SSOT 단일 빌드 — PnL · MVP · 롤업 · Action Plan."""
    from meta_governor_consumer import load_meta_state_resolved

    cfg = sys_config if isinstance(sys_config, dict) else {}
    tz_kr = pytz.timezone("Asia/Seoul")
    now = datetime.now(tz_kr)
    week_end = now.strftime("%Y-%m-%d")
    week_start = (now - timedelta(days=week_days)).strftime("%Y-%m-%d")
    as_of = now.strftime("%Y-%m-%d %H:%M")

    try:
        meta_st = meta if isinstance(meta, dict) else load_meta_state_resolved()
    except Exception:
        meta_st = {}

    df_kr = pd.DataFrame()
    df_us = pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path, timeout=60)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            df_kr = _load_week_closed_df(conn, "KR", week_start)
            df_us = _load_week_closed_df(conn, "US", week_start)
        finally:
            conn.close()
    except Exception:
        pass

    fb_kr = zero_invest_fallback_for_market("KR")
    try:
        macro_kr = build_macro_treasury_block(
            meta=meta_st,
            sys_config=cfg,
            df_closed_real=df_kr if not df_kr.empty else None,
            treasury_config_key="CENTRAL_TREASURY_KR",
            ledger_zero_invest_fallback=fb_kr,
        )
    except Exception:
        macro_kr = build_macro_treasury_block(
            meta={},
            sys_config=cfg,
            df_closed_real=None,
            treasury_config_key="CENTRAL_TREASURY_KR",
            ledger_zero_invest_fallback=fb_kr,
        )
    try:
        macro_us = build_macro_treasury_block(
            meta=meta_st,
            sys_config=cfg,
            df_closed_real=df_us if not df_us.empty else None,
            treasury_config_key="CENTRAL_TREASURY_US",
            ledger_zero_invest_fallback=zero_invest_fallback_for_market("US"),
        )
    except Exception:
        macro_us = macro_kr

    try:
        lifecycle = build_lifecycle_report_block(
            meta=meta_st,
            sys_config=cfg,
            now=now,
        )
    except Exception:
        lifecycle = build_lifecycle_report_block(meta={}, sys_config=cfg, now=now)

    try:
        kr_snap = build_weekly_market_snapshot(
            df_kr, market="KR", week_start=week_start, week_end=week_end
        )
    except Exception:
        kr_snap = None
    try:
        us_snap = build_weekly_market_snapshot(
            df_us, market="US", week_start=week_start, week_end=week_end
        )
    except Exception:
        us_snap = None

    dna_kr = build_weekly_dna_rollup(df_kr, market="KR", sys_config=cfg, meta=meta_st)
    dna_us = build_weekly_dna_rollup(df_us, market="US", sys_config=cfg, meta=meta_st)
    tags_kr = build_weekly_flow_tag_rollup(
        df_kr, market="KR", sys_config=cfg, today_str=week_end
    )
    tags_us = build_weekly_flow_tag_rollup(
        df_us, market="US", sys_config=cfg, today_str=week_end
    )

    top_sec_kr = tuple(s.sector for s in kr_snap.top_sectors) if kr_snap else ()
    top_sec_us = tuple(s.sector for s in us_snap.top_sectors) if us_snap else ()

    try:
        action = build_weekly_action_plan(
            sys_config=cfg,
            macro_kr=macro_kr,
            lifecycle=lifecycle,
            top_sectors_kr=top_sec_kr,
            top_sectors_us=top_sec_us,
            dna_kr=dna_kr,
            dna_us=dna_us,
            tags_kr=tags_kr,
            tags_us=tags_us,
        )
    except Exception:
        action = WeeklyActionPlan(
            rule_html="\n🎯 <b>[다음 주 Action Plan · Rule]</b>\n <i>Action Plan 조립 실패 — 관제탑 블록만 참조</i>\n",
            llm_tail_html="",
            facts_plain="",
        )

    return WeeklyFlowMasterSnapshot(
        week_start=week_start,
        week_end=week_end,
        as_of_kst=as_of,
        kr=kr_snap,
        us=us_snap,
        macro_kr=macro_kr,
        macro_us=macro_us,
        lifecycle=lifecycle,
        sys_config=cfg,
        dna_kr=dna_kr,
        dna_us=dna_us,
        tags_kr=tags_kr,
        tags_us=tags_us,
        action_plan=action,
    )


def build_weekly_flow_report(
    *,
    db_path: str,
    sys_config: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    week_days: int = 7,
) -> WeeklyFlowMasterSnapshot:
    """하위 호환 — build_weekly_flow_snapshot 별칭."""
    return build_weekly_flow_snapshot(
        db_path=db_path,
        sys_config=sys_config,
        meta=meta,
        week_days=week_days,
    )


def _fmt_money(market: str, value: float) -> str:
    if market == "US":
        return f"${value:+,.0f}"
    return f"{value:+,.0f}원"


def _format_market_section(snap: WeeklyFlowSnapshot) -> str:
    icon = "🇰🇷" if snap.market == "KR" else "🇺🇸"
    out = f"\n{icon} <b>[{snap.market} 일주일 자금·섹터 흐름]</b>\n"
    out += "🗓️ <b>[일자별 실현 손익·승률]</b>\n"
    if snap.daily_timeline:
        for row in snap.daily_timeline:
            short = row.exit_date[5:] if len(row.exit_date) >= 5 else row.exit_date
            wr = (row.wins / row.total * 100.0) if row.total else 0.0
            ic = "🔴" if row.pnl < 0 else "🟢"
            out += (
                f" {ic} {short}: <b>{_fmt_money(snap.market, row.pnl)}</b> "
                f"(승률 {wr:.0f}% / {row.total}건)\n"
            )
        out += (
            f" 💰 <b>주간 누적:</b> {_fmt_money(snap.market, snap.week_pnl)} "
            f"(승률 {snap.week_wr_pct:.0f}% / {snap.week_n_closed}건)\n"
        )
    else:
        out += " ↳ 이번 주 청산 데이터가 없습니다.\n"

    out += "\n🏷️ <b>[주간 주도 섹터 Top 2]</b>\n"
    if snap.top_sectors:
        for i, sec in enumerate(snap.top_sectors):
            medal = "🥇" if i == 0 else "🥈"
            out += (
                f" {medal} {html.escape(sec.sector, quote=False)}: "
                f"<b>{_fmt_money(snap.market, sec.pnl)}</b> "
                f"({sec.n_trades}건 · WR {sec.win_rate_pct:.0f}%)\n"
            )
    else:
        out += " ↳ 섹터 표본 없음\n"
    if snap.sector_rotation_path:
        out += f" 🌊 <b>편입 궤적:</b> {html.escape(snap.sector_rotation_path, quote=False)}\n"

    out += "\n🏆 <b>[이번 주 MVP 시그널 엔진]</b>\n"
    if snap.mvp_engines:
        for i, m in enumerate(snap.mvp_engines):
            medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
            sig_esc = html.escape(m.sig_core[:40], quote=False)
            nm_esc = html.escape(m.top_name, quote=False)
            cd_esc = html.escape(m.top_ticker, quote=False)
            out += (
                f" {medal} [{sig_esc}]: <b>{_fmt_money(snap.market, m.pnl)}</b> ({m.n_trades}건)\n"
                f"    ↳ 견인: <b>{nm_esc}</b>({cd_esc}) "
                f"{_fmt_money(snap.market, m.top_contrib)}\n"
            )
    else:
        out += " ↳ MVP 데이터 없음\n"
    return out


def format_weekly_governor_footer_html(bundle: WeeklyFlowMasterSnapshot) -> str:
    """ReportStateBinder SSOT — 하드코딩 UNKNOWN/0일차/50% 제거."""
    cfg = bundle.sys_config
    rk = html.escape(bundle.macro_kr.regime_key, quote=False)
    cos_raw = cfg.get("DYNAMIC_SUPERNOVA_CUTOFF")
    ml_raw = cfg.get("DYNAMIC_ML_BOX_CUTOFF")
    try:
        cos_pct = f"{float(cos_raw) * 100:.0f}%" if cos_raw is not None else "—"
    except (TypeError, ValueError):
        cos_pct = "—"
    try:
        ml_pct = f"{float(ml_raw) * 100:.0f}%" if ml_raw is not None else "—"
    except (TypeError, ValueError):
        ml_pct = "—"

    out = "\n⚙️ <b>[주말 관제탑 · MetaGovernor SSOT]</b>\n"
    out += f" ▪️ <b>국면:</b> {rk}"
    if bundle.macro_kr.regime_confidence is not None:
        out += f" (신뢰 {bundle.macro_kr.regime_confidence:.2f})"
    out += "\n"
    out += (
        f" ▪️ <b>유효 켈리:</b> 베이스 {bundle.macro_kr.base_dynamic_kelly_risk * 100:.2f}% "
        f"× Meta {bundle.macro_kr.meta_global_kelly_mult:.3f} "
        f"→ <b>{bundle.macro_kr.effective_kelly_risk * 100:.2f}%</b>\n"
    )
    out += f" ▪️ <b>초신성 허들(설정):</b> 코사인 {cos_pct} | ML박스 {ml_pct}\n"
    out += (
        f" ▪️ <b>주간 청산 누적(가상·KR):</b> {bundle.macro_kr.ledger_realized_est:+,.0f}원 · "
        f"<b>US:</b> {bundle.macro_us.ledger_realized_est:+,.0f}원\n"
    )

    lc = bundle.lifecycle
    if lc.autopilot_age_days is not None:
        out += (
            f" ▪️ <b>오토파일럿 앵커:</b> {lc.autopilot_age_days}일차 "
            f"<i>({html.escape(lc.autopilot_age_source, quote=False)})</i>\n"
        )
    else:
        out += " ▪️ <b>오토파일럿 앵커:</b> — <i>(기준일 미해결)</i>\n"
    gov_at = html.escape(lc.governor_last_run_at or "—", quote=False)
    gov_st = html.escape(lc.governor_last_run_status, quote=False)
    out += f" ▪️ <b>MetaGovernor:</b> {gov_at} | {gov_st}\n"
    out += (
        f" ▪️ <b>레지스트리:</b> LIVE {lc.n_live} · COOLED {lc.n_cooled} · "
        f"CANDIDATE {lc.n_candidate}\n"
    )
    if bundle.macro_kr.regime_notes:
        out += (
            " 🗣️ "
            + html.escape(bundle.macro_kr.regime_notes[:200], quote=False)
            + "\n"
        )
    return out


def format_weekly_flow_report_html(bundle: WeeklyFlowMasterSnapshot) -> str:
    msg = (
        f"🗺️ <b>[V100.0 퀀트 팩토리 주간 흐름(Flow) 총결산]</b>\n"
        f"📅 기간: {bundle.week_start} ~ {bundle.week_end} "
        f"(KST {html.escape(bundle.as_of_kst, quote=False)})\n"
    )
    msg += "<i>※ INCUBATOR 제외 · PnL SSOT(coalesce kelly→invest→fallback)</i>\n"
    msg += "━━━━━━━━━━━━━━━━━━\n"
    if bundle.kr:
        msg += _format_market_section(bundle.kr)
    else:
        msg += "\n🇰🇷 <b>[KR]</b> ↳ 이번 주 청산 없음\n"
    if bundle.us:
        msg += _format_market_section(bundle.us)
    else:
        msg += "\n🇺🇸 <b>[US]</b> ↳ 이번 주 청산 없음\n"

    msg += "\n━━━━━━━━━━━━━━━━━━\n<b>📊 주간 시너지 롤업</b>"
    if bundle.dna_kr:
        msg += format_weekly_dna_rollup_html(bundle.dna_kr)
    if bundle.dna_us:
        msg += format_weekly_dna_rollup_html(bundle.dna_us)
    if bundle.tags_kr:
        msg += format_weekly_flow_tag_rollup_html(bundle.tags_kr)
    if bundle.tags_us:
        msg += format_weekly_flow_tag_rollup_html(bundle.tags_us)

    msg += format_weekly_governor_footer_html(bundle)
    if bundle.action_plan:
        msg += bundle.action_plan.rule_html
        msg += bundle.action_plan.llm_tail_html
    msg += "\n━━━━━━━━━━━━━━━━━━\n"
    msg += (
        "<i>시장의 돈의 흐름·MVP 견인·관제탑 SSOT를 한 장으로 증명하는 마스터 결과지입니다.</i>"
    )
    return msg


def format_weekly_flow_report(snapshot: WeeklyFlowMasterSnapshot) -> str:
    """Telegram HTML — 주간 마스터 리포트 최종 렌더."""
    return format_weekly_flow_report_html(snapshot)


def dry_run_weekly_flow_report(
    *,
    db_path: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[WeeklyFlowMasterSnapshot, str]:
    """
    텔레그램 미발송 검증 — (snapshot, html) 반환.
    sys_config 미지정 시 load_or_create_config 시도.
    """
    cfg: Dict[str, Any]
    if isinstance(sys_config, dict):
        cfg = sys_config
    else:
        try:
            from system_auto_pilot import load_or_create_config

            cfg = load_or_create_config()
        except Exception:
            cfg = {}
    snap = build_weekly_flow_snapshot(db_path=db_path, sys_config=cfg)
    html_out = format_weekly_flow_report(snap)
    return snap, html_out


def send_weekly_flow_master_report(
    *,
    db_path: str,
    sys_config: Dict[str, Any],
    send_fn: Callable[[str], Any],
) -> None:
    try:
        snap = build_weekly_flow_snapshot(db_path=db_path, sys_config=sys_config)
        kr_n = int(snap.kr.week_n_closed) if snap.kr else 0
        us_n = int(snap.us.week_n_closed) if snap.us else 0
        html_msg = format_weekly_flow_report(snap)
        print(
            f"[weekly_flow] 텔레그램 발송 시도 — html_len={len(html_msg)} "
            f"KR청산={kr_n} US청산={us_n} "
            f"(청산 0건이어도 리포트 본문은 생성·발송함, 스킵 없음)"
        )
        sent = send_fn(html_msg)
        if sent is False:
            print("[weekly_flow] 텔레그램 발송 실패 — send_fn이 False 반환 (.env·API 응답 확인)")
        persist_weekly_baseline(
            sys_config,
            macro_effective_kelly=float(snap.macro_kr.effective_kelly_risk),
        )
    except Exception as e:
        print(f"[weekly_flow] 리포트 빌드 예외: {e}")
        send_fn(f"⚠️ <b>주간 Flow 리포트 생성 실패</b>\n{html.escape(str(e), quote=False)}")


if __name__ == "__main__":
    import os
    import sys

    _db = os.path.join(
        os.path.expanduser("~"),
        "dante_bots",
        "Dual-Screener-Bot",
        "market_data.sqlite",
    )
    _snap, _html = dry_run_weekly_flow_report(db_path=_db)
    _out = os.path.join(os.path.dirname(__file__) or ".", "_weekly_dry_run.html")
    with open(_out, "w", encoding="utf-8") as _f:
        _f.write(_html)
    sys.stdout.buffer.write(
        (
            f"[dry-run] KR closed={_snap.kr.week_n_closed if _snap.kr else 0} "
            f"US closed={_snap.us.week_n_closed if _snap.us else 0} "
            f"html_len={len(_html)} baseline_first="
            f"{_snap.action_plan.is_first_baseline_week if _snap.action_plan else '?'} "
            f"written={_out}\n"
        ).encode("utf-8", errors="replace")
    )
