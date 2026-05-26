"""Forward equity shared — DB, telegram, config, helpers."""
# auto_forward_tester.py
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
import os, time, requests
import random
import re
import threading
from datetime import datetime, timedelta
import pytz
import sqlite3
import json
from dotenv import load_dotenv

load_dotenv()
import telegram_env

TELEGRAM_TOKEN_MAIN = telegram_env.get_report_token()
TELEGRAM_CHAT_ID = telegram_env.get_report_chat_id()

from market_db_paths import MARKET_DATA_DB_PATH, market_db_read_path
from meta_governor_consumer import (
    apply_meta_kelly_merge,
    effective_max_position_pct,
    load_meta_state_resolved,
)
from toxic_antipattern_core import collect_merged_antipattern_rules
from report_feature_analyzer import (
    ReportFeatureAnalyzer,
    colosseum_db_path_for_report,
    colosseum_window_days,
    discover_numeric_feature_columns,
    extra_forward_trade_columns_for_report,
)
from forward_flow_tag_deep_dive import build_flow_tag_snapshot, format_flow_tag_report_html
from forward_report_scalar import (
    col_series,
    prepare_forward_trades_df,
    safe_float_cast,
    scalar_float,
    series_mean,
)
from forward_dual_track_queries import (
    load_dual_track_frames,
    query_latest_closed_trade_date,
)
from forward_score_bucket_deep_dive import (
    ForwardScoreBucketDeepDive,
    build_dual_track_bucket_blocks,
    build_universal_dna_block,
    format_dual_track_micro_dna_html,
    format_dual_track_tier_champion_summary_html,
    format_universal_dna_html,
)
from market_db_paths import report_db_read_path, report_read_source_label
from report_staleness_gate import evaluate_staleness, persist_staleness_to_config
from report_timekeeper import ReportTimekeeper
from html import escape as html_escape

from report_state_binder import (
    build_lifecycle_report_block,
    build_macro_treasury_block,
    format_lifecycle_section_html,
    format_macro_treasury_section_html,
)
from capital_deathmatch import CapitalDeathmatchAnalyzer, DeathmatchNarrativeBuilder

# 💡 [방향성 2번] 전문적인 DB 시스템 (CSV 폐기)
DB_PATH = MARKET_DATA_DB_PATH


def _open_market_db_ro():
    """리포트·딥다이브·듀얼트랙: 메인 DB 강제 (스냅샷 mtime 착시 방지)."""
    uri_path = report_db_read_path().replace("\\", "/")
    return sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, check_same_thread=False)


def _compress_sector_theme(raw: object) -> str:
    """긴 섹터/테마 문자열 → 퀀트 태그 (15자 캡, 서술어 제거)."""
    try:
        from ace_text_sanitize import sanitize_noun_phrase

        out = sanitize_noun_phrase(raw)
        return out or "미분류"
    except Exception:
        pass
    s = str(raw or "").strip()
    if not s or s.lower() in ("nan", "none", "미상", "null"):
        return "미분류"
    for junk in ("유망", "포착", "테마", "섹터"):
        s = s.replace(junk, "")
    for sep in (",", "·", "/", "|", " — ", " - "):
        if sep in s:
            s = s.split(sep)[0].strip()
    s = re.sub(r"\s+", "", s)
    if len(s) > 15:
        s = s[:14] + "…"
    return s or "미분류"


def _sector_convergence_summary(sectors: list) -> str:
    from collections import Counter

    clean = [_compress_sector_theme(x) for x in sectors if str(x).strip()]
    clean = [x for x in clean if x != "미분류"]
    if not clean:
        return "섹터 미기록"
    if len(clean) == 1:
        return f"[{clean[0]}]"
    cnt = Counter(clean)
    top, n1 = cnt.most_common(1)[0]
    share = n1 / len(clean)
    if share >= 0.55:
        return f"[{top}] ({share * 100:.0f}%)"
    parts = cnt.most_common(2)
    if len(parts) >= 2:
        t2, n2 = parts[1]
        return f"1위 [{top}] {n1}건 · 2위 [{t2}] {n2}건 · 수렴도 낮음(분산)"
    return "수렴도 낮음(분산)"


def _colosseum_kst_cutoff(window_days: int) -> str:
    kr_tz = pytz.timezone("Asia/Seoul")
    return (datetime.now(kr_tz).date() - timedelta(days=int(window_days))).strftime("%Y-%m-%d")


def _ace_analysis_frames(
    df: pd.DataFrame,
    league: str,
    logic: str,
    *,
    window_days: int,
    ctx=None,
) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """TOP3 종목이 아닌 로직 전체 수익 청산 코호트로 피처 스캔."""
    if ctx is not None:
        cutoff = ctx.rolling_cutoff_for_league(league)
        anchor_cap = ctx.anchor_for_league(league)
    else:
        cutoff = _colosseum_kst_cutoff(window_days)
        anchor_cap = ""
    q = df.loc[
        (df["league"] == league)
        & (df["logic"] == logic)
        & (df["exit_date"].astype(str) >= cutoff)
    ].copy()
    if anchor_cap:
        q = q.loc[q["exit_date"].astype(str).str[:10] <= anchor_cap].copy()
    if q.empty:
        return pd.DataFrame(), pd.DataFrame(), cutoff, ""
    ed = q["exit_date"].astype(str).str[:10]
    anchor = anchor_cap if anchor_cap else (str(ed.max()) if not ed.empty else "")
    winners = q.loc[pd.to_numeric(q["final_ret"], errors="coerce") > 0].sort_values(
        "final_ret", ascending=False
    )
    if len(winners) >= 5:
        ace = winners.head(min(40, len(winners)))
    else:
        ace = q.nlargest(min(8, len(q)), "final_ret")
    baseline = df.loc[
        (df["league"] == league)
        & (df["logic"] != logic)
        & (df["exit_date"].astype(str) >= cutoff)
    ].copy()
    if anchor_cap:
        baseline = baseline.loc[
            baseline["exit_date"].astype(str).str[:10] <= anchor_cap
        ].copy()
    if len(baseline) < 5:
        baseline = q.loc[~q.index.isin(ace.index)].copy()
    return ace, baseline, cutoff, anchor


def _colosseum_top3_carry_rows(sub_df: pd.DataFrame, esc) -> tuple[list, list]:
    """TOP3 표시 문자열 + 섹터 raw 목록."""
    items: list = []
    sectors_raw: list = []
    if sub_df is None or sub_df.empty:
        return items, sectors_raw
    for _, rr in sub_df.iterrows():
        cd = str(rr.get("code", "")).strip()
        nm = str(rr.get("name", "")).strip()
        label = cd if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", cd) else (
            nm if nm and nm.lower() != "nan" else cd
        )
        if len(str(label)) > 12:
            label = str(label)[:10] + "…"
        items.append(f"<b>{esc(label)}</b>({float(rr['final_ret']):+.1f}%)")
        raw_sec = str(rr.get("sector", "")).strip()
        if raw_sec and raw_sec.lower() != "nan":
            sectors_raw.append(raw_sec)
    return items, sectors_raw


def _strategy_colosseum_brief(db_path=None):
    """
    가상매매 `forward_trades` 청산 건을 로직명(sig_type 코어)별로 집계해 텔레그램용 랭킹 문자열 생성.
    스냅샷 고착 방지: colosseum_db_path_for_report() + exit_date 롤링 윈도우.
    """
    from colosseum_report_context import ColosseumReportContext

    if db_path is None:
        db_path = colosseum_db_path_for_report()
    try:
        sys_cfg = load_system_config()
    except Exception:
        sys_cfg = {}
    window_days = colosseum_window_days(sys_cfg)
    try:
        col_ctx = ColosseumReportContext.build(rolling_days=window_days)
    except Exception:
        col_ctx = None
    if col_ctx is not None:
        cutoff = min(
            col_ctx.rolling_cutoff_for_league("KR"),
            col_ctx.rolling_cutoff_for_league("US"),
        )
    else:
        cutoff = _colosseum_kst_cutoff(window_days)
    try:
        uri_path = db_path.replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, check_same_thread=False)
        try:
            cols = pd.read_sql("PRAGMA table_info(forward_trades)", conn)
            col_names = set(cols["name"].astype(str).tolist()) if cols is not None and not cols.empty else set()

            base_cols = ["sig_type", "final_ret", "code", "name"]
            opt_cols = []
            for c in ["market", "strategy_name", "exit_date", "sector", "dyn_cpv", "v_energy"]:
                if c in col_names:
                    opt_cols.append(c)
            for c in extra_forward_trade_columns_for_report():
                if c in col_names and c not in base_cols and c not in opt_cols:
                    opt_cols.append(c)
            sel = ", ".join(base_cols + opt_cols)
            df = pd.read_sql(
                f"SELECT {sel} FROM forward_trades "
                "WHERE status LIKE 'CLOSED%' AND final_ret IS NOT NULL "
                "AND IFNULL(exit_date,'') >= ?",
                conn,
                params=(cutoff,),
            )
        finally:
            conn.close()
    except Exception:
        return ""

    if df is None or df.empty:
        return (
            '\n⚔️ <b>[전략 콜로세움: 리그별 랭킹]</b>\n'
            "<i>청산 완료 데이터가 없습니다.</i>\n"
        )

    def _core_group(sig):
        clean_sig = re.sub(r"\[.*?\]", "", str(sig)).strip()
        return clean_sig if clean_sig else str(sig).replace("[", "").replace("]", "").strip()

    df = df.copy()
    df["_sig"] = df["sig_type"].astype(str)
    df = df.loc[~df["_sig"].str.contains("INCUBATOR", na=False)].copy()
    df["logic"] = df["sig_type"].apply(_core_group)
    df = df.loc[df["logic"].str.len() > 0].copy()
    df["code"] = df["code"].astype(str).str.strip() if "code" in df.columns else ""
    df["name"] = df["name"].astype(str).str.strip() if "name" in df.columns else ""
    if "strategy_name" not in df.columns:
        df["strategy_name"] = ""
    if "market" not in df.columns:
        df["market"] = ""
    df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
    df = df.dropna(subset=["final_ret"])
    if "exit_date" in df.columns:
        df["exit_date"] = df["exit_date"].astype(str).str[:10]
    else:
        df["exit_date"] = ""
    for _c in ("sector", "dyn_cpv", "v_energy"):
        if _c not in df.columns:
            df[_c] = np.nan
        elif _c != "sector":
            df[_c] = pd.to_numeric(df[_c], errors="coerce")
    for _c in extra_forward_trade_columns_for_report():
        if _c in df.columns:
            df[_c] = pd.to_numeric(df[_c], errors="coerce")

    def _league_of_row(r):
        sn = str(r.get("strategy_name", ""))
        cd = str(r.get("code", "")).strip()
        mk = str(r.get("market", "")).upper().strip()
        lg = str(r.get("logic", ""))

        if "us_" in sn.lower() or sn.upper().startswith("US"):
            return "US"
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9\-.]{0,14}", cd):
            return "US"
        if re.fullmatch(r"\d{4,8}", cd):
            return "KR"
        if mk in ("US", "KR"):
            return mk
        if "US " in lg.upper() or lg.upper().startswith("US"):
            return "US"
        return "KR"

    df["league"] = df.apply(_league_of_row, axis=1)

    rows = []
    for (league, logic), g in df.groupby(["league", "logic"]):
        fr = g["final_ret"].dropna()
        if fr.empty:
            continue
        n = int(len(fr))
        wins = int((fr > 0).sum())
        sum_ret = float(fr.sum())
        wr = (wins / n * 100.0) if n else 0.0
        disp = str(logic).strip()
        if len(disp) > 120:
            disp = disp[:117] + "..."
        rows.append(
            {"league": league, "logic": disp, "n": n, "wins": wins, "wr": wr, "sum_ret": sum_ret}
        )

    if not rows:
        return (
            '\n⚔️ <b>[전략 콜로세움: 리그별 랭킹]</b>\n'
            "<i>집계 가능한 청산 건이 없습니다.</i>\n"
        )

    def _esc(s):
        t = str(s)
        return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows_df = pd.DataFrame(rows)
    lines = ['\n⚔️ <b>[전략 콜로세움: 리그별 랭킹]</b>\n']
    if col_ctx is not None:
        lines.append(col_ctx.global_header_html())
        lines.append(f"<i>청산 표본 <b>{len(df)}</b>건 (듀얼트랙 TOP3 분리)</i>\n")
    else:
        ed_all = df["exit_date"].astype(str).str[:10]
        data_anchor = str(ed_all.max()) if not ed_all.empty else ""
        lines.append(
            f"<i>DB 실시간 · KST 앵커 <b>{_esc(data_anchor)}</b> · 롤링 <b>{window_days}</b>일 · "
            f"청산 <b>{len(df)}</b>건</i>\n"
        )
    medals = ["🥇", "🥈", "🥉"]

    def _league_top_block(league):
        part = rows_df.loc[rows_df["league"] == league].sort_values("sum_ret", ascending=False).head(3)
        if part.empty:
            return [], [f" <i>{'한국' if league == 'KR' else '미국'} 리그 데이터 없음</i>\n"]
        out = []
        top_rows_local = []
        for i, (_, r) in enumerate(part.iterrows()):
            m = medals[i] if i < len(medals) else "🏅"
            lg = _esc(r["logic"])
            out.append(
                f" {m} {lg}: 승률 {r['wr']:.1f}% / 수익 {r['sum_ret']:+.2f}% (거래 {int(r['n'])}건)\n"
            )
            top_rows_local.append({"logic": str(r["logic"]), "sum_ret": float(r["sum_ret"])})
        return top_rows_local, out

    kr_top, kr_lines = _league_top_block("KR")
    us_top, us_lines = _league_top_block("US")
    lines.append("🇰🇷 <b>[한국 실무자 랭킹 TOP 3]</b>\n")
    lines.extend(kr_lines)
    lines.append("\n🇺🇸 <b>[미국 실무자 랭킹 TOP 3]</b>\n")
    lines.extend(us_lines)

    kr_top_logic = kr_top[0]["logic"] if kr_top else ""
    us_top_logic = us_top[0]["logic"] if us_top else ""

    def _top3_display_and_ace(league, logic):
        if not logic:
            return [], [], "섹터 미기록", pd.DataFrame(), "", 0
        ace_df, base_df, _cut, anchor = _ace_analysis_frames(
            df, league, logic, window_days=window_days, ctx=col_ctx
        )
        q_all = df.loc[(df["league"] == league) & (df["logic"] == logic)].copy()
        anchor_day = (
            col_ctx.anchor_for_league(league) if col_ctx is not None else ""
        )
        roll_cut = (
            col_ctx.rolling_cutoff_for_league(league)
            if col_ctx is not None
            else _colosseum_kst_cutoff(window_days)
        )
        q_live = q_all.loc[q_all["exit_date"].astype(str).str[:10] == anchor_day].copy()
        q_live = q_live.sort_values("final_ret", ascending=False).head(3)
        q_roll = q_all.loc[
            (q_all["exit_date"].astype(str).str[:10] >= roll_cut)
            & (
                (q_all["exit_date"].astype(str).str[:10] <= anchor_day)
                if anchor_day
                else True
            )
        ].copy()
        q_roll = q_roll.sort_values("final_ret", ascending=False).head(3)
        live_items, live_secs = _colosseum_top3_carry_rows(q_live, _esc)
        champ_items, champ_secs = _colosseum_top3_carry_rows(q_roll, _esc)
        sectors_raw = live_secs + champ_secs
        if not sectors_raw and not ace_df.empty and "sector" in ace_df.columns:
            sectors_raw = ace_df["sector"].astype(str).tolist()
        sec_summary = _sector_convergence_summary(sectors_raw)
        n_feat = len(discover_numeric_feature_columns(ace_df, extra_forward_trade_columns_for_report()))
        return live_items, champ_items, sec_summary, ace_df, anchor, n_feat

    kr_live, kr_champ, kr_sec, kr_ace, kr_anchor, kr_nfeat = _top3_display_and_ace(
        "KR", kr_top_logic
    )
    us_live, us_champ, us_sec, us_ace, us_anchor, us_nfeat = _top3_display_and_ace(
        "US", us_top_logic
    )

    lines.append("\n🔍 <b>[에이스 로직 심층 부검]</b>\n")
    if col_ctx is not None:
        lines.append(col_ctx.global_header_html())

    def _ace_top3_block(league: str, logic: str, live_items, champ_items, sec: str) -> None:
        if not logic:
            return
        anchor_s = (
            col_ctx.anchor_for_league(league) if col_ctx is not None else ""
        )
        lines.append(f"📌 {league} <b>{_esc(logic)}</b>\n")
        lines.append(
            f" 🟢 당일 실전 TOP3 (앵커 <b>{_esc(anchor_s)}</b>): "
            + (", ".join(live_items) if live_items else "<i>없음</i>")
            + "\n"
        )
        lines.append(
            f" 🏛️ 롤링 챔피언 TOP3 (<b>{window_days}</b>일 MAX): "
            + (", ".join(champ_items) if champ_items else "<i>없음</i>")
            + f" · 섹터 {sec}\n"
        )

    if kr_top_logic:
        _ace_top3_block("KR", kr_top_logic, kr_live, kr_champ, kr_sec)
    if us_top_logic:
        _ace_top3_block("US", us_top_logic, us_live, us_champ, us_sec)

    dynamic_kr = False
    dynamic_us = False
    baseline_kr = pd.DataFrame()
    baseline_us = pd.DataFrame()
    kr_insights: list = []
    us_insights: list = []
    meta_st: dict = {}
    try:
        meta_st = load_meta_state_resolved()
        analyzer = ReportFeatureAnalyzer(sys_config=sys_cfg, meta=meta_st)
        if kr_top_logic and not kr_ace.empty and len(kr_ace) >= 3:
            _, baseline_kr, _, _ = _ace_analysis_frames(
                df, "KR", kr_top_logic, window_days=window_days, ctx=col_ctx
            )
            kr_insights = analyzer.collect_ace_feature_insights(kr_ace, baseline_kr)
            _kr_anchor = kr_anchor or (
                col_ctx.anchor_for_league("KR") if col_ctx is not None else ""
            )
            part_lines, ok_kr = analyzer.build_ace_deep_dive_lines(
                league="KR",
                logic_label=_esc(kr_top_logic),
                ace_df=kr_ace,
                baseline_df=baseline_kr,
                spillover_sector=kr_sec,
                data_anchor=_kr_anchor,
                window_days=window_days,
                n_features_scanned=kr_nfeat,
            )
            if ok_kr:
                for pl in part_lines:
                    lines.append(pl)
                dynamic_kr = True
        if us_top_logic and not us_ace.empty and len(us_ace) >= 3:
            _, baseline_us, _, _ = _ace_analysis_frames(
                df, "US", us_top_logic, window_days=window_days, ctx=col_ctx
            )
            us_insights = analyzer.collect_ace_feature_insights(us_ace, baseline_us)
            _us_anchor = us_anchor or (
                col_ctx.anchor_for_league("US") if col_ctx is not None else ""
            )
            part_lines, ok_us = analyzer.build_ace_deep_dive_lines(
                league="US",
                logic_label=_esc(us_top_logic),
                ace_df=us_ace,
                baseline_df=baseline_us,
                spillover_sector=us_sec,
                data_anchor=_us_anchor,
                window_days=window_days,
                n_features_scanned=us_nfeat,
            )
            if ok_us:
                for pl in part_lines:
                    lines.append(pl)
                dynamic_us = True
    except Exception as ex:
        dynamic_kr = False
        dynamic_us = False
        lines.append(f"<i>⚠️ 동적 필터 연산 스킵: {_esc(str(ex)[:120])}</i>\n")

    if (kr_live or kr_champ) and not dynamic_kr:
        lines.append(
            f"💡 KR 요약: 섹터 {kr_sec} · 동적 피처 스캔 표본 부족(청산·수익 거래 확대 후 재산출).\n"
        )
    if (us_live or us_champ) and not dynamic_us:
        lines.append(
            f"💡 US 요약: 섹터 {us_sec} · 동적 피처 스캔 표본 부족(청산·수익 거래 확대 후 재산출).\n"
        )

    try:
        from ace_evolution_refresh import refresh_ace_evolution_from_colosseum_context
        from ace_evolution_telegram import format_ace_dna_block

        playbooks = refresh_ace_evolution_from_colosseum_context(
            kr_logic=kr_top_logic or "",
            us_logic=us_top_logic or "",
            kr_ace=kr_ace,
            us_ace=us_ace,
            kr_baseline=baseline_kr,
            us_baseline=baseline_us,
            kr_insights=kr_insights,
            us_insights=us_insights,
            kr_sec=kr_sec,
            us_sec=us_sec,
            kr_anchor=kr_anchor
            or (col_ctx.anchor_for_league("KR") if col_ctx is not None else ""),
            us_anchor=us_anchor
            or (col_ctx.anchor_for_league("US") if col_ctx is not None else ""),
            window_days=window_days,
            sys_config=sys_cfg,
            meta=meta_st,
            df_closed_all=df,
        )
        if playbooks.get("KR"):
            lines.append(format_ace_dna_block(playbooks["KR"]))
        if playbooks.get("US"):
            lines.append(format_ace_dna_block(playbooks["US"]))
    except Exception as _ae_ex:
        lines.append(f"<i>⚠️ AceEvolution 갱신 스킵: {_esc(str(_ae_ex)[:100])}</i>\n")

    return "".join(lines)


def _shadow_performance_brief(sys_config):
    """SHADOW_PERFORMANCE → 텔레그램 [그림자 장부] 섹션."""
    try:
        def esc(s):
            t = str(s)
            return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        sp = sys_config.get("SHADOW_PERFORMANCE")
        if not isinstance(sp, dict):
            return ""
        blocked = sp.get("blocked") or {}
        by_reason = blocked.get("by_reason") or {}
        raw_counts = blocked.get("reason_event_counts") or {}
        lines = ['\n🛡️ <b>[그림자 장부: 위성 기여도 평가]</b>\n']
        updated = sp.get("updated_at")
        if updated:
            lines.append(f"<i>최종 산출: {esc(updated)}</i>\n")

        reason_lines = [
            ("TOXIC_ANTI_PATTERN", "📉 <b>오답노트 방어력</b>"),
            ("TOXIC_ML_TREE", "🧬 <b>ML 독성 트리 방어력</b>"),
            ("DOOMSDAY_DEFCON", "🚨 <b>둠스데이 방어력</b>"),
        ]
        for rk, title in reason_lines:
            br = by_reason.get(rk) or {}
            try:
                ntot = int(raw_counts.get(rk, 0))
            except (TypeError, ValueError):
                ntot = 0
            if ntot == 0 and not br:
                continue
            if rk == "DOOMSDAY_DEFCON":
                nskip = int(br.get("n_skipped_no_price", 0) or 0)
                lines.append(
                    f"{title}: <b>{ntot}</b>건 차단 기록"
                    f" (참고가 없음·거시 차단 {nskip}건)\n"
                )
                continue
            ne = int(br.get("n_evaluated_price", 0) or 0)
            se = float(br.get("sum_signed_defense_pct", 0.0) or 0.0)
            lines.append(
                f"{title}: <b>{ntot}</b>건 차단 / 가격평가 {ne}건 / 순방어지표 <b>{se:+.1f}%</b>\n"
            )

        sm = sp.get("smart_money_buff") or {}
        wt = sm.get("win_rate_tagged")
        wu = sm.get("win_rate_untagged")
        dlt = sm.get("delta_pct_pts")
        if wt is not None and wu is not None:
            dp = float(dlt) if dlt is not None else float(wt) - float(wu)
            lines.append(
                f"🔥 <b>스마트머니 버프</b>: 태그 종목 승률 <b>{float(wt):.0f}%</b> "
                f"(일반 <b>{float(wu):.0f}%</b>, 우위 <b>{dp:+.0f}%p</b>)\n"
            )
        else:
            lines.append(
                "🔥 <b>스마트머니 버프</b>: <i>표본 부족 (가상매매·청산 매칭 필요)</i>\n"
            )

        return "".join(lines)
    except Exception:
        return ""


def _shadow_reason_defense_is_opportunity_cost_loss(shadow_perf, reason_key):
    """
    SHADOW_PERFORMANCE.blocked.by_reason[reason_key].sum_signed_defense_pct 가
    존재하고 0 미만이면 True (방어막이 기회비용 손실 → 자율 해제 후보).
    데이터 없거나 조회 실패 시 False (안전 쪽: 차단 유지).
    """
    try:
        if not isinstance(shadow_perf, dict):
            return False
        blocked = shadow_perf.get("blocked")
        if not isinstance(blocked, dict):
            return False
        by_reason = blocked.get("by_reason")
        if not isinstance(by_reason, dict):
            return False
        br = by_reason.get(reason_key)
        if not isinstance(br, dict) or "sum_signed_defense_pct" not in br:
            return False
        return float(br.get("sum_signed_defense_pct")) < 0
    except (TypeError, ValueError):
        return False


def _telegram_plain_from_html(chunk: str) -> str:
    """HTML parse 실패 시 평문 — 의도한 굵게는 유지하지 않고 태그만 제거."""
    return re.sub(r"</?([a-zA-Z][a-zA-Z0-9]*)[^>]*>", "", chunk)


def send_telegram_msg(text, *, parse_mode: str = "HTML"):
    if not TELEGRAM_TOKEN_MAIN or not TELEGRAM_CHAT_ID:
        print("⚠️ [텔레그램] TELEGRAM_TOKEN_MAIN / TELEGRAM_CHAT_ID 미설정(.env) — 메시지 스킵")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN_MAIN}/sendMessage"
        max_len = 4000
        chunks = [text[i : i + max_len] for i in range(0, len(text), max_len)]
        use_html = str(parse_mode or "").upper() == "HTML"

        for chunk in chunks:
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk}
            if use_html:
                payload["parse_mode"] = "HTML"
            res = requests.post(url, json=payload, timeout=10)
            if res.status_code != 200:
                print(f"텔레그램 발송 실패: {res.text}")
            if use_html and res.status_code == 400:
                plain = _telegram_plain_from_html(chunk)
                res2 = requests.post(
                    url, json={"chat_id": TELEGRAM_CHAT_ID, "text": plain}, timeout=10
                )
                if res2.status_code != 200:
                    print(f"텔레그램 평문 재전송 실패: {res2.text}")
            import time

            time.sleep(0.5)
    except Exception:
        pass


def _format_forward_ledger_error_html(context: str, exc: BaseException) -> str:
    """예외 본문의 <class 'float'> 등이 HTML을 깨뜨리지 않도록 code 블록으로 감쌈."""
    return (
        f"🚨 <b>[포워드 장부 에러]</b> {html_escape(context, quote=False)}:\n"
        f"<code>{html_escape(str(exc), quote=False)}</code>"
    )

def init_forward_db():
    """장부 테이블 생성 및 V12.0 필수 컬럼 안전 추가"""
    # 💡 [V25.0] Timeout 60초 대기열 및 WAL 모드 전면 활성화
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, entry_date TEXT, market TEXT, code TEXT, name TEXT, sector TEXT,    
            sig_type TEXT, tier TEXT, total_score REAL, dyn_rs REAL, dyn_cpv REAL, dyn_tb REAL,
            is_tenbagger INTEGER, is_top_dna INTEGER, is_worst_dna INTEGER, is_death_combo INTEGER,
            entry_price REAL, v_cpv REAL, v_yang REAL, v_rs REAL, v_energy REAL, marcap_eok REAL,       
            score_marcap REAL, freq_count INTEGER, max_high REAL, min_low REAL, bars_held INTEGER DEFAULT 0,
            up_vol_sum REAL DEFAULT 0, down_vol_sum REAL DEFAULT 0, status TEXT DEFAULT 'OPEN',
            exit_date TEXT, exit_reason TEXT, flow_tags TEXT, final_ret REAL, mfe REAL
        )
    ''')


    # 👇👇 [추가] ABC 토너먼트 성적 기록을 위한 컬럼 👇👇
    for p in ['live_a', 'cand_b', 'champ_c']:
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_ret REAL DEFAULT 0.0")
        except: pass
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_status TEXT DEFAULT 'OPEN'")
        except: pass
    # 💡 [V12.0 팩트 추가] 기존 DB를 날리지 않고 안전하게 컬럼 추가
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_atr REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN exit_type TEXT DEFAULT 'UNKNOWN'")
    except: pass
    
    # 👇👇 [추가] V17.0 청산 우선순위 시뮬레이션 컬럼 👇👇
    for p in ['sim_stat', 'sim_tech']:
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_ret REAL DEFAULT 0.0")
        except: pass
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_status TEXT DEFAULT 'OPEN'")
        except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V24.0 시장 폭(Breadth) 실험 존 컬럼 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN market_breadth REAL DEFAULT 1.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_breadth REAL DEFAULT 1.0")
    except: pass
    for p in ['sim_breadth']:
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_ret REAL DEFAULT 0.0")
        except: pass
        try: cursor.execute(f"ALTER TABLE forward_trades ADD COLUMN {p}_status TEXT DEFAULT 'OPEN'")
        except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V35.0 자율 조율을 위한 진입 시점 DNA/DTW 채점표 박제 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_cos_score REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_dtw_score REAL DEFAULT 99.0")
    except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V38.0 자본 기반 리스크 패리티 컬럼 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN invest_amount REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN shares INTEGER DEFAULT 0")
    except: pass
    # 👆👆 [추가 끝] 👆👆

    # 👇👇 [추가] V39.0 동적 켈리 베팅 시뮬레이션 컬럼 👇👇
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN entry_regime TEXT DEFAULT 'UNKNOWN'")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN sim_kelly_risk_pct REAL DEFAULT 0.02")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN sim_kelly_invest REAL DEFAULT 0.0")
    except: pass
    try: cursor.execute("ALTER TABLE forward_trades ADD COLUMN sim_kelly_profit REAL DEFAULT 0.0")
    except: pass
    # 👆👆 [추가 끝] 👆👆

    # 리포트용: 원본 tier 불변, 실적 기반 표시 등급(선택)
    try:
        cursor.execute("ALTER TABLE forward_trades ADD COLUMN tier_effective TEXT")
    except Exception:
        pass

    try:
        import shadow_tracking

        shadow_tracking.init_shadow_tables(cursor)
    except Exception as e:
        print(f"⚠️ 그림자 장부 스키마 초기화 스킵: {e}")

    conn.commit()
    conn.close()


_FORWARD_TRADE_INSERT_COLS: tuple[str, ...] = (
    "entry_date",
    "market",
    "code",
    "name",
    "sector",
    "sig_type",
    "tier",
    "total_score",
    "dyn_rs",
    "dyn_cpv",
    "dyn_tb",
    "is_death_combo",
    "is_tenbagger",
    "entry_price",
    "v_cpv",
    "v_yang",
    "v_energy",
    "v_rs",
    "max_high",
    "min_low",
    "market_breadth",
    "entry_breadth",
    "entry_cos_score",
    "entry_dtw_score",
    "entry_atr",
    "invest_amount",
    "shares",
    "sim_kelly_invest",
    "entry_regime",
)


def _insert_forward_trade_row(cursor: sqlite3.Cursor, row: dict) -> None:
    """forward_trades INSERT — 컬럼·플레이스홀더 개수 SSOT (스키마 드리프트 방지)."""
    cols = _FORWARD_TRADE_INSERT_COLS
    values = tuple(row[c] for c in cols)
    if len(values) != len(cols):
        raise ValueError(
            f"forward_trades row length {len(values)} != columns {len(cols)}"
        )
    col_sql = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)
    cursor.execute(
        f"INSERT INTO forward_trades ({col_sql}) VALUES ({placeholders})",
        values,
    )


# ---------------------------------------------------------------------------
# 텔레그램 리포트 전용: 유효 보유 집계 + 좀비 OPEN 자가 치유 (매매 order 경로 미개입)
# ---------------------------------------------------------------------------
def _reporter_qty_numeric(df: pd.DataFrame) -> pd.Series:
    """current_qty·shares 중 존재하는 컬럼만 사용해 수량 시리즈(스칼라 max)를 만든다."""
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


def _reporter_is_live_open_status(st: pd.Series) -> pd.Series:
    """스키마가 OPEN 또는 ACTIVE(대소문자 무시)인 행."""
    u = st.astype(str).str.strip().str.upper()
    return u.isin(["OPEN", "ACTIVE"])


def _reporter_valid_holding_mask(df: pd.DataFrame) -> pd.Series:
    """리포트·쿼터 표시용: 살아 있는 오픈 상태이면서 실제 수량 > 0 인 행만 유효 보유."""
    if df is None or df.empty or "status" not in df.columns:
        return pd.Series(False, index=df.index if df is not None else [], dtype=bool)
    return _reporter_is_live_open_status(df["status"]) & (_reporter_qty_numeric(df) > 0)


# 실무자 리포트·좀비 정리 — 내부 exit_reason 코드 (DB 저장·표시 치환)
_EXIT_REASON_INTERNAL = frozenset(
    {
        "REPORTER_SELF_HEAL_ZOMBIE",
        "REPORTER_SELF_HEAL_FACT_CLOSE",
    }
)
_EXIT_REASON_ZOMBIE_DB = "강제청산(기간만료·데이터누락)"
_EXIT_REASON_FACT_CLOSE_DB = "강제청산(청산팩트정리)"


def _format_exit_reason_display(reason: object) -> str:
    """텔레그램 노출용 — 내부 디버그 태그는 유저 친화 문구로 치환."""
    s = str(reason or "").strip()
    if not s or s.lower() in ("none", "nan"):
        return "사유 미기록"
    mapping = {
        "REPORTER_SELF_HEAL_ZOMBIE": _EXIT_REASON_ZOMBIE_DB,
        "REPORTER_SELF_HEAL_FACT_CLOSE": _EXIT_REASON_FACT_CLOSE_DB,
        _EXIT_REASON_ZOMBIE_DB: _EXIT_REASON_ZOMBIE_DB,
        _EXIT_REASON_FACT_CLOSE_DB: _EXIT_REASON_FACT_CLOSE_DB,
    }
    return mapping.get(s, s)


def _normalize_trade_market(code: object, market: object) -> str:
    """
    code·market 불일치 교정 — KR 리포트에 US 티커 누수 방지.
    KR: 5~6자리 숫자 코드 / US: 알파벳 티커.
    """
    c = str(code or "").strip().upper()
    m = str(market or "").strip().upper()
    if re.fullmatch(r"\d{5,6}", c) or (c.isdigit() and len(c) <= 6):
        return "KR"
    if c and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", c):
        return "US"
    if m in ("KR", "US"):
        return m
    return "KR"


def _parse_mkt_group_key(mkt_group: str) -> tuple[str, str]:
    """mkt_group = '{KR|US}_{group}' — group 내 '_' 는 분리하지 않음."""
    s = str(mkt_group or "").strip()
    su = s.upper()
    if su.startswith("KR_"):
        return "KR", s[3:]
    if su.startswith("US_"):
        return "US", s[3:]
    if "_" in s:
        head, tail = s.split("_", 1)
        return head.upper(), tail
    return "KR", s


def _safe_final_ret_pct(val: object, default: float = 0.0) -> float:
    r = pd.to_numeric(val, errors="coerce")
    if r is None or (isinstance(r, float) and (np.isnan(r) or not np.isfinite(r))):
        return float(default)
    try:
        f = float(r)
    except (TypeError, ValueError):
        return float(default)
    return f if np.isfinite(f) else float(default)


def _win_loss_flat_counts(ret_series: pd.Series) -> tuple[int, int, int]:
    """동일 Series 기준 승/패/무 — NaN 은 무(0%) 처리."""
    r = pd.to_numeric(ret_series, errors="coerce").fillna(0.0)
    win = int((r > 0).sum())
    loss = int((r < 0).sum())
    flat = int((r == 0).sum())
    return win, loss, flat


def _exit_date_on_calendar(val: object) -> str:
    s = str(val or "").strip()
    return s[:10] if len(s) >= 10 else s


def _reporter_unified_vip_fleet_mask(df_open: pd.DataFrame) -> pd.Series:
    """레거시 — market 미지정 시 KR VIP 규칙."""
    return _reporter_deploy_fleet_mask(df_open, "KR")


def _reporter_deploy_fleet_mask(df_open: pd.DataFrame, market: str) -> pd.Series:
    """
    [4/9] 투입 집계: VIP(🔥/🛡️) + 투입금>0.
    US: STANDARD/US_BOWL/US_MASTER 등 실전 스캐너 sig 포함 (KR 대칭).
    """
    if df_open is None or df_open.empty:
        return pd.Series(dtype=bool)
    sig = df_open["sig_type"].astype(str)
    sk = (
        pd.to_numeric(df_open["sim_kelly_invest"], errors="coerce").fillna(0.0)
        if "sim_kelly_invest" in df_open.columns
        else pd.Series(0.0, index=df_open.index)
    )
    inv = (
        pd.to_numeric(df_open["invest_amount"], errors="coerce").fillna(0.0)
        if "invest_amount" in df_open.columns
        else pd.Series(0.0, index=df_open.index)
    )
    has_inv = (sk > 0.0) | (inv > 0.0)
    vip = sig.str.contains("🔥주도주", na=False) | sig.str.contains("🛡️차기섹터", na=False)
    if str(market or "").upper() == "US":
        us_deploy = (
            sig.str.contains("US_BOWL", na=False)
            | sig.str.contains("US_MASTER", na=False)
            | sig.str.contains("US_5EMA", na=False)
            | sig.str.contains("US_NUL", na=False)
            | sig.str.contains("SUPERNOVA", na=False)
            | (sig.str.contains("STANDARD", na=False) & sig.str.contains("US", na=False))
        )
        return (vip | us_deploy) & has_inv
    return vip & has_inv


def _daily_report_trades_for_market(df_all: pd.DataFrame, market: str) -> pd.DataFrame:
    """일일 통합 리포트 — code 기반 market 정규화 (KR/US 누수·오태깅 방지)."""
    if df_all is None or df_all.empty:
        return df_all.copy() if df_all is not None else pd.DataFrame()
    mkt = str(market or "").upper()
    code_col = "code" if "code" in df_all.columns else ("ticker" if "ticker" in df_all.columns else None)
    if code_col is None:
        return df_all[df_all["market"].astype(str).str.upper() == mkt].copy()
    mk_series = df_all["market"] if "market" in df_all.columns else pd.Series("", index=df_all.index)
    norm = [
        _normalize_trade_market(df_all.iloc[i][code_col], mk_series.iloc[i])
        for i in range(len(df_all))
    ]
    return df_all.iloc[[i for i, nm in enumerate(norm) if nm == mkt]].copy()


def _reporter_cleanup_zombie_forward_trades() -> int:
    """
    (1) 투입·수량 모두 0인데 OPEN/ACTIVE → CLOSED_ZOMBIE
    (2) 청산 팩트(exit_date 또는 final_ret) + 수량 0 인데 OPEN/ACTIVE → CLOSED_AUTO
    DROP 없음·리포트 경로 전용.
    """
    if not os.path.isfile(DB_PATH):
        return 0
    init_forward_db()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    total = 0
    try:
        colnames = {row[1] for row in conn.execute("PRAGMA table_info(forward_trades)").fetchall()}
        qty_zero = "(COALESCE(shares,0) <= 0)"
        if "current_qty" in colnames:
            qty_zero = "(COALESCE(shares,0) <= 0 AND COALESCE(current_qty,0) <= 0)"
        invest_zero = (
            "(COALESCE(sim_kelly_invest,0) <= 0 AND COALESCE(invest_amount,0) <= 0)"
        )
        status_open = "(status = 'OPEN' OR UPPER(TRIM(IFNULL(status,''))) = 'ACTIVE')"
        observe_skip = "AND IFNULL(sig_type,'') NOT LIKE '%OBSERVE_ONLY%'"
        cur = conn.execute(
            f"SELECT id FROM forward_trades WHERE {status_open} AND {qty_zero} AND {invest_zero} {observe_skip}"
        )
        ids = [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]
        exit_day = datetime.now().strftime("%Y-%m-%d")
        if ids:
            reason = _EXIT_REASON_ZOMBIE_DB
            conn.executemany(
                """
                UPDATE forward_trades
                SET status='CLOSED_ZOMBIE', exit_date=?, exit_reason=?,
                    final_ret=COALESCE(final_ret, 0.0)
                WHERE id=?
                """,
                [(exit_day, reason, i) for i in ids],
            )
            total += len(ids)

        # 청산 팩트가 있는데 status 만 살아 있는 행 → 소프트 클린 (DELETE 금지)
        fact_parts = []
        if "exit_date" in colnames:
            fact_parts.append(
                "(exit_date IS NOT NULL AND TRIM(CAST(exit_date AS TEXT)) != '')"
            )
        if "final_ret" in colnames:
            fact_parts.append("(final_ret IS NOT NULL)")
        fact_or = " OR ".join(fact_parts) if fact_parts else "0"
        fact_close = f"""
            {status_open}
            AND {qty_zero}
            AND ({fact_or})
        """
        cur2 = conn.execute(f"SELECT id FROM forward_trades WHERE {fact_close}")
        ids2 = [int(r[0]) for r in cur2.fetchall() if r and r[0] is not None]
        ids_set = set(ids)
        ids2 = [i for i in ids2 if i not in ids_set]
        if ids2:
            rsn = _EXIT_REASON_FACT_CLOSE_DB
            conn.executemany(
                """
                UPDATE forward_trades
                SET status='CLOSED_AUTO', exit_reason=?,
                    final_ret=COALESCE(final_ret, 0.0)
                WHERE id=?
                """,
                [(rsn, i) for i in ids2],
            )
            total += len(ids2)

        if total:
            conn.commit()
        return total
    finally:
        conn.close()


def _dynamic_tier_downgrade_enabled(sys_config: dict) -> bool:
    """system_config 의 ENABLE_DYNAMIC_TIER_DOWNGRADE (기본 True). False 시 원본 tier 만 사용."""
    v = sys_config.get("ENABLE_DYNAMIC_TIER_DOWNGRADE", True)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off", ""):
        return False
    return True


def _tier80_line_from_stored_effective(stored: object, wr_pct: float, n: int) -> str:
    """
    DB tier_effective 단일 값 → 텔레그램 한 줄. (표시 로직은 이 함수 한 곳만)
    stored: None / '2티어' / '3티어' / 'UNCONFIRMED' / 'DOWNGRADE_OFF'
    """
    if stored is None or pd.isna(stored):
        return (
            f"💎 실효 1티어(원점수 80점대) 승률: {wr_pct:.1f}% "
            f"(동적 강등: 유지, DB tier_effective=NULL)\n"
        )
    vs = str(stored).strip()
    if vs == "UNCONFIRMED":
        return (
            f"◽ 표본 부족 — 실효 티어 미확정 (원점수 80점대) | "
            f"관측 청산 승률: {wr_pct:.1f}% (N={n}, 최소 5건 필요)\n"
        )
    if vs == "DOWNGRADE_OFF":
        return (
            f"◽ 원점수 80점대 (동적 강등 OFF) | 청산 승률: {wr_pct:.1f}% (N={n})\n"
        )
    if vs == "2티어":
        return f"⚠️ 2티어(원점수 80점대 | 실적 강등) 승률: {wr_pct:.1f}%\n"
    if vs == "3티어":
        return f"⚠️ 3티어(원점수 80점대 | 실적 강등) 승률: {wr_pct:.1f}%\n"
    return f"◽ 실효 티어(DB): {vs} | 청산 승률: {wr_pct:.1f}% (N={n})\n"


def _tier80_sync_effective_and_report_line(
    market: str, t1_df: pd.DataFrame, sys_config: dict
) -> str:
    """
    [5/9] 단일 소스: 청산 표본으로 tier_effective 를 DB에 먼저 반영한 뒤,
    동일 커넥션에서 읽어온 값으로만 텔레그램 문구를 생성한다.
    """
    if t1_df is None or t1_df.empty:
        return ""

    n = int(len(t1_df))
    wins = int(len(t1_df[t1_df["final_ret"] > 0]))
    wr = wins / float(n) if n else 0.0
    wr_pct = wr * 100.0
    downgrade_on = _dynamic_tier_downgrade_enabled(sys_config)

    if not downgrade_on:
        db_val = "DOWNGRADE_OFF"
    elif n < 5:
        db_val = "UNCONFIRMED"
    elif wr < 0.35:
        db_val = "3티어"
    elif wr < 0.50:
        db_val = "2티어"
    else:
        db_val = None

    init_forward_db()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    _tier80_where = (
        "market = ? AND (tier = '80점대' OR (total_score >= 80 AND total_score < 90))"
    )
    try:
        if "id" in t1_df.columns and not t1_df["id"].dropna().empty:
            ids = [int(x) for x in t1_df["id"].dropna().unique()]
            ph = ",".join("?" * len(ids))
            conn.execute(
                f"UPDATE forward_trades SET tier_effective = ? WHERE id IN ({ph})",
                (db_val, *ids),
            )
        else:
            conn.execute(
                f"UPDATE forward_trades SET tier_effective = ? WHERE {_tier80_where}",
                (db_val, market),
            )
        conn.commit()
        row = conn.execute(
            f"SELECT tier_effective FROM forward_trades WHERE {_tier80_where} LIMIT 1",
            (market,),
        ).fetchone()
        stored = row[0] if row is not None else db_val
        return _tier80_line_from_stored_effective(stored, wr_pct, n)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"⚠️ tier_effective 동기화/표시 스킵 ({market}): {e}")
        return ""
    finally:
        conn.close()


def _spillover_fallback_enabled(sys_config: dict) -> bool:
    """ENABLE_SPILLOVER_FALLBACK (기본 True). False 시 LAST_GOOD 폴백 미사용."""
    v = sys_config.get("ENABLE_SPILLOVER_FALLBACK", True)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    return True


def _resolve_us_spillover_telegram_inner(sys_config: dict) -> str:
    """
    [7/9] 대괄호 안에 넣을 미국 주도 섹터 문자열.
    sector_spillover_refresh.resolve_us_spillover_display 위임 (휴일 LAST_GOOD 폴백).
    """
    try:
        from sector_spillover_refresh import resolve_us_spillover_display

        return resolve_us_spillover_display(sys_config)
    except Exception:
        lg = str(sys_config.get("US_SPILLOVER_SECTOR_LAST_GOOD") or "").strip()
        if lg and lg not in ("분석중", "NONE", ""):
            return lg
        return "데이터 없음"


def _v28_add_norm_day_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out["norm_day"] = out["entry_date"].astype(str).str.slice(0, 10)
    return out


def _v28_dominant_sector_label_for_day(
    day: str,
    df_raw: pd.DataFrame,
    map_standard_sector,
    sector_row_ok,
) -> str:
    """단일 거래일·단일 시장: 가상매매 진입 행 기준 정직 라벨."""
    if df_raw is None or df_raw.empty or "norm_day" not in df_raw.columns:
        return "데이터 없음"
    sub = df_raw.loc[df_raw["norm_day"] == day]
    if sub.empty:
        return "데이터 없음"
    mapped = sub["sector"].map(lambda x: map_standard_sector(x))
    ok = mapped.map(sector_row_ok)
    if not ok.any():
        return "필터 탈락"
    good = mapped.loc[ok]
    mode_ser = good.mode()
    if mode_ser.empty:
        return "필터 탈락"
    v = str(mode_ser.iloc[0]).strip()
    return v if v else "필터 탈락"


def _v28_us_label_with_last_good_cache(
    day: str,
    base_label: str,
    sys_config: dict,
    map_standard_sector,
) -> str:
    """US 칸: 당일 표본 없을 때 LAST_GOOD 캐시(직전 영업일) — 주말에도 금요일 섹터 표시."""
    if base_label != "데이터 없음":
        return base_label
    if not _spillover_fallback_enabled(sys_config):
        return base_label
    lg = sys_config.get("US_SPILLOVER_SECTOR_LAST_GOOD")
    lg_s = str(lg).strip() if lg is not None else ""
    if not lg_s or lg_s in ("분석중", "NONE"):
        return base_label
    asof = str(sys_config.get("US_SPILLOVER_SECTOR_AS_OF") or "").strip()[:10]
    if asof and asof > day:
        return base_label
    ms = map_standard_sector(lg_s)
    if not ms or ms == "기타/혼합":
        return base_label
    if asof == day:
        return f"캐시·{ms}"
    return f"캐시·{ms}({asof[5:] if len(asof) >= 10 else asof})"


def _deathmatch_min_n(sys_config: dict) -> int:
    from deathmatch_report import deathmatch_min_n

    return deathmatch_min_n(sys_config)


def _fmt_deathmatch_ret(ret, n_closed: int, *, n_valid=None) -> str:
    from deathmatch_report import fmt_deathmatch_ret

    return fmt_deathmatch_ret(ret, n_closed, n_valid=n_valid)


def _deathmatch_ab_verdict(n_std: int, n_sn: int, std_ret, sn_ret, n_min: int) -> str:
    from deathmatch_report import deathmatch_ab_verdict

    return deathmatch_ab_verdict(n_std, n_sn, std_ret, sn_ret, n_min)


# 💡 [시스템 연결] 관제탑 설정 — 동시 쓰기 완화(파일 락 + update_config)
from system_config_atomic import CONFIG_PATH, load_config, save_config, update_config


def load_system_config():
    return load_config()


def save_system_config(config):
    return save_config(config)


# system_auto_pilot.run_autonomous_analysis와 동일 정의: RSP/SPY, 50일 롤링 평균 대비 현재 비율 (0.97 미만 = 쏠림)
_BREADTH_CACHE_DAY = None
_BREADTH_CACHE_VAL = 1.0
_BREADTH_CACHE_LOCK = threading.Lock()

def get_cached_market_breadth():
    """시장 폭(Breadth). 당일 1회만 yfinance 호출하여 스케줄/루프 비용 절감."""
    global _BREADTH_CACHE_DAY, _BREADTH_CACHE_VAL, _BREADTH_CACHE_LOCK

    with _BREADTH_CACHE_LOCK:
        tz = pytz.timezone('America/New_York')
        day_key = datetime.now(tz).strftime('%Y-%m-%d')
        if _BREADTH_CACHE_DAY == day_key:
            return float(_BREADTH_CACHE_VAL)
        v = 1.0
        try:
            df_idx = yf.download("SPY RSP", period="6mo", interval="1d", group_by="ticker", progress=False)
            if not df_idx.empty:
                if isinstance(df_idx.columns, pd.MultiIndex) and 'SPY' in df_idx.columns.get_level_values(0):
                    spy_c = df_idx['SPY']['Close'].dropna()
                    rsp_c = df_idx['RSP']['Close'].dropna()
                else:
                    raise ValueError("breadth_multiindex_expected")
                common = spy_c.index.intersection(rsp_c.index)
                spy_c = spy_c.reindex(common).ffill()
                rsp_c = rsp_c.reindex(common).ffill()
                if len(common) >= 50:
                    v = (rsp_c.iloc[-1] / spy_c.iloc[-1]) / (
                        rsp_c.rolling(50).mean().iloc[-1] / spy_c.rolling(50).mean().iloc[-1]
                    )
                elif len(common) >= 5:
                    v = (rsp_c.iloc[-1] / spy_c.iloc[-1]) / (rsp_c.mean() / spy_c.mean())
        except Exception:
            pass
        _BREADTH_CACHE_DAY = day_key
        _BREADTH_CACHE_VAL = float(v) if np.isfinite(v) else 1.0
        return float(_BREADTH_CACHE_VAL)

def evaluate_evolved_alpha_formula(df, formula):
    """JSON에 저장된 진화 수식을 실시간으로 계산한다."""
    if df.empty:
        return None
    try:
        O = df['Open']
        H = df['High']
        L = df['Low']
        C = df['Close']
        V = df['Volume']

        def add(a, b): return a + b
        def sub(a, b): return a - b
        def mul(a, b): return a * b
        def div(a, b):
            safe_b = b.replace(0, np.nan) if isinstance(b, pd.Series) else (np.nan if b == 0 else b)
            return a / safe_b
        def rolling_mean(x, w): return x.rolling(int(w)).mean()
        def rolling_std(x, w): return x.rolling(int(w)).std()

        env = {
            'O': O, 'H': H, 'L': L, 'C': C, 'V': V,
            'add': add, 'sub': sub, 'mul': mul, 'div': div,
            'rolling_mean': rolling_mean, 'rolling_std': rolling_std
        }
        out = eval(str(formula), {"__builtins__": {}}, env)
        if isinstance(out, pd.Series):
            return float(out.replace([np.inf, -np.inf], np.nan).iloc[-1])
    except Exception:
        return None
    return None

def _resolve_incubator_parent_genes(sys_config):
    """S급 부모: DNA_SUPERNOVA_MFE_WEIGHTED → DNA_ALPHA* 챔피언 벡터 → 관제탑 스칼라 폴백."""
    sn = sys_config.get("DNA_SUPERNOVA_MFE_WEIGHTED")
    if isinstance(sn, dict) and any(k in sn for k in ("cpv", "tb", "bbe")):
        return (
            float(sn.get("cpv", 0.75)),
            float(sn.get("tb", 10.0)),
            float(sn.get("bbe", 20.0)),
            float(sys_config.get("KR_S1_RS_CUTOFF", 165.0)),
        )
    for k, v in sys_config.items():
        if isinstance(v, dict) and "DNA_ALPHA" in k and "shape" in v:
            return (
                float(v.get("cpv", 0.75)),
                float(v.get("tb", 10.0)),
                float(v.get("bbe", 20.0)),
                float(v.get("rs", sys_config.get("KR_S1_RS_CUTOFF", 165.0))),
            )
    return (
        float(sys_config.get("DYNAMIC_TRAP_LIMIT", 0.75)),
        10.0,
        float(sys_config.get("DYNAMIC_OD_HURDLE", 20.0)),
        float(sys_config.get("KR_S1_RS_CUTOFF", 165.0)),
    )

def _gaussian_gene_mutate(base, pct=0.15):
    """부모 대비 ±pct(기본 15%) 이내 가우시안 미세 변이."""
    sigma = pct / 2.5
    delta = float(np.clip(np.random.normal(0.0, sigma), -pct, pct))
    return float(base) * (1.0 + delta)


def _merge_incubator_templates(existing_incubator, mutants, max_entries=50):
    """INCUBATOR_TEMPLATES 덮어쓰기 방지: 병합 후 created_at 가장 오래된 항목부터 삭제해 최대 max_entries개 유지."""
    merged = {}
    if isinstance(existing_incubator, dict):
        for k, v in existing_incubator.items():
            merged[k] = dict(v) if isinstance(v, dict) else v
    if isinstance(mutants, dict):
        for k, v in mutants.items():
            merged[k] = dict(v) if isinstance(v, dict) else v
    if len(merged) <= max_entries:
        return merged
    ranked = []
    for k, v in merged.items():
        if isinstance(v, dict):
            ca = str(v.get("created_at") or "")[:10] or "1970-01-01"
        else:
            ca = "1970-01-01"
        ranked.append((ca, k))
    ranked.sort(key=lambda x: (x[0], x[1]))
    n_drop = len(merged) - max_entries
    for _, k in ranked[:n_drop]:
        merged.pop(k, None)
    return merged


def generate_mutant_strategies():
    """장 마감 후 인큐베이터용 돌연변이 3종: 부모(S급 DNA) 상속 + 가우시안 미세 변이."""
    sys_config = load_system_config()
    base_cpv, base_tb, base_bbe, base_rs = _resolve_incubator_parent_genes(sys_config)
    cos_parent = float(sys_config.get("DYNAMIC_ALPHA_LIMIT", 0.78))

    mutants = {}
    for i in range(1, 4):
        m_name = f"MUTANT_{i}"
        mutants[m_name] = {
            "cpv": round(_gaussian_gene_mutate(base_cpv), 3),
            "tb": round(max(0.5, _gaussian_gene_mutate(base_tb)), 3),
            "bbe": round(max(2.0, _gaussian_gene_mutate(base_bbe)), 3),
            "rs": round(_gaussian_gene_mutate(base_rs), 3),
            "cos_cutoff": round(float(np.clip(_gaussian_gene_mutate(cos_parent), 0.55, 0.95)), 3),
            "created_at": datetime.now().strftime('%Y-%m-%d'),
            "status": "INCUBATING"
        }

    existing_incubator = sys_config.get("INCUBATOR_TEMPLATES", {})
    if not isinstance(existing_incubator, dict):
        existing_incubator = {}
    sys_config["INCUBATOR_TEMPLATES"] = _merge_incubator_templates(existing_incubator, mutants, max_entries=50)
    sys_config["INCUBATOR_LAST_GEN_DATE"] = datetime.now().strftime('%Y-%m-%d')
    save_system_config(sys_config)
    send_telegram_msg("🧪 [인큐베이터] 금일 돌연변이 전략 3종(MUTANT_1~3) 생성 및 임시 저장 완료")


def get_smart_money_avg_price_from_ssot(sys_config: dict, code: object) -> float:
    """
    스마트머니 잠재 평단(SSOT): `smart_money_tracker.py` 가 기록한
    system_config['SMART_MONEY_RADAR']['picks'][종목코드]['avg_price'] 만 사용.
    실험용 smart_money_targets.json / smart_money_kalman 경로는 사용하지 않음.
    """
    try:
        if not isinstance(sys_config, dict):
            return 0.0
        rad = sys_config.get("SMART_MONEY_RADAR") or {}
        picks = rad.get("picks", {}) if isinstance(rad, dict) else {}
        if not isinstance(picks, dict):
            return 0.0
        code_str = str(code).strip()
        smart_info = picks.get(code_str)
        if smart_info is None:
            smart_info = picks.get(str(code))
        if smart_info is None:
            try:
                smart_info = picks.get(str(int(code_str)))
            except (TypeError, ValueError):
                smart_info = None
        if isinstance(smart_info, dict):
            return float(smart_info.get("avg_price", 0) or 0)
    except (TypeError, ValueError, Exception):
        pass
    return 0.0


# ==========================================
# 1. 신규 종목 가상매매 편입 엔진 (검색기에서 호출)
# ==========================================
def try_add_virtual_position(
    market,
    code,
    name,
    sig_type,
    score,
    ep,
    facts,
    sector="유망섹터",
    trade_source="STANDARD",
    satellite_tags=None,
):
    init_forward_db()
    code_str = str(code).zfill(6) if market == 'KR' else str(code)

    try:
        from sector_normalize import normalize_sector_for_db

        sector = normalize_sector_for_db(sector, market=market)
    except Exception:
        pass

    def map_standard_sector(s):
        s_str = str(s).lower()
        if any(k in s_str for k in ["반도체", "it", "ai", "소프트웨어", "모바일", "테크", "데이터"]):
            return "반도체/IT"
        if any(k in s_str for k in ["바이오", "헬스", "의료", "제약"]):
            return "바이오/헬스케어"
        if any(k in s_str for k in ["배터리", "2차전지", "화학", "에너지", "정유"]):
            return "에너지/화학"
        if any(k in s_str for k in ["금융", "은행", "증권", "지주", "투자"]):
            return "금융/지주"
        if any(k in s_str for k in ["기계", "조선", "방산", "산업재", "로봇", "전력"]):
            return "산업재/기계"
        if any(k in s_str for k in ["소비", "유통", "식품", "화장품", "엔터", "미디어"]):
            return "소비재/엔터"
        return "기타/혼합"

    sector = map_standard_sector(sector)

    # 계좌 통합 서킷 브레이커가 켜지면 신규 진입 전면 차단
    pre_sys_config = load_system_config()
    if pre_sys_config.get("GLOBAL_CIRCUIT_BREAKER", "OFF") == "ON":
        return False, "🚫 글로벌 서킷 브레이커 ON: 블랙스완 방어 모드로 신규 진입이 차단되었습니다."

    # 🛰️ [통합 방어막] 둠스데이 / 오답노트(bbox) / 스마트머니 교차검증 (모든 검색기 공통 관문)
    _sp_perf = pre_sys_config.get("SHADOW_PERFORMANCE")
    if not isinstance(_sp_perf, dict):
        _sp_perf = {}

    _dd = pre_sys_config.get("DOOMSDAY_DEFCON") or {}
    defcon_level = 5
    if isinstance(_dd, dict):
        try:
            defcon_level = int(_dd.get("level", 5))
        except (TypeError, ValueError):
            defcon_level = 5
    if defcon_level <= 2:
        if _shadow_reason_defense_is_opportunity_cost_loss(_sp_perf, "DOOMSDAY_DEFCON"):
            sig_type = f"{sig_type} [🛡️둠스데이_자율해제: 기회비용 방어]"
        else:
            try:
                import shadow_tracking
                shadow_tracking.record_blocked_trade(code_str, name, "DOOMSDAY_DEFCON", ep)
            except Exception:
                pass
            return False, "🛑 둠스데이 방어막 작동: 거시경제 발작으로 롱 포지션 진입 차단"

    _ap = pre_sys_config.get("ANTI_PATTERNS")
    _ml = _toxic_ml_antipatterns_rule_map(pre_sys_config.get("TOXIC_ML_ANTIPATTERNS"))
    merged_anti = {}
    if isinstance(_ap, dict):
        merged_anti.update(_ap)
    elif isinstance(_ap, list):
        for _i, _bounds in enumerate(_ap):
            if isinstance(_bounds, dict):
                merged_anti[f"PATTERN_{_i}"] = _bounds
    if isinstance(_ml, dict) and _ml:
        merged_anti = {**merged_anti, **_ml}

    facts_d = facts if isinstance(facts, dict) else {}
    try:
        cpv = float(facts_d.get("dyn_cpv", 0) or 0)
    except (TypeError, ValueError):
        cpv = 0.0
    try:
        tb = float(facts_d.get("dyn_tb", 0) or 0)
    except (TypeError, ValueError):
        tb = 0.0
    try:
        bbe = float(facts_d.get("v_energy", 0) or 0)
    except (TypeError, ValueError):
        bbe = 0.0
    _dr = facts_d.get("dyn_rs", None)
    try:
        dyn_rs_live = float(_dr) if _dr is not None and str(_dr).strip() != "" else float("nan")
    except (TypeError, ValueError):
        dyn_rs_live = float("nan")

    is_toxic_bbox = False
    for _, bounds in merged_anti.items():
        if not isinstance(bounds, dict):
            continue
        if evaluate_toxic_bbox_match(bounds, cpv, tb, bbe, dyn_rs_live, sector):
            is_toxic_bbox = True
            break
    if is_toxic_bbox:
        if _shadow_reason_defense_is_opportunity_cost_loss(_sp_perf, "TOXIC_ANTI_PATTERN") or _shadow_reason_defense_is_opportunity_cost_loss(_sp_perf, "TOXIC_ML_TREE"):
            sig_type = f"{sig_type} [🛡️오답노트_자율해제: 기회비용 방어]"
        else:
            try:
                import shadow_tracking
                shadow_tracking.record_blocked_trade(code_str, name, "TOXIC_ANTI_PATTERN", ep)
            except Exception:
                pass
            return False, "💀 안티패턴 면역 차단: 과거 치명적 참사주 DNA와 일치함"

    try:
        avg_price = get_smart_money_avg_price_from_ssot(pre_sys_config, code_str)
        if avg_price > 0:
            try:
                ep_f = float(ep)
            except (TypeError, ValueError):
                ep_f = 0.0
            if abs(ep_f - avg_price) / avg_price <= 0.03:
                sig_type = f"{sig_type} [🕵️세력매집_교차검증]"
    except Exception:
        pass
    
    # 💡 [V13.0 가상매매] 10점 단위 정밀 버킷 생성 (예: 85점 -> 80점대)
    score_bucket = int(score // 10) * 10
    if score_bucket >= 100: score_bucket = 90 # 100점은 90점대 최상위 티어로 병합
    tier_label = f"{score_bucket}점대"

    # 💡 [V25.0 픽스] 진입 함수에도 Timeout과 WAL 모드 필수 적용
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # 1. 중복 체크 (이미 포트폴리오에 보유 중인 종목은 제외)
    cursor.execute("SELECT id FROM forward_trades WHERE code=? AND status='OPEN'", (code_str,))
    if cursor.fetchone():
        conn.close()
        return False, "중복 보유 중"

    cursor.execute("SELECT COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN'", (market,))
    current_open_count = cursor.fetchone()[0] or 0
    if current_open_count >= 20:
        conn.close()
        return False, f"🚨 시장 쿼터 초과: {market}장 최대 보유 한도(20개)에 도달하여 신규 진입을 차단합니다."
        
    # 2. 👑 [V23.0 포트폴리오 다중화: 주도섹터 폭격(2) + 차기섹터 정찰(2)]
    tz = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    today_str = datetime.now(tz).strftime('%Y-%m-%d')

    # 현재 포트폴리오의 1위 주도 섹터 파악 (자금 쏠림 감지)
    # 💡 [픽스] '유망'이 포함된 임시 섹터명은 주도 섹터 집계에서 제외
    cursor.execute("SELECT sector FROM forward_trades WHERE market=? AND status='OPEN' AND sector NOT LIKE '%유망%' GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 1", (market,))
    dom_row = cursor.fetchone()
    dominant_sector = dom_row[0] if dom_row else "None"

    # 👇👇 [V102.7 버그 픽스] 글로벌 쿼터제 ➔ '독립 펀드매니저(로직)'별 쿼터제로 완벽 분리 👇👇
    # '점수 티어'가 아닌, 진입을 요청한 해당 '시그널 로직(sig_type)'만의 오늘 진입 내역을 불러옵니다.
    cursor.execute("SELECT sector FROM forward_trades WHERE entry_date=? AND market=? AND sig_type LIKE ?", (today_str, market, f"%{sig_type}%"))
    today_sectors = [r[0] for r in cursor.fetchall()]

    if len(today_sectors) >= 4:
        conn.close()
        return False, f"오늘의 [{sig_type}] 최대 쿼터(4개) 모두 확보됨 (스킵)"

    # 로직 분기: 진입하려는 종목이 현재 시장을 주도하는 섹터인가?
    trend_bought = sum(1 for s in today_sectors if s == dominant_sector)
    hedge_bought = sum(1 for s in today_sectors if s != dominant_sector)

    if sector == dominant_sector:
        if trend_bought >= 2:
            conn.close()
            return False, f"🚨 섹터 쿼터 초과: [{sig_type}] 엔진이 이미 주도섹터({dominant_sector}) 공격 편대 2기를 모두 파견했습니다."
        track_tag = "[🔥주도주 편대]"
    else:
        if hedge_bought >= 2:
            conn.close()
            return False, f"🛡️ 섹터 쿼터 초과: [{sig_type}] 엔진이 이미 타 섹터 정찰대 2기를 모두 파견했습니다."
        track_tag = "[🛡️차기섹터 정찰]"
    # 👆👆 [패치 완료] 👆👆

    # 시그널 타입에 트랙 태그(편대/정찰) 병합하여 기록
    sig_type = f"[{trade_source}] {sig_type} {track_tag}"

    # 👇👇 [수정] V34.0 DTW 투트랙 + V35.0 동적 커트라인 자율 매칭 👇👇
    max_alpha_cos, min_alpha_dtw = 0.0, 99.0
    alpha_bonus_score = 0.0
    max_trap_cos, min_trap_dtw = 0.0, 99.0
    is_rotation_prebuy = False
    incubator_match_name = None
    
    # 💡 [버그 픽스] 안전 변수 초기화 (에러 시 DB 엉킴 원천 방지)
    entry_atr, invest_amount, shares, sim_kelly_invest, cur_regime = 0.0, 0, 0, 0, "UNKNOWN"
    synthetic_survival_buff = False

    try:
        sys_config = load_system_config()
        table_name = f"{market}_{code_str}"
        idx_table = 'US_SPY' if market == 'US' else 'KR_KOSDAQ_IDX'
        
        # 💡 [버그 픽스] DB에 종목 테이블이 없어서 터지는 현상(no such table) 완벽 방어
        # DB에 없으면 실시간 API로 즉시 긁어와서 중단 없이 계산을 이어갑니다.
        try:
            # 👇👇 [V102.5 버그 픽스] 하이픈(-) 포함 테이블명 파싱 오류(Minus 연산자 오인) 해결 👇👇
            # 테이블명을 "{table_name}"으로 감싸주어야 BRK-B와 같은 티커를 정상 인식합니다.
            hist_df = pd.read_sql(f'SELECT * FROM "{table_name}" ORDER BY Date DESC LIMIT 300', conn).sort_values('Date')
        except:
            # 💡 [V102.4] EMA 224 정상 계산을 위한 300 거래일 데이터 확보 로직 유지
            st_dt = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
            from network_timeout import fdr_data_reader, yf_download

            hist_df = (
                fdr_data_reader(code_str, st_dt).tail(300)
                if market == 'KR'
                else yf_download(code_str, start=st_dt, progress=False).tail(300)
            )
            if isinstance(hist_df.columns, pd.MultiIndex): hist_df.columns = hist_df.columns.droplevel(1)
            hist_df = hist_df.reset_index()
            if 'index' in hist_df.columns: hist_df.rename(columns={'index': 'Date'}, inplace=True)

        try:
            # 벤치마크 지수 테이블 역시 안전하게 쌍따옴표 처리
            idx_df = pd.read_sql(f'SELECT * FROM "{idx_table}" ORDER BY Date DESC LIMIT 300', conn).sort_values('Date')
        except:
            st_dt = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
            idx_tk = '229200' if market == 'KR' else 'SPY'
            from network_timeout import fdr_data_reader, yf_download

            idx_df = (
                fdr_data_reader(idx_tk, st_dt).tail(300)
                if market == 'KR'
                else yf_download(idx_tk, start=st_dt, progress=False).tail(300)
            )
            if isinstance(idx_df.columns, pd.MultiIndex): idx_df.columns = idx_df.columns.droplevel(1)
            idx_df = idx_df.reset_index()
            if 'index' in idx_df.columns: idx_df.rename(columns={'index': 'Date'}, inplace=True)
        # 👆👆 [패치 완료] 👆👆
            
        # 💡 조건 완화: 신규 상장주나 데이터 누락을 위해 최소 60개 캔들만 있어도 무조건 계산 진행
        if len(hist_df) >= 60 and len(idx_df) >= 60:
            # 👆👆 [패치 완료] 👆👆
            c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
            idx_c = idx_df['Close'].values
            # (이하 기존 7D Z-Score 연산 및 DTW 로직 그대로 이어짐)
            
            # 1. 7D Z-Score 연산 (기존 유지)
            cpv = np.nanmean(np.where(h != l, (c - o) / (h - l), 0.5))
            v_ma20 = pd.Series(v).rolling(20).mean().values
            tb = np.nanmean(np.where(h != l, (v / v_ma20) / np.maximum((c - o) / (h - l), 0.01), 1.0))
            bb_std = pd.Series(c).rolling(20).std().values
            bbe = np.nanmax(np.where(bb_std > 0, 1.0 / ((4 * bb_std) / pd.Series(c).rolling(20).mean().values), 0)[-20:])
            rs_slope = ((c[-1] - c[0]) / c[0]) * 100
            tr = np.maximum(h - l, np.maximum(abs(h - np.roll(c, 1)), abs(l - np.roll(c, 1))))
            vcp_ratio = np.mean(tr[-20:]) / np.mean(tr) if np.mean(tr) > 0 else 1.0
            vol_flow = np.sum(np.where(c > o, v, 0)) / (np.sum(np.where(c < o, v, 0)) + 1)
            emas = [pd.Series(c).ewm(span=n).mean().iloc[-1] for n in [10, 20, 60, 112, 224]]
            ma_conv = (max(emas) - min(emas)) / min(emas) * 100
            
            idx_rs = ((idx_c[-1] - idx_c[0]) / idx_c[0]) * 100
            idx_vol = pd.Series(idx_c).pct_change().std() * 100 * np.sqrt(252)
            safe_vol = idx_vol if idx_vol > 0.1 else 1.0
            
            # 👇👇 [추가] V46.0 실시간 하락장 방어 프리미엄 주입 👇👇
            excess_return = rs_slope - idx_rs
            defiance_premium = 0.0
            if idx_rs < 0 and excess_return > 0:
                defiance_premium = abs(idx_rs) * 1.5
            
            z_rs = (excess_return + defiance_premium) / safe_vol
            # 👆👆 [추가 끝] 👆👆
            
            new_vec = np.nan_to_num(np.array([cpv, tb, bbe/safe_vol, z_rs, vcp_ratio, vol_flow, ma_conv]))
            
            # 2. [V34.0] 가격 궤적(Shape) 압축
            c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
            new_shape = np.mean(np.array_split(c_norm, 20), axis=1)

            # 3. [V34.0] 순수 파이썬 DTW 
            def calc_dtw(s, t):
                n, m = len(s), len(t)
                dtw = np.full((n+1, m+1), np.inf)
                dtw[0, 0] = 0
                for i in range(1, n+1):
                    for j in range(1, m+1):
                        cost = abs(s[i-1] - t[j-1])
                        dtw[i, j] = cost + min(dtw[i-1, j], dtw[i, j-1], dtw[i-1, j-1])
                return dtw[n, m]

            def cosine_sim(a, b):
                n_a, n_b = np.linalg.norm(a), np.linalg.norm(b)
                return np.dot(a, b) / (n_a * n_b) if n_a > 0 and n_b > 0 else 0
                
            # 투트랙 대조 (Cosine + DTW)
            for k, v_dict in sys_config.items():
                if isinstance(v_dict, dict) and 'shape' in v_dict:
                    t_vec = np.nan_to_num(np.array([v_dict.get('cpv',0), v_dict.get('tb',0), v_dict.get('bbe',0), v_dict.get('rs',0), v_dict.get('vcp',0), v_dict.get('vol',0), v_dict.get('ma',0)]))
                    t_shape = np.array(v_dict.get('shape'))
                    
                    cos_score = cosine_sim(new_vec, t_vec)
                    dtw_dist = calc_dtw(new_shape, t_shape)
                    
                    if "DNA_TRAP" in k:
                        max_trap_cos = max(max_trap_cos, cos_score)
                        min_trap_dtw = min(min_trap_dtw, dtw_dist)
                    elif "DNA_ALPHA" in k:
                        max_alpha_cos = max(max_alpha_cos, cos_score)
                        min_alpha_dtw = min(min_alpha_dtw, dtw_dist)

            # 인큐베이터 돌연변이 로직 섀도우 페이퍼 트레이딩 매칭
            incubator_templates = sys_config.get("INCUBATOR_TEMPLATES", {})
            if isinstance(incubator_templates, dict):
                for m_name, m_tpl in incubator_templates.items():
                    if not isinstance(m_tpl, dict):
                        continue
                    m_vec = np.array([
                        float(m_tpl.get("cpv", 0.0)),
                        float(m_tpl.get("tb", 0.0)),
                        float(m_tpl.get("bbe", 0.0)),
                        float(m_tpl.get("rs", 0.0))
                    ], dtype=float)
                    cur_mut_vec = np.array([cpv, tb, bbe, z_rs], dtype=float)
                    m_cos = cosine_sim(cur_mut_vec, m_vec)
                    if m_cos >= float(m_tpl.get("cos_cutoff", 0.80)):
                        incubator_match_name = m_name
                        break
            if incubator_match_name is None and isinstance(incubator_templates, dict) and isinstance(facts, dict):
                sk = facts.get("incubator_sniper_key")
                if sk and sk in incubator_templates:
                    incubator_match_name = sk

            # 진화 알파 팩터: 차단기가 아니라 코사인에 가산되는 알파 보너스(0~0.15)
            evolved_factors = sys_config.get("EVOLVED_ALPHA_FACTORS", {})
            if isinstance(evolved_factors, dict) and evolved_factors:
                alpha_vals = []
                for _, formula in evolved_factors.items():
                    v = evaluate_evolved_alpha_formula(hist_df, formula)
                    if v is not None and np.isfinite(v):
                        alpha_vals.append(v)
                if alpha_vals:
                    mv = max(alpha_vals)
                    evolved_threshold = float(sys_config.get("EVOLVED_ALPHA_THRESHOLD", 0.0))
                    if mv > evolved_threshold:
                        denom = max(abs(evolved_threshold), abs(mv) * 1e-9, 1e-12)
                        rel_excess = (mv - evolved_threshold) / denom
                        alpha_bonus_score = float(min(0.15, rel_excess * 0.15))
            
            predicted_sector = sys_config.get(f"PREDICTED_NEXT_SECTOR_{market}", "NONE")
            is_rotation_prebuy = (sector == predicted_sector) and (sector != "기타/혼합")
            spillover_sector = str(sys_config.get("US_SPILLOVER_SECTOR", "NONE"))
            is_spillover_prebuy = (market == 'KR') and (sector == spillover_sector) and (sector != "기타/혼합")

            # 💡 [V35.0] 관제탑이 하달한 동적 커트라인 로드 (하드코딩 삭제)
            dyn_cos_limit = sys_config.get("DYNAMIC_ALPHA_LIMIT", 0.75) # 자율 코사인 합격선
            dyn_ml_cutoff = sys_config.get("DYNAMIC_ML_BOX_CUTOFF", 0.50) # ML 박스 컷오프
            dyn_trap_limit = sys_config.get("DYNAMIC_TRAP_LIMIT", 0.75) # 자율 참사주 방어선
            dyn_dtw_limit = sys_config.get("DYNAMIC_DTW_LIMIT", 2.5)    # 자율 궤적 허용 거리

            # 순환매 예측 섹터 선취매는 컷오프를 15% 완화
            if is_rotation_prebuy:
                dyn_cos_limit *= 0.85
                dyn_ml_cutoff *= 0.85

            # 글로벌 스필오버 선취매는 컷오프를 10% 완화
            if is_spillover_prebuy:
                dyn_cos_limit *= 0.90
                dyn_ml_cutoff *= 0.90

            # 코사인 + 알파 보너스 하이브리드 (상한 1.0)
            max_alpha_cos_effective = min(1.0, max_alpha_cos + alpha_bonus_score)

            # 🛡️ 페일세이프 (내부수급과 궤적이 모두 자율 방어선을 넘었을 때만 기각)
            if max_trap_cos >= dyn_trap_limit and min_trap_dtw <= dyn_dtw_limit:
                if max_trap_cos > max_alpha_cos:
                    # 💡 [V53.2 데이터 기아 방지 픽스] return False 로 DB 저장을 막는 행위 원천 금지!
                    # 실매매에서만 거를 수 있도록 이름표(💀[기각/관찰용])만 달고 무조건 DB에 집어넣어 관제탑의 먹이로 줍니다.
                    sig_type = f"💀[기각/관찰용] {sig_type}"
                    track_tag = "(참사 방어막 터치 - 관찰 표본)"
            
            # 🚀 슈퍼 부스트 (코사인 + 알파 보너스 융합으로 합격 판정)
            # 💡 [100년 영속 진화 로직 적용: Cos-DTW Gate Decoupling]
            cutoff_passed = (max_alpha_cos_effective >= dyn_cos_limit) and (min_alpha_dtw <= dyn_dtw_limit)
            if cutoff_passed:
                sig_type += f" [🌟시계열 자율판독 대장주 (Cos:{max_alpha_cos_effective*100:.0f}%|DTW:{min_alpha_dtw:.1f})]"
                if is_rotation_prebuy:
                    sig_type += " [순환매 컷오프 특권 패스]"
                if is_spillover_prebuy:
                    sig_type += " [🌐스필오버 선취매]"
                if alpha_bonus_score > 0:
                    sig_type += " [🧬알파 융합 합격]"

            is_pass_cosine = cutoff_passed
            is_pass_ml_box = ("SUPERNOVA_MLBOX" in sig_type) or ("UNDERDOG_MLBOX" in sig_type) or ("MLBOX" in sig_type)
            if is_pass_cosine or is_pass_ml_box:
                spr = pre_sys_config.get("SYNTHETIC_PROVEN_RULES")
                if isinstance(spr, dict) and spr:
                    facts_f = facts if isinstance(facts, dict) else {}
                    try:
                        fd_cpv = float(facts_f.get("dyn_cpv", float("nan")))
                    except (TypeError, ValueError):
                        fd_cpv = float("nan")
                    try:
                        fd_ve = float(facts_f.get("v_energy", float("nan")))
                    except (TypeError, ValueError):
                        fd_ve = float("nan")
                    for _, rule in spr.items():
                        if not isinstance(rule, dict):
                            continue
                        try:
                            cmax = float(rule.get("condition_cpv_max", float("nan")))
                        except (TypeError, ValueError):
                            cmax = float("nan")
                        if not (np.isfinite(fd_cpv) and np.isfinite(cmax) and np.isfinite(fd_ve)):
                            continue
                        if fd_cpv < cmax and fd_cpv <= 0.4 and fd_ve >= 10.0:
                            sig_type += " [🌌극한가상훈련_생존DNA]"
                            synthetic_survival_buff = True
                            break

            # 안티 패턴(오답노트) 면역 체계: 유사도 0.85 이상이면 신규 진입 차단
            if incubator_match_name is None:
                anti_patterns = sys_config.get("ANTI_PATTERNS", [])
                if isinstance(anti_patterns, list) and anti_patterns:
                    cur_pattern_vec = np.array([cpv, tb, bbe / safe_vol, z_rs], dtype=float)
                    for ap in anti_patterns:
                        if not isinstance(ap, dict):
                            continue
                        anti_vec = np.array([
                            float(ap.get("cpv", 0.0)),
                            float(ap.get("tb", 0.0)),
                            float(ap.get("bbe", 0.0)),
                            float(ap.get("rs", 0.0))
                        ], dtype=float)
                        ap_cos = cosine_sim(cur_pattern_vec, anti_vec)
                        if ap_cos >= 0.85:
                            return False, "💀안티패턴(면역) 차단: 과거 치명적 실패 DNA와 고유사도(0.85+)로 진입 금지"

            # 👇👇 [들여쓰기 픽스 완료] 리스크 패리티 연산은 반드시 try 블록 안에 있어야 합니다 👇👇
            hist_df['prev_c'] = hist_df['Close'].shift(1)
            hist_df['tr'] = np.maximum(hist_df['High'] - hist_df['Low'], np.maximum(abs(hist_df['High'] - hist_df['prev_c']), abs(hist_df['Low'] - hist_df['prev_c'])))
            hist_df['atr'] = hist_df['tr'].ewm(span=14, adjust=False).mean()
            entry_atr = float(hist_df['atr'].iloc[-1])

            opt_sl_atr = sys_config.get(f"{market}_MASTER_S1_ATR_SL", 2.0)
            sl_price = ep - (opt_sl_atr * entry_atr)
            risk_distance = ep - sl_price

            account_size = sys_config.get("ACCOUNT_SIZE", 20000000)
            fixed_risk_pct = 0.02 
            try:
                from meta_governor_consumer import (
                    load_meta_state_resolved,
                    resolve_trading_kelly_base,
                )

                kelly_risk_pct = resolve_trading_kelly_base(
                    sys_config, load_meta_state_resolved()
                )
            except Exception:
                kelly_risk_pct = sys_config.get("DYNAMIC_KELLY_RISK", 0.01)
            w_s1 = float(sys_config.get("WEIGHT_S1", 1.0) or 1.0)
            w_s4 = float(sys_config.get("WEIGHT_S4", 1.0) or 1.0)
            if "S1" in sig_type or "SUPERNOVA" in sig_type:
                kelly_risk_pct *= w_s1
            if "S4" in sig_type or "눌림" in sig_type:
                kelly_risk_pct *= w_s4
            try:
                from meta_state_store import normalize_regime_key, resolve_config_regime_key

                _rk_m = normalize_regime_key(
                    load_meta_state_resolved().get("META_REGIME_KEY")
                )
                cur_regime = (
                    _rk_m
                    if _rk_m not in ("", "UNKNOWN")
                    else resolve_config_regime_key(sys_config)
                )
            except Exception:
                cur_regime = sys_config.get("CURRENT_REGIME_KEY", "UNKNOWN")

            # 💡 [100년 영속 진화 로직 적용: Namespace Thompson Kelly Sampler]
            # try_add 시점에 시그널 네임스페이스를 추론해 [NS]_BETA_PARAMS 기반으로 켈리 배율을 동적 샘플링한다.
            ns_prefix = f"{market}_MASTER_S1"
            if "SUPERNOVA" in sig_type:
                ns_prefix = f"{market}_SUPERNOVA_MASTER"
            else:
                if "S4" in sig_type:
                    ns_prefix = f"{market}_MASTER_S4"
                if "눌림" in sig_type:
                    ns_prefix = f"{market}_NULRIM_S4" if "S4" in sig_type else f"{market}_NULRIM_S1"
                if "5선" in sig_type:
                    ns_prefix = f"{market}_5EMA_S1"
            try:
                beta_pack = sys_config.get(f"{ns_prefix}_BETA_PARAMS", {})
                alpha = float(beta_pack.get("alpha", 0))
                beta = float(beta_pack.get("beta", 0))
                ts_sample = float(np.random.beta(alpha + 1.0, beta + 1.0))
                # 💡 [100년 영속 진화 로직 적용: Thompson Multiplier Re-Scaling]
                # 0.5(중립 승률)를 1.0배 기준점으로 재스케일링해 우수 전략의 비중 증폭을 허용
                ts_mult = float(np.clip(ts_sample / 0.5, 0.20, 1.80))
                kelly_risk_pct *= ts_mult
            except Exception:
                pass
            
            # 👇👇 [V105.0 자율 진화] 순환매 선취매 태깅 및 베팅 어드밴티지 로직 👇👇
            if is_rotation_prebuy:
                sig_type += " #순환매_선취매" # 장부 기록용 태그 박제
                # 관제탑이 주말 데스매치를 통해 우위를 증명(1.5배)했다면 켈리 비중 2배 뻥튀기
                if sys_config.get("ROTATION_ADVANTAGE_ACTIVE", False):
                    kelly_risk_pct *= 2.0 

            # 글로벌 스필오버 선취매 연동: KR에서 논리 섹터 연관 시 켈리 1.5배
            if is_spillover_prebuy:
                kelly_risk_pct *= 1.5
            if synthetic_survival_buff:
                kelly_risk_pct *= 1.5
            # 👆👆 [수정 완료] 👆👆

            # 인큐베이터 섀도우 모드: 시드 영향 완전 차단 및 독립 시그널 태깅
            if incubator_match_name is not None:
                sig_type = f"[INCUBATOR_{incubator_match_name}]"
                invest_amount = 0
                shares = 0
                sim_kelly_invest = 0

            if risk_distance > 0:
                # 👇👇 [V102.8 버그 픽스] 그룹별 실시간 복리 시드 & 예수금(가용 자산) 브레이크 엔진 👇👇
                import re
                
                # 1. 꼬리표와 헤더를 떼어내고 '본질적인 시그널(그룹) 이름'만 완벽히 추출
                # (예: "💀[기각] [SUPERNOVA] RANK_A [🔥주도주]" ➔ "RANK_A")
                clean_sig = sig_type.replace('💀[기각/관찰용] ', '')
                clean_sig = re.sub(r'^\[.*?\]\s*', '', clean_sig)
                core_group_name = clean_sig.split(' [')[0]

                # MetaGovernor: system_config 와 분리된 meta_governor_state.json 기반 Kelly 병합
                # (곱셈: META_GLOBAL_KELLY_MULT × META_NS_KELLY_MULT × META_GROUP_KELLY_MULT + kelly_cap/floor clamp)
                _meta_state = load_meta_state_resolved()
                kelly_risk_pct = apply_meta_kelly_merge(
                    kelly_risk_pct,
                    _meta_state,
                    ns_prefix=ns_prefix,
                    core_group_name=core_group_name,
                    sys_config=sys_config,
                    entry_facts=facts if isinstance(facts, dict) else {},
                    sector_mapped=str(sector),
                )
                
                # 2. 해당 그룹(로직)이 지금까지 벌어들인 누적 수익금 계산 (실현 손익)
                cursor.execute("SELECT SUM((sim_kelly_invest * final_ret) / 100.0) FROM forward_trades WHERE status LIKE 'CLOSED%' AND sig_type LIKE ?", (f"%{core_group_name}%",))
                realized_pnl = cursor.fetchone()[0]
                if realized_pnl is None: realized_pnl = 0.0
                
                # 💡 [독립 복리 시드] 기본 2,000만 원 + 이 그룹이 스스로 번 돈
                group_current_seed = account_size + realized_pnl

                # [AUM 스케일링 브레이크] 시드가 커진 그룹의 소형주 슬리피지 진입 차단
                marcap_eok = float(facts.get('marcap_eok', 0) or 0)
                if market == 'KR' and group_current_seed > 50000000 and marcap_eok < 1000:
                    return False, (
                        f"🛑 시드 비대화로 인한 소형주 슬리피지 방어: "
                        f"[{core_group_name}] 시드 {group_current_seed:,.0f}원 / 시총 {marcap_eok:,.0f}억"
                    )
                
                # 3. 해당 그룹이 현재 시장에 묶어둔 투자금 계산 (미실현 락업)
                cursor.execute("SELECT SUM(sim_kelly_invest) FROM forward_trades WHERE status = 'OPEN' AND sig_type LIKE ?", (f"%{core_group_name}%",))
                locked_cash = cursor.fetchone()[0]
                if locked_cash is None: locked_cash = 0.0
                
                # 💡 [잔여 현금] 예수금 브레이크
                available_cash = group_current_seed - locked_cash
                
                if available_cash <= 0:
                    # 예수금 부족 시 DB 저장 취소 (가짜 우상향 및 신용/미수 원천 차단)
                    return False, f"💸 예수금 부족: [{core_group_name}] 엔진의 가용 자산이 없습니다 (시드: {group_current_seed:,.0f}원 / 묶인돈: {locked_cash:,.0f}원)"

                # 4. 베팅 한도 설정 (그룹 시드의 최대 비중 vs 남은 현금 중 작은 것; MetaGovernor 가 META_MAX_POSITION_PCT 로 추가 캡)
                max_invest_limit = min(
                    group_current_seed * effective_max_position_pct(sys_config, _meta_state),
                    available_cash,
                )
                
                # 5. 실전 API로 넘어갈 '진짜 매수 수량(shares)' 산출 (미국장 환율 보정)
                exch_rate = 1350.0 if market == 'US' else 1.0
                calc_ep = ep * exch_rate
                calc_risk_dist = risk_distance * exch_rate
                
                raw_shares = max(1, int((group_current_seed * kelly_risk_pct) / calc_risk_dist))
                raw_invest = raw_shares * calc_ep
                
                # 🛡️ 켈리 베팅 안전장치 및 예수금 한도 캡(Cap) 가동
                if raw_invest > max_invest_limit:
                    sim_kelly_invest = max_invest_limit
                    shares = int(max_invest_limit / ep)
                else:
                    sim_kelly_invest = raw_invest
                    shares = raw_shares
                
                # V39.0 딥 다이브 비교를 위한 고정 2% 투입금도 동일한 그룹 시드 기반으로 보정
                raw_fixed_shares = max(1, int((group_current_seed * fixed_risk_pct) / risk_distance))
                raw_fixed_invest = raw_fixed_shares * ep
                
                if raw_fixed_invest > max_invest_limit:
                    invest_amount = max_invest_limit
                else:
                    invest_amount = raw_fixed_invest
                # 👆👆 [패치 완료] 👆👆
                if incubator_match_name is not None:
                    invest_amount, shares, sim_kelly_invest = 0, 0, 0
            else:
                shares, invest_amount, sim_kelly_invest = 0, 0, 0

    except Exception as e:
        print(f"하이브리드 벡터 매칭 에러: {e}")
    # 👆👆 [try 블록 완전 종료] 👆👆

    # 👇👇 [추가] V24.0 진입 시점의 시장 폭(Breadth) 실시간 측정 👇👇
    cur_breadth = 1.0
    try:
        b_df = yf.download("RSP SPY", period="5d", interval="1d", progress=False)
        if not b_df.empty:
            cur_breadth = (b_df['Close']['RSP'].iloc[-1] / b_df['Close']['SPY'].iloc[-1]) / \
                          (b_df['Close']['RSP'].mean() / b_df['Close']['SPY'].mean())
    except: pass
    # 👆👆 [추가 끝] 👆👆

    ep = ep * 1.005
    _facts = facts if isinstance(facts, dict) else {}
    # 3. 가상 매매 장부에 팩트 데이터와 함께 기록 (V38.0 자금 통제 변수 추가)
    insert_row = {
        "entry_date": today_str,
        "market": market,
        "code": code_str,
        "name": name,
        "sector": sector,
        "sig_type": sig_type,
        "tier": tier_label,
        "total_score": score,
        "dyn_rs": _facts.get("dyn_rs", 0),
        "dyn_cpv": _facts.get("dyn_cpv", 0),
        "dyn_tb": facts.get("dyn_tb", 0),
        "is_death_combo": int(_facts.get("is_death_combo") or 0),
        "is_tenbagger": int(_facts.get("is_tenbagger") or 0),
        "entry_price": ep,
        "v_cpv": _facts.get("v_cpv", 0),
        "v_yang": _facts.get("v_yang", 0),
        "v_energy": _facts.get("v_energy", 0),
        "v_rs": _facts.get("v_rs", 0),
        "max_high": ep,
        "min_low": ep,
        "market_breadth": round(cur_breadth, 3),
        "entry_breadth": round(cur_breadth, 3),
        "entry_cos_score": round(min(1.0, max_alpha_cos + alpha_bonus_score), 3),
        "entry_dtw_score": round(min_alpha_dtw, 3),
        "entry_atr": round(entry_atr, 4),
        "invest_amount": invest_amount,
        "shares": shares,
        "sim_kelly_invest": sim_kelly_invest,
        "entry_regime": cur_regime,
    }
    try:
        _insert_forward_trade_row(cursor, insert_row)
        if satellite_tags is not None:
            try:
                import shadow_tracking

                logged_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                shadow_tracking.insert_virtual_trade_row(
                    cursor,
                    market,
                    code_str,
                    name,
                    ep,
                    sig_type,
                    str(satellite_tags),
                    logged_at,
                )
            except Exception as shadow_exc:
                print(
                    f"⚠️ shadow_tracking INSERT 스킵 {code_str}: "
                    f"{type(shadow_exc).__name__}: {shadow_exc}"
                )
        conn.commit()
        return True, f"🎯 {tier_label} 가상매매 편입 성공: {name} ({score:.1f}점)"
    except sqlite3.Error as db_exc:
        conn.rollback()
        err = f"DB_INSERT:{type(db_exc).__name__}:{db_exc}"
        print(f"⚠️ forward_trades INSERT 실패 {code_str}: {err}")
        return False, err
    finally:
        conn.close()

