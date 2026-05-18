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
from forward_report_scalar import prepare_forward_trades_df, scalar_float
from forward_score_bucket_deep_dive import (
    ForwardScoreBucketDeepDive,
    build_universal_dna_block,
    format_bucket_blocks_telegram_html,
    format_tier_champion_summary_html,
    format_universal_dna_html,
)
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
    """무거운 집계 전용: 스냅샷이 있으면 읽기 복제본(uri=ro), 없으면 메인 DB."""
    uri_path = market_db_read_path().replace("\\", "/")
    return sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, check_same_thread=False)


def _compress_sector_theme(raw: object) -> str:
    """긴 섹터/테마 문자열 → 퀀트 태그 (예: [헬스케어/대마])."""
    s = str(raw or "").strip()
    if not s or s.lower() in ("nan", "none", "미상", "null"):
        return "미분류"
    for junk in ("유망", "포착", "테마", "섹터"):
        s = s.replace(junk, "")
    for sep in (",", "·", "/", "|", " — ", " - "):
        if sep in s:
            s = s.split(sep)[0].strip()
    s = re.sub(r"\s+", "", s)
    if len(s) > 20:
        s = s[:18] + "…"
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
    df: pd.DataFrame, league: str, logic: str, *, window_days: int
) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """TOP3 종목이 아닌 로직 전체 수익 청산 코호트로 피처 스캔."""
    cutoff = _colosseum_kst_cutoff(window_days)
    q = df.loc[
        (df["league"] == league)
        & (df["logic"] == logic)
        & (df["exit_date"].astype(str) >= cutoff)
    ].copy()
    if q.empty:
        return pd.DataFrame(), pd.DataFrame(), cutoff, ""
    ed = q["exit_date"].astype(str).str[:10]
    anchor = str(ed.max()) if not ed.empty else ""
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
    if len(baseline) < 5:
        baseline = q.loc[~q.index.isin(ace.index)].copy()
    return ace, baseline, cutoff, anchor


def _strategy_colosseum_brief(db_path=None):
    """
    가상매매 `forward_trades` 청산 건을 로직명(sig_type 코어)별로 집계해 텔레그램용 랭킹 문자열 생성.
    스냅샷 고착 방지: colosseum_db_path_for_report() + exit_date 롤링 윈도우.
    """
    if db_path is None:
        db_path = colosseum_db_path_for_report()
    try:
        sys_cfg = load_system_config()
    except Exception:
        sys_cfg = {}
    window_days = colosseum_window_days(sys_cfg)
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
    ed_all = df["exit_date"].astype(str).str[:10]
    data_anchor = str(ed_all.max()) if not ed_all.empty else ""
    lines = [
        '\n⚔️ <b>[전략 콜로세움: 리그별 랭킹]</b>\n',
        f"<i>DB 실시간 · KST 앵커 <b>{_esc(data_anchor)}</b> · 롤링 <b>{window_days}</b>일 · "
        f"청산 <b>{len(df)}</b>건</i>\n",
    ]
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
            return [], "섹터 미기록", pd.DataFrame(), "", 0
        ace_df, base_df, _cut, anchor = _ace_analysis_frames(
            df, league, logic, window_days=window_days
        )
        q_disp = df.loc[(df["league"] == league) & (df["logic"] == logic)].copy()
        q_disp = q_disp.sort_values("final_ret", ascending=False).head(3)
        items = []
        sectors_raw: list = []
        for _, rr in q_disp.iterrows():
            cd = str(rr.get("code", "")).strip()
            nm = str(rr.get("name", "")).strip()
            label = cd if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", cd) else (
                nm if nm and nm.lower() != "nan" else cd
            )
            if len(str(label)) > 12:
                label = str(label)[:10] + "…"
            items.append(f"<b>{_esc(label)}</b>({float(rr['final_ret']):+.1f}%)")
            raw_sec = str(rr.get("sector", "")).strip()
            if raw_sec and raw_sec.lower() != "nan":
                sectors_raw.append(raw_sec)
        if not sectors_raw and not ace_df.empty and "sector" in ace_df.columns:
            sectors_raw = ace_df["sector"].astype(str).tolist()
        sec_summary = _sector_convergence_summary(sectors_raw)
        n_feat = len(discover_numeric_feature_columns(ace_df, extra_forward_trade_columns_for_report()))
        return items, sec_summary, ace_df, anchor, n_feat

    kr_carry, kr_sec, kr_ace, kr_anchor, kr_nfeat = _top3_display_and_ace("KR", kr_top_logic)
    us_carry, us_sec, us_ace, us_anchor, us_nfeat = _top3_display_and_ace("US", us_top_logic)

    lines.append("\n🔍 <b>[에이스 로직 심층 부검]</b>\n")
    if kr_top_logic:
        lines.append(
            f"📌 KR <b>{_esc(kr_top_logic)}</b> TOP3: "
            + (", ".join(kr_carry) if kr_carry else "표본 부족")
            + f" · 섹터 {kr_sec}\n"
        )
    if us_top_logic:
        lines.append(
            f"📌 US <b>{_esc(us_top_logic)}</b> TOP3: "
            + (", ".join(us_carry) if us_carry else "표본 부족")
            + f" · 섹터 {us_sec}\n"
        )

    dynamic_kr = False
    dynamic_us = False
    try:
        sys_cfg = load_system_config()
        meta_st = load_meta_state_resolved()
        analyzer = ReportFeatureAnalyzer(sys_config=sys_cfg, meta=meta_st)
        if kr_top_logic and not kr_ace.empty and len(kr_ace) >= 3:
            _, baseline_kr, _, _ = _ace_analysis_frames(
                df, "KR", kr_top_logic, window_days=window_days
            )
            part_lines, ok_kr = analyzer.build_ace_deep_dive_lines(
                league="KR",
                logic_label=_esc(kr_top_logic),
                ace_df=kr_ace,
                baseline_df=baseline_kr,
                spillover_sector=kr_sec,
                data_anchor=kr_anchor or data_anchor,
                window_days=window_days,
                n_features_scanned=kr_nfeat,
            )
            if ok_kr:
                for pl in part_lines:
                    lines.append(pl)
                dynamic_kr = True
        if us_top_logic and not us_ace.empty and len(us_ace) >= 3:
            _, baseline_us, _, _ = _ace_analysis_frames(
                df, "US", us_top_logic, window_days=window_days
            )
            part_lines, ok_us = analyzer.build_ace_deep_dive_lines(
                league="US",
                logic_label=_esc(us_top_logic),
                ace_df=us_ace,
                baseline_df=baseline_us,
                spillover_sector=us_sec,
                data_anchor=us_anchor or data_anchor,
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

    if kr_carry and not dynamic_kr:
        lines.append(
            f"💡 KR 요약: 섹터 {kr_sec} · 동적 피처 스캔 표본 부족(청산·수익 거래 확대 후 재산출).\n"
        )
    if us_carry and not dynamic_us:
        lines.append(
            f"💡 US 요약: 섹터 {us_sec} · 동적 피처 스캔 표본 부족(청산·수익 거래 확대 후 재산출).\n"
        )

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
        cur = conn.execute(
            f"SELECT id FROM forward_trades WHERE {status_open} AND {qty_zero} AND {invest_zero}"
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
        init_forward_db()
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        try:
            conn.execute(
                "UPDATE forward_trades SET tier_effective = NULL WHERE market = ? AND tier = '80점대'",
                (market,),
            )
            conn.commit()
        finally:
            conn.close()
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
    try:
        conn.execute(
            "UPDATE forward_trades SET tier_effective = ? WHERE market = ? AND tier = '80점대'",
            (db_val, market),
        )
        conn.commit()
        row = conn.execute(
            "SELECT tier_effective FROM forward_trades WHERE market = ? AND tier = '80점대' LIMIT 1",
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
            hist_df = fdr.DataReader(code_str, st_dt).tail(300) if market == 'KR' else yf.download(code_str, start=st_dt, progress=False).tail(300)
            if isinstance(hist_df.columns, pd.MultiIndex): hist_df.columns = hist_df.columns.droplevel(1)
            hist_df = hist_df.reset_index()
            if 'index' in hist_df.columns: hist_df.rename(columns={'index': 'Date'}, inplace=True)

        try:
            # 벤치마크 지수 테이블 역시 안전하게 쌍따옴표 처리
            idx_df = pd.read_sql(f'SELECT * FROM "{idx_table}" ORDER BY Date DESC LIMIT 300', conn).sort_values('Date')
        except:
            st_dt = (datetime.now() - timedelta(days=450)).strftime('%Y-%m-%d')
            idx_tk = '229200' if market == 'KR' else 'SPY'
            idx_df = fdr.DataReader(idx_tk, st_dt).tail(300) if market == 'KR' else yf.download(idx_tk, start=st_dt, progress=False).tail(300)
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
            kelly_risk_pct = sys_config.get("DYNAMIC_KELLY_RISK", 0.01)
            w_s1 = float(sys_config.get("WEIGHT_S1", 1.0) or 1.0)
            w_s4 = float(sys_config.get("WEIGHT_S4", 1.0) or 1.0)
            if "S1" in sig_type or "SUPERNOVA" in sig_type:
                kelly_risk_pct *= w_s1
            if "S4" in sig_type or "눌림" in sig_type:
                kelly_risk_pct *= w_s4
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
        "dyn_rs": facts.get("dyn_rs", 0),
        "dyn_cpv": facts.get("dyn_cpv", 0),
        "dyn_tb": facts.get("dyn_tb", 0),
        "entry_price": ep,
        "v_cpv": facts.get("v_cpv", 0),
        "v_yang": facts.get("v_yang", 0),
        "v_energy": facts.get("v_energy", 0),
        "v_rs": facts.get("v_rs", 0),
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

# ==========================================
# 2. 매일 종가 흐름 추적 및 청산 엔진 (DB 기반)
# ==========================================
def track_daily_positions(market):
    init_forward_db()
    # 💡 [V25.0] 긴 작업 시 다른 스레드가 대기할 수 있도록 60초 타임아웃 적용
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    # 현재 보유 중인 종목만 불러오기
    df_active = pd.read_sql("SELECT * FROM forward_trades WHERE market=? AND status='OPEN'", conn, params=(market,))
    if df_active.empty:
        conn.close()
        return

    print(f"\n🔍 [포워드 테스팅] {market} 시장 {len(df_active)}개 종목 추적 중...")
    sys_config = load_system_config()
    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)
    total_open_loss_amount = 0.0
    
    # 👇👇 [V102.3 버그 픽스] 주말 및 공휴일 유령 카운팅(Double Counting) 원천 차단 👇👇
    tz_mkt = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    today_mkt_str = datetime.now(tz_mkt).strftime('%Y-%m-%d')
    
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    idx_ticker = '069500' if market == 'KR' else 'SPY'
    
    try:
        idx_df = fdr.DataReader(idx_ticker, start_date) if market == 'KR' else yf.download(idx_ticker, start=start_date, progress=False)
        
        # 💡 핵심: 벤치마크 지수의 가장 최근 캔들 날짜가 '해당 국가의 오늘 날짜'와 일치하는지 팩트 체크
        latest_candle_date = idx_df.index[-1].strftime('%Y-%m-%d')
        
        if latest_candle_date != today_mkt_str:
            print(f"💤 [{market}] 휴장일 감지 (최신캔들: {latest_candle_date} ≠ 오늘: {today_mkt_str}). 유령 카운팅 방어를 위해 추적을 건너뜁니다.")
            conn.close()
            return
            
        idx_close = idx_df['Close'] if market == 'KR' else idx_df['Close'].squeeze()
    except Exception as e: 
        print(f"⚠️ 벤치마크 로드 에러: {e}")
        idx_close = pd.Series(dtype=float)
    # 👆👆 [패치 완료] 👆👆

    cur_breadth_mkt = get_cached_market_breadth()
    breadth_collapse = cur_breadth_mkt < 0.97
    if breadth_collapse:
        print(
            f"🛡️ [포워드] 시장 폭 붕괴 연동 (breadth={cur_breadth_mkt:.3f} < 0.97): "
            f"기보유 청산 — MAE 손절·타임스탑 0.5배 비상 조임"
        )

    for _, r in df_active.iterrows():
        code = r['code']
        ep = r['entry_price']
        
        try:
            if market == 'US':
                import time, random
                time.sleep(random.uniform(0.3, 0.7)) # 무호흡 연사로 인한 IP 차단 완벽 방어
            df = fdr.DataReader(code, start_date) if market == 'KR' else yf.download(code, start=start_date, progress=False)
            
            # 💡 [픽스 1] yfinance MultiIndex 에러 완벽 대응 (미국장 0승 0패 마비 해결)
            if market == 'US' and isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
            if df.empty or len(df) < 20: 
                # 💡 [픽스 2] 거래정지 좀비 종목 무한 누적 방지 (30일 경과 시 강제 사형)
                try:
                    entry_dt = datetime.strptime(r['entry_date'][:10], '%Y-%m-%d')
                    if (datetime.now() - entry_dt).days > 30:
                        conn.execute("UPDATE forward_trades SET status='CLOSED_LOSS', final_ret=-15.0, exit_reason='장기 거래정지/상폐 강제청산' WHERE id=?", (r['id'],))
                except: pass
                continue
                
            c, o, h, l, v = float(df['Close'].iloc[-1]), float(df['Open'].iloc[-1]), float(df['High'].iloc[-1]), float(df['Low'].iloc[-1]), float(df['Volume'].iloc[-1])
            
            # 장중 수익률 3총사: 이후 모든 판독 로직보다 먼저 계산해 NameError 방지
            current_ret_pct = ((c - ep) / ep) * 100        # 종가 기준 수익률
            low_ret_pct = ((l - ep) / ep) * 100            # 장중 최저 수익률 (손절 터치 감시용)
            high_ret_pct = ((h - ep) / ep) * 100           # 장중 최고 수익률 (익절 터치 감시용)

            # 계좌 통합 서킷 브레이커용 당일 보유 손실 총합(손실만 누적)
            position_notional = float(r.get('sim_kelly_invest', 0) or 0)
            if position_notional <= 0:
                fallback_notional = float(r.get('invest_amount', 0) or 0)
                position_notional = fallback_notional if fallback_notional > 0 else float(ep)
            position_pnl = position_notional * (current_ret_pct / 100.0)
            if position_pnl < 0:
                total_open_loss_amount += position_pnl
            
            new_max = max(r['max_high'], h)
            new_min = min(r['min_low'], l)
            new_bars = r['bars_held'] + 1
            new_up_vol = r['up_vol_sum'] + (v if c > o else 0)
            new_down_vol = r['down_vol_sum'] + (v if c < o else 0)

            # =================================================================
            # 👑 [3차원 청산 최적화 엔진 가동] MFE/MAE, ATR, Time Stop 연산
            # =================================================================
            # 1. 14일 ATR(변동성) 실시간 연산
            df['prev_c'] = df['Close'].shift(1)
            df['tr'] = np.maximum(df['High'] - df['Low'], np.maximum(abs(df['High'] - df['prev_c']), abs(df['Low'] - df['prev_c'])))
            df['atr'] = df['tr'].ewm(span=14, adjust=False).mean()
            cur_atr = float(df['atr'].iloc[-1])
            
            # 진입 시점의 ATR이 DB에 없다면 현재 ATR로 팩트 보정 후 저장
            entry_atr = r.get('entry_atr', 0.0)
            if entry_atr == 0.0 or pd.isna(entry_atr):
                entry_atr = cur_atr
                conn.execute("UPDATE forward_trades SET entry_atr=? WHERE id=?", (entry_atr, r['id']))

            # 2. 기술적(TECH) 지표 연산 (기존 ZLEMA 및 단기데드)
            df['ema10'] = df['Close'].ewm(span=10, adjust=False).mean()
            df['ema20'] = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema1 = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])
            
            is_tech_exit = (c < cur_zlema) or (float(df['ema10'].iloc[-1]) < float(df['ema20'].iloc[-1]) and float(df['ema10'].iloc[-2]) >= float(df['ema20'].iloc[-2]))

            # 3. 🎯 관제탑 네임스페이스 매핑 및 JSON 지시사항 수신
            sys_config = load_system_config()
            active_mode = sys_config.get("ACTIVE_EXIT_MODE", "HYBRID")
            
            # 👇👇 [수정] 초신성(SUPERNOVA) 전용 독립 네임스페이스 분기 추가 👇👇
            ns_prefix = f"{market}_MASTER_S1" # 기본값
            
            if "SUPERNOVA" in r['sig_type']:
                # 초신성은 오리지널과 완전히 분리된 전용 파라미터 방을 사용합니다.
                ns_prefix = f"{market}_SUPERNOVA_MASTER"
            else:
                # 기존 오리지널 로직 분류 유지
                if "S4" in r['sig_type']: ns_prefix = f"{market}_MASTER_S4"
                if "눌림" in r['sig_type']: ns_prefix = f"{market}_NULRIM_S4" if "S4" in r['sig_type'] else f"{market}_NULRIM_S1" 
                if "5선" in r['sig_type']: ns_prefix = f"{market}_5EMA_S1" 
            # 👆👆 [수정 끝] 👆👆
            
            opt_time_stop = sys_config.get(f"{ns_prefix}_TIME_STOP", 10)
            opt_sl_atr    = sys_config.get(f"{ns_prefix}_ATR_SL", 2.0)
            if breadth_collapse:
                opt_time_stop = max(1, int(round(float(opt_time_stop) * 0.5)))
            
            # 수학적 손절가(SL) 산출: 진입가 - (관제탑 승수 * 진입변동성)
            sl_price = ep - (opt_sl_atr * entry_atr)

            # 4. ⚔️ 청산 아레나: MFE/MAE 및 관제탑 모드에 따른 수학적 사형 집행
            do_exit, exit_rsn, actual_exit_type = False, "", "HOLD"
            
            # 💡 [V51.0 핵심] 내 전략(Namespace) 방에 할당된 독립 파라미터 뇌(Brain) 꺼내오기
            ns_live_params = sys_config.get(f"{ns_prefix}_LIVE_PARAMS", sys_config)
            
            # 💡 [보강] 종목의 출신 성분(STANDARD vs SUPERNOVA)에 맞는 파라미터 팩 로드
            is_sn = "[SUPERNOVA]" in r['sig_type']
            prefix = ns_prefix # 기본값 (KR_MASTER_S1 등)

            abc_sets = {
                'live_a': ns_live_params,
                'cand_b': sys_config.get(f"{prefix}_CANDIDATE_PARAMS", {}),
                'champ_c': sys_config.get(f"{prefix}_CHAMPION_PARAMS", {})
            }

            # 모든 평행우주(A, B, C)에 대해 장중 저가(Low) 기준으로 손절 여부 판독
            for key, params in abc_sets.items():
                if not params: continue
                sl_limit = float(params.get("DYNAMIC_MAE_SL", -3.5))
                if breadth_collapse:
                    sl_limit *= 0.5
                
                # 장중 저가가 손절선을 건드렸다면 해당 평행우주는 'CLOSED_LOSS'
                if low_ret_pct <= sl_limit:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=?, {key}_status=? WHERE id=?", (sl_limit, "CLOSED_LOSS", r['id']))
                else:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # [V17.0 청산 평행우주 대결 (STAT vs TECH)]
            # 💡 [팩트] 관제탑이 내 전략방(ns_prefix) 맞춤형으로 깎아둔 실전 한계점 로드
            dyn_mae_sl = float(ns_live_params.get("DYNAMIC_MAE_SL", -3.5))
            if breadth_collapse:
                dyn_mae_sl *= 0.5
            dyn_mfe_tp = ns_live_params.get("DYNAMIC_MFE_TP", 10.0)
            od_hurdle = float(sys_config.get("DYNAMIC_OD_HURDLE", 20.0))
            is_overdrive_on = float(r.get('v_energy', 0) or 0) >= od_hurdle
            if is_overdrive_on:
                dyn_mfe_tp *= 1.10

            if r.get('sim_stat_status', 'OPEN') == 'OPEN':
                if low_ret_pct <= dyn_mae_sl: # 장중 손절 터치
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif high_ret_pct >= dyn_mfe_tp: # 장중 익절 터치
                    if c >= l + (h - l) * 0.7:
                        conn.execute("UPDATE forward_trades SET sim_stat_ret=? WHERE id=?", (current_ret_pct, r['id']))
                    else:
                        conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=? WHERE id=?", (current_ret_pct, r['id']))

            if r.get('sim_tech_status', 'OPEN') == 'OPEN':
                if low_ret_pct <= dyn_mae_sl:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif is_tech_exit:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_WIN' WHERE id=?", (current_ret_pct, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # [V24.0 시장 폭 필터링 실험 존]
            if r.get('sim_breadth_status', 'OPEN') == 'OPEN':
                e_breadth = r.get('entry_breadth', 1.0)
                if pd.isna(e_breadth): e_breadth = 1.0
                
                if e_breadth < 0.97:
                    conn.execute("UPDATE forward_trades SET sim_breadth_status='FILTERED_OUT' WHERE id=?", (r['id'],))
                else:
                    if low_ret_pct <= dyn_mae_sl:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=?, sim_breadth_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                    elif high_ret_pct >= dyn_mfe_tp:
                        if c >= l + (h - l) * 0.7:
                            conn.execute("UPDATE forward_trades SET sim_breadth_ret=? WHERE id=?", (current_ret_pct, r['id']))
                        else:
                            conn.execute("UPDATE forward_trades SET sim_breadth_ret=?, sim_breadth_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                    else:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # 1순위: MFE/MAE 절대 한계점 도달 시 무조건 청산 
            actual_exit_price = c # 기본 청산가는 종가로 세팅
            
            # 💡 [핵심 교정] 종가가 아닌 '저가(l)'와 '고가(h)'로 실전과 똑같이 슬리피지 청산
            if low_ret_pct <= dyn_mae_sl:
                do_exit, exit_rsn, actual_exit_type = True, f"수학적 MAE 장중 이탈 칼손절 ({dyn_mae_sl}%)", "STAT_MAE"
                actual_exit_price = ep * (1 + (dyn_mae_sl / 100.0)) # 손절선에서 털린 가격
            elif high_ret_pct >= dyn_mfe_tp:
                if c >= l + (h - l) * 0.7:
                    pass
                else:
                    do_exit, exit_rsn, actual_exit_type = True, f"수학적 MFE 장중 도달 익절 ({dyn_mfe_tp}%)", "STAT_MFE"
                    actual_exit_price = ep * (1 + (dyn_mfe_tp / 100.0))
            
            # RL 프록시(Q-Value 근사): 2순위 타임스탑 직전에 홀딩 엣지가 높으면 opt_time_stop만 +2일 연장(1순위 MAE/MFE 불변)
            try:
                _ots = int(round(float(opt_time_stop)))
            except (TypeError, ValueError):
                _ots = 10
            opt_time_stop_effective = max(1, _ots)
            holding_edge_score = (current_ret_pct / max(1, int(new_bars))) * (float(r.get('v_energy') or 1) / 10.0)
            if holding_edge_score > 1.5:
                opt_time_stop_effective = opt_time_stop_effective + 2

            # 2순위: 한계점 내부에서 움직일 경우, 국면 모드에 따른 추세/시간 청산
            if not do_exit:
                if active_mode == "TECH":
                    if is_tech_exit: 
                        do_exit, exit_rsn, actual_exit_type = True, "기술적 추세 이탈 (ZLEMA/데드)", "TECH"
                elif active_mode == "STAT":
                    if new_bars >= opt_time_stop_effective and current_ret_pct < 3.0:
                        do_exit, exit_rsn, actual_exit_type = True, f"통계적 유통기한 만료 ({opt_time_stop_effective}일)", "STAT_TIME"
                    elif l <= sl_price: # 💡 c <= sl_price 가 아니라 장중 저가 l 로 변경
                        do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 장중 방어 손절", "STAT_ATR"
                        actual_exit_price = sl_price
                else: # HYBRID
                    if new_bars >= opt_time_stop_effective and current_ret_pct < 3.0:
                        do_exit, exit_rsn, actual_exit_type = True, f"하이브리드 타임스탑 ({opt_time_stop_effective}일)", "HYBRID_TIME"
                    elif l <= sl_price: # 💡 c <= sl_price 가 아니라 장중 저가 l 로 변경
                        do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 장중 방어 손절", "HYBRID_ATR"
                        actual_exit_price = sl_price
                    elif is_tech_exit: 
                        do_exit, exit_rsn, actual_exit_type = True, "하이브리드 추세 이탈 익절", "HYBRID_TECH"

            # 3순위: 장기 거래정지/좀비 종목 강제 청소 (유통기한 2배 초과 시 원금 회수 가정)
            if not do_exit and new_bars >= opt_time_stop_effective * 2:
                do_exit, exit_rsn, actual_exit_type = True, "장기 거래정지/좀비종목 강제청소", "ZOMBIE_FORCE_CLOSE"
                actual_exit_price = ep


            # 5. DB 업데이트 실행 (청산 시)
            if do_exit:
                # 💡 [핵심] 최종 수익률(ret)은 희망회로 종가(c)가 아니라 '실제 증권사가 던진 가격(actual_exit_price)' 기반으로 계산
                ret = round(((actual_exit_price - ep) / ep) * 100, 2)
                mfe = round(((new_max - ep) / ep) * 100, 2)
                
                tags = []
                if mfe >= 7.0 and new_bars <= 8: tags.append("#빠른슈팅_완벽")
                elif mfe >= 7.0 and new_bars > 8: tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                
                vol_ratio = new_up_vol / (new_down_vol + 1)
                if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")

                # 👇👇 [추가] 오리지널과 초신성의 흐름(Flow) 오토 추적 분리 👇👇
                if "SUPERNOVA" in r['sig_type']:
                    # 초신성 전용 광기/투매 추적 로직 (스케일이 다름)
                    if mfe >= 20.0: tags.append("#초신성_광기폭발_성공")
                    elif mfe >= 10.0: tags.append("#초신성_1차슈팅_완료")
                    elif mfe < 3.0: tags.append("#가짜초신성_수급불발")
                    
                    if vol_ratio >= 2.0: tags.append("#미친매수세_잔류")
                    elif vol_ratio < 0.6: tags.append("#세력_엑시트_투매출회")
                else:
                    # 기존 오리지널 로직 유지
                    if mfe >= 7.0 and new_bars <= 8: tags.append("#빠른슈팅_완벽")
                    elif mfe >= 7.0 and new_bars > 8: tags.append("#지연슈팅_수명연장")
                    elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                    
                    if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                    elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")
                # 👆👆 [추가 끝] 👆👆
                
                # 🧟 [핵심 추가] 언더독(0~60점대) 전용 정밀 부검 꼬리표 부착
                if float(r.get('total_score', 100)) <= 60.0:
                    _rs = float(r.get('dyn_rs', 0) or r.get('v_rs', 0))
                    _eng = float(r.get('v_energy', 0) or 0)
                    _cpv = float(r.get('dyn_cpv', 0) or r.get('v_cpv', 0))

                    if ret > 0 or mfe >= 10.0: # 수익으로 마감했거나 장중 10% 이상 대시세를 준 경우
                        if _rs < 0: tags.append("#저득점_역배열_반등성공")
                        elif _rs > 30: tags.append("#저득점_이격과다_추가폭발")
                        
                        if _eng > 15.0: tags.append("#저득점_수급깡패_성공")
                    else: # 손실 마감 (참사주)
                        if _cpv > 0.75: tags.append("#저득점_윗꼬리_참사")
                        elif vol_ratio < 0.6: tags.append("#저득점_투매_수급붕괴")

                flow_tags = " ".join(tags)
                tz_exit = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
                exit_date = datetime.now(tz_exit).strftime('%Y-%m-%d')
                
                # 💡 관제탑이 피드백을 위해 수집할 exit_type 완벽 로깅
                conn.execute('''
                    UPDATE forward_trades 
                    SET status=?, exit_date=?, exit_reason=?, flow_tags=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?, exit_type=?
                    WHERE id=?
                ''', ('CLOSED_WIN' if ret > 0 else 'CLOSED_LOSS', exit_date, exit_rsn, flow_tags, ret, mfe, new_max, new_min, new_bars, new_up_vol, new_down_vol, actual_exit_type, r['id']))
                
                icon = "🔥스마트청산" if ret > 0 else "🛡️방어손절"
                # 💡 [V15.1 픽스] 시그널 타입(sig_type) 명시 및 점수 소수점 첫째 자리 정리
                send_telegram_msg(f"🤖 [{market} 관제탑 제어] {icon}: {r['name']} ({r['sig_type']} | {round(r['total_score'], 1)}점)\n▪️ 수익: {ret}%\n▪️ 모드: {active_mode}\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}")
            else:
                # DB 업데이트 (유지)
                conn.execute('''
                    UPDATE forward_trades 
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?
                    WHERE id=?
                ''', (new_max, new_min, new_bars, new_up_vol, new_down_vol, r['id']))
                
        except Exception as e: pass

    conn.commit()
    conn.close()

    # 블랙스완 붕괴 감지 시 전역 서킷 브레이커 ON
    if base_seed > 0:
        loss_ratio = total_open_loss_amount / float(base_seed)
        if loss_ratio <= -0.05:
            latest_config = load_system_config()
            if latest_config.get("GLOBAL_CIRCUIT_BREAKER", "OFF") != "ON":
                latest_config["GLOBAL_CIRCUIT_BREAKER"] = "ON"
                save_system_config(latest_config)
                send_telegram_msg(
                    f"🚨 <b>[GLOBAL CIRCUIT BREAKER 발동]</b>\n"
                    f"시장: {market}\n"
                    f"당일 보유 손실 합계: {total_open_loss_amount:,.0f}원\n"
                    f"기준 시드: {base_seed:,.0f}원\n"
                    f"손실률: {loss_ratio*100:.2f}%\n"
                    f"조치: 신규 진입 전면 차단(현금 관망) 모드로 전환"
                )

def send_comprehensive_daily_report(
    *,
    refresh_sentiment: bool = True,
    refresh_sector_spillover: bool = True,
    refresh_meta_governor: bool = True,
):
    """[V104.1] 국가별 9분할 정밀 리포트 (순환매 및 스필오버 복원 완료)"""
    if refresh_meta_governor:
        try:
            from meta_state_store import rebuild_meta_state

            _meta_heal = rebuild_meta_state(force=False, refresh_regime=True)
            print(f"🛰️ [일일 통합 리포트] MetaGovernor·REGIME 동기 치유: {_meta_heal}")
        except Exception as _mg_e:
            print(f"⚠️ [일일 통합 리포트] MetaGovernor 치유 실패(리포트는 계속): {_mg_e}")

    if refresh_sector_spillover:
        try:
            from sector_spillover_refresh import refresh_sector_spillover_state

            _sec_out = refresh_sector_spillover_state(save=True)
            print(f"🔄 [일일 통합 리포트] 섹터·스필오버 선행 갱신: {_sec_out}")
        except Exception as _sec_e:
            print(f"⚠️ [일일 통합 리포트] 섹터·스필오버 갱신 실패(리포트는 계속): {_sec_e}")

    if refresh_sentiment:
        try:
            from sentiment_miner import run_sentiment_mining

            _sent_out = run_sentiment_mining()
            print(f"🧠 [일일 통합 리포트] 센티먼트 선행 갱신: {_sent_out}")
        except Exception as _sent_e:
            print(f"⚠️ [일일 통합 리포트] 센티먼트 선행 갱신 실패(리포트는 계속): {_sent_e}")

    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    sys_config = load_system_config()

    try:
        _nz = _reporter_cleanup_zombie_forward_trades()
        if _nz:
            print(f"🧹 [일일 통합 리포트] 좀비 OPEN 정리: {_nz}건")
    except Exception as _ez:
        print(f"⚠️ [일일 통합 리포트] 좀비 정리 스킵: {_ez}")

    # 둠스데이·블랙홀 첩보 (위성 브리핑과 시너지 판단에 공통 사용)
    _dd = sys_config.get("DOOMSDAY_DEFCON") or {}
    defcon_level = 5
    if isinstance(_dd, dict):
        try:
            defcon_level = int(_dd.get("level", 5))
        except (TypeError, ValueError):
            defcon_level = 5
    _bh = sys_config.get("BLACKHOLE_TOXIC_COUNT", 0)
    blackhole_count = 0
    if isinstance(_bh, dict):
        try:
            blackhole_count = int(_bh.get("count", 0) or 0)
        except (TypeError, ValueError):
            blackhole_count = 0
    else:
        try:
            blackhole_count = int(_bh or 0)
        except (TypeError, ValueError):
            blackhole_count = 0

    smart_money_count = 0
    toxic_count = 0

    # 🛰️ [신경망 통합] 위성 데이터 수집 로직
    satellite_brief = "\n🛰️ <b>[팩토리 위성망 통합 첩보]</b>\n"
    try:
        _radar = (sys_config.get('SMART_MONEY_RADAR') or {})
        smart_picks = _radar.get('picks', {}) if isinstance(_radar, dict) else {}
        if not isinstance(smart_picks, dict):
            smart_picks = {}
        smart_money_count = len(smart_picks)
        anti_n = len(collect_merged_antipattern_rules(sys_config))
        toxic_count = anti_n
        satellite_brief += f" ▪️ 🕵️ 스마트머니: {smart_money_count}개 종목 매집 포착\n"
        satellite_brief += f" ▪️ 💀 오답노트: {anti_n}개의 독성 패턴 방어 중\n"
    except Exception:
        satellite_brief += " ▪️ 🕵️ 스마트머니: (조회 실패)\n ▪️ 💀 오답노트: (조회 실패)\n"

    try:
        alt_db = os.path.join(os.path.dirname(DB_PATH), 'alt_data.sqlite')
        if os.path.exists(alt_db):
            conn_alt = sqlite3.connect(f"file:{alt_db}?mode=ro", uri=True, check_same_thread=False)
            try:
                row = conn_alt.execute(
                    "SELECT usd_krw, us_10y_yield, vix_index FROM macro_daily ORDER BY date DESC LIMIT 1"
                ).fetchone()
                if row:
                    satellite_brief += f" ▪️ 💹 매크로: 환율 {row[0]}원 / 국채 {row[1]}% / VIX {row[2]}\n"
            finally:
                conn_alt.close()
    except Exception:
        pass

    try:
        from news_data_paths import (
            assert_sentiment_fresh_for_report,
            format_sentiment_satellite_line,
            today_kst_str,
        )

        if refresh_sentiment and not assert_sentiment_fresh_for_report():
            print(
                f"⚠️ [일일 통합 리포트] 당일({today_kst_str()}) 센티먼트 미확인 — "
                "리포트에 데이터 없음/스냅샷 날짜로 표시"
            )
        satellite_brief += format_sentiment_satellite_line(hide_stale_keywords=True)
    except Exception:
        satellite_brief += " ▪️ 🧠 센티먼트: 데이터 없음\n"

    # 🧠 AI 관제탑 시너지 판단 엔진 (위성·거시 지표 종합)
    strategy_insight = "\n💡 <b>[AI 관제탑 전략 브리핑]</b>\n"
    if defcon_level <= 2:
        strategy_insight += (
            "🚨 <b>[폭풍 전야]</b> 거시경제(채권/원자재) 붕괴 시그널이 감지되었습니다. "
            "스나이퍼 신규 매수를 전면 중단하고, 현금 비중을 극대화하십시오.\n"
        )
    elif defcon_level >= 4 and smart_money_count >= 5:
        strategy_insight += (
            "🚀 <b>[골디락스 공격]</b> 거시경제가 안정적이며 세력 수급이 강합니다. "
            "공격적인 롱(Long) 포지션 베팅을 권장합니다.\n"
        )
    elif toxic_count >= 100 or blackhole_count >= 10:
        strategy_insight += (
            "🕳️ <b>[숏 타격 기회]</b> 시장 내부에 독성 참사주가 무더기로 쌓이고 있습니다. "
            "인버스(숏) 베팅을 통한 시장 중립(Market Neutral) 방어망을 가동하십시오.\n"
        )
    else:
        strategy_insight += (
            "⚖️ <b>[관망 및 선별]</b> 시장 방향성이 혼조세입니다. 스나이퍼의 타점 기준을 엄격하게 높이고, "
            "확실한 개별주 장세에만 짧게 대응하십시오.\n"
        )

    ranking_brief = ""
    try:
        ranking_brief = _strategy_colosseum_brief(colosseum_db_path_for_report())
    except Exception:
        ranking_brief = ""

    shadow_brief = ""
    try:
        shadow_brief = _shadow_performance_brief(sys_config)
    except Exception:
        shadow_brief = ""

    satellite_brief += strategy_insight
    if ranking_brief:
        satellite_brief += ranking_brief
    if shadow_brief:
        satellite_brief += shadow_brief
    satellite_brief += "--------------------------------------\n"

    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)
    try:
        meta_state_daily = load_meta_state_resolved()
    except Exception:
        meta_state_daily = {}

    for market in ['KR', 'US']:
        market_icon = "🇰🇷" if market == 'KR' else "🇺🇸"

        try:
            conn = _open_market_db_ro()

            # [사전 데이터 로드] — market 열 + code 정규화 (오태깅 US/KR 교정)
            df_all_raw = pd.read_sql("SELECT * FROM forward_trades", conn)
            df_all = _daily_report_trades_for_market(df_all_raw, market)
            _sig_s = df_all['sig_type'].astype(str)
            _real_only = ~_sig_s.str.contains('INCUBATOR', na=False)
            df_real = df_all.loc[_real_only].copy()
            df_closed = df_real[df_real['status'].str.contains('CLOSED', na=False)]
            _vm = _reporter_valid_holding_mask(df_real)
            df_open = df_real.loc[_vm].copy()
            
            # ---------------------------------------------------------
            # 📑 결과지 1: 거시 국면 & 국고 현황 (ReportStateBinder)
            # ---------------------------------------------------------
            block_mt = build_macro_treasury_block(
                meta=meta_state_daily,
                sys_config=sys_config,
                df_closed_real=df_closed,
                treasury_config_key=f"CENTRAL_TREASURY_{market}",
                ledger_zero_invest_fallback=400000.0,
            )
            lead_in = ""
            if market == "KR":
                lead_in = (
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    + f"📢 <b>[일일 통합 성과 리포트]</b>\n"
                    + satellite_brief
                )
            msg1 = format_macro_treasury_section_html(
                block_mt,
                display_label=market,
                market_icon=market_icon,
                today_str=today_str,
                lead_in_html=lead_in,
                currency_suffix="원",
                amount_decimals=0,
            )
            try:
                from mutant_pending_bridge import pending_rd_telegram_fragment

                msg1 += pending_rd_telegram_fragment(sys_config)
            except Exception:
                pass
            send_telegram_msg(msg1); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 2: 생존자 리더보드 (프로듀스 101)
            # ---------------------------------------------------------
            import re
            def get_core_group(sig):
                # 💡 모든 [태그]를 완벽히 제거하여 순수 로직명만 추출 (파편화 방지)
                clean_sig = re.sub(r'\[.*?\]', '', str(sig)).strip()
                return clean_sig if clean_sig else str(sig).replace('[', '').replace(']', '').strip()

            msg2 = f"{market_icon} <b>[2/9] 로직별 복리 생존 리더보드</b>\n"
            if not df_real.empty:
                df_all_copy = df_real.copy()
                df_all_copy['group'] = df_all_copy['sig_type'].apply(get_core_group)
                leaderboard = []
                for group in df_all_copy['group'].unique():
                    g_df = df_all_copy[df_all_copy['group'] == group]
                    g_closed = g_df[g_df['status'].str.contains('CLOSED', na=False)]
                    # 💡 과거 에러 데이터(투입금 0원)를 기본 40만원(2%)으로 보정하여 복리 누락 방어
                    valid_invest = g_closed['sim_kelly_invest'].replace(0, 400000)
                    pnl = (valid_invest * g_closed['final_ret'] / 100.0).sum()
                    wr = (len(g_closed[g_closed['final_ret'] > 0]) / len(g_closed)) * 100 if len(g_closed) > 0 else 0
                    total_closed = len(g_closed)
                    pf = (
                        g_closed[g_closed['final_ret'] > 0]['final_ret'].sum()
                        / (abs(g_closed[g_closed['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
                    ) if total_closed > 0 else 0
                    leaderboard.append({
                        'g': group,
                        'bal': base_seed + pnl,
                        'wr': wr,
                        'op': int(_reporter_valid_holding_mask(g_df).sum()),
                        'tot': total_closed,
                        'pf': pf,
                    })
                
                leaderboard = sorted(leaderboard, key=lambda x: x['bal'], reverse=True)
                for i, e in enumerate(leaderboard[:15]):
                    m = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏃"
                    if e['bal'] < base_seed * 0.8: m = "📉"
                    if e['bal'] < base_seed * 0.5: m = "💀"
                    msg2 += f"{m} <b>{e['g']}</b>: {e['bal']:,.0f}원\n"
                    msg2 += f"   ↳ 승률 {e['wr']:.0f}% (PF {e['pf']:.2f}) | 누적 {e['tot']}전 | 현재 {e['op']}종목 보유\n"
            else: msg2 += " ↳ 매매 데이터 없음\n"
            send_telegram_msg(msg2); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 3: 통합 계좌 대결 (켈리 vs 고정) — CapitalDeathmatchAnalyzer
            # ---------------------------------------------------------
            dm_analyzer = CapitalDeathmatchAnalyzer(
                reference_capital=float(base_seed),
                zero_invest_fallback=400000.0,
            )
            dm_block = dm_analyzer.analyze(df_closed)
            msg3 = DeathmatchNarrativeBuilder.to_telegram_html(
                market_icon=market_icon,
                block=dm_block,
                subtitle="(정규직 로직 한정)",
            )
            send_telegram_msg(msg3); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 4: 포트폴리오 다중화 (VIP 편대 + 투입>0 기준으로 집계·한도 경고 통일)
            # ---------------------------------------------------------
            _unified = _reporter_deploy_fleet_mask(df_open, market)
            n_vip_fleet = int(_unified.sum())
            n_legacy_open = int(len(df_open) - n_vip_fleet)

            if not df_open.empty and "sim_kelly_invest" in df_open.columns:
                _sk_open = pd.to_numeric(df_open["sim_kelly_invest"], errors="coerce").fillna(0.0)
            else:
                _sk_open = pd.Series(0.0, index=df_open.index)
            _sig_open = df_open["sig_type"].astype(str)
            trend_mask = _sig_open.str.contains("🔥주도주", na=False) & _unified
            recon_mask = _sig_open.str.contains("🛡️차기섹터", na=False) & _unified
            trend_fleet = int(trend_mask.sum())
            recon_fleet = int(recon_mask.sum())
            trend_invest = float(_sk_open[trend_mask].sum())
            recon_invest = float(_sk_open[recon_mask].sum())
            total_invest = trend_invest + recon_invest
            if total_invest > 0:
                trend_weight = (trend_invest / total_invest) * 100.0
                recon_weight = (recon_invest / total_invest) * 100.0
            else:
                trend_weight = 0.0
                recon_weight = 0.0

            msg4 = f"{market_icon} <b>[4/9] 섹터 포트폴리오 다중화 현황</b>\n"
            if n_vip_fleet > 20:
                msg4 += (
                    "🚨 <b>[시스템 경고]</b> VIP 편대(주도/차기 트랙 + 투입금>0) 기준 보유가 시장 한도(20기)를 초과했습니다. "
                    "과거 레거시·표기 불일치 데이터를 점검하십시오.\n\n"
                )
            if n_legacy_open > 0:
                msg4 += (
                    f"📎 <b>포지션 팩트:</b> 현재 유효 VIP 편대 <b>{n_vip_fleet}기</b> "
                    f"(기타 레거시 OPEN <b>{n_legacy_open}기</b> 별도 보관 중 — 투입 집계·한도 경고는 VIP 편대만 반영)\n\n"
                )
            msg4 += f"🎯 <b>투입 자본 시너지 팩트 체크</b> <i>(VIP 트랙 + 투입금 양수)</i>\n"
            msg4 += (
                f" ▪️ 🔥주도주 편대: {trend_fleet}기 "
                f"(투입금: {trend_invest:,.0f}원 | 비중: {trend_weight:.1f}%)\n"
            )
            msg4 += (
                f" ▪️ 🛡️차기섹터 정찰: {recon_fleet}기 "
                f"(투입금: {recon_invest:,.0f}원 | 비중: {recon_weight:.1f}%)\n\n"
            )
            msg4 += "🗣️ <b>[관제탑 동적 시선]</b>\n"
            if total_invest == 0:
                msg4 += "현재 시장에 투입된 자본이 없습니다. 완벽한 현금 관망 상태입니다.\n"
            elif trend_weight >= 70.0:
                msg4 += (
                    f"전체 투자금의 {trend_weight:.1f}%가 주도 섹터에 강력하게 집중(Synergy)되어 있습니다. "
                    "추세 추종 극대화 모드입니다.\n"
                )
            elif recon_weight >= 70.0:
                msg4 += (
                    f"기존 주도주의 수명이 꺾였다고 판단, 자본의 {recon_weight:.1f}%를 "
                    "차기 섹터 발굴(정찰)에 선제적으로 투입 중입니다.\n"
                )
            else:
                msg4 += "주도 테마 추종과 차기 섹터 발굴에 자본을 균형 있게 배분하여 리스크를 헷징하고 있습니다.\n"
            send_telegram_msg(msg4); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 5: 티어 및 데스콤보 검증
            # ---------------------------------------------------------
            msg5 = f"{market_icon} <b>[5/9] 티어 및 데스콤보 검증</b>\n"
            t1_df = df_closed[df_closed['tier'] == '80점대']
            dc_df = df_closed[df_closed['is_death_combo'] == 1]
            try:
                msg5 += _tier80_sync_effective_and_report_line(market, t1_df, sys_config)
            except Exception as _te:
                print(f"⚠️ [5/9] tier_effective 동기화/표시 예외: {_te}")
            if not dc_df.empty: msg5 += f"💀 데스콤보 승률: {(len(dc_df[dc_df['final_ret']>0])/len(dc_df))*100:.1f}% (필터 작동 중)\n"
            if t1_df.empty and dc_df.empty: msg5 += " ↳ 검증 표본 부족\n"
            send_telegram_msg(msg5); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 6: 4차원 DNA 정밀 부검 (ReportFeatureAnalyzer 승·패 대조)
            # ---------------------------------------------------------
            msg6 = f"{market_icon} <b>[6/9] 대박주/참사주 4차원 DNA 부검</b>\n"
            _ret6 = pd.to_numeric(df_closed["final_ret"], errors="coerce")
            winners = df_closed[_ret6 >= 5.0].head(50)
            losers = df_closed[_ret6 <= -3.0].head(50)
            n_closed_mkt = int(len(df_closed))
            if n_closed_mkt == 0:
                msg6 += (
                    f"<i>⚠️ {market} 청산 표본 0건 — DNA 대조 불가. "
                    "스캐너·진입 파이프라인을 확인하세요.</i>\n"
                    f"▪ 복구: <code>./factory.sh --scan-{market.lower()}</code> "
                    f"→ <code>./factory.sh --daily-{market.lower()}</code>\n"
                )
            elif winners.empty and losers.empty:
                msg6 += (
                    f"<i>⚠️ {market} 청산 {n_closed_mkt}건 중 대박(≥5%)/참사(≤-3%) 표본 없음 — "
                    "횡보·소폭 손익 구간. 매매 중단이 아니면 임계 미충족일 수 있습니다.</i>\n"
                )
            else:
                try:
                    dna_an = ReportFeatureAnalyzer(sys_config=sys_config, meta=meta_state_daily)
                    dna_lines, _dna_ok, _dna_ins = dna_an.build_winner_loser_dna_contrast(
                        winners_df=winners,
                        losers_df=losers,
                        top_n=2,
                        min_per_group=2,
                    )
                    msg6 += "".join(dna_lines)
                except Exception as _dna_e:
                    print(f"⚠️ [6/9] DNA 대조 예외: {_dna_e}")
                    msg6 += (
                        "<i>DNA 대조 분석을 일시 생략했습니다 (예외 또는 데이터 부족).</i>\n"
                    )
            send_telegram_msg(msg6); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 7: 섹터 순환매 궤적 및 스필오버
            # ---------------------------------------------------------
            msg7 = f"{market_icon} <b>[7/9] 섹터 순환매 궤적 및 스필오버</b>\n"
            rot_df = df_real[df_real['entry_date'] >= (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')]
            
            if not rot_df.empty:
                # 💡 [픽스] '유망'이 포함된 가짜 데이터를 걸러내고 진짜 섹터만 집계
                def get_real_sector(x):
                    valid_s = [str(s) for s in x if '유망' not in str(s) and '포착' not in str(s)]
                    return pd.Series(valid_s).mode()[0] if valid_s else None
                
                daily_dom = rot_df.groupby('entry_date')['sector'].agg(get_real_sector).dropna()
                streaks, transitions = {}, {}
                current_sec, current_streak = None, 0
                
                for date, sec in daily_dom.items():
                    if sec == current_sec: current_streak += 1
                    else:
                        if current_sec is not None:
                            if current_sec not in streaks: streaks[current_sec] = []
                            streaks[current_sec].append(current_streak)
                            # 💡 6글자 무식한 절사 폐지, 15글자까지 넉넉하게 출력
                            t_key = f"{current_sec[:15]}➔{sec[:15]}"
                            transitions[t_key] = transitions.get(t_key, 0) + 1
                        current_sec = sec
                        current_streak = 1
                if current_sec is not None:
                    if current_sec not in streaks: streaks[current_sec] = []
                    streaks[current_sec].append(current_streak)

                msg7 += f"🔥 <b>현재 주도 섹터:</b> {current_sec} ({current_streak}일째 체류 중)\n"
                # 👇👇 [추가된 모니터링 로직] 👇👇
                try:
                    from sector_spillover_refresh import resolve_predicted_sector_display

                    _pred = resolve_predicted_sector_display(sys_config, market)
                except Exception:
                    _pred = str(sys_config.get(f"PREDICTED_NEXT_SECTOR_{market}") or "데이터 없음")
                msg7 += f"🔮 <b>다음 예측 섹터:</b> {_pred}\n"
                msg7 += f"⚡ <b>베팅 어드밴티지:</b> {'🔥활성화(200%)' if sys_config.get('ROTATION_ADVANTAGE_ACTIVE') else '정상(100%)'}\n\n"
                
                msg7 += "▪️ <b>섹터별 자금 체류 시간(수명):</b>\n"
                for s, lengths in streaks.items():
                    msg7 += f" - {s[:15]}: 평균 {sum(lengths)/len(lengths):.1f}일\n"

                sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:2]
                if sorted_trans:
                    msg7 += "\n▪️ <b>빈번한 자금 이동 궤적:</b>\n"
                    for p, c in sorted_trans: msg7 += f" - {p} ({c}회 관측)\n"
            else:
                msg7 += " ↳ 순환매 데이터 부족\n"

            if market == 'KR':
                actual_spillover = _resolve_us_spillover_telegram_inner(sys_config)
                msg7 += f"\n🌐 <b>한미 스필오버 연동:</b> 🇺🇸 최근 고수익 주도 섹터 [{actual_spillover}] ➔ 🇰🇷 관련 섹터 선취매 우대 적용 중\n"
            send_telegram_msg(msg7); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 8: 메타 최적화 및 반감기 (MetaGovernor 레지스트리 SSOT)
            # ---------------------------------------------------------
            try:
                lc_block = build_lifecycle_report_block(
                    meta=meta_state_daily,
                    sys_config=sys_config,
                    now=datetime.now(tz_kr),
                )
                msg8 = format_lifecycle_section_html(
                    lc_block,
                    market_icon=market_icon,
                    today_str=today_str,
                )
            except Exception as ex:
                msg8 = (
                    f"{market_icon} <b>[8/9] 메타 최적화 및 알파 반감기</b>\n"
                    f"⚠️ 생애주기 스냅샷 생성 실패: {html_escape(str(ex), quote=False)}\n"
                )
            send_telegram_msg(msg8); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 9: 시스템 데스매치 결산
            # ---------------------------------------------------------
            from deathmatch_report import (
                build_nway_deathmatch,
                format_nway_deathmatch_telegram,
                maybe_apply_deathmatch_allocation,
            )

            dm = build_nway_deathmatch(df_closed, sys_config, market=market)
            maybe_apply_deathmatch_allocation(dm, sys_config)
            _dm_label = f"{market} 청산 전체 · N-Way 로직군"
            if not dm.arms and n_closed_mkt == 0:
                _dm_label += " (청산 0 — scan 후 재확인)"
            msg9 = format_nway_deathmatch_telegram(
                market_icon,
                dm,
                lookback_label=_dm_label,
            )
            send_telegram_msg(msg9); time.sleep(1)

            conn.close()
        except Exception as e:
            send_telegram_msg(f"⚠️ {market} 리포트 에러: {e}")

def send_group_practitioner_reports():
    """활성 시그널 그룹별 실무자 개별 일일 리포트를 발송한다."""
    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    sys_config = load_system_config()
    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)

    try:
        _nz = _reporter_cleanup_zombie_forward_trades()
        if _nz:
            print(f"🧹 [실무자 리포트] 좀비 OPEN 정리: {_nz}건")
    except Exception as _ez:
        print(f"⚠️ [실무자 리포트] 좀비 정리 스킵: {_ez}")

    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        df_all = pd.read_sql("SELECT * FROM forward_trades WHERE IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'", conn)
        conn.close()

        if df_all.empty:
            return

        import re
        def get_core_group(sig):
            clean_sig = re.sub(r'\[.*?\]', '', str(sig)).strip()
            return clean_sig if clean_sig else str(sig).replace('[', '').replace(']', '').strip()

        df_all = df_all.copy()
        df_all["market"] = df_all.apply(
            lambda r: _normalize_trade_market(r.get("code"), r.get("market")),
            axis=1,
        )
        df_all['group'] = df_all['sig_type'].apply(get_core_group)
        df_all['mkt_group'] = df_all['market'].astype(str) + "_" + df_all['group'].astype(str)
        # 💡 현재 OPEN 종목이 없어도, 최근 청산 내역이 있는 실무자는 모두 보고서에 소환
        recent_cutoff = (datetime.now(tz_kr) - timedelta(days=2)).strftime('%Y-%m-%d')
        _exit_cal = df_all['exit_date'].map(_exit_date_on_calendar) if 'exit_date' in df_all.columns else ""
        active_condition = (df_all['status'] == 'OPEN') | (_exit_cal >= recent_cutoff)
        df_active = df_all[active_condition]
        active_groups = sorted([g for g in df_active['mkt_group'].dropna().unique() if str(g).strip()])

        for mkt_group in active_groups:
            market, group = _parse_mkt_group_key(mkt_group)
            g_all = df_all[
                (df_all['mkt_group'] == mkt_group)
                & (df_all['market'].astype(str).str.upper() == market)
            ].copy()
            # 코드 기준 2차 격리 (DB market 오염 방어)
            if market == "KR":
                g_all = g_all[g_all['code'].astype(str).str.match(r'^\d{5,6}$', na=False)]
            else:
                g_all = g_all[~g_all['code'].astype(str).str.match(r'^\d{5,6}$', na=False)]

            tz_mkt = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
            mkt_today_str = datetime.now(tz_mkt).strftime('%Y-%m-%d')
            market_icon = "🇰🇷" if market == 'KR' else "🇺🇸"
            g_closed = g_all[g_all['status'].astype(str).str.contains('CLOSED', na=False)].copy()
            if 'exit_date' in g_closed.columns:
                g_closed['_exit_day'] = g_closed['exit_date'].map(_exit_date_on_calendar)
                g_today_closed = g_closed[g_closed['_exit_day'] == mkt_today_str].copy()
            else:
                g_today_closed = g_closed.iloc[0:0].copy()

            if 'final_ret' in g_today_closed.columns:
                g_today_closed['_ret_pct'] = g_today_closed['final_ret'].map(_safe_final_ret_pct)
            else:
                g_today_closed['_ret_pct'] = 0.0
            win_cnt, loss_cnt, flat_cnt = _win_loss_flat_counts(g_today_closed['_ret_pct'])

            if 'sim_kelly_invest' in g_closed.columns and 'final_ret' in g_closed.columns:
                valid_invest = pd.to_numeric(g_closed['sim_kelly_invest'], errors='coerce').fillna(400000).replace(0, 400000)
                ret_c = pd.to_numeric(g_closed['final_ret'], errors='coerce').fillna(0.0)
                cum_pnl = (valid_invest * ret_c / 100.0).sum()
            else:
                cum_pnl = 0.0
            compound_seed = base_seed + cum_pnl
            
            # 💡 [픽스 3] 유효 보유만 집계: OPEN/ACTIVE + 수량>0 (좀비 OPEN 제외)
            open_cnt = int(_reporter_valid_holding_mask(g_all).sum())

            n_today = int(len(g_today_closed))
            msg = f"{market_icon} <b>[{market} 실무자 리포트]</b> {group}\n"
            msg += f"📅 오늘 성적: <b>{win_cnt}승 {loss_cnt}패</b>"
            if flat_cnt:
                msg += f" · 무{flat_cnt}"
            msg += f" (청산 <b>{n_today}</b>건)\n"
            msg += f"💰 현재 누적 복리 시드: <b>{compound_seed:,.0f}원</b>\n"
            msg += f"📦 현재 보유 종목: <b>{open_cnt}개</b>\n"

            if not g_today_closed.empty:
                msg += "📌 오늘 청산 종목:\n"
                for _, row in g_today_closed.iterrows():
                    name = row.get('name', '-')
                    reason = _format_exit_reason_display(row.get('exit_reason'))
                    ret = float(row.get('_ret_pct', _safe_final_ret_pct(row.get('final_ret'))))
                    msg += f" - {name} ({ret:+.2f}%) / {reason}\n"
            else:
                msg += "📌 오늘 청산 종목: 없음\n"

            send_telegram_msg(msg)
            time.sleep(3.5)
    except Exception as e:
        send_telegram_msg(f"⚠️ 실무자 개별 리포트 발송 에러: {e}")
# ==========================================
# 4. [방향성 5,6,7번] 퀀트 딥 다이브 분석 엔진 (특징 추출 및 티어별 성적표)
# ==========================================
def _deep_dive_cross_market_isolation_footer(df: pd.DataFrame, market: str) -> str:
    """
    tier 절대값으로 KR/US를 직접 비교하지 않도록 텔레그램 해석 가이드 + 동일 시장 내 total_score Z-구간 요약.
    """
    lines = [
        "\n◽ <b>[KR/US 격리 · tier 해석]</b>",
        "• 동일 표기의 <b>tier</b>(예: 40점대)도 <b>시장(market)별 산출 경로</b>가 다릅니다. "
        "<b>KR과 US를 tier 절대값으로 직접 비교하지 마십시오.</b>",
    ]
    ts = pd.to_numeric(df.get("total_score"), errors="coerce").dropna()
    if len(ts) >= 10:
        mu = scalar_float(ts.mean())
        sd = scalar_float(ts.std(ddof=0), 1e-9) or 1e-9
        z = (ts - mu) / sd
        lo = int((z < -0.5).sum())
        mid = int(((z >= -0.5) & (z <= 0.5)).sum())
        hi = int((z > 0.5).sum())
        lines.append(
            f"• <b>{market}장·본 윈도우 내부 total_score</b>: μ={mu:.1f} σ={sd:.1f} "
            f"→ Z≤-0.5: <b>{lo}</b>건 | -0.5~0.5: <b>{mid}</b>건 | Z&gt;0.5: <b>{hi}</b>건 "
            f"<i>(시장 간 비교 시 각 시장별로 동일 절차의 Z를 따로 산출한 뒤 해석)</i>"
        )
    return "\n".join(lines) + "\n"


def run_deep_dive_analysis(market='KR'):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    try:
        # KST 앵커: SQLite date('now') 미사용 — 롤링 컷오프·보조 쿼리 모두 동일 타임존
        kr_tz = pytz.timezone("Asia/Seoul")
        today_kst = datetime.now(kr_tz).date()
        today_str = today_kst.strftime("%Y-%m-%d")
        _cfg_dd = load_system_config()
        try:
            _rd = int(_cfg_dd.get("FORWARD_DEEP_DIVE_EXIT_WINDOW_DAYS", 90))
        except (TypeError, ValueError):
            _rd = 90
        rolling_days = _rd if _rd in (90, 180) else 90
        cutoff_rolling = (today_kst - timedelta(days=rolling_days)).strftime("%Y-%m-%d")
        cutoff_spill_30 = (today_kst - timedelta(days=30)).strftime("%Y-%m-%d")
        cutoff_rot_60 = (today_kst - timedelta(days=60)).strftime("%Y-%m-%d")

        conn = _open_market_db_ro()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'",
                (market,),
            )
            n_all_closed = int((cur.fetchone() or (0,))[0] or 0)
            df = pd.read_sql(
                """
                SELECT * FROM forward_trades
                WHERE market=? AND status LIKE 'CLOSED%' AND exit_date >= ?
                ORDER BY exit_date DESC
                """,
                conn,
                params=(market, cutoff_rolling),
            )
        finally:
            conn.close()

        if len(df) < 10:
            print(
                f"⚠️ [{market}] 최근 {rolling_days}일(KST) 청산 표본이 10건 미만입니다. "
                f"(해당 구간 {len(df)}건 / 전체 청산 누적 {n_all_closed}건, 앵커 {today_str})"
            )
            return

        df = prepare_forward_trades_df(df)
        df["Win"] = np.where(df["final_ret"] > 0, 1, 0)
        m_roll = len(df)
        report_msg = (
            f"🔬 [{market}장 포워드 테스팅 딥 다이브 분석]\n"
            f"(최근 {rolling_days}일 청산 {m_roll}건 / 전체 {n_all_closed}건, KST 기준 {today_str})\n\n"
        )

        try:
            meta_dd = load_meta_state_resolved()
        except Exception:
            meta_dd = {}

        rfa_dd = ReportFeatureAnalyzer(sys_config=_cfg_dd, meta=meta_dd)

        # ---------------------------------------------------------
        # 🌍 Universal DNA — 전역 1회 (버킷 루프 전)
        # ---------------------------------------------------------
        uni_block = build_universal_dna_block(df, analyzer=rfa_dd)
        report_msg += format_universal_dna_html(
            uni_block,
            market=market,
            rolling_days=rolling_days,
            today_str=today_str,
        )

        # ---------------------------------------------------------
        # 👑 점수대별 Micro-DNA — pd.cut 벡터 버킷 + ReportFeatureAnalyzer 동적 DNA
        # ---------------------------------------------------------
        dive = ForwardScoreBucketDeepDive(
            sys_config=_cfg_dd, meta=meta_dd, analyzer=rfa_dd
        )
        bucket_blocks = dive.build_bucket_blocks(df)
        report_msg += "👑 <b>[점수대별 Micro-DNA · 동적 피처]</b>\n"
        if bucket_blocks:
            report_msg += format_bucket_blocks_telegram_html(bucket_blocks)
            report_msg += "\n"
        else:
            report_msg += "<i>점수대별 표본 부족 또는 total_score/tier 부재.</i>\n\n"

        prep_df = ForwardScoreBucketDeepDive.assign_score_buckets(df)
        for bucket_label, t_df in prep_df.dropna(subset=["_score_bucket"]).groupby("_score_bucket", observed=True, sort=True):
            if len(t_df) < 5:
                continue
            try:
                t_int = int(str(bucket_label).replace("점대", "").strip())
            except ValueError:
                continue
            winners = t_df[pd.to_numeric(t_df["final_ret"], errors="coerce") > 5.0]
            if t_int <= 50 and len(winners) >= 3:
                _dna_cols = ("dyn_cpv", "dyn_tb", "v_energy", "dyn_rs")
                if all(c in winners.columns for c in _dna_cols):
                    ud_name = f"{market}_UNDERDOG_{t_int}점"
                    try:
                        deep_cfg = load_system_config()
                        inc_map = deep_cfg.get("INCUBATOR_TEMPLATES", {})
                        if not isinstance(inc_map, dict):
                            inc_map = {}
                        else:
                            inc_map = dict(inc_map)
                        inc_map[ud_name] = {
                            "cpv": round(scalar_float(winners["dyn_cpv"].mean()), 4),
                            "tb": round(scalar_float(winners["dyn_tb"].mean()), 4),
                            "bbe": round(scalar_float(winners["v_energy"].mean()), 4),
                            "rs": round(scalar_float(winners["dyn_rs"].mean()), 4),
                            "cos_cutoff": 0.75,
                            "created_at": datetime.now().strftime("%Y-%m-%d"),
                            "status": "INCUBATING",
                        }
                        deep_cfg["INCUBATOR_TEMPLATES"] = inc_map
                        save_system_config(deep_cfg)
                        tier_lbl = f"{t_int}점대"
                        report_msg += f"🧬 [자율 진화] {tier_lbl} 대박주 DNA가 인큐베이터({ud_name})에 신규 등재되었습니다.\n"
                    except Exception as _e:
                        report_msg += f"⚠️ 인큐베이터 DNA 주입 실패({ud_name}): {_e}\n"
        report_msg += "\n"

        report_msg += format_tier_champion_summary_html(
            bucket_blocks,
            market=market,
            rolling_days=rolling_days,
            today_str=today_str,
        )

        tag_snap = build_flow_tag_snapshot(
            df,
            sys_config=_cfg_dd,
            market=market,
            today_str=today_str,
            persist_toxic=True,
            save_config_fn=save_system_config,
            load_config_fn=load_system_config,
        )
        report_msg += format_flow_tag_report_html(
            tag_snap,
            market=market,
            rolling_days=rolling_days,
            today_str=today_str,
        )

        report_msg += _deep_dive_cross_market_isolation_footer(df, market)

        # ---------------------------------------------------------
        # 👑 엔진 7: [V28.0 한미 주도 섹터 스필오버(Spillover) 시차 분석]
        # ---------------------------------------------------------
        if market == 'KR':
            report_msg += "\n🌐 <b>[V28.0 한미 주도 섹터 스필오버(전이) 팩트 체크]</b>\n"
            try:
                sys_config = load_system_config()

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

                def _sector_row_ok(val):
                    t = str(val).strip()
                    if not t or t.lower() in ('nan', 'none'):
                        return False
                    if '유망' in t:
                        return False
                    if t == '기타/혼합':
                        return False
                    return True

                conn_sp = _open_market_db_ro()
                try:
                    us_df = pd.read_sql(
                        "SELECT entry_date, sector FROM forward_trades WHERE market='US' AND entry_date >= ?",
                        conn_sp,
                        params=(cutoff_spill_30,),
                    )
                    kr_df = pd.read_sql(
                        "SELECT entry_date, sector FROM forward_trades WHERE market='KR' AND entry_date >= ?",
                        conn_sp,
                        params=(cutoff_spill_30,),
                    )
                finally:
                    conn_sp.close()

                us_raw = _v28_add_norm_day_col(us_df)
                kr_raw = _v28_add_norm_day_col(kr_df)

                T = today_kst
                cal_days = [(T - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(6, -1, -1)]

                _bad = frozenset({"데이터 없음", "필터 탈락"})

                def _sector_norm_align(lab: str) -> str:
                    s = str(lab).strip()
                    if s.startswith("캐시·"):
                        return s[2:].strip()
                    return s

                def _is_real_sector(lab: str) -> bool:
                    s = str(lab).strip()
                    if not s or s in _bad:
                        return False
                    core = _sector_norm_align(s)
                    if not core or core in _bad:
                        return False
                    return True

                timeline_for_ssot: list[dict] = []

                if us_raw.empty and kr_raw.empty:
                    report_msg += (
                        f"⚠️ 스필오버 분석: 최근 30일(KST, 진입일 ≥ {cutoff_spill_30}) 한·미 가상매매 진입 행이 DB에 없습니다.\n"
                    )
                else:
                    report_msg += "▪️ <b>최근 7일 섹터 모멘텀 타임라인 (캘린더 앵커 T-6~T, KST):</b>\n"
                    for d in cal_days:
                        lab_us = _v28_dominant_sector_label_for_day(
                            d, us_raw, map_standard_sector, _sector_row_ok
                        )
                        lab_kr = _v28_dominant_sector_label_for_day(
                            d, kr_raw, map_standard_sector, _sector_row_ok
                        )
                        lab_us = _v28_us_label_with_last_good_cache(
                            d, lab_us, sys_config, map_standard_sector
                        )
                        report_msg += f" [{d[5:]}] 🇺🇸 {str(lab_us)[:12]} ➔ 🇰🇷 {str(lab_kr)[:12]}\n"
                        aligned = _is_real_sector(lab_us) and _is_real_sector(lab_kr) and (
                            _sector_norm_align(lab_us) == _sector_norm_align(lab_kr)
                        )
                        timeline_for_ssot.append({"d": d, "us": lab_us, "kr": lab_kr, "aligned": bool(aligned)})

                    align_days = [(T - timedelta(days=i)).strftime("%Y-%m-%d") for i in (2, 1, 0)]
                    align_count = 0
                    by_d = {row["d"]: row for row in timeline_for_ssot}
                    for ad in align_days:
                        r = by_d.get(ad)
                        if r and r.get("aligned"):
                            align_count += 1
                    observe_mult = min(1.5, 1.0 + 0.1 * float(align_count))

                    report_msg += (
                        f"\n◽ <b>[관측·점수 미반영]</b> 최근3일(KST) 한·미 표준섹터 일치 <b>{align_count}</b>회 "
                        f"→ 가상배수 <b>{observe_mult:.1f}x</b> <i>(엔진 점수·가중치에 적용 없음)</i>\n"
                    )

                    try:
                        payload = {
                            "updated_at": datetime.now(kr_tz).strftime("%Y-%m-%d %H:%M:%S %Z"),
                            "anchor_end": cal_days[-1],
                            "align_3d": int(align_count),
                            "observe_multiplier": float(observe_mult),
                            "timeline_T6_T": list(timeline_for_ssot),
                        }
                        cfg_ob = load_system_config()
                        if not isinstance(cfg_ob, dict):
                            cfg_ob = {}
                        else:
                            cfg_ob = dict(cfg_ob)
                        cfg_ob["SPILLOVER_OBSERVE_SSOT"] = payload
                        save_system_config(cfg_ob)
                    except Exception as _se:
                        report_msg += f"⚠️ SPILLOVER_OBSERVE_SSOT 저장 실패(관측만): {_se}\n"

                current_spillover = sys_config.get("US_SPILLOVER_SECTOR", "NONE")
                if current_spillover is None or str(current_spillover).strip() == "" or str(current_spillover).strip().upper() == "NONE":
                    mapped_spillover = "NONE"
                else:
                    mapped_spillover = map_standard_sector(current_spillover)

                if mapped_spillover != "NONE" and mapped_spillover != "기타/혼합":
                    report_msg += f"\n💡 <b>[관제탑 스필오버 지령]</b>\n"
                    report_msg += (
                        f"현재 미국장에서 검증된 강력한 주도 섹터는 <b>[{mapped_spillover}]</b>입니다. "
                        "한국장 스나이퍼는 해당 섹터 포착 시 켈리 비중을 1.5배로 증폭하여 선취매(Spillover) 시너지를 극대화하고 있습니다.\n"
                    )
                else:
                    report_msg += (
                        "\n💡 <b>[관제탑 스필오버 지령]</b>\n"
                        "현재 미국장에서 전이될 만한 뚜렷한 고수익 주도 섹터가 발견되지 않아, 스필오버 가중치를 대기 중입니다.\n"
                    )
            except Exception as e:
                report_msg += f"⚠️ 스필오버 분석 에러: {e}\n"

        # ---------------------------------------------------------
        # 👑 엔진 8: [V29.0 주도 섹터 순환매(Rotation) 수명 및 전이 추적]
        # ---------------------------------------------------------
        report_msg += f"\n🔄 <b>[V29.0 {market}장 주도 섹터 순환매 자금 추적]</b>\n"
        try:
            conn_rt = _open_market_db_ro()
            try:
                rot_df = pd.read_sql(
                    "SELECT entry_date, sector FROM forward_trades WHERE market=? AND entry_date >= ? ORDER BY entry_date ASC",
                    conn_rt,
                    params=(market, cutoff_rot_60),
                )
            finally:
                conn_rt.close()

            def map_standard_sector(s):
                s_str = str(s).lower()
                if any(k in s_str for k in ["반도체", "it", "ai", "소프트웨어", "모바일", "테크", "데이터"]): return "반도체/IT"
                if any(k in s_str for k in ["바이오", "헬스", "의료", "제약"]): return "바이오/헬스케어"
                if any(k in s_str for k in ["배터리", "2차전지", "화학", "에너지", "정유"]): return "에너지/화학"
                if any(k in s_str for k in ["금융", "은행", "증권", "지주", "투자"]): return "금융/지주"
                if any(k in s_str for k in ["기계", "조선", "방산", "산업재", "로봇", "전력"]): return "산업재/기계"
                if any(k in s_str for k in ["소비", "유통", "식품", "화장품", "엔터", "미디어"]): return "소비재/엔터"
                return "기타/혼합"

            rot_df['sector'] = rot_df['sector'].apply(map_standard_sector)

            if not rot_df.empty:
                # 일자별 대장 섹터 추출
                # 💡 [픽스] 가짜 섹터 배제
                def get_real_sector_deep(x):
                    valid_s = [str(s) for s in x if '유망' not in str(s) and '포착' not in str(s)]
                    return pd.Series(valid_s).mode()[0] if valid_s else None
                    
                daily_dom = rot_df.groupby('entry_date')['sector'].agg(get_real_sector_deep).dropna()
                
                streaks = {}      # 섹터별 머무는 기간(수명)
                transitions = {}  # A -> B 로의 자금 이동 횟수
                
                current_sec = None
                current_streak = 0
                
                # 순환매 체인(Markov Chain) 연산
                for date, sec in daily_dom.items():
                    if sec == current_sec:
                        current_streak += 1
                    else:
                        if current_sec is not None:
                            # 수명 기록
                            if current_sec not in streaks: streaks[current_sec] = []
                            streaks[current_sec].append(current_streak)
                            
                            # 자금 이동 궤적 기록 (A ➔ B)
                            trans_key = f"{current_sec[:15]} ➔ {sec[:15]}"
                            transitions[trans_key] = transitions.get(trans_key, 0) + 1
                        
                        current_sec = sec
                        current_streak = 1
                
                # 마지막 진행 중인 파동 기록
                if current_sec is not None:
                    if current_sec not in streaks: streaks[current_sec] = []
                    streaks[current_sec].append(current_streak)

                # 1. 섹터별 체류 수명 리포팅
                report_msg += "▪️ <b>섹터별 자금 체류 시간 (수명):</b>\n"
                for sec, lengths in streaks.items():
                    avg_len = sum(lengths) / len(lengths)
                    max_len = max(lengths)
                    report_msg += f" - {sec[:15]}: 평균 {avg_len:.1f}일 (최장 {max_len}일)\n"
                    
                # 2. 자금 이동 궤적 리포팅
                report_msg += "\n▪️ <b>가장 빈번한 자금 이동 경로 (최근 60일, KST 진입일 기준):</b>\n"
                sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:3]
                if sorted_trans:
                    for path, count in sorted_trans:
                        report_msg += f" - {path} ({count}회 관측)\n"
                else:
                    report_msg += " - 아직 뚜렷한 전이 패턴이 형성되지 않았습니다.\n"
                    
                if current_sec and sorted_trans:
                    # "A ➔ B" 형태에서 B(다음 섹터) 추출
                    top_transition = sorted_trans[0][0]
                    if "➔" in top_transition:
                        next_sec = top_transition.split("➔")[1].strip()
                    else:
                        next_sec = "다음 섹터"
                    report_msg += f"💡 <b>관제탑 동적 통찰:</b> 현재 주도 섹터인 [{current_sec}]의 수명이 다해갈 경우, 과거 패턴상 자금 이동 확률이 가장 높은 [{next_sec}] 섹터의 선취매를 준비하십시오.\n"
                else:
                    report_msg += "💡 <b>관제탑 동적 통찰:</b> 아직 뚜렷한 섹터 전이 패턴이 확보되지 않아 관망을 권장합니다.\n"
            else:
                report_msg += "⚠️ 순환매 추적을 위한 표본 데이터가 부족합니다.\n"
        except Exception as e:
            report_msg += f"⚠️ 순환매 추적 에러: {e}\n"
            
        # ---------------------------------------------------------
        # 👑 엔진 9: [V39.0 자금 관리 시뮬레이션: 고정 리스크 vs 켈리 리스크]
        # ---------------------------------------------------------
        if 'invest_amount' in df.columns and 'sim_kelly_invest' in df.columns:
            report_msg += f"\n⚖️ <b>[자금 관리 평행우주 대결 — 최근 {rolling_days}일 청산(KST) 기준 실현 손익]</b>\n"
            
            # 💡 [버그 픽스] 과거 투입금 0원 데이터 보정 (기본 40만원)
            valid_invest_fixed = pd.to_numeric(df["invest_amount"], errors="coerce").replace(0, 400000).fillna(400000)
            valid_invest_kelly = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").replace(0, 400000).fillna(400000)
            fr_dd = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0)

            total_fixed_profit = scalar_float((valid_invest_fixed * (fr_dd / 100)).sum())
            total_kelly_profit = scalar_float((valid_invest_kelly * (fr_dd / 100)).sum())

            report_msg += f"▪️ 고정 2% 베팅 누적 손익: <b>{total_fixed_profit:,.0f}원</b>\n"
            report_msg += f"▪️ 국면형 켈리 누적 손익: <b>{total_kelly_profit:,.0f}원</b>\n"
            
            if total_kelly_profit > total_fixed_profit:
                if total_kelly_profit > 0:
                    report_msg += "🏆 <b>[켈리 승리]</b> 상승장에서 비중을 싣고 하락장에서 방어한 동적 켈리 전략이 자본 증식에 유리했습니다.\n"
                else:
                    report_msg += "🛡️ <b>[켈리 선방]</b> 두 전략 모두 손실이나, 동적 켈리가 하락장에서 비중을 줄여 계좌 타격을 더 잘 방어했습니다.\n"
            else:
                if total_fixed_profit > 0:
                    report_msg += "🏆 <b>[고정 리스크 승리]</b> 휩소 장세로 인해 켈리 베팅이 엇박자를 내어, 고정 비중 투자가 더 유리했습니다.\n"
                else:
                    report_msg += "🛡️ <b>[고정 리스크 선방]</b> 두 전략 모두 손실이나, 고정 비중이 켈리의 과도한 리스크 베팅보다 타격이 적었습니다.\n"

        # 💡 [핵심 교정] 엔진 9번의 텍스트가 모두 report_msg에 담긴 후 최종 발송하도록 순서 교정
        send_telegram_msg(report_msg)
        print(f"✅ [{market}] 딥 다이브 분석 리포트 발송 완료.")
        
    except Exception as e:
        err_msg = _format_forward_ledger_error_html("딥 다이브 분석 중 에러 발생", e)
        print(err_msg)
        send_telegram_msg(err_msg)



# ==========================================
# 🕒 [무한 루프 스케줄러] 24시간 감시 및 보고 시스템
# ==========================================
def run_daily_scheduler():
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🕒 [포워드 장부 관리기] 24시간 감시 스케줄러 가동 시작!")
    print(" - 16:30 : 한국장 종가 추적 및 청산 집행")
    print(" - 17:00 : 일일 종합 리포트 텔레그램 발송")
    print(" - 06:30 : 미국장 종가 추적 및 청산 집행")
    
    while True:
        try:
            now = datetime.now(tz_kr)
            # 1. 한국장 마감 직후 (16:30) -> 종가 확인 및 청산 실행
            if now.hour == 16 and now.minute == 30:
                print("🚀 한국장 종가 추적 및 청산 업데이트 시작...")
                track_daily_positions('KR')
                time.sleep(60) # 중복 실행 방지
                
            # 2. 일일 종합 리포트 발송 (17:00)
            elif now.hour == 17 and now.minute == 0:
                print("🚀 17:00 통합 지능 리포트 발송 시작...")
                send_comprehensive_daily_report() 
                send_group_practitioner_reports()
                run_deep_dive_analysis('KR')
                run_deep_dive_analysis('US')
                generate_mutant_strategies()
                time.sleep(60)
                
            # 3. 미국장 마감 직후 (한국시간 오전 06:30) -> 종가 확인 및 청산 실행
            elif now.hour == 6 and now.minute == 30:
                print("🚀 미국장 종가 추적 및 청산 업데이트 시작...")
                track_daily_positions('US')
                time.sleep(60)

            time.sleep(10) # 10초마다 시간 확인
            
        except Exception as e:
            # 👇👇 에러 발생 시 텔레그램으로 긴급 타전 👇👇
            err_msg = _format_forward_ledger_error_html("무한 루프 구동 중 꼬임 발생", e)
            print(err_msg)
            send_telegram_msg(err_msg)
            time.sleep(60) # 에러 폭탄(Spam) 방지를 위해 1분 대기 후 재가동

if __name__ == "__main__":
    # 이 파일을 CMD에서 실행해두면 24시간 살아 숨쉬며 리포트를 보냅니다.
    run_daily_scheduler()
