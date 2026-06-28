"""
주말/월말 종합 결산 · 진화 리포트 (Grand Report).

주말 리포트(weekly_flow_report)에 덧붙여, "내 퀀트 구조가 어떻게 돌아갔고 어떻게
진화·발전했으며, 한국/미국 실무자(로직)마다 얼마씩 벌었고, 그래서 총 얼마를 벌었는지"를
한눈에 보여준다. 월 마지막 날에는 동일 골격을 더 깊게(월간 윈도우·주차별 일관성·진화 전체
이력) 확장한 월간 결산을 보낸다.

설계 원칙(비침습):
- 읽기 전용(RO) DB 접근만 사용. forward_trades / champion_precursor_genesis /
  deathmatch_* / strategy_registry 는 같은 market_data DB 에 있다.
- 손익(PnL) 산식은 기존 SSOT(weekly_flow_pnl) 를 그대로 재사용 → 일일/주간 리포트와 동일.
- NAV/통화/켈리는 live_nav_manager SSOT 사용. KR=₩ / US=$ 완전 분리.
- 모든 외부 조회(FX·진화 테이블)는 방어적으로 감싼다. 실패해도 리포트는 나간다.
- 전송은 텔레그램 4096자 제한을 고려해 섹션별로 청크 분할 발송.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import pytz

KST = pytz.timezone("Asia/Seoul")
_TG_LIMIT = 3900  # 텔레그램 4096 안전 마진

MARKETS = ("KR", "US")
MARKET_FLAG = {"KR": "🇰🇷", "US": "🇺🇸"}


# ---------------------------------------------------------------------------
# 시간 유틸
# ---------------------------------------------------------------------------
def _kst_now() -> datetime:
    return datetime.now(KST)


def _month_start(d: Optional[datetime] = None) -> datetime:
    d = d or _kst_now()
    return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def is_month_end(d: Optional[datetime] = None) -> bool:
    """오늘이 이번 달의 마지막 날인지(KST)."""
    d = d or _kst_now()
    return (d + timedelta(days=1)).day == 1


def _ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def _resolve_db_path(db_path: Optional[str]) -> str:
    if db_path:
        return db_path
    try:
        from market_db_paths import market_db_read_path

        return market_db_read_path()
    except Exception:
        pass
    try:
        from factory_data_paths import market_data_db_path

        return market_data_db_path()
    except Exception:
        return "market_data.db"


def _ro_conn(db_path: str) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=20)
    except Exception:
        conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        )
        return cur.fetchone() is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 통화/포맷
# ---------------------------------------------------------------------------
def _fmt_money(market: str, value: float) -> str:
    try:
        from live_nav_manager import format_currency

        return format_currency(market, value, with_symbol=True)
    except Exception:
        sym = "₩" if market.upper() == "KR" else "$"
        return f"{sym}{value:,.0f}"


def _signed_money(market: str, value: float) -> str:
    s = _fmt_money(market, abs(value))
    return f"+{s}" if value >= 0 else f"-{s}"


def _fx_usdkrw() -> Optional[float]:
    """USD→KRW 환율(최근 종가). 실패 시 None(환산 생략)."""
    try:
        import FinanceDataReader as fdr

        end = datetime.now()
        start = end - timedelta(days=10)
        raw = fdr.DataReader("USD/KRW", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if raw is None or getattr(raw, "empty", True):
            return None
        s = raw["Close"].dropna()
        if s.empty:
            return None
        v = float(s.iloc[-1])
        return v if v > 0 else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 데이터 로드 · 실무자(로직) 손익
# ---------------------------------------------------------------------------
def _load_closed(conn: sqlite3.Connection, market: str, start: str, end: str) -> pd.DataFrame:
    """기간 내 청산 거래(인큐베이터 제외). 기존 weekly_flow_report 로더 재사용."""
    try:
        from weekly_flow_report import _load_week_closed_df

        return _load_week_closed_df(conn, market, start, end)
    except Exception:
        try:
            df = pd.read_sql(
                """
                SELECT * FROM forward_trades
                WHERE market=? AND status LIKE 'CLOSED%'
                  AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                  AND IFNULL(exit_date,'') >= ? AND IFNULL(exit_date,'') <= ?
                ORDER BY exit_date ASC
                """,
                conn,
                params=(market, start, end + " 23:59:59"),
            )
            if df is None or df.empty:
                return pd.DataFrame()
            df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
            return df.dropna(subset=["final_ret"])
        except Exception:
            return pd.DataFrame()


def _core_group(sig: Any) -> str:
    """sig_type 에서 모든 [태그] 제거 → 순수 로직(실무자)명."""
    clean = re.sub(r"\[.*?\]", "", str(sig)).strip()
    return clean if clean else str(sig).replace("[", "").replace("]", "").strip()


def _operator_rows(df: pd.DataFrame, market: str) -> List[Dict[str, Any]]:
    """실무자(로직)별 실현손익·승률·PF·청산수. 손익 내림차순."""
    if df is None or df.empty:
        return []
    from weekly_flow_pnl import dataframe_realized_pnl_sum

    try:
        from reports.forward_report_scalar import profit_factor_from_returns
    except Exception:
        def profit_factor_from_returns(_s):  # type: ignore
            return 0.0

    work = df.copy()
    work["group"] = work["sig_type"].apply(_core_group)
    rows: List[Dict[str, Any]] = []
    for grp, g in work.groupby("group"):
        g_closed = g[g["status"].astype(str).str.contains("CLOSED", na=False)]
        n = int(len(g_closed))
        if n == 0:
            continue
        pnl = float(dataframe_realized_pnl_sum(g_closed, market=market))
        wins = int((pd.to_numeric(g_closed["final_ret"], errors="coerce") > 0).sum())
        wr = (wins / n * 100.0) if n else 0.0
        try:
            pf = float(profit_factor_from_returns(g_closed["final_ret"]))
        except Exception:
            pf = 0.0
        rows.append(
            {
                "group": str(grp),
                "pnl": pnl,
                "wr": wr,
                "pf": pf,
                "n": n,
                "avg_ret": float(pd.to_numeric(g_closed["final_ret"], errors="coerce").mean() or 0.0),
            }
        )
    rows.sort(key=lambda x: x["pnl"], reverse=True)
    return rows


def _market_summary(market: str, df_period: pd.DataFrame) -> Dict[str, Any]:
    """시장별 요약: 기간 실현손익 + NAV(누적) 상태."""
    period_pnl = 0.0
    n_closed = 0
    if df_period is not None and not df_period.empty:
        from weekly_flow_pnl import dataframe_realized_pnl_sum

        closed = df_period[df_period["status"].astype(str).str.contains("CLOSED", na=False)]
        n_closed = int(len(closed))
        period_pnl = float(dataframe_realized_pnl_sum(closed, market=market))

    nav = base = hwm = mdd = 0.0
    nav_total_n = 0
    try:
        from live_nav_manager import base_capital_for, get_market_state, live_nav

        nav = float(live_nav(market))
        base = float(base_capital_for(market))
        st = get_market_state(market)
        hwm = float(st.get("hwm", nav) or nav)
        mdd = float(st.get("mdd_pct", 0.0) or 0.0)
        nav_total_n = int(st.get("n_closed", 0) or 0)
    except Exception:
        pass

    total_ret_pct = ((nav - base) / base * 100.0) if base > 0 else 0.0
    return {
        "market": market,
        "period_pnl": period_pnl,
        "period_n": n_closed,
        "nav": nav,
        "base": base,
        "hwm": hwm,
        "mdd_pct": mdd,
        "nav_total_n": nav_total_n,
        "total_ret_pct": total_ret_pct,
        "total_pnl": nav - base,
    }


# ---------------------------------------------------------------------------
# 진화 내러티브
# ---------------------------------------------------------------------------
def _evolution_block(conn: sqlite3.Connection, start: str, end: str, *, detailed: bool) -> str:
    lines: List[str] = ["🧬 <b>[구조 진화·발전]</b>"]
    end_excl = end + " 23:59:59"

    # 1) 챔피언 전조 유전자(Genesis) — 기간 내 검증 결과
    if _table_exists(conn, "champion_precursor_genesis"):
        try:
            g = pd.read_sql(
                """
                SELECT market, champion_label, kind, status, realized_fwd_ret,
                       crowned_date, resolved_at
                FROM champion_precursor_genesis
                WHERE IFNULL(resolved_at,'') >= ? AND IFNULL(resolved_at,'') <= ?
                """,
                conn,
                params=(start, end_excl),
            )
        except Exception:
            g = pd.DataFrame()
        if g is not None and not g.empty:
            conf = int((g["status"] == "confirmed").sum())
            fail = int((g["status"] == "failed").sum())
            tox = int((g["status"] == "toxic").sum())
            lines.append(
                f"• 챔피언 전조 검증: 확증 <b>{conf}</b> · 실패 {fail} · 독성 {tox}"
            )
            if detailed:
                top = g[g["status"] == "confirmed"].copy()
                if not top.empty:
                    top["realized_fwd_ret"] = pd.to_numeric(top["realized_fwd_ret"], errors="coerce")
                    top = top.sort_values("realized_fwd_ret", ascending=False).head(5)
                    for _, r in top.iterrows():
                        rr = r.get("realized_fwd_ret")
                        rr_s = f"{float(rr):+.1f}%" if pd.notna(rr) else "—"
                        lines.append(
                            f"   ↳ {MARKET_FLAG.get(str(r['market']).upper(),'')} "
                            f"{r['champion_label']} (등극후 {rr_s})"
                        )
        else:
            lines.append("• 챔피언 전조 검증: 이번 기간 확정 표본 없음")

    # 2) 전조 예측 적중률
    if _table_exists(conn, "precursor_prediction_log"):
        try:
            p = pd.read_sql(
                """
                SELECT hit FROM precursor_prediction_log
                WHERE hit IS NOT NULL
                  AND IFNULL(resolved_at,'') >= ? AND IFNULL(resolved_at,'') <= ?
                """,
                conn,
                params=(start, end_excl),
            )
        except Exception:
            p = pd.DataFrame()
        if p is not None and not p.empty:
            hit_rate = float(pd.to_numeric(p["hit"], errors="coerce").mean() * 100.0)
            lines.append(f"• 전조 예측 적중률: <b>{hit_rate:.0f}%</b> ({len(p)}건 검증)")

    # 3) 데스매치 현 챔피언
    if _table_exists(conn, "deathmatch_champion"):
        try:
            c = pd.read_sql(
                "SELECT market, champion_label, composite_score, win_rate FROM deathmatch_champion",
                conn,
            )
        except Exception:
            c = pd.DataFrame()
        if c is not None and not c.empty:
            champs = []
            for _, r in c.iterrows():
                champs.append(
                    f"{MARKET_FLAG.get(str(r['market']).upper(),'')} {r['champion_label']}"
                )
            lines.append("• 현 데스매치 챔피언: " + " · ".join(champs))

    # 4) 데스매치 도태 이벤트
    if _table_exists(conn, "deathmatch_elimination_event"):
        try:
            e = pd.read_sql(
                """
                SELECT market, arm_id, reason, event_date
                FROM deathmatch_elimination_event
                WHERE IFNULL(event_date,'') >= ? AND IFNULL(event_date,'') <= ?
                ORDER BY event_date DESC
                """,
                conn,
                params=(start, end_excl),
            )
        except Exception:
            e = pd.DataFrame()
        if e is not None and not e.empty:
            lines.append(f"• 진화론적 도태: <b>{len(e)}</b>개 로직 퇴출/강등")
            if detailed:
                for _, r in e.head(8).iterrows():
                    lines.append(
                        f"   ↳ {MARKET_FLAG.get(str(r['market']).upper(),'')} "
                        f"{r['arm_id']} — {str(r.get('reason') or '')[:40]}"
                    )

    # 5) 전략 레지스트리(생애주기) — 상태 분포 + 기간 승격/강등
    if _table_exists(conn, "strategy_registry"):
        try:
            reg = pd.read_sql(
                "SELECT market, group_key, state, last_promoted_at, last_demoted_at, "
                "promote_reason, demote_reason FROM strategy_registry",
                conn,
            )
        except Exception:
            reg = pd.DataFrame()
        if reg is not None and not reg.empty:
            live = int((reg["state"].astype(str).str.upper() == "LIVE").sum())
            cooled = int((reg["state"].astype(str).str.upper() == "COOLED").sum())
            cand = int((reg["state"].astype(str).str.upper() == "CANDIDATE").sum())
            lines.append(f"• 전략 생애주기: LIVE {live} · COOLED {cooled} · CANDIDATE {cand}")

            def _in_win(col: str) -> pd.DataFrame:
                s = reg[col].astype(str)
                return reg[(s >= start) & (s <= end_excl)]

            promoted = _in_win("last_promoted_at")
            demoted = _in_win("last_demoted_at")
            if len(promoted) or len(demoted):
                lines.append(
                    f"• 이번 기간 승격 <b>{len(promoted)}</b> · 강등 <b>{len(demoted)}</b>"
                )
                if detailed:
                    for _, r in promoted.head(6).iterrows():
                        lines.append(
                            f"   ↳ ⬆ {MARKET_FLAG.get(str(r['market']).upper(),'')} "
                            f"{r['group_key']} — {str(r.get('promote_reason') or '')[:36]}"
                        )
                    for _, r in demoted.head(6).iterrows():
                        lines.append(
                            f"   ↳ ⬇ {MARKET_FLAG.get(str(r['market']).upper(),'')} "
                            f"{r['group_key']} — {str(r.get('demote_reason') or '')[:36]}"
                        )

    if len(lines) == 1:
        lines.append("• (이번 기간 진화 이벤트 표본 없음)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 섹션 빌더
# ---------------------------------------------------------------------------
def _regime_line(sys_config: Dict[str, Any]) -> str:
    regime = str(sys_config.get("CURRENT_REGIME_KEY", "—"))
    kelly = sys_config.get("DYNAMIC_KELLY_RISK", None)
    try:
        kelly_s = f"{float(kelly) * 100:.2f}%" if kelly is not None else "—"
    except Exception:
        kelly_s = "—"
    return f"🧭 국면 <code>{regime}</code> · 동적 켈리 {kelly_s}"


def _build_overview_section(
    *, monthly: bool, start: str, end: str, summaries: Dict[str, Dict[str, Any]],
    sys_config: Dict[str, Any],
) -> str:
    title = "🏆 <b>[월간 종합 결산]</b>" if monthly else "📒 <b>[주간 실무자 결산]</b>"
    period = f"📅 {start} ~ {end}"
    lines = [title, period, _regime_line(sys_config), ""]

    for mkt in MARKETS:
        s = summaries[mkt]
        lines.append(f"{MARKET_FLAG[mkt]} <b>{mkt}</b>")
        lines.append(
            f"   · {'이번 달' if monthly else '이번 주'} 실현손익: "
            f"<b>{_signed_money(mkt, s['period_pnl'])}</b> ({s['period_n']}건 청산)"
        )
        lines.append(
            f"   · 누적 NAV: {_fmt_money(mkt, s['nav'])} "
            f"(기준 {_fmt_money(mkt, s['base'])})"
        )
        lines.append(
            f"   · 전체 수익률: <b>{s['total_ret_pct']:+.2f}%</b> "
            f"({_signed_money(mkt, s['total_pnl'])}) · MDD {s['mdd_pct']:.1f}%"
        )
        lines.append("")

    # 통합 총계
    kr, us = summaries["KR"], summaries["US"]
    blended = (kr["total_ret_pct"] + us["total_ret_pct"]) / 2.0
    lines.append("💰 <b>[통합 총결산]</b>")
    lines.append(
        f"   · {'월간' if monthly else '주간'} 실현손익: "
        f"KR {_signed_money('KR', kr['period_pnl'])} / US {_signed_money('US', us['period_pnl'])}"
    )
    lines.append(f"   · 블렌디드 전체 수익률(평균): <b>{blended:+.2f}%</b>")
    fx = _fx_usdkrw()
    if fx:
        grand_krw = kr["total_pnl"] + us["total_pnl"] * fx
        grand_nav_krw = kr["nav"] + us["nav"] * fx
        lines.append(
            f"   · 환산 총자산: ₩{grand_nav_krw:,.0f} · 총수익 ₩{grand_krw:,.0f} "
            f"(USD/KRW {fx:,.1f})"
        )
    else:
        lines.append("   · 환산 총수익: (환율 조회 불가 — 시장별 통화 기준 참조)")
    return "\n".join(lines)


def _build_operator_section(mkt: str, rows: List[Dict[str, Any]], *, top_n: int) -> str:
    head = f"{MARKET_FLAG[mkt]} <b>[{mkt} 실무자(로직)별 수익]</b>"
    if not rows:
        return head + "\n   · 이번 기간 청산 표본 없음"
    lines = [head]
    for i, e in enumerate(rows[:top_n]):
        if i == 0:
            m = "🥇"
        elif i == 1:
            m = "🥈"
        elif i == 2:
            m = "🥉"
        else:
            m = "•"
        if e["pnl"] < 0:
            m = "📉"
        lines.append(
            f"{m} <b>{e['group']}</b>: {_signed_money(mkt, e['pnl'])}"
        )
        lines.append(
            f"   ↳ 승률 {e['wr']:.0f}% (PF {e['pf']:.2f}) · "
            f"{e['n']}건 · 평균 {e['avg_ret']:+.1f}%"
        )
    pos = sum(1 for e in rows if e["pnl"] > 0)
    neg = sum(1 for e in rows if e["pnl"] < 0)
    lines.append(f"   ▸ 흑자 {pos} / 적자 {neg} (총 {len(rows)} 실무자)")
    return "\n".join(lines)


def _build_monthly_consistency(conn: sqlite3.Connection, mkt: str, month_start: datetime, now: datetime) -> str:
    """월간 전용: 주차별 실현손익 추이(일관성)."""
    from weekly_flow_pnl import dataframe_realized_pnl_sum

    lines = [f"{MARKET_FLAG[mkt]} <b>[{mkt} 주차별 일관성]</b>"]
    cur = month_start
    wk = 1
    any_row = False
    while cur <= now:
        wk_end = min(cur + timedelta(days=6), now)
        df = _load_closed(conn, mkt, _ymd(cur), _ymd(wk_end))
        if df is not None and not df.empty:
            closed = df[df["status"].astype(str).str.contains("CLOSED", na=False)]
            pnl = float(dataframe_realized_pnl_sum(closed, market=mkt))
            n = int(len(closed))
            mark = "🟢" if pnl >= 0 else "🔴"
            lines.append(
                f"   {mark} {wk}주차({_ymd(cur)}~{_ymd(wk_end)}): "
                f"{_signed_money(mkt, pnl)} ({n}건)"
            )
            any_row = True
        cur = wk_end + timedelta(days=1)
        wk += 1
    if not any_row:
        lines.append("   · 표본 없음")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 전송
# ---------------------------------------------------------------------------
def _send_chunked(send_fn: Callable[[str], Any], text: str) -> None:
    if not text:
        return
    if len(text) <= _TG_LIMIT:
        send_fn(text)
        return
    buf = ""
    for line in text.split("\n"):
        if len(buf) + len(line) + 1 > _TG_LIMIT:
            if buf:
                send_fn(buf)
            buf = line
        else:
            buf = (buf + "\n" + line) if buf else line
    if buf:
        send_fn(buf)


def build_grand_report_sections(
    *,
    monthly: bool,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """리포트 섹션 텍스트 리스트(전송 단위)를 만든다. 순수 함수(부수효과 없음)."""
    now = _kst_now()
    if monthly:
        start_dt = _month_start(now)
    else:
        start_dt = now - timedelta(days=7)
    start, end = _ymd(start_dt), _ymd(now)

    if sys_config is None:
        try:
            from system_config_atomic import load_config

            sys_config = load_config() or {}
        except Exception:
            sys_config = {}

    path = _resolve_db_path(db_path)
    sections: List[str] = []
    conn = _ro_conn(path)
    try:
        period_dfs = {m: _load_closed(conn, m, start, end) for m in MARKETS}
        summaries = {m: _market_summary(m, period_dfs[m]) for m in MARKETS}

        sections.append(
            _build_overview_section(
                monthly=monthly, start=start, end=end,
                summaries=summaries, sys_config=sys_config,
            )
        )

        top_n = 15 if monthly else 8
        for mkt in MARKETS:
            rows = _operator_rows(period_dfs[mkt], mkt)
            sections.append(_build_operator_section(mkt, rows, top_n=top_n))

        if monthly:
            for mkt in MARKETS:
                sections.append(_build_monthly_consistency(conn, mkt, start_dt, now))

        sections.append(_evolution_block(conn, start, end, detailed=monthly))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return sections


def send_grand_report(
    *,
    monthly: bool = False,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    send_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """
    주말(주간) 또는 월말(월간) 종합 결산 리포트를 텔레그램으로 발송.
    실패해도 예외를 던지지 않는다(주말 리포트 본류 보호). 결과 dict 반환.
    """
    if send_fn is None:
        try:
            from system_auto_pilot import send_telegram_report as send_fn  # type: ignore
        except Exception:
            def send_fn(_m):  # type: ignore
                print(_m)

    out: Dict[str, Any] = {"monthly": monthly, "sent": 0, "error": None}
    try:
        sections = build_grand_report_sections(
            monthly=monthly, db_path=db_path, sys_config=sys_config
        )
        for sec in sections:
            _send_chunked(send_fn, sec)
            out["sent"] += 1
            try:
                import time

                time.sleep(1)
            except Exception:
                pass
    except Exception as ex:
        out["error"] = str(ex)
        print(f"🚨 [weekend_grand_report] 발송 실패: {ex}")
    return out


def send_grand_report_if_due(
    *,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    send_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """
    월말 전용 진입점: 오늘이 월 마지막 날이면 월간 결산을 보낸다(아니면 스킵).
    cron 을 매일 돌려도 안전하도록 자체 게이트한다.
    """
    if not is_month_end():
        return {"due": False, "monthly": True, "sent": 0}
    res = send_grand_report(
        monthly=True, db_path=db_path, sys_config=sys_config, send_fn=send_fn
    )
    res["due"] = True
    return res
