"""
주말/월말 종합 결산 · 진화 리포트 (Grand Report) — Bitget 코인 전용.

주식 weekend_grand_report.py 구조를 코인에 이식:
- KR/US 시장 → SPOT/FUTURES 시장
- ₩/$ 통화 → USDT 단일 통화
- FX 환산 → 불필요 (USDT 통합)
- forward_trades → bitget_forward_trades
- 진화 내러티브: 동일 패턴 (champion_precursor_genesis, deathmatch, strategy_registry)

설계 원칙(비침습):
- 읽기 전용(RO) DB 접근만 사용.
- 손익 산식: sim_kelly_invest * final_ret / 100 (기존 auto_pilot 패턴).
- 모든 외부 조회는 방어적으로 감싼다. 실패해도 리포트는 나간다.
- 전송은 텔레그램 4096자 제한을 고려해 섹션별 청크 분할 발송.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from bitget.infra.gc_cycle import flush_gc
from bitget.infra.clock import utc_now
from bitget.infra.bounded_reads import (
    forward_grand_report_closed_sql,
    grand_report_deathmatch_champion_sql,
    grand_report_elimination_events_sql,
    grand_report_genesis_sql,
    grand_report_registry_demoted_sql,
    grand_report_registry_promoted_sql,
    grand_report_strategy_registry_sql,
)
from bitget.infra.memory_policy import GRAND_REPORT_CLOSED_LIMIT
from bitget.infra.logging_setup import get_logger, log_exception

_TG_LIMIT = 3900
MARKETS = ("spot", "futures")
logger = get_logger("bitget.weekend_grand_report")
MARKET_FLAG = {"spot": "🟢", "futures": "🟠"}


def _utc_now() -> datetime:
    return utc_now()


def _month_start(d: Optional[datetime] = None) -> datetime:
    d = d or _utc_now()
    return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def is_month_end(d: Optional[datetime] = None) -> bool:
    d = d or _utc_now()
    return (d + timedelta(days=1)).day == 1


def _ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _resolve_db_path(db_path: Optional[str]) -> str:
    if db_path:
        return db_path
    try:
        from bitget.infra.data_paths import market_data_db_path
        return market_data_db_path()
    except Exception:
        return "bitget_market_data.sqlite"


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


def _fmt_usdt(value: float) -> str:
    return f"{value:,.2f} USDT"


def _signed_usdt(value: float) -> str:
    s = _fmt_usdt(abs(value))
    return f"+{s}" if value >= 0 else f"-{s}"


def _load_closed(conn: sqlite3.Connection, market: str, start: str, end: str) -> pd.DataFrame:
    try:
        q, params = forward_grand_report_closed_sql(
            market_type=market,
            start=start,
            end=end,
            limit=GRAND_REPORT_CLOSED_LIMIT,
        )
        df = pd.read_sql(q, conn, params=params)
        if df is None or df.empty:
            return pd.DataFrame()
        df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
        return df.dropna(subset=["final_ret"])
    except Exception:
        return pd.DataFrame()


def _realized_pnl(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    try:
        invest = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0)
        ret = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0)
        return float((invest * ret / 100.0).sum())
    except Exception:
        return 0.0


def _core_group(sig: Any) -> str:
    clean = re.sub(r"\[.*?\]", "", str(sig)).strip()
    return clean if clean else str(sig).replace("[", "").replace("]", "").strip()


def _operator_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    work = df.copy()
    work["group"] = work["sig_type"].apply(_core_group)
    rows: List[Dict[str, Any]] = []
    for grp, g in work.groupby("group"):
        g_closed = g[g["status"].astype(str).str.contains("CLOSED", na=False)]
        n = int(len(g_closed))
        if n == 0:
            continue
        pnl = _realized_pnl(g_closed)
        rets = pd.to_numeric(g_closed["final_ret"], errors="coerce")
        wins = int((rets > 0).sum())
        wr = (wins / n * 100.0) if n else 0.0
        gains = rets[rets > 0].sum()
        losses = abs(rets[rets < 0].sum())
        pf = float(gains / losses) if losses > 0 else float(gains) if gains > 0 else 0.0
        rows.append({
            "group": str(grp),
            "pnl": pnl,
            "wr": wr,
            "pf": pf,
            "n": n,
            "avg_ret": float(rets.mean() or 0.0),
        })
    rows.sort(key=lambda x: x["pnl"], reverse=True)
    return rows


def _market_summary(market: str, df_period: pd.DataFrame, cfg: Dict[str, Any]) -> Dict[str, Any]:
    period_pnl = 0.0
    n_closed = 0
    if df_period is not None and not df_period.empty:
        closed = df_period[df_period["status"].astype(str).str.contains("CLOSED", na=False)]
        n_closed = int(len(closed))
        period_pnl = _realized_pnl(closed)

    treasury_key = f"TREASURY_{market.upper()}_USDT"
    treasury = float(cfg.get(treasury_key, 100000.0) or 100000.0)
    tail_key = f"TAIL_RISK_FUND_{market.upper()}"
    tail_fund = float(cfg.get(tail_key, 0.0) or 0.0)

    return {
        "market": market,
        "period_pnl": period_pnl,
        "period_n": n_closed,
        "treasury": treasury,
        "tail_fund": tail_fund,
    }


def _evolution_block(conn: sqlite3.Connection, start: str, end: str, *, detailed: bool) -> str:
    lines: List[str] = ["🧬 <b>[구조 진화·발전]</b>"]

    if _table_exists(conn, "champion_precursor_genesis"):
        try:
            g_q, g_params = grand_report_genesis_sql(start=start, end=end)
            g = pd.read_sql(g_q, conn, params=g_params)
        except Exception:
            g = pd.DataFrame()
        if g is not None and not g.empty:
            conf = int((g["status"] == "confirmed").sum())
            fail = int((g["status"] == "failed").sum())
            tox = int((g["status"] == "toxic").sum())
            lines.append(f"• 챔피언 전조 검증: 확증 <b>{conf}</b> · 실패 {fail} · 독성 {tox}")
            if detailed:
                top = g[g["status"] == "confirmed"].copy()
                if not top.empty:
                    top["realized_fwd_ret"] = pd.to_numeric(top["realized_fwd_ret"], errors="coerce")
                    top = top.sort_values("realized_fwd_ret", ascending=False).head(5)
                    for _, r in top.iterrows():
                        rr = r.get("realized_fwd_ret")
                        rr_s = f"{float(rr):+.1f}%" if pd.notna(rr) else "—"
                        mkt_icon = MARKET_FLAG.get(str(r.get("market", "")), "")
                        lines.append(f"   ↳ {mkt_icon} {r['champion_label']} (등극후 {rr_s})")
        else:
            lines.append("• 챔피언 전조 검증: 이번 기간 확정 표본 없음")

    if _table_exists(conn, "deathmatch_champion"):
        try:
            c_q, c_params = grand_report_deathmatch_champion_sql()
            c = pd.read_sql(c_q, conn, params=c_params)
        except Exception:
            c = pd.DataFrame()
        if c is not None and not c.empty:
            champs = []
            for _, r in c.iterrows():
                mkt_icon = MARKET_FLAG.get(str(r.get("market", "")), "")
                champs.append(f"{mkt_icon} {r['champion_label']}")
            lines.append("• 현 데스매치 챔피언: " + " · ".join(champs))

    if _table_exists(conn, "deathmatch_elimination_event"):
        try:
            e_q, e_params = grand_report_elimination_events_sql(start=start, end=end)
            e = pd.read_sql(e_q, conn, params=e_params)
        except Exception:
            e = pd.DataFrame()
        if e is not None and not e.empty:
            lines.append(f"• 진화론적 도태: <b>{len(e)}</b>개 로직 퇴출/강등")
            if detailed:
                for _, r in e.head(8).iterrows():
                    mkt_icon = MARKET_FLAG.get(str(r.get("market", "")), "")
                    lines.append(
                        f"   ↳ {mkt_icon} {r['arm_id']} — {str(r.get('reason') or '')[:40]}"
                    )

    if _table_exists(conn, "strategy_registry"):
        try:
            reg_q, reg_params = grand_report_strategy_registry_sql()
            reg = pd.read_sql(reg_q, conn, params=reg_params)
        except Exception:
            reg = pd.DataFrame()
        if reg is not None and not reg.empty:
            live = int((reg["state"].astype(str).str.upper() == "LIVE").sum())
            cooled = int((reg["state"].astype(str).str.upper() == "COOLED").sum())
            cand = int((reg["state"].astype(str).str.upper() == "CANDIDATE").sum())
            lines.append(f"• 전략 생애주기: LIVE {live} · COOLED {cooled} · CANDIDATE {cand}")

            try:
                p_q, p_params = grand_report_registry_promoted_sql(start=start, end=end)
                promoted = pd.read_sql(p_q, conn, params=p_params)
                d_q, d_params = grand_report_registry_demoted_sql(start=start, end=end)
                demoted = pd.read_sql(d_q, conn, params=d_params)
            except Exception:
                promoted = pd.DataFrame()
                demoted = pd.DataFrame()

            if len(promoted) or len(demoted):
                lines.append(
                    f"• 이번 기간 승격 <b>{len(promoted)}</b> · 강등 <b>{len(demoted)}</b>"
                )
                if detailed:
                    for _, r in promoted.head(6).iterrows():
                        mkt_icon = MARKET_FLAG.get(str(r.get("market", "")), "")
                        lines.append(
                            f"   ↳ ⬆ {mkt_icon} {r['group_key']} — "
                            f"{str(r.get('promote_reason') or '')[:36]}"
                        )
                    for _, r in demoted.head(6).iterrows():
                        mkt_icon = MARKET_FLAG.get(str(r.get("market", "")), "")
                        lines.append(
                            f"   ↳ ⬇ {mkt_icon} {r['group_key']} — "
                            f"{str(r.get('demote_reason') or '')[:36]}"
                        )

    if len(lines) == 1:
        lines.append("• (이번 기간 진화 이벤트 표본 없음)")
    return "\n".join(lines)


def _build_overview_section(
    *, monthly: bool, start: str, end: str,
    summaries: Dict[str, Dict[str, Any]], sys_config: Dict[str, Any],
) -> str:
    title = "🏆 <b>[월간 종합 결산]</b>" if monthly else "📒 <b>[주간 실무자 결산]</b>"
    period_label = "이번 달" if monthly else "이번 주"
    regime = str(sys_config.get("CURRENT_REGIME_KEY", "—"))
    kelly = sys_config.get("DYNAMIC_KELLY_RISK")
    try:
        kelly_s = f"{float(kelly) * 100:.2f}%" if kelly is not None else "—"
    except Exception:
        kelly_s = "—"

    lines = [
        title,
        f"📅 {start} ~ {end}",
        f"🧭 국면 <code>{regime}</code> · 동적 켈리 {kelly_s}",
        "",
    ]

    total_pnl = 0.0
    total_n = 0
    for mkt in MARKETS:
        s = summaries[mkt]
        icon = MARKET_FLAG[mkt]
        lines.append(f"{icon} <b>{mkt.upper()}</b>")
        lines.append(
            f"   · {period_label} 실현손익: <b>{_signed_usdt(s['period_pnl'])}</b> "
            f"({s['period_n']}건 청산)"
        )
        lines.append(
            f"   · 국고: {_fmt_usdt(s['treasury'])} · 테일펀드: {_fmt_usdt(s['tail_fund'])}"
        )
        lines.append("")
        total_pnl += s["period_pnl"]
        total_n += s["period_n"]

    lines.append("💰 <b>[통합 총결산]</b>")
    lines.append(
        f"   · {period_label} 통합 실현손익: <b>{_signed_usdt(total_pnl)}</b> "
        f"({total_n}건 청산)"
    )
    total_treasury = sum(summaries[m]["treasury"] for m in MARKETS)
    lines.append(f"   · 총 국고: {_fmt_usdt(total_treasury)}")
    return "\n".join(lines)


def _build_operator_section(mkt: str, rows: List[Dict[str, Any]], *, top_n: int) -> str:
    icon = MARKET_FLAG[mkt]
    head = f"{icon} <b>[{mkt.upper()} 실무자(로직)별 수익]</b>"
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
        lines.append(f"{m} <b>{e['group']}</b>: {_signed_usdt(e['pnl'])}")
        lines.append(
            f"   ↳ 승률 {e['wr']:.0f}% (PF {e['pf']:.2f}) · "
            f"{e['n']}건 · 평균 {e['avg_ret']:+.1f}%"
        )
    pos = sum(1 for e in rows if e["pnl"] > 0)
    neg = sum(1 for e in rows if e["pnl"] < 0)
    lines.append(f"   ▸ 흑자 {pos} / 적자 {neg} (총 {len(rows)} 실무자)")
    return "\n".join(lines)


def _build_monthly_consistency(
    conn: sqlite3.Connection, mkt: str, month_start_dt: datetime, now: datetime,
) -> str:
    icon = MARKET_FLAG[mkt]
    lines = [f"{icon} <b>[{mkt.upper()} 주차별 일관성]</b>"]
    cur = month_start_dt
    wk = 1
    any_row = False
    while cur <= now:
        wk_end = min(cur + timedelta(days=6), now)
        df = _load_closed(conn, mkt, _ymd(cur), _ymd(wk_end))
        if df is not None and not df.empty:
            closed = df[df["status"].astype(str).str.contains("CLOSED", na=False)]
            pnl = _realized_pnl(closed)
            n = int(len(closed))
            mark = "🟢" if pnl >= 0 else "🔴"
            lines.append(
                f"   {mark} {wk}주차({_ymd(cur)}~{_ymd(wk_end)}): "
                f"{_signed_usdt(pnl)} ({n}건)"
            )
            any_row = True
        cur = wk_end + timedelta(days=1)
        wk += 1
    if not any_row:
        lines.append("   · 표본 없음")
    return "\n".join(lines)


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
    now = _utc_now()
    if monthly:
        start_dt = _month_start(now)
    else:
        start_dt = now - timedelta(days=7)
    start, end = _ymd(start_dt), _ymd(now)

    if sys_config is None:
        try:
            from bitget.config_hub import load_config
            sys_config = load_config() or {}
        except Exception:
            sys_config = {}

    path = _resolve_db_path(db_path)
    sections: List[str] = []
    conn = _ro_conn(path)
    try:
        period_dfs = {m: _load_closed(conn, m, start, end) for m in MARKETS}
        summaries = {m: _market_summary(m, period_dfs[m], sys_config) for m in MARKETS}

        sections.append(
            _build_overview_section(
                monthly=monthly, start=start, end=end,
                summaries=summaries, sys_config=sys_config,
            )
        )

        top_n = 15 if monthly else 8
        for mkt in MARKETS:
            rows = _operator_rows(period_dfs[mkt])
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
    flush_gc(label="grand_report_build")
    return sections


def send_grand_report(
    *,
    monthly: bool = False,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    send_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    if send_fn is None:
        try:
            from bitget.forward.shared import send_telegram_msg as send_fn  # type: ignore
        except Exception:
            def send_fn(_m: str) -> None:  # type: ignore
                logger.info("%s", _m)

    out: Dict[str, Any] = {"monthly": monthly, "sent": 0, "error": None}
    try:
        sections = build_grand_report_sections(
            monthly=monthly, db_path=db_path, sys_config=sys_config,
        )
        import time
        for sec in sections:
            _send_chunked(send_fn, sec)
            out["sent"] += 1
            time.sleep(1)
    except Exception as ex:
        out["error"] = str(ex)
        log_exception(logger, "[bitget_grand_report] send failed: %s", ex)
    return out


def send_grand_report_if_due(
    *,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    send_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    if not is_month_end():
        return {"due": False, "monthly": True, "sent": 0}
    res = send_grand_report(
        monthly=True, db_path=db_path, sys_config=sys_config, send_fn=send_fn,
    )
    res["due"] = True
    return res
