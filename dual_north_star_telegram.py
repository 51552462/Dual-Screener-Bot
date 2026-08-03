"""
듀얼 북극성 진행장부 → 디렉터 텔레그램 다이제스트.

REPORT_BOT_* (또는 TELEGRAM_*) 로 단일 HTML 메시지 발송.
주식·Bitget 큐와 분리 — director digest 전용.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

from dual_north_star_ledger import R1_BANNER_TEXT, R3_BANNER_TEXT, run_north_star_digest


def _esc(v: Any) -> str:
    return html.escape(str(v) if v is not None else "", quote=False)


def _fmt_pct(v: Optional[float], *, signed: bool = True) -> str:
    if v is None:
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if signed and x > 0:
        return f"+{x:.2f}%"
    return f"{x:.2f}%"


def _bar(score: float, width: int = 10) -> str:
    score = max(0.0, min(100.0, float(score)))
    filled = int(round(score / 100.0 * width))
    return "█" * filled + "░" * (width - filled)


def format_north_star_digest_html(snap: Dict[str, Any]) -> str:
    cadence = str(snap.get("cadence") or "daily").upper()
    date_kst = _esc(snap.get("date_kst"))
    tracks = snap.get("tracks") or {}
    ta = tracks.get("A") or {}
    tb = tracks.get("B") or {}
    cmp_ = snap.get("comparison") or {}
    comm = snap.get("ledger") or {}
    meta = snap.get("meta") or {}

    show_r1 = bool(meta.get("show_r1_caveat"))
    r1_banner = _esc(meta.get("r1_banner") or R1_BANNER_TEXT)
    show_r3 = bool(meta.get("show_r3_bitget_banner", True))
    r3_banner = _esc(meta.get("r3_banner") or R3_BANNER_TEXT)

    def _gate_line(tid: str) -> str:
        gate = (comm.get(tid) or {}) if isinstance(comm, dict) else {}
        if not gate:
            return ""
        lines = [f"상품화 게이트: <b>{_esc(gate.get('gate', 'G0'))}</b> {_esc(gate.get('gate_label', ''))}"]
        if gate.get("g3_blocked") or gate.get("not_candidate_reason"):
            lines.append(f"후보 아님 — 사유: {_esc(gate.get('not_candidate_reason', ''))}")
        elif gate.get("block_reasons"):
            reasons = gate.get("block_reasons") or []
            if reasons:
                lines.append(f"제한: {_esc(' · '.join(str(r) for r in reasons))}")
        return "\n".join(lines)

    def _track_block(t: Dict[str, Any], tid: str) -> str:
        agg = t.get("aggregate") or {}
        pr = (snap.get("period_returns") or {}).get(tid) or {}
        mdd_cap = t.get("mdd_cap_pct", "?")
        cagr_lo = t.get("cagr_target_lo", "?")
        cagr_hi = t.get("cagr_target_hi", "?")
        composite = float(agg.get("composite_score", 0) or 0)
        goal_pct = float(agg.get("return_pace_score", 0) or 0)
        mdd = float(agg.get("max_mdd_pct", 0) or 0)
        total = pr.get("total_pct")
        measure = agg.get("measure_only", False)

        lines: list[str] = []
        if tid == "B" and show_r3:
            lines.append(f"<i>{r3_banner}</i>")
        if show_r1:
            lines.append(f"<i>ℹ️ {r1_banner}</i>")
        lines.extend(
            [
                f"<b>{_esc(t.get('label', tid))}</b> · {_esc(t.get('phase_label', ''))}",
                f"목표 MDD ≤{mdd_cap}% · 연복리 {cagr_lo}~{cagr_hi}%",
                f"현재 MDD {mdd:.2f}% · 누적 {_fmt_pct(total, signed=True)}",
                f"목표달성률 {goal_pct:.0f}% · 게이트용 종합 {_bar(composite)} {composite:.0f}점",
                f"일 {_fmt_pct(pr.get('day_pct'))} · 주 {_fmt_pct(pr.get('week_pct'))} · "
                f"월 {_fmt_pct(pr.get('month_pct'))} · 연 {_fmt_pct(pr.get('year_pct'))}",
            ]
        )
        if measure:
            lines.append("B0 측정 — 수익 페이스는 게이트 산정에만 부분 반영")
        gate_txt = _gate_line(tid)
        if gate_txt:
            lines.append(gate_txt)
        if tid == "A" and t.get("markets"):
            mk = t["markets"]
            kr = mk.get("KR") or {}
            us = mk.get("US") or {}
            lines.append(
                f"KR {_fmt_pct(kr.get('return_pct'))} MDD {float(kr.get('mdd_pct', 0) or 0):.1f}% · "
                f"US {_fmt_pct(us.get('return_pct'))} MDD {float(us.get('mdd_pct', 0) or 0):.1f}%"
            )
        if tid == "B" and t.get("portfolio"):
            p = t["portfolio"]
            ft = int(t.get("forward_trades_count", 0) or 0)
            lines.append(
                f"NAV {float(p.get('nav', 0) or 0):,.0f} USDT · tier {_esc(p.get('mdd_tier', 'NORMAL'))} · trades {ft}"
            )
        if t.get("error"):
            lines.append(f"⚠️ {_esc(t['error'])}")
        return "\n".join(lines)

    title = {
        "DAILY": "📊 듀얼 북극성 · 일간",
        "WEEKLY": "📊 듀얼 북극성 · 주간",
        "MONTHLY": "📊 듀얼 북극성 · 월간",
        "YEARLY": "📊 듀얼 북극성 · 연간",
    }.get(cadence, "📊 듀얼 북극성")

    parts = [
        f"<b>{title}</b> ({date_kst})",
        "",
        "━━ Track A · 주식 ━━",
        _track_block(ta, "A"),
        "",
        "━━ Track B · Bitget ━━",
        _track_block(tb, "B"),
        "",
    ]

    leader_mode = str(cmp_.get("leader_mode") or "")
    if leader_mode == "side_by_side":
        parts.extend(
            [
                "<b>비교 모드</b>: B0 측정 — 리더 미표시 (나란히)",
                _esc(cmp_.get("leader_reason", "")),
            ]
        )
    else:
        leader = _esc(cmp_.get("leader_track", "—"))
        parts.extend(
            [
                f"🏁 <b>리더</b> (목표달성률%): {leader}",
                _esc(cmp_.get("leader_reason", "")),
            ]
        )

    parts.extend(
        [
            "",
            "<i>격리 유지 · 게이트=종합점수60/40 · 리더=목표달성률%(B1+).</i>",
        ]
    )
    return "\n".join(parts)


def send_north_star_digest(
    *,
    cadence: str = "daily",
    persist: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    snap = run_north_star_digest(cadence=cadence, persist=persist)
    html_msg = format_north_star_digest_html(snap)
    result: Dict[str, Any] = {"snap": snap, "html": html_msg, "sent": False}

    if dry_run:
        result["dry_run"] = True
        return result

    try:
        import telegram_env
        from telegram_message_queue import enqueue_telegram

        token = (telegram_env.get_report_token() or "").strip()
        chat_id = (telegram_env.get_report_chat_id() or "").strip()
        if not token or not chat_id:
            result["error"] = "REPORT_BOT_TOKEN / REPORT_BOT_CHAT_ID 미설정"
            return result
        enqueue_telegram(
            chat_id=chat_id,
            text=html_msg,
            bot_token=token,
            send_profile="html",
            target="MAIN",
        )
        result["sent"] = True
        result["queue_note"] = (
            "enqueue_telegram: FIFO per SQLite queue; 동시 트리거 시 INSERT 순서대로 async daemon 소비"
        )
    except Exception as exc:
        result["error"] = str(exc)[:200]

    return result
