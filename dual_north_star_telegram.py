"""
듀얼 북극성 진행장부 → 디렉터 텔레그램 다이제스트.

REPORT_BOT_* (또는 TELEGRAM_*) 로 단일 HTML 메시지 발송.
주식·Bitget 큐와 분리 — director digest 전용.
일간: [쉬운판] 대시보드 + [OBS_HOLD] + ---CURSOR--- / ---CLAUDE---.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

from dual_north_star_ledger import (
    OBS_HOLD_RECALL_N,
    R1_BANNER_TEXT,
    R3_BANNER_TEXT,
    run_north_star_digest,
)


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


def build_obs_hold_cursor_prompt(snap: Dict[str, Any]) -> str:
    """디렉터 → Cursor 첫 메시지용 (텔레그램 ---CURSOR--- 복붙)."""
    meta = snap.get("meta") or {}
    ledger = snap.get("ledger") or {}
    gate_a = (ledger.get("A") or {}) if isinstance(ledger, dict) else {}
    ta = (snap.get("tracks") or {}).get("A") or {}
    agg = ta.get("aggregate") or {}
    n = int(meta.get("daily_n") or meta.get("daily_snapshot_count") or 0)
    recall = int(meta.get("obs_hold_recall_n") or OBS_HOLD_RECALL_N)
    remaining = int(meta.get("obs_hold_remaining") or max(0, recall - n))
    action = str(meta.get("cursor_action") or "NONE")
    composite = float(agg.get("composite_score", 0) or 0)
    gate = str(gate_a.get("gate") or "G0")
    lines = [
        "[OBS_HOLD] Track A — North Star 관측 리뷰. Alpha Handoff 구현 금지.",
        "1) docs/work_phases/00_SESSION_SYNC.md §3 · NEXT_ACTION.md",
        "2) SSOT: VPS /var/lib/quant-factory/data/dual_north_star_ledger.json (로컬 원장 금지)",
        f"3) cursor_action={action} · daily_n={n}/{recall} · remaining={remaining}",
        f"4) gate_A={gate} · composite={composite:.2f} (참고만 · Pass/Fail·CAGR 단정 금지)",
    ]
    if action == "OBSERVE_HOLD":
        lines.append(
            "→ OBS-HOLD 유지. mega_trend/목표하향 착수 금지. "
            "조치 없음이면 3줄 요약만. 원장 n 정체·cron 장애면 OUTBOX."
        )
    elif action == "RECALL_FORK":
        lines.append(
            "→ 재소집 가능(n≥20). 코드 구현 금지. "
            "CURSOR_TO_CLAUDE.md OUTBOX에 실측 n·gate·composite append 후 "
            "디렉터에게 ---CLAUDE--- 붙여넣기 요청."
        )
    else:
        lines.append("→ daily 관측 패널 해당 없음(cadence≠daily). 대기.")
    return "\n".join(lines)


def build_obs_hold_claude_prompt(snap: Dict[str, Any]) -> str:
    """디렉터 → Claude Pro 창 복붙용 (구현 코드 작성 금지)."""
    meta = snap.get("meta") or {}
    ledger = snap.get("ledger") or {}
    gate_a = (ledger.get("A") or {}) if isinstance(ledger, dict) else {}
    ta = (snap.get("tracks") or {}).get("A") or {}
    agg = ta.get("aggregate") or {}
    n = int(meta.get("daily_n") or meta.get("daily_snapshot_count") or 0)
    recall = int(meta.get("obs_hold_recall_n") or OBS_HOLD_RECALL_N)
    remaining = int(meta.get("obs_hold_remaining") or max(0, recall - n))
    action = str(meta.get("cursor_action") or "NONE")
    composite = float(agg.get("composite_score", 0) or 0)
    gate = str(gate_a.get("gate") or "G0")
    lines = [
        "역할: Claude Pro Architect. 구현 코드 작성 금지.",
        "",
        "먼저 읽기:",
        "1) docs/work_phases/00_SESSION_SYNC.md §3",
        "2) docs/work_phases/NEXT_ACTION.md",
        "3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단",
        "",
        f"[OBS_HOLD] VPS 실측 · cursor_action={action}",
        f"· daily_n={n}/{recall} · remaining={remaining}",
        f"· gate_A={gate} · composite={composite:.2f} (참고 · 확정 판정 금지 unless n≥{recall})",
        "",
    ]
    if action == "RECALL_FORK":
        lines.extend(
            [
                "요청: 갈림길 3택 재판단 (mega_trend / 목표하향 / 관측연장).",
                "근거는 VPS 원장 수치만. OK면 CLAUDE_TO_CURSOR.md에 다음 Handoff 또는 OBS 연장.",
            ]
        )
    else:
        lines.extend(
            [
                "요청: OBS-HOLD 유지 확인. 신규 Handoff 없음이 정상.",
                f"n<{recall}이면 페이스·Pass/Fail·CAGR 확정 금지. 관측연장만.",
            ]
        )
    return "\n".join(lines)


def build_goal_dashboard(snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Track A 초등학생용 건강 진단 보드 (Bitget POST_DEPLOY_OBS UX 정렬).
    Buckets: working | problem | missing | later.
    CAGR/Pass-Fail 확정 아님. OPEN=0 alone ≠ 구멍.
    """
    from dual_north_star_ledger import collect_track_a_health, read_deploy_watch_health

    meta = snap.get("meta") or {}
    ledger = snap.get("ledger") or {}
    gate_a = (ledger.get("A") or {}) if isinstance(ledger, dict) else {}
    tracks = snap.get("tracks") or {}
    ta = tracks.get("A") or {}
    agg = ta.get("aggregate") or {}
    pr_a = (snap.get("period_returns") or {}).get("A") or {}

    health = snap.get("track_a_health")
    if not isinstance(health, dict):
        health = collect_track_a_health(snap)
    book = health.get("forward_book") if isinstance(health.get("forward_book"), dict) else {}
    if not book and isinstance(ta.get("forward_book"), dict):
        book = ta.get("forward_book") or {}
    watch = health.get("deploy_watch") if isinstance(health.get("deploy_watch"), dict) else {}
    if not watch:
        watch = read_deploy_watch_health()

    n = int(meta.get("daily_n") or meta.get("daily_snapshot_count") or 0)
    recall = int(meta.get("obs_hold_recall_n") or OBS_HOLD_RECALL_N)
    remaining = int(meta.get("obs_hold_remaining") or max(0, recall - n))
    action = str(meta.get("cursor_action") or "NONE")
    gate = str(gate_a.get("gate") or "G0")
    gate_label = str(gate_a.get("gate_label") or "")
    composite = float(agg.get("composite_score", 0) or 0)
    mdd = float(agg.get("max_mdd_pct", 0) or 0)
    mdd_cap = float(ta.get("mdd_cap_pct", 10) or 10)
    total_pct = pr_a.get("total_pct")

    working: list[dict[str, str]] = []
    problem: list[dict[str, str]] = []
    missing: list[dict[str, str]] = []
    later: list[dict[str, str]] = []

    # tg — digest pipe (this message)
    working.append(
        {
            "id": "tg",
            "title": "오늘 성적표 도착",
            "plain": "북극성 일보가 왔어요 = 보고 파이프 OK",
        }
    )

    # nav
    if ta.get("error") or ta.get("available") is False:
        problem.append(
            {
                "id": "nav",
                "title": "자산 숫자 읽기",
                "plain": f"KR/US 숫자를 못 읽어요 · {ta.get('error') or 'available=false'}",
            }
        )
    else:
        working.append(
            {
                "id": "nav",
                "title": "자산 숫자 읽기",
                "plain": "KR·US 자산(NAV) 숫자 읽기 OK",
            }
        )

    # book — CLOSED>0 = ok; OPEN=0 alone not red
    open_n = int(book.get("open_total", 0) or 0)
    closed_n = int(book.get("closed_total", 0) or 0)
    if book.get("error") and closed_n <= 0 and open_n <= 0:
        problem.append(
            {
                "id": "book",
                "title": "주식 연습 장부",
                "plain": f"장부를 못 읽어요 · {book.get('error')}",
            }
        )
    elif closed_n > 0:
        working.append(
            {
                "id": "book",
                "title": "주식 연습 장부",
                "plain": f"닫힌 자리 {closed_n} · 열린 자리 {open_n}"
                + (" (열린 0은 정상일 수 있어요)" if open_n == 0 else ""),
            }
        )
    else:
        problem.append(
            {
                "id": "book",
                "title": "주식 연습 장부",
                "plain": "닫힌 자리(CLOSED)가 0이에요 → 연습 기록이 없어요",
            }
        )

    # mdd
    if mdd <= mdd_cap:
        working.append(
            {
                "id": "mdd",
                "title": "낙폭 한도",
                "plain": f"낙폭 {mdd:.1f}% ≤ 한도 {mdd_cap:.0f}% (참고·확정 아님)",
            }
        )
    else:
        problem.append(
            {
                "id": "mdd",
                "title": "낙폭 한도",
                "plain": f"낙폭 {mdd:.1f}%가 한도 {mdd_cap:.0f}%를 넘었어요 — 긴급 점검",
            }
        )

    # ledger daily
    if n <= 0:
        problem.append(
            {
                "id": "ledger",
                "title": "북극성 하루기록",
                "plain": "하루 기록이 0개예요 · cron/원장 갱신 의",
            }
        )
    else:
        working.append(
            {
                "id": "ledger",
                "title": "북극성 하루기록",
                "plain": f"하루기록 {n}장 쌓이는 중",
            }
        )

    # deploy watch
    overall = str(watch.get("overall") or "").upper()
    if not watch.get("available"):
        missing.append(
            {
                "id": "watch",
                "title": "배포 감시판",
                "plain": f"아직 파일 없음/못 읽음 ({watch.get('error') or 'unknown'})",
            }
        )
    elif overall == "BREAK":
        problem.append(
            {
                "id": "watch",
                "title": "배포 감시판",
                "plain": f"overall=BREAK · phase={watch.get('phase') or '?'}",
            }
        )
    elif overall == "WARN" or watch.get("stale"):
        missing.append(
            {
                "id": "watch",
                "title": "배포 감시판",
                "plain": (
                    f"overall={overall or '?'} · "
                    + (
                        f"오래됨 {watch.get('age_hours')}h"
                        if watch.get("stale")
                        else "주의(WARN)"
                    )
                ),
            }
        )
    elif overall == "PASS":
        working.append(
            {
                "id": "watch",
                "title": "배포 감시판",
                "plain": f"overall=PASS · phase={watch.get('phase') or '?'}",
            }
        )
    else:
        missing.append(
            {
                "id": "watch",
                "title": "배포 감시판",
                "plain": f"overall={overall or 'SKIP'} · 참고만",
            }
        )

    # obs n/20
    if n >= recall:
        working.append(
            {
                "id": "obs",
                "title": "갈림길 열쇠",
                "plain": f"하루기록 {n}≥{recall} · 갈림길 회의 가능",
            }
        )
    else:
        missing.append(
            {
                "id": "obs",
                "title": "갈림길 열쇠",
                "plain": f"하루기록 {n}/{recall} · 남은 약 {remaining}일 (모으는 중)",
            }
        )

    # gate — never red
    if gate in ("G0", "", "G1"):
        missing.append(
            {
                "id": "gate",
                "title": "상품화 게이트",
                "plain": f"아직 {gate or 'G0'}({gate_label or '측정'}) · G2까지 멀어요",
            }
        )
    else:
        working.append(
            {
                "id": "gate",
                "title": "상품화 게이트",
                "plain": f"지금 {gate}({gate_label or ''})",
            }
        )

    later.extend(
        [
            {
                "id": "mega",
                "title": "mega_trend / 목표하향",
                "plain": "나중 · 갈림길 재소집(n≥20) 전 금지",
            },
            {
                "id": "phase_b",
                "title": "진화·킬(Phase B)",
                "plain": "나중 · 관측 끝난 뒤",
            },
            {
                "id": "live",
                "title": "실전 매매 ON",
                "plain": "나중 · G4 + 디렉터 승인 전 금지",
            },
            {
                "id": "cagr",
                "title": "연 40~70% 달성 단정",
                "plain": "나중 · G2 전 · RP-1≠달성",
            },
        ]
    )

    now_ids = {"tg", "nav", "book", "mdd", "ledger", "watch", "obs", "gate"}
    done_n = sum(1 for x in working if x["id"] in now_ids)
    need_n = (
        done_n
        + sum(1 for x in problem if x["id"] in now_ids)
        + sum(1 for x in missing if x["id"] in now_ids)
    )
    pct = int(round(100.0 * done_n / need_n)) if need_n else 0
    bar_w = 10
    filled = max(0, min(bar_w, int(round(pct / 100.0 * bar_w))))
    bar = "█" * filled + "░" * (bar_w - filled)

    if problem:
        headline = "오늘 고칠 구멍이 있어요"
        light = "🔴" if len(problem) >= 2 else "🟡"
    elif missing:
        headline = "잘 가고 있어요 · 아직 기다리는 칸이 있어요"
        light = "🟡"
    else:
        headline = "오늘 할 일 칸은 대체로 괜찮아요"
        light = "🟢"

    if action == "OBSERVE_HOLD":
        phase_plain = "관측 기간입니다. 새 실험 금지. 구멍만 보고, 성적만 모읍니다."
    elif action == "RECALL_FORK":
        phase_plain = "성적 20개 찼습니다. 회의해도 됩니다(아직 공사 시작 아님)."
    else:
        phase_plain = "주간/월간 요약입니다."

    return {
        "n": n,
        "recall": recall,
        "remaining": remaining,
        "progress_pct": float(pct),
        "progress_bar": bar,
        "progress_label": f"지금 할 일 {done_n}/{need_n}",
        "action": action,
        "gate": gate,
        "composite": composite,
        "mdd": mdd,
        "mdd_cap": mdd_cap,
        "total_pct": total_pct,
        "phase_plain": phase_plain,
        "headline": headline,
        "light": light,
        "goal_plain": "목표: 주식(KR+US) 연습·관측이 건강한지 확인 (실전·새 실험 X)",
        "working": working,
        "problem": problem,
        "missing": missing,
        "later": later,
        "how_to_read": [
            "🟢 잘 되고 있어요",
            "🔴 구멍·오류 — 손보거나 Cursor/Claude에 물어보세요",
            "🟡 아직 모으는 중 / 조심",
            "⬜ 나중 — 지금은 건드리면 안 돼요",
        ],
        # backward-compat aliases for older tests/callers
        "errors": [f"{p['title']}: {p['plain']}" for p in problem]
        or ["지금 빨간 오류 없음 (이 일보 기준)"],
        "checklist": [
            {"done": True, "text": "카테고리 A~Q · 목표 헌법(40~70% / MDD10%) 준비"},
            {"done": True, "text": "MDD 방어·관측 일보·복붙 블록 준비"},
            {"done": n >= recall, "text": f"하루 성적 모으기 {n}/{recall} (갈림길 열쇠)"},
            {"done": action == "RECALL_FORK", "text": "갈림길 회의 (mega_trend / 목표하향 / 관측연장)"},
            {"done": gate not in ("G0", "", "G1"), "text": f"게이트 전진 (지금 {gate})"},
            {"done": False, "text": "G2급 수익 페이스 증명 (56일+ · 단정 금지 중)"},
            {"done": False, "text": "상품화·실전(G4 + 디렉터 승인)"},
        ],
    }


def format_goal_dashboard_html(snap: Dict[str, Any]) -> str:
    """초등학생용 Track A 건강 진단 대시보드 HTML. daily만."""
    if str(snap.get("cadence") or "").lower() != "daily":
        return ""
    d = build_goal_dashboard(snap)
    bar = d.get("progress_bar") or _bar(float(d["progress_pct"]), width=10)
    total = d.get("total_pct")
    total_txt = _fmt_pct(total, signed=True) if total is not None else "—"

    def _sec(title: str, items: list, emoji: str, limit: int = 8) -> list[str]:
        lines = [f"<b>{emoji} {_esc(title)}</b>"]
        if not items:
            lines.append("· (없음)")
            return lines
        for it in items[:limit]:
            if isinstance(it, dict):
                lines.append(f"· <b>{_esc(it.get('title'))}</b> — {_esc(it.get('plain'))}")
            else:
                lines.append(f"· {_esc(it)}")
        return lines

    parts = [
        f"<b>{_esc(d.get('light') or '')} [쉬운판] Track A · 건강 체크</b>",
        f"<b>{_esc(d.get('headline') or '')}</b>",
        f"<i>{_esc(d.get('phase_plain') or '')}</i>",
        "",
        f"🎯 {_esc(d.get('goal_plain') or '')}",
        f"📅 {_esc(d.get('progress_label') or '')} · {bar} {float(d.get('progress_pct') or 0):.0f}%",
        f"🔑 갈림길 열쇠: 하루기록 <b>{d['n']}</b>/{d['recall']} · 남음 <b>{d['remaining']}</b>일",
        f"📊 참고점수 {d['composite']:.1f} · 게이트 <code>{_esc(d['gate'])}</code> · "
        f"누적 {total_txt} · MDD {d['mdd']:.1f}%",
        "<i>※ 참고만 · 성공/실패·연수익률 확정 안 함 · OPEN=0≠자동고장</i>",
        "",
    ]
    parts.extend(_sec("잘 되고 있어요", list(d.get("working") or []), "🟢"))
    parts.append("")
    parts.extend(_sec("구멍·오류 (손볼 것)", list(d.get("problem") or []), "🔴"))
    parts.append("")
    parts.extend(_sec("아직 기다리는 중", list(d.get("missing") or []), "🟡"))
    parts.append("")
    parts.extend(_sec("나중이에요 (지금 금지)", list(d.get("later") or []), "⬜", limit=6))
    parts.append("")
    parts.append("<b>읽는 법</b>")
    for tip in d.get("how_to_read") or []:
        parts.append(f"· {_esc(tip)}")
    return "\n".join(parts)


def format_obs_hold_section_html(snap: Dict[str, Any]) -> str:
    """일간 digest용 [OBS_HOLD] 패널 + 복붙 블록. cadence≠daily면 빈 문자열."""
    if str(snap.get("cadence") or "").lower() != "daily":
        return ""
    meta = snap.get("meta") or {}
    ledger = snap.get("ledger") or {}
    gate_a = (ledger.get("A") or {}) if isinstance(ledger, dict) else {}
    ta = (snap.get("tracks") or {}).get("A") or {}
    agg = ta.get("aggregate") or {}
    n = int(meta.get("daily_n") or meta.get("daily_snapshot_count") or 0)
    recall = int(meta.get("obs_hold_recall_n") or OBS_HOLD_RECALL_N)
    remaining = int(meta.get("obs_hold_remaining") or max(0, recall - n))
    action = str(meta.get("cursor_action") or "NONE")
    composite = float(agg.get("composite_score", 0) or 0)
    gate = str(gate_a.get("gate") or "G0")
    gate_label = str(gate_a.get("gate_label") or "")
    emoji = "🟢" if action == "RECALL_FORK" else "🟡"

    if action == "RECALL_FORK":
        status_line = "재소집 가능 — ---CLAUDE--- 를 Claude Pro에 붙여넣기"
    else:
        status_line = "관측유지 · mega_trend/목표하향 착수 금지 · 조치 없음"

    cursor_prompt = build_obs_hold_cursor_prompt(snap)
    claude_prompt = build_obs_hold_claude_prompt(snap)
    parts = [
        f"{emoji} <b>[OBS_HOLD]</b> North Star 관측",
        f"· daily <b>{n}</b>/{recall} · remaining <b>{remaining}</b>",
        f"· gate_A <code>{_esc(gate)}</code> {_esc(gate_label)} · "
        f"composite <b>{composite:.2f}</b> <i>(참고)</i>",
        f"· cursor_action=<code>{_esc(action)}</code>",
        f"· {_esc(status_line)}",
        "",
        "<b>---CURSOR---</b> <i>(아래 전부 Cursor 새 채팅 첫 메시지)</i>",
        f"<pre>{_esc(cursor_prompt)}</pre>",
        "",
        "<b>---CLAUDE---</b> <i>(아래 전부 Claude Pro 창 첫 메시지)</i>",
        f"<pre>{_esc(claude_prompt)}</pre>",
    ]
    return "\n".join(parts)


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
    ]

    dash_html = format_goal_dashboard_html(snap)
    if dash_html:
        parts.extend([dash_html, "", "━━━━━━━━━━━━━━━━", ""])

    parts.extend(
        [
            "━━ Track A · 주식 ━━",
            _track_block(ta, "A"),
            "",
            "━━ Track B · Bitget ━━",
            _track_block(tb, "B"),
            "",
        ]
    )

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

    obs_html = format_obs_hold_section_html(snap)
    if obs_html:
        parts.extend(["", "━━━━━━━━━━━━━━━━", obs_html])

    return "\n".join(parts)


def _send_report_html(message: str) -> bool:
    """REPORT_BOT direct HTTP — cron 위성 job용 (dante-async 큐 불필요)."""
    import requests
    import telegram_env

    token = (telegram_env.get_report_token() or "").strip()
    chat_id = (telegram_env.get_report_chat_id() or "").strip()
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        return resp.status_code == 200
    except Exception:
        return False


def send_north_star_digest(
    *,
    cadence: str = "daily",
    persist: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    snap = run_north_star_digest(cadence=cadence, persist=persist)
    html_msg = format_north_star_digest_html(snap)
    is_daily = str(snap.get("cadence") or "").lower() == "daily"
    result: Dict[str, Any] = {
        "snap": snap,
        "html": html_msg,
        "sent": False,
        "cursor_action": (snap.get("meta") or {}).get("cursor_action"),
        "cursor_prompt": build_obs_hold_cursor_prompt(snap) if is_daily else "",
        "claude_prompt": build_obs_hold_claude_prompt(snap) if is_daily else "",
    }

    if dry_run:
        result["dry_run"] = True
        return result

    try:
        import telegram_env

        token = (telegram_env.get_report_token() or "").strip()
        chat_id = (telegram_env.get_report_chat_id() or "").strip()
        if not token or not chat_id:
            result["error"] = "REPORT_BOT_TOKEN / REPORT_BOT_CHAT_ID 미설정"
            return result

        max_len = 4000
        chunks = [html_msg[i : i + max_len] for i in range(0, len(html_msg), max_len)] or [html_msg]
        for chunk in chunks:
            if not _send_report_html(chunk):
                result["error"] = "send_report_html failed (REPORT_BOT HTTP)"
                return result
        result["sent"] = True
        result["delivery"] = "direct_http (cron-safe; dante-async 불필요)"
    except Exception as exc:
        result["error"] = str(exc)[:200]

    return result
