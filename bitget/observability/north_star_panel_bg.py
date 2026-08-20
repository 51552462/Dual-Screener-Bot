"""
Bitget Track B · 북극성 패널 (읽기 전용).

주식 `[쉬운판]` UX를 참조하되 숫자는 Track B SSOT만 사용 (MDD 5% · B0~B3 연복리).
원장 쓰기는 factory 19:30 north-star-digest 전용 — 본 모듈은 persist=False.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional


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


def collect_bitget_north_star_snap(*, cadence: str = "daily") -> Dict[str, Any]:
    """
    dual_north_star_ledger 스냅샷 + 게이트 (쓰기 없음).
    실패 시 available=False 스텁 — POST_DEPLOY_OBS는 계속 발송.
    """
    try:
        from dual_north_star_ledger import (
            OBS_HOLD_RECALL_N,
            R3_BANNER_TEXT,
            build_snapshot,
            enrich_obs_hold_meta,
            load_ledger,
        )
    except Exception as exc:
        return {
            "available": False,
            "error": f"ledger_import:{exc}"[:160],
            "cadence": cadence,
        }

    try:
        snap = build_snapshot(cadence=cadence)
        ledger = load_ledger()
        snap["ledger"] = ledger.get("commercialization") or {}
        hist = ledger.get("history") or {}
        daily = hist.get("daily") if isinstance(hist.get("daily"), list) else []
        enrich_obs_hold_meta(snap, daily_n=len(daily))
        meta = snap.setdefault("meta", {})
        if "r3_banner" not in meta:
            meta["r3_banner"] = R3_BANNER_TEXT
        if "obs_hold_recall_n" not in meta:
            meta["obs_hold_recall_n"] = OBS_HOLD_RECALL_N
        snap["available"] = True
        return snap
    except Exception as exc:
        return {
            "available": False,
            "error": f"ledger_read:{exc}"[:160],
            "cadence": cadence,
        }


def build_bitget_goal_dashboard(ns: Dict[str, Any]) -> Dict[str, Any]:
    """Track B 초등 4칸 — 건강 진단 (CAGR 단정 금지 · B0=측정)."""
    if not ns or ns.get("available") is False:
        return {
            "headline": "북극성 원장을 못 읽어요",
            "light": "🔴",
            "goal_plain": "목표: Bitget(Track B) 연습·관측이 건강한지 확인 (실전 X)",
            "progress_pct": 0,
            "progress_bar": "░" * 10,
            "progress_label": "지금 할 일 0/1",
            "n": 0,
            "recall": 20,
            "remaining": 20,
            "gate": "G0",
            "composite": 0.0,
            "mdd": 0.0,
            "mdd_cap": 5.0,
            "total_pct": None,
            "phase": "B0",
            "phase_plain": "원장 읽기 실패",
            "working": [],
            "problem": [
                {
                    "id": "ledger",
                    "title": "북극성 원장",
                    "plain": str(ns.get("error") or "available=false"),
                }
            ],
            "missing": [],
            "later": [],
            "how_to_read": [
                "🟢 잘 되고 있어요",
                "🔴 구멍·오류 — 손보거나 Cursor/Claude에 물어보세요",
                "🟡 아직 모으는 중 / 조심",
                "⬜ 나중 — 지금은 건드리면 안 돼요",
            ],
        }

    from dual_north_star_ledger import OBS_HOLD_RECALL_N

    meta = ns.get("meta") or {}
    ledger = ns.get("ledger") or {}
    gate_b = (ledger.get("B") or {}) if isinstance(ledger, dict) else {}
    tb = (ns.get("tracks") or {}).get("B") or {}
    agg = tb.get("aggregate") or {}
    pr_b = (ns.get("period_returns") or {}).get("B") or {}
    port = tb.get("portfolio") if isinstance(tb.get("portfolio"), dict) else {}

    n = int(meta.get("daily_n") or meta.get("daily_snapshot_count") or 0)
    recall = int(meta.get("obs_hold_recall_n") or OBS_HOLD_RECALL_N)
    remaining = int(meta.get("obs_hold_remaining") or max(0, recall - n))
    gate = str(gate_b.get("gate") or "G0")
    composite = float(agg.get("composite_score", 0) or 0)
    mdd = float(agg.get("max_mdd_pct", 0) or 0)
    mdd_cap = float(tb.get("mdd_cap_pct", 5) or 5)
    total_pct = pr_b.get("total_pct")
    phase = str(tb.get("phase") or "B0").upper()
    measure_only = bool(agg.get("measure_only")) or phase.startswith("B0")
    cagr_lo = tb.get("cagr_target_lo", 12)
    cagr_hi = tb.get("cagr_target_hi", 25)

    working: list[dict[str, str]] = []
    problem: list[dict[str, str]] = []
    missing: list[dict[str, str]] = []
    later: list[dict[str, str]] = []

    working.append(
        {
            "id": "tg",
            "title": "오늘 성적표 도착",
            "plain": "Bitget 북극성 칸이 왔어요 = 보고 파이프 OK",
        }
    )

    if tb.get("error") or tb.get("available") is False:
        problem.append(
            {
                "id": "nav",
                "title": "자산 숫자 읽기",
                "plain": f"Bitget NAV를 못 읽어요 · {tb.get('error') or 'available=false'}",
            }
        )
    else:
        working.append(
            {
                "id": "nav",
                "title": "자산 숫자 읽기",
                "plain": "Bitget treasury NAV 읽기 OK",
            }
        )

    if mdd > mdd_cap:
        problem.append(
            {
                "id": "mdd",
                "title": "낙폭(MDD) 한도",
                "plain": f"MDD {mdd:.1f}% > 목표 캡 {mdd_cap:.0f}% — 손볼 구멍",
            }
        )
    elif mdd >= mdd_cap * 0.8:
        missing.append(
            {
                "id": "mdd",
                "title": "낙폭(MDD) 한도",
                "plain": f"MDD {mdd:.1f}% · 캡 {mdd_cap:.0f}% 근처 — 조심하며 관측",
            }
        )
    else:
        working.append(
            {
                "id": "mdd",
                "title": "낙폭(MDD) 한도",
                "plain": f"MDD {mdd:.1f}% ≤ 캡 {mdd_cap:.0f}% 안쪽",
            }
        )

    ft = int(tb.get("forward_trades_count", 0) or 0)
    if ft <= 0:
        problem.append(
            {
                "id": "book",
                "title": "코인 연습 장부",
                "plain": "forward trades=0 · 연습 기록이 아직 없어요",
            }
        )
    else:
        working.append(
            {
                "id": "book",
                "title": "코인 연습 장부",
                "plain": f"forward trades {ft}건 쌓임",
            }
        )

    if n >= recall:
        working.append(
            {
                "id": "daily",
                "title": "북극성 하루기록",
                "plain": f"{n}/{recall} · 갈림길 열쇠 모음 완료(참고)",
            }
        )
    else:
        missing.append(
            {
                "id": "daily",
                "title": "북극성 하루기록",
                "plain": f"{n}/{recall} · 남음 {remaining}일 (관측 유지)",
            }
        )

    if gate == "G0":
        missing.append(
            {
                "id": "gate",
                "title": "상품화 게이트",
                "plain": "지금 G0 · G1 이상은 기록·점수 더 모은 뒤",
            }
        )
    else:
        working.append(
            {
                "id": "gate",
                "title": "상품화 게이트",
                "plain": f"지금 {gate} · {gate_b.get('gate_label') or ''}".strip(),
            }
        )

    if not tb.get("c2_funding_complete"):
        missing.append(
            {
                "id": "c2",
                "title": "펀딩비(C-2) 반영",
                "plain": "아직 미반영 · paper 참고용 (실전 아님)",
            }
        )
    else:
        working.append(
            {
                "id": "c2",
                "title": "펀딩비(C-2) 반영",
                "plain": "funding PnL 반영 완료 표시",
            }
        )

    later.extend(
        [
            {
                "id": "mdd5tune",
                "title": "MDD 5% tier 튜닝",
                "plain": "나중 · 06 관측 통과 후 Handoff",
            },
            {
                "id": "live",
                "title": "실전 매매 ON",
                "plain": "나중 · P2-5 전 금지",
            },
            {
                "id": "cagr",
                "title": f"연복리 {cagr_lo}~{cagr_hi}% 단정",
                "plain": (
                    "지금은 B0 측정만 · 연목표 확정 금지"
                    if measure_only
                    else f"단계 {phase} · 참고 페이스만"
                ),
            },
            {
                "id": "g4",
                "title": "상품화 G4",
                "plain": "디렉터 수동 승인만 · 코드 자동 승격 없음",
            },
        ]
    )

    now_ids = {"tg", "nav", "mdd", "book", "daily", "gate", "c2"}
    done_n = sum(1 for x in working if x["id"] in now_ids)
    need_n = done_n + sum(1 for x in problem if x["id"] in now_ids) + sum(
        1 for x in missing if x["id"] in now_ids
    )
    pct = int(round(100.0 * done_n / need_n)) if need_n else 0
    bar = _bar(float(pct), width=10)

    if problem:
        headline = "오늘 고칠 구멍이 있어요"
        light = "🔴" if len(problem) >= 2 else "🟡"
    elif missing:
        headline = "잘 가고 있어요 · 아직 기다리는 칸이 있어요"
        light = "🟡"
    else:
        headline = "오늘 할 일 칸은 대체로 괜찮아요"
        light = "🟢"

    if measure_only:
        phase_plain = f"관측기(B0) · MDD·데이터만 봐요 · 연 {cagr_lo}~{cagr_hi}%는 아직 목표 아님"
    else:
        phase_plain = (
            f"단계 {phase} ({tb.get('phase_label') or ''}) · "
            f"연복리 참고 {cagr_lo}~{cagr_hi}% · MDD≤{mdd_cap:.0f}%"
        )

    return {
        "headline": headline,
        "light": light,
        "goal_plain": "목표: Bitget(Track B) 연습·관측이 건강한지 확인 (실전·새 실험 X)",
        "progress_pct": pct,
        "progress_bar": bar,
        "progress_label": f"지금 할 일 {done_n}/{need_n}",
        "n": n,
        "recall": recall,
        "remaining": remaining,
        "gate": gate,
        "composite": composite,
        "mdd": mdd,
        "mdd_cap": mdd_cap,
        "total_pct": total_pct,
        "phase": phase,
        "phase_plain": phase_plain,
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
        "nav": float(port.get("nav", 0) or 0) if port else None,
        "mdd_tier": str(port.get("mdd_tier") or "—") if port else "—",
        "forward_trades": ft,
    }


def format_bitget_north_star_html(ns: Optional[Dict[str, Any]] = None) -> str:
    """텔레그램 HTML — [쉬운판] Track B + 상세 목표·수익."""
    if ns is None:
        ns = collect_bitget_north_star_snap()
    d = build_bitget_goal_dashboard(ns)
    date_kst = _esc(ns.get("date_kst") or "")
    bar = d.get("progress_bar") or _bar(float(d.get("progress_pct") or 0), width=10)
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
        f"<b>📊 Bitget 북극성 · 일간</b> ({date_kst})",
        "",
        f"<b>{_esc(d.get('light') or '')} [쉬운판] Track B · 건강 체크</b>",
        f"<b>{_esc(d.get('headline') or '')}</b>",
        f"<i>{_esc(d.get('phase_plain') or '')}</i>",
        "",
        f"🎯 {_esc(d.get('goal_plain') or '')}",
        f"📅 {_esc(d.get('progress_label') or '')} · {bar} {float(d.get('progress_pct') or 0):.0f}%",
        f"🔑 하루기록 <b>{d.get('n')}</b>/{d.get('recall')} · 남음 <b>{d.get('remaining')}</b>일",
        f"📊 참고점수 {float(d.get('composite') or 0):.1f} · 게이트 <code>{_esc(d.get('gate'))}</code> · "
        f"누적 {total_txt} · MDD {float(d.get('mdd') or 0):.1f}%",
        "<i>※ 참고만 · 성공/실패·연수익률 확정 안 함 · 주식 숫자와 섞지 않음</i>",
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

    # detail block (Track B SSOT)
    tb = (ns.get("tracks") or {}).get("B") or {}
    if ns.get("available") is not False and isinstance(tb, dict) and tb:
        agg = tb.get("aggregate") or {}
        pr = (ns.get("period_returns") or {}).get("B") or {}
        gate_b = ((ns.get("ledger") or {}).get("B") or {}) if isinstance(ns.get("ledger"), dict) else {}
        meta = ns.get("meta") or {}
        mdd_cap = tb.get("mdd_cap_pct", 5)
        cagr_lo = tb.get("cagr_target_lo", 12)
        cagr_hi = tb.get("cagr_target_hi", 25)
        composite = float(agg.get("composite_score", 0) or 0)
        goal_pct = float(agg.get("return_pace_score", 0) or 0)
        mdd = float(agg.get("max_mdd_pct", 0) or 0)
        total = pr.get("total_pct")
        measure = bool(agg.get("measure_only"))
        parts.extend(
            [
                "",
                "━━━━━━━━━━━━━━━━",
                "",
                "━━ Track B · Bitget 코인 ━━",
            ]
        )
        if meta.get("show_r3_bitget_banner", True):
            parts.append(f"<i>{_esc(meta.get('r3_banner') or '')}</i>")
        if meta.get("show_r1_caveat"):
            parts.append(f"<i>ℹ️ {_esc(meta.get('r1_banner') or '')}</i>")
        parts.extend(
            [
                f"<b>{_esc(tb.get('label', 'Bitget 코인'))}</b> · {_esc(tb.get('phase_label', ''))} "
                f"(<code>{_esc(tb.get('phase', 'B0'))}</code>)",
                f"목표 MDD ≤{float(mdd_cap):.0f}% · 연복리 {float(cagr_lo):.0f}~{float(cagr_hi):.0f}%",
                f"현재 MDD {mdd:.2f}% · 누적 {_fmt_pct(total, signed=True)}",
                f"목표달성률 {goal_pct:.0f}% · 게이트용 종합 {_bar(composite)} {composite:.0f}점",
                f"일 {_fmt_pct(pr.get('day_pct'))} · 주 {_fmt_pct(pr.get('week_pct'))} · "
                f"월 {_fmt_pct(pr.get('month_pct'))} · 연 {_fmt_pct(pr.get('year_pct'))}",
                f"상품화 게이트: <b>{_esc(gate_b.get('gate', 'G0'))}</b> {_esc(gate_b.get('gate_label', ''))}",
            ]
        )
        if measure:
            parts.append("B0 측정 — 수익 페이스는 게이트 참고만 · 연목표 단정 금지")
        if gate_b.get("g3_blocked") or gate_b.get("not_candidate_reason"):
            parts.append(f"후보 아님 — 사유: {_esc(gate_b.get('not_candidate_reason', ''))}")
        elif gate_b.get("block_reasons"):
            reasons = gate_b.get("block_reasons") or []
            if reasons:
                parts.append(f"제한: {_esc(' · '.join(str(r) for r in reasons))}")
        if isinstance(tb.get("portfolio"), dict):
            p = tb["portfolio"]
            ft = int(tb.get("forward_trades_count", 0) or 0)
            parts.append(
                f"NAV {float(p.get('nav', 0) or 0):,.0f} USDT"
                f" · spot {float(p.get('spot_nav', 0) or 0):,.0f}"
                f" · futures {float(p.get('futures_nav', 0) or 0):,.0f}"
                f" · tier {_esc(p.get('mdd_tier', 'NORMAL'))} · trades {ft}"
            )
        if tb.get("error"):
            parts.append(f"⚠️ {_esc(tb['error'])}")
        parts.extend(
            [
                "",
                "<i>본 칸 = Bitget only · 주식 북극성(19:30)과 숫자 혼용 금지 · "
                "원장 SSOT=VPS dual_north_star_ledger.json</i>",
            ]
        )

    return "\n".join(parts)
