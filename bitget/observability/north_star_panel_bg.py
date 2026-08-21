"""
Bitget 코인 북극성 패널 (읽기 전용) — KR/US 주식 북극성과 분리.

UX(4칸 쉬운판)만 참조. 문구·목표·마일스톤은 Track B SSOT만:
  MDD 5% · B0 측정 / B1~B2 연 12~25% · spot/futures · paper
주식 OBS_HOLD(갈림길 n/20 · mega_trend) · KR/US 장부 · 연 40~70% — 사용 금지.
원장 쓰기는 factory 19:30 cron 전용.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

# Track B 게이트 G1 진입용 일수 (ledger SSOT) — 주식 OBS_HOLD 20과 다름
_G1_DAILY_TARGET = 28


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
    """Track B 스냅샷 + 게이트 (쓰기 없음). Track A OBS_HOLD 메타 주입 안 함."""
    try:
        from dual_north_star_ledger import (
            G1_MIN_DAILY_SNAPSHOTS,
            R3_BANNER_TEXT,
            build_snapshot,
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
        # Track A 제거 — B만 남김 (실수로 주식 칸이 붙지 않게)
        tracks = snap.get("tracks") if isinstance(snap.get("tracks"), dict) else {}
        snap["tracks"] = {"B": tracks.get("B") or {}}
        pr = snap.get("period_returns") if isinstance(snap.get("period_returns"), dict) else {}
        snap["period_returns"] = {"B": pr.get("B") or {}}
        snap.pop("comparison", None)

        ledger = load_ledger()
        comm = ledger.get("commercialization") or {}
        snap["ledger"] = {"B": (comm.get("B") or {}) if isinstance(comm, dict) else {}}

        hist = ledger.get("history") or {}
        daily = hist.get("daily") if isinstance(hist.get("daily"), list) else []
        n = len(daily)
        g1 = int(G1_MIN_DAILY_SNAPSHOTS or _G1_DAILY_TARGET)
        meta = {
            "daily_n": n,
            "g1_target_n": g1,
            "g1_remaining": max(0, g1 - n),
            "r3_banner": R3_BANNER_TEXT,
            "show_r3_bitget_banner": not bool(
                (snap["tracks"]["B"] or {}).get("c2_funding_complete")
            ),
            "show_r1_caveat": n < g1,
            "r1_banner": "B0 관측 중 · 연복리 목표 단정 금지 (코인 paper)",
        }
        snap["meta"] = meta
        snap["available"] = True
        return snap
    except Exception as exc:
        return {
            "available": False,
            "error": f"ledger_read:{exc}"[:160],
            "cadence": cadence,
        }


def build_bitget_goal_dashboard(ns: Dict[str, Any]) -> Dict[str, Any]:
    """코인 전용 4칸 건강 진단 — 주식 갈림길/OBS_HOLD 없음."""
    if not ns or ns.get("available") is False:
        return {
            "headline": "코인 북극성 원장을 못 읽어요",
            "light": "🔴",
            "goal_plain": "목표: Bitget 코인 paper가 건강한지 확인 (실전 X · KR/US 일보와 별개)",
            "progress_pct": 0,
            "progress_bar": "░" * 10,
            "progress_label": "지금 할 일 0/1",
            "n": 0,
            "g1_target": _G1_DAILY_TARGET,
            "g1_remaining": _G1_DAILY_TARGET,
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
                    "title": "코인 북극성 원장",
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
            "spot_nav": None,
            "futures_nav": None,
            "forward_trades": 0,
        }

    meta = ns.get("meta") or {}
    ledger = ns.get("ledger") or {}
    gate_b = (ledger.get("B") or {}) if isinstance(ledger, dict) else {}
    tb = (ns.get("tracks") or {}).get("B") or {}
    agg = tb.get("aggregate") or {}
    pr_b = (ns.get("period_returns") or {}).get("B") or {}
    port = tb.get("portfolio") if isinstance(tb.get("portfolio"), dict) else {}

    n = int(meta.get("daily_n") or 0)
    g1 = int(meta.get("g1_target_n") or _G1_DAILY_TARGET)
    g1_rem = int(meta.get("g1_remaining") or max(0, g1 - n))
    gate = str(gate_b.get("gate") or "G0")
    composite = float(agg.get("composite_score", 0) or 0)
    mdd = float(agg.get("max_mdd_pct", 0) or 0)
    mdd_cap = float(tb.get("mdd_cap_pct", 5) or 5)
    total_pct = pr_b.get("total_pct")
    phase = str(tb.get("phase") or "B0").upper()
    measure_only = bool(agg.get("measure_only")) or phase.startswith("B0")
    cagr_lo = float(tb.get("cagr_target_lo", 12) or 12)
    cagr_hi = float(tb.get("cagr_target_hi", 25) or 25)
    spot_nav = float(port.get("spot_nav", 0) or 0) if port else 0.0
    fut_nav = float(port.get("futures_nav", 0) or 0) if port else 0.0
    ft = int(tb.get("forward_trades_count", 0) or 0)

    working: list[dict[str, str]] = []
    problem: list[dict[str, str]] = []
    missing: list[dict[str, str]] = []
    later: list[dict[str, str]] = []

    working.append(
        {
            "id": "tg",
            "title": "코인 일보 도착",
            "plain": "Bitget 북극성 칸 = 코인 보고 파이프 OK (주식 일보와 별개)",
        }
    )

    if tb.get("error") or tb.get("available") is False:
        problem.append(
            {
                "id": "nav",
                "title": "코인 treasury NAV",
                "plain": f"못 읽어요 · {tb.get('error') or 'available=false'}",
            }
        )
    else:
        working.append(
            {
                "id": "nav",
                "title": "코인 treasury NAV",
                "plain": (
                    f"합산 OK · spot {spot_nav:,.0f} / futures {fut_nav:,.0f} USDT"
                ),
            }
        )

    if mdd > mdd_cap:
        problem.append(
            {
                "id": "mdd",
                "title": "코인 MDD 캡(5%)",
                "plain": f"MDD {mdd:.1f}% > 캡 {mdd_cap:.0f}% — 손볼 구멍",
            }
        )
    elif mdd >= mdd_cap * 0.8:
        missing.append(
            {
                "id": "mdd",
                "title": "코인 MDD 캡(5%)",
                "plain": f"MDD {mdd:.1f}% · 캡 {mdd_cap:.0f}% 근처 — 조심 관측",
            }
        )
    else:
        working.append(
            {
                "id": "mdd",
                "title": "코인 MDD 캡(5%)",
                "plain": f"MDD {mdd:.1f}% ≤ 캡 {mdd_cap:.0f}% 안쪽",
            }
        )

    if ft <= 0:
        problem.append(
            {
                "id": "book",
                "title": "코인 paper 장부",
                "plain": "bitget_forward_trades=0 · 연습 체결이 아직 없어요",
            }
        )
    else:
        working.append(
            {
                "id": "book",
                "title": "코인 paper 장부",
                "plain": f"bitget_forward_trades {ft}건",
            }
        )

    if n >= g1:
        working.append(
            {
                "id": "daily",
                "title": "G1용 하루 스냅샷",
                "plain": f"{n}/{g1}일 · G1 페이스 조건 일수 충족(참고)",
            }
        )
    else:
        missing.append(
            {
                "id": "daily",
                "title": "G1용 하루 스냅샷",
                "plain": f"{n}/{g1}일 · 남음 {g1_rem}일 (B0 측정 유지)",
            }
        )

    if gate == "G0":
        missing.append(
            {
                "id": "gate",
                "title": "코인 상품화 게이트",
                "plain": "G0 측정·구조 · G1은 기록·점수 더 모은 뒤",
            }
        )
    else:
        working.append(
            {
                "id": "gate",
                "title": "코인 상품화 게이트",
                "plain": f"{gate} · {gate_b.get('gate_label') or ''}".strip(),
            }
        )

    # C-2는 금지 착수 — 기다림(🟡)이 아니라 나중(⬜)
    later.extend(
        [
            {
                "id": "c2",
                "title": "펀딩비(C-2) 반영",
                "plain": "나중 · 지금은 금지 · paper 참고용",
            },
            {
                "id": "mdd5tune",
                "title": "MDD 5% tier 튜닝",
                "plain": "나중 · PORTFOLIO_MDD_* 손대지 않음",
            },
            {
                "id": "b2live",
                "title": "deathmatch 실배분",
                "plain": "나중 · shadow만",
            },
            {
                "id": "live",
                "title": "실전 매매 ON",
                "plain": "나중 · P2-5 · ENABLE_REAL_EXECUTION 금지",
            },
            {
                "id": "cagr",
                "title": f"연복리 {cagr_lo:.0f}~{cagr_hi:.0f}% 단정",
                "plain": (
                    "B0=측정만 · 연목표 확정 금지"
                    if measure_only
                    else f"단계 {phase} · 참고 페이스만"
                ),
            },
        ]
    )

    now_ids = {"tg", "nav", "mdd", "book", "daily", "gate"}
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
        phase_plain = (
            f"B0 검증·측정 · MDD·데이터만 · "
            f"연 {cagr_lo:.0f}~{cagr_hi:.0f}%는 아직 목표 아님 (주식 40~70%와 무관)"
        )
    else:
        phase_plain = (
            f"단계 {phase} ({tb.get('phase_label') or ''}) · "
            f"연복리 참고 {cagr_lo:.0f}~{cagr_hi:.0f}% · MDD≤{mdd_cap:.0f}%"
        )

    return {
        "headline": headline,
        "light": light,
        "goal_plain": "목표: Bitget 코인 paper·관측이 건강한지 (실전 X · KR/US 일보와 별개)",
        "progress_pct": pct,
        "progress_bar": bar,
        "progress_label": f"지금 할 일 {done_n}/{need_n}",
        "n": n,
        "g1_target": g1,
        "g1_remaining": g1_rem,
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
        "spot_nav": spot_nav,
        "futures_nav": fut_nav,
        "forward_trades": ft,
        "mdd_tier": str(port.get("mdd_tier") or "—") if port else "—",
    }


def format_bitget_north_star_html(ns: Optional[Dict[str, Any]] = None) -> str:
    """텔레그램 HTML — 코인 전용. KR/US·갈림길·OBS_HOLD 문구 없음."""
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

    spot_s = f"{float(d.get('spot_nav') or 0):,.0f}"
    fut_s = f"{float(d.get('futures_nav') or 0):,.0f}"
    parts = [
        f"<b>📊 코인 북극성 · Bitget 일간</b> ({date_kst})",
        "",
        f"<b>{_esc(d.get('light') or '')} [쉬운판] Bitget 코인 · 건강 체크</b>",
        f"<b>{_esc(d.get('headline') or '')}</b>",
        f"<i>{_esc(d.get('phase_plain') or '')}</i>",
        "",
        f"🎯 {_esc(d.get('goal_plain') or '')}",
        f"📅 {_esc(d.get('progress_label') or '')} · {bar} {float(d.get('progress_pct') or 0):.0f}%",
        f"🔑 G1용 스냅샷 <b>{d.get('n')}</b>/{d.get('g1_target')}일 · 남음 <b>{d.get('g1_remaining')}</b>일",
        f"📊 참고점수 {float(d.get('composite') or 0):.1f} · 게이트 <code>{_esc(d.get('gate'))}</code> · "
        f"누적 {total_txt} · MDD {float(d.get('mdd') or 0):.1f}% (캡 {float(d.get('mdd_cap') or 5):.0f}%)",
        f"💰 spot {_esc(spot_s)} · futures {_esc(fut_s)} USDT · trades {int(d.get('forward_trades') or 0)}",
        "<i>※ 코인 only · 주식(KR/US) 북극성·NAV·게이트와 숫자 섞지 않음 · 연수익 단정 금지</i>",
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

    tb = (ns.get("tracks") or {}).get("B") or {}
    if ns.get("available") is not False and isinstance(tb, dict) and tb:
        agg = tb.get("aggregate") or {}
        pr = (ns.get("period_returns") or {}).get("B") or {}
        gate_b = ((ns.get("ledger") or {}).get("B") or {}) if isinstance(ns.get("ledger"), dict) else {}
        meta = ns.get("meta") or {}
        mdd_cap = float(tb.get("mdd_cap_pct", 5) or 5)
        cagr_lo = float(tb.get("cagr_target_lo", 12) or 12)
        cagr_hi = float(tb.get("cagr_target_hi", 25) or 25)
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
                "━━ Bitget 코인 · 목표·수익 ━━",
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
                f"목표 MDD ≤{mdd_cap:.0f}% · 연복리 {cagr_lo:.0f}~{cagr_hi:.0f}% "
                f"<i>(주식 10%/40~70% 아님)</i>",
                f"현재 MDD {mdd:.2f}% · 누적 {_fmt_pct(total, signed=True)}",
                f"목표달성률 {goal_pct:.0f}% · 게이트용 종합 {_bar(composite)} {composite:.0f}점",
                f"일 {_fmt_pct(pr.get('day_pct'))} · 주 {_fmt_pct(pr.get('week_pct'))} · "
                f"월 {_fmt_pct(pr.get('month_pct'))} · 연 {_fmt_pct(pr.get('year_pct'))}",
                f"상품화 게이트: <b>{_esc(gate_b.get('gate', 'G0'))}</b> {_esc(gate_b.get('gate_label', ''))}",
            ]
        )
        if measure:
            parts.append("B0 측정 — 수익 페이스는 참고만 · 연목표 단정 금지")
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
                "<i>이 메시지 = Bitget 코인만. "
                "KR/US 일보(제목「주식 북극성」·19:30)와 별개 채널입니다.</i>",
            ]
        )

    return "\n".join(parts)
