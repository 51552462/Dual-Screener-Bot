"""
Mega-Trend Kill-Switch 리포트 섹션 SSOT (P4).

일일 KR 리포트 · 주간 Flow 마스터 · 주말 RL 진화 브리핑이
동일한 블록·포맷터를 공유한다.
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from mega_trend_ignition import (
    assess_toxic_kill_cooldown,
    load_mega_trend_state,
    mega_trend_unlock_enabled,
)
from mega_trend_kill_rl import (
    KILL_TYPE_CLIMAX,
    KILL_TYPE_INTERNAL_MOMENTUM,
    KILL_TYPE_TOXIC,
    MEGA_TREND_SECTOR_DELTAS_KEY,
    MEGA_TREND_SECTOR_QUARANTINE_KEY,
    classify_kill_lane,
    load_kill_rl_state,
    resolve_effective_kill_rl_state,
    sector_guard_enabled,
)


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


_KILL_TYPE_LABELS = {
    KILL_TYPE_INTERNAL_MOMENTUM: "내부동력",
    KILL_TYPE_TOXIC: "Toxic",
    KILL_TYPE_CLIMAX: "Climax",
}


def _kill_type_label(kill_type: object) -> str:
    return _KILL_TYPE_LABELS.get(str(kill_type or ""), str(kill_type or "—"))


def _resolve_latest_kill_meta(state: Mapping[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """(kill_type, kill_at, reason) — 가장 최근 킬 메타."""
    candidates: List[Tuple[str, str, str, str]] = []
    for ktype, at_key, reason_key in (
        (KILL_TYPE_INTERNAL_MOMENTUM, "internal_momentum_kill_at", "internal_momentum_kill_reason"),
        (KILL_TYPE_TOXIC, "toxic_kill_at", "toxic_kill_reason"),
        (KILL_TYPE_CLIMAX, "climax_kill_at", "climax_reason"),
    ):
        at = str(state.get(at_key) or "")[:19]
        if at:
            candidates.append((at, ktype, at, str(state.get(reason_key) or "")[:120]))
    if not candidates:
        return None, None, None
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, ktype, at_s, rsn = candidates[0]
    return ktype, at_s, rsn or None


@dataclass(frozen=True)
class MegaTrendKillReportBlock:
    """Mega-Trend 언락·킬·RL 감사 스냅샷."""

    enabled: bool
    active: bool
    primary_sector: Optional[str]
    sectors: Tuple[str, ...]
    ignited_at: Optional[str]
    cooldown_active: bool
    cooldown_days_remaining: Optional[int]
    forgiveness_revoked: bool
    internal_momentum_lost: bool
    momentum_lost_sectors: Tuple[str, ...]
    last_kill_type: Optional[str]
    last_kill_at: Optional[str]
    last_kill_reason: Optional[str]
    rl_events_pending: int
    rl_events_evaluated: int
    recent_events: Tuple[Dict[str, Any], ...]
    win_rate_min_delta: float
    mfe_reach_min_delta: float
    consecutive_loss_delta: int
    defensive_scale_delta: float
    flow_reversal_z_delta: float
    flow_z_drop_min_delta: float
    climax_vol_shrink_delta: float
    scale_out_fraction_delta: float
    internal_opportunity_cost_rate: float
    internal_defense_success_rate: float
    climax_opportunity_cost_rate: float
    climax_defense_success_rate: float
    rl_updated_at: Optional[str]
    sector_delta_highlights: Tuple[Tuple[str, float], ...]
    sector_deltas: Dict[str, Dict[str, Any]]
    sector_quarantine: Dict[str, Dict[str, Any]]
    contamination_guard: bool


def _sector_delta_highlights(
    rl: Mapping[str, Any],
    *,
    max_items: int = 3,
) -> Tuple[Tuple[str, float], ...]:
    overlays = rl.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}
    if not isinstance(overlays, Mapping):
        return ()
    ranked: List[Tuple[str, float]] = []
    for sec, block in overlays.items():
        if not isinstance(block, Mapping):
            continue
        wr = float(block.get("win_rate_min_delta") or 0.0)
        ranked.append((str(sec), wr))
    ranked.sort(key=lambda x: abs(x[1]), reverse=True)
    return tuple(ranked[:max_items])


def _rl_overlay_context(block: MegaTrendKillReportBlock) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        MEGA_TREND_SECTOR_DELTAS_KEY: dict(block.sector_deltas),
        "win_rate_min_delta": block.win_rate_min_delta,
        "mfe_reach_min_delta": block.mfe_reach_min_delta,
        "consecutive_loss_delta": block.consecutive_loss_delta,
        "defensive_scale_delta": block.defensive_scale_delta,
        "flow_reversal_z_delta": block.flow_reversal_z_delta,
        "flow_z_drop_min_delta": block.flow_z_drop_min_delta,
        "climax_vol_shrink_delta": block.climax_vol_shrink_delta,
        "scale_out_fraction_delta": block.scale_out_fraction_delta,
    }
    return ctx


def _format_sector_overlay_lines(
    rl: Mapping[str, Any],
    *,
    primary_sector: Optional[str] = None,
    max_items: int = 3,
) -> str:
    overlays = rl.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}
    if not isinstance(overlays, Mapping) or not overlays:
        return ""

    lines: List[str] = []
    if primary_sector:
        eff = resolve_effective_kill_rl_state(rl, sector=primary_sector)
        if eff.get("_kill_rl_overlay"):
            lines.append(
                f"▪ <b>primary RL</b> {_esc(primary_sector)}: "
                f"WRΔ{float(eff.get('win_rate_min_delta') or 0):+.3f} · "
                f"flowZΔ{float(eff.get('flow_reversal_z_delta') or 0):+.3f}\n"
            )

    for sec, wr in _sector_delta_highlights(rl, max_items=max_items):
        block = overlays.get(sec) or {}
        if not isinstance(block, Mapping):
            continue
        lines.append(
            f"   · {_esc(sec)} overlay WRΔ{float(block.get('win_rate_min_delta') or 0):+.3f} · "
            f"flowZΔ{float(block.get('flow_reversal_z_delta') or 0):+.3f} · "
            f"int_n={int(block.get('internal_n') or 0)} clx_n={int(block.get('climax_n') or 0)}\n"
        )
    if not lines:
        return ""
    return "▪ <b>Sector RL overlay</b>\n" + "".join(lines)


def _format_quarantine_lines(quarantine: Mapping[str, Any]) -> str:
    if not isinstance(quarantine, Mapping) or not quarantine:
        return ""
    items = sorted(quarantine.keys())[:4]
    lines = [
        f"   · {_esc(sec)} strikes={int((quarantine.get(sec) or {}).get('count') or 0)}\n"
        for sec in items
    ]
    return "▪ <b>Sector quarantine</b> (P6)\n" + "".join(lines)


def build_mega_trend_kill_report_block(
    sys_config: Optional[Mapping[str, Any]] = None,
) -> MegaTrendKillReportBlock:
    cfg = dict(sys_config) if isinstance(sys_config, Mapping) else {}
    enabled = mega_trend_unlock_enabled()
    state = load_mega_trend_state(cfg)
    rl = load_kill_rl_state(cfg)

    cooldown = assess_toxic_kill_cooldown(state)
    diag = state.get("internal_diagnostics") or {}
    if not isinstance(diag, dict):
        diag = {}

    events = list(rl.get("kill_events") or [])
    recent = sorted(
        events,
        key=lambda e: str(e.get("kill_at") or ""),
        reverse=True,
    )[:5]

    ktype, kat, krsn = _resolve_latest_kill_meta(state)
    sectors_raw = state.get("sectors") or []
    sectors = tuple(str(s) for s in sectors_raw if s)
    overlays_raw = rl.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}
    sector_deltas = (
        {str(k): dict(v) for k, v in overlays_raw.items() if isinstance(v, Mapping)}
        if isinstance(overlays_raw, Mapping)
        else {}
    )

    return MegaTrendKillReportBlock(
        enabled=enabled,
        active=bool(state.get("active")),
        primary_sector=str(state.get("primary_sector") or "") or None,
        sectors=sectors,
        ignited_at=str(state.get("ignited_at") or "")[:10] or None,
        cooldown_active=bool(cooldown.get("active")),
        cooldown_days_remaining=(
            int(cooldown["days_remaining"])
            if cooldown.get("days_remaining") is not None
            else None
        ),
        forgiveness_revoked=bool(state.get("correlation_forgiveness_revoked")),
        internal_momentum_lost=bool(diag.get("any_momentum_lost")),
        momentum_lost_sectors=tuple(
            str(s) for s in (diag.get("momentum_lost_sectors") or [])
        ),
        last_kill_type=ktype,
        last_kill_at=kat,
        last_kill_reason=krsn,
        rl_events_pending=int(rl.get("events_pending") or 0),
        rl_events_evaluated=int(rl.get("events_evaluated") or 0),
        recent_events=tuple(recent),
        win_rate_min_delta=float(rl.get("win_rate_min_delta") or 0.0),
        mfe_reach_min_delta=float(rl.get("mfe_reach_min_delta") or 0.0),
        consecutive_loss_delta=int(rl.get("consecutive_loss_delta") or 0),
        defensive_scale_delta=float(rl.get("defensive_scale_delta") or 0.0),
        flow_reversal_z_delta=float(rl.get("flow_reversal_z_delta") or 0.0),
        flow_z_drop_min_delta=float(rl.get("flow_z_drop_min_delta") or 0.0),
        climax_vol_shrink_delta=float(rl.get("climax_vol_shrink_delta") or 0.0),
        scale_out_fraction_delta=float(rl.get("scale_out_fraction_delta") or 0.0),
        internal_opportunity_cost_rate=float(
            rl.get("internal_opportunity_cost_rate")
            or rl.get("opportunity_cost_rate")
            or 0.0
        ),
        internal_defense_success_rate=float(
            rl.get("internal_defense_success_rate")
            or rl.get("defense_success_rate")
            or 0.0
        ),
        climax_opportunity_cost_rate=float(rl.get("climax_opportunity_cost_rate") or 0.0),
        climax_defense_success_rate=float(rl.get("climax_defense_success_rate") or 0.0),
        rl_updated_at=str(rl.get("updated_at") or "")[:19] or None,
        sector_delta_highlights=_sector_delta_highlights(rl),
        sector_deltas=sector_deltas,
        sector_quarantine=(
            dict(rl.get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {})
            if isinstance(rl.get(MEGA_TREND_SECTOR_QUARANTINE_KEY), Mapping)
            else {}
        ),
        contamination_guard=sector_guard_enabled(),
    )


def _format_event_line(ev: Mapping[str, Any]) -> str:
    kill_d = str(ev.get("kill_at") or "")[:10]
    sector = _esc(ev.get("sector") or "—")
    klabel = _esc(_kill_type_label(ev.get("kill_type")))
    lane = _esc(ev.get("kill_lane") or classify_kill_lane(ev.get("kill_type")))
    outcome = str(ev.get("outcome") or "pending")
    if outcome == "opportunity_cost":
        oc = "기회비용"
    elif outcome == "defense_success":
        oc = "방어성공"
    elif outcome == "pending":
        oc = "평가대기"
    else:
        oc = "중립"
    post = ev.get("post_kill") or {}
    avg = post.get("avg_ret_pct")
    n_tr = post.get("n_trades")
    tail = ""
    if avg is not None and n_tr:
        tail = f" · post {float(avg):+.2f}% (n={int(n_tr)})"
    elif outcome == "pending":
        tail = ""
    return (
        f"   · {kill_d} {sector} <b>{klabel}</b>"
        f" [{lane}] → {oc}{tail}\n"
    )


def format_mega_trend_kill_daily_html(
    block: MegaTrendKillReportBlock,
    *,
    max_events: int = 3,
) -> str:
    """일일 KR 리포트용 — 언락 상태·킬·RL 요약."""
    if not block.enabled:
        return ""

    status = "🟢 언락" if block.active else "⚫ 비활성"
    if block.cooldown_active:
        rem = block.cooldown_days_remaining if block.cooldown_days_remaining is not None else "?"
        status = f"🛑 킬쿨다운({rem}일)"

    sectors_s = ", ".join(block.sectors[:3]) if block.sectors else "—"
    primary = _esc(block.primary_sector or "—")

    out = (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🧬 <b>[Mega-Trend Kill-Switch]</b>\n"
        f"<i>장부·수급 이중 킬 · 듀얼레인 RL</i>\n"
        f"▪ 상태: <b>{status}</b> · primary <b>{primary}</b>\n"
        f"▪ 섹터: {_esc(sectors_s)}"
    )
    if block.ignited_at:
        out += f" · ignited {_esc(block.ignited_at)}"
    out += "\n"

    if block.forgiveness_revoked:
        out += "▪ 면죄부: <b>박탈</b>\n"
    if block.internal_momentum_lost:
        lost = ", ".join(block.momentum_lost_sectors[:3]) or "—"
        out += f"▪ 내부동력: <b>상실</b> ({_esc(lost)})\n"

    if block.last_kill_at:
        out += (
            f"▪ 최근킬: <b>{_esc(_kill_type_label(block.last_kill_type))}</b> "
            f"{_esc(block.last_kill_at[:10])}"
        )
        if block.last_kill_reason:
            out += f" — {_esc(block.last_kill_reason[:80])}"
        out += "\n"

    out += (
        f"▪ RL: int WRΔ{block.win_rate_min_delta:+.3f} · "
        f"연속손실Δ{block.consecutive_loss_delta:+d} · "
        f"clx flowZΔ{block.flow_reversal_z_delta:+.3f}\n"
        f"▪ 이벤트: 평가대기 <b>{block.rl_events_pending}</b> · "
        f"완료 <b>{block.rl_events_evaluated}</b>\n"
    )

    shown = 0
    for ev in block.recent_events:
        if shown >= max_events:
            break
        out += _format_event_line(ev)
        shown += 1

    out += _format_sector_overlay_lines(
        _rl_overlay_context(block),
        primary_sector=block.primary_sector,
    )
    if block.contamination_guard:
        out += _format_quarantine_lines(block.sector_quarantine)
    return out


def format_mega_trend_kill_weekly_html(
    block: MegaTrendKillReportBlock,
    *,
    evolve_result: Optional[Mapping[str, Any]] = None,
    max_events: int = 5,
) -> str:
    """주간 Flow 마스터 말미 — RL 레인·델타·사후평가 감사."""
    if not block.enabled:
        return ""

    lanes_updated: List[str] = []
    if isinstance(evolve_result, Mapping):
        lanes_updated = list(evolve_result.get("lanes_updated") or [])

    rates_int = {}
    rates_clx = {}
    if isinstance(evolve_result, Mapping):
        rates_int = dict(evolve_result.get("rates_internal") or {})
        rates_clx = dict(evolve_result.get("rates_climax") or {})

    updated = bool((evolve_result or {}).get("updated")) if evolve_result else False
    evolve_tag = "갱신됨" if updated else "유지"
    if lanes_updated:
        evolve_tag += f" ({','.join(lanes_updated)})"

    out = (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🧬 <b>[Mega-Trend Kill RL · 주간 감사]</b>\n"
        f"<i>internal(장부) / external(Climax) 듀얼 레인</i>\n"
        f"▪ 주간 RL: <b>{_esc(evolve_tag)}</b>"
    )
    if block.rl_updated_at:
        out += f" · {_esc(block.rl_updated_at)}"
    out += "\n"

    int_n = int(rates_int.get("n") or 0)
    clx_n = int(rates_clx.get("n") or 0)
    if int_n or clx_n:
        out += (
            f"▪ 표본: internal <b>{int_n}</b> "
            f"(기회 {float(rates_int.get('opportunity_cost_rate') or block.internal_opportunity_cost_rate) * 100:.0f}% / "
            f"방어 {float(rates_int.get('defense_success_rate') or block.internal_defense_success_rate) * 100:.0f}%) · "
            f"climax <b>{clx_n}</b> "
            f"(기회 {float(rates_clx.get('opportunity_cost_rate') or block.climax_opportunity_cost_rate) * 100:.0f}% / "
            f"방어 {float(rates_clx.get('defense_success_rate') or block.climax_defense_success_rate) * 100:.0f}%)\n"
        )
    else:
        out += (
            f"▪ 레인 피드백: int 기회 {block.internal_opportunity_cost_rate * 100:.0f}% / "
            f"방어 {block.internal_defense_success_rate * 100:.0f}% · "
            f"clx 기회 {block.climax_opportunity_cost_rate * 100:.0f}% / "
            f"방어 {block.climax_defense_success_rate * 100:.0f}%\n"
        )

    out += (
        f"▪ <b>Internal Δ</b>: WR {block.win_rate_min_delta:+.3f} · "
        f"MFE {block.mfe_reach_min_delta:+.3f} · "
        f"연속손실 {block.consecutive_loss_delta:+d} · "
        f"scale {block.defensive_scale_delta:+.3f}\n"
        f"▪ <b>Climax Δ</b>: flowZ {block.flow_reversal_z_delta:+.3f} · "
        f"zDrop {block.flow_z_drop_min_delta:+.3f} · "
        f"trapVol {block.climax_vol_shrink_delta:+.3f} · "
        f"scaleOut {block.scale_out_fraction_delta:+.3f}\n"
        f"▪ 킬 이벤트 큐: 대기 <b>{block.rl_events_pending}</b> · "
        f"평가완료 <b>{block.rl_events_evaluated}</b>\n"
    )

    if block.last_kill_at:
        out += (
            f"▪ 최근킬: {_esc(_kill_type_label(block.last_kill_type))} "
            f"{_esc(block.last_kill_at[:10])}\n"
        )

    shown = 0
    for ev in block.recent_events:
        if shown >= max_events:
            break
        out += _format_event_line(ev)
        shown += 1

    if not block.recent_events:
        out += "   · <i>기록된 킬 이벤트 없음</i>\n"

    sectors_updated: List[str] = []
    if isinstance(evolve_result, Mapping):
        sectors_updated = list(evolve_result.get("sectors_updated") or [])
    if sectors_updated:
        out += f"▪ 섹터 RL 갱신: <b>{_esc(','.join(sectors_updated[:5]))}</b>\n"

    quarantined: List[str] = []
    if isinstance(evolve_result, Mapping):
        quarantined = list(evolve_result.get("sectors_quarantined") or [])
    if quarantined:
        out += f"▪ 섹터 격리(P6): <b>{_esc(','.join(quarantined[:5]))}</b>\n"
    elif block.sector_quarantine:
        out += _format_quarantine_lines(block.sector_quarantine)

    if block.contamination_guard:
        out += "▪ contamination guard: <b>ON</b>\n"

    out += _format_sector_overlay_lines(
        _rl_overlay_context(block),
        primary_sector=block.primary_sector,
        max_items=5,
    )

    return out


def append_mega_trend_daily_to_satellite(
    sys_config: Mapping[str, Any],
    satellite_html: str,
) -> str:
    """일일 KR 위성 브리핑 말미에 Mega-Trend 킬 블록 부착."""
    try:
        block = build_mega_trend_kill_report_block(sys_config)
        section = format_mega_trend_kill_daily_html(block)
        if section:
            return str(satellite_html or "") + section
    except Exception:
        pass
    return str(satellite_html or "")


def build_mega_trend_kill_weekly_appendix(
    sys_config: Mapping[str, Any],
    *,
    evolve_result: Optional[Mapping[str, Any]] = None,
) -> str:
    block = build_mega_trend_kill_report_block(sys_config)
    ev = evolve_result
    if ev is None:
        rl = load_kill_rl_state(sys_config)
        summary = rl.get("last_evolve_summary")
        if isinstance(summary, dict):
            ev = {
                "updated": summary.get("updated"),
                "lanes_updated": summary.get("lanes_updated"),
                "rates_internal": summary.get("rates_internal"),
                "rates_climax": summary.get("rates_climax"),
                "sectors_updated": summary.get("sectors_updated"),
                "sectors_quarantined": summary.get("sectors_quarantined"),
                "detail": summary.get("detail"),
            }
    return format_mega_trend_kill_weekly_html(block, evolve_result=ev)


def format_mega_trend_kill_rl_evolution_telegram(
    evolve_result: Mapping[str, Any],
) -> str:
    """주말 RL 1사이클 직후 단독 텔레그램 (갱신 시)."""
    try:
        from mega_trend_kill_rl import build_kill_rl_brief

        head = build_kill_rl_brief(dict(evolve_result))
    except Exception:
        head = "🧬 <b>[Mega-Trend Kill RL]</b>"

    if not evolve_result.get("updated"):
        return head

    detail = evolve_result.get("detail") or []
    detail_s = f" <i>({', '.join(str(d) for d in detail)})</i>" if detail else ""
    return head + detail_s
