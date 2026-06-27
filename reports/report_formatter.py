"""
통합 리포트 HTML 포맷터 — [0] 둠스데이 · [0b] 숏 슬리브.
"""
from __future__ import annotations

from typing import Any


def _esc(s: Any) -> str:
    t = str(s)
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_doomsday_banner_html(
    *,
    market_icon: str,
    defcon_block: dict[str, Any],
    regime: str = "",
) -> str:
    try:
        lvl = int(defcon_block.get("level", 5))
    except (TypeError, ValueError):
        lvl = 5
    scores = defcon_block.get("scores") or {}
    g = scores.get("Global_Contagion_Score", "—")
    kr = scores.get("KR_Doom_Score", "—")
    reg = regime or defcon_block.get("regime") or "—"
    updated = defcon_block.get("updated_at") or "—"
    halt = " · <b>롱 신규 차단</b>" if lvl <= 2 else ""
    bar = "🔴" * max(0, 6 - lvl) + "🟢" * min(2, max(0, lvl - 3))
    return (
        f"🛰️ <b>[0/9] 둠스데이 레이더</b> {market_icon}\n"
        f"DEFCON <b>{lvl}</b>/5 {bar}{halt}\n"
        f"레짐 <code>{_esc(reg)}</code> | Global <b>{g}</b> · KR <b>{kr}</b>\n"
        f"<i>동기화 { _esc(updated) }</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
    )


def format_short_sleeve_html(ctx: dict[str, Any]) -> str:
    """ctx from report_collectors.collect_short_sleeve_context."""
    market_icon = ctx.get("market_icon", "")
    mode = ctx.get("inverse_mode_active", False)
    mode_s = "ON ✅" if mode else "OFF"
    lines = [
        f"🩳 <b>[0b/9] 숏·인버스 슬리브</b> {market_icon}",
        f"INVERSE_MODE_ACTIVE: <b>{mode_s}</b>",
    ]
    tail = ctx.get("tail_balance")
    if tail is not None:
        cur = "원" if ctx.get("market") == "KR" else "USD"
        lines.append(f"테일 리스크 펀드: <b>{tail:,.0f}</b>{cur}")

    for tr in ctx.get("triggers") or []:
        code = _esc(tr.get("code", "?"))
        met = tr.get("trigger_met", False)
        r5 = tr.get("hedge_5d_ret")
        thr = tr.get("threshold")
        icon = "✅" if met else "⬜"
        r5s = f"{r5:+.2f}%" if r5 is not None else "N/A"
        lines.append(f"{icon} <code>{code}</code> hedge5d={r5s} (≤{thr}%)")

    opens = ctx.get("open_inverse") or []
    if opens:
        lines.append("<b>OPEN 인버스:</b>")
        for o in opens[:4]:
            lines.append(
                f"  · <code>{_esc(o.get('code'))}</code> "
                f"{float(o.get('invest', 0) or 0):,.0f}"
            )
    else:
        lines.append("<i>OPEN 인버스 없음</i>")

    ex = ctx.get("execution") or {}
    ex_line = ex.get("summary_line")
    if ex_line:
        lines.append(f"⚙️ <b>실행:</b> {_esc(ex_line)}")
    sk = ex.get("skipped")
    ent = ex.get("entered")
    if ent and isinstance(ent, dict):
        lines.append(
            f"  ↳ 진입 <code>{_esc(ent.get('code'))}</code> "
            f"{float(ent.get('invest', 0) or 0):,.0f}"
        )
    elif sk:
        lines.append(f"  ↳ 스킵: {_esc(sk)}")

    lines.append("━━━━━━━━━━━━━━━━━━━━\n")
    return "\n".join(lines)
