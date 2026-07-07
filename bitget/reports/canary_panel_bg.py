"""Bitget canary state → daily report panel."""
from __future__ import annotations

import html
import json
import os
from typing import Any, Optional


def load_canary_state(path: Optional[str] = None) -> dict[str, Any]:
    if path is None:
        try:
            from bitget.infra.data_paths import canary_state_path

            path = canary_state_path()
        except Exception:
            return {}
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def format_canary_panel_html(state: Optional[dict[str, Any]] = None) -> str:
    st = state if isinstance(state, dict) else load_canary_state()
    if not st:
        return ""
    stress = st.get("crypto_liquidity_stress")
    contagion = st.get("macro_contagion_risk")
    comps = st.get("components") if isinstance(st.get("components"), dict) else {}
    syms = comps.get("symbols_used") or []
    btc_ret = comps.get("btc_ret_3d")
    oi_chg = comps.get("oi_change_pct_24h")
    updated = st.get("updated_at") or st.get("ts") or "—"
    lines = [
        "🛰️ <b>[코인 선행 레이더 · Canary]</b>",
        f"▪ 유동성 스트레스: <b>{float(stress or 0):.2f}</b> / 1.0",
        f"▪ 거시 전염 위험: <b>{'ON' if contagion else 'OFF'}</b>",
    ]
    if btc_ret is not None:
        lines.append(f"▪ BTC 3d: {float(btc_ret)*100:+.2f}%")
    if oi_chg is not None:
        lines.append(f"▪ Top5 OI 24h: {float(oi_chg)*100:+.2f}%")
    if syms:
        sym_s = ", ".join(html.escape(str(s), quote=False) for s in syms[:5])
        lines.append(f"▪ 감시 심볼: {sym_s}")
    lines.append(f"<i>갱신 {html.escape(str(updated), quote=False)}</i>\n")
    return "\n".join(lines)
