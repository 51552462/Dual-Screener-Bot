"""Bitget weekly proprietary regime (Shadow PRI) — SPOT/FUTURES from forward_trades."""
from __future__ import annotations

import html
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from bitget.infra.data_paths import bitget_data_dir
from bitget.forward.shared import DB_PATH

SHADOW_FILENAME = "BITGET_PROPRIETARY_REGIME_SHADOW.json"
REGIME_Z_UP = 0.35
REGIME_Z_DOWN = -0.35


def shadow_pri_path() -> str:
    return os.path.join(bitget_data_dir(), SHADOW_FILENAME)


def _save_shadow(payload: Dict[str, Any]) -> str:
    p = shadow_pri_path()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".pri_bg_", suffix=".json", dir=os.path.dirname(p) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    return p


def load_weekly_shadow_pri() -> Dict[str, Any]:
    p = shadow_pri_path()
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


def _compute_market_pri(
    conn: sqlite3.Connection,
    market_type: str,
    week_start: str,
    week_end: str,
) -> Dict[str, Any]:
    mk = str(market_type).lower()
    closed = conn.execute(
        """
        SELECT final_ret, sim_kelly_invest, status
        FROM bitget_forward_trades
        WHERE market_type=? AND exit_date >= ? AND exit_date <= ?
          AND status LIKE 'CLOSED%' AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        """,
        (mk, week_start, week_end),
    ).fetchall()
    n_closed = len(closed)
    wins = sum(1 for r in closed if float(r[0] or 0) > 0)
    pass_rate = (wins / n_closed) if n_closed else 0.5
    rets = [float(r[0] or 0) for r in closed]
    avg_ret = sum(rets) / n_closed if n_closed else 0.0
    mfe = max(rets) if rets else 0.0
    mae = min(rets) if rets else 0.0
    vol_proxy = (mfe - mae) / 10.0 if rets else 0.0

    n_open = conn.execute(
        "SELECT COUNT(*) FROM bitget_forward_trades WHERE market_type=? AND status='OPEN'",
        (mk,),
    ).fetchone()[0]

    z_pass = (pass_rate - 0.5) * 2.0
    z_ret = max(-2.0, min(2.0, avg_ret / 5.0))
    z_vol = max(-2.0, min(2.0, vol_proxy))
    composite = 0.4 * z_pass + 0.4 * z_ret + 0.2 * z_vol
    pri = max(0.0, min(100.0, 50.0 + 15.0 * composite))

    if composite >= REGIME_Z_UP:
        regime = "UP"
    elif composite <= REGIME_Z_DOWN:
        regime = "DOWN"
    else:
        regime = "SIDEWAYS"

    cold = n_closed < 3
    bullets = []
    if n_closed:
        bullets.append(f"청산 {n_closed}건 · 승률 {pass_rate*100:.0f}% · 평균 {avg_ret:+.2f}%")
    if n_open:
        bullets.append(f"OPEN {n_open}건 유지")
    if cold:
        bullets.append("콜드스타트 — 표본 부족")

    shadow_pnl = avg_ret * 0.15 if n_closed else 0.0
    return {
        "pri_score": round(pri, 2),
        "regime": regime,
        "composite_z": round(composite, 4),
        "sample_counts": {"closed_trades": n_closed, "open_positions": int(n_open or 0)},
        "narrative_bullets": bullets,
        "shadow_pnl_delta_pct": round(shadow_pnl, 4),
        "cold_start": cold,
    }


def compute_weekly_coin_pri(
    *,
    week_start: Optional[str] = None,
    week_end: Optional[str] = None,
    markets: Tuple[str, ...] = ("spot", "futures"),
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    if not week_end:
        week_end = now.strftime("%Y-%m-%d")
    if not week_start:
        week_start = (now - timedelta(days=6)).strftime("%Y-%m-%d")

    results: Dict[str, Any] = {
        "shadow_mode": True,
        "schema_version": 1,
        "week_start": week_start,
        "week_end": week_end,
        "computed_at_utc": now.isoformat(),
        "markets": {},
    }
    path = DB_PATH
    if not path or not os.path.isfile(path):
        results["error"] = "no_db"
        _save_shadow(results)
        return results

    conn = sqlite3.connect(path, timeout=60)
    try:
        for mk in markets:
            results["markets"][mk.upper()] = _compute_market_pri(conn, mk, week_start, week_end)
    finally:
        conn.close()

    spot = results["markets"].get("SPOT", {})
    fut = results["markets"].get("FUTURES", {})
    blend_z = 0.5 * float(spot.get("composite_z") or 0) + 0.5 * float(fut.get("composite_z") or 0)
    if blend_z >= REGIME_Z_UP:
        blend_regime = "UP"
    elif blend_z <= REGIME_Z_DOWN:
        blend_regime = "DOWN"
    else:
        blend_regime = "SIDEWAYS"
    results["blended"] = {
        "pri_score": round(max(0.0, min(100.0, 50.0 + 15.0 * blend_z)), 2),
        "regime": blend_regime,
        "composite_z": round(blend_z, 4),
        "shadow_pnl_delta_pct": round(
            float(spot.get("shadow_pnl_delta_pct") or 0) + float(fut.get("shadow_pnl_delta_pct") or 0),
            4,
        ),
    }
    _save_shadow(results)
    return results


def build_weekly_shadow_pri_html(
    *,
    week_start: str,
    week_end: str,
    spot_week_pnl: Optional[float] = None,
    futures_week_pnl: Optional[float] = None,
) -> str:
    data = load_weekly_shadow_pri()
    if not data or not data.get("markets"):
        try:
            data = compute_weekly_coin_pri(week_start=week_start, week_end=week_end)
        except Exception as ex:
            return f"\n🕵️ <i>[Shadow PRI] 스킵: {html.escape(str(ex)[:72], quote=False)}</i>\n"

    blended = data.get("blended") or {}
    regime = html.escape(str(blended.get("regime") or "SIDEWAYS"), quote=False)
    pri = float(blended.get("pri_score") or 50.0)
    delta = float(blended.get("shadow_pnl_delta_pct") or 0.0)
    sign = "+" if delta >= 0 else ""

    out = (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🕵️ <b>[주간 내부 국면 (Shadow PRI) · Bitget]</b>\n"
        f"📅 {html.escape(week_start, quote=False)} ~ {html.escape(week_end, quote=False)}\n"
        f"▪️ <b>종합:</b> {regime} · PRI <b>{pri:.1f}</b>/100\n"
    )
    for mk, icon in (("SPOT", "🟢"), ("FUTURES", "🟠")):
        block = data.get("markets", {}).get(mk) or {}
        if not block:
            continue
        cs = " <i>(콜드스타트)</i>" if block.get("cold_start") else ""
        out += f"▪️ {icon} <b>{mk}</b> {block.get('regime', '')} PRI {float(block.get('pri_score') or 0):.1f}{cs}\n"
        for b in (block.get("narrative_bullets") or [])[:3]:
            out += f"   · {html.escape(str(b), quote=False)}\n"
    out += f"▪️ <b>가상 시뮬:</b> PRI 연동 시 주간 수익률 <b>{sign}{delta:.2f}%p</b> 추정\n"
    bits = []
    if spot_week_pnl is not None:
        bits.append(f"SPOT {spot_week_pnl:+,.2f} USDT")
    if futures_week_pnl is not None:
        bits.append(f"FUT {futures_week_pnl:+,.2f} USDT")
    if bits:
        out += f"▪️ <i>Flow 대조: {' · '.join(bits)}</i>\n"
    return out
