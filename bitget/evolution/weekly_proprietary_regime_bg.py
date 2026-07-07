"""Bitget weekly proprietary regime (Shadow PRI) — funnel + friction + ledger SSOT."""
from __future__ import annotations

import html
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from bitget.infra.data_paths import bitget_data_dir
from bitget.infra.proprietary_friction_store_bg import normalize_friction_market
from bitget.forward.shared import DB_PATH

SHADOW_FILENAME = "BITGET_PROPRIETARY_REGIME_SHADOW.json"
MIN_SAMPLES_FULL = 5
MIN_SAMPLES_ANY = 2
REGIME_Z_UP = 0.35
REGIME_Z_DOWN = -0.35

_COMPONENT_WEIGHTS: Dict[str, float] = {
    "pass_rate_trend": 0.22,
    "mfe_level": 0.24,
    "mae_stress": 0.18,
    "closed_vol": 0.14,
    "dm_a_pressure": 0.12,
    "starvation": 0.10,
}


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


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None


def _safe_z(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < 2:
        return 0.0
    mu = float(s.mean())
    sd = float(s.std(ddof=0))
    if sd <= 1e-9:
        return 0.0
    return float((s.iloc[-1] - mu) / sd)


def _decay_z(z: float, n: int) -> float:
    w = min(1.0, max(0.0, float(n) / float(MIN_SAMPLES_FULL)))
    return float(z * w)


def _load_funnel_week(conn: sqlite3.Connection, market: str, week_start: str, week_end: str) -> pd.DataFrame:
    if not _table_exists(conn, "scan_funnel_snapshot"):
        return pd.DataFrame()
    mk = normalize_friction_market(market)
    return pd.read_sql(
        """
        SELECT ts, market, universe_size, survivors, pass_rate_pct
        FROM scan_funnel_snapshot
        WHERE market=? AND substr(ts,1,10) >= ? AND substr(ts,1,10) <= ?
        ORDER BY ts ASC
        """,
        conn,
        params=(mk, week_start, week_end),
    )


def _load_friction_events(conn: sqlite3.Connection, market: str, week_start: str, week_end: str) -> pd.DataFrame:
    if not _table_exists(conn, "regime_friction_event"):
        return pd.DataFrame()
    mk = normalize_friction_market(market)
    return pd.read_sql(
        """
        SELECT date, market, event_type
        FROM regime_friction_event
        WHERE market=? AND date >= ? AND date <= ?
        ORDER BY date ASC
        """,
        conn,
        params=(mk, week_start, week_end),
    )


def _ledger_week_metrics(
    conn: sqlite3.Connection,
    market_type: str,
    week_start: str,
    week_end: str,
) -> Dict[str, Any]:
    mk = str(market_type).lower()
    empty = {
        "n_open": 0,
        "n_closed": 0,
        "avg_mfe_live": None,
        "avg_mae_live": None,
        "closed_ret_std": None,
        "closed_mean_ret": None,
    }
    if not _table_exists(conn, "bitget_forward_trades"):
        return empty

    df_open = pd.read_sql(
        """
        SELECT entry_price, max_high, min_low, position_side
        FROM bitget_forward_trades
        WHERE market_type=? AND status='OPEN' AND entry_price IS NOT NULL AND entry_price > 0
        """,
        conn,
        params=(mk,),
    )
    df_closed = pd.read_sql(
        """
        SELECT final_ret, exit_date
        FROM bitget_forward_trades
        WHERE market_type=? AND status LIKE 'CLOSED%'
          AND substr(COALESCE(NULLIF(TRIM(exit_date),''), ''),1,10) >= ?
          AND substr(COALESCE(NULLIF(TRIM(exit_date),''), ''),1,10) <= ?
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        """,
        conn,
        params=(mk, week_start, week_end),
    )

    mfe_live: list[float] = []
    mae_live: list[float] = []
    if not df_open.empty:
        ep = pd.to_numeric(df_open["entry_price"], errors="coerce")
        mh = pd.to_numeric(df_open.get("max_high"), errors="coerce")
        ml = pd.to_numeric(df_open.get("min_low"), errors="coerce")
        side = df_open.get("position_side", "LONG").astype(str).str.upper()
        for i in range(len(df_open)):
            e = float(ep.iloc[i]) if pd.notna(ep.iloc[i]) else 0.0
            if e <= 0:
                continue
            hi = float(mh.iloc[i]) if pd.notna(mh.iloc[i]) else e
            lo = float(ml.iloc[i]) if pd.notna(ml.iloc[i]) else e
            if side.iloc[i] == "SHORT":
                mfe_live.append(((e - lo) / e) * 100.0)
                mae_live.append(((e - hi) / e) * 100.0)
            else:
                mfe_live.append(((hi - e) / e) * 100.0)
                mae_live.append(((lo - e) / e) * 100.0)

    closed_ret = pd.to_numeric(df_closed.get("final_ret"), errors="coerce").dropna()
    return {
        "n_open": int(len(df_open)),
        "n_closed": int(len(closed_ret)),
        "avg_mfe_live": float(np.mean(mfe_live)) if mfe_live else None,
        "avg_mae_live": float(np.mean(mae_live)) if mae_live else None,
        "closed_ret_std": float(closed_ret.std(ddof=0)) if len(closed_ret) >= 2 else None,
        "closed_mean_ret": float(closed_ret.mean()) if len(closed_ret) else None,
    }


def _compute_market_pri(
    conn: sqlite3.Connection,
    market_type: str,
    week_start: str,
    week_end: str,
    *,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    funnel = _load_funnel_week(conn, market_type, week_start, week_end)
    events = _load_friction_events(conn, market_type, week_start, week_end)
    ledger = _ledger_week_metrics(conn, market_type, week_start, week_end)

    n_funnel = int(len(funnel))
    if n_funnel >= MIN_SAMPLES_ANY and "pass_rate_pct" in funnel.columns:
        pr = pd.to_numeric(funnel["pass_rate_pct"], errors="coerce").dropna()
        z_pass = _decay_z(_safe_z(pr), n_funnel)
    else:
        z_pass = 0.0

    n_open = int(ledger["n_open"])
    avg_mfe = ledger.get("avg_mfe_live")
    z_mfe = _decay_z((float(avg_mfe) - 3.0) / 3.0, n_open) if avg_mfe is not None and n_open >= MIN_SAMPLES_ANY else 0.0

    avg_mae = ledger.get("avg_mae_live")
    z_mae = _decay_z((float(avg_mae) + 2.0) / 2.5, n_open) if avg_mae is not None and n_open >= MIN_SAMPLES_ANY else 0.0

    n_closed = int(ledger["n_closed"])
    cstd = ledger.get("closed_ret_std")
    z_vol = _decay_z((float(cstd) - 4.0) / 4.0, n_closed) if cstd is not None and n_closed >= MIN_SAMPLES_ANY else 0.0

    dm_a = 0
    if not events.empty:
        dm_a = int(events["event_type"].astype(str).str.contains("DM_A", na=False).sum())
    z_dm = _decay_z(-float(dm_a) / 2.0, dm_a) if dm_a > 0 else 0.0

    try:
        from bitget.evolution.elastic_threshold_bg import BitgetElasticThreshold

        _et = BitgetElasticThreshold(cfg or {}, market_type)
        starvation = float(_et.compute_starvation_index(lookback_days=7))
        vol_proxy = float(_et.volatility_proxy())
    except Exception:
        starvation = 0.5
        if n_open + n_closed > 0:
            starvation = n_open / max(1, n_open + n_closed)
        vol_proxy = 1.0
    z_starv = _decay_z(-(float(starvation) - 0.5) / 0.35, 7)
    if vol_proxy > 1.15:
        z_starv = float(z_starv * 0.85)

    components = {
        "pass_rate_trend": z_pass,
        "mfe_level": z_mfe,
        "mae_stress": z_mae,
        "closed_vol": z_vol,
        "dm_a_pressure": z_dm,
        "starvation": z_starv,
    }
    composite = sum(components[k] * _COMPONENT_WEIGHTS[k] for k in _COMPONENT_WEIGHTS)
    pri = float(np.clip(50.0 + 15.0 * composite, 0.0, 100.0))

    if composite >= REGIME_Z_UP:
        regime = "UP"
    elif composite <= REGIME_Z_DOWN:
        regime = "DOWN"
    else:
        regime = "SIDEWAYS"

    bullets: list[str] = []
    if n_funnel >= MIN_SAMPLES_ANY and not funnel.empty:
        pr_mean = float(pd.to_numeric(funnel["pass_rate_pct"], errors="coerce").mean())
        bullets.append(f"주간 평균 스캔 통과율 {pr_mean:.2f}% (표본 {n_funnel}슬롯)")
    if dm_a:
        bullets.append(f"DM-A(데스매치 청산 0건) {dm_a}회")
    if avg_mfe is not None:
        bullets.append(f"OPEN 평균 MFE {avg_mfe:+.2f}% (n={n_open})")
    if n_closed:
        bullets.append(f"청산 {n_closed}건 · 평균 {ledger.get('closed_mean_ret', 0):+.2f}%")

    actual = float(ledger.get("closed_mean_ret") or 0.0)
    regime_mult = {"UP": 1.08, "DOWN": 0.92, "SIDEWAYS": 1.0}.get(regime, 1.0)
    shadow = actual * regime_mult if n_closed >= MIN_SAMPLES_ANY else 0.0
    cold = n_funnel < MIN_SAMPLES_FULL and n_closed < MIN_SAMPLES_FULL

    return {
        "pri_score": round(pri, 2),
        "regime": regime,
        "composite_z": round(composite, 4),
        "components": {k: round(v, 4) for k, v in components.items()},
        "sample_counts": {
            "funnel_slots": n_funnel,
            "closed_trades": n_closed,
            "open_positions": n_open,
            "dm_a_events": dm_a,
            "starvation_index": round(float(starvation), 4),
            "vol_proxy": round(float(vol_proxy), 4),
        },
        "narrative_bullets": bullets,
        "shadow_pnl_delta_pct": round((shadow - actual) if n_closed >= MIN_SAMPLES_ANY else 0.0, 4),
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
        "schema_version": 2,
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

    try:
        from bitget.infra.config_manager import load_system_config

        cfg = load_system_config() or {}
    except Exception:
        cfg = {}

    conn = sqlite3.connect(path, timeout=60)
    try:
        for mk in markets:
            results["markets"][mk.upper()] = _compute_market_pri(
                conn, mk, week_start, week_end, cfg=cfg
            )
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
        "pri_score": round(float(np.clip(50.0 + 15.0 * blend_z, 0, 100)), 2),
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
    try:
        from bitget.evolution.regime_analog_bg import format_regime_analog_brief

        analog_line = format_regime_analog_brief()
        if analog_line:
            out += analog_line
    except Exception:
        pass
    return out
