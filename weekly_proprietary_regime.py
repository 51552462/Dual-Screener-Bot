"""
Proprietary Regime Index (PRI) — 주간 일괄 산출 · Shadow only · 외부 지표 제거.

평일: proprietary_friction_store + forward_trades 적재만.
토요일/weekly: 본 모듈이 PRI·3-State·가상 PnL 델타 산출.
"""
from __future__ import annotations

import html
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

from factory_data_paths import factory_data_dir
from proprietary_friction_store import ensure_proprietary_friction_schema

logger = logging.getLogger(__name__)

SHADOW_PRI_FILENAME = "PROPRIETARY_REGIME_WEEKLY_SHADOW.json"
MIN_SAMPLES_FULL = 5
MIN_SAMPLES_ANY = 2
REGIME_Z_UP = 0.45
REGIME_Z_DOWN = -0.45

_COMPONENT_WEIGHTS: Dict[str, float] = {
    "pass_rate_trend": 0.22,
    "mfe_level": 0.24,
    "mae_stress": 0.18,
    "closed_vol": 0.14,
    "dm_a_pressure": 0.12,
    "starvation": 0.10,
}


def _db_path() -> str:
    try:
        from market_db_paths import MARKET_DATA_DB_PATH

        return MARKET_DATA_DB_PATH
    except Exception:
        from forward.shared import DB_PATH

        return DB_PATH


def shadow_pri_path() -> str:
    return os.path.join(factory_data_dir(), SHADOW_PRI_FILENAME)


def _decay_z(z: float, n: int, *, min_full: int = MIN_SAMPLES_FULL) -> float:
    """콜드 스타트: 표본 부족 시 Z → 0(중립)으로 수렴."""
    if n < MIN_SAMPLES_ANY:
        return 0.0
    w = min(1.0, float(n) / float(max(1, min_full)))
    return float(z) * w


def _safe_z(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < MIN_SAMPLES_ANY:
        return 0.0
    mu = float(s.mean())
    sd = float(s.std(ddof=0))
    if sd < 1e-9:
        return 0.0
    return (float(s.iloc[-1]) - mu) / sd


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _load_funnel_week(
    conn: sqlite3.Connection,
    market: str,
    week_start: str,
    week_end: str,
) -> pd.DataFrame:
    if not _table_exists(conn, "scan_funnel_snapshot"):
        return pd.DataFrame()
    return pd.read_sql(
        """
        SELECT ts, market, universe_size, survivors, pass_rate_pct
        FROM scan_funnel_snapshot
        WHERE market=? AND substr(ts,1,10) >= ? AND substr(ts,1,10) <= ?
        ORDER BY ts ASC
        """,
        conn,
        params=(market, week_start, week_end),
    )


def _load_friction_events(
    conn: sqlite3.Connection,
    market: str,
    week_start: str,
    week_end: str,
) -> pd.DataFrame:
    if not _table_exists(conn, "regime_friction_event"):
        return pd.DataFrame()
    return pd.read_sql(
        """
        SELECT date, market, event_type
        FROM regime_friction_event
        WHERE market=? AND date >= ? AND date <= ?
        ORDER BY date ASC
        """,
        conn,
        params=(market, week_start, week_end),
    )


def _ledger_week_metrics(
    conn: sqlite3.Connection,
    market: str,
    week_start: str,
    week_end: str,
) -> Dict[str, Any]:
    empty = {
        "n_open": 0,
        "n_closed": 0,
        "avg_mfe_live": None,
        "avg_mae_live": None,
        "closed_ret_std": None,
        "avg_closed_mfe": None,
        "closed_mean_ret": None,
    }
    if not _table_exists(conn, "forward_trades"):
        return empty
    df_open = pd.read_sql(
        """
        SELECT entry_price, max_high, min_low, mfe, bars_held
        FROM forward_trades
        WHERE UPPER(TRIM(market))=? AND status='OPEN'
          AND entry_price IS NOT NULL AND entry_price > 0
        """,
        conn,
        params=(market,),
    )
    df_closed = pd.read_sql(
        """
        SELECT final_ret, mfe, exit_date
        FROM forward_trades
        WHERE UPPER(TRIM(market))=? AND status LIKE 'CLOSED%%'
          AND substr(COALESCE(NULLIF(TRIM(exit_date),''), ''),1,10) >= ?
          AND substr(COALESCE(NULLIF(TRIM(exit_date),''), ''),1,10) <= ?
        """,
        conn,
        params=(market, week_start, week_end),
    )

    mfe_live: list[float] = []
    mae_live: list[float] = []
    if not df_open.empty:
        ep = pd.to_numeric(df_open["entry_price"], errors="coerce")
        mh = pd.to_numeric(df_open.get("max_high"), errors="coerce")
        ml = pd.to_numeric(df_open.get("min_low"), errors="coerce")
        mfe_live = (((mh - ep) / ep) * 100.0).replace([np.inf, -np.inf], np.nan).dropna().tolist()
        mae_live = (((ml - ep) / ep) * 100.0).replace([np.inf, -np.inf], np.nan).dropna().tolist()

    closed_ret = pd.to_numeric(df_closed.get("final_ret"), errors="coerce").dropna()
    closed_mfe = pd.to_numeric(df_closed.get("mfe"), errors="coerce").dropna()

    return {
        "n_open": int(len(df_open)),
        "n_closed": int(len(closed_ret)),
        "avg_mfe_live": float(np.mean(mfe_live)) if mfe_live else None,
        "avg_mae_live": float(np.mean(mae_live)) if mae_live else None,
        "closed_ret_std": float(closed_ret.std(ddof=0)) if len(closed_ret) >= 2 else None,
        "avg_closed_mfe": float(closed_mfe.mean()) if len(closed_mfe) else None,
        "closed_mean_ret": float(closed_ret.mean()) if len(closed_ret) else None,
    }


def internal_ledger_volatility_proxy(market: str, *, lookback_days: int = 20) -> float:
    """
    ElasticThreshold 대체 — OPEN MFE 분산 + 청산 수익률 롤링 σ (외부 지수 없음).
  1.0=중립, >1 확대, <1 수축.
    """
    path = _db_path()
    if not path or not os.path.isfile(path):
        return 1.0
    mk = str(market or "KR").upper()
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(path, timeout=20)
        try:
            df_o = pd.read_sql(
                """
                SELECT entry_price, max_high FROM forward_trades
                WHERE UPPER(TRIM(market))=? AND status='OPEN'
                  AND entry_price > 0
                """,
                conn,
                params=(mk,),
            )
            df_c = pd.read_sql(
                """
                SELECT final_ret FROM forward_trades
                WHERE UPPER(TRIM(market))=? AND status LIKE 'CLOSED%%'
                  AND substr(COALESCE(exit_date,''),1,10) >= ?
                """,
                conn,
                params=(mk, since),
            )
        finally:
            conn.close()
        parts: list[float] = []
        if not df_o.empty:
            ep = pd.to_numeric(df_o["entry_price"], errors="coerce")
            mh = pd.to_numeric(df_o["max_high"], errors="coerce")
            mfe = ((mh - ep) / ep * 100.0).dropna()
            if len(mfe) >= 2:
                parts.append(float(mfe.std(ddof=0)))
        rets = pd.to_numeric(df_c["final_ret"], errors="coerce").dropna()
        if len(rets) >= 3:
            parts.append(float(rets.std(ddof=0)))
        if not parts:
            return 1.0
        vol = float(np.mean(parts))
        baseline = 3.0 if mk == "KR" else 2.5
        return float(np.clip(vol / max(baseline, 0.5), 0.75, 1.45))
    except Exception:
        return 1.0


@dataclass(frozen=True)
class WeeklyPriResult:
    week_start: str
    week_end: str
    market: str
    pri_score: float
    regime: str  # UP | DOWN | SIDEWAYS
    composite_z: float
    components: Dict[str, float]
    sample_counts: Dict[str, int]
    narrative_bullets: Tuple[str, ...]
    shadow_pnl_delta_pct: float
    actual_week_pnl_proxy: float
    shadow_week_pnl_proxy: float
    cold_start: bool


def _compute_market_pri(
    conn: sqlite3.Connection,
    market: str,
    week_start: str,
    week_end: str,
) -> WeeklyPriResult:
    mk = str(market).upper()
    funnel = _load_funnel_week(conn, mk, week_start, week_end)
    events = _load_friction_events(conn, mk, week_start, week_end)
    ledger = _ledger_week_metrics(conn, mk, week_start, week_end)

    n_funnel = int(len(funnel))
    if n_funnel >= MIN_SAMPLES_ANY and "pass_rate_pct" in funnel.columns:
        pr = pd.to_numeric(funnel["pass_rate_pct"], errors="coerce").dropna()
        z_pass = _decay_z(_safe_z(pr), n_funnel)
    else:
        z_pass = 0.0

    n_open = int(ledger["n_open"])
    avg_mfe = ledger.get("avg_mfe_live")
    if avg_mfe is not None and n_open >= MIN_SAMPLES_ANY:
        z_mfe = _decay_z((float(avg_mfe) - 3.0) / 3.0, n_open)
    else:
        z_mfe = 0.0

    avg_mae = ledger.get("avg_mae_live")
    if avg_mae is not None and n_open >= MIN_SAMPLES_ANY:
        z_mae = _decay_z((float(avg_mae) + 2.0) / 2.5, n_open)
    else:
        z_mae = 0.0

    n_closed = int(ledger["n_closed"])
    cstd = ledger.get("closed_ret_std")
    if cstd is not None and n_closed >= MIN_SAMPLES_ANY:
        z_vol = _decay_z((float(cstd) - 4.0) / 4.0, n_closed)
    else:
        z_vol = 0.0

    dm_a = int(
        len(events.loc[events["event_type"].astype(str).str.contains("DM_A", na=False)])
        if not events.empty
        else 0
    )
    z_dm = _decay_z(-float(dm_a) / 2.0, dm_a) if dm_a > 0 else 0.0

    try:
        from elastic_threshold import ElasticThreshold
        from config_manager import load_system_config

        cfg = load_system_config() or {}
        starv = ElasticThreshold(cfg, market=mk).compute_starvation_index()
        z_starv = _decay_z(-(float(starv) - 0.5) / 0.35, 7)
    except Exception:
        z_starv = 0.0

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
        if len(pr) >= 3:
            early = float(pr.iloc[: max(1, len(pr) // 3)].mean())
            late = float(pr.iloc[-max(1, len(pr) // 3) :].mean())
            if late < early * 0.5 and early > 0.01:
                bullets.append(
                    f"후반 통과율 급감 ({early:.2f}%→{late:.2f}%) — 마찰 증가"
                )
    if dm_a:
        bullets.append(f"DM-A(데스매치 청산 0건) {dm_a}회")
    if avg_mfe is not None:
        bullets.append(f"OPEN 평균 MFE {avg_mfe:+.2f}% (n={n_open})")
    if avg_mae is not None and avg_mae < -3.0:
        bullets.append(f"OPEN 평균 MAE {avg_mae:+.2f}% — 하방 압력")

    actual = float(ledger.get("closed_mean_ret") or 0.0)
    regime_mult = {"UP": 1.08, "DOWN": 0.92, "SIDEWAYS": 1.0}.get(regime, 1.0)
    shadow = actual * regime_mult if n_closed >= MIN_SAMPLES_ANY else 0.0
    delta = shadow - actual if n_closed >= MIN_SAMPLES_ANY else 0.0

    cold = n_funnel < MIN_SAMPLES_FULL and n_closed < MIN_SAMPLES_FULL

    return WeeklyPriResult(
        week_start=week_start,
        week_end=week_end,
        market=mk,
        pri_score=round(pri, 2),
        regime=regime,
        composite_z=round(composite, 4),
        components={k: round(v, 4) for k, v in components.items()},
        sample_counts={
            "funnel_slots": n_funnel,
            "closed_trades": n_closed,
            "open_positions": n_open,
            "dm_a_events": dm_a,
        },
        narrative_bullets=tuple(bullets[:5]),
        shadow_pnl_delta_pct=round(delta, 4),
        actual_week_pnl_proxy=round(actual, 4),
        shadow_week_pnl_proxy=round(shadow, 4),
        cold_start=cold,
    )


def compute_weekly_proprietary_regime(
    *,
    week_start: Optional[str] = None,
    week_end: Optional[str] = None,
    markets: Tuple[str, ...] = ("KR", "US"),
) -> Dict[str, Any]:
    """주간 PRI 일괄 산출 → shadow JSON (MetaGovernor 미쓰기)."""
    tz = pytz.timezone("Asia/Seoul")
    today = datetime.now(tz).date()
    if not week_end:
        week_end = today.strftime("%Y-%m-%d")
    if not week_start:
        week_start = (today - timedelta(days=6)).strftime("%Y-%m-%d")

    path = _db_path()
    ensure_proprietary_friction_schema(db_path=path)
    results: Dict[str, Any] = {
        "shadow_mode": True,
        "schema_version": 1,
        "week_start": week_start,
        "week_end": week_end,
        "computed_at_kst": datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S"),
        "markets": {},
    }

    if not path or not os.path.isfile(path):
        results["error"] = "no_db"
        _save_shadow(results)
        return results

    conn = sqlite3.connect(path, timeout=60)
    try:
        for mk in markets:
            r = _compute_market_pri(conn, mk, week_start, week_end)
            results["markets"][mk] = {
                "pri_score": r.pri_score,
                "regime": r.regime,
                "composite_z": r.composite_z,
                "components": r.components,
                "sample_counts": r.sample_counts,
                "narrative_bullets": list(r.narrative_bullets),
                "shadow_pnl_delta_pct": r.shadow_pnl_delta_pct,
                "actual_week_pnl_proxy": r.actual_week_pnl_proxy,
                "shadow_week_pnl_proxy": r.shadow_week_pnl_proxy,
                "cold_start": r.cold_start,
            }
    finally:
        conn.close()

    kr = results["markets"].get("KR", {})
    us = results["markets"].get("US", {})
    z_kr = float(kr.get("composite_z") or 0)
    z_us = float(us.get("composite_z") or 0)
    blend = 0.5 * z_kr + 0.5 * z_us
    if blend >= REGIME_Z_UP:
        blend_regime = "UP"
    elif blend <= REGIME_Z_DOWN:
        blend_regime = "DOWN"
    else:
        blend_regime = "SIDEWAYS"
    results["blended"] = {
        "pri_score": round(float(np.clip(50.0 + 15.0 * blend, 0, 100)), 2),
        "regime": blend_regime,
        "composite_z": round(blend, 4),
        "shadow_pnl_delta_pct": round(
            float(kr.get("shadow_pnl_delta_pct") or 0)
            + float(us.get("shadow_pnl_delta_pct") or 0),
            4,
        ),
    }
    _save_shadow(results)
    return results


def _save_shadow(payload: Dict[str, Any]) -> str:
    p = shadow_pri_path()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    tmp = f"{p}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)
    return p


def load_weekly_shadow_pri() -> Dict[str, Any]:
    p = shadow_pri_path()
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def build_weekly_shadow_pri_html(
    *,
    week_start: str,
    week_end: str,
    kr_week_pnl: Optional[float] = None,
    us_week_pnl: Optional[float] = None,
) -> str:
    """주간 마스터 리포트 말미 Shadow PRI 블록."""
    data = load_weekly_shadow_pri()
    if not data or not data.get("markets"):
        try:
            data = compute_weekly_proprietary_regime(
                week_start=week_start, week_end=week_end
            )
        except Exception as ex:
            return (
                f"\n🕵️ <i>[주간 내부 국면 Shadow PRI] 스킵: "
                f"{html.escape(str(ex)[:72], quote=False)}</i>\n"
            )

    blended = data.get("blended") or {}
    regime = html.escape(str(blended.get("regime") or "SIDEWAYS"), quote=False)
    pri = float(blended.get("pri_score") or 50.0)
    delta = float(blended.get("shadow_pnl_delta_pct") or 0.0)
    sign = "+" if delta >= 0 else ""

    out = (
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🕵️ <b>[주간 내부 국면 (Shadow PRI) 시뮬레이션]</b>\n"
        f"<i>MetaGovernor·실전 튜닝 미연동 · 외부 지수 배제</i>\n"
        f"📅 {html.escape(week_start, quote=False)} ~ {html.escape(week_end, quote=False)}\n"
        f"▪️ <b>종합 국면:</b> {regime} · PRI <b>{pri:.1f}</b>/100\n"
    )

    for mk in ("KR", "US"):
        block = data.get("markets", {}).get(mk) or {}
        if not block:
            continue
        icon = "🇰🇷" if mk == "KR" else "🇺🇸"
        bullets = block.get("narrative_bullets") or []
        cs = " <i>(콜드스타트)</i>" if block.get("cold_start") else ""
        out += (
            f"▪️ {icon} <b>{mk}</b> {html.escape(str(block.get('regime') or ''), quote=False)}"
            f" PRI {float(block.get('pri_score') or 0):.1f}{cs}\n"
        )
        for b in bullets[:3]:
            out += f"   · {html.escape(str(b), quote=False)}\n"

    out += (
        f"▪️ <b>가상 시뮬:</b> PRI 국면을 켈리에 연동했다면 "
        f"주간 평균 수익률 <b>{sign}{delta:.2f}%p</b> 개선 추정"
        f" <i>(청산 표본 프록시)</i>\n"
    )
    if kr_week_pnl is not None or us_week_pnl is not None:
        bits = []
        if kr_week_pnl is not None:
            bits.append(f"KR 실현 {kr_week_pnl:+,.0f}")
        if us_week_pnl is not None:
            bits.append(f"US 실현 {us_week_pnl:+,.0f}")
        out += f"▪️ <i>주간 Flow 대조: {' · '.join(bits)}</i>\n"
    return out
