"""
PIL — Bitget PRACT_01~30 실무자 리포트 어댑터.
"""
from __future__ import annotations

import re
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import pytz

from practitioner_intelligence import (
    PractitionerBrief,
    build_practitioner_brief,
    format_practitioner_brief_html,
)
from practitioner_penalty_bridge import apply_pil_vitality_penalties


def _extract_practitioner_key(sig_type: object) -> str:
    m = re.search(r"(PRACT_\d{2})", str(sig_type or ""), re.IGNORECASE)
    return m.group(1).upper() if m else "UNKNOWN"


def _map_bitget_features(df: pd.DataFrame) -> pd.DataFrame:
    """ReportFeatureAnalyzer 호환 컬럼."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if "v_cpv" in out.columns and "dyn_cpv" not in out.columns:
        out["dyn_cpv"] = pd.to_numeric(out["v_cpv"], errors="coerce")
    if "v_yang" in out.columns and "dyn_tb" not in out.columns:
        out["dyn_tb"] = pd.to_numeric(out["v_yang"], errors="coerce")
    if "v_rs" in out.columns and "dyn_rs" not in out.columns:
        out["dyn_rs"] = pd.to_numeric(out["v_rs"], errors="coerce")
    if "v_energy" in out.columns:
        out["v_energy"] = pd.to_numeric(out["v_energy"], errors="coerce")
    if "symbol" in out.columns and "name" not in out.columns:
        out["name"] = out["symbol"].astype(str)
    if "symbol" in out.columns and "code" not in out.columns:
        out["code"] = out["symbol"].astype(str)
    return out


def _valid_open_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    st = df["status"].astype(str).str.upper()
    live = st.str.contains("OPEN", na=False) & ~st.str.contains("CLOSED", na=False)
    qty = pd.to_numeric(df.get("quantity"), errors="coerce").fillna(0)
    return live & (qty > 0)


def _safe_ret(val: object) -> float:
    r = pd.to_numeric(val, errors="coerce")
    if r is None or (isinstance(r, float) and not pd.notna(r)):
        return 0.0
    try:
        f = float(r)
    except (TypeError, ValueError):
        return 0.0
    return f if pd.notna(f) else 0.0


def _win_loss_flat(ret_series: pd.Series):
    r = pd.to_numeric(ret_series, errors="coerce").fillna(0.0)
    return int((r > 0).sum()), int((r < 0).sum()), int((r == 0).sum())


def _exit_day(val: object) -> str:
    s = str(val or "").strip()
    return s[:10] if len(s) >= 10 else s


def _format_exit(reason: object) -> str:
    s = str(reason or "").strip()
    return s if s else "사유 미기록"


def send_bitget_practitioner_reports_pil(
    *,
    db_path: str,
    send_telegram_msg,
    load_system_config,
    load_meta_state_resolved,
    base_seed_usdt: float = 10000.0,
) -> Dict[str, Any]:
    """
    Bitget spot/futures × PRACT_xx — PIL 리포트 + 메타 페널티 배치.
    """
    cfg = load_system_config() if callable(load_system_config) else {}
    try:
        meta = load_meta_state_resolved() if callable(load_meta_state_resolved) else {}
    except Exception:
        meta = {}

    briefs: List[PractitionerBrief] = []
    tz = pytz.timezone("UTC")
    today_utc = datetime.now(tz).strftime("%Y-%m-%d")

    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    try:
        for market_type in ("spot", "futures"):
            df_all = pd.read_sql(
                """
                SELECT *
                FROM bitget_forward_trades
                WHERE market_type=? AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
                ORDER BY id DESC
                LIMIT 2500
                """,
                conn,
                params=(market_type,),
            )
            if df_all.empty:
                continue

            df_all = _map_bitget_features(df_all)
            df_all["practitioner_key"] = df_all["sig_type"].apply(_extract_practitioner_key)
            pil_market = f"BG_{market_type.upper()}" if market_type else "BG"
            icon = "🟢" if market_type == "spot" else "🟠"

            for p_key in sorted(k for k in df_all["practitioner_key"].unique() if k != "UNKNOWN"):
                g_all = df_all[df_all["practitioner_key"] == p_key].copy()
                if g_all.empty:
                    continue

                g_closed = g_all[g_all["status"].astype(str).str.contains("CLOSED", na=False)].copy()
                if "exit_date" in g_closed.columns:
                    g_closed["_exit_day"] = g_closed["exit_date"].map(_exit_day)
                    g_today = g_closed[g_closed["_exit_day"] == today_utc].copy()
                else:
                    g_today = g_closed.iloc[0:0].copy()

                valid_open = _valid_open_mask(g_all)
                sample_sig = str(g_all["sig_type"].iloc[0])

                brief = build_practitioner_brief(
                    market=pil_market,
                    group_key=p_key,
                    sample_sig=sample_sig,
                    g_all=g_all,
                    g_closed=g_closed,
                    g_today_closed=g_today,
                    sys_config=cfg if isinstance(cfg, dict) else {},
                    meta=meta,
                    base_seed=float(base_seed_usdt),
                    market_icon=icon,
                    mkt_today_str=today_utc,
                    valid_open_mask=valid_open,
                    format_exit_reason=_format_exit,
                    safe_ret_fn=_safe_ret,
                    win_loss_fn=_win_loss_flat,
                )
                brief.venue_label = market_type.upper()
                brief.currency_suffix = "USDT"
                briefs.append(brief)

                msg = format_practitioner_brief_html(brief)
                real_df = pd.read_sql(
                    """
                    SELECT realized_ret_pct, notional_usdt
                    FROM bitget_real_execution
                    WHERE market_type=? AND practitioner_key=?
                    ORDER BY id DESC LIMIT 200
                    """,
                    conn,
                    params=(market_type, p_key),
                )
                if not real_df.empty:
                    rr = pd.to_numeric(real_df["realized_ret_pct"], errors="coerce").fillna(0.0)
                    nn = pd.to_numeric(real_df["notional_usdt"], errors="coerce").fillna(0.0)
                    msg += (
                        f"\n💸 <b>실전 체결</b>: 평균 {float(rr.mean()):+.2f}% · "
                        f"노셔널 {float(nn.sum()):,.1f} USDT · n={len(real_df)}\n"
                    )
                send_telegram_msg(msg)
                time.sleep(0.35)
    finally:
        conn.close()

    pen: Dict[str, Any] = {}
    if briefs:
        pen = apply_pil_vitality_penalties(briefs, cfg if isinstance(cfg, dict) else {})
    return {"briefs": len(briefs), "penalties": pen}
