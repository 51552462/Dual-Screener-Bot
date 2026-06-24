"""
섹터 순환매(PREDICTED_NEXT_SECTOR_*) · 미국 스필오버(US_SPILLOVER_*) SSOT 갱신·표시.

데이터 저장: system_config (SQLite KV) — 별도 spillover_miner DB 없음.
계산 소스: market_data.sqlite forward_trades

갱신 엔진 원본:
- system_auto_pilot.run_autonomous_analysis() 엔진 1.6 (US 고MFE 스필오버)
- system_auto_pilot.run_autonomous_analysis() 엔진 12.5 (Markov 순환매 예측)
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

from market_db_paths import report_db_read_path


from sector_taxonomy import map_standard_sector  # noqa: F401 — SSOT re-export


def _today_kst() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _last_business_day_kst() -> str:
    """주말·일요일이면 직전 금요일(KST) 날짜."""
    tz = pytz.timezone("Asia/Seoul")
    d = datetime.now(tz).date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def spillover_fallback_enabled(cfg: Dict[str, Any]) -> bool:
    v = cfg.get("ENABLE_SPILLOVER_FALLBACK", True)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() not in ("0", "false", "no", "off")


def refresh_us_spillover_from_db(
    cfg: Dict[str, Any],
    db_path: Optional[str] = None,
    *,
    lookback_days: int = 7,
    mfe_min: float = 15.0,
    allow_zero_sample_fallback: bool = True,
) -> Dict[str, Any]:
    """최근 US 고MFE 청산 표본 → US_SPILLOVER_* 갱신. 표본 없으면 LAST_GOOD 유지."""
    db = db_path or report_db_read_path()
    out: Dict[str, Any] = {"updated": False, "sector": None, "as_of": None, "reason": ""}
    if not db or not os.path.isfile(db):
        out["reason"] = "no_db"
        return out

    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(db, timeout=60)
        try:
            df = pd.read_sql(
                """
                SELECT market, status, entry_date, mfe, sector
                FROM forward_trades
                WHERE market = 'US' AND entry_date >= ?
                """,
                conn,
                params=(cutoff,),
            )
        finally:
            conn.close()
    except Exception as e:
        out["reason"] = f"query_error:{e}"
        return out

    if df.empty or not all(c in df.columns for c in ("status", "mfe", "sector")):
        out["reason"] = "no_rows"
        return out

    us_hot = df[
        df["status"].astype(str).str.contains("CLOSED", na=False)
        & (pd.to_numeric(df["mfe"], errors="coerce") >= mfe_min)
    ]
    if us_hot.empty:
        if allow_zero_sample_fallback:
            try:
                from zero_sample_spillover import (
                    infer_dark_horse_sector_from_ohlcv,
                    persist_dark_horse_spillover_cfg,
                )

                dark = infer_dark_horse_sector_from_ohlcv()
                if dark.get("ok"):
                    z = persist_dark_horse_spillover_cfg(cfg, dark, save=False)
                    sector_s = str(z.get("sector") or "").strip()
                    as_of = _today_kst()
                    out.update(
                        {
                            "updated": True,
                            "sector": sector_s,
                            "as_of": as_of,
                            "reason": "zero_sample_dark_horse",
                            "zero_sample": z,
                        }
                    )
                    return out
            except Exception:
                pass
        out["reason"] = "no_hot_sample"
        return out

    top = us_hot.groupby("sector").size().sort_values(ascending=False).index[0]
    sector_s = str(top).strip()
    as_of = _today_kst()
    cfg["US_SPILLOVER_SECTOR"] = sector_s
    cfg["US_SPILLOVER_SECTOR_LAST_GOOD"] = sector_s
    cfg["US_SPILLOVER_SECTOR_AS_OF"] = as_of
    out.update({"updated": True, "sector": sector_s, "as_of": as_of, "reason": "ok"})
    return out


def refresh_predicted_sector_for_market(
    cfg: Dict[str, Any],
    market: str,
    db_path: Optional[str] = None,
    *,
    lookback_days: int = 60,
) -> Dict[str, Any]:
    """PREDICTED_NEXT_SECTOR_{market} — P1 rollup SSOT 우선, 레거시 Markov 폴백."""
    try:
        from sector_rotation_store import refresh_predicted_sector_via_store

        out = refresh_predicted_sector_via_store(cfg, market, db_path=db_path)
        if out.get("updated") or out.get("reason") == "rollup_empty_last_good":
            return out
    except Exception:
        pass

    db = db_path or report_db_read_path()
    mkt = str(market).upper()
    key = f"PREDICTED_NEXT_SECTOR_{mkt}"
    out: Dict[str, Any] = {"market": mkt, "updated": False, "predicted": None, "reason": ""}

    if not db or not os.path.isfile(db):
        out["reason"] = "no_db"
        return out

    cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(db, timeout=60)
        try:
            flow_df = pd.read_sql(
                """
                SELECT entry_date, sector
                FROM forward_trades
                WHERE entry_date >= ? AND market = ?
                ORDER BY entry_date ASC
                """,
                conn,
                params=(cutoff, mkt),
            )
        finally:
            conn.close()
    except Exception as e:
        out["reason"] = f"query_error:{e}"
        return out

    if flow_df.empty:
        out["reason"] = "no_rows"
        return out

    flow_df["sector"] = flow_df["sector"].apply(lambda s: map_standard_sector(s, market=mkt))
    daily_dom = flow_df.groupby("entry_date")["sector"].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else None
    ).dropna()

    transitions: Dict[Tuple[str, str], int] = {}
    prev_sector = None
    sec_path = [str(s).strip() for s in daily_dom.tolist() if str(s).strip()]
    for sec in sec_path:
        if prev_sector is not None and prev_sector != sec:
            k = (prev_sector, sec)
            transitions[k] = transitions.get(k, 0) + 1
        prev_sector = sec

    if not transitions or not sec_path:
        out["reason"] = "no_transitions"
        return out

    states = sorted({a for a, _ in transitions.keys()} | {b for _, b in transitions.keys()} | set(sec_path))
    s2i = {s: i for i, s in enumerate(states)}
    M = np.zeros((len(states), len(states)), dtype=float)
    for (a, b), cnt in transitions.items():
        ia, ib = s2i.get(a), s2i.get(b)
        if ia is None or ib is None:
            continue
        M[ia, ib] += float(cnt)
    for i in range(len(states)):
        row_sum = float(M[i].sum())
        if row_sum > 0:
            M[i] = M[i] / row_sum

    today_state = sec_path[-1]
    predicted = None
    i0 = s2i.get(today_state)
    if i0 is not None:
        M2 = np.matmul(M, M)
        row2 = M2[i0]
        if np.isfinite(row2).any() and float(np.nansum(row2)) > 0:
            predicted = states[int(np.nanargmax(row2))]

    if not predicted:
        predicted = max(transitions.items(), key=lambda kv: kv[1])[0][1]

    cfg[key] = predicted
    cfg[f"PREDICTED_NEXT_SECTOR_{mkt}_AS_OF"] = _today_kst()
    cfg[f"PREDICTED_NEXT_SECTOR_{mkt}_FROM"] = today_state
    cfg[f"PREDICTED_NEXT_SECTOR_{mkt}_LAST_GOOD"] = predicted
    cfg[f"PREDICTED_NEXT_SECTOR_{mkt}_LAST_GOOD_AS_OF"] = _today_kst()
    out.update({"updated": True, "predicted": predicted, "reason": "ok"})
    return out


def refresh_sector_spillover_state(
    *,
    save: bool = True,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """스필오버 + KR/US 순환매 예측 일괄 갱신 후 system_config 저장."""
    from config_manager import load_system_config, save_system_config

    cfg = load_system_config()
    if not isinstance(cfg, dict):
        cfg = {}

    spill = refresh_us_spillover_from_db(cfg, db_path)

    rotation_pipe: Dict[str, Any] = {}
    kr: Dict[str, Any] = {}
    us: Dict[str, Any] = {}
    try:
        from sector_rotation_store import run_sector_rotation_pipeline

        rotation_pipe = run_sector_rotation_pipeline(
            db_path=db_path, cfg=cfg, save_config=False
        )
        kr = (rotation_pipe.get("markets") or {}).get("KR", {}).get("predict", {})
        us = (rotation_pipe.get("markets") or {}).get("US", {}).get("predict", {})
    except Exception as ex:
        rotation_pipe = {"error": str(ex)[:120]}
        kr = refresh_predicted_sector_for_market(cfg, "KR", db_path)
        us = refresh_predicted_sector_for_market(cfg, "US", db_path)

    cross_pub: Dict[str, Any] = {}
    try:
        from cross_market_ssot import publish_us_market_snapshot

        cross_pub = publish_us_market_snapshot(cfg, db_path=db_path, source="sector_spillover_refresh", save=True)
    except Exception as ex:
        cross_pub = {"error": str(ex)[:120]}

    if save:
        save_system_config(cfg)

    return {
        "spillover": spill,
        "predicted_kr": kr,
        "predicted_us": us,
        "saved": save,
        "cross_market": cross_pub,
        "sector_rotation": rotation_pipe,
    }


def resolve_us_spillover_display(cfg: Dict[str, Any]) -> str:
    """
    리포트 [7/9] 스필오버 대괄호 문자열.
    CROSS_MARKET_SSOT 우선 · stale 시 KR 단독 모멘텀 (강제 US 스캔 없음).
    """
    try:
        from cross_market_ssot import resolve_us_spillover_display_v2

        return resolve_us_spillover_display_v2(cfg)
    except Exception:
        pass

    if not spillover_fallback_enabled(cfg):
        cur = str(cfg.get("US_SPILLOVER_SECTOR") or "").strip()
        if cur and cur not in ("분석중", "NONE", ""):
            return cur
        return "데이터 없음"

    cur = str(cfg.get("US_SPILLOVER_SECTOR") or "").strip()
    if cur and cur not in ("분석중", "NONE", ""):
        asof = str(cfg.get("US_SPILLOVER_SECTOR_AS_OF") or "").strip()[:10]
        if asof and asof != _today_kst():
            return f"{cur} (직전 갱신 {asof})"
        return cur

    lg = str(cfg.get("US_SPILLOVER_SECTOR_LAST_GOOD") or "").strip()
    asof = str(cfg.get("US_SPILLOVER_SECTOR_AS_OF") or "").strip()[:10]
    if lg and lg not in ("분석중", "NONE"):
        ref = asof or _last_business_day_kst()
        return f"{lg} (직전 영업일 캐시 {ref})"
    return "데이터 없음"


def resolve_predicted_sector_display(cfg: Dict[str, Any], market: str) -> str:
    """다음 예측 섹터 — 분석중 대신 캐시·DB 기반 표시."""
    mkt = str(market).upper()
    key = f"PREDICTED_NEXT_SECTOR_{mkt}"
    raw = str(cfg.get(key) or "").strip()
    asof = str(cfg.get(f"{key}_AS_OF") or "").strip()[:10]
    src = str(cfg.get(f"{key}_FROM") or "").strip()

    if raw and raw not in ("분석중", "분석 대기", "NONE", "", "—"):
        if asof and asof != _today_kst():
            tail = f" (산출 {asof}"
            if src:
                tail += f", 현재 주도 {src}"
            try:
                conf = float(cfg.get(f"{key}_CONFIDENCE"))
                tail += f", 신뢰 {conf:.0%}"
            except (TypeError, ValueError):
                pass
            return raw + tail + ")"
        return raw

    lg_key = f"{key}_LAST_GOOD"
    lg = str(cfg.get(lg_key) or "").strip()
    lg_asof = str(cfg.get(f"{lg_key}_AS_OF") or cfg.get(f"{key}_AS_OF") or "").strip()[:10]
    if lg and lg not in ("분석중", "분석 대기", "NONE"):
        ref = lg_asof or _last_business_day_kst()
        return f"{lg} (직전 영업일 캐시 {ref})"

    return "데이터 없음 (순환매 DB 미축적)"


def main() -> None:
    import json

    result = refresh_sector_spillover_state(save=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
