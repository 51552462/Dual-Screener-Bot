"""
Macro Doomsday Radar — 완전 격리 위성 (Satellite).

- 메인 팩토리 스크리너·트레이딩 알고리즘 미import, market_data.sqlite 미사용.
- 산출물 전용: 이 스크립트와 같은 디렉터리의 doomsday_status.json
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(_THIS_DIR, "doomsday_status.json")

ROLLING_WINDOW = 252
MIN_PERIODS = 60
MAX_RETRIES = 4
SLEEP_BASE = 1.4

T_TNX = "^TNX"
T_IRX = "^IRX"
T_HYG = "HYG"
T_IEF = "IEF"
T_HG = "HG=F"
T_GC = "GC=F"
T_KRW = "KRW=X"
T_KS11 = "^KS11"


def _atomic_write_json(path: str, payload: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _fetch_close_series(symbol: str, errors: list[str]) -> pd.Series:
    """단일 티커 종가 시계열. 네트워크·빈응답 방어."""
    for attempt in range(MAX_RETRIES):
        try:
            df = yf.Ticker(symbol).history(period="800d", interval="1d", auto_adjust=True)
            if df is None or df.empty or "Close" not in df.columns:
                raise ValueError("empty or no Close")
            s = pd.to_numeric(df["Close"], errors="coerce").astype(float)
            s = s.dropna().sort_index()
            s = s[~s.index.duplicated(keep="last")]
            if len(s) >= MIN_PERIODS:
                return s
        except Exception as e:
            errors.append(f"{symbol} attempt {attempt + 1}: {e!s}")
        time.sleep(SLEEP_BASE * (1.6**attempt))
    return pd.Series(dtype=float)


def _build_market_panel(errors: list[str]) -> pd.DataFrame:
    syms = [T_TNX, T_IRX, T_HYG, T_IEF, T_HG, T_GC, T_KRW, T_KS11]
    parts: dict[str, pd.Series] = {}
    for sym in syms:
        s = _fetch_close_series(sym, errors)
        if not s.empty:
            parts[sym] = s
        time.sleep(0.15)
    if len(parts) < 4:
        raise RuntimeError(f"너무 적은 시계열: {list(parts.keys())}")
    df = pd.concat(parts, axis=1, join="inner")
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df


def _rolling_z_score(series: pd.Series, window: int = ROLLING_WINDOW) -> pd.Series:
    s = series.astype(float)
    mu = s.rolling(window, min_periods=MIN_PERIODS).mean()
    sig = s.rolling(window, min_periods=MIN_PERIODS).std()
    return (s - mu) / sig.replace(0.0, np.nan)


def _latest_z(z: pd.Series) -> float | None:
    z = z.dropna()
    if z.empty:
        return None
    v = float(z.iloc[-1])
    return v if np.isfinite(v) else None


def _doom_scalar_from_z(z_values: list[float | None]) -> float:
    arr = np.array([x for x in z_values if x is not None and np.isfinite(x)], dtype=float)
    if arr.size == 0:
        return 0.0
    return float(np.clip(float(np.mean(np.abs(arr))) * 32.0, 0.0, 100.0))


def _classify_regime(*, global_score: float, kr_score: float) -> str:
    if global_score > 70.0:
        return "DOOMSDAY"
    if 41.0 <= kr_score <= 70.0:
        return "DEFENSIVE_KR"
    if global_score <= 40.0:
        return "BULL"
    return "ELEVATED"


def _last_float(s: pd.Series) -> float | None:
    s = s.dropna()
    if s.empty:
        return None
    v = float(s.iloc[-1])
    return v if np.isfinite(v) else None


def run_macro_doomsday_radar() -> dict:
    errors: list[str] = []
    df = _build_market_panel(errors)

    tnx = df[T_TNX].astype(float)
    irx = df[T_IRX].astype(float)
    hyg = df[T_HYG].astype(float)
    ief = df[T_IEF].astype(float)
    hg = df[T_HG].astype(float)
    gc = df[T_GC].astype(float)
    krw = df[T_KRW].astype(float)
    ks11 = df[T_KS11].astype(float)

    yield_spread = tnx - irx
    yield_spread_momo = yield_spread.diff(20)
    hyg_ief_ratio = hyg / ief.replace(0.0, np.nan)
    cu_au_ratio = hg / gc.replace(0.0, np.nan)
    krw_roc = krw.pct_change(10) * 100.0
    kospi_roc = ks11.pct_change(20) * 100.0

    z_yield_spread = _rolling_z_score(yield_spread)
    z_yield_momo = _rolling_z_score(yield_spread_momo)
    z_hyg_ief = _rolling_z_score(hyg_ief_ratio)
    z_cu_au = _rolling_z_score(cu_au_ratio)
    z_krw_roc = _rolling_z_score(krw_roc)
    z_kospi_roc = _rolling_z_score(kospi_roc)

    lz_us = [
        _latest_z(z_yield_spread),
        _latest_z(z_yield_momo),
        _latest_z(z_hyg_ief),
        _latest_z(z_cu_au),
    ]
    lz_kr = [
        _latest_z(z_krw_roc),
        _latest_z(z_kospi_roc),
    ]

    us_doom = _doom_scalar_from_z(lz_us)
    kr_doom = _doom_scalar_from_z(lz_kr)
    global_score = float(np.clip(0.6 * us_doom + 0.4 * kr_doom, 0.0, 100.0))
    regime = _classify_regime(global_score=global_score, kr_score=kr_doom)

    raw_metrics = {
        "tnx_last": _last_float(tnx),
        "irx_last": _last_float(irx),
        "yield_spread_tnx_minus_irx": _last_float(yield_spread),
        "yield_spread_20d_change": _last_float(yield_spread_momo),
        "hyg_ief_ratio": _last_float(hyg_ief_ratio),
        "copper_gold_ratio": _last_float(cu_au_ratio),
        "usdkrw_last": _last_float(krw),
        "krw_10d_pct": _last_float(krw_roc),
        "kospi_last": _last_float(ks11),
        "kospi_20d_pct": _last_float(kospi_roc),
    }

    z_snapshot = {
        "z_us_yield_spread": lz_us[0],
        "z_us_yield_spread_20d_diff": lz_us[1],
        "z_us_hyg_ief_ratio": lz_us[2],
        "z_us_copper_gold_ratio": lz_us[3],
        "z_kr_usdkrw_10d_roc": lz_kr[0],
        "z_kr_kospi_20d_roc": lz_kr[1],
    }

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "regime": regime,
        "scores": {
            "US_Doom_Score": round(us_doom, 2),
            "KR_Doom_Score": round(kr_doom, 2),
            "Global_Contagion_Score": round(global_score, 2),
        },
        "z_scores_latest": {k: (round(v, 4) if v is not None else None) for k, v in z_snapshot.items()},
        "raw_metrics_latest": {k: (round(v, 6) if v is not None else None) for k, v in raw_metrics.items()},
        "methodology": {
            "rolling_window": ROLLING_WINDOW,
            "anomaly_rule": "|Z| > 2.0 tail vs trailing 252-session local mean/std",
            "global_blend": "0.60 * US_Doom_Score + 0.40 * KR_Doom_Score",
            "regime_map": "DOOMSDAY: Global>70; DEFENSIVE_KR: 41<=KR<=70 (and not DOOMSDAY); BULL: Global<=40; else ELEVATED",
        },
        "data_last_date": str(df.index[-1].date()) if len(df) else None,
        "errors": errors[-40:],
    }


def main() -> None:
    print("🛰️ [Macro Doomsday Radar] 위성 스캔 시작 (Zero-Coupling)…")
    try:
        payload = run_macro_doomsday_radar()
    except Exception as e:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "regime": "ERROR",
            "scores": {"US_Doom_Score": None, "KR_Doom_Score": None, "Global_Contagion_Score": None},
            "z_scores_latest": {},
            "raw_metrics_latest": {},
            "methodology": {},
            "data_last_date": None,
            "errors": [str(e)],
        }
    try:
        _atomic_write_json(OUTPUT_JSON, payload)
        print(f"✅ 저장 완료: {OUTPUT_JSON}  (Regime={payload.get('regime')})")
        try:
            from doomsday_bridge import ingest_doomsday_status_file

            ing = ingest_doomsday_status_file(alert_on_escalation=True, run_inverse_cycle=True)
            print(f"🔗 [Doomsday Bridge] config 동기화: {ing}")
        except Exception as br_e:
            print(f"⚠️ [Doomsday Bridge] 동기화 실패: {br_e}")
    except Exception as e:
        print(f"🚨 JSON 저장 실패: {e}")


if __name__ == "__main__":
    main()
