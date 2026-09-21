#!/usr/bin/env python3
"""US SUPERNOVA NA last-close salvage shadow — does not touch live scan/KV.

Counts only. Enroll 0. Hunter 미호출.
"""
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import certifi
import numpy as np
import pandas as pd
import pytz
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scan_resilience import safe_supernova_dna_features

US_DOLLAR_FLOOR = 300_000.0
US_MIN_PX = 0.50
COS_CUTOFF = 0.50
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
YAHOO_SPARK = "https://query1.finance.yahoo.com/v7/finance/spark"
SPARK_BATCH = 20


def _finite(v) -> bool:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return False
    return bool(np.isfinite(f))


def _series_1d(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    s = df[col]
    if isinstance(s, pd.DataFrame):
        s = s.squeeze()
    return pd.to_numeric(s, errors="coerce")


def _load_universe() -> list[str]:
    from market_db_paths import MARKET_DATA_DB_PATH
    from us_list_survival import collect_us_list_survival

    df, src = collect_us_list_survival(db_path=MARKET_DATA_DB_PATH, fdr_module=None)
    if df is None or df.empty or "Code" not in df.columns:
        import FinanceDataReader as fdr

        df, src = collect_us_list_survival(db_path=MARKET_DATA_DB_PATH, fdr_module=fdr)
    if df is None or df.empty:
        raise SystemExit("empty US universe")
    codes = [str(c).strip() for c in df["Code"].tolist() if str(c).strip()]
    print(f"universe n={len(codes)} src={src}", flush=True)
    return codes


def _load_templates() -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    try:
        from config_manager import load_system_config
        from template_evolution import load_base_templates

        cfg = load_system_config()
        for name, vec in load_base_templates(cfg, "US").items():
            out[str(name)] = np.array(
                [float(vec[0]), float(vec[1]), float(vec[2])], dtype=float
            )
        multi = cfg.get("DNA_SUPERNOVA_US_MULTI") or {}
        if isinstance(multi, dict):
            for t_name, t_dna in multi.items():
                if not isinstance(t_dna, dict):
                    continue
                try:
                    out[str(t_name)] = np.array(
                        [
                            float(t_dna["cpv"]),
                            float(t_dna["tb"]),
                            float(t_dna["bbe"]),
                        ],
                        dtype=float,
                    )
                except (KeyError, TypeError, ValueError):
                    continue
    except Exception as ex:
        print(f"template load skip: {ex}", flush=True)
    if not out:
        out = {
            "US_RANK_A_장기매집": np.array([0.70, 10.5, 25.0]),
            "US_RANK_B_중기스윙": np.array([0.66, 9.2, 21.5]),
            "US_RANK_C_단기테마": np.array([0.60, 8.1, 17.0]),
            "US_RANK_D_초단기밈": np.array([0.55, 7.5, 13.5]),
            "US_MEME_슈팅": np.array([0.55, 8.8, 12.80]),
        }
        print("templates=hardcoded US RANK fallback", flush=True)
    else:
        print(f"templates n={len(out)}", flush=True)
    return out


def _cosine(a, b) -> float:
    va = np.asarray(a, dtype=float)
    vb = np.asarray(b, dtype=float)
    n1 = np.linalg.norm(va)
    n2 = np.linalg.norm(vb)
    if n1 <= 0 or n2 <= 0:
        return 0.0
    return float(np.dot(va, vb) / (n1 * n2))


def _quote_to_df(ts, quote) -> pd.DataFrame | None:
    if not ts:
        return None
    df = pd.DataFrame(
        {
            "Open": quote.get("open"),
            "High": quote.get("high"),
            "Low": quote.get("low"),
            "Close": quote.get("close"),
            "Volume": quote.get("volume"),
        },
        index=pd.to_datetime(ts, unit="s"),
    )
    return df if not df.empty else None


def _chart_df(symbol: str) -> pd.DataFrame | None:
    try:
        r = requests.get(
            YAHOO_CHART.format(sym=symbol),
            params={"range": "2mo", "interval": "1d", "includePrePost": "false"},
            timeout=20,
            verify=certifi.where(),
            headers={"User-Agent": "Mozilla/5.0 DualScreenerNaShadow/1.0"},
        )
        if r.status_code != 200:
            return None
        payload = r.json()
        res = (payload.get("chart") or {}).get("result") or []
        if not res:
            return None
        node = res[0]
        ts = node.get("timestamp") or []
        quote = ((node.get("indicators") or {}).get("quote") or [{}])[0]
        return _quote_to_df(ts, quote)
    except Exception:
        return None


def _spark_batch(symbols: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    if not symbols:
        return out
    try:
        r = requests.get(
            YAHOO_SPARK,
            params={
                "symbols": ",".join(symbols),
                "range": "2mo",
                "interval": "1d",
            },
            timeout=45,
            verify=certifi.where(),
            headers={"User-Agent": "Mozilla/5.0 DualScreenerNaShadow/1.0"},
        )
        if r.status_code != 200:
            return out
        rows = ((r.json().get("spark") or {}).get("result")) or []
        for item in rows:
            sym = str(item.get("symbol") or "")
            resp = (item.get("response") or [None])[0]
            if not sym or not isinstance(resp, dict):
                continue
            ts = resp.get("timestamp") or []
            quote = ((resp.get("indicators") or {}).get("quote") or [{}])[0]
            df = _quote_to_df(ts, quote)
            if df is not None:
                out[sym] = df
    except Exception:
        return out
    return out


def _download_all(tickers: list[str], workers: int = 6) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    chunks = [tickers[i : i + SPARK_BATCH] for i in range(0, len(tickers), SPARK_BATCH)]
    n = len(chunks)
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(_spark_batch, ch): ch for ch in chunks}
        for fut in as_completed(futs):
            done += 1
            try:
                got = fut.result() or {}
            except Exception:
                got = {}
            out.update(got)
            if done % 10 == 0 or done == n:
                print(f"spark {done}/{n} loaded={len(out)}", flush=True)
    return out


def _asof_slice(df: pd.DataFrame, asof: pd.Timestamp) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    idx = pd.to_datetime(df.index)
    try:
        idx = idx.tz_localize(None)
    except Exception:
        pass
    work = df.copy()
    work.index = idx
    asof_n = pd.Timestamp(asof).tz_localize(None) if getattr(asof, "tzinfo", None) else pd.Timestamp(asof)
    return work.loc[work.index.normalize() <= asof_n.normalize()]


def classify_row(
    df: pd.DataFrame,
    templates: dict[str, np.ndarray],
    now_mkt: datetime,
) -> dict[str, bool]:
    empty = {
        "has_panel": False,
        "na_last_close": False,
        "salvageable": False,
        "liq_pass": False,
        "cosine_reach": False,
        "cosine_pass": False,
    }
    if df is None or df.empty:
        return empty
    close = _series_1d(df, "Close")
    vol = _series_1d(df, "Volume")
    if close.empty:
        return empty
    last_raw = close.iloc[-1]
    finite_idx = close.index[close.map(_finite)]
    last_na = not _finite(last_raw)
    salvageable = last_na and len(finite_idx) > 0
    out = dict(empty)
    out["has_panel"] = True
    out["na_last_close"] = bool(last_na and len(finite_idx) > 0)
    out["salvageable"] = salvageable
    if not salvageable:
        return out
    last_i = finite_idx[-1]
    px = float(close.loc[last_i])
    loc = int(close.index.get_indexer([last_i])[0])
    lo = max(0, loc - 4)
    vwin = vol.iloc[lo : loc + 1].map(lambda x: float(x) if _finite(x) else np.nan)
    adv = float(np.nanmean(vwin.to_numpy())) if len(vwin) else float("nan")
    min_vol = max(2_000.0, US_DOLLAR_FLOOR / max(px, 0.01))
    liq = px >= US_MIN_PX and _finite(adv) and adv >= min_vol
    out["liq_pass"] = bool(liq)
    if not liq:
        return out
    sliced = df.iloc[: loc + 1]
    if len(sliced) < 20:
        return out
    dna = safe_supernova_dna_features(sliced, market="US", now_mkt=now_mkt)
    if not isinstance(dna, dict):
        return out
    vec = [float(dna["cpv"]), float(dna["tb"]), float(dna["bbe"])]
    if not all(_finite(x) for x in vec):
        return out
    out["cosine_reach"] = True
    best = 0.0
    for tv in templates.values():
        if len(tv) == 3:
            best = max(best, _cosine(vec, tv))
    out["cosine_pass"] = bool(best >= COS_CUTOFF)
    return out


def _tally(rows: list[dict[str, bool]]) -> dict[str, int]:
    keys = ("has_panel", "na_last_close", "liq_pass", "cosine_reach", "cosine_pass")
    return {k: int(sum(1 for r in rows if r.get(k))) for k in keys}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-tickers", type=int, default=0)
    ap.add_argument("--asof-days", type=int, default=10)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    tickers = _load_universe()
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]
        print(f"truncated n={len(tickers)}", flush=True)

    templates = _load_templates()
    et = pytz.timezone("America/New_York")
    now_mkt = datetime.now(et).replace(hour=16, minute=0, second=0, microsecond=0)

    panel = _download_all(tickers, workers=max(1, args.workers))
    print(f"panel loaded={len(panel)}/{len(tickers)}", flush=True)

    spy = _chart_df("SPY")
    if spy is None or spy.empty:
        raise SystemExit("SPY calendar download failed")
    spy_idx = pd.to_datetime(spy.index).tz_localize(None).normalize().unique()
    spy_idx = sorted(pd.DatetimeIndex(spy_idx))
    asofs = spy_idx[-max(1, args.asof_days) :]

    latest_rows = [classify_row(panel.get(tk), templates, now_mkt) for tk in tickers]
    latest = _tally(latest_rows)
    print("=== LATEST SNAPSHOT (chart 2mo last row, NA kept) ===", flush=True)
    print(
        f"universe={len(tickers)} panel={latest['has_panel']} "
        f"NA_LAST_CLOSE={latest['na_last_close']} "
        f"LIQ={latest['liq_pass']} "
        f"COSINE_REACH={latest['cosine_reach']} "
        f"COSINE_PASS>={COS_CUTOFF} {latest['cosine_pass']}",
        flush=True,
    )

    print("=== ASOF (SPY sessions, salvage among that day's last-NA only) ===", flush=True)
    for asof in asofs:
        rows = [
            classify_row(_asof_slice(panel.get(tk), asof) if tk in panel else None, templates, now_mkt)
            for tk in tickers
        ]
        t = _tally(rows)
        print(
            f"{asof.date()} panel={t['has_panel']} NA_LAST_CLOSE={t['na_last_close']} "
            f"LIQ={t['liq_pass']} REACH={t['cosine_reach']} PASS={t['cosine_pass']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
