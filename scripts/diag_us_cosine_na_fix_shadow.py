#!/usr/bin/env python3
"""US-COSINE-NA-FIX-01 1단계 섀도우 — 라이브 yf.download 패널만. 헌터 미호출.

카운트: 구경로(if pd.NA 예외 / nan 누수) vs 신경로(명시 평가불가·LIQ).
last-finite-close는 비교용일 뿐 신경로에 쓰지 않는다.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

ROOT = Path(
    os.environ.get("QUANT_FACTORY_ROOT")
    or Path(__file__).resolve().parents[1]
)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scan_liq_front_gate import (
    EVAL_UNAVAILABLE,
    LIQUIDITY,
    PASS,
    classify_liq_front_window,
    is_missing_scalar,
    old_liq_front_raises_or_leaks,
)
from yf_download_flatten import flatten_yf_download_df

CHUNK = 100
OUT_JSON = Path(
    os.environ.get(
        "NA_FIX_SHADOW_OUT",
        str(ROOT / "scripts" / "diag_us_cosine_na_fix_shadow_last.json"),
    )
)


def _load_universe() -> list[str]:
    from market_db_paths import MARKET_DATA_DB_PATH
    from us_list_survival import collect_us_list_survival

    df, src = collect_us_list_survival(
        db_path=MARKET_DATA_DB_PATH, fdr_module=None
    )
    if df is None or df.empty or "Code" not in df.columns:
        import FinanceDataReader as fdr

        df, src = collect_us_list_survival(
            db_path=MARKET_DATA_DB_PATH, fdr_module=fdr
        )
    if df is None or df.empty:
        raise SystemExit("empty US universe")
    codes = [str(c).strip() for c in df["Code"].tolist() if str(c).strip()]
    print(f"universe n={len(codes)} src={src}", flush=True)
    return codes


def _col_values(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None
    s = df[col]
    if isinstance(s, pd.DataFrame):
        s = s.squeeze()
    return s.values


def _last_finite_close_count(close_vals) -> bool:
    """비교용: 마지막 봉 NA이고 그 앞에 유한 Close가 있는가."""
    if close_vals is None or len(close_vals) == 0:
        return False
    last = close_vals[-1]
    if not is_missing_scalar(last):
        return False
    for item in close_vals[:-1][::-1]:
        if not is_missing_scalar(item):
            return True
    return False


def _preload_yf(tickers: list[str]) -> dict[str, pd.DataFrame]:
    us_data_dict: dict[str, pd.DataFrame] = {}
    n_chunk = (len(tickers) + CHUNK - 1) // CHUNK
    for ci in range(0, len(tickers), CHUNK):
        chunk = tickers[ci : ci + CHUNK]
        idx = ci // CHUNK + 1
        try:
            raw = yf.download(
                " ".join(chunk),
                period="2mo",
                group_by="ticker",
                progress=False,
                threads=False,
            )
        except Exception as ex:
            print(f"chunk {idx}/{n_chunk} download fail: {ex}", flush=True)
            continue
        if raw is None or getattr(raw, "empty", True):
            print(f"chunk {idx}/{n_chunk} empty", flush=True)
            continue
        try:
            if len(chunk) == 1:
                tk0 = chunk[0]
                sub = flatten_yf_download_df(raw.copy())
                if sub is not None and not sub.empty:
                    us_data_dict[tk0] = sub
            else:
                lvl0 = (
                    raw.columns.get_level_values(0)
                    if isinstance(raw.columns, pd.MultiIndex)
                    else None
                )
                for tk in chunk:
                    try:
                        if isinstance(raw.columns, pd.MultiIndex):
                            if lvl0 is None or tk not in lvl0:
                                continue
                            sub = raw[tk].copy()
                        else:
                            sub = raw.copy()
                        sub = flatten_yf_download_df(sub)
                        if sub is not None and not sub.empty:
                            us_data_dict[tk] = sub
                    except Exception:
                        continue
        except Exception as ex:
            print(f"chunk {idx}/{n_chunk} parse fail: {ex}", flush=True)
            continue
        print(
            f"chunk {idx}/{n_chunk} loaded={len(us_data_dict)}",
            flush=True,
        )
    return us_data_dict


def main() -> int:
    tickers = _load_universe()
    panel = _preload_yf(tickers)
    print(f"yf.download panel n={len(panel)}/{len(tickers)}", flush=True)

    counts = {
        "universe": len(tickers),
        "panel": len(panel),
        "short_or_empty": 0,
        "old_uneval_raise": 0,
        "old_leak_pass": 0,
        "old_liq": 0,
        "old_pass": 0,
        "new_eval_unavail_close": 0,
        "new_eval_unavail_vol": 0,
        "new_liq": 0,
        "new_pass": 0,
        "converted_uneval_to_evaluable": 0,
        "converted_to_liq": 0,
        "converted_to_pass": 0,
        "last_finite_close": 0,
        "garbage_blocked": 0,
    }
    samples_converted: list[str] = []
    samples_blocked: list[str] = []

    for code in tickers:
        df = panel.get(code)
        if df is None or getattr(df, "empty", True) or len(df) < 20:
            counts["short_or_empty"] += 1
            continue
        c = _col_values(df, "Close")
        v = _col_values(df, "Volume")
        if c is None or v is None or len(c) < 20 or len(v) < 20:
            counts["short_or_empty"] += 1
            continue
        if _last_finite_close_count(c):
            counts["last_finite_close"] += 1
        vol_tail = v[-5:] if len(v) >= 5 else v
        close_last = c[-1]
        old = old_liq_front_raises_or_leaks(
            market="US", close_last=close_last, vol_tail=vol_tail
        )
        if old == "UNEVAL_RAISE":
            counts["old_uneval_raise"] += 1
        elif old == "LIQUIDITY":
            counts["old_liq"] += 1
        else:
            if is_missing_scalar(close_last):
                counts["old_leak_pass"] += 1
                old = "LEAK_PASS"
            else:
                counts["old_pass"] += 1

        new_reason, new_why = classify_liq_front_window(
            market="US", close_last=close_last, vol_tail=vol_tail
        )
        if new_reason == EVAL_UNAVAILABLE:
            if new_why == "close_na":
                counts["new_eval_unavail_close"] += 1
            else:
                counts["new_eval_unavail_vol"] += 1
            if old == "LEAK_PASS" or (
                old == "PASS" and is_missing_scalar(close_last)
            ):
                counts["garbage_blocked"] += 1
                if len(samples_blocked) < 8:
                    samples_blocked.append(str(code))
        elif new_reason == LIQUIDITY:
            counts["new_liq"] += 1
        else:
            counts["new_pass"] += 1

        old_uneval = old in ("UNEVAL_RAISE",)
        new_evaluable = new_reason in (LIQUIDITY, PASS)
        if old_uneval and new_evaluable:
            counts["converted_uneval_to_evaluable"] += 1
            if new_reason == LIQUIDITY:
                counts["converted_to_liq"] += 1
            else:
                counts["converted_to_pass"] += 1
            if len(samples_converted) < 12:
                samples_converted.append(f"{code}:{new_reason}:{new_why}")

    payload = {
        "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "yf.download period=2mo group_by=ticker (live hunter path)",
        "counts": counts,
        "samples_converted": samples_converted,
        "samples_garbage_blocked": samples_blocked,
        "note": (
            "converted = old TypeError(NA in if/mean) → new LIQ or PASS. "
            "last_finite_close is comparison only; new path does not use prior bar. "
            "NA is never filled with 0."
        ),
    }
    OUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    print(f"wrote {OUT_JSON}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
