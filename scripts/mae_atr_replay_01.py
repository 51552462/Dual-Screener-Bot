"""MAE-ATR-REPLAY-01: STAT_MAE 246건 일봉 리플레이 (읽기 전용).

MAE(-3.5) 없이 HYBRID 2순위만 적용: TIME → 2×ATR → TECH → (타임×2 좀비).
장부 UPDATE / config_kv / NAV / 텔레그램 없음.

  python scripts/mae_atr_replay_01.py --db /var/lib/quant-factory/data/market_data.sqlite
"""
from __future__ import annotations

import argparse
import math
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from typing import Any

import numpy as np
import pandas as pd

_SAFE_TABLE = re.compile(r"^[A-Za-z0-9_]+$")

ATR_MULT = 2.0
TIME_STOP = 10
TIME_RET_MAX = 3.0
IMMEDIATE_MFE = 0.05
WARMUP_CAL_DAYS = 90


def _finite(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(v):
        return default
    return v


def _date_key(val: Any) -> str:
    if val is None:
        return ""
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    return s[:10] if len(s) >= 10 else s


def _pctile(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    if len(ys) == 1:
        return ys[0]
    pos = (len(ys) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ys[lo]
    w = pos - lo
    return ys[lo] * (1.0 - w) + ys[hi] * w


def _summarize(xs: list[float]) -> dict[str, Any]:
    if not xs:
        return {"n": 0}
    n = len(xs)
    mu = sum(xs) / n
    return {
        "n": n,
        "mean": round(mu, 4),
        "p25": round(_pctile(xs, 0.25) or 0.0, 4),
        "p50": round(_pctile(xs, 0.50) or 0.0, 4),
        "p75": round(_pctile(xs, 0.75) or 0.0, 4),
        "min": round(min(xs), 4),
        "max": round(max(xs), 4),
        "win_pct": round(100.0 * sum(1 for x in xs if x > 0) / n, 1),
    }


def ohlcv_table_name(market: str, code: str) -> str | None:
    name = f"{str(market).upper().strip()}_{str(code).strip()}"
    if not _SAFE_TABLE.match(name):
        return None
    return name


def load_ohlcv(conn: sqlite3.Connection, table: str, start: str) -> pd.DataFrame:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not exists:
        return pd.DataFrame()
    df = pd.read_sql(
        f'SELECT Date, Open, High, Low, Close, Volume FROM "{table}" ORDER BY Date',
        conn,
    )
    if df.empty:
        return df
    df["Date"] = df["Date"].map(_date_key)
    for col in ("Open", "High", "Low", "Close", "Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Date", "Open", "High", "Low", "Close"])
    df = df[df["Date"] >= start].reset_index(drop=True)
    return df


def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    prev_c = out["Close"].shift(1)
    tr = np.maximum(
        out["High"] - out["Low"],
        np.maximum((out["High"] - prev_c).abs(), (out["Low"] - prev_c).abs()),
    )
    out["atr"] = tr.ewm(span=14, adjust=False).mean()
    out["ema10"] = out["Close"].ewm(span=10, adjust=False).mean()
    out["ema20"] = out["Close"].ewm(span=20, adjust=False).mean()
    z1 = out["Close"].ewm(span=20, adjust=False).mean()
    z2 = z1.ewm(span=20, adjust=False).mean()
    out["zlema"] = z1 + (z1 - z2)
    return out


def replay_one(df: pd.DataFrame, *, ep: float, entry_date: str, entry_atr: float) -> dict[str, Any]:
    """HYBRID 2순위 only — STAT_MAE / MFE 러너 없음."""
    sl_price = ep - (ATR_MULT * entry_atr)
    work = _add_indicators(df)
    hold = work[work["Date"] >= entry_date].reset_index(drop=True)
    if hold.empty:
        return {"ok": False, "reason": "no_bars_after_entry"}

    full_idx = {d: i for i, d in enumerate(work["Date"].tolist())}
    bars = 0
    last_ret = None
    last_date = None
    for _, row in hold.iterrows():
        bars += 1
        o = _finite(row["Open"])
        h = _finite(row["High"])
        l = _finite(row["Low"])
        c = _finite(row["Close"])
        if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
            continue
        current_ret = ((c - ep) / ep) * 100.0
        last_ret = current_ret
        last_date = row["Date"]

        gi = full_idx.get(row["Date"])
        tech = False
        if gi is not None and gi >= 1:
            zlema = _finite(work.iloc[gi]["zlema"], default=float("nan"))
            e10 = _finite(work.iloc[gi]["ema10"], default=float("nan"))
            e20 = _finite(work.iloc[gi]["ema20"], default=float("nan"))
            p10 = _finite(work.iloc[gi - 1]["ema10"], default=float("nan"))
            p20 = _finite(work.iloc[gi - 1]["ema20"], default=float("nan"))
            dead = (
                math.isfinite(e10)
                and math.isfinite(e20)
                and math.isfinite(p10)
                and math.isfinite(p20)
                and e10 < e20
                and p10 >= p20
            )
            tech = (math.isfinite(zlema) and c < zlema) or dead

        if bars >= TIME_STOP and current_ret < TIME_RET_MAX:
            return {
                "ok": True,
                "exit_type": "HYBRID_TIME",
                "virtual_ret": round(current_ret, 4),
                "bars": bars,
                "exit_date": row["Date"],
                "censored": False,
            }
        if l <= sl_price:
            atr_ret = ((sl_price - ep) / ep) * 100.0
            return {
                "ok": True,
                "exit_type": "HYBRID_ATR",
                "virtual_ret": round(atr_ret, 4),
                "bars": bars,
                "exit_date": row["Date"],
                "censored": False,
            }
        if tech:
            return {
                "ok": True,
                "exit_type": "HYBRID_TECH",
                "virtual_ret": round(current_ret, 4),
                "bars": bars,
                "exit_date": row["Date"],
                "censored": False,
            }
        if bars >= TIME_STOP * 2:
            return {
                "ok": True,
                "exit_type": "ZOMBIE_FORCE_CLOSE",
                "virtual_ret": 0.0,
                "bars": bars,
                "exit_date": row["Date"],
                "censored": False,
            }

    return {
        "ok": True,
        "exit_type": "CENSOR_OPEN",
        "virtual_ret": round(last_ret if last_ret is not None else 0.0, 4),
        "bars": bars,
        "exit_date": last_date,
        "censored": True,
    }


def _group_label(mfe: float) -> str:
    return "immediate" if mfe <= IMMEDIATE_MFE else "had_mfe"


def _print_block(title: str, rows: list[dict[str, Any]]) -> None:
    actual = [r["actual_ret"] for r in rows]
    virt = [r["virtual_ret"] for r in rows]
    delta = [r["virtual_ret"] - r["actual_ret"] for r in rows]
    resolved = [r for r in rows if not r["censored"]]
    sa = _summarize(actual)
    sv = _summarize(virt)
    sd = _summarize(delta)
    exits = Counter(r["exit_type"] for r in rows)
    print(f"\n### {title}  n={len(rows)}")
    print(
        f"| 지표 | 실제 final_ret | 리플레이 가상 | 차이(가상-실제) |"
    )
    print("|---|---:|---:|---:|")
    print(
        f"| 평균 | {sa.get('mean', '—')} | {sv.get('mean', '—')} | {sd.get('mean', '—')} |"
    )
    print(
        f"| p50 | {sa.get('p50', '—')} | {sv.get('p50', '—')} | {sd.get('p50', '—')} |"
    )
    print(
        f"| p25 / p75 | {sa.get('p25', '—')} / {sa.get('p75', '—')} | "
        f"{sv.get('p25', '—')} / {sv.get('p75', '—')} | "
        f"{sd.get('p25', '—')} / {sd.get('p75', '—')} |"
    )
    print(
        f"| min / max | {sa.get('min', '—')} / {sa.get('max', '—')} | "
        f"{sv.get('min', '—')} / {sv.get('max', '—')} | "
        f"{sd.get('min', '—')} / {sd.get('max', '—')} |"
    )
    print(
        f"| 승률% | {sa.get('win_pct', '—')} | {sv.get('win_pct', '—')} | — |"
    )
    if resolved and len(resolved) != len(rows):
        rv = _summarize([r["virtual_ret"] for r in resolved])
        rd = _summarize([r["virtual_ret"] - r["actual_ret"] for r in resolved])
        print(
            f"| 평균(청산된 {len(resolved)}건만) | — | {rv.get('mean')} | {rd.get('mean')} |"
        )
    print("가상 exit_type:", dict(exits))


def main() -> int:
    ap = argparse.ArgumentParser(description="MAE-ATR-REPLAY-01 read-only replay")
    ap.add_argument("--db", required=True, help="market_data.sqlite (read-only)")
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    trades = conn.execute(
        """
        SELECT id, market, code, entry_date, exit_date, entry_price, entry_atr,
               mfe, final_ret
        FROM forward_trades
        WHERE status LIKE 'CLOSED%' AND exit_type = 'STAT_MAE'
        """
    ).fetchall()

    results: list[dict[str, Any]] = []
    skip = Counter()
    for t in trades:
        ep = _finite(t["entry_price"])
        atr = _finite(t["entry_atr"])
        mfe = _finite(t["mfe"])
        actual = _finite(t["final_ret"])
        entry_date = _date_key(t["entry_date"])
        market = str(t["market"] or "").upper()
        code = str(t["code"] or "").strip()
        if ep <= 0 or atr <= 0 or not entry_date:
            skip["bad_inputs"] += 1
            continue
        table = ohlcv_table_name(market, code)
        if not table:
            skip["bad_table_name"] += 1
            continue
        warm_start = (
            pd.Timestamp(entry_date) - pd.Timedelta(days=WARMUP_CAL_DAYS)
        ).strftime("%Y-%m-%d")
        df = load_ohlcv(conn, table, warm_start)
        if df.empty:
            skip["no_ohlcv_table"] += 1
            continue
        out = replay_one(df, ep=ep, entry_date=entry_date, entry_atr=atr)
        if not out.get("ok"):
            skip[str(out.get("reason") or "replay_fail")] += 1
            continue
        results.append(
            {
                "id": t["id"],
                "market": market,
                "code": code,
                "group": _group_label(mfe),
                "mfe": mfe,
                "actual_ret": actual,
                "virtual_ret": float(out["virtual_ret"]),
                "exit_type": out["exit_type"],
                "bars": out["bars"],
                "censored": bool(out["censored"]),
                "orig_exit": _date_key(t["exit_date"]),
                "virt_exit": out.get("exit_date"),
            }
        )

    print("MAE-ATR-REPLAY-01  (read-only · STAT_MAE 리플레이)")
    print(f"장부 STAT_MAE n={len(trades)}  리플레이 성공 n={len(results)}  skip={dict(skip)}")
    print(
        f"규칙: TIME_STOP={TIME_STOP}d & close<{TIME_RET_MAX}% → HYBRID_TIME(종가) · "
        f"Low≤ep-{ATR_MULT}×entry_atr → HYBRID_ATR(지정가) · "
        f"ZLEMA/데드 → HYBRID_TECH(종가) · bars≥{TIME_STOP*2} → ZOMBIE(0%) · MAE/MFE 없음"
    )
    print("단위 % · 차이 = 가상 − 실제(고정 -3.5)")

    _print_block("전체", results)
    _print_block(
        "즉시반대 (MFE≤0.05%)",
        [r for r in results if r["group"] == "immediate"],
    )
    _print_block(
        "한번유리 (MFE>0.05%)",
        [r for r in results if r["group"] == "had_mfe"],
    )

    by_mkt: dict[str, list] = defaultdict(list)
    for r in results:
        by_mkt[r["market"]].append(r)
    for m in sorted(by_mkt):
        _print_block(f"시장 {m}", by_mkt[m])

    better = sum(1 for r in results if r["virtual_ret"] > r["actual_ret"] + 1e-9)
    worse = sum(1 for r in results if r["virtual_ret"] < r["actual_ret"] - 1e-9)
    print(
        f"\n가상>실제 {better}건 · 가상<실제 {worse}건 · "
        f"동률 {len(results)-better-worse}건 · CENSOR_OPEN "
        f"{sum(1 for r in results if r['censored'])}건"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
