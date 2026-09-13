"""KR 상한가(+29%+) 이후 수익률 분포 — 읽기 전용 연구.

실전 장부/config_kv/텔레그램 무접촉. OHLCV는 FinanceDataReader.
캐시: dante_bots .../cache_limitup_ohlcv/ (git 밖).

  python scripts/limit_up_forward_stats_01.py
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LIMIT_PCT = 0.29  # 일별 등락률 +29% 이상
ERA_START = "2015-06-15"  # KRX 가격제한폭 15%→30%
WORKERS = 10
JUNK_NAME = re.compile(
    r"스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권"
    r"|KODEX|TIGER|KBSTAR|ACE|ARIRANG|KOSEF|HANARO|SOL|TIMEFOLIO",
    re.I,
)


def _cache_dir() -> str:
    root = os.path.join(
        os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "cache_limitup_ohlcv"
    )
    os.makedirs(root, exist_ok=True)
    return root


def _summarize(xs: list[float]) -> dict[str, Any]:
    if not xs:
        return {"n": 0}
    n = len(xs)
    ys = sorted(xs)
    def pct(q: float) -> float:
        pos = (n - 1) * q
        lo, hi = int(math.floor(pos)), int(math.ceil(pos))
        if lo == hi:
            return ys[lo]
        w = pos - lo
        return ys[lo] * (1.0 - w) + ys[hi] * w

    mu = sum(xs) / n
    return {
        "n": n,
        "mean_pct": round(100.0 * mu, 3),
        "p25_pct": round(100.0 * pct(0.25), 3),
        "p50_pct": round(100.0 * pct(0.50), 3),
        "p75_pct": round(100.0 * pct(0.75), 3),
        "win_pct": round(100.0 * sum(1 for x in xs if x > 0) / n, 1),
        "gt5_pct": round(100.0 * sum(1 for x in xs if x > 0.05) / n, 1),
        "lt_m5_pct": round(100.0 * sum(1 for x in xs if x < -0.05) / n, 1),
    }


def _fetch_one(code: str) -> tuple[str, pd.DataFrame | None, str]:
    cache = os.path.join(_cache_dir(), f"{code}.pkl")
    if os.path.isfile(cache) and os.path.getsize(cache) > 64:
        try:
            df = pd.read_pickle(cache)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return code, df, "cache"
        except Exception:
            pass
    try:
        import FinanceDataReader as fdr

        df = fdr.DataReader(code, ERA_START)
    except Exception as e:
        return code, None, f"err:{type(e).__name__}"
    if df is None or getattr(df, "empty", True):
        return code, None, "empty"
    try:
        df.to_pickle(cache)
    except Exception:
        pass
    return code, df, "net"


def _events_from_ohlcv(code: str, name: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    work = df.copy()
    work.columns = [str(c).strip() for c in work.columns]
    if "Close" not in work.columns or "Open" not in work.columns:
        return []
    work = work.sort_index()
    work = work[~work.index.duplicated(keep="last")]
    close = pd.to_numeric(work["Close"], errors="coerce")
    op = pd.to_numeric(work["Open"], errors="coerce")
    if "Change" in work.columns:
        chg = pd.to_numeric(work["Change"], errors="coerce")
    else:
        chg = close.pct_change()
    rows = []
    dates = list(work.index)
    streak = 0
    for i, ts in enumerate(dates):
        r = float(chg.iloc[i]) if pd.notna(chg.iloc[i]) else float("nan")
        if not math.isfinite(r) or r < LIMIT_PCT:
            streak = 0
            continue
        streak += 1
        cl = float(close.iloc[i])
        if cl <= 0:
            continue
        ev = {
            "code": code,
            "name": name,
            "date": str(ts)[:10],
            "streak": streak,
            "limit_ret": r,
        }
        if i + 1 < len(dates):
            o1 = float(op.iloc[i + 1]) if pd.notna(op.iloc[i + 1]) else float("nan")
            c1 = float(close.iloc[i + 1]) if pd.notna(close.iloc[i + 1]) else float("nan")
            if math.isfinite(o1) and o1 > 0:
                ev["next_open"] = o1 / cl - 1.0
            if math.isfinite(c1) and c1 > 0:
                ev["next_close"] = c1 / cl - 1.0
        if i + 3 < len(dates):
            c3 = float(close.iloc[i + 3]) if pd.notna(close.iloc[i + 3]) else float("nan")
            if math.isfinite(c3) and c3 > 0:
                ev["d3"] = c3 / cl - 1.0
        if i + 5 < len(dates):
            c5 = float(close.iloc[i + 5]) if pd.notna(close.iloc[i + 5]) else float("nan")
            if math.isfinite(c5) and c5 > 0:
                ev["d5"] = c5 / cl - 1.0
        rows.append(ev)
    return rows


def _print_table(title: str, buckets: dict[str, list[float]], keys: list[str]) -> None:
    print(f"\n## {title}")
    hdr = f"{'bucket':<16} {'n':>7} {'mean%':>8} {'p25':>8} {'p50':>8} {'p75':>8} {'WR%':>7} {'>5%':>7} {'<-5%':>7}"
    print(hdr)
    print("-" * len(hdr))
    for k in keys:
        s = _summarize(buckets.get(k) or [])
        if s.get("n", 0) == 0:
            print(f"{k:<16} {0:>7}")
            continue
        print(
            f"{k:<16} {s['n']:>7} {s['mean_pct']:>8.3f} {s['p25_pct']:>8.3f} "
            f"{s['p50_pct']:>8.3f} {s['p75_pct']:>8.3f} {s['win_pct']:>7.1f} "
            f"{s['gt5_pct']:>7.1f} {s['lt_m5_pct']:>7.1f}"
        )


def main() -> int:
    import FinanceDataReader as fdr

    listing = fdr.StockListing("KRX")
    mid = listing["MarketId"].astype(str).str.upper() if "MarketId" in listing.columns else ""
    if "MarketId" in listing.columns:
        listing = listing[mid.isin({"STK", "KSQ"})]
    dept = listing["Dept"].astype(str) if "Dept" in listing.columns else ""
    if "Dept" in listing.columns:
        listing = listing[~dept.str.contains("SPAC", case=False, na=False)]
    listing["Name"] = listing["Name"].astype(str)
    listing = listing[~listing["Name"].str.contains(JUNK_NAME, na=False)]
    codes = listing["Code"].astype(str).str.zfill(6).tolist()
    names = dict(zip(codes, listing["Name"].astype(str)))
    print(f"universe STK/KSQ junk-filtered n={len(codes)}  start={ERA_START}  thr=+{LIMIT_PCT*100:.0f}%")

    events: list[dict[str, Any]] = []
    ok = err = empty = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_fetch_one, c): c for c in codes}
        done = 0
        for fut in as_completed(futs):
            done += 1
            code, df, st = fut.result()
            if st.startswith("err") or df is None:
                err += 1
            elif st == "empty":
                empty += 1
            else:
                ok += 1
                events.extend(_events_from_ohlcv(code, names.get(code, ""), df))
            if done % 200 == 0 or done == len(codes):
                print(
                    f"  fetch {done}/{len(codes)} ok={ok} empty={empty} err={err} "
                    f"events={len(events)} {time.time()-t0:.0f}s",
                    flush=True,
                )

    by_next_o: dict[str, list[float]] = defaultdict(list)
    by_next_c: dict[str, list[float]] = defaultdict(list)
    by_d3: dict[str, list[float]] = defaultdict(list)
    by_d5: dict[str, list[float]] = defaultdict(list)

    def bucket(st: int) -> str:
        if st <= 1:
            return "first"
        if st == 2:
            return "day2"
        if st == 3:
            return "day3"
        return "day4plus"

    years: dict[str, int] = defaultdict(int)
    for ev in events:
        years[ev["date"][:4]] += 1
        b = bucket(int(ev["streak"]))
        if "next_open" in ev:
            by_next_o["all"].append(ev["next_open"])
            by_next_o[b].append(ev["next_open"])
        if "next_close" in ev:
            by_next_c["all"].append(ev["next_close"])
            by_next_c[b].append(ev["next_close"])
        if "d3" in ev:
            by_d3["all"].append(ev["d3"])
            by_d3[b].append(ev["d3"])
        if "d5" in ev:
            by_d5["all"].append(ev["d5"])
            by_d5[b].append(ev["d5"])

    keys = ["all", "first", "day2", "day3", "day4plus"]
    print("\n## 표본")
    print(f"events n={len(events)}  unique codes={len({e['code'] for e in events})}")
    print("year counts:", dict(sorted(years.items())))
    print("streak counts:", {k: sum(1 for e in events if bucket(e['streak'])==k) for k in keys[1:]})
    print("streak raw:", dict(pd.Series([e['streak'] for e in events]).value_counts().sort_index().head(12)))

    _print_table("다음 거래일 시가 수익률 (시가/상한가종가 - 1)", by_next_o, keys)
    _print_table("다음 거래일 종가 수익률 (종가/상한가종가 - 1)", by_next_c, keys)
    _print_table("3거래일 누적 (종가 t+3 / 상한가종가 - 1)", by_d3, keys)
    _print_table("5거래일 누적 (종가 t+5 / 상한가종가 - 1)", by_d5, keys)

    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "threshold": LIMIT_PCT,
        "era_start": ERA_START,
        "universe": len(codes),
        "events": len(events),
        "next_open": {k: _summarize(by_next_o[k]) for k in keys},
        "next_close": {k: _summarize(by_next_c[k]) for k in keys},
        "d3": {k: _summarize(by_d3[k]) for k in keys},
        "d5": {k: _summarize(by_d5[k]) for k in keys},
        "year_counts": dict(sorted(years.items())),
    }
    out_path = os.path.join(_cache_dir(), "forward_stats_summary.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\nsummary json: {out_path}")
    print(
        "\n해석 주의: 무조건부 상한가 종가 매수 후 보유. "
        "S1 필터·MAE 청산과 다름. 로컬 forward_trades=0."
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
