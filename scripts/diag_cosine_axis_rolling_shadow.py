#!/usr/bin/env python3
"""US-COSINE-AXIS-01 rolling shadow — live hunter / KV 미호출.

RANK 3D cosine only. z-score μ/σ from prior sessions (not same-day CS).
Templates use the same μ/σ.

--stage 1: live LIQ floors, percentile table (5/10/20%).
--stage 2: live floors vs $30k (US no share min; KR = $30k×1350 KRW ADV,
no 5만주). Cutoff locked to market daily 10th percentile of z-cosine.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytz

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scan_resilience import safe_supernova_dna_features

_spec = importlib.util.spec_from_file_location(
    "diag_us_cosine_na_shadow",
    ROOT / "scripts" / "diag_us_cosine_na_shadow.py",
)
_na = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_na)

LIVE_US_DOLLAR = 300_000.0
LIVE_US_SHARE_MIN = 2_000.0
PROP_US_DOLLAR = 30_000.0
KR_MIN_PX = 1000.0
KR_MIN_VOL = 50_000.0
KR_USD_FX = 1350.0  # KR analog of $30k: not 30000/px on KRW (floor ~0)
US_MIN_PX = _na.US_MIN_PX
_finite = _na._finite
_series_1d = _na._series_1d

# Live-write design only (this script does not set_config_value):
# config_kv COSINE_AXIS_STATS = {
#   "KR"|"US": {"mu":[3], "sd":[3], "n":int, "as_of":"YYYY-MM-DD",
#               "window_sessions":5, "source":"liq_pass_dna"}
# }
# Writer: end of SUPERNOVA scan (after LIQ). Reader: next scan before 3D cosine.
COSINE_AXIS_STATS_KEY = "COSINE_AXIS_STATS"

HARD_KR = {
    "RANK_A_장기매집": np.array([0.75, 11.8, 27.15]),
    "RANK_B_중기스윙": np.array([0.75, 10.0, 27.35]),
    "RANK_C_단기테마": np.array([0.60, 8.0, 19.70]),
    "RANK_D_초단기밈": np.array([0.60, 8.0, 24.45]),
}
HARD_US = {
    "US_RANK_A_장기매집": np.array([0.70, 10.5, 25.0]),
    "US_RANK_B_중기스윙": np.array([0.66, 9.2, 21.5]),
    "US_RANK_C_단기테마": np.array([0.60, 8.1, 17.0]),
    "US_RANK_D_초단기밈": np.array([0.55, 7.5, 13.5]),
    "US_MEME_슈팅": np.array([0.55, 8.8, 12.80]),
}


def _cosine(a, b) -> float:
    return _na._cosine(a, b)


def fit_mu_sd(rows: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """rows (n,3) -> mu, sd. sd floor 1e-9."""
    arr = np.asarray(rows, dtype=float)
    if arr.size == 0:
        return np.zeros(3), np.ones(3)
    mu = np.nanmean(arr, axis=0)
    sd = np.nanstd(arr, axis=0)
    sd = np.where(~np.isfinite(sd) | (sd < 1e-9), 1.0, sd)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    return mu, sd


def apply_z(vec: np.ndarray, mu: np.ndarray, sd: np.ndarray) -> np.ndarray:
    return (np.asarray(vec, dtype=float) - mu) / sd


def prior_session_rows(
    by_date: dict[str, list[np.ndarray]],
    dates: list[str],
    eval_date: str,
    roll_sessions: int,
) -> np.ndarray:
    """Vectors from the last `roll_sessions` dates strictly before eval_date."""
    prior = [d for d in dates if d < eval_date][-roll_sessions:]
    rows: list[np.ndarray] = []
    for d in prior:
        rows.extend(by_date.get(d) or [])
    if not rows:
        return np.zeros((0, 3))
    return np.vstack(rows)


def _norm_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    idx = pd.to_datetime(out.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    out.index = idx.normalize()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["Close"])
    for col in ("Open", "High", "Low"):
        if col in out.columns:
            out[col] = out[col].fillna(out["Close"])
        else:
            out[col] = out["Close"]
    if "Volume" in out.columns:
        out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce")
    return out[~out.index.duplicated(keep="last")].sort_index()


def _liq_fail(market: str, px: float, mean_vol: float, mode: str = "live") -> bool:
    """mode live = hunter floors. prop = $30k US / $30k×FX KRW, no share min."""
    if market == "US":
        if px < US_MIN_PX:
            return True
        if mode == "prop":
            return bool(mean_vol < (PROP_US_DOLLAR / max(px, 0.01)))
        min_vol = max(LIVE_US_SHARE_MIN, LIVE_US_DOLLAR / max(px, 0.01))
        return bool(mean_vol < min_vol)
    if px < KR_MIN_PX:
        return True
    if mode == "prop":
        return bool(mean_vol < ((PROP_US_DOLLAR * KR_USD_FX) / max(px, 1.0)))
    return bool(mean_vol < KR_MIN_VOL)


def _close_now(market: str) -> datetime:
    if market == "KR":
        tz = pytz.timezone("Asia/Seoul")
        return datetime.now(tz).replace(hour=15, minute=30, second=0, microsecond=0)
    tz = pytz.timezone("America/New_York")
    return datetime.now(tz).replace(hour=16, minute=0, second=0, microsecond=0)


def _dna_vec(work: pd.DataFrame, market: str, now_mkt: datetime) -> np.ndarray | None:
    if len(work) < 20:
        return None
    dna = safe_supernova_dna_features(work, market=market, now_mkt=now_mkt)
    if not isinstance(dna, dict):
        return None
    vec = np.array(
        [float(dna["cpv"]), float(dna["tb"]), float(dna["bbe"])], dtype=float
    )
    if not all(_finite(x) for x in vec):
        return None
    return vec


def _best3(vec: np.ndarray, templates: dict[str, np.ndarray]) -> float:
    best = 0.0
    for tv in templates.values():
        if len(tv) != 3:
            continue
        best = max(best, _cosine(vec, tv))
    return best


def load_rank_templates(market: str) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    try:
        from config_manager import load_system_config
        from template_evolution import load_base_templates

        cfg = load_system_config()
        for name, vec in load_base_templates(cfg, market).items():
            arr = np.array(
                [float(vec[0]), float(vec[1]), float(vec[2])], dtype=float
            )
            if len(arr) == 3:
                out[str(name)] = arr
    except Exception as ex:
        print(f"[{market}] template kv skip: {ex}", flush=True)
    if not out:
        out = {k: v.copy() for k, v in (HARD_KR if market == "KR" else HARD_US).items()}
        print(f"[{market}] templates=hardcoded RANK 3D", flush=True)
    else:
        print(f"[{market}] rank3d templates n={len(out)}", flush=True)
    return out


def load_codes(market: str) -> list[tuple[str, str]]:
    """(code, yahoo_symbol)."""
    if market == "US":
        return [(c, c) for c in _na._load_universe()]
    from krx_list_survival import collect_krx_list_survival

    df, src = collect_krx_list_survival(db_path=None, fdr_module=None)
    if df is None or df.empty:
        import FinanceDataReader as fdr

        df, src = collect_krx_list_survival(db_path=None, fdr_module=fdr)
    if df is None or df.empty or "Code" not in df.columns:
        raise SystemExit("empty KR universe")
    rows: list[tuple[str, str]] = []
    for _, r in df.iterrows():
        code = str(r.get("Code") or "").strip().zfill(6)
        if len(code) != 6:
            continue
        mk = str(r.get("Market") or "").upper()
        suf = ".KQ" if "KOSDAQ" in mk or mk in ("KQ", "KSQ") else ".KS"
        rows.append((code, code + suf))
    print(f"KR universe n={len(rows)} src={src}", flush=True)
    return rows


def _volume_usable(df: pd.DataFrame) -> bool:
    if df is None or df.empty or "Volume" not in df.columns:
        return False
    v = pd.to_numeric(df["Volume"], errors="coerce")
    tail = v.iloc[-8:]
    if int(tail.notna().sum()) < 3:
        return False
    m = float(np.nanmean(np.asarray(tail, dtype=float)))
    return bool(np.isfinite(m) and m > 0)


def fetch_panel(pairs: list[tuple[str, str]], workers: int) -> dict[str, pd.DataFrame]:
    syms = [s for _, s in pairs]
    code_of = {s: c for c, s in pairs}
    raw = _na._download_all(syms, workers=workers)
    out: dict[str, pd.DataFrame] = {}
    need_chart: list[tuple[str, str]] = []
    for c, s in pairs:
        df = raw.get(s)
        if df is not None and not getattr(df, "empty", True):
            nd = _norm_index(df)
            if _volume_usable(nd):
                out[c] = nd
                continue
        need_chart.append((c, s))
    if need_chart:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        n = len(need_chart)
        done = 0
        print(f"chart volume repair n={n}", flush=True)
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(_na._chart_df, s): c for c, s in need_chart}
            for fut in as_completed(futs):
                c = futs[fut]
                done += 1
                try:
                    df = fut.result()
                except Exception:
                    df = None
                if df is not None and not getattr(df, "empty", True):
                    nd = _norm_index(df)
                    if _volume_usable(nd):
                        out[c] = nd
                if done % 50 == 0 or done == n:
                    print(f"chart {done}/{n} vol_ok={len(out)}", flush=True)
    return out


def session_dates(panel: dict[str, pd.DataFrame], need: int) -> list[str]:
    counts: dict[str, int] = defaultdict(int)
    for df in panel.values():
        for d in df.index:
            counts[pd.Timestamp(d).strftime("%Y-%m-%d")] += 1
    if not counts:
        return []
    thresh = max(8, int(0.08 * max(len(panel), 1)))
    dates = sorted(d for d, n in counts.items() if n >= thresh)
    if not dates:
        dates = sorted(counts.keys())
    return dates[-need:]


def collect_day_rows(
    panel: dict[str, pd.DataFrame],
    market: str,
    day: str,
    now_mkt: datetime,
    mode: str = "live",
) -> tuple[list[np.ndarray], int, int]:
    """Return LIQ-pass DNA rows, n_liq_fail, n_dna_fail."""
    rows: list[np.ndarray] = []
    liq_fail = 0
    dna_fail = 0
    ts = pd.Timestamp(day)
    for df in panel.values():
        work = df.loc[df.index <= ts]
        if len(work) < 20:
            dna_fail += 1
            continue
        close = _series_1d(work, "Close")
        vol = _series_1d(work, "Volume")
        px = float(close.iloc[-1]) if len(close) else float("nan")
        v5 = np.asarray(vol.iloc[-5:] if len(vol) else [], dtype=float)
        mean_vol = float(np.nanmean(v5)) if v5.size else float("nan")
        if not np.isfinite(mean_vol):
            liq_fail += 1
            continue
        if not _finite(px) or _liq_fail(market, px, mean_vol, mode=mode):
            liq_fail += 1
            continue
        vec = _dna_vec(work, market, now_mkt)
        if vec is None:
            dna_fail += 1
            continue
        rows.append(vec)
    return rows, liq_fail, dna_fail


def collect_day_rows_dual(
    panel: dict[str, pd.DataFrame],
    market: str,
    day: str,
    now_mkt: datetime,
) -> tuple[list[np.ndarray], list[np.ndarray], dict]:
    """One DNA pass; split live vs prop LIQ (stage 2)."""
    live_rows: list[np.ndarray] = []
    prop_rows: list[np.ndarray] = []
    both_fail = 0
    dna_fail = 0
    ts = pd.Timestamp(day)
    for df in panel.values():
        work = df.loc[df.index <= ts]
        if len(work) < 20:
            dna_fail += 1
            continue
        close = _series_1d(work, "Close")
        vol = _series_1d(work, "Volume")
        px = float(close.iloc[-1]) if len(close) else float("nan")
        v5 = np.asarray(vol.iloc[-5:] if len(vol) else [], dtype=float)
        mean_vol = float(np.nanmean(v5)) if v5.size else float("nan")
        if not np.isfinite(mean_vol) or not _finite(px):
            both_fail += 1
            continue
        live_f = _liq_fail(market, px, mean_vol, mode="live")
        prop_f = _liq_fail(market, px, mean_vol, mode="prop")
        if live_f and prop_f:
            both_fail += 1
            continue
        vec = _dna_vec(work, market, now_mkt)
        if vec is None:
            dna_fail += 1
            continue
        if not live_f:
            live_rows.append(vec)
        if not prop_f:
            prop_rows.append(vec)
    meta = {
        "n_liq_pass_live": len(live_rows),
        "n_liq_pass_prop": len(prop_rows),
        "n_liq_fail_both": both_fail,
        "n_dna_fail": dna_fail,
    }
    return live_rows, prop_rows, meta


def percentile_cuts(scores: np.ndarray) -> dict[str, float]:
    if len(scores) == 0:
        return {}
    return {
        "p80_top20": float(np.percentile(scores, 80)),
        "p90_top10": float(np.percentile(scores, 90)),
        "p95_top5": float(np.percentile(scores, 95)),
    }


def _fold(combo: int, live: int) -> float | None:
    if live <= 0:
        return None
    return round(combo / live, 3)


def run_market(
    market: str,
    *,
    max_tickers: int,
    workers: int,
    roll_sessions: int,
    eval_sessions: int,
    seed: int,
    stage: int = 1,
) -> dict:
    pairs = load_codes(market)
    rng = np.random.default_rng(seed)
    if max_tickers and max_tickers < len(pairs):
        idx = rng.choice(len(pairs), size=max_tickers, replace=False)
        pairs = [pairs[int(i)] for i in idx]
        print(f"[{market}] sample n={len(pairs)}", flush=True)
    templates = load_rank_templates(market)
    print(f"[{market}] fetch charts…", flush=True)
    panel = fetch_panel(pairs, workers=workers)
    print(f"[{market}] panel={len(panel)}/{len(pairs)}", flush=True)
    need = roll_sessions + eval_sessions + 1
    dates = session_dates(panel, need)
    print(f"[{market}] session dates n={len(dates)} last={dates[-1] if dates else None}", flush=True)
    now_mkt = _close_now(market)
    by_date: dict[str, list[np.ndarray]] = {}
    by_date_prop: dict[str, list[np.ndarray]] = {}
    meta: dict[str, dict] = {}
    for d in dates:
        if stage == 2:
            live_rows, prop_rows, m = collect_day_rows_dual(panel, market, d, now_mkt)
            by_date[d] = live_rows
            by_date_prop[d] = prop_rows
            meta[d] = m
            print(
                f"[{market}] {d} live_liq={len(live_rows)} prop_liq={len(prop_rows)} "
                f"both_fail={m['n_liq_fail_both']} dna_fail={m['n_dna_fail']}",
                flush=True,
            )
        else:
            rows, liq_fail, dna_fail = collect_day_rows(
                panel, market, d, now_mkt, mode="live"
            )
            by_date[d] = rows
            meta[d] = {
                "n_liq_pass": len(rows),
                "n_liq_fail": liq_fail,
                "n_dna_fail": dna_fail,
            }
            print(
                f"[{market}] {d} liq_pass={len(rows)} liq_fail={liq_fail} dna_fail={dna_fail}",
                flush=True,
            )

    eval_dates = dates[-eval_sessions:] if len(dates) >= eval_sessions else dates[1:]
    day_reports = []
    pooled_z: list[float] = []
    pooled_raw: list[float] = []
    pooled_combo = 0
    pooled_live050 = 0
    for d in eval_dates:
        if stage == 2:
            hist = prior_session_rows(by_date_prop, dates, d, roll_sessions)
            if len(hist) < 30:
                day_reports.append(
                    {"date": d, "skip": "hist_n<30", "hist_n": int(len(hist))}
                )
                continue
            mu, sd = fit_mu_sd(hist)
            z_tpl = {k: apply_z(v, mu, sd) for k, v in templates.items()}
            live_vecs = by_date.get(d) or []
            prop_vecs = by_date_prop.get(d) or []
            live_raw = np.array(
                [_best3(v, templates) for v in live_vecs], dtype=float
            )
            z_s = np.array(
                [_best3(apply_z(v, mu, sd), z_tpl) for v in prop_vecs], dtype=float
            )
            live050 = int((live_raw >= 0.50).sum()) if len(live_raw) else 0
            p90 = float(np.percentile(z_s, 90)) if len(z_s) else None
            combo = int((z_s >= p90).sum()) if p90 is not None else 0
            rec = {
                "date": d,
                "hist_n": int(len(hist)),
                "eval_n_live": int(len(live_vecs)),
                "eval_n_prop": int(len(prop_vecs)),
                "mu": [float(x) for x in mu],
                "sd": [float(x) for x in sd],
                "live_raw_ge_050": live050,
                "p90_z": p90,
                "combo_axis_p10_liq30k": combo,
                "fold_vs_live": _fold(combo, live050),
                **meta.get(d, {}),
            }
            day_reports.append(rec)
            pooled_z.extend(float(x) for x in z_s)
            pooled_live050 += live050
            pooled_combo += combo
            print(json.dumps(rec, ensure_ascii=False), flush=True)
            continue
        hist = prior_session_rows(by_date, dates, d, roll_sessions)
        if len(hist) < 30:
            day_reports.append({"date": d, "skip": "hist_n<30", "hist_n": int(len(hist))})
            continue
        mu, sd = fit_mu_sd(hist)
        z_tpl = {k: apply_z(v, mu, sd) for k, v in templates.items()}
        live = by_date.get(d) or []
        raw_s = np.array([_best3(v, templates) for v in live], dtype=float)
        z_s = np.array(
            [_best3(apply_z(v, mu, sd), z_tpl) for v in live], dtype=float
        )
        cuts = percentile_cuts(z_s)
        rec = {
            "date": d,
            "hist_n": int(len(hist)),
            "eval_n": int(len(live)),
            "mu": [float(x) for x in mu],
            "sd": [float(x) for x in sd],
            "raw_median": float(np.median(raw_s)) if len(raw_s) else None,
            "raw_ge_050": int((raw_s >= 0.50).sum()) if len(raw_s) else 0,
            "z_median": float(np.median(z_s)) if len(z_s) else None,
            "z_ge_050": int((z_s >= 0.50).sum()) if len(z_s) else 0,
            "cuts": cuts,
            "pass_if_cut": {
                k: int((z_s >= v).sum()) for k, v in cuts.items()
            },
            **meta.get(d, {}),
        }
        day_reports.append(rec)
        pooled_z.extend(float(x) for x in z_s)
        pooled_raw.extend(float(x) for x in raw_s)
        print(json.dumps(rec, ensure_ascii=False), flush=True)

    pz = np.asarray(pooled_z, dtype=float)
    pr = np.asarray(pooled_raw, dtype=float)
    if stage == 2:
        n_days = sum(1 for r in day_reports if "combo_axis_p10_liq30k" in r)
        summary = {
            "market": market,
            "stage": 2,
            "panel": len(panel),
            "roll_sessions": roll_sessions,
            "eval_sessions": eval_sessions,
            "kr_liq_note": (
                "KR prop = $30k×1350 KRW ADV, no 50k-share min. "
                "Literal 30000/px on KRW would be ~zero floor."
                if market == "KR"
                else "US prop = mean(vol)<30000/px, no 2000-share min."
            ),
            "combo_pass_pooled": pooled_combo,
            "live_raw050_pooled": pooled_live050,
            "mean_combo_per_day": round(pooled_combo / n_days, 2) if n_days else None,
            "mean_live050_per_day": round(pooled_live050 / n_days, 2) if n_days else None,
            "fold_pooled": _fold(pooled_combo, pooled_live050),
            "days": day_reports,
            "stats_key": COSINE_AXIS_STATS_KEY,
        }
        return summary
    pool_cuts = percentile_cuts(pz)
    summary = {
        "market": market,
        "stage": 1,
        "panel": len(panel),
        "roll_sessions": roll_sessions,
        "eval_sessions": eval_sessions,
        "pooled_n": int(len(pz)),
        "raw_ge_050": int((pr >= 0.50).sum()) if len(pr) else 0,
        "z_ge_050": int((pz >= 0.50).sum()) if len(pz) else 0,
        "pooled_cuts": pool_cuts,
        "pooled_pass_if_cut": {
            k: int((pz >= v).sum()) for k, v in pool_cuts.items()
        },
        "days": day_reports,
        "stats_key": COSINE_AXIS_STATS_KEY,
    }
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=("US", "KR", "both"), default="both")
    ap.add_argument("--max-tickers", type=int, default=600)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--roll-sessions", type=int, default=5)
    ap.add_argument("--eval-sessions", type=int, default=5)
    ap.add_argument("--seed", type=int, default=24)
    ap.add_argument("--stage", type=int, choices=(1, 2), default=2)
    args = ap.parse_args()
    logging.getLogger("scan_resilience").setLevel(logging.CRITICAL)
    markets = ["US", "KR"] if args.market == "both" else [args.market]
    blob = {
        "stage": args.stage,
        "side_notes": {
            "elastic": (
                "Live ElasticThreshold.apply_pair stretches DYNAMIC_SUPERNOVA_CUTOFF "
                "before compare (starvation relief, vol tighten, META regime penalty). "
                "This shadow does not apply elastic. Live pass counts can be lower."
            ),
            "synergy": (
                "per_ticker_scan_adjustments multiplies eff_cos_cutoff "
                "(rotation/spillover can drop cutoff, min clip). Not applied here. "
                "Live pass counts can be lower than this shadow."
            ),
            "multi": (
                "DNA_SUPERNOVA_{M}_MULTI may be 24-d (20 shape + cpv/tb/bbe/rs). "
                "Handoff locks RANK 3D only; MULTI 5D path excluded."
            ),
            "liq": (
                "stage1=live floors. stage2 US=$30k no share min; "
                "KR=$30k×1350 KRW ADV no 5만주. Hunter/KV untouched."
            ),
            "cut": (
                "stage2 cut = daily 90th percentile of z-cosine among prop-LIQ "
                "(top 10%). 5%/20% not run."
            ),
            "stats_store": (
                f"Live later: config_kv '{COSINE_AXIS_STATS_KEY}' "
                "per market mu/sd/n/as_of/window_sessions. Write end-of-scan, "
                "read next session. Not written by this script."
            ),
        },
        "markets": {},
    }
    for mk in markets:
        blob["markets"][mk] = run_market(
            mk,
            max_tickers=args.max_tickers,
            workers=args.workers,
            roll_sessions=args.roll_sessions,
            eval_sessions=args.eval_sessions,
            seed=args.seed,
            stage=args.stage,
        )
    out_name = (
        "diag_cosine_axis_liq30k_p10_shadow_last.json"
        if args.stage == 2
        else "diag_cosine_axis_rolling_shadow_last.json"
    )
    out_path = ROOT / "scripts" / out_name
    out_path.write_text(json.dumps(blob, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
