"""
B-3 — Walk-Forward + Purged/Embargo CV + Deflated Sharpe (Bitget port).

루트 ``validation/walk_forward.py`` 순수 함수 **포팅** (import 금지 · CAT-MAP §5).
라이브 매매·registry/config **무쓰기** — shadow 판정 전용.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import numpy as np

_EULER_GAMMA = 0.5772156649015329

DEFAULT_WF_N_SPLITS = 3
DEFAULT_WF_MIN_TRAIN_FRAC = 0.5
# Gate: ≥12 closed (3-fold WF needs train mass at 50% min_train) · ≥5 OOS (last-fold mean stability)
DEFAULT_WF_MIN_TOTAL_TRADES = 12
DEFAULT_WF_MIN_OOS_TRADES = 5


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


def sharpe_ratio(returns: Sequence[float], periods_per_year: float = 1.0) -> float:
    r = np.asarray(list(returns), dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size < 2:
        return 0.0
    sd = float(np.std(r, ddof=1))
    if sd <= 1e-12:
        return 0.0
    sr = float(np.mean(r)) / sd
    if periods_per_year and periods_per_year > 1:
        sr *= math.sqrt(periods_per_year)
    return sr


def _skew_kurt(returns: np.ndarray) -> Tuple[float, float]:
    r = returns[np.isfinite(returns)]
    n = r.size
    if n < 3:
        return 0.0, 3.0
    m = float(np.mean(r))
    sd = float(np.std(r, ddof=0))
    if sd <= 1e-12:
        return 0.0, 3.0
    z = (r - m) / sd
    skew = float(np.mean(z ** 3))
    kurt = float(np.mean(z ** 4))
    return skew, kurt


def probabilistic_sharpe_ratio(
    observed_sr: float,
    n_samples: int,
    *,
    skew: float = 0.0,
    kurt: float = 3.0,
    sr_benchmark: float = 0.0,
) -> float:
    if n_samples < 2:
        return 0.0
    denom = 1.0 - skew * observed_sr + ((kurt - 1.0) / 4.0) * (observed_sr ** 2)
    if denom <= 1e-12:
        return 0.0
    z = (observed_sr - sr_benchmark) * math.sqrt(n_samples - 1) / math.sqrt(denom)
    return float(_norm_cdf(z))


def expected_max_sharpe(n_trials: int, sr_variance: float) -> float:
    if n_trials < 2 or sr_variance <= 0:
        return 0.0
    n = float(n_trials)
    term = (1.0 - _EULER_GAMMA) * _norm_ppf(1.0 - 1.0 / n) + \
        _EULER_GAMMA * _norm_ppf(1.0 - 1.0 / (n * math.e))
    return math.sqrt(sr_variance) * term


def deflated_sharpe_ratio(
    observed_sr: float,
    *,
    sr_variance_trials: float,
    n_trials: int,
    n_samples: int,
    skew: float = 0.0,
    kurt: float = 3.0,
) -> Dict[str, float]:
    sr_star = expected_max_sharpe(n_trials, sr_variance_trials)
    dsr = probabilistic_sharpe_ratio(
        observed_sr, n_samples, skew=skew, kurt=kurt, sr_benchmark=sr_star
    )
    psr0 = probabilistic_sharpe_ratio(
        observed_sr, n_samples, skew=skew, kurt=kurt, sr_benchmark=0.0
    )
    return {"dsr": float(dsr), "sr_star": float(sr_star), "psr0": float(psr0)}


def deflated_sharpe_from_trials(
    trial_returns: Sequence[Sequence[float]],
    *,
    target_index: Optional[int] = None,
) -> Dict[str, float]:
    out = {
        "dsr": 0.0, "sr_star": 0.0, "psr0": 0.0, "observed_sr": 0.0,
        "n_trials": 0, "n_samples": 0, "target_index": -1,
    }
    series = [np.asarray(list(s), dtype=np.float64) for s in (trial_returns or [])]
    series = [s[np.isfinite(s)] for s in series if s is not None]
    series = [s for s in series if s.size >= 2]
    if len(series) < 2:
        return out
    srs = [sharpe_ratio(s) for s in series]
    sr_var = float(np.var(np.asarray(srs), ddof=1)) if len(srs) >= 2 else 0.0
    if target_index is None:
        target_index = int(np.argmax(srs))
    target_index = max(0, min(int(target_index), len(series) - 1))
    tgt = series[target_index]
    skew, kurt = _skew_kurt(tgt)
    res = deflated_sharpe_ratio(
        srs[target_index],
        sr_variance_trials=sr_var,
        n_trials=len(series),
        n_samples=int(tgt.size),
        skew=skew,
        kurt=kurt,
    )
    out.update(res)
    out.update({
        "observed_sr": float(srs[target_index]),
        "n_trials": len(series),
        "n_samples": int(tgt.size),
        "target_index": target_index,
    })
    return out


def purged_kfold_indices(
    n_samples: int,
    n_splits: int = 5,
    *,
    embargo_pct: float = 0.01,
    label_horizon: int = 0,
) -> Iterator[Tuple[np.ndarray, np.ndarray]]:
    if n_samples <= 0 or n_splits < 2:
        return
    indices = np.arange(n_samples)
    folds = np.array_split(indices, n_splits)
    embargo = int(round(n_samples * max(0.0, embargo_pct)))
    h = max(0, int(label_horizon))
    for test_idx in folds:
        if test_idx.size == 0:
            continue
        t0, t1 = int(test_idx[0]), int(test_idx[-1])
        train_mask = np.ones(n_samples, dtype=bool)
        lo = max(0, t0 - h)
        train_mask[lo: t1 + 1] = False
        emb_end = min(n_samples, t1 + 1 + embargo)
        train_mask[t1 + 1: emb_end] = False
        train_idx = indices[train_mask]
        if train_idx.size and test_idx.size:
            yield train_idx, test_idx


def walk_forward_splits(
    n_samples: int,
    n_splits: int = 5,
    *,
    expanding: bool = True,
    min_train_frac: float = 0.3,
    embargo_pct: float = 0.0,
) -> List[Tuple[np.ndarray, np.ndarray]]:
    splits: List[Tuple[np.ndarray, np.ndarray]] = []
    if n_samples <= 0 or n_splits < 1:
        return splits
    start = int(n_samples * min(max(min_train_frac, 0.05), 0.9))
    start = max(start, 1)
    remaining = n_samples - start
    if remaining < n_splits:
        return splits
    test_size = remaining // n_splits
    embargo = int(round(n_samples * max(0.0, embargo_pct)))
    base_train = start
    for k in range(n_splits):
        ts = start + k * test_size
        te = n_samples if k == n_splits - 1 else start + (k + 1) * test_size
        train_end = max(0, ts - embargo)
        train_start = 0 if expanding else max(0, train_end - base_train)
        train_idx = np.arange(train_start, train_end)
        test_idx = np.arange(ts, te)
        if train_idx.size and test_idx.size:
            splits.append((train_idx, test_idx))
    return splits


def evaluate_oos_pass_from_returns(
    returns: Sequence[float],
    *,
    n_splits: int = DEFAULT_WF_N_SPLITS,
    min_train_frac: float = DEFAULT_WF_MIN_TRAIN_FRAC,
    min_total_trades: int = DEFAULT_WF_MIN_TOTAL_TRADES,
    min_oos_trades: int = DEFAULT_WF_MIN_OOS_TRADES,
) -> Dict[str, Any]:
    """
    Chronological closed-trade returns → last walk-forward fold OOS pass/fail.

    ``returns`` are decimal (e.g. final_ret / 100). Pass when OOS mean > 0.
    """
    r = np.asarray(list(returns), dtype=np.float64)
    r = r[np.isfinite(r)]
    n = int(r.size)
    if n < int(min_total_trades):
        return {
            "pass": False,
            "reason": "insufficient_data",
            "n_closed": n,
            "oos_n": 0,
            "oos_mean": 0.0,
            "oos_sharpe": 0.0,
        }

    splits = walk_forward_splits(
        n,
        n_splits=int(n_splits),
        min_train_frac=float(min_train_frac),
    )
    if not splits:
        return {
            "pass": False,
            "reason": "no_splits",
            "n_closed": n,
            "oos_n": 0,
            "oos_mean": 0.0,
            "oos_sharpe": 0.0,
        }

    _train_idx, test_idx = splits[-1]
    oos = r[test_idx]
    oos_n = int(oos.size)
    if oos_n < int(min_oos_trades):
        return {
            "pass": False,
            "reason": "oos_too_few",
            "n_closed": n,
            "oos_n": oos_n,
            "oos_mean": float(np.mean(oos)) if oos_n else 0.0,
            "oos_sharpe": sharpe_ratio(oos),
        }

    oos_mean = float(np.mean(oos))
    return {
        "pass": oos_mean > 0.0,
        "reason": "oos_pass" if oos_mean > 0.0 else "oos_fail",
        "n_closed": n,
        "oos_n": oos_n,
        "oos_mean": oos_mean,
        "oos_sharpe": sharpe_ratio(oos),
    }
