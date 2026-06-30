"""
[P2-1] Walk-Forward + Purged/Embargo CV + Deflated Sharpe 검증 하네스.

목적: "백테스트는 좋은데 라이브는 죽는" 과최적화(false discovery) 근절.
- López de Prado식 **Purged K-Fold + Embargo**: 라벨 평가창이 테스트구간과 겹치는 학습표본 제거(purge)
  + 테스트 직후 금수기간(embargo)으로 정보누수 차단.
- **Walk-Forward** 분할: 앵커드(확장)/롤링 시계열 분할.
- **PSR/Deflated Sharpe**: 다중검정(N회 시도) 보정 — 관측 샤프가 '우연한 최댓값' 임계를 넘는지 확률화.

전부 numpy 순수 함수(scipy 불요). 입력 부족/오류 시 방어적으로 0·빈값 반환. **라이브 매매 무영향.**
"""
from __future__ import annotations

import math
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import numpy as np

_EULER_GAMMA = 0.5772156649015329


# ── 정규분포 (scipy 없이) ────────────────────────────────────────────────────
def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """표준정규 분위수(Acklam 유리근사). p∈(0,1) 클램프."""
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


# ── Sharpe / PSR / DSR ──────────────────────────────────────────────────────
def sharpe_ratio(returns: Sequence[float], periods_per_year: float = 1.0) -> float:
    """표본 Sharpe(기본 per-period; periods_per_year>1 이면 연율화)."""
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
    kurt = float(np.mean(z ** 4))  # 정규=3
    return skew, kurt


def probabilistic_sharpe_ratio(
    observed_sr: float,
    n_samples: int,
    *,
    skew: float = 0.0,
    kurt: float = 3.0,
    sr_benchmark: float = 0.0,
) -> float:
    """PSR = P(true SR > sr_benchmark). observed_sr/sr_benchmark 는 per-period(비연율) Sharpe."""
    if n_samples < 2:
        return 0.0
    denom = 1.0 - skew * observed_sr + ((kurt - 1.0) / 4.0) * (observed_sr ** 2)
    if denom <= 1e-12:
        return 0.0
    z = (observed_sr - sr_benchmark) * math.sqrt(n_samples - 1) / math.sqrt(denom)
    return float(_norm_cdf(z))


def expected_max_sharpe(n_trials: int, sr_variance: float) -> float:
    """N회 독립 시도에서 '우연히' 나오는 최대 Sharpe의 기댓값(per-period)."""
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
    """Deflated Sharpe Ratio = PSR(sr_benchmark = expected_max_sharpe).

    반환: {dsr, sr_star(임계), psr0(vs 0)}. observed_sr 는 per-period Sharpe.
    dsr 가 0.95+ 이면 다중검정 보정 후에도 통계적으로 유의.
    """
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
    """여러 시도(전략)의 수익률 시퀀스 목록 → 대상(기본=최고 SR)의 DSR.

    trial_returns: [[r,...], [r,...], ...]. target_index None 이면 최고 Sharpe 시도 선택.
    반환: {dsr, sr_star, psr0, observed_sr, n_trials, n_samples, target_index}.
    """
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


# ── 시계열 분할(누수 차단) ───────────────────────────────────────────────────
def purged_kfold_indices(
    n_samples: int,
    n_splits: int = 5,
    *,
    embargo_pct: float = 0.01,
    label_horizon: int = 0,
) -> Iterator[Tuple[np.ndarray, np.ndarray]]:
    """Purged K-Fold + Embargo. 테스트구간과 라벨창이 겹치는 학습표본 제거 + 금수기간.

    label_horizon: 각 표본 라벨이 미래 몇 바를 참조하는지(겹침 purge 폭).
    yields (train_idx, test_idx).
    """
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
        lo = max(0, t0 - h)                       # purge: 라벨창이 테스트와 겹치는 학습표본
        train_mask[lo: t1 + 1] = False
        emb_end = min(n_samples, t1 + 1 + embargo)  # embargo: 테스트 직후 금수
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
    """앵커드(확장)/롤링 워크포워드 분할 — 항상 과거로 학습, 미래로 테스트(순방향)."""
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


# ── 원장(forward_trades) 진단 헬퍼 ───────────────────────────────────────────
def evaluate_ledger_deflated_sharpe(
    df_closed,
    *,
    ret_col: str = "final_ret",
    strategy_col: str = "sig_type",
    derive_strategy: bool = True,
    min_trades_per_strategy: int = 10,
) -> Dict[str, object]:
    """청산 원장 → 전략들을 '시도(trials)'로 보고 최고 전략의 DSR 산출.

    final_ret(%) → 소수 수익률. 전략 분해 후 표본 충분한 전략들만 trials 로 사용.
    반환: deflated_sharpe_from_trials 결과 + {strategies:[{key,n,sharpe}]}.
    """
    base = {
        "dsr": 0.0, "sr_star": 0.0, "psr0": 0.0, "observed_sr": 0.0,
        "n_trials": 0, "n_samples": 0, "target_index": -1, "strategies": [],
    }
    try:
        import pandas as pd  # 지연 임포트
    except Exception:
        return base
    if df_closed is None or getattr(df_closed, "empty", True) or ret_col not in df_closed.columns:
        return base
    df = df_closed.copy()
    df[ret_col] = pd.to_numeric(df[ret_col], errors="coerce")
    df = df.dropna(subset=[ret_col])
    if df.empty:
        return base

    # 전략 라벨
    try:
        if derive_strategy:
            from reports.forward_report_scalar import _strategy_label_from_sig

            src = strategy_col if strategy_col in df.columns else "sig_type"
            if src not in df.columns:
                return base
            df["_grp"] = df[src].apply(_strategy_label_from_sig)
        else:
            if strategy_col not in df.columns:
                return base
            df["_grp"] = df[strategy_col].astype(str)
    except Exception:
        return base

    trial_returns: List[List[float]] = []
    meta: List[Dict[str, object]] = []
    for key, g in df.groupby("_grp"):
        r = (g[ret_col].to_numpy(dtype=np.float64) / 100.0)
        r = r[np.isfinite(r)]
        if r.size < int(min_trades_per_strategy):
            continue
        trial_returns.append(r.tolist())
        meta.append({"key": str(key), "n": int(r.size), "sharpe": round(sharpe_ratio(r), 4)})
    if len(trial_returns) < 2:
        return base
    res = deflated_sharpe_from_trials(trial_returns)
    res["strategies"] = sorted(meta, key=lambda x: x["sharpe"], reverse=True)
    return res
