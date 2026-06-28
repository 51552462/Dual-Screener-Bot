"""
Project 2: Hyperbolic Time Chamber — 다중 국면(Multi-Regime) 합성 일봉 OHLCV 생성기 (완전 격리).

- 기존 market_data.sqlite / system_config.json / 스크리너 모듈과 무관.
- 출력 전용 DB: synthetic_market.sqlite, 테이블: synthetic_ohlcv (+ synthetic_meta)

[Mission 4] 은닉 마르코프(HMM) 기반 체제 전환(Regime Switching):
  Bull · Bear · Sideways · Black Swan 4개 국면이 전이행렬을 따라 확률적으로 이어지는
  '다이나믹 평행우주' 1,000일 시계열을 만든다. 각 국면은 자체 드리프트/변동성/점프 모수를 갖는다.

[Mission 1] 공정한 테스트 베드:
  전이행렬의 정상분포(stationary distribution)로 계산한 '무조건 기대수익(baseline drift)'을
  자동으로 0 에 수렴시킨다(전 국면 로그-드리프트에 보정 오프셋 가산). 따라서 어떤 전략이든
  '시장 중력'이 아니라 '진짜 알파'로만 평가된다.
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SYNTHETIC_DB_PATH = os.path.join(_THIS_DIR, "synthetic_market.sqlite")

NUM_TRADING_DAYS = 1000
NUM_PARALLEL_UNIVERSES = 100
TICKER_PREFIX = "SYN_"
RNG = np.random.default_rng()

# ── [유체 방어 #3] 장중 플래시 크래시(Intrabar Low Spike) — 합성 훈련용 ──────
#   BLACK_SWAN/HIGH_VOL 국면 봉에 확률적으로 긴 하단 꼬리(고가·시가 대비 15~20%)를
#   순간 주입했다가 종가에 회복시킨다. 종가(close)는 불변 → 무조건 드리프트 0 보정 유지.
FLASH_CRASH_REGIMES: Tuple[str, ...] = ("BLACK_SWAN", "HIGH_VOL")  # 미정의 국면은 자동 무시
FLASH_CRASH_PROB = 0.18          # 해당 국면 일자당 스파이크 발생 확률
FLASH_CRASH_MIN_DEPTH = 0.15     # 저가 깊이 하한(min(open,close) 대비)
FLASH_CRASH_MAX_DEPTH = 0.20     # 저가 깊이 상한


# ---------------------------------------------------------------------------
# 국면 정의 (로그수익 모수) — Merton 점프-확산
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Regime:
    name: str
    mu: float          # 일간 로그 드리프트
    sigma: float       # 확산 변동성
    lam: float         # 점프 발생률(Poisson)
    mu_jump: float     # 점프 평균(로그)
    sigma_jump: float  # 점프 표준편차
    vol_surge: float   # 거래량 서지 배수(국면 평균)


REGIMES: Tuple[Regime, ...] = (
    #         name          mu       sigma   lam    mu_jump  sig_jump vol_surge
    Regime("SIDEWAYS",   0.00000,  0.0110, 0.020,  0.000,   0.020,   1.00),
    Regime("BULL",       0.00120,  0.0130, 0.020,  0.006,   0.022,   1.15),
    Regime("BEAR",      -0.00110,  0.0210, 0.055, -0.012,   0.035,   1.40),
    Regime("BLACK_SWAN",-0.00450,  0.0480, 0.320, -0.040,   0.065,   3.20),
)
REGIME_INDEX: Dict[str, int] = {r.name: i for i, r in enumerate(REGIMES)}

# 행: from-state, 열: to-state. SIDEWAYS 가 끈적(sticky)하고 BLACK_SWAN 은 짧게 머문다.
#                 →SIDE   →BULL   →BEAR   →SWAN
TRANSITION = np.array([
    [0.940,  0.040,  0.018,  0.002],   # from SIDEWAYS
    [0.060,  0.910,  0.028,  0.002],   # from BULL
    [0.080,  0.020,  0.880,  0.020],   # from BEAR
    [0.250,  0.020,  0.530,  0.200],   # from BLACK_SWAN
], dtype=np.float64)


def stationary_distribution(P: np.ndarray) -> np.ndarray:
    """전이행렬 P 의 정상분포 π (πP = π) — 좌고유벡터(고유값 1)."""
    vals, vecs = np.linalg.eig(P.T)
    idx = int(np.argmin(np.abs(vals - 1.0)))
    pi = np.real(vecs[:, idx])
    pi = np.abs(pi)
    s = pi.sum()
    return pi / s if s > 0 else np.full(P.shape[0], 1.0 / P.shape[0])


def _regime_expected_simple_drift(r: Regime, mu_offset: float = 0.0) -> float:
    """국면별 1일 단순수익 기대값 근사: 확산 + 점프 (클립 무시 근사)."""
    diff = np.expm1((r.mu + mu_offset) + 0.5 * r.sigma ** 2)
    jump = r.lam * np.expm1(r.mu_jump + 0.5 * r.sigma_jump ** 2)
    return float(diff + jump)


def compute_baseline_drift_offset() -> Tuple[float, np.ndarray, float]:
    """
    정상분포 가중 무조건 드리프트가 0 이 되도록 전 국면 로그-mu 에 더할 오프셋 계산.
    반환: (offset, stationary_pi, uncorrected_drift)
    """
    pi = stationary_distribution(TRANSITION)
    raw = float(sum(p * _regime_expected_simple_drift(r) for p, r in zip(pi, REGIMES)))
    # dE[simple]/dmu ≈ exp(mu+0.5σ²) ≈ 1 이므로 offset ≈ -raw (1~2회면 충분히 0 수렴)
    offset = -raw
    for _ in range(3):
        cur = float(sum(p * _regime_expected_simple_drift(r, offset) for p, r in zip(pi, REGIMES)))
        offset -= cur
    return offset, pi, raw


BASELINE_MU_OFFSET, STATIONARY_PI, _RAW_DRIFT = compute_baseline_drift_offset()


# ---------------------------------------------------------------------------
# DB 스키마
# ---------------------------------------------------------------------------
def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS synthetic_ohlcv (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            regime TEXT,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_synth_ticker ON synthetic_ohlcv (ticker)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS synthetic_meta (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.commit()


def _sample_regime_path(n_days: int, rng: np.random.Generator) -> np.ndarray:
    """전이행렬을 따라 일자별 국면 인덱스 경로 샘플링 (정상분포에서 시작)."""
    path = np.empty(n_days, dtype=np.int64)
    state = int(rng.choice(len(REGIMES), p=STATIONARY_PI))
    cumP = np.cumsum(TRANSITION, axis=1)
    u = rng.random(n_days)
    for t in range(n_days):
        path[t] = state
        state = int(np.searchsorted(cumP[state], u[t]))
        if state >= len(REGIMES):
            state = len(REGIMES) - 1
    return path


def simulate_regime_switching_ohlcv(
    *,
    n_days: int,
    initial_price: float,
    seed: int | None = None,
) -> pd.DataFrame:
    """
    HMM 국면 경로 → 일자별 Merton 점프-확산 로그수익 → KRX ±30% 클립 → OHLCV.
    전 국면 mu 에 BASELINE_MU_OFFSET 가산(무조건 드리프트 0 보정).
    """
    rng = np.random.default_rng(seed)
    path = _sample_regime_path(n_days, rng)

    mu = np.array([REGIMES[i].mu for i in path]) + BASELINE_MU_OFFSET
    sigma = np.array([REGIMES[i].sigma for i in path])
    lam = np.array([REGIMES[i].lam for i in path])
    mu_j = np.array([REGIMES[i].mu_jump for i in path])
    sig_j = np.array([REGIMES[i].sigma_jump for i in path])
    vsurge = np.array([REGIMES[i].vol_surge for i in path])

    z = rng.standard_normal(n_days)
    n_jumps = rng.poisson(lam)
    jump_component = np.zeros(n_days, dtype=np.float64)
    mask = n_jumps > 0
    if np.any(mask):
        n_safe = np.maximum(n_jumps[mask], 1)
        y = rng.normal(mu_j[mask], sig_j[mask])
        jump_component[mask] = np.sqrt(n_safe) * y

    r_log = mu + sigma * z + jump_component
    simple_ret = np.expm1(r_log)
    clipped_ret = np.clip(simple_ret, -0.30, 0.30)

    close = np.empty(n_days, dtype=np.float64)
    close[0] = float(initial_price)
    if n_days > 1:
        close[1:] = close[0] * np.cumprod(1.0 + clipped_ret[:-1])

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]

    gap = rng.normal(0.0, 0.0045, size=n_days)
    open_px = prev_close * np.exp(gap)
    open_px[0] = close[0]
    o_rel = np.clip(open_px / np.maximum(prev_close, 1e-9) - 1.0, -0.12, 0.12)
    open_px = prev_close * (1.0 + o_rel)
    open_px[0] = close[0]

    upper_wick = rng.uniform(0.0008, 0.012, size=n_days)
    lower_wick = rng.uniform(0.0008, 0.012, size=n_days)
    body_high = np.maximum(open_px, close)
    body_low = np.minimum(open_px, close)
    high = body_high * (1.0 + upper_wick)
    low = body_low * (1.0 - lower_wick)
    high = np.maximum(high, np.maximum(open_px, close))
    low = np.minimum(low, np.minimum(open_px, close))

    # [유체 방어 #3] 장중 플래시 크래시: 꼬리 국면 봉에 확률적 Intrabar Low Spike 주입.
    #   저가(low)만 15~20% 깊게 빠지고 종가는 그대로 → 일중 급락 후 회복 캔들.
    flash_idx = [REGIME_INDEX[name] for name in FLASH_CRASH_REGIMES if name in REGIME_INDEX]
    do_flash = np.zeros(n_days, dtype=bool)
    if flash_idx:
        tail_mask = np.isin(path, flash_idx)
        do_flash = tail_mask & (rng.random(n_days) < FLASH_CRASH_PROB)
        if np.any(do_flash):
            depth = rng.uniform(FLASH_CRASH_MIN_DEPTH, FLASH_CRASH_MAX_DEPTH, size=n_days)
            spike_low = np.minimum(open_px, close) * (1.0 - depth)
            low = np.where(do_flash, np.minimum(low, spike_low), low)

    base = rng.lognormal(mean=14.0, sigma=0.45, size=n_days)
    eff_ret = np.zeros(n_days, dtype=np.float64)
    if n_days > 1:
        eff_ret[1:] = clipped_ret[:-1]
    limit_bar = (np.abs(eff_ret) >= 0.299).astype(np.float64)
    jump_bar = np.zeros(n_days, dtype=np.float64)
    if n_days > 1:
        jump_bar[1:] = (n_jumps[:-1] > 0).astype(np.float64)
    vol_mult = vsurge * (
        1.0 + 2.8 * jump_bar + 1.9 * limit_bar + 12.0 * np.abs(eff_ret)
        + 6.0 * do_flash.astype(np.float64)  # 플래시 크래시 봉 거래량 패닉 가산
    )
    volume = np.clip(base * vol_mult, 1_000.0, 5e9)

    idx = pd.bdate_range("2018-01-02", periods=n_days, freq="B")
    return pd.DataFrame(
        {
            "date": idx.strftime("%Y-%m-%d"),
            "open": open_px,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "regime": [REGIMES[i].name for i in path],
        }
    )


def write_universe_to_sqlite(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> None:
    m = df[["date", "open", "high", "low", "close", "volume", "regime"]].to_numpy(dtype=object)
    rows = [
        (ticker, str(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), str(r[6]))
        for r in m
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO synthetic_ohlcv
        (ticker, date, open, high, low, close, volume, regime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def generate_all_parallel_universes() -> None:
    os.makedirs(os.path.dirname(SYNTHETIC_DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(SYNTHETIC_DB_PATH, timeout=60.0)
    regime_counts: Dict[str, int] = {r.name: 0 for r in REGIMES}
    try:
        _init_db(conn)
        conn.execute("DELETE FROM synthetic_ohlcv")

        for k in range(1, NUM_PARALLEL_UNIVERSES + 1):
            ticker = f"{TICKER_PREFIX}{k:03d}"
            seed = 10_000 + k * 9973
            init_px = float(RNG.uniform(8_000.0, 120_000.0))
            df = simulate_regime_switching_ohlcv(
                n_days=NUM_TRADING_DAYS, initial_price=init_px, seed=seed
            )
            vc = df["regime"].value_counts()
            for name, cnt in vc.items():
                regime_counts[str(name)] = regime_counts.get(str(name), 0) + int(cnt)
            write_universe_to_sqlite(conn, ticker, df)
            if k == 1 or k % 25 == 0:
                print(f"  ✓ {ticker}  ({NUM_TRADING_DAYS} rows)")

        total_bars = sum(regime_counts.values()) or 1
        regime_mix = {n: round(c / total_bars, 4) for n, c in regime_counts.items()}
        meta = {
            "regime_mix": str(regime_mix),
            "baseline_mu_offset": f"{BASELINE_MU_OFFSET:.8f}",
            "uncorrected_drift": f"{_RAW_DRIFT:.8f}",
            "stationary_pi": str({REGIMES[i].name: round(float(STATIONARY_PI[i]), 4) for i in range(len(REGIMES))}),
            "n_tickers": str(NUM_PARALLEL_UNIVERSES),
            "n_days": str(NUM_TRADING_DAYS),
        }
        conn.executemany(
            "INSERT OR REPLACE INTO synthetic_meta (key, value) VALUES (?, ?)",
            list(meta.items()),
        )
        conn.commit()
    finally:
        conn.close()

    print(f"\n완료: {NUM_PARALLEL_UNIVERSES} 티커 × {NUM_TRADING_DAYS} 일 → {SYNTHETIC_DB_PATH}")
    print(f"국면 혼합: {regime_mix}")
    print(f"무조건 드리프트 보정: raw={_RAW_DRIFT:+.6f} → offset={BASELINE_MU_OFFSET:+.6f} (목표 ≈ 0)")


def stress_test_mutants() -> None:
    """오토파일럿 Sat 00:00 훅 별칭 — 다중 국면 합성 평행우주 재생성."""
    generate_all_parallel_universes()


if __name__ == "__main__":
    print("🌀 [Hyperbolic Time Chamber] 다중 국면 합성 시장 생성 시작…")
    print(f"  정상분포 π = {{{', '.join(f'{REGIMES[i].name}:{STATIONARY_PI[i]:.3f}' for i in range(len(REGIMES)))}}}")
    generate_all_parallel_universes()
