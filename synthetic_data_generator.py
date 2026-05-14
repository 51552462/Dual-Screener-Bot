"""
Project 2: Hyperbolic Time Chamber — 합성 일봉 OHLCV 생성기 (완전 격리).

- 기존 market_data.sqlite / system_config.json / 스크리너 모듈과 무관.
- 출력 전용 DB: synthetic_market.sqlite, 테이블: synthetic_ohlcv
"""
from __future__ import annotations

import os
import sqlite3

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 경로: 이 스크립트와 같은 디렉터리에만 DB 생성 (외부 설정 파일 미사용)
# ---------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SYNTHETIC_DB_PATH = os.path.join(_THIS_DIR, "synthetic_market.sqlite")

NUM_TRADING_DAYS = 1000
NUM_PARALLEL_UNIVERSES = 100
TICKER_PREFIX = "SYN_"
RNG = np.random.default_rng()


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
            PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_synth_ticker ON synthetic_ohlcv (ticker)"
    )
    conn.commit()


def simulate_merton_jump_diffusion_clipped_ohlcv(
    *,
    n_days: int,
    initial_price: float,
    seed: int | None = None,
) -> pd.DataFrame:
    """
    Merton-style 점프-확산 일간 수익률 → KRX ±30% 일일 변동폭 클리핑 → OHLCV.

    이산 근사 (일간 dt=1):
      로그수익률 r_t = μ + σ z_t + Σ_{k=1}^{N_t} Y_{t,k}
      N_t ~ Poisson(λ), Y ~ Normal(μ_J, σ_J²)
    단순수익률 s_t = exp(r_t) - 1 을 [-0.30, 0.30] 으로 클립 후 종가 누적.
    """
    rng = np.random.default_rng(seed)

    # 연율화 느낌의 보수적 일간 파라미터 (합성용; 튜닝 가능)
    mu = 0.00015
    sigma = 0.018
    lam = 0.035
    mu_jump = -0.02
    sigma_jump = 0.045

    z = rng.standard_normal(n_days)
    n_jumps = rng.poisson(lam, size=n_days)
    # 일별 점프 항: N=0 이면 0, N>0 이면 N개 점프 합(근사: 한 번에 합성 정규로 대체해 속도 유지)
    jump_component = np.zeros(n_days, dtype=np.float64)
    mask = n_jumps > 0
    if np.any(mask):
        # 각 일의 점프 합: sqrt(N)*Y 근사 (분산 스케일 유지)
        n_safe = np.maximum(n_jumps[mask], 1)
        y = rng.normal(mu_jump, sigma_jump, size=np.sum(mask))
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

    # Overnight gap (전일 종가 대비 시가)
    gap = rng.normal(0.0, 0.0045, size=n_days)
    open_px = prev_close * np.exp(gap)
    open_px[0] = close[0]

    c_target = close.copy()
    # 시가가 전일 종가 대비 과도한 괴리를 보이지 않도록 소프트 클립
    o_rel = np.clip(open_px / np.maximum(prev_close, 1e-9) - 1.0, -0.12, 0.12)
    open_px = prev_close * (1.0 + o_rel)
    open_px[0] = c_target[0]

    # Intraday high/low shadows
    upper_wick = rng.uniform(0.0008, 0.012, size=n_days)
    lower_wick = rng.uniform(0.0008, 0.012, size=n_days)
    body_high = np.maximum(open_px, c_target)
    body_low = np.minimum(open_px, c_target)
    high = body_high * (1.0 + upper_wick)
    low = body_low * (1.0 - lower_wick)
    high = np.maximum(high, np.maximum(open_px, c_target))
    low = np.minimum(low, np.minimum(open_px, c_target))
    close = c_target

    # Volume: 기본 + 점프/리밋 근접 시 서지 (일 t의 봉은 전일→당일 수익률 clipped_ret[t-1]과 동일 인덱스 점프)
    base = rng.lognormal(mean=14.0, sigma=0.45, size=n_days)
    eff_ret = np.zeros(n_days, dtype=np.float64)
    if n_days > 1:
        eff_ret[1:] = clipped_ret[:-1]
    limit_bar = (np.abs(eff_ret) >= 0.299).astype(np.float64)
    jump_bar = np.zeros(n_days, dtype=np.float64)
    if n_days > 1:
        jump_bar[1:] = (n_jumps[:-1] > 0).astype(np.float64)
    vol_mult = 1.0 + 2.8 * jump_bar + 1.9 * limit_bar + 12.0 * np.abs(eff_ret)
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
        }
    )


def write_universe_to_sqlite(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> None:
    m = df[["date", "open", "high", "low", "close", "volume"]].to_numpy()
    rows = [
        (ticker, str(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
        for r in m
    ]
    conn.executemany(
        """
        INSERT OR REPLACE INTO synthetic_ohlcv
        (ticker, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def generate_all_parallel_universes() -> None:
    os.makedirs(os.path.dirname(SYNTHETIC_DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(SYNTHETIC_DB_PATH, timeout=60.0)
    try:
        _init_db(conn)
        conn.execute("DELETE FROM synthetic_ohlcv")

        for k in range(1, NUM_PARALLEL_UNIVERSES + 1):
            ticker = f"{TICKER_PREFIX}{k:03d}"
            # 종목별 시드로 서로 다른 "평행 우주"
            seed = 10_000 + k * 9973
            init_px = float(RNG.uniform(8_000.0, 120_000.0))
            df = simulate_merton_jump_diffusion_clipped_ohlcv(
                n_days=NUM_TRADING_DAYS,
                initial_price=init_px,
                seed=seed,
            )
            write_universe_to_sqlite(conn, ticker, df)
            if k == 1 or k % 25 == 0:
                print(f"  ✓ {ticker}  ({NUM_TRADING_DAYS} rows)")

        conn.commit()
    finally:
        conn.close()

    print(f"\n완료: {NUM_PARALLEL_UNIVERSES} 티커 × {NUM_TRADING_DAYS} 일 → {SYNTHETIC_DB_PATH}")


if __name__ == "__main__":
    print("🌀 [Hyperbolic Time Chamber] 합성 시장 생성 시작…")
    generate_all_parallel_universes()
