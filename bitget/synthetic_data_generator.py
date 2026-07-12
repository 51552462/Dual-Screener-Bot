import json
import os
import random
import time

import numpy as np
import pandas as pd

from bitget.config_hub import load_config, save_config
from bitget.infra.clock import utc_datetime_str
from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.synthetic_data_generator")


def generate_synthetic_crypto_ohlcv(n_paths=1000, n_bars=720):
    """
    코인 전용 합성 캔들:
    - 기본 GBM
    - 점프 확산
    - 플래시 크래시 이벤트(단일 바 -30%~-55%) 주입
    """
    s0 = 100.0
    mu = 0.02 / 365.0
    sigma = 0.9 / np.sqrt(365.0)
    jump_prob = 0.02
    jump_mean = -0.02
    jump_std = 0.09

    z = np.random.standard_normal((n_paths, n_bars))
    jumps = np.random.poisson(jump_prob, (n_paths, n_bars)) * np.random.normal(jump_mean, jump_std, (n_paths, n_bars))
    rets = (mu - 0.5 * sigma ** 2) + sigma * z + jumps

    # 플래시 크래시 삽입
    crash_mask = np.random.uniform(0, 1, size=(n_paths, n_bars)) < 0.0025
    crash_magnitude = np.random.uniform(-0.55, -0.30, size=(n_paths, n_bars))
    rets = np.where(crash_mask, crash_magnitude, rets)

    log_cum = np.concatenate([np.zeros((n_paths, 1), dtype=np.float64), np.cumsum(rets[:, 1:], axis=1)], axis=1)
    close_p = s0 * np.exp(log_cum)

    open_p = np.roll(close_p, 1, axis=1)
    open_p[:, 0] = s0
    high_p = np.maximum(open_p, close_p) * np.random.uniform(1.0, 1.03, size=(n_paths, n_bars))
    low_p = np.minimum(open_p, close_p) * np.random.uniform(0.97, 1.0, size=(n_paths, n_bars))
    vol_p = (np.abs(rets) * 2_000_000.0) + np.random.randint(30_000, 120_000, size=(n_paths, n_bars))

    out = []
    for i in range(n_paths):
        out.append(
            pd.DataFrame(
                {
                    "Open": open_p[i],
                    "High": high_p[i],
                    "Low": low_p[i],
                    "Close": close_p[i],
                    "Volume": vol_p[i],
                }
            )
        )
    return out


def _estimate_survival(path_df: pd.DataFrame, side="LONG", leverage=3.0):
    close_s = path_df["Close"].astype(float).values
    if len(close_s) < 10:
        return False
    entry = float(close_s[0])
    side_u = str(side).upper()
    ret = ((close_s[-1] - entry) / entry) * 100.0 if side_u == "LONG" else ((entry - close_s[-1]) / entry) * 100.0
    roe = ret * float(leverage)
    return roe > -100.0


def stress_test_mutants():
    logger.info("[synthetic stress] generating synthetic crypto universe")
    n_paths = 1200
    syn = generate_synthetic_crypto_ohlcv(n_paths=n_paths, n_bars=720)

    long_survive = sum(_estimate_survival(df, "LONG", 3.0) for df in syn)
    short_survive = sum(_estimate_survival(df, "SHORT", 3.0) for df in syn)
    long_rate = (long_survive / n_paths) * 100.0
    short_rate = (short_survive / n_paths) * 100.0

    cfg = load_config()
    cfg["BITGET_SYNTHETIC_STRESS"] = {
        "updated_at": utc_datetime_str(),
        "paths": int(n_paths),
        "long_survival_rate": round(float(long_rate), 3),
        "short_survival_rate": round(float(short_rate), 3),
        "flash_crash_enabled": True,
    }
    save_config(cfg)
    logger.info(
        "synthetic stress complete: LONG survival %.2f%% | SHORT survival %.2f%%",
        long_rate,
        short_rate,
    )


if __name__ == "__main__":
    stress_test_mutants()
