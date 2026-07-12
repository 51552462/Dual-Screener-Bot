import json
import os
import random
import sqlite3
import time

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from bitget.config_hub import load_config, save_config
from bitget.infra.bounded_reads import forward_underdog_miner_closed_sql
from bitget.infra.clock import utc_date_key, utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.gc_cycle import flush_gc, heavy_data_cycle
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import GC_AFTER_GMM_FIT
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
logger = get_logger("bitget.underdog_miner")


def run_underdog_mining():
    logger.info("[underdog] mining low-score high-return DNA")
    try:
        conn = get_connection(DB_PATH, read_only=True)
        q, params = forward_underdog_miner_closed_sql()
        df = pd.read_sql(q, conn, params=params)
        conn.close()
    except Exception as e:
        log_exception(logger, "underdog DB load failed: %s", e)
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"])
    if len(df) < 10:
        logger.warning("underdog sample insufficient: %s (min 10)", len(df))
        return

    features = ["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"]
    x = df[features].values
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    n_clusters = max(1, min(3, len(df) // 5))
    with heavy_data_cycle(GC_AFTER_GMM_FIT):
        gmm = GaussianMixture(n_components=n_clusters, covariance_type="full", random_state=42)
        df["cluster"] = gmm.fit_predict(x_scaled)

        z90 = 1.645
        templates = {}
        for i in range(n_clusters):
            sub = df[df["cluster"] == i].copy()
            if sub.empty:
                continue
            mu_s = np.asarray(gmm.means_[i], dtype=float)
            cov_s = np.asarray(gmm.covariances_[i], dtype=float)
            diag_s = np.diag(cov_s) if cov_s.ndim == 2 else np.asarray(cov_s, dtype=float)
            diag_s = np.clip(np.nan_to_num(diag_s), 0.0, None)
            sigma_o = np.sqrt(diag_s) * np.asarray(scaler.scale_, dtype=float)
            mu_o = np.asarray(scaler.mean_, dtype=float) + (mu_s * np.asarray(scaler.scale_, dtype=float))

            box = {}
            for j, f in enumerate(features):
                lo = float(mu_o[j] - z90 * sigma_o[j])
                hi = float(mu_o[j] + z90 * sigma_o[j])
                if lo > hi:
                    lo, hi = hi, lo
                box[f"{f}_min"] = round(lo, 4)
                box[f"{f}_max"] = round(hi, 4)

            long_ratio = float((sub["position_side"].astype(str).str.upper() == "LONG").mean())
            mkt = "MIXED"
            mvals = sub["market_type"].astype(str).str.lower().unique().tolist()
            if len(mvals) == 1:
                mkt = mvals[0].upper()
            nature = "LONG_BIAS" if long_ratio >= 0.5 else "SHORT_BIAS"
            name = f"UD_CLUSTER_{i+1}_{mkt}_{nature}"
            templates[name] = {
                **box,
                "sample_size": int(len(sub)),
                "mean_ret": round(float(pd.to_numeric(sub["final_ret"], errors="coerce").mean()), 4),
                "long_ratio": round(long_ratio, 4),
                "updated_at": utc_datetime_str(),
            }

    cfg = load_config()
    old = cfg.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    if not isinstance(old, dict):
        old = {}
    tag = utc_date_key().replace("-", "")[2:]
    for k, v in templates.items():
        old[f"{k}_{tag}"] = v
    if len(old) > 12:
        for key in sorted(old.keys())[:-12]:
            old.pop(key, None)
    cfg["UNDERDOG_CLUSTER_TEMPLATES"] = old
    save_config(cfg)
    del df, x, x_scaled
    logger.info("underdog templates saved: %s", len(templates))


if __name__ == "__main__":
    run_underdog_mining()
