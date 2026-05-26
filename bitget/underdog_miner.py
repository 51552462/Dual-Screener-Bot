import json
import os
import random
import sqlite3
import time
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")


def load_config(max_retries=5):
    if not os.path.exists(CONFIG_PATH):
        return {}
    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError):
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
    return {}


def save_config(cfg, max_retries=5):
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            return False
    return False


def run_underdog_mining():
    print("🧟 [Bitget 언더독 마이닝] 60점 이하 고수익 코인 DNA 채굴 중...")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        q = """
            SELECT market_type, position_side, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret, total_score
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%'
              AND total_score <= 60
              AND final_ret >= 15.0
        """
        df = pd.read_sql(q, conn)
        conn.close()
    except Exception as e:
        print(f"🚨 DB 로드 실패: {e}")
        return

    df = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"])
    if len(df) < 10:
        print(f"⚠️ 표본 부족: {len(df)}개 (최소 10개 필요)")
        return

    features = ["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"]
    x = df[features].values
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    n_clusters = max(1, min(3, len(df) // 5))
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
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }

    cfg = load_config()
    old = cfg.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    if not isinstance(old, dict):
        old = {}
    tag = datetime.utcnow().strftime("%y%m%d")
    for k, v in templates.items():
        old[f"{k}_{tag}"] = v
    if len(old) > 12:
        for key in sorted(old.keys())[:-12]:
            old.pop(key, None)
    cfg["UNDERDOG_CLUSTER_TEMPLATES"] = old
    save_config(cfg)
    print(f"✅ 언더독 템플릿 저장 완료: {len(templates)}개")


if __name__ == "__main__":
    run_underdog_mining()
