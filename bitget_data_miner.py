import json
import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from bitget_supernova_hunter import extract_dna_from_df


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")
CSV_PATH = os.path.join(BASE_DIR, "Supernova_Flow_Tracking_Master.csv")


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def build_supernova_csv():
    if not os.path.exists(DB_PATH):
        return 0
    conn = sqlite3.connect(DB_PATH, timeout=30)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    out = []
    for (tbl,) in rows:
        if not tbl.startswith("BITGET_"):
            continue
        parts = tbl.split("_")
        if len(parts) < 5:
            continue
        tf = parts[-1].upper()
        symbol = "_".join(parts[2:-1])
        df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC', conn)
        if len(df) < 240:
            continue
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")
        dna = extract_dna_from_df(df, tf)
        if dna is None:
            continue
        out.append(
            {
                "종목코드": symbol,
                "시장": parts[1],
                "랭크": f"MTF_{tf}",
                "[D_Day_당일] 평균_CPV": dna["cpv"],
                "[D_Day_당일] 평균_진짜양봉(TB)": dna["tb"],
                "[D_Day_당일] 평균_응축에너지(BBE)": dna["bbe"],
                "[D_Day_당일] 진모멘텀(TML)": dna["tml"],
                "[D_Day_당일] 평균_시장강도(RS)": dna["rs"],
            }
        )
    conn.close()
    if not out:
        return 0
    pd.DataFrame(out).to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    return len(out)


def run_cluster_mining():
    n = build_supernova_csv()
    if n == 0:
        print("No supernova samples to mine.")
        return
    df = pd.read_csv(CSV_PATH)
    target_features = [
        "[D_Day_당일] 평균_CPV",
        "[D_Day_당일] 평균_진짜양봉(TB)",
        "[D_Day_당일] 평균_응축에너지(BBE)",
        "[D_Day_당일] 진모멘텀(TML)",
        "[D_Day_당일] 평균_시장강도(RS)",
    ]
    clean_df = df.dropna(subset=target_features).copy()
    if len(clean_df) < 10:
        print("Insufficient clean samples for KMeans.")
        return

    # 코인 거래량/변동성 스케일 차이를 흡수하기 위한 필수 정규화.
    X = clean_df[target_features].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    clean_df["Cluster"] = kmeans.fit_predict(X_scaled)

    mined = {}
    for i in range(3):
        cdf = clean_df[clean_df["Cluster"] == i]
        if cdf.empty:
            continue
        mined[f"CLUSTER_{i+1}"] = {
            "cpv_min": round(cdf["[D_Day_당일] 평균_CPV"].quantile(0.10), 4),
            "cpv_max": round(cdf["[D_Day_당일] 평균_CPV"].quantile(0.90), 4),
            "tb_min": round(cdf["[D_Day_당일] 평균_진짜양봉(TB)"].quantile(0.10), 4),
            "tb_max": round(cdf["[D_Day_당일] 평균_진짜양봉(TB)"].quantile(0.90), 4),
            "bbe_min": round(cdf["[D_Day_당일] 평균_응축에너지(BBE)"].quantile(0.10), 4),
            "bbe_max": round(cdf["[D_Day_당일] 평균_응축에너지(BBE)"].quantile(0.90), 4),
            "tml_min": round(cdf["[D_Day_당일] 진모멘텀(TML)"].quantile(0.10), 4),
            "tml_max": round(cdf["[D_Day_당일] 진모멘텀(TML)"].quantile(0.90), 4),
            "rs_min": round(cdf["[D_Day_당일] 평균_시장강도(RS)"].quantile(0.10), 4),
            "rs_max": round(cdf["[D_Day_당일] 평균_시장강도(RS)"].quantile(0.90), 4),
            "sample_size": int(len(cdf)),
        }

    cfg = load_config()
    cfg["LIVE_CLUSTER_TEMPLATES"] = mined
    cfg["LIVE_CLUSTER_UPDATED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    save_config(cfg)
    print(f"KMeans mining complete: {len(mined)} clusters")


if __name__ == "__main__":
    run_cluster_mining()
