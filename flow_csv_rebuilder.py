"""
market_data.sqlite OHLCV → Supernova_Flow_Tracking_Master.csv (파생 자산 재생성).

DB가 SSOT; CSV는 data_miner / 감사관용 파생물이다.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

FLOW_CSV_COLUMNS = [
    "종목코드",
    "시장",
    "랭크",
    "[D_Day_당일] 평균_CPV",
    "[D_Day_당일] 평균_진짜양봉(TB)",
    "[D_Day_당일] 평균_응축에너지(BBE)",
    "[D_Day_당일] 진모멘텀(TML)",
    "[D_Day_당일] 평균_시장강도(RS)",
]


def _max_tables_per_market() -> int:
    raw = (os.environ.get("FACTORY_CSV_REBUILD_MAX_TABLES") or "400").strip()
    try:
        return max(50, int(raw))
    except ValueError:
        return 400


def _extract_dna_last_bar(hist_df: pd.DataFrame) -> Optional[Dict[str, float]]:
    """최근 봉 기준 CPV/TB/BBE/TML/RS — bitget_supernova_hunter 와 동일 정의."""
    if hist_df is None or len(hist_df) < 130:
        return None
    need = {"Open", "High", "Low", "Close", "Volume"}
    if not need.issubset(hist_df.columns):
        return None
    hist_df = hist_df.tail(200).copy()
    c = hist_df["Close"].values
    o = hist_df["Open"].values
    h = hist_df["High"].values
    l = hist_df["Low"].values
    v = hist_df["Volume"].values
    for n in (10, 20, 30, 60, 112, 224):
        hist_df[f"EMA{n}"] = hist_df["Close"].ewm(span=n, adjust=False, min_periods=0).mean()
    is_aligned_30 = (hist_df["EMA10"] > hist_df["EMA20"]) & (hist_df["EMA20"] > hist_df["EMA30"])
    with np.errstate(divide="ignore", invalid="ignore"):
        v_ma20 = pd.Series(v).rolling(20).mean().values
        cpv = np.where(h != l, (c - o) / (h - l), 0.5)
        vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
        tb = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
        bb_std = pd.Series(c).rolling(20).std().values
        bb_mid = pd.Series(c).rolling(20).mean().values
        bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
        bbe = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
    idx_arr = np.arange(len(hist_df))
    r_val = hist_df["EMA10"].rolling(10).corr(pd.Series(idx_arr, index=hist_df.index)).fillna(0)
    r_squared = r_val * r_val
    ema10_3 = hist_df["EMA10"].shift(3).fillna(hist_df["EMA10"])
    ema_roc = np.where(ema10_3 != 0, ((hist_df["EMA10"] - ema10_3) / ema10_3) * 5000, 0)
    tml = np.where(is_aligned_30, ema_roc * (r_squared**2), 0)
    dday_idx = int(np.nanargmax(bbe)) if not np.isnan(bbe).all() else len(hist_df) - 1
    rs = float(((c[-1] - c[max(0, len(c) - 20)]) / max(c[max(0, len(c) - 20)], 1e-9)) * 100.0)
    return {
        "cpv": float(cpv[dday_idx]),
        "tb": float(tb[dday_idx]),
        "bbe": float(bbe[dday_idx]),
        "tml": float(tml[dday_idx]),
        "rs": rs,
    }


def _parse_kr_us_table(name: str) -> Optional[Tuple[str, str]]:
    if "__tmp" in name or name in ("forward_trades", "sqlite_sequence"):
        return None
    parts = name.split("_", 1)
    if len(parts) != 2:
        return None
    mkt, code = parts[0].upper(), parts[1]
    if mkt not in ("KR", "US") or not code:
        return None
    return mkt, code


def _codes_from_forward_trades(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    try:
        cur = conn.execute(
            "SELECT DISTINCT market, code FROM forward_trades "
            "WHERE market IS NOT NULL AND code IS NOT NULL"
        )
        out: List[Tuple[str, str]] = []
        for mkt, code in cur.fetchall():
            m = str(mkt or "").strip().upper()
            c = str(code or "").strip()
            if m in ("KR", "US") and c:
                out.append((m, c))
        return out
    except sqlite3.Error:
        return []


def _ohlcv_table_name(market: str, code: str) -> str:
    m = str(market).upper().strip()
    c = str(code).strip()
    if m == "KR":
        return f"KR_{c.zfill(6)}"
    return f"US_{c.replace('.', '-')}"


def _row_from_table(
    conn: sqlite3.Connection,
    market: str,
    code: str,
    rank_label: str = "REBUILD_DB",
) -> Optional[Dict[str, Any]]:
    tbl = _ohlcv_table_name(market, code)
    try:
        df = pd.read_sql(
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC',
            conn,
        )
    except Exception:
        return None
    if df is None or len(df) < 130:
        return None
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date")
    dna = _extract_dna_last_bar(df)
    if dna is None:
        return None
    display_code = code.zfill(6) if market == "KR" else code
    return {
        "종목코드": display_code,
        "시장": market,
        "랭크": rank_label,
        "[D_Day_당일] 평균_CPV": round(dna["cpv"], 4),
        "[D_Day_당일] 평균_진짜양봉(TB)": round(dna["tb"], 4),
        "[D_Day_당일] 평균_응축에너지(BBE)": round(dna["bbe"], 4),
        "[D_Day_당일] 진모멘텀(TML)": round(dna["tml"], 4),
        "[D_Day_당일] 평균_시장강도(RS)": round(dna["rs"], 4),
    }


def rebuild_flow_csv_from_sqlite(
    db_path: str,
    csv_path: str,
    *,
    min_rows: int = 1,
) -> int:
    """
    market_data.sqlite 에서 KR/US OHLCV 표본을 읽어 마스터 CSV를 재작성한다.
    Returns: written row count (0 = failure or insufficient data).
    """
    if not db_path or not os.path.isfile(db_path):
        logger.warning("flow_csv_rebuilder: DB missing %s", db_path)
        return 0

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        pairs: List[Tuple[str, str]] = _codes_from_forward_trades(conn)
        seen = {f"{m}:{c}" for m, c in pairs}

        if len(pairs) < min_rows:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%__tmp%'"
            ).fetchall()
            cap = _max_tables_per_market()
            kr_n = us_n = 0
            for (tbl,) in tables:
                parsed = _parse_kr_us_table(tbl)
                if not parsed:
                    continue
                mkt, code = parsed
                key = f"{mkt}:{code}"
                if key in seen:
                    continue
                if mkt == "KR" and kr_n >= cap:
                    continue
                if mkt == "US" and us_n >= cap:
                    continue
                seen.add(key)
                pairs.append((mkt, code))
                if mkt == "KR":
                    kr_n += 1
                else:
                    us_n += 1

        out: List[Dict[str, Any]] = []
        for mkt, code in pairs:
            row = _row_from_table(conn, mkt, code)
            if row:
                out.append(row)

        if len(out) < min_rows:
            logger.warning(
                "flow_csv_rebuilder: insufficient rows (%s) from %s",
                len(out),
                db_path,
            )
            return 0

        os.makedirs(os.path.dirname(os.path.abspath(csv_path)) or ".", exist_ok=True)
        pd.DataFrame(out, columns=FLOW_CSV_COLUMNS).to_csv(
            csv_path, index=False, encoding="utf-8-sig"
        )
        logger.info("flow_csv_rebuilder: wrote %s rows → %s", len(out), csv_path)
        return len(out)
    finally:
        conn.close()
