"""
Project 1: Black Hole Scanner — Anti-Pattern Short Engine (US only, 격리 숏 장부).

- `market_data.sqlite` / `forward_trades` 에는 **절대 쓰기 금지** (롱·숏 장부 분리).
- 숏 후보만 `short_data.sqlite` → `short_forward_trades` 에 기록.
- US 전용 ML 규칙: 동일 디렉터리 `us_toxic_ml_antipatterns.json` (KR `TOXIC_ML_ANTIPATTERNS` / `ANTI_PATTERNS` 미사용).
"""
from __future__ import annotations

import json
import logging
import os
import random
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf

from yf_download_flatten import flatten_yf_download_df

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
US_TOXIC_ML_JSON = os.path.join(_THIS_DIR, "us_toxic_ml_antipatterns.json")

logger = logging.getLogger(__name__)

from factory_data_paths import short_data_db_path, system_config_json_path

CONFIG_PATH = system_config_json_path()
SHORT_DB_PATH = short_data_db_path()

# 스캔 부하 상한 (yfinance Rate limit 방지)
DEFAULT_MAX_US_TICKERS = 120
YF_CHUNK = 8
OHLCV_MIN_BARS = 260
SPY_PERIOD = "730d"


def load_config(max_retries: int = 5) -> dict:
    if not os.path.exists(CONFIG_PATH):
        return {}
    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError):
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                return {}
    return {}


def save_config(config: dict, max_retries: int = 5) -> bool:
    temp_path = f"{CONFIG_PATH}.temp"
    cfg_dir = os.path.dirname(CONFIG_PATH)
    if cfg_dir:
        try:
            os.makedirs(cfg_dir, exist_ok=True)
        except OSError:
            pass
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False


def init_short_db() -> None:
    """숏 전용 DB·테이블 생성 (메인 DB와 완전 분리)."""
    d = os.path.dirname(SHORT_DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(SHORT_DB_PATH, timeout=45)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS short_forward_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_date TEXT NOT NULL,
                market TEXT NOT NULL DEFAULT 'US',
                code TEXT NOT NULL,
                name TEXT,
                trade_type TEXT NOT NULL DEFAULT 'SHORT',
                status TEXT NOT NULL DEFAULT 'OPEN',
                matched_pattern TEXT,
                dyn_cpv REAL,
                dyn_tb REAL,
                v_energy REAL,
                dyn_rs REAL,
                entry_price REAL,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_short_code_entrydate
            ON short_forward_trades(code, entry_date)
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_us_ticker_list() -> Optional[pd.DataFrame]:
    """`us_master.py` 와 동일한 US 유니버스 (NASDAQ/NYSE/AMEX). 실패 시 None (빈 DF 로 위장하지 않음)."""
    try:
        import FinanceDataReader as fdr

        df_nasdaq = fdr.StockListing("NASDAQ").assign(Market="NASDAQ")
        df_nyse = fdr.StockListing("NYSE").assign(Market="NYSE")
        df_amex = fdr.StockListing("AMEX").assign(Market="AMEX")
        df = pd.concat([df_nasdaq, df_nyse, df_amex])
        df = df[df["Symbol"].str.isalpha()]
        df["Symbol"] = df["Symbol"].str.replace(".", "-", regex=False)
        use_cols = ["Symbol", "Name", "Market"]
        for c in ("Sector", "Industry"):
            if c in df.columns:
                use_cols.append(c)
                break
        out = df[use_cols].drop_duplicates(subset=["Symbol"])
        out = out.dropna(subset=["Symbol"])
        if out.empty:
            logger.warning("blackhole_hunter: US listing query returned empty frame")
            return None
        return out
    except Exception as e:
        logger.exception("blackhole_hunter: US ticker listing (FinanceDataReader) failed: %s", e)
        return None


def _us_sector_bucket_for_tree(s: object) -> str:
    """
    US 리스팅 Sector/Industry 문자열 → ML·스캔 공통 버킷.
    `us_toxic_graveyard_analyzer._us_sector_bucket_for_tree` 와 동일 정의.
    """
    s_str = str(s).lower()
    if any(
        k in s_str
        for k in (
            "technology",
            "software",
            "semiconductor",
            "semi ",
            "it ",
            "internet",
            "computer",
            "tech",
            "saas",
            "cloud",
            "cyber",
        )
    ):
        return "US_Technology"
    if any(
        k in s_str
        for k in (
            "health",
            "biotech",
            "pharma",
            "medical",
            "drug",
            "life sci",
            "healthcare",
        )
    ):
        return "US_Healthcare"
    if any(
        k in s_str
        for k in (
            "financial",
            "bank",
            "insurance",
            "capital",
            "asset manag",
            "reit",
            "mortgage",
        )
    ):
        return "US_Financials"
    if any(
        k in s_str
        for k in (
            "energy",
            "oil",
            "gas",
            "petrol",
            "solar",
            "renewable",
            "coal",
        )
    ):
        return "US_Energy"
    if any(
        k in s_str
        for k in (
            "consumer",
            "retail",
            "restaurant",
            "apparel",
            "luxury",
            "food",
            "beverage",
            "household",
        )
    ):
        return "US_Consumer"
    if any(
        k in s_str
        for k in (
            "industrial",
            "machinery",
            "aerospace",
            "defense",
            "construction",
            "electrical",
            "transport",
        )
    ):
        return "US_Industrials"
    if any(
        k in s_str
        for k in (
            "communication",
            "telecom",
            "media",
            "entertainment",
        )
    ):
        return "US_Communication"
    if any(k in s_str for k in ("material", "chemical", "mining", "steel", "gold", "packaging")):
        return "US_Materials"
    if any(k in s_str for k in ("utility", "utilities", "electric", "water util")):
        return "US_Utilities"
    if any(k in s_str for k in ("real estate", "reit")):
        return "US_RealEstate"
    return "US_Other"


def get_dynamic_score(series_data: np.ndarray, higher_is_better: bool = True, window: int = 252) -> float:
    """us_master 와 동일 백분위 스코어 (1~10)."""
    if series_data is None or len(series_data) < 20:
        return 5.0
    s = pd.Series(series_data)
    pct_rank = s.rolling(window, min_periods=20).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    ).fillna(0.5).values[-1]
    if higher_is_better:
        return float(1.0 + (pct_rank * 9.0))
    return float(1.0 + ((1.0 - pct_rank) * 9.0))


_NUMERIC_BBOX_BASES = frozenset({"dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"})


def _fact_value_for_toxic_base(
    base: str, cpv: float, tb: float, bbe: float, dyn_rs_live: float
) -> float:
    if base == "dyn_cpv":
        return float(cpv)
    if base == "dyn_tb":
        return float(tb)
    if base == "v_energy":
        return float(bbe)
    if base == "dyn_rs":
        return float(dyn_rs_live)
    raise ValueError(base)


def evaluate_toxic_bbox_match(
    bounds: dict,
    cpv: float,
    tb: float,
    bbe: float,
    dyn_rs_live: float,
    sector_mapped: str,
    now_dt: Optional[datetime] = None,
) -> bool:
    """
    `auto_forward_tester.evaluate_toxic_bbox_match` 와 동일 규칙 (중복으로 의존성 순환 방지).
    """
    if not isinstance(bounds, dict):
        return False
    now = now_dt or datetime.now()
    tw = int(now.weekday())
    match_flags: List[bool] = []
    for key, raw in bounds.items():
        if key in ("created_at",):
            continue
        if key == "sector_match":
            match_flags.append(str(sector_mapped) == str(raw))
            continue
        if key == "weekday_match":
            try:
                wm = int(raw)
            except (TypeError, ValueError):
                match_flags.append(False)
                continue
            match_flags.append(tw == wm)
            continue
        ks = str(key)
        if ks.endswith("_max"):
            base = ks[:-4]
            if base not in _NUMERIC_BBOX_BASES:
                continue
            try:
                val = _fact_value_for_toxic_base(base, cpv, tb, bbe, dyn_rs_live)
            except ValueError:
                continue
            if base == "dyn_rs" and isinstance(val, float) and np.isnan(val):
                continue
            try:
                match_flags.append(float(val) <= float(raw))
            except (TypeError, ValueError):
                continue
            continue
        if ks.endswith("_min"):
            base = ks[:-4]
            if base not in _NUMERIC_BBOX_BASES:
                continue
            try:
                val = _fact_value_for_toxic_base(base, cpv, tb, bbe, dyn_rs_live)
            except ValueError:
                continue
            if base == "dyn_rs" and isinstance(val, float) and np.isnan(val):
                continue
            try:
                match_flags.append(float(val) > float(raw))
            except (TypeError, ValueError):
                continue
            continue
    return bool(match_flags) and all(match_flags)


def _load_us_toxic_ml_patterns() -> List[Tuple[str, dict]]:
    """KR 설정·TOXIC_ML_ANTIPATTERNS 완전 분리 — US 전용 JSON 만."""
    if not os.path.isfile(US_TOXIC_ML_JSON):
        return []
    try:
        with open(US_TOXIC_ML_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    patt = data.get("patterns") if isinstance(data, dict) else None
    if not isinstance(patt, dict):
        return []
    out: List[Tuple[str, dict]] = []
    for k, v in patt.items():
        if k == "_metadata" or not isinstance(v, dict):
            continue
        out.append((str(k), v))
    return out


def _load_spy_close() -> pd.Series:
    df = yf.download("SPY", interval="1d", period=SPY_PERIOD, progress=False, threads=False)
    df = flatten_yf_download_df(df)
    if df is None or df.empty or "Close" not in df.columns:
        return pd.Series(dtype=float)
    s = df["Close"].copy()
    s.index = pd.to_datetime(s.index).tz_localize(None)
    return s[~s.index.duplicated(keep="last")]


def _squeeze_guard_skip(df: pd.DataFrame) -> bool:
    """
    단기 압도적 랠리 + 과열 RSI → 숏 스퀴즈 리스크로 스킵.
    (역제안: 추가 필터는 여기서 확장)
    """
    if df is None or len(df) < 14:
        return False
    close = df["Close"].astype(float)
    ret5 = float(close.iloc[-1] / max(close.iloc[-6], 1e-12) - 1.0)
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_g = gain.rolling(14, min_periods=5).mean()
    avg_l = loss.rolling(14, min_periods=5).mean()
    rs = avg_g / avg_l.replace(0, np.nan)
    rsi = (100.0 - (100.0 / (1.0 + rs))).iloc[-1]
    if np.isnan(rsi):
        return False
    if ret5 >= 0.18 and float(rsi) >= 72.0:
        return True
    return False


def is_squeeze_risk(ticker_symbol: str) -> Optional[bool]:
    """
    3값 스퀴즈 가드: True=고위험, False=양 지표 모두 수치상 저위험, None=정보 없음·API 실패(블라인드).

    - 한쪽이라도 임계 초과면 True.
    - shortPercentOfFloat(또는 snake_case)와 shortRatio(또는 short_ratio)를 **둘 다**
      유효하게 읽고 각각 임계 이하일 때만 False.
    - 그 외(예외, 빈 info, 필드 누락, 파싱 불가, NaN/Inf)는 None — 숏 진입 금지(방어적 배제).
    """
    sym = str(ticker_symbol).strip()
    if not sym:
        return None
    try:
        info = yf.Ticker(sym).info
    except Exception:
        return None
    if not isinstance(info, dict) or not info:
        return None

    spf_raw = info.get("shortPercentOfFloat")
    if spf_raw is None:
        spf_raw = info.get("short_percent_of_float")

    sr_raw = info.get("shortRatio")
    if sr_raw is None:
        sr_raw = info.get("short_ratio")

    spf_ok = False
    spf_high = False
    if spf_raw is not None:
        try:
            spf = float(spf_raw)
            if not (np.isnan(spf) or np.isinf(spf)):
                if spf > 1.0:
                    spf /= 100.0
                spf_ok = True
                spf_high = spf > 0.15
        except (TypeError, ValueError):
            pass

    sr_ok = False
    sr_high = False
    if sr_raw is not None:
        try:
            sr = float(sr_raw)
            if not (np.isnan(sr) or np.isinf(sr)):
                sr_ok = True
                sr_high = sr > 5.0
        except (TypeError, ValueError):
            pass

    if spf_high or sr_high:
        return True
    if spf_ok and sr_ok and (not spf_high) and (not sr_high):
        return False
    return None


def compute_us_4d_dna_last(
    df: pd.DataFrame, spy_close: pd.Series
) -> Optional[Dict[str, float]]:
    """
    us_master `compute_top1_master_signal` 과 동일한 4D 원시값 → 장부용 스코어/원시 매핑.
    반환 키: dyn_cpv, dyn_tb, v_energy, dyn_rs (evaluate_toxic_bbox_match 인자와 동일).
    """
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    if len(df) < OHLCV_MIN_BARS:
        return None
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col not in df.columns:
            return None
    c = df["Close"].values.astype(float)
    o = df["Open"].values.astype(float)
    h = df["High"].values.astype(float)
    l = df["Low"].values.astype(float)
    v = df["Volume"].values.astype(float)

    idx = spy_close.reindex(df.index).ffill()
    if idx.isna().all():
        return None
    idx_v = idx.values.astype(float)

    with np.errstate(divide="ignore", invalid="ignore"):
        cpv = np.where(h != l, (c - o) / (h - l), 0.5)
        v_ma20 = pd.Series(v).rolling(20).mean().values
        vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
        tb_index = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
        bb_mid = pd.Series(c).rolling(20).mean().values
        bb_std = pd.Series(c).rolling(20).std().values
        bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
        bb_energy = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0.0)
        c_20 = pd.Series(c).shift(20).values
        idx_20 = pd.Series(idx_v).shift(20).values
        stock_ret = np.where(c_20 > 0, (c - c_20) / c_20, 0.0)
        idx_ret = np.where(idx_20 > 0, (idx_v - idx_20) / idx_20, 0.0001)
        idx_ret = np.where(idx_ret == 0, 0.0001, idx_ret)
        rs = (stock_ret / idx_ret) * 100.0
    rs = np.nan_to_num(rs, nan=0.0, posinf=0.0, neginf=0.0)
    cpv = np.nan_to_num(cpv, nan=0.5, posinf=1.0, neginf=0.0)
    tb_index = np.nan_to_num(tb_index, nan=1.0, posinf=1e6, neginf=0.0)
    bb_energy = np.nan_to_num(bb_energy, nan=0.0, posinf=1e6, neginf=0.0)
    dyn_cpv = get_dynamic_score(cpv, higher_is_better=False)
    dyn_tb = get_dynamic_score(tb_index, higher_is_better=True)
    dyn_rs = get_dynamic_score(rs, higher_is_better=True)
    v_energy = float(bb_energy[-1])
    return {
        "dyn_cpv": float(dyn_cpv),
        "dyn_tb": float(dyn_tb),
        "v_energy": v_energy,
        "dyn_rs": float(dyn_rs),
    }


def _extract_ticker_df(batch: pd.DataFrame, tk: str) -> Optional[pd.DataFrame]:
    if batch is None or batch.empty:
        return None
    try:
        if isinstance(batch.columns, pd.MultiIndex) and tk in batch.columns.get_level_values(0):
            sub = batch[tk].copy()
        elif isinstance(batch.columns, pd.MultiIndex):
            sub = batch.xs(tk, level=1, axis=1).copy()
        else:
            sub = batch.copy()
        sub = flatten_yf_download_df(sub)
        sub = sub[["Open", "High", "Low", "Close", "Volume"]].dropna()
        if sub.index.tz is not None:
            sub.index = sub.index.tz_convert("America/New_York").tz_localize(None)
        sub = sub[~sub.index.duplicated(keep="last")]
        return sub if len(sub) >= OHLCV_MIN_BARS else None
    except Exception:
        return None


def _insert_short_record(
    code: str,
    name: str,
    pattern: str,
    dna: Dict[str, float],
    price: float,
    entry_date: str,
) -> bool:
    init_short_db()
    conn = sqlite3.connect(SHORT_DB_PATH, timeout=45)
    try:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO short_forward_trades
            (entry_date, market, code, name, trade_type, status, matched_pattern,
             dyn_cpv, dyn_tb, v_energy, dyn_rs, entry_price, created_at)
            VALUES (?, 'US', ?, ?, 'SHORT', 'OPEN', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry_date,
                code,
                name,
                pattern,
                dna.get("dyn_cpv"),
                dna.get("dyn_tb"),
                dna.get("v_energy"),
                dna.get("dyn_rs"),
                price,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    except Exception:
        return False
    finally:
        conn.close()


def scan_blackhole_targets(max_us_tickers: int = DEFAULT_MAX_US_TICKERS) -> Dict[str, Any]:
    """
    US 티커 유니버스를 스캔해 독성 박스 일치 종목을 `short_data.sqlite` 에 기록하고
    `BLACKHOLE_TOXIC_COUNT` 를 갱신합니다. (스케줄러에서 호출)
    """
    print("🕳️ [Black Hole Scanner] US Anti-Pattern Short 타겟 스캔…")
    init_short_db()
    cfg = load_config()
    merged_rules = _load_us_toxic_ml_patterns()
    today = datetime.now().strftime("%Y-%m-%d")
    now_us = datetime.now(ZoneInfo("America/New_York"))

    if not merged_rules:
        print(f"💡 US ML 규칙 없음: {US_TOXIC_ML_JSON} (us_toxic_graveyard_analyzer.py 로 생성)")
        cfg["BLACKHOLE_TOXIC_COUNT"] = {
            "count": 0,
            "symbols": [],
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        save_config(cfg)
        return cfg["BLACKHOLE_TOXIC_COUNT"]

    spy = _load_spy_close()
    if spy.empty:
        print("🚨 SPY 벤치마크 로드 실패 — RS·DNA 계산 불가.")
        cfg["BLACKHOLE_TOXIC_COUNT"] = {
            "count": 0,
            "symbols": [],
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        save_config(cfg)
        return cfg["BLACKHOLE_TOXIC_COUNT"]

    listing = get_us_ticker_list()
    if listing is None or listing.empty:
        print("🚨 US 리스팅(FinanceDataReader) 실패 또는 빈 결과 — 스캔 중단.")
        cfg["BLACKHOLE_TOXIC_COUNT"] = {
            "count": 0,
            "symbols": [],
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        save_config(cfg)
        return cfg["BLACKHOLE_TOXIC_COUNT"]

    tickers = listing.head(int(max_us_tickers))["Symbol"].astype(str).tolist()
    tmap = {row["Symbol"]: str(row.get("Name") or row["Symbol"]) for _, row in listing.iterrows()}

    sym_to_sector: Dict[str, str] = {}
    for _, row in listing.iterrows():
        tk = str(row["Symbol"])
        raw = ""
        for col in ("Sector", "Industry"):
            if col in row.index and pd.notna(row.get(col)):
                raw = str(row[col])
                break
        sym_to_sector[tk] = _us_sector_bucket_for_tree(raw)

    # DB에 실제 삽입된 행만 집계(INSERT OR IGNORE 시 중복·무시는 rowcount==0 → 팬텀 히트 방지)
    hits_verified: List[str] = []

    for i in range(0, len(tickers), YF_CHUNK):
        chunk = tickers[i : i + YF_CHUNK]
        try:
            time.sleep(0.35)
            batch = yf.download(
                " ".join(chunk),
                interval="1d",
                period=SPY_PERIOD,
                group_by="ticker",
                progress=False,
                threads=False,
            )
        except Exception as e:
            print(f"⚠️ yfinance chunk 실패 ({chunk[:2]}…): {e}")
            continue

        for tk in chunk:
            df_t = _extract_ticker_df(batch, tk)
            if df_t is None:
                continue
            if _squeeze_guard_skip(df_t):
                continue
            dna = compute_us_4d_dna_last(df_t, spy)
            if not dna:
                continue
            cpv = float(dna["dyn_cpv"])
            tb = float(dna["dyn_tb"])
            bbe = float(dna["v_energy"])
            drs = float(dna["dyn_rs"])
            price = float(df_t["Close"].iloc[-1])

            sector_us = sym_to_sector.get(tk, "US_Other")
            matched: Optional[str] = None
            for rule_id, bounds in merged_rules:
                if evaluate_toxic_bbox_match(
                    bounds, cpv, tb, bbe, drs, sector_us, now_dt=now_us
                ):
                    matched = rule_id
                    break
            if not matched:
                continue

            squeeze = is_squeeze_risk(tk)
            if squeeze is True:
                print(f"⚠️ [스퀴즈 위험] {tk} 잔고 과다 배제")
                continue
            if squeeze is None:
                print(
                    f"⚠️ [API 블라인드] {tk} 데이터 수집 실패로 인한 방어적 배제 (Defensive Skip)"
                )
                continue

            nm = tmap.get(tk, tk)
            inserted = _insert_short_record(tk, nm, matched, dna, price, today)
            if inserted:
                if tk not in hits_verified:
                    hits_verified.append(tk)
                print(f"   💀 SHORT 타겟: {tk} ({nm}) ← {matched}")

    out = {
        "count": len(hits_verified),
        "symbols": hits_verified,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    cfg["BLACKHOLE_TOXIC_COUNT"] = out
    save_config(cfg)
    print(
        f"✅ Black Hole 스캔 완료: 오늘 DB 신규 반영 {len(hits_verified)}건 "
        f"(INSERT rowcount 검증) | short_data.sqlite"
    )
    return out


if __name__ == "__main__":
    scan_blackhole_targets()
