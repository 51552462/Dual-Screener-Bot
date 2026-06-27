"""
상한가·급등 종목 과거 ~30거래일 역추적 → 선취매 DNA → system_config 의 상한가 코호트 블록
(영속 키: LIMIT_UP_COHORT_DNA_CONFIG_KEY, 레거시 문자열 `LIMIT_UP_DNA`).
독립 위성: main / supernova_hunter 비수정.
"""
from __future__ import annotations

import json
import logging
import math
import os
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

try:
    import yfinance as yf
except ImportError:
    yf = None

from config_manager import CONFIG_PATH
from dna_schema_constants import LIMIT_UP_COHORT_DNA_CONFIG_KEY

logger = logging.getLogger(__name__)

LIMIT_UP_THRESHOLD_PCT_KR = 29.5
US_EXPLOSION_RET_PCT = 7.0
MIN_HISTORY_ROWS = 15
MAX_TARGETS_KR = 40
US_TOP_N = 10
US_UNIVERSE_SCAN = 450
BASELINE_DAYS = 20

PATTERN_KEYS = [
    "vol_compression",
    "ma_convergence",
    "pre_squeeze_narrow_range",
    "volume_dry_then_lift",
    "close_near_ma20",
    "higher_lows_tail",
    "rsi_mid_window_calm",
    "pressed_under_prior_high",
    "positive_momentum_build",
    "volatility_contraction_vs_baseline",
]


def load_config(max_retries: int = 5) -> Dict[str, Any]:
    """[장갑차] JSON 읽기 재시도."""
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


def save_config(config_data: Dict[str, Any], max_retries: int = 5) -> bool:
    """[장갑차] 임시 파일 원자적(Atomic) 저장."""
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
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


def _sleep_stealth() -> None:
    time.sleep(random.uniform(0.3, 0.7))


def _strip_column_names(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        str(c).strip() if isinstance(c, str) else c for c in out.columns
    ]
    return out


def _col_ci(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """컬럼명 대소문자 무시·공백 제거 후 첫 일치 실제 컬럼명."""
    low: Dict[str, str] = {}
    for c in df.columns:
        key = str(c).strip().lower()
        if key not in low:
            low[key] = str(c) if isinstance(c, str) else str(c)
    for cand in candidates:
        k = str(cand).strip().lower()
        if k in low:
            return low[k]
    return None


# 외부 API 스키마 변형 흡수 (단일 정규화 게이트)
_CHANGE_RATE_ALIASES: Tuple[str, ...] = (
    "ChgRate",
    "등락률",
    "등락율",
    "Change",
    "ChangesRatio",
    "ChagesRatio",
    "Rate",
    "등락",
    "Fluctuation",
    "DRate",
    "상승률",
    "Per",
    "pct_chg",
    "PctChg",
    "Change %",
    "ChangePercent",
    "Rtn",
    "CR",
)


def _ensure_change_rate(krx: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    KRX 리스팅 등에 대해 ChangeRate(%) 단일 컬럼을 보장한다 (정규화 게이트).
    - API가 제공하는 등락률·Change 계열 별칭을 유연히 매핑하고,
    - 없으면 동일 행의 (당일가 - 전일가) / 전일가 * 100 으로 자가 치유,
    - 시계열 단일 종목(리스팅이 아닌 OHLCV 뷰)에 한해서만 종가 shift 기반 파생.
    반환: (df, 오류문자열|None). 실패 시 사유 문자열.
    """
    if krx is None:
        return pd.DataFrame(), "입력 데이터 없음"
    df = _strip_column_names(krx)
    if df.empty:
        return df, "데이터 비어 있음"

    def _has_usable_rate(series: pd.Series) -> bool:
        s = pd.to_numeric(series, errors="coerce")
        return bool(s.notna().any())

    # 1) 이미 ChangeRate
    crn = _col_ci(df, ("ChangeRate",))
    if crn:
        df["ChangeRate"] = pd.to_numeric(df[crn], errors="coerce")
        if _has_usable_rate(df["ChangeRate"]):
            return df, None

    # 2) 알려진 별칭
    for alt in _CHANGE_RATE_ALIASES:
        cn = _col_ci(df, (alt,))
        if not cn:
            continue
        try:
            df["ChangeRate"] = pd.to_numeric(df[cn], errors="coerce")
            if _has_usable_rate(df["ChangeRate"]):
                return df, None
        except Exception:
            continue

    # 3) 동일 행: (당일 종가 - 전일 종가) / 전일 종가 * 100
    today_prev_specs: Tuple[Tuple[Tuple[str, ...], Tuple[str, ...]], ...] = (
        (
            ("종가", "Close", "현재가", "Price", "CurPrice", "현재가(원)"),
            ("전일종가", "PrevClose", "기준가", "전일가", "Previous Close", "Prev", "전일종가(원)"),
        ),
    )
    for today_cands, prev_cands in today_prev_specs:
        tc = _col_ci(df, today_cands)
        pc = _col_ci(df, prev_cands)
        if not tc or not pc:
            continue
        try:
            close = pd.to_numeric(df[tc], errors="coerce")
            prev = pd.to_numeric(df[pc], errors="coerce")
            denom = prev.replace(0, float("nan"))
            df["ChangeRate"] = (close - prev) / denom * 100.0
            if _has_usable_rate(df["ChangeRate"]):
                return df, None
        except Exception:
            continue

    # 4) 시계열 단독(심볼 컬럼 없음): 종가 shift — 교차단면 리스팅에는 적용하지 않음
    has_symbol = bool(
        _col_ci(df, ("Code",)) or _col_ci(df, ("Symbol",)) or _col_ci(df, ("ISU_CD",))
    )
    is_cross_section = has_symbol and len(df) > 5

    if not is_cross_section:
        close_col = _col_ci(
            df, ("Close", "close", "Adj Close", "종가", "Price", "현재가")
        )
        if close_col is None:
            return df, "종가·등락률 컬럼을 정규화 게이트에서 찾지 못함"
        try:
            close = pd.to_numeric(df[close_col], errors="coerce")
            prev = close.shift(1)
            denom = prev.replace(0, float("nan"))
            df["ChangeRate"] = (close - prev) / denom * 100.0
        except Exception as exc:
            return df, f"등락률 산출 실패: {exc}"
        if _has_usable_rate(df["ChangeRate"]):
            return df, None

    return df, "등락률·전일종가 쌍을 리스팅에서 찾지 못함 (API 스키마 변경 가능성)"


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100.0 - (100.0 / (1.0 + rs))


def _normalize_code(code: Any) -> str:
    s = str(code).strip()
    if s.isdigit():
        return s.zfill(6)[-6:]
    return s


def _pct_ret_row(ohlc: pd.DataFrame, i: int) -> float:
    if i <= 0:
        return 0.0
    prev = float(ohlc["Close"].iloc[i - 1])
    if prev <= 0:
        return 0.0
    return (float(ohlc["Close"].iloc[i]) / prev - 1.0) * 100.0


def compute_dna_window_flags(
    ohlc: pd.DataFrame, T_idx: int
) -> Optional[Dict[str, bool]]:
    """
    급등일 인덱스 T_idx 기준 T-10 ~ T-2 구간 DNA 플래그 (포렌식·파이오니어 공용).
    """
    if len(ohlc) < MIN_HISTORY_ROWS or T_idx < 11:
        return None

    win = ohlc.iloc[T_idx - 10 : T_idx - 1].copy()
    if len(win) < 9:
        return None

    baseline_start = max(0, T_idx - 10 - BASELINE_DAYS)
    baseline = ohlc.iloc[baseline_start : T_idx - 10]
    if len(baseline) < 5:
        baseline = ohlc.iloc[: max(1, T_idx - 10)]

    close = ohlc["Close"].astype(float)
    vol = ohlc["Volume"].astype(float).replace(0, float("nan"))
    low = ohlc["Low"].astype(float)

    ma5 = close.rolling(5, min_periods=3).mean()
    ma20 = close.rolling(20, min_periods=5).mean()

    med_win_vol = float(win["Volume"].median())
    med_base_vol = float(baseline["Volume"].median()) if len(baseline) else med_win_vol
    if math.isnan(med_base_vol) or med_base_vol <= 0:
        med_base_vol = med_win_vol or 1.0
    vol_compression = med_win_vol < 0.65 * med_base_vol

    i_tm2 = T_idx - 2
    m5 = float(ma5.iloc[i_tm2]) if not math.isnan(ma5.iloc[i_tm2]) else float(close.iloc[i_tm2])
    m20v = float(ma20.iloc[i_tm2]) if not math.isnan(ma20.iloc[i_tm2]) else float(close.iloc[i_tm2])
    ma_convergence = abs(m5 / m20v - 1.0) < 0.025 if m20v > 0 else False

    wc = win["Close"].replace(0, float("nan"))
    rng = (win["High"] - win["Low"]) / wc
    narrow_frac = float((rng < 0.025).mean())
    pre_squeeze_narrow_range = narrow_frac >= 0.35

    v_tm3 = float(vol.iloc[T_idx - 3]) if T_idx >= 3 else med_win_vol
    v_tm2 = float(vol.iloc[T_idx - 2]) if T_idx >= 2 else med_win_vol
    med_w = float(win["Volume"].median())
    volume_dry_then_lift = (v_tm3 < med_w * 1.05) and (v_tm2 > v_tm3 * 1.15)

    c_tm2 = float(close.iloc[T_idx - 2])
    ma20_tm2 = float(ma20.iloc[T_idx - 2]) if not math.isnan(ma20.iloc[T_idx - 2]) else c_tm2
    close_near_ma20 = abs(c_tm2 / ma20_tm2 - 1.0) < 0.04 if ma20_tm2 > 0 else False

    try:
        l2 = float(low.iloc[T_idx - 2])
        l4 = float(low.iloc[T_idx - 4])
        l6 = float(low.iloc[T_idx - 6])
        higher_lows_tail = (l2 > l4) and (l4 > l6)
    except Exception:
        higher_lows_tail = False

    win_first = win.iloc[: max(3, len(win) // 2)]
    rsi_mid = float(_rsi(win_first["Close"], 14).mean())
    rsi_mid_window_calm = not math.isnan(rsi_mid) and rsi_mid < 58.0

    prior = ohlc.iloc[max(0, T_idx - 35) : T_idx - 10]
    ph = float(prior["Close"].max()) if len(prior) else c_tm2
    px = float(close.iloc[T_idx - 8]) if T_idx >= 8 else c_tm2
    pressed_under_prior_high = ph > 0 and (px / ph) < 0.94

    positive_momentum_build = float(close.iloc[T_idx - 2]) > float(close.iloc[T_idx - 5])

    ret_win = float(win["Close"].pct_change().std())
    ret_base = (
        float(baseline["Close"].pct_change().std()) if len(baseline) > 2 else ret_win
    )
    volatility_contraction_vs_baseline = (
        ret_win < ret_base * 0.85 if ret_base and ret_base > 0 else False
    )

    return {
        "vol_compression": bool(vol_compression),
        "ma_convergence": bool(ma_convergence),
        "pre_squeeze_narrow_range": bool(pre_squeeze_narrow_range),
        "volume_dry_then_lift": bool(volume_dry_then_lift),
        "close_near_ma20": bool(close_near_ma20),
        "higher_lows_tail": bool(higher_lows_tail),
        "rsi_mid_window_calm": bool(rsi_mid_window_calm),
        "pressed_under_prior_high": bool(pressed_under_prior_high),
        "positive_momentum_build": bool(positive_momentum_build),
        "volatility_contraction_vs_baseline": bool(volatility_contraction_vs_baseline),
    }


def extract_pattern_flags(
    ohlc: pd.DataFrame,
    region: str = "KR",
    limit_threshold_pct: float = LIMIT_UP_THRESHOLD_PCT_KR,
    alt_explosion_pct: float = US_EXPLOSION_RET_PCT,
) -> Optional[Dict[str, bool]]:
    """급등일 탐지 후 DNA 윈도우 플래그."""
    if len(ohlc) < MIN_HISTORY_ROWS:
        return None
    T_idx = len(ohlc) - 1
    if T_idx < 11:
        return None

    ret_T = _pct_ret_row(ohlc, T_idx)
    thr_main = limit_threshold_pct - 0.5
    thr_alt = alt_explosion_pct

    if region.upper() == "US":
        if ret_T < thr_alt:
            for i in range(len(ohlc) - 1, max(10, len(ohlc) - 40), -1):
                if _pct_ret_row(ohlc, i) >= thr_alt:
                    T_idx = i
                    break
            else:
                if ret_T < thr_alt * 0.7:
                    return None
    else:
        if ret_T < 28.0:
            for i in range(len(ohlc) - 1, max(10, len(ohlc) - 30), -1):
                if _pct_ret_row(ohlc, i) >= thr_main:
                    T_idx = i
                    break
            else:
                if ret_T < 25.0:
                    return None

    if T_idx < 11:
        return None
    return compute_dna_window_flags(ohlc, T_idx)


def _fetch_listing_krx() -> Tuple[pd.DataFrame, Optional[str]]:
    _sleep_stealth()
    if fdr is None:
        return pd.DataFrame(), "FinanceDataReader 미설치"
    try:
        krx = fdr.StockListing("KRX")
    except Exception as exc:
        return pd.DataFrame(), f"KRX 리스트 수신 실패: {exc}"
    return _ensure_change_rate(krx)


def _fetch_ohlcv_kr(code: str) -> Optional[pd.DataFrame]:
    _sleep_stealth()
    if fdr is None:
        return None
    c = _normalize_code(code)
    start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    try:
        df = fdr.DataReader(c, start)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col not in df.columns:
            return None
    return df.sort_index().reset_index(drop=True)


def _fetch_ohlcv_us(symbol: str) -> Optional[pd.DataFrame]:
    _sleep_stealth()
    if yf is None:
        return None
    sym = str(symbol).strip().replace(".", "-")
    try:
        t = yf.Ticker(sym)
        df = t.history(period="3mo", auto_adjust=True)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    df = df.rename(columns=str.strip)
    need = {"Open", "High", "Low", "Close", "Volume"}
    if not need.issubset(set(df.columns)):
        return None
    return df.reset_index(drop=True)


def _us_listing_merge() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for ex in ("NASDAQ", "NYSE", "AMEX"):
        _sleep_stealth()
        try:
            if fdr is None:
                continue
            frames.append(fdr.StockListing(ex))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _pick_marcap_series(df: pd.DataFrame) -> pd.Series:
    for col in ("Marcap", "MarketCap", "MarCap", "marcap"):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if {"Close", "Volume"}.issubset(df.columns):
        return (
            pd.to_numeric(df["Close"], errors="coerce").fillna(0)
            * pd.to_numeric(df["Volume"], errors="coerce").fillna(0)
        )
    return pd.Series(0.0, index=df.index)


def _rank_us_top_symbols(top_n: int = US_TOP_N, universe: int = US_UNIVERSE_SCAN) -> List[str]:
    """유니버스 내 일간 수익률 상위 티커 (yfinance)."""
    if yf is None:
        return []
    base = _us_listing_merge()
    if base.empty or "Symbol" not in base.columns:
        return []

    base = base.copy()
    base["_mc"] = _pick_marcap_series(base)
    base["Symbol"] = base["Symbol"].astype(str).str.replace(".", "-", regex=False)
    base = base.sort_values("_mc", ascending=False).head(universe)

    gains: List[Tuple[str, float]] = []
    for sym in base["Symbol"].tolist():
        s = sym.strip()
        if not s or len(s) > 12:
            continue
        _sleep_stealth()
        try:
            h = yf.Ticker(s).history(period="5d", auto_adjust=True)
            if h is None or len(h) < 2:
                continue
            c = h["Close"].astype(float)
            pct = (float(c.iloc[-1]) / float(c.iloc[-2]) - 1.0) * 100.0
            if math.isfinite(pct):
                gains.append((s, pct))
        except Exception:
            continue

    gains.sort(key=lambda x: x[1], reverse=True)
    out: List[str] = []
    seen = set()
    for s, _ in gains:
        if s not in seen:
            seen.add(s)
            out.append(s)
        if len(out) >= top_n:
            break
    return out


def _cohort_to_payload(
    pattern_rows: List[Dict[str, bool]],
    codes_used: List[str],
    raw_target_n: int,
    region: str,
) -> Optional[Dict[str, Any]]:
    n = len(pattern_rows)
    if n == 0:
        return None

    threshold_votes = max(1, math.ceil(0.7 * n))
    hit_counts: Dict[str, int] = {k: 0 for k in PATTERN_KEYS}
    for pr in pattern_rows:
        for k in PATTERN_KEYS:
            if pr.get(k):
                hit_counts[k] += 1

    consensus_flags = {k: hit_counts[k] >= threshold_votes for k in PATTERN_KEYS}
    consensus_hits = sum(1 for k in PATTERN_KEYS if consensus_flags[k])

    return {
        "region": region,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "target_count": int(raw_target_n),
        "samples_analyzed": n,
        "codes_analyzed": codes_used,
        "vote_threshold_count": threshold_votes,
        "vote_threshold_ratio": 0.7,
        "pattern_hit_counts": hit_counts,
        "consensus_pattern_hits": consensus_hits,
        "consensus_met": consensus_hits >= 7,
        "pre_emptive_rule": {k: consensus_flags[k] for k in PATTERN_KEYS},
    }


def run_kr_limit_up_dna() -> Tuple[Optional[Dict[str, Any]], str]:
    """한국장 상한가 DNA 부검만 실행. (payload | None, 로그)."""
    pattern_rows: List[Dict[str, bool]] = []
    codes_used: List[str] = []
    raw_n = 0

    if fdr is None:
        return None, "KR: FinanceDataReader 없음"
    krx, rate_err = _fetch_listing_krx()
    if rate_err:
        return None, f"KR: {rate_err}"
    if krx.empty:
        return None, "KR: 데이터 비어 있음"

    cr = pd.to_numeric(krx["ChangeRate"], errors="coerce")
    if not cr.notna().any():
        return None, "KR: ChangeRate 정규화 후 유효 수치 없음"

    limit_ups = krx[cr >= LIMIT_UP_THRESHOLD_PCT_KR].copy()
    if limit_ups.empty:
        return None, "KR: 상한가 종목 없음"
    raw_n = len(limit_ups)
    if len(limit_ups) > MAX_TARGETS_KR:
        limit_ups = limit_ups.nlargest(MAX_TARGETS_KR, "ChangeRate", keep="first")

    for _, row in limit_ups.iterrows():
        code = row.get("Code", row.get("Symbol", ""))
        code_s = _normalize_code(code)
        if not code_s:
            continue
        ohlc = _fetch_ohlcv_kr(code_s)
        if ohlc is None or len(ohlc) < MIN_HISTORY_ROWS:
            continue
        flags = extract_pattern_flags(ohlc, region="KR")
        if flags:
            pattern_rows.append(flags)
            codes_used.append(code_s)

    payload = _cohort_to_payload(pattern_rows, codes_used, raw_n, "KR")
    if payload is None:
        return None, "KR: 유효 샘플 0"
    if not payload["consensus_met"]:
        return None, f"KR: 합의 패턴 {payload['consensus_pattern_hits']}/10 (<7), 저장 생략"
    return payload, "KR: OK"


def run_us_limit_up_dna() -> Tuple[Optional[Dict[str, Any]], str]:
    """미국장 급등 DNA 부검만 실행. (payload | None, 로그)."""
    pattern_rows: List[Dict[str, bool]] = []
    codes_used: List[str] = []
    raw_n = 0

    symbols = _rank_us_top_symbols(US_TOP_N, US_UNIVERSE_SCAN)
    raw_n = len(symbols)
    if not symbols:
        return None, "US: 상위 티커 추출 실패"

    for sym in symbols:
        ohlc = _fetch_ohlcv_us(sym)
        if ohlc is None or len(ohlc) < MIN_HISTORY_ROWS:
            continue
        flags = extract_pattern_flags(ohlc, region="US")
        if flags:
            pattern_rows.append(flags)
            codes_used.append(sym)

    payload = _cohort_to_payload(pattern_rows, codes_used, raw_n, "US")
    if payload is None:
        return None, "US: 유효 샘플 0"
    if not payload["consensus_met"]:
        return None, f"US: 합의 패턴 {payload['consensus_pattern_hits']}/10 (<7), 저장 생략"
    return payload, "US: OK"


def _run_region_pipeline(region: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """하위 호환: 단일 지역 부검."""
    r = str(region).upper()
    if r == "KR":
        return run_kr_limit_up_dna()
    if r == "US":
        return run_us_limit_up_dna()
    return None, f"Unknown region: {region}"


def _merge_dna_section(
    cfg: Dict[str, Any],
    region_key: str,
    new_payload: Optional[Dict[str, Any]],
) -> None:
    lump = cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY)
    if not isinstance(lump, dict):
        lump = {}
    if new_payload is not None:
        lump[region_key] = new_payload
    cfg[LIMIT_UP_COHORT_DNA_CONFIG_KEY] = lump


def _send_global_dna_report(
    kr_part: Optional[Dict[str, Any]],
    us_part: Optional[Dict[str, Any]],
    kr_msg: str,
    us_msg: str,
    *,
    include_kr: bool = True,
    include_us: bool = True,
) -> None:
    send_telegram_msg = None
    try:
        from auto_forward_tester import send_telegram_msg as _stm

        send_telegram_msg = _stm
    except Exception as e:
        logger.error("limit_up_forensics: cannot import send_telegram_msg — DNA 리포트 미발송: %s", e)
        print(f"⚠️ [limit_up_forensics] 텔레그램 모듈 import 실패, 글로벌 DNA 리포트 생략: {e}")
        return

    lines = [
        "<b>[🔬 글로벌 상한가 DNA 분석 리포트]</b>",
        f"<i>{datetime.now().strftime('%Y-%m-%d %H:%M')}</i>",
        "",
    ]
    if include_kr:
        lines.append(f"▪️ <b>KR</b>: {kr_msg}")
    if include_us:
        lines.append(f"▪️ <b>US</b>: {us_msg}")
    if include_kr or include_us:
        lines.append("")

    for tag, part, inc in (
        ("KR", kr_part, include_kr),
        ("US", us_part, include_us),
    ):
        if not inc:
            continue
        if isinstance(part, dict) and part.get("consensus_met"):
            hits = part.get("consensus_pattern_hits", 0)
            n = part.get("samples_analyzed", 0)
            rules = part.get("pre_emptive_rule") or {}
            active = [k for k, v in rules.items() if v]
            lines.append(f"✅ <b>{tag}</b> 합의 {hits}/10 | 표본 {n} | 신호 {len(active)}개")
            if active:
                lines.append(" · ".join(active[:6]) + ("…" if len(active) > 6 else ""))
        elif part is not None:
            lines.append(f"◽ <b>{tag}</b>: 합의 미달 또는 스킵")

    try:
        send_telegram_msg("\n".join(lines))
    except Exception as e:
        logger.exception("limit_up_forensics: send_telegram_msg failed for global DNA report: %s", e)
        print(f"⚠️ [limit_up_forensics] 글로벌 DNA 텔레그램 발송 실패: {e}")


def run_limit_up_forensics(
    markets: Optional[Sequence[str]] = None,
) -> None:
    """
    markets: None → KR+US / ('KR',) / ('US',) 등.
    KR·US 실행은 분리된 함수로 수행하고, 저장·텔레그램은 finally에서 수행한다.
    """
    print("🔬 [상한가 해부학 부검소] 글로벌 DNA 역추적...")
    if markets is None:
        regions = ("KR", "US")
    else:
        regions = tuple(m.upper() for m in markets)

    include_kr = "KR" in regions
    include_us = "US" in regions

    cfg = load_config()
    kr_payload: Optional[Dict[str, Any]] = None
    us_payload: Optional[Dict[str, Any]] = None
    kr_log = "N/A (스케줄 미포함)" if not include_kr else "미실행"
    us_log = "N/A (스케줄 미포함)" if not include_us else "미실행"

    try:
        if include_kr:
            try:
                kr_payload, kr_log = run_kr_limit_up_dna()
                if kr_payload:
                    _merge_dna_section(cfg, "KR", kr_payload)
                else:
                    prev = cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY)
                    if isinstance(prev, dict) and "KR" in prev:
                        kr_log += " (KR 이전 스냅샷 유지)"
            except Exception as e:
                kr_payload = None
                kr_log = f"KR: 예외 — {e}"
                print(f"⚠️ [상한가 DNA] KR 파이프라인 오류(US와 논리 분리됨): {e}")

        if include_us:
            try:
                us_payload, us_log = run_us_limit_up_dna()
                if us_payload:
                    _merge_dna_section(cfg, "US", us_payload)
                else:
                    prev = cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY)
                    if isinstance(prev, dict) and "US" in prev:
                        us_log += " (US 이전 스냅샷 유지)"
            except Exception as e:
                us_payload = None
                us_log = f"US: 예외 — {e}"
                print(f"⚠️ [상한가 DNA] US 파이프라인 오류: {e}")
    finally:
        try:
            updated_any = kr_payload is not None or us_payload is not None
            if updated_any:
                lump = cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY)
                if isinstance(lump, dict):
                    lump["updated_at_global"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    lump["last_regions_run"] = list(regions)
                if save_config(cfg):
                    print(f"✅ {LIMIT_UP_COHORT_DNA_CONFIG_KEY} (상한가 코호트 DNA, KR/US) 원자적 저장 완료")
                else:
                    print(f"⚠️ {LIMIT_UP_COHORT_DNA_CONFIG_KEY} 저장 실패")
            else:
                print(f"💡 갱신 없음 | KR: {kr_log} | US: {us_log}")
        except Exception as e:
            print(f"⚠️ 상한가 부검 저장 단계 오류: {e}")

        try:
            _send_global_dna_report(
                kr_payload or (cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY) or {}).get("KR"),
                us_payload or (cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY) or {}).get("US"),
                kr_log,
                us_log,
                include_kr=include_kr,
                include_us=include_us,
            )
        except Exception as e:
            print(f"⚠️ 상한가 부검 텔레그램 단계 오류: {e}")


if __name__ == "__main__":
    run_limit_up_forensics()
