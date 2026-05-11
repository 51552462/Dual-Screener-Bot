"""
상한가·급등 종목 과거 ~30거래일 역추적 → 선취매 DNA → system_config.json LIMIT_UP_DNA (KR / US).
독립 위성: main / supernova_hunter 비수정.
"""
from __future__ import annotations

import json
import math
import os
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

try:
    import yfinance as yf
except ImportError:
    yf = None

CONFIG_PATH = os.path.join(
    os.path.expanduser("~"),
    "dante_bots",
    "Dual-Screener-Bot",
    "system_config.json",
)

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


def _ensure_change_rate(krx: pd.DataFrame) -> pd.DataFrame:
    df = krx.copy()
    if "ChangeRate" in df.columns:
        df["ChangeRate"] = pd.to_numeric(df["ChangeRate"], errors="coerce")
        return df
    for alt in ("ChgRate", "등락률", "Change", "Rate"):
        if alt in df.columns:
            df["ChangeRate"] = pd.to_numeric(df[alt], errors="coerce")
            return df
    return df


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


def _fetch_listing_krx() -> pd.DataFrame:
    _sleep_stealth()
    if fdr is None:
        raise RuntimeError("FinanceDataReader 미설치")
    krx = fdr.StockListing("KRX")
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


def _run_region_pipeline(region: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """단일 지역 부검. (payload | None, 로그 메시지)"""
    pattern_rows: List[Dict[str, bool]] = []
    codes_used: List[str] = []
    raw_n = 0

    if region == "KR":
        if fdr is None:
            return None, "KR: FinanceDataReader 없음"
        krx = _fetch_listing_krx()
        if "ChangeRate" not in krx.columns or krx["ChangeRate"].isna().all():
            return None, "KR: 등락률 컬럼 없음"

        limit_ups = krx[krx["ChangeRate"] >= LIMIT_UP_THRESHOLD_PCT_KR].copy()
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

    else:
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

    payload = _cohort_to_payload(pattern_rows, codes_used, raw_n, region)
    if payload is None:
        return None, f"{region}: 유효 샘플 0"
    if not payload["consensus_met"]:
        return None, f"{region}: 합의 패턴 {payload['consensus_pattern_hits']}/10 (<7), 저장 생략"
    return payload, f"{region}: OK"


def _merge_dna_section(
    cfg: Dict[str, Any],
    region_key: str,
    new_payload: Optional[Dict[str, Any]],
) -> None:
    lump = cfg.get("LIMIT_UP_DNA")
    if not isinstance(lump, dict):
        lump = {}
    if new_payload is not None:
        lump[region_key] = new_payload
    cfg["LIMIT_UP_DNA"] = lump


def _send_global_dna_report(
    kr_part: Optional[Dict[str, Any]],
    us_part: Optional[Dict[str, Any]],
    kr_msg: str,
    us_msg: str,
) -> None:
    try:
        from auto_forward_tester import send_telegram_msg
    except Exception:
        return

    lines = [
        "<b>[🔬 글로벌 상한가 DNA 분석 리포트]</b>",
        f"<i>{datetime.now().strftime('%Y-%m-%d %H:%M')}</i>",
        "",
        f"▪️ <b>KR</b>: {kr_msg}",
        f"▪️ <b>US</b>: {us_msg}",
        "",
    ]
    for tag, part in (("KR", kr_part), ("US", us_part)):
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
    except Exception:
        pass


def run_limit_up_forensics(
    markets: Optional[Sequence[str]] = None,
) -> None:
    """
    markets: None → KR+US / ('KR',) / ('US',) 등.
    """
    print("🔬 [상한가 해부학 부검소] 글로벌 DNA 역추적...")
    if markets is None:
        regions = ("KR", "US")
    else:
        regions = tuple(m.upper() for m in markets)

    cfg = load_config()
    kr_payload: Optional[Dict[str, Any]] = None
    us_payload: Optional[Dict[str, Any]] = None
    kr_log = "미실행"
    us_log = "미실행"

    try:
        if "KR" in regions:
            kr_payload, kr_log = _run_region_pipeline("KR")
            if kr_payload:
                _merge_dna_section(cfg, "KR", kr_payload)
            else:
                prev = cfg.get("LIMIT_UP_DNA")
                if isinstance(prev, dict) and "KR" in prev:
                    kr_log += " (KR 이전 스냅샷 유지)"

        if "US" in regions:
            us_payload, us_log = _run_region_pipeline("US")
            if us_payload:
                _merge_dna_section(cfg, "US", us_payload)
            else:
                prev = cfg.get("LIMIT_UP_DNA")
                if isinstance(prev, dict) and "US" in prev:
                    us_log += " (US 이전 스냅샷 유지)"

        updated_any = kr_payload is not None or us_payload is not None
        if updated_any:
            lump = cfg.get("LIMIT_UP_DNA")
            if isinstance(lump, dict):
                lump["updated_at_global"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                lump["last_regions_run"] = list(regions)
            if save_config(cfg):
                print("✅ LIMIT_UP_DNA (KR/US) 원자적 저장 완료")
            else:
                print("⚠️ LIMIT_UP_DNA 저장 실패")
        else:
            print(f"💡 갱신 없음 | KR: {kr_log} | US: {us_log}")

        _send_global_dna_report(
            kr_payload or (cfg.get("LIMIT_UP_DNA") or {}).get("KR"),
            us_payload or (cfg.get("LIMIT_UP_DNA") or {}).get("US"),
            kr_log,
            us_log,
        )

    except Exception as e:
        print(f"⚠️ 상한가 부검 중 오류: {e}")


if __name__ == "__main__":
    run_limit_up_forensics()
