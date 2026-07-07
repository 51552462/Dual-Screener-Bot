import json
import os
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from bitget.config_hub import load_config as load_system_config


SYS_CONFIG = load_system_config()


def _prepare_ohlcv_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    멀티 타임프레임(DateTime) 인덱스 정규화:
    - YYYY-MM-DD HH:MM:SS 문자열 인덱스/컬럼을 안전하게 datetime으로 변환
    - 중복 인덱스 제거, 오름차순 정렬
    - OHLCV 숫자형 강제 변환
    """
    if df_raw is None or len(df_raw) == 0:
        return df_raw
    df = df_raw.copy()

    # Date 컬럼이 있으면 인덱스로 승격
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=False)
        df = df.dropna(subset=["Date"]).set_index("Date")
    else:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce", utc=False)
    df = df[~df.index.isna()]
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns])
    return df


def _tf_dynamic_window(timeframe: str) -> int:
    tfu = str(timeframe).upper()
    if tfu == "1D":
        return 365
    if tfu == "4H":
        return 365 * 6
    if tfu == "2H":
        return 365 * 12
    if tfu == "1H":
        return 365 * 24
    return 365


def get_dynamic_score(series_data, higher_is_better=True, timeframe="1D"):
    window = _tf_dynamic_window(timeframe)
    if len(series_data) < 20:
        return 5.0
    pct_rank = (
        pd.Series(series_data)
        .rolling(window, min_periods=20)
        .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False)
        .fillna(0.5)
        .values[-1]
    )
    if higher_is_better:
        return 1.0 + (pct_rank * 9.0)
    return 1.0 + ((1.0 - pct_rank) * 9.0)


def scale_score(val, best, worst):
    if best > worst:
        if val >= best:
            return 10.0
        if val <= worst:
            return 1.0
        return 1.0 + 9.0 * (val - worst) / (best - worst)
    if val <= best:
        return 10.0
    if val >= worst:
        return 1.0
    return 1.0 + 9.0 * (worst - val) / (worst - best)


def _compute_dna_flags(dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs, *, short=False):
    """한국/미국 스캐너의 DNA 플래그(is_tenbagger/is_top_dna/is_worst_dna/is_death_combo)를
    코인용으로 이식한다.

    한국/미국은 절대 임계값(예: cpv>=0.56)을 쓰지만, 코인은 심볼별 가격/변동성
    스케일이 크게 달라 그대로 이식할 수 없다. 대신 각 엔진이 이미 계산해 둔
    롤링 퍼센타일 점수(dyn_*_score, 1~10 — 해당 엔진 방향 기준 "높을수록 유리"로
    이미 정규화됨)를 재사용해 심볼/타임프레임별로 자체 보정되게 한다.
    """
    hi, lo = 8.5, 2.0
    is_top_dna = bool(dyn_rs_score >= hi and dyn_cpv_score >= hi and dyn_tb_score >= hi)
    is_worst_dna = bool(dyn_cpv_score <= lo and dyn_tb_score <= lo and score_bbe <= lo)
    rs_against_trend = (cur_rs > 0) if short else (cur_rs < 0)
    is_death_combo = bool(dyn_cpv_score <= lo and rs_against_trend)
    is_tenbagger = bool(dyn_rs_score >= 9.0 and dyn_cpv_score >= hi)
    return {
        "is_top_dna": is_top_dna,
        "is_worst_dna": is_worst_dna,
        "is_death_combo": is_death_combo,
        "is_tenbagger": is_tenbagger,
    }


def _core_factors(df: pd.DataFrame):
    c = df["Close"].values
    o = df["Open"].values
    h = df["High"].values
    l = df["Low"].values
    v = df["Volume"].values

    with np.errstate(divide="ignore", invalid="ignore"):
        cpv = np.where(h != l, (c - o) / (h - l), 0.5)
        v_ma20 = pd.Series(v).rolling(20).mean().values
        vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
        tb_index = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), 0.0)

        bb_mid = pd.Series(c).rolling(20).mean().values
        bb_std = pd.Series(c).rolling(20).std().values
        bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
        bb_energy = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
    return cpv, tb_index, bb_energy


def _rs(df: pd.DataFrame, idx_close: pd.Series):
    """
    벤치마크(BTC) 대비 상대강도. 하락장 역행: idx_ret<0 이고 stock_ret>0 이면
    auto_forward_tester(V46)와 동일하게 defiance_premium = abs(idx_ret)*1.5 를
    초과수익(excess_return)에 가산한 뒤 |idx_ret| 로 스케일한 RS 로 대체한다.
    """
    c = np.asarray(df["Close"].values, dtype=float)
    idx_s = idx_close.reindex(df.index).ffill()
    idx = np.asarray(idx_s.values, dtype=float)
    c_20 = pd.Series(c).shift(20).values
    idx_20 = np.asarray(idx_s.shift(20).values, dtype=float)
    eps = 1e-12
    pos_floor = 0.0001

    with np.errstate(divide="ignore", invalid="ignore"):
        stock_ret = np.where(c_20 > eps, (c - c_20) / c_20, 0.0)
        idx_ret = np.where(idx_20 > eps, (idx - idx_20) / idx_20, pos_floor)

        denom_legacy = np.where(np.abs(idx_ret) < eps, pos_floor, idx_ret)
        legacy_rs = (stock_ret / denom_legacy) * 100.0

        defiance = (idx_ret < -eps) & (stock_ret > eps)
        excess_return = stock_ret - idx_ret
        defiance_premium = np.where(defiance, np.abs(idx_ret) * 1.5, 0.0)
        boosted_excess = excess_return + defiance_premium
        denom_mag = np.maximum(np.abs(idx_ret), eps)
        rs_defiance = (boosted_excess / denom_mag) * 100.0

        rs = np.where(defiance, rs_defiance, legacy_rs)

    return np.nan_to_num(rs, nan=0.0, posinf=0.0, neginf=0.0)


def _tree_reject(cur_cpv: float):
    tree_fatal_cpv = SYS_CONFIG.get("TREE_FATAL_CPV", 0.85)
    if cur_cpv > tree_fatal_cpv:
        return True, f"악성 매물 캔들 한계치 초과 (CPV {cur_cpv:.2f} > {tree_fatal_cpv})"
    return False, ""


def _estimate_24h_trade_value(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    try:
        idx = pd.to_datetime(df.index)
        if len(idx) >= 2:
            delta_h = (idx[-1] - idx[-2]).total_seconds() / 3600.0
        else:
            delta_h = 24.0
        if not np.isfinite(delta_h) or delta_h <= 0:
            delta_h = 24.0
    except Exception:
        delta_h = 24.0
    bars_24h = int(max(1, round(24.0 / delta_h)))
    sub = df.tail(bars_24h)
    tv = (sub["Close"].astype(float) * sub["Volume"].astype(float)).sum()
    return float(tv) if np.isfinite(tv) else 0.0


def _coin_liquidity_rank_score(df: pd.DataFrame):
    tv24 = _estimate_24h_trade_value(df)
    if tv24 >= 5_000_000_000:
        return 10.0, "초대형 코인", tv24
    if tv24 >= 1_000_000_000:
        return 8.0, "대형 코인", tv24
    if tv24 >= 200_000_000:
        return 6.0, "중형 코인", tv24
    if tv24 >= 50_000_000:
        return 4.0, "소형 알트", tv24
    return 2.0, "잡알트", tv24


def _calc_shape20(close_arr: np.ndarray):
    if close_arr is None or len(close_arr) < 40:
        return None
    c = close_arr[-300:] if len(close_arr) > 300 else close_arr
    c = np.asarray(c, dtype=float)
    c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
    return np.array([np.mean(x) for x in np.array_split(c_norm, 20)])


def _calc_dtw(s, t):
    n, m = len(s), len(t)
    dtw = np.full((n + 1, m + 1), np.inf)
    dtw[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(float(s[i - 1]) - float(t[j - 1]))
            dtw[i, j] = cost + min(dtw[i - 1, j], dtw[i, j - 1], dtw[i - 1, j - 1])
    return float(dtw[n, m])


def _cosine(a, b, dim: int = 7):
    """주식 DNA 템플릿(7D)과 동일 길이로 정렬 후 코사인. 짧은 쪽은 0 패딩(초신성 차원 고정)."""
    d = max(7, int(dim))
    va = np.asarray(a, dtype=float).reshape(-1)
    vb = np.asarray(b, dtype=float).reshape(-1)

    def _pad_norm(x: np.ndarray) -> np.ndarray:
        x = np.nan_to_num(np.asarray(x[:d], dtype=float), nan=0.0, posinf=0.0, neginf=0.0)
        if x.size < d:
            x = np.pad(x, (0, d - x.size))
        return x

    va = _pad_norm(va)
    vb = _pad_norm(vb)
    na = np.linalg.norm(va)
    nb = np.linalg.norm(vb)
    if na <= 1e-12 or nb <= 1e-12:
        return 0.0
    return float(np.dot(va, vb) / (na * nb))


def _auto_forward_style_7d_vector(df: pd.DataFrame, idx_close: pd.Series) -> Tuple[np.ndarray, Dict[str, float]]:
    """
    auto_forward_tester try_add 내 7D 연산과 동일한 스칼라 벡터.
    순서: [cpv, tb, bbe/safe_vol, z_rs, vcp_ratio, vol_flow, ma_conv]
    (템플릿 키: cpv, tb, bbe, rs, vcp, vol, ma — rs 슬롯은 z_rs)
    """
    empty_meta = {
        "agg_cpv": 0.0,
        "agg_tb": 0.0,
        "agg_bbe": 0.0,
        "bbe_scaled": 0.0,
        "z_rs": 0.0,
        "vcp_ratio": 0.0,
        "vol_flow": 0.0,
        "ma_conv": 0.0,
        "safe_vol": 1.0,
    }
    if df is None or len(df) < 60:
        return np.zeros(7, dtype=float), empty_meta

    c = df["Close"].values.astype(float)
    o = df["Open"].values.astype(float)
    h = df["High"].values.astype(float)
    l = df["Low"].values.astype(float)
    v = df["Volume"].values.astype(float)

    idx_s = idx_close.reindex(df.index).ffill()
    idx_c = np.asarray(idx_s.values, dtype=float)
    if idx_c.shape[0] != c.shape[0]:
        idx_c = c.astype(float).copy()
    elif not (np.isfinite(idx_c[0]) and np.isfinite(idx_c[-1]) and abs(float(idx_c[0])) > 1e-15):
        idx_c = c.astype(float).copy()

    cpv = float(np.nanmean(np.where(h != l, (c - o) / (h - l), 0.5)))
    v_ma20 = pd.Series(v).rolling(20).mean().values
    tb = float(
        np.nanmean(
            np.where(
                h != l,
                (v / np.maximum(v_ma20, 1e-12)) / np.maximum((c - o) / (h - l), 0.01),
                1.0,
            )
        )
    )

    bb_std = pd.Series(c).rolling(20).std().values
    bb_mid_roll = pd.Series(c).rolling(20).mean().values
    bbe_arr = np.where((bb_std > 0) & np.isfinite(bb_mid_roll), 1.0 / ((4 * bb_std) / bb_mid_roll), 0.0)
    bbe = float(np.nanmax(bbe_arr[-20:])) if len(bbe_arr) >= 20 else float(np.nanmax(bbe_arr))

    rs_slope = ((c[-1] - c[0]) / (c[0] + 1e-12)) * 100.0

    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    mean_tr = float(np.nanmean(tr))
    vcp_ratio = float(np.mean(tr[-20:]) / mean_tr) if mean_tr > 0 else 1.0

    vol_flow = float(np.sum(np.where(c > o, v, 0.0)) / (np.sum(np.where(c < o, v, 0.0)) + 1.0))

    emas = [float(pd.Series(c).ewm(span=n, adjust=False).mean().iloc[-1]) for n in [10, 20, 60, 112, 224]]
    emin, emax = min(emas), max(emas)
    ma_conv = float((emax - emin) / (emin + 1e-12) * 100.0)

    idx_rs = ((idx_c[-1] - idx_c[0]) / (idx_c[0] + 1e-12)) * 100.0
    idx_vol = float(pd.Series(idx_c).pct_change().std() * 100.0 * np.sqrt(252))
    safe_vol = idx_vol if idx_vol > 0.1 else 1.0

    excess_return = rs_slope - idx_rs
    defiance_premium = 0.0
    if idx_rs < 0 and excess_return > 0:
        defiance_premium = abs(idx_rs) * 1.5
    z_rs = float((excess_return + defiance_premium) / safe_vol)

    bbe_scaled = bbe / safe_vol
    vec = np.nan_to_num(
        np.array([cpv, tb, bbe_scaled, z_rs, vcp_ratio, vol_flow, ma_conv], dtype=float),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    meta = {
        "agg_cpv": cpv,
        "agg_tb": tb,
        "agg_bbe": bbe,
        "bbe_scaled": float(bbe_scaled),
        "z_rs": float(z_rs),
        "vcp_ratio": float(vcp_ratio),
        "vol_flow": float(vol_flow),
        "ma_conv": float(ma_conv),
        "safe_vol": float(safe_vol),
    }
    return vec, meta


def _dbg_merge_7d(dbg: Dict, df: pd.DataFrame, idx_close: pd.Series) -> Dict:
    vec7, meta7 = _auto_forward_style_7d_vector(df, idx_close)
    out = dict(dbg)
    out.update(meta7)
    out["vcp_ratio"] = float(meta7["vcp_ratio"])
    out["vol_flow"] = float(meta7["vol_flow"])
    out["ma_conv"] = float(meta7["ma_conv"])
    out["current_vec"] = [float(x) for x in vec7]
    return out


def _dna_template_to_vec7(dna: dict) -> np.ndarray:
    """주식 템플릿 키(cpv,tb,bbe,rs,vcp,vol,ma) 또는 vec 리스트 → 길이 7 벡터."""
    if isinstance(dna.get("vec"), (list, tuple)) and len(dna.get("vec")) > 0:
        raw = list(dna.get("vec"))
        vals = [float(x) for x in raw[:7]]
        while len(vals) < 7:
            vals.append(0.0)
        return np.array(vals[:7], dtype=float)
    vcp_extra = float(
        dna.get("vcp", dna.get("vcp_ratio", 0.0)) or 0.0
    )
    vol_extra = float(dna.get("vol", dna.get("vol_flow", 0.0)) or 0.0)
    ma_extra = float(dna.get("ma", dna.get("ma_conv", 0.0)) or 0.0)
    return np.array(
        [
            float(dna.get("cpv", 0.0)),
            float(dna.get("tb", 0.0)),
            float(dna.get("bbe", 0.0)),
            float(dna.get("rs", 0.0)),
            vcp_extra,
            vol_extra,
            ma_extra,
        ],
        dtype=float,
    )


def _doppelganger_adjustment(current_vec_7: np.ndarray, close_arr):
    cur_shape = _calc_shape20(close_arr)
    if cur_shape is None:
        return 0.0, "", 0.0, 999.0

    current_vec = np.nan_to_num(
        np.asarray(current_vec_7, dtype=float).reshape(-1)[:7], nan=0.0, posinf=0.0, neginf=0.0
    )
    if current_vec.size < 7:
        current_vec = np.pad(current_vec, (0, 7 - current_vec.size))
    else:
        current_vec = current_vec[:7]

    best_name = ""
    best_cos = 0.0
    best_dtw = 999.0
    best_kind = "NONE"

    for i in [1, 2, 3]:
        a = SYS_CONFIG.get(f"DNA_ALPHA_RANK{i}")
        t = SYS_CONFIG.get(f"DNA_TRAP_RANK{i}")
        for kind, dna in (("ALPHA", a), ("TRAP", t)):
            if not isinstance(dna, dict):
                continue
            dvec = _dna_template_to_vec7(dna)
            dshape = dna.get("shape")
            if not isinstance(dshape, list) or len(dshape) != 20:
                continue
            cos = _cosine(current_vec, dvec)
            dtw = _calc_dtw(cur_shape, dshape)
            score = cos - (dtw * 0.01)
            if score > (best_cos - best_dtw * 0.01):
                best_cos = cos
                best_dtw = dtw
                best_name = str(dna.get("name", f"{kind}_{i}"))
                best_kind = kind

    if best_kind == "ALPHA" and best_cos >= 0.80 and best_dtw <= 3.0:
        msg = f"🌌 [도플갱어 매칭] 대장주 DNA 유사도 {best_cos*100:.1f}% | DTW {best_dtw:.2f} ({best_name})"
        return 10.0, msg, best_cos, best_dtw
    if best_kind == "TRAP" and best_cos >= 0.80 and best_dtw <= 3.0:
        msg = f"💀 [도플갱어 경고] 참사주 DNA 유사도 {best_cos*100:.1f}% | DTW {best_dtw:.2f} ({best_name})"
        return -30.0, msg, best_cos, best_dtw
    return 0.0, "", best_cos, best_dtw


def _build_exit_strategy(sig_type, cur_cpv, total_score, regime_weight=1.0):
    if cur_cpv >= 0.70:
        cpv_stat = f"현재 꽉 찬 양봉 (CPV {cur_cpv:.2f})"
    elif cur_cpv <= 0.40:
        cpv_stat = f"매물 소화 꼬리형 캔들 (CPV {cur_cpv:.2f})"
    else:
        cpv_stat = f"표준 캔들 (CPV {cur_cpv:.2f})"

    active_exit_mode = SYS_CONFIG.get("ACTIVE_EXIT_MODE", "HYBRID")
    ns_prefix = "COIN_MASTER_S1"
    if "S4" in sig_type:
        ns_prefix = "COIN_MASTER_S4"
    if "S6" in sig_type or "S7" in sig_type:
        ns_prefix = "COIN_NULRIM_SX"
    if "5선" in sig_type:
        ns_prefix = "COIN_5EMA_S1"
    # 💡 [코인 생태계 특화] 숏 전용 네임스페이스 — 롱(MASTER_S1) 청산 파라미터를
    # 잘못 물려받지 않도록 독립 분리(진화·학습도 롱과 별개로 축적).
    if "TV_SHORT_V1" in sig_type.upper():
        ns_prefix = "COIN_SHORT_V1"
    if "TV_SHORT_V2" in sig_type.upper():
        ns_prefix = "COIN_SHORT_V2"

    opt_time_stop = int(SYS_CONFIG.get(f"{ns_prefix}_TIME_STOP", 10))
    opt_sl_atr = float(SYS_CONFIG.get(f"{ns_prefix}_ATR_SL", 2.0))

    if active_exit_mode == "TECH":
        action = "📈 [TECH] 단기데드/ZLEMA 이탈 전까지 추세 추종."
    elif active_exit_mode == "STAT":
        action = f"🎯 [STAT] {opt_time_stop} bars 타임스탑 + ATR {opt_sl_atr:.2f} 손절."
    else:
        action = f"⚖️ [HYBRID] 추세 추종 + {opt_time_stop} bars 시간제한 + ATR {opt_sl_atr:.2f} 방어."

    if total_score >= 80:
        tier_stat = f"🔥 총점 {total_score:.1f}점 1티어: 메인 비중 허용."
    elif total_score <= 50:
        tier_stat = f"⚠️ 총점 {total_score:.1f}점: 비중 대폭 축소."
    else:
        tier_stat = f"🧭 총점 {total_score:.1f}점: 중립 비중."

    regime_msg = f"🚨 관제탑 비중 배수: {regime_weight:.2f}x"
    return f"[{cpv_stat}]\n{action}\n\n{tier_stat}\n{regime_msg}"


def compute_master_signal(df_raw: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df_raw)
    if df_raw is None or len(df_raw) < 240:
        return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f"EMA{n}"] = df["Close"].ewm(span=n, adjust=False, min_periods=0).mean()

    c, o, h, l, v = (
        df["Close"].values,
        df["Open"].values,
        df["High"].values,
        df["Low"].values,
        df["Volume"].values,
    )
    e10, e20, e30, e60 = df["EMA10"].values, df["EMA20"].values, df["EMA30"].values, df["EMA60"].values
    e112, e224, e448 = df["EMA112"].values, df["EMA224"].values, df["EMA448"].values

    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    atr = pd.Series(tr).ewm(alpha=1 / 20, adjust=False, min_periods=0).mean().values

    cpv, tb_index, bb_energy = _core_factors(df)
    rs = _rs(df, idx_close)

    is_aligned_30 = (e10 > e20) & (e20 > e30)
    is_aligned_112 = is_aligned_30 & (e30 > e60) & (e60 > e112)
    is_aligned_224 = is_aligned_112 & (e112 > e224)
    is_aligned_448 = is_aligned_224 & (e224 > e448)
    is_bullish = c > o
    show_values = is_aligned_112 & is_bullish

    with np.errstate(divide="ignore", invalid="ignore"):
        spread_112_224 = np.where(show_values, ((e112 - e224) / atr) * 100, 0)
        spread_10_30 = np.where(show_values, ((e10 - e30) / atr) * 100, 0)
        spread_10_20 = np.where(show_values, ((e10 - e20) / atr) * 100, 0)
        idx_arr = np.arange(len(c))
        r_val = pd.Series(e10).rolling(10).corr(pd.Series(idx_arr)).fillna(0).values
        r_squared = r_val * r_val
        e10_3 = np.roll(e10, 3)
        e10_3[:3] = e10[:3]
        ema_roc = np.where(e10_3 != 0, ((e10 - e10_3) / e10_3) * 5000, 0)

    true_momentum_line = np.where(is_aligned_30, ema_roc * (r_squared**2), 0)
    prev_tml = np.roll(true_momentum_line, 1)
    prev_tml[0] = 0

    cond_rising = true_momentum_line > prev_tml
    cond_blue_30 = spread_112_224 >= 30
    cond_highest_angle = (
        (true_momentum_line > spread_10_20)
        & (true_momentum_line > spread_10_30)
        & (true_momentum_line > spread_112_224)
    )
    cond_val_sig1 = (spread_10_30 >= 100) & (spread_10_20 >= 50) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle
    cond_val_sig2_3 = (spread_10_30 >= 150) & (spread_10_20 >= 100) & (true_momentum_line >= 150) & cond_blue_30 & cond_highest_angle
    raw_sig1 = is_aligned_112 & cond_val_sig1 & cond_rising
    raw_sig2 = is_aligned_224 & cond_val_sig2_3 & cond_rising
    raw_sig3 = is_aligned_448 & cond_val_sig2_3 & cond_rising

    c_3 = np.roll(c, 3)
    c_3[:3] = c[:3]
    candle_roc = np.where(c_3 != 0, ((c - c_3) / c_3) * 1000, 0)
    wma_roc = pd.Series(candle_roc).rolling(3).apply(lambda x: np.dot(x, [1, 2, 3]) / 6, raw=True).fillna(0).values
    candle_angle = np.where(is_aligned_30, wma_roc, 0)
    raw_sig4 = np.zeros(len(c), dtype=bool)
    is_candle_bottom = False
    for i in range(len(c)):
        if candle_angle[i] <= 0:
            is_candle_bottom = True
        if is_candle_bottom and (candle_angle[i] >= 50) and is_aligned_30[i] and is_bullish[i]:
            raw_sig4[i] = True
            is_candle_bottom = False

    signal_3 = raw_sig3
    signal_2 = raw_sig2 & (~signal_3)
    signal_1 = raw_sig1 & (~signal_2) & (~signal_3)
    signal_4 = raw_sig4 & (~signal_1) & (~signal_2) & (~signal_3)

    hit_s1 = bool(signal_1[-1])
    hit_s4 = bool(signal_4[-1])
    if not (hit_s1 or hit_s4):
        return False, "", df, {}

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    score_rs = scale_score(cur_rs, 2025.28, -821.13)
    score_cpv = scale_score(cur_cpv, 0.39, 0.95)
    score_bbe = scale_score(cur_bbe, 56.80, 3.80)
    score_tb = scale_score(cur_tb, 20.13, 2.47)
    score_ema = 10.0 if hit_s1 else 5.0
    score_marcap, marcap_tier, trade_value_24h = _coin_liquidity_rank_score(df)
    marcap_eok = float(trade_value_24h / 100_000_000.0)
    recent_hits = signal_1[-252:-1].sum() + signal_4[-252:-1].sum() if len(c) > 252 else signal_1[:-1].sum() + signal_4[:-1].sum()
    freq_count = int(recent_hits)
    total_score = (score_rs * 10 + score_ema * 9 + score_marcap * 8 + score_cpv * 7 + score_bbe * 6 + score_tb * 5) / 450 * 100

    vec7, meta7 = _auto_forward_style_7d_vector(df, idx_close)
    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(vec7, c)
    total_score = float(np.clip(total_score + dd_adj, 0.0, 100.0))

    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        total_score = 0.0

    dyn_rs_score = get_dynamic_score(rs, True, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
    dna_flags = _compute_dna_flags(dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs)
    tf_tag = str(timeframe).upper()
    sig_type = f"[{tf_tag}] 🔥 S1 (마스터 추세)" if hit_s1 else f"[{tf_tag}] 🚀 S4 (마스터 바닥탈출)"

    score_clipped = float(np.clip(total_score, 0.0, 100.0))
    exit_strategy = _build_exit_strategy(sig_type, cur_cpv, score_clipped, regime_weight=1.0)
    v11_comment = (
        f"📊 [System B 코인 마스터 브리핑]\n"
        f"🔹 시스템 총점: {score_clipped:.1f} / 100점\n"
        f"🔹 체급(24h 거래대금): {marcap_tier} | {trade_value_24h:,.0f} USDT\n"
        f"🔹 도플갱어: cos {dd_cos*100:.1f}% | dtw {dd_dtw:.2f}\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.3f}\n"
        f"▪️ 진짜양봉(TB): {cur_tb:.3f}\n"
        f"▪️ 응축에너지(BBE): {cur_bbe:.3f}\n"
        f"▪️ 시장상대강도(RS): {cur_rs:.3f}\n"
        f"▪️ 7D 수렴도(vcp_ratio): {meta7['vcp_ratio']:.4f}\n"
        f"▪️ 7D 거래량흐름(vol_flow): {meta7['vol_flow']:.4f}\n"
        f"▪️ 7D 이평밀집(ma_conv): {meta7['ma_conv']:.3f}% | z_rs: {meta7['z_rs']:.4f}\n"
    )
    if dd_msg:
        v11_comment += f"{dd_msg}\n"
    if rej:
        v11_comment += f"🚫 Decision Tree 기각: {reason}\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "score": score_clipped,
        "v_cpv": float(cur_cpv),
        "v_yang": float(cur_tb),
        "v_energy": float(cur_bbe),
        "v_rs": float(cur_rs),
        "tml": float(true_momentum_line[-1]),
        "dyn_rs_score": float(dyn_rs_score),
        "dyn_tb_score": float(dyn_tb_score),
        "dyn_cpv_score": float(dyn_cpv_score),
        "score_marcap": float(score_marcap),
        "marcap_tier": marcap_tier,
        "trade_value_24h": float(trade_value_24h),
        "marcap_eok": float(marcap_eok),
        "freq_count": int(freq_count),
        "v11_comment": v11_comment,
        "recommend": exit_strategy,
        "sn_score": float(dd_cos),
        "tree_rejected": rej,
        "tree_reason": reason,
        **dna_flags,
        "vcp_ratio": float(meta7["vcp_ratio"]),
        "vol_flow": float(meta7["vol_flow"]),
        "ma_conv": float(meta7["ma_conv"]),
        "agg_cpv": float(meta7["agg_cpv"]),
        "agg_tb": float(meta7["agg_tb"]),
        "agg_bbe": float(meta7["agg_bbe"]),
        "z_rs": float(meta7["z_rs"]),
        "bbe_scaled": float(meta7["bbe_scaled"]),
        "safe_vol": float(meta7["safe_vol"]),
        "current_vec": [float(x) for x in vec7],
    }


def compute_nulrim_signal(df_raw: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df_raw)
    if df_raw is None or len(df_raw) < 240:
        return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f"EMA{n}"] = df["Close"].ewm(span=n, adjust=False, min_periods=0).mean()
    c, o, h, l, v = (
        df["Close"].values,
        df["Open"].values,
        df["High"].values,
        df["Low"].values,
        df["Volume"].values,
    )
    e10, e20, e30, e60 = df["EMA10"].values, df["EMA20"].values, df["EMA30"].values, df["EMA60"].values
    e112, e224, e448 = df["EMA112"].values, df["EMA224"].values, df["EMA448"].values

    cpv, tb_index, bb_energy = _core_factors(df)
    rs = _rs(df, idx_close)
    moneyOk = (c * v) >= SYS_CONFIG.get("MIN_TRADE_VALUE", 100_000_000)
    priceOk = c >= SYS_CONFIG.get("MIN_PRICE", 1000)
    isBullish = c > o
    align112 = (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112)
    align224 = align112 & (e112 > e224)
    align448 = align224 & (e224 > e448)
    longKeep448 = e224 > e448
    prev_align448 = np.roll(align448, 1)
    prev_align448[0] = False
    prev_longKeep448 = np.roll(longKeep448, 1)
    prev_longKeep448[0] = False
    s1 = align448 & (~prev_align448) & prev_longKeep448 & isBullish
    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    prev_e20 = np.roll(e20, 1)
    prev_e20[0] = 0
    raw_s4 = align448 & (prev_c < prev_e20) & (c > e10) & isBullish
    s4 = np.zeros_like(c, dtype=bool)
    last_pb = -100
    for i in range(len(c)):
        if raw_s4[i] and (i - last_pb > 5):
            s4[i] = True
            last_pb = i
    macroBear = (e60 < e112) & (e112 < e224) & (e224 < e448)
    shortBelow = (e10 < e60) & (e20 < e60) & (e30 < e60)
    shortBull = (e10 > e20) & (e20 > e30)
    prev_shortBull = np.roll(shortBull, 1)
    prev_shortBull[0] = False
    s6 = macroBear & shortBelow & shortBull & (~prev_shortBull) & isBullish
    prev_e60 = np.roll(e60, 1)
    prev_e60[0] = np.inf
    prev_e112 = np.roll(e112, 1)
    prev_e112[0] = 0
    s7 = (e224 < e448) & (e112 < e224) & (prev_e60 <= prev_e112) & align112 & isBullish

    cond = moneyOk & priceOk
    hit_s1, hit_s4, hit_s6, hit_s7 = bool((s1 & cond)[-1]), bool((s4 & cond)[-1]), bool((s6 & cond)[-1]), bool((s7 & cond)[-1])
    if not (hit_s1 or hit_s4 or hit_s6 or hit_s7):
        return False, "", df, {}

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    score_rs = scale_score(cur_rs, 1563.0, -745.10)
    score_cpv = scale_score(cur_cpv, 0.23, 0.85)
    score_bbe = scale_score(cur_bbe, 5400.0, 10.0)
    score_tb = scale_score(cur_tb, 20.0, 5.0)
    score_marcap, marcap_tier, trade_value_24h = _coin_liquidity_rank_score(df)
    marcap_eok = float(trade_value_24h / 100_000_000.0)
    recent_hits = (s1 | s4 | s6 | s7)[-252:-1].sum() if len(c) > 252 else (s1 | s4 | s6 | s7)[:-1].sum()
    freq_count = int(recent_hits)
    total_score = (score_rs * 10 + score_marcap * 9 + score_cpv * 8 + score_bbe * 7 + score_tb * 6) / 400 * 100

    vec7, meta7 = _auto_forward_style_7d_vector(df, idx_close)
    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(vec7, c)
    total_score = float(np.clip(total_score + dd_adj, 0.0, 100.0))

    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        total_score = 0.0

    dyn_rs_score = get_dynamic_score(rs, True, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
    dna_flags = _compute_dna_flags(dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs)
    tf_tag = str(timeframe).upper()
    raw_sig = "S6" if hit_s6 else ("S7" if hit_s7 else ("S4" if hit_s4 else "S1"))
    label_map = {
        "S1": "🔥 S1 (눌림 대세)",
        "S4": "🚀 S4 (눌림 바닥탈출)",
        "S6": "🌱 S6 (바닥턴)",
        "S7": "⚡ S7 (중기턴)"
    }
    sig_type = f"[{tf_tag}] {label_map.get(raw_sig, raw_sig)}"
    score_clipped = float(np.clip(total_score, 0.0, 100.0))
    exit_strategy = _build_exit_strategy(sig_type, cur_cpv, score_clipped, regime_weight=1.0)
    v11_comment = (
        f"📊 [System B 코인 눌림 브리핑]\n"
        f"🔹 시스템 총점: {score_clipped:.1f} / 100점\n"
        f"🔹 체급(24h 거래대금): {marcap_tier} | {trade_value_24h:,.0f} USDT\n"
        f"🔹 도플갱어: cos {dd_cos*100:.1f}% | dtw {dd_dtw:.2f}\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.3f}\n"
        f"▪️ 진짜양봉(TB): {cur_tb:.3f}\n"
        f"▪️ 응축에너지(BBE): {cur_bbe:.3f}\n"
        f"▪️ 시장상대강도(RS): {cur_rs:.3f}\n"
        f"▪️ 7D 수렴도(vcp_ratio): {meta7['vcp_ratio']:.4f}\n"
        f"▪️ 7D 거래량흐름(vol_flow): {meta7['vol_flow']:.4f}\n"
        f"▪️ 7D 이평밀집(ma_conv): {meta7['ma_conv']:.3f}% | z_rs: {meta7['z_rs']:.4f}\n"
    )
    if dd_msg:
        v11_comment += f"{dd_msg}\n"
    if rej:
        v11_comment += f"🚫 Decision Tree 기각: {reason}\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "score": score_clipped,
        "v_cpv": float(cur_cpv),
        "v_yang": float(cur_tb),
        "v_energy": float(cur_bbe),
        "v_rs": float(cur_rs),
        "dyn_rs_score": float(dyn_rs_score),
        "dyn_tb_score": float(dyn_tb_score),
        "dyn_cpv_score": float(dyn_cpv_score),
        "score_marcap": float(score_marcap),
        "marcap_tier": marcap_tier,
        "trade_value_24h": float(trade_value_24h),
        "marcap_eok": float(marcap_eok),
        "freq_count": int(freq_count),
        "v11_comment": v11_comment,
        "recommend": exit_strategy,
        "sn_score": float(dd_cos),
        "tree_rejected": rej,
        "tree_reason": reason,
        **dna_flags,
        "vcp_ratio": float(meta7["vcp_ratio"]),
        "vol_flow": float(meta7["vol_flow"]),
        "ma_conv": float(meta7["ma_conv"]),
        "agg_cpv": float(meta7["agg_cpv"]),
        "agg_tb": float(meta7["agg_tb"]),
        "agg_bbe": float(meta7["agg_bbe"]),
        "z_rs": float(meta7["z_rs"]),
        "bbe_scaled": float(meta7["bbe_scaled"]),
        "safe_vol": float(meta7["safe_vol"]),
        "current_vec": [float(x) for x in vec7],
    }


def compute_ema5_signal(df_raw: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df_raw)
    if df_raw is None or len(df_raw) < 240:
        return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [5, 10, 20, 30, 60, 112, 224, 448]:
        df[f"EMA{n}"] = df["Close"].ewm(span=n, adjust=False, min_periods=0).mean()
    c, o, h, l, v = (
        df["Close"].values,
        df["Open"].values,
        df["High"].values,
        df["Low"].values,
        df["Volume"].values,
    )
    e5, e10, e20, e30 = df["EMA5"].values, df["EMA10"].values, df["EMA20"].values, df["EMA30"].values
    e60, e112, e224, e448 = df["EMA60"].values, df["EMA112"].values, df["EMA224"].values, df["EMA448"].values
    cpv, tb_index, bb_energy = _core_factors(df)
    rs = _rs(df, idx_close)

    moneyOk = (c * v) >= SYS_CONFIG.get("MIN_TRADE_VALUE", 100_000_000)
    priceOk = c >= SYS_CONFIG.get("MIN_PRICE", 1000)
    alignFullBull = (e5 > e10) & (e10 > e20) & (e20 > e30) & (e30 > e60) & (e60 > e112) & (e112 > e224) & (e224 > e448)
    isBullish = c > o
    isBodyCross5 = (o < e5) & (c > e5)
    v_prev = np.roll(v, 1)
    v_prev[0] = v[0]
    condVol = v >= (v_prev * 3)
    finalSignal = alignFullBull & isBullish & isBodyCross5 & condVol & moneyOk & priceOk
    if not bool(finalSignal[-1]):
        return False, "", df, {}

    cur_cpv, cur_tb, cur_bbe, cur_rs = cpv[-1], tb_index[-1], bb_energy[-1], rs[-1]
    score_rs = scale_score(cur_rs, 2025.28, -821.13)
    score_ema = 10.0
    score_cpv = scale_score(cur_cpv, 0.39, 0.95)
    score_bbe = scale_score(cur_bbe, 56.80, 3.80)
    score_tb = scale_score(cur_tb, 20.13, 2.47)
    score_marcap, marcap_tier, trade_value_24h = _coin_liquidity_rank_score(df)
    marcap_eok = float(trade_value_24h / 100_000_000.0)
    recent_hits = finalSignal[-252:-1].sum() if len(c) > 252 else finalSignal[:-1].sum()
    freq_count = int(recent_hits)
    total_score = (score_rs * 10 + score_ema * 9 + score_marcap * 8 + score_cpv * 7 + score_bbe * 6 + score_tb * 5) / 450 * 100

    vec7, meta7 = _auto_forward_style_7d_vector(df, idx_close)
    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(vec7, c)
    total_score = float(np.clip(total_score + dd_adj, 0.0, 100.0))
    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        total_score = 0.0
    dyn_rs_score = get_dynamic_score(rs, True, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
    dna_flags = _compute_dna_flags(dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs)
    tf_tag = str(timeframe).upper()
    sig_type = f"[{tf_tag}] 🔥 S1 (5선 관통)"
    score_clipped = float(np.clip(total_score, 0.0, 100.0))
    exit_strategy = _build_exit_strategy(sig_type, cur_cpv, score_clipped, regime_weight=1.0)
    v11_comment = (
        f"📊 [System B 코인 5EMA 브리핑]\n"
        f"🔹 시스템 총점: {score_clipped:.1f} / 100점\n"
        f"🔹 체급(24h 거래대금): {marcap_tier} | {trade_value_24h:,.0f} USDT\n"
        f"🔹 도플갱어: cos {dd_cos*100:.1f}% | dtw {dd_dtw:.2f}\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.3f}\n"
        f"▪️ 진짜양봉(TB): {cur_tb:.3f}\n"
        f"▪️ 응축에너지(BBE): {cur_bbe:.3f}\n"
        f"▪️ 시장상대강도(RS): {cur_rs:.3f}\n"
        f"▪️ 7D 수렴도(vcp_ratio): {meta7['vcp_ratio']:.4f}\n"
        f"▪️ 7D 거래량흐름(vol_flow): {meta7['vol_flow']:.4f}\n"
        f"▪️ 7D 이평밀집(ma_conv): {meta7['ma_conv']:.3f}% | z_rs: {meta7['z_rs']:.4f}\n"
    )
    if dd_msg:
        v11_comment += f"{dd_msg}\n"
    if rej:
        v11_comment += f"🚫 Decision Tree 기각: {reason}\n"

    return True, sig_type, df, {
        "sig_type": sig_type,
        "score": score_clipped,
        "v_cpv": float(cur_cpv),
        "v_yang": float(cur_tb),
        "v_energy": float(cur_bbe),
        "v_rs": float(cur_rs),
        "dyn_rs_score": float(dyn_rs_score),
        "dyn_tb_score": float(dyn_tb_score),
        "dyn_cpv_score": float(dyn_cpv_score),
        "score_marcap": float(score_marcap),
        "marcap_tier": marcap_tier,
        "trade_value_24h": float(trade_value_24h),
        "marcap_eok": float(marcap_eok),
        "freq_count": int(freq_count),
        "v11_comment": v11_comment,
        "recommend": exit_strategy,
        "sn_score": float(dd_cos),
        "tree_rejected": rej,
        "tree_reason": reason,
        **dna_flags,
        "vcp_ratio": float(meta7["vcp_ratio"]),
        "vol_flow": float(meta7["vol_flow"]),
        "ma_conv": float(meta7["ma_conv"]),
        "agg_cpv": float(meta7["agg_cpv"]),
        "agg_tb": float(meta7["agg_tb"]),
        "agg_bbe": float(meta7["agg_bbe"]),
        "z_rs": float(meta7["z_rs"]),
        "bbe_scaled": float(meta7["bbe_scaled"]),
        "safe_vol": float(meta7["safe_vol"]),
        "current_vec": [float(x) for x in vec7],
    }


def compute_tv_short_v1(df: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df)
    if df_raw is None or len(df_raw) < 200:
        return False, "", df_raw, {}

    out = df_raw.copy()
    for n in [20, 34, 60, 75, 160]:
        out[f"EMA{n}"] = out["Close"].ewm(span=n, adjust=False, min_periods=0).mean()

    e20 = out["EMA20"]
    e34 = out["EMA34"]
    e60 = out["EMA60"]
    e75 = out["EMA75"]
    e160 = out["EMA160"]
    body_high = np.maximum(out["Open"], out["Close"])

    inverse_align = (e20 < e34) & (e34 < e60) & (e60 < e75) & (e75 < e160)

    # bodyHigh crossunder ema20: prev >= prev_ema20 and curr < curr_ema20
    entry1 = inverse_align & (body_high.shift(1) >= e20.shift(1)) & (body_high < e20)

    # prev high > prev ema160 & prev close < prev ema160 & curr close < curr ema160
    entry2 = (out["High"].shift(1) > e160.shift(1)) & (out["Close"].shift(1) < e160.shift(1)) & (out["Close"] < e160)

    # inverse align & curr high > prev high & curr close < prev low
    entry3 = inverse_align & (out["High"] > out["High"].shift(1)) & (out["Close"] < out["Low"].shift(1))

    cpv, tb_index, bb_energy = _core_factors(out)
    rs = _rs(out, idx_close)

    final_signal = entry1 | entry2 | entry3
    if not bool(final_signal.iloc[-1]):
        return False, "", out, {}

    sig_type = "[TV_SHORT_V1] SHORT"
    cur_cpv = float(cpv[-1])
    cur_tb = float(tb_index[-1])
    cur_bbe = float(bb_energy[-1])
    cur_rs = float(rs[-1])

    # 💡 [코인 생태계 특화] 롱(MASTER)과 동형의 연속 스코어링 — RS는 부호를 반전해
    # "벤치마크 대비 약세일수록" 숏에 유리하도록 스케일링한다(0/100 이진 버그 수정).
    n_triggers = int(bool(entry1.iloc[-1])) + int(bool(entry2.iloc[-1])) + int(bool(entry3.iloc[-1]))
    score_ema = 10.0 if n_triggers >= 2 else (7.0 if n_triggers == 1 else 5.0)
    score_rs = scale_score(-cur_rs, 2025.28, -821.13)
    score_cpv = scale_score(cur_cpv, 0.39, 0.95)
    score_bbe = scale_score(cur_bbe, 56.80, 3.80)
    score_tb = scale_score(cur_tb, 20.13, 2.47)
    score_marcap, marcap_tier, trade_value_24h = _coin_liquidity_rank_score(out)
    marcap_eok = float(trade_value_24h / 100_000_000.0)
    recent_hits = final_signal[-252:-1].sum() if len(out) > 252 else final_signal[:-1].sum()
    freq_count = int(recent_hits)
    score = (score_rs * 10 + score_ema * 9 + score_marcap * 8 + score_cpv * 7 + score_bbe * 6 + score_tb * 5) / 450 * 100

    # 💡 [코인 생태계 특화] 숏(Short) 포지션도 의사결정나무 및 도플갱어 검증 필수 수행
    vec7, meta7 = _auto_forward_style_7d_vector(out, idx_close)
    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(vec7, out["Close"].values)
    score = float(np.clip(score + dd_adj, 0.0, 100.0))
    guard_comment = ""

    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        score = 0.0
        guard_comment += f"\n🚫 Decision Tree 기각: {reason}\n"
    if dd_msg:
        guard_comment += f"\n{dd_msg}\n"

    dyn_rs_score = get_dynamic_score(rs, False, timeframe)  # 숏: RS 낮을수록(약세) 고득점
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
    dna_flags = _compute_dna_flags(dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs, short=True)
    score_clipped = float(np.clip(score, 0.0, 100.0))
    exit_strategy = _build_exit_strategy(sig_type, cur_cpv, score_clipped, regime_weight=1.0)

    v11_comment = (
        f"📉 [TV Short V1 브리핑]\n"
        f"🔹 시스템 총점: {score_clipped:.1f} / 100점\n"
        f"🔹 체급(24h 거래대금): {marcap_tier} | {trade_value_24h:,.0f} USDT\n"
        f"🔹 도플갱어: cos {dd_cos*100:.1f}% | dtw {dd_dtw:.2f}\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.3f}\n"
        f"▪️ 진짜양봉(TB): {cur_tb:.3f}\n"
        f"▪️ 응축에너지(BBE): {cur_bbe:.3f}\n"
        f"▪️ 시장상대강도(RS): {cur_rs:.3f}\n"
        f"▪️ 트리거: e1={bool(entry1.iloc[-1])}, e2={bool(entry2.iloc[-1])}, e3={bool(entry3.iloc[-1])}"
    )
    v11_comment += guard_comment
    dbg = {
        "sig_type": sig_type,
        "score": score_clipped,
        "side": "SHORT",
        "entry_high": float(out["High"].iloc[-1]),
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs,
        "dyn_rs_score": float(dyn_rs_score),
        "dyn_tb_score": float(dyn_tb_score),
        "dyn_cpv_score": float(dyn_cpv_score),
        "score_marcap": float(score_marcap),
        "marcap_tier": marcap_tier,
        "trade_value_24h": float(trade_value_24h),
        "marcap_eok": float(marcap_eok),
        "freq_count": int(freq_count),
        "v11_comment": v11_comment,
        "recommend": exit_strategy,
        "sn_score": float(dd_cos),
        "tree_rejected": rej,
        "tree_reason": reason,
        **dna_flags,
        "entry1": bool(entry1.iloc[-1]),
        "entry2": bool(entry2.iloc[-1]),
        "entry3": bool(entry3.iloc[-1]),
    }
    dbg = _dbg_merge_7d(dbg, out, idx_close)
    return True, sig_type, out, dbg


def compute_tv_short_v2(df: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df)
    if df_raw is None or len(df_raw) < 200:
        return False, "", df_raw, {}

    out = df_raw.copy()
    for n in [20, 34, 60, 75, 120, 160]:
        out[f"EMA{n}"] = out["Close"].ewm(span=n, adjust=False, min_periods=0).mean()

    e20 = out["EMA20"]
    e34 = out["EMA34"]
    e60 = out["EMA60"]
    e75 = out["EMA75"]
    e120 = out["EMA120"]
    e160 = out["EMA160"]

    inverse_align = (e20 < e34) & (e34 < e60) & (e60 < e75) & (e75 < e120) & (e120 < e160)

    # prev high > prev ema120 & prev close < prev ema120 & curr close < curr ema120 & bearish candle
    entry2 = (
        (out["High"].shift(1) > e120.shift(1))
        & (out["Close"].shift(1) < e120.shift(1))
        & (out["Close"] < e120)
        & (out["Open"] > out["Close"])
    )

    cpv, tb_index, bb_energy = _core_factors(out)
    rs = _rs(out, idx_close)

    final_signal = inverse_align & entry2
    if not bool(final_signal.iloc[-1]):
        return False, "", out, {}

    sig_type = "[TV_SHORT_V2] SHORT"
    cur_cpv = float(cpv[-1])
    cur_tb = float(tb_index[-1])
    cur_bbe = float(bb_energy[-1])
    cur_rs = float(rs[-1])

    # 💡 [코인 생태계 특화] 롱(MASTER)과 동형의 연속 스코어링 — RS는 부호를 반전해
    # "벤치마크 대비 약세일수록" 숏에 유리하도록 스케일링한다(0/100 이진 버그 수정).
    body_range = float(out["High"].iloc[-1] - out["Low"].iloc[-1])
    body_dom = (
        float(max(0.0, out["Open"].iloc[-1] - out["Close"].iloc[-1]) / body_range)
        if body_range > 1e-9
        else 0.0
    )
    score_ema = 10.0 if body_dom >= 0.5 else (7.0 if body_dom >= 0.25 else 5.0)
    score_rs = scale_score(-cur_rs, 2025.28, -821.13)
    score_cpv = scale_score(cur_cpv, 0.39, 0.95)
    score_bbe = scale_score(cur_bbe, 56.80, 3.80)
    score_tb = scale_score(cur_tb, 20.13, 2.47)
    score_marcap, marcap_tier, trade_value_24h = _coin_liquidity_rank_score(out)
    marcap_eok = float(trade_value_24h / 100_000_000.0)
    recent_hits = final_signal[-252:-1].sum() if len(out) > 252 else final_signal[:-1].sum()
    freq_count = int(recent_hits)
    score = (score_rs * 10 + score_ema * 9 + score_marcap * 8 + score_cpv * 7 + score_bbe * 6 + score_tb * 5) / 450 * 100

    # 💡 [코인 생태계 특화] 숏(Short) 포지션도 의사결정나무 및 도플갱어 검증 필수 수행
    vec7, meta7 = _auto_forward_style_7d_vector(out, idx_close)
    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(vec7, out["Close"].values)
    score = float(np.clip(score + dd_adj, 0.0, 100.0))
    guard_comment = ""

    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        score = 0.0
        guard_comment += f"\n🚫 Decision Tree 기각: {reason}\n"
    if dd_msg:
        guard_comment += f"\n{dd_msg}\n"

    dyn_rs_score = get_dynamic_score(rs, False, timeframe)  # 숏: RS 낮을수록(약세) 고득점
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
    dna_flags = _compute_dna_flags(dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs, short=True)
    score_clipped = float(np.clip(score, 0.0, 100.0))
    exit_strategy = _build_exit_strategy(sig_type, cur_cpv, score_clipped, regime_weight=1.0)

    v11_comment = (
        f"📉 [TV Short V2 브리핑]\n"
        f"🔹 시스템 총점: {score_clipped:.1f} / 100점\n"
        f"🔹 체급(24h 거래대금): {marcap_tier} | {trade_value_24h:,.0f} USDT\n"
        f"🔹 도플갱어: cos {dd_cos*100:.1f}% | dtw {dd_dtw:.2f}\n"
        f"▪️ 캔들지배력(CPV): {cur_cpv:.3f}\n"
        f"▪️ 진짜양봉(TB): {cur_tb:.3f}\n"
        f"▪️ 응축에너지(BBE): {cur_bbe:.3f}\n"
        f"▪️ 시장상대강도(RS): {cur_rs:.3f}\n"
        f"▪️ 트리거: e2={bool(entry2.iloc[-1])}"
    )
    v11_comment += guard_comment
    dbg = {
        "sig_type": sig_type,
        "score": score_clipped,
        "side": "SHORT",
        "entry_high": float(out["High"].iloc[-1]),
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs,
        "dyn_rs_score": float(dyn_rs_score),
        "dyn_tb_score": float(dyn_tb_score),
        "dyn_cpv_score": float(dyn_cpv_score),
        "score_marcap": float(score_marcap),
        "marcap_tier": marcap_tier,
        "trade_value_24h": float(trade_value_24h),
        "marcap_eok": float(marcap_eok),
        "freq_count": int(freq_count),
        "v11_comment": v11_comment,
        "recommend": exit_strategy,
        "sn_score": float(dd_cos),
        "tree_rejected": rej,
        "tree_reason": reason,
        **dna_flags,
        "entry2": bool(entry2.iloc[-1]),
    }
    dbg = _dbg_merge_7d(dbg, out, idx_close)
    return True, sig_type, out, dbg


PRACTITIONER_RULES = {
    "P01_RSI_OVERSOLD": "rsi_cross_30_up",
    "P02_RSI_OVERBOUGHT_SHORT": "rsi_cross_70_down",
    "P03_STOCH_GOLDEN": "stoch_kd_golden_low",
    "P04_STOCH_DEAD_HIGH": "stoch_kd_dead_high",
    "P05_BB_LOWER_BOUNCE": "bb_lower_reclaim",
    "P06_BB_UPPER_REJECT": "bb_upper_reject",
    "P07_EMA20_60_GOLDEN": "ema20_60_golden",
    "P08_EMA20_60_DEAD": "ema20_60_dead",
    "P09_BREAKOUT_20D": "breakout_20",
    "P10_BREAKDOWN_20D": "breakdown_20",
    "P11_VOL_EXPANSION_LONG": "vol_expand_bull",
    "P12_VOL_EXPANSION_SHORT": "vol_expand_bear",
    "P13_MEAN_REVERT_LONG": "mean_revert_long",
    "P14_MEAN_REVERT_SHORT": "mean_revert_short",
    "P15_RSI_DIVERGENCE_LONG": "rsi_div_long",
    "P16_RSI_DIVERGENCE_SHORT": "rsi_div_short",
    "P17_ATR_SQUEEZE_BREAKUP": "atr_squeeze_up",
    "P18_ATR_SQUEEZE_BREAKDOWN": "atr_squeeze_down",
    "P19_MACD_CROSS_UP": "macd_up",
    "P20_MACD_CROSS_DOWN": "macd_down",
    "P21_ZLEMA_RECLAIM": "zlema_reclaim",
    "P22_ZLEMA_REJECT": "zlema_reject",
    "P23_MUTANT_ALPHA_1": "mutant_alpha_1",
    "P24_MUTANT_ALPHA_2": "mutant_alpha_2",
    "P25_MUTANT_ALPHA_3": "mutant_alpha_3",
    "P26_RSI50_TREND_LONG": "rsi50_trend_long",
    "P27_RSI50_TREND_SHORT": "rsi50_trend_short",
    "P28_BB_MID_CROSS_LONG": "bb_mid_cross_long",
    "P29_BB_MID_CROSS_SHORT": "bb_mid_cross_short",
    "P30_MTF_MOMENTUM_LONG": "mtf_momo_long",
}


def _practitioner_signal(df_raw: pd.DataFrame, idx_close: pd.Series, rule_key: str, label: str, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df_raw)
    if df_raw is None or len(df_raw) < 220:
        return False, "", df_raw, {}
    df = df_raw.copy()
    c = df["Close"].astype(float)
    o = df["Open"].astype(float)
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    v = df["Volume"].astype(float)

    # common indicators
    ema20 = c.ewm(span=20, adjust=False).mean()
    ema60 = c.ewm(span=60, adjust=False).mean()
    ema120 = c.ewm(span=120, adjust=False).mean()
    delta = c.diff()
    gain = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
    rsx = gain / loss.replace(0, np.nan)
    rsi = (100 - (100 / (1 + rsx))).fillna(50.0)
    ll = l.rolling(14).min()
    hh = h.rolling(14).max()
    stoch_k = ((c - ll) / (hh - ll).replace(0, np.nan) * 100).fillna(50.0)
    stoch_d = stoch_k.rolling(3).mean().fillna(50.0)
    bb_mid = c.rolling(20).mean()
    bb_std = c.rolling(20).std()
    bb_up = bb_mid + (2.0 * bb_std)
    bb_dn = bb_mid - (2.0 * bb_std)
    prev_c = c.shift(1)
    tr = np.maximum(h - l, np.maximum((h - prev_c).abs(), (l - prev_c).abs()))
    atr14 = tr.ewm(span=14, adjust=False).mean()
    vol_ma20 = v.rolling(20).mean()
    macd_fast = c.ewm(span=12, adjust=False).mean()
    macd_slow = c.ewm(span=26, adjust=False).mean()
    macd = macd_fast - macd_slow
    macd_sig = macd.ewm(span=9, adjust=False).mean()
    z1 = c.ewm(span=20, adjust=False).mean()
    z2 = z1.ewm(span=20, adjust=False).mean()
    zlema = z1 + (z1 - z2)

    # core factors for dbg
    cpv, tb_index, bb_energy = _core_factors(df)
    rs_arr = _rs(df, idx_close)
    cur_cpv = float(cpv[-1])
    cur_tb = float(tb_index[-1])
    cur_bbe = float(bb_energy[-1])
    cur_rs = float(rs_arr[-1])

    long_cond = False
    short_cond = False

    if rule_key == "rsi_cross_30_up":
        long_cond = (rsi.iloc[-2] <= 30) and (rsi.iloc[-1] > 30)
    elif rule_key == "rsi_cross_70_down":
        short_cond = (rsi.iloc[-2] >= 70) and (rsi.iloc[-1] < 70)
    elif rule_key == "stoch_kd_golden_low":
        long_cond = (stoch_k.iloc[-2] <= stoch_d.iloc[-2]) and (stoch_k.iloc[-1] > stoch_d.iloc[-1]) and (stoch_k.iloc[-1] < 30)
    elif rule_key == "stoch_kd_dead_high":
        short_cond = (stoch_k.iloc[-2] >= stoch_d.iloc[-2]) and (stoch_k.iloc[-1] < stoch_d.iloc[-1]) and (stoch_k.iloc[-1] > 70)
    elif rule_key == "bb_lower_reclaim":
        long_cond = (c.iloc[-2] < bb_dn.iloc[-2]) and (c.iloc[-1] > bb_dn.iloc[-1])
    elif rule_key == "bb_upper_reject":
        short_cond = (h.iloc[-1] > bb_up.iloc[-1]) and (c.iloc[-1] < bb_up.iloc[-1])
    elif rule_key == "ema20_60_golden":
        long_cond = (ema20.iloc[-2] <= ema60.iloc[-2]) and (ema20.iloc[-1] > ema60.iloc[-1])
    elif rule_key == "ema20_60_dead":
        short_cond = (ema20.iloc[-2] >= ema60.iloc[-2]) and (ema20.iloc[-1] < ema60.iloc[-1])
    elif rule_key == "breakout_20":
        long_cond = c.iloc[-1] > h.rolling(20).max().iloc[-2]
    elif rule_key == "breakdown_20":
        short_cond = c.iloc[-1] < l.rolling(20).min().iloc[-2]
    elif rule_key == "vol_expand_bull":
        long_cond = (v.iloc[-1] > vol_ma20.iloc[-1] * 2.0) and (c.iloc[-1] > o.iloc[-1])
    elif rule_key == "vol_expand_bear":
        short_cond = (v.iloc[-1] > vol_ma20.iloc[-1] * 2.0) and (c.iloc[-1] < o.iloc[-1])
    elif rule_key == "mean_revert_long":
        long_cond = (c.iloc[-1] < ema20.iloc[-1] - atr14.iloc[-1]) and (rsi.iloc[-1] < 35)
    elif rule_key == "mean_revert_short":
        short_cond = (c.iloc[-1] > ema20.iloc[-1] + atr14.iloc[-1]) and (rsi.iloc[-1] > 65)
    elif rule_key == "rsi_div_long":
        long_cond = (c.iloc[-1] < c.iloc[-5]) and (rsi.iloc[-1] > rsi.iloc[-5])
    elif rule_key == "rsi_div_short":
        short_cond = (c.iloc[-1] > c.iloc[-5]) and (rsi.iloc[-1] < rsi.iloc[-5])
    elif rule_key == "atr_squeeze_up":
        long_cond = (atr14.iloc[-1] < atr14.rolling(20).mean().iloc[-1]) and (c.iloc[-1] > bb_up.iloc[-1])
    elif rule_key == "atr_squeeze_down":
        short_cond = (atr14.iloc[-1] < atr14.rolling(20).mean().iloc[-1]) and (c.iloc[-1] < bb_dn.iloc[-1])
    elif rule_key == "macd_up":
        long_cond = (macd.iloc[-2] <= macd_sig.iloc[-2]) and (macd.iloc[-1] > macd_sig.iloc[-1])
    elif rule_key == "macd_down":
        short_cond = (macd.iloc[-2] >= macd_sig.iloc[-2]) and (macd.iloc[-1] < macd_sig.iloc[-1])
    elif rule_key == "zlema_reclaim":
        long_cond = (c.iloc[-2] <= zlema.iloc[-2]) and (c.iloc[-1] > zlema.iloc[-1])
    elif rule_key == "zlema_reject":
        short_cond = (c.iloc[-2] >= zlema.iloc[-2]) and (c.iloc[-1] < zlema.iloc[-1])
    elif rule_key == "mutant_alpha_1":
        alpha = ((c - ema20) / ema20.replace(0, np.nan) + (v / vol_ma20.replace(0, np.nan))).fillna(0.0)
        long_cond = alpha.iloc[-1] > alpha.rolling(30).quantile(0.8).iloc[-1]
    elif rule_key == "mutant_alpha_2":
        alpha = ((h - l) / c.replace(0, np.nan) * rsi).fillna(0.0)
        short_cond = alpha.iloc[-1] > alpha.rolling(30).quantile(0.85).iloc[-1] and c.iloc[-1] < o.iloc[-1]
    elif rule_key == "mutant_alpha_3":
        alpha = ((macd - macd_sig) * (rsi - 50)).fillna(0.0)
        long_cond = alpha.iloc[-1] > 0 and alpha.iloc[-1] > alpha.iloc[-2]
    elif rule_key == "rsi50_trend_long":
        long_cond = (rsi.iloc[-1] > 50) and (ema20.iloc[-1] > ema60.iloc[-1]) and (c.iloc[-1] > ema20.iloc[-1])
    elif rule_key == "rsi50_trend_short":
        short_cond = (rsi.iloc[-1] < 50) and (ema20.iloc[-1] < ema60.iloc[-1]) and (c.iloc[-1] < ema20.iloc[-1])
    elif rule_key == "bb_mid_cross_long":
        long_cond = (c.iloc[-2] <= bb_mid.iloc[-2]) and (c.iloc[-1] > bb_mid.iloc[-1])
    elif rule_key == "bb_mid_cross_short":
        short_cond = (c.iloc[-2] >= bb_mid.iloc[-2]) and (c.iloc[-1] < bb_mid.iloc[-1])
    elif rule_key == "mtf_momo_long":
        long_cond = (ema20.iloc[-1] > ema60.iloc[-1] > ema120.iloc[-1]) and (c.iloc[-1] > ema20.iloc[-1]) and (rsi.iloc[-1] > 55)

    if not (long_cond or short_cond):
        return False, "", df, {}

    side = "SHORT" if short_cond and not long_cond else "LONG"
    score_bbe = scale_score(cur_bbe, 56.80, 3.80)
    rs_higher_better = side != "SHORT"
    dyn_rs_score = get_dynamic_score(rs_arr, rs_higher_better, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
    dna_flags = _compute_dna_flags(
        dyn_rs_score, dyn_cpv_score, dyn_tb_score, score_bbe, cur_rs, short=(side == "SHORT")
    )
    score = float(np.clip(50.0 + abs(cur_rs) * 0.05 + abs(cur_tb) * 0.8 + abs(cur_bbe) * 0.03, 45.0, 99.0))
    sig_type = f"[{label}] {'SHORT' if side == 'SHORT' else 'LONG'}"
    v11_comment = (
        f"🧠 [{label} 실무자 브리핑]\n"
        f"🔹 총점: {score:.1f}/100 | 방향: {side}\n"
        f"▪️ CPV: {cur_cpv:.3f}\n"
        f"▪️ TB: {cur_tb:.3f}\n"
        f"▪️ BBE: {cur_bbe:.3f}\n"
        f"▪️ RS: {cur_rs:.3f}\n"
    )
    dbg = {
        "sig_type": sig_type,
        "score": score,
        "side": side,
        "entry_high": float(df["High"].iloc[-1]),
        "v_cpv": cur_cpv,
        "v_yang": cur_tb,
        "v_energy": cur_bbe,
        "v_rs": cur_rs,
        "dyn_rs_score": dyn_rs_score,
        "dyn_cpv_score": dyn_cpv_score,
        "dyn_tb_score": dyn_tb_score,
        "v11_comment": v11_comment,
        **dna_flags,
    }
    dbg = _dbg_merge_7d(dbg, df, idx_close)
    return True, sig_type, df, dbg


def compute_practitioner_01(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P01_RSI_OVERSOLD"], "P01_RSI_OVERSOLD", timeframe)
def compute_practitioner_02(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P02_RSI_OVERBOUGHT_SHORT"], "P02_RSI_OVERBOUGHT_SHORT", timeframe)
def compute_practitioner_03(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P03_STOCH_GOLDEN"], "P03_STOCH_GOLDEN", timeframe)
def compute_practitioner_04(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P04_STOCH_DEAD_HIGH"], "P04_STOCH_DEAD_HIGH", timeframe)
def compute_practitioner_05(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P05_BB_LOWER_BOUNCE"], "P05_BB_LOWER_BOUNCE", timeframe)
def compute_practitioner_06(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P06_BB_UPPER_REJECT"], "P06_BB_UPPER_REJECT", timeframe)
def compute_practitioner_07(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P07_EMA20_60_GOLDEN"], "P07_EMA20_60_GOLDEN", timeframe)
def compute_practitioner_08(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P08_EMA20_60_DEAD"], "P08_EMA20_60_DEAD", timeframe)
def compute_practitioner_09(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P09_BREAKOUT_20D"], "P09_BREAKOUT_20D", timeframe)
def compute_practitioner_10(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P10_BREAKDOWN_20D"], "P10_BREAKDOWN_20D", timeframe)
def compute_practitioner_11(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P11_VOL_EXPANSION_LONG"], "P11_VOL_EXPANSION_LONG", timeframe)
def compute_practitioner_12(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P12_VOL_EXPANSION_SHORT"], "P12_VOL_EXPANSION_SHORT", timeframe)
def compute_practitioner_13(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P13_MEAN_REVERT_LONG"], "P13_MEAN_REVERT_LONG", timeframe)
def compute_practitioner_14(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P14_MEAN_REVERT_SHORT"], "P14_MEAN_REVERT_SHORT", timeframe)
def compute_practitioner_15(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P15_RSI_DIVERGENCE_LONG"], "P15_RSI_DIVERGENCE_LONG", timeframe)
def compute_practitioner_16(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P16_RSI_DIVERGENCE_SHORT"], "P16_RSI_DIVERGENCE_SHORT", timeframe)
def compute_practitioner_17(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P17_ATR_SQUEEZE_BREAKUP"], "P17_ATR_SQUEEZE_BREAKUP", timeframe)
def compute_practitioner_18(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P18_ATR_SQUEEZE_BREAKDOWN"], "P18_ATR_SQUEEZE_BREAKDOWN", timeframe)
def compute_practitioner_19(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P19_MACD_CROSS_UP"], "P19_MACD_CROSS_UP", timeframe)
def compute_practitioner_20(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P20_MACD_CROSS_DOWN"], "P20_MACD_CROSS_DOWN", timeframe)
def compute_practitioner_21(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P21_ZLEMA_RECLAIM"], "P21_ZLEMA_RECLAIM", timeframe)
def compute_practitioner_22(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P22_ZLEMA_REJECT"], "P22_ZLEMA_REJECT", timeframe)
def compute_practitioner_23(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P23_MUTANT_ALPHA_1"], "P23_MUTANT_ALPHA_1", timeframe)
def compute_practitioner_24(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P24_MUTANT_ALPHA_2"], "P24_MUTANT_ALPHA_2", timeframe)
def compute_practitioner_25(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P25_MUTANT_ALPHA_3"], "P25_MUTANT_ALPHA_3", timeframe)
def compute_practitioner_26(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P26_RSI50_TREND_LONG"], "P26_RSI50_TREND_LONG", timeframe)
def compute_practitioner_27(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P27_RSI50_TREND_SHORT"], "P27_RSI50_TREND_SHORT", timeframe)
def compute_practitioner_28(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P28_BB_MID_CROSS_LONG"], "P28_BB_MID_CROSS_LONG", timeframe)
def compute_practitioner_29(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P29_BB_MID_CROSS_SHORT"], "P29_BB_MID_CROSS_SHORT", timeframe)
def compute_practitioner_30(df_raw, idx_close, timeframe="1D"): return _practitioner_signal(df_raw, idx_close, PRACTITIONER_RULES["P30_MTF_MOMENTUM_LONG"], "P30_MTF_MOMENTUM_LONG", timeframe)
