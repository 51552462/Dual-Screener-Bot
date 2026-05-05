import json
import os
from typing import Dict, Tuple

import numpy as np
import pandas as pd


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")


def load_system_config() -> Dict:
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


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


def _core_factors(df: pd.DataFrame):
    c = df["Close"].values
    o = df["Open"].values
    h = df["High"].values
    l = df["Low"].values
    v = df["Volume"].values

    cpv = np.where(h != l, (c - o) / (h - l), 0.5)
    v_ma20 = pd.Series(v).rolling(20).mean().values
    vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
    tb_index = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)

    bb_mid = pd.Series(c).rolling(20).mean().values
    bb_std = pd.Series(c).rolling(20).std().values
    bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
    bb_energy = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
    return cpv, tb_index, bb_energy


def _rs(df: pd.DataFrame, idx_close: pd.Series):
    c = df["Close"].values
    idx = idx_close.reindex(df.index).ffill()
    c_20 = pd.Series(c).shift(20).values
    idx_20 = idx.shift(20).values
    with np.errstate(divide="ignore", invalid="ignore"):
        stock_ret = np.where(c_20 > 0, (c - c_20) / c_20, 0.0)
        idx_ret = np.where(idx_20 > 0, (idx.values - idx_20) / idx_20, 0.0001)
        idx_ret = np.where(idx_ret == 0, 0.0001, idx_ret)
        rs = (stock_ret / idx_ret) * 100
    return np.nan_to_num(rs, nan=0.0)


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
    return np.mean(np.array_split(c_norm, 20), axis=1)


def _calc_dtw(s, t):
    n, m = len(s), len(t)
    dtw = np.full((n + 1, m + 1), np.inf)
    dtw[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(float(s[i - 1]) - float(t[j - 1]))
            dtw[i, j] = cost + min(dtw[i - 1, j], dtw[i, j - 1], dtw[i - 1, j - 1])
    return float(dtw[n, m])


def _cosine(a, b):
    va = np.asarray(a, dtype=float)
    vb = np.asarray(b, dtype=float)
    na = np.linalg.norm(va)
    nb = np.linalg.norm(vb)
    if na <= 1e-12 or nb <= 1e-12:
        return 0.0
    return float(np.dot(va, vb) / (na * nb))


def _doppelganger_adjustment(cur_cpv, cur_tb, cur_bbe, cur_rs, close_arr):
    cur_shape = _calc_shape20(close_arr)
    if cur_shape is None:
        return 0.0, "", 0.0, 999.0

    current_vec = np.array([cur_cpv, cur_tb, cur_bbe, cur_rs], dtype=float)
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
            dvec = np.array(
                [
                    float(dna.get("cpv", 0.0)),
                    float(dna.get("tb", 0.0)),
                    float(dna.get("bbe", 0.0)),
                    float(dna.get("rs", 0.0)),
                ],
                dtype=float,
            )
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
    if df_raw is None or len(df_raw) < 500:
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

    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(cur_cpv, cur_tb, cur_bbe, cur_rs, c)
    total_score = float(np.clip(total_score + dd_adj, 0.0, 100.0))

    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        total_score = 0.0

    dyn_rs_score = get_dynamic_score(rs, True, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
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
    }


def compute_nulrim_signal(df_raw: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df_raw)
    if df_raw is None or len(df_raw) < 500:
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

    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(cur_cpv, cur_tb, cur_bbe, cur_rs, c)
    total_score = float(np.clip(total_score + dd_adj, 0.0, 100.0))

    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        total_score = 0.0

    dyn_rs_score = get_dynamic_score(rs, True, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
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
    }


def compute_ema5_signal(df_raw: pd.DataFrame, idx_close: pd.Series, timeframe: str = "1D") -> Tuple[bool, str, pd.DataFrame, Dict]:
    df_raw = _prepare_ohlcv_df(df_raw)
    if df_raw is None or len(df_raw) < 500:
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

    dd_adj, dd_msg, dd_cos, dd_dtw = _doppelganger_adjustment(cur_cpv, cur_tb, cur_bbe, cur_rs, c)
    total_score = float(np.clip(total_score + dd_adj, 0.0, 100.0))
    rej, reason = _tree_reject(float(cur_cpv))
    if rej:
        total_score = 0.0
    dyn_rs_score = get_dynamic_score(rs, True, timeframe)
    dyn_tb_score = get_dynamic_score(tb_index, True, timeframe)
    dyn_cpv_score = get_dynamic_score(cpv, False, timeframe)
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
    }
