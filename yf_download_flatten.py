"""yfinance MultiIndex / (N,1) Close 방어 — 다운로드 직후 OHLCV를 1차원 Series로 정규화."""
import pandas as pd


def flatten_yf_download_df(df):
    """
    yf.download 직후, 종가·이평 계산 전에 호출.
    다중 티커 패널 전체에는 적용하지 말고 티커별 서브프레임(df_batch[tk] 등)에만 적용할 것.
    """
    if df is None or getattr(df, "empty", True):
        return df
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    # 안전장치: 컬럼에 MultiIndex가 없더라도 Close 등이 DataFrame 형태라면 1차원 Series로 강제 Squeeze
    if isinstance(df.get("Close"), pd.DataFrame):
        for col in list(df.columns):
            df[col] = df[col].squeeze()
    return df


def yf_close_series(panel, symbol):
    """멀티티커 yf.download 패널에서 심볼별 종가 시리즈 (전체 패널에 droplevel 금지, 티커 서브만 평탄화)."""
    try:
        if panel is None or getattr(panel, "empty", True):
            return pd.Series(dtype=float)
        cols = panel.columns
        if isinstance(cols, pd.MultiIndex):
            if symbol in cols.get_level_values(0):
                sub = flatten_yf_download_df(panel[symbol].copy())
            elif symbol in cols.get_level_values(1):
                sub = flatten_yf_download_df(panel.xs(symbol, level=1, axis=1).copy())
            else:
                return pd.Series(dtype=float)
            if "Close" not in sub.columns:
                return pd.Series(dtype=float)
            s = sub["Close"]
            return s.squeeze() if isinstance(s, pd.DataFrame) else s
        return pd.Series(dtype=float)
    except Exception:
        return pd.Series(dtype=float)
