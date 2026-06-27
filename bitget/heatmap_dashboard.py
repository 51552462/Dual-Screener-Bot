import os
import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

from bitget.infra.data_paths import market_db_read_path

st.set_page_config(page_title="Bitget Sector Heatmap", layout="wide")
st.title("Bitget Sector Heatmap")

DB_PATH = market_db_read_path()


def _coin_sector(symbol: str) -> str:
    s = str(symbol).upper()
    if any(k in s for k in ("DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF")):
        return "Meme"
    if any(k in s for k in ("UNI", "AAVE", "MKR", "COMP", "CRV", "SNX", "LDO")):
        return "DeFi"
    if any(k in s for k in ("SOL", "AVAX", "ADA", "DOT", "ATOM", "TIA", "NEAR")):
        return "Layer1"
    if any(k in s for k in ("ARB", "OP", "MATIC", "IMX", "ZKS")):
        return "Layer2"
    if any(k in s for k in ("RNDR", "FET", "AGIX", "TAO", "WLD", "AI")):
        return "AI"
    if any(k in s for k in ("LINK", "BAND", "API3", "PYTH")):
        return "Oracle"
    if any(k in s for k in ("BTC", "ETH", "BNB")):
        return "Majors"
    return "Others"


@st.cache_data(ttl=60)
def load_open_positions():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        q = """
            SELECT symbol, market_type, position_side, timeframe, total_score,
                   margin_used, sim_kelly_invest, leverage, entry_price
            FROM bitget_forward_trades
            WHERE status='OPEN'
        """
        df = pd.read_sql(q, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


df = load_open_positions()
if df.empty:
    st.warning("⏳ OPEN 포지션 데이터가 없습니다.")
    st.stop()

df = df.copy()
df["margin_used"] = pd.to_numeric(df["margin_used"], errors="coerce").fillna(0.0)
df["sim_kelly_invest"] = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0)
df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce").fillna(1.0).clip(lower=1.0)
df["position_side"] = df["position_side"].astype(str).str.upper().fillna("LONG")
df["sector"] = df["symbol"].apply(_coin_sector)
df["capital_usdt"] = df["sim_kelly_invest"].where(df["sim_kelly_invest"] > 0, df["margin_used"] * df["leverage"])
df["signed_capital"] = df.apply(lambda r: r["capital_usdt"] if r["position_side"] == "LONG" else -r["capital_usdt"], axis=1)

agg = (
    df.groupby(["sector", "position_side"], as_index=False)
    .agg(
        open_positions=("symbol", "count"),
        capital_usdt=("capital_usdt", "sum"),
        signed_capital=("signed_capital", "sum"),
        avg_score=("total_score", "mean"),
    )
)

col1, col2, col3 = st.columns(3)
col1.metric("총 OPEN", f"{len(df):,}건")
col2.metric("총 투입 노셔널", f"{df['capital_usdt'].sum():,.2f} USDT")
col3.metric("순 익스포저(롱-숏)", f"{df['signed_capital'].sum():+,.2f} USDT")

fig = px.treemap(
    agg,
    path=[px.Constant("Bitget Universe"), "sector", "position_side"],
    values="capital_usdt",
    color="signed_capital",
    color_continuous_scale="RdBu_r",
    color_continuous_midpoint=0.0,
    hover_data=["open_positions", "avg_score"],
)
fig.update_traces(
    texttemplate="<b>%{label}</b><br>%{value:,.0f}U",
    hovertemplate="<b>%{label}</b><br>노셔널=%{value:,.2f} USDT<br>순익스포저=%{color:,.2f} USDT<br>OPEN=%{customdata[0]}<br>평균점수=%{customdata[1]:.1f}",
)
fig.update_layout(template="plotly_dark", height=780, margin=dict(t=25, l=8, r=8, b=8))
st.plotly_chart(fig, use_container_width=True)

st.info("💡 박스 크기=투입 노셔널, 색상=롱(+)/숏(-) 순익스포저. 섹터 단위 자금 쏠림과 방향성을 동시에 확인합니다.")
