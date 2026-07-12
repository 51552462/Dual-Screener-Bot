import os
import sqlite3

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from bitget.infra.bounded_reads import forward_dashboard_closed_sql
from bitget.infra.data_paths import market_db_read_path
from bitget.infra.shared_db_connector import get_connection
from bitget.dashboard_ops_panel import render_ops_gauge_panel

st.set_page_config(page_title="Bitget Quant Factory Control Tower", layout="wide")
st.title("Bitget Quant Factory Control Tower")

render_ops_gauge_panel(hours=6.0)
st.markdown("---")

DB_PATH = market_db_read_path()


@st.cache_data(ttl=60)
def load_factory_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = get_connection(DB_PATH, read_only=True, check_same_thread=False)
        query, params = forward_dashboard_closed_sql()
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        if not df.empty:
            if "exit_date" in df.columns:
                df["exit_date"] = pd.to_datetime(df["exit_date"], errors="coerce")
            df = df.sort_values("exit_date")
        return df
    except Exception:
        return pd.DataFrame()


def _pf(ret_s: pd.Series) -> float:
    s = pd.to_numeric(ret_s, errors="coerce").dropna()
    if s.empty:
        return 0.0
    wins = s[s > 0].sum()
    losses = abs(s[s <= 0].sum()) + 0.1
    return float(wins / losses)


df = load_factory_data()
if df.empty:
    st.warning("⏳ 아직 Bitget 청산 데이터가 충분하지 않습니다.")
    st.stop()

df = df.copy()
df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0)
df["margin_used"] = pd.to_numeric(df["margin_used"], errors="coerce").fillna(0.0)
df["sim_kelly_invest"] = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0)
df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce").fillna(1.0).clip(lower=1.0)
df["position_side"] = df["position_side"].astype(str).str.upper().fillna("LONG")

# 코인 전용 손익 계산: 롱/숏 방향성은 final_ret에 이미 반영, 레버리지는 notional(sim_kelly_invest)로 반영
df["notional_usdt"] = df["sim_kelly_invest"].where(df["sim_kelly_invest"] > 0, df["margin_used"] * df["leverage"])
df["profit_usdt"] = df["notional_usdt"] * (df["final_ret"] / 100.0)

total_trades = int(len(df))
win_rate = float((df["final_ret"] > 0).mean() * 100.0) if total_trades else 0.0
total_pnl = float(df["profit_usdt"].sum())
avg_lev = float(df["leverage"].mean()) if total_trades else 1.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("총 청산 트레이드", f"{total_trades:,}건")
c2.metric("통합 승률", f"{win_rate:.1f}%")
c3.metric("누적 실현 손익", f"{total_pnl:+,.2f} USDT")
c4.metric("평균 레버리지", f"{avg_lev:.2f}x")

st.markdown("---")

st.subheader("📈 롱/숏 누적 복리 곡선 (레버리지 반영)")
fig = go.Figure()
for side, color in (("LONG", "#00CC96"), ("SHORT", "#EF553B")):
    sub = df[df["position_side"] == side].copy()
    if sub.empty:
        continue
    sub = sub.sort_values("exit_date")
    sub["cum_profit"] = sub["profit_usdt"].cumsum()
    fig.add_trace(
        go.Scatter(
            x=sub["exit_date"],
            y=sub["cum_profit"],
            mode="lines",
            name=f"{side} ({len(sub)})",
            line=dict(width=3, color=color),
        )
    )
fig.update_layout(template="plotly_dark", height=420, hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)

st.subheader("🌌 코인 4D DNA 우주 분포 (CPV-TB-BBE-RS)")
dna = df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"]).copy()
if dna.empty:
    st.info("DNA 시각화 데이터가 부족합니다.")
else:
    dna["DNA_Class"] = dna["final_ret"].apply(lambda x: "WIN" if x > 0 else "LOSS")
    fig3 = px.scatter_3d(
        dna,
        x="dyn_cpv",
        y="dyn_tb",
        z="v_energy",
        color="DNA_Class",
        symbol="position_side",
        size=dna["notional_usdt"].clip(lower=1.0),
        hover_name="symbol",
        hover_data=["timeframe", "final_ret", "dyn_rs", "leverage", "market_type"],
        title="X: CPV | Y: TB | Z: BBE | 색: 손익 | 심볼: 롱/숏 | 크기: 노셔널",
    )
    fig3.update_layout(template="plotly_dark", height=640)
    st.plotly_chart(fig3, use_container_width=True)

st.subheader("🧪 롱/숏 품질 요약")
rows = []
for side in ("LONG", "SHORT"):
    sub = df[df["position_side"] == side]
    if sub.empty:
        continue
    rows.append(
        {
            "side": side,
            "trades": int(len(sub)),
            "win_rate_%": round(float((sub["final_ret"] > 0).mean() * 100.0), 2),
            "pf": round(_pf(sub["final_ret"]), 3),
            "avg_ret_%": round(float(sub["final_ret"].mean()), 3),
            "pnl_usdt": round(float(sub["profit_usdt"].sum()), 3),
        }
    )
if rows:
    st.dataframe(pd.DataFrame(rows), use_container_width=True)
