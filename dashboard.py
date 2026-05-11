import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os

# ==========================================
# 1. 환경 설정 및 DB 무결성 연결 (Read-Only)
# ==========================================
st.set_page_config(page_title="Dante Quant Factory Control Tower", layout="wide")
st.title("🚀 퀀트 팩토리 관제탑 실시간 대시보드")

DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')

@st.cache_data(ttl=60)  # 60초마다 데이터 자동 갱신 (메인 DB 부하 방지)
def load_factory_data():
    if not os.path.exists(DB_PATH):
        st.error(f"🚨 DB 파일을 찾을 수 없습니다: {DB_PATH}")
        return pd.DataFrame()
    try:
        # uri=True와 mode=ro를 통해 읽기 전용 강제 (메인 봇 쓰기와 충돌 방지)
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False)
        # WAL은 보통 쓰기 측에서 이미 켜져 있음. RO 연결에서 변경이 막힐 수 있어 실패 시 무시.
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.OperationalError:
            pass

        query = "SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%'"
        df = pd.read_sql(query, conn)
        conn.close()

        if not df.empty and "exit_date" in df.columns:
            df["exit_date"] = pd.to_datetime(df["exit_date"])
            df = df.sort_values("exit_date")
        return df
    except Exception as e:
        st.error(f"🚨 DB 연결 실패 또는 데이터 대기 중... ({e})")
        return pd.DataFrame()

df = load_factory_data()

if df.empty:
    st.warning("⏳ 아직 청산된 데이터(CLOSED)가 충분하지 않습니다. 팩토리가 가동될 때까지 대기합니다.")
else:
    # ==========================================
    # 2. 상단 지표 섹션 (Global Metrics)
    # ==========================================
    total_trades = len(df)
    win_rate = len(df[df["final_ret"] > 0]) / total_trades * 100 if total_trades > 0 else 0
    total_pnl = (
        (df["sim_kelly_invest"] * (df["final_ret"] / 100)).sum()
        if "sim_kelly_invest" in df.columns
        else 0
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 청산 종목 (데이터 셋)", f"{total_trades:,.0f}개")
    col2.metric("시스템 통합 승률", f"{win_rate:.1f}%")
    col3.metric("가상 실현 누적 손익", f"{total_pnl:,.0f}원")
    col4.metric("대시보드 상태", "🟢 WAL 안전 연결됨")

    st.markdown("---")

    # ==========================================
    # 3. ⚔️ A, B, C 평행우주 누적 복리 수익률 (Equity Curve)
    # ==========================================
    st.subheader("📈 평행우주 로직별 누적 복리 수익금 (Equity Curve)")

    valid_invest = (
        df["sim_kelly_invest"].replace(0, 400000)
        if "sim_kelly_invest" in df.columns
        else pd.Series([400000] * len(df), index=df.index)
    )

    df = df.copy()
    df["profit_amount"] = valid_invest * (df["final_ret"] / 100)

    fig_equity = go.Figure()

    groups = {
        "A (오리지널)": df[df["sig_type"].str.contains("STANDARD", na=False)],
        "B (초신성)": df[
            df["sig_type"].str.contains("SUPERNOVA_COSINE|SUPERNOVA_MLBOX", na=False, regex=True)
        ],
        "C (야수/BEAST)": df[df["sig_type"].str.contains("SUPERNOVA_BEAST", na=False)],
        "UD (언더독/잡주)": df[df["sig_type"].str.contains("UNDERDOG", na=False)],
    }

    colors = ["gold", "cyan", "magenta", "lime"]

    for (name, group_df), color in zip(groups.items(), colors):
        if group_df.empty:
            continue
        g = group_df.sort_values("exit_date").copy()
        g["cum_profit"] = g["profit_amount"].cumsum()
        fig_equity.add_trace(
            go.Scatter(
                x=g["exit_date"],
                y=g["cum_profit"],
                mode="lines",
                name=name,
                line=dict(color=color, width=3),
            )
        )

    fig_equity.update_layout(height=400, template="plotly_dark", hovermode="x unified")
    st.plotly_chart(fig_equity, use_container_width=True)

    st.markdown("---")

    # ==========================================
    # 4. 🧬 언더독(50점 이하) 대박주 vs 참사주 3D 산점도 (DNA)
    # ==========================================
    st.subheader("🌌 언더독(0~60점대) DNA 3차원 우주 분포 (CPV-TB-BBE)")

    ud_df = df[df["total_score"] <= 60].copy() if "total_score" in df.columns else pd.DataFrame()

    if (
        not ud_df.empty
        and "dyn_cpv" in ud_df.columns
        and "dyn_tb" in ud_df.columns
        and "v_energy" in ud_df.columns
    ):
        # 3D 렌더링 폭파 방지용 썩은 데이터(NaN) 도려내기
        ud_df = ud_df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "final_ret"])
        
        if ud_df.empty:
            st.info("💡 유효한 3D 데이터가 확보되지 않아 차트 렌더링을 일시 생략합니다.")
        else:
            def classify_result(ret):
                if ret >= 10:
                    return "🔥 대박주 (10%+)"
                if ret > 0:
                    return "👍 일반수익"
                if ret <= -5:
                    return "💀 참사주 (-5% 이하)"
                return "📉 일반손실"

            ud_df["DNA_Class"] = ud_df["final_ret"].apply(classify_result)

            hover_cfg = ["final_ret", "total_score", "sig_type"]
            hover_cfg = [c for c in hover_cfg if c in ud_df.columns]

            scatter_kw = dict(
                data_frame=ud_df,
                x="dyn_cpv",
                y="dyn_tb",
                z="v_energy",
                color="DNA_Class",
                color_discrete_map={
                    "🔥 대박주 (10%+)": "#00FF00",
                    "👍 일반수익": "#008000",
                    "💀 참사주 (-5% 이하)": "#FF0000",
                    "📉 일반손실": "#808080",
                },
                title="X축: 윗꼬리 방어(CPV) | Y축: 진짜양봉(TB) | Z축: 응축에너지(BBE)",
                opacity=0.8,
            )
            if "name" in ud_df.columns:
                scatter_kw["hover_name"] = "name"
            if hover_cfg:
                scatter_kw["hover_data"] = hover_cfg

            fig_3d = px.scatter_3d(**scatter_kw)

            fig_3d.update_layout(height=600, template="plotly_dark")
            st.plotly_chart(fig_3d, use_container_width=True)
    else:
        st.info("💡 아직 60점 이하 언더독 종목의 3D 분석에 필요한 충분한 데이터가 모이지 않았습니다.")
