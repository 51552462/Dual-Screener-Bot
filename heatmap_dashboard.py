import streamlit as st
import time
import random
import FinanceDataReader as fdr
import plotly.express as px
import pandas as pd
import numpy as np

# 페이지 기본 설정
st.set_page_config(page_title="Dante Quant Sector Heatmap", layout="wide")
st.title("🔥 KOSPI/KOSDAQ 실시간 섹터 자금 흐름 히트맵")
st.markdown("시장의 피(자금)가 어디로 쏠리고 있는지 우주에서 내려다봅니다.")

@st.cache_data(ttl=300)  # 5분(300초)마다 데이터 갱신
def load_market_map():
    try:
        # KRX 상장종목 전체 리스트 호출 (섹터 정보 포함)
        df = fdr.StockListing('KRX')
        time.sleep(random.uniform(0.3, 0.7))

        # 'Sector'(산업군) 데이터가 있는 종목만 필터링
        df = df[df['Sector'].notna() & (df['Sector'] != '')].copy()

        # 💡 API 과부하 방지를 위해 시가총액/거래대금 상위 300개 주요 종목만 샘플링
        # 실제 환경에서는 증권사 API 실시간 등락률 연동. 여기서는 시각화 UI 구현용
        df = df.head(300).copy()

        # 임시 실시간 등락률 시뮬레이션 (API 연결 전 UI 테스트용: -10% ~ +10%)
        # [주의] 이 부분은 추후 실시간 현재가 API(Kiwoom/PyKis 등)로 교체해야 완벽해집니다.
        np.random.seed(42)  # UI 데모용 고정 시드 (실가동시 제거)
        df['ChangeRate'] = np.random.uniform(-10.0, 10.0, size=len(df))

        # 시가총액(가중치) 시뮬레이션
        df['MarketCap_Mock'] = np.random.randint(1000, 50000, size=len(df))

        return df
    except Exception as e:
        st.error(f"데이터 로드 에러: {e}")
        return pd.DataFrame()

market_df = load_market_map()

if not market_df.empty:
    # --- 전처리 ---
    # 색상 지정을 위해 -범위는 파란색, +범위는 빨간색으로 매핑
    market_df['Color_Score'] = market_df['ChangeRate']

    # 등락률 텍스트 포맷팅
    market_df['Label'] = (
        market_df['Name'].astype(str)
        + "<br>"
        + market_df['ChangeRate'].round(2).astype(str)
        + "%"
    )

    # --- Plotly Treemap (히트맵 바둑판) 그리기 ---
    fig = px.treemap(
        market_df,
        path=[px.Constant("한국 증시 (KRX)"), 'Sector', 'Name'],
        values='MarketCap_Mock',  # 박스 크기 (시가총액 기준)
        color='Color_Score',      # 박스 색상 (등락률 기준)
        color_continuous_scale='RdBu_r',  # Red(상승) - White(보합) - Blue(하락)
        color_continuous_midpoint=0,      # 0%가 흰색이 되도록 기준점 설정
        hover_name='Label',
        custom_data=['ChangeRate', 'Sector']
    )

    fig.update_traces(
        texttemplate="<b>%{label}</b><br>%{customdata[0]:.2f}%",
        textposition="middle center",
        hovertemplate='<b>종목명:</b> %{label}<br><b>섹터:</b> %{customdata[1]}<br><b>등락률:</b> %{customdata[0]:.2f}%'
    )

    fig.update_layout(
        height=800,
        margin=dict(t=30, l=10, r=10, b=10),
        template="plotly_dark",
        coloraxis_colorbar=dict(title="등락률 (%)")
    )

    st.plotly_chart(fig, use_container_width=True)

    st.info("💡 박스의 크기는 종목의 시가총액(영향력)을 의미하며, 색상은 붉을수록 상승(자금 유입), 푸를수록 하락(자금 유출)을 의미합니다.")
else:
    st.warning("데이터를 불러오는 중입니다...")
