import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os
import time
import random

# ==========================================
# 1. 환경 설정 및 DB 무결성 연결 (Read-Only)
# ==========================================
st.set_page_config(page_title="Dante Quant Factory Control Tower", layout="wide")
st.title("🚀 퀀트 팩토리 관제탑 실시간 대시보드")

DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')

def _max_drawdown_from_cum_pnl(cum: pd.Series) -> float:
    """누적 손익(원) 곡선에서 최대 낙폭(MDD), 음수 비율(0 ~ -100% 근사)."""
    if cum is None or len(cum) < 2:
        return 0.0
    s = pd.to_numeric(cum, errors="coerce").fillna(0.0)
    peak = s.cummax()
    dd = (s - peak) / peak.replace(0, float("nan"))
    m = dd.min()
    if pd.isna(m) or not np.isfinite(m):
        return 0.0
    return float(m) * 100.0


def _profit_factor_from_pnl(pnl: pd.Series) -> float:
    """Profit Factor = 총 이익 / 총 손실(절댓값). profit_amount 기준."""
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return 0.0
    gains = s[s > 0].sum()
    losses = s[s < 0].sum()
    if losses >= 0:
        return float(gains) if gains > 0 else 0.0
    return float(gains / abs(losses))


def _logic_group_series(sig_series: pd.Series) -> pd.Series:
    """벡터화된 로직군 라벨 (대용량에도 UI 스레드 부담 최소)."""
    s = sig_series.fillna("").astype(str)
    out = pd.Series("기타", index=sig_series.index, dtype=object)
    out = out.mask(s.str.contains("STANDARD", na=False), "A (오리지널)")
    out = out.mask(
        s.str.contains("SUPERNOVA_COSINE|SUPERNOVA_MLBOX", na=False, regex=True),
        "B (초신성)",
    )
    out = out.mask(s.str.contains("SUPERNOVA_BEAST", na=False), "C (야수/BEAST)")
    out = out.mask(s.str.contains("UNDERDOG", na=False), "UD (언더독/잡주)")
    return out


@st.cache_data(ttl=60)  # 60초마다 데이터 자동 갱신 (메인 DB 부하 방지)
def load_factory_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    last_err = None
    for attempt in range(12):
        try:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30.0, check_same_thread=False)
            try:
                conn.execute("PRAGMA query_only=ON;")
            except sqlite3.OperationalError:
                pass
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
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                time.sleep(min(2.0, 0.04 * (2 ** attempt) + random.uniform(0, 0.12)))
                continue
            break
        except Exception as e:
            last_err = e
            break
    if last_err is not None:
        st.session_state["_factory_load_err"] = str(last_err)
    return pd.DataFrame()

df_all = load_factory_data()

if df_all.empty:
    err = st.session_state.pop("_factory_load_err", None)
    if err:
        st.error(f"🚨 DB 연결 실패 또는 데이터 대기 중... ({err})")
    st.warning("⏳ 아직 청산된 데이터(CLOSED)가 충분하지 않습니다. 팩토리가 가동될 때까지 대기합니다.")
else:
    # ==========================================
    # 사이드바: 관측 기준일 (As-of) — DB 재조회 없이 메모리 필터만
    # ==========================================
    st.sidebar.markdown("### 📅 관측 기준일 (Lookback As-of)")
    st.sidebar.caption("선택한 날짜는 이미 로드된 표만 필터합니다. DB를 다시 읽지 않습니다.")
    if "exit_date" in df_all.columns and not df_all["exit_date"].isna().all():
        _ed = df_all["exit_date"].dt.normalize()
        d_min = _ed.min().date()
        d_max = _ed.max().date()
        as_of = st.sidebar.date_input(
            "청산일 기준 As-of (이 날짜 이전·당일 청산만 표시)",
            value=d_max,
            min_value=d_min,
            max_value=d_max,
            key="dashboard_as_of_date",
        )
        df = df_all.loc[df_all["exit_date"].dt.normalize() <= pd.Timestamp(as_of)].copy()
    else:
        df = df_all.copy()

    if df.empty:
        st.warning("⏳ 선택한 As-of 조건에 해당하는 청산 데이터가 없습니다. 날짜를 넓혀 주세요.")
    else:
        # ==========================================
        # 사이드바: 3D DNA 필터 (A/B/C/UD + 점수대)
        # ==========================================
        st.sidebar.markdown("### 🧬 3D DNA 뷰 필터")
        st.sidebar.caption("로직군·점수대를 바꾸면 산점도에 반영됩니다. (축: CPV, TB, BBE)")
        show_a = st.sidebar.checkbox("A (오리지널)", value=True, key="flt_a")
        show_b = st.sidebar.checkbox("B (초신성)", value=True, key="flt_b")
        show_c = st.sidebar.checkbox("C (야수/BEAST)", value=True, key="flt_c")
        show_ud = st.sidebar.checkbox("UD (언더독/잡주)", value=True, key="flt_ud")
        score_lo, score_hi = st.sidebar.slider(
            "total_score 범위 (0~100)",
            min_value=0,
            max_value=100,
            value=(0, 100),
            step=1,
        )

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

        valid_invest = (
            df["sim_kelly_invest"].replace(0, 400000)
            if "sim_kelly_invest" in df.columns
            else pd.Series([400000] * len(df), index=df.index)
        )

        df = df.copy()
        df["profit_amount"] = valid_invest * (df["final_ret"] / 100)
        pf_global = _profit_factor_from_pnl(df["profit_amount"])

        low_n = total_trades < 30
        wr_label = "⚠️ 시스템 통합 승률" if low_n else "시스템 통합 승률"
        pf_label = "⚠️ 시스템 통합 PF" if low_n else "시스템 통합 PF"

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("총 청산 종목 (데이터 셋)", f"{total_trades:,.0f}개")
        col2.metric(wr_label, f"{win_rate:.1f}%", help=f"표본 N={total_trades}")
        col3.metric(pf_label, f"{pf_global:,.2f}", help=f"표본 N={total_trades} · profit_amount 기준")
        col4.metric("가상 실현 누적 손익", f"{total_pnl:,.0f}원")
        col5.metric("대시보드 상태", "🟢 WAL 안전 연결됨")

        if low_n:
            st.markdown(
                '<p style="opacity:0.85;color:#ffb74d;">⚠️ 표본 수 N&lt;30 — 승률·PF는 통계적 불확실성이 큽니다. 해석에 유의하세요.</p>',
                unsafe_allow_html=True,
            )

        groups = {
            "A (오리지널)": df[df["sig_type"].str.contains("STANDARD", na=False)],
            "B (초신성)": df[
                df["sig_type"].str.contains("SUPERNOVA_COSINE|SUPERNOVA_MLBOX", na=False, regex=True)
            ],
            "C (야수/BEAST)": df[df["sig_type"].str.contains("SUPERNOVA_BEAST", na=False)],
            "UD (언더독/잡주)": df[df["sig_type"].str.contains("UNDERDOG", na=False)],
        }

        st.markdown("#### 📊 로직군별 고급 지표 (Profit Factor · MDD)")
        st.caption("PF·MDD는 청산 건의 `profit_amount`·`final_ret` 기반 누적 곡선으로 산출합니다.")
        gm_cols = st.columns(4)
        for i, (gname, gdf) in enumerate(groups.items()):
            with gm_cols[i % 4]:
                if gdf.empty:
                    st.metric(gname, "데이터 없음")
                    st.caption("승률: — (N=0) | PF: — | MDD: —")
                else:
                    gn = len(gdf)
                    g_wr = len(gdf[gdf["final_ret"] > 0]) / gn * 100 if gn else 0.0
                    pf = _profit_factor_from_pnl(gdf["profit_amount"])
                    gg = gdf.sort_values("exit_date").copy()
                    gg["cum_profit"] = gg["profit_amount"].cumsum()
                    mdd = _max_drawdown_from_cum_pnl(gg["cum_profit"])
                    unc = "⚠️ " if gn < 30 else ""
                    st.metric(gname, f"n={gn:,}")
                    st.caption(
                        f"{unc}승률: {g_wr:.1f}% (N={gn}) | PF: {pf:,.2f} | MDD: {mdd:.1f}%"
                    )

        st.markdown("---")

        # ==========================================
        # 3. ⚔️ A, B, C 평행우주 누적 복리 수익률 (Equity Curve)
        # ==========================================
        st.subheader("📈 평행우주 로직별 누적 복리 수익금 (Equity Curve)")

        fig_equity = go.Figure()

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
        # 3b. 섹터 × 로직군 교차 히트맵 (실현 손익·승률)
        # ==========================================
        st.subheader("🔲 섹터 × 로직군 교차 히트맵 (실현 손익)")
        st.caption("행: 섹터 · 열: A/B/C/UD · 색: 누적 profit_amount (As-of 필터 적용, DB 추가 조회 없음)")
        logic_labels = ["A (오리지널)", "B (초신성)", "C (야수/BEAST)", "UD (언더독/잡주)"]
        if "sector" not in df.columns:
            st.info("💡 `sector` 컬럼이 없어 교차 히트맵을 생략합니다.")
        else:
            hx = df.copy()
            hx["sector"] = hx["sector"].fillna("미분류").astype(str).replace("", "미분류")
            hx["로직군"] = _logic_group_series(hx["sig_type"])
            hx = hx[hx["로직군"].isin(logic_labels)]
            if hx.empty:
                st.info("💡 A/B/C/UD 로직에 해당하는 청산 건이 없어 히트맵을 생략합니다.")
            else:
                agg = (
                    hx.groupby(["sector", "로직군"], observed=False)
                    .agg(
                        pnl=("profit_amount", "sum"),
                        n=("profit_amount", "count"),
                        wins=("final_ret", lambda x: float((x > 0).sum())),
                    )
                    .reset_index()
                )
                agg["win_rate"] = np.where(agg["n"] > 0, agg["wins"] / agg["n"] * 100.0, 0.0)
                pivot_pnl = agg.pivot(index="sector", columns="로직군", values="pnl").reindex(
                    columns=logic_labels
                )
                pivot_pnl = pivot_pnl.fillna(0.0)
                pivot_n = agg.pivot(index="sector", columns="로직군", values="n").reindex(columns=logic_labels)
                pivot_n = pivot_n.fillna(0).astype(int)
                pivot_wr = agg.pivot(index="sector", columns="로직군", values="win_rate").reindex(
                    columns=logic_labels
                )
                pivot_wr = pivot_wr.fillna(0.0)

                text_z = np.array(
                    [
                        [
                            f"{p:,.0f}<br>n={int(n)}<br>승률 {wr:.0f}%"
                            for p, n, wr in zip(pivot_pnl.iloc[ri], pivot_n.iloc[ri], pivot_wr.iloc[ri])
                        ]
                        for ri in range(len(pivot_pnl.index))
                    ],
                    dtype=object,
                )

                fig_heat = go.Figure(
                    data=go.Heatmap(
                        z=pivot_pnl.values,
                        x=pivot_pnl.columns.tolist(),
                        y=pivot_pnl.index.tolist(),
                        text=text_z,
                        texttemplate="%{text}",
                        hovertemplate=(
                            "<b>%{y}</b> / %{x}<br>"
                            "누적손익: %{z:,.0f} 원<br>"
                            "<extra></extra>"
                        ),
                        colorscale="RdYlGn",
                        zmid=0,
                    )
                )
                fig_heat.update_layout(
                    height=max(360, min(900, 28 * len(pivot_pnl.index) + 120)),
                    template="plotly_dark",
                    xaxis_title="로직군",
                    yaxis_title="섹터",
                    margin=dict(l=8, r=8, t=40, b=8),
                )
                st.plotly_chart(fig_heat, use_container_width=True)

        st.markdown("---")

        # ==========================================
        # 4. 🧬 언더독(50점 이하) 대박주 vs 참사주 3D 산점도 (DNA)
        # ==========================================
        st.subheader("🌌 언더독(0~60점대) DNA 3차원 우주 분포 (CPV-TB-BBE)")
        st.markdown(
            "*사이드바에서 A/B/C/UD 및 점수 범위를 조절하면 클러스터가 실시간으로 갱신됩니다.*"
        )

        if not (show_a or show_b or show_c or show_ud):
            st.warning("💡 사이드바에서 로직군(A/B/C/UD)을 최소 1개 이상 켜 주세요.")
        else:
            mask_a = df["sig_type"].str.contains("STANDARD", na=False)
            mask_b = df["sig_type"].str.contains("SUPERNOVA_COSINE|SUPERNOVA_MLBOX", na=False, regex=True)
            mask_c = df["sig_type"].str.contains("SUPERNOVA_BEAST", na=False)
            mask_ud = df["sig_type"].str.contains("UNDERDOG", na=False)

            logic_mask = (
                (show_a & mask_a)
                | (show_b & mask_b)
                | (show_c & mask_c)
                | (show_ud & mask_ud)
            )

            plot_df = df.loc[logic_mask].copy()
            if "total_score" in plot_df.columns:
                plot_df = plot_df[
                    (plot_df["total_score"] >= score_lo) & (plot_df["total_score"] <= score_hi)
                ]

            if (
                not plot_df.empty
                and "dyn_cpv" in plot_df.columns
                and "dyn_tb" in plot_df.columns
                and "v_energy" in plot_df.columns
            ):
                plot_df = plot_df.dropna(subset=["dyn_cpv", "dyn_tb", "v_energy", "final_ret"])

                if plot_df.empty:
                    st.info("💡 선택한 필터 조건에서 유효한 3D DNA 데이터가 없어 차트를 생략합니다.")
                else:
                    def classify_result(ret):
                        if ret >= 10:
                            return "🔥 대박주 (10%+)"
                        if ret > 0:
                            return "👍 일반수익"
                        if ret <= -5:
                            return "💀 참사주 (-5% 이하)"
                        return "📉 일반손실"

                    plot_df["DNA_Class"] = plot_df["final_ret"].apply(classify_result)

                    hover_cfg = ["final_ret", "total_score", "sig_type"]
                    hover_cfg = [c for c in hover_cfg if c in plot_df.columns]

                    scatter_kw = dict(
                        data_frame=plot_df,
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
                    if "name" in plot_df.columns:
                        scatter_kw["hover_name"] = "name"
                    if hover_cfg:
                        scatter_kw["hover_data"] = hover_cfg

                    fig_3d = px.scatter_3d(**scatter_kw)

                    fig_3d.update_layout(height=600, template="plotly_dark")
                    st.plotly_chart(fig_3d, use_container_width=True)
            elif plot_df.empty:
                st.info("💡 필터 조건에 맞는 청산 건이 없습니다. 점수 범위·로직군을 넓혀 보세요.")
            else:
                st.info("💡 아직 60점 이하 언더독 종목의 3D 분석에 필요한 충분한 데이터가 모이지 않았습니다.")
