"""
Dante Quant Factory — KOSPI/KOSDAQ 실데이터 섹터 히트맵 (War Room).
- market_data.sqlite: 읽기 전용(uri=ro)으로 OPEN 보유 종목만 조회·오버레이
- 시장 전체: pykrx 등락률·시총·거래대금 + FDR 리스트로 섹터(업종) 병합
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

from market_db_paths import market_db_read_path

# 페이지 기본 설정
st.set_page_config(page_title="Dante Quant Sector Heatmap", layout="wide")
st.title("🔥 KOSPI/KOSDAQ 실시간 섹터 자금 흐름 히트맵")
st.markdown("시장의 피(자금)가 어디로 쏠리고 있는지 우주에서 내려다봅니다. **실제 등락률·거래대금·시총** 기준입니다.")


def _pick_col(df: pd.DataFrame, *needles: str) -> Optional[str]:
    for c in df.columns:
        s = str(c)
        if all(n in s for n in needles):
            return s
    return None


def _coerce_float_series(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        s = s.astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _krx_last_two_trading_days() -> Tuple[Optional[str], Optional[str]]:
    """삼성전자 OHLCV로 최근 두 영업일(등락률 구간)."""
    try:
        from pykrx import stock as krx
    except ImportError:
        return None, None
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=21)).strftime("%Y%m%d")
    try:
        o = krx.get_market_ohlcv(start, end, "005930")
    except Exception:
        return None, None
    if o is None or len(o) < 2:
        return None, None
    idx = list(o.index)
    d1 = pd.Timestamp(idx[-2]).strftime("%Y%m%d")
    d2 = pd.Timestamp(idx[-1]).strftime("%Y%m%d")
    return d1, d2


def _fetch_market_block_pykrx(market: str, d_from: str, d_to: str) -> pd.DataFrame:
    from pykrx import stock as krx

    chg = krx.get_market_price_change_by_ticker(d_from, d_to, market)
    if chg is None or chg.empty:
        return pd.DataFrame()

    chg = chg.copy()
    chg.index = chg.index.astype(str).str.zfill(6)

    cap = None
    try:
        cap = krx.get_market_cap(d_to, market=market)
    except TypeError:
        try:
            cap = krx.get_market_cap(d_to)
        except Exception:
            cap = None
    except Exception:
        try:
            cap = krx.get_market_cap(d_to)
        except Exception:
            cap = None

    if cap is not None and not cap.empty:
        cap = cap.copy()
        cap.index = cap.index.astype(str).str.zfill(6)
        cap = cap.reindex(chg.index)
        cap = cap.rename(columns={c: f"cap__{c}" for c in cap.columns})
        out = chg.join(cap, how="left")
    else:
        out = chg

    pct_col = _pick_col(out, "등락률") or _pick_col(out, "등락")
    tv_col = _pick_col(out, "거래", "대금") or _pick_col(out, "거래대금")
    if tv_col is None:
        for c in out.columns:
            if str(c).startswith("cap__") and "거래" in str(c) and "대금" in str(c):
                tv_col = c
                break
    mcap_col = _pick_col(out, "시가총액")
    if mcap_col is None:
        for c in out.columns:
            if str(c).startswith("cap__") and "시가총액" in str(c):
                mcap_col = c
                break

    name_col = _pick_col(out, "종목") or _pick_col(out, "종목명")
    if not name_col or name_col not in out.columns:
        name_series = pd.Series(out.index.astype(str), index=out.index)
    else:
        name_series = out[name_col].astype(str)

    rows = pd.DataFrame(
        {
            "code": out.index.astype(str).str.zfill(6),
            "name": name_series,
            "market": market,
            "chg_pct": _coerce_float_series(out[pct_col]) if pct_col else pd.Series(0.0, index=out.index),
            "trade_value": _coerce_float_series(out[tv_col]) if tv_col else pd.Series(0.0, index=out.index),
            "mcap": _coerce_float_series(out[mcap_col]) if mcap_col else pd.Series(0.0, index=out.index),
        }
    )
    rows["trade_value"] = rows["trade_value"].fillna(0.0).clip(lower=0.0)
    rows["mcap"] = rows["mcap"].fillna(0.0).clip(lower=0.0)
    rows["size_value"] = rows["trade_value"].where(rows["trade_value"] > 0, rows["mcap"] * 1e-6)
    rows["size_value"] = rows["size_value"].replace(0, pd.NA).fillna(rows["mcap"] * 1e-6)
    rows["size_value"] = rows["size_value"].clip(lower=1.0)
    rows["chg_pct"] = rows["chg_pct"].fillna(0.0)
    return rows


def _listing_sector_map(market: str) -> pd.DataFrame:
    try:
        import FinanceDataReader as fdr
    except ImportError:
        return pd.DataFrame(columns=["code", "sector_raw", "market"])

    tag = "KOSPI" if market == "KOSPI" else "KOSDAQ"
    try:
        lst = fdr.StockListing(tag)
    except Exception:
        return pd.DataFrame(columns=["code", "sector_raw", "market"])

    code_col = "Code" if "Code" in lst.columns else ("Symbol" if "Symbol" in lst.columns else None)
    if code_col is None:
        return pd.DataFrame(columns=["code", "sector_raw", "market"])

    sec_col = None
    for c in ("Industry", "업종", "Sector", "sector", "분류"):
        if c in lst.columns:
            sec_col = c
            break
    if sec_col is None:
        sec_col = lst.columns[min(3, len(lst.columns) - 1)]

    out = pd.DataFrame(
        {
            "code": lst[code_col].astype(str).str.zfill(6),
            "sector_raw": lst[sec_col].astype(str).fillna("기타"),
            "market": market,
        }
    )
    return out


@st.cache_data(ttl=120, show_spinner=True)
def load_kr_market_treemap_frame() -> Tuple[pd.DataFrame, str, str, str]:
    """
    KOSPI+KOSDAQ 전 종목 실데이터 프레임, (d_from, d_to, err_msg).
    err_msg 비어 있으면 성공.
    """
    d_from, d_to = _krx_last_two_trading_days()
    if not d_from or not d_to:
        return pd.DataFrame(), "", "", "pykrx 미설치이거나 KRX OHLCV 조회에 실패했습니다. `pip install pykrx` 후 장중/마감 데이터를 확인하세요."

    parts = []
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            block = _fetch_market_block_pykrx(mkt, d_from, d_to)
        except Exception as e:
            return pd.DataFrame(), d_from, d_to, f"{mkt} 시세 조회 실패: {e}"
        if block.empty:
            continue
        smap = _listing_sector_map(mkt)
        if not smap.empty:
            block = block.merge(smap[["code", "sector_raw"]], on="code", how="left")
        else:
            block["sector_raw"] = "기타"
        block["sector_raw"] = block["sector_raw"].fillna("기타").replace("", "기타")
        parts.append(block)

    if not parts:
        return pd.DataFrame(), d_from, d_to, "시장 전체 데이터가 비어 있습니다."

    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["code"], keep="last")
    return df, d_from, d_to, ""


@st.cache_data(ttl=60, show_spinner=True)
def load_factory_open_codes() -> Tuple[set, str]:
    """
    팩토리 보유(KR) 종목코드 집합. DB 없으면 빈 set.
    읽기 전용 uri=ro, query_only=ON.
    """
    codes: set = set()
    err = ""
    db_path = market_db_read_path()
    if not os.path.exists(db_path):
        return codes, "no_db"

    for attempt in range(8):
        try:
            conn = sqlite3.connect(
                f"file:{db_path.replace(os.sep, '/')}?mode=ro",
                uri=True,
                timeout=30.0,
                check_same_thread=False,
            )
            try:
                conn.execute("PRAGMA query_only=ON;")
            except sqlite3.OperationalError:
                pass
            q = """
                SELECT DISTINCT code, market
                FROM forward_trades
                WHERE status = 'OPEN'
            """
            df = pd.read_sql(q, conn)
            conn.close()
            if df is None or df.empty:
                return codes, ""
            for _, r in df.iterrows():
                m = str(r.get("market") or "").strip().upper()
                if m in ("US", "GLOBAL"):
                    continue
                c = str(r.get("code") or "").strip().zfill(6)
                if len(c) == 6 and c.isdigit():
                    codes.add(c)
            return codes, ""
        except sqlite3.OperationalError as e:
            err = str(e)
            msg = err.lower()
            if "locked" in msg or "busy" in msg:
                time.sleep(0.06 * (attempt + 1))
                continue
            return codes, err
        except Exception as e:
            return codes, str(e)

    return codes, err or "DB busy"


market_df, d_from, d_to, krx_err = load_kr_market_treemap_frame()
open_codes, db_err = load_factory_open_codes()

if krx_err:
    st.error(krx_err)
    st.stop()

if market_df.empty:
    st.warning("시장 데이터가 비어 있습니다. 장 개장일·네트워크·pykrx를 확인하세요.")
    st.stop()

if db_err and db_err != "no_db":
    st.caption(f"⚠️ OPEN 포지션 DB 읽기 경고: {db_err}")

market_df["is_factory"] = market_df["code"].isin(open_codes)
market_df["label_leaf"] = market_df.apply(
    lambda r: (("🌟 " if r["is_factory"] else "") + str(r["name"]) + f" ({r['code']})"),
    axis=1,
)

market_df["hover_tip"] = market_df.apply(
    lambda r: (
        ("🌟 [팩토리 OPEN 스나이퍼 보유] " if r["is_factory"] else "")
        + f"{r['name']} ({r['code']}) | 등락 {r['chg_pct']:.2f}% | "
        f"거래대금 {r['trade_value']:,.0f}원 | 시총 {r['mcap']:,.0f}원"
    ),
    axis=1,
)

fig = px.treemap(
    market_df,
    path=[px.Constant("한국 증시 (실시간)"), "market", "sector_raw", "label_leaf"],
    values="size_value",
    color="chg_pct",
    color_continuous_scale="RdYlGn",
    color_continuous_midpoint=0.0,
    hover_name="hover_tip",
)

fig.update_traces(
    texttemplate="<b>%{label}</b>",
    textposition="middle center",
)

fig.update_layout(
    height=800,
    margin=dict(t=30, l=10, r=10, b=10),
    template="plotly_dark",
    coloraxis_colorbar=dict(title="등락률 (%)"),
)

st.success(
    f"📡 **실데이터** pykrx 구간 `{d_from}` → `{d_to}` | 트리맵 크기: 거래대금(없으면 시총 스케일) | 색: 등락률"
)
if open_codes:
    st.info(f"🌟 **팩토리 OPEN** {len(open_codes)}종 — 트리맵 라벨에 별 표시")
else:
    st.caption("현재 `forward_trades`에 OPEN KR 포지션이 없습니다. 시장 전체 히트맵만 표시합니다.")

st.plotly_chart(fig, use_container_width=True)

st.info(
    "💡 **구조:** 한국 증시 → 시장(KOSPI/KOSDAQ) → 업종(FinanceDataReader 리스트) → 종목. "
    "보유 중인 종목은 라벨 앞에 🌟이 붙고 호버에 강조 문구가 포함됩니다. "
    "DB는 `mode=ro` 읽기 전용만 사용합니다."
)
