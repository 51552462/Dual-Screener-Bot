"""
Dante Quant Factory — 스마트머니 레이더 (SQLite 미사용, system_config.json 만).

SSOT(단일 진실원): 본 스크립트 산출물 `SMART_MONEY_RADAR.picks`[*].avg_price 만이
스나이퍼/가상매매 교차검증의 스마트머니 평단으로 사용됨. smart_money_targets.json 등 실험 트랙 미사용.

- 외국인·기관 실순매수: pykrx 순매수 상위(소수 요청) 우선, 실패 시 네이버 순매수 iframe.
- 2천 종 루프 금지: 상위 랭킹만 수집 후 다이버전스(최근 5일 가격 +1.8% 이하) 필터.
- avg_price: (다일) pykrx 일별 외인·기관 양수 순매수량 + 종가 시계열에 대해 1차원 상태공간 칼만(잠재 평균단가),
  관측잡음 R_t를 일별 순매수 규모에 반비례(대량 매수일 저분산). 집계-only 경로는 기존 순매수 VWAP.
- SMART_MONEY_RADAR.picks 스키마 고정: name, avg_price, divergence_score.
"""
import json
import math
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from factory_data_paths import system_config_json_path

CONFIG_PATH = system_config_json_path()

try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

# 네이버 순매수 랭킹 iframe (소수 요청으로 상위 종목만 확보)
NAVER_DEAL_RANK_URL = "https://finance.naver.com/sise/sise_deal_rank_iframe.naver"
NAVER_GUBUN_FOREIGN = "9000"
NAVER_GUBUN_INSTITUTION = "1000"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

try:
    from pykrx import stock as krx_stock
except ImportError:
    krx_stock = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None


def load_config(max_retries=5):
    """
    [장갑차 로직] JSONDecodeError 및 파일 잠금(Lock) 방어막 적용
    """
    if not os.path.exists(CONFIG_PATH):
        return {}

    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 읽기 최종 실패 (동시 쓰기 과부하): {e}")
                return {}
    return {}


def save_config(config, max_retries=5):
    """
    [장갑차 로직] 임시 파일 원자적(Atomic) 덮어쓰기 및 권한 방어막 적용
    """
    temp_path = f"{CONFIG_PATH}.temp"
    cfg_dir = os.path.dirname(CONFIG_PATH)
    if cfg_dir:
        try:
            os.makedirs(cfg_dir, exist_ok=True)
        except OSError:
            pass
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 쓰기 최종 실패: {e}")
        except Exception as e:
            print(f"⚠️ 설정 파일 원자적 저장 중 알 수 없는 에러: {e}")
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False


def _sleep_jitter():
    time.sleep(random.uniform(0.35, 0.85))


def _calendar_weekday_dates_yyyymmdd(n: int) -> List[str]:
    d0 = datetime.now()
    out: List[str] = []
    cur = d0
    while len(out) < n * 2 and (d0 - cur).days < 45:
        if cur.weekday() < 5:
            out.append(cur.strftime("%Y%m%d"))
        cur -= timedelta(days=1)
    return list(reversed(out[-n:]))


def _recent_trade_dates_yyyymmdd(n: int = 10) -> List[str]:
    """삼성전자 OHLCV 인덱스로 최근 영업일 목록 확보 (캘린더 추정보다 안전)."""
    if krx_stock is None:
        return _calendar_weekday_dates_yyyymmdd(n)

    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=35)).strftime("%Y%m%d")
    try:
        ohlcv = krx_stock.get_market_ohlcv(start, end, "005930")
        _sleep_jitter()
    except Exception:
        ohlcv = pd.DataFrame()

    if ohlcv is None or ohlcv.empty:
        return _calendar_weekday_dates_yyyymmdd(n)

    idx = [pd.Timestamp(x).strftime("%Y%m%d") for x in ohlcv.index]
    return idx[-n:]


def _krx_col(df: pd.DataFrame, *needles: str) -> Optional[str]:
    for c in df.columns:
        s = str(c)
        if all(n in s for n in needles):
            return c
    return None


def _try_pykrx_flow_leaderboard(
    from_ymd: str, to_ymd: str, per_market_head: int = 120
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """
    KRX 투자자별 순매수 상위(기간 합산) → 종목별 외인+기관 순매수 거래대금·거래량 합산.
    성공 시 (acc, True), 실패/빈데이터 시 ({}, False).
    """
    if krx_stock is None:
        return {}, False

    acc: Dict[str, Dict[str, Any]] = {}

    for market in ("KOSPI", "KOSDAQ"):
        for investor in ("외국인", "기관합계"):
            try:
                df = krx_stock.get_market_net_purchases_of_equities(
                    from_ymd, to_ymd, market, investor
                )
            except Exception:
                df = pd.DataFrame()
            _sleep_jitter()
            if df is None or df.empty:
                continue

            name_col = _krx_col(df, "종목") or (df.columns[0] if len(df.columns) else None)
            krw_c = _krx_col(df, "순매수", "대금") or "순매수거래대금"
            vol_c = _krx_col(df, "순매수", "거래량") or "순매수거래량"

            sub = df.head(per_market_head)
            for code, row in sub.iterrows():
                code_s = str(code).zfill(6)
                krw = float(row.get(krw_c, 0) or 0)
                vol = float(row.get(vol_c, 0) or 0)
                if krw <= 0 and vol <= 0:
                    continue
                slot = acc.setdefault(
                    code_s,
                    {"name": str(row.get(name_col, "") or "").strip(), "krw": 0.0, "vol": 0.0},
                )
                if not slot["name"] and name_col:
                    slot["name"] = str(row.get(name_col, "") or "").strip()
                slot["krw"] += krw
                slot["vol"] += vol

    return acc, bool(acc)


def _parse_naver_deal_blocks(html: str) -> List[List[Tuple[str, str, float, float]]]:
    """
    iframe HTML에서 테이블 블록별 (종목코드, 종목명, 수량천주, 금액백만원) 행 목록.
    """
    if BeautifulSoup is None:
        print("⚠️ BeautifulSoup4 미설치: pip install beautifulsoup4 (네이버 폴백용)")
        return []

    soup = BeautifulSoup(html, "html.parser")
    blocks: List[List[Tuple[str, str, float, float]]] = []

    for table in soup.find_all("table"):
        rows_out: List[Tuple[str, str, float, float]] = []
        for tr in table.find_all("tr"):
            a = tr.find("a", href=re.compile(r"code=\d{6}"))
            if not a or not a.get("href"):
                continue
            m = re.search(r"code=(\d{6})", a["href"])
            if not m:
                continue
            code = m.group(1)
            name = a.get_text(strip=True)
            tds = [td.get_text(strip=True).replace(",", "") for td in tr.find_all("td")]
            nums: List[float] = []
            for t in tds:
                try:
                    if t and re.fullmatch(r"-?\d+", t):
                        nums.append(float(t))
                except ValueError:
                    continue
            if len(nums) < 2:
                continue
            qty_k, amt_mil = nums[0], nums[1]
            rows_out.append((code, name, qty_k, amt_mil))
        if rows_out:
            blocks.append(rows_out)

    return blocks


def _fetch_naver_rank_for_date(date_yyyymmdd: str, investor_gubun: str) -> List[Tuple[str, str, float, float]]:
    try:
        r = requests.get(
            NAVER_DEAL_RANK_URL,
            params={"investor_gubun": investor_gubun, "type": "buy", "date": date_yyyymmdd},
            headers=HTTP_HEADERS,
            timeout=20,
        )
        r.encoding = "euc-kr"
    except requests.RequestException as e:
        print(f"⚠️ 네이버 순매수 요청 실패 ({investor_gubun}, {date_yyyymmdd}): {e}")
        return []

    blocks = _parse_naver_deal_blocks(r.text)
    if not blocks:
        return []
    return blocks[-1]


def _naver_aggregate_flow(
    trade_dates: List[str], max_days: int = 6, top_per_day: int = 35
) -> Dict[str, Dict[str, Any]]:
    """
    최근 영업일별로 외국인(9000)+기관(1000) 순매수 상위만 수집·합산 (요청 수: 일수×2).
    수량: 천주, 금액: 백만원 → 주수 = qty_k*1000, 원 = amt_mil*1e6.
    """
    acc: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"name": "", "krw": 0.0, "vol": 0.0, "days": 0}
    )

    use_dates = trade_dates[-max_days:] if len(trade_dates) > max_days else trade_dates

    for d in use_dates:
        day_hits: Dict[str, int] = defaultdict(int)
        for gubun in (NAVER_GUBUN_FOREIGN, NAVER_GUBUN_INSTITUTION):
            rows = _fetch_naver_rank_for_date(d, gubun)
            _sleep_jitter()
            for code, name, qty_k, amt_mil in rows[:top_per_day]:
                if qty_k <= 0 or amt_mil <= 0:
                    continue
                sh = qty_k * 1000.0
                krw = amt_mil * 1_000_000.0
                slot = acc[code]
                if not slot["name"]:
                    slot["name"] = name
                slot["krw"] += krw
                slot["vol"] += sh
                day_hits[code] += 1
            _sleep_jitter()

        for c in day_hits:
            acc[c]["days"] += 1

    return dict(acc)


def _flow_vwap(slot: Dict[str, Any]) -> float:
    v = float(slot.get("vol") or 0)
    k = float(slot.get("krw") or 0)
    if v > 0 and k > 0:
        return k / v
    return 0.0


def _ohlcv_start_end_str(from_ymd: str, to_ymd: str) -> Tuple[str, str]:
    d_from = datetime.strptime(from_ymd, "%Y%m%d")
    d_to = datetime.strptime(to_ymd, "%Y%m%d")
    start = (d_from - timedelta(days=5)).strftime("%Y-%m-%d")
    end = d_to.strftime("%Y-%m-%d")
    return start, end


def _close_column(df: pd.DataFrame) -> Optional[str]:
    for c in ("종가", "Close", "close"):
        if c in df.columns:
            return str(c)
    return None


def fetch_ohlcv_for_divergence(code: str, from_ymd: str, to_ymd: str) -> pd.DataFrame:
    """
    다이버전스·폴백 종가용 OHLCV. pykrx 우선, 없거나 실패 시 FinanceDataReader(가격만).
    """
    if krx_stock is not None:
        try:
            df = krx_stock.get_market_ohlcv(from_ymd, to_ymd, code)
            _sleep_jitter()
            if df is not None and len(df) >= 6:
                return df
        except Exception:
            pass
    if fdr is not None:
        try:
            start_s, end_s = _ohlcv_start_end_str(from_ymd, to_ymd)
            df = fdr.DataReader(code, start_s, end_s)
            _sleep_jitter()
            if df is not None and len(df) >= 6:
                return df
        except Exception:
            pass
    return pd.DataFrame()


def _pykrx_daily_flow_points(
    code: str, from_ymd: str, to_ymd: str
) -> Optional[List[Tuple[float, float]]]:
    """
    pykrx 일별 투자자 순매수 + OHLCV에서 (종가, 외인+기관 양수 순매수량 합) 시계열.
    기존 `_try_pykrx_daily_smart_vwap`와 동일한 API 호출·컬럼 선택·루프 조건.
    """
    if krx_stock is None:
        return None
    try:
        dv = krx_stock.get_market_trading_volume_by_date(from_ymd, to_ymd, code)
        _sleep_jitter()
        px = krx_stock.get_market_ohlcv(from_ymd, to_ymd, code)
        _sleep_jitter()
    except Exception:
        return None

    if dv is None or dv.empty or px is None or px.empty:
        return None

    def pick_col(frame: pd.DataFrame, label: str) -> Optional[str]:
        for c in frame.columns:
            if label in str(c):
                return str(c)
        return None

    c_inst = pick_col(dv, "기관합계")
    c_fr = pick_col(dv, "외국인")
    c_close = _krx_col(px, "종가") or "종가"
    if not c_inst or not c_fr:
        return None

    out: List[Tuple[float, float]] = []
    for ts in px.index:
        if ts not in dv.index:
            continue
        row = dv.loc[ts]
        try:
            iv = float(row[c_inst])
            fv = float(row[c_fr])
        except (TypeError, ValueError):
            continue
        buy_vol = max(0.0, iv) + max(0.0, fv)
        if buy_vol <= 0:
            continue
        try:
            close_px = float(px.loc[ts, c_close])
        except Exception:
            continue
        out.append((close_px, buy_vol))

    return out if out else None


def _kalman_latent_avg_cost_1d(points: List[Tuple[float, float]]) -> Optional[float]:
    """
    잠재 상태 x_t = '스마트머니 참 평균 단가'(local level / random walk).
    관측 z_t = 일별 프록시 가격(여기서는 양수 순매수일 종가), z_t = x_t + v_t.
    관측 분산 R_t는 일별 순매수량 v_t에 반비례(대량 순매수일 R_t 축소 → 상태 강하게 갱신).
    """
    if not points:
        return None
    prices = np.array([p for p, _ in points], dtype=np.float64)
    vols = np.array([max(v, 0.0) for _, v in points], dtype=np.float64)
    if prices.size == 0 or not np.all(np.isfinite(prices)):
        return None
    if not np.any(vols > 0):
        return float(np.mean(prices))

    med_p = float(np.median(prices))
    med_v = float(np.median(vols[vols > 0])) if np.any(vols > 0) else float(np.median(vols))
    if med_v <= 0:
        med_v = 1.0

    # 공정 잡음: 잠재 평균단가의 미세 변동
    q_var = max((0.002 * med_p) ** 2, 1.0)
    # 기준 관측 분산 스케일(가격 스케일에 맞춤)
    r_base = max((0.012 * med_p) ** 2, 25.0)
    r_lo = max((0.003 * med_p) ** 2, 4.0)
    r_hi = max((0.09 * med_p) ** 2, 10_000.0)

    x = float(prices[0])
    v0 = float(max(vols[0], 0.2 * med_v))
    r0 = r_base * (med_v / v0)
    P = float(np.clip(r0, r_lo, r_hi))

    # [모멘텀 가중] 연속 대량 순매수일(=참 매집 가속)에는 관측 신뢰를 더 높여
    # 잠재 평단이 최근 체결가로 더 빠르게 수렴하도록 R_t 를 추가 축소한다.
    # streak: 순매수량이 중앙값을 초과한 연속 일수(최대 4일까지 가중, 최대 R_t≈1/3 로 축소).
    streak = 1 if vols[0] > med_v else 0
    momentum_cap = 4
    momentum_gain = 0.5

    for t in range(1, prices.size):
        x_pred = x
        P_pred = P + q_var
        z = float(prices[t])
        vt = float(max(vols[t], 0.2 * med_v))

        if vols[t] > med_v:
            streak += 1
        else:
            streak = 0
        # 연속 매집일수록 momentum_factor ↓ → R_t ↓ → 칼만 게인 K ↑ → 평단 반응 가속
        momentum_factor = 1.0 / (1.0 + momentum_gain * float(min(streak, momentum_cap)))

        Rt = r_base * (med_v / vt) * momentum_factor
        Rt = float(np.clip(Rt, r_lo, r_hi))
        S = P_pred + Rt
        if S <= 1e-18:
            K = 0.0
        else:
            K = P_pred / S
        x = x_pred + K * (z - x_pred)
        P = (1.0 - K) * P_pred

    return float(x)


def _try_pykrx_daily_smart_vwap(code: str, from_ymd: str, to_ymd: str) -> Optional[float]:
    """
    일별 외국인·기관 순매수량이 양수인 날만 종가로 가중한 VWAP (가능할 때만).
    데이터 경로는 `_pykrx_daily_flow_points`와 동일(수치적으로 기존과 동일한 가중평균).
    """
    pts = _pykrx_daily_flow_points(code, from_ymd, to_ymd)
    if not pts:
        return None
    num = sum(p * v for p, v in pts)
    den = sum(v for _, v in pts)
    if den <= 0:
        return None
    return num / den


def _persist_investor_flow_timeseries(trade_dates: List[str]) -> None:
    """[P0-1] 단일거래일 외인+기관 순매수 리더보드를 일별 시계열(kr_investor_flow)로 영속화.

    최근 5영업일 중 미적재분만 백필 → 5일 누적 모멘텀/다이버전스 팩터가 콜드스타트 없이
    가동된다. 전부 방어적(pykrx 부재/실패 시 조용히 스킵, 기존 라다 산출에 무영향).
    """
    if not trade_dates:
        return
    try:
        from kr_flow_factor import existing_flow_dates, persist_daily_flow
    except Exception as ex:
        print(f"⚠️ [수급 시계열] 모듈 로드 실패 — 적재 스킵: {ex}")
        return
    try:
        have = existing_flow_dates()
        recent = trade_dates[-5:]
        total = 0
        for d in recent:
            d_norm = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            if d_norm in have:
                continue
            acc, ok = _try_pykrx_flow_leaderboard(d, d, per_market_head=200)
            if not ok or not acc:
                continue
            total += persist_daily_flow(d_norm, acc)
        if total:
            print(f"🗄️ [수급 시계열] kr_investor_flow 적재/갱신: {total}행 (최근 {len(recent)}영업일 백필)")
    except Exception as ex:
        print(f"⚠️ [수급 시계열] 적재 스킵(비치명적): {ex}")


def run_smart_money_tracker():
    print("🕵️ [스마트 머니 레이더] 외인·기관 실제 순매수(상위 랭킹) + 가격 다이버전스 스캔 중...")

    _thr = resolve_stealth_thresholds()
    kr_price_cut = float(_thr["kr_price_cut"])
    print(
        f" ↳ 국면={_thr['regime']} → KR 매집 다이버전스 컷 +{kr_price_cut:.1f}% "
        f"({'방어적(데드캣 차단)' if _thr['defensive'] else '평시'})"
    )

    trade_dates = _recent_trade_dates_yyyymmdd(12)
    if len(trade_dates) < 5:
        print("🚨 최근 영업일 캘린더를 만들 수 없습니다.")
        return

    from_ymd, to_ymd = trade_dates[0], trade_dates[-1]

    flow_map: Dict[str, Dict[str, Any]] = {}
    used_krx_leader = False

    krx_map, ok = _try_pykrx_flow_leaderboard(from_ymd, to_ymd)
    if ok:
        flow_map = krx_map
        used_krx_leader = True
        print(f" ↳ KRX(pykrx) 순매수 상위 병합 유니버스: {len(flow_map)}종 (기간 {from_ymd}~{to_ymd})")
    else:
        flow_map = _naver_aggregate_flow(trade_dates, max_days=7, top_per_day=35)
        print(
            f" ↳ 네이버 순매수 iframe 폴백 유니버스: {len(flow_map)}종 "
            f"(최근 영업일 subset, 외인+기관 상위만)"
        )

    ranked = sorted(flow_map.items(), key=lambda kv: kv[1].get("krw", 0.0), reverse=True)
    candidates = ranked[:55]

    pykrx_daily_flow_ok = False
    if used_krx_leader:
        probe = _try_pykrx_daily_smart_vwap("005930", from_ymd, to_ymd)
        pykrx_daily_flow_ok = probe is not None and probe > 0

    smart_picks: Dict[str, Dict[str, Any]] = {}

    for code, slot in candidates:
        krw = float(slot.get("krw") or 0.0)
        if krw <= 0:
            continue

        ohlcv = fetch_ohlcv_for_divergence(code, from_ymd, to_ymd)
        if ohlcv is None or len(ohlcv) < 6:
            continue

        close_col = _close_column(ohlcv) or _krx_col(ohlcv, "종가") or "종가"
        tail = ohlcv.tail(5)
        try:
            p0 = float(tail[close_col].iloc[0])
            p1 = float(tail[close_col].iloc[-1])
        except Exception:
            continue

        if p0 <= 0:
            continue

        price_change_pct = (p1 - p0) / p0 * 100.0
        if price_change_pct > kr_price_cut:
            continue

        vwap_flow: Optional[float] = None
        daily_pts: Optional[List[Tuple[float, float]]] = None
        if used_krx_leader and pykrx_daily_flow_ok:
            daily_pts = _pykrx_daily_flow_points(code, from_ymd, to_ymd)

        if daily_pts:
            vwap_k = _kalman_latent_avg_cost_1d(daily_pts)
            if vwap_k is not None and vwap_k > 0:
                vwap_flow = vwap_k
            else:
                vwap_flow = _flow_vwap(slot)
        else:
            vwap_flow = _flow_vwap(slot)

        if vwap_flow is None or vwap_flow <= 0:
            vwap_flow = float(ohlcv[close_col].iloc[-1])

        stock_name = str(slot.get("name") or "").strip() or code

        flat_component = max(0.0, 2.5 - abs(price_change_pct))
        flow_component = min(45.0, math.log10(krw + 1.0) * 7.5)
        day_bonus = min(15.0, float(slot.get("days", 0) or 0) * 2.0) if not used_krx_leader else 0.0
        divergence_score = round(flat_component + flow_component + day_bonus, 2)

        smart_picks[code] = {
            "name": stock_name,
            "avg_price": round(vwap_flow, 0),
            "divergence_score": divergence_score,
        }

    # [0건 방어] 픽이 없을 때 예전 값을 남기면 리포트가 stale 라다를 "데이터 있음"으로 오인한다.
    # 항상 덮어써서(상태를 명시) 리포트가 오늘 상황을 정확히 인지하게 한다.
    config = load_config()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    if smart_picks:
        config["SMART_MONEY_RADAR"] = {
            "updated_at": now_s,
            "status": "ok",
            "picks": smart_picks,
        }
        if save_config(config):
            print(
                f"✅ 스캔 완료: {len(smart_picks)}개 종목이 관제탑 JSON(SMART_MONEY_RADAR.picks)에 반영되었습니다."
            )
        else:
            print(
                f"🚨 스캔 결과 {len(smart_picks)}개 종목을 산출했으나 system_config.json 저장에 실패했습니다. "
                f"경로 존재 여부를 확인하세요: {CONFIG_PATH}"
            )
    else:
        config["SMART_MONEY_RADAR"] = {
            "updated_at": now_s,
            "status": "no_smart_money_today",
            "picks": {},
        }
        save_config(config)
        print(
            "⚠️ 오늘 조건을 만족하는 외인·기관 매집 다이버전스 0건 — "
            "SMART_MONEY_RADAR 를 no_smart_money_today 로 명시 초기화했습니다."
        )

    # [P0-1] 라다 JSON 산출과 독립적으로, 외인+기관 순매수를 일별 시계열로 영속화한다.
    # (진입 관문 try_add_virtual_position 의 '수급 모멘텀/다이버전스' 가산 팩터 소비원)
    _persist_investor_flow_timeseries(trade_dates)

    # [Mega-Trend Unlock] 내부1진단→내부1킬→내부2킬→외부3→점화
    try:
        from mega_trend_climax import refresh_mega_trend_climax_kill
        from mega_trend_ignition import refresh_mega_trend_ignition
        from mega_trend_internal_kill import refresh_mega_trend_internal_momentum_kill
        from mega_trend_internal_monitor import refresh_mega_trend_internal_diagnostics
        from mega_trend_toxic_kill import refresh_mega_trend_toxic_graveyard_kill

        cfg_mt = load_config()
        refresh_mega_trend_internal_diagnostics(cfg_mt, save_config_fn=save_config)
        refresh_mega_trend_internal_momentum_kill(cfg_mt, save_config_fn=save_config)
        refresh_mega_trend_toxic_graveyard_kill(cfg_mt, save_config_fn=save_config)
        refresh_mega_trend_climax_kill(cfg_mt, save_config_fn=save_config)
        refresh_mega_trend_ignition(cfg_mt, save_config_fn=save_config)
    except Exception as ex:
        print(f"⚠️ [Mega-Trend Unlock] 스킵(비치명적): {ex}")

    # [P1-4] 같은 KR 일일 잡에 공매도/대차잔고 시계열 백필을 피기백(별도 크론 불필요).
    #   (진입 관문의 '숏스퀴즈 가산 / 크라우디드 숏 경계' 팩터 소비원). 전부 방어적.
    try:
        from short_interest_fetcher import backfill_recent as _backfill_short

        _backfill_short()
    except Exception as ex:
        print(f"⚠️ [공매도 시계열] 백필 스킵(비치명적): {ex}")

    # [P1-3a] 펀더멘털(PER/PBR/EPS/BPS) 스냅샷 백필 — 키 불필요(pykrx). 진입 가치·퀄리티 교차검증.
    try:
        from fundamentals_fetcher import backfill_recent as _backfill_fund

        _backfill_fund()
    except Exception as ex:
        print(f"⚠️ [펀더멘털] 백필 스킵(비치명적): {ex}")

    # [P1-3b] DART 공시 이벤트 백필 — OPENDART_API_KEY 있을 때만 동작(없으면 no-op).
    try:
        from dart_fetcher import backfill_recent as _backfill_dart

        _backfill_dart()
    except Exception as ex:
        print(f"⚠️ [DART] 백필 스킵(비치명적): {ex}")


# =============================================================================
# US 다크풀 / 기관 유동성 프록시 엔진
# -----------------------------------------------------------------------------
# 13F(분기 지연)·유료 다크풀 피드 없이, 야후 파이낸스 일봉 거래량을 쥐어짜
# "저변동성 거래량 폭증(Volume Anomaly on Low Volatility)"을 기관 매집 징후로 역추적한다.
#   가설: 다크풀/블록 체결로 기관이 매집하면, 거래량은 폭증하지만 가격은 거의 안 움직인다
#         (공급을 흡수하며 호가를 밀어올리지 않음) → 신선한 매집 캔들.
# 산출물은 KR 라다와 동일 스키마로 SMART_MONEY_RADAR_US 에 적재된다.
# =============================================================================

# 메가캡 기술주 + 핵심 ETF (정적 SSOT — 13F 의존 제거, 유동성 상위만)
US_DARK_POOL_UNIVERSE: List[str] = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "AVGO", "TSLA", "TSM",
    "ORCL", "ADBE", "CRM", "AMD", "CSCO", "ACN", "INTC", "QCOM", "TXN", "IBM",
    "INTU", "NOW", "AMAT", "MU", "ADI", "LRCX", "KLAC", "SNPS", "CDNS", "PANW",
    "ANET", "APH", "MSI", "FTNT", "ADSK", "NXPI", "MCHP", "MRVL", "CTSH", "HPQ",
    "DELL", "WDAY", "PLTR", "CRWD", "SHOP", "UBER", "ABNB", "NFLX",
]
US_DARK_POOL_ETFS: List[str] = ["SPY", "QQQ"]

US_VOLUME_SURGE_MULT = 2.0        # 20일 평균 대비 200% 이상 폭증 (평시 기준)
US_LOW_VOLATILITY_ABS_PCT = 2.0   # 당일 가격 변동률 |%| 상한 (저변동성=매집, 평시 기준)
US_LOOKBACK_DAYS = 70             # 야후 페치 윈도(20일 평균 + 여유)
US_VOL_AVG_WINDOW = 20            # 거래량 기준선 윈도
US_RECENT_SCAN_CANDLES = 3        # 최근 N거래일 내 이상치 탐색(최근 우선)

# [장세 적응형 매집 임계값] BEAR/HIGH_VOL 에서는 데드캣 바운스(가짜 매수세)를 거르기 위해
# 거래량 폭증 기준을 높이고(3.0), 가격 변동 컷오프를 더 보수적으로 조인다.
US_VOLUME_SURGE_MULT_DEFENSIVE = 3.0      # BEAR/HIGH_VOL: 300% 이상만 진성 매집 인정
US_LOW_VOLATILITY_ABS_PCT_DEFENSIVE = 1.2  # BEAR/HIGH_VOL: 변동 ±1.2% 이내만(더 엄격)
KR_DIVERGENCE_PRICE_CUT = 1.8             # KR 평시: 최근 5일 +1.8% 이하만(매집 다이버전스)
KR_DIVERGENCE_PRICE_CUT_DEFENSIVE = 0.8   # BEAR/HIGH_VOL: +0.8% 이하만(데드캣 차단)
_DEFENSIVE_REGIMES = {"BEAR", "HIGH_VOL"}


def _resolve_current_regime() -> str:
    """system_config(SQLite SSOT)에서 현재 국면 키를 읽는다(무 네트워크·실패 시 UNKNOWN)."""
    try:
        from config_manager import get_config_value

        rk = get_config_value("CURRENT_REGIME_KEY", None)
        if rk:
            return str(rk).strip().upper()
    except Exception:
        pass
    try:
        cfg = load_config()
        return str(cfg.get("CURRENT_REGIME_KEY", "UNKNOWN") or "UNKNOWN").strip().upper()
    except Exception:
        return "UNKNOWN"


def resolve_stealth_thresholds(regime: Optional[str] = None) -> Dict[str, float]:
    """
    국면별 매집 탐지 임계값 번들. BEAR/HIGH_VOL → 방어적(엄격), 그 외 → 평시 기준.
    반환: us_surge_mult, us_lowvol_pct, kr_price_cut, defensive(bool 0/1).
    """
    reg = (regime or _resolve_current_regime() or "UNKNOWN").strip().upper()
    defensive = reg in _DEFENSIVE_REGIMES
    if defensive:
        return {
            "regime": reg,
            "us_surge_mult": US_VOLUME_SURGE_MULT_DEFENSIVE,
            "us_lowvol_pct": US_LOW_VOLATILITY_ABS_PCT_DEFENSIVE,
            "kr_price_cut": KR_DIVERGENCE_PRICE_CUT_DEFENSIVE,
            "defensive": 1.0,
        }
    return {
        "regime": reg,
        "us_surge_mult": US_VOLUME_SURGE_MULT,
        "us_lowvol_pct": US_LOW_VOLATILITY_ABS_PCT,
        "kr_price_cut": KR_DIVERGENCE_PRICE_CUT,
        "defensive": 0.0,
    }


def _flatten_yf_columns(df: "pd.DataFrame") -> "pd.DataFrame":
    """yfinance 단일 티커가 MultiIndex 컬럼을 줄 때 1레벨로 평탄화."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) and c else c for c in df.columns]
    return df


def _fetch_us_daily_ohlcv(ticker: str, start: str) -> Optional["pd.DataFrame"]:
    try:
        from network_timeout import yf_download
    except Exception:
        try:
            import yfinance as _yf  # 최후 폴백

            def yf_download(t, **kw):  # type: ignore
                return _yf.download(t, progress=False, **kw)
        except Exception:
            return None
    try:
        df = yf_download(ticker, start=start, progress=False)
    except Exception:
        return None
    if df is None or len(df) == 0:
        return None
    df = _flatten_yf_columns(df)
    return df


def _detect_dark_pool_accumulation(
    df: "pd.DataFrame",
    ticker: str,
    *,
    surge_mult: float = US_VOLUME_SURGE_MULT,
    lowvol_pct: float = US_LOW_VOLATILITY_ABS_PCT,
) -> Optional[Dict[str, Any]]:
    """저변동성+거래량 폭증 캔들(가장 최근 우선) → 기관 매집 픽. 임계값은 국면 적응형."""
    if df is None or len(df) < US_VOL_AVG_WINDOW + 2:
        return None
    if "Volume" not in df.columns or "Close" not in df.columns:
        return None

    vol = pd.to_numeric(df["Volume"], errors="coerce").astype(float)
    close = pd.to_numeric(df["Close"], errors="coerce").astype(float)
    high = pd.to_numeric(df.get("High", close), errors="coerce").astype(float)
    low = pd.to_numeric(df.get("Low", close), errors="coerce").astype(float)
    avg_base = vol.shift(1).rolling(US_VOL_AVG_WINDOW).mean()  # 당일 제외 후행 평균

    n = len(df)
    lo_bound = max(n - 1 - US_RECENT_SCAN_CANDLES, US_VOL_AVG_WINDOW)
    for i in range(n - 1, lo_bound, -1):
        base = float(avg_base.iloc[i]) if pd.notna(avg_base.iloc[i]) else 0.0
        v_i = float(vol.iloc[i]) if pd.notna(vol.iloc[i]) else 0.0
        if base <= 0 or v_i <= 0:
            continue
        ratio = v_i / base
        if ratio < surge_mult:
            continue
        prev_close = float(close.iloc[i - 1]) if pd.notna(close.iloc[i - 1]) else 0.0
        cl = float(close.iloc[i]) if pd.notna(close.iloc[i]) else 0.0
        if prev_close <= 0 or cl <= 0:
            continue
        chg_pct = (cl - prev_close) / prev_close * 100.0
        if abs(chg_pct) > lowvol_pct:
            continue  # 가격이 크게 움직였으면 매집(흡수)이 아니라 일반 모멘텀

        hi = float(high.iloc[i]) if pd.notna(high.iloc[i]) else cl
        lw = float(low.iloc[i]) if pd.notna(low.iloc[i]) else cl
        typ = (hi + lw + cl) / 3.0 if hi > 0 and lw > 0 else cl  # 매집 평단 프록시(typical price)

        # 점수: 거래량 폭증 강도(상한 60) + 저변동성 가점(상한 ~12)
        flat_component = max(0.0, lowvol_pct - abs(chg_pct)) * 6.0
        surge_component = min(60.0, ratio * 12.0)
        divergence_score = round(flat_component + surge_component, 2)

        return {
            "name": ticker,
            "avg_price": round(typ, 2),
            "divergence_score": divergence_score,
            "volume_ratio": round(ratio, 2),
            "chg_pct": round(chg_pct, 2),
            "anomaly_date": str(df.index[i])[:10],
        }
    return None


def run_us_dark_pool_proxy() -> Dict[str, Dict[str, Any]]:
    print("🕵️ [US 다크풀 프록시] 저변동성 거래량 폭증(기관 매집) 역추적 중...")
    _thr = resolve_stealth_thresholds()
    _surge = float(_thr["us_surge_mult"])
    _lowvol = float(_thr["us_lowvol_pct"])
    print(
        f" ↳ 국면={_thr['regime']} → US 매집 임계 vol×{_surge:.1f}·|chg|≤{_lowvol:.1f}% "
        f"({'방어적(데드캣 차단)' if _thr['defensive'] else '평시'})"
    )
    start = (datetime.now() - timedelta(days=US_LOOKBACK_DAYS + 30)).strftime("%Y-%m-%d")
    tickers = list(dict.fromkeys(US_DARK_POOL_UNIVERSE + US_DARK_POOL_ETFS))

    picks: Dict[str, Dict[str, Any]] = {}
    for tk in tickers:
        df = _fetch_us_daily_ohlcv(tk, start)
        if df is None:
            continue
        try:
            hit = _detect_dark_pool_accumulation(df, tk, surge_mult=_surge, lowvol_pct=_lowvol)
        except Exception as ex:
            print(f"   ↳ {tk} 분석 예외(스킵): {ex}")
            hit = None
        if hit:
            picks[tk] = hit
        time.sleep(random.uniform(0.12, 0.35))

    config = load_config()
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M")
    if picks:
        config["SMART_MONEY_RADAR_US"] = {"updated_at": now_s, "status": "ok", "picks": picks}
        save_config(config)
        top = sorted(picks.values(), key=lambda d: d.get("divergence_score", 0), reverse=True)[:3]
        ex = ", ".join(f"{d['name']}(vol×{d.get('volume_ratio')})" for d in top)
        print(f"✅ US 다크풀 프록시: {len(picks)}개 기관 매집 징후 → SMART_MONEY_RADAR_US ({ex})")
    else:
        config["SMART_MONEY_RADAR_US"] = {
            "updated_at": now_s,
            "status": "no_smart_money_today",
            "picks": {},
        }
        save_config(config)
        print("⚠️ US 저변동성 거래량 폭증 0건 — SMART_MONEY_RADAR_US 를 no_smart_money_today 로 초기화했습니다.")
    return picks


def run_all_smart_money() -> None:
    """KR(외인·기관 순매수 다이버전스) + US(다크풀 저변동성 거래량 프록시) 통합 갱신."""
    try:
        run_smart_money_tracker()
    except Exception as ex:
        print(f"⚠️ [스마트머니] KR 트래커 실패: {ex}")
    try:
        run_us_dark_pool_proxy()
    except Exception as ex:
        print(f"⚠️ [스마트머니] US 다크풀 프록시 실패: {ex}")


if __name__ == "__main__":
    run_all_smart_money()
