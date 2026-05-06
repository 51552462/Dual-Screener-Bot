# Shared KRX listing pipeline: API → CSV cache → sqlite KR_* table names.
# Import this from screeners / hunters; do not duplicate the 3-tier logic.
from __future__ import annotations

import os
import re
import sqlite3
import warnings
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

DEFAULT_DB_PATH = os.path.join(
    os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite"
)
KRX_LIST_CACHE_BASENAME = "krx_list_cache.csv"

_DEFAULT_JUNK = r"스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권|KODEX|TIGER|KBSTAR|ACE|ARIRANG|KOSEF|HANARO|SOL|TIMEFOLIO|WOORI|히어로즈|마이티|디딤|BNK|PLUS"
_CODE_RE = re.compile(r"^KR_(\d{6})$")
_MIN_LIVE_ROWS = 300


def default_krx_list_cache_path(db_path: str | None = None) -> str:
    base = db_path or DEFAULT_DB_PATH
    return os.path.join(os.path.dirname(base), KRX_LIST_CACHE_BASENAME)


def _safe_write_cache(df: pd.DataFrame, path: str) -> None:
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        df.to_csv(path, index=False)
    except Exception:
        pass


def _read_cache_csv(path: str) -> pd.DataFrame | None:
    if not path or not os.path.isfile(path):
        return None
    try:
        snap = pd.read_csv(path)
    except Exception:
        return None
    if snap is None or snap.empty:
        return None
    return snap


def _normalize_columns(d: pd.DataFrame) -> pd.DataFrame:
    d = d.copy()
    if "Symbol" in d.columns and "Code" not in d.columns:
        d["Code"] = d["Symbol"]
    if "종목코드" in d.columns and "Code" not in d.columns:
        d["Code"] = d["종목코드"]
    if "회사명" in d.columns and "Name" not in d.columns:
        d = d.rename(columns={"회사명": "Name"})
    if "종목명" in d.columns and "Name" not in d.columns:
        d = d.rename(columns={"종목명": "Name"})
    if not all(c in d.columns for c in ("Code", "Name", "Market")):
        raise ValueError("필수 컬럼 누락")
    d["Code"] = d["Code"].astype(str).str.strip().str.zfill(6)
    if "Marcap" not in d.columns:
        if "MarketCap" in d.columns:
            d["Marcap"] = d["MarketCap"]
        elif "시가총액" in d.columns:
            d["Marcap"] = d["시가총액"]
        else:
            d["Marcap"] = 0.0
    if d["Marcap"].dtype == object:
        d["Marcap"] = (
            d["Marcap"].astype(str).str.replace(",", "", regex=False)
        )
    d["Marcap"] = pd.to_numeric(d["Marcap"], errors="coerce").fillna(0.0)
    return d.drop_duplicates(subset=["Code"], ignore_index=True)


def _finalize_standard(
    df: pd.DataFrame, junk_pattern: str | None, apply_junk_filter: bool
) -> pd.DataFrame:
    out = df.copy()
    out["Marcap"] = pd.to_numeric(out["Marcap"], errors="coerce").fillna(0.0)
    if apply_junk_filter and junk_pattern and "Name" in out.columns:
        out = out[
            ~out["Name"].astype(str).str.contains(junk_pattern, regex=True)
        ].copy()
    return out[["Code", "Name", "Market", "Marcap"]].dropna(
        subset=["Code", "Name", "Market"]
    )


def _stage3_sqlite_codes(db_path: str) -> pd.DataFrame | None:
    try:
        if not db_path or not os.path.isfile(db_path):
            return None
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            tables = pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'KR_%'",
                conn,
            )
        finally:
            conn.close()
    except Exception:
        return None
    if tables is None or tables.empty or "name" not in tables.columns:
        return None
    codes = []
    for raw in tables["name"].astype(str):
        m = _CODE_RE.match(raw.strip())
        if m:
            codes.append(m.group(1))
    if not codes:
        return None
    uq = sorted(set(codes))
    tab = pd.DataFrame(
        {
            "Code": uq,
            "Name": uq,
            "Market": "KRX",
            "Marcap": 0.0,
        }
    )
    return tab


def _fetch_from_naver_finance() -> pd.DataFrame:
    """
    네이버 시세(시장별 종목 리스트) HTML을 직접 파싱해서
    DataFrame(Code, Name, Market, Marcap)으로 반환한다.
    """
    result: list[dict] = []
    # 0: 코스피, 1: 코스닥
    for sosok, mkt in [(0, "KOSPI"), (1, "KOSDAQ")]:
        for page in range(1, 100):  # 코스닥 최대 페이지 여유있게 100까지
            try:
                url = (
                    "https://finance.naver.com/sise/sise_market_sum.naver"
                    f"?sosok={sosok}&page={page}"
                )
                res = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )
                soup = BeautifulSoup(res.text, "html.parser")

                table = soup.select_one("table.type_2")
                if not table:
                    break

                rows = table.select("tbody tr")
                page_has_data = False
                for row in rows:
                    a_tag = row.select_one("a.tltle")
                    if not a_tag:
                        continue

                    page_has_data = True
                    name = a_tag.text.strip()
                    href = a_tag.get("href", "") or ""
                    code = href.split("code=")[-1].strip()
                    code = code.split("&")[0].strip()

                    cols = row.select("td")
                    marcap = 0.0
                    if len(cols) >= 7:
                        try:
                            # 네이버 시가총액은 '억' 단위이므로 1억을 곱해 원 단위로 맞춤
                            marcap = (
                                float(cols[6].text.replace(",", "").strip())
                                * 100000000
                            )
                        except Exception:
                            pass

                    result.append(
                        {"Code": code, "Name": name, "Market": mkt, "Marcap": marcap}
                    )

                # 해당 페이지에 종목이 없으면 마지막 페이지를 넘긴 것
                if not page_has_data:
                    break
            except Exception:
                break

            # 과도한 요청 방지(데드락/차단 확률도 함께 낮춤)
            time.sleep(0.2)

    return pd.DataFrame(result)


def collect_krx_list_survival(
    *,
    db_path: str | None = None,
    primary_cache_csv: str | None = None,
    legacy_cache_paths: tuple[str, ...] | list[str] = (),
    junk_pattern: str | None = _DEFAULT_JUNK,
    apply_junk_filter: bool = True,
    min_live_rows: int = _MIN_LIVE_ROWS,
    fdr_module=None,
):
    """
    Tier 1: 네이버(우선) → 실패 시 FDR StockListing(KRX then KOSPI+KOSDAQ).
    성공하면 CSV 캐시를 덮어쓴다.
    Tier 2: Read krx_list_cache.csv (+ optional legacy paths).
    Tier 3: Derive codes from sqlite tables named KR_######.
    Returns (DataFrame, source) where source is 'live' | 'cache' | 'sqlite' | 'fail'.
    DataFrame columns: Code, Name, Market, Marcap (empty only on total failure).
    """
    warnings.filterwarnings("ignore", category=FutureWarning)

    resolved_db = db_path or DEFAULT_DB_PATH
    resolved_cache = primary_cache_csv or default_krx_list_cache_path(resolved_db)

    fdr = fdr_module
    if fdr is None:
        import FinanceDataReader as _fdr

        fdr = _fdr

    # ── Tier 1: live ───────────────────────────────────────────────
    print(f"[KRX listing] 1단계 라이브 수집 시도 (min_rows={min_live_rows})")
    live_raw = None

    # 1.1 단계: 네이버 직접 스크래핑 (가장 강력하고 안전함)
    try:
        print("[KRX listing] 1.1단계 네이버 파이낸스 직접 수집 시도...")
        live_raw = _fetch_from_naver_finance()
        if live_raw is not None:
            print(f"[KRX listing] 1.1단계 네이버 수집 완료: rows={len(live_raw)}")
    except Exception as e:
        print(f"[KRX listing] 네이버 수집 실패: {e}")
        live_raw = None

    # 1.2 단계: 네이버가 실패했을 경우에만 기존 FDR 시도
    if live_raw is None or len(live_raw) < min_live_rows:
        try:
            print("[KRX listing] 1.2단계 FDR StockListing 우회 시도...")
            try:
                live_raw = fdr.StockListing("KRX")
            except Exception:
                live_raw = None

            if live_raw is None or len(live_raw) < min_live_rows:
                try:
                    k1 = fdr.StockListing("KOSPI")
                    k2 = fdr.StockListing("KOSDAQ")
                    live_raw = pd.concat([k1, k2], ignore_index=True)
                except Exception:
                    live_raw = None
        except Exception as e:
            print(f"[KRX listing] 1.2단계 FDR 우회 실패: {e}")
            live_raw = None

    if live_raw is not None and len(live_raw) >= min_live_rows:
        try:
            d = _normalize_columns(live_raw)
            finalized = _finalize_standard(
                d, junk_pattern, apply_junk_filter
            )
            if finalized is not None and not finalized.empty:
                _safe_write_cache(finalized, resolved_cache)
                print(
                    f"[KRX listing] 1단계 성공: {len(finalized)}종목, 캐시 덮어쓰기 완료"
                )
                return finalized, "live"
            print(
                "[KRX listing] 1단계 라이브 수신 후 정규화/필터 결과 비어 있음 → 2단계로 전환"
            )
        except Exception:
            print("[KRX listing] 1단계 라이브 정규화 중 예외 → 2단계로 전환")
    else:
        print("[KRX listing] 1단계 라이브 데이터 부족/실패 → 2단계 CSV로 전환")

    # ── Tier 2: CSV caches ─────────────────────────────────────────
    n_legacy = len(tuple(legacy_cache_paths))
    print(
        f"[KRX listing] 2단계 CSV 복구: 주캐시 + 레거시 {n_legacy}경로 순회"
    )
    cache_candidates = [resolved_cache, *tuple(legacy_cache_paths)]
    for cpath in cache_candidates:
        try:
            snap = _read_cache_csv(cpath)
            if snap is None or snap.empty:
                continue
            snap = snap.copy()
            snap["Code"] = snap["Code"].astype(str).str.strip().str.zfill(6)
            if "Name" not in snap.columns:
                snap["Name"] = snap["Code"]
            if "Market" not in snap.columns:
                snap["Market"] = "KRX"
            if "Marcap" not in snap.columns:
                snap["Marcap"] = 0.0
            out = _finalize_standard(snap, junk_pattern, apply_junk_filter)
            if out is not None and not out.empty:
                try:
                    if cpath != resolved_cache:
                        _safe_write_cache(out, resolved_cache)
                except Exception:
                    pass
                print(
                    f"[KRX listing] 2단계 성공: {len(out)}종목 ← {cpath}"
                )
                return out, "cache"
        except Exception:
            continue

    print("[KRX listing] 2단계 CSV 모두 실패 또는 비어 있음 → 3단계 sqlite로 전환")

    # ── Tier 3: sqlite KR_###### ─────────────────────────────────
    print(f"[KRX listing] 3단계 sqlite 역추출 시도: {resolved_db}")
    try:
        tab = _stage3_sqlite_codes(resolved_db)
        if tab is not None and not tab.empty:
            out = _finalize_standard(tab, junk_pattern, False)
            if out is not None and not out.empty:
                print(f"[KRX listing] 3단계 성공: {len(out)}종목 (KR_###### 테이블명 기준)")
                return out, "sqlite"
        print("[KRX listing] 3단계 sqlite에서 유효 KR_###### 테이블 없음 또는 결과 비어 있음")
    except Exception:
        print("[KRX listing] 3단계 sqlite 접속/쿼리 예외")

    print("[KRX listing] Tier 4 하드코딩 코어 10종목 활성화")
    tier4 = pd.DataFrame(
        [
            {"Code": "005930", "Name": "삼성전자", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "000660", "Name": "SK하이닉스", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "005380", "Name": "현대차", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "068270", "Name": "셀트리온", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "105560", "Name": "KB금융", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "005935", "Name": "삼성전자우", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "012330", "Name": "현대모비스", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "055550", "Name": "신한지주", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "207940", "Name": "삼성바이오로직스", "Market": "KOSPI", "Marcap": 0.0},
            {"Code": "035420", "Name": "NAVER", "Market": "KOSPI", "Marcap": 0.0},
        ]
    )
    return tier4, "tier4"
