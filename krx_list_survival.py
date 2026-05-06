# Shared KRX listing pipeline: API → CSV cache → sqlite KR_* table names.
# Import this from screeners / hunters; do not duplicate the 3-tier logic.
from __future__ import annotations

import os
import re
import sqlite3
import warnings

import pandas as pd

DEFAULT_DB_PATH = os.path.join(
    os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "market_data.sqlite"
)
KRX_LIST_CACHE_BASENAME = "krx_list_cache.csv"

_DEFAULT_JUNK = r"스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권"
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
    Tier 1: FDR StockListing(KRX), then KOSPI+KOSDAQ. On success, overwrite CSV cache.
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
    print(
        f"[KRX listing] 1단계 FDR 라이브 시도 (min_rows={min_live_rows}, 캐시={resolved_cache})"
    )
    live_raw = None
    try:
        try:
            live_raw = fdr.StockListing("KRX")
        except Exception:
            live_raw = None
        if live_raw is None:
            print("[KRX listing] 1단계 StockListing('KRX') 실패 또는 None → KOSPI+KOSDAQ 우회 호출")
        elif len(live_raw) < min_live_rows:
            print(
                f"[KRX listing] 1단계 KRX 응답 행수 부족({len(live_raw)}<{min_live_rows}) → "
                "KOSPI+KOSDAQ 우회 호출"
            )
        if live_raw is None or len(live_raw) < min_live_rows:
            try:
                k1 = fdr.StockListing("KOSPI")
                k2 = fdr.StockListing("KOSDAQ")
                live_raw = pd.concat([k1, k2], ignore_index=True)
            except Exception:
                live_raw = None
            if live_raw is None or len(live_raw) < min_live_rows:
                print(
                    "[KRX listing] 1단계 FDR 라이브 불가(우회 포함) → 2단계 로컬 CSV로 전환"
                )

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
                    "[KRX listing] 1단계 FDR 수신 후 정규화/필터 결과 비어 있음 → 2단계로 전환"
                )
            except Exception:
                print(
                    "[KRX listing] 1단계 FDR 정규화 중 예외 → 2단계로 전환"
                )
    except Exception:
        print("[KRX listing] 1단계 FDR 블록 예외 → 2단계로 전환")

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

    print("[KRX listing] 전 단계 실패: 빈 DataFrame 반환(source=fail)")
    return pd.DataFrame(columns=["Code", "Name", "Market", "Marcap"]), "fail"
