"""
KRX 보통주(Equity) 유니버스 — 시장구분 메타데이터·파생상품 코드·이름 junk SSOT.

- FDR StockListing('KRX'): MarketId STK/KSQ, Dept SPAC 제외
- FDR StockListing('ETF/KR'): ETF 코드 전량 제외 집합
- pykrx(가능 시): get_etf/get_etn/get_elw 티커 제외 집합 병합
- DEFAULT_JUNK_NAME_PATTERN: ETF/ETN/스팩/브랜드명 등 Name regex (3중 방어망 1층)
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional, Set, Tuple

import pandas as pd

# 레거시·supernova·data_updater 공통 Name 거름망 (junk_pattern SSOT)
DEFAULT_JUNK_NAME_PATTERN = (
    r"스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권"
    r"|KODEX|TIGER|KBSTAR|ACE|ARIRANG|KOSEF|HANARO|SOL|TIMEFOLIO|WOORI"
    r"|히어로즈|마이티|디딤|BNK|PLUS"
)
_KR_MIN_ENTRY_PRICE = 1000.0

# FDR KRX MarketId — 상장 보통주·코스닥 주권 (ETF/ETN/ELW/KONEX 제외)
_FDR_EQUITY_MARKET_IDS = frozenset({"STK", "KSQ"})
_FDR_EQUITY_MARKET_NAMES = frozenset({"KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"})
_SPAC_DEPT_RE = re.compile(r"SPAC", re.I)
# 한국 거래소 우선주 관례(동일 보통주와 별도 종목코드) — 시장구분 메타 보조
_PREFERRED_NAME_RE = re.compile(r"^.+우(B)?$")


def _junk_name_re(pattern: str | None) -> re.Pattern[str] | None:
    pat = (pattern if pattern is not None else DEFAULT_JUNK_NAME_PATTERN).strip()
    if not pat:
        return None
    return re.compile(pat, re.I)


def _name_matches_junk(name: object, pattern: str | None = None) -> bool:
    nm = str(name or "").strip()
    if not nm or nm.lower() in ("nan", "none"):
        return False
    rx = _junk_name_re(pattern)
    if rx is None:
        return False
    return bool(rx.search(nm))


def reject_kr_virtual_entry(
    code: object,
    name: object,
    *,
    entry_price: object = None,
    junk_pattern: str | None = None,
) -> Tuple[bool, str]:
    """
    try_add_virtual_position 최종 관문 — (allowed, reason).
    allowed=True 이면 진입 허용.
    """
    code_s = str(code or "").strip().zfill(6)
    nm = str(name or "").strip()

    if _name_matches_junk(nm, junk_pattern):
        return False, f"이름 거름망: {nm[:40]}"

    if nm and _PREFERRED_NAME_RE.match(nm):
        return False, f"우선주: {nm[:40]}"

    try:
        excl = load_derivative_exclusion_codes()
        if code_s and code_s in excl:
            return False, f"파생상품 코드 집합: {code_s}"
    except Exception:
        pass

    if entry_price is not None:
        try:
            px = float(entry_price)
            if px > 0 and px < _KR_MIN_ENTRY_PRICE:
                return False, f"동전주(진입가 {px:.0f}원 < {_KR_MIN_ENTRY_PRICE:.0f}원)"
        except (TypeError, ValueError):
            pass

    return True, ""


@lru_cache(maxsize=1)
def load_derivative_exclusion_codes() -> frozenset[str]:
    """ETF·ETN·ELW 티커 코드(6자리) 합집합. 실패 시 빈 집합."""
    codes: set[str] = set()
    codes |= _fdr_etf_codes()
    codes |= _pykrx_derivative_codes()
    return frozenset(codes)


def _fdr_etf_codes() -> set[str]:
    out: set[str] = set()
    try:
        import FinanceDataReader as fdr

        etf = fdr.StockListing("ETF/KR")
        if etf is None or etf.empty:
            return out
        col = "Symbol" if "Symbol" in etf.columns else "Code"
        if col not in etf.columns:
            return out
        out.update(etf[col].astype(str).str.strip().str.zfill(6))
    except Exception:
        pass
    return out


def _pykrx_derivative_codes(max_lookback_days: int = 8) -> set[str]:
    out: set[str] = set()
    try:
        from pykrx import stock as krx
    except ImportError:
        return out

    fetchers = (
        getattr(krx, "get_etf_ticker_list", None),
        getattr(krx, "get_etn_ticker_list", None),
        getattr(krx, "get_elw_ticker_list", None),
    )
    today = datetime.now().date()
    for day_offset in range(max_lookback_days):
        d = today - timedelta(days=day_offset)
        if d.weekday() >= 5:
            continue
        ymd = d.strftime("%Y%m%d")
        got_any = False
        for fn in fetchers:
            if fn is None:
                continue
            try:
                tickers = fn(ymd)
                if tickers is not None and len(tickers) > 0:
                    out.update(str(t).zfill(6) for t in tickers)
                    got_any = True
            except Exception:
                continue
        if got_any:
            break
    return out


def _attach_fdr_market_metadata(df: pd.DataFrame, fdr_module=None) -> pd.DataFrame:
    """Code 기준 FDR KRX 스냅샷에서 MarketId·Dept 병합."""
    if df is None or df.empty or "Code" not in df.columns:
        return df
    try:
        fdr = fdr_module
        if fdr is None:
            import FinanceDataReader as fdr  # type: ignore[no-redef]
        snap = fdr.StockListing("KRX")
        if snap is None or snap.empty:
            return df
        snap = snap.copy()
        snap["Code"] = snap["Code"].astype(str).str.strip().str.zfill(6)
        meta_cols = [c for c in ("Code", "MarketId", "Dept", "Market", "ISU_CD") if c in snap.columns]
        meta = snap[meta_cols].drop_duplicates(subset=["Code"])
        out = df.copy()
        out["Code"] = out["Code"].astype(str).str.strip().str.zfill(6)
        return out.merge(meta, on="Code", how="left", suffixes=("", "_fdr"))
    except Exception:
        return df


def filter_krx_equity_universe(
    df: pd.DataFrame,
    *,
    derivative_exclude: Optional[Set[str]] = None,
    fdr_module=None,
    junk_pattern: str | None = DEFAULT_JUNK_NAME_PATTERN,
) -> pd.DataFrame:
    """
    보통주 유니버스만 남긴다. 입력에 MarketId 없으면 FDR 메타 병합 후 필터.
    junk_pattern: Name regex — None/빈 문자열이면 이름 regex 스킵.
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()

    excl = derivative_exclude if derivative_exclude is not None else load_derivative_exclusion_codes()
    out = df.copy()
    if "Code" in out.columns:
        out["Code"] = out["Code"].astype(str).str.strip().str.zfill(6)

    if "MarketId" not in out.columns and "Dept" not in out.columns:
        out = _attach_fdr_market_metadata(out, fdr_module=fdr_module)

    if "MarketId" in out.columns:
        mid = out["MarketId"].astype(str).str.strip().str.upper()
        out = out[mid.isin(_FDR_EQUITY_MARKET_IDS)].copy()
    elif "Market" in out.columns:
        mk = out["Market"].astype(str).str.strip().str.upper()
        out = out[mk.isin(_FDR_EQUITY_MARKET_NAMES)].copy()

    if "Dept" in out.columns:
        dept = out["Dept"].astype(str)
        out = out[~dept.str.contains(_SPAC_DEPT_RE, na=False)].copy()

    if excl and "Code" in out.columns:
        out = out[~out["Code"].isin(excl)].copy()

    if "Name" in out.columns:
        nm = out["Name"].astype(str)
        out = out[~nm.str.match(_PREFERRED_NAME_RE, na=False)].copy()
        junk_rx = _junk_name_re(junk_pattern)
        if junk_rx is not None:
            nm2 = out["Name"].astype(str)
            out = out[~nm2.str.contains(junk_rx, na=False)].copy()

    return out.reset_index(drop=True)
