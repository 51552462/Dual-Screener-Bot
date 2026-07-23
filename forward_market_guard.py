"""
Market frame guard — code·market 불일치 행 제거 또는 fail-fast.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Literal, Optional

import pandas as pd

logger = logging.getLogger(__name__)

GuardMode = Literal["scrub", "strict"]


class MarketContaminationError(RuntimeError):
    """요청 market과 불일치하는 행이 존재할 때 (strict 모드)."""


def normalize_trade_market(code: object, market: object) -> str:
    """
    code·market 불일치 교정 — KR: 숫자 코드 / US: 알파벳 티커.
    """
    c = str(code or "").strip().upper()
    m = str(market or "").strip().upper()
    if re.fullmatch(r"\d{5,6}", c) or (c.isdigit() and len(c) <= 6):
        return "KR"
    if c and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", c):
        return "US"
    if m in ("KR", "US"):
        return m
    return "KR"


def _resolve_guard_mode(mode: Optional[str]) -> GuardMode:
    raw = (mode or os.environ.get("MARKET_GUARD_MODE", "scrub")).strip().lower()
    return "strict" if raw == "strict" else "scrub"


def enforce_market_frame(
    df: Optional[pd.DataFrame],
    market: str,
    *,
    context: str = "",
    mode: Optional[str] = None,
) -> pd.DataFrame:
    """
    DataFrame 행이 요청 market과 일치하는지 검증.
    scrub: 불일치 행 제거 + 로그
    strict: MarketContaminationError
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()

    mkt = str(market or "").upper()
    guard = _resolve_guard_mode(mode)
    out = df.copy()
    ctx = context or "enforce_market_frame"
    leak_mask = pd.Series(False, index=out.index)

    if "market" in out.columns:
        bad_mkt = out["market"].astype(str).str.upper().str.strip() != mkt
        leak_mask = leak_mask | bad_mkt

    code_col = None
    for col in ("code", "ticker"):
        if col in out.columns:
            code_col = col
            break

    if code_col is not None:
        mk_series = (
            out["market"] if "market" in out.columns else pd.Series("", index=out.index)
        )
        norm = [
            normalize_trade_market(out.iloc[i][code_col], mk_series.iloc[i])
            for i in range(len(out))
        ]
        code_leak = pd.Series([nm != mkt for nm in norm], index=out.index)
        leak_mask = leak_mask | code_leak

    if not leak_mask.any():
        return out

    n_leak = int(leak_mask.sum())
    sample = ""
    if code_col is not None:
        sample = out.loc[leak_mask, [code_col, "market"] if "market" in out.columns else [code_col]].head(
            5
        ).to_dict(orient="records")

    msg = f"{ctx}: {n_leak} row(s) market mismatch (expected {mkt}) sample={sample}"
    if guard == "strict":
        raise MarketContaminationError(msg)

    logger.warning(msg)
    try:
        import ops_logger

        ops_logger.insert_ops_event(
            component="forward_market_guard",
            severity="WARN",
            event="market.contamination.scrub",
            payload={"context": ctx, "market": mkt, "removed": n_leak, "sample": sample},
        )
    except Exception as ex:
        logger.debug("%s: ops_logger skip: %s", ctx, ex)

    return out.loc[~leak_mask].copy()


# ===========================================================================
# [초월적 시차 전이] 아침 9시 갭 페이크(Gap Fake) 판별 및 스마트머니 강도 게이트
# ===========================================================================
def evaluate_gap_fake_and_intensity(
    gap_pct: float,
    smart_money_net_15m: float,
    prev_day_volume: float,
    *,
    gap_threshold: float = 2.0,
    intensity_threshold: float = 0.05
) -> dict:
    """
    미국장 전이로 인해 한국장 아침에 갭상승(gap_threshold 이상) 출발한 종목을 검증.
    15분간의 스마트머니(외인/기관) 순매수를 전일 거래대금으로 나눈 '수급 강도(Intensity)'를 평가.
    """
    # 1. 갭상승이 임계치 미만이면 일반 진입으로 패스
    if gap_pct < gap_threshold:
        return {"status": "PASS", "reason": "normal_start", "intensity": 0.0}
    
    # 2. 거래대금 데이터가 없으면 극도로 보수적 차단 (방어 우선)
    if prev_day_volume <= 0:
        return {"status": "BLOCK", "reason": "missing_volume_data", "intensity": 0.0}
    
    # 3. 수급 강도(Intensity) 산출
    intensity = smart_money_net_15m / prev_day_volume
    
    # 4. 판별: 수급 강도가 약하거나 음수(-)면 개인 덤핑(Gap & Fade)으로 간주
    if intensity < intensity_threshold:
        return {
            "status": "BLOCK", 
            "reason": f"gap_fake_detected (Intensity: {intensity:.4f} < {intensity_threshold})",
            "intensity": intensity
        }
    
    # 5. 통과: 세력의 진짜 돌파 (Gap & Go)
    return {
        "status": "PASS", 
        "reason": f"gap_and_go_confirmed (Intensity: {intensity:.4f})",
        "intensity": intensity
    }