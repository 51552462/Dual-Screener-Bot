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
