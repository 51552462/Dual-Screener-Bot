"""
[9/9] 데스매치 ↔ AceEvolution 통합 — 초신성(M) vs 오리지널(A) 승률 대결.
"""
from __future__ import annotations

import html
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from evolution.ace_evolution_schema import flow_tag_prefix
from evolution.deathmatch_report import (
    ArmDeathmatchRow,
    _effective_final_ret_pct,
    build_arm_row,
    fmt_deathmatch_ret,
)


def trade_has_ace_evolution_tag(row: Any, market: str) -> bool:
    tags = str(row.get("flow_tags") if hasattr(row, "get") else (row or "")).upper()
    prefix = flow_tag_prefix(market).upper()
    return prefix in tags or "ACE_EVOL" in tags


def _logic_matches(row: Any, logic_core: str) -> bool:
    sig = str(row.get("sig_type") if hasattr(row, "get") else "").strip()
    core = re.sub(r"\[.*?\]", "", sig).strip()
    target = str(logic_core or "").strip()
    if not target:
        return False
    return target in core or target in sig


def split_mutant_vs_original(
    df_closed: pd.DataFrame,
    *,
    market: str,
    playbook: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Mutant: flow_tags ACE_EVOL_* 또는 (playbook logic_core 일치 + 상위 수익 청산)
    Original: deathmatch A (오리지널) 분류 축에 해당하는 청산.
    """
    if df_closed is None or df_closed.empty:
        return pd.DataFrame(), pd.DataFrame()

    work = df_closed.copy()
    if "market" in work.columns:
        work = work[work["market"].astype(str).str.upper() == str(market).upper()]

    pb = playbook if isinstance(playbook, dict) else {}
    logic_core = str(pb.get("logic_core") or "")

    mutant_idx: List[Any] = []
    for idx, row in work.iterrows():
        if trade_has_ace_evolution_tag(row, market):
            mutant_idx.append(idx)
            continue
        if logic_core and _logic_matches(row, logic_core):
            try:
                ret = float(pd.to_numeric(row.get("final_ret"), errors="coerce"))
            except (TypeError, ValueError):
                ret = 0.0
            if ret > 0:
                mutant_idx.append(idx)

    mutant_df = work.loc[mutant_idx].copy() if mutant_idx else pd.DataFrame()

    from evolution.deathmatch_report import classify_strategy_arm

    orig_idx = []
    for idx, row in work.iterrows():
        arm = classify_strategy_arm(row.get("sig_type"))
        if arm == "A (오리지널)":
            orig_idx.append(idx)
    original_df = work.loc[orig_idx].copy() if orig_idx else pd.DataFrame()
    return mutant_df, original_df


def build_ace_deathmatch_comparison(
    df_closed: pd.DataFrame,
    *,
    market: str,
    playbook: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    mutant_df, original_df = split_mutant_vs_original(df_closed, market=market, playbook=playbook)
    m_row = build_arm_row("M (Ace DNA)", mutant_df) if not mutant_df.empty else ArmDeathmatchRow(
        label="M (Ace DNA)", n_closed=0, n_valid=0, mean_ret=None, win_rate_pct=None, profit_factor=None
    )
    a_row = build_arm_row("A (오리지널)", original_df) if not original_df.empty else ArmDeathmatchRow(
        label="A (오리지널)", n_closed=0, n_valid=0, mean_ret=None, win_rate_pct=None, profit_factor=None
    )

    verdict = "표본 부족 — 익일 ACE_EVOL 태깅 후 재집계"
    if m_row.n_valid >= 3 and a_row.n_valid >= 3:
        if m_row.mean_ret is not None and a_row.mean_ret is not None:
            diff = float(m_row.mean_ret) - float(a_row.mean_ret)
            if diff > 0.5:
                verdict = f"초신성(M) 우위 — 격차 {diff:+.2f}%p"
            elif diff < -0.5:
                verdict = f"오리지널(A) 우위 — 격차 {diff:+.2f}%p"
            else:
                verdict = f"동률권 — 격차 {diff:+.2f}%p"

    return {
        "market": str(market).upper(),
        "mutant": m_row,
        "original": a_row,
        "verdict": verdict,
        "playbook_logic": str((playbook or {}).get("logic_core") or ""),
        "observe_only": bool((playbook or {}).get("observe_only", True)),
    }


def format_ace_evolution_oneliner(comp: Dict[str, Any]) -> str:
    """챔피언 하단 1줄 — 초신성(M) vs 오리지널(A) 평균 승률."""
    if not comp:
        return ""
    m: ArmDeathmatchRow = comp.get("mutant")
    a: ArmDeathmatchRow = comp.get("original")
    if m is None or a is None:
        return ""

    m_wr = f"{m.win_rate_pct:.1f}%" if m.win_rate_pct is not None and m.n_valid > 0 else "—"
    a_wr = f"{a.win_rate_pct:.1f}%" if a.win_rate_pct is not None and a.n_valid > 0 else "—"
    tail = ""
    if m.win_rate_pct is not None and a.win_rate_pct is not None and m.n_valid >= 1 and a.n_valid >= 1:
        diff = float(m.win_rate_pct) - float(a.win_rate_pct)
        if abs(diff) > 0.5:
            tail = f" · {'M' if diff > 0 else 'A'} 우위 {abs(diff):.1f}%p"
        else:
            tail = " · 동률권"
    else:
        verdict = str(comp.get("verdict") or "").strip()
        if verdict:
            tail = f" · {html.escape(verdict[:56], quote=False)}"

    return (
        f"🧬 <b>[진화]</b> 초신성(M) 그룹 평균 승률 {m_wr} vs 오리지널(A) {a_wr}{tail}"
    )


def format_ace_deathmatch_telegram_block(comp: Dict[str, Any]) -> str:
    if not comp:
        return ""
    m: ArmDeathmatchRow = comp.get("mutant")
    a: ArmDeathmatchRow = comp.get("original")
    if m is None or a is None:
        return ""

    flag = "🇰🇷" if comp.get("market") == "KR" else "🇺🇸"
    obs = " · <i>관측 모드</i>" if comp.get("observe_only") else ""
    logic = html.escape(str(comp.get("playbook_logic") or "—"), quote=False)

    m_wr = f" · 승률 {m.win_rate_pct:.1f}%" if m.win_rate_pct is not None else ""
    a_wr = f" · 승률 {a.win_rate_pct:.1f}%" if a.win_rate_pct is not None else ""
    lines = [
        "",
        f"{flag} <b>🧬 Ace DNA vs 오리지널 (데스매치 연동)</b>{obs}",
        f" 로직 <code>{logic}</code>",
        f" · <b>M (초신성/Ace DNA)</b>: {fmt_deathmatch_ret(m.mean_ret, m.n_closed, n_valid=m.n_valid)}{m_wr}",
        f" · <b>A (오리지널)</b>: {fmt_deathmatch_ret(a.mean_ret, a.n_closed, n_valid=a.n_valid)}{a_wr}",
        f" 💡 {html.escape(str(comp.get('verdict') or ''), quote=False)}",
    ]
    return "\n".join(lines) + "\n"


def compute_t1_feedback_win_rate(
    df_closed: pd.DataFrame,
    *,
    market: str,
    as_of_kst: str,
    playbook: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[float], int]:
    """KR fast-decay: playbook as_of 다음날 청산만 집계."""
    if df_closed is None or df_closed.empty or "exit_date" not in df_closed.columns:
        return None, 0
    try:
        from datetime import datetime, timedelta

        d0 = datetime.strptime(str(as_of_kst)[:10], "%Y-%m-%d")
        d1 = (d0 + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return None, 0

    work = df_closed.copy()
    work["_ed"] = work["exit_date"].astype(str).str[:10]
    day_df = work[work["_ed"] == d1]
    if day_df.empty:
        return None, 0

    mutant_df, _ = split_mutant_vs_original(day_df, market=market, playbook=playbook)
    if mutant_df.empty:
        return None, 0
    ret = _effective_final_ret_pct(mutant_df).dropna()
    if ret.empty:
        return None, 0
    wr = float((ret > 0).sum() / len(ret) * 100.0)
    return wr, int(len(ret))
