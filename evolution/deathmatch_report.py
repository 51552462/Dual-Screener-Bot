"""
[9/9] N-Way 시스템 데스매치 — forward_trades 청산 기준 동적 로직군 랭킹.

- 하드코딩 A/B 2축 폐기 → sig_type 기반 로직군 자동 분류·집계
- final_ret 결측·문자열 혼입 시 profit_amount/invest_amount 로 보조 산출
- nan 비교 시 '동일' 오판 방지
"""
from __future__ import annotations

import html
import math
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def deathmatch_min_n(sys_config: dict) -> int:
    v = sys_config.get("DEATHMATCH_MIN_TRADES_PER_ARM", 5)
    try:
        n = int(v)
    except (TypeError, ValueError):
        n = 5
    return max(1, n)


def deathmatch_min_n_for_market(
    sys_config: dict,
    market: str,
    *,
    n_closed: int = 0,
) -> int:
    """
    US 등 표본이 적을 때 arm별 최소 건수 완화 (0건 메시지와 구분).
    DEATHMATCH_MIN_TRADES_PER_ARM_US 로 시장별 override 가능.
    """
    base = deathmatch_min_n(sys_config)
    m = str(market or "").upper()
    if m == "US":
        try:
            us_override = sys_config.get("DEATHMATCH_MIN_TRADES_PER_ARM_US")
            if us_override is not None:
                return max(1, int(us_override))
        except (TypeError, ValueError):
            pass
        if 0 < n_closed < base * 3:
            return max(2, min(base, n_closed // max(1, 2) or 2))
    return base


def classify_strategy_arm(sig_type: Any) -> Optional[str]:
    """
    sig_type → 로직군 라벨. INCUBATOR·빈 값 제외.
    우선순위: UD → BEAST → 초신성 코어 → SUPERNOVA 일반 → 블랙홀 → STANDARD → 기타(원문 축약).
    """
    s = str(sig_type or "").strip()
    if not s or "INCUBATOR" in s.upper():
        return None
    try:
        from evolution.fluid_evolution_bridge import is_fluid_scout_sig

        if is_fluid_scout_sig(s):
            return None
    except Exception:
        if "SCOUT" in s.upper() or "🔭" in s:
            return None
    su = s.upper()
    if "UNDERDOG" in su:
        return "UD (언더독)"
    if "SUPERNOVA_BEAST" in su or ("BEAST" in su and "SUPERNOVA" in su):
        return "C (야수/BEAST)"
    if "SUPERNOVA_COSINE" in su or "SUPERNOVA_MLBOX" in su or "SUPERNOVA" in su:
        return "B (초신성)"
    if "BLACKHOLE" in su or "BLACK_HOLE" in su:
        return "BH (블랙홀)"
    if "STANDARD" in su:
        return "A (오리지널)"
    clean = re.sub(r"\[.*?\]", "", s).strip()
    clean = re.sub(r"\s+", " ", clean)[:36]
    return f"기타·{clean}" if clean else "기타·미분류"


def _clean_final_ret_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)
    raw = df["final_ret"] if "final_ret" in df.columns else pd.Series(dtype=float)
    if raw.dtype == object or raw.astype(str).str.contains("%", na=False).any():
        as_str = raw.astype(str).str.replace("%", "", regex=False).str.strip()
        out = pd.to_numeric(as_str, errors="coerce")
    else:
        out = pd.to_numeric(raw, errors="coerce")
    return out


def _effective_final_ret_pct(df: pd.DataFrame) -> pd.Series:
    """final_ret 우선; 유효 숫자 부족 시 profit_amount/invest_amount 로 역산(%)."""
    base = _clean_final_ret_series(df)
    n_valid = int(base.notna().sum())
    if n_valid >= max(1, len(df) // 3):
        return base
    if "profit_amount" in df.columns and "invest_amount" in df.columns:
        inv = pd.to_numeric(df["invest_amount"], errors="coerce").replace(0, np.nan)
        pnl = pd.to_numeric(df["profit_amount"], errors="coerce")
        derived = (pnl / inv) * 100.0
        merged = base.copy()
        merged = merged.fillna(derived)
        return merged
    return base


def _profit_factor_from_ret(ret: pd.Series) -> Optional[float]:
    from reports.forward_report_scalar import profit_factor_from_returns

    r = ret.dropna()
    if r.empty:
        return None
    pf = profit_factor_from_returns(r)
    return float(pf) if pf > 0 else None


@dataclass
class ArmDeathmatchRow:
    label: str
    n_closed: int
    n_valid: int
    mean_ret: Optional[float]
    win_rate_pct: Optional[float]
    profit_factor: Optional[float]
    rank: int = 0


@dataclass
class NWayDeathmatchResult:
    arms: List[ArmDeathmatchRow] = field(default_factory=list)
    verdict: str = ""
    allocation_note: str = ""
    n_min: int = 5


def build_arm_row(label: str, df_arm: pd.DataFrame) -> ArmDeathmatchRow:
    n_closed = int(len(df_arm))
    ret = _effective_final_ret_pct(df_arm)
    valid = ret.dropna()
    n_valid = int(len(valid))
    mean_ret: Optional[float] = None
    win_rate: Optional[float] = None
    pf: Optional[float] = None
    if n_valid > 0:
        m = float(valid.mean())
        if math.isfinite(m):
            mean_ret = m
        win_rate = float((valid > 0).sum() / n_valid * 100.0)
        pf = _profit_factor_from_ret(valid)
    return ArmDeathmatchRow(
        label=label,
        n_closed=n_closed,
        n_valid=n_valid,
        mean_ret=mean_ret,
        win_rate_pct=win_rate,
        profit_factor=pf,
    )


def build_nway_deathmatch(
    df_closed: pd.DataFrame,
    sys_config: Optional[dict] = None,
    *,
    lookback_days: Optional[int] = None,
    market: Optional[str] = None,
) -> NWayDeathmatchResult:
    """
    Registry Battle Royal → NWayDeathmatchResult (호환 래퍼).
  """
    from evolution.deathmatch_battle_royale import run_battle_royal, battle_royal_to_nway

    br = run_battle_royal(
        df_closed,
        sys_config,
        market=str(market or "KR").upper(),
        lookback_days=lookback_days,
        persist=True,
    )
    return battle_royal_to_nway(br)


def build_nway_deathmatch_legacy_sigtype(
    df_closed: pd.DataFrame,
    sys_config: Optional[dict] = None,
    *,
    lookback_days: Optional[int] = None,
    market: Optional[str] = None,
) -> NWayDeathmatchResult:
    """sig_type 로직군 분류 N-Way (폴백·대시보드용)."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    n_closed = len(df_closed) if df_closed is not None else 0
    n_min = deathmatch_min_n_for_market(cfg, market or "", n_closed=n_closed)
    out = NWayDeathmatchResult(n_min=n_min)

    if df_closed is None or df_closed.empty or "sig_type" not in df_closed.columns:
        out.verdict = "청산 표본 없음 — 데스매치 보류"
        return out

    work = df_closed.copy()
    if lookback_days is not None and lookback_days > 0 and "exit_date" in work.columns:
        cutoff = (pd.Timestamp.now() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        work = work[work["exit_date"].astype(str) >= cutoff]

    work["_arm"] = work["sig_type"].map(classify_strategy_arm)
    work = work[work["_arm"].notna()]

    if work.empty:
        out.verdict = "분류 가능한 로직군 청산 0건"
        return out

    arms: List[ArmDeathmatchRow] = []
    for label, grp in work.groupby("_arm", sort=False):
        arms.append(build_arm_row(str(label), grp.drop(columns=["_arm"], errors="ignore")))

    def _sort_key(a: ArmDeathmatchRow) -> Tuple[int, float]:
        if a.mean_ret is None or not math.isfinite(a.mean_ret):
            return (0, -1e18)
        return (1, float(a.mean_ret))

    arms.sort(key=_sort_key, reverse=True)
    for i, a in enumerate(arms, start=1):
        a.rank = i
    out.arms = arms
    out.verdict = nway_deathmatch_verdict(arms, n_min)
    out.allocation_note = format_allocation_proposal_note(arms, n_min, cfg)
    return out


def nway_deathmatch_verdict(arms: List[ArmDeathmatchRow], n_min: int, *, eps: float = 0.05) -> str:
    """
    1위 vs 꼴찌 — 양쪽 모두 유효 평균·표본 충족 시에만 승패 서술.
    nan·단일축만 있으면 '동일' 문구 금지.
    """
    eligible = [
        a
        for a in arms
        if a.n_valid >= n_min and a.mean_ret is not None and math.isfinite(float(a.mean_ret))
    ]
    if len(eligible) == 0:
        labels = ", ".join(f"{a.label}({a.n_closed})" for a in arms[:6])
        return f"유효 표본 부족 — 전 로직군 최소 {n_min}건·숫자 수익률 필요 ({labels})"
    if len(eligible) == 1:
        a = eligible[0]
        return f"단일 유효 축: <b>{html.escape(a.label, quote=False)}</b> 평균 {a.mean_ret:+.2f}% (N={a.n_valid})"

    eligible.sort(key=lambda x: float(x.mean_ret), reverse=True)
    top, bot = eligible[0], eligible[-1]
    diff = float(top.mean_ret) - float(bot.mean_ret)

    if abs(diff) <= eps:
        return (
            f"표본 충족 {len(eligible)}개 축 — 1위·꼴찌 평균 차이 {diff:+.2f}%p로 "
            f"<b>{html.escape(top.label, quote=False)}</b> vs "
            f"<b>{html.escape(bot.label, quote=False)}</b> 사실상 동률"
        )
    return (
        f"N-Way 결승: 1위 <b>{html.escape(top.label, quote=False)}</b> "
        f"{top.mean_ret:+.2f}% (N={top.n_valid}) ▶ 꼴찌 "
        f"<b>{html.escape(bot.label, quote=False)}</b> {bot.mean_ret:+.2f}% "
        f"(격차 {diff:+.2f}%p)"
    )


def fmt_deathmatch_ret(mean_ret: Optional[float], n_rows: int, *, n_valid: Optional[int] = None) -> str:
    if n_rows <= 0:
        return "산출 불가 (청산 0건)"
    nv = n_valid if n_valid is not None else n_rows
    if nv <= 0 or mean_ret is None or not math.isfinite(float(mean_ret)):
        return "N/A (유효 수익률 없음)"
    return f"{float(mean_ret):+.2f}%"


def format_nway_deathmatch_telegram(
    market_icon: str,
    result: NWayDeathmatchResult,
    *,
    lookback_label: str = "전체 청산",
) -> str:
    """Telegram HTML [9/9] 본문."""
    lines = [
        f"{market_icon} <b>[9/9] 시스템 데스매치 결산 (N-Way)</b>",
        f"📎 기준: {html.escape(lookback_label, quote=False)} · "
        f"비교 최소 각 <b>{result.n_min}</b>건 (유효 수익률)",
        "",
        "<b>🏆 로직군 랭킹 (평균 청산 수익률)</b>",
    ]
    if not result.arms:
        lines.append(" ↳ 청산·분류 가능 표본 없음")
    else:
        for a in result.arms:
            ret_s = fmt_deathmatch_ret(a.mean_ret, a.n_closed, n_valid=a.n_valid)
            wr_s = f"{a.win_rate_pct:.1f}%" if a.win_rate_pct is not None else "—"
            pf_s = f"{a.profit_factor:.2f}" if a.profit_factor is not None else "—"
            rank_icon = "🥇" if a.rank == 1 else f"{a.rank}."
            lines.append(
                f" {rank_icon} <b>{html.escape(a.label, quote=False)}</b>: "
                f"{ret_s} · N={a.n_closed}(유효{a.n_valid}) · 승률 {wr_s} · PF {pf_s}"
            )
    lines.append("")
    lines.append(f"💡 <b>결론:</b> {result.verdict}")
    if result.allocation_note:
        lines.append("")
        lines.append(result.allocation_note)
    return "\n".join(lines) + "\n"


def compute_allocation_proposal(
    arms: List[ArmDeathmatchRow],
    n_min: int,
    sys_config: Optional[dict] = None,
    *,
    bottom_pct: float = 0.2,
    top_boost_mult: float = 1.25,
    standby_mult: float = 0.0,
) -> Dict[str, Any]:
    """
    하위 20% STANDBY · 상위 가중 제안 (실행은 DEATHMATCH_APPLY_ALLOCATION=1 일 때만).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        bottom_pct = float(cfg.get("DEATHMATCH_BOTTOM_PCT", bottom_pct))
    except (TypeError, ValueError):
        pass
    bottom_pct = min(0.5, max(0.05, bottom_pct))

    ranked = [
        a
        for a in arms
        if a.n_valid >= n_min and a.mean_ret is not None and math.isfinite(float(a.mean_ret))
    ]
    ranked.sort(key=lambda x: float(x.mean_ret), reverse=True)
    n = len(ranked)
    if n == 0:
        return {"standby_labels": [], "boost_labels": [], "weight_mult": {}, "eligible_n": 0}

    n_bottom = max(1, int(math.ceil(n * bottom_pct)))
    standby = [a.label for a in ranked[-n_bottom:]]
    boost = [a.label for a in ranked[: max(1, n - n_bottom)]]

    weight_mult: Dict[str, float] = {}
    for a in arms:
        if a.label in standby:
            weight_mult[a.label] = standby_mult
        elif a.label in boost:
            weight_mult[a.label] = top_boost_mult
        else:
            weight_mult[a.label] = 1.0

    return {
        "standby_labels": standby,
        "boost_labels": boost,
        "weight_mult": weight_mult,
        "eligible_n": n,
        "bottom_pct": bottom_pct,
    }


def format_allocation_proposal_note(
    arms: List[ArmDeathmatchRow],
    n_min: int,
    sys_config: Optional[dict] = None,
) -> str:
    prop = compute_allocation_proposal(arms, n_min, sys_config)
    if prop.get("eligible_n", 0) < 2:
        return (
            "<i>💼 자본 재분배 제안: 유효 축 부족 — "
            "DEATHMATCH_APPLY_ALLOCATION 적용 보류</i>"
        )
    st = prop.get("standby_labels") or []
    bt = prop.get("boost_labels") or []
    st_s = html.escape(", ".join(st[:5]), quote=False) if st else "—"
    bt_s = html.escape(", ".join(bt[:5]), quote=False) if bt else "—"
    apply_on = str(
        (sys_config or {}).get("DEATHMATCH_APPLY_ALLOCATION", os.environ.get("DEATHMATCH_APPLY_ALLOCATION", "0"))
    ).strip().lower() in ("1", "true", "yes", "on")
    mode = "자동 연동 ON" if apply_on else "관측 전용(OFF)"
    return (
        f"💼 <b>[자본주의 데스매치 제안 · {mode}]</b>\n"
        f" ▪ 하위 {int(prop.get('bottom_pct', 0.2) * 100)}% STANDBY 후보: <i>{st_s}</i>\n"
        f" ▪ 상위 축 비중 ↑ 후보: <i>{bt_s}</i>\n"
        f" ▪ 실행: <code>DEATHMATCH_APPLY_ALLOCATION=1</code> → "
        f"<code>META_GROUP_KELLY_MULT</code> (group_key × health overlay)"
    )


def apply_allocation_proposal_to_config(
    sys_config: dict,
    proposal: Dict[str, Any],
    *,
    save: bool = True,
) -> dict:
    """제안을 system_config 에 기록. DEATHMATCH_APPLY_ALLOCATION=1 일 때만 OMS 가 소비."""
    from config_manager import save_system_config

    cfg = dict(sys_config)
    cfg["DEATHMATCH_ALLOCATION_PROPOSAL"] = {
        "standby_labels": list(proposal.get("standby_labels") or []),
        "boost_labels": list(proposal.get("boost_labels") or []),
        "weight_mult": dict(proposal.get("weight_mult") or {}),
    }
    cfg["DEATHMATCH_ALLOCATION_AS_OF"] = pd.Timestamp.now().strftime("%Y-%m-%d")
    if save:
        save_system_config(cfg)
    return cfg


def maybe_apply_deathmatch_allocation(
    result: NWayDeathmatchResult,
    sys_config: dict,
    *,
    battle_royale: Any = None,
    market: Optional[str] = None,
) -> None:
    """
    DEATHMATCH_APPLY_ALLOCATION=1:
      - battle_royale 제공 시 P2 루프(META_GROUP_KELLY_MULT 연동)
      - 없으면 레거시 label 기반 제안만 config 저장
    """
    flag = str(
        sys_config.get("DEATHMATCH_APPLY_ALLOCATION", os.environ.get("DEATHMATCH_APPLY_ALLOCATION", "1"))
    ).strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        return
    if battle_royale is not None:
        try:
            from evolution.deathmatch_allocation import maybe_apply_deathmatch_allocation_p2

            maybe_apply_deathmatch_allocation_p2(
                battle_royale, result, sys_config, market=market
            )
            return
        except Exception as ex:
            import logging

            logging.getLogger(__name__).warning(
                "Deathmatch P2 allocation failed, legacy fallback: %s", ex
            )
    prop = compute_allocation_proposal(result.arms, result.n_min, sys_config)
    apply_allocation_proposal_to_config(sys_config, prop, save=True)


# --- 레거시 A/B API (bitget·구 호출자 호환) ---

def arm_mean_final_ret(df_arm: pd.DataFrame) -> Tuple[Optional[float], int, int]:
    row = build_arm_row("_legacy", df_arm)
    return row.mean_ret, row.n_closed, row.n_valid


def deathmatch_ab_verdict(
    n_std: int,
    n_sn: int,
    std_ret: Optional[float],
    sn_ret: Optional[float],
    n_min: int,
) -> str:
    """구 2축 API — 내부적으로 N-Way 판정으로 위임."""
    a_fin = std_ret is not None and math.isfinite(float(std_ret))
    b_fin = sn_ret is not None and math.isfinite(float(sn_ret))
    arms = [
        ArmDeathmatchRow("A (오리지널)", n_std, n_std if a_fin else 0, std_ret, None, None, 0),
        ArmDeathmatchRow("B (초신성)", n_sn, n_sn if b_fin else 0, sn_ret, None, None, 0),
    ]
    return nway_deathmatch_verdict(arms, n_min)
