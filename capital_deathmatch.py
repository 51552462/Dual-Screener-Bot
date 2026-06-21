"""
자금 관리 데스매치(고정 비중 vs 동적 켈리) — 청산 시계열 기반 MDD·비중·연패·방어 추정.

ReportStateBinder 와 책임 분리: 본 모듈은 forward_trades(청산 스냅샷)만 해석한다.
"""

from __future__ import annotations

import html
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StrategyPathMetrics:
    """단일 전략 경로 요약 — equity_t = R + cumsum(pnl), MDD는 피크 대비 비율."""

    cum_pnl: float
    mdd_pct: float
    avg_deploy_pct: float
    max_losing_streak: int
    ending_equity: float
    total_return_pct: float


@dataclass(frozen=True)
class StreakDefenseSlice:
    """
    마이크로 방어 윈도우(동일 부울 마스크) 집계.
    - 연패 마스크: streak_length = 최대 연속 손실 길이.
    - MDD 마스크: streak_length는 윈도우 청산 건수와 동일하게 둘 수 있음.
    cum_pnl_*: 구간 손익 합(음수면 순출혈).
    """

    streak_length: int
    n_trades_in_window: int
    avg_notional_fixed: float
    avg_notional_kelly: float
    cum_pnl_fixed: float
    cum_pnl_kelly: float


@dataclass(frozen=True)
class CapitalDeathmatchBlock:
    """
    mdd_diff_pp: 양(+)이면 켈리 MDD가 더 얕음(방어 우위), %p.
    defense_amount_est: (mdd_diff_pp/100)*R 근사.
    micro_defense: 리포트에 노출할 마이크로 방어 슬라이스(연패 또는 Peak→Trough 중 선택).
    micro_defense_kind: none | streak | mdd
    micro_defense_episode_mdd_pct: kind==mdd 일 때 고정 경로 해당 에피소드 최심 낙폭(%).
    """

    n_trades: int
    win_trades: int
    overall_win_rate_pct: float
    sort_key_used: str
    reference_capital: float
    fixed: StrategyPathMetrics
    kelly: StrategyPathMetrics
    mdd_diff_pp: float
    defense_amount_est: float
    micro_defense: Optional[StreakDefenseSlice] = None
    micro_defense_kind: str = "none"
    micro_defense_episode_mdd_pct: Optional[float] = None


def _safe_ref_capital(r: float) -> float:
    x = float(r or 0.0)
    return x if x > 1e-9 else 1e-9


def _gross_bleed(cum_pnl: float) -> float:
    """음수 PnL만 '출혈' 규모로 양수 환산."""
    v = float(cum_pnl)
    return float(-v) if v < 0.0 else 0.0


def _micro_defense_bleed_saved(sd: StreakDefenseSlice) -> float:
    """고정 출혈 − 켈리 출혈 (양수면 켈리 방어)."""
    return _gross_bleed(sd.cum_pnl_fixed) - _gross_bleed(sd.cum_pnl_kelly)


def _coerce_pnl_columns(
    df: pd.DataFrame,
    *,
    zero_invest_fallback: float,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    ret = pd.to_numeric(df.get("final_ret"), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    k_inv = pd.to_numeric(df.get("sim_kelly_invest"), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    f_inv = pd.to_numeric(df.get("invest_amount"), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if zero_invest_fallback is not None and float(zero_invest_fallback) > 0:
        z = float(zero_invest_fallback)
        k_inv = k_inv.replace(0.0, z)
        f_inv = f_inv.replace(0.0, z)
    k_pnl = k_inv * ret / 100.0
    f_pnl = f_inv * ret / 100.0
    return k_pnl, f_pnl, ret


def _equity_mdd_and_return(
    pnl: np.ndarray,
    *,
    reference_capital: float,
) -> Tuple[float, float, float, float]:
    """
    equity_t = R + cumsum(pnl_t); peak = cummax(equity); dd_t = equity/peak - 1.
    MDD% = -min(dd_t)*100 (양수 스칼라 = 낙폭 깊이). Pandas cumsum/cummax 사용.
    """
    R = _safe_ref_capital(reference_capital)
    pnl = np.asarray(pnl, dtype=np.float64)
    if pnl.size == 0:
        return 0.0, 0.0, float(R), 0.0
    s = pd.Series(pnl, dtype=np.float64)
    eq = R + s.cumsum()
    peak = eq.cummax()
    dd_ratio = np.where(peak.to_numpy(dtype=np.float64) > 0.0, (eq / peak).to_numpy(dtype=np.float64) - 1.0, 0.0)
    mdd_pct = float(-np.min(dd_ratio) * 100.0)
    cum_pnl = float(np.sum(pnl))
    end_pnl = float(eq.iloc[-1]) if len(eq) else float(R)
    tot_ret_pct = float(cum_pnl / R * 100.0)
    from reports.forward_report_scalar import scalar_float as _sf

    return (
        _sf(cum_pnl, 0.0),
        _sf(mdd_pct, 0.0),
        _sf(end_pnl, R),
        _sf(tot_ret_pct, 0.0),
    )


def _avg_deploy_pct(notional: np.ndarray, reference_capital: float) -> float:
    R = _safe_ref_capital(reference_capital)
    if notional.size == 0:
        return 0.0
    return float(np.mean(notional / R) * 100.0)


def _max_losing_streak(ret: np.ndarray) -> int:
    if ret.size == 0:
        return 0
    lose = (ret <= 0.0).astype(np.int32)
    padded = np.concatenate([[0], lose, [0]])
    d = np.diff(padded)
    starts = np.where(d == 1)[0]
    ends = np.where(d == -1)[0]
    if len(starts) == 0:
        return 0
    return int(np.max(ends - starts))


def _longest_consecutive_loss_mask(
    ret: np.ndarray,
    tiebreak_pnl: np.ndarray,
) -> Tuple[np.ndarray, int]:
    """
    연속 final_ret<=0 구간 중 길이가 최대인 구간을 선택.
    동률이면 tiebreak_pnl 구간 합이 더 작을수록(더 큰 고정측 출혈) 선택.
    반환: (부울 마스크, 해당 구간 길이). 연패 없으면 전부 False, 길이 0.
    """
    n = int(ret.size)
    mask = np.zeros(n, dtype=bool)
    if n == 0:
        return mask, 0
    ret = np.asarray(ret, dtype=np.float64)
    tiebreak_pnl = np.asarray(tiebreak_pnl, dtype=np.float64)
    best_len, best_tb, best_lo, best_hi = -1, np.inf, 0, 0
    i = 0
    while i < n:
        if ret[i] > 0.0:
            i += 1
            continue
        j = i
        while j < n and ret[j] <= 0.0:
            j += 1
        length = j - i
        tb = float(np.sum(tiebreak_pnl[i:j])) if j > i else 0.0
        if length > best_len or (length == best_len and tb < best_tb):
            best_len, best_tb, best_lo, best_hi = length, tb, i, j
        i = j
    if best_len <= 0:
        return mask, 0
    mask[best_lo:best_hi] = True
    return mask, int(best_len)


def _streak_metrics_pandas(
    loss_streak_mask: np.ndarray,
    streak_length: int,
    f_inv_a: np.ndarray,
    k_inv_a: np.ndarray,
    f_pnl: np.ndarray,
    k_pnl: np.ndarray,
) -> StreakDefenseSlice:
    """부울 마스크를 Pandas Series로 올려 동일 인덱스에서 노셔널·PnL 집계."""
    m = pd.Series(np.asarray(loss_streak_mask, dtype=bool), copy=False)
    if streak_length <= 0 or not bool(m.any()):
        return StreakDefenseSlice(0, 0, 0.0, 0.0, 0.0, 0.0)
    fiv = pd.Series(np.asarray(f_inv_a, dtype=np.float64))
    kiv = pd.Series(np.asarray(k_inv_a, dtype=np.float64))
    fpn = pd.Series(np.asarray(f_pnl, dtype=np.float64))
    kpn = pd.Series(np.asarray(k_pnl, dtype=np.float64))
    return StreakDefenseSlice(
        streak_length=streak_length,
        n_trades_in_window=int(m.sum()),
        avg_notional_fixed=float(fiv[m].mean()),
        avg_notional_kelly=float(kiv[m].mean()),
        cum_pnl_fixed=float(fpn[m].sum()),
        cum_pnl_kelly=float(kpn[m].sum()),
    )


def _normalize_micro_mask_mode(raw: str) -> str:
    m = str(raw or "auto").strip().lower()
    return m if m in ("auto", "streak", "mdd") else "auto"


def _pick_micro_defense(
    *,
    streak_slice: Optional[StreakDefenseSlice],
    mdd_slice: Optional[StreakDefenseSlice],
    mdd_episode_pct: float,
    mode: str,
) -> Tuple[Optional[StreakDefenseSlice], str, Optional[float]]:
    """출혈량 차이(고정−켈리)가 큰 쪽을 auto에서 우선. 동점 시 의미 있는 MDD%면 mdd 우선."""
    mode = _normalize_micro_mask_mode(mode)
    vs = streak_slice if streak_slice is not None and streak_slice.n_trades_in_window > 0 else None
    vm = mdd_slice if mdd_slice is not None and mdd_slice.n_trades_in_window > 0 else None
    ep = float(mdd_episode_pct) if vm is not None else None

    if mode == "streak":
        if vs is not None:
            return vs, "streak", None
        if vm is not None:
            return vm, "mdd", ep
        return None, "none", None

    if mode == "mdd":
        if vm is not None:
            return vm, "mdd", ep
        if vs is not None:
            return vs, "streak", None
        return None, "none", None

    # auto
    if vs is None and vm is None:
        return None, "none", None
    if vs is None:
        return vm, "mdd", ep
    if vm is None:
        return vs, "streak", None

    ss = _micro_defense_bleed_saved(vs)
    ms = _micro_defense_bleed_saved(vm)
    if ms > ss + 1e-6:
        return vm, "mdd", ep
    if ss > ms + 1e-6:
        return vs, "streak", None
    if float(mdd_episode_pct) >= 0.5:
        return vm, "mdd", ep
    return vs, "streak", None


class CapitalDeathmatchAnalyzer:
    """
    청산만 정렬된 프레임을 받아 두 전략 경로를 벡터화한다.
    """

    @staticmethod
    def _peak_to_trough_drawdown_mask(
        f_pnl: np.ndarray,
        *,
        reference_capital: float,
    ) -> Tuple[np.ndarray, float]:
        """
        고정(Fixed) 전략 에퀴티 곡선에서 글로벌 MDD가 만들어지는
        [전고점 시점 ~ 최저점(최대 낙폭) 시점] 구간을 True로 마스킹.
        Pandas cumsum / cummax 만 사용 (이미 메모리에 있는 PnL 배열).
        """
        R = _safe_ref_capital(reference_capital)
        pnl = pd.Series(np.asarray(f_pnl, dtype=np.float64), copy=False)
        n = int(len(pnl))
        mask = np.zeros(n, dtype=bool)
        if n == 0:
            return mask, 0.0
        eq = R + pnl.cumsum()
        peak = eq.cummax()
        pv = peak.to_numpy(dtype=np.float64, copy=False)
        ev = eq.to_numpy(dtype=np.float64, copy=False)
        dd_ratio = np.where(pv > 0.0, ev / pv - 1.0, 0.0)
        mdd_pct = float(-np.min(dd_ratio) * 100.0)
        if mdd_pct < 1e-12:
            return mask, 0.0
        trough_i = int(np.argmin(dd_ratio))
        window = ev[: trough_i + 1]
        peak_i = int(np.argmax(window))
        if peak_i > trough_i:
            peak_i = trough_i
        mask[peak_i : trough_i + 1] = True
        return mask, mdd_pct

    def __init__(
        self,
        *,
        reference_capital: float,
        zero_invest_fallback: float = 400000.0,
        micro_defense_mask: str = "auto",
    ) -> None:
        self.reference_capital = float(reference_capital)
        self.zero_invest_fallback = float(zero_invest_fallback)
        self.micro_defense_mask = _normalize_micro_mask_mode(micro_defense_mask)

    def analyze(self, df_closed: pd.DataFrame) -> CapitalDeathmatchBlock:
        if df_closed is None or df_closed.empty:
            z = StrategyPathMetrics(0.0, 0.0, 0.0, 0, self.reference_capital, 0.0)
            return CapitalDeathmatchBlock(
                n_trades=0,
                win_trades=0,
                overall_win_rate_pct=0.0,
                sort_key_used="(empty)",
                reference_capital=self.reference_capital,
                fixed=z,
                kelly=z,
                mdd_diff_pp=0.0,
                defense_amount_est=0.0,
                micro_defense=None,
                micro_defense_kind="none",
                micro_defense_episode_mdd_pct=None,
            )

        df = df_closed.copy()
        sort_parts = []
        if "exit_date" in df.columns:
            df["_sort_exit"] = pd.to_datetime(df["exit_date"], errors="coerce")
            sort_parts.append("_sort_exit")
        else:
            df["_sort_exit"] = pd.NaT
        if "id" in df.columns:
            df["_sort_id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0.0)
            sort_parts.append("_sort_id")
        else:
            df["_sort_id"] = np.arange(len(df), dtype=np.float64)
            sort_parts.append("_sort_id")

        asc = [True] * len(sort_parts)
        df = df.sort_values(by=sort_parts, ascending=asc, na_position="last").reset_index(drop=True)
        sort_key_used = ",".join(["exit_date" if c == "_sort_exit" else "id" for c in sort_parts])

        k_pnl_s, f_pnl_s, ret_s = _coerce_pnl_columns(
            df,
            zero_invest_fallback=self.zero_invest_fallback,
        )
        k_pnl = np.nan_to_num(k_pnl_s.to_numpy(dtype=np.float64, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
        f_pnl = np.nan_to_num(f_pnl_s.to_numpy(dtype=np.float64, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
        ret = np.nan_to_num(ret_s.to_numpy(dtype=np.float64, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
        k_inv = pd.to_numeric(df.get("sim_kelly_invest"), errors="coerce").fillna(0.0)
        f_inv = pd.to_numeric(df.get("invest_amount"), errors="coerce").fillna(0.0)
        if self.zero_invest_fallback > 0:
            z = float(self.zero_invest_fallback)
            k_inv = k_inv.replace(0.0, z)
            f_inv = f_inv.replace(0.0, z)

        f_inv_a = f_inv.to_numpy(dtype=np.float64, copy=False)
        k_inv_a = k_inv.to_numpy(dtype=np.float64, copy=False)
        R = self.reference_capital

        loss_streak_mask, streak_len = _longest_consecutive_loss_mask(ret, f_pnl)
        streak_slice: Optional[StreakDefenseSlice] = None
        if streak_len > 0 and np.any(loss_streak_mask):
            streak_slice = _streak_metrics_pandas(
                loss_streak_mask,
                streak_len,
                f_inv_a,
                k_inv_a,
                f_pnl,
                k_pnl,
            )

        mdd_mask, mdd_episode_pct = CapitalDeathmatchAnalyzer._peak_to_trough_drawdown_mask(
            f_pnl,
            reference_capital=R,
        )
        mdd_slice: Optional[StreakDefenseSlice] = None
        if np.any(mdd_mask):
            nw_m = int(np.sum(mdd_mask))
            mdd_slice = _streak_metrics_pandas(
                mdd_mask,
                nw_m,
                f_inv_a,
                k_inv_a,
                f_pnl,
                k_pnl,
            )

        mask_mode = _normalize_micro_mask_mode(os.environ.get("DEATHMATCH_MICRO_MASK", self.micro_defense_mask))
        micro_sel, micro_kind, micro_ep = _pick_micro_defense(
            streak_slice=streak_slice,
            mdd_slice=mdd_slice,
            mdd_episode_pct=mdd_episode_pct,
            mode=mask_mode,
        )
        k_cum, k_mdd, k_end, k_tr = _equity_mdd_and_return(k_pnl, reference_capital=R)
        f_cum, f_mdd, f_end, f_tr = _equity_mdd_and_return(f_pnl, reference_capital=R)

        k_metrics = StrategyPathMetrics(
            cum_pnl=k_cum,
            mdd_pct=k_mdd,
            avg_deploy_pct=_avg_deploy_pct(k_inv.to_numpy(dtype=np.float64, copy=False), R),
            max_losing_streak=_max_losing_streak(ret),
            ending_equity=k_end,
            total_return_pct=k_tr,
        )
        f_metrics = StrategyPathMetrics(
            cum_pnl=f_cum,
            mdd_pct=f_mdd,
            avg_deploy_pct=_avg_deploy_pct(f_inv.to_numpy(dtype=np.float64, copy=False), R),
            max_losing_streak=_max_losing_streak(ret),
            ending_equity=f_end,
            total_return_pct=f_tr,
        )

        mdd_diff_pp = float(f_metrics.mdd_pct - k_metrics.mdd_pct)
        defense_amount_est = max(0.0, mdd_diff_pp) / 100.0 * _safe_ref_capital(R)

        n = int(len(df))
        wins = int(np.sum(ret > 0.0))
        wr = float(wins / n * 100.0) if n > 0 else 0.0

        return CapitalDeathmatchBlock(
            n_trades=n,
            win_trades=wins,
            overall_win_rate_pct=wr,
            sort_key_used=sort_key_used,
            reference_capital=float(R),
            fixed=f_metrics,
            kelly=k_metrics,
            mdd_diff_pp=mdd_diff_pp,
            defense_amount_est=defense_amount_est,
            micro_defense=micro_sel,
            micro_defense_kind=micro_kind,
            micro_defense_episode_mdd_pct=micro_ep,
        )


class DeathmatchNarrativeBuilder:
    """지표 → 텔레그램 HTML(부분 서브셋)."""

    @staticmethod
    def to_telegram_html(
        *,
        market_icon: str,
        block: CapitalDeathmatchBlock,
        subtitle: str = "(정규직 로직 한정)",
    ) -> str:
        b = block
        lines: list[str] = []
        lines.append(
            f"{market_icon} <b>[3/9] 자금 관리 전략 데스매치</b> <i>{html.escape(subtitle, quote=False)}</i>\n"
        )
        lines.append(
            f"📊 표본: 총 {b.n_trades}전 {b.win_trades}승 "
            f"(승률 {b.overall_win_rate_pct:.1f}%) · 정렬: <code>{html.escape(b.sort_key_used, quote=False)}</code>\n"
        )
        lines.append(f"⚓ 기준 시드 R={b.reference_capital:,.0f} 원 (MDD·비중 % 동일 분모)\n\n")

        fk = b.fixed
        kk = b.kelly
        from reports.forward_report_scalar import fmt_money, fmt_pct, scalar_float

        mkt = "US" if "🇺🇸" in str(market_icon) else "KR"
        lines.append(
            f"🛡️ <b>[고정 비중]</b> 누적 <b>{fmt_money(fk.cum_pnl, market=mkt, signed=True)}</b> | "
            f"총수익률 {fmt_pct(fk.total_return_pct)} | MDD <b>{fmt_pct(fk.mdd_pct, signed=False)}</b> | "
            f"평균 투입 {scalar_float(fk.avg_deploy_pct):.2f}% | 최대 연패 <b>{fk.max_losing_streak}</b>회\n"
        )
        lines.append(
            f"💰 <b>[동적 켈리]</b> 누적 <b>{fmt_money(kk.cum_pnl, market=mkt, signed=True)}</b> | "
            f"총수익률 {fmt_pct(kk.total_return_pct)} | MDD <b>{fmt_pct(kk.mdd_pct, signed=False)}</b> | "
            f"평균 투입 {scalar_float(kk.avg_deploy_pct):.2f}% | 최대 연패 <b>{kk.max_losing_streak}</b>회\n"
        )

        if b.micro_defense is not None and b.micro_defense.n_trades_in_window > 0:
            lines.append(
                DeathmatchNarrativeBuilder._micro_defense_html(
                    b.micro_defense,
                    kind=b.micro_defense_kind,
                    episode_mdd_pct=b.micro_defense_episode_mdd_pct,
                )
            )
            lines.append("\n")

        lines.append("🧭 <b>[팩트 브리핑]</b>\n")
        lines.append(DeathmatchNarrativeBuilder._dynamic_commentary(b))
        return "".join(lines)

    @staticmethod
    def _dynamic_commentary(b: CapitalDeathmatchBlock) -> str:
        if b.n_trades == 0:
            return "<i>청산 표본이 없어 비교할 수 없습니다.</i>\n"

        fk = b.fixed
        kk = b.kelly
        parts: list[str] = []

        deploy_delta = fk.avg_deploy_pct - kk.avg_deploy_pct
        if abs(deploy_delta) >= 0.05:
            kelly_dir = "낮게" if deploy_delta > 0 else "높게"
            deploy_suffix = (
                f" — 켈리가 평균 {abs(deploy_delta):.2f}%p 더 {kelly_dir} 배팅했습니다."
            )
        else:
            deploy_suffix = " (실질적으로 유사)."
        parts.append(
            f"평균 투입 비중은 고정 경로 <b>{fk.avg_deploy_pct:.2f}%</b>, "
            f"켈리 경로 <b>{kk.avg_deploy_pct:.2f}%</b>{deploy_suffix}"
        )
        parts.append(
            f" 동일 시드 대비 MDD는 고정 <b>{fk.mdd_pct:.2f}%</b> vs 켈리 <b>{kk.mdd_pct:.2f}%</b>"
            f"{f', 차이 <b>{b.mdd_diff_pp:+.2f}%p</b>' if abs(b.mdd_diff_pp) >= 0.01 else ''}."
        )
        if b.defense_amount_est > 0 and b.mdd_diff_pp > 0.01:
            parts.append(
                f"피크 대비 낙폭 기준으로 보면 켈리 쪽이 깊이를 <b>{b.mdd_diff_pp:.2f}%p</b> 얕게 유지했고, "
                f"이를 시드 금액으로 환산하면 대략 <b>{b.defense_amount_est:,.0f} 원</b> 수준의 방어 여유로 해석할 수 있습니다."
            )
        elif b.mdd_diff_pp < -0.01:
            parts.append(
                f"이번 구간에서는 켈리 경로의 MDD가 고정 대비 <b>{-b.mdd_diff_pp:.2f}%p</b> 더 깊었습니다 — "
                "변동 구간에서 켈리 배팅이 과열되었는지 점검할 만합니다."
            )

        if fk.max_losing_streak != kk.max_losing_streak:
            parts.append(
                f"최장 연패는 고정 <b>{fk.max_losing_streak}</b>회, 켈리 <b>{kk.max_losing_streak}</b>회입니다."
            )
        else:
            parts.append(
                f"최장 연패(청산 순서·승패 기준)는 양 경로 모두 <b>{fk.max_losing_streak}</b>회입니다 "
                f"(동일 <code>final_ret</code> 시퀀스를 공유)."
            )

        eps = 0.02 * _safe_ref_capital(b.reference_capital)
        pnl_gap = kk.cum_pnl - fk.cum_pnl
        if pnl_gap > eps:
            parts.append("누적 손익은 켈리 경로가 우위입니다.")
        elif pnl_gap < -eps:
            parts.append("누적 손익은 고정 경로가 우위입니다.")
        else:
            parts.append("누적 손익은 두 경로가 근접합니다.")

        return " ".join(parts) + "\n"

    @staticmethod
    def _micro_defense_html(
        sd: StreakDefenseSlice,
        *,
        kind: str,
        episode_mdd_pct: Optional[float],
    ) -> str:
        kind = (kind or "streak").strip().lower()
        bleed_f = _gross_bleed(sd.cum_pnl_fixed)
        bleed_k = _gross_bleed(sd.cum_pnl_kelly)
        parts: list[str] = []
        if kind == "mdd":
            em = float(episode_mdd_pct) if episode_mdd_pct is not None else 0.0
            parts.append(
                f"🎯 <b>[최대 MDD 하락 구간 · 마스킹]</b> "
                f"고정 경로 Peak→Trough · 최심 낙폭 약 <b>{em:.2f}%</b> · "
                f"<b>{sd.n_trades_in_window}</b>건 윈도우.\n"
            )
        else:
            parts.append(
                f"🎯 <b>[최장 연패 구간 · 마스킹]</b> "
                f"청산 순서상 최대 <b>{sd.streak_length}</b>연패(<b>{sd.n_trades_in_window}</b>건) 윈도우.\n"
            )
        parts.append(
            f" ▪ 평균 노셔널: 고정 <b>{sd.avg_notional_fixed:,.0f}</b>원 vs 켈리 <b>{sd.avg_notional_kelly:,.0f}</b>원\n"
        )
        parts.append(
            f" ▪ 구간 누적 손익: 고정 <b>{sd.cum_pnl_fixed:+,.0f}</b>원 "
            f"(순출혈 약 <b>{bleed_f:,.0f}</b>원) | "
            f"켈리 <b>{sd.cum_pnl_kelly:+,.0f}</b>원 (순출혈 약 <b>{bleed_k:,.0f}</b>원)\n"
        )
        avg_gap = sd.avg_notional_fixed - sd.avg_notional_kelly
        bleed_saved = bleed_f - bleed_k
        story: list[str] = []
        kelly_overheat = bleed_k > bleed_f + 1.0 and sd.avg_notional_kelly > sd.avg_notional_fixed * 1.02
        win_lab = "이 Peak→Trough 하락 구간에서" if kind == "mdd" else f"특히 이 <b>{sd.streak_length}</b>연패 구간에서"

        if bleed_saved > 1.0:
            if kind == "mdd":
                story.append(
                    f"{win_lab} 고정은 거래당 평균 <b>{sd.avg_notional_fixed:,.0f}</b>원을 투입해 누적 "
                    f"<b>{sd.cum_pnl_fixed:+,.0f}</b>원(출혈 약 <b>{bleed_f:,.0f}</b>원)을 기록한 반면, "
                    f"켈리는 평균 투입을 <b>{sd.avg_notional_kelly:,.0f}</b>원으로 두어 "
                    f"누적 <b>{sd.cum_pnl_kelly:+,.0f}</b>원(출혈 약 <b>{bleed_k:,.0f}</b>원)으로 "
                    f"동일 청산 시퀀스에서 노출을 수축시켰습니다."
                )
            else:
                story.append(
                    f"{win_lab} 고정은 거래당 평균 "
                    f"<b>{sd.avg_notional_fixed:,.0f}</b>원을 투입해 누적 "
                    f"<b>{sd.cum_pnl_fixed:+,.0f}</b>원(출혈 약 <b>{bleed_f:,.0f}</b>원)을 기록한 반면, "
                    f"켈리는 평균 투입을 <b>{sd.avg_notional_kelly:,.0f}</b>원으로 두어 "
                    f"누적 <b>{sd.cum_pnl_kelly:+,.0f}</b>원(출혈 약 <b>{bleed_k:,.0f}</b>원)으로 "
                    f"동일 승패열에서 노출을 수축시켰습니다."
                )
            story.append(f"출혈 규모 기준 약 <b>{bleed_saved:,.0f}</b>원의 차이입니다.")
        elif kelly_overheat:
            lab = "이 하락·연패 구간" if kind == "mdd" else "이 연패 구간"
            story.append(
                f"{lab}에서는 켈리가 고정보다 평균 노셔널·출혈이 컸습니다 "
                f"(추가 출혈 약 <b>{bleed_k - bleed_f:,.0f}</b>원). 메타 켈리 승수·레짐을 점검하세요."
            )
        elif sd.streak_length >= 1 and abs(avg_gap) >= 1.0:
            story.append(
                f"평균 노셔널 차이(고정−켈리)는 거래당 <b>{avg_gap:+,.0f}</b>원이며, "
                f"구간 손익은 고정 <b>{sd.cum_pnl_fixed:+,.0f}</b>원 vs 켈리 <b>{sd.cum_pnl_kelly:+,.0f}</b>원입니다."
            )
        else:
            story.append(
                "해당 마스킹 구간에서 두 전략의 평균 노셔널·구간 손익이 크게 벌어지지 않았습니다."
            )
        return "".join(parts) + "🗣️ <i>" + " ".join(story) + "</i>\n"
