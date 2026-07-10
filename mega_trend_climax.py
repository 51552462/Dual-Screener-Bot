"""
Mega-Trend Climax Kill-Switch (3번) — 광기 종료 감지·언락 해제·섹터 포지션 방어 청산.

트리거 (OR, 섹터별 평가):
  · Flow Reversal — 스마트머니 순매도 역전 (Z 급락/음전/순매도 전환/누적 음수)
  · Climax Trap — 거래량 동반 하락 장악형 캔들 (섹터 프록시 OHLCV)

연동: smart_money_tracker · limit_up_forensics · exit_dynamics · forward/ledger
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    load_mega_trend_state,
    mega_trend_unlock_enabled,
    resolve_kr_code_sector,
)

MEGA_TREND_CLIMAX_EXIT_TAG = "MEGA_TREND_CLIMAX"


def _climax_config_base() -> Dict[str, Any]:
    def _f(key: str, default: float) -> float:
        try:
            return float(os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    return {
        "flow_reversal_z": _f("MEGA_TREND_FLOW_REVERSAL_Z", 0.0),
        "flow_z_drop_min": _f("MEGA_TREND_FLOW_Z_DROP_MIN", 1.5),
        "flow_outflow_krw_max": _f("MEGA_TREND_FLOW_OUTFLOW_KRW_MAX", 0.0),
        "climax_vol_shrink": _f("MEGA_TREND_CLIMAX_VOL_SHRINK", 0.85),
        "scale_out_fraction": _f("MEGA_TREND_CLIMAX_SCALE_OUT", 0.75),
        "scale_out_full_threshold": _f("MEGA_TREND_CLIMAX_SCALE_FULL_THRESH", 0.92),
        "proxy_top_n": int(os.environ.get("MEGA_TREND_CLIMAX_PROXY_TOP_N", "8")),
        "runner_defense_loss_pct": _f("MEGA_TREND_CLIMAX_RUNNER_LOSS_PCT", -1.5),
    }


def climax_config(sector: Optional[str] = None) -> Dict[str, Any]:
    """Climax Kill 임계치 — env + external-lane Kill RL delta (P5: sector overlay)."""
    base = _climax_config_base()
    try:
        from mega_trend_kill_rl import apply_kill_rl_climax_adjustments, load_kill_rl_state

        return apply_kill_rl_climax_adjustments(
            base, rl_state=load_kill_rl_state(), sector=sector
        )
    except Exception:
        return base


def _recent_trade_ymd() -> Optional[str]:
    try:
        from smart_money_tracker import _recent_trade_dates_yyyymmdd

        dates = _recent_trade_dates_yyyymmdd(1)
        return dates[-1] if dates else None
    except Exception:
        return None


def _detect_recent_flow_flip(
    sector: str,
    conn: Optional[sqlite3.Connection] = None,
    *,
    window_days: int = 5,
) -> Dict[str, Any]:
    """최근 2일 순매도 전환 — 이전 window 대비 매수→매도 flip."""
    from kr_flow_factor import aggregate_sector_flow_by_date
    from sector_taxonomy import map_standard_sector

    sec = map_standard_sector(sector, market="KR")
    out: Dict[str, Any] = {"flip": False, "reason": "", "recent_2d_krw": 0.0, "prior_krw": 0.0}
    by_date = aggregate_sector_flow_by_date(conn)
    dates = sorted(by_date.keys())
    if len(dates) < window_days + 2:
        out["reason"] = "insufficient_flow_dates"
        return out

    recent_2 = sum(float(by_date[d].get(sec, 0.0)) for d in dates[-2:])
    prior = sum(float(by_date[d].get(sec, 0.0)) for d in dates[-window_days - 2 : -2])
    out["recent_2d_krw"] = round(recent_2, 2)
    out["prior_krw"] = round(prior, 2)
    if prior > 0 and recent_2 < 0:
        out.update({"flip": True, "reason": "smart_money_2d_outflow_flip"})
    elif prior > 1e8 and recent_2 < prior * -0.15:
        out.update({"flip": True, "reason": "smart_money_flow_reversal_flip"})
    return out


def detect_sector_flow_reversal(
    sector: str,
    conn: Optional[sqlite3.Connection] = None,
    *,
    ignition_flow_z: Optional[float] = None,
) -> Dict[str, Any]:
    """스마트머니 순매도 역전 — Z 급락·음전·누적 순매도·단기 flip."""
    from kr_flow_factor import compute_sector_flow_zscore

    cfg = climax_config(sector=sector)
    flow = compute_sector_flow_zscore(sector, conn=conn)
    flip = _detect_recent_flow_flip(sector, conn=conn)
    z = flow.get("z_score")
    window_krw = flow.get("window_krw")
    out: Dict[str, Any] = {
        "reversal": False,
        "z_score": z,
        "window_krw": window_krw,
        "reason": "",
        "flow_detail": flow,
        "flow_flip": flip,
    }

    if flip.get("flip"):
        out.update({"reversal": True, "reason": str(flip.get("reason") or "flow_flip")})
        return out

    if window_krw is not None:
        try:
            wk = float(window_krw)
            if wk < float(cfg["flow_outflow_krw_max"]):
                out.update(
                    {
                        "reversal": True,
                        "reason": f"net_outflow_window_krw={wk:.0f}",
                    }
                )
                return out
        except (TypeError, ValueError):
            pass

    if z is None:
        out["reason"] = "insufficient_flow_data"
        return out

    z_f = float(z)
    rev_z = float(cfg["flow_reversal_z"])
    drop_min = float(cfg["flow_z_drop_min"])
    ign = float(ignition_flow_z) if ignition_flow_z is not None else None

    if z_f <= rev_z:
        out.update({"reversal": True, "reason": f"flow_z_crossed_below_{rev_z:g}"})
        return out
    if ign is not None and (ign - z_f) >= drop_min:
        out.update(
            {
                "reversal": True,
                "reason": f"flow_z_drop_{ign - z_f:.2f}_from_ignition_{ign:.2f}",
            }
        )
    return out


def _sector_proxy_codes(sector: str, trade_ymd: str, top_n: int) -> List[str]:
    try:
        from mega_trend_ignition import _fetch_market_turnover_block
    except Exception:
        return []

    rows: List[Dict[str, Any]] = []
    for mkt in ("KOSPI", "KOSDAQ"):
        block = _fetch_market_turnover_block(trade_ymd, mkt)
        if block.empty:
            continue
        sub = block[block["sector"] == sector]
        for _, r in sub.iterrows():
            rows.append({"code": r["code"], "tv": float(r["trade_value"])})
    rows.sort(key=lambda x: x["tv"], reverse=True)
    return [str(x["code"]).zfill(6) for x in rows[:top_n]]


def _fetch_ohlcv_preferred(code: str, *, use_forensics: bool = True) -> Optional[pd.DataFrame]:
    """limit_up_forensics OHLCV 우선, 폴백 FDR."""
    if use_forensics:
        try:
            from limit_up_forensics import _fetch_ohlcv_kr

            df = _fetch_ohlcv_kr(code)
            if df is not None and not df.empty:
                return df.tail(12).copy()
        except Exception:
            pass
    try:
        import FinanceDataReader as fdr
    except ImportError:
        return None
    try:
        df = fdr.DataReader(code, "KR")
        if df is None or df.empty:
            return None
        return df.tail(12).copy()
    except Exception:
        return None


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Date" in out.columns and out.index.name != "Date":
        out = out.set_index("Date")
    colmap: Dict[Any, str] = {}
    for c in out.columns:
        cl = str(c).lower()
        if cl in ("open", "시가"):
            colmap[c] = "open"
        elif cl in ("high", "고가"):
            colmap[c] = "high"
        elif cl in ("low", "저가"):
            colmap[c] = "low"
        elif cl in ("close", "종가"):
            colmap[c] = "close"
        elif "vol" in cl or "거래량" in str(c):
            colmap[c] = "volume"
    out = out.rename(columns=colmap)
    need = {"open", "high", "low", "close", "volume"}
    if not need.issubset(set(out.columns)):
        return pd.DataFrame()
    out = out[list(need)].astype(float)
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()]
    return out.sort_index()


def _aggregate_sector_proxy_bars(
    codes: Sequence[str],
    *,
    use_forensics: bool = True,
) -> pd.DataFrame:
    """날짜 정렬 후 섹터 프록시 OHLCV — 종목별 일자 매칭 평균."""
    buckets: Dict[pd.Timestamp, Dict[str, List[float]]] = {}
    cols = ("open", "high", "low", "close", "volume")
    for code in codes:
        raw = _fetch_ohlcv_preferred(code, use_forensics=use_forensics)
        if raw is None or raw.empty:
            continue
        norm = _normalize_ohlcv(raw)
        if norm.empty:
            continue
        for dt, row in norm.iterrows():
            ts = pd.Timestamp(dt).normalize()
            slot = buckets.setdefault(ts, {c: [] for c in cols})
            for c in cols:
                try:
                    slot[c].append(float(row[c]))
                except (TypeError, ValueError):
                    pass
    if not buckets:
        return pd.DataFrame()
    rows: List[Dict[str, float]] = []
    for dt in sorted(buckets.keys()):
        slot = buckets[dt]
        if not slot["close"]:
            continue
        rows.append({c: float(np.mean(slot[c])) for c in cols if slot[c]})
    return pd.DataFrame(rows)


def detect_climax_trap_from_bars(
    bars: pd.DataFrame,
    *,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """순수 OHLCV 바에서 Climax Trap 판정."""
    cfg = climax_config(sector=sector)
    out: Dict[str, Any] = {"climax_trap": False, "reason": ""}
    if bars is None or len(bars) < 3:
        out["reason"] = "insufficient_bars"
        return out

    prev = bars.iloc[-2]
    last = bars.iloc[-1]
    vol_shrink = float(cfg["climax_vol_shrink"])

    vol_decline = float(last["volume"]) < float(prev["volume"]) * vol_shrink
    bearish = float(last["close"]) < float(last["open"])
    engulf = (
        float(last["open"]) >= float(prev["close"])
        and float(last["close"]) <= float(prev["open"])
        and float(last["low"]) <= float(prev["low"])
    )
    price_down = float(last["close"]) < float(prev["close"])

    if vol_decline and bearish and engulf and price_down:
        out.update(
            {
                "climax_trap": True,
                "reason": "volume_shrink_bearish_engulfing",
                "volume_ratio": round(
                    float(last["volume"]) / max(1.0, float(prev["volume"])), 4
                ),
            }
        )
    else:
        out["reason"] = (
            f"no_trap vol_decline={vol_decline} bearish={bearish} "
            f"engulf={engulf} price_down={price_down}"
        )
    return out


def detect_sector_climax_trap(
    sector: str,
    trade_ymd: Optional[str] = None,
    *,
    use_forensics: bool = True,
) -> Dict[str, Any]:
    """거래량 동반 하락 장악형 캔들 — limit_up_forensics OHLCV 우선."""
    cfg = climax_config(sector=sector)
    ymd = trade_ymd or _recent_trade_ymd()
    out: Dict[str, Any] = {
        "climax_trap": False,
        "reason": "",
        "trade_date": ymd,
        "source": "forensics" if use_forensics else "fdr",
    }
    if not ymd:
        out["reason"] = "no_trade_date"
        return out

    codes = _sector_proxy_codes(sector, ymd, int(cfg["proxy_top_n"]))
    if len(codes) < 2:
        out["reason"] = "insufficient_proxy_codes"
        return out

    bars = _aggregate_sector_proxy_bars(codes, use_forensics=use_forensics)
    if bars is None or len(bars) < 3:
        out["reason"] = "insufficient_proxy_bars"
        return out

    trap = detect_climax_trap_from_bars(bars, sector=sector)
    out.update(trap)
    out["proxy_codes"] = codes
    return out


def evaluate_sector_climax(
    sector: str,
    state: Mapping[str, Any],
    conn: Optional[sqlite3.Connection] = None,
    *,
    trade_ymd: Optional[str] = None,
    use_forensics: bool = True,
) -> Dict[str, Any]:
    """단일 섹터 광기 종료 판정."""
    ign_z = state.get("flow_z") if str(state.get("primary_sector")) == str(sector) else None
    rev = detect_sector_flow_reversal(str(sector), conn=conn, ignition_flow_z=ign_z)
    trap = detect_sector_climax_trap(str(sector), trade_ymd=trade_ymd, use_forensics=use_forensics)
    kill = bool(rev.get("reversal")) or bool(trap.get("climax_trap"))
    exit_mode = "full" if trap.get("climax_trap") else "scale_out"
    reasons: List[str] = []
    if rev.get("reversal"):
        reasons.append(str(rev.get("reason") or "flow_reversal"))
    if trap.get("climax_trap"):
        reasons.append(str(trap.get("reason") or "climax_trap"))
    return {
        "kill": kill,
        "exit_mode": exit_mode,
        "sector": sector,
        "flow_reversal": rev,
        "climax_trap": trap,
        "reason": " | ".join(reasons) if reasons else "no_climax_signal",
    }


def evaluate_mega_trend_climax(
    state: Mapping[str, Any],
    conn: Optional[sqlite3.Connection] = None,
    *,
    trade_ymd: Optional[str] = None,
    use_forensics: bool = True,
) -> Dict[str, Any]:
    """활성 MEGA_TREND 모든 섹터 광기 종료 판정 — 하나라도 kill이면 발동."""
    if not isinstance(state, Mapping) or not state.get("active"):
        return {"kill": False, "reason": "mega_trend_inactive"}

    sectors = list(state.get("sectors") or [])
    primary = state.get("primary_sector")
    if primary and str(primary) not in [str(s) for s in sectors]:
        sectors.insert(0, str(primary))
    if not sectors:
        return {"kill": False, "reason": "no_sectors"}

    sector_verdicts: List[Dict[str, Any]] = []
    kill_sector: Optional[str] = None
    kill_verdict: Optional[Dict[str, Any]] = None

    for sec in sectors:
        v = evaluate_sector_climax(
            str(sec), state, conn, trade_ymd=trade_ymd, use_forensics=use_forensics
        )
        sector_verdicts.append(v)
        if v.get("kill") and kill_verdict is None:
            kill_verdict = v
            kill_sector = str(sec)

    if not kill_verdict:
        return {
            "kill": False,
            "reason": "no_climax_signal",
            "sector_verdicts": sector_verdicts,
            "sectors": sectors,
        }

    all_affected = list(
        {
            str(s)
            for s in sectors
            if any(
                sv.get("kill") and str(sv.get("sector")) == str(s)
                for sv in sector_verdicts
            )
        }
    )
    return {
        "kill": True,
        "exit_mode": kill_verdict.get("exit_mode", "full"),
        "sector": kill_sector,
        "sectors": all_affected or [kill_sector],
        "flow_reversal": kill_verdict.get("flow_reversal"),
        "climax_trap": kill_verdict.get("climax_trap"),
        "reason": kill_verdict.get("reason"),
        "sector_verdicts": sector_verdicts,
    }


def _resolve_exit_fraction(
    exit_mode: str,
    *,
    cur_ret_pct: float = 0.0,
    sector: Optional[str] = None,
) -> float:
    cfg = climax_config(sector=sector)
    if exit_mode == "full":
        return 1.0
    if exit_mode == "defensive_exit":
        try:
            from mega_trend_toxic_kill import resolve_defensive_exit_fraction

            return resolve_defensive_exit_fraction()
        except Exception:
            pass
    try:
        import exit_dynamics as xdyn

        # 광기 후반 Flow Reversal — 방어 국면 scale-out (exit_dynamics M1)
        base = xdyn.fluid_scale_out_fraction("HIGH_VOL", 10.0, 0.3)
        frac = max(float(cfg["scale_out_fraction"]), float(base))
        if cur_ret_pct >= 15.0:
            frac = max(frac, 0.65)
        return min(frac, float(cfg["scale_out_full_threshold"]))
    except Exception:
        return float(cfg["scale_out_fraction"])


def _close_status_from_ret(cur_ret: float) -> str:
    return "CLOSED_WIN" if float(cur_ret) >= 0 else "CLOSED_LOSS"


def liquidate_mega_trend_sector_positions(
    conn: sqlite3.Connection,
    sectors: Sequence[str],
    *,
    exit_mode: str = "full",
    exit_reason: str = "",
) -> Dict[str, Any]:
    """MEGA_TREND 섹터 OPEN 포지션 — 전량 청산 또는 강력 Scale-out (자본 즉시 축소)."""
    if not sectors:
        return {"liquidated": 0, "scaled": 0, "skipped": 0}

    sector_set = {str(s) for s in sectors}
    today = datetime.now().strftime("%Y-%m-%d")
    liquidated = 0
    scaled = 0
    skipped = 0
    last_frac = 0.0

    try:
        rows = conn.execute(
            """
            SELECT id, code, sector, sig_type, scaled_out_frac, sim_stat_ret,
                   free_runner, status, sim_kelly_invest, invest_amount,
                   realized_partial_ret
            FROM forward_trades
            WHERE market='KR' AND status='OPEN'
            """
        ).fetchall()
    except Exception as ex:
        return {"liquidated": 0, "scaled": 0, "skipped": 0, "error": str(ex)}

    for row in rows or []:
        (
            rid,
            code,
            sec_raw,
            sig,
            scaled_frac,
            sim_ret,
            free_runner,
            status,
            sk_inv,
            inv_amt,
            realized_partial,
        ) = row
        sec = resolve_kr_code_sector(code, sec_raw)
        if sec not in sector_set:
            skipped += 1
            continue

        cfg = climax_config(sector=sec)

        try:
            cur_ret = float(sim_ret or 0.0)
        except (TypeError, ValueError):
            cur_ret = 0.0
        try:
            invest = float(sk_inv or inv_amt or 0.0)
        except (TypeError, ValueError):
            invest = 0.0

        frac = _resolve_exit_fraction(exit_mode, cur_ret_pct=cur_ret, sector=sec)
        last_frac = frac
        rsn = exit_reason or f"{MEGA_TREND_CLIMAX_EXIT_TAG}_{exit_mode.upper()}"

        if exit_mode == "full" or frac >= float(cfg["scale_out_full_threshold"]):
            st = _close_status_from_ret(cur_ret)
            conn.execute(
                """
                UPDATE forward_trades
                SET status=?, exit_date=?, exit_reason=?, final_ret=?,
                    scaled_out_frac=1.0, free_runner=0,
                    sim_stat_status=?, sim_tech_status=?, sim_breadth_status=?
                WHERE id=?
                """,
                (st, today, rsn, cur_ret, st, st, st, rid),
            )
            liquidated += 1
        else:
            remain_mult = max(0.0, 1.0 - frac)
            new_invest = round(invest * remain_mult, 2)
            partial_component = round(cur_ret * frac, 4)
            new_realized = round(float(realized_partial or 0.0) + partial_component, 4)
            conn.execute(
                """
                UPDATE forward_trades
                SET scaled_out_frac=?, realized_partial_ret=?, free_runner=1,
                    sim_kelly_invest=?, invest_amount=?, exit_reason=?
                WHERE id=?
                """,
                (
                    round(frac, 4),
                    new_realized,
                    new_invest,
                    new_invest,
                    rsn + f"(scale_out_{frac:.0%})",
                    rid,
                ),
            )
            scaled += 1

    try:
        conn.commit()
    except Exception:
        pass

    return {
        "liquidated": liquidated,
        "scaled": scaled,
        "skipped": skipped,
        "exit_mode": exit_mode,
        "exit_fraction": last_frac,
    }


def _deactivate_mega_trend_state(
    state: Dict[str, Any],
    verdict: Mapping[str, Any],
) -> Dict[str, Any]:
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state["active"] = False
    state["rotation_advantage_active"] = False
    state["climax_kill_at"] = now_s
    state["climax_reason"] = verdict.get("reason")
    state["climax_exit_mode"] = verdict.get("exit_mode")
    state["climax_verdict"] = {
        k: verdict.get(k)
        for k in ("sector", "sectors", "exit_mode", "reason", "sector_verdicts")
        if k in verdict
    }
    state["deactivated_at"] = now_s
    return state


def refresh_mega_trend_climax_kill(
    config: Dict[str, Any],
    *,
    save_config_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    conn: Optional[sqlite3.Connection] = None,
    use_forensics: bool = True,
) -> Dict[str, Any]:
    """활성 MEGA_TREND 언락 섹터 광기 종료 감시 → 해제 + 포지션 방어."""
    if not mega_trend_unlock_enabled():
        return {"kill": False, "reason": "disabled"}

    state = load_mega_trend_state(config)
    if not state.get("active"):
        return {"kill": False, "reason": "mega_trend_inactive"}

    own_conn = False
    c = conn
    if c is None:
        try:
            import auto_forward_tester as aft

            c = sqlite3.connect(aft.DB_PATH, timeout=30)
            own_conn = True
        except Exception:
            c = None

    try:
        verdict = evaluate_mega_trend_climax(
            state, c, use_forensics=use_forensics
        )
        if not verdict.get("kill"):
            return verdict

        kill_ignited_at = state.get("ignited_at")
        state = _deactivate_mega_trend_state(dict(state), verdict)
        config[MEGA_TREND_CONFIG_KEY] = state

        liq: Dict[str, Any] = {"liquidated": 0, "scaled": 0}
        if c is not None:
            liq = liquidate_mega_trend_sector_positions(
                c,
                verdict.get("sectors") or [verdict.get("sector")],
                exit_mode=str(verdict.get("exit_mode") or "full"),
                exit_reason=f"{MEGA_TREND_CLIMAX_EXIT_TAG}: {verdict.get('reason', '')}",
            )

        if save_config_fn:
            save_config_fn(config)

        try:
            from mega_trend_kill_rl import (
                KILL_TYPE_CLIMAX,
                record_mega_trend_kill_event,
            )

            record_mega_trend_kill_event(
                config,
                sector=str(verdict.get("sector") or ""),
                kill_type=KILL_TYPE_CLIMAX,
                reason=str(verdict.get("reason") or ""),
                exit_mode=str(verdict.get("exit_mode") or "full"),
                ignited_at=str(kill_ignited_at or "") or None,
                snapshot={
                    "sectors": verdict.get("sectors"),
                    "sector_verdicts": verdict.get("sector_verdicts"),
                    "liquidation": liq,
                    "ignited_at": kill_ignited_at,
                    "climax_trap": verdict.get("climax_trap"),
                    "flow_reversal": verdict.get("flow_reversal"),
                },
            )
            if save_config_fn:
                save_config_fn(config)
        except Exception:
            pass

        print(
            f"🛑 [Mega-Trend Climax Kill] {verdict.get('sector')} — "
            f"{verdict.get('reason')} | "
            f"청산={liq.get('liquidated', 0)} · scale-out={liq.get('scaled', 0)}"
        )
        verdict["liquidation"] = liq
        verdict["state"] = state
        return verdict
    finally:
        if own_conn and c is not None:
            try:
                c.close()
            except Exception:
                pass


def run_climax_watch_from_forensics(
    config: Dict[str, Any],
    *,
    save_config_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Dict[str, Any]:
    """limit_up_forensics 일일 잡 전용 — forensics OHLCV 기반 climax 감시."""
    return refresh_mega_trend_climax_kill(
        config,
        save_config_fn=save_config_fn,
        use_forensics=True,
    )
