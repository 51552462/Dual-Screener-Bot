"""
오버드라이브(Overdrive) 텔레메트리 SSOT — v_energy × DYNAMIC_OD_HURDLE → 익절목표 ×1.10.

문제(Ch.3):
  · ledger 는 dyn_mfe_tp 에만 반영하고, 감사관은 exit_reason '오버드라이브' substring 만 집계
  · 0% 승률·전량 손절일 때 eligible=0 인데도 OVERDRIVE_SILENT 오탐
  · eligible 손절 건은 '가속 대상이었으나 익절 경로 미도달' 사실이 기록되지 않음

설계:
  · 진입: sig_type `#오버드라이브후보(v≥H)`
  · 청산 flow_tags: `#오버드라이브_가속` / `#오버드라이브_대상_손절` / `#오버드라이브_익절가속`
  · 감사: eligible / logged / loss_as_target / hurdle / v_energy 분포 기반 정밀 규칙
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

OD_TP_BOOST_MULT = 1.10
OD_FLOW_TAG_ACCEL = "#오버드라이브_가속"
OD_FLOW_TAG_LOSS_TARGET = "#오버드라이브_대상_손절"
OD_FLOW_TAG_WIN_TP = "#오버드라이브_익절가속"
OD_ENTRY_TAG_PREFIX = "#오버드라이브후보"

_OD_REASON_MARKERS = ("오버드라이브", "OD_TP")
_OD_LOGGED_FLOW_TAGS = (
    OD_FLOW_TAG_ACCEL,
    OD_FLOW_TAG_LOSS_TARGET,
    OD_FLOW_TAG_WIN_TP,
)

# 손절·시간청산 등 익절 가속과 무관한 exit_type (오버드라이브 미발동이 정상)
OD_LOSS_EXIT_TYPES = frozenset(
    {
        "STAT_MAE",
        "STAT_ATR",
        "STAT_TIME",
        "HYBRID_ATR",
        "HYBRID_TIME",
        "HYBRID_TECH",
        "TECH",
        "ZOMBIE_FORCE_CLOSE",
    }
)


def resolve_od_hurdle(sys_config: Optional[Dict[str, Any]]) -> float:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        return float(cfg.get("DYNAMIC_OD_HURDLE", 20.0) or 20.0)
    except (TypeError, ValueError):
        return 20.0


def evaluate_overdrive_eligibility(
    v_energy: Any,
    od_hurdle: Optional[float] = None,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """v_energy ≥ hurdle → eligible (익절목표 ×1.10 대상)."""
    hurdle = float(od_hurdle if od_hurdle is not None else resolve_od_hurdle(sys_config))
    try:
        ve = float(v_energy or 0.0)
    except (TypeError, ValueError):
        ve = 0.0
    eligible = ve >= hurdle
    return {
        "eligible": eligible,
        "v_energy": ve,
        "od_hurdle": hurdle,
        "tp_boost_mult": OD_TP_BOOST_MULT if eligible else 1.0,
        "margin": round(ve - hurdle, 4),
    }


def annotate_entry_overdrive_candidate(
    sig_type: str,
    v_energy: Any,
    sys_config: Optional[Dict[str, Any]] = None,
) -> str:
    """진입 sig_type 에 오버드라이브 후보 태그(중복 방지)."""
    sig = str(sig_type or "")
    if OD_ENTRY_TAG_PREFIX in sig:
        return sig
    ev = evaluate_overdrive_eligibility(v_energy, sys_config=sys_config)
    if ev["eligible"]:
        h = ev["od_hurdle"]
        ve = ev["v_energy"]
        return f"{sig} {OD_ENTRY_TAG_PREFIX}(v{ve:g}≥{h:g})"
    return sig


def _is_loss_sl_exit(
    final_ret: Any,
    exit_type: Any,
    exit_reason: Any,
) -> bool:
    try:
        ret = float(final_ret or 0.0)
    except (TypeError, ValueError):
        ret = 0.0
    if ret > 0:
        return False
    et = str(exit_type or "").strip().upper()
    if et in OD_LOSS_EXIT_TYPES:
        return True
    rsn = str(exit_reason or "")
    if any(k in rsn for k in ("손절", "MAE", "이탈", "방어", "ATR")):
        return True
    return ret <= 0


def _is_win_tp_exit(
    final_ret: Any,
    exit_type: Any,
    exit_reason: Any,
) -> bool:
    try:
        ret = float(final_ret or 0.0)
    except (TypeError, ValueError):
        ret = 0.0
    if ret <= 0:
        return False
    et = str(exit_type or "").strip().upper()
    if "MFE" in et or et in ("STAT_MFE_FULL",):
        return True
    rsn = str(exit_reason or "")
    return any(k in rsn for k in ("익절", "MFE", "슈팅", "추세 이탈 익절"))


def build_overdrive_exit_tags(
    *,
    is_overdrive_on: bool,
    v_energy: Any,
    od_hurdle: float,
    final_ret: Any,
    exit_type: Any,
    exit_reason: Any,
) -> List[str]:
    """청산 flow_tags 용 오버드라이브 태그(매매 결과 불변·텔레메트리 전용)."""
    if not is_overdrive_on:
        return []
    tags = [OD_FLOW_TAG_ACCEL]
    if _is_loss_sl_exit(final_ret, exit_type, exit_reason):
        tags.append(OD_FLOW_TAG_LOSS_TARGET)
    elif _is_win_tp_exit(final_ret, exit_type, exit_reason):
        tags.append(OD_FLOW_TAG_WIN_TP)
    return tags


def append_overdrive_exit_reason(
    exit_rsn: str,
    *,
    is_overdrive_on: bool,
    od_hurdle: float,
    dyn_mfe_tp_base: Optional[float] = None,
    dyn_mfe_tp_boosted: Optional[float] = None,
) -> str:
    """exit_reason 에 오버드라이브 사실만 덧붙임(청산 가격·ret 불변)."""
    rsn = str(exit_rsn or "")
    if not is_overdrive_on:
        return rsn
    if any(m in rsn for m in _OD_REASON_MARKERS):
        return rsn
    extra = f" [오버드라이브가속:v_energy≥{od_hurdle:g}]"
    if dyn_mfe_tp_base is not None and dyn_mfe_tp_boosted is not None:
        try:
            extra += (
                f" [OD_TP×{OD_TP_BOOST_MULT:g}:"
                f"{float(dyn_mfe_tp_base):g}→{float(dyn_mfe_tp_boosted):g}%]"
            )
        except (TypeError, ValueError):
            pass
    return rsn + extra


def _row_logged_overdrive(row: pd.Series) -> bool:
    rsn = str(row.get("exit_reason") or "")
    if any(m in rsn for m in _OD_REASON_MARKERS):
        return True
    ft = str(row.get("flow_tags") or "")
    return any(t in ft for t in _OD_LOGGED_FLOW_TAGS)


def _row_eligible(row: pd.Series, hurdle: float) -> bool:
    try:
        ve = float(row.get("v_energy") or 0.0)
    except (TypeError, ValueError):
        ve = 0.0
    return ve >= hurdle


def summarize_overdrive_closed_day(
    df_closed: pd.DataFrame,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    od_hurdle: Optional[float] = None,
) -> Dict[str, Any]:
    """
    당일 청산 프레임 → 오버드라이브 일일 요약(감사관 dossier 입력).
    """
    hurdle = float(od_hurdle if od_hurdle is not None else resolve_od_hurdle(sys_config))
    out: Dict[str, Any] = {
        "n_closed": 0,
        "od_hurdle": hurdle,
        "eligible_count": 0,
        "logged_count": 0,
        "loss_as_target_count": 0,
        "win_tp_accel_count": 0,
        "all_loss_sl_day": False,
        "supernova_closed_count": 0,
        "v_energy_max": None,
        "v_energy_p90": None,
        "telemetry_gap_count": 0,
    }
    if df_closed is None or df_closed.empty:
        return out

    work = df_closed.copy()
    out["n_closed"] = len(work)

    if "sig_type" in work.columns:
        out["supernova_closed_count"] = int(
            work["sig_type"].astype(str).str.contains("SUPERNOVA", na=False).sum()
        )

    ve_series = pd.to_numeric(
        work["v_energy"] if "v_energy" in work.columns else pd.Series(dtype=float),
        errors="coerce",
    ).dropna()
    if not ve_series.empty:
        out["v_energy_max"] = float(ve_series.max())
        out["v_energy_p90"] = float(ve_series.quantile(0.9))

    eligible_mask = work.apply(lambda r: _row_eligible(r, hurdle), axis=1)
    logged_mask = work.apply(_row_logged_overdrive, axis=1)
    out["eligible_count"] = int(eligible_mask.sum())
    out["logged_count"] = int(logged_mask.sum())
    out["telemetry_gap_count"] = int((eligible_mask & ~logged_mask).sum())

    loss_target = 0
    win_tp = 0
    all_loss_sl = True
    for _, row in work.iterrows():
        ret = row.get("final_ret")
        et = row.get("exit_type")
        er = row.get("exit_reason")
        if not _is_loss_sl_exit(ret, et, er):
            all_loss_sl = False
        if _row_eligible(row, hurdle):
            if _is_loss_sl_exit(ret, et, er):
                loss_target += 1
            elif _is_win_tp_exit(ret, et, er):
                win_tp += 1
    out["loss_as_target_count"] = loss_target
    out["win_tp_accel_count"] = win_tp
    out["all_loss_sl_day"] = all_loss_sl and out["n_closed"] > 0
    return out


def overdrive_audit_thresholds(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else cfg

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "min_closed": float(_i("OVERDRIVE_AUDIT_MIN_CLOSED", 3)),
        "min_eligible_gap": float(_i("OVERDRIVE_TELEMETRY_GAP_MIN", 1)),
        "hurdle_stale_ratio": _f("OVERDRIVE_HURDLE_STALE_RATIO", 0.60),
        "hurdle_stale_min_sn": float(_i("OVERDRIVE_HURDLE_STALE_MIN_SN", 3)),
        "catastrophic_wr_pct": _f("OVERSEER_WIN_RATE_CATASTROPHIC_PCT", 5.0),
    }


def detect_overdrive_audit_anomalies(
    summary: Dict[str, Any],
    *,
    win_rate_today_pct: Optional[float] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    """
    오버드라이브 감사 이상 목록 — OVERDRIVE_SILENT 오탐 제거·정밀 규칙.
    """
    th = overdrive_audit_thresholds(sys_config)
    n = int(summary.get("n_closed") or 0)
    eligible = int(summary.get("eligible_count") or 0)
    logged = int(summary.get("logged_count") or 0)
    gap = int(summary.get("telemetry_gap_count") or 0)
    hurdle = float(summary.get("od_hurdle") or resolve_od_hurdle(sys_config))
    all_loss_sl = bool(summary.get("all_loss_sl_day"))
    sn_n = int(summary.get("supernova_closed_count") or 0)
    ve_max = summary.get("v_energy_max")
    catastrophic = (
        win_rate_today_pct is not None
        and win_rate_today_pct <= th["catastrophic_wr_pct"]
    )

    out: List[Dict[str, str]] = []
    if n < th["min_closed"]:
        return out

    # 1) 텔레메트리 단선 — eligible 인데 기록 누락
    if gap >= th["min_eligible_gap"]:
        out.append(
            {
                "code": "OVERDRIVE_TELEMETRY_GAP",
                "severity": "CRITICAL" if gap >= 3 else "WARN",
                "headline": "오버드라이브 대상 청산인데 텔레메트리 누락",
                "evidence": (
                    f"eligible=<b>{eligible}</b> · logged=<b>{logged}</b> · "
                    f"gap=<b>{gap}</b> · hurdle=<b>{hurdle:g}</b>"
                ),
            }
        )

    # 2) 허들 과대 — 초신성 다수인데 v_energy 가 hurdle 에 미달
    if (
        eligible == 0
        and sn_n >= th["hurdle_stale_min_sn"]
        and ve_max is not None
        and float(ve_max) < hurdle * th["hurdle_stale_ratio"]
    ):
        out.append(
            {
                "code": "OVERDRIVE_HURDLE_STALE",
                "severity": "WARN",
                "headline": "DYNAMIC_OD_HURDLE 이 당일 v_energy 대비 과대",
                "evidence": (
                    f"hurdle=<b>{hurdle:g}</b> · v_max=<b>{float(ve_max):g}</b> · "
                    f"SN청산=<b>{sn_n}</b> — R&D 허들 동기화·DYNAMIC_OD_HURDLE 점검."
                ),
            }
        )

    # 3) 구 OVERDRIVE_SILENT — 좁은 조건만: eligible≥3·기록0·전량손절 아님·붕괴일 아님
    if (
        eligible >= 3
        and logged == 0
        and not all_loss_sl
        and not catastrophic
    ):
        out.append(
            {
                "code": "OVERDRIVE_SILENT",
                "severity": "WARN",
                "headline": "오버드라이브 대상 다수인데 가속 기록 0건",
                "evidence": (
                    f"eligible=<b>{eligible}</b> · logged=<b>0</b> · "
                    f"청산=<b>{n}</b> — ledger 텔레메트리·dyn_mfe_tp 경로 점검."
                ),
            }
        )

    # 4) 전량 손절+0 eligible → 정상 — OVERDRIVE_SILENT 억제 (7/8 오탐 해소)
    #    (OVERDRIVE_EXPECTED_IDLE 은 overseer_audit_binder 가 dossier 기반으로 별도 기록)

    return out
