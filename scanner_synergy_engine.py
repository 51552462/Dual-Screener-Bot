"""
스캐너 진입단 시너지 — 거시(Macro)·한미 스필오버·순환매가 코사인/ML 컷오프·점수에 직접 반영.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from macro_context_snapshot import (
    ENABLE_MACRO_SYNERGY_WEIGHTING_KEY,
    build_macro_context_snapshot,
    compute_clamped_synergy_multiplier,
)

logger = logging.getLogger(__name__)

# 컷오프 완화 하한 (과도한 완화 방지)
_MIN_COS_MULT = 0.72
_MIN_ML_MULT = 0.75
_MAX_SCORE_BONUS = 12.0


@dataclass(frozen=True)
class ScanSynergyContext:
    """스캔 배치당 1회 로드."""
    market: str
    macro_snapshot: Dict[str, Any]
    macro_mult: float = 1.0
    predicted_sector: str = ""
    spillover_sector: str = ""


@dataclass(frozen=True)
class ScanSynergyAdjustments:
    cos_cutoff_mult: float = 1.0
    ml_cutoff_mult: float = 1.0
    score_bonus_pts: float = 0.0
    tags: tuple = field(default_factory=tuple)


def macro_synergy_enabled(cfg: Mapping[str, Any]) -> bool:
    return bool(cfg.get(ENABLE_MACRO_SYNERGY_WEIGHTING_KEY, True))


def load_scan_synergy_context(
    cfg: Mapping[str, Any],
    market: str,
) -> ScanSynergyContext:
    mk = str(market).upper()
    snap: Dict[str, Any] = {}
    macro_mult = 1.0
    if macro_synergy_enabled(cfg):
        try:
            snap = build_macro_context_snapshot(cfg)
            macro_mult, _ = compute_clamped_synergy_multiplier(
                snap, "", cfg
            )
        except Exception as ex:
            logger.warning("macro synergy snapshot skip: %s", ex)
    pred = str(cfg.get(f"PREDICTED_NEXT_SECTOR_{mk}", "") or "").strip()
    spill = ""
    try:
        from macro_context_snapshot import _resolve_effective_us_spillover_sector

        spill = _resolve_effective_us_spillover_sector(cfg)
    except Exception:
        spill = str(cfg.get("US_SPILLOVER_SECTOR", "") or "").strip()
    return ScanSynergyContext(
        market=mk,
        macro_snapshot=snap,
        macro_mult=float(macro_mult) if macro_mult > 0 else 1.0,
        predicted_sector=pred,
        spillover_sector=spill,
    )


def per_ticker_scan_adjustments(
    ctx: ScanSynergyContext,
    *,
    sector: str,
    cfg: Mapping[str, Any],
) -> ScanSynergyAdjustments:
    """
    cos/ml_cutoff_mult < 1 이면 합격 문턱 완화. score_bonus_pts 는 final_score 가산.
    """
    cos_m = 1.0
    ml_m = 1.0
    bonus = 0.0
    tags: list = []

    sec = str(sector or "").strip()
    if sec in ("", "기타/혼합", "유망섹터 포착"):
        sec_norm = ""
    else:
        sec_norm = sec

    if macro_synergy_enabled(cfg) and ctx.macro_mult != 1.0:
        # 배수>1 → 컷 완화, <1 → 약간 강화
        relax = max(_MIN_COS_MULT, min(1.0, 1.0 / ctx.macro_mult))
        cos_m = min(cos_m, relax)
        ml_m = min(ml_m, max(_MIN_ML_MULT, relax))
        if ctx.macro_mult > 1.02:
            bonus += min(4.0, (ctx.macro_mult - 1.0) * 40.0)
            tags.append("MACRO_SYNERGY")

    if sec_norm and ctx.predicted_sector and sec_norm == ctx.predicted_sector:
        cos_m *= 0.85
        ml_m *= 0.85
        bonus += 3.0
        tags.append("ROTATION_PRE")

    spill_match = False
    if ctx.market == "KR" and sec_norm:
        try:
            from cross_market_ssot import kr_stock_matches_spillover

            spill_match = kr_stock_matches_spillover(sec_norm, dict(cfg))
        except Exception as ex:
            logger.debug("spillover scan match: %s", ex)
            spill_match = (
                bool(ctx.spillover_sector)
                and ctx.spillover_sector not in ("NONE", "분석중", "")
                and sec_norm == ctx.spillover_sector
            )
    if spill_match:
        cos_m *= 0.90
        ml_m *= 0.90
        bonus += 4.0
        tags.append("SPILLOVER_PRE")

    if sec_norm and ctx.macro_snapshot:
        try:
            _, meta = compute_clamped_synergy_multiplier(
                ctx.macro_snapshot, sec_norm, cfg
            )
            if meta.get("components"):
                bonus += min(3.0, len(meta["components"]) * 1.5)
        except Exception:
            pass

    cos_m = max(_MIN_COS_MULT, cos_m)
    ml_m = max(_MIN_ML_MULT, ml_m)
    bonus = min(_MAX_SCORE_BONUS, bonus)
    return ScanSynergyAdjustments(
        cos_cutoff_mult=cos_m,
        ml_cutoff_mult=ml_m,
        score_bonus_pts=bonus,
        tags=tuple(tags),
    )
