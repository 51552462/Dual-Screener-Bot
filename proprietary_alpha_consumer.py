"""
Alpha Consumer Layer — Offline R&D 산출물(HIDDEN_SPILLOVER_THEME_*) → 실시간 스코어 반영.

외부 API 없음: system_config · meta_governor_state · 로컬 JSON 아티팩트만 소비.
"""
from __future__ import annotations

import glob
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

_KELLY_FLOOR = 0.5
_KELLY_CEIL = 1.0

# Regime별 프리미엄 상한 (고정 1.05 금지 — meta 레짐이 결정)
_REGIME_CAP: Dict[str, float] = {
    "BULL": 1.15,
    "CHOP": 1.05,
    "SIDEWAYS": 1.05,
    "WHIPSAW": 1.05,
    "BEAR": 1.02,
    "HIGH_VOL": 1.02,
    "UNKNOWN": 1.03,
}

_MATCH_KIND_SCALE: Dict[str, float] = {
    "ticker": 1.0,
    "sector_spillover": 0.88,
    "sector": 0.78,
    "sector_fuzzy": 0.72,
}


@dataclass(frozen=True)
class FluidThemePremium:
    """calculate_fluid_theme_premium() 산출 — 레짐·켈리·테마 신뢰도 합성."""

    regime_key: str
    regime_cap: float
    global_kelly_mult: float
    kelly_decay: float
    theme_confidence: float
    match_kind: str
    boost_mult: float
    log_line: str = ""

    @property
    def active(self) -> bool:
        return self.boost_mult > 1.0001


@dataclass(frozen=True)
class HiddenThemeContext:
    """스캔 배치당 1회 로드 — HIDDEN_SPILLOVER_THEME_{KR|US}."""

    market: str
    active: bool = False
    tickers: frozenset = field(default_factory=frozenset)
    sector_hint: str = ""
    confidence: float = 0.0
    method: str = ""
    source: str = ""
    theme_key: str = ""
    meta_snapshot: Dict[str, Any] = field(default_factory=dict)


def _resolve_meta(meta: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(meta, dict) and meta:
        return dict(meta)
    try:
        from meta_governor_consumer import load_meta_state_resolved

        return load_meta_state_resolved() or {}
    except Exception:
        try:
            from meta_governor import load_meta_governor_state

            return load_meta_governor_state() or {}
        except Exception:
            return {}


def _normalize_regime_for_premium(regime_raw: Any) -> str:
    u = str(regime_raw or "").strip().upper()
    if u in _REGIME_CAP:
        return u
    if u in ("CHOP", "WHIPSAW"):
        return "CHOP"
    if u in ("SIDEWAYS",):
        return "SIDEWAYS"
    return "UNKNOWN"


def _regime_cap_mult(regime_key: str) -> float:
    return float(_REGIME_CAP.get(regime_key, _REGIME_CAP["UNKNOWN"]))


def _kelly_decay_strength(global_kelly_mult: float) -> float:
    """
    META_GLOBAL_KELLY_MULT 곱셈 역산 감쇠.
    gkm=1.0 → 1.0 (풀 프리미엄), gkm≤0.5 → 0.0 (1.0x 수렴).
    """
    try:
        g = float(global_kelly_mult)
    except (TypeError, ValueError):
        g = 1.0
    span = max(1e-6, _KELLY_CEIL - _KELLY_FLOOR)
    return max(0.0, min(1.0, (g - _KELLY_FLOOR) / span))


def _confidence_strength(confidence: float) -> float:
    """테마 confidence → [0,1] 연속 스케일 (고정 배수 없음)."""
    try:
        c = float(confidence)
    except (TypeError, ValueError):
        c = 0.5
    return max(0.0, min(1.0, (c - 0.35) / 0.60))


def calculate_fluid_theme_premium(
    *,
    theme_confidence: float,
    match_kind: str = "ticker",
    meta: Optional[Mapping[str, Any]] = None,
) -> FluidThemePremium:
    """
    meta_governor_state 기반 유동 프리미엄 배수.

    - Regime: BULL cap 1.15 · CHOP 1.05 · BEAR/HIGH_VOL 1.02
    - Kelly: META_GLOBAL_KELLY_MULT 역산 감쇠 → 연속 손실 시 1.0x 수렴
    """
    m = _resolve_meta(meta)
    regime_raw = m.get("META_REGIME_KEY", "UNKNOWN")
    regime_key = _normalize_regime_for_premium(regime_raw)
    regime_cap = _regime_cap_mult(regime_key)

    try:
        gkm = float(m.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    except (TypeError, ValueError):
        gkm = 1.0

    if str(m.get("META_TREASURY_MODE") or "").upper() == "DEFENSE":
        gkm = min(gkm, max(_KELLY_FLOOR, gkm * 0.92))

    kelly_decay = _kelly_decay_strength(gkm)
    conf_s = _confidence_strength(theme_confidence)
    kind = str(match_kind or "ticker").lower()
    match_scale = float(_MATCH_KIND_SCALE.get(kind, 0.75))

    uplift_budget = (regime_cap - 1.0) * conf_s * match_scale
    uplift_applied = uplift_budget * kelly_decay
    boost_mult = 1.0 + uplift_applied

    log_line = (
        f"🌊 [Fluid Premium] Regime: {regime_key}, "
        f"Kelly: {gkm:.3f} (decay {kelly_decay:.2f}), "
        f"Boost: {boost_mult:.2f}x"
    )

    return FluidThemePremium(
        regime_key=regime_key,
        regime_cap=regime_cap,
        global_kelly_mult=gkm,
        kelly_decay=kelly_decay,
        theme_confidence=float(theme_confidence),
        match_kind=kind,
        boost_mult=round(boost_mult, 4),
        log_line=log_line,
    )


def _artifact_root() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "artifacts", "proprietary_rnd")


def _normalize_ticker(code: object, market: str) -> str:
    raw = str(code or "").strip().upper()
    raw = re.sub(r"\.(US|KS|KQ)$", "", raw)
    if str(market).upper() == "KR":
        digits = re.sub(r"\D", "", raw)
        return digits.zfill(6) if digits else raw
    return raw


def _normalize_sector(sector: str) -> str:
    s = str(sector or "").strip()
    if not s or s in ("기타/혼합", "유망섹터 포착", "Unknown"):
        return ""
    try:
        from sector_spillover_refresh import map_standard_sector

        return str(map_standard_sector(s) or s).strip()
    except Exception:
        return s


def _load_theme_dict(cfg: Mapping[str, Any], market: str) -> Tuple[Dict[str, Any], str]:
    mk = str(market or "KR").upper()
    key = f"HIDDEN_SPILLOVER_THEME_{mk}"
    theme = cfg.get(key)
    if isinstance(theme, dict) and theme.get("tickers"):
        return theme, key

    pattern = os.path.join(_artifact_root(), f"hidden_theme_{mk}_*.json")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    for path in files[:3]:
        try:
            with open(path, encoding="utf-8") as fh:
                loaded = json.load(fh)
            if isinstance(loaded, dict) and loaded.get("tickers"):
                return loaded, key
        except (OSError, json.JSONDecodeError):
            continue
    return {}, key


def load_hidden_theme_context(
    cfg: Mapping[str, Any],
    market: str,
    *,
    meta: Optional[Mapping[str, Any]] = None,
) -> HiddenThemeContext:
    """HIDDEN_SPILLOVER_THEME_* — config 우선, 없으면 최신 로컬 아티팩트."""
    mk = str(market or "KR").upper()
    theme, theme_key = _load_theme_dict(cfg, mk)
    meta_snap = _resolve_meta(meta)
    if not theme:
        return HiddenThemeContext(
            market=mk, active=False, theme_key=theme_key, meta_snapshot=meta_snap
        )

    raw_tickers = theme.get("tickers") or []
    norm_tickers = frozenset(
        _normalize_ticker(t, mk) for t in raw_tickers if str(t or "").strip()
    )
    try:
        conf = float(theme.get("confidence", 0.5) or 0.5)
    except (TypeError, ValueError):
        conf = 0.5

    return HiddenThemeContext(
        market=mk,
        active=bool(norm_tickers or theme.get("sector_hint")),
        tickers=norm_tickers,
        sector_hint=_normalize_sector(str(theme.get("sector_hint") or "")),
        confidence=conf,
        method=str(theme.get("method") or ""),
        source="config_or_artifact",
        theme_key=theme_key,
        meta_snapshot=meta_snap,
    )


def _theme_match(
    ctx: HiddenThemeContext,
    *,
    ticker_code: str,
    sector: str,
    cfg: Optional[Mapping[str, Any]] = None,
) -> Tuple[bool, str]:
    """(matched, match_kind) — ticker 직접 매칭 우선."""
    if not ctx.active:
        return False, ""

    code_n = _normalize_ticker(ticker_code, ctx.market)
    if code_n and code_n in ctx.tickers:
        return True, "ticker"

    sec_n = _normalize_sector(sector)
    if not sec_n or not ctx.sector_hint:
        return False, ""

    if sec_n == ctx.sector_hint:
        return True, "sector"

    if ctx.market == "KR" and cfg is not None:
        try:
            from cross_market_ssot import kr_stock_matches_spillover

            if kr_stock_matches_spillover(sec_n, dict(cfg)):
                spill = str(
                    (cfg.get("US_ZERO_SAMPLE_SPILLOVER") or {}).get("sector_std")
                    or cfg.get("US_SPILLOVER_SECTOR")
                    or ""
                ).strip()
                if spill and _normalize_sector(spill) == ctx.sector_hint:
                    return True, "sector_spillover"
        except Exception as ex:
            logger.debug("hidden theme spillover match: %s", ex)

    if ctx.sector_hint in sec_n or sec_n in ctx.sector_hint:
        return True, "sector_fuzzy"
    return False, ""


def apply_hidden_theme_score_boost(
    base_score: float,
    *,
    ctx: Optional[HiddenThemeContext],
    ticker_code: str,
    sector: str,
    market: str,
    cfg: Optional[Mapping[str, Any]] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> Tuple[float, float, str, str]:
    """
    히든 테마 매칭 시 calculate_fluid_theme_premium() 배수 적용.
    Hard cap: 가산분 ≤ base_score × (regime_cap - 1).
    Returns: (boosted_score, mult_applied, tag, fluid_log_line)
    """
    try:
        score = float(base_score)
    except (TypeError, ValueError):
        return float(base_score or 0.0), 1.0, "", ""

    if score <= 0.0 or ctx is None or not ctx.active:
        return score, 1.0, "", ""

    matched, kind = _theme_match(
        ctx, ticker_code=ticker_code, sector=sector, cfg=cfg
    )
    if not matched:
        return score, 1.0, "", ""

    meta_use = meta if meta is not None else ctx.meta_snapshot
    premium = calculate_fluid_theme_premium(
        theme_confidence=ctx.confidence,
        match_kind=kind,
        meta=meta_use,
    )
    mult = float(premium.boost_mult)
    if mult <= 1.0001:
        return score, 1.0, "", premium.log_line

    regime_uplift_cap = premium.regime_cap
    boosted_raw = score * mult
    hard_cap = score * regime_uplift_cap
    boosted = min(boosted_raw, hard_cap)
    tag = f"HIDDEN_THEME_{kind.upper()}"
    return round(boosted, 4), round(mult, 4), tag, premium.log_line
