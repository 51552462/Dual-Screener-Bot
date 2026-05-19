"""
LLM·통계 Playbook 검증 — 팩트 범위 밖 rule 폐기, confidence 산출.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from ace_evolution_clamp import compute_dynamic_multiplier_bounds
from ace_evolution_schema import SCHEMA_VERSION, default_playbook
from ace_evolution_ttl import apply_ttl_to_playbook, default_ttl_days
from ace_text_sanitize import (
    sanitize_human_insight,
    sanitize_noun_phrase,
    sanitize_playbook_text_fields,
    sanitize_theme_tokens,
)


def _parse_json_blob(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    t = text.strip()
    m = re.search(r"\{[\s\S]*\}", t)
    if m:
        t = m.group(0)
    try:
        obj = json.loads(t)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def _feature_bounds(fact_pack: Dict[str, Any], column: str) -> Optional[Tuple[float, float]]:
    """top3 + feature_stats에서 대략적 min/max."""
    vals: List[float] = []
    for row in fact_pack.get("top3_trades") or []:
        if isinstance(row, dict) and row.get(column) is not None:
            try:
                vals.append(float(row[column]))
            except (TypeError, ValueError):
                pass
    for fs in fact_pack.get("feature_stats") or []:
        if isinstance(fs, dict) and fs.get("column") == column:
            for part in (fs.get("ace_summary", ""),):
                nums = re.findall(r"[-+]?\d*\.?\d+", str(part))
                for n in nums[:4]:
                    try:
                        vals.append(float(n))
                    except ValueError:
                        pass
    if not vals:
        return None
    return min(vals) * 0.85, max(vals) * 1.25


def validate_rules(
    rules: List[Dict[str, Any]],
    fact_pack: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rules or []):
        if not isinstance(r, dict):
            continue
        rt = str(r.get("type") or "")
        if rt == "feature_band":
            col = str(r.get("column") or "")
            bounds = _feature_bounds(fact_pack, col)
            if not bounds:
                continue
            try:
                val = float(r.get("value"))
            except (TypeError, ValueError):
                continue
            lo, hi = bounds
            if val < lo or val > hi:
                continue
            try:
                bonus = float(r.get("bonus", 0.03))
            except (TypeError, ValueError):
                bonus = 0.03
            out.append(
                {
                    "id": r.get("id") or f"r{i+1}",
                    "type": "feature_band",
                    "column": col,
                    "op": str(r.get("op") or "gte"),
                    "value": val,
                    "bonus": min(0.06, max(0.01, bonus)),
                }
            )
        elif rt == "theme_token":
            clean = sanitize_theme_tokens(r.get("tokens"))
            if not clean:
                continue
            try:
                bonus = float(r.get("bonus", 0.02))
            except (TypeError, ValueError):
                bonus = 0.02
            out.append(
                {
                    "id": r.get("id") or f"r{i+1}",
                    "type": "theme_token",
                    "tokens": clean,
                    "match": str(r.get("match") or "any"),
                    "bonus": min(0.04, max(0.01, bonus)),
                }
            )
        elif rt == "logic_match":
            pat = sanitize_noun_phrase(r.get("pattern") or fact_pack.get("logic_core") or "", max_chars=24)
            if not pat:
                continue
            out.append(
                {
                    "id": r.get("id") or f"r{i+1}",
                    "type": "logic_match",
                    "pattern": pat,
                    "priority_rank": int(r.get("priority_rank", -1)),
                }
            )
    return out[:5]


def stats_only_playbook(fact_pack: Dict[str, Any], *, observe_only: bool = True) -> Dict[str, Any]:
    market = str(fact_pack.get("market") or "KR").upper()
    pb = default_playbook(market)
    pb.update(
        {
            "schema_version": SCHEMA_VERSION,
            "market": market,
            "logic_core": str(fact_pack.get("logic_core") or ""),
            "as_of_kst": str(fact_pack.get("as_of_kst") or ""),
            "window_days": int(fact_pack.get("window_days") or 14),
            "n_ace": int(fact_pack.get("n_ace") or 0),
            "n_baseline": int(fact_pack.get("n_baseline") or 0),
            "observe_only": observe_only,
            "provenance": {"source": "stats_only"},
        }
    )
    rules: List[Dict[str, Any]] = []
    p_vals: List[float] = []
    for i, fs in enumerate(fact_pack.get("feature_stats") or []):
        if not isinstance(fs, dict):
            continue
        col = str(fs.get("column") or "")
        try:
            p_vals.append(float(fs.get("p_value", 1.0)))
        except (TypeError, ValueError):
            p_vals.append(1.0)
        nums = re.findall(r"[-+]?\d*\.?\d+", str(fs.get("ace_summary", "")))
        if col and nums:
            try:
                val = float(nums[0])
                rules.append(
                    {
                        "id": f"s{i+1}",
                        "type": "feature_band",
                        "column": col,
                        "op": "gte",
                        "value": val,
                        "bonus": 0.03,
                    }
                )
            except ValueError:
                pass
    sec = str(fact_pack.get("sector_summary") or "")
    tokens = sanitize_theme_tokens(re.findall(r"\[([^\]]+)\]", sec))
    if tokens:
        rules.append(
            {
                "id": "theme1",
                "type": "theme_token",
                "tokens": tokens,
                "match": "any",
                "bonus": 0.02,
            }
        )
    rules.append(
        {
            "id": "logic1",
            "type": "logic_match",
            "pattern": str(fact_pack.get("logic_core") or ""),
            "priority_rank": -1,
        }
    )
    pb["rules"] = validate_rules(rules, fact_pack)
    pb["min_p_value"] = min(p_vals) if p_vals else 1.0
    pb["confidence"] = _confidence_from_stats(pb["min_p_value"], int(pb["n_ace"]))
    labels = [fs.get("label") for fs in (fact_pack.get("feature_stats") or [])[:2] if isinstance(fs, dict)]
    pb["human_insight_ko"] = sanitize_human_insight(
        f"{market} {pb['logic_core']}: "
        + "·".join(sanitize_noun_phrase(x) for x in labels if x)[:24]
        + f" p={pb['min_p_value']:.2f} N={pb['n_ace']}"
    )
    mult_min, mult_max = compute_dynamic_multiplier_bounds(pb)
    pb["mult_min"] = mult_min
    pb["mult_max"] = mult_max
    return apply_ttl_to_playbook(sanitize_playbook_text_fields(pb), market=market)


def _confidence_from_stats(min_p: float, n_ace: int) -> float:
    c = 0.45
    if min_p <= 0.05:
        c += 0.25
    elif min_p <= 0.10:
        c += 0.15
    elif min_p <= 0.20:
        c += 0.08
    if n_ace >= 12:
        c += 0.20
    elif n_ace >= 8:
        c += 0.12
    elif n_ace >= 5:
        c += 0.06
    return round(min(0.98, max(0.35, c)), 2)


def merge_llm_playbook(
    llm_raw: Dict[str, Any],
    fact_pack: Dict[str, Any],
    *,
    observe_only: bool = True,
) -> Tuple[Dict[str, Any], str]:
    market = str(fact_pack.get("market") or "KR").upper()
    notes: List[str] = []
    pb = default_playbook(market)
    pb.update(
        {
            "schema_version": SCHEMA_VERSION,
            "market": market,
            "logic_core": sanitize_noun_phrase(
                llm_raw.get("logic_core") or fact_pack.get("logic_core") or ""
            ),
            "as_of_kst": str(fact_pack.get("as_of_kst") or ""),
            "window_days": int(fact_pack.get("window_days") or 14),
            "n_ace": int(fact_pack.get("n_ace") or 0),
            "n_baseline": int(fact_pack.get("n_baseline") or 0),
            "observe_only": observe_only,
            "human_insight_ko": sanitize_human_insight(llm_raw.get("human_insight_ko") or ""),
            "ttl_days": default_ttl_days(market),
        }
    )
    try:
        pb["confidence"] = round(min(1.0, max(0.0, float(llm_raw.get("confidence", 0.5)))), 2)
    except (TypeError, ValueError):
        pb["confidence"] = 0.5
    try:
        pb["min_p_value"] = float(llm_raw.get("min_p_value", fact_pack.get("min_p_value", 1.0)))
    except (TypeError, ValueError):
        pb["min_p_value"] = 1.0

    stats_p = [
        float(fs.get("p_value", 1.0))
        for fs in (fact_pack.get("feature_stats") or [])
        if isinstance(fs, dict)
    ]
    if stats_p:
        pb["min_p_value"] = min(pb["min_p_value"], min(stats_p))

    raw_rules = llm_raw.get("rules") if isinstance(llm_raw.get("rules"), list) else []
    pb["rules"] = validate_rules(raw_rules, fact_pack)
    if not pb["rules"]:
        notes.append("llm_rules_empty_fallback_stats")
        fallback = stats_only_playbook(fact_pack, observe_only=observe_only)
        pb["rules"] = fallback.get("rules", [])
        if not pb["human_insight_ko"]:
            pb["human_insight_ko"] = fallback.get("human_insight_ko", "")

    try:
        pb["max_stack_bonus"] = min(0.12, max(0.04, float(llm_raw.get("max_stack_bonus", 0.08))))
    except (TypeError, ValueError):
        pb["max_stack_bonus"] = 0.08

    mult_min, mult_max = compute_dynamic_multiplier_bounds(pb)
    pb["mult_min"] = mult_min
    pb["mult_max"] = mult_max
    pb["provenance"] = {
        "source": "llm+stats",
        "stats_top_features": [fs.get("column") for fs in (fact_pack.get("feature_stats") or [])[:3]],
    }
    pb = apply_ttl_to_playbook(sanitize_playbook_text_fields(pb), market=market)
    return pb, ";".join(notes)


def parse_and_validate_llm_response(text: str, fact_pack: Dict[str, Any], *, observe_only: bool) -> Tuple[Dict[str, Any], str]:
    obj = _parse_json_blob(text)
    if not obj:
        pb = stats_only_playbook(fact_pack, observe_only=observe_only)
        return sanitize_playbook_text_fields(pb), "json_parse_failed_stats_fallback"
    pb, notes = merge_llm_playbook(obj, fact_pack, observe_only=observe_only)
    return sanitize_playbook_text_fields(pb), notes
