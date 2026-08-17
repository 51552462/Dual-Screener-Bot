"""Canonical sub-phase ID normalization — exact match only."""

from __future__ import annotations

import re

# BULL-RECENCY-01, I-GMM-DNA-01 — multi-segment IDs
_MULTI_SEGMENT_ID = re.compile(
    r"([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+)",
    re.IGNORECASE,
)
# A-1, A-10, V-2a — short segment IDs (must not substring-match)
_SHORT_ID = re.compile(
    r"\b([A-Z]-\d+[A-Z]?)\b",
    re.IGNORECASE,
)


def normalize_subphase_id(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"\*+", "", text).strip()
    # Prefer longer multi-segment IDs first
    m = _MULTI_SEGMENT_ID.search(cleaned)
    if m:
        return m.group(1).upper()
    m = _SHORT_ID.search(cleaned)
    if m:
        return m.group(1).upper()
    token = cleaned.split()[0].strip("`")
    if re.match(r"^[A-Z][A-Z0-9-]+$", token, re.I):
        return token.upper()
    return cleaned.upper()


def subphase_ids_match(a: str, b: str) -> bool:
    """Exact canonical ID equality only — A-1 must not match A-10."""
    id_a = normalize_subphase_id(a)
    id_b = normalize_subphase_id(b)
    if not id_a or not id_b:
        return False
    return id_a == id_b
