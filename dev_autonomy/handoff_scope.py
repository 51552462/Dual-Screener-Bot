"""Extract Handoff allowed file paths — machine-readable sections only."""

from __future__ import annotations

import re
from typing import Optional

# Machine-readable allowlist markers only
_ALLOWED_SECTION = re.compile(
    r"(?:^|\n)\s*(?:#{1,3}\s*)?(?:allowed\s*files?|allowed\s*paths?|modify\s*only)\s*[:：]\s*\n",
    re.IGNORECASE,
)
_ALLOWED_INLINE = re.compile(
    r"(?:^|\n)\s*Allowed\s*:\s*`?([a-zA-Z0-9_./-]+\.(?:py|md|mdc|sh))`?",
    re.IGNORECASE | re.MULTILINE,
)
_BULLET_IN_SECTION = re.compile(
    r"^\s*[-*]\s+[`']?([a-zA-Z0-9_./-]+\.(?:py|md|mdc|sh))[`']?\s*$",
    re.MULTILINE,
)

# Prose exclusion — never treat as allowed
_DO_NOT_MODIFY = re.compile(
    r"do\s+not\s+modify|금지|수정\s*금지|never\s+modify",
    re.IGNORECASE,
)


def _norm_path(p: str) -> str:
    norm = p.replace("\\", "/").strip()
    while norm.startswith("./"):
        norm = norm[2:]
    return norm.lstrip("/")


def _line_is_exclusion_context(line: str) -> bool:
    return bool(_DO_NOT_MODIFY.search(line))


def extract_allowed_paths(handoff_section: str) -> Optional[list[str]]:
    """
    Parse only explicit machine-readable allowed-files sections.
    Returns None if scope cannot be determined → fail closed.
    """
    if not handoff_section.strip():
        return None

    found: set[str] = set()

    # Inline "Allowed: path"
    for m in _ALLOWED_INLINE.finditer(handoff_section):
        line_start = handoff_section.rfind("\n", 0, m.start()) + 1
        line_end = handoff_section.find("\n", m.start())
        line = handoff_section[line_start:line_end if line_end != -1 else len(handoff_section)]
        if _line_is_exclusion_context(line):
            continue
        found.add(_norm_path(m.group(1)))

    # Section block after "Allowed files:" header
    for m in _ALLOWED_SECTION.finditer(handoff_section):
        block = handoff_section[m.end():]
        end = re.search(r"\n## |\n---|\Z", block)
        block = block[: end.start() if end else len(block)]
        for line in block.splitlines():
            if _line_is_exclusion_context(line):
                continue
            bm = _BULLET_IN_SECTION.match(line)
            if bm:
                found.add(_norm_path(bm.group(1)))
            # backtick path on its own line
            tick = re.search(r"`([a-zA-Z0-9_./-]+\.(?:py|md|mdc|sh))`", line)
            if tick and not _line_is_exclusion_context(line):
                found.add(_norm_path(tick.group(1)))

    if not found:
        return None

    return sorted(found)


def path_matches_allowlist(changed: str, allowed: list[str]) -> bool:
    """Repository-relative exact path match — foo.py != pkg/foo.py."""
    norm = _norm_path(changed)
    allowed_norm = {_norm_path(a) for a in allowed}
    return norm in allowed_norm
