"""Safety guard — runs before any AI invocation and after mutation."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from dev_autonomy.handoff_scope import extract_allowed_paths, path_matches_allowlist
from dev_autonomy.paths import BITGET_FORBIDDEN_ROOT_WRITES, REPO_ROOT
from dev_autonomy.types import ResolvedState, SafetyDecision, Track
from dev_autonomy.worktree import WorktreeStatus

SECRET_PATH_PATTERNS = (
    r"(^|/)\.env$",
    r"(^|/)\.env\.",
    r"credentials",
    r"/secrets?/",
    r"api_key",
    r"private_key",
    r"\.pem$",
    r"token\.json",
)

PRODUCTION_PATH_PATTERNS = (
    r"^deploy/",
    r"^bitget/deploy/",
    r"systemd/",
    r"/cron",
    r"factory\.sh",
    r"update_factory",
    r"update_bitget",
)

RISK_CODE_PATTERNS = (
    r"performance_budget_governor",
    r"meta_governor",
    r"regime_kelly_failsafe",
    r"forward/gates",
    r"execution_safety",
)

RISK_CONTENT_PATTERNS = (
    r"ENABLE_REAL_EXECUTION\s*=\s*[\"']?true",
    r"KILL_SWITCH\s*=\s*[\"']?true",
    r"MAX_LEVERAGE\s*=",
    r"ENABLE_REAL_EXECUTION\s*=\s*1",
    r"WALK_FORWARD_PROMOTION_BLOCK_ENABLED\s*=\s*1",
    r"git\s+commit",
    r"git\s+push",
    r"systemctl\s+",
    r"\bssh\s+",
    r"update_factory\.sh",
    r"update_bitget\.sh",
    r"LIVE_PROMOTION",
    r"registry.*LIVE",
)

DEPLOY_COMMAND_PATTERNS = (
    r"\bssh\b",
    r"\bgit\s+push\b",
    r"\bgit\s+commit\b",
    r"\bgit\s+reset\b",
    r"\bgit\s+clean\b",
    r"update_factory",
    r"update_bitget",
    r"systemctl",
    r"\bsudo\s+",
    r"\bscp\b",
    r"lightsail",
)

DEFERRED_KEYWORDS = (
    "deferred",
    "postpone",
    "later",
    "보류",
    "후순위",
    "director action required",
    "wait_director",
    "재rerun 금지",
    "shrink 재rerun",
)

# Track IV may only touch IV docs + related tests
IV_ALLOWED_PREFIXES = (
    "docs/independent_verification/",
    "tests/test_independent",
    "tests/test_iv_",
)

AUTONOMY_PACKAGE_PREFIX = "dev_autonomy/"


def _norm(path: str) -> str:
    norm = path.replace("\\", "/")
    while norm.startswith("./"):
        norm = norm[2:]
    return norm.lstrip("/")


def path_matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    norm = _norm(path)
    for pat in patterns:
        if re.search(pat, norm, re.IGNORECASE):
            return True
    return False


def is_secret_path(path: str) -> bool:
    norm = _norm(path)
    if norm == ".env" or norm.endswith("/.env") or norm.startswith(".env."):
        return True
    return path_matches_any(path, SECRET_PATH_PATTERNS)


def check_track_path_write(track: Track, path: str) -> SafetyDecision | None:
    norm = _norm(path)

    if track == Track.A:
        if norm.startswith("bitget/"):
            return SafetyDecision(
                allowed=False,
                category="cross_track",
                reason=f"Track A cannot write bitget path: {path}",
                human_required=True,
            )
        if norm.startswith("docs/independent_verification/"):
            return SafetyDecision(
                allowed=False,
                category="cross_track",
                reason=f"Track A cannot write IV path: {path}",
                human_required=True,
            )

    if track == Track.B:
        if not norm.startswith("bitget/"):
            for forbidden in BITGET_FORBIDDEN_ROOT_WRITES:
                if norm == forbidden or norm.startswith(forbidden):
                    return SafetyDecision(
                        allowed=False,
                        category="bitget_forbidden_root",
                        reason=f"Bitget track forbidden root write: {path}",
                        human_required=True,
                    )
            if not norm.startswith("dev_autonomy/"):
                return SafetyDecision(
                    allowed=False,
                    category="cross_track",
                    reason=f"Track B cannot write non-bitget path: {path}",
                    human_required=True,
                )

    if track == Track.IV:
        allowed = any(norm.startswith(p) for p in IV_ALLOWED_PREFIXES)
        if not allowed and not norm.startswith(AUTONOMY_PACKAGE_PREFIX):
            return SafetyDecision(
                allowed=False,
                category="iv_scope",
                reason=f"Track IV cannot write implementation path: {path}",
                human_required=True,
            )

    return None


def check_path_safety(path: str, track: Track) -> SafetyDecision | None:
    if is_secret_path(path):
        return SafetyDecision(
            allowed=False,
            category="secret",
            reason=f"secret/env path blocked: {path}",
            human_required=True,
        )
    track_block = check_track_path_write(track, path)
    if track_block:
        return track_block
    if path_matches_any(path, PRODUCTION_PATH_PATTERNS):
        return SafetyDecision(
            allowed=False,
            category="production",
            reason=f"production/deploy path blocked: {path}",
            human_required=True,
        )
    if path_matches_any(path, RISK_CODE_PATTERNS):
        return SafetyDecision(
            allowed=False,
            category="critical_risk",
            reason=f"critical risk path blocked: {path}",
            human_required=True,
        )
    return None


def check_text_commands(text: str) -> SafetyDecision | None:
    lower = text.lower()
    for pat in DEPLOY_COMMAND_PATTERNS:
        if re.search(pat, lower):
            return SafetyDecision(
                allowed=False,
                category="deploy_vps",
                reason=f"deploy/VPS/destructive command pattern blocked",
                human_required=True,
            )
    compact = lower.replace(" ", "")
    if re.search(r"enable_real_execution\s*=\s*true", compact):
        return SafetyDecision(
            allowed=False,
            category="real_execution",
            reason="ENABLE_REAL_EXECUTION enablement blocked",
            human_required=True,
        )
    if "enable_real_execution=1" in compact:
        return SafetyDecision(
            allowed=False,
            category="real_execution",
            reason="ENABLE_REAL_EXECUTION enablement blocked",
            human_required=True,
        )
    return None


def check_deferred_instruction(text: str) -> SafetyDecision | None:
    lower = text.lower()
    for kw in DEFERRED_KEYWORDS:
        if kw.lower() in lower:
            return SafetyDecision(
                allowed=False,
                category="deferred",
                reason=f"deferred/postponed instruction detected ({kw})",
                human_required=True,
            )
    return None


def _get_path_mutation_content(path: str, repo_root: Path) -> str:
    """Read diff for tracked files; read full file content for untracked/new files."""
    proc = subprocess.run(
        ["git", "diff", "HEAD", "--", path],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    diff_text = (proc.stdout or "").strip()
    if diff_text:
        return diff_text[:8000]
    full = repo_root / path
    if full.is_file():
        try:
            return full.read_text(encoding="utf-8", errors="replace")[:8000]
        except OSError:
            return ""
    return ""


def check_diff_content(path: str, repo_root: Path) -> SafetyDecision | None:
    content = _get_path_mutation_content(path, repo_root)
    if not content:
        return None
    lower = content.lower()
    for pat in RISK_CONTENT_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return SafetyDecision(
                allowed=False,
                category="risk_content",
                reason=f"risk-critical content change in {path}",
                human_required=True,
            )
    return None


def evaluate_allowlist(
    changed_paths: list[str],
    handoff_section: str,
    allow_dev_autonomy_writes: bool,
) -> SafetyDecision:
    allowed = extract_allowed_paths(handoff_section)
    if allowed is None:
        return SafetyDecision(
            allowed=False,
            category="allowlist_unknown",
            reason="cannot determine Handoff allowed-file scope — fail closed",
            human_required=True,
        )

    for path in changed_paths:
        norm = _norm(path)
        if norm.startswith(AUTONOMY_PACKAGE_PREFIX):
            if allow_dev_autonomy_writes:
                continue
            return SafetyDecision(
                allowed=False,
                category="dev_autonomy_scope",
                reason=f"dev_autonomy/ write not in Handoff allowlist: {path}",
                human_required=True,
            )
        if not path_matches_allowlist(path, allowed):
            return SafetyDecision(
                allowed=False,
                category="allowlist_violation",
                reason=f"changed path outside Handoff allowlist: {path}",
                human_required=True,
            )
    return SafetyDecision(allowed=True, category="ok", reason="allowlist ok")


def evaluate_diff_safety(changed_paths: list[str], track: Track) -> SafetyDecision:
    for path in changed_paths:
        block = check_path_safety(path, track)
        if block:
            return block
    return SafetyDecision(allowed=True, category="ok", reason="diff paths allowed")


def evaluate_post_mutation_safety(
    changed_paths: list[str],
    track: Track,
    repo_root: Path = REPO_ROOT,
    handoff_section: str = "",
    allow_dev_autonomy_writes: bool = False,
) -> SafetyDecision:
    if not changed_paths:
        return SafetyDecision(allowed=True, category="ok", reason="no changes")

    for path in changed_paths:
        block = check_path_safety(path, track)
        if block:
            return block
        content_block = check_diff_content(path, repo_root)
        if content_block:
            return content_block

    if handoff_section:
        allow_block = evaluate_allowlist(changed_paths, handoff_section, allow_dev_autonomy_writes)
        if not allow_block.allowed:
            return allow_block

    return SafetyDecision(allowed=True, category="ok", reason="post-mutation checks passed")


def evaluate_pre_ai_safety(
    state: ResolvedState,
    worktree: WorktreeStatus,
    mode_requires_mutation: bool,
    handoff_excerpt: str = "",
    next_action_text: str = "",
) -> SafetyDecision:
    if state.status_canonical == "WAIT_DIRECTOR":
        return SafetyDecision(
            allowed=False,
            category="wait_director",
            reason="WAIT_DIRECTOR — human stop",
            human_required=True,
        )

    if state.deferred_hint:
        return SafetyDecision(
            allowed=False,
            category="deferred",
            reason=state.block_reason or "deferred/postponed active work",
            human_required=True,
        )

    deferred = check_deferred_instruction(next_action_text or handoff_excerpt)
    if deferred:
        return deferred

    if worktree.pre_existing_dirty and mode_requires_mutation:
        return SafetyDecision(
            allowed=False,
            category="dirty_worktree",
            reason=worktree.reason,
            human_required=True,
        )

    if state.blocked:
        return SafetyDecision(
            allowed=False,
            category="state_blocked",
            reason=state.block_reason,
            human_required=state.human_required,
        )

    if state.status_canonical in ("CONFLICT", "UNKNOWN"):
        return SafetyDecision(
            allowed=False,
            category="state_unknown",
            reason=state.block_reason or state.status_canonical,
            human_required=True,
        )

    if state.status_canonical != "WAIT_CURSOR_IMPL":
        return SafetyDecision(
            allowed=False,
            category="not_wait_cursor_impl",
            reason=f"status={state.status_canonical}",
            human_required=state.human_required,
        )

    if not state.handoff_available:
        return SafetyDecision(
            allowed=False,
            category="no_handoff",
            reason="Handoff section missing for current sub-phase",
            human_required=True,
        )

    cmd_block = check_text_commands(handoff_excerpt)
    if cmd_block:
        return cmd_block

    if state.vps_or_deploy_hint:
        return SafetyDecision(
            allowed=False,
            category="vps_deploy",
            reason="VPS/deploy/server hint in active state",
            human_required=True,
        )

    return SafetyDecision(allowed=True, category="ok", reason="pre-AI checks passed")
