"""Shared types for dev autonomy P0."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class Track(str, Enum):
    A = "A"
    B = "B"
    IV = "IV"


class RunMode(str, Enum):
    STATUS = "STATUS"
    SHADOW = "SHADOW"
    SAFE_SINGLE_CYCLE = "SAFE_SINGLE_CYCLE"
    LOCAL_P0_LOOP = "LOCAL_P0_LOOP"


class OrchestratorPhase(str, Enum):
    IDLE = "IDLE"
    SAFETY_BLOCKED = "SAFETY_BLOCKED"
    CURSOR_IMPL = "CURSOR_IMPL"
    VALIDATION = "VALIDATION"
    CLAUDE_VERIFY = "CLAUDE_VERIFY"
    IMPLEMENTATION_VERIFIED = "IMPLEMENTATION_VERIFIED"
    FAILED_REQUIRES_REVIEW = "FAILED_REQUIRES_REVIEW"
    WAIT_DIRECTOR = "WAIT_DIRECTOR"
    STOPPED = "STOPPED"


@dataclass
class SourceRef:
    path: str
    mtime: float
    size: int


@dataclass
class ResolvedState:
    track: Track
    phase: str
    subphase: str
    status_raw: str
    status_canonical: str
    next_actor: str
    handoff_available: bool
    blocked: bool
    block_reason: str
    human_required: bool
    conflict: bool
    conflict_detail: str
    source_files: Dict[str, SourceRef] = field(default_factory=dict)
    vps_or_deploy_hint: bool = False
    notes: List[str] = field(default_factory=list)
    subphase_id: str = ""
    deferred_hint: bool = False


@dataclass
class SafetyDecision:
    allowed: bool
    category: str
    reason: str
    human_required: bool = False


@dataclass
class ValidationResult:
    passed: bool
    exit_code: int
    commands: List[str] = field(default_factory=list)
    summaries: List[str] = field(default_factory=list)
    forbidden_paths: List[str] = field(default_factory=list)
    changed_paths: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class AdapterResult:
    ok: bool
    available: bool
    verdict: str
    detail: str
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RoundRecord:
    round_id: str
    timestamp: str
    track: str
    subphase: str
    actor: str
    action: str
    source_status: str
    files_touched: List[str] = field(default_factory=list)
    validation_commands: List[str] = field(default_factory=list)
    exit_codes: List[int] = field(default_factory=list)
    test_summary: str = ""
    safety_decision: str = ""
    claude_verdict: str = ""
    retry_count: int = 0
    final_status: str = ""
