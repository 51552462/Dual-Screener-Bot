"""P0 orchestrator — STATUS / SHADOW / SAFE_SINGLE_CYCLE / LOCAL_P0_LOOP."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dev_autonomy.adapters import (
    ClaudeCodeVerifier,
    ClaudeVerifierAdapter,
    CursorCliExecutor,
    CursorExecutorAdapter,
    FakeClaudeVerifier,
    FakeCursorExecutor,
)
from dev_autonomy.audit_log import log_round
from dev_autonomy.context_pack import build_claude_pack, build_cursor_pack
from dev_autonomy.paths import SHADOW_REPORT_DIR, TRACK_SSOT
from dev_autonomy.safety_guard import evaluate_pre_ai_safety
from dev_autonomy.state_resolver import resolve_state
from dev_autonomy.types import OrchestratorPhase, ResolvedState, RoundRecord, RunMode, Track
from dev_autonomy.validation_gate import validate_after_cursor
from dev_autonomy.worktree import capture_git_baseline, collect_diff_excerpt, inspect_worktree


MAX_IMPL_RETRIES = 3
MAX_PING_PONG_ROUNDS = 5


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class P0Orchestrator:
    def __init__(
        self,
        cursor_adapter: Optional[CursorExecutorAdapter] = None,
        claude_adapter: Optional[ClaudeVerifierAdapter] = None,
        enable_autonomous_write: bool = False,
        allow_dev_autonomy_writes: bool = False,
    ):
        self.cursor = cursor_adapter or CursorCliExecutor()
        self.claude = claude_adapter or ClaudeCodeVerifier()
        self.enable_autonomous_write = enable_autonomous_write
        self.allow_dev_autonomy_writes = allow_dev_autonomy_writes
        self.cursor_call_count = 0
        self.last_validation_evidence: dict = {}

    def run_status(self, track: Track) -> Dict[str, Any]:
        state = resolve_state(track)
        worktree = inspect_worktree()
        cursor_avail, cursor_detail = self.cursor.availability()
        claude_avail, claude_detail = self.claude.availability()
        return {
            "mode": RunMode.STATUS.value,
            "state": self._state_dict(state),
            "worktree": {
                "dirty": worktree.dirty,
                "pre_existing_dirty": worktree.pre_existing_dirty,
                "reason": worktree.reason,
                "dirty_paths_count": len(worktree.dirty_paths),
            },
            "interfaces": {
                "cursor_available": cursor_avail,
                "cursor_detail": cursor_detail,
                "claude_available": claude_avail,
                "claude_detail": claude_detail,
            },
        }

    def run_shadow(self, track: Track) -> Dict[str, Any]:
        state = resolve_state(track)
        worktree = inspect_worktree()
        cursor_pack = build_cursor_pack(state)
        handoff_excerpt = cursor_pack.get("handoff_section", "")
        next_action_text = ""
        na_path = TRACK_SSOT[track]["next_action"]
        if na_path.is_file():
            next_action_text = na_path.read_text(encoding="utf-8", errors="replace")[:6000]
        safety = evaluate_pre_ai_safety(
            state,
            worktree,
            mode_requires_mutation=False,
            handoff_excerpt=handoff_excerpt,
            next_action_text=next_action_text,
        )
        cursor_avail, cursor_detail = self.cursor.availability()
        claude_avail, claude_detail = self.claude.availability()

        transition = "no_ai_call"
        if safety.allowed and state.status_canonical == "WAIT_CURSOR_IMPL":
            transition = "would_cursor_then_validation"
        elif state.status_canonical == "WAIT_CLAUDE_OK":
            transition = "would_claude_verify_only"
        elif state.blocked:
            transition = "blocked"

        report = {
            "mode": RunMode.SHADOW.value,
            "timestamp": _utc_now(),
            "state": self._state_dict(state),
            "worktree": {
                "pre_existing_dirty": worktree.pre_existing_dirty,
                "reason": worktree.reason,
            },
            "safety": {
                "allowed": safety.allowed,
                "category": safety.category,
                "reason": safety.reason,
            },
            "would_run_tests": cursor_pack.get("test_commands", []),
            "would_build_context_keys": list(cursor_pack.keys()),
            "expected_transition": transition,
            "interfaces": {
                "cursor": cursor_detail,
                "claude": claude_detail,
            },
            "notes": state.notes,
        }

        SHADOW_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = SHADOW_REPORT_DIR / f"shadow_{track.value}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        report["shadow_report_path"] = str(out)
        return report

    def run_safe_single_cycle(self, track: Track) -> Dict[str, Any]:
        return self._run_cycle(track, max_rounds=1)

    def run_local_p0_loop(self, track: Track) -> Dict[str, Any]:
        return self._run_cycle(track, max_rounds=MAX_PING_PONG_ROUNDS)

    def _run_cycle(self, track: Track, max_rounds: int) -> Dict[str, Any]:
        state = resolve_state(track)
        worktree = inspect_worktree()
        if worktree.pre_existing_dirty:
            return self._stop(
                state,
                OrchestratorPhase.WAIT_DIRECTOR,
                f"dirty worktree: {worktree.reason}",
            )

        if not self.enable_autonomous_write:
            return self._stop(
                state,
                OrchestratorPhase.SAFETY_BLOCKED,
                "AUTONOMOUS_WRITE_DISABLED",
            )

        impl_retries = 0
        rounds = 0
        last_validation_summary = ""

        while rounds < max_rounds:
            rounds += 1
            round_id = str(uuid.uuid4())[:12]

            if state.status_canonical != "WAIT_CURSOR_IMPL":
                if state.status_canonical == "WAIT_CLAUDE_OK":
                    return self._claude_phase(state, round_id, last_validation_summary)
                return self._stop(state, OrchestratorPhase.WAIT_DIRECTOR, state.block_reason)

            cursor_pack = build_cursor_pack(state)
            handoff_excerpt = cursor_pack.get("handoff_section", "")
            next_action_text = ""
            na_path = TRACK_SSOT[track]["next_action"]
            if na_path.is_file():
                next_action_text = na_path.read_text(encoding="utf-8", errors="replace")[:6000]
            safety = evaluate_pre_ai_safety(
                state,
                worktree,
                mode_requires_mutation=True,
                handoff_excerpt=handoff_excerpt,
                next_action_text=next_action_text,
            )
            if not safety.allowed:
                log_round(
                    RoundRecord(
                        round_id=round_id,
                        timestamp=_utc_now(),
                        track=track.value,
                        subphase=state.subphase,
                        actor="safety",
                        action="block",
                        source_status=state.status_canonical,
                        safety_decision=safety.reason,
                        final_status="SAFETY_BLOCKED",
                    )
                )
                return self._stop(state, OrchestratorPhase.SAFETY_BLOCKED, safety.reason)

            cursor_avail, cursor_detail = self.cursor.availability()
            if not cursor_avail:
                log_round(
                    RoundRecord(
                        round_id=round_id,
                        timestamp=_utc_now(),
                        track=track.value,
                        subphase=state.subphase,
                        actor="cursor",
                        action="skip",
                        source_status=state.status_canonical,
                        final_status=cursor_detail,
                    )
                )
                return self._stop(
                    state,
                    OrchestratorPhase.FAILED_REQUIRES_REVIEW,
                    cursor_detail,
                )

            git_baseline = capture_git_baseline()
            if not git_baseline.ok:
                return self._stop(
                    state,
                    OrchestratorPhase.SAFETY_BLOCKED,
                    git_baseline.error or "git baseline capture failed",
                )

            cursor_result = self.cursor.run_implementation(cursor_pack)
            self.cursor_call_count += 1
            if not cursor_result.ok:
                impl_retries += 1
                log_round(
                    RoundRecord(
                        round_id=round_id,
                        timestamp=_utc_now(),
                        track=track.value,
                        subphase=state.subphase,
                        actor="cursor",
                        action="impl_fail",
                        source_status=state.status_canonical,
                        retry_count=impl_retries,
                        final_status=cursor_result.detail,
                    )
                )
                if impl_retries >= MAX_IMPL_RETRIES:
                    return self._stop(
                        state,
                        OrchestratorPhase.FAILED_REQUIRES_REVIEW,
                        f"cursor failed {impl_retries} times",
                    )
                continue

            validation = validate_after_cursor(
                track,
                cursor_pack.get("test_commands", []),
                baseline=git_baseline,
                handoff_section=handoff_excerpt,
                allow_dev_autonomy_writes=self.allow_dev_autonomy_writes,
            )
            diff_excerpt = collect_diff_excerpt(validation.changed_paths)
            self.last_validation_evidence = {
                "changed_paths": validation.changed_paths,
                "test_commands": validation.commands,
                "test_exit_code": validation.exit_code,
                "diff_excerpt": diff_excerpt,
                "errors": validation.errors,
            }
            last_validation_summary = "\n".join(validation.summaries)[:4000]
            log_round(
                RoundRecord(
                    round_id=round_id,
                    timestamp=_utc_now(),
                    track=track.value,
                    subphase=state.subphase,
                    actor="validation",
                    action="pytest",
                    source_status=state.status_canonical,
                    files_touched=validation.changed_paths,
                    validation_commands=validation.commands,
                    exit_codes=[validation.exit_code],
                    test_summary=last_validation_summary[:500],
                    retry_count=impl_retries,
                    final_status="PASS" if validation.passed else "FAIL",
                )
            )

            if not validation.passed:
                impl_retries += 1
                if impl_retries >= MAX_IMPL_RETRIES:
                    return self._stop(
                        state,
                        OrchestratorPhase.FAILED_REQUIRES_REVIEW,
                        "validation failed max retries",
                    )
                continue

            # Validation PASS -> WAIT_CLAUDE_OK (state transition logical, SSOT not auto-written)
            return self._claude_phase(state, round_id, last_validation_summary)

        return self._stop(state, OrchestratorPhase.STOPPED, "max ping-pong rounds exceeded")

    def _claude_phase(
        self,
        state: ResolvedState,
        round_id: str,
        validation_summary: str,
    ) -> Dict[str, Any]:
        claude_avail, claude_detail = self.claude.availability()
        if not claude_avail:
            log_round(
                RoundRecord(
                    round_id=round_id,
                    timestamp=_utc_now(),
                    track=state.track.value,
                    subphase=state.subphase,
                    actor="claude",
                    action="unavailable",
                    source_status=state.status_canonical,
                    claude_verdict=claude_detail,
                    final_status="EXTERNAL_ARCHITECT_UNAVAILABLE",
                )
            )
            return {
                "phase": OrchestratorPhase.FAILED_REQUIRES_REVIEW.value,
                "reason": claude_detail,
                "state": self._state_dict(state),
                "note": "validation may have passed but verifier unavailable — fail closed",
            }

        claude_pack = build_claude_pack(
            state,
            "",
            self.last_validation_evidence.get("changed_paths", []),
            validation_summary,
            test_commands=self.last_validation_evidence.get("test_commands"),
            test_exit_code=self.last_validation_evidence.get("test_exit_code"),
            diff_excerpt=self.last_validation_evidence.get("diff_excerpt", ""),
        )
        claude_result = self.claude.verify(claude_pack)
        log_round(
            RoundRecord(
                round_id=round_id,
                timestamp=_utc_now(),
                track=state.track.value,
                subphase=state.subphase,
                actor="claude",
                action="verify",
                source_status=state.status_canonical,
                claude_verdict=claude_result.verdict,
                final_status=claude_result.verdict,
            )
        )

        if claude_result.verdict == "OK":
            return {
                "phase": OrchestratorPhase.IMPLEMENTATION_VERIFIED.value,
                "state": self._state_dict(state),
                "claude": claude_result.verdict,
                "note": "IMPLEMENTATION_VERIFIED != SUB_PHASE_DONE != LIVE_READY",
                "dod_invariants": {
                    "implementation_verified": True,
                    "sub_phase_done": False,
                    "live_ready": False,
                    "backtest_pass_is_not_live_promotion": True,
                    "stage_2_paper_observation_required": True,
                    "stage_3_06_checklist_required": True,
                },
            }
        if claude_result.verdict == "MODIFY":
            return {
                "phase": OrchestratorPhase.CURSOR_IMPL.value,
                "state": self._state_dict(state),
                "claude": claude_result.verdict,
                "note": "would retry cursor — not auto-run in single response",
            }

        return self._stop(state, OrchestratorPhase.FAILED_REQUIRES_REVIEW, claude_result.detail)

    def _stop(
        self,
        state: ResolvedState,
        phase: OrchestratorPhase,
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "phase": phase.value,
            "reason": reason,
            "state": self._state_dict(state),
        }

    def _state_dict(self, state: ResolvedState) -> Dict[str, Any]:
        return {
            "track": state.track.value,
            "phase": state.phase,
            "subphase": state.subphase,
            "subphase_id": state.subphase_id,
            "status_raw": state.status_raw,
            "status_canonical": state.status_canonical,
            "next_actor": state.next_actor,
            "handoff_available": state.handoff_available,
            "blocked": state.blocked,
            "block_reason": state.block_reason,
            "human_required": state.human_required,
            "conflict": state.conflict,
            "vps_or_deploy_hint": state.vps_or_deploy_hint,
            "deferred_hint": state.deferred_hint,
            "notes": state.notes,
        }
