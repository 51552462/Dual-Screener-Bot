"""Read-only report intake and deterministic work routing.

This module deliberately does not call Cursor, Claude, git, Telegram, SSH, or
deployment commands.  It converts existing structured report sources into a
deduplicated local queue that a later, separately approved executor can read.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable

from dev_autonomy.paths import AUTONOMY_DATA_DIR
from dev_autonomy.state_resolver import resolve_state
from dev_autonomy.types import ResolvedState, Track


SCHEMA_VERSION = "dev_autonomy.report.v1"
DEFAULT_QUEUE_DB = AUTONOMY_DATA_DIR / "control_plane.sqlite"
# Routing alarm only.  This never changes a trading MDD cap or position size.
MDD_REVIEW_UTILIZATION = 0.85


class ControlAction(str, Enum):
    OBSERVE_ONLY = "OBSERVE_ONLY"
    CLAUDE_REVIEW = "CLAUDE_REVIEW"
    CURSOR_IMPLEMENT = "CURSOR_IMPLEMENT"
    WAIT_WEEKEND = "WAIT_WEEKEND"
    SAFETY_HALT = "SAFETY_HALT"
    QUARANTINE = "QUARANTINE"


@dataclass(frozen=True)
class NormalizedReport:
    report_id: str
    source: str
    track: str
    observed_at: str
    source_status: str
    cursor_action: str = ""
    environment: str = "observation"
    metrics: dict[str, Any] = field(default_factory=dict)
    flags: tuple[str, ...] = ()
    payload_hash: str = ""
    schema: str = SCHEMA_VERSION


@dataclass(frozen=True)
class ControlDecision:
    action: ControlAction
    reason_code: str
    reason: str
    priority: str
    work_state: str
    execution_authorized: bool = False


@dataclass(frozen=True)
class AutonomyEnvelope:
    envelope_id: str
    valid_from: datetime
    valid_until: datetime
    allowed_tracks: frozenset[str]
    allowed_actions: frozenset[str]
    max_tasks_per_day: int
    require_pull_request: bool = True
    allow_deploy: bool = False
    allow_live: bool = False
    allow_merge: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AutonomyEnvelope":
        required = (
            "envelope_id",
            "valid_from",
            "valid_until",
            "allowed_tracks",
            "allowed_actions",
            "max_tasks_per_day",
        )
        missing = [key for key in required if key not in data]
        if missing:
            raise ValueError(f"missing envelope fields: {', '.join(missing)}")

        start = _parse_datetime(str(data["valid_from"]))
        end = _parse_datetime(str(data["valid_until"]))
        if end <= start:
            raise ValueError("valid_until must be after valid_from")

        tracks = frozenset(str(v).upper() for v in data["allowed_tracks"])
        if not tracks or not tracks.issubset({track.value for track in Track}):
            raise ValueError("allowed_tracks contains an unknown track")

        actions = frozenset(str(v).upper() for v in data["allowed_actions"])
        permitted = {ControlAction.CURSOR_IMPLEMENT.value, ControlAction.CLAUDE_REVIEW.value}
        if not actions or not actions.issubset(permitted):
            raise ValueError("allowed_actions may only contain review/implementation")

        max_tasks = int(data["max_tasks_per_day"])
        if max_tasks < 1 or max_tasks > 3:
            raise ValueError("max_tasks_per_day must be between 1 and 3")

        unsafe = {
            "allow_deploy": bool(data.get("allow_deploy", False)),
            "allow_live": bool(data.get("allow_live", False)),
            "allow_merge": bool(data.get("allow_merge", False)),
        }
        if any(unsafe.values()):
            raise ValueError("Phase 1 envelope cannot enable deploy, live, or merge")

        return cls(
            envelope_id=str(data["envelope_id"]).strip(),
            valid_from=start,
            valid_until=end,
            allowed_tracks=tracks,
            allowed_actions=actions,
            max_tasks_per_day=max_tasks,
            require_pull_request=bool(data.get("require_pull_request", True)),
            **unsafe,
        )

    def allows(self, report: NormalizedReport, action: ControlAction, now: datetime) -> bool:
        current = _as_utc(now)
        return (
            self.valid_from <= current <= self.valid_until
            and report.track in self.allowed_tracks
            and action.value in self.allowed_actions
            and self.require_pull_request
            and not self.allow_deploy
            and not self.allow_live
            and not self.allow_merge
        )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    return _as_utc(parsed)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _payload_hash(payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _integer(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _report_id(source: str, track: str, observed_at: str, payload_hash: str) -> str:
    return f"{source}:{track}:{observed_at}:{payload_hash[:16]}"


def load_envelope(path: Path | None) -> AutonomyEnvelope | None:
    if path is None:
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("envelope must be a JSON object")
    return AutonomyEnvelope.from_dict(data)


def normalize_north_star_ledger(ledger: dict[str, Any]) -> NormalizedReport:
    latest = ledger.get("latest") if isinstance(ledger.get("latest"), dict) else ledger
    tracks = latest.get("tracks") if isinstance(latest.get("tracks"), dict) else {}
    track_a = tracks.get("A") if isinstance(tracks.get("A"), dict) else {}
    aggregate = track_a.get("aggregate") if isinstance(track_a.get("aggregate"), dict) else {}
    book = track_a.get("forward_book") if isinstance(track_a.get("forward_book"), dict) else {}
    meta = latest.get("meta") if isinstance(latest.get("meta"), dict) else {}
    history = ledger.get("history") if isinstance(ledger.get("history"), dict) else {}
    daily_history = history.get("daily") if isinstance(history.get("daily"), list) else []
    period_returns = latest.get("period_returns")
    period_a = period_returns.get("A") if isinstance(period_returns, dict) else {}

    daily_n = _integer(meta.get("daily_n"))
    if daily_n is None:
        daily_n = len(daily_history)
    recall_n = _integer(meta.get("obs_hold_recall_n")) or 20
    action = str(meta.get("cursor_action") or "").upper()
    if not action:
        cadence = str(latest.get("cadence") or "daily").lower()
        action = "RECALL_FORK" if cadence == "daily" and daily_n >= recall_n else "OBSERVE_HOLD"

    observed_at = str(
        latest.get("generated_at")
        or latest.get("created_at")
        or latest.get("ts_utc")
        or ledger.get("updated_at")
        or latest.get("date_kst")
        or "unknown"
    )
    digest = {
        "latest": latest,
        "daily_n": daily_n,
        "commercialization": ledger.get("commercialization"),
    }
    payload_hash = _payload_hash(digest)
    metrics = {
        "mdd_pct": _number(aggregate.get("max_mdd_pct")),
        "mdd_cap_pct": _number(track_a.get("mdd_cap_pct")),
        "cumulative_return_pct": _number(period_a.get("total_pct")),
        "composite_score": _number(aggregate.get("composite_score")),
        "closed_total": _integer(book.get("closed_total")),
        "open_total": _integer(book.get("open_total")),
        "forward_trades_count": _integer(track_a.get("forward_trades_count")),
        "daily_n": daily_n,
        "recall_n": recall_n,
    }
    flags: list[str] = []
    if metrics["closed_total"] == 0 and (metrics["forward_trades_count"] or 0) > 0:
        flags.append("CLOSED_COUNT_INCONSISTENT")
    if daily_n >= recall_n and action == "OBSERVE_HOLD":
        flags.append("RECALL_ACTION_INCONSISTENT")

    return NormalizedReport(
        report_id=_report_id("north_star", "A", observed_at, payload_hash),
        source="north_star_ledger",
        track="A",
        observed_at=observed_at,
        source_status=action,
        cursor_action=action,
        metrics=metrics,
        flags=tuple(flags),
        payload_hash=payload_hash,
    )


def read_north_star_ledger(path: Path) -> NormalizedReport:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("north-star ledger must be a JSON object")
    return normalize_north_star_ledger(data)


def normalize_bitget_digest(
    payload: dict[str, Any],
    *,
    observed_at: str,
    severity: str = "INFO",
) -> NormalizedReport:
    checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
    dna = checks.get("dna_rank") if isinstance(checks.get("dna_rank"), dict) else {}
    diagnosis = dna.get("diagnosis") if isinstance(dna.get("diagnosis"), dict) else {}
    action = str(
        diagnosis.get("cursor_action") or payload.get("cursor_action") or payload.get("status") or "NONE"
    ).upper()
    dashboard = payload.get("dashboard") if isinstance(payload.get("dashboard"), dict) else {}
    problems = dashboard.get("problem") if isinstance(dashboard.get("problem"), list) else []
    payload_hash = _payload_hash(payload)
    flags: list[str] = []
    if str(severity).upper() in {"ERROR", "CRITICAL"}:
        flags.append("SOURCE_ERROR")

    return NormalizedReport(
        report_id=_report_id("bitget_obs", "B", observed_at, payload_hash),
        source="bitget_ops_events",
        track="B",
        observed_at=observed_at,
        source_status=action,
        cursor_action=action,
        metrics={
            "problem_count": len(problems),
            "dna_state": diagnosis.get("state"),
        },
        flags=tuple(flags),
        payload_hash=payload_hash,
    )


def read_latest_bitget_digest(path: Path) -> NormalizedReport | None:
    uri = f"file:{path.resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ops_events'").fetchone()
        if not table:
            return None
        row = conn.execute(
            "SELECT ts_utc, severity, payload_json FROM ops_events WHERE event=? ORDER BY ts_utc DESC, id DESC LIMIT 1",
            ("post_deploy_obs_digest_daily",),
        ).fetchone()
    if not row:
        return None
    payload = json.loads(row[2])
    if not isinstance(payload, dict):
        raise ValueError("Bitget digest payload must be a JSON object")
    return normalize_bitget_digest(payload, observed_at=str(row[0]), severity=str(row[1]))


def normalize_ssot_state(state: ResolvedState) -> NormalizedReport:
    observed_epoch = max((ref.mtime for ref in state.source_files.values()), default=0.0)
    observed_at = datetime.fromtimestamp(observed_epoch, tz=timezone.utc).isoformat() if observed_epoch else "unknown"
    payload = {
        "track": state.track.value,
        "subphase": state.subphase_id,
        "status": state.status_canonical,
        "handoff": state.handoff_available,
        "conflict": state.conflict,
        "vps": state.vps_or_deploy_hint,
        "deferred": state.deferred_hint,
    }
    payload_hash = _payload_hash(payload)
    flags: list[str] = []
    if state.conflict:
        flags.append("SSOT_CONFLICT")
    if state.vps_or_deploy_hint:
        flags.append("VPS_OR_DEPLOY")
    if state.deferred_hint:
        flags.append("DEFERRED")
    if not state.handoff_available:
        flags.append("HANDOFF_MISSING")

    return NormalizedReport(
        report_id=_report_id("ssot", state.track.value, observed_at, payload_hash),
        source="repository_ssot",
        track=state.track.value,
        observed_at=observed_at,
        source_status=state.status_canonical,
        metrics={"subphase": state.subphase_id},
        flags=tuple(flags),
        payload_hash=payload_hash,
    )


def decide_report(
    report: NormalizedReport,
    *,
    envelope: AutonomyEnvelope | None = None,
    now: datetime | None = None,
) -> ControlDecision:
    """Route a report.  No branch here grants execution permission."""
    current = now or datetime.now(timezone.utc)
    status = report.source_status.upper()
    cursor_action = report.cursor_action.upper()
    flags = set(report.flags)

    if report.environment.lower() == "live" or "REAL_EXECUTION_ENABLED" in flags:
        return _decision(ControlAction.SAFETY_HALT, "LIVE_EXECUTION", "live execution requires director intervention")

    mdd = _number(report.metrics.get("mdd_pct"))
    cap = _number(report.metrics.get("mdd_cap_pct"))
    if mdd is not None and cap is not None and cap > 0:
        if mdd >= cap:
            return _decision(ControlAction.SAFETY_HALT, "MDD_CAP_BREACH", f"MDD {mdd:.2f}% reached cap {cap:.2f}%")
        if (mdd / cap) >= MDD_REVIEW_UTILIZATION:
            return _decision(
                ControlAction.CLAUDE_REVIEW, "MDD_BUFFER_LOW", f"MDD is using {(mdd / cap) * 100:.1f}% of its cap"
            )

    if "CLOSED_COUNT_INCONSISTENT" in flags:
        return _decision(
            ControlAction.CLAUDE_REVIEW,
            "DATA_INTEGRITY_CLOSED_COUNT",
            "forward trades exist but CLOSED count is zero",
        )
    if "RECALL_ACTION_INCONSISTENT" in flags or "SSOT_CONFLICT" in flags:
        return _decision(ControlAction.QUARANTINE, "SOURCE_CONFLICT", "report/SSOT signals disagree")
    if "SOURCE_ERROR" in flags:
        return _decision(ControlAction.CLAUDE_REVIEW, "SOURCE_ERROR", "report source emitted an error")

    if "HANDOFF_MISSING" in flags and status in {"WAIT_CURSOR_IMPL", "WAIT_CLAUDE_OK"}:
        return _decision(
            ControlAction.QUARANTINE,
            "HANDOFF_MISSING",
            "active implementation/verification status has no matching Handoff",
        )

    if status in {"WAIT_CURSOR_VPS", "WAIT_DIRECTOR"} or cursor_action in {
        "DIRECTOR_SSH_CHECK",
        "DIRECTOR_ACTION",
    }:
        return _decision(
            ControlAction.WAIT_WEEKEND, "DIRECTOR_REQUIRED", "VPS/director action is outside weekday autonomy"
        )

    if "DEFERRED" in flags or (
        "VPS_OR_DEPLOY" in flags and status in {"DONE", "SUB_DONE", "CLOSED", "PARK", "IMPLEMENTATION_VERIFIED"}
    ):
        return _decision(
            ControlAction.WAIT_WEEKEND,
            "WEEKEND_OPERATION",
            "approved work is waiting for a director-only VPS/deploy step",
        )

    if status == "WAIT_CLAUDE_OK" or cursor_action in {
        "RECALL_FORK",
        "REPORT_TO_CLAUDE",
        "PHASE2_CANDIDATE",
    }:
        return _decision(ControlAction.CLAUDE_REVIEW, "ARCHITECT_REVIEW", "Claude review or fork decision is due")

    if status == "WAIT_CURSOR_IMPL":
        desired = ControlAction.CURSOR_IMPLEMENT
        if envelope is not None and envelope.allows(report, desired, current):
            return ControlDecision(
                action=desired,
                reason_code="ENVELOPE_ELIGIBLE",
                reason="Handoff is eligible for a bounded pull-request task",
                priority="normal",
                work_state="QUEUED",
                execution_authorized=False,
            )
        return _decision(
            ControlAction.WAIT_WEEKEND,
            "ENVELOPE_REQUIRED",
            "implementation needs a current weekly autonomy envelope",
        )

    passive = {
        "NONE",
        "OBSERVE_HOLD",
        "OBSERVE_LIQ_BAND",
        "DONE",
        "SUB_DONE",
        "CLOSED",
        "PARK",
        "IMPLEMENTATION_VERIFIED",
    }
    if status in passive or cursor_action in passive:
        return _decision(
            ControlAction.OBSERVE_ONLY, "NO_ACTION_DUE", "observation continues; no implementation is authorized"
        )

    return _decision(
        ControlAction.QUARANTINE, "UNKNOWN_SIGNAL", f"unrecognized status/action: {status or cursor_action or 'empty'}"
    )


def _decision(action: ControlAction, code: str, reason: str) -> ControlDecision:
    priority = "urgent" if action == ControlAction.SAFETY_HALT else "review"
    if action == ControlAction.OBSERVE_ONLY:
        priority = "info"
    state = {
        ControlAction.OBSERVE_ONLY: "OBSERVED",
        ControlAction.CLAUDE_REVIEW: "QUEUED",
        ControlAction.CURSOR_IMPLEMENT: "QUEUED",
        ControlAction.WAIT_WEEKEND: "AWAITING_DIRECTOR",
        ControlAction.SAFETY_HALT: "HALTED",
        ControlAction.QUARANTINE: "QUARANTINED",
    }[action]
    return ControlDecision(
        action=action,
        reason_code=code,
        reason=reason,
        priority=priority,
        work_state=state,
        execution_authorized=False,
    )


class ControlPlaneStore:
    """Append-only intake plus one deterministic decision per report."""

    def __init__(self, path: Path):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                track TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                normalized_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS decisions (
                report_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                reason TEXT NOT NULL,
                priority TEXT NOT NULL,
                work_state TEXT NOT NULL,
                execution_authorized INTEGER NOT NULL DEFAULT 0,
                decided_at TEXT NOT NULL,
                FOREIGN KEY(report_id) REFERENCES reports(report_id)
            )
            """
        )
        return conn

    def record(self, report: NormalizedReport, decision: ControlDecision) -> bool:
        normalized = json.dumps(asdict(report), ensure_ascii=False, sort_keys=True)
        now = _now_iso()
        with self._connect() as conn:
            before = conn.total_changes
            conn.execute(
                "INSERT OR IGNORE INTO reports VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    report.report_id,
                    report.source,
                    report.track,
                    report.observed_at,
                    report.payload_hash,
                    normalized,
                    now,
                ),
            )
            inserted = conn.total_changes > before
            conn.execute(
                "INSERT OR IGNORE INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    report.report_id,
                    decision.action.value,
                    decision.reason_code,
                    decision.reason,
                    decision.priority,
                    decision.work_state,
                    int(decision.execution_authorized),
                    now,
                ),
            )
        return inserted


def scan_reports(
    *,
    north_star_path: Path | None = None,
    bitget_ops_path: Path | None = None,
    include_ssot: bool = True,
) -> tuple[list[NormalizedReport], list[str]]:
    reports: list[NormalizedReport] = []
    errors: list[str] = []
    if north_star_path is not None:
        try:
            reports.append(read_north_star_ledger(north_star_path))
        except Exception as exc:
            errors.append(f"north_star:{type(exc).__name__}:{exc}")
    if bitget_ops_path is not None:
        try:
            report = read_latest_bitget_digest(bitget_ops_path)
            if report is not None:
                reports.append(report)
        except Exception as exc:
            errors.append(f"bitget_ops:{type(exc).__name__}:{exc}")
    if include_ssot:
        for track in Track:
            try:
                reports.append(normalize_ssot_state(resolve_state(track)))
            except Exception as exc:
                errors.append(f"ssot_{track.value}:{type(exc).__name__}:{exc}")
    return reports, errors


def evaluate_reports(
    reports: Iterable[NormalizedReport],
    *,
    envelope: AutonomyEnvelope | None,
    store: ControlPlaneStore | None,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for report in reports:
        decision = decide_report(report, envelope=envelope)
        inserted = store.record(report, decision) if store is not None else False
        output.append(
            {
                "report": asdict(report),
                "decision": {**asdict(decision), "action": decision.action.value},
                "inserted": inserted,
            }
        )
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Quant report control plane (intake only)")
    parser.add_argument("--north-star-ledger", type=Path)
    parser.add_argument("--bitget-ops-db", type=Path)
    parser.add_argument("--envelope", type=Path)
    parser.add_argument("--queue-db", type=Path, default=DEFAULT_QUEUE_DB)
    parser.add_argument("--no-ssot", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist the queue")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        envelope = load_envelope(args.envelope)
    except Exception as exc:
        sys.stderr.write(f"invalid envelope: {exc}\n")
        return 2

    reports, errors = scan_reports(
        north_star_path=args.north_star_ledger,
        bitget_ops_path=args.bitget_ops_db,
        include_ssot=not args.no_ssot,
    )
    store = None if args.dry_run else ControlPlaneStore(args.queue_db)
    evaluated = evaluate_reports(reports, envelope=envelope, store=store)
    payload = {
        "schema": SCHEMA_VERSION,
        "mode": "INTAKE_ONLY",
        "execution_authorized": False,
        "reports": evaluated,
        "errors": errors,
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=not args.json, indent=2) + "\n")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
