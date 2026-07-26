"""Fast Safety Policy Admin CLI — inspect, validate, backup, verify, apply, rollback (Chapter 3-B0D2B3B4)."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping, Sequence
from enum import Enum
from pathlib import Path
from typing import Any

from fast_safety_policy_admin import (
    ApplyResult,
    FastSafetyReadStatus,
    RollbackResult,
    apply_disabled_policy,
    apply_enabled_policy,
    create_backup_record,
    inspect_fast_safety_policy,
    rollback_policy_absent,
    rollback_policy_value,
    validate_admin_apply_document,
    verify_fast_safety_policy,
)
from fast_safety_policy_admin_artifacts import (
    AppliedCheckpoint,
    BackupRecord,
    applied_checkpoint_to_document,
    decode_artifact_json,
    encode_artifact_json,
    load_applied_checkpoint,
    load_approval_manifest,
    load_backup_record,
    save_applied_checkpoint,
    save_backup_record,
    validate_approval_manifest_for_enabled_policy,
)
from fast_safety_policy_store import FAST_SAFETY_POLICY_KEYS, policy_key_for_market

EXIT_OK = 0
EXIT_VALIDATION_ERROR = 1
EXIT_OPERATIONAL_ERROR = 2
EXIT_CONCURRENCY_ERROR = 3
EXIT_RECOVERY_REQUIRED = 4

_CONCURRENCY_REASON = "concurrency conflict"

_COMMANDS = frozenset(
    {
        "inspect",
        "validate",
        "backup",
        "verify",
        "apply-disabled",
        "apply-enabled",
        "rollback-value",
        "rollback-absent",
    }
)

_INSPECT_ALLOWED = frozenset({"market", "db_path"})
_VALIDATE_ALLOWED = frozenset({"document", "expected_enabled"})
_BACKUP_ALLOWED = frozenset({"market", "db_path", "created_at", "output", "overwrite"})
_VERIFY_ALLOWED = frozenset(
    {"market", "db_path", "expected_status", "expected_policy_sha256"}
)
_APPLY_DISABLED_ALLOWED = frozenset(
    {
        "market",
        "db_path",
        "backup",
        "generated_at",
        "checkpoint_created_at",
        "checkpoint_output",
        "execute",
        "overwrite",
    }
)
_APPLY_ENABLED_ALLOWED = frozenset(
    {
        "market",
        "db_path",
        "backup",
        "document",
        "approval",
        "checkpoint_created_at",
        "checkpoint_output",
        "execute",
        "overwrite",
    }
)
_ROLLBACK_ALLOWED = frozenset({"db_path", "backup", "checkpoint", "execute"})

_PRESENT_CLASSIFICATIONS = frozenset(
    {
        FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
        FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
    }
)

_ROLLBACK_CONCURRENCY_REASONS = frozenset(
    {
        "concurrency conflict",
        "checkpoint mismatch",
        "current row missing",
    }
)
_ROLLBACK_VALIDATION_REASONS = frozenset(
    {
        "backup was absent",
        "backup was not absent",
        "backup version mismatch",
        "market mismatch",
        "config key mismatch",
        "config key not whitelisted",
        "created_at missing",
        "absent backup has row version",
        "absent backup has value json",
        "absent backup has checksum",
        "absent backup classification mismatch",
        "invalid previous row version",
        "previous value json missing",
        "backup checksum missing",
        "backup checksum format invalid",
        "backup checksum mismatch",
        "invalid previous classification",
        "backup json invalid",
        "backup classification stale",
        "checkpoint version mismatch",
        "checkpoint market/key mismatch",
        "missing backup value json",
        "backup classification mismatch",
    }
)
_ROLLBACK_READ_ERROR_REASONS = frozenset({"read failed"})
_ROLLBACK_VERIFICATION_REASONS = frozenset(
    {
        "restored hash mismatch",
        "restored classification mismatch",
        "row still present",
        "not absent after rollback",
        "rollback failed",
    }
)

_OPTIONAL_FIELDS = (
    "market",
    "db_path",
    "document",
    "expected_enabled",
    "created_at",
    "output",
    "expected_status",
    "expected_policy_sha256",
    "backup",
    "approval",
    "generated_at",
    "checkpoint_created_at",
    "checkpoint_output",
    "checkpoint",
)

_EXPECTED_ENABLED_MAP = {
    "any": None,
    "enabled": True,
    "disabled": False,
}

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_READ_STATUS_VALUES = frozenset(status.value for status in FastSafetyReadStatus)


def _to_json_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _to_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_value(item) for item in value]
    raise TypeError("value not json serializable")


def _emit_json(document: Mapping[str, Any]) -> None:
    sys.stdout.write(encode_artifact_json(document) + "\n")


def _usage_error(message: str = "usage-error") -> int:
    sys.stderr.write(f"{message}\n")
    return EXIT_OPERATIONAL_ERROR


def _operational_error(message: str = "internal-error") -> int:
    sys.stderr.write(f"{message}\n")
    return EXIT_OPERATIONAL_ERROR


def _validate_market(
    market: object,
) -> tuple[str | None, str | None, str]:
    if not isinstance(market, str):
        return None, None, "invalid market"
    market_text = market.strip().upper()
    config_key = policy_key_for_market(market_text)
    if config_key is None:
        return None, None, "unsupported market"
    if config_key not in FAST_SAFETY_POLICY_KEYS.values():
        return None, None, "unsupported market"
    return market_text, config_key, ""


def _require_non_empty_str(value: object, field: str) -> tuple[str | None, str | None]:
    if not isinstance(value, str) or not value.strip():
        return None, f"missing {field}"
    return value, None


def _forbidden_optionals(args: argparse.Namespace, allowed: frozenset[str]) -> list[str]:
    forbidden: list[str] = []
    for field in _OPTIONAL_FIELDS:
        if field in allowed:
            continue
        if getattr(args, field) is not None:
            forbidden.append(field)
    if "overwrite" not in allowed and args.overwrite:
        forbidden.append("overwrite")
    if "execute" not in allowed and args.execute:
        forbidden.append("execute")
    return forbidden


def _validate_command_args(args: argparse.Namespace) -> str | None:
    command = args.command
    if command == "inspect":
        allowed = _INSPECT_ALLOWED
    elif command == "validate":
        allowed = _VALIDATE_ALLOWED
    elif command == "backup":
        allowed = _BACKUP_ALLOWED
    elif command == "verify":
        allowed = _VERIFY_ALLOWED
    elif command == "apply-disabled":
        allowed = _APPLY_DISABLED_ALLOWED
    elif command == "apply-enabled":
        allowed = _APPLY_ENABLED_ALLOWED
    elif command in {"rollback-value", "rollback-absent"}:
        allowed = _ROLLBACK_ALLOWED
    else:
        return "unknown command"

    forbidden = _forbidden_optionals(args, allowed)
    if forbidden:
        return "forbidden arguments for command"

    if command in {"inspect", "backup", "verify"}:
        if args.market is None:
            return "missing market"
        if args.db_path is None:
            return "missing db-path"
        if not isinstance(args.db_path, str) or not args.db_path.strip():
            return "missing db-path"

    if command == "validate":
        if args.document is None:
            return "missing document"
        if not isinstance(args.document, str) or not args.document.strip():
            return "missing document"

    if command == "backup":
        if args.created_at is None:
            return "missing created-at"
        if not isinstance(args.created_at, str) or not args.created_at.strip():
            return "missing created-at"
        if args.output is None:
            return "missing output"
        if not isinstance(args.output, str) or not args.output.strip():
            return "missing output"

    if command in {"apply-disabled", "apply-enabled"}:
        if args.market is None:
            return "missing market"
        if args.db_path is None:
            return "missing db-path"
        if not isinstance(args.db_path, str) or not args.db_path.strip():
            return "missing db-path"
        if args.backup is None:
            return "missing backup"
        if not isinstance(args.backup, str) or not args.backup.strip():
            return "missing backup"
        if args.checkpoint_created_at is None:
            return "missing checkpoint-created-at"
        if (
            not isinstance(args.checkpoint_created_at, str)
            or not args.checkpoint_created_at.strip()
        ):
            return "missing checkpoint-created-at"
        if args.checkpoint_output is None:
            return "missing checkpoint-output"
        if not isinstance(args.checkpoint_output, str) or not args.checkpoint_output.strip():
            return "missing checkpoint-output"

    if command == "apply-disabled":
        if args.generated_at is None:
            return "missing generated-at"
        if not isinstance(args.generated_at, str) or not args.generated_at.strip():
            return "missing generated-at"

    if command == "apply-enabled":
        if args.document is None:
            return "missing document"
        if not isinstance(args.document, str) or not args.document.strip():
            return "missing document"
        if args.approval is None:
            return "missing approval"
        if not isinstance(args.approval, str) or not args.approval.strip():
            return "missing approval"

    if command in {"rollback-value", "rollback-absent"}:
        if args.db_path is None:
            return "missing db-path"
        if not isinstance(args.db_path, str) or not args.db_path.strip():
            return "missing db-path"
        if args.backup is None:
            return "missing backup"
        if not isinstance(args.backup, str) or not args.backup.strip():
            return "missing backup"
        if args.checkpoint is None:
            return "missing checkpoint"
        if not isinstance(args.checkpoint, str) or not args.checkpoint.strip():
            return "missing checkpoint"

    return None


def _inspect_response(result: Any, *, ok: bool, reason: str) -> dict[str, Any]:
    document = result.document
    return {
        "command": "inspect",
        "ok": ok,
        "reason": reason,
        "market": result.market,
        "config_key": result.config_key,
        "status": result.status.value,
        "row_version": result.row_version,
        "value_json_sha256": result.value_json_sha256,
        "document": _to_json_value(document) if document is not None else None,
        "error": result.error,
    }


def _run_inspect(args: argparse.Namespace) -> int:
    market, config_key, market_reason = _validate_market(args.market)
    if market is None:
        _emit_json(
            {
                "command": "inspect",
                "ok": False,
                "reason": market_reason,
                "market": args.market.strip().upper()
                if isinstance(args.market, str)
                else "",
                "config_key": None,
                "status": FastSafetyReadStatus.READ_ERROR.value,
                "row_version": None,
                "value_json_sha256": None,
                "document": None,
                "error": market_reason,
            }
        )
        return EXIT_VALIDATION_ERROR

    db_path, db_reason = _require_non_empty_str(args.db_path, "db-path")
    assert db_path is not None

    result = inspect_fast_safety_policy(market, db_path=db_path)
    if result.status is FastSafetyReadStatus.READ_ERROR:
        _emit_json(_inspect_response(result, ok=False, reason=result.error or "read error"))
        if result.error and result.error not in {"unsupported market", "unsupported config key"}:
            sys.stderr.write("read-error\n")
        return EXIT_OPERATIONAL_ERROR

    _emit_json(_inspect_response(result, ok=True, reason=""))
    return EXIT_OK


def _run_validate(args: argparse.Namespace) -> int:
    expected_choice = args.expected_enabled if args.expected_enabled is not None else "any"
    expected_enabled = _EXPECTED_ENABLED_MAP[expected_choice]

    try:
        with open(args.document, encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        sys.stderr.write("file-read-error\n")
        return EXIT_OPERATIONAL_ERROR

    try:
        document = decode_artifact_json(text)
    except json.JSONDecodeError:
        _emit_json(
            {
                "command": "validate",
                "ok": False,
                "reason": "malformed json",
                "payload": None,
                "policy_document_sha256": None,
            }
        )
        return EXIT_VALIDATION_ERROR
    except (TypeError, ValueError):
        _emit_json(
            {
                "command": "validate",
                "ok": False,
                "reason": "document must be a mapping",
                "payload": None,
                "policy_document_sha256": None,
            }
        )
        return EXIT_VALIDATION_ERROR

    validation = validate_admin_apply_document(
        document,
        expected_enabled=expected_enabled,
    )
    payload = validation.payload
    _emit_json(
        {
            "command": "validate",
            "ok": validation.ok,
            "reason": validation.reason,
            "payload": _to_json_value(payload) if payload is not None else None,
            "policy_document_sha256": validation.policy_document_sha256,
        }
    )
    return EXIT_OK if validation.ok else EXIT_VALIDATION_ERROR


def _run_backup(args: argparse.Namespace) -> int:
    market, config_key, market_reason = _validate_market(args.market)
    if market is None:
        _emit_json(
            {
                "command": "backup",
                "ok": False,
                "reason": market_reason,
                "market": args.market.strip().upper()
                if isinstance(args.market, str)
                else "",
                "config_key": None,
                "backup_version": None,
                "previous_absent": None,
                "previous_row_version": None,
                "previous_classification": None,
                "backup_value_json_sha256": None,
                "artifact_written": False,
            }
        )
        return EXIT_VALIDATION_ERROR

    db_path, _ = _require_non_empty_str(args.db_path, "db-path")
    assert db_path is not None

    created_at, created_reason = _require_non_empty_str(args.created_at, "created-at")
    if created_at is None:
        _emit_json(
            {
                "command": "backup",
                "ok": False,
                "reason": created_reason or "missing created-at",
                "market": market,
                "config_key": config_key,
                "backup_version": None,
                "previous_absent": None,
                "previous_row_version": None,
                "previous_classification": None,
                "backup_value_json_sha256": None,
                "artifact_written": False,
            }
        )
        return EXIT_VALIDATION_ERROR

    record = create_backup_record(
        market,
        created_at=created_at,
        db_path=db_path,
    )
    if record is None:
        inspection = inspect_fast_safety_policy(market, db_path=db_path)
        if inspection.status is FastSafetyReadStatus.READ_ERROR:
            _emit_json(
                {
                    "command": "backup",
                    "ok": False,
                    "reason": inspection.error or "read error",
                    "market": market,
                    "config_key": config_key,
                    "backup_version": None,
                    "previous_absent": None,
                    "previous_row_version": None,
                    "previous_classification": None,
                    "backup_value_json_sha256": None,
                    "artifact_written": False,
                }
            )
            sys.stderr.write("read-error\n")
            return EXIT_OPERATIONAL_ERROR

        _emit_json(
            {
                "command": "backup",
                "ok": False,
                "reason": "backup blocked",
                "market": market,
                "config_key": config_key,
                "backup_version": None,
                "previous_absent": None,
                "previous_row_version": None,
                "previous_classification": inspection.status.value,
                "backup_value_json_sha256": None,
                "artifact_written": False,
            }
        )
        return EXIT_VALIDATION_ERROR

    output_path = args.output
    assert isinstance(output_path, str)

    from pathlib import Path

    if Path(output_path).exists() and not args.overwrite:
        sys.stderr.write("file-write-error\n")
        _emit_json(
            {
                "command": "backup",
                "ok": False,
                "reason": "output exists",
                "market": record.market,
                "config_key": record.config_key,
                "backup_version": record.backup_version,
                "previous_absent": record.previous_absent,
                "previous_row_version": record.previous_row_version,
                "previous_classification": record.previous_classification,
                "backup_value_json_sha256": record.backup_value_json_sha256,
                "artifact_written": False,
            }
        )
        return EXIT_OPERATIONAL_ERROR

    try:
        save_backup_record(output_path, record, overwrite=args.overwrite)
    except OSError:
        sys.stderr.write("file-write-error\n")
        _emit_json(
            {
                "command": "backup",
                "ok": False,
                "reason": "file write failed",
                "market": record.market,
                "config_key": record.config_key,
                "backup_version": record.backup_version,
                "previous_absent": record.previous_absent,
                "previous_row_version": record.previous_row_version,
                "previous_classification": record.previous_classification,
                "backup_value_json_sha256": record.backup_value_json_sha256,
                "artifact_written": False,
            }
        )
        return EXIT_OPERATIONAL_ERROR

    _emit_json(
        {
            "command": "backup",
            "ok": True,
            "reason": "",
            "market": record.market,
            "config_key": record.config_key,
            "backup_version": record.backup_version,
            "previous_absent": record.previous_absent,
            "previous_row_version": record.previous_row_version,
            "previous_classification": record.previous_classification,
            "backup_value_json_sha256": record.backup_value_json_sha256,
            "artifact_written": True,
        }
    )
    return EXIT_OK


def _run_verify(args: argparse.Namespace) -> int:
    market, config_key, market_reason = _validate_market(args.market)
    if market is None:
        _emit_json(
            {
                "command": "verify",
                "ok": False,
                "reason": market_reason,
                "market": args.market.strip().upper()
                if isinstance(args.market, str)
                else "",
                "config_key": None,
                "status": FastSafetyReadStatus.READ_ERROR.value,
                "row_version": None,
                "value_json_sha256": None,
                "policy_document_sha256": None,
                "error": market_reason,
            }
        )
        return EXIT_VALIDATION_ERROR

    db_path, _ = _require_non_empty_str(args.db_path, "db-path")
    assert db_path is not None

    expected_status: FastSafetyReadStatus | None = None
    if args.expected_status is not None:
        if args.expected_status not in _READ_STATUS_VALUES:
            _emit_json(
                {
                    "command": "verify",
                    "ok": False,
                    "reason": "invalid expected status",
                    "market": market,
                    "config_key": config_key,
                    "status": FastSafetyReadStatus.READ_ERROR.value,
                    "row_version": None,
                    "value_json_sha256": None,
                    "policy_document_sha256": None,
                    "error": "invalid expected status",
                }
            )
            return EXIT_VALIDATION_ERROR
        expected_status = FastSafetyReadStatus(args.expected_status)

    expected_policy_sha256 = args.expected_policy_sha256
    if expected_policy_sha256 is not None:
        if not isinstance(expected_policy_sha256, str) or not _HEX64_RE.fullmatch(
            expected_policy_sha256
        ):
            _emit_json(
                {
                    "command": "verify",
                    "ok": False,
                    "reason": "invalid expected policy sha256",
                    "market": market,
                    "config_key": config_key,
                    "status": FastSafetyReadStatus.READ_ERROR.value,
                    "row_version": None,
                    "value_json_sha256": None,
                    "policy_document_sha256": None,
                    "error": "invalid expected policy sha256",
                }
            )
            return EXIT_VALIDATION_ERROR

    result = verify_fast_safety_policy(
        market,
        expected_status=expected_status,
        expected_policy_sha256=expected_policy_sha256,
        db_path=db_path,
    )
    inspection = result.inspection

    if inspection.status is FastSafetyReadStatus.READ_ERROR:
        _emit_json(
            {
                "command": "verify",
                "ok": False,
                "reason": result.reason or inspection.error or "read error",
                "market": inspection.market,
                "config_key": inspection.config_key,
                "status": inspection.status.value,
                "row_version": inspection.row_version,
                "value_json_sha256": inspection.value_json_sha256,
                "policy_document_sha256": result.policy_document_sha256,
                "error": inspection.error,
            }
        )
        sys.stderr.write("read-error\n")
        return EXIT_OPERATIONAL_ERROR

    _emit_json(
        {
            "command": "verify",
            "ok": result.ok,
            "reason": result.reason,
            "market": inspection.market,
            "config_key": inspection.config_key,
            "status": inspection.status.value,
            "row_version": inspection.row_version,
            "value_json_sha256": inspection.value_json_sha256,
            "policy_document_sha256": result.policy_document_sha256,
            "error": inspection.error,
        }
    )
    return EXIT_OK if result.ok else EXIT_VALIDATION_ERROR


def _apply_response(
    command: str,
    *,
    ok: bool,
    reason: str,
    executed: bool,
    market: str | None,
    config_key: str | None,
    expected_classification: str,
    backup_previous_absent: bool | None,
    checkpoint_artifact_written: bool = False,
    checkpoint: Mapping[str, Any] | None = None,
    requires_rollback: bool = False,
    recovery_required: bool = False,
    policy_document_sha256: str | None = None,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "command": command,
        "ok": ok,
        "reason": reason,
        "executed": executed,
        "market": market,
        "config_key": config_key,
        "expected_classification": expected_classification,
        "backup_previous_absent": backup_previous_absent,
        "checkpoint_artifact_written": checkpoint_artifact_written,
        "checkpoint": _to_json_value(checkpoint) if checkpoint is not None else None,
        "requires_rollback": requires_rollback,
        "recovery_required": recovery_required,
    }
    if policy_document_sha256 is not None:
        document["policy_document_sha256"] = policy_document_sha256
    return document


def _check_backup_against_current_state(
    backup: BackupRecord,
    *,
    db_path: str,
) -> tuple[bool, str, int]:
    inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
    status = inspection.status

    if status is FastSafetyReadStatus.READ_ERROR:
        return False, inspection.error or "read error", EXIT_OPERATIONAL_ERROR

    if status in {
        FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
        FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON,
    }:
        return False, "apply blocked", EXIT_VALIDATION_ERROR

    if backup.previous_absent:
        if status is not FastSafetyReadStatus.ABSENT:
            return False, "stale backup", EXIT_CONCURRENCY_ERROR
        return True, "", EXIT_OK

    if status.value != backup.previous_classification:
        return False, "stale backup", EXIT_CONCURRENCY_ERROR
    if inspection.row_version != backup.previous_row_version:
        return False, "stale backup", EXIT_CONCURRENCY_ERROR
    if inspection.value_json_sha256 != backup.backup_value_json_sha256:
        return False, "stale backup", EXIT_CONCURRENCY_ERROR
    return True, "", EXIT_OK


def _preflight_checkpoint_output(
    path: str | os.PathLike[str],
    *,
    overwrite: bool,
) -> None:
    path_text = os.fspath(path).strip()
    if not path_text:
        raise ValueError("empty checkpoint output path")

    target = Path(path_text)
    parent = target.parent
    if not parent.exists():
        raise FileNotFoundError("parent directory does not exist")
    if not parent.is_dir():
        raise NotADirectoryError("parent path is not a directory")
    if not overwrite and target.exists():
        raise FileExistsError("target already exists")

    fd, temp_path_str = tempfile.mkstemp(
        prefix=".fast_safety_checkpoint_preflight_",
        suffix=".tmp",
        dir=str(parent),
    )
    os.close(fd)
    os.unlink(temp_path_str)


def _load_backup_artifact(path: str) -> tuple[BackupRecord | None, str, int]:
    try:
        return load_backup_record(path), "", EXIT_OK
    except OSError:
        sys.stderr.write("file-read-error\n")
        return None, "file-read-error", EXIT_OPERATIONAL_ERROR
    except json.JSONDecodeError:
        return None, "malformed backup", EXIT_VALIDATION_ERROR
    except (TypeError, ValueError):
        return None, "invalid backup", EXIT_VALIDATION_ERROR
    except Exception:
        return None, "internal-error", EXIT_OPERATIONAL_ERROR


def _load_enabled_policy_document(
    path: str,
) -> tuple[Mapping[str, Any] | None, str | None, int]:
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        sys.stderr.write("file-read-error\n")
        return None, None, EXIT_OPERATIONAL_ERROR

    try:
        document = decode_artifact_json(text)
    except json.JSONDecodeError:
        return None, "malformed json", EXIT_VALIDATION_ERROR
    except (TypeError, ValueError):
        return None, "document must be a mapping", EXIT_VALIDATION_ERROR
    except Exception:
        return None, "internal-error", EXIT_OPERATIONAL_ERROR

    validation = validate_admin_apply_document(document, expected_enabled=True)
    if not validation.ok:
        return None, validation.reason or "invalid policy document", EXIT_VALIDATION_ERROR
    if validation.policy_document_sha256 is None:
        return None, "missing policy checksum", EXIT_VALIDATION_ERROR
    return document, validation.policy_document_sha256, EXIT_OK


def _validate_enabled_approval(
    approval_path: str,
    *,
    market: str,
    config_key: str,
    policy_document: Mapping[str, Any],
) -> tuple[bool, str, int]:
    try:
        manifest = load_approval_manifest(approval_path)
    except OSError:
        sys.stderr.write("file-read-error\n")
        return False, "file-read-error", EXIT_OPERATIONAL_ERROR
    except json.JSONDecodeError:
        return False, "malformed manifest", EXIT_VALIDATION_ERROR
    except (TypeError, ValueError):
        return False, "invalid manifest", EXIT_VALIDATION_ERROR
    except Exception:
        return False, "internal-error", EXIT_OPERATIONAL_ERROR

    if manifest.market != market:
        return False, "market mismatch", EXIT_VALIDATION_ERROR
    if manifest.config_key != config_key:
        return False, "config key mismatch", EXIT_VALIDATION_ERROR

    result = validate_approval_manifest_for_enabled_policy(manifest, policy_document)
    if not result.ok:
        return False, result.reason, EXIT_VALIDATION_ERROR
    return True, "", EXIT_OK


def _apply_result_exit_code(result: ApplyResult) -> int:
    if result.reason == _CONCURRENCY_REASON:
        return EXIT_CONCURRENCY_ERROR
    if result.reason in {"write failed"}:
        return EXIT_OPERATIONAL_ERROR
    return EXIT_VALIDATION_ERROR


def _handle_apply_result(
    result: ApplyResult,
    *,
    command: str,
    checkpoint_output: str,
    overwrite: bool,
    market: str,
    config_key: str,
    expected_classification: str,
    backup_previous_absent: bool,
    policy_document_sha256: str | None = None,
) -> int:
    if result.requires_rollback and result.checkpoint is None:
        return _operational_error()
    if result.ok and (result.checkpoint is None or result.requires_rollback):
        return _operational_error()
    if not result.ok and result.checkpoint is None and result.requires_rollback:
        return _operational_error()

    if (
        result.ok
        and result.checkpoint is not None
        and not result.requires_rollback
    ):
        checkpoint_doc = applied_checkpoint_to_document(result.checkpoint)
        try:
            save_applied_checkpoint(
                checkpoint_output,
                result.checkpoint,
                overwrite=overwrite,
            )
        except OSError:
            sys.stderr.write("checkpoint-artifact-error\n")
            _emit_json(
                _apply_response(
                    command,
                    ok=False,
                    reason="checkpoint-artifact-write-failed",
                    executed=True,
                    market=market,
                    config_key=config_key,
                    expected_classification=expected_classification,
                    backup_previous_absent=backup_previous_absent,
                    checkpoint_artifact_written=False,
                    checkpoint=checkpoint_doc,
                    requires_rollback=result.requires_rollback,
                    recovery_required=True,
                    policy_document_sha256=policy_document_sha256,
                )
            )
            return EXIT_RECOVERY_REQUIRED

        _emit_json(
            _apply_response(
                command,
                ok=True,
                reason="",
                executed=True,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup_previous_absent,
                checkpoint_artifact_written=True,
                checkpoint=checkpoint_doc,
                requires_rollback=False,
                recovery_required=False,
                policy_document_sha256=policy_document_sha256,
            )
        )
        return EXIT_OK

    if (
        not result.ok
        and result.checkpoint is not None
        and result.requires_rollback
    ):
        checkpoint_doc = applied_checkpoint_to_document(result.checkpoint)
        artifact_written = False
        try:
            save_applied_checkpoint(
                checkpoint_output,
                result.checkpoint,
                overwrite=overwrite,
            )
            artifact_written = True
        except OSError:
            pass

        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=result.reason,
                executed=True,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup_previous_absent,
                checkpoint_artifact_written=artifact_written,
                checkpoint=checkpoint_doc,
                requires_rollback=True,
                recovery_required=True,
                policy_document_sha256=policy_document_sha256,
            )
        )
        return EXIT_RECOVERY_REQUIRED

    if not result.ok and result.checkpoint is None and not result.requires_rollback:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=result.reason,
                executed=True,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup_previous_absent,
                checkpoint_artifact_written=False,
                checkpoint=None,
                requires_rollback=False,
                recovery_required=False,
                policy_document_sha256=policy_document_sha256,
            )
        )
        return _apply_result_exit_code(result)

    return _operational_error()


def _rollback_response(
    command: str,
    *,
    ok: bool,
    reason: str,
    executed: bool,
    market: str | None,
    config_key: str | None,
    backup: BackupRecord | None,
    checkpoint: AppliedCheckpoint | None,
    current_status: str | None,
    final_status: FastSafetyReadStatus | None,
    recovery_required: bool,
) -> dict[str, Any]:
    return {
        "command": command,
        "ok": ok,
        "reason": reason,
        "executed": executed,
        "market": market,
        "config_key": config_key,
        "backup_previous_absent": backup.previous_absent if backup is not None else None,
        "backup_previous_classification": (
            backup.previous_classification if backup is not None else None
        ),
        "checkpoint_row_version": checkpoint.row_version if checkpoint is not None else None,
        "checkpoint_value_json_sha256": (
            checkpoint.value_json_sha256 if checkpoint is not None else None
        ),
        "checkpoint_classification": (
            checkpoint.classification if checkpoint is not None else None
        ),
        "current_status": current_status,
        "final_status": final_status.value if final_status is not None else None,
        "recovery_required": recovery_required,
    }


def _load_checkpoint_artifact(path: str) -> tuple[AppliedCheckpoint | None, str, int]:
    try:
        return load_applied_checkpoint(path), "", EXIT_OK
    except OSError:
        sys.stderr.write("file-read-error\n")
        return None, "file-read-error", EXIT_OPERATIONAL_ERROR
    except json.JSONDecodeError:
        return None, "malformed checkpoint", EXIT_VALIDATION_ERROR
    except (TypeError, ValueError):
        return None, "invalid checkpoint", EXIT_VALIDATION_ERROR
    except Exception:
        return None, "internal-error", EXIT_OPERATIONAL_ERROR


def _validate_rollback_command_mode(
    backup: BackupRecord,
    *,
    value_mode: bool,
) -> tuple[bool, str]:
    if value_mode:
        if backup.previous_absent:
            return False, "backup was absent"
        if backup.previous_classification not in _PRESENT_CLASSIFICATIONS:
            return False, "invalid previous classification"
        if (
            backup.previous_row_version is None
            or isinstance(backup.previous_row_version, bool)
            or backup.previous_row_version <= 0
        ):
            return False, "invalid previous row version"
        if not isinstance(backup.previous_value_json, str):
            return False, "previous value json missing"
        if backup.backup_value_json_sha256 is None:
            return False, "backup checksum missing"
        return True, ""

    if not backup.previous_absent:
        return False, "backup was not absent"
    if backup.previous_classification != FastSafetyReadStatus.ABSENT.value:
        return False, "backup classification mismatch"
    return True, ""


def _validate_rollback_artifact_consistency(
    backup: BackupRecord,
    checkpoint: AppliedCheckpoint,
) -> tuple[bool, str]:
    _, config_key, market_reason = _validate_market(backup.market)
    if config_key is None:
        return False, market_reason
    if backup.market != checkpoint.market:
        return False, "market mismatch"
    if backup.config_key != checkpoint.config_key:
        return False, "config key mismatch"
    expected_key = policy_key_for_market(backup.market)
    if expected_key is None or backup.config_key != expected_key:
        return False, "config key mismatch"
    if backup.config_key not in FAST_SAFETY_POLICY_KEYS.values():
        return False, "config key not whitelisted"
    return True, ""


def _check_checkpoint_against_current_state(
    backup: BackupRecord,
    checkpoint: AppliedCheckpoint,
    *,
    db_path: str,
) -> tuple[bool, str, int, FastSafetyReadStatus | None]:
    inspection = inspect_fast_safety_policy(checkpoint.market, db_path=db_path)
    current_status = inspection.status

    if current_status is FastSafetyReadStatus.READ_ERROR:
        return (
            False,
            inspection.error or "read error",
            EXIT_OPERATIONAL_ERROR,
            current_status,
        )

    if current_status is FastSafetyReadStatus.ABSENT:
        return False, "stale checkpoint", EXIT_CONCURRENCY_ERROR, current_status

    if current_status in {
        FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
        FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON,
    }:
        return False, "rollback blocked", EXIT_VALIDATION_ERROR, current_status

    if (
        inspection.status.value != checkpoint.classification
        or inspection.row_version != checkpoint.row_version
        or inspection.value_json_sha256 != checkpoint.value_json_sha256
    ):
        return False, "stale checkpoint", EXIT_CONCURRENCY_ERROR, current_status

    return True, "", EXIT_OK, current_status


def _rollback_exit_code(result: RollbackResult) -> tuple[int, bool]:
    reason = result.reason
    if not reason:
        return EXIT_OK, False
    if reason in _ROLLBACK_CONCURRENCY_REASONS:
        return EXIT_CONCURRENCY_ERROR, False
    if reason in _ROLLBACK_VALIDATION_REASONS:
        return EXIT_VALIDATION_ERROR, False
    if reason in _ROLLBACK_READ_ERROR_REASONS:
        return EXIT_OPERATIONAL_ERROR, False
    if reason in _ROLLBACK_VERIFICATION_REASONS:
        return EXIT_RECOVERY_REQUIRED, True
    return EXIT_RECOVERY_REQUIRED, True


def _run_rollback(args: argparse.Namespace, *, value_mode: bool) -> int:
    command = "rollback-value" if value_mode else "rollback-absent"

    db_path, _ = _require_non_empty_str(args.db_path, "db-path")
    assert db_path is not None
    backup_path, _ = _require_non_empty_str(args.backup, "backup")
    assert backup_path is not None
    checkpoint_path, _ = _require_non_empty_str(args.checkpoint, "checkpoint")
    assert checkpoint_path is not None

    backup, backup_reason, backup_code = _load_backup_artifact(backup_path)
    if backup is None:
        _emit_json(
            _rollback_response(
                command,
                ok=False,
                reason=backup_reason,
                executed=False,
                market=None,
                config_key=None,
                backup=None,
                checkpoint=None,
                current_status=None,
                final_status=None,
                recovery_required=False,
            )
        )
        return backup_code

    checkpoint, checkpoint_reason, checkpoint_code = _load_checkpoint_artifact(
        checkpoint_path
    )
    if checkpoint is None:
        _emit_json(
            _rollback_response(
                command,
                ok=False,
                reason=checkpoint_reason,
                executed=False,
                market=backup.market,
                config_key=backup.config_key,
                backup=backup,
                checkpoint=None,
                current_status=None,
                final_status=None,
                recovery_required=False,
            )
        )
        return checkpoint_code

    consistency_ok, consistency_reason = _validate_rollback_artifact_consistency(
        backup,
        checkpoint,
    )
    if not consistency_ok:
        _emit_json(
            _rollback_response(
                command,
                ok=False,
                reason=consistency_reason,
                executed=False,
                market=backup.market,
                config_key=backup.config_key,
                backup=backup,
                checkpoint=checkpoint,
                current_status=None,
                final_status=None,
                recovery_required=False,
            )
        )
        return EXIT_VALIDATION_ERROR

    mode_ok, mode_reason = _validate_rollback_command_mode(
        backup,
        value_mode=value_mode,
    )
    if not mode_ok:
        _emit_json(
            _rollback_response(
                command,
                ok=False,
                reason=mode_reason,
                executed=False,
                market=backup.market,
                config_key=backup.config_key,
                backup=backup,
                checkpoint=checkpoint,
                current_status=None,
                final_status=None,
                recovery_required=False,
            )
        )
        return EXIT_VALIDATION_ERROR

    state_ok, state_reason, state_code, current_status = (
        _check_checkpoint_against_current_state(
            backup,
            checkpoint,
            db_path=db_path,
        )
    )
    current_status_value = (
        current_status.value if current_status is not None else None
    )
    if not state_ok:
        _emit_json(
            _rollback_response(
                command,
                ok=False,
                reason=state_reason,
                executed=False,
                market=backup.market,
                config_key=backup.config_key,
                backup=backup,
                checkpoint=checkpoint,
                current_status=current_status_value,
                final_status=None,
                recovery_required=False,
            )
        )
        if state_code == EXIT_OPERATIONAL_ERROR:
            sys.stderr.write("read-error\n")
        return state_code

    if not args.execute:
        _emit_json(
            _rollback_response(
                command,
                ok=True,
                reason="validated-no-write",
                executed=False,
                market=backup.market,
                config_key=backup.config_key,
                backup=backup,
                checkpoint=checkpoint,
                current_status=current_status_value,
                final_status=None,
                recovery_required=False,
            )
        )
        return EXIT_OK

    try:
        if value_mode:
            result = rollback_policy_value(
                backup,
                checkpoint,
                db_path=db_path,
            )
        else:
            result = rollback_policy_absent(
                backup,
                checkpoint,
                db_path=db_path,
            )
    except Exception:
        return _operational_error()

    if result.ok:
        if value_mode:
            success = result.final_status.value == backup.previous_classification
        else:
            success = result.final_status is FastSafetyReadStatus.ABSENT
        if success:
            _emit_json(
                _rollback_response(
                    command,
                    ok=True,
                    reason="",
                    executed=True,
                    market=backup.market,
                    config_key=backup.config_key,
                    backup=backup,
                    checkpoint=checkpoint,
                    current_status=current_status_value,
                    final_status=result.final_status,
                    recovery_required=False,
                )
            )
            return EXIT_OK

    exit_code, recovery_required = _rollback_exit_code(result)
    _emit_json(
        _rollback_response(
            command,
            ok=False,
            reason=result.reason,
            executed=True,
            market=backup.market,
            config_key=backup.config_key,
            backup=backup,
            checkpoint=checkpoint,
            current_status=current_status_value,
            final_status=result.final_status,
            recovery_required=recovery_required,
        )
    )
    if exit_code == EXIT_RECOVERY_REQUIRED:
        sys.stderr.write("rollback-verification-error\n")
    return exit_code


def _run_apply_disabled(args: argparse.Namespace) -> int:
    command = "apply-disabled"
    expected_classification = FastSafetyReadStatus.PRESENT_DISABLED_VALID.value

    market, config_key, market_reason = _validate_market(args.market)
    if market is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=market_reason,
                executed=False,
                market=args.market.strip().upper()
                if isinstance(args.market, str)
                else "",
                config_key=None,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR

    db_path, _ = _require_non_empty_str(args.db_path, "db-path")
    assert db_path is not None
    backup_path, _ = _require_non_empty_str(args.backup, "backup")
    assert backup_path is not None
    generated_at, generated_reason = _require_non_empty_str(args.generated_at, "generated-at")
    if generated_at is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=generated_reason or "missing generated-at",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR
    checkpoint_created_at, checkpoint_reason = _require_non_empty_str(
        args.checkpoint_created_at,
        "checkpoint-created-at",
    )
    if checkpoint_created_at is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=checkpoint_reason or "missing checkpoint-created-at",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR
    checkpoint_output, output_reason = _require_non_empty_str(
        args.checkpoint_output,
        "checkpoint-output",
    )
    if checkpoint_output is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=output_reason or "missing checkpoint-output",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR

    backup, backup_reason, backup_code = _load_backup_artifact(backup_path)
    if backup is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=backup_reason,
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return backup_code

    if backup.market != market or backup.config_key != config_key:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason="backup market/key mismatch",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
            )
        )
        return EXIT_VALIDATION_ERROR

    try:
        _preflight_checkpoint_output(checkpoint_output, overwrite=args.overwrite)
    except FileExistsError:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason="checkpoint output exists",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
            )
        )
        return EXIT_OPERATIONAL_ERROR
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError, ValueError):
        return _operational_error()

    state_ok, state_reason, state_code = _check_backup_against_current_state(
        backup,
        db_path=db_path,
    )
    if not state_ok:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=state_reason,
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
            )
        )
        if state_code == EXIT_OPERATIONAL_ERROR:
            sys.stderr.write("read-error\n")
        return state_code

    if not args.execute:
        _emit_json(
            _apply_response(
                command,
                ok=True,
                reason="validated-no-write",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
            )
        )
        return EXIT_OK

    try:
        result = apply_disabled_policy(
            market,
            generated_at,
            backup,
            checkpoint_created_at=checkpoint_created_at,
            db_path=db_path,
        )
    except Exception:
        return _operational_error()

    return _handle_apply_result(
        result,
        command=command,
        checkpoint_output=checkpoint_output,
        overwrite=args.overwrite,
        market=market,
        config_key=config_key,
        expected_classification=expected_classification,
        backup_previous_absent=backup.previous_absent,
    )


def _run_apply_enabled(args: argparse.Namespace) -> int:
    command = "apply-enabled"
    expected_classification = FastSafetyReadStatus.PRESENT_ENABLED_VALID.value

    market, config_key, market_reason = _validate_market(args.market)
    if market is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=market_reason,
                executed=False,
                market=args.market.strip().upper()
                if isinstance(args.market, str)
                else "",
                config_key=None,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR

    db_path, _ = _require_non_empty_str(args.db_path, "db-path")
    assert db_path is not None
    backup_path, _ = _require_non_empty_str(args.backup, "backup")
    assert backup_path is not None
    document_path, _ = _require_non_empty_str(args.document, "document")
    assert document_path is not None
    approval_path, _ = _require_non_empty_str(args.approval, "approval")
    assert approval_path is not None
    checkpoint_created_at, checkpoint_reason = _require_non_empty_str(
        args.checkpoint_created_at,
        "checkpoint-created-at",
    )
    if checkpoint_created_at is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=checkpoint_reason or "missing checkpoint-created-at",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR
    checkpoint_output, output_reason = _require_non_empty_str(
        args.checkpoint_output,
        "checkpoint-output",
    )
    if checkpoint_output is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=output_reason or "missing checkpoint-output",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return EXIT_VALIDATION_ERROR

    backup, backup_reason, backup_code = _load_backup_artifact(backup_path)
    if backup is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=backup_reason,
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=None,
            )
        )
        return backup_code

    if backup.market != market or backup.config_key != config_key:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason="backup market/key mismatch",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
            )
        )
        return EXIT_VALIDATION_ERROR

    policy_document, policy_sha256, policy_code = _load_enabled_policy_document(
        document_path
    )
    if policy_document is None:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=policy_sha256 or "invalid policy document",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
            )
        )
        return policy_code

    approval_ok, approval_reason, approval_code = _validate_enabled_approval(
        approval_path,
        market=market,
        config_key=config_key,
        policy_document=policy_document,
    )
    if not approval_ok:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=approval_reason,
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
                policy_document_sha256=policy_sha256,
            )
        )
        return approval_code

    try:
        _preflight_checkpoint_output(checkpoint_output, overwrite=args.overwrite)
    except FileExistsError:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason="checkpoint output exists",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
                policy_document_sha256=policy_sha256,
            )
        )
        return EXIT_OPERATIONAL_ERROR
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError, ValueError):
        return _operational_error()

    state_ok, state_reason, state_code = _check_backup_against_current_state(
        backup,
        db_path=db_path,
    )
    if not state_ok:
        _emit_json(
            _apply_response(
                command,
                ok=False,
                reason=state_reason,
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
                policy_document_sha256=policy_sha256,
            )
        )
        if state_code == EXIT_OPERATIONAL_ERROR:
            sys.stderr.write("read-error\n")
        return state_code

    if not args.execute:
        _emit_json(
            _apply_response(
                command,
                ok=True,
                reason="validated-no-write",
                executed=False,
                market=market,
                config_key=config_key,
                expected_classification=expected_classification,
                backup_previous_absent=backup.previous_absent,
                policy_document_sha256=policy_sha256,
            )
        )
        return EXIT_OK

    try:
        result = apply_enabled_policy(
            policy_document,
            backup,
            checkpoint_created_at=checkpoint_created_at,
            db_path=db_path,
        )
    except Exception:
        return _operational_error()

    return _handle_apply_result(
        result,
        command=command,
        checkpoint_output=checkpoint_output,
        overwrite=args.overwrite,
        market=market,
        config_key=config_key,
        expected_classification=expected_classification,
        backup_previous_absent=backup.previous_absent,
        policy_document_sha256=policy_sha256,
    )


def _dispatch(args: argparse.Namespace) -> int:
    validation_error = _validate_command_args(args)
    if validation_error is not None:
        return _usage_error()

    if args.command == "inspect":
        return _run_inspect(args)
    if args.command == "validate":
        return _run_validate(args)
    if args.command == "backup":
        return _run_backup(args)
    if args.command == "verify":
        return _run_verify(args)
    if args.command == "apply-disabled":
        return _run_apply_disabled(args)
    if args.command == "apply-enabled":
        return _run_apply_enabled(args)
    if args.command == "rollback-value":
        return _run_rollback(args, value_mode=True)
    if args.command == "rollback-absent":
        return _run_rollback(args, value_mode=False)
    return _usage_error()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fast_safety_policy_admin_cli")
    parser.add_argument(
        "command",
        choices=sorted(_COMMANDS),
    )
    parser.add_argument("--market", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--document", default=None)
    parser.add_argument(
        "--expected-enabled",
        choices=["any", "enabled", "disabled"],
        default=None,
    )
    parser.add_argument("--created-at", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--overwrite", action="store_true", default=False)
    parser.add_argument("--expected-status", default=None)
    parser.add_argument("--expected-policy-sha256", default=None)
    parser.add_argument("--backup", default=None)
    parser.add_argument("--approval", default=None)
    parser.add_argument("--generated-at", default=None)
    parser.add_argument("--checkpoint-created-at", default=None)
    parser.add_argument("--checkpoint-output", default=None)
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--execute", action="store_true", default=False)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        return _dispatch(args)
    except SystemExit as exc:
        if isinstance(exc.code, int) and exc.code != 0:
            return EXIT_OPERATIONAL_ERROR
        return EXIT_OK
    except Exception:
        return _operational_error()


if __name__ == "__main__":
    raise SystemExit(main())
