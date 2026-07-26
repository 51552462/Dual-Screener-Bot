"""Fast Safety Policy Admin CLI — read-only inspect, validate, backup, verify (Chapter 3-B0D2B3B2)."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any

from fast_safety_policy_admin import (
    FastSafetyReadStatus,
    create_backup_record,
    inspect_fast_safety_policy,
    validate_admin_apply_document,
    verify_fast_safety_policy,
)
from fast_safety_policy_admin_artifacts import (
    decode_artifact_json,
    encode_artifact_json,
    save_backup_record,
)
from fast_safety_policy_store import FAST_SAFETY_POLICY_KEYS, policy_key_for_market

EXIT_OK = 0
EXIT_VALIDATION_ERROR = 1
EXIT_OPERATIONAL_ERROR = 2

_COMMANDS = frozenset({"inspect", "validate", "backup", "verify"})

_INSPECT_ALLOWED = frozenset({"market", "db_path"})
_VALIDATE_ALLOWED = frozenset({"document", "expected_enabled"})
_BACKUP_ALLOWED = frozenset({"market", "db_path", "created_at", "output", "overwrite"})
_VERIFY_ALLOWED = frozenset(
    {"market", "db_path", "expected_status", "expected_policy_sha256"}
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
