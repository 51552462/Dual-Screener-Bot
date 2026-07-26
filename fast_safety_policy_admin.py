"""Fast Safety Policy Admin Core — inspect, validate, OCC apply, rollback (Chapter 3-B0D2B3A)."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

from config_manager import (
    ConfigConcurrencyError,
    ConfigKvRow,
    delete_config_kv_if_match,
    insert_config_kv_if_absent,
    read_config_kv_row,
    replace_config_kv_json_if_match,
    sha256_utf8,
    update_config_kv_if_match,
)
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    FAST_SAFETY_POLICY_VERSION,
    build_fast_safety_policy_payload,
    load_fast_safety_policy_snapshot,
    policy_key_for_market,
)

FAST_SAFETY_BACKUP_VERSION = "fast-safety-backup-v1"
FAST_SAFETY_CHECKPOINT_VERSION = "fast-safety-applied-checkpoint-v1"

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_DISABLED_PAYLOAD_KEYS = frozenset(
    {"enabled", "market", "version", "generated_at"}
)


class FastSafetyReadStatus(str, Enum):
    ABSENT = "ABSENT"
    PRESENT_ENABLED_VALID = "PRESENT_ENABLED_VALID"
    PRESENT_DISABLED_VALID = "PRESENT_DISABLED_VALID"
    PRESENT_INVALID_DOCUMENT = "PRESENT_INVALID_DOCUMENT"
    PRESENT_UNDECODABLE_JSON = "PRESENT_UNDECODABLE_JSON"
    READ_ERROR = "READ_ERROR"


APPLY_ALLOWED = frozenset(
    {
        FastSafetyReadStatus.ABSENT,
        FastSafetyReadStatus.PRESENT_ENABLED_VALID,
        FastSafetyReadStatus.PRESENT_DISABLED_VALID,
    }
)


@dataclass(frozen=True)
class InspectResult:
    market: str
    config_key: str | None
    status: FastSafetyReadStatus
    row_version: int | None
    value_json_sha256: str | None
    document: Mapping[str, Any] | None
    error: str


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    reason: str
    payload: Mapping[str, Any] | None
    policy_document_sha256: str | None


@dataclass(frozen=True)
class BackupRecord:
    backup_version: str
    created_at: str
    market: str
    config_key: str
    previous_absent: bool
    previous_row_version: int | None
    previous_value_json: str | None
    backup_value_json_sha256: str | None
    previous_classification: str


@dataclass(frozen=True)
class AppliedCheckpoint:
    checkpoint_version: str
    created_at: str
    market: str
    config_key: str
    row_version: int
    value_json_sha256: str
    policy_document_sha256: str
    classification: str


@dataclass(frozen=True)
class ApplyResult:
    ok: bool
    reason: str
    checkpoint: AppliedCheckpoint | None
    requires_rollback: bool


@dataclass(frozen=True)
class VerifyResult:
    ok: bool
    reason: str
    inspection: InspectResult
    policy_document_sha256: str | None


@dataclass(frozen=True)
class RollbackResult:
    ok: bool
    reason: str
    final_status: FastSafetyReadStatus


@dataclass(frozen=True)
class _InspectContext:
    result: InspectResult
    row: ConfigKvRow | None


def _reject_non_standard_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


def _normalize_market_text(market: object) -> str:
    try:
        return str(market).strip().upper()
    except Exception:
        return ""


def _resolve_market_and_key(
    market: object,
) -> tuple[str | None, str | None, str]:
    market_text = _normalize_market_text(market)
    config_key = policy_key_for_market(market_text)
    if config_key is None:
        return None, None, "unsupported market"
    allowed_keys = set(FAST_SAFETY_POLICY_KEYS.values())
    if config_key not in allowed_keys:
        return None, None, "unsupported config key"
    return market_text, config_key, ""


def _frozen_mapping(value: dict[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


def _decode_json_strict(value_json: str) -> tuple[Any | None, FastSafetyReadStatus | None]:
    try:
        decoded = json.loads(
            value_json,
            parse_constant=_reject_non_standard_json_constant,
        )
    except json.JSONDecodeError:
        return None, FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON
    except ValueError:
        return None, FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON
    return decoded, None


def _is_exact_disabled_payload(payload: Mapping[str, Any]) -> bool:
    return (
        set(payload.keys()) == _DISABLED_PAYLOAD_KEYS
        and payload.get("enabled") is False
        and payload.get("version") == FAST_SAFETY_POLICY_VERSION
        and isinstance(payload.get("market"), str)
        and isinstance(payload.get("generated_at"), (int, float))
        and not isinstance(payload.get("generated_at"), bool)
    )


def _classify_decoded_document(
    market: str,
    document: Mapping[str, Any],
) -> FastSafetyReadStatus:
    doc_market = _normalize_market_text(document.get("market"))
    if doc_market != market:
        return FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT

    payload = build_fast_safety_policy_payload(document)
    if payload is None:
        return FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT

    if payload["enabled"]:
        def _getter(key: str, default: Any = None) -> Any:
            return document

        snapshot = load_fast_safety_policy_snapshot(
            market,
            get_value=_getter,
        )
        if snapshot is None:
            return FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT
        return FastSafetyReadStatus.PRESENT_ENABLED_VALID

    if _is_exact_disabled_payload(payload):
        return FastSafetyReadStatus.PRESENT_DISABLED_VALID
    return FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT


def _inspect_context(
    market: object,
    *,
    db_path: str,
) -> _InspectContext:
    resolved_market, config_key, resolve_error = _resolve_market_and_key(market)
    if resolved_market is None or config_key is None:
        return _InspectContext(
            result=InspectResult(
                market=_normalize_market_text(market),
                config_key=None,
                status=FastSafetyReadStatus.READ_ERROR,
                row_version=None,
                value_json_sha256=None,
                document=None,
                error=resolve_error,
            ),
            row=None,
        )

    try:
        row = read_config_kv_row(config_key, db_path=db_path)
    except Exception as exc:
        return _InspectContext(
            result=InspectResult(
                market=resolved_market,
                config_key=config_key,
                status=FastSafetyReadStatus.READ_ERROR,
                row_version=None,
                value_json_sha256=None,
                document=None,
                error=type(exc).__name__,
            ),
            row=None,
        )

    if row is None:
        return _InspectContext(
            result=InspectResult(
                market=resolved_market,
                config_key=config_key,
                status=FastSafetyReadStatus.ABSENT,
                row_version=None,
                value_json_sha256=None,
                document=None,
                error="",
            ),
            row=None,
        )

    decoded, decode_status = _decode_json_strict(row.value_json)
    if decode_status is not None:
        return _InspectContext(
            result=InspectResult(
                market=resolved_market,
                config_key=config_key,
                status=decode_status,
                row_version=row.version,
                value_json_sha256=sha256_utf8(row.value_json),
                document=None,
                error="",
            ),
            row=row,
        )

    if not isinstance(decoded, Mapping):
        return _InspectContext(
            result=InspectResult(
                market=resolved_market,
                config_key=config_key,
                status=FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
                row_version=row.version,
                value_json_sha256=sha256_utf8(row.value_json),
                document=None,
                error="",
            ),
            row=row,
        )

    status = _classify_decoded_document(resolved_market, decoded)
    document_copy = _frozen_mapping(dict(decoded)) if status in {
        FastSafetyReadStatus.PRESENT_ENABLED_VALID,
        FastSafetyReadStatus.PRESENT_DISABLED_VALID,
    } else None

    return _InspectContext(
        result=InspectResult(
            market=resolved_market,
            config_key=config_key,
            status=status,
            row_version=row.version,
            value_json_sha256=sha256_utf8(row.value_json),
            document=document_copy,
            error="",
        ),
        row=row,
    )


def compute_policy_document_sha256(document: Mapping[str, Any]) -> str:
    if "metadata" in document:
        raise ValueError("metadata not allowed")
    payload = build_fast_safety_policy_payload(document)
    if payload is None:
        raise ValueError("invalid policy document")
    canonical = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return sha256_utf8(canonical)


def validate_admin_apply_document(
    document: object,
    *,
    expected_enabled: bool | None = None,
) -> ValidationResult:
    try:
        if not isinstance(document, Mapping):
            return ValidationResult(
                ok=False,
                reason="document must be a mapping",
                payload=None,
                policy_document_sha256=None,
            )
        if "metadata" in document:
            return ValidationResult(
                ok=False,
                reason="metadata not allowed",
                payload=None,
                policy_document_sha256=None,
            )
        payload = build_fast_safety_policy_payload(document)
        if payload is None:
            return ValidationResult(
                ok=False,
                reason="invalid policy document",
                payload=None,
                policy_document_sha256=None,
            )
        if expected_enabled is not None and payload["enabled"] != expected_enabled:
            return ValidationResult(
                ok=False,
                reason="enabled mismatch",
                payload=None,
                policy_document_sha256=None,
            )
        checksum = compute_policy_document_sha256(payload)
        return ValidationResult(
            ok=True,
            reason="",
            payload=_frozen_mapping(dict(payload)),
            policy_document_sha256=checksum,
        )
    except Exception:
        return ValidationResult(
            ok=False,
            reason="validation failed",
            payload=None,
            policy_document_sha256=None,
        )


def inspect_fast_safety_policy(
    market: object,
    *,
    db_path: str,
) -> InspectResult:
    return _inspect_context(market, db_path=db_path).result


def create_backup_record(
    market: object,
    *,
    created_at: str,
    db_path: str,
) -> BackupRecord | None:
    if not isinstance(created_at, str) or not created_at:
        return None

    context = _inspect_context(market, db_path=db_path)
    inspection = context.result
    if inspection.status not in APPLY_ALLOWED:
        return None

    resolved_market = inspection.market
    config_key = inspection.config_key
    if config_key is None:
        return None

    if inspection.status is FastSafetyReadStatus.ABSENT:
        return BackupRecord(
            backup_version=FAST_SAFETY_BACKUP_VERSION,
            created_at=created_at,
            market=resolved_market,
            config_key=config_key,
            previous_absent=True,
            previous_row_version=None,
            previous_value_json=None,
            backup_value_json_sha256=None,
            previous_classification=FastSafetyReadStatus.ABSENT.value,
        )

    row = context.row
    if row is None:
        return None

    return BackupRecord(
        backup_version=FAST_SAFETY_BACKUP_VERSION,
        created_at=created_at,
        market=resolved_market,
        config_key=config_key,
        previous_absent=False,
        previous_row_version=row.version,
        previous_value_json=row.value_json,
        backup_value_json_sha256=sha256_utf8(row.value_json),
        previous_classification=inspection.status.value,
    )


def _validate_backup_record(
    backup: BackupRecord,
    *,
    expected_market: str,
) -> tuple[bool, str]:
    if backup.backup_version != FAST_SAFETY_BACKUP_VERSION:
        return False, "backup version mismatch"
    if backup.market != expected_market:
        return False, "market mismatch"
    expected_key = policy_key_for_market(expected_market)
    if expected_key is None or backup.config_key != expected_key:
        return False, "config key mismatch"
    if backup.config_key not in FAST_SAFETY_POLICY_KEYS.values():
        return False, "config key not whitelisted"
    if not isinstance(backup.created_at, str) or not backup.created_at:
        return False, "created_at missing"

    if backup.previous_absent:
        if backup.previous_row_version is not None:
            return False, "absent backup has row version"
        if backup.previous_value_json is not None:
            return False, "absent backup has value json"
        if backup.backup_value_json_sha256 is not None:
            return False, "absent backup has checksum"
        if backup.previous_classification != FastSafetyReadStatus.ABSENT.value:
            return False, "absent backup classification mismatch"
        return True, ""

    if backup.previous_row_version is None or isinstance(
        backup.previous_row_version,
        bool,
    ):
        return False, "invalid previous row version"
    if backup.previous_row_version <= 0:
        return False, "invalid previous row version"
    if not isinstance(backup.previous_value_json, str):
        return False, "previous value json missing"
    if backup.backup_value_json_sha256 is None:
        return False, "backup checksum missing"
    if not _HEX64_RE.fullmatch(backup.backup_value_json_sha256):
        return False, "backup checksum format invalid"
    if sha256_utf8(backup.previous_value_json) != backup.backup_value_json_sha256:
        return False, "backup checksum mismatch"

    allowed_present = {
        FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
        FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
    }
    if backup.previous_classification not in allowed_present:
        return False, "invalid previous classification"

    decoded, decode_status = _decode_json_strict(backup.previous_value_json)
    if decode_status is not None or not isinstance(decoded, Mapping):
        return False, "backup json invalid"

    classification = _classify_decoded_document(expected_market, decoded)
    if classification.value != backup.previous_classification:
        return False, "backup classification stale"

    return True, ""


def verify_fast_safety_policy(
    market: object,
    *,
    expected_status: FastSafetyReadStatus | None = None,
    expected_policy_sha256: str | None = None,
    db_path: str,
) -> VerifyResult:
    try:
        inspection = inspect_fast_safety_policy(market, db_path=db_path)
        if inspection.status in {
            FastSafetyReadStatus.READ_ERROR,
            FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
            FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON,
        }:
            return VerifyResult(
                ok=False,
                reason=f"unverifiable status: {inspection.status.value}",
                inspection=inspection,
                policy_document_sha256=None,
            )

        if expected_status is not None and inspection.status != expected_status:
            return VerifyResult(
                ok=False,
                reason="status mismatch",
                inspection=inspection,
                policy_document_sha256=None,
            )

        if inspection.status is FastSafetyReadStatus.ABSENT:
            return VerifyResult(
                ok=True,
                reason="",
                inspection=inspection,
                policy_document_sha256=None,
            )

        document = inspection.document
        if document is None:
            return VerifyResult(
                ok=False,
                reason="missing document",
                inspection=inspection,
                policy_document_sha256=None,
            )

        payload = build_fast_safety_policy_payload(document)
        if payload is None:
            return VerifyResult(
                ok=False,
                reason="invalid document",
                inspection=inspection,
                policy_document_sha256=None,
            )

        policy_sha256 = compute_policy_document_sha256(payload)
        if (
            expected_policy_sha256 is not None
            and policy_sha256 != expected_policy_sha256
        ):
            return VerifyResult(
                ok=False,
                reason="policy checksum mismatch",
                inspection=inspection,
                policy_document_sha256=policy_sha256,
            )

        if inspection.status is FastSafetyReadStatus.PRESENT_ENABLED_VALID:
            def _getter(key: str, default: Any = None) -> Any:
                return document

            snapshot = load_fast_safety_policy_snapshot(
                inspection.market,
                get_value=_getter,
            )
            if snapshot is None:
                return VerifyResult(
                    ok=False,
                    reason="enabled snapshot missing",
                    inspection=inspection,
                    policy_document_sha256=policy_sha256,
                )
        elif inspection.status is FastSafetyReadStatus.PRESENT_DISABLED_VALID:
            snapshot = load_fast_safety_policy_snapshot(
                inspection.market,
                get_value=lambda key, default=None: document,
            )
            if snapshot is not None:
                return VerifyResult(
                    ok=False,
                    reason="disabled snapshot present",
                    inspection=inspection,
                    policy_document_sha256=policy_sha256,
                )

        return VerifyResult(
            ok=True,
            reason="",
            inspection=inspection,
            policy_document_sha256=policy_sha256,
        )
    except Exception:
        inspection = inspect_fast_safety_policy(market, db_path=db_path)
        return VerifyResult(
            ok=False,
            reason="verify failed",
            inspection=inspection,
            policy_document_sha256=None,
        )


def _build_applied_checkpoint(
    *,
    created_at: str,
    market: str,
    row: ConfigKvRow,
    policy_document_sha256: str,
    classification: FastSafetyReadStatus,
) -> AppliedCheckpoint:
    if classification not in {
        FastSafetyReadStatus.PRESENT_ENABLED_VALID,
        FastSafetyReadStatus.PRESENT_DISABLED_VALID,
    }:
        raise ValueError("invalid checkpoint classification")
    return AppliedCheckpoint(
        checkpoint_version=FAST_SAFETY_CHECKPOINT_VERSION,
        created_at=created_at,
        market=market,
        config_key=row.key,
        row_version=row.version,
        value_json_sha256=sha256_utf8(row.value_json),
        policy_document_sha256=policy_document_sha256,
        classification=classification.value,
    )


def apply_disabled_policy(
    market: object,
    generated_at: object,
    backup: BackupRecord,
    *,
    checkpoint_created_at: str,
    db_path: str,
) -> ApplyResult:
    resolved_market, config_key, resolve_error = _resolve_market_and_key(market)
    if resolved_market is None or config_key is None:
        return ApplyResult(
            ok=False,
            reason=resolve_error,
            checkpoint=None,
            requires_rollback=False,
        )

    backup_ok, backup_reason = _validate_backup_record(
        backup,
        expected_market=resolved_market,
    )
    if not backup_ok:
        return ApplyResult(
            ok=False,
            reason=backup_reason,
            checkpoint=None,
            requires_rollback=False,
        )

    disabled_doc = {
        "enabled": False,
        "market": resolved_market,
        "version": FAST_SAFETY_POLICY_VERSION,
        "generated_at": generated_at,
    }
    validation = validate_admin_apply_document(
        disabled_doc,
        expected_enabled=False,
    )
    if not validation.ok or validation.payload is None:
        return ApplyResult(
            ok=False,
            reason=validation.reason or "invalid disabled document",
            checkpoint=None,
            requires_rollback=False,
        )

    payload = dict(validation.payload)
    policy_sha256 = validation.policy_document_sha256
    if policy_sha256 is None:
        return ApplyResult(
            ok=False,
            reason="missing policy checksum",
            checkpoint=None,
            requires_rollback=False,
        )

    try:
        if backup.previous_absent:
            row = insert_config_kv_if_absent(
                config_key,
                payload,
                db_path=db_path,
            )
        else:
            row = update_config_kv_if_match(
                config_key,
                expected_version=backup.previous_row_version,
                expected_value_json_sha256=backup.backup_value_json_sha256,
                new_value=payload,
                db_path=db_path,
            )
    except ConfigConcurrencyError:
        return ApplyResult(
            ok=False,
            reason="concurrency conflict",
            checkpoint=None,
            requires_rollback=False,
        )
    except Exception:
        return ApplyResult(
            ok=False,
            reason="write failed",
            checkpoint=None,
            requires_rollback=False,
        )

    checkpoint = _build_applied_checkpoint(
        created_at=checkpoint_created_at,
        market=resolved_market,
        row=row,
        policy_document_sha256=policy_sha256,
        classification=FastSafetyReadStatus.PRESENT_DISABLED_VALID,
    )

    verify = verify_fast_safety_policy(
        resolved_market,
        expected_status=FastSafetyReadStatus.PRESENT_DISABLED_VALID,
        expected_policy_sha256=policy_sha256,
        db_path=db_path,
    )
    if verify.ok:
        return ApplyResult(
            ok=True,
            reason="",
            checkpoint=checkpoint,
            requires_rollback=False,
        )
    return ApplyResult(
        ok=False,
        reason=verify.reason,
        checkpoint=checkpoint,
        requires_rollback=True,
    )


def apply_enabled_policy(
    document: object,
    backup: BackupRecord,
    *,
    checkpoint_created_at: str,
    db_path: str,
) -> ApplyResult:
    validation = validate_admin_apply_document(
        document,
        expected_enabled=True,
    )
    if not validation.ok or validation.payload is None:
        return ApplyResult(
            ok=False,
            reason=validation.reason or "invalid enabled document",
            checkpoint=None,
            requires_rollback=False,
        )

    payload = dict(validation.payload)
    market = payload["market"]
    resolved_market, config_key, resolve_error = _resolve_market_and_key(market)
    if resolved_market is None or config_key is None:
        return ApplyResult(
            ok=False,
            reason=resolve_error,
            checkpoint=None,
            requires_rollback=False,
        )

    if backup.market != resolved_market or backup.config_key != config_key:
        return ApplyResult(
            ok=False,
            reason="backup market/key mismatch",
            checkpoint=None,
            requires_rollback=False,
        )

    backup_ok, backup_reason = _validate_backup_record(
        backup,
        expected_market=resolved_market,
    )
    if not backup_ok:
        return ApplyResult(
            ok=False,
            reason=backup_reason,
            checkpoint=None,
            requires_rollback=False,
        )

    policy_sha256 = validation.policy_document_sha256
    if policy_sha256 is None:
        return ApplyResult(
            ok=False,
            reason="missing policy checksum",
            checkpoint=None,
            requires_rollback=False,
        )

    try:
        if backup.previous_absent:
            row = insert_config_kv_if_absent(
                config_key,
                payload,
                db_path=db_path,
            )
        else:
            row = update_config_kv_if_match(
                config_key,
                expected_version=backup.previous_row_version,
                expected_value_json_sha256=backup.backup_value_json_sha256,
                new_value=payload,
                db_path=db_path,
            )
    except ConfigConcurrencyError:
        return ApplyResult(
            ok=False,
            reason="concurrency conflict",
            checkpoint=None,
            requires_rollback=False,
        )
    except Exception:
        return ApplyResult(
            ok=False,
            reason="write failed",
            checkpoint=None,
            requires_rollback=False,
        )

    checkpoint = _build_applied_checkpoint(
        created_at=checkpoint_created_at,
        market=resolved_market,
        row=row,
        policy_document_sha256=policy_sha256,
        classification=FastSafetyReadStatus.PRESENT_ENABLED_VALID,
    )

    verify = verify_fast_safety_policy(
        resolved_market,
        expected_status=FastSafetyReadStatus.PRESENT_ENABLED_VALID,
        expected_policy_sha256=policy_sha256,
        db_path=db_path,
    )
    if verify.ok:
        return ApplyResult(
            ok=True,
            reason="",
            checkpoint=checkpoint,
            requires_rollback=False,
        )
    return ApplyResult(
        ok=False,
        reason=verify.reason,
        checkpoint=checkpoint,
        requires_rollback=True,
    )


def rollback_policy_value(
    backup: BackupRecord,
    applied_checkpoint: AppliedCheckpoint,
    *,
    db_path: str,
) -> RollbackResult:
    if backup.previous_absent:
        return RollbackResult(
            ok=False,
            reason="backup was absent",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    backup_ok, backup_reason = _validate_backup_record(
        backup,
        expected_market=backup.market,
    )
    if not backup_ok:
        return RollbackResult(
            ok=False,
            reason=backup_reason,
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if applied_checkpoint.checkpoint_version != FAST_SAFETY_CHECKPOINT_VERSION:
        return RollbackResult(
            ok=False,
            reason="checkpoint version mismatch",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if (
        backup.market != applied_checkpoint.market
        or backup.config_key != applied_checkpoint.config_key
    ):
        return RollbackResult(
            ok=False,
            reason="checkpoint market/key mismatch",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if backup.config_key not in FAST_SAFETY_POLICY_KEYS.values():
        return RollbackResult(
            ok=False,
            reason="config key not whitelisted",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if backup.previous_value_json is None:
        return RollbackResult(
            ok=False,
            reason="missing backup value json",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    try:
        current_row = read_config_kv_row(backup.config_key, db_path=db_path)
    except Exception:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="read failed",
            final_status=inspection.status,
        )

    if current_row is None:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="current row missing",
            final_status=inspection.status,
        )

    if (
        current_row.version != applied_checkpoint.row_version
        or sha256_utf8(current_row.value_json)
        != applied_checkpoint.value_json_sha256
    ):
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="checkpoint mismatch",
            final_status=inspection.status,
        )

    try:
        replace_config_kv_json_if_match(
            backup.config_key,
            expected_version=applied_checkpoint.row_version,
            expected_value_json_sha256=applied_checkpoint.value_json_sha256,
            replacement_value_json=backup.previous_value_json,
            db_path=db_path,
        )
    except ConfigConcurrencyError:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="concurrency conflict",
            final_status=inspection.status,
        )
    except Exception:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="rollback failed",
            final_status=inspection.status,
        )

    inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
    if inspection.value_json_sha256 != backup.backup_value_json_sha256:
        return RollbackResult(
            ok=False,
            reason="restored hash mismatch",
            final_status=inspection.status,
        )
    if inspection.status.value != backup.previous_classification:
        return RollbackResult(
            ok=False,
            reason="restored classification mismatch",
            final_status=inspection.status,
        )

    return RollbackResult(
        ok=True,
        reason="",
        final_status=inspection.status,
    )


def rollback_policy_absent(
    backup: BackupRecord,
    applied_checkpoint: AppliedCheckpoint,
    *,
    db_path: str,
) -> RollbackResult:
    if not backup.previous_absent:
        return RollbackResult(
            ok=False,
            reason="backup was not absent",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    backup_ok, backup_reason = _validate_backup_record(
        backup,
        expected_market=backup.market,
    )
    if not backup_ok:
        return RollbackResult(
            ok=False,
            reason=backup_reason,
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if backup.previous_classification != FastSafetyReadStatus.ABSENT.value:
        return RollbackResult(
            ok=False,
            reason="backup classification mismatch",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if applied_checkpoint.checkpoint_version != FAST_SAFETY_CHECKPOINT_VERSION:
        return RollbackResult(
            ok=False,
            reason="checkpoint version mismatch",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if (
        backup.market != applied_checkpoint.market
        or backup.config_key != applied_checkpoint.config_key
    ):
        return RollbackResult(
            ok=False,
            reason="checkpoint market/key mismatch",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    if backup.config_key not in FAST_SAFETY_POLICY_KEYS.values():
        return RollbackResult(
            ok=False,
            reason="config key not whitelisted",
            final_status=FastSafetyReadStatus.READ_ERROR,
        )

    try:
        current_row = read_config_kv_row(backup.config_key, db_path=db_path)
    except Exception:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="read failed",
            final_status=inspection.status,
        )

    if current_row is None:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="current row missing",
            final_status=inspection.status,
        )

    if (
        current_row.version != applied_checkpoint.row_version
        or sha256_utf8(current_row.value_json)
        != applied_checkpoint.value_json_sha256
    ):
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="checkpoint mismatch",
            final_status=inspection.status,
        )

    try:
        delete_config_kv_if_match(
            backup.config_key,
            expected_version=applied_checkpoint.row_version,
            expected_value_json_sha256=applied_checkpoint.value_json_sha256,
            db_path=db_path,
        )
    except ConfigConcurrencyError:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="concurrency conflict",
            final_status=inspection.status,
        )
    except Exception:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="rollback failed",
            final_status=inspection.status,
        )

    if read_config_kv_row(backup.config_key, db_path=db_path) is not None:
        inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
        return RollbackResult(
            ok=False,
            reason="row still present",
            final_status=inspection.status,
        )

    inspection = inspect_fast_safety_policy(backup.market, db_path=db_path)
    if inspection.status is not FastSafetyReadStatus.ABSENT:
        return RollbackResult(
            ok=False,
            reason="not absent after rollback",
            final_status=inspection.status,
        )

    return RollbackResult(
        ok=True,
        reason="",
        final_status=FastSafetyReadStatus.ABSENT,
    )


__all__ = [
    "APPLY_ALLOWED",
    "FAST_SAFETY_BACKUP_VERSION",
    "FAST_SAFETY_CHECKPOINT_VERSION",
    "AppliedCheckpoint",
    "ApplyResult",
    "BackupRecord",
    "FastSafetyReadStatus",
    "InspectResult",
    "RollbackResult",
    "ValidationResult",
    "VerifyResult",
    "apply_disabled_policy",
    "apply_enabled_policy",
    "compute_policy_document_sha256",
    "create_backup_record",
    "inspect_fast_safety_policy",
    "rollback_policy_absent",
    "rollback_policy_value",
    "validate_admin_apply_document",
    "verify_fast_safety_policy",
]
