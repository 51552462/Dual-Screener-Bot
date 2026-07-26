"""Fast Safety Policy Admin artifacts — JSON codecs, approval validation (Chapter 3-B0D2B3B1)."""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config_manager import sha256_utf8
from fast_safety_policy_admin import (
    FAST_SAFETY_BACKUP_VERSION,
    FAST_SAFETY_CHECKPOINT_VERSION,
    AppliedCheckpoint,
    BackupRecord,
    FastSafetyReadStatus,
    compute_policy_document_sha256,
    validate_admin_apply_document,
)
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    build_fast_safety_policy_payload,
    policy_key_for_market,
)

FAST_SAFETY_APPROVAL_MANIFEST_VERSION = "fast-safety-approval-manifest-v1"
FAST_SAFETY_APPROVAL_OPERATION = "apply-enabled"

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_ALLOWED_MARKETS = frozenset({"KR", "US"})

_BACKUP_RECORD_KEYS = frozenset(
    {
        "backup_version",
        "created_at",
        "market",
        "config_key",
        "previous_absent",
        "previous_row_version",
        "previous_value_json",
        "backup_value_json_sha256",
        "previous_classification",
    }
)

_APPLIED_CHECKPOINT_KEYS = frozenset(
    {
        "checkpoint_version",
        "created_at",
        "market",
        "config_key",
        "row_version",
        "value_json_sha256",
        "policy_document_sha256",
        "classification",
    }
)

_APPROVAL_MANIFEST_KEYS = frozenset(
    {
        "manifest_version",
        "created_at",
        "market",
        "config_key",
        "operation",
        "policy_document_sha256",
        "strategy_identities",
    }
)

_APPROVAL_IDENTITY_KEYS = frozenset({"market", "group_key", "strategy_id"})

_PRESENT_CLASSIFICATIONS = frozenset(
    {
        FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
        FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
    }
)

_KNOWN_CLASSIFICATIONS = _PRESENT_CLASSIFICATIONS | {
    FastSafetyReadStatus.ABSENT.value,
}


@dataclass(frozen=True)
class ApprovalIdentity:
    market: str
    group_key: str
    strategy_id: str


@dataclass(frozen=True)
class FastSafetyApprovalManifest:
    manifest_version: str
    created_at: str
    market: str
    config_key: str
    operation: str
    policy_document_sha256: str
    strategy_identities: tuple[ApprovalIdentity, ...]


@dataclass(frozen=True)
class ApprovalValidationResult:
    ok: bool
    reason: str


def _reject_non_standard_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


def _require_exact_keys(
    document: Mapping[str, Any],
    expected_keys: frozenset[str],
) -> None:
    actual = frozenset(document.keys())
    if actual != expected_keys:
        missing = sorted(expected_keys - actual)
        unknown = sorted(actual - expected_keys)
        if missing and unknown:
            raise ValueError(
                f"missing keys {missing} and unknown keys {unknown}"
            )
        if missing:
            raise ValueError(f"missing keys {missing}")
        raise ValueError(f"unknown keys {unknown}")


def _require_created_at(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("created_at must be str")
    if not value.strip():
        raise ValueError("created_at must be non-empty")
    return value


def _require_market(value: object, *, field: str = "market") -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be str")
    market = value.strip().upper()
    if market not in _ALLOWED_MARKETS:
        raise ValueError(f"unsupported {field}")
    return market


def _require_config_key_for_market(market: str, config_key: object) -> str:
    if not isinstance(config_key, str):
        raise TypeError("config_key must be str")
    expected = policy_key_for_market(market)
    if expected is None or config_key != expected:
        raise ValueError("config key mismatch")
    if config_key not in FAST_SAFETY_POLICY_KEYS.values():
        raise ValueError("config key not whitelisted")
    return config_key


def _require_hex64(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be str")
    if not _HEX64_RE.fullmatch(value):
        raise ValueError(f"{field} must be lowercase hex64")
    return value


def _require_positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be int")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value


def _decode_json_strict(text: str) -> Any:
    if not isinstance(text, str):
        raise TypeError("text must be str")
    return json.loads(
        text,
        parse_constant=_reject_non_standard_json_constant,
    )


def _validate_strategy_identity_item(raw: object) -> ApprovalIdentity:
    if not isinstance(raw, Mapping):
        raise TypeError("strategy identity must be a mapping")
    _require_exact_keys(raw, _APPROVAL_IDENTITY_KEYS)

    market = _require_market(raw["market"], field="market")
    if not isinstance(raw["group_key"], str):
        raise TypeError("group_key must be str")
    group_key = raw["group_key"].strip()
    if not group_key:
        raise ValueError("group_key must be non-empty")

    if not isinstance(raw["strategy_id"], str):
        raise TypeError("strategy_id must be str")
    strategy_id = raw["strategy_id"].strip()
    if not strategy_id:
        raise ValueError("strategy_id must be non-empty")

    from fast_safety_strategy_identity import build_strategy_identity

    computed = build_strategy_identity(market, group_key)
    if computed is None:
        raise ValueError("invalid strategy identity")
    if computed.strategy_id != strategy_id:
        raise ValueError("strategy_id mismatch")
    if computed.market != market:
        raise ValueError("identity market mismatch")
    if computed.group_key != group_key:
        raise ValueError("group_key mismatch")

    return ApprovalIdentity(
        market=market,
        group_key=group_key,
        strategy_id=strategy_id,
    )


def backup_record_to_document(record: BackupRecord) -> dict[str, Any]:
    return {
        "backup_version": record.backup_version,
        "created_at": record.created_at,
        "market": record.market,
        "config_key": record.config_key,
        "previous_absent": record.previous_absent,
        "previous_row_version": record.previous_row_version,
        "previous_value_json": record.previous_value_json,
        "backup_value_json_sha256": record.backup_value_json_sha256,
        "previous_classification": record.previous_classification,
    }


def backup_record_from_document(document: object) -> BackupRecord:
    if not isinstance(document, Mapping):
        raise TypeError("document must be a mapping")
    _require_exact_keys(document, _BACKUP_RECORD_KEYS)

    backup_version = document["backup_version"]
    if backup_version != FAST_SAFETY_BACKUP_VERSION:
        raise ValueError("backup version mismatch")

    created_at = _require_created_at(document["created_at"])
    market = _require_market(document["market"])
    config_key = _require_config_key_for_market(market, document["config_key"])

    previous_absent = document["previous_absent"]
    if not isinstance(previous_absent, bool):
        raise TypeError("previous_absent must be bool")

    previous_classification = document["previous_classification"]
    if not isinstance(previous_classification, str):
        raise TypeError("previous_classification must be str")
    if previous_classification not in _KNOWN_CLASSIFICATIONS:
        raise ValueError("invalid previous classification")

    previous_row_version = document["previous_row_version"]
    previous_value_json = document["previous_value_json"]
    backup_value_json_sha256 = document["backup_value_json_sha256"]

    if previous_absent:
        if previous_row_version is not None:
            raise ValueError("absent backup has row version")
        if previous_value_json is not None:
            raise ValueError("absent backup has value json")
        if backup_value_json_sha256 is not None:
            raise ValueError("absent backup has checksum")
        if previous_classification != FastSafetyReadStatus.ABSENT.value:
            raise ValueError("absent backup classification mismatch")
    else:
        row_version = _require_positive_int(
            previous_row_version,
            field="previous_row_version",
        )
        if not isinstance(previous_value_json, str):
            raise TypeError("previous_value_json must be str")
        checksum = _require_hex64(
            backup_value_json_sha256,
            field="backup_value_json_sha256",
        )
        if sha256_utf8(previous_value_json) != checksum:
            raise ValueError("backup checksum mismatch")
        if previous_classification not in _PRESENT_CLASSIFICATIONS:
            raise ValueError("invalid previous classification")

        decoded = _decode_json_strict(previous_value_json)
        if not isinstance(decoded, Mapping):
            raise ValueError("backup json invalid")

        doc_market = str(decoded.get("market", "")).strip().upper()
        if doc_market != market:
            raise ValueError("backup document market mismatch")

        payload = build_fast_safety_policy_payload(decoded)
        if payload is None:
            raise ValueError("backup json invalid")

        enabled = payload["enabled"]
        if (
            previous_classification
            == FastSafetyReadStatus.PRESENT_ENABLED_VALID.value
            and not enabled
        ) or (
            previous_classification
            == FastSafetyReadStatus.PRESENT_DISABLED_VALID.value
            and enabled
        ):
            raise ValueError("backup classification stale")

        previous_row_version = row_version
        backup_value_json_sha256 = checksum

    return BackupRecord(
        backup_version=backup_version,
        created_at=created_at,
        market=market,
        config_key=config_key,
        previous_absent=previous_absent,
        previous_row_version=previous_row_version,
        previous_value_json=previous_value_json,
        backup_value_json_sha256=backup_value_json_sha256,
        previous_classification=previous_classification,
    )


def applied_checkpoint_to_document(checkpoint: AppliedCheckpoint) -> dict[str, Any]:
    return {
        "checkpoint_version": checkpoint.checkpoint_version,
        "created_at": checkpoint.created_at,
        "market": checkpoint.market,
        "config_key": checkpoint.config_key,
        "row_version": checkpoint.row_version,
        "value_json_sha256": checkpoint.value_json_sha256,
        "policy_document_sha256": checkpoint.policy_document_sha256,
        "classification": checkpoint.classification,
    }


def applied_checkpoint_from_document(document: object) -> AppliedCheckpoint:
    if not isinstance(document, Mapping):
        raise TypeError("document must be a mapping")
    _require_exact_keys(document, _APPLIED_CHECKPOINT_KEYS)

    checkpoint_version = document["checkpoint_version"]
    if checkpoint_version != FAST_SAFETY_CHECKPOINT_VERSION:
        raise ValueError("checkpoint version mismatch")

    created_at = _require_created_at(document["created_at"])
    market = _require_market(document["market"])
    config_key = _require_config_key_for_market(market, document["config_key"])
    row_version = _require_positive_int(document["row_version"], field="row_version")
    value_json_sha256 = _require_hex64(
        document["value_json_sha256"],
        field="value_json_sha256",
    )
    policy_document_sha256 = _require_hex64(
        document["policy_document_sha256"],
        field="policy_document_sha256",
    )

    classification = document["classification"]
    if not isinstance(classification, str):
        raise TypeError("classification must be str")
    if classification not in _PRESENT_CLASSIFICATIONS:
        raise ValueError("invalid classification")

    return AppliedCheckpoint(
        checkpoint_version=checkpoint_version,
        created_at=created_at,
        market=market,
        config_key=config_key,
        row_version=row_version,
        value_json_sha256=value_json_sha256,
        policy_document_sha256=policy_document_sha256,
        classification=classification,
    )


def approval_manifest_to_document(
    manifest: FastSafetyApprovalManifest,
) -> dict[str, Any]:
    return {
        "manifest_version": manifest.manifest_version,
        "created_at": manifest.created_at,
        "market": manifest.market,
        "config_key": manifest.config_key,
        "operation": manifest.operation,
        "policy_document_sha256": manifest.policy_document_sha256,
        "strategy_identities": [
            {
                "market": identity.market,
                "group_key": identity.group_key,
                "strategy_id": identity.strategy_id,
            }
            for identity in manifest.strategy_identities
        ],
    }


def approval_manifest_from_document(document: object) -> FastSafetyApprovalManifest:
    if not isinstance(document, Mapping):
        raise TypeError("document must be a mapping")
    _require_exact_keys(document, _APPROVAL_MANIFEST_KEYS)

    manifest_version = document["manifest_version"]
    if manifest_version != FAST_SAFETY_APPROVAL_MANIFEST_VERSION:
        raise ValueError("manifest version mismatch")

    created_at = _require_created_at(document["created_at"])
    market = _require_market(document["market"])
    config_key = _require_config_key_for_market(market, document["config_key"])

    operation = document["operation"]
    if operation != FAST_SAFETY_APPROVAL_OPERATION:
        raise ValueError("operation mismatch")

    policy_document_sha256 = _require_hex64(
        document["policy_document_sha256"],
        field="policy_document_sha256",
    )

    raw_identities = document["strategy_identities"]
    if not isinstance(raw_identities, list) or not raw_identities:
        raise ValueError("strategy_identities must be a non-empty list")

    identities: list[ApprovalIdentity] = []
    seen_group_keys: set[str] = set()
    seen_strategy_ids: set[str] = set()
    for item in raw_identities:
        identity = _validate_strategy_identity_item(item)
        if identity.market != market:
            raise ValueError("identity market mismatch")
        if identity.group_key in seen_group_keys:
            raise ValueError("duplicate group_key")
        if identity.strategy_id in seen_strategy_ids:
            raise ValueError("duplicate strategy_id")
        seen_group_keys.add(identity.group_key)
        seen_strategy_ids.add(identity.strategy_id)
        identities.append(identity)

    return FastSafetyApprovalManifest(
        manifest_version=manifest_version,
        created_at=created_at,
        market=market,
        config_key=config_key,
        operation=operation,
        policy_document_sha256=policy_document_sha256,
        strategy_identities=tuple(identities),
    )


def validate_approval_manifest_for_enabled_policy(
    manifest: object,
    policy_document: object,
) -> ApprovalValidationResult:
    try:
        if not isinstance(manifest, FastSafetyApprovalManifest):
            return ApprovalValidationResult(
                ok=False,
                reason="manifest must be FastSafetyApprovalManifest",
            )

        validation = validate_admin_apply_document(
            policy_document,
            expected_enabled=True,
        )
        if not validation.ok or validation.payload is None:
            return ApprovalValidationResult(
                ok=False,
                reason=validation.reason or "invalid policy document",
            )

        payload = validation.payload
        payload_market = str(payload["market"]).strip().upper()
        if payload_market != manifest.market:
            return ApprovalValidationResult(ok=False, reason="market mismatch")

        expected_key = policy_key_for_market(manifest.market)
        if expected_key is None or manifest.config_key != expected_key:
            return ApprovalValidationResult(ok=False, reason="config key mismatch")

        if manifest.operation != FAST_SAFETY_APPROVAL_OPERATION:
            return ApprovalValidationResult(ok=False, reason="operation mismatch")

        policy_sha256 = validation.policy_document_sha256
        if policy_sha256 is None:
            return ApprovalValidationResult(
                ok=False,
                reason="missing policy checksum",
            )
        if policy_sha256 != manifest.policy_document_sha256:
            return ApprovalValidationResult(
                ok=False,
                reason="policy checksum mismatch",
            )

        from fast_safety_strategy_identity import build_strategy_identity

        seen_group_keys: set[str] = set()
        seen_strategy_ids: set[str] = set()
        manifest_strategy_ids: set[str] = set()
        for identity in manifest.strategy_identities:
            if identity.market != manifest.market:
                return ApprovalValidationResult(
                    ok=False,
                    reason="identity market mismatch",
                )
            if identity.group_key in seen_group_keys:
                return ApprovalValidationResult(
                    ok=False,
                    reason="duplicate group_key",
                )
            if identity.strategy_id in seen_strategy_ids:
                return ApprovalValidationResult(
                    ok=False,
                    reason="duplicate strategy_id",
                )
            seen_group_keys.add(identity.group_key)
            seen_strategy_ids.add(identity.strategy_id)
            manifest_strategy_ids.add(identity.strategy_id)

            computed = build_strategy_identity(identity.market, identity.group_key)
            if computed is None:
                return ApprovalValidationResult(
                    ok=False,
                    reason="invalid strategy identity",
                )
            if computed.strategy_id != identity.strategy_id:
                return ApprovalValidationResult(
                    ok=False,
                    reason="strategy_id mismatch",
                )

        base_map = payload.get("base_kelly_by_strategy")
        if not isinstance(base_map, Mapping):
            return ApprovalValidationResult(
                ok=False,
                reason="missing base_kelly_by_strategy",
            )
        policy_strategy_ids = frozenset(
            key for key in base_map.keys() if isinstance(key, str)
        )
        if policy_strategy_ids != frozenset(manifest_strategy_ids):
            return ApprovalValidationResult(
                ok=False,
                reason="strategy identity set mismatch",
            )

        alpha_map = payload.get("alpha_overlay_by_strategy")
        if not isinstance(alpha_map, Mapping):
            return ApprovalValidationResult(
                ok=False,
                reason="missing alpha_overlay_by_strategy",
            )
        alpha_strategy_ids = frozenset(
            key for key in alpha_map.keys() if isinstance(key, str)
        )
        if alpha_strategy_ids != frozenset(manifest_strategy_ids):
            return ApprovalValidationResult(
                ok=False,
                reason="alpha overlay strategy set mismatch",
            )

        return ApprovalValidationResult(ok=True, reason="ok")
    except Exception:
        return ApprovalValidationResult(ok=False, reason="validation failed")


def encode_artifact_json(document: Mapping[str, Any]) -> str:
    if not isinstance(document, Mapping):
        raise TypeError("document must be a mapping")
    return json.dumps(
        document,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def decode_artifact_json(text: str) -> Mapping[str, Any]:
    decoded = _decode_json_strict(text)
    if not isinstance(decoded, Mapping):
        raise TypeError("document must be a mapping")
    return dict(decoded)


def write_artifact_json_atomic(
    path: str | os.PathLike[str],
    document: Mapping[str, Any],
    *,
    overwrite: bool = False,
) -> None:
    target_path = Path(path)
    encoded = encode_artifact_json(document)

    parent = target_path.parent
    if not parent.exists():
        raise FileNotFoundError(f"parent directory does not exist: {parent}")
    if not parent.is_dir():
        raise NotADirectoryError(f"parent path is not a directory: {parent}")

    fd, temp_path_str = tempfile.mkstemp(
        prefix=".fast_safety_artifact_",
        suffix=".json.tmp",
        dir=str(parent),
    )
    temp_path = Path(temp_path_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

        if overwrite:
            os.replace(temp_path, target_path)
            temp_path = Path()
        else:
            if target_path.exists():
                raise FileExistsError(f"target already exists: {target_path}")
            try:
                os.link(temp_path, target_path)
            except FileExistsError:
                raise
            os.unlink(temp_path)
            temp_path = Path()
    except Exception:
        if temp_path.exists():
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        raise


def _read_text_file(path: str | os.PathLike[str]) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def save_backup_record(
    path: str | os.PathLike[str],
    record: BackupRecord,
    *,
    overwrite: bool = False,
) -> None:
    write_artifact_json_atomic(
        path,
        backup_record_to_document(record),
        overwrite=overwrite,
    )


def load_backup_record(path: str | os.PathLike[str]) -> BackupRecord:
    return backup_record_from_document(
        decode_artifact_json(_read_text_file(path))
    )


def save_applied_checkpoint(
    path: str | os.PathLike[str],
    checkpoint: AppliedCheckpoint,
    *,
    overwrite: bool = False,
) -> None:
    write_artifact_json_atomic(
        path,
        applied_checkpoint_to_document(checkpoint),
        overwrite=overwrite,
    )


def load_applied_checkpoint(path: str | os.PathLike[str]) -> AppliedCheckpoint:
    return applied_checkpoint_from_document(
        decode_artifact_json(_read_text_file(path))
    )


def save_approval_manifest(
    path: str | os.PathLike[str],
    manifest: FastSafetyApprovalManifest,
    *,
    overwrite: bool = False,
) -> None:
    write_artifact_json_atomic(
        path,
        approval_manifest_to_document(manifest),
        overwrite=overwrite,
    )


def load_approval_manifest(path: str | os.PathLike[str]) -> FastSafetyApprovalManifest:
    return approval_manifest_from_document(
        decode_artifact_json(_read_text_file(path))
    )


__all__ = [
    "FAST_SAFETY_APPROVAL_MANIFEST_VERSION",
    "FAST_SAFETY_APPROVAL_OPERATION",
    "ApprovalIdentity",
    "FastSafetyApprovalManifest",
    "ApprovalValidationResult",
    "backup_record_to_document",
    "backup_record_from_document",
    "applied_checkpoint_to_document",
    "applied_checkpoint_from_document",
    "approval_manifest_to_document",
    "approval_manifest_from_document",
    "validate_approval_manifest_for_enabled_policy",
    "encode_artifact_json",
    "decode_artifact_json",
    "write_artifact_json_atomic",
    "save_backup_record",
    "load_backup_record",
    "save_applied_checkpoint",
    "load_applied_checkpoint",
    "save_approval_manifest",
    "load_approval_manifest",
]
