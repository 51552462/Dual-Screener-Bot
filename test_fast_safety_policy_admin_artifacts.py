from __future__ import annotations

import json
import math
import os
import tempfile
import unittest
from pathlib import Path

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
from fast_safety_policy_admin_artifacts import (
    FAST_SAFETY_APPROVAL_MANIFEST_VERSION,
    FAST_SAFETY_APPROVAL_OPERATION,
    ApprovalIdentity,
    FastSafetyApprovalManifest,
    applied_checkpoint_from_document,
    applied_checkpoint_to_document,
    approval_manifest_from_document,
    approval_manifest_to_document,
    backup_record_from_document,
    backup_record_to_document,
    decode_artifact_json,
    encode_artifact_json,
    load_applied_checkpoint,
    load_approval_manifest,
    load_backup_record,
    save_applied_checkpoint,
    save_approval_manifest,
    save_backup_record,
    validate_approval_manifest_for_enabled_policy,
    write_artifact_json_atomic,
)
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    FAST_SAFETY_POLICY_VERSION,
)
from fast_safety_strategy_identity import build_strategy_identity

_CREATED_AT = "2026-07-26T12:00:00Z"
_CHECKPOINT_AT = "2026-07-26T12:00:01Z"
_GROUP_KEY = "MFE_진화형_황금타점"


def _identity(*, market: str = "KR") -> ApprovalIdentity:
    built = build_strategy_identity(market, _GROUP_KEY)
    assert built is not None
    return ApprovalIdentity(
        market=built.market,
        group_key=built.group_key,
        strategy_id=built.strategy_id,
    )


def _valid_enabled_document(*, market: str = "KR") -> dict:
    identity = _identity(market=market)
    return {
        "enabled": True,
        "market": market,
        "version": FAST_SAFETY_POLICY_VERSION,
        "generated_at": 100.0,
        "base_kelly_by_strategy": {identity.strategy_id: 0.08},
        "absolute_kelly_cap": 0.10,
    }


def _valid_manifest_for_document(
    document: dict,
    *,
    market: str = "KR",
) -> FastSafetyApprovalManifest:
    validation = validate_admin_apply_document(document, expected_enabled=True)
    assert validation.ok
    assert validation.policy_document_sha256 is not None
    identity = _identity(market=market)
    return FastSafetyApprovalManifest(
        manifest_version=FAST_SAFETY_APPROVAL_MANIFEST_VERSION,
        created_at=_CREATED_AT,
        market=market,
        config_key=FAST_SAFETY_POLICY_KEYS[market],
        operation=FAST_SAFETY_APPROVAL_OPERATION,
        policy_document_sha256=validation.policy_document_sha256,
        strategy_identities=(identity,),
    )


class FastSafetyPolicyAdminArtifactsTests(unittest.TestCase):
    def test_01_absent_backup_record_round_trip(self) -> None:
        record = BackupRecord(
            backup_version=FAST_SAFETY_BACKUP_VERSION,
            created_at=_CREATED_AT,
            market="KR",
            config_key=FAST_SAFETY_POLICY_KEYS["KR"],
            previous_absent=True,
            previous_row_version=None,
            previous_value_json=None,
            backup_value_json_sha256=None,
            previous_classification=FastSafetyReadStatus.ABSENT.value,
        )
        restored = backup_record_from_document(backup_record_to_document(record))
        self.assertEqual(restored, record)

    def test_02_present_backup_record_round_trip(self) -> None:
        raw_json = (
            ' {"enabled": true, "market": "KR", '
            f'"version": "{FAST_SAFETY_POLICY_VERSION}", '
            f'"generated_at": 100.0, '
            f'"base_kelly_by_strategy": {{"{_identity().strategy_id}": 0.08}}, '
            '"absolute_kelly_cap": 0.10} '
        )
        checksum = sha256_utf8(raw_json)
        record = BackupRecord(
            backup_version=FAST_SAFETY_BACKUP_VERSION,
            created_at=_CREATED_AT,
            market="KR",
            config_key=FAST_SAFETY_POLICY_KEYS["KR"],
            previous_absent=False,
            previous_row_version=3,
            previous_value_json=raw_json,
            backup_value_json_sha256=checksum,
            previous_classification=FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
        )
        document = backup_record_to_document(record)
        restored = backup_record_from_document(document)
        self.assertEqual(restored, record)
        self.assertEqual(restored.previous_value_json, raw_json)
        self.assertEqual(restored.backup_value_json_sha256, checksum)

    def test_03_backup_record_exact_schema_rejection(self) -> None:
        base = backup_record_to_document(
            BackupRecord(
                backup_version=FAST_SAFETY_BACKUP_VERSION,
                created_at=_CREATED_AT,
                market="KR",
                config_key=FAST_SAFETY_POLICY_KEYS["KR"],
                previous_absent=True,
                previous_row_version=None,
                previous_value_json=None,
                backup_value_json_sha256=None,
                previous_classification=FastSafetyReadStatus.ABSENT.value,
            )
        )
        missing = dict(base)
        del missing["created_at"]
        with self.assertRaises(ValueError):
            backup_record_from_document(missing)

        unknown = dict(base, extra=True)
        with self.assertRaises(ValueError):
            backup_record_from_document(unknown)

        wrong_type = dict(base, previous_absent="yes")
        with self.assertRaises(TypeError):
            backup_record_from_document(wrong_type)

        bool_version = dict(base)
        bool_version["previous_absent"] = False
        bool_version["previous_row_version"] = True
        bool_version["previous_value_json"] = "{}"
        bool_version["backup_value_json_sha256"] = "0" * 64
        bool_version["previous_classification"] = (
            FastSafetyReadStatus.PRESENT_ENABLED_VALID.value
        )
        with self.assertRaises(TypeError):
            backup_record_from_document(bool_version)

    def test_04_backup_record_checksum_and_classification_rejection(self) -> None:
        raw_json = json.dumps(_valid_enabled_document())
        record_doc = backup_record_to_document(
            BackupRecord(
                backup_version=FAST_SAFETY_BACKUP_VERSION,
                created_at=_CREATED_AT,
                market="KR",
                config_key=FAST_SAFETY_POLICY_KEYS["KR"],
                previous_absent=False,
                previous_row_version=1,
                previous_value_json=raw_json,
                backup_value_json_sha256="0" * 64,
                previous_classification=FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
            )
        )
        with self.assertRaises(ValueError):
            backup_record_from_document(record_doc)

        bad_classification = dict(record_doc)
        bad_classification["backup_value_json_sha256"] = sha256_utf8(raw_json)
        bad_classification["previous_classification"] = "PRESENT_INVALID"
        with self.assertRaises(ValueError):
            backup_record_from_document(bad_classification)

    def test_05_applied_checkpoint_round_trip(self) -> None:
        document = _valid_enabled_document()
        validation = validate_admin_apply_document(document, expected_enabled=True)
        assert validation.policy_document_sha256 is not None
        checkpoint = AppliedCheckpoint(
            checkpoint_version=FAST_SAFETY_CHECKPOINT_VERSION,
            created_at=_CHECKPOINT_AT,
            market="KR",
            config_key=FAST_SAFETY_POLICY_KEYS["KR"],
            row_version=2,
            value_json_sha256=sha256_utf8(json.dumps(document, sort_keys=True)),
            policy_document_sha256=validation.policy_document_sha256,
            classification=FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
        )
        restored = applied_checkpoint_from_document(
            applied_checkpoint_to_document(checkpoint)
        )
        self.assertEqual(restored, checkpoint)

    def test_06_applied_checkpoint_schema_version_hash_type_rejection(self) -> None:
        document = _valid_enabled_document()
        validation = validate_admin_apply_document(document, expected_enabled=True)
        assert validation.policy_document_sha256 is not None
        checkpoint = AppliedCheckpoint(
            checkpoint_version=FAST_SAFETY_CHECKPOINT_VERSION,
            created_at=_CHECKPOINT_AT,
            market="KR",
            config_key=FAST_SAFETY_POLICY_KEYS["KR"],
            row_version=2,
            value_json_sha256=sha256_utf8(json.dumps(document, sort_keys=True)),
            policy_document_sha256=validation.policy_document_sha256,
            classification=FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
        )
        base = applied_checkpoint_to_document(checkpoint)

        bad_version = dict(base, checkpoint_version="bad")
        with self.assertRaises(ValueError):
            applied_checkpoint_from_document(bad_version)

        bad_hash = dict(base, value_json_sha256="ZZ" * 32)
        with self.assertRaises(ValueError):
            applied_checkpoint_from_document(bad_hash)

        bad_type = dict(base, row_version=True)
        with self.assertRaises(TypeError):
            applied_checkpoint_from_document(bad_type)

        missing = dict(base)
        del missing["classification"]
        with self.assertRaises(ValueError):
            applied_checkpoint_from_document(missing)

    def test_07_approval_manifest_round_trip(self) -> None:
        manifest = _valid_manifest_for_document(_valid_enabled_document())
        restored = approval_manifest_from_document(
            approval_manifest_to_document(manifest)
        )
        self.assertEqual(restored, manifest)

    def test_08_approval_manifest_schema_version_operation_type_rejection(self) -> None:
        manifest = _valid_manifest_for_document(_valid_enabled_document())
        base = approval_manifest_to_document(manifest)

        bad_version = dict(base, manifest_version="bad")
        with self.assertRaises(ValueError):
            approval_manifest_from_document(bad_version)

        bad_operation = dict(base, operation="apply-disabled")
        with self.assertRaises(ValueError):
            approval_manifest_from_document(bad_operation)

        bad_type = dict(base)
        bad_type["strategy_identities"] = "not-a-list"
        with self.assertRaises(ValueError):
            approval_manifest_from_document(bad_type)

        unknown = dict(base, ticket="OPS-1")
        with self.assertRaises(ValueError):
            approval_manifest_from_document(unknown)

    def test_09_strict_json_decode(self) -> None:
        with self.assertRaises(json.JSONDecodeError):
            decode_artifact_json("{not-json")

        with self.assertRaises(TypeError):
            decode_artifact_json('["list"]')

        with self.assertRaises(TypeError):
            decode_artifact_json(123)  # type: ignore[arg-type]

    def test_10_nan_infinity_rejection(self) -> None:
        for raw in (
            '{"x": NaN}',
            '{"x": Infinity}',
            '{"x": -Infinity}',
        ):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    decode_artifact_json(raw)

        with self.assertRaises(ValueError):
            encode_artifact_json({"x": math.nan})
        with self.assertRaises(ValueError):
            encode_artifact_json({"x": math.inf})

    def test_11_canonical_json_determinism(self) -> None:
        first = {"b": 2, "a": 1, "nested": {"z": "한글", "y": 2}}
        second = {"nested": {"y": 2, "z": "한글"}, "a": 1, "b": 2}
        encoded_first = encode_artifact_json(first)
        encoded_second = encode_artifact_json(second)
        self.assertEqual(encoded_first, encoded_second)
        self.assertNotIn(" ", encoded_first)
        self.assertIn("한글", encoded_first)

    def test_12_atomic_write_success_and_load_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "backup.json"
            record = BackupRecord(
                backup_version=FAST_SAFETY_BACKUP_VERSION,
                created_at=_CREATED_AT,
                market="KR",
                config_key=FAST_SAFETY_POLICY_KEYS["KR"],
                previous_absent=True,
                previous_row_version=None,
                previous_value_json=None,
                backup_value_json_sha256=None,
                previous_classification=FastSafetyReadStatus.ABSENT.value,
            )
            save_backup_record(path, record)
            loaded = load_backup_record(path)
            self.assertEqual(loaded, record)

    def test_13_overwrite_false_protection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "artifact.json"
            first = {"a": 1}
            second = {"a": 2}
            write_artifact_json_atomic(path, first, overwrite=False)
            original = path.read_text(encoding="utf-8")
            with self.assertRaises(FileExistsError):
                write_artifact_json_atomic(path, second, overwrite=False)
            self.assertEqual(path.read_text(encoding="utf-8"), original)
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(tmp_dir)))

    def test_14_overwrite_true_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "artifact.json"
            write_artifact_json_atomic(path, {"a": 1}, overwrite=False)
            write_artifact_json_atomic(path, {"b": 2}, overwrite=True)
            self.assertEqual(
                decode_artifact_json(path.read_text(encoding="utf-8")),
                {"b": 2},
            )
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(tmp_dir)))

    def test_15_enabled_policy_approval_manifest_validation_success(self) -> None:
        document = _valid_enabled_document()
        manifest = _valid_manifest_for_document(document)
        result = validate_approval_manifest_for_enabled_policy(manifest, document)
        self.assertTrue(result.ok)
        self.assertEqual(result.reason, "ok")
        expected_checksum = compute_policy_document_sha256(document)
        self.assertEqual(manifest.policy_document_sha256, expected_checksum)

    def test_16_approval_manifest_cross_validation_failures(self) -> None:
        document = _valid_enabled_document()
        manifest = _valid_manifest_for_document(document)
        other = build_strategy_identity("KR", "OTHER_GROUP")
        assert other is not None

        cases = {
            "wrong_checksum": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256="0" * 64,
                    strategy_identities=manifest.strategy_identities,
                ),
                document,
            ),
            "wrong_market": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market="US",
                    config_key=FAST_SAFETY_POLICY_KEYS["US"],
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=manifest.strategy_identities,
                ),
                document,
            ),
            "wrong_config_key": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=FAST_SAFETY_POLICY_KEYS["US"],
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=manifest.strategy_identities,
                ),
                document,
            ),
            "wrong_identity": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=(
                        ApprovalIdentity(
                            market="KR",
                            group_key=_GROUP_KEY,
                            strategy_id="strat:deadbeefdeadbeef",
                        ),
                    ),
                ),
                document,
            ),
            "duplicate_identity": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=(
                        manifest.strategy_identities[0],
                        manifest.strategy_identities[0],
                    ),
                ),
                document,
            ),
            "missing_strategy_identity": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=(),
                ),
                document,
            ),
            "extra_strategy_identity": (
                FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=(
                        manifest.strategy_identities[0],
                        ApprovalIdentity(
                            market="KR",
                            group_key="OTHER_GROUP",
                            strategy_id=other.strategy_id,
                        ),
                    ),
                ),
                document,
            ),
            "metadata_policy": (
                manifest,
                {**document, "metadata": {"owner": "ops"}},
            ),
            "disabled_policy": (
                manifest,
                {
                    "enabled": False,
                    "market": "KR",
                    "version": FAST_SAFETY_POLICY_VERSION,
                    "generated_at": 100.0,
                },
            ),
        }

        for name, (case_manifest, case_document) in cases.items():
            with self.subTest(name=name):
                result = validate_approval_manifest_for_enabled_policy(
                    case_manifest,
                    case_document,
                )
                self.assertFalse(result.ok, msg=name)


if __name__ == "__main__":
    unittest.main()
