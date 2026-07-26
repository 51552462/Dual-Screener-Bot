from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from typing import Any
from unittest.mock import patch

import config_manager
from config_manager import (
    ConfigConcurrencyError,
    insert_config_kv_if_absent,
    read_config_kv_row,
    sha256_utf8,
    update_config_kv_if_match,
)
from fast_safety_policy_admin import (
    FAST_SAFETY_BACKUP_VERSION,
    FAST_SAFETY_CHECKPOINT_VERSION,
    AppliedCheckpoint,
    BackupRecord,
    FastSafetyReadStatus,
    apply_disabled_policy,
    apply_enabled_policy,
    compute_policy_document_sha256,
    create_backup_record,
    inspect_fast_safety_policy,
    rollback_policy_absent,
    rollback_policy_value,
    validate_admin_apply_document,
    verify_fast_safety_policy,
)
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    FAST_SAFETY_POLICY_VERSION,
)


_STRATEGY_ID = "strat:abc123456789abcd"
_CREATED_AT = "2026-07-26T12:00:00Z"
_CHECKPOINT_AT = "2026-07-26T12:00:01Z"


def _db_path(tmp_dir: str, name: str = "config.sqlite") -> str:
    return os.path.join(tmp_dir, name)


def _guard_production_db_connect(
    original_connect: type[sqlite3.Connection],
) -> type[sqlite3.Connection]:
    production = os.path.normpath(config_manager.CONFIG_DB_PATH)

    def guarded(database: str, *args, **kwargs):
        if os.path.normpath(str(database)) == production:
            raise AssertionError("must not open production CONFIG_DB_PATH")
        return original_connect(database, *args, **kwargs)

    return guarded  # type: ignore[return-value]


def _valid_enabled_document(
    *,
    market: str = "KR",
    generated_at: float = 100.0,
    base: dict[str, float] | None = None,
    cap: float = 0.10,
) -> dict[str, Any]:
    return {
        "enabled": True,
        "market": market,
        "version": FAST_SAFETY_POLICY_VERSION,
        "generated_at": generated_at,
        "base_kelly_by_strategy": base or {_STRATEGY_ID: 0.08},
        "absolute_kelly_cap": cap,
    }


def _valid_disabled_document(
    *,
    market: str = "KR",
    generated_at: float = 50.0,
) -> dict[str, Any]:
    return {
        "enabled": False,
        "market": market,
        "version": FAST_SAFETY_POLICY_VERSION,
        "generated_at": generated_at,
    }


def _insert_raw_json(
    db: str,
    key: str,
    value_json: str,
    *,
    version: int = 1,
) -> None:
    conn = sqlite3.connect(db, timeout=30.0)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS config_kv (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                version INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO config_kv (key, value_json, version) VALUES (?, ?, ?)",
            (key, value_json, version),
        )
        conn.commit()
    finally:
        conn.close()


class FastSafetyPolicyAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        self._sqlite_connect_patcher = patch(
            "sqlite3.connect",
            side_effect=_guard_production_db_connect(sqlite3.connect),
        )
        self._sqlite_connect_patcher.start()

    def tearDown(self) -> None:
        self._sqlite_connect_patcher.stop()

    def test_01_inspect_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            result = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(result.status, FastSafetyReadStatus.ABSENT)
            self.assertEqual(result.market, "KR")
            self.assertEqual(result.config_key, FAST_SAFETY_POLICY_KEYS["KR"])
            self.assertIsNone(result.row_version)
            self.assertIsNone(result.document)

    def test_02_inspect_present_enabled_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            doc = _valid_enabled_document(market="KR")
            insert_config_kv_if_absent(key, doc, db_path=db)
            result = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(result.status, FastSafetyReadStatus.PRESENT_ENABLED_VALID)
            self.assertEqual(result.row_version, 1)
            self.assertIsNotNone(result.document)
            assert result.document is not None
            self.assertTrue(result.document["enabled"])

    def test_03_inspect_present_disabled_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["US"]
            doc = _valid_disabled_document(market="US")
            insert_config_kv_if_absent(key, doc, db_path=db)
            result = inspect_fast_safety_policy(" us ", db_path=db)
            self.assertEqual(result.status, FastSafetyReadStatus.PRESENT_DISABLED_VALID)
            self.assertEqual(result.market, "US")
            self.assertIsNotNone(result.document)

    def test_04_inspect_present_invalid_document(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]

            mismatch = _valid_enabled_document(market="US")
            insert_config_kv_if_absent(key, mismatch, db_path=db)
            result = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(
                result.status,
                FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
            )

            insert_config_kv_if_absent(
                FAST_SAFETY_POLICY_KEYS["US"],
                _valid_enabled_document(market="US"),
                db_path=db,
            )
            bad_key_doc = _valid_enabled_document(market="US")
            bad_key_doc["unexpected"] = True
            update_config_kv_if_match(
                FAST_SAFETY_POLICY_KEYS["US"],
                expected_version=1,
                expected_value_json_sha256=sha256_utf8(
                    read_config_kv_row(
                        FAST_SAFETY_POLICY_KEYS["US"],
                        db_path=db,
                    ).value_json
                ),
                new_value=bad_key_doc,
                db_path=db,
            )
            result_unknown = inspect_fast_safety_policy("US", db_path=db)
            self.assertEqual(
                result_unknown.status,
                FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
            )

            for raw in ("[]", '"text"', "123"):
                with self.subTest(raw=raw):
                    _insert_raw_json(db, key, raw, version=99)
                    non_mapping = inspect_fast_safety_policy("KR", db_path=db)
                    self.assertEqual(
                        non_mapping.status,
                        FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT,
                    )

    def test_05_inspect_present_undecodable_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            cases = (
                "{not-json",
                '{"x": NaN}',
                '{"x": Infinity}',
                '{"x": -Infinity}',
            )
            for idx, raw in enumerate(cases, start=1):
                with self.subTest(raw=raw):
                    _insert_raw_json(db, key, raw, version=idx)
                    result = inspect_fast_safety_policy("KR", db_path=db)
                    self.assertEqual(
                        result.status,
                        FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON,
                    )

    def test_06_inspect_read_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            with patch(
                "fast_safety_policy_admin.read_config_kv_row",
                side_effect=RuntimeError("db down"),
            ):
                result = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(result.status, FastSafetyReadStatus.READ_ERROR)
            self.assertEqual(result.error, "RuntimeError")

    def test_07_validate_admin_apply_document(self) -> None:
        enabled = _valid_enabled_document(market="KR")
        enabled_result = validate_admin_apply_document(
            enabled,
            expected_enabled=True,
        )
        self.assertTrue(enabled_result.ok)
        self.assertIsNotNone(enabled_result.payload)
        self.assertIsNotNone(enabled_result.policy_document_sha256)

        disabled = _valid_disabled_document(market="KR")
        disabled_result = validate_admin_apply_document(
            disabled,
            expected_enabled=False,
        )
        self.assertTrue(disabled_result.ok)

        with_metadata = _valid_enabled_document(market="KR")
        with_metadata["metadata"] = {"owner": "ops"}
        metadata_result = validate_admin_apply_document(with_metadata)
        self.assertFalse(metadata_result.ok)
        self.assertEqual(metadata_result.reason, "metadata not allowed")

        non_mapping = validate_admin_apply_document(["enabled"])
        self.assertFalse(non_mapping.ok)
        self.assertEqual(non_mapping.reason, "document must be a mapping")

    def test_08_compute_policy_document_sha256(self) -> None:
        doc_a = {
            "enabled": True,
            "market": "KR",
            "version": FAST_SAFETY_POLICY_VERSION,
            "generated_at": 100.0,
            "absolute_kelly_cap": "0.10",
            "base_kelly_by_strategy": {_STRATEGY_ID: "0.08"},
        }
        doc_b = {
            "generated_at": 100.0,
            "market": "KR",
            "enabled": True,
            "base_kelly_by_strategy": {_STRATEGY_ID: 0.08},
            "absolute_kelly_cap": 0.10,
            "version": FAST_SAFETY_POLICY_VERSION,
        }
        checksum_a = compute_policy_document_sha256(doc_a)
        checksum_b = compute_policy_document_sha256(doc_b)
        self.assertEqual(checksum_a, checksum_b)
        self.assertRegex(checksum_a, r"^[0-9a-f]{64}$")

        with_metadata = dict(doc_a)
        with_metadata["metadata"] = {"x": 1}
        with self.assertRaises(ValueError):
            compute_policy_document_sha256(with_metadata)

    def test_09_create_backup_record_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            self.assertIsNotNone(backup)
            assert backup is not None
            self.assertEqual(backup.backup_version, FAST_SAFETY_BACKUP_VERSION)
            self.assertTrue(backup.previous_absent)
            self.assertIsNone(backup.previous_row_version)
            self.assertIsNone(backup.previous_value_json)
            self.assertEqual(
                backup.previous_classification,
                FastSafetyReadStatus.ABSENT.value,
            )

    def test_10_create_backup_record_present_enabled_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            enabled_doc = _valid_enabled_document(market="KR")
            enabled_row = insert_config_kv_if_absent(key, enabled_doc, db_path=db)
            enabled_backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            self.assertIsNotNone(enabled_backup)
            assert enabled_backup is not None
            self.assertFalse(enabled_backup.previous_absent)
            self.assertEqual(enabled_backup.previous_row_version, enabled_row.version)
            self.assertEqual(
                enabled_backup.previous_value_json,
                enabled_row.value_json,
            )
            self.assertEqual(
                enabled_backup.backup_value_json_sha256,
                sha256_utf8(enabled_row.value_json),
            )
            self.assertEqual(
                enabled_backup.previous_classification,
                FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
            )

            disabled_doc = _valid_disabled_document(market="US")
            us_key = FAST_SAFETY_POLICY_KEYS["US"]
            disabled_row = insert_config_kv_if_absent(
                us_key,
                disabled_doc,
                db_path=db,
            )
            disabled_backup = create_backup_record(
                "US",
                created_at=_CREATED_AT,
                db_path=db,
            )
            self.assertIsNotNone(disabled_backup)
            assert disabled_backup is not None
            self.assertEqual(
                disabled_backup.previous_classification,
                FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
            )
            self.assertEqual(
                disabled_backup.backup_value_json_sha256,
                sha256_utf8(disabled_row.value_json),
            )

    def test_11_apply_disabled_from_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            assert backup is not None
            result = apply_disabled_policy(
                "KR",
                200.0,
                backup,
                checkpoint_created_at=_CHECKPOINT_AT,
                db_path=db,
            )
            self.assertTrue(result.ok)
            assert result.checkpoint is not None
            self.assertEqual(result.checkpoint.row_version, 1)
            self.assertEqual(
                result.checkpoint.checkpoint_version,
                FAST_SAFETY_CHECKPOINT_VERSION,
            )
            verify = verify_fast_safety_policy(
                "KR",
                expected_status=FastSafetyReadStatus.PRESENT_DISABLED_VALID,
                expected_policy_sha256=result.checkpoint.policy_document_sha256,
                db_path=db,
            )
            self.assertTrue(verify.ok)

    def test_12_apply_enabled_over_valid_existing_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            disabled_doc = _valid_disabled_document(market="KR", generated_at=10.0)
            insert_config_kv_if_absent(key, disabled_doc, db_path=db)
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            assert backup is not None
            enabled_doc = _valid_enabled_document(market="KR", generated_at=300.0)
            result = apply_enabled_policy(
                enabled_doc,
                backup,
                checkpoint_created_at=_CHECKPOINT_AT,
                db_path=db,
            )
            self.assertTrue(result.ok)
            assert result.checkpoint is not None
            self.assertEqual(result.checkpoint.row_version, 2)
            verify = verify_fast_safety_policy(
                "KR",
                expected_status=FastSafetyReadStatus.PRESENT_ENABLED_VALID,
                expected_policy_sha256=result.checkpoint.policy_document_sha256,
                db_path=db,
            )
            self.assertTrue(verify.ok)

    def test_13_apply_occ_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            row = insert_config_kv_if_absent(
                key,
                _valid_disabled_document(market="KR"),
                db_path=db,
            )
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            assert backup is not None
            update_config_kv_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                new_value=_valid_disabled_document(market="KR", generated_at=999.0),
                db_path=db,
            )
            result = apply_disabled_policy(
                "KR",
                400.0,
                backup,
                checkpoint_created_at=_CHECKPOINT_AT,
                db_path=db,
            )
            self.assertFalse(result.ok)
            self.assertEqual(result.reason, "concurrency conflict")
            self.assertIsNone(result.checkpoint)
            self.assertFalse(result.requires_rollback)
            current = read_config_kv_row(key, db_path=db)
            self.assertEqual(current.version, 2)
            self.assertNotEqual(current.value_json, row.value_json)

    def test_14_stop_states_block_apply(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]

            _insert_raw_json(db, key, "{bad", version=1)
            undecodable_backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            self.assertIsNone(undecodable_backup)

            _insert_raw_json(
                db,
                key,
                json.dumps(_valid_enabled_document(market="US")),
                version=2,
            )
            invalid_backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            self.assertIsNone(invalid_backup)

            with patch(
                "fast_safety_policy_admin.read_config_kv_row",
                side_effect=RuntimeError("read failed"),
            ):
                read_error_backup = create_backup_record(
                    "KR",
                    created_at=_CREATED_AT,
                    db_path=db,
                )
            self.assertIsNone(read_error_backup)

            invalid_backup = BackupRecord(
                backup_version=FAST_SAFETY_BACKUP_VERSION,
                created_at=_CREATED_AT,
                market="KR",
                config_key=key,
                previous_absent=False,
                previous_row_version=1,
                previous_value_json='{"enabled": false}',
                backup_value_json_sha256="0" * 64,
                previous_classification=FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
            )
            enabled_doc = _valid_enabled_document(market="KR")
            with patch(
                "fast_safety_policy_admin.insert_config_kv_if_absent",
            ) as mock_insert, patch(
                "fast_safety_policy_admin.update_config_kv_if_match",
            ) as mock_update:
                disabled_result = apply_disabled_policy(
                    "KR",
                    1.0,
                    invalid_backup,
                    checkpoint_created_at=_CHECKPOINT_AT,
                    db_path=db,
                )
                enabled_result = apply_enabled_policy(
                    enabled_doc,
                    invalid_backup,
                    checkpoint_created_at=_CHECKPOINT_AT,
                    db_path=db,
                )
                mock_insert.assert_not_called()
                mock_update.assert_not_called()
            self.assertFalse(disabled_result.ok)
            self.assertFalse(enabled_result.ok)

    def test_15_verify_enabled_disabled_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            absent = verify_fast_safety_policy("KR", db_path=db)
            self.assertTrue(absent.ok)
            self.assertEqual(absent.inspection.status, FastSafetyReadStatus.ABSENT)

            key = FAST_SAFETY_POLICY_KEYS["KR"]
            disabled_doc = _valid_disabled_document(market="KR")
            insert_config_kv_if_absent(key, disabled_doc, db_path=db)
            disabled = verify_fast_safety_policy(
                "KR",
                expected_status=FastSafetyReadStatus.PRESENT_DISABLED_VALID,
                db_path=db,
            )
            self.assertTrue(disabled.ok)
            self.assertIsNotNone(disabled.policy_document_sha256)

            enabled_doc = _valid_enabled_document(market="KR", generated_at=500.0)
            row = read_config_kv_row(key, db_path=db)
            update_config_kv_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                new_value=enabled_doc,
                db_path=db,
            )
            enabled = verify_fast_safety_policy(
                "KR",
                expected_status=FastSafetyReadStatus.PRESENT_ENABLED_VALID,
                db_path=db,
            )
            self.assertTrue(enabled.ok)
            self.assertIsNotNone(enabled.policy_document_sha256)

    def test_16_rollback_policy_value_exact_raw_restore(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            raw_enabled = (
                ' {"enabled": true, "market": "KR", '
                f'"version": "{FAST_SAFETY_POLICY_VERSION}", '
                f'"generated_at": 100.0, '
                f'"base_kelly_by_strategy": {{"{_STRATEGY_ID}": 0.08}}, '
                '"absolute_kelly_cap": 0.10} '
            )
            _insert_raw_json(db, key, raw_enabled, version=1)
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            assert backup is not None
            apply_result = apply_disabled_policy(
                "KR",
                700.0,
                backup,
                checkpoint_created_at=_CHECKPOINT_AT,
                db_path=db,
            )
            self.assertTrue(apply_result.ok)
            assert apply_result.checkpoint is not None

            rollback = rollback_policy_value(
                backup,
                apply_result.checkpoint,
                db_path=db,
            )
            self.assertTrue(rollback.ok)
            self.assertEqual(
                rollback.final_status,
                FastSafetyReadStatus.PRESENT_ENABLED_VALID,
            )
            row = read_config_kv_row(key, db_path=db)
            self.assertEqual(row.value_json, raw_enabled)
            self.assertEqual(
                sha256_utf8(row.value_json),
                backup.backup_value_json_sha256,
            )

    def test_17_rollback_policy_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            assert backup is not None
            apply_result = apply_disabled_policy(
                "KR",
                800.0,
                backup,
                checkpoint_created_at=_CHECKPOINT_AT,
                db_path=db,
            )
            self.assertTrue(apply_result.ok)
            assert apply_result.checkpoint is not None
            self.assertIsNotNone(read_config_kv_row(FAST_SAFETY_POLICY_KEYS["KR"], db_path=db))

            rollback = rollback_policy_absent(
                backup,
                apply_result.checkpoint,
                db_path=db,
            )
            self.assertTrue(rollback.ok)
            self.assertEqual(rollback.final_status, FastSafetyReadStatus.ABSENT)
            self.assertIsNone(
                read_config_kv_row(FAST_SAFETY_POLICY_KEYS["KR"], db_path=db)
            )
            inspection = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(inspection.status, FastSafetyReadStatus.ABSENT)

    def test_18_checkpoint_mismatch_blocks_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            row = insert_config_kv_if_absent(
                key,
                _valid_disabled_document(market="KR"),
                db_path=db,
            )
            backup = create_backup_record(
                "KR",
                created_at=_CREATED_AT,
                db_path=db,
            )
            assert backup is not None
            apply_result = apply_enabled_policy(
                _valid_enabled_document(market="KR", generated_at=900.0),
                backup,
                checkpoint_created_at=_CHECKPOINT_AT,
                db_path=db,
            )
            self.assertTrue(apply_result.ok)
            assert apply_result.checkpoint is not None
            checkpoint = apply_result.checkpoint
            current = read_config_kv_row(key, db_path=db)

            bad_version = rollback_policy_value(
                backup,
                AppliedCheckpoint(
                    checkpoint_version=checkpoint.checkpoint_version,
                    created_at=checkpoint.created_at,
                    market=checkpoint.market,
                    config_key=checkpoint.config_key,
                    row_version=checkpoint.row_version + 10,
                    value_json_sha256=checkpoint.value_json_sha256,
                    policy_document_sha256=checkpoint.policy_document_sha256,
                    classification=checkpoint.classification,
                ),
                db_path=db,
            )
            self.assertFalse(bad_version.ok)
            self.assertEqual(bad_version.reason, "checkpoint mismatch")
            unchanged = read_config_kv_row(key, db_path=db)
            self.assertEqual(unchanged.value_json, current.value_json)
            self.assertEqual(unchanged.version, current.version)

            bad_hash = rollback_policy_value(
                backup,
                AppliedCheckpoint(
                    checkpoint_version=checkpoint.checkpoint_version,
                    created_at=checkpoint.created_at,
                    market=checkpoint.market,
                    config_key=checkpoint.config_key,
                    row_version=checkpoint.row_version,
                    value_json_sha256="0" * 64,
                    policy_document_sha256=checkpoint.policy_document_sha256,
                    classification=checkpoint.classification,
                ),
                db_path=db,
            )
            self.assertFalse(bad_hash.ok)
            self.assertEqual(bad_hash.reason, "checkpoint mismatch")
            still_unchanged = read_config_kv_row(key, db_path=db)
            self.assertEqual(still_unchanged.value_json, current.value_json)


if __name__ == "__main__":
    unittest.main()
