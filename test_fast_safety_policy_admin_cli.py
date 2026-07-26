from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from typing import Any
from unittest.mock import patch

import config_manager
from config_manager import (
    insert_config_kv_if_absent,
    read_config_kv_row,
    sha256_utf8,
    update_config_kv_if_match,
)
from fast_safety_policy_admin import (
    FAST_SAFETY_BACKUP_VERSION,
    FAST_SAFETY_CHECKPOINT_VERSION,
    AppliedCheckpoint,
    ApplyResult,
    BackupRecord,
    FastSafetyReadStatus,
    InspectResult,
    compute_policy_document_sha256,
    create_backup_record,
    inspect_fast_safety_policy,
)
from fast_safety_policy_admin_artifacts import (
    FAST_SAFETY_APPROVAL_MANIFEST_VERSION,
    FAST_SAFETY_APPROVAL_OPERATION,
    ApprovalIdentity,
    FastSafetyApprovalManifest,
    load_applied_checkpoint,
    load_backup_record,
    save_approval_manifest,
    save_backup_record,
    validate_admin_apply_document,
)
from fast_safety_policy_admin_cli import (
    EXIT_CONCURRENCY_ERROR,
    EXIT_OPERATIONAL_ERROR,
    EXIT_RECOVERY_REQUIRED,
    EXIT_VALIDATION_ERROR,
    main,
)
from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    FAST_SAFETY_POLICY_VERSION,
)
from fast_safety_strategy_identity import build_strategy_identity

_STRATEGY_ID = "strat:abc123456789abcd"
_CREATED_AT = "2026-07-26T12:00:00Z"
_CHECKPOINT_AT = "2026-07-26T12:00:01Z"
_GENERATED_AT = "200.0"
_GROUP_KEY = "MFE_진화형_황금타점"


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


def _approval_identity(*, market: str = "KR") -> ApprovalIdentity:
    built = build_strategy_identity(market, _GROUP_KEY)
    assert built is not None
    return ApprovalIdentity(
        market=built.market,
        group_key=built.group_key,
        strategy_id=built.strategy_id,
    )


def _valid_enabled_policy_document(
    *,
    market: str = "KR",
    generated_at: float = 100.0,
) -> dict[str, Any]:
    identity = _approval_identity(market=market)
    return {
        "enabled": True,
        "market": market,
        "version": FAST_SAFETY_POLICY_VERSION,
        "generated_at": generated_at,
        "base_kelly_by_strategy": {identity.strategy_id: 0.08},
        "absolute_kelly_cap": 0.10,
    }


def _valid_manifest_for_document(
    document: dict[str, Any],
    *,
    market: str = "KR",
) -> FastSafetyApprovalManifest:
    validation = validate_admin_apply_document(document, expected_enabled=True)
    assert validation.ok
    assert validation.policy_document_sha256 is not None
    identity = _approval_identity(market=market)
    return FastSafetyApprovalManifest(
        manifest_version=FAST_SAFETY_APPROVAL_MANIFEST_VERSION,
        created_at=_CREATED_AT,
        market=market,
        config_key=FAST_SAFETY_POLICY_KEYS[market],
        operation=FAST_SAFETY_APPROVAL_OPERATION,
        policy_document_sha256=validation.policy_document_sha256,
        strategy_identities=(identity,),
    )


def _write_backup(db: str, tmp_dir: str, *, market: str = "KR") -> str:
    backup_path = os.path.join(tmp_dir, "backup.json")
    record = create_backup_record(
        market,
        created_at=_CREATED_AT,
        db_path=db,
    )
    assert record is not None
    save_backup_record(backup_path, record)
    return backup_path


def _apply_disabled_argv(
    db: str,
    backup_path: str,
    checkpoint_path: str,
    *,
    execute: bool = False,
    overwrite: bool = False,
) -> list[str]:
    argv = [
        "apply-disabled",
        "--market",
        "KR",
        "--db-path",
        db,
        "--backup",
        backup_path,
        "--generated-at",
        _GENERATED_AT,
        "--checkpoint-created-at",
        _CHECKPOINT_AT,
        "--checkpoint-output",
        checkpoint_path,
    ]
    if execute:
        argv.append("--execute")
    if overwrite:
        argv.append("--overwrite")
    return argv


def _apply_enabled_argv(
    db: str,
    backup_path: str,
    document_path: str,
    approval_path: str,
    checkpoint_path: str,
    *,
    execute: bool = False,
    overwrite: bool = False,
) -> list[str]:
    argv = [
        "apply-enabled",
        "--market",
        "KR",
        "--db-path",
        db,
        "--backup",
        backup_path,
        "--document",
        document_path,
        "--approval",
        approval_path,
        "--checkpoint-created-at",
        _CHECKPOINT_AT,
        "--checkpoint-output",
        checkpoint_path,
    ]
    if execute:
        argv.append("--execute")
    if overwrite:
        argv.append("--overwrite")
    return argv


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


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = main(argv)
    return code, stdout.getvalue(), stderr.getvalue()


class FastSafetyPolicyAdminCliTests(unittest.TestCase):
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
            code, stdout, stderr = _run_cli(
                ["inspect", "--market", "KR", "--db-path", db]
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertEqual(payload["command"], "inspect")
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["status"], FastSafetyReadStatus.ABSENT.value)
            self.assertEqual(payload["market"], "KR")
            self.assertEqual(payload["config_key"], FAST_SAFETY_POLICY_KEYS["KR"])

    def test_02_inspect_enabled_disabled_valid(self) -> None:
        cases = (
            ("KR", _valid_enabled_document(market="KR"), FastSafetyReadStatus.PRESENT_ENABLED_VALID),
            ("US", _valid_disabled_document(market="US"), FastSafetyReadStatus.PRESENT_DISABLED_VALID),
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            for market, document, expected_status in cases:
                with self.subTest(market=market, expected_status=expected_status.value):
                    key = FAST_SAFETY_POLICY_KEYS[market]
                    before = read_config_kv_row(key, db_path=db)
                    insert_config_kv_if_absent(key, document, db_path=db)
                    code, stdout, stderr = _run_cli(
                        ["inspect", "--market", market, "--db-path", db]
                    )
                    self.assertEqual(code, 0)
                    self.assertEqual(stderr, "")
                    payload = json.loads(stdout.strip())
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["status"], expected_status.value)
                    after = read_config_kv_row(key, db_path=db)
                    if before is None:
                        self.assertIsNotNone(after)
                    else:
                        self.assertEqual(after.version, before.version)
                        self.assertEqual(after.value_json, before.value_json)

    def test_03_inspect_invalid_undecodable(self) -> None:
        cases = (
            (_valid_enabled_document(market="US"), FastSafetyReadStatus.PRESENT_INVALID_DOCUMENT),
            ("{not-json", FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON),
            ('{"x": NaN}', FastSafetyReadStatus.PRESENT_UNDECODABLE_JSON),
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            for idx, (raw_or_doc, expected_status) in enumerate(cases, start=1):
                with self.subTest(expected_status=expected_status.value):
                    if isinstance(raw_or_doc, str):
                        _insert_raw_json(db, key, raw_or_doc, version=idx)
                    else:
                        insert_config_kv_if_absent(key, raw_or_doc, db_path=db)
                    code, stdout, stderr = _run_cli(
                        ["inspect", "--market", "KR", "--db-path", db]
                    )
                    self.assertEqual(code, 0)
                    self.assertEqual(stderr, "")
                    payload = json.loads(stdout.strip())
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["status"], expected_status.value)

    def test_04_inspect_read_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            with patch(
                "fast_safety_policy_admin_cli.inspect_fast_safety_policy",
                return_value=InspectResult(
                    market="KR",
                    config_key=FAST_SAFETY_POLICY_KEYS["KR"],
                    status=FastSafetyReadStatus.READ_ERROR,
                    row_version=None,
                    value_json_sha256=None,
                    document=None,
                    error="RuntimeError",
                ),
            ):
                code, stdout, stderr = _run_cli(
                    ["inspect", "--market", "KR", "--db-path", db]
                )
            self.assertEqual(code, EXIT_OPERATIONAL_ERROR)
            payload = json.loads(stdout.strip())
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["status"], FastSafetyReadStatus.READ_ERROR.value)
            self.assertNotIn("Traceback", stderr)
            self.assertNotIn(db, stderr)

    def test_05_validate_enabled_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            doc_path = os.path.join(tmp_dir, "policy.json")
            document = _valid_enabled_document(market="KR")
            with open(doc_path, "w", encoding="utf-8") as handle:
                json.dump(document, handle)
            code, stdout, stderr = _run_cli(
                [
                    "validate",
                    "--document",
                    doc_path,
                    "--expected-enabled",
                    "enabled",
                ]
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["ok"])
            self.assertIsNotNone(payload["payload"])
            self.assertEqual(payload["payload"]["enabled"], True)
            self.assertEqual(payload["payload"]["market"], "KR")
            expected_checksum = compute_policy_document_sha256(document)
            self.assertEqual(payload["policy_document_sha256"], expected_checksum)

    def test_06_validate_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cases = [
                (
                    "malformed",
                    "{not-json",
                    lambda path: ["validate", "--document", path],
                ),
                (
                    "metadata",
                    {**_valid_enabled_document(market="KR"), "metadata": {"x": 1}},
                    lambda path: ["validate", "--document", path],
                ),
                (
                    "enabled-mismatch",
                    _valid_disabled_document(market="KR"),
                    lambda path: [
                        "validate",
                        "--document",
                        path,
                        "--expected-enabled",
                        "enabled",
                    ],
                ),
            ]
            for name, content, build_argv in cases:
                with self.subTest(case=name):
                    doc_path = os.path.join(tmp_dir, f"{name}.json")
                    with open(doc_path, "w", encoding="utf-8") as handle:
                        if isinstance(content, str):
                            handle.write(content)
                        else:
                            json.dump(content, handle)
                    argv = build_argv(doc_path)
                    code, stdout, stderr = _run_cli(argv)
                    self.assertEqual(code, EXIT_VALIDATION_ERROR)
                    self.assertEqual(stderr, "")
                    payload = json.loads(stdout.strip())
                    self.assertFalse(payload["ok"])

    def test_07_validate_file_read_error(self) -> None:
        missing = os.path.join(tempfile.gettempdir(), "missing-fast-safety-policy.json")
        code, stdout, stderr = _run_cli(
            ["validate", "--document", missing]
        )
        self.assertEqual(code, EXIT_OPERATIONAL_ERROR)
        self.assertNotIn("Traceback", stderr)
        self.assertNotIn(missing, stderr)
        self.assertIn("file-read-error", stderr)

    def test_08_backup_absent_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            output = os.path.join(tmp_dir, "backup.json")
            code, stdout, stderr = _run_cli(
                [
                    "backup",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--created-at",
                    _CREATED_AT,
                    "--output",
                    output,
                ]
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["ok"])
            self.assertTrue(payload["artifact_written"])
            self.assertTrue(os.path.isfile(output))
            loaded = load_backup_record(output)
            self.assertEqual(loaded.backup_version, FAST_SAFETY_BACKUP_VERSION)
            self.assertTrue(loaded.previous_absent)
            absent = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(absent.status, FastSafetyReadStatus.ABSENT)

    def test_09_backup_present_success(self) -> None:
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
            _insert_raw_json(db, key, raw_enabled, version=3)
            before = read_config_kv_row(key, db_path=db)
            output = os.path.join(tmp_dir, "backup.json")
            code, stdout, stderr = _run_cli(
                [
                    "backup",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--created-at",
                    _CREATED_AT,
                    "--output",
                    output,
                ]
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["ok"])
            loaded = load_backup_record(output)
            self.assertEqual(loaded.previous_value_json, raw_enabled)
            self.assertEqual(
                loaded.backup_value_json_sha256,
                sha256_utf8(raw_enabled),
            )
            after = read_config_kv_row(key, db_path=db)
            self.assertEqual(after.version, before.version)
            self.assertEqual(after.value_json, before.value_json)

    def test_10_backup_stop_states_blocked(self) -> None:
        cases = (
            (_valid_enabled_document(market="US"), "invalid"),
            ("{not-json", "undecodable"),
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            for idx, (raw_or_doc, label) in enumerate(cases, start=1):
                with self.subTest(state=label):
                    output = os.path.join(tmp_dir, f"blocked-{label}.json")
                    if isinstance(raw_or_doc, str):
                        _insert_raw_json(db, key, raw_or_doc, version=idx)
                    else:
                        insert_config_kv_if_absent(key, raw_or_doc, db_path=db)
                    code, stdout, stderr = _run_cli(
                        [
                            "backup",
                            "--market",
                            "KR",
                            "--db-path",
                            db,
                            "--created-at",
                            _CREATED_AT,
                            "--output",
                            output,
                        ]
                    )
                    self.assertEqual(code, EXIT_VALIDATION_ERROR)
                    self.assertEqual(stderr, "")
                    payload = json.loads(stdout.strip())
                    self.assertFalse(payload["ok"])
                    self.assertFalse(os.path.exists(output))

    def test_11_backup_overwrite_protection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            output = os.path.join(tmp_dir, "backup.json")
            original = '{"keep": true}'
            with open(output, "w", encoding="utf-8") as handle:
                handle.write(original)
            code, stdout, stderr = _run_cli(
                [
                    "backup",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--created-at",
                    _CREATED_AT,
                    "--output",
                    output,
                ]
            )
            self.assertEqual(code, EXIT_OPERATIONAL_ERROR)
            with open(output, encoding="utf-8") as handle:
                self.assertEqual(handle.read(), original)
            code_ok, stdout_ok, stderr_ok = _run_cli(
                [
                    "backup",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--created-at",
                    _CREATED_AT,
                    "--output",
                    output,
                    "--overwrite",
                ]
            )
            self.assertEqual(code_ok, 0)
            self.assertEqual(stderr_ok, "")
            payload = json.loads(stdout_ok.strip())
            self.assertTrue(payload["ok"])
            loaded = load_backup_record(output)
            self.assertTrue(loaded.previous_absent)

    def test_12_verify_enabled_disabled_absent_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            absent_code, absent_stdout, absent_stderr = _run_cli(
                ["verify", "--market", "KR", "--db-path", db]
            )
            self.assertEqual(absent_code, 0)
            self.assertEqual(absent_stderr, "")
            absent_payload = json.loads(absent_stdout.strip())
            self.assertTrue(absent_payload["ok"])
            self.assertEqual(
                absent_payload["status"],
                FastSafetyReadStatus.ABSENT.value,
            )

            key = FAST_SAFETY_POLICY_KEYS["KR"]
            disabled_doc = _valid_disabled_document(market="KR")
            insert_config_kv_if_absent(key, disabled_doc, db_path=db)
            disabled_checksum = compute_policy_document_sha256(disabled_doc)
            disabled_code, disabled_stdout, disabled_stderr = _run_cli(
                [
                    "verify",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--expected-status",
                    FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
                    "--expected-policy-sha256",
                    disabled_checksum,
                ]
            )
            self.assertEqual(disabled_code, 0)
            self.assertEqual(disabled_stderr, "")

            enabled_doc = _valid_enabled_document(market="KR", generated_at=500.0)
            row = read_config_kv_row(key, db_path=db)
            from config_manager import update_config_kv_if_match

            update_config_kv_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                new_value=enabled_doc,
                db_path=db,
            )
            enabled_checksum = compute_policy_document_sha256(enabled_doc)
            enabled_code, enabled_stdout, enabled_stderr = _run_cli(
                [
                    "verify",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--expected-status",
                    FastSafetyReadStatus.PRESENT_ENABLED_VALID.value,
                    "--expected-policy-sha256",
                    enabled_checksum,
                ]
            )
            self.assertEqual(enabled_code, 0)
            self.assertEqual(enabled_stderr, "")

    def test_13_verify_mismatch_and_invalid_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            insert_config_kv_if_absent(
                key,
                _valid_enabled_document(market="KR"),
                db_path=db,
            )
            bad_hash_code, bad_hash_stdout, bad_hash_stderr = _run_cli(
                [
                    "verify",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--expected-policy-sha256",
                    "ZZ" * 32,
                ]
            )
            self.assertEqual(bad_hash_code, EXIT_VALIDATION_ERROR)
            self.assertEqual(bad_hash_stderr, "")

            mismatch_code, mismatch_stdout, mismatch_stderr = _run_cli(
                [
                    "verify",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--expected-status",
                    FastSafetyReadStatus.ABSENT.value,
                ]
            )
            self.assertEqual(mismatch_code, EXIT_VALIDATION_ERROR)
            self.assertEqual(mismatch_stderr, "")
            mismatch_payload = json.loads(mismatch_stdout.strip())
            self.assertFalse(mismatch_payload["ok"])

            with patch(
                "fast_safety_policy_admin_cli.verify_fast_safety_policy",
            ) as mock_verify:
                mock_verify.return_value = type(
                    "VerifyResult",
                    (),
                    {
                        "ok": False,
                        "reason": "policy checksum mismatch",
                        "inspection": inspect_fast_safety_policy("KR", db_path=db),
                        "policy_document_sha256": "a" * 64,
                    },
                )()
                checksum_code, checksum_stdout, checksum_stderr = _run_cli(
                    [
                        "verify",
                        "--market",
                        "KR",
                        "--db-path",
                        db,
                        "--expected-policy-sha256",
                        "b" * 64,
                    ]
                )
            self.assertEqual(checksum_code, EXIT_VALIDATION_ERROR)
            self.assertEqual(checksum_stderr, "")

    def test_14_cli_safety_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            with patch(
                "fast_safety_policy_admin_cli.inspect_fast_safety_policy",
            ) as mock_inspect:
                code, stdout, stderr = _run_cli(
                    ["inspect", "--market", "JP", "--db-path", db]
                )
                mock_inspect.assert_not_called()
            self.assertEqual(code, EXIT_VALIDATION_ERROR)
            self.assertNotIn("Traceback", stderr)

            missing_db_code, missing_db_stdout, missing_db_stderr = _run_cli(
                ["inspect", "--market", "KR"]
            )
            self.assertEqual(missing_db_code, EXIT_OPERATIONAL_ERROR)
            self.assertIn("usage-error", missing_db_stderr)

            forbidden_code, forbidden_stdout, forbidden_stderr = _run_cli(
                [
                    "inspect",
                    "--market",
                    "KR",
                    "--db-path",
                    db,
                    "--document",
                    os.path.join(tmp_dir, "x.json"),
                ]
            )
            self.assertEqual(forbidden_code, EXIT_OPERATIONAL_ERROR)
            self.assertIn("usage-error", forbidden_stderr)

            with patch(
                "config_manager.insert_config_kv_if_absent",
            ) as mock_insert, patch(
                "config_manager.update_config_kv_if_match",
            ) as mock_update, patch(
                "config_manager.replace_config_kv_json_if_match",
            ) as mock_replace, patch(
                "config_manager.delete_config_kv_if_match",
            ) as mock_delete:
                _run_cli(["inspect", "--market", "KR", "--db-path", db])
                _run_cli(
                    [
                        "backup",
                        "--market",
                        "KR",
                        "--db-path",
                        db,
                        "--created-at",
                        _CREATED_AT,
                        "--output",
                        os.path.join(tmp_dir, "b.json"),
                    ]
                )
                _run_cli(["verify", "--market", "KR", "--db-path", db])
                mock_insert.assert_not_called()
                mock_update.assert_not_called()
                mock_replace.assert_not_called()
                mock_delete.assert_not_called()

            ok_code, ok_stdout, ok_stderr = _run_cli(
                ["inspect", "--market", "KR", "--db-path", db]
            )
            self.assertEqual(ok_code, 0)
            self.assertEqual(ok_stderr, "")
            lines = ok_stdout.splitlines()
            self.assertEqual(len(lines), 1)
            json.loads(lines[0])

    def test_15_apply_disabled_dry_run_from_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            with patch(
                "fast_safety_policy_admin_cli.apply_disabled_policy",
            ) as mock_apply:
                code, stdout, stderr = _run_cli(
                    _apply_disabled_argv(db, backup_path, checkpoint_path)
                )
                mock_apply.assert_not_called()
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["ok"])
            self.assertFalse(payload["executed"])
            self.assertFalse(payload["checkpoint_artifact_written"])
            self.assertIsNone(payload["checkpoint"])
            absent = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(absent.status, FastSafetyReadStatus.ABSENT)
            self.assertFalse(os.path.exists(checkpoint_path))

    def test_16_apply_disabled_execute_from_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            code, stdout, stderr = _run_cli(
                _apply_disabled_argv(
                    db,
                    backup_path,
                    checkpoint_path,
                    execute=True,
                )
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["ok"])
            self.assertTrue(payload["executed"])
            self.assertTrue(payload["checkpoint_artifact_written"])
            loaded = load_applied_checkpoint(checkpoint_path)
            self.assertEqual(loaded.row_version, 1)
            verify = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(
                verify.status,
                FastSafetyReadStatus.PRESENT_DISABLED_VALID,
            )

    def test_17_apply_disabled_over_present_valid_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            kr_key = FAST_SAFETY_POLICY_KEYS["KR"]
            us_key = FAST_SAFETY_POLICY_KEYS["US"]
            enabled_doc = _valid_enabled_document(market="KR", generated_at=100.0)
            insert_config_kv_if_absent(kr_key, enabled_doc, db_path=db)
            us_doc = _valid_disabled_document(market="US", generated_at=10.0)
            insert_config_kv_if_absent(us_key, us_doc, db_path=db)
            us_before = read_config_kv_row(us_key, db_path=db)
            backup_path = _write_backup(db, tmp_dir)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            code, stdout, stderr = _run_cli(
                _apply_disabled_argv(
                    db,
                    backup_path,
                    checkpoint_path,
                    execute=True,
                )
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["executed"])
            kr_row = read_config_kv_row(kr_key, db_path=db)
            assert kr_row is not None
            self.assertEqual(kr_row.version, 2)
            loaded = load_applied_checkpoint(checkpoint_path)
            self.assertEqual(loaded.row_version, 2)
            self.assertEqual(
                loaded.value_json_sha256,
                sha256_utf8(kr_row.value_json),
            )
            us_after = read_config_kv_row(us_key, db_path=db)
            assert us_before is not None and us_after is not None
            self.assertEqual(us_after.version, us_before.version)
            self.assertEqual(us_after.value_json, us_before.value_json)

    def test_18_apply_enabled_dry_run_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            document = _valid_enabled_policy_document(market="KR")
            doc_path = os.path.join(tmp_dir, "policy.json")
            with open(doc_path, "w", encoding="utf-8") as handle:
                json.dump(document, handle)
            manifest = _valid_manifest_for_document(document, market="KR")
            approval_path = os.path.join(tmp_dir, "approval.json")
            save_approval_manifest(approval_path, manifest)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            with patch(
                "fast_safety_policy_admin_cli.apply_enabled_policy",
            ) as mock_apply:
                code, stdout, stderr = _run_cli(
                    _apply_enabled_argv(
                        db,
                        backup_path,
                        doc_path,
                        approval_path,
                        checkpoint_path,
                    )
                )
                mock_apply.assert_not_called()
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["ok"])
            self.assertFalse(payload["executed"])
            expected_checksum = compute_policy_document_sha256(document)
            self.assertEqual(payload["policy_document_sha256"], expected_checksum)
            absent = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(absent.status, FastSafetyReadStatus.ABSENT)

    def test_19_apply_enabled_execute_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            document = _valid_enabled_policy_document(market="KR", generated_at=300.0)
            doc_path = os.path.join(tmp_dir, "policy.json")
            with open(doc_path, "w", encoding="utf-8") as handle:
                json.dump(document, handle)
            manifest = _valid_manifest_for_document(document, market="KR")
            approval_path = os.path.join(tmp_dir, "approval.json")
            save_approval_manifest(approval_path, manifest)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            code, stdout, stderr = _run_cli(
                _apply_enabled_argv(
                    db,
                    backup_path,
                    doc_path,
                    approval_path,
                    checkpoint_path,
                    execute=True,
                )
            )
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["executed"])
            loaded = load_applied_checkpoint(checkpoint_path)
            expected_checksum = compute_policy_document_sha256(document)
            self.assertEqual(loaded.policy_document_sha256, expected_checksum)
            verify = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(
                verify.status,
                FastSafetyReadStatus.PRESENT_ENABLED_VALID,
            )

    def test_20_enabled_approval_missing_malformed_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            document = _valid_enabled_policy_document(market="KR")
            doc_path = os.path.join(tmp_dir, "policy.json")
            with open(doc_path, "w", encoding="utf-8") as handle:
                json.dump(document, handle)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            missing_approval = os.path.join(tmp_dir, "missing-approval.json")
            cases = (
                (
                    "missing",
                    missing_approval,
                    EXIT_OPERATIONAL_ERROR,
                ),
                (
                    "malformed",
                    os.path.join(tmp_dir, "bad-approval.json"),
                    EXIT_VALIDATION_ERROR,
                ),
            )
            for label, approval_path, expected_code in cases:
                with self.subTest(case=label):
                    if label == "malformed":
                        with open(approval_path, "w", encoding="utf-8") as handle:
                            handle.write("{not-json")
                    with patch(
                        "fast_safety_policy_admin_cli.apply_enabled_policy",
                    ) as mock_apply:
                        code, stdout, stderr = _run_cli(
                            _apply_enabled_argv(
                                db,
                                backup_path,
                                doc_path,
                                approval_path,
                                checkpoint_path,
                            )
                        )
                        mock_apply.assert_not_called()
                    self.assertEqual(code, expected_code)
                    self.assertNotIn("Traceback", stderr)
                    absent = inspect_fast_safety_policy("KR", db_path=db)
                    self.assertEqual(absent.status, FastSafetyReadStatus.ABSENT)

    def test_21_enabled_approval_cross_validation_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            document = _valid_enabled_policy_document(market="KR")
            doc_path = os.path.join(tmp_dir, "policy.json")
            with open(doc_path, "w", encoding="utf-8") as handle:
                json.dump(document, handle)
            manifest = _valid_manifest_for_document(document, market="KR")
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            cases = {
                "wrong-checksum": FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256="a" * 64,
                    strategy_identities=manifest.strategy_identities,
                ),
                "wrong-market": FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market="US",
                    config_key=FAST_SAFETY_POLICY_KEYS["US"],
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=manifest.strategy_identities,
                ),
                "wrong-config-key": FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key="wrong.key",
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=manifest.strategy_identities,
                ),
                "wrong-identity": FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=(
                        ApprovalIdentity(
                            market="KR",
                            group_key="wrong",
                            strategy_id="strat:wrongwrongwrongwrong",
                        ),
                    ),
                ),
                "missing-identity": FastSafetyApprovalManifest(
                    manifest_version=manifest.manifest_version,
                    created_at=manifest.created_at,
                    market=manifest.market,
                    config_key=manifest.config_key,
                    operation=manifest.operation,
                    policy_document_sha256=manifest.policy_document_sha256,
                    strategy_identities=(),
                ),
                "extra-identity": FastSafetyApprovalManifest(
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
            }
            for name, bad_manifest in cases.items():
                with self.subTest(case=name):
                    approval_path = os.path.join(tmp_dir, f"approval-{name}.json")
                    save_approval_manifest(approval_path, bad_manifest, overwrite=True)
                    with patch(
                        "fast_safety_policy_admin_cli.apply_enabled_policy",
                    ) as mock_apply:
                        code, stdout, stderr = _run_cli(
                            _apply_enabled_argv(
                                db,
                                backup_path,
                                doc_path,
                                approval_path,
                                checkpoint_path,
                            )
                        )
                        mock_apply.assert_not_called()
                    self.assertEqual(code, EXIT_VALIDATION_ERROR)
                    self.assertEqual(stderr, "")

    def test_22_stale_backup_preflight_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = FAST_SAFETY_POLICY_KEYS["KR"]
            row = insert_config_kv_if_absent(
                key,
                _valid_disabled_document(market="KR"),
                db_path=db,
            )
            backup_path = _write_backup(db, tmp_dir)
            update_config_kv_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                new_value=_valid_disabled_document(market="KR", generated_at=999.0),
                db_path=db,
            )
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            with patch(
                "fast_safety_policy_admin_cli.apply_disabled_policy",
            ) as mock_apply:
                code, stdout, stderr = _run_cli(
                    _apply_disabled_argv(
                        db,
                        backup_path,
                        checkpoint_path,
                        execute=True,
                    )
                )
                mock_apply.assert_not_called()
            self.assertEqual(code, EXIT_CONCURRENCY_ERROR)
            payload = json.loads(stdout.strip())
            self.assertFalse(payload["ok"])
            self.assertFalse(payload["executed"])
            current = read_config_kv_row(key, db_path=db)
            assert current is not None
            self.assertEqual(current.version, 2)

    def test_23_core_concurrency_conflict_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            with patch(
                "fast_safety_policy_admin_cli.apply_disabled_policy",
                return_value=ApplyResult(
                    ok=False,
                    reason="concurrency conflict",
                    checkpoint=None,
                    requires_rollback=False,
                ),
            ):
                code, stdout, stderr = _run_cli(
                    _apply_disabled_argv(
                        db,
                        backup_path,
                        checkpoint_path,
                        execute=True,
                    )
                )
            self.assertEqual(code, EXIT_CONCURRENCY_ERROR)
            payload = json.loads(stdout.strip())
            self.assertFalse(payload["ok"])
            self.assertTrue(payload["executed"])
            self.assertIsNone(payload["checkpoint"])
            self.assertFalse(payload["recovery_required"])
            self.assertFalse(os.path.exists(checkpoint_path))

    def test_24_stop_states_apply_blocked(self) -> None:
        checkpoint_path_name = "checkpoint.json"
        for label, expected_code in (
            ("invalid", EXIT_VALIDATION_ERROR),
            ("undecodable", EXIT_VALIDATION_ERROR),
            ("read-error", EXIT_OPERATIONAL_ERROR),
        ):
            with self.subTest(state=label):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    db = _db_path(tmp_dir)
                    key = FAST_SAFETY_POLICY_KEYS["KR"]
                    checkpoint_path = os.path.join(tmp_dir, checkpoint_path_name)
                    absent_backup = BackupRecord(
                        backup_version=FAST_SAFETY_BACKUP_VERSION,
                        created_at=_CREATED_AT,
                        market="KR",
                        config_key=key,
                        previous_absent=True,
                        previous_row_version=None,
                        previous_value_json=None,
                        backup_value_json_sha256=None,
                        previous_classification=FastSafetyReadStatus.ABSENT.value,
                    )
                    backup_path = os.path.join(tmp_dir, "backup.json")
                    save_backup_record(backup_path, absent_backup)

                    if label == "invalid":
                        insert_config_kv_if_absent(
                            key,
                            _valid_enabled_document(market="US"),
                            db_path=db,
                        )
                    elif label == "undecodable":
                        _insert_raw_json(db, key, "{bad", version=1)

                    inspect_patch = patch(
                        "fast_safety_policy_admin_cli.inspect_fast_safety_policy",
                        return_value=InspectResult(
                            market="KR",
                            config_key=key,
                            status=FastSafetyReadStatus.READ_ERROR,
                            row_version=None,
                            value_json_sha256=None,
                            document=None,
                            error="RuntimeError",
                        ),
                    )
                    with patch(
                        "fast_safety_policy_admin_cli.apply_disabled_policy",
                    ) as mock_apply, inspect_patch if label == "read-error" else contextlib.nullcontext():
                        code, stdout, stderr = _run_cli(
                            _apply_disabled_argv(
                                db,
                                backup_path,
                                checkpoint_path,
                                execute=True,
                            )
                        )
                        mock_apply.assert_not_called()
                    self.assertEqual(code, expected_code)
                    self.assertNotIn("Traceback", stderr)

    def test_25_checkpoint_output_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            existing = os.path.join(tmp_dir, "exists.json")
            with open(existing, "w", encoding="utf-8") as handle:
                handle.write("{}")
            missing_parent = os.path.join(tmp_dir, "missing", "checkpoint.json")
            cases = (
                ("exists-no-overwrite", existing, False),
                ("missing-parent", missing_parent, False),
            )
            for label, checkpoint_path, overwrite in cases:
                with self.subTest(case=label):
                    argv = _apply_disabled_argv(
                        db,
                        backup_path,
                        checkpoint_path,
                        execute=True,
                        overwrite=overwrite,
                    )
                    with patch(
                        "fast_safety_policy_admin_cli.apply_disabled_policy",
                    ) as mock_apply:
                        code, stdout, stderr = _run_cli(argv)
                        mock_apply.assert_not_called()
                    self.assertEqual(code, EXIT_OPERATIONAL_ERROR)
                    absent = inspect_fast_safety_policy("KR", db_path=db)
                    self.assertEqual(absent.status, FastSafetyReadStatus.ABSENT)

            unwritable_parent = os.path.join(tmp_dir, "readonly")
            os.mkdir(unwritable_parent)
            readonly_checkpoint = os.path.join(unwritable_parent, "checkpoint.json")
            with patch(
                "fast_safety_policy_admin_cli.tempfile.mkstemp",
                side_effect=PermissionError("denied"),
            ), patch(
                "fast_safety_policy_admin_cli.apply_disabled_policy",
            ) as mock_apply:
                code, stdout, stderr = _run_cli(
                    _apply_disabled_argv(
                        db,
                        backup_path,
                        readonly_checkpoint,
                        execute=True,
                    )
                )
                mock_apply.assert_not_called()
            self.assertEqual(code, EXIT_OPERATIONAL_ERROR)

    def test_26_db_write_success_checkpoint_save_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            with patch(
                "fast_safety_policy_admin_cli.save_applied_checkpoint",
                side_effect=OSError("write failed"),
            ):
                code, stdout, stderr = _run_cli(
                    _apply_disabled_argv(
                        db,
                        backup_path,
                        checkpoint_path,
                        execute=True,
                    )
                )
            self.assertEqual(code, EXIT_RECOVERY_REQUIRED)
            self.assertIn("checkpoint-artifact-error", stderr)
            self.assertNotIn("Traceback", stderr)
            self.assertNotIn(db, stdout)
            self.assertNotIn(checkpoint_path, stdout)
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["executed"])
            self.assertFalse(payload["checkpoint_artifact_written"])
            self.assertTrue(payload["recovery_required"])
            self.assertIsNotNone(payload["checkpoint"])
            verify = inspect_fast_safety_policy("KR", db_path=db)
            self.assertEqual(
                verify.status,
                FastSafetyReadStatus.PRESENT_DISABLED_VALID,
            )

    def test_27_post_apply_verify_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            backup_path = _write_backup(db, tmp_dir)
            checkpoint_path = os.path.join(tmp_dir, "checkpoint.json")
            checkpoint = AppliedCheckpoint(
                checkpoint_version=FAST_SAFETY_CHECKPOINT_VERSION,
                created_at=_CHECKPOINT_AT,
                market="KR",
                config_key=FAST_SAFETY_POLICY_KEYS["KR"],
                row_version=1,
                value_json_sha256="b" * 64,
                policy_document_sha256="c" * 64,
                classification=FastSafetyReadStatus.PRESENT_DISABLED_VALID.value,
            )
            with patch(
                "fast_safety_policy_admin_cli.apply_disabled_policy",
                return_value=ApplyResult(
                    ok=False,
                    reason="verify failed",
                    checkpoint=checkpoint,
                    requires_rollback=True,
                ),
            ), patch(
                "fast_safety_policy_admin_cli.save_applied_checkpoint",
            ) as mock_save:
                code, stdout, stderr = _run_cli(
                    _apply_disabled_argv(
                        db,
                        backup_path,
                        checkpoint_path,
                        execute=True,
                    )
                )
                mock_save.assert_called_once()
            self.assertEqual(code, EXIT_RECOVERY_REQUIRED)
            payload = json.loads(stdout.strip())
            self.assertTrue(payload["recovery_required"])
            self.assertTrue(payload["requires_rollback"])
            self.assertIsNotNone(payload["checkpoint"])


if __name__ == "__main__":
    unittest.main()
