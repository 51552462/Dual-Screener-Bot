from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import config_manager
from config_manager import (
    ConfigConcurrencyError,
    ConfigKvRow,
    _encode_json,
    delete_config_kv_if_match,
    insert_config_kv_if_absent,
    read_config_kv_row,
    replace_config_kv_json_if_match,
    sha256_utf8,
    update_config_kv_if_match,
)


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


class FastSafetyConfigKvAtomicTests(unittest.TestCase):
    def test_01_read_config_kv_row_absent_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            self.assertIsNone(
                read_config_kv_row("FAST_SAFETY_POLICY_KR", db_path=db)
            )

    def test_02_insert_config_kv_if_absent_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            value = {"enabled": True, "market": "KR"}
            row = insert_config_kv_if_absent(
                "FAST_SAFETY_POLICY_KR",
                value,
                db_path=db,
            )
            expected_json = _encode_json(value)
            self.assertEqual(row.version, 1)
            self.assertEqual(row.value_json, expected_json)
            read_back = read_config_kv_row("FAST_SAFETY_POLICY_KR", db_path=db)
            self.assertEqual(read_back, ConfigKvRow("FAST_SAFETY_POLICY_KR", expected_json, 1))

    def test_03_insert_config_kv_if_absent_duplicate_raises_and_row_unchanged(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            first_value = {"enabled": True, "market": "KR"}
            first_row = insert_config_kv_if_absent(key, first_value, db_path=db)
            with self.assertRaises(ConfigConcurrencyError):
                insert_config_kv_if_absent(
                    key,
                    {"enabled": False, "market": "US"},
                    db_path=db,
                )
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, first_row)

    def test_04_update_config_kv_if_match_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            initial = {"enabled": True, "market": "KR", "cap": 0.1}
            row = insert_config_kv_if_absent(key, initial, db_path=db)
            updated = {"enabled": True, "market": "KR", "cap": 0.2}
            new_row = update_config_kv_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                new_value=updated,
                db_path=db,
            )
            self.assertEqual(new_row.version, 2)
            self.assertEqual(new_row.value_json, _encode_json(updated))
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, new_row)

    def test_05_update_config_kv_if_match_wrong_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(key, {"enabled": True}, db_path=db)
            with self.assertRaises(ConfigConcurrencyError):
                update_config_kv_if_match(
                    key,
                    expected_version=999,
                    expected_value_json_sha256=sha256_utf8(row.value_json),
                    new_value={"enabled": False},
                    db_path=db,
                )
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, row)

    def test_06_update_config_kv_if_match_wrong_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(key, {"enabled": True}, db_path=db)
            wrong_hash = sha256_utf8('{"enabled": false}')
            with self.assertRaises(ConfigConcurrencyError):
                update_config_kv_if_match(
                    key,
                    expected_version=row.version,
                    expected_value_json_sha256=wrong_hash,
                    new_value={"enabled": False},
                    db_path=db,
                )
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, row)

    def test_07_replace_config_kv_json_if_match_preserves_raw_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(
                key,
                {"b": 2, "a": 1},
                db_path=db,
            )
            replacement = '  {"b": 2, "a": 1}  '
            replaced = replace_config_kv_json_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                replacement_value_json=replacement,
                db_path=db,
            )
            self.assertEqual(replaced.version, 2)
            self.assertEqual(replaced.value_json, replacement)
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, replaced)

    def test_08_replace_config_kv_json_if_match_rejects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(key, {"enabled": True}, db_path=db)
            for invalid in ("not-json", '{"x": NaN}', '{"x": Infinity}'):
                with self.subTest(invalid=invalid):
                    with self.assertRaises(ValueError):
                        replace_config_kv_json_if_match(
                            key,
                            expected_version=row.version,
                            expected_value_json_sha256=sha256_utf8(row.value_json),
                            replacement_value_json=invalid,
                            db_path=db,
                        )
                    read_back = read_config_kv_row(key, db_path=db)
                    self.assertEqual(read_back, row)

    def test_09_delete_config_kv_if_match_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(key, {"enabled": True}, db_path=db)
            delete_config_kv_if_match(
                key,
                expected_version=row.version,
                expected_value_json_sha256=sha256_utf8(row.value_json),
                db_path=db,
            )
            self.assertIsNone(read_config_kv_row(key, db_path=db))

    def test_10_delete_config_kv_if_match_wrong_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(key, {"enabled": True}, db_path=db)
            with self.assertRaises(ConfigConcurrencyError):
                delete_config_kv_if_match(
                    key,
                    expected_version=999,
                    expected_value_json_sha256=sha256_utf8(row.value_json),
                    db_path=db,
                )
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, row)

    def test_11_delete_config_kv_if_match_wrong_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = _db_path(tmp_dir)
            key = "FAST_SAFETY_POLICY_KR"
            row = insert_config_kv_if_absent(key, {"enabled": True}, db_path=db)
            wrong_hash = sha256_utf8('{"enabled": false}')
            with self.assertRaises(ConfigConcurrencyError):
                delete_config_kv_if_match(
                    key,
                    expected_version=row.version,
                    expected_value_json_sha256=wrong_hash,
                    db_path=db,
                )
            read_back = read_config_kv_row(key, db_path=db)
            self.assertEqual(read_back, row)

    def test_12_explicit_db_path_isolation_without_touching_production_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_a = _db_path(tmp_dir, "a.sqlite")
            db_b = _db_path(tmp_dir, "b.sqlite")
            key = "FAST_SAFETY_POLICY_KR"
            value_a = {"enabled": True, "market": "KR"}
            value_b = {"enabled": True, "market": "US"}

            with patch(
                "sqlite3.connect",
                side_effect=_guard_production_db_connect(sqlite3.connect),
            ):
                row_a = insert_config_kv_if_absent(key, value_a, db_path=db_a)
                row_b = insert_config_kv_if_absent(key, value_b, db_path=db_b)

            self.assertEqual(read_config_kv_row(key, db_path=db_a), row_a)
            self.assertEqual(read_config_kv_row(key, db_path=db_b), row_b)
            self.assertNotEqual(row_a.value_json, row_b.value_json)


if __name__ == "__main__":
    unittest.main()
