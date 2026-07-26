from __future__ import annotations

import unittest
from typing import Any

from fast_safety_policy_store import (
    FAST_SAFETY_POLICY_KEYS,
    FAST_SAFETY_POLICY_VERSION,
    disable_fast_safety_policy,
    load_fast_safety_policy_snapshot,
    policy_key_for_market,
    write_fast_safety_policy_document,
)


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
        "base_kelly_by_strategy": base or {"strat:abc123456789abcd": 0.08},
        "absolute_kelly_cap": cap,
    }


class FastSafetyPolicyStoreTests(unittest.TestCase):
    def test_01_policy_key_for_market_kr_us_and_invalid(self) -> None:
        self.assertEqual(policy_key_for_market("KR"), FAST_SAFETY_POLICY_KEYS["KR"])
        self.assertEqual(policy_key_for_market(" us "), FAST_SAFETY_POLICY_KEYS["US"])
        self.assertIsNone(policy_key_for_market("JP"))
        self.assertIsNone(policy_key_for_market(""))
        self.assertIsNone(policy_key_for_market(None))

    def test_02_missing_policy_returns_none(self) -> None:
        store: dict[str, Any] = {}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

    def test_03_disabled_policy_returns_none(self) -> None:
        store = {
            FAST_SAFETY_POLICY_KEYS["KR"]: {
                "enabled": False,
                "market": "KR",
                "version": FAST_SAFETY_POLICY_VERSION,
                "generated_at": 1.0,
            }
        }

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

    def test_04_valid_kr_enabled_policy_loads_snapshot(self) -> None:
        doc = _valid_enabled_document(market="KR")
        store = {FAST_SAFETY_POLICY_KEYS["KR"]: doc}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        snapshot = load_fast_safety_policy_snapshot("KR", get_value=getter)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.market, "KR")
        self.assertEqual(snapshot.version, FAST_SAFETY_POLICY_VERSION)
        self.assertEqual(snapshot.generated_at, 100.0)
        self.assertEqual(
            dict(snapshot.base_kelly_by_strategy),
            {"strat:abc123456789abcd": 0.08},
        )
        self.assertEqual(snapshot.absolute_kelly_cap, 0.10)

    def test_05_document_market_mismatch_rejected(self) -> None:
        doc = _valid_enabled_document(market="US")
        store = {FAST_SAFETY_POLICY_KEYS["KR"]: doc}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

    def test_06_version_mismatch_rejected(self) -> None:
        doc = _valid_enabled_document(market="KR")
        doc["version"] = "other-version"
        store = {FAST_SAFETY_POLICY_KEYS["KR"]: doc}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

    def test_07_enabled_not_bool_rejected(self) -> None:
        doc = _valid_enabled_document(market="KR")
        doc["enabled"] = 1
        store = {FAST_SAFETY_POLICY_KEYS["KR"]: doc}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )
        self.assertFalse(write_fast_safety_policy_document(doc))

    def test_08_invalid_generated_at_rejected(self) -> None:
        doc = _valid_enabled_document(market="KR", generated_at=-1.0)
        store = {FAST_SAFETY_POLICY_KEYS["KR"]: doc}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )
        self.assertFalse(write_fast_safety_policy_document(doc))

    def test_09_invalid_base_kelly_or_cap_rejected(self) -> None:
        bad_base = _valid_enabled_document(
            market="KR",
            base={"strat:abc123456789abcd": 0.0},
        )
        bad_cap = _valid_enabled_document(market="KR", cap=0.0)

        self.assertFalse(write_fast_safety_policy_document(bad_base))
        self.assertFalse(write_fast_safety_policy_document(bad_cap))

        store = {FAST_SAFETY_POLICY_KEYS["KR"]: bad_base}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

    def test_10_non_neutral_alpha_rejected(self) -> None:
        doc = _valid_enabled_document(market="KR")
        doc["alpha_overlay_by_strategy"] = {
            "strat:abc123456789abcd": 1.5,
        }
        doc["max_alpha_overlay"] = 1.5

        self.assertFalse(write_fast_safety_policy_document(doc))

        store = {FAST_SAFETY_POLICY_KEYS["KR"]: doc}

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

    def test_11_valid_document_written_to_market_key(self) -> None:
        store: dict[str, Any] = {}
        captured: dict[str, Any] = {}

        def setter(key: str, value: Any) -> None:
            captured["key"] = key
            captured["value"] = value
            store[key] = value

        doc = _valid_enabled_document(market="US")
        self.assertTrue(
            write_fast_safety_policy_document(doc, set_value=setter)
        )
        self.assertEqual(captured["key"], FAST_SAFETY_POLICY_KEYS["US"])
        self.assertEqual(captured["value"]["enabled"], True)
        self.assertEqual(captured["value"]["market"], "US")
        self.assertEqual(captured["value"]["version"], FAST_SAFETY_POLICY_VERSION)

        def getter(key: str, default: Any = None) -> Any:
            return store.get(key, default)

        snapshot = load_fast_safety_policy_snapshot("US", get_value=getter)
        self.assertIsNotNone(snapshot)

    def test_12_writer_rejects_unknown_key_and_non_mapping(self) -> None:
        doc = _valid_enabled_document(market="KR")
        doc["unexpected"] = True
        self.assertFalse(write_fast_safety_policy_document(doc))
        self.assertFalse(write_fast_safety_policy_document(["enabled"]))

        def getter(key: str, default: Any = None) -> Any:
            if key == FAST_SAFETY_POLICY_KEYS["KR"]:
                return doc
            return default

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=getter)
        )

        bad_metadata = _valid_enabled_document(market="KR")
        bad_metadata["metadata"] = ["not-a-mapping"]

        def bad_metadata_getter(key: str, default: Any = None) -> Any:
            if key == FAST_SAFETY_POLICY_KEYS["KR"]:
                return bad_metadata
            return default

        self.assertFalse(write_fast_safety_policy_document(bad_metadata))
        self.assertIsNone(
            load_fast_safety_policy_snapshot(
                "KR",
                get_value=bad_metadata_getter,
            )
        )

        disabled_extra = {
            "enabled": False,
            "market": "KR",
            "version": FAST_SAFETY_POLICY_VERSION,
            "generated_at": 1.0,
            "base_kelly_by_strategy": {"x": 0.1},
        }
        self.assertFalse(write_fast_safety_policy_document(disabled_extra))

    def test_13_write_normalizes_copy_independent_of_source_mutation(self) -> None:
        captured: dict[str, Any] = {}

        def setter(key: str, value: Any) -> None:
            captured["key"] = key
            captured["value"] = value

        source = _valid_enabled_document(market="KR")
        source["metadata"] = {"owner": "ops"}
        self.assertTrue(
            write_fast_safety_policy_document(source, set_value=setter)
        )

        stored = captured["value"]
        source["base_kelly_by_strategy"]["strat:abc123456789abcd"] = 0.99
        source["metadata"]["owner"] = "mutated"

        self.assertEqual(
            stored["base_kelly_by_strategy"]["strat:abc123456789abcd"],
            0.08,
        )
        self.assertEqual(stored["metadata"]["owner"], "ops")

    def test_14_getter_setter_exceptions_do_not_propagate(self) -> None:
        def bad_getter(key: str, default: Any = None) -> Any:
            raise RuntimeError("getter failed")

        def bad_setter(key: str, value: Any) -> None:
            raise RuntimeError("setter failed")

        self.assertIsNone(
            load_fast_safety_policy_snapshot("KR", get_value=bad_getter)
        )
        self.assertFalse(
            write_fast_safety_policy_document(
                _valid_enabled_document(market="KR"),
                set_value=bad_setter,
            )
        )
        self.assertFalse(
            disable_fast_safety_policy(
                "KR",
                1.0,
                set_value=bad_setter,
            )
        )


if __name__ == "__main__":
    unittest.main()
