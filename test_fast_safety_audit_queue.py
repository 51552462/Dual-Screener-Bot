"""Tests for the bounded in-memory Fast Safety audit emitter."""

import unittest
from unittest.mock import patch

from fast_safety_audit_queue import BoundedAuditEmitter


class BoundedAuditEmitterTests(unittest.TestCase):
    def test_default_capacity_creation(self):
        emitter = BoundedAuditEmitter()

        for index in range(1024):
            self.assertTrue(emitter.try_emit({"index": index}))

        self.assertFalse(emitter.try_emit({"index": 1024}))
        self.assertEqual(emitter.qsize(), 1024)

    def test_invalid_maxsize_is_rejected(self):
        invalid_values = (True, False, 0, -1, 1.5, "10", None)

        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    BoundedAuditEmitter(value)

    def test_emit_then_try_get(self):
        emitter = BoundedAuditEmitter(maxsize=2)

        self.assertTrue(emitter.try_emit({"event": "accepted"}))
        item = emitter.try_get()

        self.assertEqual(dict(item), {"event": "accepted"})
        self.assertEqual(emitter.qsize(), 0)

        with self.assertRaises(TypeError):
            item["event"] = "changed"

    def test_queue_full_returns_false(self):
        emitter = BoundedAuditEmitter(maxsize=1)

        self.assertTrue(emitter.try_emit({"event": 1}))
        self.assertFalse(emitter.try_emit({"event": 2}))
        self.assertEqual(emitter.qsize(), 1)

    def test_non_mapping_event_returns_false(self):
        emitter = BoundedAuditEmitter()

        for value in (None, 1, "event", [("key", "value")], object()):
            with self.subTest(value=value):
                self.assertFalse(emitter.try_emit(value))

        self.assertEqual(emitter.qsize(), 0)

    def test_put_nowait_exception_does_not_escape(self):
        emitter = BoundedAuditEmitter()

        internal_queue = emitter._BoundedAuditEmitter__queue
        with patch.object(
            internal_queue,
            "put_nowait",
            side_effect=RuntimeError("injected failure"),
        ):
            self.assertFalse(emitter.try_emit({"event": "safe"}))

    def test_original_mapping_mutation_does_not_change_event(self):
        emitter = BoundedAuditEmitter()
        original = {"status": "before", "count": 1}

        self.assertTrue(emitter.try_emit(original))

        original["status"] = "after"
        original["count"] = 99

        item = emitter.try_get()
        self.assertEqual(
            dict(item),
            {"status": "before", "count": 1},
        )

    def test_drain_limit_preserves_fifo_and_remaining_events(self):
        emitter = BoundedAuditEmitter(maxsize=4)

        for index in range(4):
            self.assertTrue(emitter.try_emit({"index": index}))

        drained = emitter.drain(limit=2)

        self.assertEqual(
            [item["index"] for item in drained],
            [0, 1],
        )
        self.assertEqual(emitter.qsize(), 2)

        remaining = emitter.drain()
        self.assertEqual(
            [item["index"] for item in remaining],
            [2, 3],
        )
        self.assertEqual(emitter.qsize(), 0)
        self.assertEqual(emitter.drain(limit=0), ())

    def test_empty_queue_try_get_returns_none(self):
        emitter = BoundedAuditEmitter()

        self.assertIsNone(emitter.try_get())


if __name__ == "__main__":
    unittest.main()
