"""Tests for Fast Safety Audit Runtime (Chapter 3-B0D3A4B)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from fast_safety_audit_queue import BoundedAuditEmitter
from fast_safety_audit_runtime import (
    FastSafetyAuditDrainResult,
    FastSafetyAuditRuntime,
    create_fast_safety_audit_runtime,
    drain_fast_safety_audit_runtime,
)


class FastSafetyAuditRuntimeTests(unittest.TestCase):
    def test_shadow_off_does_not_create_emitter(self) -> None:
        with patch(
            "fast_safety_audit_runtime.BoundedAuditEmitter",
        ) as emitter_cls:
            runtime = create_fast_safety_audit_runtime(shadow_enabled=False)

        emitter_cls.assert_not_called()
        self.assertFalse(runtime.shadow_enabled)
        self.assertFalse(runtime.ready)
        self.assertIsNone(runtime.emitter)
        self.assertEqual(runtime.reason, "shadow-disabled")

    def test_invalid_shadow_enabled_is_safe_off(self) -> None:
        invalid_values = (1, 0, "true", None)
        for value in invalid_values:
            with self.subTest(value=value):
                with patch(
                    "fast_safety_audit_runtime.BoundedAuditEmitter",
                ) as emitter_cls:
                    runtime = create_fast_safety_audit_runtime(
                        shadow_enabled=value,  # type: ignore[arg-type]
                    )

                emitter_cls.assert_not_called()
                self.assertFalse(runtime.shadow_enabled)
                self.assertFalse(runtime.ready)
                self.assertIsNone(runtime.emitter)
                self.assertEqual(runtime.reason, "invalid-shadow-enabled")

    def test_shadow_on_creates_emitter_once(self) -> None:
        with patch(
            "fast_safety_audit_runtime.BoundedAuditEmitter",
            wraps=BoundedAuditEmitter,
        ) as emitter_cls:
            runtime = create_fast_safety_audit_runtime(shadow_enabled=True)

        emitter_cls.assert_called_once()
        self.assertTrue(runtime.shadow_enabled)
        self.assertTrue(runtime.ready)
        self.assertIsNotNone(runtime.emitter)
        self.assertEqual(runtime.reason, "runtime-ready")

    def test_maxsize_passed_to_emitter_constructor(self) -> None:
        for maxsize in (1, 8, 1024):
            with self.subTest(maxsize=maxsize):
                with patch(
                    "fast_safety_audit_runtime.BoundedAuditEmitter",
                ) as emitter_cls:
                    emitter_cls.return_value = MagicMock(
                        spec=BoundedAuditEmitter
                    )
                    create_fast_safety_audit_runtime(
                        shadow_enabled=True,
                        maxsize=maxsize,
                    )

                emitter_cls.assert_called_once_with(maxsize=maxsize)

    def test_emitter_creation_exception_isolated(self) -> None:
        with patch(
            "fast_safety_audit_runtime.BoundedAuditEmitter",
            side_effect=RuntimeError("constructor failed"),
        ):
            runtime = create_fast_safety_audit_runtime(shadow_enabled=True)

        self.assertTrue(runtime.shadow_enabled)
        self.assertFalse(runtime.ready)
        self.assertIsNone(runtime.emitter)
        self.assertEqual(runtime.reason, "runtime-error")

    def test_runtime_instances_do_not_share_queue(self) -> None:
        first = create_fast_safety_audit_runtime(shadow_enabled=True)
        second = create_fast_safety_audit_runtime(shadow_enabled=True)

        assert first.emitter is not None
        assert second.emitter is not None
        self.assertIsNot(first.emitter, second.emitter)

        self.assertTrue(first.emitter.try_emit({"event": "first"}))
        self.assertEqual(first.emitter.qsize(), 1)
        self.assertEqual(second.emitter.qsize(), 0)

    def test_empty_queue_drain(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        sink = MagicMock(return_value=True)

        result = drain_fast_safety_audit_runtime(runtime, sink)

        sink.assert_not_called()
        self.assertEqual(
            result,
            FastSafetyAuditDrainResult(
                attempted=True,
                drained_count=0,
                delivered_count=0,
                failed_count=0,
                remaining_count=0,
                reason="drain-empty",
            ),
        )

    def test_fifo_drain_success(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None

        events = (
            {"index": 0},
            {"index": 1},
            {"index": 2},
        )
        for event in events:
            self.assertTrue(runtime.emitter.try_emit(event))

        received: list[dict[str, int]] = []

        def sink(event):
            received.append(dict(event))
            return True

        result = drain_fast_safety_audit_runtime(runtime, sink)

        self.assertEqual(received, [{"index": 0}, {"index": 1}, {"index": 2}])
        self.assertEqual(result.attempted, True)
        self.assertEqual(result.drained_count, 3)
        self.assertEqual(result.delivered_count, 3)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(result.remaining_count, 0)
        self.assertEqual(result.reason, "drain-complete")

    def test_drain_limit(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None

        for index in range(3):
            self.assertTrue(runtime.emitter.try_emit({"index": index}))

        received: list[dict[str, int]] = []

        def sink(event):
            received.append(dict(event))
            return True

        result = drain_fast_safety_audit_runtime(runtime, sink, limit=2)

        self.assertEqual(received, [{"index": 0}, {"index": 1}])
        self.assertEqual(result.drained_count, 2)
        self.assertEqual(result.delivered_count, 2)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(result.remaining_count, 1)

    def test_queue_full_bounded_behavior_preserved(self) -> None:
        runtime = create_fast_safety_audit_runtime(
            shadow_enabled=True,
            maxsize=1,
        )
        assert runtime.emitter is not None

        self.assertTrue(runtime.emitter.try_emit({"event": "first"}))
        self.assertFalse(runtime.emitter.try_emit({"event": "second"}))
        self.assertEqual(runtime.emitter.qsize(), 1)

        received: list[dict[str, str]] = []

        def sink(event):
            received.append(dict(event))
            return True

        result = drain_fast_safety_audit_runtime(runtime, sink)

        self.assertEqual(received, [{"event": "first"}])
        self.assertEqual(result.drained_count, 1)
        self.assertEqual(result.delivered_count, 1)
        self.assertEqual(result.remaining_count, 0)

    def test_sink_false_is_failure_without_retry_or_requeue(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None
        self.assertTrue(runtime.emitter.try_emit({"event": "only"}))

        sink = MagicMock(return_value=False)
        result = drain_fast_safety_audit_runtime(runtime, sink)

        sink.assert_called_once()
        self.assertEqual(result.drained_count, 1)
        self.assertEqual(result.delivered_count, 0)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.remaining_count, 0)
        self.assertEqual(result.reason, "drain-partial")

    def test_sink_exception_isolated_and_next_event_continues(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None

        self.assertTrue(runtime.emitter.try_emit({"index": 0}))
        self.assertTrue(runtime.emitter.try_emit({"index": 1}))

        sink = MagicMock(side_effect=[RuntimeError("sink failed"), True])
        result = drain_fast_safety_audit_runtime(runtime, sink)

        self.assertEqual(sink.call_count, 2)
        self.assertEqual(result.drained_count, 2)
        self.assertEqual(result.delivered_count, 1)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.remaining_count, 0)
        self.assertEqual(result.reason, "drain-partial")

    def test_partial_failure_counts_and_fifo(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None

        for index in range(3):
            self.assertTrue(runtime.emitter.try_emit({"index": index}))

        sink = MagicMock(side_effect=[True, False, True])
        result = drain_fast_safety_audit_runtime(runtime, sink)

        self.assertEqual(sink.call_count, 3)
        self.assertEqual(result.drained_count, 3)
        self.assertEqual(result.delivered_count, 2)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.remaining_count, 0)
        self.assertEqual(result.reason, "drain-partial")

    def test_invalid_sink_and_limit_leave_queue_unchanged(self) -> None:
        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None
        self.assertTrue(runtime.emitter.try_emit({"event": "kept"}))

        cases = (
            ("sink_none", None, None),
            ("sink_non_callable", 123, None),
            ("limit_true", MagicMock(return_value=True), True),
            ("limit_false", MagicMock(return_value=True), False),
            ("limit_negative", MagicMock(return_value=True), -1),
            ("limit_float", MagicMock(return_value=True), 1.5),
            ("limit_string", MagicMock(return_value=True), "1"),
        )

        for label, sink, limit in cases:
            with self.subTest(label=label):
                before_qsize = runtime.emitter.qsize()
                result = drain_fast_safety_audit_runtime(
                    runtime,
                    sink,  # type: ignore[arg-type]
                    limit=limit,  # type: ignore[arg-type]
                )

                self.assertFalse(result.attempted)
                self.assertIn(
                    result.reason,
                    ("invalid-sink", "invalid-limit"),
                )
                self.assertEqual(runtime.emitter.qsize(), before_qsize)
                if callable(sink):
                    sink.assert_not_called()

    def test_invalid_runtime_and_drain_exception_isolation(self) -> None:
        off_runtime = create_fast_safety_audit_runtime(shadow_enabled=False)
        result_none = drain_fast_safety_audit_runtime(
            None,  # type: ignore[arg-type]
            lambda event: True,
        )
        self.assertFalse(result_none.attempted)
        self.assertEqual(result_none.reason, "invalid-runtime")

        result_off = drain_fast_safety_audit_runtime(
            off_runtime,
            lambda event: True,
        )
        self.assertFalse(result_off.attempted)
        self.assertEqual(result_off.reason, "runtime-not-ready")

        runtime = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime.emitter is not None
        self.assertTrue(runtime.emitter.try_emit({"event": "kept"}))

        with patch.object(
            runtime.emitter,
            "drain",
            side_effect=RuntimeError("drain failed"),
        ):
            result_drain_error = drain_fast_safety_audit_runtime(
                runtime,
                lambda event: True,
            )

        self.assertTrue(result_drain_error.attempted)
        self.assertEqual(result_drain_error.reason, "drain-error")
        self.assertEqual(result_drain_error.drained_count, 0)

        runtime_qsize = create_fast_safety_audit_runtime(shadow_enabled=True)
        assert runtime_qsize.emitter is not None
        self.assertTrue(runtime_qsize.emitter.try_emit({"event": "one"}))

        with patch.object(
            runtime_qsize.emitter,
            "drain",
            return_value=({"event": "one"},),
        ), patch.object(
            runtime_qsize.emitter,
            "qsize",
            side_effect=RuntimeError("qsize failed"),
        ):
            result_qsize_error = drain_fast_safety_audit_runtime(
                runtime_qsize,
                lambda event: True,
            )

        self.assertTrue(result_qsize_error.attempted)
        self.assertEqual(result_qsize_error.reason, "drain-error")
        self.assertEqual(result_qsize_error.drained_count, 1)
        self.assertEqual(result_qsize_error.delivered_count, 1)
        self.assertEqual(result_qsize_error.failed_count, 0)


if __name__ == "__main__":
    unittest.main()
