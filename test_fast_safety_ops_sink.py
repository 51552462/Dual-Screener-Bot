"""Tests for Fast Safety Ops Telemetry Sink Adapter (Chapter 3-B0D3A4E)."""

from __future__ import annotations

import copy
import unittest
from dataclasses import FrozenInstanceError
from types import MappingProxyType

from fast_safety_ops_sink import (
    FastSafetyOpsEnvelope,
    build_fast_safety_ops_envelope,
    create_fast_safety_ops_sink,
)


def _sample_audit_event(**overrides: object) -> dict[str, object]:
    """Fixture aligned with fast_safety_kernel.build_audit_event schema."""
    base: dict[str, object] = {
        "event_type": "fast_safety_kelly_decision",
        "severity": "NORMAL",
        "market": "US",
        "strategy_id": "s1",
        "base_kelly": 0.10,
        "alpha_overlay": 1.20,
        "selected_family": "drawdown",
        "selected_risk": "dd_hard",
        "risk_multiplier": 0.50,
        "uncapped_kelly": 0.06,
        "effective_cap": 0.04,
        "cap_source": "sector_remaining",
        "final_kelly": 0.04,
        "blocked": False,
        "reason": "",
        "policy_version": "policy-v1",
        "live_risk_version": "live-v1",
    }
    base.update(overrides)
    return base


class _RecordingWriter:
    def __init__(
        self,
        *,
        result: object = True,
        raises: bool = False,
    ) -> None:
        self.result = result
        self.raises = raises
        self.calls: list[dict[str, object]] = []

    def __call__(
        self,
        *,
        component: str,
        severity: str,
        event: str,
        payload: object,
    ) -> object:
        self.calls.append(
            {
                "component": component,
                "severity": severity,
                "event": event,
                "payload": payload,
            }
        )
        if self.raises:
            raise RuntimeError("writer failed")
        return self.result


class FastSafetyOpsSinkTests(unittest.TestCase):
    def test_normal_envelope_creation(self) -> None:
        event = _sample_audit_event()
        envelope = build_fast_safety_ops_envelope(event)

        self.assertIsNotNone(envelope)
        assert envelope is not None
        self.assertEqual(envelope.component, "fast_safety")
        self.assertEqual(envelope.severity, "NORMAL")
        self.assertEqual(envelope.event, "fast_safety_kelly_decision")
        self.assertEqual(envelope.payload["market"], "US")
        self.assertEqual(envelope.payload["strategy_id"], "s1")
        self.assertAlmostEqual(envelope.payload["base_kelly"], 0.10)
        self.assertAlmostEqual(envelope.payload["alpha_overlay"], 1.20)
        self.assertEqual(envelope.payload["selected_family"], "drawdown")
        self.assertEqual(envelope.payload["selected_risk"], "dd_hard")
        self.assertAlmostEqual(envelope.payload["risk_multiplier"], 0.50)
        self.assertAlmostEqual(envelope.payload["uncapped_kelly"], 0.06)
        self.assertAlmostEqual(envelope.payload["effective_cap"], 0.04)
        self.assertEqual(envelope.payload["cap_source"], "sector_remaining")
        self.assertAlmostEqual(envelope.payload["final_kelly"], 0.04)
        self.assertFalse(envelope.payload["blocked"])
        self.assertEqual(envelope.payload["reason"], "")
        self.assertEqual(envelope.payload["policy_version"], "policy-v1")
        self.assertEqual(envelope.payload["live_risk_version"], "live-v1")

    def test_event_type_and_severity_not_in_payload(self) -> None:
        envelope = build_fast_safety_ops_envelope(_sample_audit_event())

        self.assertIsNotNone(envelope)
        assert envelope is not None
        self.assertNotIn("event_type", envelope.payload)
        self.assertNotIn("severity", envelope.payload)

    def test_unknown_and_sensitive_fields_removed(self) -> None:
        event = _sample_audit_event(
            db_path="/secret/db.sqlite",
            traceback="Traceback (most recent call last): ...",
            symbol="AAPL",
            account_id="acct-123",
            custom_secret="top-secret",
        )
        envelope = build_fast_safety_ops_envelope(event)

        self.assertIsNotNone(envelope)
        assert envelope is not None
        for key in (
            "db_path",
            "traceback",
            "symbol",
            "account_id",
            "custom_secret",
        ):
            self.assertNotIn(key, envelope.payload)

    def test_input_mapping_immutable(self) -> None:
        event = _sample_audit_event(
            nested={"inner": [1, {"k": "v"}]},
        )
        before = copy.deepcopy(event)
        build_fast_safety_ops_envelope(event)
        self.assertEqual(event, before)

    def test_envelope_and_payload_immutability(self) -> None:
        envelope = build_fast_safety_ops_envelope(_sample_audit_event())
        self.assertIsNotNone(envelope)
        assert envelope is not None

        with self.assertRaises(FrozenInstanceError):
            envelope.component = "other"  # type: ignore[misc]

        with self.assertRaises(TypeError):
            envelope.payload["market"] = "KR"  # type: ignore[index]

        writer = _RecordingWriter()
        sink = create_fast_safety_ops_sink(writer)
        self.assertIsNotNone(sink)
        assert sink is not None
        sink(_sample_audit_event())
        self.assertEqual(len(writer.calls), 1)
        payload = writer.calls[0]["payload"]
        assert isinstance(payload, dict)
        payload["market"] = "KR"
        self.assertEqual(envelope.payload["market"], "US")

    def test_non_mapping_input_rejected(self) -> None:
        for value in (None, [], (), "text", 42, object()):
            with self.subTest(value=type(value).__name__):
                self.assertIsNone(build_fast_safety_ops_envelope(value))

    def test_event_type_validation(self) -> None:
        cases = (
            ("None", _sample_audit_event(event_type=None)),
            ("int", _sample_audit_event(event_type=1)),
            ("empty", _sample_audit_event(event_type="")),
            ("whitespace", _sample_audit_event(event_type="   ")),
        )
        for label, event in cases:
            with self.subTest(label=label):
                self.assertIsNone(build_fast_safety_ops_envelope(event))

        with self.subTest(label="missing"):
            event = _sample_audit_event()
            del event["event_type"]
            self.assertIsNone(build_fast_safety_ops_envelope(event))

    def test_severity_validation(self) -> None:
        cases = (
            ("missing", _sample_audit_event(severity="x")),
            ("None", _sample_audit_event(severity=None)),
            ("int", _sample_audit_event(severity=1)),
            ("empty", _sample_audit_event(severity="")),
            ("whitespace", _sample_audit_event(severity="   ")),
        )
        for label, event in cases:
            with self.subTest(label=label):
                if label == "missing":
                    del event["severity"]
                self.assertIsNone(build_fast_safety_ops_envelope(event))

    def test_safe_scalars_allowed(self) -> None:
        scalar_cases = (
            ("none", None),
            ("str", "value"),
            ("bool", True),
            ("int", 7),
            ("float", 0.25),
        )
        for label, scalar in scalar_cases:
            with self.subTest(label=label):
                event = _sample_audit_event(market=scalar)
                envelope = build_fast_safety_ops_envelope(event)
                self.assertIsNotNone(envelope)
                assert envelope is not None
                self.assertEqual(envelope.payload["market"], scalar)

    def test_unsafe_values_rejected(self) -> None:
        class _Custom:
            pass

        unsafe_values = (
            float("nan"),
            float("inf"),
            float("-inf"),
            {"nested": True},
            [1, 2],
            (1, 2),
            {1, 2},
            b"bytes",
            _Custom(),
        )
        for value in unsafe_values:
            with self.subTest(value=repr(value)):
                event = _sample_audit_event(market=value)
                self.assertIsNone(build_fast_safety_ops_envelope(event))

    def test_valid_writer_sink_creation(self) -> None:
        writer = _RecordingWriter()
        sink = create_fast_safety_ops_sink(writer)

        self.assertIsNotNone(sink)
        self.assertTrue(callable(sink))
        self.assertEqual(len(writer.calls), 0)

    def test_writer_call_keyword_contract(self) -> None:
        writer = _RecordingWriter()
        sink = create_fast_safety_ops_sink(writer)
        self.assertIsNotNone(sink)
        assert sink is not None

        event = _sample_audit_event(
            db_path="/secret",
            symbol="AAPL",
        )
        self.assertTrue(sink(event))

        self.assertEqual(len(writer.calls), 1)
        call = writer.calls[0]
        self.assertEqual(call["component"], "fast_safety")
        self.assertEqual(call["severity"], "NORMAL")
        self.assertEqual(call["event"], "fast_safety_kelly_decision")
        payload = call["payload"]
        self.assertIsInstance(payload, dict)
        self.assertNotIsInstance(payload, MappingProxyType)
        self.assertNotIn("event_type", payload)
        self.assertNotIn("db_path", payload)
        self.assertNotIn("symbol", payload)
        self.assertEqual(set(payload.keys()), set(event.keys()) - {
            "event_type",
            "severity",
            "db_path",
            "symbol",
        })

    def test_writer_success_is_exact_true(self) -> None:
        returns = (True, False, None, 0, 1, "true")
        for result in returns:
            with self.subTest(result=repr(result)):
                writer = _RecordingWriter(result=result)
                sink = create_fast_safety_ops_sink(writer)
                self.assertIsNotNone(sink)
                assert sink is not None
                self.assertEqual(sink(_sample_audit_event()), result is True)

    def test_invalid_writer_invalid_event_writer_exception_isolation(self) -> None:
        with self.subTest(case="writer_none"):
            self.assertIsNone(create_fast_safety_ops_sink(None))

        with self.subTest(case="writer_non_callable"):
            self.assertIsNone(create_fast_safety_ops_sink("not-callable"))

        writer = _RecordingWriter(raises=True)
        sink = create_fast_safety_ops_sink(writer)
        self.assertIsNotNone(sink)
        assert sink is not None

        with self.subTest(case="invalid_audit_event"):
            self.assertFalse(sink("not-a-mapping"))  # type: ignore[arg-type]

        with self.subTest(case="writer_raises"):
            self.assertFalse(sink(_sample_audit_event()))
            payload = writer.calls[-1]["payload"]
            self.assertIsInstance(payload, dict)
            self.assertNotIn("exception", payload)
            self.assertNotIn("traceback", payload)
            self.assertNotIn("error_message", payload)


if __name__ == "__main__":
    unittest.main()
