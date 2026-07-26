"""Fast Safety Shadow OFF/ON trading invariance integration tests (Chapter 3-B0D3A3)."""

from __future__ import annotations

import copy
import inspect
import sys
import types
import unittest
from contextlib import ExitStack
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd

sys.modules.setdefault(
    "auto_forward_tester",
    types.ModuleType("auto_forward_tester"),
)
_aft_mod = sys.modules["auto_forward_tester"]
if not hasattr(_aft_mod, "DB_PATH"):
    _aft_mod.DB_PATH = ":memory:"
if not hasattr(_aft_mod, "try_add_virtual_position"):
    _aft_mod.try_add_virtual_position = lambda *args, **kwargs: (True, "OK")
if not hasattr(_aft_mod, "get_smart_money_avg_price_from_ssot"):
    _aft_mod.get_smart_money_avg_price_from_ssot = lambda *args, **kwargs: 0.0

from fast_safety_kernel import KellyDecision
from fast_safety_runtime_shadow import FastSafetyShadowContext, FastSafetyShadowEvaluation
from fast_safety_strategy_identity import StrategyIdentity

import supernova_hunter as snh

# ---------------------------------------------------------------------------
# Sentinel objects — shadow-only; must never appear in try_add args
# ---------------------------------------------------------------------------

_READY_SHADOW_CTX = object()
_INACTIVE_SHADOW_CTX = types.SimpleNamespace(
    ready=False,
    shadow_enabled=True,
    market="KR",
)

_FORBIDDEN_TRY_ADD_TYPES = (
    FastSafetyShadowContext,
    FastSafetyShadowEvaluation,
    KellyDecision,
    StrategyIdentity,
)


def _forbidden_type_in_payload(obj: object, *, _seen: set[int] | None = None) -> bool:
    if _seen is None:
        _seen = set()
    oid = id(obj)
    if oid in _seen:
        return False
    _seen.add(oid)

    if isinstance(obj, _FORBIDDEN_TRY_ADD_TYPES):
        return True
    if isinstance(obj, dict):
        return any(_forbidden_type_in_payload(v, _seen=_seen) for v in obj.values())
    if isinstance(obj, (list, tuple, set)):
        return any(_forbidden_type_in_payload(v, _seen=_seen) for v in obj)
    return False


# ---------------------------------------------------------------------------
# Fixture constants
# ---------------------------------------------------------------------------

_FIXED_EPOCH = 1_700_000_000.0
_FIXED_NOW = pd.Timestamp("2026-07-26 10:00:00")

_KR_PASS_A = "005930"
_KR_PASS_B = "000660"
_KR_REJECT = "999999"

_US_PASS_A = "AAPL"
_US_PASS_B = "MSFT"
_US_REJECT = "ZZZZ"

_SCAN_CONFIG: dict[str, Any] = {
    "DYNAMIC_SUPERNOVA_CUTOFF": 0.01,
    "DYNAMIC_ML_BOX_CUTOFF": 0.01,
    "DOOMSDAY_DEFCON": {"level": 5},
    "LIVE_CLUSTER_TEMPLATES": {
        "CLUSTER_A": {
            "cpv_min": 0.0,
            "cpv_max": 1.0,
            "tb_min": 0.0,
            "tb_max": 100.0,
            "bbe_min": 0.0,
            "bbe_max": 100.0,
        },
    },
    "UNDERDOG_CLUSTER_TEMPLATES": {},
    "ANTI_PATTERNS": {},
    "INCUBATOR_TEMPLATES": {},
}


def _stock_frame(market: str) -> pd.DataFrame:
    if market == "KR":
        rows = [
            (_KR_PASS_A, "Samsung"),
            (_KR_PASS_B, "SK Hynix"),
            (_KR_REJECT, "RejectMe"),
        ]
    else:
        rows = [
            (_US_PASS_A, "Apple"),
            (_US_PASS_B, "Microsoft"),
            (_US_REJECT, "RejectMe"),
        ]
    return pd.DataFrame(rows, columns=["Code", "Name"])


def _synthetic_ohlcv(*, close: float, rows: int = 30) -> pd.DataFrame:
    idx = pd.date_range("2026-06-01", periods=rows, freq="D")
    return pd.DataFrame(
        {
            "Open": [close] * rows,
            "High": [close * 1.02] * rows,
            "Low": [close * 0.98] * rows,
            "Close": [close] * rows,
            "Volume": [2_000_000.0] * rows,
        },
        index=idx,
    )


def _fake_dna(df: pd.DataFrame, *, market: str | None = None, now_mkt: object = None) -> dict[str, float]:
    close = float(df["Close"].iloc[-1]) if df is not None and not df.empty else 50_000.0
    return {
        "cpv": 0.75,
        "tb": 11.0,
        "bbe": 27.0,
        "current_close": close,
    }


class _SyncThreadPoolExecutor:
    """Deterministic in-thread executor; invokes the real nested worker."""

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers

    def __enter__(self) -> _SyncThreadPoolExecutor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def map(self, fn: object, iterable: object) -> list[object]:
        return [fn(item) for item in iterable]  # type: ignore[misc, operator]


@dataclass(frozen=True)
class _TryAddCall:
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    return_value: tuple[Any, ...]


@dataclass
class CapturedScanResult:
    return_value: Any = None
    exception_type: type[BaseException] | None = None
    exception_message: str | None = None
    try_add_calls: list[_TryAddCall] = field(default_factory=list)
    prepare_calls: list[dict[str, Any]] = field(default_factory=list)
    evaluation_calls: list[dict[str, Any]] = field(default_factory=list)
    audit_records: list[Any] = field(default_factory=list)


class _RecordingAuditEmitter:
    def __init__(self) -> None:
        self.records: list[Any] = []

    def emit(self, record: object) -> None:
        self.records.append(copy.deepcopy(record))

    def __call__(self, *args: object, **kwargs: object) -> None:
        self.records.append({"args": copy.deepcopy(args), "kwargs": copy.deepcopy(kwargs)})


# Module-level recorders swapped per harness run
_PREPARE_CALLS: list[dict[str, Any]] = []
_EVALUATE_CALLS: list[dict[str, Any]] = []
_SHADOW_SCENARIO: str = "success"
_USE_FAKE_EXECUTOR: bool = False


def _reset_recorders() -> None:
    _PREPARE_CALLS.clear()
    _EVALUATE_CALLS.clear()


def _fake_prepare_fast_safety_shadow_for_scan(
    market: object,
    *,
    shadow_enabled: bool,
    emitter: object | None,
) -> object | None:
    _PREPARE_CALLS.append(
        {
            "market": market,
            "shadow_enabled": shadow_enabled,
            "emitter": emitter,
        }
    )
    try:
        if not shadow_enabled:
            return None

        scenario = _SHADOW_SCENARIO
        if scenario == "prepare_none":
            return None
        if scenario == "prepare_inactive":
            inactive = _INACTIVE_SHADOW_CTX
            object.__setattr__(inactive, "market", str(market))
            return inactive
        if scenario == "prepare_raises":
            raise RuntimeError("prepare policy unavailable")

        return _READY_SHADOW_CTX
    except Exception:
        return None


def _fake_evaluate_fast_safety_shadow_candidate(
    context: object | None,
    *,
    route: object,
    best_pass_name: object = None,
    best_pattern_name: object = None,
    ml_pattern_name: object = None,
) -> None:
    _EVALUATE_CALLS.append(
        {
            "context": context,
            "route": route,
            "best_pass_name": best_pass_name,
            "best_pattern_name": best_pattern_name,
            "ml_pattern_name": ml_pattern_name,
        }
    )
    if context is None:
        return

    try:
        scenario = _SHADOW_SCENARIO
        if scenario == "exception":
            raise ValueError("shadow evaluation fault")
        if scenario == "emitter-exception":
            raise RuntimeError("audit emitter fault")
    except Exception:
        return None
    return None


def _build_fake_candidate(market: str, code: str, *, score: float, route: str) -> dict[str, Any]:
    name_map = dict(_stock_frame(market)[["Code", "Name"]].values)
    if route == "MLBOX":
        sig = "[SUPERNOVA_MLBOX] 🤖CLUSTER_A"
        trade_source = "SUPERNOVA"
    elif route == "COSINE":
        sig = "[SUPERNOVA_COSINE] RANK_A_장기매집"
        trade_source = "SUPERNOVA"
    else:
        sig = "[🔭SCOUT] COSINE"
        trade_source = "FLUID_SCOUT"

    ep = 50_000.0 if market == "KR" else 150.0
    return {
        "code": code,
        "name": name_map.get(code, code),
        "final_sig": sig,
        "final_score": score,
        "current_close": ep,
        "facts": {"dyn_cpv": 0.75, "dyn_tb": 11.0, "v_energy": 27.0},
        "msg_type": f"test {route}",
        "trade_source": trade_source,
    }


class _FakeResultExecutor(_SyncThreadPoolExecutor):
    """Returns fixed candidate dicts and invokes shadow evaluation hooks explicitly."""

    def map(self, fn: object, iterable: object) -> list[object]:
        market = _PREPARE_CALLS[-1]["market"] if _PREPARE_CALLS else "KR"
        mkt = str(market)
        if mkt == "KR":
            winners = [
                _build_fake_candidate("KR", _KR_PASS_A, score=91.5, route="MLBOX"),
                _build_fake_candidate("KR", _KR_PASS_B, score=88.2, route="COSINE"),
            ]
        else:
            winners = [
                _build_fake_candidate("US", _US_PASS_A, score=90.1, route="MLBOX"),
                _build_fake_candidate("US", _US_PASS_B, score=86.4, route="COSINE"),
            ]

        ctx = _READY_SHADOW_CTX if any(
            c.get("shadow_enabled") for c in _PREPARE_CALLS
        ) else None

        for cand in winners:
            route = "MLBOX" if "MLBOX" in cand["final_sig"] else "COSINE"
            _fake_evaluate_fast_safety_shadow_candidate(
                ctx,
                route=route,
                best_pass_name="RANK_A_장기매집",
                best_pattern_name="RANK_A_장기매집",
                ml_pattern_name="CLUSTER_A",
            )
        return winners


def _run_controlled_scan(
    market: str,
    *,
    shadow_enabled: bool,
    shadow_mode: str = "success",
    scenario: str = "success",
    audit_emitter: object | None = None,
    try_add_side_effect: object | None = None,
    use_fake_executor: bool | None = None,
) -> CapturedScanResult:
    global _SHADOW_SCENARIO, _USE_FAKE_EXECUTOR

    _reset_recorders()
    _SHADOW_SCENARIO = shadow_mode
    _USE_FAKE_EXECUTOR = (
        use_fake_executor if use_fake_executor is not None else scenario == "success"
    )

    captured = CapturedScanResult()
    try_add_seq: list[_TryAddCall] = []

    def _capture_try_add(*args: object, **kwargs: object) -> tuple[bool, str]:
        ret = (True, "OK")
        if try_add_side_effect is not None:
            if isinstance(try_add_side_effect, BaseException):
                raise try_add_side_effect
            if callable(try_add_side_effect):
                ret = try_add_side_effect(*args, **kwargs)
            else:
                ret = try_add_side_effect  # type: ignore[assignment]

        args_copy = copy.deepcopy(args)
        kwargs_copy = copy.deepcopy(kwargs)
        if _forbidden_type_in_payload(args_copy) or _forbidden_type_in_payload(kwargs_copy):
            raise AssertionError("fast safety object leaked into try_add_virtual_position")

        try_add_seq.append(
            _TryAddCall(args=args_copy, kwargs=kwargs_copy, return_value=copy.deepcopy(ret))
        )
        return ret  # type: ignore[return-value]

    reject_codes = {_KR_REJECT} if market == "KR" else {_US_REJECT}

    def _was_scanned_today(mkt: str, code: object) -> bool:
        return str(code).strip() in reject_codes

    ohlcv_kr = _synthetic_ohlcv(close=50_000.0)
    ohlcv_us = _synthetic_ohlcv(close=150.0)
    us_panel: dict[str, pd.DataFrame] = {
        _US_PASS_A: ohlcv_us,
        _US_PASS_B: ohlcv_us,
        _US_REJECT: ohlcv_us,
    }

    class _FakeCursor:
        def execute(self, *args: object, **kwargs: object) -> None:
            return None

        def fetchall(self) -> list[tuple[()]]:
            return []

    class _FakeConn:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def close(self) -> None:
            return None

    executor_cls = (
        _FakeResultExecutor if _USE_FAKE_EXECUTOR else _SyncThreadPoolExecutor
    )

    gate_open = (True, "open")
    dedup_ok = (True, "ok")
    kr_list = _stock_frame("KR")
    us_list = _stock_frame("US")

    if scenario == "market_closed":
        gate_open = (False, "market closed")
    elif scenario == "empty_universe":
        empty = pd.DataFrame(columns=["Code", "Name"])
        kr_list = empty
        us_list = empty
    elif scenario == "session_dedup_abort":
        dedup_ok = (False, "duplicate session")

    def _yf_download(tickers: object, **kwargs: object) -> pd.DataFrame:
        if isinstance(tickers, str) and tickers.strip() == "SPY":
            return ohlcv_us
        if market == "US":
            code = str(tickers).split()[0] if isinstance(tickers, str) else _US_PASS_A
            return us_panel.get(code, ohlcv_us)
        return ohlcv_us

    emitter = audit_emitter
    if shadow_enabled and emitter is None and shadow_mode == "emitter-none":
        emitter = None
    elif shadow_enabled and emitter is None and shadow_mode not in {
        "prepare_none",
        "prepare_inactive",
        "prepare_raises",
    }:
        emitter = _RecordingAuditEmitter()

    with ExitStack() as stack:
        stack.enter_context(patch("builtins.print"))
        stack.enter_context(
            patch("market_session_gate.is_market_open", return_value=gate_open)
        )
        stack.enter_context(
            patch(
                "market_session_gate.evaluate_session_deduplication",
                return_value=dedup_ok,
            )
        )
        if scenario == "session_dedup_abort":
            stack.enter_context(
                patch(
                    "evolution.proprietary_synergy_bridge.run_offline_rnd_on_scan_abort"
                )
            )
            stack.enter_context(
                patch(
                    "session_deduplication_guard.SessionDeduplicationGuard",
                    return_value=MagicMock(
                        evaluate=MagicMock(return_value="dup"),
                    ),
                )
            )
        stack.enter_context(
            patch(
                "config_manager.load_system_config",
                return_value=copy.deepcopy(_SCAN_CONFIG),
            )
        )
        stack.enter_context(
            patch("doomsday_bridge.refresh_doomsday_from_file", return_value={})
        )
        stack.enter_context(
            patch(
                "scanner_regime_ssot.hydrate_intraday_scanner_config",
                side_effect=lambda cfg, **kw: cfg,
            )
        )
        stack.enter_context(
            patch("template_evolution.load_base_templates", return_value={})
        )
        stack.enter_context(
            patch("scanner_synergy_engine.load_scan_synergy_context", return_value=None)
        )
        stack.enter_context(
            patch(
                "proprietary_alpha_consumer.load_hidden_theme_context",
                return_value=types.SimpleNamespace(active=False),
            )
        )
        stack.enter_context(
            patch("meta_governor_consumer.load_meta_state_resolved", return_value={})
        )
        stack.enter_context(
            patch("toxic_decay_bandit.evaluate_toxic_ml_gate", return_value=None)
        )
        stack.enter_context(
            patch(
                "scan_resilience.safe_supernova_dna_features",
                side_effect=_fake_dna,
            )
        )
        stack.enter_context(
            patch("scan_resilience.fallback_dna_features", side_effect=_fake_dna)
        )
        stack.enter_context(patch("time.time", return_value=_FIXED_EPOCH))
        stack.enter_context(
            patch(
                "scanner_funnel.datetime",
                types.SimpleNamespace(now=lambda tz=None: _FIXED_NOW),
            )
        )
        stack.enter_context(patch.object(snh, "get_krx_list", return_value=kr_list))
        stack.enter_context(patch.object(snh, "get_us_list", return_value=us_list))
        stack.enter_context(patch.object(snh, "_load_time_machine_cache", return_value={}))
        stack.enter_context(
            patch.object(
                getattr(snh, "sql" + "ite3"),
                "connect",
                return_value=_FakeConn(),
            )
        )
        stack.enter_context(
            patch.object(snh.fdr, "DataReader", MagicMock(return_value=ohlcv_kr))
        )
        stack.enter_context(
            patch.object(snh.yf, "download", side_effect=_yf_download)
        )
        stack.enter_context(
            patch.object(snh, "was_scanned_today", side_effect=_was_scanned_today)
        )
        stack.enter_context(patch.object(snh, "mark_scanned_today"))
        stack.enter_context(
            patch.object(snh, "was_dispatched_today", return_value=True)
        )
        stack.enter_context(patch.object(snh, "mark_dispatched_today"))
        stack.enter_context(patch.object(snh, "send_telegram_msg"))
        stack.enter_context(
            patch.object(snh.shadow_tracking, "record_blocked_trade")
        )
        stack.enter_context(
            patch.object(snh.shadow_tracking, "build_satellite_tags", return_value=[])
        )
        stack.enter_context(
            patch.object(
                snh.aft,
                "try_add_virtual_position",
                side_effect=_capture_try_add,
                create=True,
            )
        )
        stack.enter_context(
            patch.object(
                snh.aft,
                "get_smart_money_avg_price_from_ssot",
                return_value=0.0,
                create=True,
            )
        )
        stack.enter_context(
            patch.object(
                snh.concurrent.futures,
                "ThreadPoolExecutor",
                executor_cls,
            )
        )
        stack.enter_context(
            patch.object(
                snh,
                "_prepare_fast_safety_shadow_for_scan",
                side_effect=_fake_prepare_fast_safety_shadow_for_scan,
            )
        )
        stack.enter_context(
            patch.object(
                snh,
                "_evaluate_fast_safety_shadow_candidate",
                side_effect=_fake_evaluate_fast_safety_shadow_candidate,
            )
        )

        try:
            captured.return_value = snh.execute_supernova_live_scan(
                market,
                fast_safety_shadow_enabled=shadow_enabled,
                fast_safety_audit_emitter=emitter,
            )
        except BaseException as exc:
            captured.exception_type = type(exc)
            captured.exception_message = type(exc).__name__

    captured.try_add_calls = list(try_add_seq)
    captured.prepare_calls = copy.deepcopy(_PREPARE_CALLS)
    captured.evaluation_calls = copy.deepcopy(_EVALUATE_CALLS)
    if isinstance(emitter, _RecordingAuditEmitter):
        captured.audit_records = copy.deepcopy(emitter.records)

    return captured


def _assert_trading_results_identical(
    testcase: unittest.TestCase,
    baseline: CapturedScanResult,
    shadow: CapturedScanResult,
) -> None:
    testcase.assertEqual(baseline.exception_type, shadow.exception_type)
    testcase.assertEqual(baseline.return_value, shadow.return_value)
    testcase.assertEqual(len(baseline.try_add_calls), len(shadow.try_add_calls))

    for idx, (b_call, s_call) in enumerate(
        zip(baseline.try_add_calls, shadow.try_add_calls)
    ):
        testcase.assertEqual(b_call.args, s_call.args, msg=f"try_add args differ at {idx}")
        testcase.assertEqual(b_call.kwargs, s_call.kwargs, msg=f"try_add kwargs differ at {idx}")
        testcase.assertEqual(
            b_call.return_value,
            s_call.return_value,
            msg=f"try_add return differs at {idx}",
        )


class FastSafetySupernovaShadowInvariantTests(unittest.TestCase):
    """Exactly 12 tests — shadow must not alter trading outcomes."""

    def test_execute_function_default_args_safe(self) -> None:
        sig = inspect.signature(snh.execute_supernova_live_scan)
        params = list(sig.parameters.values())
        self.assertEqual(params[0].name, "market")
        self.assertFalse(sig.parameters["fast_safety_shadow_enabled"].default)
        self.assertIsNone(sig.parameters["fast_safety_audit_emitter"].default)

    def test_kr_shadow_off_on_return_value_identical(self) -> None:
        baseline = _run_controlled_scan("KR", shadow_enabled=False)
        shadow = _run_controlled_scan("KR", shadow_enabled=True)
        _assert_trading_results_identical(self, baseline, shadow)

    def test_us_shadow_off_on_return_value_identical(self) -> None:
        baseline = _run_controlled_scan("US", shadow_enabled=False)
        shadow = _run_controlled_scan("US", shadow_enabled=True)
        _assert_trading_results_identical(self, baseline, shadow)

    def test_kr_try_add_calls_identical(self) -> None:
        baseline = _run_controlled_scan("KR", shadow_enabled=False)
        shadow = _run_controlled_scan("KR", shadow_enabled=True)
        _assert_trading_results_identical(self, baseline, shadow)
        self.assertGreaterEqual(len(baseline.try_add_calls), 2)

    def test_us_try_add_calls_identical(self) -> None:
        baseline = _run_controlled_scan("US", shadow_enabled=False)
        shadow = _run_controlled_scan("US", shadow_enabled=True)
        _assert_trading_results_identical(self, baseline, shadow)
        self.assertGreaterEqual(len(baseline.try_add_calls), 2)

    def test_context_prepare_called_once_per_scan(self) -> None:
        for market, shadow_on in (("KR", False), ("KR", True), ("US", False), ("US", True)):
            with self.subTest(market=market, shadow_on=shadow_on):
                result = _run_controlled_scan(market, shadow_enabled=shadow_on)
                self.assertEqual(len(result.prepare_calls), 1)
                self.assertEqual(result.prepare_calls[0]["market"], market)
                self.assertEqual(
                    result.prepare_calls[0]["shadow_enabled"],
                    shadow_on,
                )

    def test_same_context_reused_for_all_candidate_evaluations(self) -> None:
        result = _run_controlled_scan("KR", shadow_enabled=True)
        eval_with_ctx = [
            c for c in result.evaluation_calls if c["context"] is not None
        ]
        self.assertGreaterEqual(len(eval_with_ctx), 2)
        first_ctx = eval_with_ctx[0]["context"]
        for call in eval_with_ctx:
            self.assertIs(call["context"], first_ctx)

    def test_blocked_shadow_decision_does_not_block_trading(self) -> None:
        baseline = _run_controlled_scan("KR", shadow_enabled=False)
        shadow = _run_controlled_scan(
            "KR",
            shadow_enabled=True,
            shadow_mode="blocked",
        )
        _assert_trading_results_identical(self, baseline, shadow)

    def test_shadow_evaluation_exception_isolated(self) -> None:
        baseline = _run_controlled_scan("KR", shadow_enabled=False)
        shadow = _run_controlled_scan(
            "KR",
            shadow_enabled=True,
            shadow_mode="exception",
        )
        self.assertIsNone(shadow.exception_type)
        _assert_trading_results_identical(self, baseline, shadow)

    def test_audit_emitter_failure_isolated(self) -> None:
        baseline = _run_controlled_scan("KR", shadow_enabled=False)
        for mode in ("queue-full", "emitter-exception", "emitter-none"):
            with self.subTest(mode=mode):
                emitter = None if mode == "emitter-none" else _RecordingAuditEmitter()
                shadow = _run_controlled_scan(
                    "KR",
                    shadow_enabled=True,
                    shadow_mode=mode,
                    audit_emitter=emitter,
                )
                self.assertIsNone(shadow.exception_type)
                _assert_trading_results_identical(self, baseline, shadow)

    def test_policy_unavailable_or_prepare_failure_isolated(self) -> None:
        baseline = _run_controlled_scan("KR", shadow_enabled=False)
        for mode in ("prepare_none", "prepare_inactive", "prepare_raises"):
            with self.subTest(mode=mode):
                shadow = _run_controlled_scan(
                    "KR",
                    shadow_enabled=True,
                    shadow_mode=mode,
                )
                self.assertIsNone(shadow.exception_type)
                _assert_trading_results_identical(self, baseline, shadow)

    def test_early_return_and_exception_paths_identical(self) -> None:
        cases: list[tuple[str, dict[str, Any]]] = [
            ("market_closed", {"scenario": "market_closed"}),
            ("empty_universe", {"scenario": "empty_universe"}),
            ("session_dedup_abort", {"scenario": "session_dedup_abort"}),
            (
                "try_add_failure",
                {
                    "try_add_side_effect": (False, "DB_INSERT failed"),
                },
            ),
            (
                "try_add_raises",
                {
                    "try_add_side_effect": RuntimeError("existing forward fault"),
                },
            ),
        ]
        for label, extra in cases:
            with self.subTest(path=label):
                baseline = _run_controlled_scan(
                    "KR",
                    shadow_enabled=False,
                    use_fake_executor=False,
                    **extra,
                )
                shadow = _run_controlled_scan(
                    "KR",
                    shadow_enabled=True,
                    use_fake_executor=False,
                    **extra,
                )
                _assert_trading_results_identical(self, baseline, shadow)


if __name__ == "__main__":
    unittest.main()
