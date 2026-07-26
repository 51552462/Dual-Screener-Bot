"""
[M2] 진화형 볼록 래칫 κ 의 주간 강화학습(RL) 업데이트.

지난주 청산 거래에서
  · whipsaw_rate (조기 청산 비율): 트레일이 너무 빡빡해 일찍 털린 비율
  · giveback_rate (이익 반납 비율): 고점 대비 이익을 과도하게 반납한 비율
을 측정하여 exit_dynamics.update_ratchet_kappa_rl 로 κ 곡선을 자가 진화시킨다.

Whipsaw vs Giveback 은 상호 배타(Mutually Exclusive): whipsaw 우선, 아니면 giveback.
"""
from __future__ import annotations

import copy
import logging
import math
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Mapping, Optional

import exit_dynamics as xd

logger = logging.getLogger(__name__)

try:
    from exit_dynamics import update_ratchet_kappa_rl as _UPDATE_RATCHET_KAPPA_RL
except ImportError:
    _UPDATE_RATCHET_KAPPA_RL = None


MFE_THRESHOLD_PCT = 5.0
WHIPSAW_FINAL_RET_THRESHOLD_PCT = 1.0
GIVEBACK_RETAINED_RATIO = 0.50

_CLOSED_STATUSES = frozenset(
    {
        "CLOSED",
        "EXITED",
        "SOLD",
        "DONE",
        "FILLED_EXIT",
        "CLOSE",
    }
)


def _finite_float(value: object) -> Optional[float]:
    """값을 유한 float로 변환한다. 실패하거나 NaN/Inf이면 None."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _first_finite(
    trade: Mapping[str, Any],
    keys: tuple[str, ...],
) -> Optional[float]:
    """여러 호환 키 중 처음 발견되는 유한 실수를 반환한다."""
    for key in keys:
        if key not in trade:
            continue
        value = _finite_float(trade.get(key))
        if value is not None:
            return value
    return None


def _is_closed_trade(trade: Mapping[str, Any]) -> bool:
    """
    거래가 청산 상태인지 판별한다.

    우선순위
    --------
    1. status가 있으면 CLOSED*, EXITED, SOLD 등의 명시적 상태를 사용한다.
    2. status가 없으면 final_ret 계열 값의 존재를 청산 완료의 폴백으로 본다.

    명시적으로 OPEN인 거래는 final_ret가 있더라도 제외한다.
    """
    raw_status = trade.get("status")

    if raw_status is not None:
        status = str(raw_status).strip().upper()
        if status.startswith("OPEN"):
            return False
        if status.startswith("CLOSED") or status in _CLOSED_STATUSES:
            return True
        return False

    return any(
        key in trade and trade.get(key) is not None
        for key in ("final_ret", "realized_return", "return_pct", "pnl_pct")
    )


def _resolve_update_function() -> Callable[..., dict[str, Any]]:
    """exit_dynamics.update_ratchet_kappa_rl 의존성을 지연 확인한다."""
    if _UPDATE_RATCHET_KAPPA_RL is None:
        raise ImportError(
            "exit_dynamics.update_ratchet_kappa_rl could not be imported. "
            "Ensure exit_dynamics is on PYTHONPATH."
        )
    return _UPDATE_RATCHET_KAPPA_RL


def _state_delta(
    old_state: Mapping[str, Any],
    new_state: Mapping[str, Any],
    key: str,
) -> Optional[float]:
    """리포트용 상태 변화량. 두 값이 모두 유한할 때만 계산한다."""
    old_value = _finite_float(old_state.get(key))
    new_value = _finite_float(new_state.get(key))
    if old_value is None or new_value is None:
        return None
    return round(new_value - old_value, 6)


def run_weekly_ratchet_rl_cycle(
    trades_list: list[dict],
    current_ratchet_state: dict,
    eta: float = 0.04,
) -> dict:
    r"""
    주간 청산 거래를 집계하고 Ratchet RL 상태를 한 번 업데이트한다.

    Whipsaw / Giveback 은 상호 배타(Mutually Exclusive):
    whipsaw 조건이면 whipsaw_count 만 증가, 그렇지 않고 giveback 이면 giveback_count 만 증가.

    Whipsaw rate
    -------------
        W = Count(MFE >= 5.0 and FinalRet < 1.0) / N_closed

    Giveback rate (whipsaw 제외)
    -----------------------------
        G = Count(MFE >= 5.0 and FinalRet <= 0.50 * MFE, not whipsaw) / N_closed
    """
    old_state = (
        copy.deepcopy(current_ratchet_state)
        if isinstance(current_ratchet_state, dict)
        else {}
    )

    if not isinstance(trades_list, list):
        trades = []
        input_warning = "trades_list_not_list"
    else:
        trades = trades_list
        input_warning = ""

    parsed_eta = _finite_float(eta)
    if parsed_eta is None:
        learning_rate = 0.04
        eta_warning = "eta_non_finite_defaulted_to_0.04"
    else:
        learning_rate = max(0.0, min(1.0, parsed_eta))
        eta_warning = (
            "eta_clamped_to_[0,1]"
            if learning_rate != parsed_eta
            else ""
        )

    valid_closed_count = 0
    eligible_mfe_count = 0
    whipsaw_count = 0
    giveback_count = 0
    invalid_trade_count = 0
    open_or_unclosed_count = 0

    for raw_trade in trades:
        if not isinstance(raw_trade, Mapping):
            invalid_trade_count += 1
            continue

        if not _is_closed_trade(raw_trade):
            open_or_unclosed_count += 1
            continue

        mfe = _first_finite(
            raw_trade,
            ("mfe", "peak_mfe", "max_favorable_excursion"),
        )
        final_ret = _first_finite(
            raw_trade,
            ("final_ret", "realized_return", "return_pct", "pnl_pct"),
        )

        if mfe is None or final_ret is None:
            invalid_trade_count += 1
            continue

        valid_closed_count += 1

        if mfe < MFE_THRESHOLD_PCT:
            continue

        eligible_mfe_count += 1

        is_whipsaw = final_ret < WHIPSAW_FINAL_RET_THRESHOLD_PCT
        is_giveback = final_ret <= GIVEBACK_RETAINED_RATIO * mfe

        if is_whipsaw:
            whipsaw_count += 1
        elif is_giveback:
            giveback_count += 1

    if valid_closed_count == 0:
        warnings = [
            warning
            for warning in (input_warning, eta_warning)
            if warning
        ]
        log_msg = (
            "주간 Ratchet RL 유지: 유효 청산 거래가 없어 "
            "W=0.0000, G=0.0000이며 기존 상태를 변경하지 않았습니다."
        )
        if warnings:
            log_msg += " 경고=" + ", ".join(warnings)

        return {
            "new_state": old_state,
            "whipsaw_rate": 0.0,
            "giveback_rate": 0.0,
            "sample_count": 0,
            "eligible_mfe_count": 0,
            "whipsaw_count": 0,
            "giveback_count": 0,
            "invalid_trade_count": invalid_trade_count,
            "open_or_unclosed_count": open_or_unclosed_count,
            "eta_used": round(learning_rate, 6),
            "update_applied": False,
            "state_delta": {
                "kappa_max": 0.0,
                "kappa_min": 0.0,
                "convexity": 0.0,
                "hit_and_run_index": 0.0,
            },
            "log_msg": log_msg,
        }

    whipsaw_rate = max(
        0.0,
        min(1.0, whipsaw_count / valid_closed_count),
    )
    giveback_rate = max(
        0.0,
        min(1.0, giveback_count / valid_closed_count),
    )

    try:
        update_fn = _resolve_update_function()
        new_state = update_fn(
            copy.deepcopy(old_state),
            whipsaw_rate=whipsaw_rate,
            giveback_rate=giveback_rate,
            eta=learning_rate,
        )
        if not isinstance(new_state, dict):
            raise TypeError(
                "update_ratchet_kappa_rl must return a dict state."
            )
        update_applied = True
        update_error = ""
    except Exception as exc:
        new_state = old_state
        update_applied = False
        update_error = f"{type(exc).__name__}: {exc}"

    imbalance = whipsaw_rate - giveback_rate
    if not update_applied:
        action_text = "업데이트 실패로 기존 상태 유지"
    elif imbalance > 1e-12:
        action_text = "조기 청산 우세 → 트레일 폭 확대 방향"
    elif imbalance < -1e-12:
        action_text = "이익 반납 우세 → 트레일 폭 축소 방향"
    else:
        action_text = "두 비율 균형 → kappa 폭 변화 없음"

    log_msg = (
        f"주간 Ratchet RL: 유효청산 {valid_closed_count}건, "
        f"MFE≥{MFE_THRESHOLD_PCT:.1f}% {eligible_mfe_count}건, "
        f"Whipsaw {whipsaw_count}건(W={whipsaw_rate:.4f}), "
        f"Giveback {giveback_count}건(G={giveback_rate:.4f}); {action_text}."
    )
    if update_error:
        log_msg += f" error={update_error}"
    extra_warnings = [
        warning for warning in (input_warning, eta_warning) if warning
    ]
    if extra_warnings:
        log_msg += " 경고=" + ", ".join(extra_warnings)

    return {
        "new_state": new_state,
        "whipsaw_rate": round(whipsaw_rate, 6),
        "giveback_rate": round(giveback_rate, 6),
        "sample_count": valid_closed_count,
        "eligible_mfe_count": eligible_mfe_count,
        "whipsaw_count": whipsaw_count,
        "giveback_count": giveback_count,
        "invalid_trade_count": invalid_trade_count,
        "open_or_unclosed_count": open_or_unclosed_count,
        "eta_used": round(learning_rate, 6),
        "update_applied": update_applied,
        "state_delta": {
            "kappa_max": _state_delta(
                old_state, new_state, "kappa_max"
            ),
            "kappa_min": _state_delta(
                old_state, new_state, "kappa_min"
            ),
            "convexity": _state_delta(
                old_state, new_state, "convexity"
            ),
            "hit_and_run_index": _state_delta(
                old_state, new_state, "hit_and_run_index"
            ),
        },
        "log_msg": log_msg,
    }


def _load_recent_forward_trades(
    db_path: str,
    *,
    lookback_days: int = 7,
    now: Optional[datetime] = None,
) -> list[dict]:
    """market_data.sqlite forward_trades — 최근 lookback_days 청산 표본."""
    now = now or datetime.now()
    cutoff = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    uri = str(db_path).replace("\\", "/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT *
            FROM forward_trades
            WHERE substr(IFNULL(exit_date, entry_date), 1, 10) >= ?
            """,
            (cutoff,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _ratchet_state_from_config(cfg: Optional[Dict[str, Any]]) -> dict:
    """EXIT_RATCHET_STATE 로드 — 없으면 DEFAULT_RATCHET_STATE."""
    base = dict(xd.DEFAULT_RATCHET_STATE)
    if isinstance(cfg, dict):
        st = cfg.get(xd.RATCHET_STATE_KEY)
        if isinstance(st, dict):
            base.update(st)
    return base


def run_weekly_ratchet_rl_pipeline(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
    lookback_days: int = 7,
    eta: float = 0.04,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    실전 파이프라인: DB → run_weekly_ratchet_rl_cycle → config 영속화.

    Returns
    -------
    run_weekly_ratchet_rl_cycle 결과 + old_state, persisted, rates(legacy 호환).
    """
    own_cfg = sys_config is None
    cfg: Dict[str, Any]
    if own_cfg:
        try:
            from config_manager import load_system_config

            cfg = dict(load_system_config() or {})
        except Exception:
            cfg = {}
    else:
        cfg = sys_config  # type: ignore[assignment]

    if db_path is None:
        try:
            from market_db_paths import market_db_read_path

            db_path = market_db_read_path()
        except Exception:
            db_path = None

    old_state = _ratchet_state_from_config(cfg)
    trades_list: list[dict] = []
    load_error = ""
    if db_path:
        try:
            trades_list = _load_recent_forward_trades(
                db_path,
                lookback_days=lookback_days,
                now=now,
            )
        except Exception as ex:
            load_error = str(ex)
            logger.warning("Ratchet RL forward_trades load failed: %s", ex)

    result = run_weekly_ratchet_rl_cycle(trades_list, old_state, eta=eta)
    if load_error:
        result["log_msg"] += f" db_error={load_error}"

    persisted = False
    if persist and result.get("update_applied"):
        new_state = result["new_state"]
        if isinstance(new_state, dict):
            new_state = dict(new_state)
            new_state["updated_at"] = (now or datetime.now()).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            result["new_state"] = new_state
            cfg[xd.RATCHET_STATE_KEY] = new_state
            if own_cfg:
                try:
                    from config_manager import update_system_config

                    update_system_config({xd.RATCHET_STATE_KEY: new_state})
                    persisted = True
                except Exception as ex:
                    logger.warning("Ratchet RL config persist failed: %s", ex)
            else:
                persisted = True

    result["old_state"] = old_state
    result["persisted"] = persisted
    result["rates"] = {
        "n": result.get("sample_count", 0),
        "whipsaw_rate": result.get("whipsaw_rate", 0.0),
        "giveback_rate": result.get("giveback_rate", 0.0),
    }
    result["updated"] = bool(result.get("update_applied"))
    result["state"] = result.get("new_state", old_state)
    return result


# ---------------------------------------------------------------------------
# Legacy / bitget 호환 API
# ---------------------------------------------------------------------------
def _read_runner_trades(db_path: str, cutoff: str):
    uri = str(db_path).replace("\\", "/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
    try:
        cur = conn.execute(
            """
            SELECT mfe, final_ret, exit_type, bars_held
            FROM forward_trades
            WHERE (free_runner=1 OR scaled_out_frac > 0)
              AND status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL AND mfe IS NOT NULL
              AND substr(IFNULL(exit_date, entry_date),1,10) >= ?
            """,
            (cutoff,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def compute_runner_rates(rows) -> Dict[str, Any]:
    """rows: [(mfe, final_ret, exit_type, bars_held), ...] → whipsaw/giveback 비율."""
    n = len(rows)
    if n == 0:
        return {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}

    mfes = []
    givebacks = []
    parsed = []
    for mfe, fr, et, bars in rows:
        try:
            mfe_f = float(mfe)
            fr_f = float(fr)
        except (TypeError, ValueError):
            continue
        gb = (mfe_f - fr_f) / max(mfe_f, 1.0)
        gb = max(0.0, min(1.0, gb))
        givebacks.append(gb)
        mfes.append(mfe_f)
        parsed.append((mfe_f, fr_f, str(et or ""), gb))

    if not parsed:
        return {"n": 0, "whipsaw_rate": 0.0, "giveback_rate": 0.0}

    giveback_rate = sum(givebacks) / len(givebacks)

    srt = sorted(mfes)
    idx = max(0, int(0.60 * (len(srt) - 1)))
    p60_mfe = srt[idx]
    whips = sum(
        1 for (mfe_f, fr_f, et, gb) in parsed
        if et == "RUNNER_TRAIL" and mfe_f <= p60_mfe
    )
    whipsaw_rate = whips / len(parsed)

    return {
        "n": len(parsed),
        "whipsaw_rate": round(whipsaw_rate, 4),
        "giveback_rate": round(giveback_rate, 4),
        "p60_mfe": round(p60_mfe, 2),
    }


def evolve_ratchet_kappa(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    lookback_days: int = 7,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """주간 κ RL 1사이클 — run_weekly_ratchet_rl_pipeline 래퍼."""
    result = run_weekly_ratchet_rl_pipeline(
        sys_config=cfg,
        db_path=db_path,
        lookback_days=lookback_days,
        eta=0.04,
        persist=persist,
        now=now,
    )
    if result.get("sample_count", 0) == 0:
        return {
            "updated": False,
            "reason": "insufficient_runner_sample",
            "rates": result.get("rates", {}),
            "state": result.get("state", {}),
        }
    return {
        "updated": result.get("updated", False),
        "rates": result.get("rates", {}),
        "old_state": result.get("old_state", {}),
        "state": result.get("state", {}),
        "log_msg": result.get("log_msg", ""),
    }


def evolve_mega_trend_kill_sensitivity(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    lookback_days: int = 90,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    [Mega-Trend 3번] 주말 킬스위치 민감도 RL 1사이클.
    exit_ratchet_rl 주간 진화 루프와 동일 cron 경로에서 호출.
    """
    try:
        from mega_trend_kill_rl import evolve_mega_trend_kill_sensitivity as _evolve

        return _evolve(cfg, db_path=db_path, persist=persist, now=now)
    except Exception as ex:
        return {"updated": False, "reason": str(ex), "state": {}}


def build_mega_trend_kill_rl_brief(result: Dict[str, Any]) -> str:
    try:
        from mega_trend_kill_rl import build_kill_rl_brief

        return build_kill_rl_brief(result)
    except Exception:
        return f"[Mega-Trend Kill RL] {result}"


def build_ratchet_brief(result: Dict[str, Any]) -> str:
    st = result.get("state", result.get("new_state", {}))
    rates = result.get("rates", {})
    log_msg = result.get("log_msg", "")
    if not result.get("updated") and not result.get("update_applied"):
        brief = (
            f"🪝 <b>[래칫 κ RL]</b> 표본 부족({rates.get('n', 0)}건) — "
            f"κ_max {st.get('kappa_max', '—')} 유지"
        )
    else:
        delta = result.get("state_delta", {})
        brief = (
            f"🪝 <b>[래칫 κ RL]</b> 조기청산 {rates.get('whipsaw_rate', 0) * 100:.0f}% / "
            f"이익반납 {rates.get('giveback_rate', 0) * 100:.0f}% → "
            f"κ_max {st.get('kappa_max')} · κ_min {st.get('kappa_min')} · "
            f"곡선 {st.get('curve')}(c={st.get('convexity')})"
        )
        if delta:
            brief += (
                f" (Δκ_max={delta.get('kappa_max', 0):+.4f}, "
                f"Δκ_min={delta.get('kappa_min', 0):+.4f})"
            )
    if log_msg:
        brief += f"\n▪️ {log_msg}"
    return brief


def test_weekly_ratchet_rl_cycle() -> None:
    """조기 청산, 이익 반납, 정상 청산, 무표본 및 이상값을 검증한다."""
    initial_state = {
        "kappa_max": 0.12,
        "kappa_min": 0.05,
        "anchor_ret": 20.0,
        "convexity": 1.0,
        "curve": "linear",
        "hit_and_run_index": 0.0,
        "custom_metadata": {"source": "test"},
    }

    whipsaw_heavy = [
        {"status": "CLOSED_WIN", "mfe": 8.0, "final_ret": 0.5},
        {"status": "CLOSED", "mfe": 7.0, "final_ret": -0.2},
        {"status": "EXITED", "mfe": 10.0, "final_ret": 0.9},
        {"status": "CLOSED", "mfe": 4.0, "final_ret": 2.0},
    ]
    result_w = run_weekly_ratchet_rl_cycle(
        whipsaw_heavy,
        initial_state,
    )
    assert result_w["sample_count"] == 4
    assert result_w["whipsaw_count"] == 3
    assert result_w["giveback_count"] == 0
    assert abs(result_w["whipsaw_rate"] - 0.75) < 1e-12
    assert abs(result_w["giveback_rate"] - 0.0) < 1e-12
    assert result_w["update_applied"] is True
    assert result_w["new_state"]["custom_metadata"] == {"source": "test"}
    assert result_w["new_state"]["kappa_max"] > initial_state["kappa_max"]
    assert result_w["new_state"]["kappa_min"] > initial_state["kappa_min"]

    giveback_heavy = [
        {"status": "CLOSED", "mfe": 10.0, "final_ret": 4.0},
        {"status": "CLOSED", "mfe": 8.0, "final_ret": 3.0},
        {"status": "CLOSED", "mfe": 12.0, "final_ret": 5.0},
        {"status": "CLOSED", "mfe": 9.0, "final_ret": 7.0},
    ]
    result_g = run_weekly_ratchet_rl_cycle(
        giveback_heavy,
        initial_state,
    )
    assert result_g["whipsaw_rate"] == 0.0
    assert result_g["giveback_rate"] == 0.75
    assert result_g["new_state"]["kappa_max"] < initial_state["kappa_max"]
    assert result_g["new_state"]["kappa_min"] < initial_state["kappa_min"]
    assert result_g["new_state"]["convexity"] < initial_state["convexity"]

    healthy = [
        {"status": "CLOSED", "mfe": 10.0, "final_ret": 8.0},
        {"status": "CLOSED", "mfe": 6.0, "final_ret": 4.0},
        {"status": "CLOSED", "mfe": 4.0, "final_ret": 2.0},
    ]
    result_h = run_weekly_ratchet_rl_cycle(healthy, initial_state)
    assert result_h["whipsaw_rate"] == 0.0
    assert result_h["giveback_rate"] == 0.0
    assert result_h["new_state"]["kappa_max"] == initial_state["kappa_max"]
    assert result_h["new_state"]["kappa_min"] == initial_state["kappa_min"]

    empty = run_weekly_ratchet_rl_cycle([], initial_state)
    assert empty["sample_count"] == 0
    assert empty["update_applied"] is False
    assert empty["new_state"] == initial_state
    assert empty["new_state"] is not initial_state

    mixed_invalid = [
        {"status": "OPEN", "mfe": 9.0, "final_ret": 0.0},
        {"status": "CLOSED", "mfe": float("nan"), "final_ret": 1.0},
        {"status": "CLOSED", "mfe": 8.0, "final_ret": float("inf")},
        {"status": "CLOSED", "mfe": 8.0, "final_ret": 6.0},
        "not-a-dict",
    ]
    result_invalid = run_weekly_ratchet_rl_cycle(  # type: ignore[arg-type]
        mixed_invalid,
        initial_state,
    )
    assert result_invalid["sample_count"] == 1
    assert result_invalid["invalid_trade_count"] == 3
    assert result_invalid["open_or_unclosed_count"] == 1
    assert result_invalid["whipsaw_rate"] == 0.0
    assert result_invalid["giveback_rate"] == 0.0

    alias_keys = [
        {
            "peak_mfe": 10.0,
            "realized_return": 4.0,
        }
    ]
    result_alias = run_weekly_ratchet_rl_cycle(
        alias_keys,
        initial_state,
        eta=float("nan"),
    )
    assert result_alias["sample_count"] == 1
    assert result_alias["giveback_rate"] == 1.0
    assert result_alias["eta_used"] == 0.04

    clamped_eta = run_weekly_ratchet_rl_cycle(
        healthy,
        initial_state,
        eta=2.0,
    )
    assert clamped_eta["eta_used"] == 1.0

    print("test_weekly_ratchet_rl_cycle: all tests passed")
    print(result_w["log_msg"])
    print(result_g["log_msg"])
    print(empty["log_msg"])


if __name__ == "__main__":
    test_weekly_ratchet_rl_cycle()
