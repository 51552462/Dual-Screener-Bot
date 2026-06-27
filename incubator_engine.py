"""
Project 2: Hyperbolic Time Chamber — 인큐베이터 평가 엔진 (완전 격리).

- 입력: 동일 디렉터리 `synthetic_market.sqlite` 의 `synthetic_ohlcv` 만 (단일 read_sql).
- 출력: `mutant_hall_of_fame.json` (테스터/실장부와 무관).
- 일자·티커 축에 대한 백테스트 루프 없음: 전 구간·전 종목 동시 벡터 연산.
- (선택) TELEGRAM_* 환경변수 또는 동일 디렉터리 `.env` 로 요약 전송 — 코어 수학 경로와 분리.
"""
from __future__ import annotations

import ast
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd
import requests

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SYNTHETIC_DB = os.path.join(_THIS_DIR, "synthetic_market.sqlite")
OUTPUT_JSON = os.path.join(_THIS_DIR, "mutant_hall_of_fame.json")
TELEGRAM_ERROR_LOG = os.path.join(_THIS_DIR, "telegram_error_log.txt")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MutantDataCube:
    """날짜 × 티커 정렬 와이드 패널 (모든 연산은 (T,N) 브로드캐스트)."""

    open: pd.DataFrame
    high: pd.DataFrame
    low: pd.DataFrame
    close: pd.DataFrame
    volume: pd.DataFrame
    vol_ma5: pd.DataFrame
    dates: pd.Index
    tickers: pd.Index


def _atomic_write_json(path: str, obj: Mapping[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_synthetic_cube_from_sqlite(db_path: str = SYNTHETIC_DB) -> MutantDataCube:
    """
    SQLite → 메모리 단일 로드 후 피벗으로 (날짜×티커) 큐브 구성.
    티커·일자 축 이중 for 없음: pivot_table 한 번 + 정렬 벡터화.
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"합성 DB 없음: {db_path}")

    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False) as c:
        raw = pd.read_sql(
            "SELECT ticker, date, open, high, low, close, volume FROM synthetic_ohlcv",
            c,
        )

    if raw.empty:
        idx = pd.DatetimeIndex([], name="date")
        cols = pd.Index([], name="ticker")
        empty = pd.DataFrame(index=idx, columns=cols, dtype=np.float64)
        return MutantDataCube(empty, empty, empty, empty, empty, empty, idx, cols)

    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw.dropna(subset=["date"])
    wide = pd.pivot_table(
        raw,
        index="date",
        columns="ticker",
        values=["open", "high", "low", "close", "volume"],
        aggfunc="first",
    ).sort_index()
    wide = wide.sort_index(axis=1, level=1)

    o = wide["open"].astype(np.float64)
    h = wide["high"].astype(np.float64)
    l_ = wide["low"].astype(np.float64)
    c_ = wide["close"].astype(np.float64)
    v = wide["volume"].astype(np.float64)
    vol_ma5 = v.rolling(5, min_periods=1).mean()

    return MutantDataCube(
        open=o,
        high=h,
        low=l_,
        close=c_,
        volume=v,
        vol_ma5=vol_ma5,
        dates=o.index,
        tickers=o.columns,
    )


def _eval_engine() -> str:
    try:
        import numexpr  # noqa: F401

        return "numexpr"
    except Exception:
        return "python"


_BIN_OPS_ALLOWED: Tuple[type, ...] = (
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.BitAnd,
    ast.BitOr,
)
_CMP_OPS_ALLOWED: Tuple[type, ...] = (
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
)
_UNARY_OPS_ALLOWED: Tuple[type, ...] = (ast.UAdd, ast.USub)


def is_safe_expression(expr_string: str) -> bool:
    """
    pd.eval 앞단 게이트: Call·Attribute·임포트·비화이트 연산 등 코드 주입 경로 차단 (순수 ast, O(n) 트리 순회).
    """
    if not isinstance(expr_string, str):
        return False
    s = expr_string.strip()
    if not s:
        return False
    try:
        tree = ast.parse(s, mode="eval")
    except SyntaxError:
        return False

    for node in ast.walk(tree):
        if isinstance(node, ast.Expression):
            continue
        if isinstance(node, ast.BinOp):
            if not isinstance(node.op, _BIN_OPS_ALLOWED):
                return False
            continue
        if isinstance(node, ast.UnaryOp):
            if not isinstance(node.op, _UNARY_OPS_ALLOWED):
                return False
            continue
        if isinstance(node, ast.Compare):
            if not all(isinstance(op, _CMP_OPS_ALLOWED) for op in node.ops):
                return False
            continue
        if isinstance(node, ast.Name):
            if not isinstance(node.ctx, ast.Load):
                return False
            continue
        if isinstance(node, ast.Load):
            continue
        if isinstance(node, ast.Constant):
            v = node.value
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                continue
            return False
        if isinstance(node, ast.Num):  # py<3.8 호환
            continue
        if isinstance(node, ast.operator):
            if not isinstance(node, _BIN_OPS_ALLOWED):
                return False
            continue
        if isinstance(node, ast.cmpop):
            if not isinstance(node, _CMP_OPS_ALLOWED):
                return False
            continue
        if isinstance(node, ast.unaryop):
            if not isinstance(node, _UNARY_OPS_ALLOWED):
                return False
            continue
        return False
    return True


def evaluate_mutant_strategies(
    data_cube: MutantDataCube,
    strategies: Sequence[Mapping[str, str]],
) -> pd.DataFrame:
    """
    모든 돌연변이 전략을 (날짜×티커) 행렬에 대해 동시에 평가.
    매수 시그널: expr 이 True 인 셀. 익일 수익률: close[t+1]/close[t]-1 (벡터 shift).
    """
    O, H, L, C, V, VM5 = (
        data_cube.open,
        data_cube.high,
        data_cube.low,
        data_cube.close,
        data_cube.volume,
        data_cube.vol_ma5,
    )
    fwd1 = C.shift(-1) / C - 1.0
    Vlag1 = V.shift(1)

    eng = _eval_engine()
    local_base: dict[str, pd.DataFrame] = {
        "open": O,
        "high": H,
        "low": L,
        "close": C,
        "volume": V,
        "vol_ma5": VM5,
        "vol_lag1": Vlag1,
    }

    rows: list[dict[str, Any]] = []
    for st in strategies:
        name = str(st.get("name", "unnamed"))
        expr = str(st.get("expr", "")).strip()
        if not expr:
            continue
        if not is_safe_expression(expr):
            if os.environ.get("INCUBATOR_EXPR_DEBUG"):
                print(f"[incubator] skip unsafe expr ({name}): {expr[:120]!r}")
            continue
        try:
            sig = pd.eval(expr, local_dict=local_base, engine=eng)
        except Exception:
            sig = pd.eval(expr, local_dict=local_base, engine="python")
        sig = sig.fillna(False).astype(bool)

        m = np.asarray(sig, dtype=bool)
        r = np.asarray(fwd1, dtype=np.float64)
        valid = m & np.isfinite(r)
        n_sig = int(np.sum(valid))
        if n_sig == 0:
            rows.append(
                {
                    "name": name,
                    "expr": expr,
                    "n_signals": 0,
                    "win_rate": 0.0,
                    "avg_return": 0.0,
                }
            )
            continue
        rv = r[valid]
        win_rate = float(np.mean(rv > 0.0))
        avg_ret = float(np.mean(rv))
        rows.append(
            {
                "name": name,
                "expr": expr,
                "n_signals": n_sig,
                "win_rate": win_rate,
                "avg_return": avg_ret,
            }
        )

    return pd.DataFrame(rows)


def default_mock_strategies() -> tuple[dict[str, str], ...]:
    """pd.eval / numexpr 호환 문자열 (소문자 컬럼명 = 와이드 DataFrame 변수)."""
    return (
        {"name": "M1_gap_up", "expr": "close > open * 1.05"},
        {"name": "M2_weak_close_high_vol", "expr": "close < open * 0.995 & volume > vol_lag1 * 1.2"},
        {"name": "M3_bull_range", "expr": "close > low * 1.02 & close > open"},
        {"name": "M4_upper_wick_pressure", "expr": "high > close * 1.01 & close < open * 1.002"},
        {"name": "M5_vol_breakout", "expr": "close > open * 1.02 & volume > vol_ma5 * 1.5"},
    )


def run_incubator(
    strategies: Sequence[Mapping[str, str]] | None = None,
    top_k: int = 5,
) -> dict[str, Any]:
    cube = load_synthetic_cube_from_sqlite()
    strat_list = tuple(strategies) if strategies is not None else default_mock_strategies()
    perf = evaluate_mutant_strategies(cube, strat_list)
    if perf.empty:
        ranked = perf
    else:
        ranked = perf.sort_values(
            by=["win_rate", "avg_return", "n_signals"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    top = ranked.head(int(top_k)).to_dict(orient="records")
    if len(cube.dates) > 0:
        d0 = pd.Timestamp(cube.dates[0]).strftime("%Y-%m-%d")
        d1 = pd.Timestamp(cube.dates[-1]).strftime("%Y-%m-%d")
        training_window = f"{d0} to {d1}"
    else:
        training_window = "N/A to N/A"
    n_panel = int(cube.close.size)
    lineage = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_samples_used": n_panel,
        "training_window": training_window,
        "version": str(uuid4()),
    }
    payload = {
        "_metadata": lineage,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_db": os.path.basename(SYNTHETIC_DB),
        "table": "synthetic_ohlcv",
        "cube_shape": [int(len(cube.dates)), int(len(cube.tickers))],
        "n_strategies_evaluated": int(len(strat_list)),
        "hall_of_fame": top,
        "full_scoreboard": ranked.to_dict(orient="records"),
    }
    _atomic_write_json(OUTPUT_JSON, payload)
    return payload


def _load_incubator_dotenv_optional() -> None:
    """동일 디렉터리 `.env` 를 읽어, 아직 없는 키만 os.environ 에 주입 (토큰 노출 로그 없음)."""
    path = os.path.join(_THIS_DIR, ".env")
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, val)
    except Exception as e:
        logger.warning("incubator_engine: optional .env load failed (%s): %s", path, e)
        return


def _telegram_operator_alert(message: str) -> None:
    """로컬 블랙박스 기록 + 콘솔 고가시성 경고 (팩토리 중단 없음)."""
    try:
        with open(TELEGRAM_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(message.rstrip() + "\n")
    except Exception as e:
        logger.error("incubator_engine: telegram_error_log append failed: %s", e)
    print("\n" + "*" * 72)
    print("⚠️  TELEGRAM REPORT NOT DELIVERED — CHECK telegram_error_log.txt  ⚠️")
    print("*" * 72)
    print(message)
    print("*" * 72 + "\n")


def send_telegram_report(text: str) -> None:
    """
    Telegram Bot API sendMessage (requests).
    실패 시 예외를 밖으로 던지지 않고 로컬 로그·콘솔에 기록 — 팩토리 본류 중단 없음.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        import telegram_env

        token = telegram_env.get_lab_token()
        chat_id = telegram_env.get_lab_chat_id()
        if not token or not chat_id:
            _telegram_operator_alert(
                f"[{ts}] ERROR: Telegram Token or Chat ID is missing. Message skipped."
            )
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = {"chat_id": chat_id, "text": (text or "")[:4096]}
        requests.post(url, json=body, timeout=20)
    except Exception as e:
        _telegram_operator_alert(
            f"[{ts}] ERROR: Telegram send failed: {type(e).__name__}: {e}"
        )


def format_incubator_telegram_message_ko(saved_payload: Mapping[str, Any]) -> str:
    """상위 3개 전략 요약 (한국어, 수치는 퍼센트 표기)."""
    lines: list[str] = [
        "🏆 [정신과 시간의 방] 인큐베이터 벡터 평가 완료",
        f"· 큐브 크기 (일×종): {saved_payload.get('cube_shape')}",
        f"· UTC 산출: {saved_payload.get('generated_at', '')}",
        "",
    ]
    top = list(saved_payload.get("hall_of_fame") or [])[:3]
    if not top:
        lines.append("상위 전략이 비어 있거나 합성 데이터가 부족합니다.")
        return "\n".join(lines)

    lines.append("── 상위 3 전략 ──")
    for i, row in enumerate(top, 1):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "?"))
        expr = str(row.get("expr", "")).strip()
        if len(expr) > 220:
            expr = expr[:217] + "..."
        try:
            wr = float(row.get("win_rate") or 0.0) * 100.0
        except (TypeError, ValueError):
            wr = 0.0
        try:
            ar = float(row.get("avg_return") or 0.0) * 100.0
        except (TypeError, ValueError):
            ar = 0.0
        try:
            ns = int(row.get("n_signals") or 0)
        except (TypeError, ValueError):
            ns = 0
        lines.append(f"{i}위 — {name}")
        lines.append(f"  수식: {expr}")
        lines.append(f"  승률: {wr:.2f}% | 익일 평균수익률: {ar:.3f}% | 시그널 수: {ns:,}")
        lines.append("")
    lines.append("상세: mutant_hall_of_fame.json 참조.")
    return "\n".join(lines).rstrip()


def main() -> None:
    try:
        out = run_incubator()
    except FileNotFoundError as e:
        print(f"⚠️ {e}")
        _atomic_write_json(
            OUTPUT_JSON,
            {
                "_metadata": {
                    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "n_samples_used": 0,
                    "training_window": "N/A to N/A",
                    "version": str(uuid4()),
                },
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "error": str(e),
                "hall_of_fame": [],
            },
        )
        return
    print(f"✅ 인큐베이터 완료: {OUTPUT_JSON} | 큐브 {out['cube_shape']} | 상위 {len(out['hall_of_fame'])} 전략 기록")

    try:
        _load_incubator_dotenv_optional()
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            saved = json.load(f)
        send_telegram_report(format_incubator_telegram_message_ko(saved))
    except Exception as e:
        logger.exception("incubator_engine: post-run telegram / JSON read failed: %s", e)
        print(f"⚠️ [incubator_engine] 결과 텔레그램/후처리 실패: {e}")


if __name__ == "__main__":
    main()
