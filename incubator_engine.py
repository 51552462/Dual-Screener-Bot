"""
Project 2: Hyperbolic Time Chamber — 인큐베이터 평가 엔진 (완전 격리, 유전 진화형).

- 입력: 동일 디렉터리 `synthetic_market.sqlite` 의 `synthetic_ohlcv` 만 (단일 read_sql).
- 출력: `mutant_hall_of_fame.json` (테스터/실장부와 무관).
- 일자·티커 축 백테스트 루프 없음: 전 구간·전 종목 동시 벡터 연산.

[Mission 1] 랭킹 정상화: 승률(win_rate)이 아니라 **샤프(Sharpe) 1순위 · 기대값(Expectancy) 2순위**.
            합성 시장의 무조건 드리프트(baseline) 대비 **초과 알파(excess_return)** 도 함께 계측.
[Mission 2] 고정 5종 mock 폐기 → `genetic_expr_builder` 로 매주 수천 개 돌연변이 군집 평가.
[Mission 3] 직전 세대 챔피언 생존율 + 합성 시장 국면(regime)으로 교배/돌연변이/신규 비율 자동 변속.
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

import genetic_expr_builder as gp

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SYNTHETIC_DB = os.path.join(_THIS_DIR, "synthetic_market.sqlite")
OUTPUT_JSON = os.path.join(_THIS_DIR, "mutant_hall_of_fame.json")
TELEGRAM_ERROR_LOG = os.path.join(_THIS_DIR, "telegram_error_log.txt")

DEFAULT_POPULATION = int(os.environ.get("INCUBATOR_POP_SIZE", "1000") or "1000")
MIN_SIGNALS = int(os.environ.get("INCUBATOR_MIN_SIGNALS", "50") or "50")

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
    """SQLite → 메모리 단일 로드 후 피벗으로 (날짜×티커) 큐브 구성."""
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
        open=o, high=h, low=l_, close=c_, volume=v, vol_ma5=vol_ma5,
        dates=o.index, tickers=o.columns,
    )


def read_synthetic_regime(db_path: str = SYNTHETIC_DB) -> str:
    """synthetic_meta.regime_mix 에서 SIDEWAYS 제외 우세 국면 추정(없으면 SIDEWAYS)."""
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False) as c:
            row = c.execute("SELECT value FROM synthetic_meta WHERE key='regime_mix'").fetchone()
        if not row:
            return "SIDEWAYS"
        mix = ast.literal_eval(str(row[0]))
        if not isinstance(mix, dict) or not mix:
            return "SIDEWAYS"
        return str(max(mix.items(), key=lambda kv: float(kv[1]))[0])
    except Exception:
        return "SIDEWAYS"


def build_local_vars(cube: MutantDataCube) -> dict[str, pd.DataFrame]:
    """genetic_expr_builder.VARIABLES 표준 변수 집합을 (T,N) 와이드로 구성."""
    O, H, L, C, V, VM5 = cube.open, cube.high, cube.low, cube.close, cube.volume, cube.vol_ma5
    eps = 1e-9
    return {
        "open": O, "high": H, "low": L, "close": C, "volume": V,
        "vol_ma5": VM5, "vol_lag1": V.shift(1),
        "ma5": C.rolling(5, min_periods=1).mean(),
        "ma10": C.rolling(10, min_periods=1).mean(),
        "ma20": C.rolling(20, min_periods=1).mean(),
        "ret1": C / C.shift(1) - 1.0,
        "body": (C - O) / (O + eps),
        "hl_range": (H - L) / (C + eps),
        "vol_ratio": V / (VM5 + eps),
    }


# ---------------------------------------------------------------------------
# 안전 표현식 게이트 (pd.eval 앞단)
# ---------------------------------------------------------------------------
_BIN_OPS_ALLOWED: Tuple[type, ...] = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.BitAnd, ast.BitOr)
_CMP_OPS_ALLOWED: Tuple[type, ...] = (ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE)
_UNARY_OPS_ALLOWED: Tuple[type, ...] = (ast.UAdd, ast.USub)


def is_safe_expression(expr_string: str) -> bool:
    """Call·Attribute·임포트·비화이트 연산 차단 (순수 ast 트리 순회)."""
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


def _eval_engine() -> str:
    try:
        import numexpr  # noqa: F401

        return "numexpr"
    except Exception:
        return "python"


def evaluate_mutant_strategies(
    cube: MutantDataCube,
    strategies: Sequence[Mapping[str, str]],
) -> Tuple[pd.DataFrame, float]:
    """
    모든 돌연변이 전략을 (날짜×티커) 행렬에 대해 동시에 평가.
    매수 시그널 셀의 익일 수익률 분포로 승률·평균·표준편차·샤프·기대값·초과알파 산출.
    반환: (성과 DataFrame, baseline_drift)
    """
    C = cube.close
    fwd1 = C.shift(-1) / C - 1.0
    r_all = np.asarray(fwd1, dtype=np.float64)
    finite_all = r_all[np.isfinite(r_all)]
    baseline = float(np.mean(finite_all)) if finite_all.size else 0.0

    local_base = build_local_vars(cube)
    eng = _eval_engine()

    rows: list[dict[str, Any]] = []
    for st in strategies:
        name = str(st.get("name", "unnamed"))
        expr = str(st.get("expr", "")).strip()
        origin = str(st.get("origin", ""))
        if not expr or not is_safe_expression(expr):
            continue
        try:
            sig = pd.eval(expr, local_dict=local_base, engine=eng)
        except Exception:
            try:
                sig = pd.eval(expr, local_dict=local_base, engine="python")
            except Exception:
                continue
        sig = sig.fillna(False).astype(bool)

        m = np.asarray(sig, dtype=bool)
        valid = m & np.isfinite(r_all)
        n_sig = int(np.sum(valid))
        if n_sig == 0:
            rows.append({
                "name": name, "expr": expr, "origin": origin, "n_signals": 0,
                "win_rate": 0.0, "avg_return": 0.0, "std_return": 0.0,
                "sharpe": 0.0, "expectancy": 0.0, "excess_return": -baseline,
            })
            continue
        rv = r_all[valid]
        win_rate = float(np.mean(rv > 0.0))
        avg_ret = float(np.mean(rv))
        std_ret = float(np.std(rv))
        sharpe = float(avg_ret / (std_ret + 1e-9))
        rows.append({
            "name": name, "expr": expr, "origin": origin, "n_signals": n_sig,
            "win_rate": win_rate, "avg_return": avg_ret, "std_return": std_ret,
            "sharpe": round(sharpe, 6), "expectancy": round(avg_ret * n_sig, 6),
            "excess_return": round(avg_ret - baseline, 8),
        })

    return pd.DataFrame(rows), baseline


def _rank_performance(perf: pd.DataFrame) -> pd.DataFrame:
    """[Mission 1] 샤프 1순위 · 기대값 2순위 · 시그널 수 3순위. 표본 부족은 후순위."""
    if perf.empty:
        return perf
    perf = perf.copy()
    perf["_enough"] = (perf["n_signals"] >= MIN_SIGNALS).astype(int)
    ranked = perf.sort_values(
        by=["_enough", "sharpe", "expectancy", "n_signals"],
        ascending=[False, False, False, False],
    ).drop(columns=["_enough"]).reset_index(drop=True)
    return ranked


def _load_previous_champions(path: str = OUTPUT_JSON) -> list[str]:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    out: list[str] = []
    for row in (data.get("hall_of_fame") or []):
        if isinstance(row, dict):
            e = str(row.get("expr", "")).strip()
            if e and is_safe_expression(e):
                out.append(e)
    return out


def _champion_survival_rate(cube: MutantDataCube, champions: Sequence[str]) -> float:
    """직전 챔피언들을 새 큐브에서 재평가 — 샤프>0(또는 초과알파>0) 비율."""
    champs = [c for c in champions if c]
    if not champs:
        return 1.0
    perf, _base = evaluate_mutant_strategies(
        cube, [{"name": f"prev_{i}", "expr": e} for i, e in enumerate(champs)]
    )
    if perf.empty:
        return 0.0
    alive = perf[(perf["n_signals"] >= MIN_SIGNALS) & (perf["sharpe"] > 0.0)]
    return float(len(alive) / max(1, len(perf)))


def run_incubator(
    strategies: Sequence[Mapping[str, str]] | None = None,
    top_k: int = 10,
    *,
    population: int | None = None,
    seed: int | None = None,
) -> dict[str, Any]:
    cube = load_synthetic_cube_from_sqlite()
    pop_n = int(population or DEFAULT_POPULATION)

    if strategies is not None:
        strat_list = list(strategies)
        regime = read_synthetic_regime()
        gear = gp.regime_gear(regime).as_dict()
        survival = 1.0
        prev_champs: list[str] = []
    else:
        regime = read_synthetic_regime()
        prev_champs = _load_previous_champions()
        survival = _champion_survival_rate(cube, prev_champs) if prev_champs else 1.0
        parents = prev_champs or list(gp.default_seed_strategies())
        gear = gp.regime_gear(regime, champion_survival_rate=survival).as_dict()
        strat_list = gp.generate_population(
            parents, n=pop_n, regime=regime,
            champion_survival_rate=survival, seed=seed,
        )

    perf, baseline = evaluate_mutant_strategies(cube, strat_list)
    ranked = _rank_performance(perf)
    top = ranked.head(int(top_k)).to_dict(orient="records")

    if len(cube.dates) > 0:
        d0 = pd.Timestamp(cube.dates[0]).strftime("%Y-%m-%d")
        d1 = pd.Timestamp(cube.dates[-1]).strftime("%Y-%m-%d")
        training_window = f"{d0} to {d1}"
    else:
        training_window = "N/A to N/A"

    lineage = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_samples_used": int(cube.close.size),
        "training_window": training_window,
        "version": str(uuid4()),
    }
    payload = {
        "_metadata": lineage,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_db": os.path.basename(SYNTHETIC_DB),
        "table": "synthetic_ohlcv",
        "cube_shape": [int(len(cube.dates)), int(len(cube.tickers))],
        "regime": regime,
        "evolution_gear": gear,
        "champion_survival_rate": round(float(survival), 4),
        "n_prev_champions": len(prev_champs),
        "baseline_drift": round(float(baseline), 8),
        "n_strategies_evaluated": int(len(strat_list)),
        "ranking_key": "sharpe>expectancy>n_signals (excess over baseline)",
        "hall_of_fame": top,
        "full_scoreboard": ranked.head(200).to_dict(orient="records"),
    }
    _atomic_write_json(OUTPUT_JSON, payload)
    return payload


# ---------------------------------------------------------------------------
# 텔레그램 (코어 수학 경로와 분리)
# ---------------------------------------------------------------------------
def _load_incubator_dotenv_optional() -> None:
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


def _telegram_operator_alert(message: str) -> None:
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
    gear = saved_payload.get("evolution_gear") or {}
    lines: list[str] = [
        "🧬 [정신과 시간의 방] 유전 진화 평가 완료",
        f"· 큐브(일×종): {saved_payload.get('cube_shape')} | 평가 수식: {saved_payload.get('n_strategies_evaluated'):,}개",
        f"· 국면: {saved_payload.get('regime')} | 챔피언 생존율: {float(saved_payload.get('champion_survival_rate', 0))*100:.0f}%",
        f"· 변속기어 교배/변이/신규: {gear.get('crossover_rate')}/{gear.get('mutation_rate')}/{gear.get('random_rate')}",
        f"· 시장 무조건 드리프트(baseline): {float(saved_payload.get('baseline_drift', 0))*100:.4f}%",
        "",
    ]
    top = list(saved_payload.get("hall_of_fame") or [])[:3]
    if not top:
        lines.append("상위 전략이 비어 있거나 합성 데이터가 부족합니다.")
        return "\n".join(lines)

    lines.append("── 상위 3 전략 (샤프 1순위) ──")
    for i, row in enumerate(top, 1):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "?"))
        expr = str(row.get("expr", "")).strip()
        if len(expr) > 200:
            expr = expr[:197] + "..."
        wr = float(row.get("win_rate") or 0.0) * 100.0
        ar = float(row.get("avg_return") or 0.0) * 100.0
        ex = float(row.get("excess_return") or 0.0) * 100.0
        sh = float(row.get("sharpe") or 0.0)
        ns = int(row.get("n_signals") or 0)
        lines.append(f"{i}위 — {name} [{row.get('origin','')}]")
        lines.append(f"  수식: {expr}")
        lines.append(
            f"  샤프: {sh:.3f} | 초과알파: {ex:+.4f}% | 승률: {wr:.1f}% | "
            f"익일평균: {ar:+.3f}% | 시그널: {ns:,}"
        )
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
    print(
        f"✅ 인큐베이터 완료: {OUTPUT_JSON} | 큐브 {out['cube_shape']} | "
        f"국면 {out['regime']} | 평가 {out['n_strategies_evaluated']}개 | 상위 {len(out['hall_of_fame'])} 기록"
    )

    try:
        from config_manager import load_system_config, save_system_config
        from re_evolution_loser_mutation import (
            collect_re_evolution_incubator_seed_hints,
            run_re_evolution_loser_mutation_cycle,
        )

        cfg = load_system_config() or {}
        updated, re_logs = run_re_evolution_loser_mutation_cycle(cfg)
        save_system_config(updated)
        hints = collect_re_evolution_incubator_seed_hints(updated)
        print("🔄 [Re-Evolution P2] Loser Mutation:")
        for ln in re_logs:
            print(ln.replace("<b>", "").replace("</b>", ""))
        if hints:
            print(f"   RE_EVOL 인큐베이터 시드: {len(hints)}건")
    except Exception as e:
        logger.warning("incubator_engine: re_evolution loser mutation hook: %s", e)
        print(f"⚠️ [Re-Evolution P2] 스킵: {e}")

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
