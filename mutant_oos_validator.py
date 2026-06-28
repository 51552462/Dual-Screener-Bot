"""
Mutant OOS Validator — 합성 인큐베이터 챔피언의 실데이터(읽기 전용) 검증 게이트.

- incubator_engine.py / 스크리너 미수정.
- 입력: 동일 디렉터리 mutant_hall_of_fame.json (hall_of_fame)
- 출력: validated_live_mutants.json (합격 전략만)
- DB: market_data.sqlite 는 URI mode=ro + PRAGMA query_only=ON 만 사용.
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
import requests

from market_db_paths import market_db_read_path

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
HALL_OF_FAME_JSON = os.path.join(_THIS_DIR, "mutant_hall_of_fame.json")
VALIDATED_JSON = os.path.join(_THIS_DIR, "validated_live_mutants.json")
TELEGRAM_ERROR_LOG = os.path.join(_THIS_DIR, "telegram_error_log.txt")

# [Mission 1] 361행 NameError 수정: market_db_paths 의 단일 경로 SSOT 를 모듈 상수로 노출.
MARKET_DB = market_db_read_path()

# 최근 약 6개월 영업일(여유)
OOS_MIN_BARS = 130
MAX_TICKERS_SAMPLE = 100
# [Mission 5] 합격 기준 지능화: '실데이터 무조건 드리프트(baseline)' 대비 초과 알파로 게이트.
PROMOTE_MIN_WIN_RATE = 0.50
PROMOTE_MIN_EXCESS_ALPHA = float(os.environ.get("OOS_MIN_EXCESS_ALPHA", "0.00005") or "0.00005")
PROMOTE_MIN_SIGNALS = int(os.environ.get("OOS_MIN_SIGNALS", "30") or "30")


def _atomic_write_json(path: str, obj: Mapping[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_dotenv_optional() -> None:
    p = os.path.join(_THIS_DIR, ".env")
    if not os.path.isfile(p):
        return
    try:
        with open(p, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k:
                    os.environ.setdefault(k, v)
    except Exception:
        return


def _telegram_operator_alert(message: str) -> None:
    """로컬 블랙박스 기록 + 콘솔 고가시성 경고 (팩토리 중단 없음)."""
    try:
        with open(TELEGRAM_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(message.rstrip() + "\n")
    except Exception:
        pass
    print("\n" + "*" * 72)
    print("⚠️  TELEGRAM REPORT NOT DELIVERED — CHECK telegram_error_log.txt  ⚠️")
    print("*" * 72)
    print(message)
    print("*" * 72 + "\n")


def send_telegram_report(text: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        _load_dotenv_optional()
        import telegram_env

        token = telegram_env.get_lab_token()
        chat_id = telegram_env.get_lab_chat_id()
        if not token or not chat_id:
            _telegram_operator_alert(
                f"[{ts}] ERROR: Telegram Token or Chat ID is missing. Message skipped."
            )
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": (text or "")[:4096]}, timeout=25)
    except Exception as e:
        _telegram_operator_alert(
            f"[{ts}] ERROR: Telegram send failed: {type(e).__name__}: {e}"
        )


def _open_market_db_ro() -> Optional[sqlite3.Connection]:
    p = market_db_read_path()
    if not os.path.exists(p):
        return None
    try:
        uri = f"file:{p.replace(chr(92), '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.execute("PRAGMA query_only=ON;")
        return conn
    except Exception:
        return None


def _list_price_tables(conn: sqlite3.Connection) -> list[str]:
    try:
        cur = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND (name LIKE 'KR_%' OR name LIKE 'US_%')
            ORDER BY name
            """
        )
        return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception:
        return []


def _read_ohlcv_table(conn: sqlite3.Connection, table: str) -> Optional[pd.DataFrame]:
    try:
        cutoff = (datetime.now() - timedelta(days=190)).strftime("%Y-%m-%d")
        q = (
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{table}" '
            f"WHERE Date >= ? ORDER BY Date ASC"
        )
        df = pd.read_sql(q, conn, params=(cutoff,))
    except Exception:
        return None
    if df is None or len(df) < OOS_MIN_BARS:
        try:
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{table}" ORDER BY Date ASC',
                conn,
            )
        except Exception:
            return None
        if df is None or len(df) < OOS_MIN_BARS:
            return None
    df = df.tail(OOS_MIN_BARS + 40).copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    for c in ("Open", "High", "Low", "Close", "Volume"):
        if c not in df.columns:
            return None
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if len(df) < OOS_MIN_BARS:
        return None
    return df.reset_index(drop=True)


def _prepare_eval_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    genetic_expr_builder.VARIABLES 표준 변수 집합을 실데이터로 동일 구성.
    (인큐베이터·라이브와 변수 정의가 1:1 이어야 OOS 가 공정하다.)
    """
    out = pd.DataFrame(
        {
            "open": df["Open"].astype(np.float64),
            "high": df["High"].astype(np.float64),
            "low": df["Low"].astype(np.float64),
            "close": df["Close"].astype(np.float64),
            "volume": df["Volume"].astype(np.float64),
        }
    )
    eps = 1e-9
    out["vol_ma5"] = out["volume"].rolling(5, min_periods=1).mean()
    out["vol_lag1"] = out["volume"].shift(1)
    out["ma5"] = out["close"].rolling(5, min_periods=1).mean()
    out["ma10"] = out["close"].rolling(10, min_periods=1).mean()
    out["ma20"] = out["close"].rolling(20, min_periods=1).mean()
    out["ret1"] = out["close"] / out["close"].shift(1) - 1.0
    out["body"] = (out["close"] - out["open"]) / (out["open"] + eps)
    out["hl_range"] = (out["high"] - out["low"]) / (out["close"] + eps)
    out["vol_ratio"] = out["volume"] / (out["vol_ma5"] + eps)
    return out


def _panel_baseline_drift(frames_by_key: dict[str, pd.DataFrame]) -> float:
    """실데이터 패널의 무조건(랜덤) 익일 수익 평균 = '시장 중력' 기준선."""
    allr: list[float] = []
    for raw in frames_by_key.values():
        c = pd.to_numeric(raw["Close"], errors="coerce").astype(np.float64)
        fwd = (c.shift(-1) / c - 1.0).to_numpy()
        fwd = fwd[np.isfinite(fwd)]
        if fwd.size:
            allr.extend(fwd.tolist())
    return float(np.mean(np.array(allr))) if allr else 0.0


def _eval_engine() -> str:
    try:
        import numexpr  # noqa: F401

        return "numexpr"
    except Exception:
        return "python"


def _oos_forward_returns_at_signals(expr: str, ev: pd.DataFrame) -> Optional[np.ndarray]:
    """단일 종목: 시그널 발생일 익일 수익률 벡터. pd.eval 실패 시 None (시그널 0건과 구분)."""
    eng = _eval_engine()
    local_base = {col: ev[col] for col in ev.columns}
    try:
        sig = pd.eval(expr, local_dict=local_base, engine=eng)
    except Exception:
        try:
            sig = pd.eval(expr, local_dict=local_base, engine="python")
        except Exception:
            return None
    sig = pd.Series(sig).fillna(False).astype(bool)
    fwd1 = ev["close"].shift(-1) / ev["close"] - 1.0
    m = np.asarray(sig, dtype=bool)
    r = np.asarray(fwd1, dtype=np.float64)
    valid = m & np.isfinite(r)
    return r[valid]


def _fdr_fallback_panel() -> dict[str, pd.DataFrame]:
    """DB에 테이블이 없을 때만 — 소수 대형주 6개월."""
    out: dict[str, pd.DataFrame] = {}
    try:
        import FinanceDataReader as fdr
    except Exception:
        return out
    end = datetime.now()
    start = end - timedelta(days=210)
    for code in ("005930", "000660", "035420", "051910", "006400", "035720"):
        try:
            d = fdr.DataReader(code, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
            if d is None or len(d) < 60:
                continue
            d = d.copy()
            if isinstance(d.columns, pd.MultiIndex):
                d.columns = [str(c[0]) if isinstance(c, tuple) else str(c) for c in d.columns]
            need = {"Open", "High", "Low", "Close", "Volume"}
            if not need.issubset(set(d.columns)):
                continue
            d = d.reset_index()
            date_col = "Date" if "Date" in d.columns else d.columns[0]
            d = d.rename(columns={date_col: "Date"})
            out[f"KR_{code}"] = d[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        except Exception:
            continue
    return out


def load_champions_from_hall_of_fame(
    path: str = HALL_OF_FAME_JSON,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    hall = data.get("hall_of_fame") if isinstance(data, dict) else None
    if not isinstance(hall, list):
        return []
    out: list[dict[str, Any]] = []
    for row in hall[:top_n]:
        if isinstance(row, dict) and str(row.get("expr", "")).strip():
            out.append(dict(row))
    return out


def run_oos_validation(
    champions: Sequence[Mapping[str, Any]] | None = None,
    top_n: int = 10,
    rng_seed: int = 42,
) -> dict[str, Any]:
    champs = list(champions) if champions is not None else load_champions_from_hall_of_fame(top_n=top_n)
    rng = random.Random(rng_seed)

    frames_by_key: dict[str, pd.DataFrame] = {}
    conn = _open_market_db_ro()
    tables: list[str] = []
    if conn is not None:
        try:
            tables = _list_price_tables(conn)
            rng.shuffle(tables)
            tables = tables[:MAX_TICKERS_SAMPLE]
            for t in tables:
                raw = _read_ohlcv_table(conn, t)
                if raw is not None:
                    frames_by_key[t] = raw
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if len(frames_by_key) < 5:
        frames_by_key = _fdr_fallback_panel()

    baseline = _panel_baseline_drift(frames_by_key)

    promoted: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    for c in champs:
        name = str(c.get("name", "?"))
        expr = str(c.get("expr", "")).strip()
        if not expr:
            continue
        all_r: list[float] = []
        all_win: list[float] = []
        eval_failed = False
        for _tbl, raw in frames_by_key.items():
            ev = _prepare_eval_frame(raw)
            rv = _oos_forward_returns_at_signals(expr, ev)
            if rv is None:
                eval_failed = True
                break
            if rv.size == 0:
                continue
            all_r.extend(rv.tolist())
            all_win.extend((rv > 0.0).astype(float).tolist())

        if eval_failed:
            summaries.append(
                {
                    "name": name,
                    "expr": expr,
                    "oos_win_rate": None,
                    "oos_avg_return": None,
                    "n_signals": 0,
                    "pass": False,
                    "reason": "eval_error",
                }
            )
            continue

        if not all_r:
            summaries.append(
                {
                    "name": name,
                    "expr": expr,
                    "oos_win_rate": None,
                    "oos_avg_return": None,
                    "n_signals": 0,
                    "pass": False,
                    "reason": "no_signals_on_real_panel",
                }
            )
            continue

        oos_wr = float(np.mean(np.array(all_win)))
        oos_ar = float(np.mean(np.array(all_r)))
        excess_alpha = oos_ar - baseline
        n_sig = int(len(all_r))
        # [Mission 5] 합격 = 실데이터 베이스라인 대비 초과 알파 + 최소 승률 + 최소 표본.
        passed = (
            excess_alpha > PROMOTE_MIN_EXCESS_ALPHA
            and oos_wr > PROMOTE_MIN_WIN_RATE
            and n_sig >= PROMOTE_MIN_SIGNALS
        )
        rec = {
            "name": name,
            "expr": expr,
            "synthetic_win_rate": c.get("win_rate"),
            "synthetic_avg_return": c.get("avg_return"),
            "synthetic_sharpe": c.get("sharpe"),
            "oos_win_rate": round(oos_wr, 6),
            "oos_avg_return": round(oos_ar, 8),
            "oos_baseline_drift": round(baseline, 8),
            "oos_excess_alpha": round(excess_alpha, 8),
            "n_signals": n_sig,
            "n_tickers_used": len(frames_by_key),
            "pass": passed,
        }
        summaries.append(rec)
        if passed:
            row = {k: v for k, v in rec.items() if k != "pass"}
            row["validated_at"] = datetime.now(timezone.utc).isoformat()
            promoted.append(row)

    payload = {
        "validated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_source": "market_data.sqlite_ro" if os.path.exists(MARKET_DB) else "fdr_fallback",
        "n_tickers_panel": len(frames_by_key),
        "baseline_drift": round(baseline, 8),
        "thresholds": {
            "min_win_rate": PROMOTE_MIN_WIN_RATE,
            "min_excess_alpha": PROMOTE_MIN_EXCESS_ALPHA,
            "min_signals": PROMOTE_MIN_SIGNALS,
        },
        "promoted": promoted,
        "all_results": summaries,
    }
    _atomic_write_json(VALIDATED_JSON, payload)
    return payload


def _format_telegram_top1(payload: dict[str, Any]) -> str:
    summ = payload.get("all_results") or []
    top = summ[0] if summ else {}
    name = str(top.get("name", "?"))
    wr = top.get("oos_win_rate")
    passed = bool(top.get("pass"))
    reason = str(top.get("reason") or "")
    ex = top.get("oos_excess_alpha")
    if reason == "eval_error":
        body = f"실전 OOS: 표현식 평가 실패(eval_error) — {name} → 불합격"
    elif wr is None:
        body = "실전 OOS: 시그널 없음 또는 데이터 부족 → 불합격"
    else:
        wr_pct = float(wr) * 100.0
        ex_pct = float(ex or 0.0) * 100.0
        verdict = "최종 합격" if passed else "최종 불합격"
        body = (
            f"가상 1등({name}) 실데이터: 승률 {wr_pct:.2f}% · "
            f"초과알파 {ex_pct:+.4f}% → [{verdict}]"
        )
    n_promo = len(payload.get("promoted") or [])
    base_pct = float(payload.get("baseline_drift", 0.0)) * 100.0
    return (
        "🛡️ [실전 OOS 검증 완료]\n"
        f"{body}\n"
        f"· 시장 베이스라인: {base_pct:+.4f}% (이걸 이겨야 합격)\n"
        f"· 합격 승격 전략 수: {n_promo}\n"
        f"· 패널 종목 수: {payload.get('n_tickers_panel', 0)}"
    )


def main() -> None:
    print("🛡️ Mutant OOS Validator — 실데이터 게이트…")
    try:
        out = run_oos_validation()
    except Exception as e:
        print(f"⚠️ OOS 검증 실패: {e}")
        _atomic_write_json(
            VALIDATED_JSON,
            {
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "promoted": [],
            },
        )
        send_telegram_report(
            f"🛡️ [실전 OOS 검증 완료] 오류로 중단: {e}\n→ [최종 불합격/미실행]"
        )
        return
    print(f"✅ 저장: {VALIDATED_JSON} | 합격 {len(out.get('promoted') or [])}건")
    try:
        from mutant_pending_bridge import sync_validated_json_into_pending

        n_add, sync_msg = sync_validated_json_into_pending()
        print(f"[mutant_pending_bridge] {sync_msg} (신규 {n_add})")
    except Exception as sync_e:
        print(f"⚠️ [mutant_pending_bridge] PENDING 동기화 스킵: {sync_e}")

    # [Mission 5] 수동 승인 게이트 우회 — OOS 통과 유전자 수식을 INCUBATOR_TEMPLATES 에
    # 즉시 자동 병합하고, 최소 켈리(탐색 모드) 밴딧 밸브로만 실전 투입.
    auto_msg = ""
    try:
        from mutant_pending_bridge import auto_merge_validated_into_incubator

        n_auto, auto_msg = auto_merge_validated_into_incubator()
        print(f"[auto_promote] {auto_msg} (자동 병합 {n_auto})")
    except Exception as auto_e:
        print(f"⚠️ [auto_promote] 자동 병합 스킵: {auto_e}")

    msg = _format_telegram_top1(out)
    if auto_msg:
        msg += f"\n🚀 자동 승격(탐색 켈리): {auto_msg}"
    send_telegram_report(msg)


if __name__ == "__main__":
    main()
