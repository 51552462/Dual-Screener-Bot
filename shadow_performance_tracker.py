"""
그림자 장부 성과 평가: 차단 이력 대비 사후 가격, 위성 태그별 기여도 요약 → SHADOW_PERFORMANCE 저장.
"""
from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timedelta
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

try:
    import yfinance as yf
except ImportError:
    yf = None

from config_manager import CONFIG_PATH
from market_db_paths import market_db_read_path


def _db_path_ro() -> str:
    return market_db_read_path()


def _connect_ro(max_retries: int = 5):
    uri_path = _db_path_ro().replace("\\", "/")
    uri = f"file:{uri_path}?mode=ro"
    last_err = None
    for attempt in range(max_retries):
        try:
            return sqlite3.connect(uri, uri=True, check_same_thread=False)
        except sqlite3.OperationalError as e:
            last_err = e
            time.sleep(random.uniform(0.05, 0.2))
    if last_err:
        raise last_err
    raise RuntimeError("RO connect failed")


def _infer_market(code: str) -> str:
    s = str(code).strip()
    if re.fullmatch(r"\d{1,6}", s):
        return "KR"
    return "US"


def fetch_last_close_stealth(code: str, market: str) -> Optional[float]:
    """스텔스 지터 후 최종 종가 조회 (외부 API)."""
    time.sleep(random.uniform(0.3, 0.7))
    try:
        if market == "KR" and fdr is not None:
            c = str(code).zfill(6)[-6:]
            st = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
            df = fdr.DataReader(c, st)
            if df is not None and not df.empty and "Close" in df.columns:
                return float(df["Close"].iloc[-1])
        if market == "US" and yf is not None:
            sym = str(code).replace("/", "-").strip()
            if not sym:
                return None
            d = yf.Ticker(sym).history(period="5d")
            if d is not None and not d.empty and "Close" in d.columns:
                return float(d["Close"].iloc[-1])
    except Exception:
        pass
    return None


def _load_config_armored(max_retries: int = 5) -> Dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        return {}
    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError):
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
    return {}


def _save_config_atomic_armored(cfg: Dict[str, Any], max_retries: int = 5) -> bool:
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                return False
        except Exception:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False


def fetch_blocked_history(limit: int = 500) -> List[Tuple]:
    try:
        conn = _connect_ro()
        try:
            cur = conn.execute(
                """
                SELECT id, code, name, reason, entry_price, blocked_at
                FROM blocked_trade_history
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            return cur.fetchall()
        finally:
            conn.close()
    except Exception:
        return []


def fetch_virtual_trade_history(limit: int = 2000) -> pd.DataFrame:
    try:
        conn = _connect_ro()
        try:
            return pd.read_sql(
                f"""
                SELECT id, market, code, name, entry_price, sig_type, satellite_tags, logged_at
                FROM virtual_trade_history
                ORDER BY id DESC
                LIMIT {int(limit)}
                """,
                conn,
            )
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame()


def _signed_defense_pct(entry_price: float, current_price: float) -> Optional[float]:
    try:
        ep = float(entry_price)
        px = float(current_price)
        if ep <= 0 or px <= 0:
            return None
        return (ep - px) / ep * 100.0
    except (TypeError, ValueError):
        return None


def evaluate_blocked_trades(rows: List[Tuple], max_eval: int = 200) -> Dict[str, Any]:
    """
    reason별: 평가 건수, 서명 방어율 합계, 양수 방어 건수.
    entry_price<=0 (둠스데이 등)는 가격 비교 생략하되 건수만 집계 가능.
    """
    by_reason: Dict[str, Dict[str, Any]] = {}
    seen_code_block: set = set()
    reason_event_counts = Counter(str(r[3]) or "UNKNOWN" for r in rows)

    n_processed = 0
    for row in rows:
        if n_processed >= max_eval:
            break
        _id, code, name, reason, entry_price, blocked_at = row
        reason_k = str(reason or "UNKNOWN")
        ck = f"{reason_k}|{code}"
        if ck in seen_code_block:
            continue
        seen_code_block.add(ck)

        if reason_k not in by_reason:
            by_reason[reason_k] = {
                "n_evaluated_price": 0,
                "n_skipped_no_price": 0,
                "n_fetch_failed": 0,
                "sum_signed_defense_pct": 0.0,
                "sum_positive_defense_pct": 0.0,
                "n_positive_defense": 0,
                "n_negative_opportunity_cost": 0,
            }

        try:
            ep = float(entry_price or 0)
        except (TypeError, ValueError):
            ep = 0.0

        if ep <= 0:
            by_reason[reason_k]["n_skipped_no_price"] += 1
            n_processed += 1
            continue

        mkt = _infer_market(code)
        px = fetch_last_close_stealth(str(code), mkt)
        if px is None:
            by_reason[reason_k]["n_fetch_failed"] += 1
            n_processed += 1
            continue

        d = _signed_defense_pct(ep, px)
        if d is None:
            n_processed += 1
            continue

        br = by_reason[reason_k]
        br["n_evaluated_price"] += 1
        br["sum_signed_defense_pct"] += float(d)
        if d > 0:
            br["n_positive_defense"] += 1
            br["sum_positive_defense_pct"] += float(d)
        elif d < 0:
            br["n_negative_opportunity_cost"] += 1
        n_processed += 1

    return {
        "by_reason": by_reason,
        "max_eval_cap": max_eval,
        "reason_event_counts": dict(reason_event_counts),
    }


def evaluate_smart_money_buff(vdf: pd.DataFrame) -> Dict[str, Any]:
    """virtual_trade_history 태그 vs 청산 실적(간접 매칭)."""
    out: Dict[str, Any] = {
        "win_rate_tagged": None,
        "win_rate_untagged": None,
        "delta_pct_pts": None,
        "n_tagged_matched": 0,
        "n_untagged_matched": 0,
    }
    if vdf is None or vdf.empty:
        return out

    try:
        conn = _connect_ro()
        try:
            ft = pd.read_sql(
                """
                SELECT market, code, entry_date, final_ret
                FROM forward_trades
                WHERE status LIKE 'CLOSED%' AND final_ret IS NOT NULL
                """,
                conn,
            )
        finally:
            conn.close()
    except Exception:
        return out

    if ft.empty:
        return out

    ft = ft.copy()
    ft["_cn"] = ft.apply(
        lambda r: str(r["code"]).split(".")[0].zfill(6)
        if str(r["market"]).upper() == "KR"
        else str(r["code"]).replace("/", "-").strip(),
        axis=1,
    )

    wins_tag, tot_tag = 0, 0
    wins_ut, tot_ut = 0, 0

    for _, vr in vdf.iterrows():
        tags = str(vr.get("satellite_tags") or "")
        smart_on = "SMART_MONEY_ACTIVE=yes" in tags
        code = str(vr.get("code") or "").strip()
        mkt = str(vr.get("market") or "").strip().upper()
        if mkt not in ("KR", "US"):
            mkt = _infer_market(code)
        log_at = str(vr.get("logged_at") or "")[:10]
        if len(log_at) < 10:
            continue

        ckr = str(code).zfill(6) if mkt == "KR" else str(code).replace("/", "-")
        sub = ft[(ft["market"].astype(str).str.upper() == mkt) & (ft["_cn"] == ckr)]
        if sub.empty:
            continue
        sub = sub.copy()
        sub["_ed"] = pd.to_datetime(sub["entry_date"].astype(str).str[:10], errors="coerce")
        ld = pd.to_datetime(log_at, errors="coerce")
        if pd.isna(ld):
            continue
        sub = sub[sub["_ed"].notna() & (sub["_ed"] >= ld - pd.Timedelta(days=3))]
        if sub.empty:
            continue
        row = sub.sort_values("_ed").iloc[0]
        try:
            fr = float(row["final_ret"])
        except (TypeError, ValueError):
            continue

        win = fr > 0
        if smart_on:
            tot_tag += 1
            if win:
                wins_tag += 1
        else:
            tot_ut += 1
            if win:
                wins_ut += 1

    out["n_tagged_matched"] = tot_tag
    out["n_untagged_matched"] = tot_ut
    if tot_tag > 0:
        out["win_rate_tagged"] = round(wins_tag / tot_tag * 100.0, 1)
    if tot_ut > 0:
        out["win_rate_untagged"] = round(wins_ut / tot_ut * 100.0, 1)
    if out["win_rate_tagged"] is not None and out["win_rate_untagged"] is not None:
        out["delta_pct_pts"] = round(
            float(out["win_rate_tagged"]) - float(out["win_rate_untagged"]), 1
        )

    return out


def run_shadow_performance_evaluation(
    blocked_limit: int = 400,
    max_price_checks: int = 200,
    virtual_limit: int = 1500,
) -> Dict[str, Any]:
    """메인 배치: 지표 계산 후 SHADOW_PERFORMANCE 페이로드 반환 및 저장."""
    rows = fetch_blocked_history(blocked_limit)
    blocked_stats = evaluate_blocked_trades(rows, max_eval=max_price_checks)
    vdf = fetch_virtual_trade_history(virtual_limit)
    smart_stats = evaluate_smart_money_buff(vdf)

    payload: Dict[str, Any] = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "blocked": blocked_stats,
        "smart_money_buff": smart_stats,
        "notes": "signed_defense_pct = (entry-current)/entry*100 per evaluated block",
    }

    cfg = _load_config_armored()
    if not isinstance(cfg, dict):
        cfg = {}
    cfg["SHADOW_PERFORMANCE"] = payload
    _save_config_atomic_armored(cfg)
    return payload


def run_shadow_snapshot_report() -> str:
    try:
        p = run_shadow_performance_evaluation()
        return json.dumps(p, ensure_ascii=False, indent=2)[:8000]
    except Exception as e:
        return f"[ShadowTracker] 오류: {e}"


if __name__ == "__main__":
    print(run_shadow_snapshot_report())
