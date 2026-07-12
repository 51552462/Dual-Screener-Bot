"""
Bitget AI 상시 감사관 — 23:50 KST 일일 리포트 + bounded Tier-2 reads.

Clock boundary (intentional):
  - `overseer_loop` / `gather_daily_system_facts` → Asia/Seoul (KST) audit calendar day.
  - `exit_date` in ledger is UTC date-only → SQL uses prev+cur UTC union for KST day overlap.
  - ops_events / trading logic elsewhere → `bitget.infra.clock` UTC SSOT.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytz
import requests
from dotenv import load_dotenv

load_dotenv()
from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
from bitget.governance.meta_consumer import load_meta_state_resolved
from bitget.infra.bounded_reads import overseer_daily_closed_sql, overseer_rnd_day_count_sql
from bitget.infra.data_paths import flow_csv_path, market_data_db_path
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import OVERSEER_CSV_STATUS_ROW_CAP
from bitget.infra.shared_db_connector import get_connection

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

DB_PATH = market_data_db_path()
CSV_PATH = flow_csv_path()
ALT_CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Supernova_Flow_Tracking_Master.csv")
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()
logger = get_logger("bitget.ai_overseer")


def load_config(max_retries=5):
    from bitget.infra import config_manager

    return config_manager.load_system_config() or {}


def send_telegram_alert(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        log_exception(logger, "telegram send failed: %s", e)


GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 규칙 감사 본문은 유지됩니다."


def _gemini_raw_fallback_response(contents: str, detail: str = "") -> SimpleNamespace:
    from llm_gemini_core import LlmCallSpec, deterministic_fallback

    body = deterministic_fallback(
        LlmCallSpec(task_id="bitget_overseer", user_payload=str(contents or "")[:200])
    )
    if detail:
        body += f" ({detail[:80]})"
    return SimpleNamespace(text=body)


def safe_generate_content(*, model, contents, max_retries=5):
    from llm_gemini_core import safe_generate_content as _core_safe

    return _core_safe(
        model=model,
        contents=contents,
        max_retries=max_retries,
        task_id="bitget_overseer",
    )


def _kst_today_str() -> str:
    """KST audit calendar day — intentional local TZ for operator daily report."""
    tz_kr = pytz.timezone("Asia/Seoul")
    return datetime.now(tz_kr).strftime("%Y-%m-%d")


def _csv_status_row_count(path: str) -> tuple[int, bool]:
    """Line-count CSV rows (header excluded) without loading full DataFrame."""
    cap = int(OVERSEER_CSV_STATUS_ROW_CAP)
    try:
        with open(path, encoding="utf-8-sig") as f:
            n = sum(1 for _ in f)
        data_rows = max(0, n - 1)
        if data_rows > cap:
            return cap, True
        return data_rows, False
    except OSError:
        return 0, False


def gather_daily_system_facts():
    today_str = _kst_today_str()
    report_data = {
        "date": today_str,
        "trades": {},
        "rnd_data_count": 0,
        "config_status": {},
        "csv_status": "Missing",
    }

    try:
        conn = get_connection(DB_PATH, read_only=True)
        closed_q, closed_params = overseer_daily_closed_sql(today=today_str)
        df_closed = pd.read_sql(closed_q, conn, params=closed_params)

        rnd_q, rnd_params = overseer_rnd_day_count_sql(today=today_str)
        rnd_row = conn.execute(rnd_q, rnd_params).fetchone()
        report_data["rnd_data_count"] = int(rnd_row[0] if rnd_row else 0)

        if not df_closed.empty:
            ret = pd.to_numeric(df_closed["final_ret"], errors="coerce").fillna(0.0)
            side = df_closed.get("position_side", pd.Series([], dtype=str)).astype(str).str.upper()
            report_data["trades"] = {
                "total_closed": int(len(df_closed)),
                "wins": int((ret > 0).sum()),
                "losses": int((ret <= 0).sum()),
                "avg_ret": float(round(float(ret.mean()), 2)),
                "long_count": int((side == "LONG").sum()),
                "short_count": int((side == "SHORT").sum()),
            }
        else:
            report_data["trades"] = "오늘 청산된 종목이 없습니다."
        conn.close()
    except Exception as e:
        report_data["db_error"] = str(e)

    try:
        config = load_config()
        meta = load_meta_state_resolved()
        report_data["config_status"] = {
            "regime": config.get("CURRENT_REGIME_KEY", "UNKNOWN"),
            "meta_regime": meta.get("META_REGIME_KEY", "UNKNOWN"),
            "meta_governor_last_run": meta.get("META_GOVERNOR_LAST_RUN_AT"),
            "meta_governor_status": meta.get("META_GOVERNOR_LAST_RUN_STATUS"),
            "kelly_risk": config.get("DYNAMIC_KELLY_RISK", 0),
            "meta_kelly_cap": (meta.get("META_REGIME_ACTION") or {}).get("kelly_cap"),
            "treasury_spot": config.get("TREASURY_SPOT_USDT", 0),
            "treasury_futures": config.get("TREASURY_FUTURES_USDT", 0),
            "breadth_status": config.get("CRYPTO_BREADTH_STATUS", "UNKNOWN"),
            "predicted_sector": config.get("PREDICTED_NEXT_SECTOR", "UNKNOWN"),
        }
    except Exception as e:
        report_data["config_error"] = str(e)

    try:
        if os.path.exists(CSV_PATH):
            n_rows, truncated = _csv_status_row_count(CSV_PATH)
            suffix = "+" if truncated else ""
            report_data["csv_status"] = f"정상 (누적 {n_rows}{suffix}행 보존 중)"
        elif os.path.exists(ALT_CSV_PATH):
            n_rows, truncated = _csv_status_row_count(ALT_CSV_PATH)
            suffix = "+" if truncated else ""
            report_data["csv_status"] = f"정상(대체 CSV) (누적 {n_rows}{suffix}행 보존 중)"
        else:
            report_data["csv_status"] = "🚨 경고: Bitget CSV 파일이 삭제되었거나 존재하지 않습니다."
    except Exception as e:
        report_data["csv_error"] = str(e)

    return report_data


def run_ai_auditor():
    logger.info("[AI overseer] system ledger scan start")
    facts = gather_daily_system_facts()

    prompt = f"""
    너는 코인 자동매매 시스템 전문 감사관이다.
    아래는 Bitget 팩토리의 오늘 팩트 데이터다.

    [시스템 팩트 데이터]
    {json.dumps(facts, indent=2, ensure_ascii=False)}

    [감사 지침]
    1. Long/Short 균형과 오늘 청산 성과를 평가한다.
    2. 코인 국면(regime), breadth, 켈리 비중이 일치하는지 점검한다.
    3. R&D 데이터 수집량과 CSV 생존 여부를 점검한다.
    4. 문제점과 강점을 짧고 날카롭게 작성한다.
    5. 시작 문구는 "👁️ [Bitget AI 상시 감사관 일일 리포트]" 로 시작한다.
    """

    try:
        ai_res = safe_generate_content(model="gemini-2.5-flash", contents=prompt)
        ai_text = (getattr(ai_res, "text", "") or "").strip() or f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{prompt}"
        send_telegram_alert(ai_text)
        logger.info("[AI overseer] telegram report sent")
    except Exception as e:
        log_exception(logger, "[AI overseer] gemini/analysis error: %s", e)
        send_telegram_alert(f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{prompt}\n\n(상세: {e})")


def overseer_loop():
    from bitget.infra.daemon_loop import (
        OVERSEER_ERROR_SEC,
        OVERSEER_POLL_SEC,
        OVERSEER_POST_AUDIT_SLEEP_SEC,
        LocalTick,
        sleep_or_backoff,
    )

    tz_kr = pytz.timezone("Asia/Seoul")
    tick = LocalTick(tz_kr)
    gemini_enabled = bool((os.environ.get("GEMINI_API_KEY") or "").strip())
    audit_hm_key = ""
    loop_error = False

    logger.info("[AI overseer] permanent audit loop waiting")
    logger.info(" - daily audit at 23:50 KST")
    if not gemini_enabled:
        logger.warning(
            "[AI disabled] GEMINI_API_KEY missing — loop stays up, 23:50 audit skipped"
        )

    while True:
        try:
            tick.refresh()
            if tick.hour == 23 and tick.minute == 50:
                if gemini_enabled and tick.hm_key != audit_hm_key:
                    run_ai_auditor()
                    audit_hm_key = tick.hm_key
                loop_error = False
                sleep_or_backoff(
                    normal_sec=OVERSEER_POST_AUDIT_SLEEP_SEC,
                    after_error=False,
                )
                continue
            loop_error = False
            sleep_or_backoff(normal_sec=OVERSEER_POLL_SEC, after_error=loop_error)
        except Exception as e:
            log_exception(logger, "overseer scheduler error: %s", e)
            loop_error = True
            sleep_or_backoff(
                normal_sec=OVERSEER_POLL_SEC,
                after_error=loop_error,
                error_sec=OVERSEER_ERROR_SEC,
            )


if __name__ == "__main__":
    overseer_loop()
