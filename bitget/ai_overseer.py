import json
import os
import random
import sqlite3
import time
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytz
import requests
from dotenv import load_dotenv

load_dotenv()
from bitget.env import bitget_telegram_chat_id, bitget_telegram_token

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")
CSV_PATH = os.path.join(BASE_DIR, "bitget_supernova_flow_tracking_master.csv")
ALT_CSV_PATH = os.path.join(BASE_DIR, "Supernova_Flow_Tracking_Master.csv")
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()


def load_config(max_retries=5):
    if not os.path.exists(CONFIG_PATH):
        return {}
    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] Bitget 설정(JSON) 읽기 최종 실패: {e}")
                return {}
    return {}


def send_telegram_alert(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"텔레그램 발송 실패: {e}")


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


def gather_daily_system_facts():
    tz_kr = pytz.timezone("Asia/Seoul")
    today_str = datetime.now(tz_kr).strftime("%Y-%m-%d")
    report_data = {
        "date": today_str,
        "trades": {},
        "rnd_data_count": 0,
        "config_status": {},
        "csv_status": "Missing",
    }

    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        df_closed = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE entry_date <= ? AND status LIKE 'CLOSED%' AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'",
            conn,
            params=(today_str,),
        )
        if "exit_date" in df_closed.columns:
            df_closed = df_closed[df_closed["exit_date"].astype(str).str.startswith(today_str)]

        df_rnd = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE entry_date=? AND sig_type LIKE '%[R&D_%'",
            conn,
            params=(today_str,),
        )
        report_data["rnd_data_count"] = int(len(df_rnd))

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
        report_data["config_status"] = {
            "regime": config.get("CURRENT_REGIME_KEY", "UNKNOWN"),
            "kelly_risk": config.get("DYNAMIC_KELLY_RISK", 0),
            "treasury_spot": config.get("TREASURY_SPOT_USDT", 0),
            "treasury_futures": config.get("TREASURY_FUTURES_USDT", 0),
            "breadth_status": config.get("CRYPTO_BREADTH_STATUS", "UNKNOWN"),
            "predicted_sector": config.get("PREDICTED_NEXT_SECTOR", "UNKNOWN"),
        }
    except Exception as e:
        report_data["config_error"] = str(e)

    try:
        if os.path.exists(CSV_PATH):
            df_csv = pd.read_csv(CSV_PATH)
            report_data["csv_status"] = f"정상 (누적 {len(df_csv)}행 보존 중)"
        elif os.path.exists(ALT_CSV_PATH):
            df_csv = pd.read_csv(ALT_CSV_PATH)
            report_data["csv_status"] = f"정상(대체 CSV) (누적 {len(df_csv)}행 보존 중)"
        else:
            report_data["csv_status"] = "🚨 경고: Bitget CSV 파일이 삭제되었거나 존재하지 않습니다."
    except Exception as e:
        report_data["csv_error"] = str(e)

    return report_data


def run_ai_auditor():
    print("👁️ [Bitget AI 최고 감시자] 시스템 장부 스캔 및 분석 중...")
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
        print("✅ [Bitget AI 최고 감시자] 텔레그램 직보 완료.")
    except Exception as e:
        err_msg = f"🚨 <b>[Bitget AI 감시자 에러]</b> Gemini 통신/분석 중 오류:\n{e}"
        print(err_msg)
        send_telegram_alert(f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{prompt}\n\n(상세: {e})")


def overseer_loop():
    tz_kr = pytz.timezone("Asia/Seoul")
    print("🛡️ [Bitget AI 상시 감사관 시스템] 영구 가동 대기 중...")
    print(" - 매일 23:50 KST 에 시스템 전역을 부검하고 분석 리포트를 발송합니다.")
    if not (os.environ.get("GEMINI_API_KEY") or "").strip():
        print("⚠️ [AI 비활성화] GEMINI_API_KEY 미설정 — 감사 루프는 유지되나 23:50 감사 스킵.")

    while True:
        try:
            now = datetime.now(tz_kr)
            if now.hour == 23 and now.minute == 50:
                if (os.environ.get("GEMINI_API_KEY") or "").strip():
                    run_ai_auditor()
                time.sleep(65)
            time.sleep(30)
        except Exception as e:
            print(f"감시자 스케줄러 에러: {e}")
            time.sleep(60)


if __name__ == "__main__":
    overseer_loop()
