# ai_overseer.py
import os
import time
import random
import json
import sqlite3
from types import SimpleNamespace
import pandas as pd
from datetime import datetime, timedelta
import pytz
import requests

# 👇 Gemini는 보조 모듈 — 키/패키지 없어도 import·프로세스는 계속 가동 (지연 로드)
GEMINI_API_KEY = ""
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or ""
# 👆

import telegram_env

from overseer_audit_binder import (
    OVERSEER_LLM_SYSTEM_PROMPT,
    build_llm_narrative_prompt,
    build_overseer_audit_dossier,
    detect_audit_anomalies,
    format_overseer_audit_html,
)

# ==========================================
# 💡 [환경 설정 및 API 연결]
# ==========================================
TELEGRAM_TOKEN = telegram_env.get_overseer_token()
TELEGRAM_CHAT_ID = telegram_env.get_overseer_chat_id()

from factory_data_paths import flow_csv_path, system_config_json_path
from market_db_paths import market_db_read_path

DB_PATH = market_db_read_path()
CONFIG_PATH = system_config_json_path()
CSV_PATH = flow_csv_path()


def load_config(max_retries=5):
    """[장갑차 로직] JSONDecodeError 및 파일 잠금(Lock) 방어막 적용"""
    if not os.path.exists(CONFIG_PATH):
        return {}

    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 읽기 최종 실패 (동시 쓰기 과부하): {e}")
                return {}
    return {}


def send_telegram_alert(text):
    """최고 감시자의 경고 및 분석 리포트를 텔레그램으로 전송합니다."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        max_len = 4000
        chunks = [text[i : i + max_len] for i in range(0, len(text), max_len)]
        for chunk in chunks:
            res = requests.post(
                url,
                json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                timeout=10,
            )
            if res.status_code == 400:
                requests.post(
                    url,
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk},
                    timeout=10,
                )
            time.sleep(0.5)
    except Exception as e:
        print(f"텔레그램 발송 실패: {e}")


GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 규칙 감사 본문은 이미 전송되었습니다."

_GEMINI_HEARTBEAT: dict[str, object] = {
    "phase": "idle",
    "detail": "",
    "attempt": 0,
    "updated_mono": 0.0,
}


def gemini_heartbeat_snapshot() -> dict[str, object]:
    import time as _t

    out = dict(_GEMINI_HEARTBEAT)
    out["updated_mono"] = float(out.get("updated_mono") or 0.0)
    out["staleness_sec"] = round(_t.monotonic() - out["updated_mono"], 2) if out["updated_mono"] else None
    return out


def _gemini_hb_set(phase: str, detail: str = "", attempt: int = 0) -> None:
    import time as _t

    _GEMINI_HEARTBEAT["phase"] = phase
    _GEMINI_HEARTBEAT["detail"] = (detail or "")[:500]
    _GEMINI_HEARTBEAT["attempt"] = int(attempt)
    _GEMINI_HEARTBEAT["updated_mono"] = _t.monotonic()


def _gemini_raw_fallback_response(detail: str = "") -> SimpleNamespace:
    body = f"{GEMINI_RAW_FALLBACK_PREFIX}"
    if detail:
        body += f"\n\n(상세: {detail})"
    return SimpleNamespace(text=body)


def safe_generate_content(*, model, contents, max_retries=5):
    if not (os.environ.get("GEMINI_API_KEY") or "").strip():
        _gemini_hb_set("disabled", "GEMINI_API_KEY unset")
        return _gemini_raw_fallback_response("AI 비활성화(Gemini 미설정)")
    last_err = ""
    import google.generativeai as genai

    _gk = (os.environ.get("GEMINI_API_KEY") or "").strip().split(",")[0].strip()
    try:
        genai.configure(api_key=_gk)
    except Exception:
        pass
    for attempt in range(max_retries):
        try:
            _gemini_hb_set("rate_limit_sleep", "pre_call_sleep", attempt)
            time.sleep(3.5)
            _gemini_hb_set("calling", model or "", attempt)
            gmodel = genai.GenerativeModel(model)
            try:
                response = gmodel.generate_content(contents)
                _gemini_hb_set("idle", "ok")
                return response
            except Exception as gen_e:
                last_err = str(gen_e)
                err_lower = last_err.lower()
                if "429" in last_err or "RESOURCE_EXHAUSTED" in last_err or "quota" in err_lower:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 10 + random.uniform(1, 5)
                        _gemini_hb_set("backoff_429", last_err[:200], attempt)
                        send_telegram_alert(
                            f"⏳ [API 속도 조절 중] {wait_time:.1f}초 대기 후 재시도... ({attempt+1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                _gemini_hb_set("idle", f"fallback:{last_err[:120]}")
                return _gemini_raw_fallback_response(last_err)
        except Exception as e:
            _gemini_hb_set("idle", f"outer:{str(e)[:120]}")
            return _gemini_raw_fallback_response(str(e))

    _gemini_hb_set("idle", "retries_exhausted")
    return _gemini_raw_fallback_response(last_err or "재시도 소진")


def gather_daily_system_facts():
    """레거시 호환 — Rules-first dossier 로 대체됨. 수동 트리거용 최소 팩트."""
    cfg = load_config()
    try:
        from meta_governor_consumer import load_meta_state_resolved

        meta = load_meta_state_resolved()
    except Exception:
        meta = {}
    d = build_overseer_audit_dossier(
        sys_config=cfg,
        meta=meta,
        db_path=DB_PATH,
        csv_path=CSV_PATH,
    )
    return {
        "date": d.as_of_kst,
        "trades_closed_today": d.trades_closed_today,
        "trades_entry_today": d.trades_entry_today,
        "regime": d.meta_regime_key,
        "effective_kelly": d.effective_kelly_risk,
        "csv_status": d.csv_status,
    }


def run_ai_auditor():
    """Rules-first 감사(Anomaly) → SSOT 본문 → 선택적 LLM 해석."""
    print("👁️ [AI 최고 감시자] Rules-first 감사 스캔 중...")
    try:
        from factory_artifact_guard import ensure_factory_artifacts

        heal = ensure_factory_artifacts()
        print(f"🩹 [Self-Heal] pre-audit artifacts: {heal}")
    except Exception as e:
        print(f"⚠️ [Self-Heal] pre-audit guard skipped: {e}")
    cfg = load_config()
    try:
        from factory_artifact_guard import ensure_meta_governor_state
        from meta_governor_consumer import load_meta_state_resolved

        ensure_meta_governor_state()
        meta = load_meta_state_resolved()
    except Exception as e:
        print(f"⚠️ meta load: {e}")
        meta = {}

    dossier = build_overseer_audit_dossier(
        sys_config=cfg,
        meta=meta,
        db_path=DB_PATH,
        csv_path=CSV_PATH,
    )
    anomalies = detect_audit_anomalies(dossier, sys_config=cfg)

    msg = format_overseer_audit_html(dossier, anomalies)

    if (os.environ.get("GEMINI_API_KEY") or "").strip():
        try:
            user_prompt = build_llm_narrative_prompt(dossier, anomalies)
            full_prompt = f"{OVERSEER_LLM_SYSTEM_PROMPT}\n\n{user_prompt}"
            ai_res = safe_generate_content(
                model="gemini-2.5-flash",
                contents=full_prompt,
            )
            ai_text = (getattr(ai_res, "text", None) or "").strip()
            if ai_text and GEMINI_RAW_FALLBACK_PREFIX not in ai_text:
                msg += "━━━ <b>[LLM 해석 · Ruthless QA]</b> ━━━\n"
                msg += ai_text + "\n"
            elif ai_text:
                msg += f"\n<i>{ai_text}</i>\n"
        except Exception as e:
            msg += f"\n<i>LLM 해석 스킵: {e}</i>\n"
    else:
        msg += "\n<i>LLM 비활성(GEMINI_API_KEY 없음) — 규칙 감사만 발송.</i>\n"

    send_telegram_alert(msg)
    print("✅ [AI 최고 감시자] 텔레그램 직보 완료.")


def overseer_loop():
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🛡️ [AI 상시 감사관 시스템] 영구 가동 대기 중...")
    print(" - 매일 23:50 에 Rules-first 감사 + 선택 LLM 해석 발송")
    if not (os.environ.get("GEMINI_API_KEY") or "").strip():
        print("⚠️ [AI 비활성화] API 키가 없어도 규칙 감사(Anomaly)는 발송됩니다.")

    while True:
        try:
            now = datetime.now(tz_kr)
            if now.hour == 23 and now.minute == 50:
                run_ai_auditor()
                time.sleep(65)

            time.sleep(30)
        except Exception as e:
            print(f"감시자 스케줄러 에러: {e}")
            time.sleep(60)


if __name__ == "__main__":
    overseer_loop()
