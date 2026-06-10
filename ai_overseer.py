# ai_overseer.py — Rules-first 감사 + LLM 해석 (llm_gemini_core SSOT)
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

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or ""

import telegram_env

from overseer_audit_binder import (
    OVERSEER_LLM_SYSTEM_PROMPT,
    build_llm_narrative_prompt,
    build_overseer_audit_dossier,
    detect_audit_anomalies,
    format_overseer_audit_html,
)

TELEGRAM_TOKEN = telegram_env.get_overseer_token()
TELEGRAM_CHAT_ID = telegram_env.get_overseer_chat_id()

from factory_data_paths import flow_csv_path, system_config_json_path
from market_db_paths import market_db_read_path

DB_PATH = market_db_read_path()
CONFIG_PATH = system_config_json_path()
CSV_PATH = flow_csv_path()

GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 규칙 감사 본문은 이미 전송되었습니다."

_GEMINI_HEARTBEAT: dict[str, object] = {
    "phase": "idle",
    "detail": "",
    "attempt": 0,
    "updated_mono": 0.0,
}


def load_config(max_retries=5):
    """SQLite config_kv SSOT — system_config.json 직접 읽기 금지 (Split-Brain 제거)."""
    try:
        from config_manager import load_system_config

        return load_system_config(max_retries=max_retries) or {}
    except Exception as e:
        print(f"🚨 [치명적 방어] 관제탑 뇌(SQLite SSOT) 읽기 최종 실패: {e}")
        return {}


def send_telegram_alert(text):
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
    """llm_gemini_core SSOT 위임."""
    from llm_gemini_core import safe_generate_content as _core_safe

    _gemini_hb_set("calling", model or "", 0)
    res = _core_safe(
        model=model,
        contents=contents,
        max_retries=max_retries,
        task_id="legacy",
    )
    _gemini_hb_set("idle", "ok")
    return res


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
    print("🛡️ [AI 최고 감시자] Rules-first 감사 스캔 중...")
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

    try:
        from meta_state_store import is_meta_state_degraded

        if is_meta_state_degraded(meta):
            rk = meta.get("META_REGIME_KEY", "UNKNOWN")
            st = meta.get("META_GOVERNOR_LAST_RUN_STATUS", "NEVER")
            at = meta.get("META_GOVERNOR_LAST_RUN_AT", "—")
            raise RuntimeError(
                "overseer blocked: meta state degraded "
                f"(regime={rk} status={st} last_at={at}) — "
                "fix meta_governor_sync before audit report"
            )
    except RuntimeError:
        raise
    except Exception as e:
        print(f"⚠️ meta degraded check skipped: {e}")

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
            from llm_gemini_core import LlmCallSpec, generate_text_sync

            user_prompt = build_llm_narrative_prompt(dossier, anomalies)
            spec = LlmCallSpec(
                task_id="overseer_audit",
                system_prompt=OVERSEER_LLM_SYSTEM_PROMPT,
                user_payload=user_prompt,
                model="gemini-2.5-flash",
                timeout_sec=75.0,
                max_attempts=2,
            )
            ai_res = generate_text_sync(spec, max_wait_sec=180.0)
            ai_text = (ai_res.text or "").strip()
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
    tz_kr = pytz.timezone("Asia/Seoul")
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
