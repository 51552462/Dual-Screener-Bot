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

# 👇👇 [수정] 대표님이 쓰시는 안전한 .env 방식 및 google-generativeai 표준 임포트 👇👇
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

genai.configure(api_key=GEMINI_API_KEY)
# 👆👆 [적용 완료] 👆👆

# ==========================================
# 💡 [환경 설정 및 API 연결]
# ==========================================
TELEGRAM_TOKEN = "8709452406:AAHGVhTN8hu1ujA_xYUR8GvMPrd-qpMoSRk"
TELEGRAM_CHAT_ID = "6838834566"

# (이하 DB_PATH 등 시스템 경로 설정 코드는 그대로 유지...)

# 시스템 전역 경로 매핑 (기존 시스템과 100% 호환)
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')
CSV_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Supernova_Flow_Tracking_Master.csv')


def load_config(max_retries=5):
    """
    [장갑차 로직] JSONDecodeError 및 파일 잠금(Lock) 방어막 적용
    """
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
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"텔레그램 발송 실패: {e}")

GEMINI_RAW_FALLBACK_PREFIX = "⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:"


def _gemini_raw_fallback_response(contents: str, detail: str = "") -> SimpleNamespace:
    body = f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{contents}"
    if detail:
        body += f"\n\n(상세: {detail})"
    return SimpleNamespace(text=body)


def safe_generate_content(*, model, contents, max_retries=5):
    """429 에러 발생 시 지수 백오프 후, 최종 실패 시에도 원본 프롬프트를 담아 절대 None을 반환하지 않음."""
    last_err = ""
    for attempt in range(max_retries):
        try:
            # 💡 [핵심] API 호출 전 기본적으로 3.5초를 쉬어서 1분에 17회 이상 호출되지 않도록 강제 속도 조절
            time.sleep(3.5)
            gmodel = genai.GenerativeModel(model)
            try:
                response = gmodel.generate_content(contents)
                return response
            except Exception as gen_e:
                last_err = str(gen_e)
                err_lower = last_err.lower()
                if "429" in last_err or "RESOURCE_EXHAUSTED" in last_err or "quota" in err_lower:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 10 + random.uniform(1, 5)
                        send_telegram_alert(f"⏳ [API 속도 조절 중] 구글 제한에 걸려 {wait_time:.1f}초 대기 후 재시도합니다... (시도 {attempt+1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                return _gemini_raw_fallback_response(contents, last_err)
        except Exception as e:
            return _gemini_raw_fallback_response(contents, str(e))

    send_telegram_alert("🚨 [API 감시자 에러] 5회 재시도에도 불구하고 API 통신에 실패했습니다. 원본 데이터로 대체 전송합니다.")
    return _gemini_raw_fallback_response(contents, last_err or "재시도 소진")

def gather_daily_system_facts():
    """시스템의 3대 장부(DB, JSON, CSV)에서 오늘 하루의 '팩트'만 무결성으로 추출합니다."""
    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    
    report_data = {
        "date": today_str,
        "trades": {},
        "overdrive_count": 0,
        "rnd_data_count": 0,
        "config_status": {},
        "csv_status": "Missing"
    }

    # 1. SQLite 장부 (forward_trades) 스캔
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        
        # 오늘 청산된 종목 데이터
        df_closed = pd.read_sql(f"SELECT * FROM forward_trades WHERE exit_date = '{today_str}'", conn)
        
        # 오늘 진입한 R&D 샌드박스 데이터
        df_rnd = pd.read_sql(f"SELECT * FROM forward_trades WHERE entry_date = '{today_str}' AND sig_type LIKE '%[R&D_%'", conn)
        
        report_data["rnd_data_count"] = len(df_rnd)
        
        if not df_closed.empty:
            report_data["trades"]["total_closed"] = len(df_closed)
            report_data["trades"]["wins"] = len(df_closed[df_closed['final_ret'] > 0])
            report_data["trades"]["loses"] = len(df_closed[df_closed['final_ret'] <= 0])
            
            # Numpy float64 직렬화 에러 방지를 위해 기본 float 강제 형변환
            avg_val = df_closed['final_ret'].mean()
            report_data["trades"]["avg_ret"] = float(round(avg_val, 2)) if not pd.isna(avg_val) else 0.0
            
            # 오버드라이브 발동 여부 팩트 체크
            od_trades = df_closed[df_closed['exit_reason'].str.contains('오버드라이브', na=False)]
            report_data["overdrive_count"] = len(od_trades)
        else:
            report_data["trades"] = "오늘 청산된 종목이 없습니다."
            
        conn.close()
    except Exception as e:
        report_data["db_error"] = str(e)

    # 2. 관제탑 JSON (system_config.json) 스캔
    try:
        if os.path.exists(CONFIG_PATH):
            config = load_config()
            report_data["config_status"] = {
                "regime": config.get("CURRENT_REGIME_KEY", "UNKNOWN"),
                "kelly_risk": config.get("DYNAMIC_KELLY_RISK", 0),
                "treasury_kr": config.get("CENTRAL_TREASURY_KR", 0),
                "treasury_us": config.get("CENTRAL_TREASURY_US", 0),
                "supernova_cutoff": config.get("DYNAMIC_SUPERNOVA_CUTOFF", 0),
                "predicted_sector": config.get("PREDICTED_NEXT_SECTOR", "UNKNOWN")
            }
    except Exception as e:
        report_data["config_error"] = str(e)

    # 3. CSV 마이닝 데이터 증발 여부 스캔
    try:
        if os.path.exists(CSV_PATH):
            df_csv = pd.read_csv(CSV_PATH)
            report_data["csv_status"] = f"정상 (누적 {len(df_csv)}행 보존 중)"
        else:
            report_data["csv_status"] = "🚨 경고: CSV 파일이 삭제되었거나 존재하지 않습니다."
    except Exception as e:
        report_data["csv_error"] = str(e)

    return report_data

def run_ai_auditor():
    """추출된 팩트 데이터를 Gemini API에 먹여 분석시키고 텔레그램으로 보고합니다."""
    print("👁️ [AI 최고 감시자] 시스템 장부 스캔 및 분석 중...")
    facts = gather_daily_system_facts()
    
    # 저(Gemini)에게 내려질 시스템 감사 프롬프트
    prompt = f"""
    너는 전 세계 상위 1% 퀀트 시스템 분석가이자 '최고 감시자(Overseer)'야.
    아래는 오늘 우리 자동매매 시스템이 만들어낸 데이터 장부(DB, JSON, CSV)의 팩트 요약이야.

    [시스템 팩트 데이터]
    {json.dumps(facts, indent=2, ensure_ascii=False)}

    [감사 지침]
    1. 오버드라이브 로직이 제대로 작동했는지 확인해. (청산이 있었는데 오버드라이브가 0건이면 휩소에 털렸거나 로직이 닫혔을 가능성 경고)
    2. R&D 데이터 수집량과 CSV 파일 생존 여부를 확인해. (데이터가 증발했으면 치명적 버그로 경고)
    3. 현재 거시 국면(Regime)과 켈리 리스크 비중이 시장 상황에 맞게 잘 조율되어 있는지 평가해.
    4. 분석은 짧고, 팩트 기반으로, 날카롭고 직관적으로 작성해. 불필요한 인사는 생략하고 핵심 문제점이나 잘된 점만 집어내.
    5. 보고서 양식을 "👁️ [AI 상시 감사관 일일 리포트]" 로 시작해줘.
    """

    try:
        # 👇👇 [수정] google-generativeai 표준(GenerativeModel.generate_content) 호출 👇👇
        ai_res = safe_generate_content(
            model='gemini-2.5-flash', # 대표님이 사용하시는 최신 속도형 모델 적용
            contents=prompt,
        )
        # 텔레그램 직보 (safe_generate_content는 항상 .text 보유 — 실패 시 원본 프롬프트 폴백)
        ai_text = (getattr(ai_res, "text", None) or "").strip() or f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{prompt}"
        send_telegram_alert(ai_text)
        print("✅ [AI 최고 감시자] 텔레그램 직보 완료.")
        # 👆👆 [적용 완료] 👆👆
        
    except Exception as e:
        err_msg = f"🚨 <b>[AI 감시자 에러]</b> Gemini API 통신 또는 분석 중 오류 발생:\n{e}"
        print(err_msg)
        send_telegram_alert(f"{GEMINI_RAW_FALLBACK_PREFIX}\n\n{prompt}\n\n(상세: {e})")

# ==========================================
# 🕒 [자정 감시 스케줄러]
# ==========================================
def overseer_loop():
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🛡️ [AI 상시 감사관 시스템] 영구 가동 대기 중...")
    print(" - 매일 23:50 에 시스템 전역을 부검하고 분석 리포트를 발송합니다.")
    
    while True:
        try:
            now = datetime.now(tz_kr)
            # 매일 밤 23시 50분 (하루가 끝나기 직전) 시스템 총점검
            if now.hour == 23 and now.minute == 50:
                run_ai_auditor()
                time.sleep(65) # 중복 실행 방지
            
            time.sleep(30)
        except Exception as e:
            print(f"감시자 스케줄러 에러: {e}")
            time.sleep(60)

if __name__ == "__main__":
    overseer_loop()
