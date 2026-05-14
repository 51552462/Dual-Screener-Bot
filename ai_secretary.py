# ai_secretary.py (한/미/신규 통합 AI Q&A 비서 - 최종 완벽 확정)
import os
import time
import requests
import threading
import traceback
from datetime import datetime

GEMINI_API_KEY = ""
genai = None  # type: ignore
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or ""
try:
    if GEMINI_API_KEY.strip():
        import google.generativeai as _genai_mod

        _genai_mod.configure(api_key=GEMINI_API_KEY.strip().split(",")[0].strip())
        genai = _genai_mod
except Exception:
    genai = None

# ==========================================
# 🤖 2. 텔레그램 봇 토큰 — .env → telegram_env (시장별 비서)
# ==========================================
import telegram_env

KR_TOKEN = telegram_env.get_secretary_kr_token()
US_TOKEN = telegram_env.get_secretary_us_token()
NEW_TOKEN = telegram_env.get_secretary_new_token()

# 💡 봇 내부 트래픽 통제용 락 (무분별한 API 호출 방어)
ai_request_lock = threading.Lock()

def listen_and_reply(token, market_name):
    last_update_id = 0
    print(f"🤖 [{market_name} AI 비서] 텔레그램 질문 수신 대기 중...")
    
    while True:
        try:
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 10}
            res = requests.get(url, params=params, timeout=45).json()
            
            if res.get("ok"):
                for item in res.get("result", []):
                    last_update_id = item["update_id"]
                    msg = item.get("message", {})
                    if not msg: continue
                    
                    chat_id = msg.get("chat", {}).get("id")
                    text = msg.get("text", "")
                    
                    if text.startswith("/질문"):
                        question = text.replace("/질문", "").strip()
                        if question:
                            print(f"\n💡 [{market_name}] 질문 수신: {question}")
                            try:
                                # 타이핑 중(...) 액션 보내기
                                requests.post(f"https://api.telegram.org/bot{token}/sendChatAction", json={"chat_id": chat_id, "action": "typing"}, timeout=5)
                                
                                today_date = datetime.now().strftime('%Y년 %m월 %d일')
                                prompt = f"""너는 여의도와 월스트리트를 아우르는 냉철한 탑 애널리스트야.
오늘 날짜는 {today_date}이야. 반드시 최신 구글 검색 결과를 바탕으로 팩트만 짧고 명확하게 답변해.
질문: {question}"""
                                
                                ai_text = ""
                                # ⭐️ 동시에 질문이 들어와도 하나씩 차분히 처리하도록 락(Lock) 걸기
                                with ai_request_lock:
                                    try:
                                        gmodel = genai.GenerativeModel('gemini-2.5-flash', tools='google_search_retrieval')
                                        try:
                                            ai_res = gmodel.generate_content(prompt)
                                        except Exception as gen_e:
                                            err_msg = str(gen_e)
                                            print(f"❌ [{market_name}] AI generate_content 실패: {err_msg}")
                                            ai_text = f"⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:\n\n{prompt}"
                                        else:
                                            time.sleep(2) # 무료 한도 보호용 강제 휴식
                                            ai_text = ai_res.text.strip() if ai_res.text else f"⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:\n\n{prompt}"
                                        
                                    except Exception as ai_e:
                                        err_msg = str(ai_e)
                                        print(f"❌ [{market_name}] AI 에러: {err_msg}")
                                        ai_text = f"⚠️ [AI 요약 실패 - API 한도 초과] 아래는 원본 데이터입니다:\n\n{prompt}"
                                
                                # 답변 전송
                                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": f"🤖 [AI 비서 답변]\n\n{ai_text}", "reply_to_message_id": msg.get("message_id")}, timeout=10)
                                print(f"✅ [{market_name}] 답변 전송 완료!")
                                
                            except Exception as inner_e:
                                print(f"❌ [{market_name}] 텔레그램 전송 중 에러 발생: {inner_e}")
                                
        except Exception as e:
            time.sleep(2)

def run_secretary():
    if genai is None:
        print("⚠️ [AI 비활성화] API 키가 없어 해당 기능을 스킵합니다.")
        while True:
            time.sleep(60)
        return
    threading.Thread(target=listen_and_reply, args=(KR_TOKEN, "한국장"), daemon=True).start()
    threading.Thread(target=listen_and_reply, args=(US_TOKEN, "미국장"), daemon=True).start()
    threading.Thread(target=listen_and_reply, args=(NEW_TOKEN, "신규방"), daemon=True).start()
    
    while True:
        time.sleep(60)

if __name__ == "__main__":
    run_secretary()
