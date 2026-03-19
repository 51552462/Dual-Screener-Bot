# ai_secretary.py (한/미/신규 통합 AI Q&A 비서)
import os
import time
import requests
import threading
import traceback
from datetime import datetime

from dotenv import load_dotenv
from google import genai
from google.genai import types

# ==========================================
# 🔑 1. API 키 세팅 (.env 안전 파일 방식 적용)
# ==========================================
load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

client = genai.Client(api_key=GEMINI_API_KEY)

# ==========================================
# 🤖 2. 텔레그램 봇 토큰 (3개로 완벽 분리)
# ==========================================
KR_TOKEN = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
US_TOKEN = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
NEW_TOKEN = "8004222500:AAFS9rPPtiQiNx4SxGgYOnODFGULqLTNO8M"

# 💡 봇 내부 트래픽 통제용 락
ai_request_lock = threading.Lock()

def listen_and_reply(token, market_name):
    last_update_id = 0
    print(f"🤖 [{market_name} AI 비서] 텔레그램 질문 수신 대기 중...")
    
    while True:
        try:
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 10}
            res = requests.get(url, params=params, timeout=15).json()
            
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
                                with ai_request_lock:
                                    try:
                                        ai_res = client.models.generate_content(
                                            model='gemini-2.5-flash',
                                            contents=prompt,
                                            config=types.GenerateContentConfig(
                                                tools=[{"google_search": {}}] # 구글 검색 기능 켜기
                                            )
                                        )
                                        time.sleep(2) # 무료 한도 보호용 강제 휴식
                                        ai_text = ai_res.text.strip() if ai_res.text else "⚠️ 답변을 생성하지 못했습니다."
                                        
                                    except Exception as ai_e:
                                        err_msg = str(ai_e)
                                        # ⭐️ 트래픽 초과 시 지저분한 영어 에러 대신 깔끔하게 방어!
                                        if 'Quota exceeded' in err_msg:
                                            ai_text = "⚠️ [AI 시스템 알림]\n오늘 구글 AI가 답변할 수 있는 일일 한도가 모두 소진되었습니다. 내일 다시 질문해 주세요!"
                                        elif '429' in err_msg or 'RESOURCE_EXHAUSTED' in err_msg:
                                            ai_text = "⏳ [AI 시스템 알림]\n현재 다른 검색기 봇들이 AI를 동시에 사용 중이라 트래픽이 몰렸습니다. 1분 정도 후에 다시 질문해 주세요!"
                                        else:
                                            ai_text = "❌ AI 서버에서 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."
                                            print(f"❌ [{market_name}] AI 에러: {err_msg}")
                                
                                # 답변 전송
                                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": f"🤖 [AI 비서 답변]\n\n{ai_text}", "reply_to_message_id": msg.get("message_id")}, timeout=10)
                                print(f"✅ [{market_name}] 답변 전송 완료!")
                                
                            except Exception as inner_e:
                                print(f"❌ [{market_name}] 텔레그램 전송 중 에러 발생: {inner_e}")
                                
        except Exception as e:
            time.sleep(2)
        
        time.sleep(1.5)

threading.Thread(target=listen_and_reply, args=(KR_TOKEN, "한국장"), daemon=True).start()
threading.Thread(target=listen_and_reply, args=(US_TOKEN, "미국장"), daemon=True).start()
threading.Thread(target=listen_and_reply, args=(NEW_TOKEN, "신규방"), daemon=True).start()

while True:
    time.sleep(60)
