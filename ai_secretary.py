# ai_secretary.py (한/미 통합 AI Q&A 비서 - 2.5-flash 최종 확정)
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
# 🤖 2. 텔레그램 봇 토큰
# ==========================================
KR_TOKEN = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
US_TOKEN = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"
KR_TOKEN = "8004222500:AAFS9rPPtiQiNx4SxGgYOnODFGULqLTNO8M"

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
                                
                                # ==========================================
                                # 📅 3. 실시간 날짜 파악 및 프롬프트 생성
                                # ==========================================
                                today_date = datetime.now().strftime('%Y년 %m월 %d일')
                                prompt = f"""너는 여의도와 월스트리트를 아우르는 냉철한 탑 애널리스트야.
오늘 날짜는 {today_date}이야. 반드시 최신 구글 검색 결과를 바탕으로 팩트만 짧고 명확하게 답변해.
질문: {question}"""
                                
                                # ==========================================
                                # 🔎 4. 구글 검색 엔진(Grounding) 장착
                                # ==========================================
                                ai_res = client.models.generate_content(
                                    model='gemini-2.5-flash',
                                    contents=prompt,
                                    config=types.GenerateContentConfig(
                                        tools=[{"google_search": {}}] # 구글 검색 기능 켜기
                                    )
                                )
                                
                                ai_text = ai_res.text.strip() if ai_res.text else "⚠️ 답변을 생성하지 못했습니다."
                                
                                # 답변 전송
                                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": f"🤖 [AI 비서 팩트체크]\n\n{ai_text}", "reply_to_message_id": msg.get("message_id")}, timeout=10)
                                print(f"✅ [{market_name}] 답변 전송 완료!")
                                
                            except Exception as inner_e:
                                print(f"❌ [{market_name}] AI 처리 중 에러 발생:")
                                traceback.print_exc()
                                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": f"⚠️ AI 서버 처리 에러: {inner_e}", "reply_to_message_id": msg.get("message_id")}, timeout=5)
        
        except Exception as e:
            # ==========================================
            # 🚨 5. 미국장 봇 침묵 원인 추적용 에러 출력
            # ==========================================
            print(f"❌ [{market_name}] 텔레그램 통신/수신 에러: {e}")
            time.sleep(2)
        
        time.sleep(1.5)

# 한국장과 미국장 봇을 각각의 독립된 스레드에서 동시 실행
threading.Thread(target=listen_and_reply, args=(KR_TOKEN, "한국장"), daemon=True).start()
threading.Thread(target=listen_and_reply, args=(US_TOKEN, "미국장"), daemon=True).start()

while True:
    time.sleep(60)
