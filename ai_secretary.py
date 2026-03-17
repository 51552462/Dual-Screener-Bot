# ai_secretary.py (한/미 통합 AI Q&A 비서 - 절대 멈추지 않는 무적 버전)
import time
import requests
import threading
import traceback
from google import genai

# ==========================================
# 🔑 대표님 세팅 (Gemini API 키 입력)
# ==========================================
GEMINI_API_KEY = "AIzaSyAagV9SDlZ72CUmYK8JDZaP937CeHrqV7Q"

# API 키 누락 방어망
if "여기에" in GEMINI_API_KEY:
    print("🚨 [치명적 에러] GEMINI_API_KEY가 입력되지 않았습니다. 코드를 열고 키를 넣어주세요!")

client = genai.Client(api_key=GEMINI_API_KEY)

# 🇰🇷 한국장 봇 토큰
KR_TOKEN = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
# 🇺🇸 미국장 봇 토큰
US_TOKEN = "7791873924:AAHcaajPux8r0KVydUqpQjaqAeYlwxrZ7tg"

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
                            
                            # ⭐️ 개별 메시지 처리 블록 (에러가 나도 봇이 안 죽고 다음 질문을 받음)
                            try:
                                # 1. 타이핑 액션 (타임아웃 5초 강제 지정)
                                requests.post(f"https://api.telegram.org/bot{token}/sendChatAction", 
                                              json={"chat_id": chat_id, "action": "typing"}, timeout=5)
                                
                                prompt = f"너는 여의도와 월스트리트를 아우르는 냉철한 탑 애널리스트야. 다음 주식 관련 질문에 팩트 기반으로 짧고 명확하게 답변해줘.\n질문: {question}"
                                
                                # 2. AI 답변 생성 
                                ai_res = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                                ai_text = ai_res.text.strip() if ai_res.text else "⚠️ 답변을 생성하지 못했습니다."
                                
                                # 3. 답변 전송 (타임아웃 10초 강제 지정)
                                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                                              json={"chat_id": chat_id, "text": f"🤖 [AI 비서 팩트체크]\n\n{ai_text}", "reply_to_message_id": msg.get("message_id")}, 
                                              timeout=10)
                                print(f"✅ [{market_name}] 답변 전송 완료!")
                                
                            except Exception as inner_e:
                                # ⭐️ 에러의 진짜 원인을 숨기지 않고 터미널에 낱낱이 출력
                                print(f"❌ [{market_name}] AI 처리 중 에러 발생:")
                                traceback.print_exc()
                                
                                # 텔레그램으로도 에러 났다고 보고하기
                                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                                              json={"chat_id": chat_id, "text": "⚠️ AI 팩트체크 중 서버 에러가 발생했습니다. (API 키 오류 또는 구글 서버 지연)", "reply_to_message_id": msg.get("message_id")}, 
                                              timeout=5)
        except Exception as e:
            time.sleep(2)
        time.sleep(1.5)

# 두 봇의 귀를 동시에 열어서 백그라운드 가동
threading.Thread(target=listen_and_reply, args=(KR_TOKEN, "한국장"), daemon=True).start()
threading.Thread(target=listen_and_reply, args=(US_TOKEN, "미국장"), daemon=True).start()

while True:
    time.sleep(60)
