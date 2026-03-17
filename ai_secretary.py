# ai_secretary.py (한/미 통합 AI Q&A 비서)
import time
import requests
import threading
from google import genai

# ==========================================
# 🔑 대표님 세팅 (Gemini API 키 입력)
# ==========================================
GEMINI_API_KEY = "AIzaSyAagV9SDlZ72CUmYK8JDZaP937CeHrqV7Q"
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
                    
                    # '/질문' 이라는 단어로 시작할 때만 반응
                    if text.startswith("/질문"):
                        question = text.replace("/질문", "").strip()
                        if question:
                            print(f"💡 [{market_name}] 질문 수신: {question}")
                            
                            # 봇이 '타이핑 중...' 상태임을 텔레그램에 표시
                            requests.post(f"https://api.telegram.org/bot{token}/sendChatAction", json={"chat_id": chat_id, "action": "typing"})
                            
                            prompt = f"""
                            너는 여의도와 월스트리트를 아우르는 냉철한 탑 애널리스트야. 
                            다음 주식 관련 질문에 팩트 기반으로 짧고 명확하게 답변해줘. 종목 추천은 절대 하지마.
                            질문: {question}
                            """
                            ai_res = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                            
                            reply_url = f"https://api.telegram.org/bot{token}/sendMessage"
                            requests.post(reply_url, json={
                                "chat_id": chat_id, 
                                "text": f"🤖 [AI 비서 팩트체크]\n\n{ai_res.text.strip()}", 
                                "reply_to_message_id": msg.get("message_id")
                            })
                            print(f"✅ [{market_name}] 답변 완료")
        except Exception as e:
            time.sleep(2)
        time.sleep(1)

# 두 봇의 귀를 동시에 열어서 백그라운드 가동 (멀티스레딩)
threading.Thread(target=listen_and_reply, args=(KR_TOKEN, "한국장"), daemon=True).start()
threading.Thread(target=listen_and_reply, args=(US_TOKEN, "미국장"), daemon=True).start()

# 프로그램이 종료되지 않도록 유지
while True:
    time.sleep(60)