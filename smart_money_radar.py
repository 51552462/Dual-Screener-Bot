import os
import json
import FinanceDataReader as fdr
from datetime import datetime, timedelta

CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')

def load_config():
    if not os.path.exists(CONFIG_PATH): return {}
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f: return json.load(f)

def save_config(config):
    temp_path = f"{CONFIG_PATH}.temp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CONFIG_PATH)
    except: pass

def run_smart_money_scan():
    print("🕵️ [스마트 머니 레이더] 기관/외인 수급 폭발 종목 스캔 중...")
    
    # 예시: KRX 전체 종목 중 외국인/기관 수급 강도(또는 거래대금 상위)를 스캔하는 로직
    # (실제 API나 크롤링 연동 시 이 부분을 고도화)
    try:
        df = fdr.StockListing('KRX')
        # 임시 시뮬레이션: 시총 상위 중 수급이 몰린다고 가정된 종목 5개 추출
        smart_money_picks = df['Code'].head(5).tolist() 
    except:
        smart_money_picks = []

    # 관제탑 뇌(Config)에 조용히 메모 남기기
    if smart_money_picks:
        config = load_config()
        config['SMART_MONEY_PICKS'] = smart_money_picks
        config['SMART_MONEY_UPDATED'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        save_config(config)
        print(f"✅ 스마트 머니 픽 {len(smart_money_picks)}개 관제탑 업데이트 완료.")

if __name__ == "__main__":
    run_smart_money_scan()
