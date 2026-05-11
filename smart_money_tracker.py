import os
import json
import time
import random
import FinanceDataReader as fdr
from datetime import datetime, timedelta

CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')


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


def save_config(config, max_retries=5):
    """
    [장갑차 로직] 임시 파일 원자적(Atomic) 덮어쓰기 및 권한 방어막 적용
    """
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] 관제탑 뇌(JSON) 쓰기 최종 실패: {e}")
        except Exception as e:
            print(f"⚠️ 설정 파일 원자적 저장 중 알 수 없는 에러: {e}")
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False

def run_smart_money_tracker():
    print("🕵️ [스마트 머니 레이더] 기관/외인 은밀한 매집 패턴 스캔 중...")
    
    try:
        # 코스피 시총 상위 100개 종목 추출 (수급 분석 유니버스)
        kospi = fdr.StockListing('KOSPI')
        time.sleep(random.uniform(0.3, 0.7))
        codes = kospi['Code'].head(100).tolist()
    except Exception as e:
        print(f"🚨 종목 리스트 로드 실패: {e}")
        return

    smart_picks = {}
    scanned = 0
    
    for code in codes:
        scanned += 1
        if scanned % 20 == 0: print(f" ↳ {scanned}/100 종목 수급 엑스레이 판독 중...")
        
        try:
            # 최근 15일치 데이터 로드
            start_date = (datetime.now() - timedelta(days=25)).strftime('%Y-%m-%d')
            df = fdr.DataReader(code, start_date)
            time.sleep(random.uniform(0.3, 0.7))
            if len(df) < 10: continue
            
            # [다이버전스 판별 로직]
            # 1. 가격: 최근 5일간 주가 하락 또는 횡보 (-5% ~ +1%)
            recent_5d = df.tail(5)
            price_change = (recent_5d['Close'].iloc[-1] - recent_5d['Close'].iloc[0]) / recent_5d['Close'].iloc[0] * 100
            
            # 2. 수급: 실제 환경에선 KRX 외인/기관 순매수 데이터를 연동해야 함.
            # 여기서는 거래량 밀집도(Volume Accumulation)로 스마트 머니를 추정(Proxy)함.
            avg_vol_20d = df['Volume'].mean()
            recent_vol_surge = recent_5d[recent_5d['Volume'] > avg_vol_20d * 1.5]
            
            # 가격은 안 올랐는데, 평균 대비 1.5배 이상 거래량이 터진 날이 2일 이상 있다면 '은밀한 매집'으로 간주
            if price_change <= 1.0 and len(recent_vol_surge) >= 2:
                # 세력 추정 평단가 (거래대금 가중 평균 - 0 나누기 방어)
                vol_sum = recent_5d['Volume'].sum()
                vwap = (recent_5d['Close'] * recent_5d['Volume']).sum() / vol_sum if vol_sum > 0 else recent_5d['Close'].iloc[-1]
                
                stock_name = kospi[kospi['Code'] == code]['Name'].values[0]
                smart_picks[code] = {
                    "name": stock_name,
                    "avg_price": round(vwap, 0),
                    "divergence_score": round(abs(price_change) + len(recent_vol_surge), 2)
                }
        except:
            continue

    # 관제탑 JSON 업데이트 (기존 데이터 보존)
    if smart_picks:
        config = load_config()
        config['SMART_MONEY_RADAR'] = {
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M'),
            "picks": smart_picks
        }
        save_config(config)
        print(f"✅ 스캔 완료: {len(smart_picks)}개의 수급 다이버전스(매집) 종목이 관제탑에 전달되었습니다.")
    else:
        print("⚠️ 오늘 시장에서는 뚜렷한 기관/외인 매집 다이버전스가 포착되지 않았습니다.")

if __name__ == "__main__":
    run_smart_money_tracker()
