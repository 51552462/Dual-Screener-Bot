import os
import json
import time
import random
import yfinance as yf
from datetime import datetime

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


def fetch_recent_close(ticker):
    """야후 등 원격 시세 조회 사이에 미세 지터로 과도한 연속 호출을 완충합니다."""
    out = None
    try:
        data = yf.Ticker(ticker).history(period="5d")
        if not data.empty:
            out = float(data["Close"].iloc[-1])
    except Exception:
        pass
    time.sleep(random.uniform(0.3, 0.7))
    return out


def run_doomsday_radar():
    print("🚨 [둠스데이 레이더] 채권/원자재 스마트 머니 이탈 스캔 중...")

    # 1. 미 국채 10년물 - 3개월물 금리 역전 (Recession Indicator)
    yield_10y = fetch_recent_close('^TNX')
    yield_3m = fetch_recent_close('^IRX')
    yield_curve_inverted = False
    if yield_10y and yield_3m:
        yield_curve_inverted = (yield_10y - yield_3m) < -0.5  # -50bp 이상 심각한 역전

    # 2. 구리/금 비율 추락 (Economic Slowdown)
    copper = fetch_recent_close('HG=F')
    gold = fetch_recent_close('GC=F')
    copper_gold_crash = False
    cg_ratio = 0.0
    if copper and gold:
        cg_ratio = copper / gold
        copper_gold_crash = cg_ratio < 0.0018

    # 3. 하이일드 위험 프리미엄 폭발 (Credit Crunch)
    hyg = fetch_recent_close('HYG')
    ief = fetch_recent_close('IEF')
    credit_crunch = False
    if hyg and ief:
        credit_crunch = (hyg / ief) < 0.75

    # 🚨 데프콘(DEFCON) 경보 단계 계산 (5: 평시 ~ 1: 둠스데이)
    defcon_level = 5
    risk_factors = sum([yield_curve_inverted, copper_gold_crash, credit_crunch])

    if risk_factors == 1:
        defcon_level = 4
    elif risk_factors == 2:
        defcon_level = 2
    elif risk_factors == 3:
        defcon_level = 1

    config = load_config()
    config['DOOMSDAY_DEFCON'] = {
        "level": defcon_level,
        "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M'),
        "signals": {
            "yield_inverted": yield_curve_inverted,
            "copper_gold_crash": copper_gold_crash,
            "credit_crunch": credit_crunch,
        },
    }
    save_config(config)

    print(f"✅ 레이더 스캔 완료. 현재 팩토리 방어 태세: DEFCON {defcon_level}")
    if defcon_level <= 2:
        print("⚠️ [초긴급] 주식 시장 밖에서 피비린내가 진동합니다. 전면 현금화 및 숏 포지션 대비 요망!")


if __name__ == "__main__":
    run_doomsday_radar()
