import json
import os
import random
import time
from datetime import datetime

import requests

from bitget.rate_limit_guard import throttle

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")


def load_config(max_retries=5):
    if not os.path.exists(CONFIG_PATH):
        return {}
    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError) as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] Bitget 설정(JSON) 읽기 최종 실패: {e}")
                return {}
    return {}


def save_config(config, max_retries=5):
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                print(f"🚨 [치명적 방어] Bitget 설정(JSON) 쓰기 최종 실패: {e}")
        except Exception as e:
            print(f"⚠️ 설정 파일 원자적 저장 중 알 수 없는 에러: {e}")
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False


def _fetch_global_crypto():
    url = "https://api.coingecko.com/api/v3/global"
    throttle("http.coingecko.global.doom", 0.35)
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    data = res.json().get("data", {})
    return {
        "btc_dominance": float(data.get("market_cap_percentage", {}).get("btc", 0.0) or 0.0),
        "market_cap_change_24h": float(data.get("market_cap_change_percentage_24h_usd", 0.0) or 0.0),
    }


def _fetch_eth_btc_ratio():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    throttle("http.coingecko.simple_price.doom", 0.35)
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    j = res.json()
    btc = float(j.get("bitcoin", {}).get("usd", 0.0) or 0.0)
    eth = float(j.get("ethereum", {}).get("usd", 0.0) or 0.0)
    return (eth / btc) if btc > 0 else 0.0


def run_doomsday_radar():
    print("🚨 [Bitget 둠스데이 레이더] 비트코인 도미넌스/알트 약세/시총 붕괴 스캔 중...")

    btc_dominance = 0.0
    market_cap_change_24h = 0.0
    eth_btc_ratio = 0.0
    try:
        g = _fetch_global_crypto()
        btc_dominance = float(g["btc_dominance"])
        market_cap_change_24h = float(g["market_cap_change_24h"])
    except Exception as e:
        print(f"⚠️ 글로벌 코인 매크로 로드 실패: {e}")
    try:
        eth_btc_ratio = float(_fetch_eth_btc_ratio())
    except Exception as e:
        print(f"⚠️ ETH/BTC 비율 로드 실패: {e}")

    # 코인 전용 리스크 신호
    dominance_spike = btc_dominance >= 58.0
    alt_weakness = eth_btc_ratio > 0 and eth_btc_ratio < 0.045
    market_cap_crash = market_cap_change_24h <= -6.0

    defcon_level = 5
    risk_factors = sum([dominance_spike, alt_weakness, market_cap_crash])
    if risk_factors == 1:
        defcon_level = 4
    elif risk_factors == 2:
        defcon_level = 2
    elif risk_factors >= 3:
        defcon_level = 1

    config = load_config()
    config["DOOMSDAY_DEFCON"] = {
        "level": defcon_level,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "signals": {
            "btc_dominance_spike": dominance_spike,
            "alt_weakness_eth_btc": alt_weakness,
            "crypto_market_cap_crash": market_cap_crash,
        },
        "metrics": {
            "btc_dominance": round(float(btc_dominance), 3),
            "eth_btc_ratio": round(float(eth_btc_ratio), 6),
            "market_cap_change_24h_usd_pct": round(float(market_cap_change_24h), 3),
        },
    }
    save_config(config)

    print(f"✅ 레이더 스캔 완료. 현재 Bitget 팩토리 방어 태세: DEFCON {defcon_level}")
    if defcon_level <= 2:
        print("⚠️ [초긴급] 비트코인 쏠림/알트 붕괴 리스크가 높습니다. 레버리지 축소 및 방어 모드 권장.")


if __name__ == "__main__":
    run_doomsday_radar()
