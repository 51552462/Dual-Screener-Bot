import os
import json
import time
import random
from datetime import datetime

import FinanceDataReader as fdr

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
            with open(temp_path, "w", encoding="utf-8") as f:
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


def load_anti_patterns():
    """system_config.json에서 ANTI_PATTERNS만 읽기 전용으로 로드합니다."""
    try:
        config = load_config()
        if not isinstance(config, dict):
            return {}
        return config.get('ANTI_PATTERNS', {}) or {}
    except Exception as e:
        print(f"⚠️ 설정 파일 읽기 실패: {e}")
        return {}


def _iter_anti_bounds(anti_patterns):
    """ANTI_PATTERNS가 dict(트리 룰) 또는 list(면역 DNA 누적)일 때 모두 순회."""
    if isinstance(anti_patterns, dict):
        for t_name, bounds in anti_patterns.items():
            if isinstance(bounds, dict):
                yield str(t_name), bounds
    elif isinstance(anti_patterns, list):
        for i, bounds in enumerate(anti_patterns):
            if isinstance(bounds, dict):
                yield f"PATTERN_{i}", bounds


def scan_blackhole_targets():
    print("🕳️ [블랙홀 스캐너] 시장 내 참사주(Toxic DNA) 무더기 포착 중...")
    anti_patterns = load_anti_patterns()

    if not anti_patterns:
        print("💡 아직 관제탑에 등록된 독성 패턴(Anti-Pattern)이 없습니다.")
        config = load_config()
        config["BLACKHOLE_TOXIC_COUNT"] = {
            "count": 0,
            "stocks": [],
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        save_config(config)
        return

    try:
        # 코스피 200 종목을 대상으로 엑스레이 스캔 (가상 데이터 스캔 시뮬레이션)
        universe = fdr.StockListing('KOSPI')
        time.sleep(random.uniform(0.3, 0.7))
        codes = universe['Code'].head(200).tolist()
    except Exception as e:
        print(f"🚨 종목 유니버스 로드 실패: {e}")
        config = load_config()
        config["BLACKHOLE_TOXIC_COUNT"] = {
            "count": 0,
            "stocks": [],
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        save_config(config)
        return

    toxic_count = 0
    toxic_stocks = []

    # 💡 실가동 시에는 실시간 현재가/CPV/에너지 데이터를 API로 받아와야 함
    # 여기서는 블랙홀 로직의 아키텍처(구조)를 시뮬레이션
    for code in codes:
        # 예시: 특정 종목의 실시간 데이터 (cpv, bbe)를 계산했다고 가정
        mock_cpv = 0.85
        mock_bbe = 1.5

        is_toxic = False
        for t_name, bounds in _iter_anti_bounds(anti_patterns):
            match_flags = []
            if 'dyn_cpv_max' in bounds:
                match_flags.append(mock_cpv <= bounds['dyn_cpv_max'])
            if 'dyn_cpv_min' in bounds:
                match_flags.append(mock_cpv > bounds['dyn_cpv_min'])
            if 'v_energy_max' in bounds:
                match_flags.append(mock_bbe <= bounds['v_energy_max'])
            if 'v_energy_min' in bounds:
                match_flags.append(mock_bbe > bounds['v_energy_min'])

            if match_flags and all(match_flags):
                is_toxic = True
                break

        if is_toxic:
            toxic_stocks.append(code)
            toxic_count += 1

    # 🚨 [시너지] 관제탑 뇌(JSON)에 블랙홀 스캔 결과 보고
    config = load_config()
    config["BLACKHOLE_TOXIC_COUNT"] = {
        "count": toxic_count,
        "stocks": toxic_stocks,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    save_config(config)

    # 🚨 [핵심 우회 로직] 개별주 숏 대신 인버스 ETF 롱(Long) 전략
    if toxic_count >= 10:  # 우량주 200개 중 10개 이상에서 참사 패턴이 발생했다면 (시장 하락 전조)
        print(f"⚠️ [경보] 코스피 우량주 {toxic_count}개에서 폭포수 하락(Toxic) DNA 발견!")
        print("📉 개별 공매도 대신 [KODEX 200선물인버스2X (252670)] 강력 매수(Long) 시그널 발생!")

        # 여기서 자동매매 API(한국투자증권 등)를 통해 252670 종목 매수 로직 연동

    else:
        print(f"🟢 현재 시장은 안정적입니다. (발견된 참사주: {toxic_count}개)")


if __name__ == "__main__":
    scan_blackhole_targets()
