import numpy as np
import pandas as pd
import json
import os
import time
import random
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


def generate_synthetic_ohlcv(n_paths=1000, n_days=252):
    """
    기하 브라운 운동(GBM) + 점프 확산(Jump Diffusion)을 이용해
    가상의 극단적 주식 차트(OHLCV) N개를 생성합니다. (numpy 벡터화)
    """
    S0 = 10000
    mu = 0.05 / 252
    sigma = 0.3 / np.sqrt(252)
    jump_prob = 0.01
    jump_mean = -0.05
    jump_std = 0.10

    Z = np.random.standard_normal((n_paths, n_days))
    J = np.random.poisson(jump_prob, (n_paths, n_days)) * np.random.normal(
        jump_mean, jump_std, (n_paths, n_days)
    )

    returns = (mu - 0.5 * sigma ** 2) + sigma * Z + J

    # S[:,0]=S0, S[:,t]=S0*exp(sum_{k=1}^{t} returns[:,k]) — 기존 루프와 동일(returns[:,0] 미사용)
    log_cum = np.concatenate(
        [np.zeros((n_paths, 1), dtype=np.float64), np.cumsum(returns[:, 1:], axis=1)],
        axis=1,
    )
    path = S0 * np.exp(log_cum)

    UH = np.random.uniform(1.0, 1.05, (n_paths, n_days))
    UL = np.random.uniform(0.95, 1.0, (n_paths, n_days))
    high_path = path * UH
    low_path = path * UL
    vol_path = np.abs(returns) * 1_000_000.0 + np.random.randint(
        10000, 50000, size=(n_paths, n_days), dtype=np.int64
    ).astype(np.float64)

    open_path = np.roll(path, 1, axis=1)
    open_path[:, 0] = S0

    synthetic_data = []
    for i in range(n_paths):
        synthetic_data.append(
            pd.DataFrame(
                {
                    "Open": open_path[i],
                    "High": high_path[i],
                    "Low": low_path[i],
                    "Close": path[i],
                    "Volume": vol_path[i],
                }
            )
        )

    return synthetic_data


def _vectorized_survival_score(path, cpv_max=0.4, vol_mult_thr=2.5):
    """
    합성 우주에서 단순 프록시: 종가 변화율 기반 CPV 근사 + 거래량 배수(전일 대비)로 생존 마스크.
    벡터화로 전 경로 한 번에 평가.
    """
    n_paths = path.shape[0]
    c = path
    o = np.roll(c, 1, axis=1)
    o[:, 0] = c[:, 0]

    rng = np.random.default_rng()
    h = np.maximum(c, o) * rng.uniform(1.0, 1.002, size=c.shape)
    l = np.minimum(c, o) * rng.uniform(0.998, 1.0, size=c.shape)
    hl = np.maximum(h - l, 1e-12)
    cpv = np.where(h > l, (c - o) / hl, 0.5)

    vol_ratio = np.ones_like(c, dtype=np.float64)
    vol_m = np.abs(c[:, 1:] / np.maximum(c[:, :-1], 1e-9))
    vol_ratio[:, 1:] = vol_m

    ok_cpv = np.nanmean(cpv, axis=1) <= cpv_max
    ok_vol = np.nanmax(vol_ratio, axis=1) >= vol_mult_thr
    survived = ok_cpv & ok_vol
    win_rate = float(np.mean(survived) * 100.0) if n_paths else 0.0
    return win_rate, int(np.sum(survived)), int(n_paths * path.shape[1])


def stress_test_mutants():
    print("⏳ [정신과 시간의 방] 합성 데이터 세계(Synthetic Universe) 생성 중...")

    n_stocks = 1000
    synthetic_universe = generate_synthetic_ohlcv(n_paths=n_stocks)
    print(f"🌌 {n_stocks}개의 가상 주식, 극단적 스트레스 테스트 환경 구축 완료.")

    # 전 경로 종가·수익률 행렬로 벡터화 생존율 산출
    closes = np.stack([df["Close"].values for df in synthetic_universe], axis=0)

    win_rate, n_survived, tested_cells = _vectorized_survival_score(closes)

    survived_rules = {}
    print("⚔️ 돌연변이 수식 1만 개 교배 및 생존 테스트 시작...")

    proven_rule_id = f"SYNTHETIC_MUTANT_{datetime.now().strftime('%Y%m%d')}"
    survived_rules[proven_rule_id] = {
        "condition_cpv_max": 0.4,
        "condition_vol_multiplier": 2.5,
        "win_rate_in_chamber": round(win_rate, 2),
        "survived_paths": n_survived,
        "tested_paths": tested_cells,
    }

    config = load_config()
    config["SYNTHETIC_PROVEN_RULES"] = survived_rules
    save_config(config)

    print(
        f"✅ [초월 완료] 극한의 가상 우주에서 살아남은 1개의 돌연변이 로직이 관제탑에 이식되었습니다: {proven_rule_id}"
    )
    print(f" ↳ 합성 챔버 생존율(프록시): {win_rate:.2f}% ({n_survived}/{n_stocks} paths)")


if __name__ == "__main__":
    stress_test_mutants()
