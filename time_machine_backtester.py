import os
import gc
import json
import time
import random
import ast
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import timedelta
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
warnings.filterwarnings('ignore')


def _process_pool_max_workers():
    """OOM 보수: MAX_WORKERS 환경변수·system_config.json 우선, 기본 1."""
    env = os.environ.get("MAX_WORKERS", "").strip()
    if env:
        try:
            return max(1, int(env))
        except ValueError:
            pass
    try:
        cfg = load_config()
        v = cfg.get("MAX_WORKERS", 1)
        return max(1, int(v))
    except Exception:
        return 1

# 1. 팩토리 뇌(Config) 읽기 전용 경로 (config_manager·factory_data_dir 과 동일 루트)
try:
    from factory_data_paths import factory_data_dir

    CONFIG_PATH = os.path.join(factory_data_dir(), "system_config.json")
except ImportError:
    from factory_data_paths import system_config_json_path

    CONFIG_PATH = system_config_json_path()

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


def load_factory_brain_readonly():
    """메인 시스템의 뇌를 읽기 전용으로 복제해 옵니다."""
    if not os.path.exists(CONFIG_PATH):
        print("🚨 관제탑 파일을 찾을 수 없습니다.")
        return {}
    return load_config()

# 2. 레짐 매트릭스 (결정론적 구간 라벨 — 몬테카를로 없음)
REGIME_PERIODS = {
    "2008년 서브프라임 금융위기": {
        "start": "2008-09-01", "end": "2009-03-31", "regime": "EXTREME_CRASH",
    },
    "COVID-19 코로나 폭락장": {
        "start": "2020-02-01", "end": "2020-05-31", "regime": "EXTREME_CRASH",
    },
    "2022년 글로벌 금리인상 폭락장": {
        "start": "2022-01-01", "end": "2022-06-30", "regime": "EXTREME_CRASH",
    },
    "2018년 미중 무역분쟁 하락장": {
        "start": "2018-09-01", "end": "2018-12-31", "regime": "EXTREME_CRASH",
    },
    "2020~2021 유동성 초강세 (대형 상승)": {
        "start": "2020-10-01", "end": "2021-11-30", "regime": "MASSIVE_BULL",
    },
    "2023년 중반 횡보·침체": {
        "start": "2023-05-01", "end": "2023-08-31", "regime": "CHOPPY_STAGNANT",
    },
}

# 하위 호환: 기존 코드가 참조하는 CRASH_PERIODS (극한 붕괴 레짐만 — 시작·종료)
CRASH_PERIODS = {
    k: {"start": v["start"], "end": v["end"]}
    for k, v in REGIME_PERIODS.items()
    if v.get("regime") == "EXTREME_CRASH"
}

# 타임머신이 사용하는 템플릿은 '현재 뇌' 기준이므로 과거 구간 적용 시 룩어헤드가 존재함을 명시한다.
LOOKAHEAD_BIAS_WARNING_HTML = (
    "⚠️ <b>[Lookahead Bias 경고]</b> 본 백테스트는 '현재 시점'까지 학습된 미래의 템플릿(정답지)을 과거 데이터에 적용한 결과입니다. "
    "이는 로직의 '범용적 견고성(Robustness)'을 증명할 뿐, 완벽한 Out-Of-Sample 성과를 보장하지 않으므로 과신(Overfitting)을 경계하십시오."
)


def _print_lookahead_bias_warning() -> None:
    print(LOOKAHEAD_BIAS_WARNING_HTML)


def evaluate_alpha_formula_series(df, formula):
    """
    JSON(EVOLVED_ALPHA_FACTORS) 수식을 AST 샌드박스에서 평가해 시계열(Series)로 반환.
    supernova_hunter.evaluate_alpha_formula 와 동일한 허용 네임스페이스(읽기 전용 백테스터 내장).
    """
    if df is None or getattr(df, 'empty', True):
        return None

    ALLOWED_NAMES = {'O', 'H', 'L', 'C', 'V', 'add', 'sub', 'mul', 'div', 'rolling_mean', 'rolling_std'}
    try:
        formula_str = str(formula).strip()
        tree = ast.parse(formula_str, mode='eval')
        node_count = 0
        for node in ast.walk(tree):
            node_count += 1
            if node_count > 150:
                return None
            if isinstance(node, ast.Name) and node.id not in ALLOWED_NAMES:
                return None
    except Exception:
        return None

    O = df['Open']
    H = df['High']
    L = df['Low']
    C = df['Close']
    V = df['Volume']

    def add(a, b): return a + b
    def sub(a, b): return a - b
    def mul(a, b): return a * b
    def div(a, b):
        safe_b = b.replace(0, float('nan')) if hasattr(b, 'replace') else (float('nan') if b == 0 else b)
        return a / safe_b
    def rolling_mean(x, w): return x.rolling(int(w)).mean()
    def rolling_std(x, w): return x.rolling(int(w)).std()

    env = {
        'O': O, 'H': H, 'L': L, 'C': C, 'V': V,
        'add': add, 'sub': sub, 'mul': mul, 'div': div,
        'rolling_mean': rolling_mean, 'rolling_std': rolling_std
    }

    try:
        result = eval(formula_str, {"__builtins__": {}}, env)
        if isinstance(result, pd.Series):
            return result.replace([np.inf, -np.inf], np.nan)
    except Exception:
        return None

    return None

def calculate_dna_factors(df, evolved_factors=None):
    """과거 차트에서 실시간 팩토리와 똑같은 3D DNA(CPV, TB, BBE)를 추출합니다."""
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    # 20일 이동평균 기반
    v_ma20 = pd.Series(v).rolling(20).mean().values
    
    # CPV (윗꼬리 방어력)
    cpv = np.where(h != l, (c - o) / (h - l), 0.5)
    
    # TB (진짜 양봉 수급)
    vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
    tb = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
    
    # BBE (응축 에너지)
    bb_std = pd.Series(c).rolling(20).std().values
    bb_mid = pd.Series(c).rolling(20).mean().values
    bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
    bbe = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
    
    df['dyn_cpv'] = cpv
    df['dyn_tb'] = tb
    df['v_energy'] = bbe

    # EVOLVED_ALPHA_FACTORS: 템플릿 alpha_<슬롯>_min/max 와 직결되는 시계열 컬럼
    if isinstance(evolved_factors, dict):
        for slot_key, formula in evolved_factors.items():
            if not isinstance(formula, str) or not str(formula).strip():
                continue
            col = f'_ev_alpha_{slot_key}'
            ser = evaluate_alpha_formula_series(df, formula.strip())
            if ser is not None and len(ser):
                df[col] = ser
            else:
                df[col] = np.nan

    return df

def _row_matches_template_bounds(row, bounds, evolved_slot_keys):
    """기존 3D 박스 + (템플릿에 정의된 경우) 진화 알파 슬롯별 바운딩."""
    if not isinstance(bounds, dict):
        return False
    if not (
        bounds.get('dyn_cpv_min', -99) <= row['dyn_cpv'] <= bounds.get('dyn_cpv_max', 99) and
        bounds.get('dyn_tb_min', -99) <= row['dyn_tb'] <= bounds.get('dyn_tb_max', 999) and
        bounds.get('v_energy_min', -99) <= row['v_energy'] <= bounds.get('v_energy_max', 999)
    ):
        return False
    for sk in evolved_slot_keys:
        akmin, akmax = f'alpha_{sk}_min', f'alpha_{sk}_max'
        if akmin not in bounds or akmax not in bounds:
            continue
        col = f'_ev_alpha_{sk}'
        if col not in row.index:
            return False
        try:
            val = float(row[col])
        except (TypeError, ValueError):
            return False
        if not np.isfinite(val) or np.isnan(val):
            return False
        try:
            lo = float(bounds[akmin])
            hi = float(bounds[akmax])
        except (TypeError, ValueError):
            return False
        if not (lo <= val <= hi):
            return False
    return True

def _backtest_one_ticker(code, fetch_start, end_dt, start_dt, all_templates, evolved_factors):
    """단일 종목: 다운로드 → DNA/알파 → 템플릿 매칭 → 15일 MFE/MAE. (ProcessPool 워커에서 호출)"""
    out = []
    evolved_slot_keys = list(evolved_factors.keys()) if isinstance(evolved_factors, dict) else []
    fetch_latency_s = None
    t_mark = None
    df = None

    try:
        time.sleep(random.uniform(0.05, 0.18))
        t_mark = time.perf_counter()
        df = fdr.DataReader(code, fetch_start, end_dt)
        fetch_latency_s = time.perf_counter() - t_mark
    except Exception:
        el = (time.perf_counter() - t_mark) if t_mark is not None else None
        if df is not None:
            del df
        return {"trades": [], "fetch_latency_s": el, "gate": "fetch_error"}

    warmup_df = None
    test_df = None
    try:
        if df is None or getattr(df, "empty", True):
            if df is not None:
                del df
            return {"trades": [], "fetch_latency_s": fetch_latency_s, "gate": "skip_empty"}
        if len(df) < 30:
            del df
            return {"trades": [], "fetch_latency_s": fetch_latency_s, "gate": "skip_short"}

        df = df.sort_index()
        if df.index.has_duplicates:
            df = df[~df.index.duplicated(keep="last")]

        start_ts = pd.Timestamp(start_dt)
        warmup_df = df[df.index < start_ts]

        # 테스트 구간(가격 경로·미래 15일 평가용)
        test_df = df[df.index >= start_ts]
        if len(test_df) < 16:
            del test_df
            del warmup_df
            del df
            return {"trades": [], "fetch_latency_s": fetch_latency_s, "gate": "skip_regime_window"}

        for i in range(len(test_df) - 15):
            # 💡 [룩어헤드 방지] 평가일 i까지: test_df.iloc[:i+1] 만 허용 + 롤 워밍업용 사전 구간(warmup_df)만 병합
            past_in_regime = test_df.iloc[: i + 1]
            current_history_df = pd.concat([warmup_df, past_in_regime]).sort_index()
            current_history_df = current_history_df[~current_history_df.index.duplicated(keep="last")]
            if isinstance(current_history_df, pd.Series):
                current_history_df = current_history_df.to_frame().T
            if len(current_history_df) < 30:
                del past_in_regime
                del current_history_df
                continue

            hist = calculate_dna_factors(current_history_df.copy(), evolved_factors=evolved_factors)
            current_row = hist.iloc[-1]

            is_passed = False
            matched_tpl = ""
            for t_name, bounds in all_templates.items():
                if _row_matches_template_bounds(current_row, bounds, evolved_slot_keys):
                    is_passed = True
                    matched_tpl = t_name
                    break

            if is_passed:
                entry_price = float(current_row["Close"])
                future_15d = test_df.iloc[i + 1 : i + 16]

                max_high = future_15d['High'].max()
                min_low = future_15d['Low'].min()

                mfe = (max_high - entry_price) / entry_price * 100
                mae = (min_low - entry_price) / entry_price * 100

                final_ret = 0.0
                for _, f_row in future_15d.iterrows():
                    cur_mfe = (f_row['High'] - entry_price) / entry_price * 100
                    cur_mae = (f_row['Low'] - entry_price) / entry_price * 100

                    if cur_mae <= -3.5:
                        final_ret = -3.5
                        break
                    elif cur_mfe >= 10.0:
                        final_ret = 10.0
                        break

                if final_ret == 0.0:
                    final_ret = (future_15d.iloc[-1]['Close'] - entry_price) / entry_price * 100

                out.append({
                    'date': test_df.index[i].strftime('%Y-%m-%d'),
                    'code': code,
                    'template': matched_tpl,
                    'mfe': mfe,
                    'mae': mae,
                    'final_ret': final_ret
                })
                del future_15d

            del hist
            del current_row
            del past_in_regime
            del current_history_df

        del df
        del warmup_df
        del test_df
        return {"trades": out, "fetch_latency_s": fetch_latency_s, "gate": "success"}
    except Exception:
        if df is not None:
            try:
                del df
            except Exception:
                pass
        if warmup_df is not None:
            try:
                del warmup_df
            except Exception:
                pass
        if test_df is not None:
            try:
                del test_df
            except Exception:
                pass
        return {"trades": out, "fetch_latency_s": fetch_latency_s, "gate": "processing_error"}

def _summarize_trade_results(results):
    """청산 시뮬 결과 리스트 → 승률·PF·평균 수익률."""
    if not results:
        return {"total_trades": 0, "win_rate": 0.0, "pf": 0.0, "avg_pnl": 0.0}
    res_df = pd.DataFrame(results)
    total_trades = len(res_df)
    wins = res_df[res_df['final_ret'] > 0]
    loses = res_df[res_df['final_ret'] <= 0]
    win_rate = len(wins) / total_trades * 100 if total_trades > 0 else 0.0
    avg_pnl = float(res_df['final_ret'].mean())
    pf = wins['final_ret'].sum() / (abs(loses['final_ret'].sum()) + 0.1) if not loses.empty else 99.9
    return {"total_trades": total_trades, "win_rate": win_rate, "pf": float(pf), "avg_pnl": avg_pnl}


def _print_regime_fetch_diagnostics(regime_label: str, attempted: int, packs: list) -> None:
    """fdr 수집 지연(p50/p90) 및 표본 붕괴 방지용 데이터 게이트 요약(콘솔만, DB 미기록)."""
    if attempted <= 0:
        return
    n = attempted
    dl_ok = sum(
        1
        for p in packs
        if p.get("gate") in ("success", "skip_regime_window", "processing_error")
    )
    data_skip = sum(1 for p in packs if p.get("gate") in ("skip_empty", "skip_short"))
    fetch_err = sum(1 for p in packs if p.get("gate") == "fetch_error")
    regime_skip = sum(1 for p in packs if p.get("gate") == "skip_regime_window")
    proc_err = sum(1 for p in packs if p.get("gate") == "processing_error")

    lat_vals = []
    for p in packs:
        v = p.get("fetch_latency_s")
        if v is not None and np.isfinite(v):
            lat_vals.append(float(v))
    if lat_vals:
        arr = np.asarray(lat_vals, dtype=float)
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        lat_line = f"⏱ fdr.DataReader 지연 p50={p50:.3f}s · p90={p90:.3f}s (측정 n={len(lat_vals)})"
    else:
        lat_line = "⏱ fdr.DataReader 지연: 측정 샘플 없음"

    ok_pct = 100.0 * dl_ok / n
    skip_pct = 100.0 * data_skip / n
    print(
        f"   📡 [진단·데이터] {regime_label} | 시도={n} | ≥30봉 다운로드 성공 {dl_ok} ({ok_pct:.1f}%) | "
        f"공백·단축 스킵 {data_skip} (스킵비 {skip_pct:.1f}%) | 수신예외 {fetch_err} | "
        f"레짐구간부족 {regime_skip} | 처리예외 {proc_err}"
    )
    print(f"   {lat_line}")

def run_time_machine_backtest(target_period_name, stock_list):
    print(f"\n⏳ 타임머신 가동: [{target_period_name}] 차원으로 이동합니다...")
    if target_period_name not in REGIME_PERIODS:
        print(f"🚨 알 수 없는 레짐 키: {target_period_name}")
        return
    period = REGIME_PERIODS[target_period_name]
    start_dt, end_dt = period["start"], period["end"]

    # ------------------------------------------------------------------
    # [WFO / 블랙박스] 레짐 end 이전(포함) 날짜 중 가장 최근 config 스냅샷 탐색 — 현재는 뼈대만
    # 향후: 아래 경로 JSON을 로드해 당시 동결 설정으로 시뮬 → Lookahead Bias 제거
    # 지금: 스냅샷이 없을 수 있으므로 기존 load_factory_brain_readonly() Fallback 유지
    # ------------------------------------------------------------------
    try:
        from config_manager import find_latest_config_snapshot_on_or_before
    except ImportError:
        find_latest_config_snapshot_on_or_before = None  # type: ignore

    snap_for_regime = None
    if find_latest_config_snapshot_on_or_before is not None:
        try:
            snap_for_regime = find_latest_config_snapshot_on_or_before(end_dt)
        except Exception:
            snap_for_regime = None

    if snap_for_regime and os.path.isfile(snap_for_regime):
        print(
            f"📂 [config_snapshots] 레짐 end={end_dt} 기준 가장 가까운 과거 스냅샷 발견: {snap_for_regime}"
        )
        print(
            "    → (향후) 미래에는 현재 system_config.json 대신 이 스냅샷을 로드하여 "
            "Lookahead Bias를 완벽히 제거할 예정입니다. (현재 실행은 기존 경로 Fallback)"
        )
    else:
        print(
            f"📂 [config_snapshots] end={end_dt} 이전에 사용할 스냅샷 없음 — "
            "현재 관제탑 설정(system_config.json 경로)으로 Fallback 유지."
        )
        print(
            "    → (향후) 일별 아카이브(system_config_YYYYMMDD.json)가 쌓이면 "
            "해당 시점 동결 설정으로 타임머신에 주입합니다."
        )

    config = load_factory_brain_readonly()
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {})
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    all_templates = {**ml_templates, **ud_templates}
    evolved_factors = config.get("EVOLVED_ALPHA_FACTORS")
    if not isinstance(evolved_factors, dict):
        evolved_factors = {}
    
    if not all_templates:
        print("⚠️ 팩토리에 학습된 템플릿(무기)이 없습니다. 테스트를 종료합니다.")
        return

    _print_lookahead_bias_warning()
    results = []
    packs = []
    fetch_start = (pd.to_datetime(start_dt) - timedelta(days=40)).strftime('%Y-%m-%d')
    
    scanned = 0
    n_total = len(stock_list)
    max_workers = _process_pool_max_workers()

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [
            ex.submit(_backtest_one_ticker, code, fetch_start, end_dt, start_dt, all_templates, evolved_factors)
            for code in stock_list
        ]
        for fut in as_completed(futs):
            scanned += 1
            if scanned % 20 == 0:
                print(f" ↳ {scanned}/{n_total}개 종목 시뮬레이션 중...")
            try:
                p = fut.result()
                if not isinstance(p, dict):
                    p = {"trades": [], "fetch_latency_s": None, "gate": "fetch_error"}
                packs.append(p)
                results.extend(p.get("trades", []))
            except Exception:
                packs.append({"trades": [], "fetch_latency_s": None, "gate": "fetch_error"})

    # 결과 결산
    if not results:
        print(f"\n🛡️ 결과: {target_period_name} 동안 템플릿에 걸려든 종목이 없습니다. (위험 완벽 회피)")
        _print_lookahead_bias_warning()
        _print_regime_fetch_diagnostics(target_period_name, n_total, packs)
        return

    stats = _summarize_trade_results(results)
    total_trades = stats["total_trades"]
    win_rate = stats["win_rate"]
    avg_pnl = stats["avg_pnl"]
    pf = stats["pf"]
    
    print(f"\n🏆 <b>[{target_period_name} 백테스트 결과]</b>")
    print(f" ▪️ 총 진입 횟수: {total_trades}회")
    print(f" ▪️ 승률: {win_rate:.1f}%")
    print(f" ▪️ 평균 수익률: {avg_pnl:+.2f}%")
    print(f" ▪️ 손익비(PF): {pf:.2f}")
    _print_lookahead_bias_warning()
    
    if avg_pnl > 0:
        print("💡 결론: 우리 AI의 로직은 역사적인 폭락장에서도 수익을 창출하며 살아남는 압도적 방어력을 증명했습니다.")
    else:
        print("💡 결론: 폭락장의 타격을 피하지 못했습니다. 안티 패턴(참사 방어막)을 더 강화해야 합니다.")

    _print_regime_fetch_diagnostics(target_period_name, n_total, packs)
    return stats

def run_time_machine_regime_matrix(stock_list, pf_robust_threshold=1.2):
    """
    전 레짐(REGIME_PERIODS)에 대해 동일 유니버스로 타임머신을 순회 실행하고,
    승률·PF 매트릭스를 출력한다. (몬테카를로 없음)
    """
    print("\n🧭 [레짐 매트릭스] 전 기후대 스트레스 — 원허트 원더 vs 올웨더 판별")
    config = load_factory_brain_readonly()
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {})
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    all_templates = {**ml_templates, **ud_templates}
    evolved_factors = config.get("EVOLVED_ALPHA_FACTORS")
    if not isinstance(evolved_factors, dict):
        evolved_factors = {}

    if not all_templates:
        print("⚠️ 팩토리에 학습된 템플릿(무기)이 없습니다. 레짐 매트릭스를 종료합니다.")
        return

    _print_lookahead_bias_warning()
    n_total = len(stock_list)
    max_workers = _process_pool_max_workers()
    matrix_rows = []

    for regime_name, meta in REGIME_PERIODS.items():
        start_dt, end_dt = meta["start"], meta["end"]
        rtype = meta.get("regime", "UNKNOWN")
        print(f"\n⏳ 타임머신 가동: [{regime_name}] ({rtype}) 차원으로 이동합니다...")
        fetch_start = (pd.to_datetime(start_dt) - timedelta(days=40)).strftime('%Y-%m-%d')
        results = []
        packs = []
        scanned = 0
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futs = [
                ex.submit(_backtest_one_ticker, code, fetch_start, end_dt, start_dt, all_templates, evolved_factors)
                for code in stock_list
            ]
            for fut in as_completed(futs):
                scanned += 1
                if scanned % 20 == 0:
                    print(f" ↳ [{regime_name}] {scanned}/{n_total}개 종목 시뮬레이션 중...")
                try:
                    p = fut.result()
                    if not isinstance(p, dict):
                        p = {"trades": [], "fetch_latency_s": None, "gate": "fetch_error"}
                    packs.append(p)
                    results.extend(p.get("trades", []))
                except Exception:
                    packs.append({"trades": [], "fetch_latency_s": None, "gate": "fetch_error"})

        stats = _summarize_trade_results(results)
        stats.update({"regime_name": regime_name, "regime": rtype})
        matrix_rows.append(stats)

        if stats["total_trades"] == 0:
            print(f" 🛡️ [{regime_name}] 진입 0건 — 해당 구간에 템플릿 매칭 없음")
        else:
            print(
                f" ▪️ 승률: {stats['win_rate']:.1f}% | PF: {stats['pf']:.2f} | "
                f"평균수익률: {stats['avg_pnl']:+.2f}% | n={stats['total_trades']}"
            )
        _print_regime_fetch_diagnostics(regime_name, n_total, packs)
        del results
        del packs
        gc.collect()

    print("\n" + "=" * 72)
    print("📊 <b>[레짐 매트릭스 요약]</b> 승률(%) / Profit Factor / 표본수")
    _print_lookahead_bias_warning()
    print("=" * 72)
    evaluable = [r for r in matrix_rows if r["total_trades"] > 0]
    for row in matrix_rows:
        rn = row["regime_name"]
        rt = row["regime"]
        n = row["total_trades"]
        if n == 0:
            print(f" • [{rt:18s}] {rn[:28]:30s}  승률: —   PF: —    n=0")
        else:
            print(
                f" • [{rt:18s}] {rn[:28]:30s}  승률: {row['win_rate']:5.1f}%  PF: {row['pf']:6.2f}  n={n}"
            )

    if not evaluable:
        print("\n⚠️ 모든 레짐에서 진입이 없어 견고성(ROBUST) 판정을 생략합니다.")
        return matrix_rows

    robust = all(r["pf"] > pf_robust_threshold for r in evaluable)
    print("\n" + "-" * 72)
    if robust:
        print(
            f"✅ <b>알파 견고성: ROBUST</b> — 진입이 있었던 모든 레짐에서 PF > {pf_robust_threshold} "
            f"(원허트 원더가 아닌 올웨더 성격)"
        )
    else:
        print(
            f"⚠️ <b>알파 견고성: 미달</b> — PF ≤ {pf_robust_threshold} 인 레짐이 존재합니다 "
            f"(구간 편향·과최적화 점검 권장)."
        )
        for r in evaluable:
            if r["pf"] <= pf_robust_threshold:
                print(f"   ↳ 약점 레짐: {r['regime_name']} (PF={r['pf']:.2f})")
    print("=" * 72)
    return matrix_rows

if __name__ == "__main__":
    # Windows(spawn)에서 ProcessPoolExecutor 사용 시, 워커는 이 모듈을 재임포트하므로
    # 진입점은 반드시 이 가드 안에 두는 것이 안전하다.
    # 코스피 시총 상위 100개 랜덤 추출 (테스트 속도를 위해 100개만 스캔)
    print("증권사 API 연결 및 테스트 종목(코스피 우량주) 준비 중...")
    try:
        kospi = fdr.StockListing('KOSPI')
        time.sleep(random.uniform(0.3, 0.7))
        test_universe = kospi['Code'].tolist()[:100]
    except:
        test_universe = ['005930', '000660', '035420', '051910', '005380'] # 실패 시 삼성전자 등 하드코딩

    run_time_machine_regime_matrix(test_universe)
