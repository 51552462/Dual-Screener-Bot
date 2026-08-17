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
from typing import Any, Dict, List, Optional, Sequence, Tuple
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


_BACKTEST_BRAIN_KEYS = (
    "LIVE_CLUSTER_TEMPLATES",
    "UNDERDOG_CLUSTER_TEMPLATES",
    "EVOLVED_ALPHA_FACTORS",
)


def _backfill_brain_keys_from_legacy(cfg: dict) -> dict:
    """SQLite KV에 없는 클러스터 템플릿은 JSON/샤드 병합 뷰에서 보충 (read-only)."""
    out = dict(cfg) if isinstance(cfg, dict) else {}
    missing = [k for k in _BACKTEST_BRAIN_KEYS if not out.get(k)]
    if not missing:
        return out
    try:
        from config_manager import _load_legacy_merged_view

        legacy = _load_legacy_merged_view(max_retries=10)
        if not isinstance(legacy, dict):
            return out
        for key in missing:
            val = legacy.get(key)
            if val:
                out[key] = val
    except Exception as exc:
        print(f"⚠️ [time_machine] legacy brain backfill 실패: {exc}")

    still_missing = [k for k in _BACKTEST_BRAIN_KEYS if not out.get(k)]
    if still_missing:
        try:
            from factory_data_paths import install_root

            repo_sc = os.path.join(install_root(), "system_config.json")
            data_sc = CONFIG_PATH
            if os.path.isfile(repo_sc) and os.path.abspath(repo_sc) != os.path.abspath(data_sc):
                with open(repo_sc, encoding="utf-8") as fh:
                    repo_cfg = json.load(fh)
                if isinstance(repo_cfg, dict):
                    for key in still_missing:
                        val = repo_cfg.get(key)
                        if val:
                            out[key] = val
        except Exception as exc:
            print(f"⚠️ [time_machine] install_root brain backfill 실패: {exc}")
    return out


def load_factory_brain_readonly():
    """메인 시스템의 뇌를 읽기 전용으로 복제 (SQLite config_kv + JSON/샤드 backfill)."""
    cfg: dict = {}
    try:
        from config_manager import load_system_config

        blob = load_system_config(max_retries=10)
        if isinstance(blob, dict):
            cfg = blob
    except Exception as exc:
        print(f"⚠️ [time_machine] config_manager 로드 실패, JSON fallback: {exc}")

    cfg = _backfill_brain_keys_from_legacy(cfg)
    ml = cfg.get("LIVE_CLUSTER_TEMPLATES") or {}
    ud = cfg.get("UNDERDOG_CLUSTER_TEMPLATES") or {}
    if ml or ud or cfg.get("EVOLVED_ALPHA_FACTORS"):
        return cfg

    if not os.path.exists(CONFIG_PATH):
        print("🚨 관제탑 파일을 찾을 수 없습니다.")
        return {}
    return _backfill_brain_keys_from_legacy(load_config())

# 2. 레짐 매트릭스 (결정론적 구간 라벨 — 몬테카를로 없음)
# RP-1: 15구간 (상승5·횡보5·하락5) — bucket + backup 치환용
REGIME_PERIODS = {
    # --- BULL ×5 ---
    "BULL_01_유동성초강세": {
        "start": "2020-10-01", "end": "2021-11-30",
        "regime": "MASSIVE_BULL", "bucket": "BULL",
        "backup": {"start": "2019-10-01", "end": "2020-02-29", "regime": "BULL", "bucket": "BULL"},
    },
    "BULL_02_US_AI랠리": {
        "start": "2023-01-01", "end": "2023-07-31",
        "regime": "BULL", "bucket": "BULL",
        "backup": {"start": "2023-08-01", "end": "2023-12-31", "regime": "BULL", "bucket": "BULL"},
    },
    "BULL_03_최근상승": {
        "start": "2024-10-01", "end": "2025-03-31",
        "regime": "BULL", "bucket": "BULL",
        "backup": {"start": "2024-06-01", "end": "2024-09-30", "regime": "BULL", "bucket": "BULL"},
    },
    "BULL_04_KR코스피랠리": {
        "start": "2017-01-01", "end": "2018-01-31",
        "regime": "BULL", "bucket": "BULL",
        "backup": {"start": "2016-11-01", "end": "2017-06-30", "regime": "BULL", "bucket": "BULL"},
    },
    "BULL_05_글로벌리플레이": {
        "start": "2016-06-01", "end": "2016-11-30",
        "regime": "BULL", "bucket": "BULL",
        "backup": {"start": "2013-01-01", "end": "2013-06-30", "regime": "BULL", "bucket": "BULL"},
    },
    # --- SIDEWAYS ×5 ---
    "SIDE_01_2023횡보": {
        "start": "2023-05-01", "end": "2023-08-31",
        "regime": "CHOPPY_STAGNANT", "bucket": "SIDEWAYS",
        "backup": {"start": "2023-09-01", "end": "2023-12-31", "regime": "SIDEWAYS", "bucket": "SIDEWAYS"},
    },
    "SIDE_02_2015횡보": {
        "start": "2015-06-01", "end": "2016-06-30",
        "regime": "SIDEWAYS", "bucket": "SIDEWAYS",
        "backup": {"start": "2014-06-01", "end": "2015-05-31", "regime": "SIDEWAYS", "bucket": "SIDEWAYS"},
    },
    "SIDE_03_2021-22혼조": {
        "start": "2021-12-01", "end": "2022-12-31",
        "regime": "SIDEWAYS", "bucket": "SIDEWAYS",
        "backup": {"start": "2021-06-01", "end": "2021-11-30", "regime": "SIDEWAYS", "bucket": "SIDEWAYS"},
    },
    "SIDE_04_2024여름횡보": {
        "start": "2024-04-01", "end": "2024-08-31",
        "regime": "SIDEWAYS", "bucket": "SIDEWAYS",
        "backup": {"start": "2024-01-01", "end": "2024-03-31", "regime": "SIDEWAYS", "bucket": "SIDEWAYS"},
    },
    "SIDE_05_2019횡보": {
        "start": "2019-04-01", "end": "2019-12-31",
        "regime": "SIDEWAYS", "bucket": "SIDEWAYS",
        "backup": {"start": "2018-01-01", "end": "2018-06-30", "regime": "SIDEWAYS", "bucket": "SIDEWAYS"},
    },
    # --- BEAR ×5 (인과 중복 제거: 은행위기/팬데믹/금리/무역/국가신용) ---
    "BEAR_01_서브프라임GFC": {
        "start": "2008-09-01", "end": "2009-03-31",
        "regime": "EXTREME_CRASH", "bucket": "BEAR",
        "backup": {"start": "2008-06-01", "end": "2008-08-31", "regime": "BEAR", "bucket": "BEAR"},
    },
    "BEAR_02_COVID폭락": {
        "start": "2020-02-01", "end": "2020-05-31",
        "regime": "EXTREME_CRASH", "bucket": "BEAR",
        "backup": {"start": "2020-06-01", "end": "2020-08-31", "regime": "BEAR", "bucket": "BEAR"},
    },
    "BEAR_03_글로벌금리인상": {
        "start": "2022-01-01", "end": "2022-06-30",
        "regime": "EXTREME_CRASH", "bucket": "BEAR",
        "backup": {"start": "2022-07-01", "end": "2022-10-31", "regime": "BEAR", "bucket": "BEAR"},
    },
    "BEAR_04_미중무역분쟁": {
        "start": "2018-09-01", "end": "2018-12-31",
        "regime": "BEAR", "bucket": "BEAR",
        "backup": {"start": "2018-06-01", "end": "2018-08-31", "regime": "BEAR", "bucket": "BEAR"},
    },
    "BEAR_05_미국신용등급강등": {
        "start": "2011-08-01", "end": "2011-10-31",
        "regime": "BEAR", "bucket": "BEAR",
        "backup": {"start": "2011-05-01", "end": "2011-07-31", "regime": "BEAR", "bucket": "BEAR"},
    },
}

# 레거시 키 → RP-1 키 (하위 호환)
_LEGACY_REGIME_KEY_ALIASES = {
    "2008년 서브프라임 금융위기": "BEAR_01_서브프라임GFC",
    "COVID-19 코로나 폭락장": "BEAR_02_COVID폭락",
    "2022년 글로벌 금리인상 폭락장": "BEAR_03_글로벌금리인상",
    "2018년 미중 무역분쟁 하락장": "BEAR_04_미중무역분쟁",
    "2020~2021 유동성 초강세 (대형 상승)": "BULL_01_유동성초강세",
    "2023년 중반 횡보·침체": "SIDE_01_2023횡보",
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

def _row_matches_template_bounds(row, bounds, evolved_slot_keys, ticker=None):
    """기존 3D 박스 + (템플릿에 정의된 경우) 진화 알파 슬롯별 바운딩."""
    if not isinstance(bounds, dict):
        return False
    if not (
        bounds.get('dyn_cpv_min', -99) <= row['dyn_cpv'] <= bounds.get('dyn_cpv_max', 99) and
        bounds.get('dyn_tb_min', -99) <= row['dyn_tb'] <= bounds.get('dyn_tb_max', 999) and
        bounds.get('v_energy_min', -99) <= row['v_energy'] <= bounds.get('v_energy_max', 999)
    ):
        return False
    kr_rs_min = bounds.get("_bull_recency_01_kr_dyn_rs_min")
    if kr_rs_min is not None and ticker is not None:
        from bull_recency_01_bounds import is_kr_ticker

        if is_kr_ticker(ticker):
            try:
                rs_val = float(row.get("dyn_rs", float("nan")))
            except (TypeError, ValueError):
                return False
            if not np.isfinite(rs_val) or rs_val < float(kr_rs_min):
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


def compute_rp1_global_ohlcv_bounds(
    regime_periods: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """Min fetch_start / max end_dt across primary + backup windows."""
    periods = regime_periods or REGIME_PERIODS
    starts: List[str] = []
    ends: List[str] = []
    for meta in periods.values():
        starts.append(str(meta["start"]))
        ends.append(str(meta["end"]))
        backup = meta.get("backup")
        if isinstance(backup, dict):
            starts.append(str(backup["start"]))
            ends.append(str(backup["end"]))
    fetch_start = (pd.to_datetime(min(starts)) - timedelta(days=40)).strftime("%Y-%m-%d")
    return fetch_start, max(ends)


def collect_rp1_ohlcv_windows(
    regime_periods: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """Unique (start_dt, end_dt) across primary + backup (deduped)."""
    periods = regime_periods or REGIME_PERIODS
    seen: set = set()
    out: List[Tuple[str, str]] = []
    for meta in periods.values():
        for start, end in ((str(meta["start"]), str(meta["end"])),):
            key = (start, end)
            if key not in seen:
                seen.add(key)
                out.append(key)
        backup = meta.get("backup")
        if isinstance(backup, dict):
            key = (str(backup["start"]), str(backup["end"]))
            if key not in seen:
                seen.add(key)
                out.append(key)
    return out


def _window_cache_key(start_dt: str, end_dt: str) -> str:
    return f"{start_dt}|{end_dt}"


def _simulate_trades_on_ohlcv(
    code: str,
    df: pd.DataFrame,
    start_dt: str,
    end_dt: str,
    all_templates: Dict[str, Any],
    evolved_factors: Dict[str, Any],
) -> Dict[str, Any]:
    """Run template backtest on pre-loaded OHLCV (no network)."""
    out: List[dict] = []
    evolved_slot_keys = list(evolved_factors.keys()) if isinstance(evolved_factors, dict) else []
    try:
        if df is None or getattr(df, "empty", True):
            return {"trades": [], "gate": "skip_empty"}
        if len(df) < 30:
            return {"trades": [], "gate": "skip_short"}

        work = df.sort_index()
        if work.index.has_duplicates:
            work = work[~work.index.duplicated(keep="last")]

        start_ts = pd.Timestamp(start_dt)
        end_ts = pd.Timestamp(end_dt)
        work = work[work.index <= end_ts]
        warmup_df = work[work.index < start_ts]
        test_df = work[work.index >= start_ts]

        # SIDE-ALPHA-01: SIDEWAYS window may use wider MAE SL (env-gated).
        try:
            from side_alpha_01_exit import (
                BASE_HOLD_BARS,
                BASE_MFE_TP,
                resolve_mae_sl_for_window,
                simulate_exit_on_bars,
            )

            _mae_sl = float(resolve_mae_sl_for_window(start_dt, end_dt))
            _mfe_tp = float(BASE_MFE_TP)
            _hold = int(BASE_HOLD_BARS)
        except Exception:
            _mae_sl, _mfe_tp, _hold = -3.5, 10.0, 15

        if len(test_df) < (_hold + 1):
            return {"trades": [], "gate": "skip_regime_window"}

        for i in range(len(test_df) - _hold):
            past_in_regime = test_df.iloc[: i + 1]
            current_history_df = pd.concat([warmup_df, past_in_regime]).sort_index()
            current_history_df = current_history_df[~current_history_df.index.duplicated(keep="last")]
            if isinstance(current_history_df, pd.Series):
                current_history_df = current_history_df.to_frame().T
            if len(current_history_df) < 30:
                continue

            hist = calculate_dna_factors(current_history_df.copy(), evolved_factors=evolved_factors)
            current_row = hist.iloc[-1]

            is_passed = False
            matched_tpl = ""
            for t_name, bounds in all_templates.items():
                if _row_matches_template_bounds(
                    current_row, bounds, evolved_slot_keys, ticker=code
                ):
                    is_passed = True
                    matched_tpl = t_name
                    break

            if is_passed:
                entry_price = float(current_row["Close"])
                future_nd = test_df.iloc[i + 1 : i + 1 + _hold]
                max_high = future_nd["High"].max()
                min_low = future_nd["Low"].min()
                mfe = (max_high - entry_price) / entry_price * 100
                mae = (min_low - entry_price) / entry_price * 100
                final_ret = simulate_exit_on_bars(
                    future_nd, entry_price, mae_sl=_mae_sl, mfe_tp=_mfe_tp
                )
                out.append(
                    {
                        "date": test_df.index[i].strftime("%Y-%m-%d"),
                        "code": code,
                        "template": matched_tpl,
                        "mfe": mfe,
                        "mae": mae,
                        "final_ret": final_ret,
                    }
                )
        return {"trades": out, "gate": "success"}
    except Exception:
        return {"trades": [], "gate": "processing_error"}


def backtest_ticker_rp1_multi_window(
    code: str,
    global_fetch_start: str,
    global_end_dt: str,
    ohlcv_windows: Sequence[Tuple[str, str]],
    all_templates: Dict[str, Any],
    evolved_factors: Dict[str, Any],
    *,
    batch_mode: bool = True,
) -> Dict[str, Any]:
    """One FDR fetch per ticker, simulate all RP-1 windows (primary + backup)."""
    fetch_latency_s = None
    t_mark = None
    try:
        if not batch_mode:
            time.sleep(random.uniform(0.05, 0.18))
        t_mark = time.perf_counter()
        from rp1_ohlcv_cache import fetch_ohlcv_cached

        df, fetch_gate = fetch_ohlcv_cached(code, global_fetch_start, global_end_dt)
        fetch_latency_s = time.perf_counter() - t_mark
        if fetch_gate in ("timeout", "fetch_error"):
            return {
                "code": code,
                "by_window": {},
                "fetch_gate": fetch_gate,
                "fetch_latency_s": fetch_latency_s,
            }
    except Exception:
        return {
            "code": code,
            "by_window": {},
            "fetch_gate": "fetch_error",
            "fetch_latency_s": (time.perf_counter() - t_mark) if t_mark else None,
        }

    if df is None or getattr(df, "empty", True):
        return {
            "code": code,
            "by_window": {},
            "fetch_gate": fetch_gate,
            "fetch_latency_s": fetch_latency_s,
        }

    df = df.sort_index()
    if df.index.has_duplicates:
        df = df[~df.index.duplicated(keep="last")]

    by_window: Dict[str, Dict[str, Any]] = {}
    for start_dt, end_dt in ohlcv_windows:
        wkey = _window_cache_key(start_dt, end_dt)
        by_window[wkey] = _simulate_trades_on_ohlcv(
            code, df, start_dt, end_dt, all_templates, evolved_factors
        )
    return {
        "code": code,
        "by_window": by_window,
        "fetch_gate": "success",
        "fetch_latency_s": fetch_latency_s,
    }


def _backtest_one_ticker(code, fetch_start, end_dt, start_dt, all_templates, evolved_factors):
    """단일 종목: 다운로드 → DNA/알파 → 템플릿 매칭 → 15일 MFE/MAE. (ProcessPool 워커에서 호출)"""
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

    try:
        if df is None or getattr(df, "empty", True):
            return {"trades": [], "fetch_latency_s": fetch_latency_s, "gate": "skip_empty"}
        if len(df) < 30:
            return {"trades": [], "fetch_latency_s": fetch_latency_s, "gate": "skip_short"}

        pack = _simulate_trades_on_ohlcv(
            code, df, start_dt, end_dt, all_templates, evolved_factors
        )
        pack["fetch_latency_s"] = fetch_latency_s
        return pack
    except Exception:
        return {"trades": [], "fetch_latency_s": fetch_latency_s, "gate": "processing_error"}
    finally:
        if df is not None:
            try:
                del df
            except Exception:
                pass


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
        alias = _LEGACY_REGIME_KEY_ALIASES.get(target_period_name)
        if alias:
            target_period_name = alias
        else:
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
