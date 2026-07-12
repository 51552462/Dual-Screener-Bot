import ast
import concurrent.futures
import json
import os
import random
import sqlite3
import time

import numpy as np
import pandas as pd
import memory_bounds

from bitget.infra.gc_cycle import flush_gc
from bitget.infra.memory_policy import GC_AFTER_SCAN_TABLE, OHLCV_SIGNAL_BAR_LIMIT, SUPERNOVA_SCAN_MAX_WORKERS
import requests

import bitget.shadow_tracking as bitget_shadow_tracking
from bitget.config_hub import load_config, save_config
from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
from bitget.forward_tester import compute_evolved_alpha_bonus_score, try_add_virtual_position
from bitget.infra.bounded_reads import (
    sqlite_bitget_ohlcv_tf_tables_sql,
    sqlite_bitget_scan_tables_sql,
)
from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()
scanned_today_cache = {"spot": set(), "futures": set()}
MARKET_TYPES = ("spot", "futures")
LOG_FILE_SNIPER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sent_log_bitget_supernova.txt")
logger = get_logger("bitget.supernova_hunter")


def _resolve_elastic_scan_cutoffs(cfg, market_type: str):
    """주식 supernova_hunter 의 ElasticThreshold.apply_pair 1차 게이트 (코인 SSOT)."""
    base_cos = float(cfg.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.50))
    base_ml = float(cfg.get("DYNAMIC_ML_BOX_CUTOFF", 0.50))
    try:
        from bitget.evolution.elastic_threshold_bg import BitgetElasticThreshold

        state = BitgetElasticThreshold(cfg, market_type).apply_pair(base_cos, base_ml)
        return float(state.cos_cutoff), float(state.ml_cutoff), state
    except Exception:
        return base_cos, base_ml, None


def _best_ml_box_ratio(cpv: float, tb: float, bbe: float, live_clusters: dict) -> float:
    best = 0.0
    if not isinstance(live_clusters, dict):
        return best
    for bounds in live_clusters.values():
        if not isinstance(bounds, dict):
            continue
        hit = 0
        dims = 0
        for key, val in (("cpv", cpv), ("tb", tb), ("bbe", bbe)):
            lo = bounds.get(f"{key}_min")
            hi = bounds.get(f"{key}_max")
            if lo is None or hi is None:
                continue
            dims += 1
            if float(lo) <= float(val) <= float(hi):
                hit += 1
        if dims > 0:
            best = max(best, hit / float(dims))
    return float(best)


def _evaluate_supernova_scan_gate(
    *,
    eff_cos: float,
    best_dtw: float,
    best_ml_ratio: float,
    eff_cos_cutoff: float,
    eff_ml_cutoff: float,
    dtw_cutoff: float,
    elastic_state,
    cfg: dict,
):
    """
    Returns (allowed, is_scout, scout_path, pass_ml, pass_cosine).
    주식 supernova_hunter: cosine/ML 합격 또는 scout near-miss.
    """
    is_pass_cosine = (float(eff_cos) >= float(eff_cos_cutoff)) and (float(best_dtw) <= float(dtw_cutoff))
    is_pass_ml_box = float(best_ml_ratio) >= float(eff_ml_cutoff)
    if is_pass_ml_box or is_pass_cosine:
        return True, False, "", is_pass_ml_box, is_pass_cosine

    if elastic_state is None:
        return False, False, "", False, False

    try:
        from bitget.evolution.elastic_threshold_bg import evaluate_scout_candidate

        verdict = evaluate_scout_candidate(
            is_pass_cosine=False,
            is_pass_ml_box=False,
            best_cos_sim=float(eff_cos),
            eff_cos_cutoff=float(eff_cos_cutoff),
            ml_score=float(best_ml_ratio),
            eff_ml_cutoff=float(eff_ml_cutoff),
            state=elastic_state,
            sys_config=cfg if isinstance(cfg, dict) else {},
        )
    except Exception:
        return False, False, "", False, False

    if verdict.eligible:
        return True, True, str(verdict.path or ""), False, False
    return False, False, "", False, False


def send_telegram_msg(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception:
        pass


def _tf_scale(tf: str) -> int:
    tfu = str(tf).upper()
    # 1D 기준 1, 4H=6, 2H=12, 1H=24 (하루 캔들 수 비례)
    return {"1D": 1, "4H": 6, "2H": 12, "1H": 24}.get(tfu, 1)


def _scaled_lookbacks(tf: str):
    s = _tf_scale(tf)
    return 150 * s, 200 * s


def evaluate_alpha_formula(df, formula):
    """수식 문자열을 안전한 네임스페이스에서 평가해 시계열을 반환. (AST 샌드박스 검증 추가)"""
    if df is None or getattr(df, 'empty', True):
        return None

    # 1. 샌드박스 검증: 허용된 변수/함수만 있는지, 트리가 너무 깊지 않은지 AST 사전 검사
    ALLOWED_NAMES = {'O', 'H', 'L', 'C', 'V', 'add', 'sub', 'mul', 'div', 'rolling_mean', 'rolling_std'}
    try:
        formula_str = str(formula).strip()
        tree = ast.parse(formula_str, mode='eval')
        node_count = 0
        for node in ast.walk(tree):
            node_count += 1
            if node_count > 150:  # 무한 루프나 비정상적으로 깊은 수식(메모리 폭발) 사전 차단
                return None
            if isinstance(node, ast.Name) and node.id not in ALLOWED_NAMES:
                return None
    except Exception:
        return None

    # 2. 기존 환경 변수 셋업
    O = df["Open"]
    H = df["High"]
    L = df["Low"]
    C = df["Close"]
    V = df["Volume"]

    def add(a, b): return a + b
    def sub(a, b): return a - b
    def mul(a, b): return a * b
    def div(a, b):
        import numpy as np
        safe_b = b.replace(0, np.nan) if isinstance(b, pd.Series) else (np.nan if b == 0 else b)
        return a / safe_b
    def rolling_mean(x, w): return x.rolling(int(w)).mean()
    def rolling_std(x, w): return x.rolling(int(w)).std()

    env = {
        "O": O, "H": H, "L": L, "C": C, "V": V,
        "add": add, "sub": sub, "mul": mul, "div": div,
        "rolling_mean": rolling_mean, "rolling_std": rolling_std,
    }

    # 3. 안전이 검증된 수식만 eval() 실행
    try:
        import numpy as np
        out = eval(formula_str, {"__builtins__": {}}, env)
        if isinstance(out, pd.Series):
            return out.replace([np.inf, -np.inf], np.nan)
    except Exception:
        return None
    return None


def generate_random_alpha_formula():
    windows = [5, 10, 20, 30, 60]
    terminals = ["O", "H", "L", "C", "V"]
    depth = random.randint(3, 5)

    def build_expr(d):
        if d <= 1:
            return random.choice(terminals)
        op = random.choice(["add", "sub", "mul", "div", "rolling_mean", "rolling_std"])
        if op in ("rolling_mean", "rolling_std"):
            return f"{op}({build_expr(d-1)}, {random.choice(windows)})"
        left = build_expr(d - 1)
        right = build_expr(max(1, d - 2))
        return f"{op}({left}, {right})"

    return build_expr(depth)


def _mutate_alpha_formula_ast(formula: str):
    windows = (5, 10, 20, 30, 60)
    terms = ("O", "H", "L", "C", "V")
    binops = ("add", "sub", "mul", "div")
    rolls = ("rolling_mean", "rolling_std")
    try:
        tree = ast.parse(formula.strip(), mode="eval")
    except Exception:
        return None

    class _M(ast.NodeTransformer):
        def visit_Constant(self, node):
            if isinstance(node.value, int) and node.value in windows:
                return ast.copy_location(ast.Constant(value=random.choice(windows)), node)
            return node

        def visit_Name(self, node):
            if isinstance(node.ctx, ast.Load) and node.id in terms:
                return ast.copy_location(ast.Name(id=random.choice(terms), ctx=ast.Load()), node)
            return node

        def visit_Call(self, node):
            self.generic_visit(node)
            if isinstance(node.func, ast.Name):
                fn = node.func.id
                if fn in binops:
                    return ast.copy_location(ast.Call(func=ast.Name(id=random.choice(binops), ctx=ast.Load()), args=node.args, keywords=[]), node)
                if fn in rolls:
                    return ast.copy_location(ast.Call(func=ast.Name(id=random.choice(rolls), ctx=ast.Load()), args=node.args, keywords=[]), node)
            return node

    tree2 = _M().visit(tree)
    ast.fix_missing_locations(tree2)
    try:
        return ast.unparse(tree2.body if isinstance(tree2, ast.Expression) else tree2)
    except Exception:
        return None


def _read_tables(conn, timeframe):
    tf = timeframe.upper()
    sql, params = sqlite_bitget_ohlcv_tf_tables_sql(timeframe=tf)
    rows = conn.execute(sql, params).fetchall()
    out = []
    for (name,) in rows:
        if name.startswith("BITGET_") and name.endswith(f"_{tf}"):
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{name}"'
                f"{memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_SIGNAL_BAR_LIMIT)}",
                conn,
            )
            if not df.empty:
                df = df.sort_values("Date")
            if len(df) >= 240:
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date")
                out.append((name, df))
            else:
                del df
                flush_gc(label=GC_AFTER_OHLCV_BATCH)
    return out


def extract_dna_from_df(df_raw, timeframe="1D"):
    lb150, lb200 = _scaled_lookbacks(timeframe)
    hist_df = df_raw.tail(lb200).copy()
    if len(hist_df) < max(130, lb150 - 20):
        return None
    c, o, h, l, v = (
        hist_df["Close"].values,
        hist_df["Open"].values,
        hist_df["High"].values,
        hist_df["Low"].values,
        hist_df["Volume"].values,
    )
    for n in [10, 20, 30, 60, 112, 224]:
        hist_df[f"EMA{n}"] = hist_df["Close"].ewm(span=n, adjust=False, min_periods=0).mean()

    is_aligned_30 = (hist_df["EMA10"] > hist_df["EMA20"]) & (hist_df["EMA20"] > hist_df["EMA30"])
    with np.errstate(divide='ignore', invalid='ignore'):
        v_ma20 = pd.Series(v).rolling(20).mean().values
        cpv = np.where(h != l, (c - o) / (h - l), 0.5)
        vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
        tb = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
        bb_std = pd.Series(c).rolling(20).std().values
        bb_mid = pd.Series(c).rolling(20).mean().values
        bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
        bbe = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
    idx_arr = np.arange(len(hist_df))
    r_val = hist_df["EMA10"].rolling(10).corr(pd.Series(idx_arr, index=hist_df.index)).fillna(0)
    r_squared = r_val * r_val
    ema10_3 = hist_df["EMA10"].shift(3).fillna(hist_df["EMA10"])
    ema_roc = np.where(ema10_3 != 0, ((hist_df["EMA10"] - ema10_3) / ema10_3) * 5000, 0)
    tml = np.where(is_aligned_30, ema_roc * (r_squared**2), 0)
    dday_idx = int(np.nanargmax(bbe)) if not np.isnan(bbe).all() else len(hist_df) - 1
    c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
    new_shape = [float(np.mean(x)) for x in np.array_split(c_norm, 20)]
    return {
        "cpv": float(cpv[dday_idx]),
        "tb": float(tb[dday_idx]),
        "bbe": float(bbe[dday_idx]),
        "tml": float(tml[dday_idx]),
        "rs": float(((c[-1] - c[max(0, len(c) - 20)]) / max(c[max(0, len(c) - 20)], 1e-9)) * 100.0),
        "shape": new_shape,
        "timeframe": str(timeframe).upper(),
    }


def evolve_alpha_factors(timeframe="1D"):
    conn = get_connection(DB_PATH, read_only=True)
    samples = [df for _, df in _read_tables(conn, timeframe)[:80]]
    conn.close()
    if not samples:
        logger.info("No sample data for alpha evolution.")
        return

    def ic_for_formula(formula):
        all_x, all_y = [], []
        for df in samples:
            alpha = evaluate_alpha_formula(df, formula)
            if alpha is None:
                continue
            a = pd.to_numeric(alpha, errors="coerce")
            if len(a.dropna()) < 60:
                continue
            if float(a.dropna().std(ddof=0)) < 1e-9:
                continue
            fwd5 = (df["Close"].shift(-5) - df["Close"]) / df["Close"]
            pair = pd.concat([a.rename("a"), fwd5.rename("r")], axis=1).dropna()
            if len(pair) < 50:
                continue
            all_x.append(pair["a"])
            all_y.append(pair["r"])
        if not all_x:
            return None
        x = pd.concat(all_x, ignore_index=True)
        y = pd.concat(all_y, ignore_index=True)
        if len(x) < 300:
            return None
        ic = x.corr(y, method="spearman")
        return float(ic) if pd.notna(ic) else None

    cfg = load_config()
    prev = cfg.get("EVOLVED_ALPHA_FACTORS", {})
    elites = [str(v).strip() for v in prev.values() if isinstance(v, str) and str(v).strip()]
    scored = []
    seen = set()

    def push(f):
        if f in seen:
            return
        ic = ic_for_formula(f)
        if ic is None:
            return
        seen.add(f)
        scored.append((f, ic))

    for e in elites:
        push(e)
    for _ in range(300):
        base = random.choice(elites) if elites else generate_random_alpha_formula()
        mf = _mutate_alpha_formula_ast(base) or generate_random_alpha_formula()
        push(mf)
    for _ in range(700):
        push(generate_random_alpha_formula())

    if not scored:
        logger.info("No valid evolved formulas.")
        return
    scored.sort(key=lambda x: x[1], reverse=True)
    top = scored[:3]
    cfg["EVOLVED_ALPHA_FACTORS"] = {f"ALPHA_{i+1}": top[i][0] for i in range(len(top))}
    cfg["EVOLVED_ALPHA_THRESHOLD"] = float(np.mean([ic for _, ic in top]) * 0.5)
    cfg["EVOLVED_ALPHA_TIMEFRAME"] = str(timeframe).upper()
    cfg["EVOLVED_ALPHA_UPDATED_AT"] = utc_datetime_str()
    save_config(cfg)
    logger.info("Evolved alpha factors saved.")


def _get_similarity(vec1, vec2):
    n1, n2 = np.linalg.norm(vec1), np.linalg.norm(vec2)
    return float(np.dot(vec1, vec2) / (n1 * n2)) if n1 > 0 and n2 > 0 else 0.0


def _calc_dtw(s, t):
    n, m = len(s), len(t)
    dtw = np.full((n + 1, m + 1), np.inf)
    dtw[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(float(s[i - 1]) - float(t[j - 1]))
            dtw[i, j] = cost + min(dtw[i - 1, j], dtw[i, j - 1], dtw[i - 1, j - 1])
    return float(dtw[n, m])


def _load_tables_for_scan(conn, market_type, timeframe):
    sql, params = sqlite_bitget_scan_tables_sql(
        market_type=market_type,
        timeframe=timeframe,
        exclude_btc=True,
    )
    return [r[0] for r in conn.execute(sql, params).fetchall()]


def execute_supernova_live_scan(market_type, timeframe):
    market_type = str(market_type).lower()
    tf = str(timeframe).upper()
    logger.info("[%s/%s] supernova live scan start", market_type, tf)

    cfg = load_config()
    try:
        from scanner_regime_ssot import hydrate_intraday_scanner_config

        cfg = hydrate_intraday_scanner_config(cfg, market=f"BG_{market_type.upper()}")
    except Exception as _ssot_e:
        logger.warning("[%s] scanner Kelly SSOT skip: %s", market_type, _ssot_e)
    dynamic_cos_cutoff = float(cfg.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.50))
    dynamic_dtw_cutoff = float(cfg.get("DYNAMIC_DTW_LIMIT", 2.5))
    eff_cos_cutoff, eff_ml_cutoff, elastic_state = _resolve_elastic_scan_cutoffs(cfg, market_type)
    if elastic_state is not None:
        logger.info(
            "[Elastic gate/%s] cos=%.3f ml=%.3f starv=%.2f vol=%.2f",
            market_type,
            eff_cos_cutoff,
            eff_ml_cutoff,
            elastic_state.starvation_index,
            elastic_state.vol_proxy,
        )

    templates = {}
    rank_templates = cfg.get("LIVE_CLUSTER_TEMPLATES", {})
    if isinstance(rank_templates, dict):
        for name, b in rank_templates.items():
            if not isinstance(b, dict):
                continue
            cpv_c = (float(b.get("cpv_min", 0.0)) + float(b.get("cpv_max", 0.0))) / 2.0
            tb_c = (float(b.get("tb_min", 0.0)) + float(b.get("tb_max", 0.0))) / 2.0
            bbe_c = (float(b.get("bbe_min", 0.0)) + float(b.get("bbe_max", 0.0))) / 2.0
            templates[name] = {"vec": np.array([cpv_c, tb_c, bbe_c], dtype=float), "shape": None}

    conn = get_connection(DB_PATH, read_only=True)
    tables = _load_tables_for_scan(conn, market_type, tf)
    conn.close()
    if not tables:
        return

    def worker(tbl):
        cconn = None
        try:
            symbol = "_".join(tbl.split("_")[2:-1])
            uniq = f"{symbol}:{tf}"
            if uniq in scanned_today_cache.get(market_type, set()):
                return None

            cconn = get_connection(DB_PATH, read_only=True)
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}"'
                f"{memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_SIGNAL_BAR_LIMIT)}",
                cconn,
            )
            if not df.empty:
                df = df.sort_values("Date")
            if df.empty or len(df) < 220:
                return None
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
            c, o, h, l, v = df["Close"].values, df["Open"].values, df["High"].values, df["Low"].values, df["Volume"].values
            if len(c) < 60:
                return None

            with np.errstate(divide='ignore', invalid='ignore'):
                v_ma20 = pd.Series(v).rolling(20).mean().values
                cpv = np.where(h != l, (c - o) / (h - l), 0.5)[-1]
                vol_mult = (v[-1] / v_ma20[-1]) if v_ma20[-1] > 0 else 1.0
                tb = vol_mult / max(cpv, 0.01) if cpv > 0 else vol_mult / 0.01
                bb_std = pd.Series(c).rolling(20).std().values[-1]
                bb_mid = pd.Series(c).rolling(20).mean().values[-1]
                bb_width = (4 * bb_std) / bb_mid if bb_mid > 0 else 0.01
                bbe = (1.0 / bb_width) * vol_mult if bb_width > 0 else 0
            current_vec = np.nan_to_num(np.array([cpv, tb, bbe], dtype=float))
            c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
            current_shape = np.mean(np.array_split(c_norm[-200:], 20), axis=1)

            best_name = "UNKNOWN"
            best_cos = 0.0
            best_dtw = 999.0
            for t_name, t in templates.items():
                t_vec = t["vec"]
                cos = _get_similarity(current_vec, t_vec)
                dtw = _calc_dtw(current_shape, current_shape if t["shape"] is None else t["shape"])
                if cos > best_cos:
                    best_cos = cos
                    best_dtw = dtw
                    best_name = t_name

            ev_df = df[["Open", "High", "Low", "Close", "Volume"]].tail(300).copy()
            for col in ev_df.columns:
                ev_df[col] = pd.to_numeric(ev_df[col], errors="coerce")
            ev_df = ev_df.dropna(subset=("Open", "High", "Low", "Close", "Volume"), how="any")
            alpha_bonus = compute_evolved_alpha_bonus_score(cfg, ev_df)
            eff_cos = min(1.0, best_cos + alpha_bonus)
            best_ml_ratio = _best_ml_box_ratio(float(cpv), float(tb), float(bbe), rank_templates)
            allowed, is_scout, scout_path, is_pass_ml, is_pass_cos = _evaluate_supernova_scan_gate(
                eff_cos=eff_cos,
                best_dtw=best_dtw,
                best_ml_ratio=best_ml_ratio,
                eff_cos_cutoff=eff_cos_cutoff,
                eff_ml_cutoff=eff_ml_cutoff,
                dtw_cutoff=dynamic_dtw_cutoff,
                elastic_state=elastic_state,
                cfg=cfg,
            )
            if not allowed:
                return None

            facts = {
                "v_cpv": float(cpv),
                "v_yang": float(tb),
                "v_energy": float(bbe),
                "v_rs": float(((c[-1] - c[-20]) / c[-20]) * 100.0 if c[-20] != 0 else 0.0),
                "dyn_rs": 0.0,
                "dyn_cpv": 0.0,
                "dyn_tb": 0.0,
                "sn_score": float(best_cos),
                "dtw_score": float(best_dtw if np.isfinite(best_dtw) else 999.0),
                "ml_box_score": float(best_ml_ratio),
            }
            if is_scout:
                facts["_fluid_scout"] = True
                sig_type = (
                    f"[🔭SCOUT/{scout_path}][SUPERNOVA][{tf}] {best_name} "
                    f"(Cos:{eff_cos * 100:.1f}%|DTW:{best_dtw:.2f}|starv"
                    f"{float(getattr(elastic_state, 'starvation_index', 0) or 0):.0%})"
                )
            elif is_pass_ml:
                sig_type = (
                    f"[SUPERNOVA_MLBOX][{tf}] 🤖 {best_name} "
                    f"(ML:{best_ml_ratio*100:.1f}%|Cos:{eff_cos*100:.1f}%)"
                )
                facts["ml_box_pass"] = True
            else:
                sig_type = f"[SUPERNOVA][{tf}] 🦅 {best_name} (Cos:{eff_cos*100:.1f}%|DTW:{best_dtw:.2f})"
            result = {
                "symbol": symbol,
                "sig_type": sig_type,
                "score": eff_cos * 100.0,
                "entry_price": float(c[-1]),
                "facts": facts,
                "uniq": uniq,
            }
            del df, c, o, h, l, v
            flush_gc(label=GC_AFTER_SCAN_TABLE)
            return result
        except Exception:
            return None
        finally:
            if cconn is not None:
                try:
                    cconn.close()
                except Exception:
                    pass

    valid = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SUPERNOVA_SCAN_MAX_WORKERS) as executor:
        for r in executor.map(worker, tables):
            if r:
                valid.append(r)
    flush_gc(label="supernova_scan_batch")

    for target in valid:
        ok, msg = try_add_virtual_position(
            market_type=market_type,
            symbol=target["symbol"],
            timeframe=tf,
            sig_type=target["sig_type"],
            score=target["score"],
            entry_price=target["entry_price"],
            facts=target["facts"],
        )
        if not ok:
            rsn = str(msg)
            if ("ANTI_PATTERNS" in rsn) or ("TOXIC" in rsn) or ("DOOMSDAY" in rsn):
                try:
                    bitget_shadow_tracking.record_blocked_trade(
                        symbol=target["symbol"],
                        reason=rsn,
                        entry_price=float(target["entry_price"]),
                        market_type=market_type,
                        name=target["symbol"],
                        position_side="LONG",
                        timeframe=tf,
                    )
                except Exception:
                    pass
        if ok:
            scanned_today_cache[market_type].add(target["uniq"])
            # 💡 [버그 픽스] 스나이퍼 발사 기록을 물리 파일에 즉시 저장
            try:
                with open(LOG_FILE_SNIPER, "a", encoding="utf-8") as f:
                    f.write(f"{market_type}|{target['uniq']}\n")
            except Exception:
                pass
            
            send_telegram_msg(
                f"🦅 <b>[SUPERNOVA 실시간 저격]</b>\n"
                f"시장: {market_type} | TF: {tf}\n"
                f"종목: {target['symbol']}\n"
                f"시그널: {target['sig_type']}\n"
                f"점수: {target['score']:.1f}\n"
                f"장부: {msg}"
            )
    logger.info("[%s/%s] supernova live scan done: %s candidates", market_type, tf, len(valid))


def run_live_sniper_scheduler():
    from bitget.infra.daemon_loop import (
        SNIPER_SCHEDULER_ERROR_SEC,
        SNIPER_SCHEDULER_POLL_SEC,
        SNIPER_SCHEDULER_POST_SCAN_SLEEP_SEC,
        UtcTick,
        sleep_or_backoff,
    )

    logger.info("supernova sniper scheduler waiting (1H/2H/4H/1D UTC candle close)")
    logger.info(" - 1H: every hour")
    logger.info(" - 2H: even hours")
    logger.info(" - 4H: 0/4/8/12/16/20")
    logger.info(" - 1D: 00:05 UTC")
    tick = UtcTick()
    tick.refresh()
    last_day = tick.day_key

    # 💡 [버그 픽스] 서버 재부팅 시 기억 복원
    if os.path.exists(LOG_FILE_SNIPER):
        try:
            with open(LOG_FILE_SNIPER, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
                if lines and lines[0] == last_day:
                    for line in lines[1:]:
                        if "|" in line:
                            m_type, uniq = line.split("|", 1)
                            if m_type in scanned_today_cache:
                                scanned_today_cache[m_type].add(uniq)
        except Exception:
            pass

    loop_error = False
    while True:
        try:
            tick.refresh()
            if tick.day_key != last_day:
                scanned_today_cache["spot"].clear()
                scanned_today_cache["futures"].clear()
                last_day = tick.day_key
                try:
                    with open(LOG_FILE_SNIPER, "w", encoding="utf-8") as f:
                        f.write(tick.day_key + "\n")
                except Exception:
                    pass

            if tick.minute == 0:
                for mt in MARKET_TYPES:
                    execute_supernova_live_scan(mt, "1H")
                    if tick.hour % 2 == 0:
                        execute_supernova_live_scan(mt, "2H")
                    if tick.hour % 4 == 0:
                        execute_supernova_live_scan(mt, "4H")
                loop_error = False
                sleep_or_backoff(
                    normal_sec=SNIPER_SCHEDULER_POST_SCAN_SLEEP_SEC,
                    after_error=False,
                )
                continue
            if tick.hour == 0 and tick.minute == 5:
                for mt in MARKET_TYPES:
                    execute_supernova_live_scan(mt, "1D")
                loop_error = False
                sleep_or_backoff(
                    normal_sec=SNIPER_SCHEDULER_POST_SCAN_SLEEP_SEC,
                    after_error=False,
                )
                continue
            loop_error = False
            sleep_or_backoff(normal_sec=SNIPER_SCHEDULER_POLL_SEC, after_error=loop_error)
        except Exception as e:
            log_exception(logger, "supernova sniper scheduler error: %s", e)
            loop_error = True
            sleep_or_backoff(
                normal_sec=SNIPER_SCHEDULER_POLL_SEC,
                after_error=loop_error,
                error_sec=SNIPER_SCHEDULER_ERROR_SEC,
            )


def mine_supernova_templates_by_timeframe(timeframe="1D"):
    """
    원본 로직 복구:
    1) 랭크(클러스터)별 1차 분류
    2) BBE 중간값 기준 2차 정밀 분리
       - STEALTH (조용한 매집)
       - VOLATILE (변동성 폭발)
    3) 타임프레임별 템플릿 저장
    """
    tf = str(timeframe).upper()
    conn = get_connection(DB_PATH, read_only=True)
    samples = _read_tables(conn, tf)
    conn.close()
    if not samples:
        logger.info("[%s] no samples for template mining", tf)
        return {}

    # 1) D-Day DNA 추출
    dna_rows = []
    for tname, df in samples:
        dna = extract_dna_from_df(df, tf)
        if dna is None:
            continue
        dna["source_table"] = tname
        # 간이 랭크: TML 분위로 A/B/C/D
        tml = float(dna.get("tml", 0.0))
        if tml >= 180:
            rank = "A"
        elif tml >= 100:
            rank = "B"
        elif tml >= 40:
            rank = "C"
        else:
            rank = "D"
        dna["rank"] = rank
        dna_rows.append(dna)

    if not dna_rows:
        logger.info("[%s] no valid DNA", tf)
        return {}

    # 2) 랭크별 1차 분류
    rank_dnas = {"A": [], "B": [], "C": [], "D": []}
    for d in dna_rows:
        rank_dnas[str(d.get("rank", "D"))].append(d)

    def _make_template(sub_dnas):
        if not sub_dnas:
            return None
        return {
            "cpv": float(np.mean([x["cpv"] for x in sub_dnas])),
            "tb": float(np.mean([x["tb"] for x in sub_dnas])),
            "bbe": float(np.mean([x["bbe"] for x in sub_dnas])),
            "rs": float(np.mean([x["rs"] for x in sub_dnas])),
            "tml": float(np.mean([x["tml"] for x in sub_dnas])),
            "shape": np.mean([x["shape"] for x in sub_dnas], axis=0).tolist(),
            "sample_size": int(len(sub_dnas)),
            "timeframe": tf,
        }

    # 3) BBE 중간값 기반 STEALTH/VOLATILE 2차 분리 (원본 핵심)
    tf_templates = {}
    split_counts = {"stealth": 0, "volatile": 0}
    for rank, dnas in rank_dnas.items():
        if not dnas:
            continue
        median_bbe = float(np.median([d["bbe"] for d in dnas]))
        stealth_dnas = [d for d in dnas if float(d["bbe"]) <= median_bbe]
        volatile_dnas = [d for d in dnas if float(d["bbe"]) > median_bbe]

        stealth_tpl = _make_template(stealth_dnas)
        volatile_tpl = _make_template(volatile_dnas)

        if stealth_tpl:
            key = f"{tf}_RANK_{rank}_STEALTH"
            tf_templates[key] = stealth_tpl
            split_counts["stealth"] += 1
        if volatile_tpl:
            key = f"{tf}_RANK_{rank}_VOLATILE"
            tf_templates[key] = volatile_tpl
            split_counts["volatile"] += 1

    cfg = load_config()
    multi_key = "DNA_SUPERNOVA_MTF_TEMPLATES"
    existing = cfg.get(multi_key, {})
    if not isinstance(existing, dict):
        existing = {}
    # 타임프레임 단위로 덮어써 최신화
    for k in list(existing.keys()):
        if str(k).startswith(f"{tf}_"):
            existing.pop(k, None)
    existing.update(tf_templates)
    cfg[multi_key] = existing
    cfg[f"{tf}_TEMPLATE_UPDATED_AT"] = utc_datetime_str()
    save_config(cfg)

    send_telegram_msg(
        f"🧬 <b>[{tf} 템플릿 복구 완료]</b>\n"
        f"STEALTH {split_counts['stealth']}개 | VOLATILE {split_counts['volatile']}개\n"
        f"총 저장: {len(tf_templates)}개"
    )
    logger.info("[%s] STEALTH/VOLATILE templates saved: %s", tf, len(tf_templates))
    return tf_templates

