import ast
import os
import random
import sqlite3
import subprocess
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

try:
    from sklearn.cluster import KMeans
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn"])
    from sklearn.cluster import KMeans

from bitget.config_hub import load_config, save_config_atomic
from bitget.infra.data_paths import flow_csv_path, market_data_db_path
from bitget.supernova_hunter import extract_dna_from_df

DB_PATH = market_data_db_path()
CSV_PATH = flow_csv_path()
TIMEFRAMES = ["1D", "4H", "2H", "1H"]


def _load_mfe_winners(timeframe: str, mfe_min: float = 8.0) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    sql = """
        SELECT
            id, entry_date, exit_date, market_type, symbol, timeframe, position_side, sig_type,
            dyn_cpv, dyn_tb, v_energy, dyn_rs, v_rs, mfe, final_ret
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
          AND UPPER(timeframe)=?
          AND COALESCE(mfe, 0) >= ?
    """
    df = pd.read_sql(sql, conn, params=(str(timeframe).upper(), float(mfe_min)))
    conn.close()
    return df


def _fit_gmm_templates(df: pd.DataFrame, n_components: int = 3):
    features = ["dyn_cpv", "dyn_tb", "v_energy", "dyn_rs"]
    xdf = df.copy()
    xdf["dyn_rs"] = pd.to_numeric(xdf["dyn_rs"], errors="coerce").fillna(pd.to_numeric(xdf["v_rs"], errors="coerce"))
    for col in features:
        xdf[col] = pd.to_numeric(xdf[col], errors="coerce")
    xdf = xdf.dropna(subset=features)
    if len(xdf) < 12:
        return {}

    x = xdf[features].values
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    k = int(max(2, min(n_components, len(xdf) // 4)))
    gmm = GaussianMixture(n_components=k, covariance_type="full", random_state=42)
    labels = gmm.fit_predict(x_scaled)
    xdf["cluster"] = labels

    z90 = 1.6448536269514722
    templates = {}
    for i in range(k):
        sub = xdf[xdf["cluster"] == i].copy()
        if sub.empty:
            continue
        mu = np.asarray(gmm.means_[i], dtype=float)
        cov = np.asarray(gmm.covariances_[i], dtype=float)
        diag = np.diag(cov) if cov.ndim == 2 else np.asarray(cov, dtype=float)
        diag = np.clip(np.nan_to_num(diag, nan=0.0), 0.0, None)
        sigma = np.sqrt(diag) * np.asarray(scaler.scale_, dtype=float)
        mu_orig = np.asarray(scaler.mean_, dtype=float) + (mu * np.asarray(scaler.scale_, dtype=float))

        bounds = {}
        for j, col in enumerate(features):
            lo = float(mu_orig[j] - (z90 * sigma[j]))
            hi = float(mu_orig[j] + (z90 * sigma[j]))
            if lo > hi:
                lo, hi = hi, lo
            bounds[f"{col}_min"] = round(lo, 4)
            bounds[f"{col}_max"] = round(hi, 4)

        side_ratio = float((sub["position_side"].astype(str).str.upper() == "SHORT").mean())
        templates[f"GMM_CLUSTER_{i+1}"] = {
            **bounds,
            "sample_size": int(len(sub)),
            "mean_mfe": round(float(pd.to_numeric(sub["mfe"], errors="coerce").mean()), 4),
            "mean_ret": round(float(pd.to_numeric(sub["final_ret"], errors="coerce").mean()), 4),
            "short_ratio": round(side_ratio, 4),
        }
    return templates


def mine_bitget_dna_templates():
    cfg = load_config()
    all_templates = cfg.get("BITGET_GMM_DNA_TEMPLATES", {})
    if not isinstance(all_templates, dict):
        all_templates = {}

    mined_count = 0
    for tf in TIMEFRAMES:
        df = _load_mfe_winners(tf, mfe_min=float(cfg.get("BITGET_MIN_MFE_FOR_MINING", 8.0)))
        templates = _fit_gmm_templates(df, n_components=int(cfg.get("BITGET_GMM_CLUSTERS", 3)))
        tf_key = f"TF_{tf}"
        all_templates[tf_key] = {
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "source_rows": int(len(df)),
            "templates": templates,
        }
        mined_count += len(templates)

    cfg["BITGET_GMM_DNA_TEMPLATES"] = all_templates
    cfg["BITGET_GMM_DNA_UPDATED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    save_config_atomic(cfg)
    print(f"✅ Bitget GMM DNA mining complete: {mined_count} templates.")


def evaluate_alpha_formula(df: pd.DataFrame, formula: str):
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

    class _Mutator(ast.NodeTransformer):
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

    tree2 = _Mutator().visit(tree)
    ast.fix_missing_locations(tree2)
    try:
        return ast.unparse(tree2.body if isinstance(tree2, ast.Expression) else tree2)
    except Exception:
        return None


def _crossover_alpha_formula_ast(formula1: str, formula2: str):
    try:
        t1 = ast.parse(str(formula1).strip(), mode="eval")
        t2 = ast.parse(str(formula2).strip(), mode="eval")
    except Exception:
        return None

    def collect_nodes(tree):
        out = []
        for n in ast.walk(tree):
            if isinstance(n, (ast.Call, ast.BinOp, ast.Name)):
                out.append(n)
        return out

    p1 = collect_nodes(t1)
    p2 = collect_nodes(t2)
    if not p1 or not p2:
        return None
    n1 = random.choice(p1)
    n2 = random.choice(p2)
    if type(n1) is not type(n2):
        return None
    try:
        n2_clone = ast.parse(ast.unparse(n2), mode="eval").body
    except Exception:
        return None

    class _Swap(ast.NodeTransformer):
        def __init__(self, target, repl):
            self.target = target
            self.repl = repl

        def generic_visit(self, node):
            if node is self.target:
                return self.repl
            return super().generic_visit(node)

    try:
        child = _Swap(n1, n2_clone).visit(t1)
        ast.fix_missing_locations(child)
        return ast.unparse(child.body if isinstance(child, ast.Expression) else child)
    except Exception:
        return None


def _table_name(market_type: str, symbol: str, timeframe: str) -> str:
    prefix = "SPOT" if str(market_type).lower() == "spot" else "FUT"
    return f"BITGET_{prefix}_{str(symbol)}_{str(timeframe).upper()}"


def _load_recent_mfe_training_samples(timeframe: str, days: int = 30):
    tf = str(timeframe).upper()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    q = """
        SELECT market_type, symbol, timeframe, entry_date, mfe
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
          AND UPPER(timeframe)=?
          AND DATE(entry_date) >= DATE('now', ?)
          AND COALESCE(mfe, 0) > 0
    """
    trades = pd.read_sql(q, conn, params=(tf, f"-{int(days)} day"))
    samples = []
    if trades.empty:
        conn.close()
        return samples

    for _, r in trades.iterrows():
        tbl = _table_name(r["market_type"], r["symbol"], r["timeframe"])
        try:
            h = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC',
                conn,
            )
        except Exception:
            continue
        if len(h) < 120:
            continue
        h["Date"] = pd.to_datetime(h["Date"], errors="coerce")
        h = h.dropna(subset=["Date"]).set_index("Date").sort_index()
        if h.empty:
            continue
        entry_dt = pd.to_datetime(str(r["entry_date"]), errors="coerce")
        if pd.isna(entry_dt):
            continue
        samples.append((h, entry_dt, float(r.get("mfe", 0.0) or 0.0)))
    conn.close()
    return samples


def evolve_bitget_ast_formulas(timeframe: str = "1D"):
    samples = _load_recent_mfe_training_samples(timeframe=timeframe, days=30)
    if not samples:
        print(f"⚠️ No recent 30d MFE samples for TF {timeframe}.")
        return

    def mfe_ic_for_formula(formula: str):
        x_vals = []
        y_vals = []
        for df, entry_dt, target_mfe in samples:
            alpha = evaluate_alpha_formula(df, formula)
            if alpha is None:
                continue
            a = pd.to_numeric(alpha, errors="coerce")
            a = a.replace([np.inf, -np.inf], np.nan).dropna()
            if len(a) < 60 or float(a.std(ddof=0)) < 1e-9:
                continue
            sub = a.loc[a.index <= entry_dt]
            if sub.empty:
                continue
            x_vals.append(float(sub.iloc[-1]))
            y_vals.append(float(target_mfe))
        if len(x_vals) < 30:
            return None
        x = pd.Series(x_vals, dtype=float)
        y = pd.Series(y_vals, dtype=float)
        if float(x.std(ddof=0)) < 1e-9:
            return None
        ic = x.corr(y, method="spearman")
        return float(ic) if pd.notna(ic) else None

    cfg = load_config()
    prev = cfg.get("BITGET_EVOLVED_ALPHA_FACTORS", {})
    elites = [str(v).strip() for v in prev.values() if isinstance(v, str) and str(v).strip()]

    scored = []
    seen = set()

    def push_formula(f):
        if f in seen:
            return
        ic = mfe_ic_for_formula(f)
        if ic is None:
            return
        seen.add(f)
        scored.append((f, ic))

    for f in elites:
        push_formula(f)
    for _ in range(260):
        base = random.choice(elites) if elites else generate_random_alpha_formula()
        mf = _mutate_alpha_formula_ast(base) or generate_random_alpha_formula()
        push_formula(mf)
    if len(elites) >= 2:
        for _ in range(140):
            p1, p2 = random.sample(elites, 2)
            cf = _crossover_alpha_formula_ast(p1, p2)
            if not cf:
                continue
            push_formula(cf)
    for _ in range(600):
        push_formula(generate_random_alpha_formula())

    if not scored:
        print("⚠️ No valid evolved alpha formula produced.")
        return
    scored.sort(key=lambda x: x[1], reverse=True)
    top = scored[:3]

    cfg["BITGET_EVOLVED_ALPHA_FACTORS"] = {f"ALPHA_{i+1}": top[i][0] for i in range(len(top))}
    cfg["BITGET_EVOLVED_ALPHA_THRESHOLD"] = float(np.mean([ic for _, ic in top]) * 0.5)
    cfg["BITGET_EVOLVED_ALPHA_TIMEFRAME"] = str(timeframe).upper()
    cfg["BITGET_EVOLVED_ALPHA_FIT_TARGET"] = "MFE_30D"
    cfg["BITGET_EVOLVED_ALPHA_SAMPLE_SIZE"] = int(len(samples))
    cfg["BITGET_EVOLVED_ALPHA_UPDATED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    save_config_atomic(cfg)
    print(f"✅ Bitget AST alpha evolution complete for {timeframe} (target=MFE_30D).")


def build_supernova_csv():
    if not os.path.exists(DB_PATH):
        return 0
    conn = sqlite3.connect(DB_PATH, timeout=30)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%__tmp%'").fetchall()
    out = []
    for (tbl,) in rows:
        if not tbl.startswith("BITGET_") or "__tmp" in tbl:
            continue
        parts = tbl.split("_")
        if len(parts) < 5:
            continue
        tf = parts[-1].upper()
        symbol = "_".join(parts[2:-1])
        df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC', conn)
        if len(df) < 240:
            continue
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date")
        dna = extract_dna_from_df(df, tf)
        if dna is None:
            continue
        out.append(
            {
                "종목코드": symbol,
                "시장": parts[1],
                "랭크": f"MTF_{tf}",
                "[D_Day_당일] 평균_CPV": dna["cpv"],
                "[D_Day_당일] 평균_진짜양봉(TB)": dna["tb"],
                "[D_Day_당일] 평균_응축에너지(BBE)": dna["bbe"],
                "[D_Day_당일] 진모멘텀(TML)": dna["tml"],
                "[D_Day_당일] 평균_시장강도(RS)": dna["rs"],
            }
        )
    conn.close()
    if not out:
        return 0
    pd.DataFrame(out).to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    return len(out)


def run_cluster_mining():
    n = build_supernova_csv()
    if n == 0:
        print("No supernova samples to mine.")
        return
    df = pd.read_csv(CSV_PATH)
    target_features = [
        "[D_Day_당일] 평균_CPV",
        "[D_Day_당일] 평균_진짜양봉(TB)",
        "[D_Day_당일] 평균_응축에너지(BBE)",
        "[D_Day_당일] 진모멘텀(TML)",
        "[D_Day_당일] 평균_시장강도(RS)",
    ]
    clean_df = df.dropna(subset=target_features).copy()
    if len(clean_df) < 10:
        print("Insufficient clean samples for KMeans.")
        return

    X = clean_df[target_features].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    clean_df["Cluster"] = kmeans.fit_predict(X_scaled)

    mined = {}
    for i in range(3):
        cdf = clean_df[clean_df["Cluster"] == i]
        if cdf.empty:
            continue
        mined[f"CLUSTER_{i+1}"] = {
            "cpv_min": float(round(cdf["[D_Day_당일] 평균_CPV"].quantile(0.10), 4)),
            "cpv_max": float(round(cdf["[D_Day_당일] 평균_CPV"].quantile(0.90), 4)),
            "tb_min": float(round(cdf["[D_Day_당일] 평균_진짜양봉(TB)"].quantile(0.10), 4)),
            "tb_max": float(round(cdf["[D_Day_당일] 평균_진짜양봉(TB)"].quantile(0.90), 4)),
            "bbe_min": float(round(cdf["[D_Day_당일] 평균_응축에너지(BBE)"].quantile(0.10), 4)),
            "bbe_max": float(round(cdf["[D_Day_당일] 평균_응축에너지(BBE)"].quantile(0.90), 4)),
            "tml_min": float(round(cdf["[D_Day_당일] 진모멘텀(TML)"].quantile(0.10), 4)),
            "tml_max": float(round(cdf["[D_Day_당일] 진모멘텀(TML)"].quantile(0.90), 4)),
            "rs_min": float(round(cdf["[D_Day_당일] 평균_시장강도(RS)"].quantile(0.10), 4)),
            "rs_max": float(round(cdf["[D_Day_당일] 평균_시장강도(RS)"].quantile(0.90), 4)),
            "sample_size": int(len(cdf)),
        }

    cfg = load_config()
    cfg["LIVE_CLUSTER_TEMPLATES"] = mined
    cfg["LIVE_CLUSTER_UPDATED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    save_config_atomic(cfg)
    print(f"KMeans mining complete: {len(mined)} clusters")


def run_bitget_data_miner(timeframes=None):
    tfs = [str(x).upper() for x in (timeframes or TIMEFRAMES)]
    mine_bitget_dna_templates()
    for tf in tfs:
        evolve_bitget_ast_formulas(tf)
    try:
        run_cluster_mining()
    except Exception as exc:
        print(f"⚠️ cluster mining skipped: {exc}")
    print("🚀 bitget_data_miner run complete.")


if __name__ == "__main__":
    run_bitget_data_miner()
