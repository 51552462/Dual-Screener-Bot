# supernova_hunter.py (V53.2 글로벌 초신성 역추적 & 텔레그램 보고 엔진)
import os, time, json, sqlite3, ast
import pandas as pd
import numpy as np
import yfinance as yf
import FinanceDataReader as fdr
import concurrent.futures
import random
from datetime import datetime, timedelta
import pytz
import warnings
from io import StringIO
import requests
warnings.filterwarnings('ignore')
import auto_forward_tester as aft
from yf_download_flatten import flatten_yf_download_df
scanned_today_cache = {'KR': set(), 'US': set()}

# ==========================================
# 💡 [환경 설정 및 텔레그램 세팅]
# ==========================================
CONFIG_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'system_config.json')
TELEGRAM_TOKEN = "7988939051:AAH18gmMs9syze2g4zo7Xd2stMdyREg66rI"
TELEGRAM_CHAT_ID = "6838834566"

def send_telegram_msg(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except: pass

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as f: return json.load(f)
    return {}

def save_config(data):
    temp_path = f"{CONFIG_PATH}.temp"
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CONFIG_PATH)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        print(f"⚠️ JSON 관제탑 원자적 저장 실패: {e}")

def generate_random_alpha_formula():
    """O/H/L/C/V와 연산자를 조합해 깊이 3~5의 랜덤 수식을 생성."""
    windows = [5, 10, 20, 30, 60]
    terminals = ['O', 'H', 'L', 'C', 'V']
    depth = random.randint(3, 5)

    def build_expr(d):
        if d <= 1:
            return random.choice(terminals)

        op = random.choice(['add', 'sub', 'mul', 'div', 'rolling_mean', 'rolling_std'])
        if op in ('rolling_mean', 'rolling_std'):
            return f"{op}({build_expr(d-1)}, {random.choice(windows)})"
        left = build_expr(d - 1)
        right = build_expr(max(1, d - 2))
        return f"{op}({left}, {right})"

    return build_expr(depth)

def evaluate_alpha_formula(df, formula):
    """수식 문자열을 안전한 네임스페이스에서 평가해 시계열을 반환."""
    if df.empty:
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
        safe_b = b.replace(0, np.nan) if isinstance(b, pd.Series) else (np.nan if b == 0 else b)
        return a / safe_b
    def rolling_mean(x, w): return x.rolling(int(w)).mean()
    def rolling_std(x, w): return x.rolling(int(w)).std()

    env = {
        'O': O, 'H': H, 'L': L, 'C': C, 'V': V,
        'add': add, 'sub': sub, 'mul': mul, 'div': div,
        'rolling_mean': rolling_mean, 'rolling_std': rolling_std
    }

    try:
        result = eval(formula, {"__builtins__": {}}, env)
        if isinstance(result, pd.Series):
            return result.replace([np.inf, -np.inf], np.nan)
    except Exception:
        return None
    return None


def _mutate_alpha_formula_ast(formula: str) -> str:
    """엘리트 수식 AST 소변형(윈도·O/H/L/C/V·동 arity 연산자만 교체). unparse 불가 시 None."""
    windows = (5, 10, 20, 30, 60)
    terms = ('O', 'H', 'L', 'C', 'V')
    binops = ('add', 'sub', 'mul', 'div')
    rolls = ('rolling_mean', 'rolling_std')
    try:
        tree = ast.parse(formula.strip(), mode='eval')
    except Exception:
        return None

    class _M(ast.NodeTransformer):
        def visit_Num(self, node):
            if isinstance(node.n, int) and node.n in windows:
                return ast.copy_location(ast.Num(n=random.choice(windows)), node)
            return node

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
                    neo = random.choice(binops)
                    return ast.copy_location(
                        ast.Call(func=ast.Name(id=neo, ctx=ast.Load()), args=node.args, keywords=[]), node
                    )
                if fn in rolls and len(node.args) >= 2:
                    neo = random.choice(rolls)
                    return ast.copy_location(
                        ast.Call(func=ast.Name(id=neo, ctx=ast.Load()), args=node.args, keywords=[]), node
                    )
            return node

    tree2 = _M().visit(tree)
    ast.fix_missing_locations(tree2)
    try:
        if isinstance(tree2, ast.Expression):
            return ast.unparse(tree2.body)
        return ast.unparse(tree2)
    except Exception:
        return None


def _crossover_alpha_formula_ast(formula1: str, formula2: str) -> str:
    """💡 [V_NEXT 진화 로직 적용] 두 수식 AST 서브트리를 교차 교환해 자식 수식을 생성. 실패 시 None."""
    try:
        t1 = ast.parse(str(formula1).strip(), mode='eval')
        t2 = ast.parse(str(formula2).strip(), mode='eval')
    except Exception:
        return None

    def _collect_swappable_nodes(tree):
        nodes = []
        for n in ast.walk(tree):
            # 안전한 연산 의미를 보존하기 위해 Call/BinOp/Name만 교차 대상으로 허용
            if isinstance(n, (ast.Call, ast.BinOp, ast.Name)):
                nodes.append(n)
        return nodes

    try:
        pool1 = _collect_swappable_nodes(t1)
        pool2 = _collect_swappable_nodes(t2)
        if not pool1 or not pool2:
            return None
        n1 = random.choice(pool1)
        n2 = random.choice(pool2)
        if type(n1) is not type(n2):
            return None
        n2_clone = ast.copy_location(ast.fix_missing_locations(ast.parse(ast.unparse(n2), mode='eval').body), n1)
    except Exception:
        return None

    class _Swap(ast.NodeTransformer):
        def __init__(self, target_node, replacement_node):
            self.target_node = target_node
            self.replacement_node = replacement_node

        def generic_visit(self, node):
            if node is self.target_node:
                return self.replacement_node
            return super().generic_visit(node)

    try:
        child_tree = _Swap(n1, n2_clone).visit(t1)
        ast.fix_missing_locations(child_tree)
        if isinstance(child_tree, ast.Expression):
            return ast.unparse(child_tree.body)
        return ast.unparse(child_tree)
    except Exception:
        return None


def evolve_alpha_factors():
    """무작위 + 엘리트 보존 + 돌연변이 + 교차(Crossover)로 IC 상위 직교 알파 최대 3개 저장."""
    print("🧠 [알파 인큐베이터] 무작위 수식 진화 평가 시작...")
    stock_pool = []
    try:
        kr_codes = get_krx_list()['Code'].tolist()[:30]
        us_codes = get_us_list()['Code'].tolist()[:30]
        stock_pool = [('KR', c) for c in kr_codes] + [('US', c) for c in us_codes]
    except Exception as e:
        print(f"⚠️ 종목 풀 구성 실패: {e}")
        return

    start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')

    def fetch_df(item):
        mkt, code = item
        try:
            if mkt == 'KR':
                df = fdr.DataReader(code, start_date)
            else:
                df = yf.download(code, start=start_date, progress=False)
                df = flatten_yf_download_df(df)
            if df.empty or len(df) < 120:
                return None
            need_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(c in df.columns for c in need_cols):
                return None
            return df[need_cols].dropna().tail(300).copy()
        except Exception:
            return None

    sample_dfs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for res in executor.map(fetch_df, stock_pool):
            if res is not None:
                sample_dfs.append(res)

    if not sample_dfs:
        print("⚠️ 알파 진화용 샘플 데이터가 없습니다.")
        return

    def _ic_spearman_for_formula(formula: str):
        all_x, all_y = [], []
        for df in sample_dfs:
            alpha = evaluate_alpha_formula(df, formula)
            if alpha is None:
                continue
            # AST 돌연변이 쓰레기 수식 방어: 상수 0·과도 NaN·무변동
            a = pd.to_numeric(alpha, errors='coerce')
            n_a = len(a)
            if n_a == 0:
                continue
            if float(a.isna().sum()) / float(n_a) > 0.30:
                continue
            a_fin = a.dropna()
            if len(a_fin) == 0:
                continue
            if (a_fin == 0).all():
                continue
            std_a = float(a_fin.std(ddof=0))
            if not np.isfinite(std_a) or std_a < 1e-9:
                continue
            future_ret_5d = (df['Close'].shift(-5) - df['Close']) / df['Close']
            pair = pd.concat([alpha.rename('alpha'), future_ret_5d.rename('ret5')], axis=1).dropna()
            if len(pair) < 30:
                continue
            all_x.append(pair['alpha'])
            all_y.append(pair['ret5'])
        if not all_x:
            return None
        x = pd.concat(all_x, ignore_index=True)
        y = pd.concat(all_y, ignore_index=True)
        if len(x) < 200:
            return None
        ic = x.corr(y, method='spearman')
        if pd.notna(ic):
            return float(ic)
        return None

    def _pearson_alpha_vs_first(first_formula: str, cand_formula: str):
        """표본 종목별로 두 수식 알파를 동일 인덱스로 맞춘 뒤 이어붙여, 1위 대비 후보의 피어슨 상관을 계산."""
        all_a, all_b = [], []
        for df in sample_dfs:
            ag = evaluate_alpha_formula(df, first_formula)
            bg = evaluate_alpha_formula(df, cand_formula)
            if ag is None or bg is None:
                continue
            a = pd.to_numeric(ag, errors='coerce')
            b = pd.to_numeric(bg, errors='coerce')
            pair = pd.concat([a.rename('_a'), b.rename('_b')], axis=1).dropna()
            if len(pair) < 30:
                continue
            na = pair.iloc[:, 0]
            nb = pair.iloc[:, 1]
            n_a = len(na)
            if n_a == 0 or float(na.isna().sum()) / float(n_a) > 0.30:
                continue
            if (na == 0).all() or (nb == 0).all():
                continue
            if float(na.std(ddof=0)) < 1e-9 or float(nb.std(ddof=0)) < 1e-9:
                continue
            all_a.append(na.reset_index(drop=True))
            all_b.append(nb.reset_index(drop=True))
        if not all_a:
            return None
        xa = pd.concat(all_a, ignore_index=True)
        xb = pd.concat(all_b, ignore_index=True)
        if len(xa) < 200:
            return None
        r = xa.corr(xb, method='pearson')
        if pd.notna(r):
            return float(r)
        return None

    # 💡 [100년 영속 진화 로직 적용: Orthogonal Alpha Gram-Schmidt Gate]
    def _alpha_vector_for_formula(formula: str):
        """수식별 알파 시계열을 표본 전체에서 이어붙인 벡터를 생성. 실패 시 None."""
        all_s = []
        for _df in sample_dfs:
            _a = evaluate_alpha_formula(_df, formula)
            if _a is None:
                continue
            _s = pd.to_numeric(_a, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
            if len(_s) < 30:
                continue
            # 💡 [100년 영속 진화 로직 적용: Orthogonal Alpha Time-Index Integrity]
            # 날짜 인덱스를 절대 버리지 않고 유지해, 동일한 날짜끼리만 정렬·집계합니다.
            all_s.append(_s.sort_index())
        if not all_s:
            return None

        # 종목별 알파 시계열을 날짜 축으로 정렬한 뒤, 날짜별 평균 알파로 대표 벡터 생성
        alpha_by_date = pd.concat(all_s, axis=1).mean(axis=1, skipna=True).dropna().astype(float)
        if len(alpha_by_date) < 200:
            return None
        return alpha_by_date

    # 💡 [100년 영속 진화 로직 적용: Orthogonal Alpha Gram-Schmidt Gate]
    def _is_orthogonal_candidate(selected_formulas, cand_formula: str):
        """
        기존 선택군 + 후보의 상관행렬식(det)을 검사.
        det가 0에 가까우면 다중공선성(선형 종속)으로 판정해 후보를 폐기.
        """
        pool = list(selected_formulas) + [cand_formula]
        series_map = {}
        for f in pool:
            v = _alpha_vector_for_formula(f)
            if v is None:
                return False
            series_map[f] = v

        # 💡 [100년 영속 진화 로직 적용: Orthogonal Alpha Time-Index Integrity]
        # 후보군 시계열을 날짜 인덱스 기준으로 교집합 병합해 동시점 상관행렬만 계산
        mat_cols = [series_map[f].rename(f) for f in pool]
        mat_df = pd.concat(mat_cols, axis=1).dropna()
        if len(mat_df) < 200:
            return False

        try:
            corr = mat_df.corr(method='pearson').values
            det = float(np.linalg.det(corr))
            if not np.isfinite(det):
                return False
            # 3x3 기준 det가 매우 작으면 사실상 선형 종속(비직교)으로 간주
            return det > 0.05
        except Exception:
            return False

    def _forward_elite_gate():
        """알파 융합 태그 청산 건이 장부에서 양호하면 기존 EVOLVED 알파 엘리트 보존."""
        dbp = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')
        if not os.path.exists(dbp):
            return False
        try:
            conn = sqlite3.connect(dbp, timeout=30)
            sub = pd.read_sql(
                """SELECT final_ret FROM forward_trades
                   WHERE status LIKE 'CLOSED%' AND sig_type LIKE '%알파 융합%'""",
                conn,
            )
            conn.close()
        except Exception:
            return False
        if sub is None or len(sub) < 5:
            return False
        wr = float((sub['final_ret'] > 0).mean())
        mr = float(sub['final_ret'].mean())
        return wr >= 0.48 and mr > 0.0

    # // [수정 전] config["EVOLVED_ALPHA_FACTORS"] = { ALPHA_i: top3[i] } 무조건 덮어쓰기
    # // [수정 후] 장부 실적 기반 엘리트 보존 + 일부 AST 변형으로 1000회 풀 구성
    cfg_pre = load_config()
    prev_factors = cfg_pre.get("EVOLVED_ALPHA_FACTORS")
    if not isinstance(prev_factors, dict):
        prev_factors = {}
    elite_formulas = [str(v).strip() for v in prev_factors.values() if isinstance(v, str) and str(v).strip()]
    use_elite = _forward_elite_gate() and bool(elite_formulas)
    if use_elite:
        print(f"🛡️ [엘리트주의] forward_trades 실적 양호 — 기존 알파 {len(elite_formulas)}개를 진화 풀에 보존·AST 변형 시드로 사용합니다.")

    scored = []
    seen = set()

    def _push_formula(formula: str):
        if formula in seen:
            return
        icv = _ic_spearman_for_formula(formula)
        if icv is None:
            return
        seen.add(formula)
        scored.append((formula, icv))

    n_total = 1000
    n_mut = 0
    n_cross = 0
    if use_elite:
        n_mut = min(max(80, len(elite_formulas) * 35), n_total // 2)
        # 💡 [V_NEXT 진화 로직 적용] 엘리트 기반 교차 자식 생성 슬롯 확보 (최대 20%)
        n_cross = min(max(40, len(elite_formulas) * 15), n_total // 5)
    n_rand = max(0, n_total - n_mut - n_cross)

    if use_elite:
        for ef in elite_formulas:
            _push_formula(ef)

    for _ in range(n_mut):
        base = random.choice(elite_formulas)
        mf = _mutate_alpha_formula_ast(base)
        if not mf or mf == base:
            mf = generate_random_alpha_formula()
        _push_formula(mf)

    # 💡 [V_NEXT 진화 로직 적용] 엘리트 간 AST 교차(Crossover)로 자식 수식 생성
    for _ in range(n_cross):
        try:
            if len(elite_formulas) < 2:
                break
            p1, p2 = random.sample(elite_formulas, 2)
            cf = _crossover_alpha_formula_ast(p1, p2)
            if not cf or cf in (p1, p2):
                # 교차 실패 시 돌연변이로 안전 폴백
                cf = _mutate_alpha_formula_ast(random.choice([p1, p2])) or generate_random_alpha_formula()
            _push_formula(cf)
        except Exception:
            _push_formula(generate_random_alpha_formula())

    for _ in range(n_rand):
        _push_formula(generate_random_alpha_formula())

    if not scored:
        print("⚠️ 유효한 알파 수식이 없어 저장을 건너뜁니다. (기존 EVOLVED_ALPHA_FACTORS 유지)")
        return

    scored.sort(key=lambda x: x[1], reverse=True)
    top3 = []
    if scored:
        # 💡 [100년 영속 진화 로직 적용: Orthogonal Alpha Gram-Schmidt Gate]
        top3.append(scored[0])
        for cand in scored[1:]:
            if len(top3) >= 3:
                break
            # 1차 관문: 기존 1위 대비 과상관 후보 제거
            first_formula = top3[0][0]
            pr = _pearson_alpha_vs_first(first_formula, cand[0])
            if pr is not None and abs(pr) >= 0.85:
                continue
            # 2차 관문: 선택군 전체 상관행렬식(det)으로 다중공선성 제거
            selected_formulas = [f for f, _ in top3]
            if not _is_orthogonal_candidate(selected_formulas, cand[0]):
                continue
            top3.append(cand)

    config = load_config()
    config["EVOLVED_ALPHA_FACTORS"] = {
        f"ALPHA_{i+1}": top3[i][0] for i in range(len(top3))
    }
    config["EVOLVED_ALPHA_THRESHOLD"] = float(np.mean([ic for _, ic in top3]) * 0.5)
    save_config(config)

    msg = "🧬 <b>[알파 진화 완료]</b>\n"
    for i, (formula, ic) in enumerate(top3, 1):
        msg += f"▪️ ALPHA_{i} (IC {ic:.4f}): <code>{formula}</code>\n"
    send_telegram_msg(msg)
    print("✅ EVOLVED_ALPHA_FACTORS 저장 완료")

# ==========================================
# 💡 [전체 상장 종목 리스트 수집기]
# ==========================================
def get_krx_list():
    headers = {'User-Agent': 'Mozilla/5.0'}
    df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", headers=headers, verify=False).text), header=0)[0]
    df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", headers=headers, verify=False).text), header=0)[0]
    df = pd.concat([df_ks, df_kq])
    df['Code'] = df['종목코드'].astype(str).str.zfill(6)
    df = df.rename(columns={'회사명': 'Name'})
    junk_pattern = '스팩|ETN|ETF|우$|홀딩스|리츠|선물|인버스|제[0-9]+호|신주인수권'
    return df[~df['Name'].str.contains(junk_pattern, regex=True)][['Code', 'Name']].drop_duplicates('Code')

def get_us_list():
    try:
        df_nasdaq = fdr.StockListing('NASDAQ')
        df_nyse = fdr.StockListing('NYSE')
        df_amex = fdr.StockListing('AMEX')
        df = pd.concat([df_nasdaq, df_nyse, df_amex])
        df = df[df['Symbol'].str.isalpha()]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].rename(columns={'Symbol': 'Code'}).drop_duplicates('Code')
    except: return pd.DataFrame()

# ==========================================
# 💡 [핵심] 타임머신 DNA 추출기 (한/미 시장 완벽 분리형 3단계 기만술 및 랭크 정밀 필터)
# ==========================================
def extract_dna_from_df(df_raw, benchmarks, target_date, rank_name="UNKNOWN", market="KR"):
    try:
        # 💡 [수정] 150일 -> 200일로 늘려 6개월(약 125 거래일) 이상의 데이터를 완벽 확보
        hist_df = df_raw[df_raw.index <= target_date].tail(200).copy()
        if len(hist_df) < 130: return None # 6개월 데이터가 안 되면 기각
        
        c, o, h, l, v = hist_df['Close'].values, hist_df['Open'].values, hist_df['High'].values, hist_df['Low'].values, hist_df['Volume'].values
        trd_val_eok = (c * v) / 100_000_000 
        
        for n in [10, 20, 30, 60, 112, 224]:
            hist_df[f'EMA{n}'] = hist_df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
        
        is_aligned_30 = (hist_df['EMA10'] > hist_df['EMA20']) & (hist_df['EMA20'] > hist_df['EMA30'])
        is_aligned_112 = is_aligned_30 & (hist_df['EMA30'] > hist_df['EMA60']) & (hist_df['EMA60'] > hist_df['EMA112'])
        
        v_ma20 = pd.Series(v).rolling(20).mean().values
        cpv = np.where(h != l, (c - o) / (h - l), 0.5)
        vol_mult = np.where(v_ma20 > 0, v / v_ma20, 1.0)
        tb = np.where(cpv > 0, vol_mult / np.maximum(cpv, 0.01), vol_mult / 0.01)
        
        bb_std = pd.Series(c).rolling(20).std().values
        bb_mid = pd.Series(c).rolling(20).mean().values
        bb_width = np.where(bb_mid > 0, (4 * bb_std) / bb_mid, 0.01)
        bbe = np.where(bb_width > 0, (1.0 / bb_width) * vol_mult, 0)
        
        idx_arr = np.arange(len(hist_df))
        r_val = hist_df['EMA10'].rolling(10).corr(pd.Series(idx_arr, index=hist_df.index)).fillna(0)
        r_squared = r_val * r_val
        ema10_3 = hist_df['EMA10'].shift(3).fillna(hist_df['EMA10'])
        ema_roc = np.where(ema10_3 != 0, ((hist_df['EMA10'] - ema10_3) / ema10_3) * 5000, 0)
        tml = np.where(is_aligned_30, ema_roc * (r_squared ** 2), 0)
        hist_df['TML'] = tml
        hist_df['ALL_UP'] = (tml > 0) & is_aligned_30 & (hist_df['EMA112'] > hist_df['EMA224'])

        # 이격도 산출
        prev_c = np.roll(c, 1); prev_c[0] = c[0]
        tr_arr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
        hist_df['ATR20'] = pd.Series(tr_arr).ewm(alpha=1/20, adjust=False, min_periods=0).mean()
        spread_10_20 = np.where(hist_df['EMA10'] > hist_df['EMA20'], ((hist_df['EMA10'] - hist_df['EMA20']) / hist_df['ATR20']) * 100, 0)
        spread_112_224 = np.where(hist_df['EMA112'] > hist_df['EMA224'], ((hist_df['EMA112'] - hist_df['EMA224']) / hist_df['ATR20']) * 100, 0)

        # ---------------------------------------------------------
        # 👑 [슈퍼노바 정리본 100% 반영: 한국/미국 분리 3단계 기만술 가동]
        # ---------------------------------------------------------
        # 👇👇 [수술 1: 대표님 고유 수치 기반 진짜 폭발 초입(T-1) 탐지] 👇👇
        breakout_signals = []
        for i in range(20, len(hist_df) - 5): # 최근 5일 제외 (노이즈 방지)
            # 조건: 거래대금 2배 터짐 + TML 10 이상 급등 + 양봉(CPV 0.5초과)
            if (trd_val_eok[i] > np.mean(trd_val_eok[max(0, i-20):i]) * 2.0) and \
               (tml[i] >= 10.0) and (cpv[i] > 0.5):
                future_ret = (max(h[i:i+5]) - c[i]) / c[i] * 100
                if future_ret >= 10.0: # 가짜 돌파 거르고 진짜 대박 파동만 선별
                    breakout_signals.append((i, future_ret))
        
        if breakout_signals:
            # 수익률이 가장 컸던 날을 찾아, 정확히 그 '하루 전(T-1)'을 타겟으로 고정!
            dday_idx = sorted(breakout_signals, key=lambda x: x[1], reverse=True)[0][0] - 1
        else:
            # 폭발이 없으면 안전하게 에너지(BBE) 최대 응축일로 대체
            dday_idx = np.nanargmax(bbe) if not np.isnan(bbe).all() else len(hist_df) - 1
            
        t7_idx = max(0, dday_idx - 5)
        t30_idx = max(0, dday_idx - 20)
        t120_idx = max(0, dday_idx - 120)
        # 👆👆 [수술 1 끝] 👆👆
        # 👇👇 [추가] 6개월 전 장기 매집/횡보 판독 로직 👇👇
        # 6개월 전 시점의 변동성(ATR) 대비 가격이 밴드 내에 수렴하고 있었는지 확인
        long_term_base = (hist_df['ATR20'].iloc[t120_idx] / hist_df['Close'].iloc[t120_idx] * 100) < 5.0
        if "Rank A" in rank_name and not long_term_base:
            return None # A랭크(6개월 장기 매집형)인데 6개월 전 횡보 매집 구간이 없으면 가차 없이 기각
        
        c_20 = c[max(0, dday_idx-20)] if dday_idx >= 20 else c[0]
        stock_ret = ((c[dday_idx] - c_20) / c_20) * 100 if c_20 > 0 else 0

        if market == 'KR':
            idx_c = benchmarks['KR'][benchmarks['KR'].index <= target_date].tail(150)['Close'].values
            idx_20 = idx_c[max(0, dday_idx-20)] if dday_idx >= 20 else idx_c[0]
            idx_ret = ((idx_c[dday_idx] - idx_20) / idx_20) * 100 if idx_20 > 0 else 0.0001
            rs = np.full(len(c), (stock_ret / (idx_ret if idx_ret != 0 else 0.0001)) * 100)

            # 💡 [100년 영속 진화 로직 적용: Wide-Net Rank Gate KR]
            # 한국장 하드코딩 소수점 필터를 제거하고 Rank A~D 구조만 유지한 느슨한 논리 허들로 완화
            if "Rank A" in rank_name:
                c_30 = (cpv[t30_idx] > 0.0) and (bbe[t30_idx] > 3.0)
                c_7 = (cpv[t7_idx] > 0.0) and (tb[t7_idx] > 2.0)
                c_0 = (cpv[dday_idx] > 0.0) and (tb[dday_idx] > 2.0) and (bbe[dday_idx] > 3.0)
            elif "Rank B" in rank_name:
                c_30 = (cpv[t30_idx] > 0.0) and (tb[t30_idx] > 2.0)
                c_7 = (cpv[t7_idx] > 0.0) and (bbe[t7_idx] > 3.0)
                c_0 = (cpv[dday_idx] > 0.0) and (bbe[dday_idx] > 3.0)
            elif "Rank C" in rank_name:
                c_30 = (tb[t30_idx] > 2.0) and (bbe[t30_idx] > 3.0)
                c_7 = (cpv[t7_idx] > 0.0)
                c_0 = (cpv[dday_idx] > 0.0) and (tb[dday_idx] > 2.0)
            elif "Rank D" in rank_name:
                c_30 = (bbe[t30_idx] > 3.0)
                c_7 = (tb[t7_idx] > 2.0)
                c_0 = (cpv[dday_idx] > 0.0) and (bbe[dday_idx] > 3.0)
            else:
                c_30 = (cpv[t30_idx] > 0.0) and (bbe[t30_idx] > 3.0)
                c_7 = (cpv[t7_idx] > 0.0) and (tb[t7_idx] > 2.0)
                c_0 = (cpv[dday_idx] > 0.0) and (tb[dday_idx] > 2.0) and (bbe[dday_idx] > 3.0)
            if not (c_30 and c_7 and c_0): return None
                
            final_rs = rs[dday_idx]

        elif market == 'US':
            spy_c = benchmarks['SPY'][benchmarks['SPY'].index <= target_date].tail(150)['Close'].values
            qqq_c = benchmarks['QQQ'][benchmarks['QQQ'].index <= target_date].tail(150)['Close'].values
            
            spy_20 = spy_c[max(0, dday_idx-20)] if dday_idx >= 20 else spy_c[0]
            qqq_20 = qqq_c[max(0, dday_idx-20)] if dday_idx >= 20 else qqq_c[0]
            spy_ret = ((spy_c[dday_idx] - spy_20) / spy_20) * 100 if spy_20 > 0 else 0.0001
            qqq_ret = ((qqq_c[dday_idx] - qqq_20) / qqq_20) * 100 if qqq_20 > 0 else 0.0001
            
            rs_spy = np.full(len(c), (stock_ret / (spy_ret if spy_ret != 0 else 0.0001)) * 100)
            rs_qqq = np.full(len(c), (stock_ret / (qqq_ret if qqq_ret != 0 else 0.0001)) * 100)

            # 💡 [100년 영속 진화 로직 적용: Wide-Net Rank Gate US]
            # 미국장도 단일 q1/q2/q3 하드코딩을 제거하고 Rank A~D 분기 기반 최소 허들로 완화
            if "Rank A" in rank_name:
                c_30 = (cpv[t30_idx] > 0.0) and (bbe[t30_idx] > 3.0)
                c_7 = (cpv[t7_idx] > 0.0) and (tb[t7_idx] > 2.0)
                c_0 = (cpv[dday_idx] > 0.0) and (tb[dday_idx] > 2.0) and (bbe[dday_idx] > 3.0)
            elif "Rank B" in rank_name:
                c_30 = (cpv[t30_idx] > 0.0) and (tb[t30_idx] > 2.0)
                c_7 = (cpv[t7_idx] > 0.0) and (bbe[t7_idx] > 3.0)
                c_0 = (cpv[dday_idx] > 0.0) and (bbe[dday_idx] > 3.0)
            elif "Rank C" in rank_name:
                c_30 = (tb[t30_idx] > 2.0) and (bbe[t30_idx] > 3.0)
                c_7 = (cpv[t7_idx] > 0.0)
                c_0 = (cpv[dday_idx] > 0.0) and (tb[dday_idx] > 2.0)
            elif "Rank D" in rank_name:
                c_30 = (bbe[t30_idx] > 3.0)
                c_7 = (tb[t7_idx] > 2.0)
                c_0 = (cpv[dday_idx] > 0.0) and (bbe[dday_idx] > 3.0)
            else:
                c_30 = (cpv[t30_idx] > 0.0) and (bbe[t30_idx] > 3.0)
                c_7 = (cpv[t7_idx] > 0.0) and (tb[t7_idx] > 2.0)
                c_0 = (cpv[dday_idx] > 0.0) and (tb[dday_idx] > 2.0) and (bbe[dday_idx] > 3.0)
            if not (c_30 and c_7 and c_0): return None
            final_rs = rs_spy[dday_idx]

        # 형상 압축 및 반환
        c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
        new_shape = np.mean(np.array_split(c_norm, 20), axis=1).tolist()
        
        return {
            'rank_name': rank_name,
            'cpv': cpv[-1], 'tb': tb[-1], 'bbe': bbe[-1], 'rs': final_rs, 
            'vcp': 1.0, 'vol': 1.0, 'ma': 0.0, 'shape': new_shape,
            'tml': tml[dday_idx], 'trd_val': trd_val_eok[-1]
        }
    except: return None

# ==========================================
# 🚀 메인 역추적 로직 (Rank A~D 및 미국/한국 분리 마이닝)
# ==========================================
def hunt_supernovas(market):
    print(f"\n🚀 [{market}] 전체 시장 3단계 기만술 타임머신 역추적 가동...")
    send_telegram_msg(f"⏳ <b>[{market} 초신성 타임머신 가동]</b>\n전체 상장 종목을 대상으로 과거 노이즈를 제거하고 '3단계 기만술'을 통과한 찐 대박주만 스캔합니다. (약 10~20분 소요)")
    
    now = datetime.now()
    start_date = (now - timedelta(days=200)).strftime('%Y-%m-%d')
    
    try:
        if market == 'US':
            spy_df = yf.download('SPY', start=start_date, progress=False)
            spy_df = flatten_yf_download_df(spy_df)
            qqq_df = yf.download('QQQ', start=start_date, progress=False)
            qqq_df = flatten_yf_download_df(qqq_df)
            spy_df.index = pd.to_datetime(spy_df.index).tz_localize(None)
            qqq_df.index = pd.to_datetime(qqq_df.index).tz_localize(None)
            benchmarks = {'SPY': spy_df, 'QQQ': qqq_df}
        else:
            idx_df = fdr.DataReader('069500', start_date)
            idx_df.index = pd.to_datetime(idx_df.index).tz_localize(None)
            benchmarks = {'KR': idx_df}
    except: return

    stock_list = get_krx_list() if market == 'KR' else get_us_list()
    tickers = stock_list['Code'].tolist()
    name_map = dict(zip(stock_list['Code'], stock_list['Name']))
    
    results = []
    scanned_count = 0
    
    def process_ticker(code):
        try:
            if market == 'KR':
                df = fdr.DataReader(code, start_date)
            else:
                df = yf.download(code, start=start_date, progress=False)
                df = flatten_yf_download_df(df)
            if df.empty or len(df) < 130: return None
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            c = df['Close'].values
            if c[-1] < (1000 if market == 'KR' else 3.0): return None 
            
            ret_1w = (c[-1] - c[-8]) / c[-8] * 100 if len(c) >= 8 else 0
            ret_1m = (c[-1] - c[-20]) / c[-20] * 100 if len(c) >= 20 else 0
            ret_3m = (c[-1] - c[-60]) / c[-60] * 100 if len(c) >= 60 else 0
            ret_6m = (c[-1] - c[-120]) / c[-120] * 100 if len(c) >= 120 else 0
            
            return {'code': code, 'df': df, 'ret_1w': ret_1w, 'ret_1m': ret_1m, 'ret_3m': ret_3m, 'ret_6m': ret_6m}
        except: return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        for res in executor.map(process_ticker, tickers):
            scanned_count += 1
            if scanned_count % 500 == 0:
                print(f"   ↳ 진행 중... {scanned_count}/{len(tickers)}개 스캔 완료")
            if res: results.append(res)
            
    if not results: return
    
    res_df = pd.DataFrame(results)
    
    top_6m = res_df.sort_values('ret_6m', ascending=False).head(10)
    rem_3m = res_df[~res_df['code'].isin(top_6m['code'])]
    top_3m = rem_3m.sort_values('ret_3m', ascending=False).head(10)
    rem_1m = rem_3m[~rem_3m['code'].isin(top_3m['code'])]
    top_1m = rem_1m.sort_values('ret_1m', ascending=False).head(10)
    rem_1w = rem_1m[~rem_1m['code'].isin(top_1m['code'])]
    top_1w = rem_1w.sort_values('ret_1w', ascending=False).head(10)
    
    supernovas = []
    for _, r in top_6m.iterrows(): supernovas.append((r['code'], r['df'], now, "🥇 Rank A: '6개월' 장기 매집형 (진성 대장주)"))
    for _, r in top_3m.iterrows(): supernovas.append((r['code'], r['df'], now, "🥈 Rank B: '3개월' 중기 매집형 (강력한 스윙 추세주)"))
    for _, r in top_1m.iterrows(): supernovas.append((r['code'], r['df'], now, "🥉 Rank C: '1개월' 단기 매집형 (트렌드 테마주)"))
    for _, r in top_1w.iterrows(): supernovas.append((r['code'], r['df'], now, "🏅 Rank D: '10일' 초단기/밈(Meme) 작전주"))

    dna_list = []
    rank_counts = {"A":0, "B":0, "C":0, "D":0}
    
    for code, df, target_date, rank_name in supernovas:
        dna = extract_dna_from_df(df, benchmarks, target_date.strftime('%Y-%m-%d'), rank_name, market)
        if dna: 
            dna_list.append(dna)
            if "Rank A" in rank_name: rank_counts["A"] += 1
            elif "Rank B" in rank_name: rank_counts["B"] += 1
            elif "Rank C" in rank_name: rank_counts["C"] += 1
            elif "Rank D" in rank_name: rank_counts["D"] += 1
        
    if not dna_list: return

    # 👇👇 [수술 지점: V102.0 머신러닝을 위한 순도 100% 팩트 CSV 추출 파이프라인] 👇👇
    try:
        csv_path = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Supernova_Flow_Tracking_Master.csv')
        csv_data = []
        
        # data_miner.py가 정확히 읽을 수 있도록 한글 컬럼명 1:1 완벽 매핑
        for d in dna_list:
            csv_data.append({
                '종목코드': d.get('code', '000000'),
                '시장': market,
                '랭크': d['rank_name'],
                '[D_Day_당일] 평균_CPV': round(d['cpv'], 4),
                '[D_Day_당일] 평균_진짜양봉(TB)': round(d['tb'], 4),
                '[D_Day_당일] 평균_응축에너지(BBE)': round(d['bbe'], 4),
                '[D_Day_당일] 진모멘텀(TML)': round(d['tml'], 4),
                '[D_Day_당일] 평균_시장강도(RS)': round(d['rs'], 4)
            })

        df_csv = pd.DataFrame(csv_data)
        
        # 한국장(KR)이 먼저 돌고 미국장(US)이 돌기 때문에, 파일이 있으면 아래에 이어붙임(Append)
        if os.path.exists(csv_path):
            df_csv.to_csv(csv_path, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            df_csv.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
        print(f"💾 [{market}] {len(csv_data)}개의 D-Day 표본 데이터가 K-Means 마이닝용 CSV에 성공적으로 적재되었습니다.")
    except Exception as e:
        print(f"⚠️ CSV 추출 파이프라인 에러: {e}")
    # 👆👆 [수술 지점 완료] 👆👆
    
    # 👇👇 [기존 단일 평균(Centroid) 멍청한 로직 완전 삭제 후 교체] 👇👇
    # 👑 1. 랭크별로 DNA 1차 분류
    rank_dnas = {"A": [], "B": [], "C": [], "D": []}
    for d in dna_list:
        if "Rank A" in d['rank_name']: rank_dnas["A"].append(d)
        elif "Rank B" in d['rank_name']: rank_dnas["B"].append(d)
        elif "Rank C" in d['rank_name']: rank_dnas["C"].append(d)
        elif "Rank D" in d['rank_name']: rank_dnas["D"].append(d)

    market_templates = {}
    
    # 👑 2. 랭크 내에서 다시 '조용한 매집(Stealth)'과 '변동성 폭발(Volatile)' 2차 정밀 분리
    for rank, dnas in rank_dnas.items():
        if not dnas: continue
        
        # BBE(응축 에너지)의 중간값을 기준으로 조용함과 변동성을 가름 (대표님 지적 완벽 반영)
        median_bbe = np.median([d['bbe'] for d in dnas])
        
        stealth_dnas = [d for d in dnas if d['bbe'] <= median_bbe]
        volatile_dnas = [d for d in dnas if d['bbe'] > median_bbe]
        
        def make_template(sub_dnas):
            if not sub_dnas: return None
            return {
                'cpv': np.mean([d['cpv'] for d in sub_dnas]),
                'tb': np.mean([d['tb'] for d in sub_dnas]),
                'bbe': np.mean([d['bbe'] for d in sub_dnas]),
                'rs': np.mean([d['rs'] for d in sub_dnas]),
                'shape': np.mean([d['shape'] for d in sub_dnas], axis=0).tolist()
            }
            
        if stealth_dnas:
            market_templates[f"RANK_{rank}_STEALTH"] = make_template(stealth_dnas)
        if volatile_dnas:
            market_templates[f"RANK_{rank}_VOLATILE"] = make_template(volatile_dnas)

    # 👇👇 [수술 1] 템플릿 버전 관리 및 누적 저장 (세포 분열) 👇👇
    config = load_config()
    multi_key = f"DNA_SUPERNOVA_{market}_MULTI"
    treasury_key = f"CENTRAL_TREASURY_{market}" # 💡 국고 키 세팅
    
    # 1. 기존 템플릿 뭉치와 현재 국고 잔액 로드
    existing_templates = config.get(multi_key, {})
    current_treasury = config.get(treasury_key, 0)
    
    # 2. 오늘 날짜를 버전 번호로 생성 (예: V_240528)
    version_tag = datetime.now().strftime('V_%y%m%d')
    
    # 3. 💡 [신규 추가] 신입 채용 자금 지원 및 파산 방어 로직
    new_added = 0
    rejected_due_to_funds = 0

    # // [수정 후] ARCHIVED_TEMPLATES(cp v/tb/bbe) 코사인 교차검증 — 좀비 템플릿 무한 부활 차단
    archived_root = config.get("ARCHIVED_TEMPLATES") or {}
    if not isinstance(archived_root, dict):
        archived_root = {}
    market_archived = archived_root.get(market, {})
    if not isinstance(market_archived, dict):
        market_archived = {}
    archived_vectors = []
    for _aname, entry in market_archived.items():
        if not isinstance(entry, dict):
            continue
        td = entry.get("template_data")
        if not isinstance(td, dict):
            continue
        try:
            archived_vectors.append(
                [float(td.get("cpv", 0.0)), float(td.get("tb", 0.0)), float(td.get("bbe", 0.0))]
            )
        except (TypeError, ValueError):
            continue

    def _cosine_sim_archived_dna(a, b):
        va = np.asarray(a, dtype=np.float64)
        vb = np.asarray(b, dtype=np.float64)
        na = np.linalg.norm(va)
        nb = np.linalg.norm(vb)
        if na < 1e-15 or nb < 1e-15:
            return 0.0
        return float(np.dot(va, vb) / (na * nb))

    for t_name, t_dna in market_templates.items():
        # // [수정 전] 아카이브(도태 DNA)와의 유사도 검사 없이 바로 국고 차감·편입
        # // [수정 후] 아카이브와 코사인 ≥95%면 편입하지 않음
        new_vec = [float(t_dna.get("cpv", 0.0)), float(t_dna.get("tb", 0.0)), float(t_dna.get("bbe", 0.0))]
        skip_zombie = False
        for av in archived_vectors:
            if _cosine_sim_archived_dna(new_vec, av) >= 0.95:
                print(f"🚫 [안티 패턴 일치로 채용 거절] {market} {t_name} (ARCHIVED_TEMPLATES DNA 대비 cos≥0.95)")
                skip_zombie = True
                break
        if skip_zombie:
            continue

        # 국고에 2,000만 원 이상 남아있는지 팩트 체크
        if current_treasury >= 20000000:
            current_treasury -= 20000000 # 국고에서 2,000만 원 즉시 차감
            versioned_name = f"{t_name}_{version_tag}"
            existing_templates[versioned_name] = t_dna
            new_added += 1
        else:
            rejected_due_to_funds += 1 # 자금 부족으로 채용 거절 (동결)
            
    # 💡 변경된 국고 잔액을 config(관제탑)에 즉각 반영
    config[treasury_key] = current_treasury

    # 4. 최대 보유 한도 방어 (서버 터짐 방지를 위해 최대 50개 유지)
    if len(existing_templates) > 50:
        sorted_keys = sorted(existing_templates.keys())
        excess = len(existing_templates) - 50
        for k in sorted_keys[:excess]:
            del existing_templates[k]
            
    config[multi_key] = existing_templates
    save_config(config)
    
    # 텔레그램 리포트 내용 수정 (자금 순환 및 파산 경고 포함)
    report_msg = f"🚀 <b>[{market} 템플릿 세포 분열 완료]</b>\n"
    if new_added > 0:
        report_msg += f"💡 신규 변이 유전자 {new_added}개가 각각 2,000만 원의 초기 시드를 배정받아 관제탑에 투입되었습니다.\n"
    report_msg += f"🏦 잔여 국고: {current_treasury:,.0f}원\n"
    report_msg += f"🧪 <b>[현재 관제탑 보유 템플릿 수: 총 {len(existing_templates)}개]</b>\n"
    
    # 파산 방어망 작동 시 텔레그램 긴급 알림
    if rejected_due_to_funds > 0:
        report_msg += f"\n🚨 <b>[시스템 경고]</b> 국고 자금 부족으로 {rejected_due_to_funds}개의 신규 로직 채용이 동결되었습니다.\n"
        
    send_telegram_msg(report_msg)
    print(f"✅ [{market}] 다차원 DNA 템플릿 누적 갱신 및 국고 반영 완료!")

# ==========================================
# 🚀 [V101.0 신규 엔진] 초신성 실시간 멀티스레드 스나이퍼
# ==========================================
def execute_supernova_live_scan(market):
    print(f"\n🦅 [{market}] 초신성 멀티스레드 스나이퍼 가동 (15배속 비동기 병렬 스캔)...")
    
    # 1. 템플릿 및 기준값 로드
    ideal_templates = {}
    config = load_config()
    multi_key = f"DNA_SUPERNOVA_{market}_MULTI"
    surviving_templates = config.get(multi_key, {})
    
    for t_name, t_dna in surviving_templates.items():
        ideal_templates[t_name] = np.array([t_dna['cpv'], t_dna['tb'], t_dna['bbe']])
    
    if market == 'KR':
        ideal_templates['RANK_A_장기매집'] = np.array([0.75, 11.8, 27.15])
        ideal_templates['RANK_B_중기스윙'] = np.array([0.75, 10.0, 27.35])
        ideal_templates['RANK_C_단기테마'] = np.array([0.60, 8.0, 19.70])
        ideal_templates['RANK_D_초단기밈'] = np.array([0.60, 8.0, 24.45])
    elif market == 'US':
        # 💡 [100년 영속 진화 로직 적용: US Rank Template Symmetry]
        ideal_templates['US_RANK_A_장기매집'] = np.array([0.70, 10.5, 25.0])
        ideal_templates['US_RANK_B_중기스윙'] = np.array([0.66, 9.2, 21.5])
        ideal_templates['US_RANK_C_단기테마'] = np.array([0.60, 8.1, 17.0])
        ideal_templates['US_RANK_D_초단기밈'] = np.array([0.55, 7.5, 13.5])
        ideal_templates['US_MEME_슈팅'] = np.array([0.55, 8.8, 12.80])

    mfe_weighted = config.get("DNA_SUPERNOVA_MFE_WEIGHTED")
    if mfe_weighted:
        ideal_templates['MFE_진화형_황금타점'] = np.array([mfe_weighted['cpv'], mfe_weighted['tb'], mfe_weighted.get('bbe', 20.0)])

    # 관제탑 컷오프(정규직/인큐베이터 기본 밸브에 사용)
    dynamic_cos_cutoff = config.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.50)
    dynamic_ml_cutoff = config.get("DYNAMIC_ML_BOX_CUTOFF", 0.50)

    # DNA_ALPHA_ / NEW_EVOLUTION_ 정규직 승격 템플릿 — 이상형 3D + 스나이퍼 cos_cutoff 밸브
    ideal_template_cutoffs = {}
    for _k, _v in config.items():
        if not (_k.startswith("DNA_ALPHA_") or _k.startswith("NEW_EVOLUTION_")):
            continue
        if not isinstance(_v, dict):
            continue
        try:
            ideal_templates[_k] = np.nan_to_num(np.array([
                float(_v.get("cpv", 0.0)),
                float(_v.get("tb", 0.0)),
                float(_v.get("bbe", 0.0)),
            ], dtype=float))
            ideal_template_cutoffs[_k] = float(_v.get("cos_cutoff", dynamic_cos_cutoff))
        except (TypeError, ValueError):
            continue

    # 인큐베이터 돌연변이 템플릿을 스나이퍼 이상형 풀에 편입 (3D cpv/tb/bbe, 코사인은 각 cos_cutoff 적용)
    incubator_cfg = config.get("INCUBATOR_TEMPLATES", {})
    if isinstance(incubator_cfg, dict):
        for iname, itpl in incubator_cfg.items():
            if not isinstance(itpl, dict):
                continue
            try:
                ideal_templates[f"INCUBATOR_{iname}"] = np.nan_to_num(np.array([
                    float(itpl.get("cpv", 0.0)),
                    float(itpl.get("tb", 0.0)),
                    float(itpl.get("bbe", 0.0)),
                ], dtype=float))
                ideal_template_cutoffs[f"INCUBATOR_{iname}"] = float(itpl.get("cos_cutoff", 0.80))
            except (TypeError, ValueError):
                continue

    live_clusters = config.get('LIVE_CLUSTER_TEMPLATES', {})

    # 대상 종목 및 현재 보유 현황 로드
    stock_list = get_krx_list() if market == 'KR' else get_us_list()
    tickers = stock_list['Code'].tolist()
    
    def get_similarity(vec1, vec2):
        n1, n2 = np.linalg.norm(vec1), np.linalg.norm(vec2)
        return np.dot(vec1, vec2) / (n1 * n2) if n1 > 0 and n2 > 0 else 0

    try:
        conn = sqlite3.connect(aft.DB_PATH, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT code FROM forward_trades WHERE market=? AND status='OPEN'", (market,))
        open_positions = {row[0] for row in cursor.fetchall()}
        conn.close()
    except: open_positions = set()

    # 💡 [핵심 1] 단일 스레드 병목 탈출을 위한 "개별 종목 연산 작업(Worker)" 분리
    def process_live_ticker(code):
        if code in open_positions or code in scanned_today_cache[market]:
            return None
            
        try:
            # 병목의 원인인 API 호출을 각 스레드가 동시에 분산해서 처리
            if market == 'KR':
                df = fdr.DataReader(code, (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d'))
            else:
                df = yf.download(code, period="2mo", progress=False)
                df = flatten_yf_download_df(df)
            if df.empty or len(df) < 20: return None

            # 관제탑 EVOLVED_ALPHA_FACTORS → 실시간 알파 (ML N차원 바운딩 박스용)
            current_alphas = {}
            _evolved = config.get("EVOLVED_ALPHA_FACTORS")
            if isinstance(_evolved, dict):
                for _slot_key, _formula in _evolved.items():
                    if not isinstance(_formula, str) or not str(_formula).strip():
                        continue
                    _ser = evaluate_alpha_formula(df, _formula.strip())
                    if _ser is not None and len(_ser) > 0:
                        current_alphas[_slot_key] = _ser
            
            c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
            current_close = c[-1]
            
            if market == 'KR' and current_close < 1000: return None 
            if market == 'US' and current_close < 1.0: return None  
            if np.mean(v[-5:]) < 50000: return None            
            
            # DNA 벡터 3차원 추출
            v_ma20 = pd.Series(v).rolling(20).mean().values
            cpv = np.where(h != l, (c - o) / (h - l), 0.5)[-1]
            
            # 👇👇 [V102.2 버그 픽스] 장중 시간대별 거래량 동적 외삽법(Extrapolation) 엔진 👇👇
            tz_market = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
            now_mkt = datetime.now(tz_market)
            
            # 개장 시간 세팅 (한국장 09:00, 미국장 09:30)
            open_h = 9
            open_m = 0 if market == 'KR' else 30
            
            # 장 시작 후 몇 분이나 지났는지 계산
            elapsed_mins = (now_mkt.hour - open_h) * 60 + (now_mkt.minute - open_m)
            
            # 정규장 총 시간은 390분 (6.5시간). 에러 방지를 위해 1~390분 사이로 강력한 캡(Cap) 씌우기
            elapsed_mins = max(1, min(390, elapsed_mins))
            
            # 💡 핵심: 현재 거래량을 남은 시간 비율만큼 뻥튀기하여 '오늘 마감 예상 거래량' 산출
            # (예: 오전 9시 30분이라면 30분 경과 -> 390/30 = 13배 보정)
            est_daily_volume = v[-1] * (390.0 / elapsed_mins)
            
            # 기존의 날것(v[-1]) 대신 보정된 예상 거래량(est_daily_volume)을 20일 평균과 대조!
            vol_mult = (est_daily_volume / v_ma20[-1]) if v_ma20[-1] > 0 else 1.0
            # 👆👆 [외삽법 패치 완료] 👆👆
            
            tb = vol_mult / max(cpv, 0.01) if cpv > 0 else vol_mult / 0.01
            bb_std = pd.Series(c).rolling(20).std().values[-1]
            bb_mid = pd.Series(c).rolling(20).mean().values[-1]
            bb_width = (4 * bb_std) / bb_mid if bb_mid > 0 else 0.01
            bbe = (1.0 / bb_width) * vol_mult if bb_width > 0 else 0
            
            # 1. 코사인 유사도 연산 (템플릿별 컷: 인큐베이터는 cos_cutoff, 그 외는 DYNAMIC_SUPERNOVA_CUTOFF)
            best_sim = 0.0
            best_pattern_name = "UNKNOWN"
            best_pass_sim = 0.0
            best_pass_name = "UNKNOWN"
            is_pass_cosine = False
            current_vec_3d = np.nan_to_num(np.array([cpv, tb, bbe]))
            
            for t_name, base_vec in ideal_templates.items():
                sim = get_similarity(current_vec_3d, base_vec)
                if sim > best_sim:
                    best_sim = sim
                    best_pattern_name = t_name
                t_cut = ideal_template_cutoffs.get(t_name, dynamic_cos_cutoff)
                if sim >= t_cut:
                    is_pass_cosine = True
                    if sim > best_pass_sim:
                        best_pass_sim = sim
                        best_pass_name = t_name
            
            # 2. ML 클러스터 바운딩 박스 연산
            is_pass_ml_box = False
            ml_match_count = 0 
            ml_pattern_name = "UNKNOWN"
            
            for c_name, bounds in live_clusters.items():
                if not isinstance(bounds, dict):
                    continue
                ml_match_count = 0
                total_dims = 3
                if bounds.get('cpv_min', -99) <= cpv <= bounds.get('cpv_max', 99): ml_match_count += 1
                if bounds.get('tb_min', -99) <= tb <= bounds.get('tb_max', 999): ml_match_count += 1
                if bounds.get('bbe_min', -99) <= bbe <= bounds.get('bbe_max', 999): ml_match_count += 1
                for slot_key in current_alphas:
                    akmin = f'alpha_{slot_key}_min'
                    akmax = f'alpha_{slot_key}_max'
                    if akmin not in bounds or akmax not in bounds:
                        continue
                    try:
                        lo = float(bounds[akmin])
                        hi = float(bounds[akmax])
                    except (TypeError, ValueError):
                        continue
                    _ser = current_alphas[slot_key]
                    try:
                        aval = float(_ser.iloc[-1])
                    except Exception:
                        try:
                            aval = float(_ser.values[-1])
                        except Exception:
                            continue
                    if np.isnan(aval):
                        continue
                    total_dims += 1
                    if lo <= aval <= hi:
                        ml_match_count += 1
                ml_score = ml_match_count / float(total_dims) if total_dims > 0 else 0.0
                
                if ml_score >= dynamic_ml_cutoff:
                    is_pass_ml_box = True
                    ml_pattern_name = c_name
                    break

            # 합격한 종목만 선별하여 데이터 반환 (DB 저장은 여기서 하지 않음 - 락 방어)
            if is_pass_ml_box or is_pass_cosine:
                if is_pass_ml_box:
                    final_sig = f"[SUPERNOVA_MLBOX] 🤖{ml_pattern_name}"
                    final_score = ml_score * 100
                    msg_type = f"🤖 ML 클러스터 통과 (기준:{dynamic_ml_cutoff*100:.0f}%)"
                else:
                    _cos_label = best_pass_name if best_pass_name != "UNKNOWN" else best_pattern_name
                    _cos_sim = best_pass_sim if best_pass_name != "UNKNOWN" else best_sim
                    final_sig = f"[SUPERNOVA_COSINE] {_cos_label}"
                    final_score = _cos_sim * 100
                    _cut_used = ideal_template_cutoffs.get(_cos_label, dynamic_cos_cutoff)
                    msg_type = f"🦅 코사인 컷오프 통과 (기준:{float(_cut_used)*100:.0f}%)"
                
                fdict = {'dyn_cpv': cpv, 'dyn_tb': tb, 'v_energy': bbe}
                if (not is_pass_ml_box) and str(best_pass_name).startswith("INCUBATOR_"):
                    fdict["incubator_sniper_key"] = str(best_pass_name)[len("INCUBATOR_"):]
                return {
                    'code': code,
                    'name': stock_list[stock_list['Code']==code]['Name'].values[0],
                    'final_sig': final_sig,
                    'final_score': final_score,
                    'current_close': current_close,
                    'facts': fdict,
                    'msg_type': msg_type
                }
            return None
        except: return None

    # 💡 [핵심 2] ThreadPoolExecutor를 이용한 15배속 동시 타격 (병목 돌파)
    valid_targets = []
    import concurrent.futures
    
    # 15개의 작업자(Thread)가 2500개 종목을 동시에 나눠서 다운로드하고 분석합니다.
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        for result in executor.map(process_live_ticker, tickers):
            if result:
                valid_targets.append(result)

    # 💡 [핵심 3] 발굴된 종목 장부 기록 (DB 락 방지를 위해 메인 스레드에서 순차적 기록)
    for target in valid_targets:
        is_success, msg = aft.try_add_virtual_position(
            market=market, 
            code=target['code'], 
            name=target['name'],
            sig_type=target['final_sig'], 
            score=target['final_score'], 
            ep=target['current_close'],
            facts=target['facts'],
            trade_source="SUPERNOVA" 
        )
        
        if is_success:
            scanned_today_cache[market].add(target['code'])
            send_telegram_msg(f"<b>{target['msg_type']}</b>\n{target['code']} / {target['final_sig']}\n일치율: {target['final_score']:.1f}%\n가상매매 장부에 정밀 분리되어 편입되었습니다.")
            
    print(f"✅ [{market}] 멀티스레드 스나이퍼 쾌속 스캔 및 DB 기록 완료!")
# 👇👇 [기존 run_miner_scheduler 함수를 이걸로 덮어쓰세요] 👇👇
def run_miner_scheduler():
    """1주일에 한 번 과거 데이터를 마이닝하여 템플릿 갱신 및 CSV 추출을 수행하는 봇"""
    tz_kr = pytz.timezone('Asia/Seoul')
    
    while True:
        try:
            now = datetime.now(tz_kr)
            # 매주 월요일 17:00 템플릿 갱신
            if now.weekday() == 0 and now.hour == 17 and now.minute == 0:
                
                # // [수정 전] 매주 CSV os.remove → 실전 피드백 없이 과거 표본만 반복 학습되는 루프 유발
                # csv_path = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Supernova_Flow_Tracking_Master.csv')
                # if os.path.exists(csv_path):
                #     os.remove(csv_path)
                # standard_csv_path = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'Standard_Flow_Master.csv')
                # if os.path.exists(standard_csv_path):
                #     os.remove(standard_csv_path)
                #     print("🗑️ [데이터 클리닝] 지난주 머신러닝용 CSV 데이터를 성공적으로 포맷했습니다.")
                # // [수정 후] CSV 보존 + data_miner에서 forward_trades DNA 병합으로 실전 수익 피드백 반영

                hunt_supernovas('KR')
                hunt_supernovas('US')
                evolve_alpha_factors()
                
                # 💡 [V100.1 핵심 픽스] data_miner 파일 방어막 구축
                try:
                    import data_miner
                    print("🔄 [스케줄러] 타임머신 완료. K-Means 클러스터 마이닝으로 자동 이관합니다...")
                    data_miner.run_cluster_mining()
                except ModuleNotFoundError:
                    print("🚨 [경고] 'data_miner.py' 파일을 찾을 수 없어 ML 마이닝을 건너뜁니다.")
                except Exception as e:
                    print(f"🚨 [에러] 마이닝 실행 중 오류 발생: {e}")
                
                time.sleep(65) 
            time.sleep(30)
        except Exception as e:
            print(f"⚠️ 마이너 스케줄러 루프 에러: {e}")
            time.sleep(60)
# 👆👆 [덮어쓰기 완료] 👆👆

def run_live_sniper_scheduler():
    """매일 4번 지정된 시간에 실시간 시장을 스캔하고 쏘는 봇"""
    tz_kr = pytz.timezone('Asia/Seoul')
    print("🕒 [초신성 실시간 스나이퍼] 대기 중...")
    print(" - 🇰🇷 한국 타격: 09:00, 09:30, 15:00, 16:00 (KST)")
    print(" - 🇺🇸 미국 타격: 23:30, 00:00, 05:00, 06:00 (KST)")
    
    global scanned_today_cache
    last_cleared_day = datetime.now(tz_kr).day

    while True:
        try:
            now = datetime.now(tz_kr)
            time_str = f"{now.hour:02d}:{now.minute:02d}"
            
            # 날짜가 바뀌면 어제 쐈던 기록(캐시) 초기화
            if now.day != last_cleared_day:
                scanned_today_cache = {'KR': set(), 'US': set()}
                last_cleared_day = now.day

            kr_target_times = ["09:00", "09:30", "15:00", "16:00"]
            us_target_times = ["23:30", "00:00", "05:00", "06:00"]
            
            if time_str in kr_target_times:
                execute_supernova_live_scan('KR')
                time.sleep(65) 
                
            elif time_str in us_target_times:
                execute_supernova_live_scan('US')
                time.sleep(65) 

            time.sleep(20) 
            
        except Exception as e:
            print(f"스나이퍼 스케줄러 에러: {e}")
            time.sleep(60)

# ==========================================
# 🕒 [메인 래퍼 함수] main.py 연동 및 동시 가동 (V100.1 무중단 재가동 패치)
# ==========================================
def run_scheduler():
    """main.py에서 호출 시 기존 템플릿을 확인하고, 무중단으로 스나이퍼를 가동시키는 래퍼 함수"""
    import threading
    
    # 💡 [핵심 수술] 관제탑(JSON)을 열어서 기존에 쌓아둔 템플릿이 있는지 먼저 팩트 체크합니다.
    config = load_config()
    kr_templates = config.get("DNA_SUPERNOVA_KR_MULTI", {})
    us_templates = config.get("DNA_SUPERNOVA_US_MULTI", {})
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {})

    # 만약 기존 데이터가 텅 비어있는 '진짜 최초 실행'일 때만 초기화를 돌립니다.
    if not kr_templates or not us_templates or not ml_templates:
        print("🚀 [최초 가동] 기존 템플릿이 없습니다. 즉시 1회 타임머신 스캔을 시작하여 기초 템플릿을 생성합니다...")
        if not kr_templates: hunt_supernovas('KR')
        if not us_templates: hunt_supernovas('US')
        
        # ML 박스 데이터 마이닝도 1회 강제 실행
        try:
            import data_miner
            print("🔄 [최초 가동] K-Means 클러스터 마이닝 1회 자동 실행...")
            data_miner.run_cluster_mining()
        except Exception as e:
            print(f"⚠️ 마이닝 초기화 에러: {e}")
            
    # 💡 이미 쌓아둔 데이터가 있다면? 무거운 스캔을 건너뛰고 1초 만에 감시 모드로 복귀!
    else:
        print(f"✅ [초기화 스킵] 기존에 보존된 템플릿(KR: {len(kr_templates)}개, US: {len(us_templates)}개, ML: {len(ml_templates)}개)을 성공적으로 로드했습니다.")
        print("⚡ 타임머신 스캔을 건너뛰고 즉시 [실시간 스나이퍼 모드]로 복귀합니다.")

    # 1. 템플릿 갱신 마이너는 백그라운드 스레드로 분리하여 '매주 월요일 17시'에만 조용히 실행되게 놔둡니다.
    t_miner = threading.Thread(target=run_miner_scheduler, daemon=True)
    t_miner.start()
    
    # 2. 실시간 진입 스나이퍼는 현재 스레드에서 무한 실행 (main.py의 멀티스레딩과 완벽 호환)
    run_live_sniper_scheduler()

if __name__ == "__main__":
    run_scheduler()
