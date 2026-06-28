"""
Genetic Programming Mutator — 정신과 시간의 방(Hyperbolic Time Chamber) 유전 수식 진화 엔진.

기존 인큐베이터가 `default_mock_strategies()`의 고정 5종만 영원히 반복하던 문제를 해결한다.
파이썬 `ast` 모듈로 시장 지표·연산자를 무작위 결합한 매수 시그널 수식(expr)을 **트리 단위로**
생성·교배(crossover)·돌연변이(mutation)시켜, 매 세대 수천 개의 새로운 돌연변이 군집을 창조한다.

핵심 설계
- 생성물은 항상 `incubator_engine.is_safe_expression` 게이트를 통과하는 **순수 비교/논리식**이다.
  (Call·Attribute·임포트 없음. `&`/`|`는 BitAnd/BitOr BinOp 로, 각 비교절은 괄호로 격리.)
- pd.eval / numexpr 호환: 변수는 소문자 와이드 컬럼명(close, ma20, vol_ratio …).
- Mission 3(동적 변속 기어): 국면(regime)과 챔피언 생존율에 따라 교배·돌연변이·신규 비율을
  자동 재조정한다. 안정장(BULL/SIDEWAYS)=교배 위주(수렴), 격변장(HIGH_VOL/BEAR)·챔피언 전멸=
  돌연변이/신규 위주(탐색).
"""
from __future__ import annotations

import ast
import random
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# 유전자 풀(Terminal / Variable set) — 인큐베이터·OOS 검증기·라이브가 공통으로 제공해야 함.
# ---------------------------------------------------------------------------
PRICE_VARS: Tuple[str, ...] = ("open", "high", "low", "close", "ma5", "ma10", "ma20")
VOL_VARS: Tuple[str, ...] = ("volume", "vol_ma5", "vol_lag1")
SIGNED_SCALAR_VARS: Tuple[str, ...] = ("ret1", "body")  # 0 근처(부호 있음)
RATIO_VARS: Tuple[str, ...] = ("hl_range", "vol_ratio")  # 양수 비율

#: 인큐베이터/검증기/라이브가 local_dict 에 반드시 채워야 하는 표준 변수 집합.
VARIABLES: Tuple[str, ...] = PRICE_VARS + VOL_VARS + SIGNED_SCALAR_VARS + RATIO_VARS

_CMP_OPS = (ast.Lt, ast.Gt, ast.LtE, ast.GtE)
_BOOL_OPS = (ast.BitAnd, ast.BitOr)


# ---------------------------------------------------------------------------
# Mission 3: Exploration–Exploitation 동적 변속 기어
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class EvolutionGear:
    regime: str
    crossover_rate: float
    mutation_rate: float
    random_rate: float
    elite_keep: int

    def as_dict(self) -> dict:
        return {
            "regime": self.regime,
            "crossover_rate": round(self.crossover_rate, 3),
            "mutation_rate": round(self.mutation_rate, 3),
            "random_rate": round(self.random_rate, 3),
            "elite_keep": self.elite_keep,
        }


def regime_gear(regime: str, *, champion_survival_rate: float = 1.0) -> EvolutionGear:
    """
    국면 + 챔피언 생존율 → (교배/돌연변이/신규) 비율.

    - BULL / SIDEWAYS : 안정장 → 교배 80%+ 로 우상향 수익 굳히기(exploitation).
    - HIGH_VOL / BEAR : 격변장 → 돌연변이/신규 대폭 상향(exploration).
    - 챔피언 생존율이 낮을수록(기존 유전자 전멸) 돌연변이/신규를 추가 가산.
    """
    reg = str(regime or "SIDEWAYS").upper()
    if reg in ("BULL", "SIDEWAYS", "CHOP"):
        cx, mut, rnd = 0.80, 0.13, 0.07
    elif reg in ("HIGH_VOL", "BEAR", "BLACK_SWAN", "CRASH"):
        cx, mut, rnd = 0.35, 0.45, 0.20
    else:  # UNKNOWN 등 → 균형
        cx, mut, rnd = 0.55, 0.30, 0.15

    # 챔피언 전멸 방어: 생존율 낮을수록 탐색(돌연변이+신규) 강제 가산.
    surv = max(0.0, min(1.0, float(champion_survival_rate)))
    panic = (1.0 - surv) * 0.40  # 생존율 0 → +0.40 탐색
    shift = min(panic, cx - 0.10) if cx > 0.10 else 0.0
    cx -= shift
    mut += shift * 0.6
    rnd += shift * 0.4

    total = cx + mut + rnd
    cx, mut, rnd = cx / total, mut / total, rnd / total
    elite = 12 if reg in ("BULL", "SIDEWAYS", "CHOP") else 6
    return EvolutionGear(reg, cx, mut, rnd, elite)


# ---------------------------------------------------------------------------
# AST 원자(atom) 생성 — 단일 비교절
# ---------------------------------------------------------------------------
def _name(n: str) -> ast.Name:
    return ast.Name(id=n, ctx=ast.Load())


def _const(v: float) -> ast.Constant:
    return ast.Constant(value=round(float(v), 4))


def _mul(var: str, k: float) -> ast.BinOp:
    return ast.BinOp(left=_name(var), op=ast.Mult(), right=_const(k))


def _random_atom(rng: random.Random) -> ast.Compare:
    """의미 있는 단일 비교절(Compare) 하나를 무작위 생성."""
    kind = rng.random()
    op = rng.choice(_CMP_OPS)()

    if kind < 0.45:
        # 가격 vs 가격*상수 (예: close > open*1.03)
        left = _name(rng.choice(PRICE_VARS))
        rhs_var = rng.choice(PRICE_VARS)
        k = rng.choice([0.90, 0.95, 0.98, 0.99, 1.0, 1.01, 1.02, 1.03, 1.05, 1.10])
        right: ast.expr = _mul(rhs_var, k) if k != 1.0 else _name(rhs_var)
    elif kind < 0.70:
        # 거래량 vs 거래량*상수 (예: volume > vol_ma5*1.5)
        left = _name(rng.choice(VOL_VARS))
        rhs_var = rng.choice(VOL_VARS)
        k = rng.choice([0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0])
        right = _mul(rhs_var, k) if k != 1.0 else _name(rhs_var)
    elif kind < 0.88:
        # 부호 스칼라 vs 상수 (예: ret1 > 0.01, body < -0.005)
        left = _name(rng.choice(SIGNED_SCALAR_VARS))
        right = _const(rng.choice([-0.03, -0.02, -0.01, -0.005, 0.0, 0.005, 0.01, 0.02, 0.03]))
    else:
        # 비율 vs 상수 (예: vol_ratio > 1.8, hl_range > 0.03)
        left = _name(rng.choice(RATIO_VARS))
        right = _const(rng.choice([0.01, 0.02, 0.03, 0.05, 1.2, 1.5, 1.8, 2.2]))

    return ast.Compare(left=left, ops=[op], comparators=[right])


def _random_tree(rng: random.Random, *, max_clauses: int = 3) -> ast.expr:
    """1~max_clauses 개의 비교절을 &/| BinOp 로 결합한 트리."""
    n = rng.randint(1, max(1, int(max_clauses)))
    node: ast.expr = _random_atom(rng)
    for _ in range(n - 1):
        op = rng.choice(_BOOL_OPS)()
        node = ast.BinOp(left=node, op=op, right=_random_atom(rng))
    return node


# ---------------------------------------------------------------------------
# 직렬화 / 검증
# ---------------------------------------------------------------------------
def _tree_to_expr(node: ast.expr) -> str:
    wrapped = ast.Expression(body=node)
    ast.fix_missing_locations(wrapped)
    return ast.unparse(wrapped)


def _parse(expr: str) -> Optional[ast.expr]:
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError:
        return None
    return tree.body


def _is_safe(expr: str) -> bool:
    """incubator_engine.is_safe_expression 와 동일 게이트(순환 import 안전 폴백)."""
    try:
        from incubator_engine import is_safe_expression

        return bool(is_safe_expression(expr))
    except Exception:
        return _parse(expr) is not None


# ---------------------------------------------------------------------------
# 유전 연산자: 교배(crossover) / 돌연변이(mutation)
# ---------------------------------------------------------------------------
def _collect_subtrees(node: ast.expr) -> List[ast.expr]:
    """교배·치환 대상이 되는 Compare / BinOp(BitAnd|BitOr) 노드 수집."""
    out: List[ast.expr] = []

    class _V(ast.NodeVisitor):
        def visit_Compare(self, n: ast.Compare) -> None:
            out.append(n)
            self.generic_visit(n)

        def visit_BinOp(self, n: ast.BinOp) -> None:
            if isinstance(n.op, _BOOL_OPS):
                out.append(n)
            self.generic_visit(n)

    _V().visit(node)
    return out


class _ReplaceNth(ast.NodeTransformer):
    def __init__(self, target_idx: int, new_node: ast.expr) -> None:
        self.i = -1
        self.target = target_idx
        self.new = new_node

    def _maybe(self, node: ast.expr) -> ast.expr:
        self.i += 1
        if self.i == self.target:
            return self.new
        return node

    def visit_Compare(self, node: ast.Compare) -> ast.expr:
        self.generic_visit(node)
        return self._maybe(node)

    def visit_BinOp(self, node: ast.BinOp) -> ast.expr:
        self.generic_visit(node)
        if isinstance(node.op, _BOOL_OPS):
            return self._maybe(node)
        return node


def crossover(expr_a: str, expr_b: str, rng: random.Random) -> Optional[str]:
    """부모 A 의 임의 서브트리를 부모 B 의 임의 서브트리로 치환."""
    a = _parse(expr_a)
    b = _parse(expr_b)
    if a is None or b is None:
        return None
    subs_b = _collect_subtrees(b)
    if not subs_b:
        return None
    donor = _copy(rng.choice(subs_b))
    eligible_a = _collect_subtrees(a)
    if not eligible_a:
        return None
    target = rng.randrange(len(eligible_a))
    new_root = _ReplaceNth(target, donor).visit(_copy(a))
    expr = _tree_to_expr(new_root)
    return expr if _is_safe(expr) else None


def _copy(node: ast.AST) -> ast.AST:
    return ast.fix_missing_locations(ast.parse(ast.unparse(ast.Expression(body=node)), mode="eval").body)  # type: ignore[arg-type]


def mutate(expr: str, rng: random.Random) -> Optional[str]:
    """상수 가우시안 변이 / 비교·논리 연산자 플립 / 비교절 통째 교체 중 하나."""
    root = _parse(expr)
    if root is None:
        return None
    op_kind = rng.random()

    if op_kind < 0.40:
        consts = _find_constants(root)
        if consts:
            c = rng.choice(consts)
            factor = rng.uniform(0.80, 1.25)
            if abs(c.value) < 1e-9:
                c.value = round(rng.choice([-0.01, 0.01, 0.005, -0.005]), 4)
            else:
                c.value = round(float(c.value) * factor, 4)
        else:
            return _replace_random_atom(root, rng)
    elif op_kind < 0.65:
        if not _flip_a_cmp_op(root, rng):
            return _replace_random_atom(root, rng)
    elif op_kind < 0.80:
        if not _flip_a_bool_op(root, rng):
            return _replace_random_atom(root, rng)
    else:
        return _replace_random_atom(root, rng)

    expr2 = _tree_to_expr(root)
    return expr2 if _is_safe(expr2) else None


def _find_constants(node: ast.expr) -> List[ast.Constant]:
    out: List[ast.Constant] = []
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float)) and not isinstance(n.value, bool):
            out.append(n)
    return out


def _flip_a_cmp_op(node: ast.expr, rng: random.Random) -> bool:
    cmps = [n for n in ast.walk(node) if isinstance(n, ast.Compare)]
    if not cmps:
        return False
    c = rng.choice(cmps)
    flip = {ast.Lt: ast.Gt, ast.Gt: ast.Lt, ast.LtE: ast.GtE, ast.GtE: ast.LtE}
    c.ops = [flip.get(type(o), type(o))() for o in c.ops]
    return True


def _flip_a_bool_op(node: ast.expr, rng: random.Random) -> bool:
    bins = [n for n in ast.walk(node) if isinstance(n, ast.BinOp) and isinstance(n.op, _BOOL_OPS)]
    if not bins:
        return False
    b = rng.choice(bins)
    b.op = ast.BitOr() if isinstance(b.op, ast.BitAnd) else ast.BitAnd()
    return True


def _replace_random_atom(root: ast.expr, rng: random.Random) -> Optional[str]:
    eligible = _collect_subtrees(root)
    if not eligible:
        new = _random_atom(rng)
        expr = _tree_to_expr(new)
        return expr if _is_safe(expr) else None
    target = rng.randrange(len(eligible))
    new_root = _ReplaceNth(target, _random_atom(rng)).visit(root)
    expr = _tree_to_expr(new_root)
    return expr if _is_safe(expr) else None


# ---------------------------------------------------------------------------
# 군집 생성(Population) — 매 세대 N 개 돌연변이 창조
# ---------------------------------------------------------------------------
def random_strategy(rng: random.Random, *, max_clauses: int = 3) -> str:
    for _ in range(8):
        expr = _tree_to_expr(_random_tree(rng, max_clauses=max_clauses))
        if _is_safe(expr):
            return expr
    return "close > open"


def generate_population(
    champion_exprs: Optional[Sequence[str]] = None,
    *,
    n: int = 1000,
    regime: str = "SIDEWAYS",
    champion_survival_rate: float = 1.0,
    max_clauses: int = 3,
    seed: Optional[int] = None,
    name_prefix: str = "GP",
) -> List[dict]:
    """
    지난주 생존 챔피언(부모) + 국면 변속 기어로 N 개의 새로운 수식 군집을 창조.
    반환: [{"name","expr","origin"}], 중복 expr 제거.
    """
    rng = random.Random(seed)
    parents = [e for e in (champion_exprs or []) if isinstance(e, str) and e.strip() and _is_safe(e)]
    gear = regime_gear(regime, champion_survival_rate=champion_survival_rate)

    out: List[dict] = []
    seen: set[str] = set()

    def _add(expr: Optional[str], origin: str) -> None:
        if not expr or expr in seen or not _is_safe(expr):
            return
        seen.add(expr)
        out.append({"name": f"{name_prefix}_{len(out):04d}", "expr": expr, "origin": origin})

    # 1) 엘리트 보존: 부모 챔피언 원본을 그대로 다음 세대로 (생존 검증 재확인용)
    for e in parents[: gear.elite_keep]:
        _add(e, "elite")

    guard = 0
    max_guard = int(n) * 25 + 200
    while len(out) < int(n) and guard < max_guard:
        guard += 1
        roll = rng.random()
        if parents and roll < gear.crossover_rate and len(parents) >= 1:
            pa = rng.choice(parents)
            pb = rng.choice(parents) if len(parents) > 1 else pa
            _add(crossover(pa, pb, rng), "crossover")
        elif parents and roll < gear.crossover_rate + gear.mutation_rate:
            _add(mutate(rng.choice(parents), rng), "mutation")
        else:
            _add(random_strategy(rng, max_clauses=max_clauses), "random")

    # 부모가 전혀 없으면(콜드 스타트) 전부 무작위로 채움
    while len(out) < int(n) and guard < max_guard * 2:
        guard += 1
        _add(random_strategy(rng, max_clauses=max_clauses), "random")

    return out


def default_seed_strategies() -> Tuple[str, ...]:
    """콜드 스타트(이전 챔피언 없음) 시 1세대 부모로 쓰는 소수 시드 수식."""
    return (
        "(close > open * 1.02) & (volume > vol_ma5 * 1.5)",
        "(close > ma20) & (ret1 > 0.01)",
        "(close > ma5) & (ma5 > ma20)",
        "(body > 0.005) & (vol_ratio > 1.3)",
        "(close > low * 1.02) & (close > open)",
    )


if __name__ == "__main__":
    pop = generate_population(default_seed_strategies(), n=12, regime="BULL", seed=7)
    print(f"gear(BULL) = {regime_gear('BULL').as_dict()}")
    print(f"gear(BEAR, surv=0.0) = {regime_gear('BEAR', champion_survival_rate=0.0).as_dict()}")
    for r in pop:
        print(f"  [{r['origin']:9s}] {r['name']}: {r['expr']}")
