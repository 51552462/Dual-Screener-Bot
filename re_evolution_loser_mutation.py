"""
Re-Evolution Phase 2 — Loser's Mutation (실패 DNA 개조).

3-Strike 강등(META_RE_EVOLUTION_DEMOTED) 로직의 DNA를 주말 인큐베이터로 전송:
  · 섀도우/실전 패자 청산 부검 → failure_mode 진단
  · 챔피언 DNA 교배(crossover) + 실패 모드 강제 돌연변이
  · Child Mutant → INCUBATOR_TEMPLATES (가상매매 검증 경로)
"""
from __future__ import annotations

import copy
import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def re_evolution_mutation_config(
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    block = cfg.get("RE_EVOLUTION_MUTATION") or {}
    base = block if isinstance(block, dict) else cfg

    def _i(key: str, default: int) -> int:
        try:
            return int(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "enabled": _cfg_bool(cfg, "ENABLE_RE_EVOLUTION_LOSER_MUTATION", True),
        "max_mutants": _i("RE_EVOLUTION_MAX_LOSER_MUTANTS", 6),
        "lookback_rows": _i("RE_EVOLUTION_LOSER_LOOKBACK", 40),
        "mutation_rate": _f("RE_EVOLUTION_MUTATION_RATE", 0.05),
        "loss_diag_pct": _f("RE_EVOLUTION_DIAG_LOSS_PCT", -3.0),
    }


def _pending_demotion_candidates(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """mutation_done 이 아닌 강등 레코드."""
    raw = meta.get("META_RE_EVOLUTION_DEMOTED")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if item.get("mutation_done"):
            continue
        if not item.get("mutation_pending", True):
            continue
        gk = str(item.get("group_key") or "").strip()
        if gk:
            out.append(dict(item))
    return out


def fetch_loser_closed_rows(
    market: str,
    group_key: str,
    *,
    lookback: int = 40,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """그룹·섀도우(RE_EVOL_SHADOW) 청산 표본."""
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return []

    path = db_path
    if not path:
        try:
            from market_db_paths import market_db_read_path

            path = market_db_read_path()
        except Exception:
            path = None
    if not path or not os.path.isfile(path):
        return []

    like_g = f"%{gk}%"
    like_shadow = "%RE_EVOL_SHADOW%"
    try:
        conn = sqlite3.connect(path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                """
                SELECT sig_type, final_ret, bars_held, dyn_cpv, dyn_tb,
                       v_cpv, v_energy, entry_price, max_high, min_low,
                       invest_amount, sim_kelly_invest, exit_reason
                FROM forward_trades
                WHERE market=? AND status LIKE 'CLOSED%%'
                  AND (sig_type LIKE ? OR sig_type LIKE ?)
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (mk, like_g, like_shadow, int(lookback)),
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as ex:
        logger.warning("fetch_loser_closed_rows failed: %s", ex)
        return []


def resolve_parent_dna_template(
    sys_config: Dict[str, Any],
    market: str,
    group_key: str,
    closed_rows: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], str]:
    """
    패자 부모 DNA — DNA_SUPERNOVA MULTI → INCUBATOR → 청산 평균 추론.
    Returns (template_dict, source_label).
    """
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()

    multi_key = f"DNA_SUPERNOVA_{mk}_MULTI"
    pool = sys_config.get(multi_key) or {}
    if isinstance(pool, dict):
        if gk in pool and isinstance(pool[gk], dict):
            return copy.deepcopy(pool[gk]), f"{multi_key}:{gk}"
        for name, tpl in pool.items():
            if not isinstance(tpl, dict):
                continue
            if gk in str(name) or str(name) in gk:
                return copy.deepcopy(tpl), f"{multi_key}:{name}"

    inc = sys_config.get("INCUBATOR_TEMPLATES") or {}
    if isinstance(inc, dict):
        for name, tpl in inc.items():
            if not isinstance(tpl, dict):
                continue
            if gk in str(name) or str(name) in gk:
                return copy.deepcopy(tpl), f"INCUBATOR:{name}"

    rows = closed_rows or []
    if rows:
        cpvs, tbs, bbes, rss = [], [], [], []
        for r in rows:
            try:
                cpvs.append(float(r.get("dyn_cpv") or r.get("v_cpv") or 0))
            except (TypeError, ValueError):
                pass
            try:
                tbs.append(float(r.get("dyn_tb") or 0))
            except (TypeError, ValueError):
                pass
            try:
                bbes.append(float(r.get("v_energy") or 0))
            except (TypeError, ValueError):
                pass
        if cpvs or tbs:
            tpl = {
                "cpv": round(sum(cpvs) / len(cpvs), 4) if cpvs else 1.0,
                "tb": round(sum(tbs) / len(tbs), 4) if tbs else 10.0,
                "bbe": round(sum(bbes) / len(bbes), 4) if bbes else 1.5,
                "rs": 180.0,
                "cos_cutoff": 0.78,
                "stop_loss_pct": 0.04,
                "kelly_risk_pct": 0.015,
            }
            return tpl, "ledger_infer"

    return {
        "cpv": 1.0,
        "tb": 10.0,
        "bbe": 1.5,
        "rs": 180.0,
        "cos_cutoff": 0.78,
        "stop_loss_pct": 0.04,
        "kelly_risk_pct": 0.015,
    }, "default_seed"


def _child_mutant_name(market: str, group_key: str, stamp: str) -> str:
    gk_s = re.sub(r"[^\w]", "_", str(group_key or "UNK"))[:20]
    h = hashlib.sha256(f"{market}|{group_key}|{stamp}".encode()).hexdigest()[:8]
    return f"RE_EVOL_{str(market).upper()}_{gk_s}_{h}"


def _mark_demotion_mutated(
    meta: Dict[str, Any],
    strategy_id: str,
    *,
    child_name: str,
    diagnosis: Dict[str, Any],
) -> None:
    sid = str(strategy_id or "")
    log: List[Dict[str, Any]] = list(meta.get("META_RE_EVOLUTION_MUTATION_LOG") or [])
    log.append(
        {
            "strategy_id": sid,
            "child_name": child_name,
            "failure_mode": diagnosis.get("failure_mode"),
            "mutation_kind": diagnosis.get("mutation_kind"),
            "at": datetime.now(timezone.utc).isoformat(),
        }
    )
    meta["META_RE_EVOLUTION_MUTATION_LOG"] = log[-80:]

    demoted: List[Dict[str, Any]] = []
    for item in meta.get("META_RE_EVOLUTION_DEMOTED") or []:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("strategy_id") or "") == sid:
            row["mutation_done"] = True
            row["mutation_pending"] = False
            row["child_mutant_name"] = child_name
            row["failure_mode"] = diagnosis.get("failure_mode")
            row["mutated_at"] = datetime.now(timezone.utc).isoformat()
        demoted.append(row)
    meta["META_RE_EVOLUTION_DEMOTED"] = demoted


def run_re_evolution_loser_mutation_cycle(
    sys_config: Dict[str, Any],
    *,
    meta: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    주말 훅 — 강등 패자 DNA 개조 → INCUBATOR_TEMPLATES.

    Returns (updated_config, log_lines).
    meta 미지정 시 load/save meta_governor_state.
    """
    from dna_mutator import (
        build_loser_child_mutant,
        diagnose_loser_from_closures,
        select_champion_template,
    )
    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved

    cfg = copy.deepcopy(sys_config)
    mc = re_evolution_mutation_config(cfg)
    logs: List[str] = []

    if not mc["enabled"]:
        logs.append("▪️ Re-Evolution Loser Mutation 비활성")
        return cfg, logs

    meta_u = dict(meta) if isinstance(meta, dict) else dict(load_meta_state_resolved())
    pending = _pending_demotion_candidates(meta_u)
    if not pending:
        logs.append("▪️ Re-Evolution 대기 패자 0건 — mutation 스킵")
        return cfg, logs

    inc = cfg.get("INCUBATOR_TEMPLATES")
    inc = dict(inc) if isinstance(inc, dict) else {}
    gene_pool = cfg.get("MUTANT_GENE_POOL")
    gene_pool = dict(gene_pool) if isinstance(gene_pool, dict) else {}

    stamp = datetime.now().strftime("%y%m%d%H%M")
    created = 0
    max_n = int(mc["max_mutants"])
    new_child_names: List[str] = []

    for cand in pending:
        if created >= max_n:
            break
        mk = str(cand.get("market") or "KR").upper()
        gk = str(cand.get("group_key") or "").strip()
        sid = str(cand.get("strategy_id") or "")
        if not gk:
            continue

        rows = fetch_loser_closed_rows(
            mk,
            gk,
            lookback=int(mc["lookback_rows"]),
            db_path=forward_db_path,
        )
        diagnosis = diagnose_loser_from_closures(
            rows,
            loss_threshold_pct=float(mc["loss_diag_pct"]),
        )

        loser_tpl, dna_src = resolve_parent_dna_template(cfg, mk, gk, rows)
        champ = select_champion_template(cfg, mk, exclude_group=gk)
        champ_tpl = champ[1] if champ else None
        champ_name = champ[0] if champ else None

        child = build_loser_child_mutant(
            loser_tpl,
            diagnosis,
            champ_tpl,
            rate=float(mc["mutation_rate"]),
            parent_label=f"{gk}←{dna_src}",
        )
        child["parent_group"] = gk
        child["parent_strategy_id"] = sid
        child["parent_dna_source"] = dna_src
        child["champion_crossover"] = champ_name
        child["market"] = mk
        diagnosis["mutation_kind"] = child.get("mutation_kind")

        child_name = _child_mutant_name(mk, gk, f"{stamp}_{created}")
        inc[child_name] = child
        gene_pool[child_name] = {
            "market": mk,
            "parent_group": gk,
            "parent_strategy_id": sid,
            "champion": champ_name,
            "failure_mode": diagnosis.get("failure_mode"),
            "mutation_kind": child.get("mutation_kind"),
            "re_evolution": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _mark_demotion_mutated(meta_u, sid, child_name=child_name, diagnosis=diagnosis)
        new_child_names.append(child_name)
        created += 1

        logs.append(
            f"▪️ {mk} <b>{child_name}</b> ← {gk} "
            f"({diagnosis.get('failure_mode')} · {child.get('mutation_kind')}"
            f"{f' × {champ_name}' if champ_name else ''})"
        )

    cfg["INCUBATOR_TEMPLATES"] = inc
    cfg["MUTANT_GENE_POOL"] = gene_pool
    cfg["RE_EVOLUTION_MUTATION_LAST_RUN"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    meta_u["META_RE_EVOLUTION_MUTATION_LAST_RUN"] = cfg["RE_EVOLUTION_MUTATION_LAST_RUN"]
    prior_children = list(meta_u.get("META_RE_EVOLUTION_CHILDREN") or [])
    meta_u["META_RE_EVOLUTION_CHILDREN"] = sorted(
        set(prior_children + new_child_names)
    )[-100:]

    save_meta_governor_state_atomic(meta_u)
    invalidate_meta_state_cache()

    logs.insert(0, f"🔄 Re-Evolution Loser Mutation: {created} child mutant(s)")
    return cfg, logs


def collect_re_evolution_incubator_seed_hints(
    sys_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    incubator_engine 주말 실행용 — 최근 RE_EVOL child 요약(리포트/로그).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    inc = cfg.get("INCUBATOR_TEMPLATES") or {}
    if not isinstance(inc, dict):
        return []
    hints: List[Dict[str, Any]] = []
    for name, tpl in inc.items():
        if not str(name).startswith("RE_EVOL_"):
            continue
        if not isinstance(tpl, dict) or not tpl.get("re_evolution"):
            continue
        hints.append(
            {
                "name": name,
                "parent_group": tpl.get("parent_group"),
                "failure_mode": tpl.get("failure_mode"),
                "mutation_kind": tpl.get("mutation_kind"),
            }
        )
    return hints[-20:]
