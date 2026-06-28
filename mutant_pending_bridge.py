"""
OOS 검증 합격 전략(validated_live_mutants.json) → 설정 PENDING_MUTANTS 대기열.
실전 INCUBATOR_TEMPLATES 병합은 APPROVE_PENDING_MUTANTS_TO_INCUBATOR == True 일 때만(수동 승인 게이트).
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Mapping

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
VALIDATED_LIVE_MUTANTS_JSON = os.path.join(_THIS_DIR, "validated_live_mutants.json")

PENDING_MUTANTS_CONFIG_KEY = "PENDING_MUTANTS"
APPROVE_PENDING_MUTANTS_FLAG_KEY = "APPROVE_PENDING_MUTANTS_TO_INCUBATOR"


def _sanitize_template_key(name: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", str(name).strip())[:40]
    return s or "MUTANT"


def sync_validated_json_into_pending() -> tuple[int, str]:
    """
    validated_live_mutants.json 의 promoted[] 를 읽어 PENDING_MUTANTS 에 병합(이름+expr 기준 중복 제거).
    Live INCUBATOR_TEMPLATES 는 건드리지 않는다.
    """
    from config_manager import load_system_config, save_system_config

    if not os.path.isfile(VALIDATED_LIVE_MUTANTS_JSON):
        return 0, "validated_live_mutants.json 없음"
    try:
        with open(VALIDATED_LIVE_MUTANTS_JSON, "r", encoding="utf-8") as f:
            blob = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        return 0, f"JSON 로드 실패: {e}"

    promoted = blob.get("promoted") if isinstance(blob, dict) else None
    if not isinstance(promoted, list) or not promoted:
        return 0, "promoted 비어 있음"

    cfg = dict(load_system_config())
    pending_root = cfg.get(PENDING_MUTANTS_CONFIG_KEY)
    if not isinstance(pending_root, dict):
        pending_root = {}
    existing = list(pending_root.get("strategies") or [])
    seen = {(str(x.get("name")), str(x.get("expr"))) for x in existing if isinstance(x, dict)}
    added = 0
    for row in promoted:
        if not isinstance(row, dict):
            continue
        key = (str(row.get("name")), str(row.get("expr", "")))
        if key in seen:
            continue
        seen.add(key)
        entry = {
            "name": row.get("name"),
            "expr": row.get("expr"),
            "oos_win_rate": row.get("oos_win_rate"),
            "oos_avg_return": row.get("oos_avg_return"),
            "n_signals": row.get("n_signals"),
            "validated_at": row.get("validated_at"),
            "status": "PENDING_APPROVAL",
        }
        existing.append(entry)
        added += 1

    pending_root["strategies"] = existing
    pending_root["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pending_root["source_json"] = os.path.basename(VALIDATED_LIVE_MUTANTS_JSON)
    cfg[PENDING_MUTANTS_CONFIG_KEY] = pending_root
    save_system_config(cfg)
    return added, f"PENDING_MUTANTS +{added}건 반영"


AUTO_PROMOTE_FLAG_KEY = "AUTO_PROMOTE_GP_MUTANTS"
GP_MUTANT_MAX_LIVE = 24  # INCUBATOR_TEMPLATES 내 자동승격 유전자 동시 생존 상한


def auto_merge_validated_into_incubator() -> tuple[int, str]:
    """
    [Mission 5] 수동 승인 게이트(APPROVE_PENDING_MUTANTS_TO_INCUBATOR) 우회.
    validated_live_mutants.json 의 promoted[] 유전자 수식을 INCUBATOR_TEMPLATES 에 즉시 병합하고,
    template_bandit 의 '탐색 모드 최소 켈리' 밸브로만 실전에 투입되도록 자본 통제를 건다.

    안전장치
      - cfg[AUTO_PROMOTE_GP_MUTANTS] 가 명시적으로 False 면 비활성(기본 ON).
      - 동일 expr 중복 병합 방지.
      - 자동 승격 유전자(GP_MUT_*)는 최대 GP_MUTANT_MAX_LIVE 개만 생존(오래된 것부터 정리).
    """
    from config_manager import load_system_config, save_system_config

    if not os.path.isfile(VALIDATED_LIVE_MUTANTS_JSON):
        return 0, "validated_live_mutants.json 없음"
    try:
        with open(VALIDATED_LIVE_MUTANTS_JSON, "r", encoding="utf-8") as f:
            blob = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        return 0, f"JSON 로드 실패: {e}"

    promoted = blob.get("promoted") if isinstance(blob, dict) else None
    if not isinstance(promoted, list) or not promoted:
        return 0, "promoted 비어 있음"

    cfg = dict(load_system_config())
    if cfg.get(AUTO_PROMOTE_FLAG_KEY) is False:
        return 0, "AUTO_PROMOTE_GP_MUTANTS=False (자동 승격 비활성)"

    inc = dict(cfg.get("INCUBATOR_TEMPLATES") or {})
    existing_exprs = {
        str(v.get("mutant_oos_expr") or "")
        for v in inc.values()
        if isinstance(v, dict) and v.get("mutant_oos_expr")
    }

    try:
        import template_bandit as tb
    except Exception:
        tb = None

    added = 0
    for row in promoted:
        if not isinstance(row, dict):
            continue
        expr = str(row.get("expr") or "").strip()
        if not expr or expr in existing_exprs:
            continue
        existing_exprs.add(expr)
        name = _sanitize_template_key(row.get("name") or "MUTANT")
        key = f"GP_MUT_{name}"
        inc[key] = {
            "cpv": 0.5,
            "tb": 10.83,
            "bbe": 16.12,
            "rs": 7.0,
            "cos_cutoff": 0.99,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "GP_AUTO_EXPLORING",
            "capital_mode": "EXPLORE_MIN_KELLY",
            "mutant_oos_expr": expr,
            "oos_win_rate": row.get("oos_win_rate"),
            "oos_excess_alpha": row.get("oos_excess_alpha"),
            "oos_avg_return": row.get("oos_avg_return"),
            "n_signals": row.get("n_signals"),
            "source": "gp_oos_auto_promoted",
        }
        if tb is not None:
            tb.init_exploration_arm(cfg, key)
        added += 1

    if added == 0:
        return 0, "신규 자동 승격 대상 없음(중복)"

    # 동시 생존 상한: GP_MUT_* 중 오래된 것부터 정리
    gp_keys = [(str(v.get("created_at") or ""), k) for k, v in inc.items()
               if isinstance(v, dict) and str(k).startswith("GP_MUT_")]
    if len(gp_keys) > GP_MUTANT_MAX_LIVE:
        gp_keys.sort()
        for _, k in gp_keys[: len(gp_keys) - GP_MUTANT_MAX_LIVE]:
            inc.pop(k, None)

    cfg["INCUBATOR_TEMPLATES"] = inc
    if tb is not None:
        cfg[tb.BANDIT_KEY] = cfg.get(tb.BANDIT_KEY, {})
    save_system_config(cfg)
    return added, f"INCUBATOR_TEMPLATES 자동 병합 +{added} (탐색 켈리 밸브, 생존 {min(len(gp_keys), GP_MUTANT_MAX_LIVE)})"


def pending_rd_telegram_fragment(cfg: Mapping[str, Any]) -> str:
    root = cfg.get(PENDING_MUTANTS_CONFIG_KEY) if isinstance(cfg, dict) else None
    n = 0
    if isinstance(root, dict):
        rows = root.get("strategies")
        if isinstance(rows, list):
            n = sum(1 for r in rows if isinstance(r, dict) and r.get("status") == "PENDING_APPROVAL")
    if n <= 0:
        return ""
    flag = bool(cfg.get(APPROVE_PENDING_MUTANTS_FLAG_KEY)) if isinstance(cfg, dict) else False
    gate = "ON(다음 주기에 인큐베이터로 원자 병합 시도)" if flag else "OFF(수동 승인 대기)"
    return (
        f"\n🔬 <b>[R&D] {n}개의 신규 전략 승인 대기 중</b> "
        f"(<code>{PENDING_MUTANTS_CONFIG_KEY}</code>) · 승인 게이트: <b>{gate}</b>\n"
        f"<i>※ 실전 Live 엔진 자동 병합 없음. <code>{APPROVE_PENDING_MUTANTS_FLAG_KEY}=true</code> 후 "
        f"supernova 등 승인 지점에서만 <code>INCUBATOR_TEMPLATES</code>에 반영됩니다.</i>\n"
    )


def apply_pending_mutants_if_approved() -> str:
    """
    APPROVE_PENDING_MUTANTS_TO_INCUBATOR 가 True 일 때만 PENDING_MUTANTS → INCUBATOR_TEMPLATES 원자 반영.
    cos_cutoff=0.99 로 사실상 코사인 매칭 비활성(샌드박스) — 수치 튜닝은 운영자가 후속 조정.
    """
    from config_manager import load_system_config, save_system_config

    cfg = dict(load_system_config())
    if not bool(cfg.get(APPROVE_PENDING_MUTANTS_FLAG_KEY)):
        return "skip: 승인 플래그 OFF"
    pending_root = cfg.get(PENDING_MUTANTS_CONFIG_KEY)
    if not isinstance(pending_root, dict):
        cfg[APPROVE_PENDING_MUTANTS_FLAG_KEY] = False
        save_system_config(cfg)
        return "skip: PENDING_MUTANTS 없음"
    rows_all = [r for r in (pending_root.get("strategies") or []) if isinstance(r, dict)]
    rows_to_merge = [r for r in rows_all if r.get("status") == "PENDING_APPROVAL"]
    if not rows_to_merge:
        cfg[APPROVE_PENDING_MUTANTS_FLAG_KEY] = False
        save_system_config(cfg)
        return "skip: 승인 대기(PENDING_APPROVAL) 없음"

    inc = dict(cfg.get("INCUBATOR_TEMPLATES") or {})
    for row in rows_to_merge:
        name = _sanitize_template_key(row.get("name") or "MUTANT")
        key = f"OOSVAL_{name}"
        inc[key] = {
            "cpv": 0.5,
            "tb": 10.83,
            "bbe": 16.12,
            "rs": 7.0,
            "cos_cutoff": 0.99,
            "created_at": datetime.now().strftime("%Y-%m-%d"),
            "status": "OOS_IMPORTED_PENDING_CALIBRATION",
            "mutant_oos_expr": str(row.get("expr") or ""),
            "oos_win_rate": row.get("oos_win_rate"),
            "oos_avg_return": row.get("oos_avg_return"),
            "n_signals": row.get("n_signals"),
            "source": "mutant_oos_validated",
        }
        row["status"] = "MERGED"

    cfg["INCUBATOR_TEMPLATES"] = inc
    cfg[PENDING_MUTANTS_CONFIG_KEY] = {
        "strategies": [r for r in rows_all if r.get("status") != "MERGED"],
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_action": "merged_to_incubator_templates",
        "last_merged_count": len(rows_to_merge),
    }
    cfg[APPROVE_PENDING_MUTANTS_FLAG_KEY] = False
    save_system_config(cfg)
    return f"merged {len(rows_to_merge)} rows into INCUBATOR_TEMPLATES (sandbox cos_cutoff=0.99)"


if __name__ == "__main__":
    added, msg = sync_validated_json_into_pending()
    print(added, msg)
