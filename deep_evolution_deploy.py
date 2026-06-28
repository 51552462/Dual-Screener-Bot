"""
Mission 3 / 7 — 신·구 템플릿 병렬 앙상블 + 인간개입 없는 자율 배포 + 영향도 추적.

주말 장기기억(Deep Archive)으로 찾아낸 '과거 유사장세의 전설적 승자 DNA' 를, 기존 Base
템플릿(RANK_A~D 등)을 **삭제·덮어쓰지 않고** `[🧬DEEP_EVOLVED_v1]`, `[🧬DEEP_EVOLVED_v2]` …
형태의 독립 이름으로 **병렬(Append)** 투입한다(supernova_hunter 가 NEW_EVOLUTION_* 접두로
ideal_templates 에 자동 로드). 각 신형 템플릿은 mab_capital_allocator·template_bandit 가
독립 'Arm' 으로 인식하여 구형/신형 간 켈리 자본을 유동 분산(앙상블 다각화)한다.

배포 템플릿 이름에 메타태그가 박혀, 진입 종목이 가상/실전 장부(forward_trades.sig_type)에
태그를 달고 다니므로 신형 템플릿의 WR·MFE·누적PnL 을 버전별로 독립 계측(Impact Attribution).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

DEEP_EVOLVED_MARKER = "🧬DEEP_EVOLVED"     # 전 버전 공통 마커(집계 LIKE 용)
DEEP_EVOLVED_TAG = DEEP_EVOLVED_MARKER       # 하위호환(멤버십 체크용)
DEPLOY_PREFIX = "NEW_EVOLUTION_"             # supernova_hunter 가 자동 로드하는 접두사
DEPLOY_REGISTRY_KEY = "DEEP_EVOLVED_DEPLOYED"
IMPACT_KEY = "DEEP_EVOLVED_IMPACT"
MAX_VERSIONS_KEY = "DEEP_EVOLVED_MAX_VERSIONS"
DEFAULT_MAX_VERSIONS = 3                      # 병렬 유지할 신형 arm 상한(서버/포트 안정)
DEFAULT_DEPLOY_COS_CUTOFF = 0.80


def version_tag(version: int) -> str:
    return f"{DEEP_EVOLVED_MARKER}_v{int(version)}"


def deployed_template_name(market: str, version: int = 1) -> str:
    """라이브 ideal_templates 키 — 이름에 버전 메타태그를 박아 장부에 강제 부착."""
    return f"{DEPLOY_PREFIX}[{version_tag(version)}]_{str(market).upper()}"


def _set_cfg_value(key: str, value: Any) -> bool:
    try:
        from config_manager import set_config_value

        set_config_value(key, value)
        return True
    except Exception:
        return False


def _registry(cfg: Dict[str, Any]) -> Dict[str, Any]:
    reg = cfg.get(DEPLOY_REGISTRY_KEY)
    if not isinstance(reg, dict):
        reg = {}
    return reg


def next_version(cfg: Dict[str, Any]) -> int:
    """레지스트리의 최대 버전 + 1 (없으면 1). 모든 시장에 동일 버전 번호를 부여."""
    reg = _registry(cfg)
    mx = 0
    for meta in reg.values():
        if isinstance(meta, dict):
            try:
                mx = max(mx, int(meta.get("version", 0)))
            except (TypeError, ValueError):
                continue
    return mx + 1


# ---------------------------------------------------------------------------
# 자율 배포 (병렬 추가 + 오래된 버전 은퇴)
# ---------------------------------------------------------------------------
def auto_deploy(
    deep_result: Dict[str, Any],
    cfg: Dict[str, Any],
    *,
    markets: Tuple[str, ...] = ("KR", "US"),
    persist: bool = True,
) -> Dict[str, Any]:
    """
    favorable archetype DNA 를 신형 병렬 템플릿으로 자동 배포(Base 불변·Append-only).
    상한(DEFAULT_MAX_VERSIONS) 초과 시 가장 오래된 버전을 은퇴시켜 포트 안정성을 지킨다.
    """
    out: Dict[str, Any] = {"deployed": [], "retired": [], "version": None,
                           "episode": deep_result.get("best_episode"), "applied": False}
    if not bool(deep_result.get("front_run_favorable", False)):
        out["reason"] = "not_favorable"
        return out
    archetype = deep_result.get("archetype_dna")
    if not isinstance(archetype, dict):
        out["reason"] = "no_archetype"
        return out

    try:
        import template_bandit as tb
    except Exception:
        tb = None

    reg = _registry(cfg)
    ver = next_version(cfg)
    out["version"] = ver
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for mk in markets:
        dna = archetype.get(str(mk).upper())
        if not isinstance(dna, dict):
            continue
        try:
            cpv, tbar, bbe = float(dna["cpv"]), float(dna["tb"]), float(dna["bbe"])
        except (KeyError, TypeError, ValueError):
            continue
        name = deployed_template_name(mk, ver)
        cfg[name] = {
            "cpv": round(cpv, 4), "tb": round(tbar, 4), "bbe": round(bbe, 4),
            "cos_cutoff": DEFAULT_DEPLOY_COS_CUTOFF,
            "source": "DEEP_ARCHIVE", "version": ver,
            "episode": deep_result.get("best_episode"), "deployed_at": now,
        }
        reg[name] = {
            "market": str(mk).upper(), "version": ver,
            "tag": version_tag(ver), "episode": deep_result.get("best_episode"),
            "dna": [round(cpv, 4), round(tbar, 4), round(bbe, 4)], "deployed_at": now,
        }
        if tb is not None:
            try:
                tb.init_bandit(cfg, name, shadow_wins=1, shadow_losses=0)
            except Exception:
                pass
        out["deployed"].append(name)

    cfg[DEPLOY_REGISTRY_KEY] = reg
    out["applied"] = bool(out["deployed"])

    # 상한 초과 → 가장 오래된 버전 은퇴(병렬 arm 폭주 방지)
    try:
        max_versions = int(cfg.get(MAX_VERSIONS_KEY, DEFAULT_MAX_VERSIONS))
    except (TypeError, ValueError):
        max_versions = DEFAULT_MAX_VERSIONS
    out["retired"] = _retire_old_versions(cfg, keep=max_versions)

    if persist and out["applied"]:
        for name in out["deployed"]:
            _set_cfg_value(name, cfg[name])
        _set_cfg_value(DEPLOY_REGISTRY_KEY, cfg[DEPLOY_REGISTRY_KEY])
        if tb is not None:
            _set_cfg_value(tb.BANDIT_KEY, cfg.get(tb.BANDIT_KEY, {}))
    return out


def _retire_old_versions(cfg: Dict[str, Any], *, keep: int) -> List[str]:
    """현재 살아있는 버전 중 최신 `keep` 개만 남기고 오래된 버전 템플릿을 제거."""
    reg = _registry(cfg)
    versions = sorted({int(m.get("version", 0)) for m in reg.values() if isinstance(m, dict)})
    if len(versions) <= max(1, keep):
        return []
    drop = set(versions[: len(versions) - keep])
    retired: List[str] = []
    for name, meta in list(reg.items()):
        if not isinstance(meta, dict):
            continue
        if int(meta.get("version", 0)) in drop:
            cfg.pop(name, None)              # 라이브 ideal_templates 에서 제거
            reg.pop(name, None)
            try:
                import template_bandit as tb

                st = cfg.get(tb.BANDIT_KEY)
                if isinstance(st, dict):
                    st.pop(name, None)
            except Exception:
                pass
            retired.append(name)
    cfg[DEPLOY_REGISTRY_KEY] = reg
    return retired


def ensure_deep_tag(sig_type: Any, cfg: Optional[Dict[str, Any]] = None) -> str:
    """진입 sig 가 배포된 deep 템플릿을 참조하면 해당 버전 태그를 강제 부착(이중 안전)."""
    sig = str(sig_type or "")
    if DEEP_EVOLVED_MARKER in sig:
        return sig
    reg = (cfg or {}).get(DEPLOY_REGISTRY_KEY) if isinstance(cfg, dict) else None
    if isinstance(reg, dict):
        for name, meta in reg.items():
            if name and name in sig:
                tag = meta.get("tag", DEEP_EVOLVED_MARKER) if isinstance(meta, dict) else DEEP_EVOLVED_MARKER
                return f"{sig} [{tag}]"
    return sig


# ---------------------------------------------------------------------------
# 영향도 추적 (Impact Attribution) — 전체/시장별/템플릿별
# ---------------------------------------------------------------------------
def _impact_from_conn(
    conn: sqlite3.Connection,
    *,
    market: Optional[str] = None,
    like: str = DEEP_EVOLVED_MARKER,
) -> Dict[str, Any]:
    where = ["status LIKE 'CLOSED%'", "IFNULL(sig_type,'') LIKE ?"]
    params: List[Any] = [f"%{like}%"]
    if market:
        where.append("market = ?")
        params.append(str(market).upper())
    sql = "SELECT final_ret, mfe FROM forward_trades WHERE " + " AND ".join(where)
    rows = conn.execute(sql, params).fetchall()
    n = len(rows)
    if n == 0:
        return {"n": 0, "wins": 0, "wr": 0.0, "avg_mfe": 0.0, "max_mfe": 0.0,
                "cum_pnl": 0.0, "profit_factor": 0.0}
    rets = [float(r[0]) for r in rows if r[0] is not None]
    mfes = [float(r[1]) for r in rows if r[1] is not None]
    wins = sum(1 for r in rets if r > 0)
    gross_win = sum(r for r in rets if r > 0)
    gross_loss = abs(sum(r for r in rets if r <= 0))
    pf = gross_win / (gross_loss + 1e-9) if gross_loss > 0 else (gross_win if gross_win > 0 else 0.0)
    return {
        "n": n, "wins": wins, "wr": round(wins / n, 4),
        "avg_mfe": round(sum(mfes) / len(mfes), 4) if mfes else 0.0,
        "max_mfe": round(max(mfes), 4) if mfes else 0.0,
        "cum_pnl": round(sum(rets), 4), "profit_factor": round(pf, 4),
    }


def track_deep_evolution_impact(
    *,
    db_path: Optional[str] = None,
    markets: Tuple[str, ...] = ("KR", "US"),
    cfg: Optional[Dict[str, Any]] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """
    `🧬DEEP_EVOLVED` 태그 청산 거래만 골라 전체/시장별/템플릿(버전)별 WR·MFE·PnL·PF 를 독립 계측.
    cfg(레지스트리)가 있으면 배포된 각 신형 템플릿별로도 분해한다.
    """
    if db_path is None:
        try:
            from market_db_paths import MARKET_DATA_DB_PATH

            db_path = MARKET_DATA_DB_PATH
        except Exception:
            db_path = None

    empty = {"n": 0, "wins": 0, "wr": 0.0, "avg_mfe": 0.0, "max_mfe": 0.0,
             "cum_pnl": 0.0, "profit_factor": 0.0}
    result: Dict[str, Any] = {
        "tag": DEEP_EVOLVED_MARKER,
        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "by_market": {}, "by_template": {}, "total": dict(empty),
    }
    if not db_path:
        result["error"] = "no_db_path"
        return result

    try:
        uri = str(db_path).replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=20)
    except sqlite3.Error as ex:
        result["error"] = str(ex)
        return result
    try:
        try:
            for mk in markets:
                result["by_market"][str(mk).upper()] = _impact_from_conn(conn, market=mk)
            result["total"] = _impact_from_conn(conn)
            reg = (cfg or {}).get(DEPLOY_REGISTRY_KEY) if isinstance(cfg, dict) else None
            if isinstance(reg, dict):
                for name in reg.keys():
                    result["by_template"][name] = _impact_from_conn(conn, like=name)
        except sqlite3.Error as ex:
            result["error"] = str(ex)
    finally:
        conn.close()

    if persist:
        _set_cfg_value(IMPACT_KEY, result)
    return result
