"""
Mega-Trend Toxic Graveyard Kill-Switch (내부 2번).

MEGA_TREND 언락 섹터에서 연속 손실 → Toxic BBox(ANTI_PATTERNS) 자체 생성·감지 시:
  · Correlation Forgiveness 즉시 박탈
  · defensive_exit_mode 유체 청산(Fluid Scale-out)

연동: clustered_immune_vaccine · toxic_graveyard_analyzer · exit_dynamics
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from exit_dynamics import fluid_scale_out_fraction
from mega_trend_climax import (
    MEGA_TREND_CLIMAX_EXIT_TAG,
    _deactivate_mega_trend_state,
    liquidate_mega_trend_sector_positions,
)
from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    load_mega_trend_state,
    mega_trend_unlock_enabled,
    resolve_kr_code_sector,
)

MEGA_TREND_TOXIC_WATCH_KEY = "toxic_watch"
FORGIVENESS_REVOKED_KEY = "correlation_forgiveness_revoked"


def toxic_kill_config(sector: Optional[str] = None) -> Dict[str, Any]:
    def _i(key: str, default: int) -> int:
        try:
            return int(os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    base = {
        "consecutive_loss_min": _i("MEGA_TREND_TOXIC_CONSEC_LOSS_MIN", 3),
        "defensive_scale_out_min": float(
            os.environ.get("MEGA_TREND_TOXIC_DEFENSIVE_SCALE_MIN", "0.82")
        ),
        "scan_lookback_days": _i("MEGA_TREND_TOXIC_SCAN_LOOKBACK_DAYS", 14),
    }
    try:
        from mega_trend_kill_rl import apply_kill_rl_toxic_adjustments, load_kill_rl_state

        return apply_kill_rl_toxic_adjustments(
            base, rl_state=load_kill_rl_state(), sector=sector
        )
    except Exception:
        return base


def _map_sector_label(sector: object) -> str:
    from sector_taxonomy import map_standard_sector

    return map_standard_sector(sector, market="KR")


def _iter_anti_pattern_rules(
    config: Mapping[str, Any],
) -> List[Tuple[str, Dict[str, Any]]]:
    """ANTI_PATTERNS + TOXIC_ML rules → [(key, rule_dict)]."""
    out: List[Tuple[str, Dict[str, Any]]] = []
    ap = config.get("ANTI_PATTERNS")
    if isinstance(ap, dict):
        for k, v in ap.items():
            if isinstance(v, dict):
                out.append((str(k), v))
    elif isinstance(ap, list):
        for i, v in enumerate(ap):
            if isinstance(v, dict):
                out.append((f"_idx_{i}", v))

    ml = config.get("TOXIC_ML_ANTIPATTERNS")
    if isinstance(ml, dict):
        rules = ml.get("rules")
        if isinstance(rules, dict):
            for k, v in rules.items():
                if isinstance(v, dict):
                    out.append((f"ML_{k}", v))
    return out


def _rule_sector(rule: Mapping[str, Any]) -> Optional[str]:
    """toxic bbox 의 sector_match 또는 mega_trend_sector."""
    if not isinstance(rule, Mapping):
        return None
    for key in ("mega_trend_sector", "sector_match", "sector"):
        raw = rule.get(key)
        if raw is not None and str(raw).strip():
            return _map_sector_label(raw)
    return None


def _rule_created_after(rule: Mapping[str, Any], since: Optional[str]) -> bool:
    if not since:
        return True
    created = str(rule.get("created_at") or "")[:10]
    if not created:
        return True
    return created >= str(since)[:10]


def _is_toxic_bbox_rule(rule: Mapping[str, Any]) -> bool:
    """독성 클러스터 bbox — DNA min/max 또는 TOXIC/IMMUNE/CLUSTERED 소스."""
    if not isinstance(rule, Mapping):
        return False
    src = str(rule.get("source") or "").upper()
    if any(
        tag in src
        for tag in ("TOXIC", "IMMUNE", "CLUSTERED", "DEEP_EVOLVED", "GRAVEYARD")
    ):
        return True
    if str(rule.get("label") or "").upper().startswith(("TOXIC", "VACCINE", "IMMUNE")):
        return True
    keys = set(str(k) for k in rule.keys())
    return bool(keys & {"dyn_cpv_min", "dyn_cpv_max", "v_energy_min", "dyn_tb_min"})


def scan_sector_toxic_bbox_signals(
    config: Mapping[str, Any],
    sector: str,
    *,
    ignited_at: Optional[str] = None,
    known_keys: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    ANTI_PATTERNS / TOXIC_ML 에서 해당 섹터 Toxic BBox 신규·기존 스캔.
    """
    target = _map_sector_label(sector)
    known = {str(k) for k in (known_keys or [])}
    matched: List[Dict[str, Any]] = []
    new_keys: List[str] = []

    for key, rule in _iter_anti_pattern_rules(config):
        if not _is_toxic_bbox_rule(rule):
            continue
        rule_sec = _rule_sector(rule)
        if rule_sec and rule_sec != target:
            continue
        if rule.get("mega_trend_unlock") and rule_sec != target:
            continue
        if rule_sec is None and not rule.get("mega_trend_unlock"):
            continue
        if not _rule_created_after(rule, ignited_at):
            continue
        matched.append({"key": key, "rule": dict(rule)})
        if key not in known:
            new_keys.append(key)

    fade_hit = False
    fade_detail: Optional[Dict[str, Any]] = None
    fade_targets = config.get("TOXIC_FADE_TARGETS")
    if isinstance(fade_targets, dict):
        for _gk, info in fade_targets.items():
            if not isinstance(info, dict):
                continue
            fs = _map_sector_label(info.get("sector"))
            if fs == target:
                fade_hit = True
                fade_detail = {"group": _gk, **info}
                break

    toxic_signal = bool(new_keys) or fade_hit
    reason_parts: List[str] = []
    if new_keys:
        reason_parts.append(f"new_toxic_bbox:{','.join(new_keys[:3])}")
    if fade_hit and fade_detail:
        reason_parts.append(
            f"toxic_fade_target:{fade_detail.get('group')} "
            f"wr={fade_detail.get('win_rate')}"
        )

    return {
        "sector": target,
        "toxic_signal": toxic_signal,
        "new_rule_keys": new_keys,
        "matched_rules": matched,
        "toxic_fade_hit": fade_hit,
        "toxic_fade_detail": fade_detail,
        "reason": " | ".join(reason_parts) if reason_parts else "no_toxic_signal",
    }


def detect_consecutive_loss_streak(
    trades: Sequence[Mapping[str, Any]],
    *,
    min_streak: int = 3,
) -> Dict[str, Any]:
    """최근 청산부터 연속 손실 스트릭."""
    closed = [
        t
        for t in reversed(list(trades or []))
        if "CLOSED" in str(t.get("status") or "").upper()
    ]
    streak = 0
    last_loss: Optional[Dict[str, Any]] = None
    for t in closed:
        try:
            ret = float(t.get("final_ret") if t.get("final_ret") is not None else t.get("sim_stat_ret") or 0)
        except (TypeError, ValueError):
            ret = 0.0
        if ret < 0:
            streak += 1
            if last_loss is None:
                last_loss = dict(t)
        else:
            break

    return {
        "streak": streak,
        "triggered": streak >= int(min_streak),
        "last_loss_trade": last_loss,
    }


def register_mega_trend_sector_toxic_bbox(
    config: Dict[str, Any],
    sector: str,
    trade: Mapping[str, Any],
    *,
    streak: int = 0,
) -> Dict[str, Any]:
    """
    연속 손실 체결 DNA → sector_match Toxic BBox 등록 (clustered_immune_vaccine).
    """
    from clustered_immune_vaccine import register_failed_template

    try:
        cpv = float(trade.get("dyn_cpv") or 0)
        tb = float(trade.get("dyn_tb") or 0)
        bbe = float(trade.get("v_energy") or 0)
    except (TypeError, ValueError):
        return {"registered": False, "reason": "invalid_dna"}

    if cpv == 0 and tb == 0 and bbe == 0:
        return {"registered": False, "reason": "zero_dna"}

    sec = _map_sector_label(sector)
    label = f"MEGA_TREND_{sec}_L{streak}"
    reg = register_failed_template(
        config,
        name=label,
        dna=[cpv, tb, bbe],
        market="KR",
        win_rate=0.0,
    )
    if not reg.get("registered"):
        return reg

    ap = config.get("ANTI_PATTERNS")
    key_hint = f"IMMUNE_{label}"
    annotated = False
    if isinstance(ap, dict):
        for k, v in list(ap.items()):
            if isinstance(v, dict) and str(v.get("label") or "") == label:
                v["sector_match"] = sec
                v["mega_trend_sector"] = sec
                v["mega_trend_unlock"] = True
                v["consecutive_loss_streak"] = int(streak)
                v["source"] = "MEGA_TREND_TOXIC_CLUSTER"
                ap[k] = v
                key_hint = k
                annotated = True
                break
    if not annotated:
        if not isinstance(ap, dict):
            ap = {}
        ap[key_hint] = {
            "label": label,
            "sector_match": sec,
            "mega_trend_sector": sec,
            "mega_trend_unlock": True,
            "consecutive_loss_streak": int(streak),
            "source": "MEGA_TREND_TOXIC_CLUSTER",
            "dyn_cpv_min": max(0.0, cpv - 0.05),
            "dyn_cpv_max": min(1.0, cpv + 0.05),
            "dyn_tb_min": max(0.0, tb - 1.0),
            "dyn_tb_max": tb + 1.0,
            "v_energy_min": max(0.0, bbe - 2.0),
            "v_energy_max": bbe + 2.0,
            "created_at": datetime.now().strftime("%Y-%m-%d"),
        }
    config["ANTI_PATTERNS"] = ap
    return {
        "registered": True,
        "rule_key": key_hint,
        "sector": sec,
        "prune": reg.get("prune"),
    }


def revoke_mega_trend_correlation_forgiveness(
    state: Dict[str, Any],
    *,
    reason: str = "",
) -> Dict[str, Any]:
    """MEGA_TREND 면죄부·ROTATION 가산 즉시 박탈."""
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state[FORGIVENESS_REVOKED_KEY] = True
    state["rotation_advantage_active"] = False
    state["forgiveness_revoked_at"] = now_s
    state["forgiveness_revoked_reason"] = reason or "toxic_graveyard_kill"
    return state


def is_mega_trend_forgiveness_revoked(
    config: Optional[Mapping[str, Any]] = None,
) -> bool:
    state = load_mega_trend_state(config)
    return bool(state.get(FORGIVENESS_REVOKED_KEY))


def resolve_defensive_exit_fraction() -> float:
    """exit_dynamics M1 — 방어적 유체 청산 비율."""
    cfg = toxic_kill_config()
    base = float(
        fluid_scale_out_fraction("BEAR", volatility_pct=12.0, edge_score=0.25)
    )
    return max(base, float(cfg["defensive_scale_out_min"]))


def evaluate_mega_trend_toxic_kill(
    config: Mapping[str, Any],
    conn: Optional[sqlite3.Connection] = None,
    *,
    auto_register_bbox: bool = True,
) -> Dict[str, Any]:
    """
    [2번] Toxic Graveyard 연동 킬스위치 판정 — 내부 장부·ANTI_PATTERNS 만 사용.
    """
    state = load_mega_trend_state(config)
    if not state.get("active"):
        return {"kill": False, "reason": "mega_trend_inactive"}

    if state.get(FORGIVENESS_REVOKED_KEY) and not state.get("active"):
        return {"kill": False, "reason": "already_revoked_inactive"}

    sectors = list(state.get("sectors") or [])
    primary = state.get("primary_sector")
    if primary and str(primary) not in [str(s) for s in sectors]:
        sectors.insert(0, str(primary))

    ignited_at = state.get("ignited_at")
    watch = state.get(MEGA_TREND_TOXIC_WATCH_KEY) or {}
    known_keys = list(watch.get("known_rule_keys") or [])
    cfg = toxic_kill_config()
    known_keys = list(watch.get("known_rule_keys") or [])

    from mega_trend_internal_monitor import fetch_mega_trend_sector_trades

    own = conn is None
    c = conn
    if c is None:
        try:
            import auto_forward_tester as aft

            c = sqlite3.connect(aft.DB_PATH, timeout=30)
            own = True
        except Exception:
            c = None

    sector_verdicts: List[Dict[str, Any]] = []
    kill_sector: Optional[str] = None
    kill_verdict: Optional[Dict[str, Any]] = None
    registered_keys: List[str] = []

    try:
        for sec in sectors:
            sec_s = str(sec)
            cfg = toxic_kill_config(sector=sec_s)
            trades: List[Dict[str, Any]] = []
            if c is not None:
                trades = fetch_mega_trend_sector_trades_extended(
                    c, sec_s, ignited_at=ignited_at, conn_fetch=fetch_mega_trend_sector_trades
                )

            streak_info = detect_consecutive_loss_streak(
                trades, min_streak=int(cfg["consecutive_loss_min"])
            )
            reg_info: Dict[str, Any] = {"registered": False}
            if (
                auto_register_bbox
                and streak_info.get("triggered")
                and streak_info.get("last_loss_trade")
                and isinstance(config, dict)
            ):
                reg_info = register_mega_trend_sector_toxic_bbox(
                    config,
                    sec_s,
                    streak_info["last_loss_trade"],
                    streak=int(streak_info.get("streak") or 0),
                )
                if reg_info.get("registered") and reg_info.get("rule_key"):
                    registered_keys.append(str(reg_info["rule_key"]))
                    known_keys.append(str(reg_info["rule_key"]))

            scan = scan_sector_toxic_bbox_signals(
                config,
                sec_s,
                ignited_at=ignited_at,
                known_keys=known_keys,
            )
            triggered = bool(scan.get("toxic_signal")) or bool(
                reg_info.get("registered")
            )
            verdict = {
                "sector": sec_s,
                "kill": triggered,
                "exit_mode": "defensive_exit",
                "streak": streak_info,
                "registration": reg_info,
                "toxic_scan": scan,
                "reason": scan.get("reason")
                if scan.get("toxic_signal")
                else (
                    f"consecutive_loss_streak_{streak_info.get('streak')}"
                    if streak_info.get("triggered")
                    else "no_toxic_signal"
                ),
            }
            sector_verdicts.append(verdict)
            if triggered and kill_verdict is None:
                kill_verdict = verdict
                kill_sector = sec_s
    finally:
        if own and c is not None:
            try:
                c.close()
            except Exception:
                pass

    if not kill_verdict:
        return {
            "kill": False,
            "reason": "no_toxic_signal",
            "sector_verdicts": sector_verdicts,
            "registered_keys": registered_keys,
        }

    all_affected = [v["sector"] for v in sector_verdicts if v.get("kill")]
    return {
        "kill": True,
        "exit_mode": "defensive_exit",
        "sector": kill_sector,
        "sectors": all_affected or [kill_sector],
        "reason": f"toxic_graveyard: {kill_verdict.get('reason', '')}",
        "sector_verdicts": sector_verdicts,
        "registered_keys": registered_keys,
        "toxic_scan": kill_verdict.get("toxic_scan"),
        "streak": kill_verdict.get("streak"),
    }


def fetch_mega_trend_sector_trades_extended(
    conn: sqlite3.Connection,
    sector: str,
    *,
    ignited_at: Optional[str] = None,
    conn_fetch: Callable[..., List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """fetch_mega_trend_sector_trades + DNA 필드 보강."""
    base = conn_fetch(conn, sector, ignited_at=ignited_at)
    if not base:
        return base
    ids = [int(t["id"]) for t in base if t.get("id") is not None]
    if not ids:
        return base
    placeholders = ",".join("?" * len(ids))
    try:
        rows = conn.execute(
            f"""
            SELECT id, dyn_cpv, dyn_tb, v_energy, dyn_rs
            FROM forward_trades WHERE id IN ({placeholders})
            """,
            ids,
        ).fetchall()
    except Exception:
        return base
    dna_map = {int(r[0]): {"dyn_cpv": r[1], "dyn_tb": r[2], "v_energy": r[3], "dyn_rs": r[4]} for r in rows}
    out = []
    for t in base:
        merged = dict(t)
        merged.update(dna_map.get(int(t.get("id") or 0), {}))
        out.append(merged)
    return out


def refresh_mega_trend_toxic_graveyard_kill(
    config: Dict[str, Any],
    *,
    save_config_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """
    [2번] Toxic Graveyard 킬스위치 — 면죄부 박탈 + defensive_exit 청산 + 언락 해제.
    """
    if not mega_trend_unlock_enabled():
        return {"kill": False, "reason": "disabled"}

    state = load_mega_trend_state(config)
    if not state.get("active"):
        return {"kill": False, "reason": "mega_trend_inactive"}

    own_conn = False
    c = conn
    if c is None:
        try:
            import auto_forward_tester as aft

            c = sqlite3.connect(aft.DB_PATH, timeout=30)
            own_conn = True
        except Exception:
            c = None

    try:
        verdict = evaluate_mega_trend_toxic_kill(config, c)
        watch = dict(state.get(MEGA_TREND_TOXIC_WATCH_KEY) or {})
        all_keys = list(watch.get("known_rule_keys") or [])
        for k in verdict.get("registered_keys") or []:
            if k not in all_keys:
                all_keys.append(k)
        for v in verdict.get("sector_verdicts") or []:
            scan = v.get("toxic_scan") or {}
            for k in scan.get("new_rule_keys") or []:
                if k not in all_keys:
                    all_keys.append(k)
            for m in scan.get("matched_rules") or []:
                mk = m.get("key")
                if mk and mk not in all_keys:
                    all_keys.append(str(mk))

        watch["known_rule_keys"] = all_keys[-200:]
        watch["last_scan_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state = dict(load_mega_trend_state(config))
        state[MEGA_TREND_TOXIC_WATCH_KEY] = watch

        if not verdict.get("kill"):
            config[MEGA_TREND_CONFIG_KEY] = state
            if save_config_fn:
                save_config_fn(config)
            return verdict

        state = revoke_mega_trend_correlation_forgiveness(
            state, reason=str(verdict.get("reason") or "")
        )
        kill_ignited_at = state.get("ignited_at")
        state["toxic_kill_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state["toxic_kill_reason"] = verdict.get("reason")
        state = _deactivate_mega_trend_state(state, verdict)
        config[MEGA_TREND_CONFIG_KEY] = state

        frac = resolve_defensive_exit_fraction()
        liq: Dict[str, Any] = {"liquidated": 0, "scaled": 0}
        if c is not None:
            liq = liquidate_mega_trend_sector_positions(
                c,
                verdict.get("sectors") or [verdict.get("sector")],
                exit_mode="defensive_exit",
                exit_reason=(
                    f"{MEGA_TREND_CLIMAX_EXIT_TAG}_TOXIC: {verdict.get('reason', '')}"
                ),
            )
            liq["defensive_exit_fraction"] = frac

        if save_config_fn:
            save_config_fn(config)

        try:
            from mega_trend_kill_rl import record_mega_trend_kill_event

            record_mega_trend_kill_event(
                config,
                sector=str(verdict.get("sector") or ""),
                kill_type="toxic_graveyard",
                reason=str(verdict.get("reason") or ""),
                exit_mode=str(verdict.get("exit_mode") or "defensive_exit"),
                ignited_at=str(kill_ignited_at or "") or None,
                snapshot={
                    "sectors": verdict.get("sectors"),
                    "streak": (verdict.get("streak") or {}).get("streak"),
                    "liquidation": liq,
                    "ignited_at": kill_ignited_at,
                },
            )
            if save_config_fn:
                save_config_fn(config)
        except Exception:
            pass

        print(
            f"☠️ [Mega-Trend Toxic Kill] {verdict.get('sector')} — "
            f"{verdict.get('reason')} | "
            f"면죄부박탈 · defensive_exit scale={frac:.0%} · "
            f"청산={liq.get('liquidated', 0)} scaled={liq.get('scaled', 0)}"
        )
        verdict["liquidation"] = liq
        verdict["state"] = state
        verdict["forgiveness_revoked"] = True
        return verdict
    finally:
        if own_conn and c is not None:
            try:
                c.close()
            except Exception:
                pass
