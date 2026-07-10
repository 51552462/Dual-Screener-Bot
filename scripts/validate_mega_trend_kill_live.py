#!/usr/bin/env python3
"""
Mega-Trend Kill-Switch 실전 검증 (P0~P6).

읽기 전용 + dry-run evolve 기본. --persist-evolve 시 RL 상태 영속화.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

# repo root on path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from config_manager import CONFIG_DB_PATH, load_system_config
from factory_data_paths import factory_data_dir
from market_db_paths import MARKET_DATA_DB_PATH, market_db_read_path
from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    assess_toxic_kill_cooldown,
    load_mega_trend_state,
    mega_trend_unlock_enabled,
)
from mega_trend_kill_rl import (
    MEGA_TREND_CONTAMINATION_AUDIT_KEY,
    MEGA_TREND_KILL_RL_STATE_KEY,
    MEGA_TREND_SECTOR_DELTAS_KEY,
    MEGA_TREND_SECTOR_QUARANTINE_KEY,
    evolve_mega_trend_kill_sensitivity,
    kill_rl_config,
    load_kill_rl_state,
    sector_guard_enabled,
)
from reports.mega_trend_kill_report_section import (
    build_mega_trend_kill_report_block,
    build_mega_trend_kill_weekly_appendix,
    format_mega_trend_kill_daily_html,
)


def _safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", errors="backslashreplace").decode("ascii"))


def _discover_forward_db() -> str:
    try:
        from auto_forward_tester import DB_PATH

        return str(DB_PATH)
    except Exception:
        return market_db_read_path()


def _section(title: str) -> None:
    _safe_print(f"\n{'=' * 60}")
    _safe_print(title)
    _safe_print("=" * 60)


def _forward_trade_stats(db_path: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"db": db_path, "exists": os.path.isfile(db_path)}
    if not out["exists"]:
        return out
    conn = sqlite3.connect(db_path)
    try:
        out["kr_total"] = conn.execute(
            "SELECT COUNT(*) FROM forward_trades WHERE market='KR'"
        ).fetchone()[0]
        out["kr_open"] = conn.execute(
            "SELECT COUNT(*) FROM forward_trades WHERE market='KR' AND status='OPEN'"
        ).fetchone()[0]
        out["kr_megatrend"] = conn.execute(
            """
            SELECT COUNT(*) FROM forward_trades
            WHERE market='KR' AND (
                sig_type LIKE '%MegaTrend%'
                OR sig_type LIKE '%MEGA_TREND%'
                OR sig_type LIKE '%순환매%'
            )
            """
        ).fetchone()[0]
        out["recent"] = [
            {
                "entry": (r[0] or "")[:10],
                "sector": r[1],
                "sig_type": r[2],
                "status": r[3],
                "ret": r[4],
            }
            for r in conn.execute(
                """
                SELECT entry_date, sector, sig_type, status, sim_stat_ret
                FROM forward_trades
                WHERE market='KR'
                ORDER BY entry_date DESC
                LIMIT 8
                """
            ).fetchall()
        ]
    except Exception as ex:
        out["error"] = str(ex)
    finally:
        conn.close()
    return out


def _print_mega_trend_state(cfg: Mapping[str, Any]) -> None:
    state = load_mega_trend_state(cfg)
    cooldown = assess_toxic_kill_cooldown(state)
    print(f"unlock_enabled: {mega_trend_unlock_enabled()}")
    print(f"active: {state.get('active')}")
    print(f"primary_sector: {state.get('primary_sector')}")
    print(f"sectors: {state.get('sectors')}")
    print(f"ignited_at: {state.get('ignited_at')}")
    print(f"cooldown: {cooldown}")
    for k in (
        "internal_momentum_kill_at",
        "toxic_kill_at",
        "climax_kill_at",
        "correlation_forgiveness_revoked",
    ):
        if state.get(k) is not None:
            print(f"{k}: {state.get(k)}")
    diag = state.get("internal_diagnostics") or {}
    if diag:
        print(
            f"internal_diagnostics: any_lost={diag.get('any_momentum_lost')} "
            f"sectors={diag.get('momentum_lost_sectors')}"
        )


def _print_rl_state(cfg: Mapping[str, Any]) -> None:
    rl = load_kill_rl_state(cfg)
    cfg_rl = kill_rl_config()
    print(f"sector_guard: {sector_guard_enabled()}")
    print(f"rl_config: min_events={cfg_rl['min_events_to_update']} "
          f"min_sector={cfg_rl['min_sector_events']} "
          f"min_purity={cfg_rl['min_sector_purity']}")
    for k in (
        "win_rate_min_delta",
        "flow_reversal_z_delta",
        "consecutive_loss_delta",
        "events_pending",
        "events_evaluated",
        "updated_at",
    ):
        print(f"{k}: {rl.get(k)}")
    events = list(rl.get("kill_events") or [])
    print(f"kill_events: {len(events)}")
    for ev in events[-10:]:
        print(
            f"  · {str(ev.get('kill_at', ''))[:10]} "
            f"{ev.get('sector_std') or ev.get('sector')} "
            f"{ev.get('kill_type')} → {ev.get('outcome')} "
            f"purity={ev.get('sector_purity')} flag={ev.get('contamination_flag')}"
        )
    overlays = rl.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}
    print(f"sector_deltas keys: {list(overlays.keys())}")
    for sec, block in list(overlays.items())[:5]:
        if isinstance(block, Mapping):
            print(
                f"  · {sec}: WRΔ={block.get('win_rate_min_delta')} "
                f"int_n={block.get('internal_n')} clx_n={block.get('climax_n')} "
                f"purity={block.get('avg_purity')} ign={block.get('bound_ignited_at')}"
            )
    quarantine = rl.get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {}
    print(f"sector_quarantine: {list(quarantine.keys())}")
    audit = rl.get(MEGA_TREND_CONTAMINATION_AUDIT_KEY) or []
    print(f"contamination_audit entries: {len(audit)}")
    summary = rl.get("last_evolve_summary") or {}
    if summary:
        print("last_evolve_summary:")
        print(json.dumps(summary, ensure_ascii=False, indent=2))


def _run_evolve(
    cfg: Dict[str, Any],
    *,
    db_path: str,
    persist: bool,
) -> Dict[str, Any]:
    return evolve_mega_trend_kill_sensitivity(
        cfg,
        db_path=db_path,
        persist=persist,
    )


def _print_report_preview(cfg: Mapping[str, Any], evolve_result: Optional[Mapping[str, Any]]) -> None:
    block = build_mega_trend_kill_report_block(cfg)
    daily = format_mega_trend_kill_daily_html(block)
    weekly = build_mega_trend_kill_weekly_appendix(cfg, evolve_result=evolve_result)
    _safe_print("\n--- Daily HTML preview (first 1200 chars) ---")
    _safe_print((daily or "(empty - unlock disabled or no block)")[:1200])
    _safe_print("\n--- Weekly appendix preview (first 1200 chars) ---")
    _safe_print((weekly or "(empty)")[:1200])


def _init_forward_schema(db_path: str) -> bool:
    try:
        from auto_forward_tester import init_forward_db

        init_forward_db(db_path)
        return True
    except Exception as ex:
        _safe_print(f"init_forward_db failed: {ex}")
        return False


def _run_smoke_pipeline() -> Dict[str, Any]:
    """P0~P6 end-to-end smoke — 프로덕션 config 미변경(in-memory only)."""
    from datetime import timedelta

    from mega_trend_kill_rl import (
        KILL_TYPE_CLIMAX,
        KILL_TYPE_INTERNAL_MOMENTUM,
        CONTAMINATION_FLAG_HIGH,
        CONTAMINATION_FLAG_OK,
        record_mega_trend_kill_event,
    )

    sector = "반도체/IT"
    recent = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    old = (datetime.now() - timedelta(days=40)).strftime("%Y-%m-%d")

    cfg: Dict[str, Any] = {
        MEGA_TREND_CONFIG_KEY: {
            "active": True,
            "primary_sector": sector,
            "sectors": [sector],
            "ignited_at": "2026-02-01",
        },
        MEGA_TREND_KILL_RL_STATE_KEY: {
            "kill_events": [
                {
                    "sector": sector,
                    "sector_std": sector,
                    "kill_at": recent,
                    "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                    "kill_lane": "internal",
                    "outcome": "opportunity_cost",
                    "sector_purity": 0.90,
                    "contamination_flag": CONTAMINATION_FLAG_OK,
                    "ignited_at": "2026-02-01",
                },
                {
                    "sector": sector,
                    "sector_std": sector,
                    "kill_at": recent,
                    "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                    "kill_lane": "internal",
                    "outcome": "defense_success",
                    "sector_purity": 0.92,
                    "contamination_flag": CONTAMINATION_FLAG_OK,
                    "ignited_at": "2026-02-01",
                },
                {
                    "sector": sector,
                    "sector_std": sector,
                    "kill_at": recent,
                    "kill_type": KILL_TYPE_CLIMAX,
                    "kill_lane": "external",
                    "outcome": "opportunity_cost",
                    "sector_purity": 0.88,
                    "contamination_flag": CONTAMINATION_FLAG_OK,
                    "ignited_at": "2026-02-01",
                },
                {
                    "sector": sector,
                    "sector_std": sector,
                    "kill_at": old,
                    "kill_type": KILL_TYPE_INTERNAL_MOMENTUM,
                    "outcome": "defense_success",
                    "sector_purity": 0.15,
                    "contamination_flag": CONTAMINATION_FLAG_HIGH,
                },
            ]
        },
    }

    record_mega_trend_kill_event(
        cfg,
        sector="반도체",
        kill_type=KILL_TYPE_INTERNAL_MOMENTUM,
        reason="smoke_test_record",
        ignited_at="2026-02-01",
        snapshot={"sectors": [sector]},
    )

    evolve_out = evolve_mega_trend_kill_sensitivity(cfg, db_path=None, persist=False)
    block = build_mega_trend_kill_report_block(cfg)
    daily_ok = bool(format_mega_trend_kill_daily_html(block))
    weekly_ok = bool(build_mega_trend_kill_weekly_appendix(cfg, evolve_result=evolve_out))

    recorded = cfg[MEGA_TREND_KILL_RL_STATE_KEY]["kill_events"][-1]
    st = evolve_out.get("state") or {}
    overlays = st.get(MEGA_TREND_SECTOR_DELTAS_KEY) or {}
    quarantine = st.get(MEGA_TREND_SECTOR_QUARANTINE_KEY) or {}

    return {
        "record_sector_std": recorded.get("sector_std"),
        "evolve_updated": evolve_out.get("updated"),
        "sectors_updated": evolve_out.get("sectors_updated"),
        "sectors_quarantined": evolve_out.get("sectors_quarantined"),
        "overlay_keys": list(overlays.keys()),
        "quarantine_keys": list(quarantine.keys()),
        "daily_report_ok": daily_ok,
        "weekly_report_ok": weekly_ok,
        "contamination_guard": (st.get("last_evolve_summary") or {}).get("contamination_guard"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Mega-Trend Kill-Switch live validation")
    parser.add_argument(
        "--persist-evolve",
        action="store_true",
        help="주말 RL evolve 결과를 system_config에 영속화 (기본: dry-run)",
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="forward DB 경로 (기본: market_db_read_path)",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="forward_trades 스키마 없으면 init_forward_db 실행",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="P0~P6 in-memory smoke (프로덕션 config 미변경)",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="검증 결과 JSON 저장 경로",
    )
    args = parser.parse_args()

    db_path = args.db_path.strip() or _discover_forward_db()
    report: Dict[str, Any] = {"at": datetime.now().isoformat(timespec="seconds")}

    _section("1. 환경·경로")
    print("factory_data_dir:", factory_data_dir())
    print("CONFIG_DB:", CONFIG_DB_PATH, "exists:", os.path.isfile(CONFIG_DB_PATH))
    print("MARKET_DB:", MARKET_DATA_DB_PATH, "exists:", os.path.isfile(MARKET_DATA_DB_PATH))
    print("READ_DB:", db_path, "exists:", os.path.isfile(db_path))
    print("ENABLE_MEGA_TREND_UNLOCK:", os.environ.get("ENABLE_MEGA_TREND_UNLOCK", "(unset)"))
    print("MEGA_TREND_KILL_RL_SECTOR_GUARD:", os.environ.get("MEGA_TREND_KILL_RL_SECTOR_GUARD", "(default 1)"))

    cfg = load_system_config()
    if not isinstance(cfg, dict):
        cfg = {}

    _section("2. Mega-Trend 언락 상태")
    _print_mega_trend_state(cfg)

    _section("3. Kill RL 상태 (P3~P6)")
    _print_rl_state(cfg)

    _section("4. forward_trades 장부")
    stats = _forward_trade_stats(db_path)
    if stats.get("error") == "no such table: forward_trades" and args.init_db:
        _safe_print("forward_trades 없음 -> init_forward_db 실행")
        if _init_forward_schema(db_path):
            stats = _forward_trade_stats(db_path)
    report["forward_stats"] = stats
    _safe_print(json.dumps(stats, ensure_ascii=False, indent=2))

    _section("5. 주말 RL evolve (dry-run)" if not args.persist_evolve else "5. 주말 RL evolve (PERSIST)")
    if not os.path.isfile(db_path):
        print("SKIP: DB 없음")
        evolve_out: Dict[str, Any] = {"updated": False, "reason": "no_db"}
    else:
        evolve_out = _run_evolve(dict(cfg), db_path=db_path, persist=args.persist_evolve)
        report["evolve"] = {
            "updated": evolve_out.get("updated"),
            "reason": evolve_out.get("reason"),
            "lanes_updated": evolve_out.get("lanes_updated"),
            "sectors_updated": evolve_out.get("sectors_updated"),
            "sectors_quarantined": evolve_out.get("sectors_quarantined"),
            "last_evolve_summary": (evolve_out.get("state") or {}).get("last_evolve_summary"),
        }
        _safe_print(json.dumps(report["evolve"], ensure_ascii=False, indent=2))

    _section("6. 리포트/Telegram 블록 미리보기")
    _print_report_preview(cfg, evolve_out if isinstance(evolve_out, dict) else None)

    if args.smoke:
        _section("6b. P0~P6 Smoke (in-memory)")
        smoke = _run_smoke_pipeline()
        report["smoke"] = smoke
        _safe_print(json.dumps(smoke, ensure_ascii=False, indent=2))

    _section("7. 검증 체크리스트")
    checks: List[str] = []
    rl = load_kill_rl_state(cfg)
    events = rl.get("kill_events") or []
    if mega_trend_unlock_enabled():
        checks.append("[ ] ENABLE_MEGA_TREND_UNLOCK=1 확인됨")
    else:
        checks.append("[!] ENABLE_MEGA_TREND_UNLOCK 미설정 — 일일 킬 파이프라인 비활성")
    if os.path.isfile(db_path):
        checks.append("[x] forward DB 접근 가능")
    else:
        checks.append("[!] forward DB 없음")
    if events:
        checks.append(f"[x] kill_events {len(events)}건 기록됨")
    else:
        checks.append("[ ] kill_events 없음 — 킬 발동 이력 필요 (실전 RL 학습 대기)")
    if sector_guard_enabled():
        checks.append("[x] P6 contamination guard ON")
    if isinstance(evolve_out, dict) and evolve_out.get("updated"):
        checks.append("[x] evolve 1사이클 갱신 발생")
    elif isinstance(evolve_out, dict) and evolve_out.get("reason") == "insufficient_evaluated_events":
        checks.append("[ ] evolve: 평가 완료 이벤트 부족 (정상 — 표본 쌓일 때까지 대기)")
    if args.smoke and (report.get("smoke") or {}).get("evolve_updated"):
        checks.append("[x] smoke: evolve 갱신 확인")
    if args.smoke and (report.get("smoke") or {}).get("record_sector_std") == "반도체/IT":
        checks.append("[x] smoke: sector_std 정규화 확인")
    report["checks"] = checks
    for line in checks:
        _safe_print(line)

    if args.json_out:
        out_path = args.json_out
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        _safe_print(f"\nJSON report: {out_path}")

    _safe_print(f"\n완료 @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
