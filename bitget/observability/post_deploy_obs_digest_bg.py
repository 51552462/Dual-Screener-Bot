"""
POST_DEPLOY_OBS / I-GMM-01b — daily observation digest → REPORT_BOT.

Read-only. Does not touch gates.py or gmm_dna_alpha_sync.py.
Includes Cursor / Claude Pro copy-paste blocks for the director.
"""
from __future__ import annotations

import html
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_EVENT = "post_deploy_obs_digest_daily"
_COMPONENT = "observability.post_deploy_obs"
_KST = ZoneInfo("Asia/Seoul")

# data_miner._fit_gmm_templates: if len(xdf) < 12 → return {}
_GMM_FIT_MIN_ROWS = 12

_DNA_STATE_PLAIN = {
    "RANK_OK": "DNA 다 컸어요 – 오늘은 그냥 넘어가도 돼요",
    "DATA_WAIT_LOW_MFE": "DNA 재료가 아직 덜 모였어요 – 계속 기다리면 돼요",
    "GMM_EMPTY": "재료는 쌓였는데 DNA를 안 만들었어요 – 디렉터가 서버에서 한 번 돌려주세요",
    "SYNC_FAIL": "DNA는 만들었는데 연결이 안 붙어요 – Cursor·Claude에게 보여주세요",
    "DB_PATH_OR_ENV": "저장소를 못 찾았어요 – 디렉터가 서버 상태를 봐주세요",
    "UNKNOWN": "무슨 상황인지 애매해요 – 숫자 메모를 Cursor·Claude에게 보여주세요",
}


def digest_enabled() -> bool:
    env = os.environ.get("POST_DEPLOY_OBS_DIGEST_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("POST_DEPLOY_OBS_DIGEST_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import POST_DEPLOY_OBS_DIGEST_ENABLED

    return bool(POST_DEPLOY_OBS_DIGEST_ENABLED)


def dna_diagnosis_enabled() -> bool:
    env = os.environ.get("POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    try:
        from bitget.infra.memory_policy import POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED

        return bool(POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED)
    except Exception:
        return True


def diagnose_dna_state(
    config: dict,
    n_closed_by_tf: Dict[str, int],
    n_mfe8_by_tf: Dict[str, int],
    gmm_min_rows: int,
    *,
    db_ok: bool = True,
    rank_all_present: Optional[bool] = None,
    checked_at: Optional[str] = None,
    last_error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ordered DNA why-diagnosis (Handoff POST_DEPLOY_OBS-DNA-UX-01 Spec 1).
    First matching rule wins — do not reorder without Claude re-verify.
    """
    at = checked_at or datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    gmm = config.get("BITGET_GMM_DNA_TEMPLATES") if isinstance(config, dict) else None
    from bitget.observability.gmm_dna_alpha_report_bg import count_gmm_template_clusters

    cluster_n = count_gmm_template_clusters(gmm)
    templates_present = cluster_n > 0

    if rank_all_present is None:
        rank_all_present = all(
            isinstance((config or {}).get(f"CRYPTO_DNA_ALPHA_RANK{i}"), dict)
            and (config or {}).get(f"CRYPTO_DNA_ALPHA_RANK{i}")
            for i in (1, 2, 3)
        )

    closed = {str(k).upper(): int(v or 0) for k, v in (n_closed_by_tf or {}).items()}
    mfe8 = {str(k).upper(): int(v or 0) for k, v in (n_mfe8_by_tf or {}).items()}
    min_rows = int(gmm_min_rows)

    state: Optional[str] = None
    action: Optional[str] = None
    if not db_ok:
        state, action = "DB_PATH_OR_ENV", "DIRECTOR_SSH_CHECK"
    elif rank_all_present:
        state, action = "RANK_OK", "NONE"
    elif not mfe8 or all(int(v) < min_rows for v in mfe8.values()):
        state, action = "DATA_WAIT_LOW_MFE", "OBSERVE_HOLD"
    elif not templates_present:
        state, action = "GMM_EMPTY", "DIRECTOR_SSH_CHECK"
    elif templates_present and not rank_all_present:
        state, action = "SYNC_FAIL", "REPORT_TO_CLAUDE"
    else:
        state, action = "UNKNOWN", "REPORT_TO_CLAUDE"

    return {
        "state": state,
        "cursor_action": action,
        "plain": _DNA_STATE_PLAIN.get(state or "", _DNA_STATE_PLAIN["UNKNOWN"]),
        "checked_at": at,
        "n_closed_by_tf": closed,
        "n_mfe8_by_tf": mfe8,
        "templates_present": templates_present,
        "gmm_cluster_n": cluster_n if templates_present else (0 if isinstance(gmm, dict) else None),
        "last_error": last_error,
        "gmm_min_rows": min_rows,
    }


def collect_dna_diagnosis(
    *,
    forward_db_path: Optional[str] = None,
    rank_keys_present: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """Load config + TF/MFE probes then diagnose (read-only)."""
    at = datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    cfg: Dict[str, Any] = {}
    db_ok = True
    last_err: Optional[str] = None
    try:
        from bitget.config_hub import load_config

        loaded = load_config()
        if not isinstance(loaded, dict):
            db_ok = False
            last_err = "load_config_not_dict"
        else:
            cfg = loaded
    except Exception as ex:
        db_ok = False
        last_err = f"load_config:{ex}"[:200]

    from bitget.observability.gmm_dna_alpha_report_bg import (
        GMM_FIT_MIN_ROWS_OBSERVED,
        collect_closed_mfe_counts_by_tf,
        resolve_bitget_min_mfe_for_mining,
    )

    mfe_min = resolve_bitget_min_mfe_for_mining(cfg if db_ok else None)
    n_closed, n_mfe, cnt_err = collect_closed_mfe_counts_by_tf(
        forward_db_path=forward_db_path,
        mfe_min=mfe_min,
    )
    if cnt_err:
        db_ok = False
        last_err = cnt_err

    if rank_keys_present is not None:
        rank_all = all(bool(rank_keys_present.get(f"RANK{i}")) for i in (1, 2, 3))
    else:
        rank_all = None

    return diagnose_dna_state(
        cfg,
        n_closed or {},
        n_mfe or {},
        GMM_FIT_MIN_ROWS_OBSERVED,
        db_ok=db_ok,
        rank_all_present=rank_all,
        checked_at=at,
        last_error=last_err,
    )


def _esc(v: Any) -> str:
    return html.escape(str(v) if v is not None else "", quote=False)


def _probe_cmd(cmd: List[str], *, timeout: float = 8.0) -> Tuple[str, str]:
    """Return (status, detail) where status in ok|fail|unknown."""
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        out = ((proc.stdout or "") + (proc.stderr or "")).strip()
        if proc.returncode == 0:
            return "ok", (out[:200] if out else "ok")
        return "fail", (out[:200] if out else f"exit={proc.returncode}")
    except FileNotFoundError:
        return "unknown", "command_missing"
    except (OSError, subprocess.SubprocessError) as ex:
        return "unknown", str(ex)[:200]


def _probe_path_exists(path: str) -> Tuple[str, str]:
    if os.path.isfile(path) or os.path.isdir(path):
        return "ok", path
    return "fail", f"missing:{path}"


def _server_ops_probes() -> Dict[str, Any]:
    """Best-effort server install probes — never invent success."""
    logrotate = _probe_path_exists("/etc/logrotate.d/bitget-dante")
    if logrotate[0] != "ok":
        # template name may vary
        alt = _probe_path_exists("/etc/logrotate.d/dante-bitget")
        if alt[0] == "ok":
            logrotate = alt

    backup = _probe_cmd(["systemctl", "is-active", "dante-bitget-backup.timer"])
    overseer = _probe_cmd(["pgrep", "-af", "ai_overseer"])
    if overseer[0] == "ok" and "ai_overseer" not in overseer[1]:
        overseer = ("fail", "no_ai_overseer_process")
    report_token = "ok" if (os.environ.get("REPORT_BOT_TOKEN") or "").strip() else "fail"
    report_chat = "ok" if (os.environ.get("REPORT_BOT_CHAT_ID") or "").strip() else "fail"
    return {
        "l1_logrotate": {"status": logrotate[0], "detail": logrotate[1]},
        "l2_backup_timer": {"status": backup[0], "detail": backup[1]},
        "ai_overseer": {"status": overseer[0], "detail": overseer[1]},
        "report_bot_token": {"status": report_token, "detail": "env"},
        "report_bot_chat": {"status": report_chat, "detail": "env"},
    }


def _traffic(ok: bool, warn: bool = False) -> str:
    if ok:
        return "🟢"
    if warn:
        return "🟡"
    return "🔴"


def _ops_status_ok(probes: Dict[str, Any], key: str) -> bool:
    return str((probes.get(key) or {}).get("status") or "") == "ok"


def _count_recent_ops_event(event: str, *, days: int = 14) -> Optional[int]:
    """Return count of ops_events rows, or None if DB unavailable."""
    try:
        from bitget.infra.data_paths import ops_events_db_path
        from bitget.infra.clock import utc_hours_ago_iso

        path = ops_events_db_path()
        if not path or not os.path.isfile(path):
            return None
        since = utc_hours_ago_iso(float(days) * 24.0)
        import sqlite3

        conn = sqlite3.connect(path, timeout=10)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM ops_events WHERE event=? AND ts_utc >= ?",
                (event, since),
            ).fetchone()
            return int(row[0] if row else 0)
        finally:
            conn.close()
    except Exception:
        return None


def build_kid_dashboard(snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Elementary-school checklist toward Track B B0 goals (paper).
    Buckets: working | problem | missing | later (do-not-touch-now).
    """
    checks = snap.get("checks") or {}
    book = checks.get("forward_book") or {}
    cos = checks.get("cos_eff") or {}
    dna = checks.get("dna_rank") or {}
    ops = snap.get("server_ops") or {}

    working: List[Dict[str, str]] = []
    problem: List[Dict[str, str]] = []
    missing: List[Dict[str, str]] = []
    later: List[Dict[str, str]] = []

    # --- runtime / today ---
    if book.get("ok"):
        working.append(
            {
                "id": "book",
                "title": "가상 연습 장부",
                "plain": f"닫힌 자리 {book.get('closed_total', 0)} · 열린 자리 {book.get('open_total', 0)}",
            }
        )
    else:
        problem.append(
            {
                "id": "book",
                "title": "가상 연습 장부",
                "plain": "아직 거래 기록이 없어요 (OPEN/CLOSED=0)",
            }
        )

    if dna_diagnosis_enabled() and isinstance(dna.get("diagnosis"), dict):
        diag = dna.get("diagnosis") or {}
        st = str(diag.get("state") or "")
        plain = str(diag.get("plain") or _DNA_STATE_PLAIN["UNKNOWN"])
        item = {"id": "dna", "title": "DNA 이름표(RANK)", "plain": plain}
        if st == "RANK_OK":
            working.append(item)
        elif st == "DATA_WAIT_LOW_MFE":
            missing.append(item)
        else:
            problem.append(item)
    elif dna.get("ok"):
        working.append(
            {
                "id": "dna",
                "title": "DNA 이름표(RANK)",
                "plain": "설정에 RANK 키가 있어요",
            }
        )
    else:
        problem.append(
            {
                "id": "dna",
                "title": "DNA 이름표(RANK)",
                "plain": "RANK1~3이 비어 있어요 → 점수 연결이 안 될 수 있어요",
            }
        )

    if cos.get("ok"):
        working.append(
            {
                "id": "cos",
                "title": "Cos 점수 기록",
                "plain": f"표본 {cos.get('sample_count')}개 · 0만 반복은 아님",
            }
        )
    elif cos.get("warn"):
        missing.append(
            {
                "id": "cos",
                "title": "Cos 점수 기록",
                "plain": "오늘은 표본이 아직 없어요 (로그에서 Cos를 못 찾음)",
            }
        )
    else:
        problem.append(
            {
                "id": "cos",
                "title": "Cos 점수 기록",
                "plain": "Cos가 0에만 고정된 것 같아요",
            }
        )

    sf = checks.get("short_funnel") or {}
    if sf:
        sf_item = {
            "id": "short_funnel",
            "title": "숏(선물) 연습",
            "plain": str(sf.get("plain") or "숏 퍼널"),
        }
        st = str(sf.get("state") or "")
        if st == "SHORT_ACTIVE":
            working.append(sf_item)
        else:
            missing.append(sf_item)
        sector = str(sf.get("predicted_sector") or "UNKNOWN")
        sec_item = {
            "id": "predicted_sector",
            "title": "다음 섹터 힌트",
            "plain": f"predicted_sector={sector}",
        }
        if sector and sector not in ("UNKNOWN", ""):
            working.append(sec_item)
        else:
            missing.append(sec_item)

    if _ops_status_ok(ops, "l1_logrotate"):
        working.append({"id": "l1", "title": "로그 자동 정리(L-1)", "plain": "켜져 있어요"})
    elif ops.get("l1_logrotate"):
        problem.append(
            {
                "id": "l1",
                "title": "로그 자동 정리(L-1)",
                "plain": str((ops.get("l1_logrotate") or {}).get("detail") or "꺼짐"),
            }
        )

    if _ops_status_ok(ops, "l2_backup_timer"):
        working.append({"id": "l2", "title": "DB 자동 백업(L-2)", "plain": "타이머 켜짐"})
    elif ops.get("l2_backup_timer"):
        problem.append(
            {
                "id": "l2",
                "title": "DB 자동 백업(L-2)",
                "plain": f"꺼져 있음 · {(ops.get('l2_backup_timer') or {}).get('detail')}",
            }
        )

    if _ops_status_ok(ops, "ai_overseer"):
        working.append({"id": "overseer", "title": "AI 감사관", "plain": "프로세스 켜짐"})
    elif ops.get("ai_overseer"):
        problem.append(
            {
                "id": "overseer",
                "title": "AI 감사관",
                "plain": f"안 켜짐 · {(ops.get('ai_overseer') or {}).get('detail')}",
            }
        )

    if _ops_status_ok(ops, "report_bot_token") and _ops_status_ok(ops, "report_bot_chat"):
        working.append({"id": "tg", "title": "텔레그램 리포트봇", "plain": "토큰·채팅방 OK"})
    else:
        problem.append({"id": "tg", "title": "텔레그램 리포트봇", "plain": "토큰/채팅방 설정 확인 필요"})

    # weekly 01b rows (optional)
    n01b = _count_recent_ops_event("gmm_dna_alpha_report_weekly", days=14)
    if n01b is None:
        missing.append(
            {
                "id": "r01b",
                "title": "주간 DNA 숫자 리포트(01b)",
                "plain": "아직 확인 못 함 (ops DB)",
            }
        )
    elif n01b > 0:
        working.append(
            {
                "id": "r01b",
                "title": "주간 DNA 숫자 리포트(01b)",
                "plain": f"최근 2주에 {n01b}번 쌓임",
            }
        )
    else:
        missing.append(
            {
                "id": "r01b",
                "title": "주간 DNA 숫자 리포트(01b)",
                "plain": "아직 0번 — 주간 배치 후 쌓여야 해요",
            }
        )

    # code-done / goal path (static + deferred)
    working.append(
        {
            "id": "paper",
            "title": "연습 모드(paper)",
            "plain": "실전 주문은 꺼져 있어요 (이게 맞아요)",
        }
    )
    working.append(
        {
            "id": "code_tracks",
            "title": "코드 트랙 A·B·C·D·I-GMM",
            "plain": "만들어 둔 코드는 Claude OK까지 끝났어요",
        }
    )

    later.extend(
        [
            {
                "id": "mdd5",
                "title": "MDD 5%로 더 조이기",
                "plain": "나중 · 연습 관측(06) 끝난 뒤",
            },
            {
                "id": "c2",
                "title": "펀딩비 반영(C-2)",
                "plain": "나중 · 지금은 금지",
            },
            {
                "id": "b2live",
                "title": "deathmatch 실배분",
                "plain": "나중 · 지금은 shadow만",
            },
            {
                "id": "live",
                "title": "실전 매매 ON",
                "plain": "나중 · P2-5 전 금지",
            },
            {
                "id": "cagr",
                "title": "연 12~25% 목표",
                "plain": "지금은 B0(숫자 모으기) · 수익 목표 아님",
            },
        ]
    )

    # progress = now-lane items that are working vs (working+problem+missing) excluding later & static code labels
    now_ids_ok = {
        "book",
        "dna",
        "cos",
        "l1",
        "l2",
        "overseer",
        "tg",
        "r01b",
        "short_funnel",
        "predicted_sector",
    }
    done_n = sum(1 for x in working if x["id"] in now_ids_ok)
    need_n = done_n + sum(1 for x in problem if x["id"] in now_ids_ok) + sum(
        1 for x in missing if x["id"] in now_ids_ok
    )
    pct = int(round(100.0 * done_n / need_n)) if need_n else 0
    bar_w = 10
    filled = max(0, min(bar_w, int(round(pct / 100.0 * bar_w))))
    bar = "█" * filled + "░" * (bar_w - filled)

    if problem:
        headline = "오늘 고칠 구멍이 있어요"
        light = "🔴" if len(problem) >= 2 else "🟡"
    elif missing:
        headline = "잘 가고 있어요 · 아직 기다리는 칸이 있어요"
        light = "🟡"
    else:
        headline = "오늘 할 일 칸은 대체로 괜찮아요"
        light = "🟢"

    return {
        "headline": headline,
        "light": light,
        "goal_plain": "목표: 연습매매가 건강하게 돌아가는지 1~2주 확인 (실전·큰돈 배분 X)",
        "progress_pct": pct,
        "progress_bar": bar,
        "progress_label": f"지금 할 일 {done_n}/{need_n}",
        "working": working,
        "problem": problem,
        "missing": missing,
        "later": later,
        "how_to_read": [
            "🟢 잘 되고 있어요",
            "🔴 구멍·오류 — 손보거나 Cursor/Claude에 물어보세요",
            "🟡 아직 모으는 중 / 조심",
            "⬜ 나중 — 지금은 건드리면 안 돼요",
        ],
    }


def compute_post_deploy_obs_digest(
    *,
    window_days: int = 2,
    forward_db_path: Optional[str] = None,
    log_text: Optional[str] = None,
    log_dir: Optional[str] = None,
    include_server_probes: bool = True,
) -> Dict[str, Any]:
    from bitget.observability.gmm_dna_alpha_report_bg import (
        compute_weekly_gmm_dna_alpha_report_bg,
    )

    report = compute_weekly_gmm_dna_alpha_report_bg(
        window_days=window_days,
        forward_db_path=forward_db_path,
        log_text=log_text,
        log_dir=log_dir,
    )
    open_by = report.get("open_count_by_market") or {}
    closed_by = report.get("closed_count_by_market") or {}
    open_n = int(sum(int(v) for v in open_by.values()))
    closed_n = int(sum(int(v) for v in closed_by.values()))
    rank = report.get("dna_rank_keys_present") or {}
    rank_ok = bool(rank.get("RANK1") or rank.get("RANK2") or rank.get("RANK3"))
    cos_n = report.get("cos_eff_sample_count")
    zero_ratio = report.get("cos_eff_zero_ratio")
    log_src = str(report.get("log_source_used") or "unavailable")
    cos_ok = (
        cos_n is not None
        and int(cos_n) > 0
        and zero_ratio is not None
        and float(zero_ratio) < 0.999
    )
    cos_warn = cos_n is None or int(cos_n or 0) == 0 or log_src == "unavailable"
    book_ok = open_n > 0 or closed_n > 0

    dna_diag: Optional[Dict[str, Any]] = None
    if dna_diagnosis_enabled():
        dna_diag = collect_dna_diagnosis(
            forward_db_path=forward_db_path,
            rank_keys_present=rank,
        )
        # RANK_OK requires all three; digest ok light follows diagnosis when present
        if dna_diag.get("state") == "RANK_OK":
            rank_ok = True
        elif dna_diag.get("state") == "DATA_WAIT_LOW_MFE":
            rank_ok = False

    checks = {
        "forward_book": {
            "ok": book_ok,
            "open_total": open_n,
            "closed_total": closed_n,
            "open_by_market": open_by,
            "closed_by_market": closed_by,
            "light": _traffic(book_ok, warn=not book_ok),
            "expect": "OPEN or CLOSED > 0 after I-GMM deploy",
        },
        "cos_eff": {
            "ok": cos_ok,
            "warn": cos_warn and not cos_ok,
            "sample_count": cos_n,
            "zero_ratio": zero_ratio,
            "mean_nonzero": report.get("cos_eff_mean_nonzero"),
            "log_source_used": log_src,
            "light": _traffic(cos_ok, warn=cos_warn),
            "expect": "Cos_eff not stuck at 0.000 only",
        },
        "dna_rank": {
            "ok": bool(
                (dna_diag or {}).get("state") == "RANK_OK"
                if dna_diag
                else rank_ok
            ),
            "keys_present": rank,
            "shape_source_distribution": report.get("shape_source_distribution") or {},
            "diagnosis": dna_diag,
            "light": (
                "🟢"
                if dna_diag and dna_diag.get("state") == "RANK_OK"
                else (
                    "🟡"
                    if dna_diag and dna_diag.get("state") == "DATA_WAIT_LOW_MFE"
                    else _traffic(rank_ok)
                )
            ),
            "expect": "CRYPTO_DNA_ALPHA_RANK1~3 present",
        },
    }
    try:
        from bitget.config_hub import load_config as _load_cfg
        from bitget.observability.short_funnel_report_bg import (
            attach_predicted_sector,
            collect_short_funnel_report,
        )

        _sf = collect_short_funnel_report(forward_db_path=forward_db_path)
        try:
            _sf = attach_predicted_sector(_sf, _load_cfg())
        except Exception:
            _sf = attach_predicted_sector(_sf, {})
        checks["short_funnel"] = _sf
    except Exception as _sf_exc:
        checks["short_funnel"] = {
            "state": "SHORT_EMPTY",
            "light": "🟡",
            "plain": "숏 퍼널을 못 읽었어요",
            "last_error": str(_sf_exc)[:200],
            "predicted_sector": "UNKNOWN",
        }
    probes = _server_ops_probes() if include_server_probes else {}
    now_kst = datetime.now(_KST)
    payload = {
        "digest_id": "BITGET_POST_DEPLOY_OBS_DAILY",
        "date_kst": now_kst.strftime("%Y-%m-%d"),
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "window_days": int(window_days),
        "checks": checks,
        "server_ops": probes,
        "gmm_report_slice": {
            "cos_eff_sample_count": cos_n,
            "cos_eff_zero_ratio": zero_ratio,
            "cos_eff_mean_nonzero": report.get("cos_eff_mean_nonzero"),
            "log_source_used": log_src,
            "dna_rank_keys_present": rank,
            "shape_source_distribution": report.get("shape_source_distribution") or {},
        },
        "forbidden": [
            "C-2 funding",
            "MDD 5% tune",
            "B-2 live alloc",
            "ENABLE_REAL_EXECUTION",
        ],
        "ssot": {
            "checklist": "bitget/docs/work_phases/track_b_POST_DEPLOY_OBS_체크리스트.md",
            "next_action": "bitget/docs/work_phases/track_b_NEXT_ACTION.md",
            "outbox": "bitget/docs/work_phases/track_b_CURSOR_TO_CLAUDE.md",
        },
    }
    payload["overall_light"] = (
        "🟢"
        if book_ok and cos_ok and rank_ok
        else ("🟡" if book_ok or rank_ok else "🔴")
    )
    payload["dashboard"] = build_kid_dashboard(payload)
    # dashboard light wins for human glance when ops reds exist
    if payload["dashboard"].get("light") == "🔴":
        payload["overall_light"] = "🔴"
    elif payload["dashboard"].get("light") == "🟡" and payload["overall_light"] == "🟢":
        payload["overall_light"] = "🟡"

    # Track B 북극성 (읽기 전용 · 원장 쓰기는 19:30 factory cron)
    try:
        from bitget.observability.north_star_panel_bg import (
            build_bitget_goal_dashboard,
            collect_bitget_north_star_snap,
        )

        ns = collect_bitget_north_star_snap(cadence="daily")
        payload["north_star"] = {
            "available": bool(ns.get("available", True)),
            "error": ns.get("error"),
            "date_kst": ns.get("date_kst"),
            "dashboard": build_bitget_goal_dashboard(ns),
            "track_b": (ns.get("tracks") or {}).get("B"),
            "period_returns_b": (ns.get("period_returns") or {}).get("B"),
            "gate_b": ((ns.get("ledger") or {}).get("B") if isinstance(ns.get("ledger"), dict) else None),
            "meta": ns.get("meta"),
            "_snap": ns,  # format HTML용 (persist payload에서 축약 가능)
        }
    except Exception as ex:
        payload["north_star"] = {
            "available": False,
            "error": f"north_star_panel:{ex}"[:160],
            "dashboard": None,
            "_snap": {"available": False, "error": str(ex)[:160]},
        }
    return payload


def format_cursor_paste(snap: Dict[str, Any]) -> str:
    dash = snap.get("dashboard") or {}
    dna = ((snap.get("checks") or {}).get("dna_rank") or {})
    slim = {
        "digest_id": snap.get("digest_id"),
        "date_kst": snap.get("date_kst"),
        "overall_light": snap.get("overall_light"),
        "dashboard_headline": dash.get("headline"),
        "progress": dash.get("progress_label"),
        "problem": dash.get("problem"),
        "missing": dash.get("missing"),
        "dna_diagnosis": dna.get("diagnosis"),
        "checks": snap.get("checks"),
        "server_ops": snap.get("server_ops"),
        "forbidden": snap.get("forbidden"),
        "ssot": snap.get("ssot"),
    }
    body = json.dumps(slim, ensure_ascii=False, indent=2)
    return (
        "---CURSOR---\n"
        "Track B (Bitget) · 일일 목표 체크리스트/대시보드 관측. 코드/알파/실전 수정 금지.\n"
        "모드: 배포·관측만. SSOT: track_b_NEXT_ACTION.md · track_b_POST_DEPLOY_OBS_체크리스트.md\n"
        "아래 JSON만 읽고 3줄 쉬운 요약 + 이상 시 CURSOR_TO_CLAUDE OUTBOX 한 줄. Handoff 없으면 구현 금지.\n"
        "금지: C-2 · MDD5% · B-2 live · ENABLE_REAL_EXECUTION\n"
        f"{body}\n"
        "---END---"
    )


def format_claude_paste(snap: Dict[str, Any]) -> str:
    dash = snap.get("dashboard") or {}
    slim = {
        "digest_id": snap.get("digest_id"),
        "date_kst": snap.get("date_kst"),
        "overall_light": snap.get("overall_light"),
        "dashboard": {
            "headline": dash.get("headline"),
            "progress_pct": dash.get("progress_pct"),
            "problem": dash.get("problem"),
            "missing": dash.get("missing"),
            "working_ids": [x.get("id") for x in (dash.get("working") or [])],
        },
        "checks": snap.get("checks"),
        "server_ops": snap.get("server_ops"),
        "gmm_report_slice": snap.get("gmm_report_slice"),
        "forbidden": snap.get("forbidden"),
    }
    body = json.dumps(slim, ensure_ascii=False, indent=2)
    return (
        "Bitget Track B · 일일 목표 체크리스트 스냅샷 (Claude Pro · CAT-I/L).\n"
        "역할: 관측 판정만. 신규 코딩 Handoff는 이상이 명확하고 디렉터가 요청할 때만.\n"
        "읽기: track_b_CURSOR_TO_CLAUDE · track_b_NEXT_ACTION · 05.\n"
        "금지: C-2 funding · MDD 5% · B-2 live · ENABLE_REAL_EXECUTION.\n"
        "Ask: (1) 정상/이상 (2) Handoff 필요 여부 — 필요 시 CAT-HANDOFF 1개만 파일용.\n"
        f"{body}"
    )


def format_digest_html(snap: Dict[str, Any]) -> str:
    """Kid-friendly dashboard first (Telegram message 1)."""
    dash = snap.get("dashboard") or build_kid_dashboard(snap)
    lines: List[str] = [
        f"<b>코인 연습 · 오늘 한눈에</b> · {_esc(snap.get('date_kst'))} · {_esc(dash.get('light'))}",
        f"<i>{_esc(dash.get('goal_plain'))}</i>",
        "",
        f"<b>{_esc(dash.get('headline'))}</b>",
        f"진행 {_esc(dash.get('progress_bar'))} {_esc(dash.get('progress_pct'))}% · {_esc(dash.get('progress_label'))}",
        "",
    ]

    def _sec(title: str, items: List[Dict[str, str]], mark: str) -> None:
        lines.append(f"<b>{mark} {title}</b>")
        if not items:
            lines.append("· (없음)")
        else:
            for it in items:
                lines.append(f"· {_esc(it.get('title'))}: {_esc(it.get('plain'))}")
        lines.append("")

    _sec("잘 되고 있어요", list(dash.get("working") or []), "🟢")
    _sec("구멍·오류 (손볼 것)", list(dash.get("problem") or []), "🔴")
    _sec("아직 기다리는 중", list(dash.get("missing") or []), "🟡")
    _sec("나중이에요 (지금 금지)", list(dash.get("later") or []), "⬜")

    lines.append("<b>읽는 법</b>")
    for tip in dash.get("how_to_read") or []:
        lines.append(f"· {_esc(tip)}")
    lines.append("")
    lines.append("<i>평소엔 이 메시지만 보세요. 🔴가 많거나 모를 때만 아래 복붙.</i>")
    return "\n".join(lines)


def format_numbers_html(snap: Dict[str, Any]) -> str:
    """Short technical numbers (Telegram message 2)."""
    checks = snap.get("checks") or {}
    book = checks.get("forward_book") or {}
    cos = checks.get("cos_eff") or {}
    dna = checks.get("dna_rank") or {}
    diag = dna.get("diagnosis") if isinstance(dna.get("diagnosis"), dict) else None
    lines = [
        f"<b>숫자 메모</b> · {_esc(snap.get('overall_light'))}",
        f"장부 OPEN={_esc(book.get('open_total'))} CLOSED={_esc(book.get('closed_total'))} {_esc(book.get('closed_by_market'))}",
        (
            f"Cos n={_esc(cos.get('sample_count'))} zero={_esc(cos.get('zero_ratio'))} "
            f"src={_esc(cos.get('log_source_used'))}"
        ),
        f"DNA keys={_esc(dna.get('keys_present'))} shape={_esc(dna.get('shape_source_distribution'))}",
    ]
    sf = checks.get("short_funnel") or {}
    if sf:
        lines.append(
            f"SHORT funnel state={_esc(sf.get('state'))} "
            f"openL/S={_esc((sf.get('open_by_side') or {}).get('LONG'))}/"
            f"{_esc((sf.get('open_by_side') or {}).get('SHORT'))} "
            f"blocked={_esc(sf.get('blocked_short_total'))} "
            f"top={_esc(sf.get('blocked_short_top_bucket'))} "
            f"sector={_esc(sf.get('predicted_sector'))}"
        )
        lines.append(f"blocked_buckets={_esc(sf.get('blocked_short_by_bucket'))}")
    if diag:
        lines.extend(
            [
                (
                    f"DNA진단 state={_esc(diag.get('state'))} "
                    f"action={_esc(diag.get('cursor_action'))} "
                    f"at={_esc(diag.get('checked_at'))}"
                ),
                f"n_closed_by_tf={_esc(diag.get('n_closed_by_tf'))}",
                f"n_mfe8_by_tf={_esc(diag.get('n_mfe8_by_tf'))} min_rows={_esc(diag.get('gmm_min_rows'))}",
                (
                    f"templates_present={_esc(diag.get('templates_present'))} "
                    f"gmm_cluster_n={_esc(diag.get('gmm_cluster_n'))} "
                    f"last_error={_esc(diag.get('last_error'))}"
                ),
            ]
        )
    return "\n".join(lines)


def format_paste_followup_html(snap: Dict[str, Any]) -> str:
    dna = ((snap.get("checks") or {}).get("dna_rank") or {})
    diag = dna.get("diagnosis") if isinstance(dna.get("diagnosis"), dict) else {}
    action = str((diag or {}).get("cursor_action") or "")
    hint = ""
    if action == "DIRECTOR_SSH_CHECK":
        hint = (
            "<i>DNA: track_b_POST_DEPLOY_OBS_체크리스트.md §1 — "
            "BITGET_DB_STORAGE_PATH 확인 후 mine_bitget_dna_templates → sync --force</i>\n"
        )
    elif action == "REPORT_TO_CLAUDE":
        hint = (
            "<i>DNA: state="
            + _esc((diag or {}).get("state"))
            + " · 숫자 메모 첨부해 track_b_CURSOR_TO_CLAUDE Ask 작성</i>\n"
        )
    # Spec 5: only expose paste blocks when action needs director/AI
    if action not in ("DIRECTOR_SSH_CHECK", "REPORT_TO_CLAUDE"):
        # still allow paste when other problem lanes exist (ops reds)
        dash = snap.get("dashboard") or {}
        if not (dash.get("problem") or []):
            if hint:
                return hint.strip()
            return (
                "<i>평소엔 대시보드·숫자 메모만. DNA action="
                + _esc(action or "NONE")
                + " → 복붙 생략</i>"
            )

    cursor = _esc(format_cursor_paste(snap))
    claude = _esc(format_claude_paste(snap))
    return (
        (hint if hint else "")
        + "<b>📋 Cursor 복붙</b>\n"
        f"<pre>{cursor}</pre>\n"
        "<b>📋 Claude Pro 복붙</b>\n"
        f"<pre>{claude}</pre>"
    )



def persist_digest(snap: Dict[str, Any]) -> bool:
    from bitget.infra.ops_logger import insert_ops_event

    # ops_events 용량 — north_star._snap 전체 대신 요약만
    payload = dict(snap)
    ns = payload.get("north_star")
    if isinstance(ns, dict) and "_snap" in ns:
        slim = dict(ns)
        slim.pop("_snap", None)
        payload["north_star"] = slim

    return bool(
        insert_ops_event(
            component=_COMPONENT,
            severity="INFO",
            event=_EVENT,
            payload=payload,
        )
    )


def _send_report_html(message: str) -> bool:
    """REPORT_BOT direct HTTP — HTML 400 시 plain 재시도 (telegram_html_delivery SSOT)."""
    import requests
    import telegram_env
    from telegram_html_delivery import post_telegram_message

    token = (telegram_env.get_report_token() or "").strip()
    chat_id = (telegram_env.get_report_chat_id() or "").strip()
    if not token or not chat_id:
        logger.warning(
            "post_deploy_obs digest send skipped — REPORT_BOT token/chat missing"
        )
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = str(message or "")
    # Telegram hard limit 4096 — 한 칸이 길면 잘라 보냄
    max_len = 3500
    pieces = [text[i : i + max_len] for i in range(0, len(text), max_len)] or [""]
    try:
        for piece in pieces:
            if not piece.strip():
                continue
            resp = post_telegram_message(
                url=url,
                chat_id=chat_id,
                text=piece,
                parse_mode="HTML",
                timeout=20.0,
                session=requests,
            )
            code = int(getattr(resp, "status_code", 0) or 0)
            if code != 200:
                body = (getattr(resp, "text", None) or "")[:240]
                logger.warning(
                    "post_deploy_obs digest send HTTP %s: %s",
                    code,
                    body,
                )
                return False
        return True
    except Exception as ex:
        logger.warning("post_deploy_obs digest send failed: %s", ex)
        return False


def send_digest_messages(snap: Dict[str, Any]) -> Dict[str, Any]:
    from bitget.observability.north_star_panel_bg import format_bitget_north_star_html

    ns_block = snap.get("north_star") or {}
    ns_html = format_bitget_north_star_html(ns_block.get("_snap") or ns_block)
    summary = format_digest_html(snap)
    numbers = format_numbers_html(snap)
    paste = format_paste_followup_html(snap)
    # 1) Bitget 북극성 쉬운판+목표  2) 코인 연습 관측  3) 숫자  4) 복붙
    chunks: List[str] = [ns_html, summary, numbers]
    max_len = 3500
    if len(paste) <= max_len:
        chunks.append(paste)
    else:
        chunks.append(
            "<b>📋 Cursor 복붙</b>\n<pre>"
            + _esc(format_cursor_paste(snap))[:3200]
            + "</pre>"
        )
        chunks.append(
            "<b>📋 Claude Pro 복붙</b>\n<pre>"
            + _esc(format_claude_paste(snap))[:3200]
            + "</pre>"
        )

    sent = 0
    for ch in chunks:
        if _send_report_html(ch):
            sent += 1
        else:
            return {"sent": False, "chunks_ok": sent, "error": "REPORT_BOT send failed"}
    return {"sent": True, "chunks_ok": sent, "delivery": "REPORT_BOT direct_http"}


def run_post_deploy_obs_digest_job(
    *,
    window_days: int = 2,
    dry_run: bool = False,
    send: bool = True,
    persist: bool = True,
    forward_db_path: Optional[str] = None,
    log_text: Optional[str] = None,
    include_server_probes: bool = True,
) -> Optional[Dict[str, Any]]:
    if not digest_enabled():
        logger.info("post_deploy_obs digest disabled — skip")
        return None

    snap = compute_post_deploy_obs_digest(
        window_days=window_days,
        forward_db_path=forward_db_path,
        log_text=log_text,
        include_server_probes=include_server_probes,
    )
    snap["cursor_paste"] = format_cursor_paste(snap)
    snap["claude_paste"] = format_claude_paste(snap)

    result: Dict[str, Any] = {"snap": snap, "persisted": False, "telegram": None}
    if persist:
        result["persisted"] = persist_digest(snap)
    if dry_run or not send:
        result["dry_run"] = True
        return result
    result["telegram"] = send_digest_messages(snap)
    return result


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Bitget POST_DEPLOY_OBS daily telegram digest")
    p.add_argument("--window-days", type=int, default=2)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-send", action="store_true")
    p.add_argument("--no-persist", action="store_true")
    p.add_argument("--no-server-probes", action="store_true")
    args = p.parse_args(argv)
    out = run_post_deploy_obs_digest_job(
        window_days=args.window_days,
        dry_run=args.dry_run,
        send=not args.no_send and not args.dry_run,
        persist=not args.no_persist,
        include_server_probes=not args.no_server_probes,
    )
    if out is None:
        print("disabled")
        return 0
    print(json.dumps({
        "overall_light": (out.get("snap") or {}).get("overall_light"),
        "persisted": out.get("persisted"),
        "telegram": out.get("telegram"),
        "dry_run": out.get("dry_run"),
    }, ensure_ascii=False, indent=2))
    if args.dry_run:
        try:
            from bitget.observability.north_star_panel_bg import format_bitget_north_star_html

            ns = (out.get("snap") or {}).get("north_star") or {}
            print("---BITGET_NORTH_STAR---")
            print(format_bitget_north_star_html(ns.get("_snap") or ns))
        except Exception as ex:
            print("---BITGET_NORTH_STAR---")
            print(f"(skip) {ex}")
        print("---CURSOR_PASTE---")
        print((out.get("snap") or {}).get("cursor_paste"))
        print("---CLAUDE_PASTE---")
        print((out.get("snap") or {}).get("claude_paste"))
    tg = out.get("telegram") or {}
    if out.get("dry_run"):
        return 0
    return 0 if tg.get("sent") else 1


if __name__ == "__main__":
    raise SystemExit(main())
