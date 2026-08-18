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

    if dna.get("ok"):
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
    now_ids_ok = {"book", "dna", "cos", "l1", "l2", "overseer", "tg", "r01b"}
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
            "ok": rank_ok,
            "keys_present": rank,
            "shape_source_distribution": report.get("shape_source_distribution") or {},
            "light": _traffic(rank_ok),
            "expect": "CRYPTO_DNA_ALPHA_RANK1~3 present",
        },
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
    return payload


def format_cursor_paste(snap: Dict[str, Any]) -> str:
    dash = snap.get("dashboard") or {}
    slim = {
        "digest_id": snap.get("digest_id"),
        "date_kst": snap.get("date_kst"),
        "overall_light": snap.get("overall_light"),
        "dashboard_headline": dash.get("headline"),
        "progress": dash.get("progress_label"),
        "problem": dash.get("problem"),
        "missing": dash.get("missing"),
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
    return "\n".join(
        [
            f"<b>숫자 메모</b> · {_esc(snap.get('overall_light'))}",
            f"장부 OPEN={_esc(book.get('open_total'))} CLOSED={_esc(book.get('closed_total'))} {_esc(book.get('closed_by_market'))}",
            (
                f"Cos n={_esc(cos.get('sample_count'))} zero={_esc(cos.get('zero_ratio'))} "
                f"src={_esc(cos.get('log_source_used'))}"
            ),
            f"DNA keys={_esc(dna.get('keys_present'))} shape={_esc(dna.get('shape_source_distribution'))}",
        ]
    )


def format_paste_followup_html(snap: Dict[str, Any]) -> str:
    cursor = _esc(format_cursor_paste(snap))
    claude = _esc(format_claude_paste(snap))
    return (
        "<b>📋 Cursor 복붙</b>\n"
        f"<pre>{cursor}</pre>\n"
        "<b>📋 Claude Pro 복붙</b>\n"
        f"<pre>{claude}</pre>"
    )



def persist_digest(snap: Dict[str, Any]) -> bool:
    from bitget.infra.ops_logger import insert_ops_event

    return bool(
        insert_ops_event(
            component=_COMPONENT,
            severity="INFO",
            event=_EVENT,
            payload=dict(snap),
        )
    )


def _send_report_html(message: str) -> bool:
    import requests
    import telegram_env

    token = (telegram_env.get_report_token() or "").strip()
    chat_id = (telegram_env.get_report_chat_id() or "").strip()
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=20)
        return resp.status_code == 200
    except Exception as ex:
        logger.warning("post_deploy_obs digest send failed: %s", ex)
        return False


def send_digest_messages(snap: Dict[str, Any]) -> Dict[str, Any]:
    summary = format_digest_html(snap)
    numbers = format_numbers_html(snap)
    paste = format_paste_followup_html(snap)
    chunks: List[str] = [summary, numbers]
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
