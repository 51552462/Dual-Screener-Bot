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
    return payload


def format_cursor_paste(snap: Dict[str, Any]) -> str:
    slim = {
        "digest_id": snap.get("digest_id"),
        "date_kst": snap.get("date_kst"),
        "overall_light": snap.get("overall_light"),
        "checks": snap.get("checks"),
        "server_ops": snap.get("server_ops"),
        "forbidden": snap.get("forbidden"),
        "ssot": snap.get("ssot"),
    }
    body = json.dumps(slim, ensure_ascii=False, indent=2)
    return (
        "---CURSOR---\n"
        "Track B (Bitget) · POST_DEPLOY_OBS 일일 관측. 코드/알파/실전 수정 금지.\n"
        "모드: 배포·관측만. SSOT: track_b_NEXT_ACTION.md · track_b_POST_DEPLOY_OBS_체크리스트.md\n"
        "아래 JSON만 읽고 3줄 요약 + 이상 시 CURSOR_TO_CLAUDE OUTBOX 한 줄. Handoff 없으면 구현 금지.\n"
        "금지: C-2 · MDD5% · B-2 live · ENABLE_REAL_EXECUTION\n"
        f"{body}\n"
        "---END---"
    )


def format_claude_paste(snap: Dict[str, Any]) -> str:
    slim = {
        "digest_id": snap.get("digest_id"),
        "date_kst": snap.get("date_kst"),
        "overall_light": snap.get("overall_light"),
        "checks": snap.get("checks"),
        "server_ops": snap.get("server_ops"),
        "gmm_report_slice": snap.get("gmm_report_slice"),
        "forbidden": snap.get("forbidden"),
    }
    body = json.dumps(slim, ensure_ascii=False, indent=2)
    return (
        "Bitget Track B · POST_DEPLOY_OBS 일일 스냅샷 (Claude Pro · 묶음 C / CAT-I).\n"
        "역할: 관측 판정만. 신규 코딩 Handoff는 이상이 명확하고 디렉터가 요청할 때만.\n"
        "읽기: track_b_CURSOR_TO_CLAUDE · track_b_NEXT_ACTION · 05 I-GMM/01b.\n"
        "금지: C-2 funding · MDD 5% · B-2 live · ENABLE_REAL_EXECUTION.\n"
        "Ask: (1) 관측 정상/이상 (2) Handoff 필요 여부 — 필요 시 CAT-HANDOFF 1개만 파일용.\n"
        f"{body}"
    )


def format_digest_html(snap: Dict[str, Any]) -> str:
    checks = snap.get("checks") or {}
    book = checks.get("forward_book") or {}
    cos = checks.get("cos_eff") or {}
    dna = checks.get("dna_rank") or {}
    ops = snap.get("server_ops") or {}

    def _ops_line(key: str, label: str) -> str:
        row = ops.get(key) or {}
        st = str(row.get("status") or "unknown")
        light = {"ok": "🟢", "fail": "🔴"}.get(st, "🟡")
        return f"{light} {label}: {_esc(st)} · {_esc(row.get('detail'))}"

    lines = [
        f"<b>Bitget 관측 일일</b> · {_esc(snap.get('date_kst'))} KST · {_esc(snap.get('overall_light'))}",
        "<i>I-GMM 배포 후 1~2주 · paper only · 실전/funding/MDD5% 금지</i>",
        "",
        f"{_esc(book.get('light'))} <b>장부 OPEN/CLOSED</b>",
        f"OPEN={_esc(book.get('open_total'))} · CLOSED={_esc(book.get('closed_total'))}",
        f"by market OPEN {_esc(book.get('open_by_market'))}",
        "",
        f"{_esc(cos.get('light'))} <b>Cos_eff</b>",
        (
            f"n={_esc(cos.get('sample_count'))} · zero_ratio={_esc(cos.get('zero_ratio'))} · "
            f"mean_nz={_esc(cos.get('mean_nonzero'))} · src={_esc(cos.get('log_source_used'))}"
        ),
        "",
        f"{_esc(dna.get('light'))} <b>DNA RANK / shape</b>",
        f"keys={_esc(dna.get('keys_present'))}",
        f"shape={_esc(dna.get('shape_source_distribution'))}",
        "",
        "<b>서버 ops (자동 probe)</b>",
        _ops_line("l1_logrotate", "L-1 logrotate"),
        _ops_line("l2_backup_timer", "L-2 backup.timer"),
        _ops_line("ai_overseer", "ai_overseer"),
        _ops_line("report_bot_token", "REPORT_BOT_TOKEN"),
        _ops_line("report_bot_chat", "REPORT_BOT_CHAT_ID"),
        "",
        "<b>관측 포인트 (뭐를 보면 되나)</b>",
        "1) 가상 장부에 자리(OPEN)가 생기는가",
        "2) Cos_eff=0.000 만 반복되지 않는가",
        "3) CRYPTO_DNA_ALPHA_RANK 키가 있는가",
        "4) 주간 01b 리포트가 쌓이는가 (weekly)",
        "5) L-1/L-2/overseer 켜져 있는가",
        "",
        "<b>이상 시</b>: 아래 복붙 → Cursor 또는 Claude. 정상 시 보관만.",
    ]
    return "\n".join(lines)


def format_paste_followup_html(snap: Dict[str, Any]) -> str:
    cursor = _esc(format_cursor_paste(snap))
    claude = _esc(format_claude_paste(snap))
    # Telegram HTML: use <pre> for paste blocks (escape already applied)
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
    paste = format_paste_followup_html(snap)
    # Prefer sending summary first; paste may be long → chunk
    chunks: List[str] = [summary]
    max_len = 3500
    if len(paste) <= max_len:
        chunks.append(paste)
    else:
        # send Cursor / Claude separately as plain-ish short intros + truncated pre
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
