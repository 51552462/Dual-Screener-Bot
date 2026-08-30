"""Weekday control-plane cycle, job outbox, optional Claude review and Telegram digest.

The runner is safe by default: it persists deterministic decisions and writes
job packets, but does not call an AI or Telegram unless the matching CLI flag
is supplied.  Cursor packets remain candidates; this module never grants code
write, Git, deployment, SSH, live-trading, or merge authority.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from dev_autonomy.adapters import ClaudeCodeVerifier, ClaudeVerifierAdapter
from dev_autonomy.control_plane import (
    DEFAULT_QUEUE_DB,
    AutonomyEnvelope,
    ControlAction,
    ControlPlaneStore,
    evaluate_reports,
    load_envelope,
    scan_reports,
)
from dev_autonomy.paths import AUTONOMY_DATA_DIR


JOB_SCHEMA = "dev_autonomy.job.v1"
DEFAULT_OUTBOX_DIR = AUTONOMY_DATA_DIR / "outbox"

_ACTION_ICON = {
    ControlAction.OBSERVE_ONLY.value: "🟢",
    ControlAction.CLAUDE_REVIEW.value: "🟡",
    ControlAction.CURSOR_IMPLEMENT.value: "🛠",
    ControlAction.WAIT_WEEKEND.value: "📅",
    ControlAction.SAFETY_HALT.value: "🔴",
    ControlAction.QUARANTINE.value: "⛔",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _job_provider(action: str) -> str:
    return {
        ControlAction.CLAUDE_REVIEW.value: "claude",
        ControlAction.CURSOR_IMPLEMENT.value: "cursor",
        ControlAction.WAIT_WEEKEND.value: "director",
        ControlAction.SAFETY_HALT.value: "safety",
        ControlAction.QUARANTINE.value: "quarantine",
    }.get(action, "observer")


def _job_path(outbox_dir: Path, report_id: str) -> Path:
    digest = hashlib.sha256(report_id.encode("utf-8")).hexdigest()[:20]
    return outbox_dir / f"job_{digest}.json"


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def build_job_packet(evaluated: dict[str, Any]) -> dict[str, Any]:
    report = evaluated["report"]
    decision = evaluated["decision"]
    action = str(decision["action"])
    return {
        "schema": JOB_SCHEMA,
        "created_at": _utc_now(),
        "job_id": report["report_id"],
        "provider": _job_provider(action),
        "action": action,
        "track": report["track"],
        "execution_authorized": False,
        "report": report,
        "decision": decision,
        "hard_limits": {
            "allow_live": False,
            "allow_deploy": False,
            "allow_merge": False,
            "allow_ssh": False,
            "require_pull_request": True,
        },
    }


def format_telegram_digest(payload: dict[str, Any]) -> str:
    reports = payload.get("reports") or []
    new_count = sum(1 for item in reports if item.get("inserted"))
    lines = [
        "🤖 <b>[퀀트 자동화 관제]</b>",
        f"새 결과 <b>{new_count}</b>건 · 전체 판독 {len(reports)}건",
    ]
    for item in reports[:8]:
        report = item.get("report") or {}
        decision = item.get("decision") or {}
        action = str(decision.get("action") or "UNKNOWN")
        icon = _ACTION_ICON.get(action, "⚪")
        track = html.escape(str(report.get("track") or "?"))
        reason = html.escape(str(decision.get("reason") or ""))
        freshness = "신규" if item.get("inserted") else "중복"
        lines.append(f"{icon} Track {track} · {action} · {freshness}")
        if reason:
            lines.append(f"└ {reason[:240]}")

    ai_results = payload.get("claude_results") or []
    for result in ai_results[:4]:
        verdict = html.escape(str(result.get("verdict") or "UNKNOWN"))
        detail = html.escape(str(result.get("detail") or ""))
        lines.append(f"🧠 Claude: <b>{verdict}</b> · {detail[:240]}")

    errors = payload.get("errors") or []
    if errors:
        lines.append(f"⚠️ 판독 오류 {len(errors)}건")
        for error in errors[:3]:
            lines.append(f"└ {html.escape(str(error))[:240]}")

    lines.extend(
        [
            "",
            "실전 주문·배포·병합 권한: <b>없음</b>",
            "주말 승인 필요 항목은 자동으로 대기합니다.",
        ]
    )
    return "\n".join(lines)


def send_telegram_digest(text: str) -> tuple[bool, str]:
    try:
        import telegram_env
        from telegram_html_delivery import post_telegram_message

        token = (telegram_env.get_report_token() or "").strip()
        chat_id = (telegram_env.get_report_chat_id() or "").strip()
        if not token or not chat_id:
            return False, "TELEGRAM_CREDENTIALS_MISSING"
        response = post_telegram_message(
            url=f"https://api.telegram.org/bot{token}/sendMessage",
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            timeout=15,
        )
        status = int(getattr(response, "status_code", 0) or 0)
        return 200 <= status < 300, f"HTTP_{status}"
    except Exception as exc:
        return False, f"TELEGRAM_SEND_FAILED:{type(exc).__name__}"


def run_weekday_cycle(
    *,
    north_star_path: Path | None,
    bitget_ops_path: Path | None,
    envelope: AutonomyEnvelope | None,
    queue_db: Path,
    outbox_dir: Path,
    include_ssot: bool = True,
    dry_run: bool = False,
    run_claude_review: bool = False,
    notify_telegram: bool = False,
    claude_adapter: ClaudeVerifierAdapter | None = None,
    telegram_sender: Callable[[str], tuple[bool, str]] = send_telegram_digest,
) -> dict[str, Any]:
    reports, errors = scan_reports(
        north_star_path=north_star_path,
        bitget_ops_path=bitget_ops_path,
        include_ssot=include_ssot,
    )
    store = None if dry_run else ControlPlaneStore(queue_db)
    evaluated = evaluate_reports(reports, envelope=envelope, store=store)
    packet_paths: list[str] = []
    claude_results: list[dict[str, Any]] = []

    for item in evaluated:
        if not item.get("inserted") or dry_run:
            continue
        action = str(item["decision"]["action"])
        if action == ControlAction.OBSERVE_ONLY.value:
            continue
        packet = build_job_packet(item)
        path = _job_path(outbox_dir, str(item["report"]["report_id"]))
        _write_json_atomic(path, packet)
        packet_paths.append(str(path))

        if run_claude_review and action == ControlAction.CLAUDE_REVIEW.value:
            adapter = claude_adapter or ClaudeCodeVerifier(enabled=True)
            result = adapter.verify(
                {
                    "task": "Review this normalized quant report and its deterministic routing decision.",
                    "report": item["report"],
                    "decision": item["decision"],
                    "limits": packet["hard_limits"],
                    "required_output": "OK, MODIFY, or REJECT with a short Korean reason",
                }
            )
            result_payload = {
                "job_id": packet["job_id"],
                "provider": "claude",
                "ok": result.ok,
                "available": result.available,
                "verdict": result.verdict,
                "detail": result.detail,
            }
            claude_results.append(result_payload)
            _write_json_atomic(path.with_suffix(".result.json"), result_payload)

    payload: dict[str, Any] = {
        "schema": "dev_autonomy.weekday_run.v1",
        "timestamp": _utc_now(),
        "mode": "DRY_RUN" if dry_run else "WEEKDAY_CONTROL",
        "execution_authorized": False,
        "reports": evaluated,
        "errors": errors,
        "job_packets": packet_paths,
        "claude_results": claude_results,
        "telegram": {"requested": notify_telegram, "sent": False, "detail": "NOT_REQUESTED"},
    }

    has_new = any(item.get("inserted") for item in evaluated)
    if notify_telegram and not dry_run and (has_new or errors):
        sent, detail = telegram_sender(format_telegram_digest(payload))
        payload["telegram"] = {"requested": True, "sent": sent, "detail": detail}
    elif notify_telegram:
        payload["telegram"] = {"requested": True, "sent": False, "detail": "NO_NEW_REPORT"}

    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Quant weekday automation control cycle")
    parser.add_argument("--north-star-ledger", type=Path)
    parser.add_argument("--bitget-ops-db", type=Path)
    parser.add_argument("--envelope", type=Path)
    parser.add_argument("--queue-db", type=Path, default=DEFAULT_QUEUE_DB)
    parser.add_argument("--outbox-dir", type=Path, default=DEFAULT_OUTBOX_DIR)
    parser.add_argument("--no-ssot", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-claude-review", action="store_true")
    parser.add_argument("--notify-telegram", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    try:
        envelope = load_envelope(args.envelope)
    except Exception as exc:
        sys.stderr.write(f"invalid envelope: {exc}\n")
        return 2

    payload = run_weekday_cycle(
        north_star_path=args.north_star_ledger,
        bitget_ops_path=args.bitget_ops_db,
        envelope=envelope,
        queue_db=args.queue_db,
        outbox_dir=args.outbox_dir,
        include_ssot=not args.no_ssot,
        dry_run=args.dry_run,
        run_claude_review=args.run_claude_review,
        notify_telegram=args.notify_telegram,
    )
    sys.stdout.write(json.dumps(payload, ensure_ascii=not args.json, indent=2) + "\n")
    if payload["errors"]:
        return 1
    if args.notify_telegram and payload["telegram"]["detail"] not in {"NO_NEW_REPORT", "NOT_REQUESTED"}:
        return 0 if payload["telegram"]["sent"] else 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
