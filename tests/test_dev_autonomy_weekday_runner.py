"""Weekday runner and real headless-adapter wiring tests — no paid AI calls."""

from __future__ import annotations

import json
import subprocess

from dev_autonomy.adapters import ClaudeCodeVerifier, CursorCliExecutor, FakeClaudeVerifier
from dev_autonomy.control_plane import NormalizedReport
from dev_autonomy.paths import REPO_ROOT
from dev_autonomy.weekday_runner import (
    build_job_packet,
    format_telegram_digest,
    run_weekday_cycle,
)


def _review_report() -> NormalizedReport:
    return NormalizedReport(
        report_id="north-star:A:2026-08-30:r1",
        source="north_star_ledger",
        track="A",
        observed_at="2026-08-30T00:00:00Z",
        source_status="RECALL_FORK",
        cursor_action="RECALL_FORK",
        metrics={"mdd_pct": 8.98, "mdd_cap_pct": 10.0},
        payload_hash="abc",
    )


def test_job_packet_never_grants_execution_authority():
    packet = build_job_packet(
        {
            "report": {
                "report_id": "r1",
                "track": "A",
            },
            "decision": {
                "action": "CURSOR_IMPLEMENT",
                "execution_authorized": False,
            },
        }
    )
    assert packet["provider"] == "cursor"
    assert packet["execution_authorized"] is False
    assert not any(packet["hard_limits"][key] for key in ("allow_live", "allow_deploy", "allow_merge", "allow_ssh"))
    assert packet["pr_policy"]["draft_pr_only"] is True
    assert packet["pr_policy"]["auto_merge"] is False


def test_weekday_cycle_deduplicates_jobs_claude_and_telegram(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "dev_autonomy.weekday_runner.scan_reports",
        lambda **_kwargs: ([_review_report()], []),
    )
    sent: list[str] = []

    def _send(text: str) -> tuple[bool, str]:
        sent.append(text)
        return True, "HTTP_200"

    kwargs = {
        "north_star_path": None,
        "bitget_ops_path": None,
        "envelope": None,
        "queue_db": tmp_path / "queue.sqlite",
        "outbox_dir": tmp_path / "outbox",
        "run_claude_review": True,
        "notify_telegram": True,
        "claude_adapter": FakeClaudeVerifier(verdict="MODIFY"),
        "telegram_sender": _send,
    }
    first = run_weekday_cycle(**kwargs)
    second = run_weekday_cycle(**kwargs)

    assert first["reports"][0]["inserted"] is True
    assert first["claude_results"][0]["verdict"] == "MODIFY"
    assert first["telegram"]["sent"] is True
    assert len(first["job_packets"]) == 1
    assert list((tmp_path / "outbox").glob("job_*.json"))
    assert second["reports"][0]["inserted"] is False
    assert second["claude_results"] == []
    assert second["telegram"]["detail"] == "NO_NEW_REPORT"
    assert len(sent) == 1


def test_digest_is_short_korean_operator_summary():
    payload = {
        "reports": [
            {
                "inserted": True,
                "report": {"track": "A"},
                "decision": {"action": "WAIT_WEEKEND", "reason": "director required"},
            }
        ],
        "claude_results": [],
        "errors": [],
    }
    text = format_telegram_digest(payload)
    assert "퀀트 자동화 관제" in text
    assert "WAIT_WEEKEND" in text
    assert "실전 주문·배포·병합 권한" in text


def test_cursor_write_requires_explicit_constructor_opt_in(tmp_path):
    adapter = CursorCliExecutor(enable_write=False, executable="agent", repo_root=tmp_path)
    result = adapter.run_implementation({"task": "x"})
    assert not result.ok
    assert result.available
    assert result.detail == "AUTONOMOUS_WRITE_DISABLED"


def test_cursor_headless_uses_force_sandbox_and_no_shell(tmp_path, monkeypatch):
    calls: list[list[str]] = []

    def _run(command, **kwargs):
        calls.append(command)
        assert kwargs["cwd"] == tmp_path
        assert kwargs["check"] is False
        return subprocess.CompletedProcess(command, 0, stdout='{"result":"ok"}', stderr="")

    monkeypatch.setattr("dev_autonomy.adapters.subprocess.run", _run)
    adapter = CursorCliExecutor(enable_write=True, executable="agent", repo_root=tmp_path)
    result = adapter.run_implementation({"allowed_paths": ["src/x.py"]})

    assert result.ok
    assert calls
    assert calls[0][:3] == ["agent", "-p", "--force"]
    assert calls[0][calls[0].index("--sandbox") + 1] == "enabled"
    assert "--workspace" in calls[0]


def test_cursor_project_policy_denies_external_and_policy_mutation():
    policy = json.loads((REPO_ROOT / ".cursor" / "cli.json").read_text(encoding="utf-8"))
    denied = set(policy["permissions"]["deny"])
    assert {
        "Shell(git)",
        "Shell(gh)",
        "Shell(ssh)",
        "Read(.env*)",
        "Write(.cursor/**)",
        "Write(.git)",
        "Write(.git/**)",
        "Write(deploy/**)",
        "WebFetch(*)",
        "Mcp(*:*)",
    }.issubset(denied)


def test_claude_headless_is_read_only_and_parses_verdict(tmp_path, monkeypatch):
    calls: list[list[str]] = []

    def _run(command, **_kwargs):
        calls.append(command)
        if "--version" in command:
            return subprocess.CompletedProcess(command, 0, stdout="2.1.999", stderr="")
        wrapped = {"result": json.dumps({"verdict": "OK", "detail": "검증 통과"})}
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(wrapped), stderr="")

    monkeypatch.setattr("dev_autonomy.adapters.subprocess.run", _run)
    adapter = ClaudeCodeVerifier(enabled=True, executable="claude", repo_root=tmp_path)
    result = adapter.verify({"changed_paths": ["src/x.py"]})

    assert result.ok
    assert result.verdict == "OK"
    command = calls[-1]
    assert command[command.index("--permission-mode") + 1] == "dontAsk"
    assert command[command.index("--tools") + 1] == "Read,Glob,Grep"
    assert "acceptEdits" not in command
