"""AI adapter interfaces — real CLI when available, fail-closed otherwise."""

from __future__ import annotations

import json
import shutil
import subprocess
from abc import ABC, abstractmethod
from typing import Any, Dict

from dev_autonomy.types import AdapterResult


class CursorExecutorAdapter(ABC):
    @abstractmethod
    def availability(self) -> tuple[bool, str]:
        ...

    @abstractmethod
    def run_implementation(self, context_pack: Dict[str, Any]) -> AdapterResult:
        ...


class ClaudeVerifierAdapter(ABC):
    @abstractmethod
    def availability(self) -> tuple[bool, str]:
        ...

    @abstractmethod
    def verify(self, context_pack: Dict[str, Any]) -> AdapterResult:
        ...


class CursorCliExecutor(CursorExecutorAdapter):
    """Probe Cursor editor CLI; autonomous write disabled in P0."""

    WRITE_DISABLED = "AUTONOMOUS_WRITE_DISABLED"

    def probe(self) -> Dict[str, Any]:
        exe = shutil.which("cursor") or shutil.which("cursor.cmd")
        if not exe:
            return {"installed": False, "detail": "CURSOR_CLI_NOT_FOUND"}
        try:
            proc = subprocess.run(
                [exe, "--version"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            version = (proc.stdout or proc.stderr or "").strip().splitlines()[0]
            return {
                "installed": True,
                "version": version,
                "headless_available": False,
                "write_enabled": False,
                "detail": "CURSOR_HEADLESS_UNAVAILABLE",
            }
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"installed": False, "detail": str(exc)}

    def availability(self) -> tuple[bool, str]:
        info = self.probe()
        if not info.get("installed"):
            return False, info.get("detail", "CURSOR_CLI_NOT_FOUND")
        return False, f"{info.get('detail')} (editor: {info.get('version', '')})"

    def run_implementation(self, context_pack: Dict[str, Any]) -> AdapterResult:
        return AdapterResult(
            ok=False,
            available=False,
            verdict="SKIP",
            detail=self.WRITE_DISABLED,
            raw={"context_keys": list(context_pack.keys())},
        )


class ClaudeCodeVerifier(ClaudeVerifierAdapter):
    def probe(self) -> Dict[str, Any]:
        if not shutil.which("claude"):
            return {"installed": False, "detail": "EXTERNAL_ARCHITECT_UNAVAILABLE"}
        try:
            proc = subprocess.run(
                ["claude", "--version"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            if proc.returncode != 0:
                return {"installed": False, "detail": "EXTERNAL_ARCHITECT_UNAVAILABLE"}
            return {
                "installed": True,
                "version": (proc.stdout or proc.stderr or "").strip(),
                "scripted_verify": False,
            }
        except (OSError, subprocess.TimeoutExpired):
            return {"installed": False, "detail": "EXTERNAL_ARCHITECT_UNAVAILABLE"}

    def availability(self) -> tuple[bool, str]:
        info = self.probe()
        if not info.get("installed"):
            return False, info.get("detail", "EXTERNAL_ARCHITECT_UNAVAILABLE")
        return False, "EXTERNAL_ARCHITECT_UNAVAILABLE (scripted verify disabled)"

    def verify(self, context_pack: Dict[str, Any]) -> AdapterResult:
        ok, detail = self.availability()
        if not ok:
            return AdapterResult(
                ok=False,
                available=False,
                verdict="EXTERNAL_ARCHITECT_UNAVAILABLE",
                detail=detail,
            )
        # Scripted non-interactive invocation would go here when Director enables.
        return AdapterResult(
            ok=False,
            available=True,
            verdict="NOT_IMPLEMENTED",
            detail="Claude CLI present but scripted verify not wired in P0",
            raw={"context_keys": list(context_pack.keys())},
        )


class FakeCursorExecutor(CursorExecutorAdapter):
    """Test double — records pack without mutation."""

    def __init__(self, succeed: bool = True):
        self.succeed = succeed
        self.last_pack: Dict[str, Any] | None = None

    def availability(self) -> tuple[bool, str]:
        return True, "fake"

    def run_implementation(self, context_pack: Dict[str, Any]) -> AdapterResult:
        self.last_pack = context_pack
        if not self.succeed:
            return AdapterResult(ok=False, available=True, verdict="FAIL", detail="fake failure")
        return AdapterResult(ok=True, available=True, verdict="PASS", detail="fake pass")


class FakeClaudeVerifier(ClaudeVerifierAdapter):
    def __init__(self, verdict: str = "OK"):
        self.verdict = verdict
        self.last_pack: Dict[str, Any] | None = None

    def availability(self) -> tuple[bool, str]:
        return True, "fake"

    def verify(self, context_pack: Dict[str, Any]) -> AdapterResult:
        self.last_pack = context_pack
        return AdapterResult(
            ok=self.verdict == "OK",
            available=True,
            verdict=self.verdict,
            detail=f"fake {self.verdict}",
        )
