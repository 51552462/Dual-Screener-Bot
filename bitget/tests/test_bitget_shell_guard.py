"""bitget.sh shell guard invariants (static — no subprocess spawn)."""
from __future__ import annotations

import unittest
from pathlib import Path


class TestBitgetShellDailyAuditGuard(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sh = (
            Path(__file__).resolve().parents[1] / "deploy" / "bitget.sh"
        ).read_text(encoding="utf-8")

    def test_daily_audit_pgrep_guard_present(self):
        self.assertIn("_bitget_live_daily_audit_lines", self.sh)
        self.assertIn("runner --mode daily_audit", self.sh)
        self.assertIn("SKIP: another daily_audit job is already running", self.sh)

    def test_daily_audit_guard_excludes_self_pid(self):
        self.assertIn('[[ "$pid" -eq "$$" ]]', self.sh)

    def test_daily_audit_guard_skips_zombie_processes(self):
        self.assertIn('[[ "$state" == "Z" ]]', self.sh)

    def test_daily_audit_guard_exit_zero_on_skip(self):
        # cron-safe: duplicate daily must not fail the cron job
        block = self.sh.split("case \"$MODE\" in", 1)[1].split("esac", 1)[0]
        self.assertIn("daily_audit)", block)
        self.assertIn("exit 0", block)


if __name__ == "__main__":
    unittest.main()
