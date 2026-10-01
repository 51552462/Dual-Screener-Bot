"""Local-only helper (NOT deployed to server).

Extract the first ```bash block under a heading from a COMMITTED Handoff blob,
print line count / CR count / sha256, and (with --run) feed the bytes to
`ssh ... bash -s` over binary stdin. Output is saved to a local file.

Usage:
  python vb_run.py <commit> <heading-prefix> <out-file> [--run]
Example:
  python vb_run.py f7f767e "### 8-1. W-" w_out.txt --run
"""
import hashlib
import os
import subprocess
import sys

HANDOFF = "bitget/docs/work_phases/CLAUDE_TO_CURSOR.md"
PEM = os.environ.get("BOT2_PEM", r"C:\Users\최용진\Downloads\LightsailDefaultKey-ap-northeast-2.pem")
HOST = os.environ.get("BOT2_HOST", "ubuntu@3.36.90.195")


def main() -> int:
    commit, heading, out_file = sys.argv[1], sys.argv[2], sys.argv[3]
    run = "--run" in sys.argv[4:]
    blob = subprocess.run(["git", "show", f"{commit}:{HANDOFF}"], capture_output=True, check=True).stdout
    assert b"\r" not in blob, "CR in committed Handoff"
    t = blob.decode("utf-8")
    i = t.index(heading)
    j = t.index("```bash\n", i) + len("```bash\n")
    k = t.index("\n```\n", j)
    b = t[j : k + 1].encode("utf-8")
    print("HEADING", heading, "| BLOCK_LINES", b.count(b"\n"), "CR", b.count(b"\r"),
          "SHA256", hashlib.sha256(b).hexdigest(), flush=True)
    if not run:
        return 0
    p = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-i", PEM, HOST, "bash -s"],
        input=b, capture_output=True,
    )
    out = p.stdout.decode("utf-8", "replace")
    err = p.stderr.decode("utf-8", "replace")
    with open(out_file, "w", encoding="utf-8", newline="") as f:
        f.write(out + "\n--STDERR--\n" + err + f"\nSSH_RC={p.returncode}\n")
    print("SSH_RC", p.returncode, "OUT_BYTES", len(out), "ERR_BYTES", len(err),
          "CR_IN_OUT", out.count("\r"), "CR_IN_ERR", err.count("\r"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
