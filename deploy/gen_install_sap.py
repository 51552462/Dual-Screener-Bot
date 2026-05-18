import base64
from pathlib import Path

root = Path(__file__).resolve().parent.parent
src = (root / "system_auto_pilot.py").read_bytes()
b64 = base64.b64encode(src).decode("ascii")
chunks = [b64[i : i + 76] for i in range(0, len(b64), 76)]
body = "\n".join(chunks)
script = f"""#!/usr/bin/env bash
# Restore system_auto_pilot.py from embedded base64 (factory CLI included).
set -euo pipefail
ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/.." && pwd)"
TARGET="${{ROOT}}/system_auto_pilot.py"
B64_FILE="$(mktemp)"
trap 'rm -f "$B64_FILE"' EXIT
cat << 'B64EOF' > "$B64_FILE"
{body}
B64EOF
base64 -d "$B64_FILE" > "$TARGET"
echo "Wrote $TARGET ($(wc -l < "$TARGET") lines)"
python3 -m py_compile "$TARGET"
echo "OK: py_compile passed"
"""
out = Path(__file__).resolve().parent / "install_system_auto_pilot.sh"
out.write_text(script, encoding="utf-8", newline="\n")
print(f"wrote {out} ({out.stat().st_size} bytes)")
