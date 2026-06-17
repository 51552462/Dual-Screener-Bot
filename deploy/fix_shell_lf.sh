#!/usr/bin/env bash
# 저장소 내 *.sh CRLF → LF (서버에서 Windows 줄바꿈으로 set -o pipefail 깨짐 방지)
set -eu -o pipefail
ROOT="$(cd "${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}" && pwd)"
count=0
while IFS= read -r -d '' f; do
  if grep -q $'\r' "$f" 2>/dev/null; then
    sed -i 's/\r$//' "$f"
    count=$((count + 1))
  fi
done < <(find "$ROOT" -type f -name '*.sh' ! -path '*/.git/*' -print0 2>/dev/null)
echo "fix_shell_lf: ${count} file(s) normalized under ${ROOT}"
