#!/bin/bash
# CAT-L-FENCE-02 · S1 — git pull only. No installer / update_bitget.sh.
bash <<'S1' 2>&1 | tee /tmp/fence02_S1.out
# CAT-L-FENCE-02 · S1 — 코드 반영은 git pull 만. update_bitget.sh / 설치기 실행 금지.
set -u
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
echo "HEAD(before): $(git log -1 --format='%h %ad %s' --date=iso)"
git fetch --quiet && {
  echo "== 들어올 커밋 =="; git log --format='%h %ad %s' --date=short 'HEAD..@{u}'
  echo "== 변경 요약 =="; git diff --stat 'HEAD..@{u}' | tail -40
}
git pull --ff-only && echo "HEAD(after): $(git log -1 --format='%h %ad %s' --date=iso)"
S1
echo "S1_EXIT=$?"
