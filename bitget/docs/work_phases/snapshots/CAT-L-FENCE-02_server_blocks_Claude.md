# CAT-L-FENCE-02 · 서버 실행 블록 (Claude 확인본) — 2026-09-28

Cursor가 OUTBOX에 올린 S0/B 명령을 검토해 **서버 실행 전 보완**한 판입니다. 로직은 그대로이고 바뀐 곳은 아래 "바뀐 점"에 있습니다. 이 블록들은 `git pull`(S1)과 `/tmp` 기록 외에는 서버를 바꾸지 않습니다. 설치기·`update_bitget.sh`·`daemon-reload`는 실행하지 않습니다.

## 순서

1. **S1** 실행 (코드 반영은 `git pull`만)
2. **S0** 실행 (생성기 vs 라이브 cron.d 비교)
3. **B** 실행 (같은 세션에서 이어서 가능)
4. 세 출력 파일 내용을 Claude에게 그대로 회신
   ```bash
   cat /tmp/fence02_S1.out /tmp/fence02_S0.out /tmp/fence02_B.out
   ```

**중단 규칙**
- S1에서 pull이 실패하면(로컬 변경, fast-forward 불가) 거기서 멈추고 출력만 회신
- S0에서 `S2_EMPTY_DIFF=yes` · `S2_ENV_SAME=yes` · `GIT_CLEAN=yes` **세 개가 모두 나오지 않으면** 설치 금지, 출력만 회신
- 세 개가 모두 yes여도 **`update_bitget.sh`·설치기는 Claude 확인 뒤에** 실행 (S3)

## 바뀐 점 (Cursor 원본 대비)

| # | 변경 | 이유 |
|---|------|------|
| 1 | `set -eu`를 `bash <<'…'` 서브셸 안으로 격리 | 대화형 SSH 셸에 `set -e`가 남으면 이후 빈 `grep` 하나로 세션이 끊길 수 있음. 같은 세션에서 B를 이어 돌릴 예정이라 실제 위험 |
| 2 | 비교에서 `SHELL=`/`PATH=`/`CRON_TZ=` 줄을 빼지 않고 **환경 줄 diff를 따로 판정**(`S2_ENV_SAME`) | 이 줄들은 모든 잡의 동작(PATH, 스케줄 시간대)에 영향. 제외하면 설치기가 값을 바꿔도 S2가 PASS할 수 있음 |
| 3 | 줄 수 출력 + 빈 결과끼리는 PASS 불가, `GIT_CLEAN` 확인 | 빈 출력==빈 출력이 PASS로 보이는 것 방지. 생성기 import가 추적 파일(`bitget.crontab.example`)을 건드렸다면 다음 `git pull --ff-only`가 막힘 |
| 4 | Python 안의 경로를 하드코딩 대신 `INSTALL_ROOT`에서 읽음 | 셸과 Python이 다른 경로를 볼 가능성 제거 |
| 5 | S1에서 pull 전 **들어올 커밋 목록·HEAD 전후** 출력 | 서버가 현재 어느 커밋인지, 무엇이 새로 들어오는지 기록이 없음(아래 A5-EVENTLOG-01 확인용) |
| 6 | B에 `/tmp/fence02_first_scan.cap` 추가 | Step 2 때 서버에 걸어 둔 16:01 UTC 첫 스캔 캡처. Step 3 B 블록에서 빠져 있었음 |
| 7 | B: journalctl 접근 가능 여부 사전 확인, `2>/dev/null`로 오류를 숨기지 않음 | 접근이 안 되면 "OOM 없음"이 가짜로 나옴. OOM 소급은 이번 수정의 결과 지표라 오탐이 치명적 |
| 8 | B: OOM 패턴을 `oom`에서 `out of memory`/`oom-kill`/`killed process`로 | 다른 단어에 걸리는 오탐 방지 |
| 9 | B: `sqlite3 -readonly`, `find`에 `TZ=UTC`, `sudo` 제거, PID 자동 선택(`pgrep -u ubuntu`) | 읽기전용 보장, 서버 시간대와 무관하게 UTC 기준, 수동 PID 입력 제거 |

## S1 — 코드 반영 (pull만)

```bash
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
```

## S0 — 생성기 vs 라이브 (읽기전용)

```bash
bash <<'S0' 2>&1 | tee /tmp/fence02_S0.out
# CAT-L-FENCE-02 · S0/S2 (Claude 확인본) — 읽기전용. 설치기/update_bitget.sh 실행 아님.
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"; export INSTALL_ROOT
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"; [ -x "$PY" ] || PY=python3
cd "$INSTALL_ROOT" || { echo "S2_EMPTY_DIFF=no (INSTALL_ROOT 없음)"; exit 1; }
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
if ! "$PY" - >"$T/gen.raw" <<'PY'
import os
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
root = Path(os.environ["INSTALL_ROOT"])
spec = spec_from_file_location("gen_cron", root / "bitget" / "deploy" / "generate_bitget_crontab.py")
mod = module_from_spec(spec); spec.loader.exec_module(mod)
print(mod.render_bitget_crontab(str(root)), end="")
PY
then echo "S2_EMPTY_DIFF=no (generator 실행 실패)"; exit 1; fi
[ -r "$LIVE" ] || { echo "S2_EMPTY_DIFF=no (live 읽기 불가)"; exit 1; }
body() { grep -vE '^[[:space:]]*(#|$)' "$1" | sed 's/[[:space:]]*$//'; }
envs() { grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$1"; }
jobs() { grep -vE '^[A-Za-z_][A-Za-z0-9_]*=' "$1"; }
body "$T/gen.raw" >"$T/gen.all"; body "$LIVE" >"$T/live.all"
envs "$T/gen.all" >"$T/gen.env"; envs "$T/live.all" >"$T/live.env"
jobs "$T/gen.all" >"$T/gen.job"; jobs "$T/live.all" >"$T/live.job"
echo "줄 수 — jobs: gen=$(wc -l <"$T/gen.job") live=$(wc -l <"$T/live.job") / env: gen=$(wc -l <"$T/gen.env") live=$(wc -l <"$T/live.env")"
echo "=== JOB diff (live → gen) · 비어 있으면 PASS ==="
if [ -s "$T/gen.job" ] && [ -s "$T/live.job" ] && diff -u "$T/live.job" "$T/gen.job"; then echo "S2_EMPTY_DIFF=yes"; else echo "S2_EMPTY_DIFF=no"; fi
echo "=== ENV diff (SHELL/PATH/CRON_TZ 등) ==="
if diff -u "$T/live.env" "$T/gen.env"; then echo "S2_ENV_SAME=yes"; else echo "S2_ENV_SAME=no"; fi
echo "=== git ==="
echo "HEAD=$(git rev-parse --short HEAD)"
git status --short --untracked-files=no | head -20
[ -z "$(git status --short --untracked-files=no)" ] && echo "GIT_CLEAN=yes" || echo "GIT_CLEAN=no"
S0
```

## B — 실스캔 소급 캡처 (읽기전용)

```bash
bash <<'B' 2>&1 | tee /tmp/fence02_B.out
# CAT-L-FENCE-02 · S4/B (Claude 확인본) — 읽기전용
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
SINCE='2026-09-27 13:55'
date -u
echo '--- 0. Step 2 때 걸어 둔 첫 스캔 캡처 ---'
cat /tmp/fence02_first_scan.cap 2>/dev/null || echo 'NO_FIRST_SCAN_CAP'
echo '--- 1. 실행 중 스캔의 cgroup ---'
ps -eo pid,user,etime,cgroup:90,cmd | grep -E 'scan_|daily_audit|weekly_evolution|--scan-|systemd-run' | grep -v grep || echo 'NO_SCAN_RUNNING_NOW'
echo '--- 6. slice / 부모 slice ---'
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax -p MemoryCurrent -p MemoryAccounting
systemctl show bitget.slice bitget-cron.slice -p MemoryHigh -p MemoryMax -p MemoryAccounting
echo '--- 5. 실행 중 scope (CPUQuota) ---'
for u in $(systemctl list-units --type=scope --no-legend | awk '/run-/{print $1}'); do
  echo "[$u]"; systemctl show "$u" -p Slice -p CPUQuotaPerSecUSec -p MemoryHigh -p MemoryMax
done
echo '--- 3. environ (실행 중인 ubuntu 스캔이 있을 때만) ---'
for p in $(pgrep -u ubuntu -f -- '--scan-' | head -3); do
  echo "[pid=$p] $(ps -o user=,cgroup= -p "$p")"
  tr '\0' '\n' <"/proc/$p/environ" 2>/dev/null | grep -E '^(HOME|USER|LOGNAME|PATH|PWD)='
done
echo "(참고) 현재 셸: HOME=$HOME USER=${USER:-} LOGNAME=${LOGNAME:-}"
echo '--- 4. 로그 소유권 (wrapper 이후 새 로그) ---'
TZ=UTC find "$INSTALL_ROOT" -name '*.log' -newermt "$SINCE" -printf '%TY-%Tm-%Td %TH:%TM %u:%g %p\n' 2>/dev/null | head -80
ls -l /var/log/bitget 2>/dev/null || true
echo '--- 2. cron / systemd-run ---'
journalctl -u cron -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_CRON_OK' || echo 'JOURNAL_CRON_UNREADABLE → 아래 결과 신뢰 불가(sudo로 재시도)'
echo "systemd-run 실행 라인 수: $(journalctl -u cron --utc --since "$SINCE" --no-pager 2>/dev/null | grep -c 'systemd-run')"
journalctl -u cron --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'error|failed' | tail -40 || true
echo '--- 9. 커널 OOM 소급 ---'
journalctl -k -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_K_OK' || echo 'JOURNAL_K_UNREADABLE → 아래 OOM 결과 신뢰 불가(sudo로 재시도)'
journalctl -k --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'out of memory|oom-kill|oom_reaper|killed process' || echo 'OOM_GREP_EMPTY'
echo '--- slice memory.events / peak (slice가 활성일 때만 존재) ---'
CG=/sys/fs/cgroup/bitget.slice/bitget-cron.slice/bitget-cron-heavy.slice
cat "$CG/memory.events" 2>/dev/null || echo 'NO_MEMORY_EVENTS'
cat "$CG/memory.peak" 2>/dev/null || echo 'NO_MEMORY_PEAK'
echo '--- 8. LIFECAP ---'
sqlite3 -readonly /var/lib/quant-bitget/data/bitget_job_lifetime.sqlite 'SELECT mode,pid,started_utc FROM job_starts;' 2>&1 | head -20
grep -RniE 'lifecap skip|LIFECAP WOULD_KILL|LIFECAP ENFORCE' "$INSTALL_ROOT/bitget" --include='*.log' 2>/dev/null | tail -20 || true
echo '--- 7. cron.d wrapper 줄 (28 기대) ---'
grep -c 'systemd-run' /etc/cron.d/dual-screener-bitget
grep -E 'systemd-run' /etc/cron.d/dual-screener-bitget | grep -oE -- '--(scan-[^ ]+|daily-audit|weekly-evolution)' | sort
B
```
