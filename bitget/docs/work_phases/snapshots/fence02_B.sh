#!/bin/bash
# CAT-L-FENCE-02 · B — read-only retrospective capture
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
echo "B_EXIT=$?"
