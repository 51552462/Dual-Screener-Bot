#!/bin/bash
# CAT-L-FENCE-02 · Step 3 B resume 2026-09-29 — read-only
# --fence-check requires LIVE path (Handoff omitted it; argparse would rewrite crontab if run as flag-only)
bash <<'B' 2>&1 | tee /tmp/fence02_B.out
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"
[ -x "$PY" ] || PY=python3
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
SINCE='2026-09-29 00:00'
date -u
echo '--- 0. Step 2 첫 스캔 캡처(참고, TIMEOUT 가능성 있음) ---'
cat /tmp/fence02_first_scan.cap 2>/dev/null || echo 'NO_FIRST_SCAN_CAP'
echo '--- 1. 실행 중 스캔 cgroup ---'
ps -eo pid,user,etime,cgroup:90,cmd | grep -E 'scan_|daily_audit|weekly_evolution|--scan-|systemd-run' | grep -v grep || echo 'NO_SCAN_RUNNING_NOW'
echo '--- 6. slice / 부모 slice ---'
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax -p MemoryCurrent -p MemoryAccounting
systemctl show bitget.slice bitget-cron.slice -p MemoryHigh -p MemoryMax -p MemoryAccounting
echo '--- 5. 실행 중 scope (CPUQuota) ---'
for u in $(systemctl list-units --type=scope --no-legend | awk '/run-/{print $1}'); do
  echo "[$u]"; systemctl show "$u" -p Slice -p CPUQuotaPerSecUSec -p MemoryHigh -p MemoryMax
done
echo '--- 3. environ ---'
for p in $(pgrep -u ubuntu -f -- '--scan-' | head -3); do
  echo "[pid=$p] $(ps -o user=,cgroup= -p "$p")"
  tr '\0' '\n' <"/proc/$p/environ" 2>/dev/null | grep -E '^(HOME|USER|LOGNAME|PATH|PWD)='
done
echo '--- 4. 로그 소유권 ---'
TZ=UTC find "$INSTALL_ROOT" -name '*.log' -newermt "$SINCE" -printf '%TY-%Tm-%Td %TH:%TM %u:%g %p\n' 2>/dev/null | head -80
echo '--- 2. cron / systemd-run ---'
journalctl -u cron -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_CRON_OK' || echo 'JOURNAL_CRON_UNREADABLE'
journalctl -u cron --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'error|failed' | tail -40 || true
echo '--- 9. 커널 OOM 소급 ---'
journalctl -k -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_K_OK' || echo 'JOURNAL_K_UNREADABLE'
journalctl -k --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'out of memory|oom-kill|oom_reaper|killed process' || echo 'OOM_GREP_EMPTY'
echo '--- 8. LIFECAP ---'
grep -RniE 'lifecap skip|LIFECAP WOULD_KILL|LIFECAP ENFORCE' "$INSTALL_ROOT/bitget" --include='*.log' 2>/dev/null | tail -20 || true
echo '--- 7. wrapper 개수 (1차 python 게이트, 2차 grep 보조) ---'
"$PY" "$INSTALL_ROOT/bitget/deploy/generate_bitget_crontab.py" --install-root "$INSTALL_ROOT" --fence-check "$LIVE"
grep -v '^\s*#' /etc/cron.d/dual-screener-bitget | grep -c systemd-run
B
echo "B_EXIT=$?"
