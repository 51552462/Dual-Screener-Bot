# 우분투 서버 — 패치 업로드 · 100% 재가동 · 정상 확인 치트시트

**용도:** Cursor(또는 로컬)에서 코드를 수정한 뒤, Ubuntu 서버에 **매번 동일하게** 적용·재기동·검증할 때 **복붙만** 하면 되는 명령 모음.  
**범위:** 주식 팩토리(KR/US) · `bitget/` 제외 (`update_bitget.sh`는 별도).

---

## 0. 경로 (한 번만 확인)

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
# 다른 서버: export INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
```

아래 모든 블록은 이 `INSTALL_ROOT`를 씁니다.

---

## 1. 패치할 때마다 — 신버전 업로드 + 100% 재가동

### 1-A. 로컬에서 (코드 수정 후, 서버 들어가기 전)

```bash
git add -A
git commit -m "설명: 무엇을 고쳤는지"
git push
```

### 1-B. 서버에서 — 표준 한 블록 (가장 많이 씀)

```bash
# =============================================================================
# [패치 표준] git pull → 백업 → systemd 재배포 → 서비스 100% 재기동
# =============================================================================
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT"

# factory cron 일회 작업 락 해제 (있을 때만)
rm -f "$INSTALL_ROOT/.factory_runtime.lock" 2>/dev/null || true

# ★ 핵심: 주식 팩토리 공식 업데이트 (bitget 건드리지 않음)
sudo INSTALL_ROOT="$INSTALL_ROOT" DEPLOY_USER=ubuntu ./update_factory.sh

# 실행 권한
chmod +x factory.sh update_factory.sh deploy_quant_factory.sh scripts/*.sh 2>/dev/null || true

echo "✅ 패치 반영 완료 — 아래 §2 검증 블록 실행"
```

### 1-C. 한 줄 단축

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && rm -f .factory_runtime.lock 2>/dev/null; sudo ./update_factory.sh
```

또는:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && make update
```

### 1-D. `update_factory.sh`가 하는 일 (참고)

1. SQLite·설정 → `/var/backups/dante-pre-update/` 백업  
2. `git pull --ff-only`  
3. `deploy_quant_factory.sh` (systemd 유닛 경로 갱신)  
4. `dante-factory` / `dante-async` / `dante-dashboard` stop  
5. 스키마 마이그레이션 (ALTER only)  
6. 위 서비스 + timer 재시작  
7. (선택) 텔레그램 배포 완료 알림  

**데이터 DB·`.env`는 삭제하지 않음.**

### 1-E. cron 파일도 repo에서 갱신했을 때 (선택)

cron 템플릿을 수정한 패치라면 **한 번 더**:

```bash
sudo cp /etc/cron.d/dual-screener-factory /etc/cron.d/dual-screener-factory.bak.$(date +%Y%m%d_%H%M%S)
sudo cp "$INSTALL_ROOT/deploy/factory.crontab.example" /etc/cron.d/dual-screener-factory
sudo chmod 644 /etc/cron.d/dual-screener-factory
sudo systemctl reload cron
```

> `/etc/cron.d/`에는 `${FACTORY_USER}` 같은 변수 **사용 금지** — `CRON_FACTORY_SYNTAX_FIX.md` 참고.

### 1-F. 코인(Bitget)만 패치했을 때 (주식과 분리)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
sudo ./bitget/deploy/update_bitget.sh
```

**주식과 동시에 두 update 스크립트를 병렬 실행하지 말 것.**

---

## 2. 잘 돌아가는지 — 30초 빠른 확인

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT"

echo "=== 서비스 ==="
systemctl is-active dante-factory dante-async dante-dashboard

echo "=== 타이머 ==="
systemctl list-timers | grep -E 'dante-snapshot|dante-watchdog|dante-backup'

echo "=== cron 경로 ==="
grep -v '^#' /etc/cron.d/dual-screener-factory | grep factory.sh | head -2

echo "=== 락 ==="
ls "$INSTALL_ROOT/.factory_runtime.lock" 2>/dev/null && echo "⚠️ 락 있음" || echo "lock OK"

echo "=== 최근 로그 ==="
sudo journalctl -u dante-factory -u dante-async --since "15 min ago" --no-pager | tail -12
ls -lt logs/factory_*.log 2>/dev/null | head -3
```

**정상:** `dante-factory` · `dante-async` = `active`, timer에 NEXT 시각, 락 없음, journal에 연속 failed 없음.

---

## 3. 잘 돌아가는지 — 종합 검진 (패치 직후 권장)

```bash
# =============================================================================
# [종합 검진] KR/US 100% 상태
# =============================================================================
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT"
source venv/bin/activate
set -a && source .env && set +a

echo "=== [1] systemd ==="
for u in dante-factory dante-async dante-dashboard; do
  printf "  %-22s %s\n" "$u" "$(systemctl is-active "$u" 2>/dev/null)"
done

echo "=== [2] timer ==="
systemctl list-timers --all | grep -E 'dante-snapshot|dante-watchdog|dante-backup' || echo "  ⚠️ timer 없음"

echo "=== [3] cron (변수 \${ 없어야 함) ==="
grep '\${' /etc/cron.d/dual-screener-factory 2>/dev/null && echo "  ❌ cron에 \${ 잔존" || echo "  OK"
grep factory.sh /etc/cron.d/dual-screener-factory | head -1

echo "=== [4] DB 워터마크 · lag ==="
python - <<'PY'
from datetime import datetime
import pytz, sqlite3
from market_db_paths import MARKET_DATA_DB_PATH
from reports.daily_report_context import DailyReportContext
ctx = DailyReportContext.build(ref_kst=datetime.now(pytz.timezone("Asia/Seoul")))
conn = sqlite3.connect(MARKET_DATA_DB_PATH)
for m in ("KR", "US"):
    wm = conn.execute("""
        SELECT MAX(substr(COALESCE(NULLIF(trim(exit_date),''), entry_date),1,10))
        FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'
    """, (m,)).fetchone()[0]
    lag = ctx.lag_for(m)
    grade = "GREEN" if lag == 0 else ("YELLOW" if lag == 1 else "RED")
    print(f"  {m}: watermark={wm} anchor={ctx.anchor_for(m)} lag={lag} → {grade}")
conn.close()
PY

echo "=== [5] 텔레그램 큐 ==="
systemctl is-active dante-async
python - <<'PY'
import os, sqlite3
from telegram_message_queue import MESSAGE_QUEUE_DB_PATH
if os.path.isfile(MESSAGE_QUEUE_DB_PATH):
    c = sqlite3.connect(MESSAGE_QUEUE_DB_PATH)
    n = c.execute("SELECT COUNT(*) FROM msg_queue WHERE status='PENDING'").fetchone()[0]
    print("  PENDING:", n, "| path:", MESSAGE_QUEUE_DB_PATH)
    c.close()
else:
    print("  queue file 없음")
PY

echo "=== [6] MetaGovernor ==="
python -c "
from meta_state_store import load_meta_governor_state_unified, is_meta_state_degraded
from meta_governor import meta_state_path
s = load_meta_governor_state_unified(meta_state_path())
print('  degraded:', is_meta_state_degraded(s))
print('  last_run:', s.get('META_GOVERNOR_LAST_RUN_AT'))
" 2>/dev/null || true

echo "=== 검진 DONE ==="
```

---

## 4. 시간대별 — “오늘 스케줄이 돌았나”

| 확인 시점 (KST) | 볼 것 | 명령 |
|-----------------|-------|------|
| 평일 17:00 이후 | KR 일일 | `ls -lt $INSTALL_ROOT/logs/factory_daily_audit_kr_*.log \| head -1` |
| 화~토 07:00 이후 | US 일일 | `ls -lt $INSTALL_ROOT/logs/factory_daily_audit_us_*.log \| head -1` |
| 평일 장중 | KR 스캔 | `ls -lt $INSTALL_ROOT/logs/factory_scan_kr_*.log \| head -3` |
| 밤~새벽 | US 스캔 | `ls -lt $INSTALL_ROOT/logs/factory_scan_us_*.log \| head -3` |
| 토 10:30 이후 | 주간 | `ls -lt $INSTALL_ROOT/logs/factory_weekly_master_*.log \| head -1` |

```bash
# 최근 cron이 factory를 실행했는지
sudo grep -E 'factory\.sh|dual-screener-factory' /var/log/syslog 2>/dev/null | tail -15

# 최신 로그 tail + 실패 여부
L=$(ls -t /home/ubuntu/dante_bots/Dual-Screener-Bot/logs/factory_daily_audit_kr_*.log 2>/dev/null | head -1)
[[ -n "$L" ]] && tail -15 "$L" && grep -E 'PIPELINE ABORT|exit=' "$L" | tail -3
```

상세: `STOCK_FACTORY_SCHEDULE_VERIFICATION.md`

---

## 5. 이상할 때만 — 추가 문구

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot

# 락·좀비 정리 + factory/async 재기동
rm -f "$INSTALL_ROOT/.factory_runtime.lock"
"$INSTALL_ROOT/scripts/reset_factory_pipeline.sh"

# 주식 서비스만 재시작 (코인 건드리지 않음)
sudo systemctl restart dante-factory dante-async dante-dashboard
sudo systemctl restart dante-snapshot.timer dante-watchdog.timer dante-backup.timer

# 텔레그램만 재시작
sudo systemctl restart dante-async
sudo journalctl -u dante-async -n 40 --no-pager

# 실시간 로그
sudo journalctl -u dante-factory -u dante-async -f
```

| 증상 | 볼 문서 |
|------|---------|
| 텔레그램 먹통 | `TELEGRAM_REGRESSION_ANALYSIS.md` |
| RED·표본 0 | `REPORT_STATE_ANALYSIS.md` |
| cron 안 돎 | `CRON_FACTORY_SYNTAX_FIX.md` |
| Bitget 충돌 | `STOCK_COIN_CONFLICT_RESOLUTION.md` |

---

## 6. 매 패치 루틴 (체크리스트)

```
[로컬]  git push
[서버]  §1-B 패치 표준 블록
[서버]  §2 빠른 확인 (30초)
[서버]  §3 종합 검진 (중요 패치 시)
[선택]  cron/텔레그램/스캔 스모크
```

**성공 기준**

- [ ] `sudo ./update_factory.sh` exit 0  
- [ ] `dante-factory` · `dante-async` = `active`  
- [ ] timer 3종 NEXT 예약됨  
- [ ] cron에 `\${` 없음, `factory.sh` 경로 맞음  
- [ ] `.factory_runtime.lock` 없음  
- [ ] (선택) 텔레그램 `✅ [Dual-Screener 팩토리] ... 재기동 완료`  

---

## 7. 한 페이지 요약 (즐겨찾기용)

```bash
# ── 패치 ──
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && rm -f .factory_runtime.lock 2>/dev/null; sudo ./update_factory.sh

# ── 확인 ──
systemctl is-active dante-factory dante-async dante-dashboard
systemctl list-timers | grep dante
grep '\${' /etc/cron.d/dual-screener-factory || echo "cron OK"
ls -lt /home/ubuntu/dante_bots/Dual-Screener-Bot/logs/factory_*.log | head -5
```

---

## 8. 관련 문서

| 파일 | 내용 |
|------|------|
| `STANDARD_UPDATE_RESTART_MANUAL.md` | update_factory 7단계 상세 |
| `FACTORY_FULL_OPS_MANUAL.md` | 최초 설치·cron 등록 |
| `STOCK_FACTORY_SCHEDULE_VERIFICATION.md` | 시간대별 검증 |
| `STOCK_FACTORY_MASTER_BLUEPRINT.md` | 전체 구조 (Gemini용) |
