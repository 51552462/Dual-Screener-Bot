# 표준 배포 및 재시작 매뉴얼 (Ubuntu · 주식 팩토리)

**범위:** `Dual-Screener-Bot` **주식 팩토리** (`bitget/` · `update_bitget.sh` · `dante-bitget-*` **제외**)  
**목적:** 패치 반영 → 서비스 재기동 → 정상 가동 확인을 **매번 동일한 순서**로 수행

---

## 1. 시스템 환경 요약

### 1.1 경로 SSOT

```bash
# 서버 clone 경로 (환경에 맞게 하나만 사용)
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
# export INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
```

| 항목 | 값 |
|------|-----|
| 코드·venv | `$INSTALL_ROOT` (`venv/bin/python` 표준) |
| 환경변수 | `$INSTALL_ROOT/.env` (`DB_STORAGE_PATH`, Telegram 등) |
| 데이터 DB | `{DB_STORAGE_PATH}/market_data.sqlite` 또는 `$INSTALL_ROOT/*.sqlite` |
| Factory 락 | `$INSTALL_ROOT/.factory_runtime.lock` |
| cron SSOT | `/etc/cron.d/dual-screener-factory` (`factory.sh`) |

### 1.2 systemd 유닛 (주식)

| 유닛 | 역할 |
|------|------|
| `dante-factory.service` | `system_auto_pilot.py --daemon` (위성·유지보수) |
| `dante-async.service` | `async_telegram_daemon.py` (텔레그램 큐) |
| `dante-dashboard.service` | Streamlit 관제 (8501) |
| `dante-snapshot.timer` | CQRS DB 스냅샷 (5분) |
| `dante-watchdog.timer` | `watchdog.py` (5분) |
| `dante-backup.timer` | DR 백업 (03:00) |

> 레거시 `dante-main` / `dante-streamlit` 경로는 `deploy/ubuntu/install.sh` — **신규 배포는 `deploy_quant_factory.sh` SSOT.**

### 1.3 공식 업데이트 스크립트

| 스크립트 | 역할 |
|----------|------|
| **`update_factory.sh`** | **표준 1줄 배포** — 백업 → git pull → 유닛 재배포 → stop → 마이그레이션 → restart → timer |
| `deploy_quant_factory.sh` | systemd 유닛·timer 설치/갱신 (`update_factory` [3/7]에서 호출) |
| `deploy/ubuntu/post_update_notify.sh` | 배포 성공 텔레그램 알림 (`update_factory` 말미) |
| `factory.sh` | cron 일회성 파이프라인 (scan/daily/weekly) — **데몬과 별도** |

`update_factory.sh` 내부 7단계:

1. SQLite·아티팩트 → `/var/backups/dante-pre-update/<UTC>/`
2. `git pull --ff-only` (ubuntu 유저)
3. `deploy_quant_factory.sh` (ExecStart·경로 갱신)
4. `systemctl stop` dante-factory / dashboard / async
5. 구버전 `.venv` 잔존 프로세스 SIGTERM + `sqlite_schema_guard` ALTER
6. `systemctl restart` dante-factory / dashboard / async
7. timer restart + `post_update_notify`

---

## 2. 표준 배포 (권장 — 복붙용)

패치를 **로컬에서 commit & push** 한 뒤, Ubuntu에서 아래 **한 블록** 실행.

```bash
# =============================================================================
# STANDARD DEPLOY — Dual-Screener 주식 팩토리 (bitget 제외)
# =============================================================================
set -euo pipefail

export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
export DEPLOY_USER="${DEPLOY_USER:-ubuntu}"

cd "$INSTALL_ROOT"

# --- [선행] factory.sh 일회 작업이 돌고 있으면 락만 해제 (선택) ---
rm -f "$INSTALL_ROOT/.factory_runtime.lock" 2>/dev/null || true

# --- [핵심] 공식 업데이트 (백업·pull·stop·migrate·restart·notify) ---
sudo INSTALL_ROOT="$INSTALL_ROOT" DEPLOY_USER="$DEPLOY_USER" ./update_factory.sh

# --- [후행] 실행 권한·cron 경로 점검 ---
chmod +x factory.sh update_factory.sh deploy_quant_factory.sh scripts/*.sh 2>/dev/null || true

echo "=== DEPLOY DONE ==="
```

동일 동작 단축:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo ./update_factory.sh
# 또는
make update
```

---

## 3. 수동 파이프라인 (단계별 — 문제 발생 시)

`update_factory.sh`가 실패하거나 단계별 확인이 필요할 때.

### ③-① 안전 종료 (Stop / Kill)

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT"

# factory.sh cron 작업이 잡고 있는 락 해제
rm -f .factory_runtime.lock

# 장기 서비스 graceful stop (데이터 파일은 건드리지 않음)
sudo systemctl stop dante-factory.service dante-dashboard.service dante-async.service

# INSTALL_ROOT 아래 일회성 factory.sh 프로세스 정리 (있을 때만)
pkill -u ubuntu -f "${INSTALL_ROOT}/factory.sh" 2>/dev/null || true
sleep 2

# 상태 확인 (inactive 이상)
systemctl is-active dante-factory dante-async dante-dashboard || true
```

> **하지 말 것:** `kill -9` on random python, `systemctl stop` on `dante-bitget-*`, `git clean -fdx` (DB 삭제 위험)

---

### ③-② 최신 코드 반영 (Pull · 권한)

```bash
cd "$INSTALL_ROOT"

# 변경사항이 서버에만 있으면 pull 전에 stash/정리 (팀 정책에 따름)
sudo -u ubuntu git -C "$INSTALL_ROOT" status
sudo -u ubuntu git -C "$INSTALL_ROOT" pull --ff-only

# 스크립트 실행 권한
chmod +x factory.sh update_factory.sh deploy_quant_factory.sh
chmod +x scripts/*.sh 2>/dev/null || true

# systemd 유닛 경로 재설치 (INSTALL_ROOT 변경 시 필수)
sudo INSTALL_ROOT="$INSTALL_ROOT" ./deploy_quant_factory.sh
```

코드를 **rsync/scp로만** 올린 경우:

```bash
# git pull 대신 파일 동기화 후 반드시:
sudo INSTALL_ROOT="$INSTALL_ROOT" ./deploy_quant_factory.sh
sudo INSTALL_ROOT="$INSTALL_ROOT" DEPLOY_USER=ubuntu ./update_factory.sh
# 또는 stop → migrate → restart 구간만 수동 실행 (§3-③)
```

---

### ③-③ 100% 재가동 (Restart · Timer · Cron)

```bash
# 스키마 마이그레이션 (ALTER only — 데이터 보존)
sudo -E -u ubuntu env INSTALL_ROOT="$INSTALL_ROOT" PYTHONPATH="$INSTALL_ROOT" \
  "$INSTALL_ROOT/venv/bin/python" -c "
import sqlite_schema_guard
sqlite_schema_guard.ensure_market_db_core_schema(heal=True, heal_snapshot=True)
print('schema guard OK')
"

# 서비스 재기동
sudo systemctl daemon-reload
sudo systemctl restart dante-factory.service dante-dashboard.service dante-async.service

# 주기 작업 타이머
sudo systemctl enable dante-snapshot.timer dante-watchdog.timer dante-backup.timer
sudo systemctl restart dante-snapshot.timer dante-watchdog.timer dante-backup.timer

# cron (factory.sh 스케줄) — FACTORY 경로가 INSTALL_ROOT와 일치하는지
grep FACTORY /etc/cron.d/dual-screener-factory 2>/dev/null || true
# 불일치 시:
# sudo sed -i "s|FACTORY=.*|FACTORY=${INSTALL_ROOT}|" /etc/cron.d/dual-screener-factory
sudo systemctl reload cron 2>/dev/null || sudo service cron reload
```

---

### ③-④ 건강 검진 (Health Check)

```bash
cd "$INSTALL_ROOT"
source .env 2>/dev/null || true
source venv/bin/activate

echo "=== [1] systemd active ==="
systemctl is-active dante-factory dante-async dante-dashboard
systemctl list-timers --all | grep -E 'dante-snapshot|dante-watchdog|dante-backup'

echo "=== [2] 최근 로그 (에러 없는지) ==="
sudo journalctl -u dante-factory -u dante-async --since "10 min ago" --no-pager | tail -40

echo "=== [3] DB·워터마크 ==="
python - <<'PY'
from datetime import datetime
import pytz, sqlite3
from market_db_paths import MARKET_DATA_DB_PATH
from reports.daily_report_context import DailyReportContext
ctx = DailyReportContext.build(ref_kst=datetime.now(pytz.timezone("Asia/Seoul")))
conn = sqlite3.connect(MARKET_DATA_DB_PATH)
for m in ("KR","US"):
    wm = conn.execute("""
        SELECT MAX(substr(COALESCE(NULLIF(trim(exit_date),''), entry_date),1,10))
        FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'
    """, (m,)).fetchone()[0]
    print(m, "watermark", wm, "lag", ctx.lag_for(m))
conn.close()
PY

echo "=== [4] MetaGovernor ==="
python -c "
from meta_state_store import load_meta_governor_state_unified, is_meta_state_degraded
from meta_governor import meta_state_path
s = load_meta_governor_state_unified(meta_state_path())
print('degraded', is_meta_state_degraded(s), 'last', s.get('META_GOVERNOR_LAST_RUN_AT'))
"

echo "=== [5] factory 락 없음 ==="
ls -la .factory_runtime.lock 2>/dev/null || echo "lock clear"

echo "=== HEALTH CHECK DONE ==="
```

실시간 로그 팔로우:

```bash
sudo journalctl -u dante-factory -u dante-dashboard -u dante-async -u dante-watchdog -f
```

---

## 4. 패치 후 선택 검증 (스모크 테스트)

배포 직후 **수동 1회** (장외면 `--force-scan-outside-session`):

```bash
cd "$INSTALL_ROOT" && source venv/bin/activate

# 일일 리포트 스모크 (텔레그램 발송)
# ./factory.sh --daily-kr --lock-timeout 600

# 주간만 재발송
# ./factory.sh --weekly

# dry-run 주간
# python weekly_flow_report.py
```

---

## 5. 트러블슈팅

| 증상 | 조치 |
|------|------|
| `git pull` 충돌 | 서버 로컬 변경 stash/폐기 후 pull; **DB·`.env`는 커밋 금지** |
| `dante-factory` failed | `journalctl -u dante-factory -n 80 --no-pager` |
| `factory lock busy` | `rm -f .factory_runtime.lock` 후 cron 재실행 |
| venv 없음 | `python3 -m venv venv && ./venv/bin/pip install -r requirements.txt` (팀 표준에 따름) |
| timer inactive | `sudo systemctl enable --now dante-snapshot.timer` |
| Bitget만 갱신 필요 | **`bitget/deploy/update_bitget.sh`** — 이 매뉴얼과 **분리** |

---

## 6. 배포 전·후 체크리스트

**배포 전**

- [ ] 패치 `git push` 완료
- [ ] `.env` / `DB_STORAGE_PATH` 백업 불필요 (`update_factory`가 자동 백업)
- [ ] 장중 대량 `factory.sh` 실행 중이 아님 (또는 락 해제 예정)

**배포**

- [ ] `sudo ./update_factory.sh` exit 0
- [ ] 텔레그램 `✅ [Dual-Screener 팩토리] ... 재기동 완료` 수신 (선택)

**배포 후**

- [ ] `dante-factory` / `dante-async` = `active`
- [ ] timer 3개 `active` 또는 next run 예약됨
- [ ] `journalctl` 최근 10분 ERROR 없음
- [ ] 워터마크·MetaGovernor degraded 아님

---

## 7. 관련 문서

| 파일 | 내용 |
|------|------|
| `RUNBOOK.md` | systemd·journalctl 상세 |
| `UBUNTU_FACTORY_FULL_RESTORE.md` | 장애 복구·데이터 동기화 |
| `FACTORY_RECOVERY_MASTER.md` | 4대 증상 통합 복구 |
| `deploy/factory.crontab.example` | cron 템플릿 |
| `Makefile` | `make update` → `update_factory.sh` |

---

## 8. 한 페이지 치트시트

```bash
# 배포
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo ./update_factory.sh

# 상태
systemctl is-active dante-factory dante-async dante-dashboard
systemctl list-timers | grep dante

# 로그
sudo journalctl -u dante-factory -u dante-async --since "30 min ago" --no-pager | tail -50

# 락
rm -f /home/ubuntu/dante_bots/Dual-Screener-Bot/.factory_runtime.lock
```

**원칙:** 주식 팩토리 패치 = **`sudo ./update_factory.sh` 한 번**이 기본. 수동 파이프라인은 실패 분석·학습용.
