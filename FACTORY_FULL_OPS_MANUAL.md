# 주식 팩토리 100% 운영 명령어 매뉴얼 (Ubuntu · bitget 제외)

**범위:** `Dual-Screener-Bot` **주식 팩토리 전체**  
**제외:** `bitget/` · `update_bitget.sh` · `dante-bitget-*` · Bitget cron

**이 문서의 3가지 목적**

| # | 목적 | 해당 섹션 |
|---|------|-----------|
| 1 | 구조가 **24시간 자동**으로 돌게 세팅 | [§1 최초·자동 가동](#1-최초-설치--100-자동-가동-세팅) |
| 2 | **패치할 때마다** 코드를 최신으로 덮어쓰고 재기동 | [§2 패치·업데이트](#2-패치할-때마다--코드-덮어쓰기--재기동) |
| 3 | **100% 잘 돌아가는지** 확인 | [§3 건강 검진](#3-100-정상-가동-확인-명령어) |

---

## 경로 설정 (모든 블록 공통)

서버에서 **한 번만** 본인 경로에 맞게 고칩니다.

```bash
# ★ 실제 clone 경로 중 하나만 사용
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
# export INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot

export DEPLOY_USER=ubuntu
cd "$INSTALL_ROOT"
```

---

## 자동으로 돌아가는 구조 (한눈에)

```
┌─────────────────────────────────────────────────────────────┐
│  systemd (상시 데몬)                                         │
│  · dante-factory      → system_auto_pilot.py --daemon       │
│  · dante-async        → 텔레그램 큐                          │
│  · dante-dashboard    → Streamlit 관제 (8501)                 │
│  · dante-snapshot.timer  → DB 스냅샷 (5분)                  │
│  · dante-watchdog.timer  → watchdog.py (5분)                │
│  · dante-backup.timer    → DR 백업 (03:00)                  │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│  cron /etc/cron.d/dual-screener-factory (factory.sh)        │
│  · 평일 09~15:30 30분마다  --scan-kr                        │
│  · 평일 16:35           --daily-kr                          │
│  · 화~토 22~06시 30분마다 --scan-us                         │
│  · 화~토 06:45          --daily-us                          │
│  · 토 10:05             --weekly                            │
└─────────────────────────────────────────────────────────────┘
```

**bitget은 이 문서 범위 밖** — 주식만 켜면 위 두 축이 **100%** 입니다.

---

## 1. 최초 설치 · 100% 자동 가동 세팅

> **이미 서버에 돌고 있었다면** §1은 건너뛰고 [§2 패치](#2-패치할-때마다--코드-덮어쓰기--재기동)만 쓰면 됩니다.

### 1-A. 저장소 · venv (최초 1회)

```bash
# =============================================================================
# [최초] clone + venv — bitget 폴더는 repo 일부이나 주식 운영과 무관
# =============================================================================
set -euo pipefail

export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
sudo mkdir -p "$(dirname "$INSTALL_ROOT")"
sudo chown ubuntu:ubuntu "$(dirname "$INSTALL_ROOT")"

# 이미 clone 되어 있으면 pull만
if [[ ! -d "$INSTALL_ROOT/.git" ]]; then
  sudo -u ubuntu git clone <YOUR_REPO_URL> "$INSTALL_ROOT"
fi

cd "$INSTALL_ROOT"
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# 실행 권한
chmod +x factory.sh update_factory.sh deploy_quant_factory.sh scripts/*.sh
mkdir -p logs
```

### 1-B. .env (최초 1회 · DB·텔레그램)

```bash
cd "$INSTALL_ROOT"
chmod 600 .env   # 이미 있으면 내용만 확인

# 필수 확인 항목 (예시 — 실제 키는 서버에만)
# DB_STORAGE_PATH=/var/lib/quant-factory/data   (또는 INSTALL_ROOT)
# TELEGRAM_BOT_TOKEN=...
# TELEGRAM_CHAT_ID=...
grep -E '^DB_STORAGE_PATH|^TELEGRAM' .env || echo "⚠️ .env 키 확인 필요"
```

### 1-C. systemd 100% 등록 (상시 데몬 + 타이머)

```bash
# =============================================================================
# [최초] systemd — 주식 팩토리 전 유닛 enable
# =============================================================================
cd "$INSTALL_ROOT"
sudo INSTALL_ROOT="$INSTALL_ROOT" ./deploy_quant_factory.sh

# 활성 확인
systemctl is-active dante-factory dante-async dante-dashboard
systemctl list-timers --all | grep -E 'dante-snapshot|dante-watchdog|dante-backup'
```

### 1-D. cron 100% 등록 (스캔·일일·주간 자동)

```bash
# =============================================================================
# [최초] cron — factory.sh 스케줄 등록
# =============================================================================
cd "$INSTALL_ROOT"

# 템플릿 복사 후 FACTORY 경로를 INSTALL_ROOT로 수정
sudo cp deploy/factory.crontab.example /etc/cron.d/dual-screener-factory
sudo sed -i "s|FACTORY=.*|FACTORY=${INSTALL_ROOT}|" /etc/cron.d/dual-screener-factory
sudo chmod 644 /etc/cron.d/dual-screener-factory

# cron 재로드
sudo systemctl reload cron 2>/dev/null || sudo service cron reload

# 등록 확인
grep -E 'FACTORY|factory.sh' /etc/cron.d/dual-screener-factory
```

### 1-E. 최초 세팅 후 즉시 검증

```bash
cd "$INSTALL_ROOT"
sudo ./update_factory.sh          # 유닛·venv·스키마까지 한 번에 맞춤
# 아래 §3 전체 건강 검진 블록 실행
```

---

## 2. 패치할 때마다 — 코드 덮어쓰기 · 재기동

### 2-A. 표준 방법 (권장) — `git pull`로 코드만 교체

**동작 요약**

| 항목 | 패치 시 동작 |
|------|----------------|
| **Python 코드** | `git pull --ff-only` → **최신으로 덮어씀** |
| **systemd 유닛** | `deploy_quant_factory.sh` → 경로·ExecStart **재설치** |
| **venv 엔진** | 구 `.venv` 잔존 프로세스 종료 후 `venv`로 **재기동** |
| **SQLite·`.env`** | **삭제 안 함** — pull 전 `/var/backups/dante-pre-update/`에 **자동 백업** |

로컬에서 `git push` 한 뒤, Ubuntu에서 **아래 한 블록만** 실행합니다.

```bash
# =============================================================================
# [패치 표준] 코드 덮어쓰기 + 서비스 100% 재기동 (bitget 제외)
# =============================================================================
set -euo pipefail

export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
export DEPLOY_USER="${DEPLOY_USER:-ubuntu}"

cd "$INSTALL_ROOT"

# cron/factory 일회 작업 락 해제 (있을 때만)
rm -f "$INSTALL_ROOT/.factory_runtime.lock" 2>/dev/null || true

# ★ 핵심: 백업 → git pull → 유닛 재배포 → stop → migrate → restart → timer
sudo INSTALL_ROOT="$INSTALL_ROOT" DEPLOY_USER="$DEPLOY_USER" ./update_factory.sh

# 권한·cron 경로 재확인
chmod +x factory.sh update_factory.sh deploy_quant_factory.sh scripts/*.sh 2>/dev/null || true
grep "^FACTORY=" /etc/cron.d/dual-screener-factory

echo "✅ 패치 반영 완료 — §3 건강 검진 실행"
```

**한 줄 단축**

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo ./update_factory.sh
# 또는: make update
```

**성공 신호:** 텔레그램 `✅ [Dual-Screener 팩토리] ... 재기동 완료` (선택)

---

### 2-B. 폴더 통째로 새로 올린 경우 (rsync/scp · git 없음)

코드를 **통째로 덮어쓴 뒤** 반드시 systemd·서비스 재기동까지 해야 합니다.

```bash
# =============================================================================
# [패치 대안] 로컬 → 서버 rsync 후 재기동 (DB·.env는 제외하고 동기화 권장)
# =============================================================================
# 로컬 PC에서 (예시):
# rsync -avz --exclude '.git' --exclude 'venv' --exclude '*.sqlite' \
#   --exclude '.env' --exclude 'logs/' \
#   ./Dual-Screener-Bot/ ubuntu@SERVER:/home/ubuntu/dante_bots/Dual-Screener-Bot/

# 서버에서:
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT"
chmod +x factory.sh update_factory.sh deploy_quant_factory.sh scripts/*.sh
sudo INSTALL_ROOT="$INSTALL_ROOT" ./update_factory.sh
```

> **주의:** `rm -rf` 로 INSTALL_ROOT 통째 삭제 후 재clone 하면 **같은 경로의 `.env`·DB도 날아갈 수 있음**. 반드시 백업 후 진행.

---

### 2-C. 패치 전·후 안전 종료만 따로 할 때

`update_factory.sh` 실패 시 수동 분리 실행:

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT"

# --- 종료 ---
rm -f .factory_runtime.lock
sudo systemctl stop dante-factory dante-dashboard dante-async
pkill -u ubuntu -f "${INSTALL_ROOT}/factory.sh" 2>/dev/null || true

# --- 코드 덮어쓰기 ---
sudo -u ubuntu git -C "$INSTALL_ROOT" pull --ff-only

# --- 재가동 ---
sudo INSTALL_ROOT="$INSTALL_ROOT" ./deploy_quant_factory.sh
sudo systemctl restart dante-factory dante-dashboard dante-async
sudo systemctl restart dante-snapshot.timer dante-watchdog.timer dante-backup.timer
```

---

### 2-D. 패치 후 데이터·파이프라인이 오래 멈춰 있었을 때 (선택)

코드 패치와 별도 — **13일 공백** 등 복구 시에만:

```bash
cd "$INSTALL_ROOT"
chmod +x scripts/*.sh

# 락·좀비 정리 + dante-factory 재기동
./scripts/reset_factory_pipeline.sh

# OHLCV·exit_date·track·리포트 (장시간 — 필요 시)
# ./scripts/master_sync_kr_us.sh
# ./scripts/force_data_sync.sh
# ./scripts/force_sample_recovery.sh
```

---

## 3. 100% 정상 가동 확인 명령어

패치 직후·매일 아침·장애 의심 시 **복붙용** 검진 블록입니다.

### 3-A. 원샷 종합 검진 (권장)

```bash
# =============================================================================
# [건강 검진] 주식 팩토리 100% 상태 체크
# =============================================================================
set -euo pipefail

export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT"
source .env 2>/dev/null || true
source venv/bin/activate

echo ""
echo "========== [1/7] systemd 서비스 =========="
for u in dante-factory dante-async dante-dashboard; do
  printf "%-22s %s\n" "$u" "$(systemctl is-active "$u" 2>/dev/null || echo unknown)"
done

echo ""
echo "========== [2/7] systemd 타이머 =========="
systemctl list-timers --all 2>/dev/null | grep -E 'dante-snapshot|dante-watchdog|dante-backup' || echo "⚠️ timer 없음"

echo ""
echo "========== [3/7] cron (factory.sh) =========="
if [[ -f /etc/cron.d/dual-screener-factory ]]; then
  grep -E '^FACTORY=|factory.sh' /etc/cron.d/dual-screener-factory
else
  echo "⚠️ /etc/cron.d/dual-screener-factory 없음 — §1-D 실행 필요"
fi

echo ""
echo "========== [4/7] factory 락·프로세스 =========="
ls -la .factory_runtime.lock 2>/dev/null || echo "lock: 없음 (정상)"
pgrep -af "${INSTALL_ROOT}/(factory.sh|system_auto_pilot.py --daemon)" 2>/dev/null | head -5 || echo "관련 프로세스 없음"

echo ""
echo "========== [5/7] DB 워터마크 · Staleness lag =========="
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

echo ""
echo "========== [6/7] MetaGovernor · 최근 로그 에러 =========="
python -c "
from meta_state_store import load_meta_governor_state_unified, is_meta_state_degraded
from meta_governor import meta_state_path
s = load_meta_governor_state_unified(meta_state_path())
print('  degraded:', is_meta_state_degraded(s))
print('  last_run:', s.get('META_GOVERNOR_LAST_RUN_AT'))
" 2>/dev/null || echo "  MetaGovernor 조회 스킵"

sudo journalctl -u dante-factory -u dante-async --since "30 min ago" --no-pager -p err 2>/dev/null | tail -10 \
  || echo "  최근 ERROR 없음 또는 journal 접근 필요"

echo ""
echo "========== [7/7] 판정 기준 =========="
cat <<'EOF'
  ✅ 정상: dante-factory/async = active, timer 3개 next 실행 예약,
           cron FACTORY 경로 일치, lock 없음, lag 0~1 (GREEN/YELLOW)
  ⚠️  주의: lag >= 2 (RED) → 파이프라인·track 미실행 — REPORT_STATE_ANALYSIS.md 참고
  ❌ 이상: dante-factory failed → journalctl -u dante-factory -n 80 --no-pager
EOF
echo ""
echo "========== HEALTH CHECK DONE =========="
```

---

### 3-B. 빠른 30초 체크

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot

systemctl is-active dante-factory dante-async dante-dashboard
systemctl list-timers | grep dante
grep FACTORY /etc/cron.d/dual-screener-factory
ls .factory_runtime.lock 2>/dev/null && echo "⚠️ lock 존재" || echo "lock OK"
sudo journalctl -u dante-factory --since "15 min ago" --no-pager | tail -15
```

---

### 3-C. 실시간 로그 모니터링

```bash
# 상시 데몬 + 텔레그램 + 워치독
sudo journalctl -u dante-factory -u dante-async -u dante-watchdog -f

# cron이 돌린 factory.sh 로그 (파일)
tail -f /home/ubuntu/dante_bots/Dual-Screener-Bot/logs/factory_*.log
```

---

### 3-D. 수동 스모크 (자동 cron 대신 1회 돌려보기)

```bash
cd "$INSTALL_ROOT"
source venv/bin/activate

# 장외 복구 테스트 시
# ./factory.sh --scan-kr --force-scan-outside-session

# 일일 KR만 (텔레그램 발송)
# ./factory.sh --daily-kr

# 전체 일일 (KR+US)
# ./factory.sh --daily
```

---

### 3-E. 정상 판정 체크리스트

| 항목 | 정상 | 이상 시 조치 |
|------|------|-------------|
| `dante-factory` | `active` | `journalctl -u dante-factory -n 80` |
| `dante-async` | `active` | 텔레그램 큐·`.env` 토큰 확인 |
| `dante-dashboard` | `active` (선택) | `8501` 포트·Streamlit 로그 |
| 3개 timer | `active` + NEXT 표시 | `sudo systemctl enable --now dante-snapshot.timer` |
| cron | `FACTORY=$INSTALL_ROOT` | §1-D 재등록 |
| `.factory_runtime.lock` | 없음 | `rm -f` 후 `reset_factory_pipeline.sh` |
| KR/US lag | 0~1 | `master_sync` / `force_data_sync` (데이터 공백 시) |
| 텔레그램 RED | lag≥2일 때만 | 버그 아님 — `REPORT_STATE_ANALYSIS.md` |

---

## 4. 일상 운영 치트시트

```bash
# ── 패치 (가장 자주 씀) ──
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo ./update_factory.sh

# ── 상태 ──
systemctl is-active dante-factory dante-async dante-dashboard
systemctl list-timers | grep dante

# ── 건강 검진 ──
# → §3-A 블록 전체 복붙

# ── 락·좀비 ──
rm -f /home/ubuntu/dante_bots/Dual-Screener-Bot/.factory_runtime.lock
./scripts/reset_factory_pipeline.sh

# ── 로그 ──
sudo journalctl -u dante-factory -u dante-async --since "1 hour ago" --no-pager | tail -50
```

---

## 5. 관련 문서

| 파일 | 내용 |
|------|------|
| `STANDARD_UPDATE_RESTART_MANUAL.md` | 배포 7단계 상세 |
| `REPORT_STATE_ANALYSIS.md` | RED·Fail-safe·표본 0 (버그 vs 방어) |
| `RUNBOOK.md` | journalctl·systemd 상세 |
| `deploy/factory.crontab.example` | cron 원본 템플릿 |

**원칙:** 주식 팩토리 = **`deploy_quant_factory.sh`(최초) + cron(§1-D) + 패치마다 `update_factory.sh` + §3 검진**. bitget은 별도 문서·스크립트로만 관리.
