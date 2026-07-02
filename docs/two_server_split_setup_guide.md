# Two-Server 분리 세팅 가이드 (4GB × 2)

> **전제 (2026-07)**  
> | 서버 | IP | 역할 |
> |------|-----|------|
> | `Ubuntu-4GB-Bot` | 52.78.29.151 | **주식 (KR/US)** |
> | `Ubuntu-4GB-Bot-2` | 15.165.236.69 | **코인 (Bitget)** |

공통: Lightsail 서울, Ubuntu 22.04, 4GB / 2 vCPU / 80GB SSD.

---

## 0. 한눈에 (순서)

```
[1] 양쪽 공통: OS 패키지 + git clone + venv
[2] 구 서버(52.78): Bitget 중지·비활성화  ← 먼저!
[3] 구 서버: 주식만 재확인 (deploy_quant_factory)
[4] 신 서버(15.165): repo + Bitget 설치
[5] 구 → 신: Bitget 데이터 rsync
[6] 신 서버: Bitget 기동 + health
[7] 구 서버: Bitget 데이터 정리(디스크 확보) — 검증 후
```

---

## 1. 공통 변수 (양쪽 서버)

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
export REPO_URL=<your-github-repo-url>   # private 이면 deploy key 설정
```

---

## 2. 양쪽 서버 — 최초 1회 (신규 Bot-2 필수, Bot-1은 이미 있으면 스킵)

SSH로 각 서버 접속 (`ubuntu` 유저).

```bash
sudo apt-get update
sudo apt-get install -y git python3 python3-venv python3-pip sqlite3 rsync curl

sudo mkdir -p /home/ubuntu/dante_bots
sudo chown ubuntu:ubuntu /home/ubuntu/dante_bots
cd /home/ubuntu/dante_bots

# 아직 없을 때만 clone
git clone "$REPO_URL" Dual-Screener-Bot
cd Dual-Screener-Bot

python3 -m venv venv
./venv/bin/pip install -U pip wheel
./venv/bin/pip install -r requirements.txt
./venv/bin/pip install -r bitget/requirements-bitget.txt
```

---

## 3. 서버 A — `Ubuntu-4GB-Bot` (52.78.29.151) 주식 전용

### 3.1 Bitget 먼저 끄기 (이중 실행 방지)

```bash
cd $INSTALL_ROOT

# Bitget systemd 전부 중지·비활성
sudo systemctl stop dante-bitget-factory dante-bitget-ws dante-bitget-queue-worker \
  dante-bitget-async dante-bitget-dashboard dante-bitget-heatmap 2>/dev/null || true
sudo systemctl disable dante-bitget-factory dante-bitget-ws dante-bitget-queue-worker \
  dante-bitget-async dante-bitget-dashboard dante-bitget-heatmap 2>/dev/null || true
sudo systemctl stop dante-bitget-watchdog.timer dante-bitget-snapshot.timer 2>/dev/null || true
sudo systemctl disable dante-bitget-watchdog.timer dante-bitget-snapshot.timer 2>/dev/null || true

# Bitget cron 제거
sudo rm -f /etc/cron.d/dual-screener-bitget
sudo systemctl reload cron 2>/dev/null || true
```

### 3.2 주식 `.env` 확인

`$INSTALL_ROOT/.env` 예시:

```bash
DB_STORAGE_PATH=/var/lib/quant-factory/data   # 권장: 코드 루트와 분리
MAX_WORKERS=1
TELEGRAM_CONCURRENCY_LIMIT=4

# 코인 서버로 옮겼으면 canary는 당분간 OFF (또는 rsync 경로 설정)
CRYPTO_CANARY_PENALTY_ENABLED=0
```

```bash
sudo mkdir -p /var/lib/quant-factory/data
sudo chown ubuntu:ubuntu /var/lib/quant-factory/data
```

기존 `market_data.sqlite`가 `INSTALL_ROOT`에 있으면 `DB_STORAGE_PATH`로 **이전** (한 번만):

```bash
mv $INSTALL_ROOT/market_data.sqlite /var/lib/quant-factory/data/ 2>/dev/null || true
mv $INSTALL_ROOT/treasury_state.json /var/lib/quant-factory/data/ 2>/dev/null || true
mv $INSTALL_ROOT/system_config.json /var/lib/quant-factory/data/ 2>/dev/null || true
```

### 3.3 주식 스택 설치·기동

```bash
cd $INSTALL_ROOT
git pull --ff-only
sudo INSTALL_ROOT=$INSTALL_ROOT ./deploy_quant_factory.sh
```

검증:

```bash
systemctl is-active dante-factory dante-async dante-dashboard
ls /etc/cron.d/dual-screener-factory-kr /etc/cron.d/dual-screener-factory-us
bash deploy/audit_factory_stack.sh
```

### 3.4 (검증 후) 디스크 확보 — Bitget 데이터 삭제

**Bot-2에서 Bitget health OK 확인 후에만** 실행.

```bash
# bitget 데이터 루트 확인 후 삭제 (경로는 환경마다 다름)
python3 -c "from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())"
# 예: rm -f /path/to/bitget_market_data.sqlite ...  (백업 후)
```

---

## 4. 서버 B — `Ubuntu-4GB-Bot-2` (15.165.236.69) 코인 전용

### 4.1 SSH 키 (구 서버 → 신 서버 rsync용)

Bot-2에서:

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
cat ~/.ssh/id_ed25519.pub   # 이 줄을 Bot-1의 ubuntu ~/.ssh/authorized_keys 에 추가
```

### 4.2 Bitget `.env`

`$INSTALL_ROOT/bitget/.env` (또는 루트 `.env`에 병합):

```bash
BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_YIELD_TO_FACTORY=0
BITGET_MAX_WORKERS=4
BITGET_SKIP_INLINE_TELEGRAM=1
BITGET_ASYNC_TELEGRAM=1

BITGET_ACCESS_KEY=...
BITGET_SECRET_KEY=...
BITGET_PASSPHRASE=...
BITGET_BOT_TOKEN=...
BITGET_BOT_CHAT_ID=...
```

```bash
sudo mkdir -p /var/lib/quant-bitget/data
sudo chown ubuntu:ubuntu /var/lib/quant-bitget/data
chmod 600 $INSTALL_ROOT/bitget/.env
```

### 4.3 구 서버에서 Bitget 데이터 복사

**Bot-2**에서 실행 (Bot-1 IP):

```bash
OLD=ubuntu@52.78.29.151
OLD_DATA=/home/ubuntu/dante_bots/Dual-Screener-Bot   # Bot-1 실제 경로에 맞게 조정

# bitget DB·설정 (락 파일 제외)
rsync -avz --progress \
  --exclude='.bitget_runtime.lock' \
  --exclude='.bitget_data_refresh.lock' \
  $OLD:$OLD_DATA/bitget/bitget_market_data.sqlite \
  /var/lib/quant-bitget/data/ 2>/dev/null || true

rsync -avz --progress \
  $OLD:$OLD_DATA/bitget/bitget_system_config.json \
  /var/lib/quant-bitget/data/ 2>/dev/null || true

# DB_STORAGE_PATH 쓰던 경우 Bot-1에서 경로 확인 후 rsync
# ssh $OLD 'python3 -c "from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())"'
```

`bitget/.env` 도 Bot-1에서 복사:

```bash
scp $OLD:$OLD_DATA/bitget/.env $INSTALL_ROOT/bitget/.env
chmod 600 $INSTALL_ROOT/bitget/.env
```

### 4.4 Bitget 스택 설치·기동

```bash
cd $INSTALL_ROOT
git pull --ff-only

sudo INSTALL_ROOT=$INSTALL_ROOT ./bitget/deploy/deploy_bitget_factory.sh
sudo INSTALL_ROOT=$INSTALL_ROOT bash bitget/deploy/install_bitget_cron.sh

sudo systemctl start dante-bitget-ws dante-bitget-async dante-bitget-factory dante-bitget-queue-worker
sudo systemctl start dante-bitget-watchdog.timer dante-bitget-snapshot.timer

# dashboard/heatmap 은 RAM 절약 시 나중에:
# sudo systemctl start dante-bitget-dashboard dante-bitget-heatmap
```

검증:

```bash
systemctl is-active dante-bitget-factory dante-bitget-ws dante-bitget-queue-worker dante-bitget-async
./bitget/deploy/bitget.sh --health
journalctl -u dante-bitget-factory -n 50 --no-pager
```

### 4.5 주식 유닛은 Bot-2에 없어야 함

```bash
systemctl is-active dante-factory 2>/dev/null || echo "OK: no equity factory"
```

있으면 `disable` — Bot-2는 코인만.

---

## 5. 업데이트 (이후 일상)

| 서버 | 명령 |
|------|------|
| Bot-1 주식 | `cd $INSTALL_ROOT && sudo ./update_factory.sh` |
| Bot-2 코인 | `sudo INSTALL_ROOT=$INSTALL_ROOT ./bitget/deploy/update_bitget.sh` |

**서로의 update 스크립트를 다른 서버에서 실행하지 말 것.**

---

## 6. 최종 체크리스트

| # | Bot-1 (주식) | Bot-2 (코인) |
|---|--------------|--------------|
| 1 | `dante-factory` active | `dante-bitget-factory` active |
| 2 | factory-kr/us cron 존재 | dual-screener-bitget cron 존재 |
| 3 | `dante-bitget-*` **disabled** | `dante-factory` **없음/disabled** |
| 4 | `market_data.sqlite` 로컬 | `bitget_market_data.sqlite` 로컬 |
| 5 | `audit_factory_stack.sh` OK | `bitget.sh --health` OK |
| 6 | 디스크 사용률 < 80% | 디스크 사용률 < 80% |

---

## 7. 트러블슈팅

| 증상 | 조치 |
|------|------|
| 코인 텔레그램 안 옴 | `BITGET_BOT_TOKEN` / `CHAT_ID`, `dante-bitget-async` active |
| `database is locked` | 락 파일 삭제: `reset_bitget_pipeline.sh` (Bot-2만) |
| 주식 SKIPPED_LOCK | Bot-1: `scripts/reset_factory_pipeline.sh` |
| 양쪽 동시에 Bitget 돔 | Bot-1에서 bitget disable 재확인 |
| RAM 부족 | Bot-2: dashboard/heatmap 끄기, `BITGET_MAX_WORKERS=2` |

---

*관련: `docs/ubuntu_server_procurement_guide_2026.md`, `RUNBOOK.md`, `bitget/RUNBOOK.md`*
