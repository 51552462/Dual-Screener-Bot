# Ubuntu 서버 — Bitget 코인 팩토리 격리 배포 가이드

> **목표:** 같은 서버·같은 Git 저장소에서 **미국/한국 주식 팩토리**와 **Bitget 코인 팩토리**를  
> **겹치지 않는 “별도 방”** 으로 동시에 운영한다.  
> 주식 쪽 `dante-factory` / `update_factory.sh` 는 **건드리지 않는다.**

---

## 1. “방”이란 무엇인가

코드는 **하나의 저장소**(`Dual-Screener-Bot`)를 공유하지만, 런타임은 아래 5가지로 **완전 분리**한다.

| 격리 축 | 주식 (KR/US) | Bitget (코인) |
|---------|--------------|---------------|
| systemd 유닛 | `dante-factory`, `dante-dashboard`, `dante-async`, … | `dante-bitget-*` 전용 |
| 업데이트 스크립트 | `sudo ./update_factory.sh` | `sudo ./bitget/deploy/update_bitget.sh` |
| 데이터 디렉터리 | `DB_STORAGE_PATH` → `market_data.sqlite` 등 | `BITGET_DB_STORAGE_PATH` → `bitget_market_data.sqlite` 등 |
| Streamlit 포트 | **8501** | **8511** (관제탑), **8512** (히트맵) |
| Telegram | `TELEGRAM_*` / 주식 봇 토큰 | `BITGET_TELEGRAM_*` / Bitget 전용 봇·채널 |
| cron | `factory.sh --scan-kr/us/…` | `bitget/deploy/bitget.sh --scan-all/…` |
| ops DB | `ops_events.sqlite` (루트) | `bitget_ops_events.sqlite` (Bitget data dir) |

**공유해도 되는 것:** Git clone, `venv`, Python 패키지, `.env` 파일 하나(키만 분리해서 기입).

**절대 공유하면 안 되는 것:** SQLite DB 파일, Telegram 큐 DB, cron job 이름·스케줄이 같은 스크립트를 두 번 호출하는 설정.

---

## 2. 서버 사전 준비

### 2.1 OS·패키지

```bash
sudo apt update
sudo apt install -y git python3 python3-venv sqlite3 curl
# Streamlit 대시보드용 (이미 venv에 있으면 생략)
# pip install -r requirements.txt 는 아래 venv 단계에서
```

### 2.2 방화벽 (선택)

외부에서 대시보드 접속 시 **포트를 구분**한다.

| 포트 | 용도 |
|------|------|
| 8501 | 주식 Streamlit (`dante-dashboard`) |
| 8511 | Bitget 관제탑 |
| 8512 | Bitget 히트맵 |

```bash
# 예: UFW
sudo ufw allow 8501/tcp comment 'stock dashboard'
sudo ufw allow 8511/tcp comment 'bitget dashboard'
sudo ufw allow 8512/tcp comment 'bitget heatmap'
```

---

## 3. 저장소 업로드 (clone 또는 pull)

주식 팩토리가 **이미 같은 경로**에 있다면 **새로 clone 하지 않고** `git pull` 만 하면 된다.

```bash
# 신규 서버 예시
sudo mkdir -p /home/ubuntu
sudo chown ubuntu:ubuntu /home/ubuntu
sudo -u ubuntu git clone <YOUR_REPO_URL> /home/ubuntu/Dual-Screener-Bot
cd /home/ubuntu/Dual-Screener-Bot
```

**권장 `INSTALL_ROOT`:** `/home/ubuntu/Dual-Screener-Bot`  
(주식 `deploy_quant_factory.sh` 와 동일 경로를 쓰면 venv·코드를 한 번만 관리할 수 있다.)

---

## 4. Python venv (주식·Bitget 공용 1개)

```bash
cd /home/ubuntu/Dual-Screener-Bot
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
deactivate
```

주식 systemd·Bitget systemd 모두 **`${INSTALL_ROOT}/venv`** 를 사용한다.

---

## 5. 데이터 “방” 디렉터리 만들기 (가장 중요)

Bitget DB·로그·설정을 **주식 `DB_STORAGE_PATH` 밖**에 둔다.

```bash
sudo mkdir -p /var/lib/bitget-factory/data
sudo mkdir -p /var/lib/bitget-factory/logs
sudo chown -R ubuntu:ubuntu /var/lib/bitget-factory
```

이 경로에 아래 파일들이 생성된다 (자동).

| 파일 | 역할 |
|------|------|
| `bitget_market_data.sqlite` | OHLCV·forward_trades (쓰기) |
| `bitget_market_data_snapshot.sqlite` | CQRS 읽기 복제본 |
| `bitget_system_config.sqlite` / `.json` | 설정 |
| `bitget_ops_events.sqlite` | heartbeat·gauge |
| `bitget_message_queue.sqlite` | Telegram 큐 |
| `validation/` | Phase 7 baseline |

---

## 6. 환경 변수 (.env) — 겹치지 않게 설정

저장소 루트 **`/home/ubuntu/Dual-Screener-Bot/.env`** 에 주식·Bitget 키를 **접두사로 분리**해 넣는다.

```bash
cd /home/ubuntu/Dual-Screener-Bot
chmod 600 .env
nano .env   # 또는 bitget/.env 에 Bitget만 따로
```

### 6.1 주식 (기존 — 변경 최소)

```bash
# 주식 데이터 루트 (예시 — 이미 운영 중이면 그대로 유지)
DB_STORAGE_PATH=/var/lib/quant-factory/data

# 주식 Telegram (기존 키)
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### 6.2 Bitget 전용 (신규 추가)

```bash
# --- Bitget 격리 SSOT ---
BITGET_DB_STORAGE_PATH=/var/lib/bitget-factory/data
BITGET_LOG_DIR=/var/lib/bitget-factory/logs

BITGET_DASHBOARD_PORT=8511
BITGET_HEATMAP_PORT=8512

# Bitget API (실거래·private WS)
BITGET_ACCESS_KEY=...
BITGET_SECRET_KEY=...
BITGET_PASSPHRASE=...

# Bitget Telegram (주식 봇과 다른 bot token / chat id 권장)
BITGET_TELEGRAM_TOKEN=...
BITGET_TELEGRAM_CHAT_ID=...

# systemd factory 유닛에서 inline 텔레그램 끄고 async 전용 큐 사용
BITGET_SKIP_INLINE_TELEGRAM=1
BITGET_ASYNC_TELEGRAM=1

# 실주문 (기본: 꺼짐 + dry-run)
ENABLE_REAL_EXECUTION=false
REAL_EXECUTION_DRY_RUN=true

# cron SSOT cutover 완료 후
# BITGET_PIPELINE_SSOT=1
```

선택: `bitget/.env` 에 Bitget 블록만 두고, systemd가 `EnvironmentFile=-.../bitget/.env` 로 추가 로드한다.  
(`dante-bitget-*.service` 는 루트 `.env` 와 `bitget/.env` **둘 다** 읽는다.)

템플릿: `bitget/deploy/bitget_resource_limits.env.example`

### 6.3 절대 하지 말 것

- `BITGET_DB_STORAGE_PATH` 를 주식 `DB_STORAGE_PATH` 와 **같은 폴더**로 두지 않는다.
- Bitget cron 에 `factory.sh` 를 쓰거나, 주식 cron 에 `bitget.sh` 를 넣지 않는다.
- `update_factory.sh` 로 Bitget을 재시작하거나, `update_bitget.sh` 로 `dante-factory` 를 재시작하지 않는다.

---

## 7. 주식 팩토리 (이미 돌고 있으면 건너뛰기)

```bash
cd /home/ubuntu/Dual-Screener-Bot
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./deploy_quant_factory.sh
sudo systemctl start dante-factory dante-dashboard dante-async
sudo systemctl start dante-watchdog.timer dante-snapshot.timer
```

상태 확인:

```bash
systemctl is-active dante-factory dante-dashboard dante-async
```

---

## 8. Bitget 팩토리 설치 (주식과 독립)

```bash
cd /home/ubuntu/Dual-Screener-Bot
chmod +x bitget/deploy/bitget.sh \
         bitget/deploy/update_bitget.sh \
         bitget/deploy/deploy_bitget_factory.sh \
         bitget/deploy/entrypoints/*.sh

sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot \
  ./bitget/deploy/deploy_bitget_factory.sh
```

설치되는 유닛 (주식 `dante-*` 와 **이름이 다름**):

- `dante-bitget-ws.service`
- `dante-bitget-factory.service`
- `dante-bitget-async.service`
- `dante-bitget-dashboard.service`
- `dante-bitget-heatmap.service`
- `dante-bitget-watchdog.timer`
- `dante-bitget-snapshot.timer`

---

## 9. Bitget 기동

```bash
# 1) 헬스체크 (DB·경로·모드 확인)
cd /home/ubuntu/Dual-Screener-Bot
./bitget/deploy/bitget.sh --health

# 2) systemd 전체 기동
sudo systemctl start dante-bitget-ws
sudo systemctl start dante-bitget-async
sudo systemctl start dante-bitget-factory
sudo systemctl start dante-bitget-dashboard
sudo systemctl start dante-bitget-heatmap
sudo systemctl start dante-bitget-watchdog.timer
sudo systemctl start dante-bitget-snapshot.timer
```

상태 확인:

```bash
systemctl is-active \
  dante-bitget-ws \
  dante-bitget-factory \
  dante-bitget-async \
  dante-bitget-dashboard \
  dante-bitget-heatmap

systemctl list-timers 'dante-bitget-*' --no-pager
```

로그:

```bash
journalctl -u dante-bitget-factory -u dante-bitget-ws -f
```

---

## 10. cron “방” 추가 (Bitget 전용)

```bash
sudo cp /home/ubuntu/Dual-Screener-Bot/bitget/deploy/bitget.crontab.example \
        /etc/cron.d/dual-screener-bitget
sudo nano /etc/cron.d/dual-screener-bitget
```

**반드시 수정:**

```bash
BITGET=/home/ubuntu/Dual-Screener-Bot    # 실제 INSTALL_ROOT
BITGET_USER=ubuntu
```

cron 파일 상단에 Bitget data path 를 cron 환경에 넣으려면:

```bash
BITGET_DB_STORAGE_PATH=/var/lib/bitget-factory/data
BITGET_LOG_DIR=/var/lib/bitget-factory/logs
```

주식 cron (`/etc/cron.d/` 의 `factory.sh` 항목)과 **파일 이름·스케줄만** 구분하면 된다.  
같은 서버에서 두 cron 파일이 **동시에** 있어도 무방하다.

---

## 11. 격리 검증 체크리스트

배포 후 아래를 순서대로 확인한다.

```bash
cd /home/ubuntu/Dual-Screener-Bot
source venv/bin/activate

# Bitget infra
python -m bitget.pipelines.runner --mode health

# 주식 유닛 여전히 active
systemctl is-active dante-factory

# Bitget 유닛 active
systemctl is-active dante-bitget-factory

# 데이터 경로 분리
python -c "
from factory_data_paths import factory_data_dir
from bitget.infra.data_paths import bitget_data_dir
print('stock data:', factory_data_dir())
print('bitget data:', bitget_data_dir())
"
# 두 경로가 달라야 함

# 포트 리스닝
ss -tlnp | grep -E '8501|8511|8512'
```

| 확인 항목 | 기대 결과 |
|-----------|-----------|
| `bitget_data_dir()` | `/var/lib/bitget-factory/data` (또는 설정한 경로) |
| `factory_data_dir()` | `/var/lib/quant-factory/data` 등 **다른 경로** |
| `dante-factory` / `dante-bitget-factory` | 둘 다 `active` |
| Telegram | 주식 알림·Bitget 알림이 **서로 다른 봇/채널** |
| `./bitget/deploy/bitget.sh --health` | `[OK] bitget infra` |

---

## 12. 일상 업데이트 (git pull 후)

### 주식만 업데이트

```bash
cd /home/ubuntu/Dual-Screener-Bot
sudo ./update_factory.sh
```

### Bitget만 업데이트 (주식 무터치)

```bash
cd /home/ubuntu/Dual-Screener-Bot
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/update_bitget.sh
```

### 코드는 같이 pull, 재시작은 각각

```bash
sudo -u ubuntu git -C /home/ubuntu/Dual-Screener-Bot pull --ff-only
sudo ./update_factory.sh          # 주식 재기동
sudo ./bitget/deploy/update_bitget.sh   # Bitget 재기동
```

---

## 13. 프로세스 토폴로지 (한 서버, 두 방)

```
/home/ubuntu/Dual-Screener-Bot/          ← Git + venv 공유
├── .env                                 ← DB_STORAGE_PATH + BITGET_* 분리
├── factory.sh / update_factory.sh       ← 주식 전용
└── bitget/
    ├── deploy/bitget.sh                 ← Bitget cron 전용
    ├── deploy/update_bitget.sh          ← Bitget 업데이트 전용
    └── RUNBOOK.md

[주식 방]                          [Bitget 방]
dante-factory.service              dante-bitget-factory.service
dante-dashboard :8501              dante-bitget-dashboard :8511
dante-async                        dante-bitget-async
dante-watchdog.timer               dante-bitget-watchdog.timer
/var/lib/quant-factory/data        /var/lib/bitget-factory/data
cron: factory.sh                   cron: bitget.sh
```

---

## 14. 자주 하는 실수

| 실수 | 증상 | 해결 |
|------|------|------|
| `BITGET_DB_STORAGE_PATH` 미설정 | DB가 `bitget/` 코드 폴더에 생성 | `.env` 에 절대 경로 설정 후 재기동 |
| 같은 Telegram 봇·채널 | 알림 섞임 | `BITGET_TELEGRAM_*` 별도 봇/채널 |
| `python -m bitget.main` 수동 실행 | systemd·cron과 **중복** 스레드 | prod 에서는 사용 금지 (deprecated) |
| `update_factory.sh` 만 실행 | Bitget 코드는 갱신됐는데 서비스 구버전 | `update_bitget.sh` 별도 실행 |
| 포트 8511 충돌 | dashboard 기동 실패 | `BITGET_DASHBOARD_PORT` 변경 |

---

## 15. 실거래 전 단계 (권장)

1. `ENABLE_REAL_EXECUTION=false`, `REAL_EXECUTION_DRY_RUN=true` 유지
2. `./bitget/deploy/bitget.sh --start-parallel` → 48h 병렬 관찰
3. `./bitget/deploy/bitget.sh --record-baseline` → `./bitget/deploy/bitget.sh --validate`
4. `./bitget/deploy/bitget.sh --cutover-check`
5. 준비되면 `.env` 에 `BITGET_PIPELINE_SSOT=1`

상세: `bitget/docs/implementation_phase_7.md`

---

## 16. 관련 문서

| 문서 | 내용 |
|------|------|
| [../RUNBOOK.md](../RUNBOOK.md) | Bitget 운영·로그·재시작 |
| [README.md](./README.md) | Phase 구현 인덱스 |
| [../../RUNBOOK.md](../../RUNBOOK.md) | 주식 팩토리 런북 (루트) |
| [../../bitget_architecture_upgrade_plan.md](../../bitget_architecture_upgrade_plan.md) | 전체 설계 |

---

## 17. 한 줄 요약

**같은 repo·venv 위에, `BITGET_DB_STORAGE_PATH` + `dante-bitget-*` + `bitget.sh` cron 으로  
주식(`dante-*` + `factory.sh`)과 완전히 다른 “방”을 만든다.**
