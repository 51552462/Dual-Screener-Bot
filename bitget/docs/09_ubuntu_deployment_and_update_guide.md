# 09 — Ubuntu 서버 배포·업데이트 가이드 (tmux → systemd)

> **대상:** 예전에 `tmux attach -t coin_bot` 으로 코인 봇을 수동 운영하던 사용자  
> **목표:** Phase 1~8 이후 **systemd 기반 데몬**으로 전환하고, 주식 팩토리와 **완전 격리**된 상태로 운영·업데이트  
> **수정 범위:** 이 문서만 (`bitget/docs/`). 주식 루트 파일은 건드리지 않음.

**관련 문서:** [ubuntu_isolated_deploy_guide.md](./ubuntu_isolated_deploy_guide.md) (상세 격리) · [RUNBOOK.md](../RUNBOOK.md) · [08_phase8_track_a_execution_report.md](./08_phase8_track_a_execution_report.md)

---

## 0. 한눈에 보기

| 예전 (tmux) | 지금 (systemd) |
|-------------|----------------|
| `tmux new -s coin_bot` 후 수동 실행 | OS가 **재부팅 후에도** 자동 기동 |
| SSH 끊기면 세션 관리 필요 | `systemctl` 로 상태·로그 관리 |
| `python -m bitget.main` 등 레거시 | `dante-bitget-factory` → `bitget_auto_pilot --daemon` |
| DB가 `bitget/` 패키지 폴더에 섞임 | `BITGET_DB_STORAGE_PATH` 로 **물리적 분리** |
| 코드 `git pull` 시 DB 위험 | **데이터 방** 은 그대로, **코드만** 갈아끼움 |

**표준 업데이트 한 줄 (권장):**

```bash
cd /home/ubuntu/Dual-Screener-Bot
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/update_bitget.sh
```

이 스크립트가 **DB 백업 → git pull → systemd 재설치 → Bitget만 재시작** 을 한 번에 처리한다.  
주식 `dante-factory` 등은 **절대 건드리지 않는다.**

---

## 1. tmux에서 systemd로 — 패러다임 전환

### 1.1 예전 방식 (더 이상 사용하지 않음)

```bash
# ❌ 레거시 — Phase 1에서 차단됨
tmux attach -t coin_bot
python -m bitget.main
# 또는 tmux 안에서 factory_launcher / auto_pilot 수동 실행
```

**문제점**

- SSH 세션이 끊기면 프로세스가 죽거나, tmux를 다시 붙여야 함
- 재부팅 후 **수동으로 다시** tmux + 봇 실행 필요
- `bitget.main` / `factory_launcher` / `sentinel` 이 **중복 실행**되면 스캔·파이프라인이 두 번 돌아감
- 로그가 터미널에만 남아 **운영 추적이 어려움**

### 1.2 지금 방식 (프로덕션 SSOT)

**코드는 Git 저장소 한 곳** (`Dual-Screener-Bot`)에 있지만, **24시간 돌아가는 코인 봇은 OS(systemd)가 관리**한다.

```
더 이상 tmux 창을 띄울 필요 없음
        │
        ▼
systemd가 백그라운드에서 24/7 관리
        │
        ├── 5개 상시 데몬 (service)
        │     dante-bitget-ws          WebSocket (시세·주문)
        │     dante-bitget-factory     24/7 팩토리 (bitget_auto_pilot)
        │     dante-bitget-async       Telegram 비동기 큐
        │     dante-bitget-dashboard   관제탑 Streamlit :8511
        │     dante-bitget-heatmap       히트맵 Streamlit :8512
        │
        └── 2개 타이머 (timer) — 주기적 one-shot
              dante-bitget-watchdog.timer   5분마다 heartbeat 감시
              dante-bitget-snapshot.timer     5분마다 DB 스냅샷 백업
```

**cron (one-shot)** 은 스캔·일일감사 등 **정해진 시각 작업**용:

```bash
./bitget/deploy/bitget.sh --scan-all
./bitget/deploy/bitget.sh --daily-audit
```

**정리:** tmux는 **개발·디버깅용**으로만 쓰고, 프로덕션 24/7은 **systemd + cron** 이 담당한다.

### 1.3 tmux 세션 정리 (전환 시 1회)

```bash
# 기존 coin_bot 세션 확인
tmux ls

# 안에 돌아가는 레거시 프로세스 종료 후 세션 삭제
tmux kill-session -t coin_bot   # 세션 이름이 다르면 해당 이름으로

# 혹시 남은 레거시 프로세스 확인 (Ubuntu)
pgrep -af 'bitget.main|factory_launcher|coin_bot' || true
```

이후에는 **`systemctl start dante-bitget-*`** 만 사용한다.

### 1.4 자주 쓰는 systemd 명령

```bash
# 상태
systemctl is-active dante-bitget-factory dante-bitget-ws dante-bitget-async

# 실시간 로그 (tmux 대신)
sudo journalctl -u dante-bitget-factory -u dante-bitget-ws -f

# 재시작 (코인만)
sudo systemctl restart dante-bitget-factory

# 타이머 확인
systemctl list-timers 'dante-bitget-*' --no-pager
```

---

## 2. 환경 변수(.env) — 주식과 겹치지 않는 “물리적 방”

### 2.1 왜 `BITGET_DB_STORAGE_PATH` 가 핵심인가

Git 저장소(`Dual-Screener-Bot`) 안의 **`bitget/` 폴더는 코드**다.  
`git pull` 할 때마다 이 폴더 안의 파일이 **덮어씌워진다.**

DB를 `bitget/` 안에 두면:

- 업데이트 시 경로·권한 꼬임
- 주식 `DB_STORAGE_PATH` 와 혼동
- 백업·복구가 어려움

**해결:** 코인 전용 데이터 루트를 **저장소 밖**에 둔다.

```
/home/ubuntu/Dual-Screener-Bot/     ← 코드 (git pull 대상)
/var/lib/bitget-factory/data/      ← 코인 DB·설정 (git과 무관, 영구 보존)
/var/lib/bitget-factory/logs/      ← 코인 로그
```

`BITGET_DB_STORAGE_PATH` 를 이렇게 설정하면, **코드를 아무리 갈아끼워도 DB는 그대로** 남는다.

### 2.2 데이터 디렉터리 최초 생성

```bash
sudo mkdir -p /var/lib/bitget-factory/data
sudo mkdir -p /var/lib/bitget-factory/logs
sudo chown -R ubuntu:ubuntu /var/lib/bitget-factory
```

이 경로에 자동 생성되는 주요 파일:

| 파일 | 역할 |
|------|------|
| `bitget_market_data.sqlite` | OHLCV · forward_trades · real_execution |
| `bitget_market_data_snapshot.sqlite` | CQRS 읽기용 스냅샷 |
| `bitget_system_config.sqlite` | Regime · Kelly · 스캐너 설정 (SSOT) |
| `bitget_ops_events.sqlite` | heartbeat · validation gauge |
| `bitget_message_queue.sqlite` | Telegram 큐 |
| `bitget_system_config.json` | bootstrap용 (SQLite가 SSOT) |

### 2.3 `.env` 예시 (주식 + Bitget 한 파일, 키만 분리)

저장소 루트: `/home/ubuntu/Dual-Screener-Bot/.env`

```bash
cd /home/ubuntu/Dual-Screener-Bot
chmod 600 .env
nano .env
```

```bash
# =============================================================================
# 주식 팩토리 (기존 — 그대로 유지)
# =============================================================================
DB_STORAGE_PATH=/var/lib/quant-factory/data
TELEGRAM_BOT_TOKEN=your_stock_bot_token
TELEGRAM_CHAT_ID=your_stock_chat_id

# =============================================================================
# Bitget 코인 팩토리 — 주식과 절대 경로를 공유하지 않음
# =============================================================================

# ★ 가장 중요: 코인 DB·설정의 물리적 방
BITGET_DB_STORAGE_PATH=/var/lib/bitget-factory/data
BITGET_LOG_DIR=/var/lib/bitget-factory/logs

# 대시보드 포트 (주식 8501 과 충돌 방지)
BITGET_DASHBOARD_PORT=8511
BITGET_HEATMAP_PORT=8512

# Bitget API (거래소)
BITGET_ACCESS_KEY=your_bitget_api_key
BITGET_SECRET_KEY=your_bitget_secret
BITGET_PASSPHRASE=your_bitget_passphrase

# Bitget Telegram (주식 봇과 다른 토큰·채널 권장)
BITGET_TELEGRAM_TOKEN=your_bitget_bot_token
BITGET_TELEGRAM_CHAT_ID=your_bitget_chat_id

# systemd: 인라인 텔레그램 끄고 async 큐 사용
BITGET_SKIP_INLINE_TELEGRAM=1
BITGET_ASYNC_TELEGRAM=1

# Watchdog heartbeat 컴포넌트 (레거시 bitget.main 아님!)
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot

# 실주문 안전 (기본: 꺼짐)
ENABLE_REAL_EXECUTION=false
REAL_EXECUTION_DRY_RUN=true

# Cutover 완료 후에만 (48h parallel 이후)
# BITGET_PIPELINE_SSOT=1
```

**선택:** Bitget 설정만 `bitget/.env` 에 분리해도 된다.  
systemd 유닛은 **루트 `.env` + `bitget/.env` 둘 다** 읽는다 (`EnvironmentFile=-...`).

템플릿: `bitget/deploy/bitget_resource_limits.env.example`

### 2.4 절대 하지 말 것

| 금지 | 이유 |
|------|------|
| `BITGET_DB_STORAGE_PATH` = `DB_STORAGE_PATH` | 주식·코인 DB 충돌 |
| `update_factory.sh` 로 Bitget 재시작 | 주식 전용 스크립트 |
| `update_bitget.sh` 로 `dante-factory` 재시작 | 코인 전용 스크립트 |
| `python -m bitget.main` 프로덕션 실행 | Phase 1에서 **BLOCKED** (중복 실행) |
| cron에 `factory.sh` 와 `bitget.sh` 스케줄 혼동 | 이중 스캔·이중 daily_audit |

### 2.5 격리 확인 (배포 후 1회)

```bash
cd /home/ubuntu/Dual-Screener-Bot
source venv/bin/activate

python -c "
from bitget.infra.data_paths import bitget_data_dir, market_data_db_path
print('bitget data dir :', bitget_data_dir())
print('market DB       :', market_data_db_path())
"

# 기대: /var/lib/bitget-factory/data/...  (설정한 경로)
./bitget/deploy/bitget.sh --health
```

---

## 3. 최초 설치 (Ubuntu, 1회)

주식 팩토리가 **이미 같은 서버**에서 돌고 있어도, Bitget은 **아래만** 추가하면 된다.

### 3.1 사전 조건

```bash
sudo apt update
sudo apt install -y git python3 python3-venv sqlite3

cd /home/ubuntu/Dual-Screener-Bot
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
deactivate
```

### 3.2 `.env` + 데이터 방 (§2 참고)

### 3.3 systemd 유닛 설치

```bash
cd /home/ubuntu/Dual-Screener-Bot
chmod +x bitget/deploy/bitget.sh \
         bitget/deploy/update_bitget.sh \
         bitget/deploy/deploy_bitget_factory.sh \
         bitget/deploy/entrypoints/*.sh

sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot \
  ./bitget/deploy/deploy_bitget_factory.sh
```

### 3.4 기동 순서

```bash
# 헬스체크
./bitget/deploy/bitget.sh --health

# WS → async → factory → UI → timer
sudo systemctl start dante-bitget-ws
sudo systemctl start dante-bitget-async
sudo systemctl start dante-bitget-factory
sudo systemctl start dante-bitget-dashboard
sudo systemctl start dante-bitget-heatmap
sudo systemctl start dante-bitget-watchdog.timer
sudo systemctl start dante-bitget-snapshot.timer
```

### 3.5 cron (선택)

```bash
sudo cp bitget/deploy/bitget.crontab.example /etc/cron.d/dual-screener-bitget
sudo nano /etc/cron.d/dual-screener-bitget
# BITGET=/home/ubuntu/Dual-Screener-Bot, BITGET_USER=ubuntu 수정
```

---

## 4. 데이터 보존 + 무중단 신버전 업데이트

### 4.1 원리

```
┌─────────────────────────────────────────────────────────────┐
│  /var/lib/bitget-factory/data/   ← DB (git pull과 무관)     │
│  bitget_market_data.sqlite, bitget_system_config.sqlite …   │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ BITGET_DB_STORAGE_PATH
                              │
┌─────────────────────────────────────────────────────────────┐
│  /home/ubuntu/Dual-Screener-Bot/   ← 코드 (git pull 대상)   │
│  bitget/*.py, deploy/, pipelines/ …                         │
└─────────────────────────────────────────────────────────────┘
```

**`git pull` 은 코드만 바꾼다.**  
`BITGET_DB_STORAGE_PATH` 가 저장소 밖이면 **쌓인 OHLCV·포워드 트레이드·설정은 그대로** 유지된다.

### 4.2 표준 업데이트 절차 (권장)

로컬(Cursor)에서 개발 → GitHub push → 서버에서 **아래 한 번** 실행:

```bash
cd /home/ubuntu/Dual-Screener-Bot

# ★ 이 한 줄이 전체 파이프라인 (주식 untouched)
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/update_bitget.sh
```

`update_bitget.sh` 내부 단계:

| 단계 | 내용 |
|------|------|
| **[1/5] 백업** | `BITGET_DB_STORAGE_PATH` 의 SQLite → `/var/backups/bitget-pre-update/<UTC>/` |
| **[2/5] git pull** | `git pull --ff-only` (ubuntu 사용자) |
| **[3/5] 유닛 재설치** | `deploy_bitget_factory.sh` (systemd 템플릿 갱신) |
| **[4/5] graceful stop** | `dante-bitget-*` 서비스만 정지 |
| **[5/5] restart** | WS → async → factory → dashboard → heatmap + timer |

**주식 `dante-factory`, `dante-dashboard`, `update_factory.sh` 는 호출되지 않는다.**

### 4.3 수동 git pull + 업데이트 (diff 확인 후)

변경 내용을 먼저 보고 싶을 때:

```bash
cd /home/ubuntu/Dual-Screener-Bot
git fetch origin
git log HEAD..origin/main --oneline    # 브랜치명에 맞게 조정
git diff HEAD..origin/main -- bitget/  # bitget 변경만 미리보기

# 확인 후 업데이트 (pull은 스크립트 안에서도 실행됨)
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/update_bitget.sh
```

이미 `git pull` 을 했다면 스크립트의 pull 단계는 **Already up to date** 로 넘어간다.

### 4.4 업데이트 후 검증

```bash
# 서비스 alive
systemctl is-active \
  dante-bitget-ws \
  dante-bitget-factory \
  dante-bitget-async \
  dante-bitget-dashboard \
  dante-bitget-heatmap

# 인프라
./bitget/deploy/bitget.sh --health

# Phase 8 Track A — 아키텍처·regime (선택)
source venv/bin/activate
python -m unittest bitget.tests.test_phase8_track_a_mock_e2e -v
python -c "from bitget.validation.regime_audit import run_regime_kelly_audit as r; print(r()['passed'], r()['message'])"

# cutover 정보 (아직 SSOT=0 이면 passed=False 정상)
./bitget/deploy/bitget.sh --cutover-check
```

### 4.5 백업에서 복구 (만약을 위해)

```bash
ls -lt /var/backups/bitget-pre-update/
# 예: 20260614_120000_utc/

STAMP=20260614_120000_utc
sudo systemctl stop dante-bitget-factory dante-bitget-ws

cp /var/backups/bitget-pre-update/${STAMP}/bitget_market_data.sqlite \
   /var/lib/bitget-factory/data/
cp /var/backups/bitget-pre-update/${STAMP}/bitget_system_config.sqlite \
   /var/lib/bitget-factory/data/

sudo systemctl start dante-bitget-ws dante-bitget-factory
```

---

## 5. 개발 워크플로 (로컬 → GitHub → Ubuntu)

```
┌──────────────┐     git push      ┌──────────────┐    update_bitget.sh    ┌──────────────┐
│ 로컬 Cursor  │ ───────────────► │   GitHub     │ ─────────────────────► │ Ubuntu 서버  │
│ Track A CI   │                  │   remote     │   (DB 보존·코드만)     │ Track B/C    │
└──────────────┘                  └──────────────┘                        └──────────────┘
     mock E2E                           │                                      data_refresh
     26 unittest                        │                                      daily_audit
                                        │                                      cutover
```

| 단계 | 환경 | 할 일 |
|------|------|-------|
| 1 | 로컬 | Phase 8 Track A unittest 통과 후 push |
| 2 | Ubuntu | `update_bitget.sh` |
| 3 | Ubuntu | `unittest` + `regime_audit` |
| 4 | Ubuntu | Track B: `data_refresh`, `scan-all` |
| 5 | Ubuntu | Track C: 48h parallel → `BITGET_PIPELINE_SSOT=1` |

---

## 6. 트러블슈팅

| 증상 | 확인 |
|------|------|
| factory가 바로 죽음 | `journalctl -u dante-bitget-factory -n 80` |
| WS 없이 factory 실패 | `systemctl is-active dante-bitget-ws` → factory는 WS **After** 의존 |
| DB 경로 혼동 | `python -c "from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())"` |
| Watchdog 오경보 | `.env` 에 `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot` |
| 텔레그램 폭주 | `BITGET_ASYNC_TELEGRAM=1`, `dante-bitget-async` active 확인 |
| 주식까지 재시작됨 | **`update_bitget.sh` 만** 사용했는지 확인 (`update_factory.sh` 아님) |

---

## 7. 체크리스트 요약

### 최초 전환 (tmux → systemd)

- [ ] `BITGET_DB_STORAGE_PATH` 저장소 **밖** 경로 설정
- [ ] 기존 `tmux coin_bot` 세션 종료
- [ ] `deploy_bitget_factory.sh` 실행
- [ ] 5 데몬 + 2 타이머 `is-active`
- [ ] `bitget.sh --health` OK
- [ ] 주식 `dante-factory` 여전히 active

### 매번 코드 업데이트

- [ ] GitHub에 push 완료
- [ ] `sudo INSTALL_ROOT=... ./bitget/deploy/update_bitget.sh`
- [ ] 백업 경로 로그 확인 (`/var/backups/bitget-pre-update/`)
- [ ] `dante-bitget-factory` active
- [ ] (선택) unittest / regime_audit

---

## 8. 참고 링크

| 문서 | 내용 |
|------|------|
| [ubuntu_isolated_deploy_guide.md](./ubuntu_isolated_deploy_guide.md) | 격리·cron·방화벽 상세 |
| [08_phase8_track_a_execution_report.md](./08_phase8_track_a_execution_report.md) | 로컬 CI·regime_audit |
| [07_phase8_feasibility_review.md](./07_phase8_feasibility_review.md) | Track A/B/C 로드맵 |
| [RUNBOOK.md](../RUNBOOK.md) | 운영 런북 SSOT |

---

**요약:** tmux 수동 운영은 끝났다. **systemd 5 데몬 + 2 타이머**가 24/7을 맡고, **`BITGET_DB_STORAGE_PATH`** 덕분에 `git pull`·`update_bitget.sh` 로 **코드만 갱신해도 DB는 안전**하다. 주식과 Bitget은 **이름·경로·스크립트가 다른 별도 방**이다.
