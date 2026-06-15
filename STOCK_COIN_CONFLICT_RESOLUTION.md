# 주식·코인 동시 구동 충돌 분석 및 완벽 분리 가이드

**작성 목적:** Ubuntu 1대에서 **주식 팩토리(KR/US)** 와 **Bitget 코인 팩토리**를 같이 올린 뒤 주식 파이프라인이 멈춘 현상의 **원인 팩트 체크**와 **0% 간섭 분리 아키텍처** 제시.

**범위:** 코드 수정 없음 — 저장소·systemd·cron·경로 설계 기준 분석.

---

## Executive Summary

| 질문 | 답 |
|------|-----|
| `.factory_runtime.lock`을 두 시스템이 **공유**하나? | **아니오.** 주식=`<INSTALL_ROOT>/.factory_runtime.lock`, 코인=`<BITGET_DATA>/.bitget_runtime.lock` |
| systemd 유닛·포트가 **충돌**하나? | **설계상 분리됨** (`dante-*` vs `dante-bitget-*`, 8501 vs 8511/8512) |
| cron 파일이 **덮어쓰기**되나? | **파일명이 다름** (`dual-screener-factory` vs `dual-screener-bitget`) — 단, **잘못 편집·단일 파일 통합** 시 위험 |
| 주식이 멈춘 **가장 유력한 원인** | ① **리소스(RAM/CPU) 경쟁·OOM** ② **잘못된 업데이트 스크립트**(`update_factory`로 주식 stop 후 실패) ③ **`BITGET_DB_STORAGE_PATH` 미설정**으로 데이터·부하 혼선 ④ **tmux/수동 중복 실행** ⑤ **13일 공백 등 기존 데이터 정체**(버그 아님, `REPORT_STATE_ANALYSIS.md`) |

**결론:** 락 파일 **이름 공유**는 원인이 **아님**. 같은 `INSTALL_ROOT`·`venv`·`.env`를 **의도적으로 공유**하는 구조이므로, **데이터 경로·업데이트 스크립트·cron·리소스 상한**을 분리하지 않으면 주식이 “강제 종료·스킵”된 것처럼 보일 수 있다.

---

## 1. 환경·파일 충돌 분석

### 1.1 공유 vs 분리 매트릭스

| 축 | 주식 (KR/US) | Bitget (코인) | 충돌 위험 |
|----|--------------|---------------|-----------|
| **Git / 코드** | `INSTALL_ROOT` (repo 루트) | **동일 repo** (`bitget/` 하위) | 낮음 — `git pull` 동시 실행만 주의 |
| **venv** | `${INSTALL_ROOT}/venv` | **동일 venv** (설계상 공유) | 중간 — `pip install`·의존성 변경 시 양쪽 영향 |
| **`.env`** | `${INSTALL_ROOT}/.env` | 루트 `.env` **+** `bitget/.env` (후자가 덮어씀) | **높음** — 키 혼동·경로 오설정 |
| **데이터 루트** | `DB_STORAGE_PATH` → `market_data.sqlite` 등 | `BITGET_DB_STORAGE_PATH` → `bitget_market_data.sqlite` 등 | **높음** — 미설정 시 `bitget/` 패키지 폴더에 DB 생성 |
| **런타임 락** | `.factory_runtime.lock` (repo 루트) | `.bitget_runtime.lock` (bitget data dir) | **없음** (파일 분리) |
| **Telegram 큐 DB** | `message_queue.sqlite` (주식 data dir) | `bitget_message_queue.sqlite` | 없음 (분리 시) |
| **ops / heartbeat** | `ops_events.sqlite` | `bitget_ops_events.sqlite` | 없음 (분리 시) |
| **MetaGovernor state** | `meta_governor_state.json` (repo 루트) | `bitget_meta_governor_state.json` | 낮음 |

### 1.2 락(Lock) 파일 — **공유하지 않음 (코드 증거)**

**주식** (`factory_runtime.py`):

```87:89:factory_runtime.py
def _default_lock_path() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, ".factory_runtime.lock")
```

- `factory.sh` / cron 일회 job만 이 flock 사용.
- 코인 `bitget.sh`는 **이 파일을 읽지 않음**.

**코인** (`bitget/infra/data_paths.py`):

```167:168:bitget/infra/data_paths.py
def runtime_lock_path() -> str:
    return os.path.join(bitget_data_dir(), ".bitget_runtime.lock")
```

- `bitget/infra/runtime.py` 전용 flock.

→ **서로의 cron 실행을 flock으로 막는 구조가 아님.**  
주식이 “lock busy”로 스킵되면 **같은 주식 cron/factory.sh가 겹친 경우**이지, 코인 락 때문이 아님.

### 1.3 `.env` · 데이터 경로 — **가장 흔한 설정 충돌**

**주식 SSOT** (`factory_data_paths.py`): `DB_STORAGE_PATH` 만 사용.

**코인 SSOT** (`bitget/infra/data_paths.py`): `BITGET_DB_STORAGE_PATH` 우선.  
단, JSON 폴백에 **`DB_STORAGE_PATH` 키도 읽음**:

```34:35:bitget/infra/data_paths.py
        v = data.get("BITGET_DB_STORAGE_PATH") or data.get("DB_STORAGE_PATH")
```

**위험 시나리오**

1. `BITGET_DB_STORAGE_PATH` 미설정 → 코인 DB가 `bitget/` 패키지 디렉터리에 생성 (repo 안).
2. `bitget_system_config.json`에 실수로 주식 `DB_STORAGE_PATH`만 기입 → 코인이 **주식 데이터 디렉터리**에 SQLite 생성·잠금 경합.
3. 루트 `.env`를 코인 배포 시 덮어쓰기 → 주식 `TELEGRAM_*` / `DB_STORAGE_PATH` 유실.

**코인 entrypoint는 항상 루트 `.env`를 먼저 로드** (`run_bitget_daemon.sh`, `bitget.sh`):

```13:24:bitget/deploy/entrypoints/run_bitget_daemon.sh
if [[ -f "${ROOT}/.env" ]]; then
  source "${ROOT}/.env"
fi
if [[ -f "${BITGET_ROOT}/.env" ]]; then
  source "${BITGET_ROOT}/.env"
fi
```

→ **키 접두사 분리**(`DB_*` vs `BITGET_*`) 필수. 파일 하나에 섞되 **이름은 절대 공유하지 말 것**.

### 1.4 `update_factory.sh`의 프로세스 정리 — 코인 간접 영향 가능

주식 업데이트 시 `INSTALL_ROOT` 아래 프로세스를 `pgrep -f`로 스캔 후 일부 SIGTERM:

```211:218:update_factory.sh
    [[ "$cmd" == *"${root_canon}"* ]] || continue
    if [[ "$cmd" == *"${old_py}"* ]] \
      || [[ "$cmd" == *"factory_launcher.py"* ]] \
      || [[ "$cmd" == *"async_telegram_daemon.py"* ]] \
      || [[ "$cmd" == *"main.py"* && "$cmd" == *"${root_canon}"* ]]; then
```

- 코인 데몬 `python -m bitget.pipelines.bitget_auto_pilot` → **일반적으로 kill 대상 아님**.
- 코인 async `python -m bitget.async_telegram_daemon` → **kill 대상 아님** (경로에 `async_telegram_daemon.py` 없음).
- **레거시** `main.py` / 루트 `async_telegram_daemon.py` 수동 실행 시 → **같은 INSTALL_ROOT면 함께 종료**될 수 있음.

주식 업데이트는 **명시적으로** 아래만 stop/restart:

```294:303:update_factory.sh
systemctl stop dante-factory.service dante-dashboard.service dante-async.service
...
systemctl restart dante-factory.service dante-dashboard.service dante-async.service
```

**`dante-bitget-*`는 건드리지 않음** — 반대로 `update_bitget.sh`도 `dante-factory`를 stop하지 않음.

**위험:** `sudo ./update_factory.sh` 중 **[4/7] stop은 됐는데 [6/7] restart 실패** → **주식만 inactive로 남음** (코인과 무관하게 주식 전멸).

### 1.5 venv 공유

양쪽 systemd·wrapper 모두 `${INSTALL_ROOT}/venv` 사용 (의도된 설계, `bitget/docs/ubuntu_isolated_deploy_guide.md`).

- **장점:** 패키지 한 번 설치.
- **단점:** 코인 전용 대용량 패키지·`pip install`이 주식 런타임을 깨뜨릴 수 있음.
- **완벽 분리 옵션:** 코인 전용 venv (`/var/lib/bitget-factory/venv`) — §3 참고.

---

## 2. Systemd · Cron 충돌 분석

### 2.1 systemd 유닛 — **이름·포트 분리 (설계 정상)**

| 구분 | 주식 | 코인 |
|------|------|------|
| 코어 데몬 | `dante-factory.service` | `dante-bitget-factory.service` |
| 텔레그램 큐 | `dante-async.service` | `dante-bitget-async.service` |
| 대시보드 | `dante-dashboard.service` **:8501** | `dante-bitget-dashboard.service` **:8511** |
| 기타 | snapshot/watchdog/backup timer | ws/heatmap/snapshot/watchdog timer |
| 업데이트 | `update_factory.sh` | `bitget/deploy/update_bitget.sh` |

코인 factory에 **리소스 상한** (주식에는 기본 없음):

```18:19:bitget/deploy/systemd/dante-bitget-factory.service.in
MemoryMax=2G
CPUQuota=150%
```

→ cgroup이 **제대로 동작하면** 코인 OOM 시 **코인만** 죽는 것이 의도.  
→ **서버 RAM이 작거나** 코인을 **tmux에서 추가 실행**하면 **커널 전역 OOM**으로 **주식도 SIGKILL** 가능 (`bitget/docs/10_single_server_resource_management.md`).

### 2.2 Watchdog — **서로 다른 유닛만 재시작**

| Watchdog | 감시 DB | 재시작 대상 |
|----------|---------|-------------|
| 주식 `watchdog.py` | `ops_events.sqlite` | `sudo systemctl restart dante-factory` |
| 코인 `bitget/watchdog.py` | `bitget_ops_events.sqlite` | `dante-bitget-factory` (기본) |

→ **상대방 서비스를 재시작하지 않음** (코드상 분리).

### 2.3 Cron — **파일 분리가 SSOT, 통합 편집이 결함**

| 파일 | 스크립트 | 타임존 |
|------|----------|--------|
| `/etc/cron.d/dual-screener-factory` | `./factory.sh` | `CRON_TZ=Asia/Seoul` |
| `/etc/cron.d/dual-screener-bitget` | `bitget/deploy/bitget.sh` | `CRON_TZ=UTC` |

**구조적 결함이 되는 경우 (운영 실수)**

- `dual-screener-factory` 내용을 bitget으로 **교체** → 주식 스캔/일일 **전멸**.
- `FACTORY=` / `BITGET=` 경로를 **서로 다른 clone**으로 잘못 지정 → 한쪽만 동작.
- 두 스택 cron이 **동시 피크**(예: 16:35 KR daily + 코인 hourly scan) → CPU·디스크 IO 경쟁으로 주식 job **타임아웃·lock 연장** (논리 충돌은 아니나 체감 “멈춤”).

**코인 cron 밀도 (예시)** — `bitget.crontab.example`:

- `*/15` track, `*/5` watchdog, `10 * * *` scan → **상시 부하**.
- 주식과 **같은 소형 인스턴스**에서는 RAM 피크 겹침 주의.

### 2.4 포트·프로세스

| 포트 | 용도 |
|------|------|
| 8501 | 주식 Streamlit |
| 8511 | Bitget 관제탑 |
| 8512 | Bitget 히트맵 |

동일 포트 이중 바인딩 시 **나중에 뜬 쪽 failed** — 반대 스택이 죽는 것은 아니나, **failed 유닛이 재시작 루프**를 만들 수 있음.

---

## 3. 100% 동시 가동 — 완벽 분리 아키텍처

### 3.1 권장 토폴로지 (단일 서버 · 단일 Git clone)

```
/home/ubuntu/Dual-Screener-Bot/          ← INSTALL_ROOT (코드만 공유)
├── venv/                              ← (옵션 A) 공유 venv  또는  (옵션 B) 코인 전용 venv
├── .env                               ← 주식 키만 (DB_STORAGE_PATH, TELEGRAM_*)
├── bitget/.env                        ← 코인 키만 (BITGET_DB_STORAGE_PATH, BITGET_TELEGRAM_*)
│
/var/lib/quant-factory/data/           ← 주식 DB·ops·message_queue (DB_STORAGE_PATH)
/var/lib/bitget-factory/data/          ← 코인 DB 전부 (BITGET_DB_STORAGE_PATH)
/var/lib/bitget-factory/logs/          ← BITGET_LOG_DIR
```

**절대 금지**

- 두 스택이 **같은 디렉터리**에 `*.sqlite` 쓰기
- `update_factory.sh`로 코인 재기동, `update_bitget.sh`로 주식 재기동
- tmux에서 `bitget.main` / 중복 `factory.sh` **수동 병행**
- cron **한 파일에** 주식·코인 job 혼합

### 3.2 환경 변수 분리 템플릿

**`/home/ubuntu/Dual-Screener-Bot/.env` (주식)**

```bash
DB_STORAGE_PATH=/var/lib/quant-factory/data
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
# DANTE_OPS_EVENTS_DB=/var/lib/quant-factory/data/ops_events.sqlite  # 선택
```

**`/home/ubuntu/Dual-Screener-Bot/bitget/.env` (코인)**

```bash
BITGET_DB_STORAGE_PATH=/var/lib/bitget-factory/data
BITGET_LOG_DIR=/var/lib/bitget-factory/logs
BITGET_TELEGRAM_BOT_TOKEN=...
BITGET_TELEGRAM_CHAT_ID=...
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
BITGET_WATCHDOG_STATE_DIR=/var/lib/bitget-factory/watchdog_state
```

### 3.3 락·상태 파일 분리 (현재 코드 + 운영 규칙)

| 자원 | 주식 | 코인 |
|------|------|------|
| Pipeline flock | `<INSTALL_ROOT>/.factory_runtime.lock` | `<BITGET_DATA>/.bitget_runtime.lock` |
| Watchdog state | `/var/lib/dante-watchdog` | `$BITGET_WATCHDOG_STATE_DIR` |
| Schedule lock | (주식 별도) | `bitget_schedule_lock_state.json` in data dir |

**추가 운영 규칙:** `reset_factory_pipeline.sh` / `rm .factory_runtime.lock`은 **주식 전용**. 코인 락은 `bitget/data/.bitget_runtime.lock`만 건드림.

### 3.4 systemd · cron 설치 순서 (최초 1회)

```bash
export INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
cd "$INSTALL_ROOT"

# 1) 주식 (기존과 동일)
sudo INSTALL_ROOT="$INSTALL_ROOT" ./deploy_quant_factory.sh
sudo cp deploy/factory.crontab.example /etc/cron.d/dual-screener-factory
sudo sed -i "s|FACTORY=.*|FACTORY=${INSTALL_ROOT}|" /etc/cron.d/dual-screener-factory

# 2) 코인 (주식 유닛 비터치)
sudo INSTALL_ROOT="$INSTALL_ROOT" ./bitget/deploy/deploy_bitget_factory.sh
sudo cp bitget/deploy/bitget.crontab.example /etc/cron.d/dual-screener-bitget
sudo sed -i "s|BITGET=.*|BITGET=${INSTALL_ROOT}|" /etc/cron.d/dual-screener-bitget

sudo systemctl reload cron
```

### 3.5 업데이트 규칙 (패치마다)

```bash
# 주식 패치 후
cd "$INSTALL_ROOT" && sudo ./update_factory.sh

# 코인 패치 후 (별도 실행, 시간 간격 두기 권장)
cd "$INSTALL_ROOT" && sudo ./bitget/deploy/update_bitget.sh
```

**동시에 두 update 스크립트를 병렬 실행하지 말 것** — 동일 `.git` lock 경쟁.

### 3.6 리소스 분리 (주식 우선)

1. 코인: `dante-bitget-*.service`에 `MemoryMax` / `MemoryHigh` 유지 (`bitget_resource_limits.env.example` 참고).
2. 주식: RAM 여유 확보 — 스왑 2GB+ 권장 (`bitget/docs/10_single_server_resource_management.md`).
3. **tmux 수동 코인 프로세스 금지** — systemd와 이중 기동 시 CPU·DB·텔레그램 이중화.

### 3.7 (옵션) venv 완전 분리

공유 venv가 불안하면:

```bash
python3 -m venv /var/lib/bitget-factory/venv
/var/lib/bitget-factory/venv/bin/pip install -r "$INSTALL_ROOT/requirements.txt"
```

`bitget/deploy/entrypoints/*.sh`에서 코인만 해당 venv를 activate 하도록 서버 로컬 패치 또는 `BITGET_VENV` 래퍼 — **코드 변경 없이**는 공유 venv 유지가 현재 SSOT.

---

## 4. 주식이 멈췄을 때 — 진단·복구 명령어 (Ubuntu 복붙)

### 4.1 충돌 여부 빠른 진단

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
# 또는 /home/ubuntu/Dual-Screener-Bot

echo "=== systemd ==="
systemctl is-active dante-factory dante-async dante-dashboard
systemctl is-active dante-bitget-factory dante-bitget-async dante-bitget-ws 2>/dev/null || true

echo "=== cron 파일 존재·경로 ==="
grep -E '^FACTORY=|factory.sh' /etc/cron.d/dual-screener-factory 2>/dev/null || echo "⚠️ 주식 cron 없음"
grep -E '^BITGET=|bitget.sh' /etc/cron.d/dual-screener-bitget 2>/dev/null || echo "⚠️ 코인 cron 없음"

echo "=== 락 (서로 다른 파일이어야 함) ==="
ls -la "$INSTALL_ROOT/.factory_runtime.lock" 2>/dev/null || echo "주식 lock 없음"
python3 -c "
from bitget.infra.data_paths import runtime_lock_path
print('코인 lock:', runtime_lock_path())
import os; p=runtime_lock_path(); print('  exists:', os.path.exists(p))
" 2>/dev/null || echo "bitget 경로 import 실패 — BITGET_DB_STORAGE_PATH 확인"

echo "=== 데이터 경로 (같으면 위험) ==="
grep -E '^DB_STORAGE_PATH|^BITGET_DB_STORAGE_PATH' "$INSTALL_ROOT/.env" "$INSTALL_ROOT/bitget/.env" 2>/dev/null

echo "=== 중복/레거시 프로세스 ==="
pgrep -af 'factory.sh|bitget.sh|system_auto_pilot|bitget_auto_pilot|bitget.main' || true

echo "=== 최근 OOM (커널) ==="
dmesg -T 2>/dev/null | grep -i 'out of memory' | tail -5 || journalctl -k -g 'oom' --no-pager 2>/dev/null | tail -5

echo "=== 주식 최근 에러 로그 ==="
sudo journalctl -u dante-factory -u dante-async --since "2 hours ago" -p err --no-pager | tail -20
```

### 4.2 주식만 100% 재가동 (코인 건드리지 않음)

```bash
cd "$INSTALL_ROOT"
rm -f .factory_runtime.lock
sudo systemctl restart dante-factory dante-async dante-dashboard
sudo systemctl restart dante-snapshot.timer dante-watchdog.timer dante-backup.timer
systemctl is-active dante-factory dante-async
```

### 4.3 데이터 경로 분리 후 코인 재배포 (코인만)

```bash
sudo mkdir -p /var/lib/bitget-factory/{data,logs,watchdog_state}
sudo chown -R ubuntu:ubuntu /var/lib/bitget-factory

# bitget/.env 에 BITGET_DB_STORAGE_PATH 설정 후
sudo INSTALL_ROOT="$INSTALL_ROOT" ./bitget/deploy/update_bitget.sh
```

### 4.4 양쪽 동시 건강 검진

```bash
# 주식
systemctl is-active dante-factory dante-async && \
  sudo journalctl -u dante-factory --since "30 min ago" --no-pager | tail -5

# 코인
systemctl is-active dante-bitget-factory dante-bitget-async && \
  sudo journalctl -u dante-bitget-factory --since "30 min ago" --no-pager | tail -5
```

---

## 5. 정상 동시 가동 체크리스트

| # | 항목 | 정상 |
|---|------|------|
| 1 | `dante-factory` | `active` |
| 2 | `dante-bitget-factory` | `active` (코인 사용 시) |
| 3 | `dual-screener-factory` cron | `FACTORY=$INSTALL_ROOT` + `factory.sh` 행 존재 |
| 4 | `dual-screener-bitget` cron | `BITGET=$INSTALL_ROOT` + `bitget.sh` 행 존재 |
| 5 | `DB_STORAGE_PATH` ≠ `BITGET_DB_STORAGE_PATH` | 서로 다른 절대 경로 |
| 6 | 주식 lock | repo 루트 `.factory_runtime.lock` only |
| 7 | 코인 lock | data dir `.bitget_runtime.lock` only |
| 8 | 포트 | 8501 / 8511 / 8512 각각 LISTEN |
| 9 | `pgrep bitget.main` | **없음** (레거시 수동 실행 금지) |
| 10 | 텔레그램 RED (lag 13일) | 데이터 공백 시 **정상 방어** — `REPORT_STATE_ANALYSIS.md` |

---

## 6. 관련 문서

| 파일 | 내용 |
|------|------|
| `bitget/docs/ubuntu_isolated_deploy_guide.md` | 코인 격리 배포 SSOT |
| `bitget/docs/10_single_server_resource_management.md` | RAM·tmux·cgroup |
| `FACTORY_FULL_OPS_MANUAL.md` | 주식 운영 명령 |
| `STANDARD_UPDATE_RESTART_MANUAL.md` | 주식 패치·재시작 |
| `REPORT_STATE_ANALYSIS.md` | RED·Fail-safe (버그 vs 데이터 공백) |

---

## 7. 한 줄 요약

**주식과 코인은 락·systemd·cron·포트가 코드상 분리되어 있으나, 같은 서버·같은 repo·같은 venv·같은 `.env` 파일을 쓰기 때문에 `BITGET_DB_STORAGE_PATH` 미분리·리소스 과점·잘못된 update/cron 편집·tmux 이중 실행이 주식 “전멸”의 실제 원인 후보다. 완벽 분리 = 데이터 디렉터리 2개 + cron 파일 2개 + update 스크립트 2개 + 코인 MemoryMax + 주식 `update_factory` 성공 여부 확인.**
