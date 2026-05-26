# LOCK_MECHANISM_AUDIT — Factory `SKIPPED_LOCK` 전수 감사

**감사 일시:** 2026-05-27  
**증상:** 텔레그램 `[Factory Job] scan_kr · SKIPPED_LOCK` (이전 동일 잡 실행 중 - DB 보호)  
**구현 모듈:** `factory_runtime.py` (유일한 팩토리 전역 락)

---

## Executive Summary

| 항목 | 내용 |
|------|------|
| **락 방식** | 프로젝트 루트 **파일** `.factory_runtime.lock` + Linux **`fcntl.flock` (비차단 EX)** |
| **DB 락 아님** | SQLite `database is locked` 와 **별개** |
| **SKIPPED_LOCK 의미** | 120초(`lock_timeout_sec`) 동안 flock 획득 실패 → `JobSkipError` → 이번 cron 회차 스킵 |
| **systemd 재시작** | 프로세스 종료 시 커널이 flock **자동 해제** — 파일 내용만 남을 수 있음 |
| **P0 패치** | 락 파일에 **PID 기록** + **dead PID self-heal** + **max-age(기본 2h) 정책** + 텔레그램 holder 상세 |

---

## 1. 락(Lock) 구현체 분석

### 1.1 호출 경로

```
cron / factory.sh --scan-kr
  → system_auto_pilot.py --mode scan_kr
  → factory_runtime.dispatch_factory_mode()
  → factory_job_lock("scan_kr", timeout_sec=120)
  → scan pipeline (supernova_scan_kr, kr_bowl_scan, …)
```

`factory_launcher.py` / `main.py` (systemd `dante-factory`) 는 **연속 스캐너**이며 **`factory_runtime` 락을 사용하지 않음**.  
`scan_kr` SKIPPED_LOCK 은 **다른 `factory.sh` / `system_auto_pilot` 잡**이 락을 잡고 있을 때 발생.

### 1.2 메커니즘 상세

| 항목 | 구현 |
|------|------|
| **락 파일** | `{REPO_ROOT}/.factory_runtime.lock` (`_default_lock_path()`) |
| **동기화** | `fcntl.flock(fd, LOCK_EX \| LOCK_NB)` — 프로세스 단위 배타 락 |
| **대기** | 최대 `lock_timeout_sec` (기본 **120초**, 1초 간격 재시도) |
| **Windows** | `sys.platform == "win32"` → **no-op** (락 없음, 개발용) |
| **메타데이터 (패치 후)** | 3줄: `mode` / `started_at` (KST ISO) / `pid` |
| **해제** | `finally`에서 `LOCK_UN` + fd close |
| **텔레그램** | `notify_factory_run` — `SKIPPED_LOCK` 만 별도 발송 (`OK`는 무음) |

### 1.3 구버전(패치 전) 한계

- 메타데이터에 **PID 없음** (mode + timestamp 만).
- flock 실패 시 **무조건 120초 대기 후 스킵** — holder가 죽었는지·유령인지 구분 불가.
- 락 파일 텍스트만 남고 **실제 flock은 해제**된 경우에도, **다른 살아 있는 프로세스**가 flock을 잡고 있으면 동일 증상.

### 1.4 `fcntl.flock` vs “유령 락 파일”

- **프로세스가 죽으면** (SIGKILL 포함) 해당 fd가 닫히며 **flock은 OS가 해제**.
- 디스크上的 `.factory_runtime.lock` **파일은 삭제되지 않음** — 내용만 오래된 PID/시각을 가리킬 수 있음.
- 따라서 **“파일만 보고 rm”은 위험** — **flock 보유 프로세스**를 먼저 확인해야 함.

### 1.5 관련 없는 락 (혼동 방지)

| 모듈 | 용도 |
|------|------|
| `bitget/schedule_lock.py` | Bitget 스케줄 (equity factory 와 별도) |
| `telegram_message_queue.py` | 스레드 `RLock` |
| SQLite | `database is locked` — WAL/동시 쓰기 이슈 |

---

## 2. 유령 락(Stale Lock) 감지 및 Self-Healing (P0)

### 2.1 구현 요약 (`factory_runtime.py` 패치)

1. **락 획득 시** `_write_lock_metadata`: `mode`, KST `started_at`, **`os.getpid()`**.
2. **flock 실패 루프마다** `_attempt_stale_lock_self_heal()`:
   - `_parse_lock_metadata` + `_pid_is_alive` (zombie `State: Z` → dead).
   - **Dead PID** → 동일 fd로 `LOCK_NB` 재시도 → 성공 시 self-heal 로그 후 진행.
   - **Orphan 파일** (메타 없음, age ≥ 60s) → 동일 재시도.
   - **Alive + age > FACTORY_LOCK_MAX_AGE_SEC** (기본 **7200s = 2h**):
     - 기본: 스킵 + 상세 메시지 (장시간 정상 스캔 보호).
     - `FACTORY_LOCK_BREAK_ON_MAX_AGE=1` → holder에 **SIGTERM** 후 재시도.
3. **SKIPPED_LOCK 텔레그램**에 `skipped_lock_detail` (holder mode/pid/alive/age) 포함.

### 2.2 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `FACTORY_LOCK_MAX_AGE_SEC` | `7200` | 메타 age가 이 값 초과 시 “오래된 락” 판단 참고 |
| `FACTORY_LOCK_BREAK_ON_MAX_AGE` | `0` | `1`이면 max-age 초과 + **alive** holder에 SIGTERM (위험 — 장시간 scan만 켜기) |

### 2.3 Self-heal이 **하지 않는** 경우 (정상 스킵)

- Holder PID **살아 있음** + age ≤ 2h → **실제 중복 실행 방지** (의도된 `SKIPPED_LOCK`).
- Dead PID인데도 flock busy → **다른 프로세스**가 락 보유 → 수동 `ps` / `/proc/locks` 필요.

### 2.4 배포

```bash
git pull origin main
# 서비스 재시작 후 다음 scan_kr cron 관찰
```

---

## 3. 현재 상황 진단 가이드 (서버 터미널)

**리포지토리 루트** (`factory.sh` 있는 디렉터리)에서 실행.

### 3.1 락 파일·메타 확인

```bash
cd /path/to/Dual-Screener-Bot
cat -n .factory_runtime.lock
ls -la --time-style=full-iso .factory_runtime.lock
```

예시 (패치 후):

```text
1  scan_kr
2  2026-05-27T14:30:00+09:00
3  2847193
```

### 3.2 PID가 살아 있는지

```bash
HOLDER_PID=$(sed -n '3p' .factory_runtime.lock)
echo "holder_pid=$HOLDER_PID"
ps -p "$HOLDER_PID" -o pid,etime,cmd
```

- **출력 없음** → PID 죽음 (flock은 보통 해제됨). 패치 후 self-heal이 다음 회차에 복구 시도.
- **`system_auto_pilot.py --mode`** 가 보이면 → **진짜 실행 중** (유령 아님).

### 3.3 팩토리 관련 프로세스 전체

```bash
ps aux | grep -E 'system_auto_pilot|factory\.sh' | grep -v grep
```

### 3.4 누가 flock을 잡았는지 (Linux)

```bash
LOCK_INO=$(stat -c '%i' .factory_runtime.lock)
grep "$LOCK_INO" /proc/locks
```

### 3.5 로그

```bash
ls -lt logs/factory_scan_kr_*.log | head -3
tail -80 logs/factory_scan_kr_*.log | tail -80
journalctl -u dante-factory -n 50 --no-pager   # systemd 스캐너 (별도 락)
```

### 3.6 수동 조치 (주의)

| 상황 | 권장 |
|------|------|
| PID **없음**, 다음 cron도 SKIPPED_LOCK | 패치 pull 후 대기; 또는 `grep /proc/locks` 로 live holder 확인 |
| PID **있고** 오래 걸림 | 정상 장시간 scan — **kill 하지 말 것** |
| PID **있고** 명백히 hung | `kill -TERM $HOLDER_PID` → 10초 후 `kill -KILL` (최후) |
| **파일만 rm** | ❌ 권장하지 않음 — flock은 **프로세스**가 보유; rm은 메타만 지움 |

```bash
# 최후: holder 종료 후에도 이상 시 (holder 없을 때만)
rm -f .factory_runtime.lock
```

### 3.7 해석 체크리스트

| 관측 | 해석 |
|------|------|
| PID alive + `supernova` CPU/로그 활발 | **정상 중복 방지** — 스킵 맞음 |
| PID dead + SKIPPED_LOCK 반복 | **패치 전** 버전 또는 **타 프로세스 flock** → pull + `/proc/locks` |
| PID dead + 패치 후 1회 스킵 후 OK | self-heal 성공 |
| scan_kr 2h+ + `FACTORY_LOCK_BREAK_ON_MAX_AGE=1` | 운영 정책으로만 장시간 holder 종료 |

---

## 4. 역제안 (운영·CI)

1. **cron 겹침 방지:** `scan_kr` 주기 > 최악 실행 시간(예: 90분) 보장.
2. **텔레그램:** SKIPPED_LOCK 수신 시 위 3.1–3.4 명령으로 holder 확인 (이제 detail 필드 포함).
3. **테스트:** `python -m pytest tests/test_factory_runtime_lock.py -q`
4. **장시간 scan 허용 시:** `FACTORY_LOCK_MAX_AGE_SEC` 를 14400 등으로 상향 — **alive breaker는 끈 채로** 유지.

---

## 5. 변경 파일

| 파일 | 변경 |
|------|------|
| `factory_runtime.py` | PID 메타, self-heal, `skipped_lock_detail` |
| `tests/test_factory_runtime_lock.py` | 메타 파싱·dead PID heal 단위 테스트 |
| `LOCK_MECHANISM_AUDIT.md` | 본 문서 |

---

*팩토리 equity 잡의 단일 전역 락은 `factory_runtime.factory_job_lock` 이며, Bitget·DB 내부 락과 혼동하지 말 것.*
