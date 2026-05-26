# HANG_AND_LOCK_AUDIT — 무한 대기·SKIPPED_LOCK 연쇄 파업 감사

**감사 일시:** 2026-05-27  
**증상:** `./factory.sh --daily-us` 실행 중 Hang → `Ctrl+C` (SIGINT) → 이후 모든 팩토리 잡 `SKIPPED_LOCK` 연쇄  
**근본 결합:** (①) 외부 API 무타임아웃 (②) SIGINT 시 락·파일 미정리 (③) dead PID 메타만으로는 flock busy 복구 실패

---

## Executive Summary

| 레이어 | P0 조치 |
|--------|---------|
| **Hang** | `network_timeout.py` — `fdr`/`yf`를 스레드 풀 + **기본 30초** 상한 (`FACTORY_HTTP_TIMEOUT_SEC`) |
| **Hot path** | `forward/ledger.py` `track_daily_positions` (US `yf.download` 루프 = Hang 최다발) |
| **Stale lock** | `factory_runtime.py` — dead PID 시 **lock 파일 삭제** + flock 재획득, 진입 전 선제 purge |
| **Graceful shutdown** | SIGINT/SIGTERM → flock 해제 + **`.factory_runtime.lock` 삭제** |

---

## 1. 타임아웃(Timeout) 부재로 인한 무한 대기 (P0)

### 1.1 `--daily-us` 파이프라인과 Hang 지점

```
factory.sh --daily-us
  → meta_governor_sync → artifact_guard → sentiment_mining
  → track_daily_positions_us  ← ★ Hang 다발
  → deep_dive_us → doomsday → PIL → comprehensive → overseer
```

| Step | 외부 I/O | 타임아웃(패치 전) |
|------|----------|-------------------|
| `track_daily_positions("US")` | **종목별 `yf.download`** | ❌ 없음 (N종목 × 무한 대기 가능) |
| 동일 (KR) | `fdr.DataReader` | ❌ 없음 |
| `sentiment_miner` | HTTP/Gemini | 일부 모듈만 timeout |
| `deep_dive` / DB | sqlite | `connect(timeout=60)` 등 |

**Hang 시나리오:** US OPEN 포지션 다수 → `yf.download` 한 티커에서 네트워크 정지 → 프로세스 **살아 있음** + **flock 보유** → 다음 cron 전부 `SKIPPED_LOCK` (정상 방어이나 사용자는 “유령 락”으로 오인).

### 1.2 전수 스캔 요약 (대표)

| 모듈 | 패턴 | daily-us 관련 |
|------|------|----------------|
| `forward/ledger.py` | `yf.download`, `fdr.DataReader` | **직접** |
| `forward/shared.py` | fallback `yf`/`fdr` | 가상 포지션·신규 진입 |
| `supernova_hunter.py` / `usa.py` | 다수 `yf`/`fdr` | `scan_*` (별도 cron) |
| `system_auto_pilot.py` | 혼재 | 일부 `timeout=15` |
| `forward/shared.py` `send_telegram` | `requests.post(..., timeout=10)` | ✅ |

### 1.3 P0 구현 — `network_timeout.py`

```python
# 환경 변수 (초, 기본 30, 범위 5~300)
FACTORY_HTTP_TIMEOUT_SEC=30
# 별칭
FACTORY_NETWORK_TIMEOUT_SEC=30

from network_timeout import fdr_data_reader, yf_download
# fdr.DataReader / yf.download 대체 — ThreadPoolExecutor.result(timeout=...)
```

**적용 파일 (이번 패치):**

- `forward/ledger.py` — 벤치마크 SPY + **종목별 OHLCV** (`TimeoutError` 시 해당 종목 스킵)
- `forward/shared.py` — DB miss 시 `hist_df` / `idx_df` fallback 2곳

**미적용 (P2 백로그):** `supernova_hunter.py`, `usa.py`, `us_master.py` 등 스캐너 대량 — 동일 헬퍼로 순차 이관 권장.

### 1.4 운영 튜닝

```bash
# US 종목 많을 때 45~60초로 상향 가능
export FACTORY_HTTP_TIMEOUT_SEC=45
./factory.sh --daily-us
```

---

## 2. 유령 락(Stale Lock) Self-healing (P0)

### 2.1 락 구현 (재확인)

- **파일:** `{REPO}/.factory_runtime.lock`
- **동기화:** Linux `fcntl.flock(LOCK_EX|LOCK_NB)` — **프로세스 종료 시 커널이 flock 해제**
- **메타 (3줄):** `mode` / `started_at` / **`pid`**

**중요:** Hang 중 **프로세스는 살아 있음** → flock 유지 → `SKIPPED_LOCK`은 **오동작이 아님**.  
**Ctrl+C 후**에도 동일하면: (a) 프로세스가 아직 살아 있거나 (b) **다른 터미널/ cron 잡**이 보유.

### 2.2 패치 전 Self-heal 한계

- dead PID인데 **다른 live 프로세스**가 flock 보유 → “flock still busy”만 반복
- **lock 파일 미삭제** → 메타만 오래됨, 진단 혼란

### 2.3 P0 패치 (`factory_runtime.py`)

| 기능 | 동작 |
|------|------|
| `_maybe_purge_stale_lock_file` | PID dead 또는 orphan(60s+) → **`os.unlink` lock 파일** |
| 진입 전 | `factory_job_lock` 시작 시 1회 purge |
| heal 루프 | dead PID + flock busy → close fd → purge → **파일 재오픈** 후 재시도 |
| `_pid_is_alive` | `os.kill(pid,0)` + `/proc/pid/status` zombie(`Z`) 제외 |

### 2.4 Ctrl+C와 flock

- **정상 SIGINT:** `finally`에서 `LOCK_UN` (파일은 유지, 다음 dead-PID purge 가능)
- **시그널 핸들러:** `LOCK_UN` + **lock 파일 삭제** (연쇄 SKIPPED_LOCK 완화)

---

## 3. Graceful Shutdown (SIGINT/SIGTERM)

### 3.1 구현

`_factory_lock_shutdown_guard` — 락 획득 후 pipeline 실행 구간에 설치:

1. `SIGINT` / `SIGTERM` 수신  
2. `_emergency_release_factory_lock` — flock 해제, fd close, **unlink `.factory_runtime.lock`**  
3. `KeyboardInterrupt` 또는 `SystemExit` 재발생  

**정상 완료 종료:** flock만 해제, 파일 유지 (다음 실행 시 dead PID면 purge).

### 3.2 Hang + Ctrl+C 시나리오

| 단계 | 상태 |
|------|------|
| `track_daily_positions_us` 중 yf hang | 프로세스 live, **flock held** |
| Ctrl+C | (패치 후) lock 파일 삭제 시도 → 다음 잡 진행 가능 |
| Hang 프로세스가 SIGINT 무시 | `kill -TERM <pid>` / `kill -KILL` 필요 |

---

## 4. 현재 상황 진단 가이드 (서버)

```bash
cd /path/to/Dual-Screener-Bot

# 1) 락 메타
cat -n .factory_runtime.lock

# 2) holder PID 생존
PID=$(sed -n '3p' .factory_runtime.lock)
ps -p "$PID" -o pid,etime,stat,cmd

# 3) factory 전체
ps aux | grep -E 'system_auto_pilot|factory\.sh' | grep -v grep

# 4) flock 보유 (Linux)
grep "$(stat -c '%i' .factory_runtime.lock 2>/dev/null)" /proc/locks

# 5) Hang 의심 — US track 로그
tail -f logs/factory_daily_audit_us_*.log
```

| 관측 | 조치 |
|------|------|
| PID **live** + CPU/로그 진행 | 정상 대기 또는 Hang — `FACTORY_HTTP_TIMEOUT_SEC` 적용 후 재배포 |
| PID **없음** + SKIPPED_LOCK | `git pull` 후 purge 패치; 또는 `rm -f .factory_runtime.lock` (**holder 없을 때만**) |
| PID live + **D/Z stat** | `kill -TERM $PID` → 실패 시 `kill -KILL` |
| 여러 `system_auto_pilot` | 중복 cron — 스케줄 정리 |

**응급 (holder 없음 확인 후):**

```bash
rm -f .factory_runtime.lock
./factory.sh --daily-us
```

---

## 5. 역제안 (P1~P2)

1. **Step wall-clock 상한:** `factory_runtime.run_step`에 `FACTORY_STEP_MAX_SEC` (예: 3600) — 전체 US track 상한.  
2. **스캐너 일괄 timeout:** `supernova_hunter` / `usa.py` → `network_timeout` 이관.  
3. **pre-commit / cron 프리플라이트:** `python -c "from factory_runtime import _maybe_purge_stale_lock_file; ..."`  
4. **systemd:** `TimeoutStopSec` 이내 종료 + `KillMode=mixed` (이미 `dante-factory.service.in` 참고) — **factory.sh one-shot**은 별도 터미널 프로세스.  
5. **텔레그램:** SKIPPED_LOCK 시 `skipped_lock_detail`에 PID/age/alive 표시 (기존).

---

## 6. 변경 파일

| 파일 | 내용 |
|------|------|
| `network_timeout.py` | **신규** — bounded fdr/yf |
| `forward/ledger.py` | track_daily_positions timeout |
| `forward/shared.py` | fallback OHLCV timeout |
| `factory_runtime.py` | purge, signal handler, heal reopen |
| `tests/test_network_timeout.py` | 단위 테스트 |
| `tests/test_factory_runtime_lock.py` | purge 테스트 추가 |
| `HANG_AND_LOCK_AUDIT.md` | 본 문서 |

---

## 7. 배포·검증

```bash
git pull origin main
export FACTORY_HTTP_TIMEOUT_SEC=30
python -m pytest tests/test_factory_runtime_lock.py tests/test_network_timeout.py -q
# stuck 시
rm -f .factory_runtime.lock   # holder 없을 때만
./factory.sh --daily-us
```

---

*Hang은 “죽은 락”이 아니라 “살아 있는 락”인 경우가 많습니다. timeout + signal 정리 후에도 SKIPPED_LOCK이면 `ps`/`/proc/locks`로 live holder를 반드시 확인하십시오.*
