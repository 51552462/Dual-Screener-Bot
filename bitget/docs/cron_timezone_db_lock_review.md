# Bitget `database is locked` 장애 — 크론·시간대 전체 검토

> 작성: 2026-06-28 (KST) · 범위: 한국/미국(주식) + 코인(Bitget) 크론·시간대 검토 및 DB 락 원인 분석
> 상태: **read-only 검토 (코드 미수정)**

---

## 0. 스크린샷 진단 (장애 증상)

세 알람 모두 **코인(Bitget) 쪽**이고, 공통 원인은 `database is locked` 이다.

| 잡 | run_id(=KST) | window | 실패 스텝 |
|---|---|---|---|
| `track_positions` | 07:15:49 | 07:15 → 07:19 | `track_spot` (critical) DB locked → `track_futures` 좀비가드 스킵 |
| `scan_spot_dante` | 02:40:20 | 02:40 → **07:20** (4h40m) | `scan` (critical) DB locked |
| `scan_futures_dante_r2` | 06:50:49 | 06:50 → **08:04** (74m) | `scan` (critical) DB locked |

먼저 확정한 사실 2가지:

1. **알람의 모든 시각은 KST 기준이다.**
   `infra/runtime.py`의 `dispatch_bitget_mode()`가 `run_id`·`started_at`을 `Asia/Seoul`로 찍는다(`runtime.py` 516~518줄).
   반면 코인 cron은 `CRON_TZ=UTC`. → **알람 시각(KST)과 cron 슬롯(UTC) 사이 9시간 차이**를 항상 염두에 둬야 한다.
2. **`scan_spot_dante`의 run_id(02:40 KST)는 SSOT 슬롯(14:03 KST)과 전혀 안 맞는다.**
   `scan_futures_dante_r2`도 SSOT 03:49 KST인데 06:50에 시작했다. window가 1~5시간으로 비정상적으로 길다.
   → 정규 cron 슬롯이 아니라 **watchdog(`*/5`)이 실패 파이프라인을 반복 재시도**하며 같은 락에 계속 막혀 실패→알람을 양산하는 패턴으로 보인다.

---

## PART A — 한국/미국 퀀트 크론·시간대 (`deploy/factory.*.crontab.example`)

SSOT: `factory_scan_schedule.py` → `generate_factory_crontab.py`. 구조는 **깔끔하고 충돌이 잘 격리**돼 있다.

### KR (`CRON_TZ=Asia/Seoul`, Mon–Fri)
```
10:00 supernova · 10:50 nulrim · 11:40 dante · 12:30 ema5 · 13:20 master · 14:10 bowl
15:00 supernova_r2 · 15:20 nulrim_r2        ← 2회차(15:30 마감 전 역산 배치)
08:00 data-refresh(벌크 OHLCV) · 15:40/16:20 limit-up · 16:10 smart-money
17:00 doomsday · 18:45 daily-kr · 06:45(화~토) daily-us · 토 10:05 weekly
```
- 1회차 50분 간격(글로벌 flock으로 직렬화), 2회차는 마감 마진 역산.
- **분(minute)이 전부 :00/:10/:20.../:45** 즉 5의 배수.

### US (`CRON_TZ=Asia/Seoul`, but ET-clock SSOT)
```
*/5 22,23 * * *  factory_slot_dispatcher.py --market US
*/5 0-6  * * *   factory_slot_dispatcher.py --market US
```
- cron 자체는 KST지만 **실제 슬롯 판정은 `factory_slot_dispatcher`가 ET(America/New_York)로** 한다.
- DST 무관하게 ET 10:00~16:00을 KST 22:00~07:00 창에서 5분 폴링. → **DST에 안전.**

**평가:** 한국/미국 구조의 cron·시간대 설계는 문제 없음. flock 직렬화 + 5의 배수 분 + ET 디스패처까지 일관됨.
**이번 장애의 원인이 아니다.**

---

## PART B — 코인(Bitget) 크론·시간대 (`bitget/deploy/bitget.crontab.example`)

SSOT: `bitget_scan_schedule.py` → `generate_bitget_crontab.py`. `CRON_TZ=UTC`, 24/7.
분(minute)은 의도적으로 **5의 배수가 아님**(주식 :x0/:x5와 같은 분 회피 — `_assert_collision_free()`).

UTC 슬롯을 **KST(+9)로 환산**해 주식 바쁜 시간과 겹치는지 본 것이 핵심.

| 코인 잡 (UTC) | → KST | 그 시각 주식(KR/US) 상태 |
|---|---|---|
| spot supernova 00:07 | **09:07** | KR 개장 직전(유휴) |
| ops health/audit 00:15/00:20 | 09:15/09:20 | KR 개장 직전(유휴) |
| fut supernova 01:23 | **10:23** | KR 1회차 진행중 |
| spot nulrim 02:39 | 11:39 | KR 진행중 |
| fut nulrim 03:47 | 12:47 | KR 진행중 |
| spot dante 05:03 | **14:03** | KR master(13:20)~ema5_r2 부근 |
| fut dante 06:19 | 15:19 | KR nulrim_r2(15:20)/limit-up(15:40) |
| spot ema5 07:33 | 16:33 | smart-money(16:10)/limit-up(16:20) |
| fut ema5 08:49 | 17:49 | doomsday(17:00) 직후 |
| spot master 10:03 | 19:03 | KR 마감 후 |
| fut shadow 11:19 | 20:19 | 유휴 |
| spot shadow 12:33 | 21:33 | US 폴링 시작 직전 |
| fut supernova_r2 13:49 | **22:49** | US 세션(폴링중) |
| spot supernova_r2 15:03 | 00:03 | US 세션 |
| fut nulrim_r2 16:19 | 01:19 | US 세션 |
| spot nulrim_r2 17:33 | 02:33 | US 세션 |
| fut dante_r2 18:49 | **03:49** | US 세션 |
| spot dante_r2 20:03 | 05:03 | US 막판 |
| fut ema5_r2 21:19 | **06:19** | US 마감 직후 |
| spot ema5_r2 22:33 | 07:33 | US 마감 후 |
| data-refresh `43 */4` | 05:43/09:43/13:43/17:43/21:43/01:43 | 주식 시간대 일부 겹침 |
| track-positions `*/15`, reconcile `:53`, watchdog `*/5` | 24/7 | 항상 |

**관찰:** 코인 스캔의 다수(14:03~21:33 KST = KR 장중·장후, 22:49~07:33 KST = US 세션)가 **주식 바쁜 시간과 겹친다.**
이건 24/7 코인 특성상 불가피하며, 설계상 `bitget_schedule_guard`의 **yield-to-factory**(주식 무거운 잡 lock 감지 시 코인 스캔 양보)로 막게 돼 있다.

---

## PART C — `database is locked`의 진짜 원인 (시간대 겹침이 아님)

검토 결과, **한국/미국 ↔ 코인 사이의 DB 충돌은 아니다.**
`bitget/infra/data_paths.py` 9번 줄에 명시돼 있고, 코인은 자체 `bitget_market_data.sqlite` 등 별도 파일을 쓴다.
락은 **코인 스택 내부의 동시 writer 경합**이다. 근거:

### 1. `data_refresh`만 별도 락을 쓴다
`job_lock_path()`(`data_paths.py` 176~180줄)에서 `data_refresh`는 `.bitget_data_refresh.lock`, 나머지(scan/track/reconcile)는 `.bitget_runtime.lock`.
→ **OHLCV 벌크 수집(긴 write 트랜잭션)이 scan/track과 동시에** 같은 `bitget_market_data.sqlite`에 쓴다.
WAL은 동시 writer 1개만 허용 → 나머지는 busy_timeout 후 `database is locked`.

### 2. 24/7 systemd 데몬이 같은 DB를 계속 쓴다 (cron flock 밖)
- `run_bitget_ws.sh` → `ws_supervisor` (실시간 캔들 ingest, 상시 writer)
- `run_bitget_daemon.sh` → `bitget_auto_pilot --daemon` (스나이퍼 루프)
- 스냅샷 타이머 **5분마다** `src.backup(dst)`로 메인 DB 전체를 읽음(`snapshot_service.py` 34~44줄)
- watchdog 타이머 5분마다
→ cron 스캔이 이들과 겹치면 yield-to-factory와 무관하게 락.

### 3. `track_positions`는 yield 대상이 아니다
가드의 `_YIELD_GATED_MODES = ("scan_", "data_refresh")`(`bitget_schedule_guard.py` 29줄)에 track이 없다.
`*/15`로 항상 돌고 `track_daily_positions`가 접속 직후 `PRAGMA journal_mode=WAL`을 실행(`ledger.py` 569~570줄)
→ 다른 writer가 잡고 있으면 즉시 `database is locked`. **첫 번째 스크린샷이 정확히 이 경로.**

### 4. busy_timeout/락 정책이 파일마다 제각각
| 파일 | 설정 |
|---|---|
| `forward/ledger.py` | `connect(timeout=60)` + WAL, **busy_timeout PRAGMA 없음** |
| `master_scanner.py` | `timeout=20` |
| `mtf_data_updater.py` | `timeout=60` + `busy_timeout=7000`(7초) |
| `trading/reconciliation.py` | `timeout=120` + `busy_timeout=15000` |

→ 7초/20초짜리 짧은 타임아웃이 먼저 터지면서 락 실패가 잦아짐.

### 5. watchdog 재시도 루프 추정
run_id가 SSOT 슬롯과 안 맞고 window가 1~5시간 → 한 번 실패한 스캔을 watchdog가 반복 재시도하며
계속 락에 막혀 알람을 양산하는 것으로 보인다.

---

## 요약

- **한국/미국 cron·시간대(PART A): 정상.** 이번 장애와 무관 — 건드릴 필요 없음.
- **코인 cron·시간대(PART B): 시간 배치 자체는 충돌 회피 설계대로**지만, 스캔의 다수가 주식 바쁜 시간과 겹쳐 yield-to-factory에 의존하는 구조.
- **`database is locked`의 실제 원인(PART C): 시간대 겹침이 아니라 "코인 자체 DB의 동시 writer 경합"**
  - ① `data_refresh` 별도 락
  - ② 24/7 ws/sniper/snapshot 데몬
  - ③ yield 비대상 `track_positions`
  - ④ 들쭉날쭉한 busy_timeout
  - ⑤ watchdog 재시도 루프

---

## 다음 단계 후보 (수정은 승인 후 진행)

- **(a)** 전 모듈 SQLite 접속에 `busy_timeout` 통일(예: 30~60s) + `PRAGMA journal_mode=WAL` 진입 락 회피
- **(b)** `track_positions`를 단일 직렬 락/리트라이로 보호 (yield 대상에 포함 또는 전용 retry)
- **(c)** 24/7 데몬(ws/sniper)과 cron 스캔의 writer 직렬화 (공용 write 락)
- **(d)** watchdog 재시도 정책 점검 — 락 실패는 재시도 대신 다음 슬롯으로 양보
