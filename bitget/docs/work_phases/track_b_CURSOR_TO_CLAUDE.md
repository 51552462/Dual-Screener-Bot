# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-09-15 00:41 KST · **1800초 Claude 검증 OK** · 다음=09-15 15:07 UTC 4종

---

## OUTBOX — 1800초 계산 검증 접수 (코드/서버 변경 없음)

Claude: 744×2=1488, 1800≈실측 최대×2.4 · 상한 미초과 · drop-in 경로 OK · 09-05~08 락 제외 OK.
L-3b **아직 Done 아님.** 트리거: 내일 4종 전부 정상 → Done 선언 + 3번 전체전환 논의.
락 미해제 후속은 지금 안 막음.

---

## OUTBOX — L-3b-fix 실측 + 적용 (로직 미변경)

---

## OUTBOX — L-3b-fix 실측 + 적용 (로직 미변경)

**실측 (cron 시절 stamped log, 시작~끝 있는 최근 4일)**

| 날짜 | 시작 | 끝 | 초 |
|------|------|-----|-----|
| 09-09 | 15:07:02 | 15:19:26 | 744 |
| 09-10 | 15:07:03 | 15:19:20 | 737 |
| 09-11 | 15:07:03 | 15:19:18 | 735 |
| 09-12 | 15:07:02 | 15:19:16 | 734 |

≈ **12.3분**. journalctl cron/factory grep는 비어 스탬프 로그 사용.
1회차 하한 15:07:03–15:20:59 = 13분+ (미완료).
09-05~08의 7200s는 lock-until-shutdown이라 **소요시간으로 안 씀**.

**확정값:** 12.3분 × 2 ≈ 25분 → Handoff 여유 예시 따라 **BITGET_QUEUE_WORKER_STALE_SEC=1800** (50분 상한 안 넘김). 기본 600이 1회차 킬 원인과 일치(15분째 watchdog).

**적용:** repo `.env` APPEND `=1800` · watchdog drop-in `queue-stale.conf` · `restart dante-bitget-queue-worker` → **active**.
워치독은 cron/timer가 `.env`를 source — 다음 */5부터 1800 적용.

**09-15 15:07 UTC 아직 남음.** 그때 start→done + MemoryCurrent 샘플 + watchdog restart 0 + OOM 0.

---

## OUTBOX — L-3b run1 완료 여부 (원문)

---

## OUTBOX — L-3b run1 완료 여부 (원문)

캡처: 15:22–15:23 UTC. **done / FAILED / attempt=2/3 로그 없음.**

```
Sep 14 15:07:03 ... [INFO] queue exec start id=1 engine=BITGET mode=scan_futures_ema5_r2 prio=5 attempt=1/3
Sep 14 15:20:59 ... [INFO] bitget_artifact_guard: ... healed: True
Sep 14 15:22:17 ... Starting Bitget heartbeat watchdog
Sep 14 15:22:18 ... sudo ubuntu : COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 14 15:22:18 ... Stopping Bitget task queue worker
Sep 14 15:22:18 ... [WARNING] bitget lock signal 15 — releasing .../.bitget_runtime.lock
Sep 14 15:23:05 ... Main process exited, code=exited, status=143
Sep 14 15:23:05 ... Failed with result 'exit-code'
Sep 14 15:23:05 ... Started Bitget task queue worker
Sep 14 15:23:06 ... [INFO] queue worker started
```

`journalctl ... | grep id=1|scan_futures_ema5_r2` 해당분은 **start 한 줄뿐**. 재시작 후 재픽업 로그 없음.

MemoryCurrent 샘플 (피크 확정 아님):
- 15:12 UTC ~91MB (95539200)
- 15:22 UTC ~133MB (139001856) ← 관측 최고
- 15:23 UTC restart 후 ~91MB (95481856)

1회차 판정: **끝까지 정상 종료 아님.** claim/start 성공 후 ~15분에 watchdog가 queue-worker를 restart → SIGTERM/143. 워커 코드의 완료 로그(`queue exec start` 짝) 없음.

Ask: L-3b 관찰 중 watchdog(5분 크론)이 RUNNING 잡을 끊는 건지 확인. 코드 수정은 이번 세션 안 함. 09-15 2회차도 같은 패턴이면 Done 불가.

---

## OUTBOX — L-3b run1 (09-14 15:07 UTC 슬롯) · 원문

---

## OUTBOX — L-3b run1 (09-14 15:07 UTC 슬롯) · 원문

캡처 시각: **2026-09-14 15:12:57 UTC** (슬롯 +6분). **1회만으로 됐다 금지.**

로그 키워드는 `claim`이 아니라 **`queue exec start`**.

```
===== 1 journalctl =====
Sep 14 15:07:03 ip-172-26-7-213 bash[1938]: [2026-09-15 00:07:03] [INFO] queue exec start id=1 engine=BITGET mode=scan_futures_ema5_r2 prio=5 attempt=1/3
===== 2 dmesg =====
NO_DMESG_KILLED
===== 3 MemoryCurrent =====
MemoryCurrent=95539200
===== crontab =====
7 15 * * *  .../bitget.sh --enqueue --scan-futures-ema5-r2
```

해석: enqueue→워커 픽업 **확인**. **done 아직 없음**(스캔 진행 중). OOM killed 0. Current≈91MB. run2=09-15 15:07 UTC 이후 동일 3종.

---

## OUTBOX — 2026-09-14 · L-3a Claude OK 접수 · L-3b 정정

**L-3a:** Cursor 구현 ✅ · 서버 적용 확인 ✅(원문 2건) · **Claude OK 2026-09-14**. 05 기록함.
**L-3b:** 적용됨. **Done 아님.** `NO_OOM_SINCE_APPLY`는 첫 슬롯 전이라 증거 아님.
Done 기준: **09-14 및 09-15 15:07 UTC 이후** 3종 캡처 2회분. 1회만으로 됐다 금지.
전체 큐 전환: 보류. 에스컬레이션 해제.

3종 (슬롯 지난 뒤):
```
journalctl -u dante-bitget-queue-worker --since "2026-09-14 15:00 UTC" | grep -iE "claim|done|scan_futures_ema5_r2"
dmesg -T --since "2026-09-13 15:39 UTC" | grep -i killed
systemctl show dante-bitget-queue-worker -p MemoryCurrent
```
run2는 since를 09-15 15:00 UTC 로.

---

## 디렉터 → Claude Pro 붙여넣기 (이 블록만)

```
---CLAUDE---
[CAT-L] L-3a/L-3b 서버 적용 결과 원문. 디렉터는 SSH 안 함. Cursor 캡처.

요청: L-3a는 값 확인되면 바로 OK. L-3b는 "일단 적용됨"만 확인(24~48h Done 아님). 전체 큐 전환은 범위 밖.

queue-worker: 적용 전 실측 MemoryMax=2G 있었음 → 「없으면 896M」미해당. High=1.5G / Max=2G 유지.

## L-3a systemctl show 원문 (적용 직후 2026-09-13 15:39 UTC)

===== SHOW factory =====
MemoryCurrent=192512
MemoryHigh=1288490188
MemoryMax=1610612736
===== SHOW ws =====
MemoryCurrent=3289088
MemoryHigh=209715200
MemoryMax=268435456
===== SHOW async =====
MemoryCurrent=3121152
MemoryHigh=104857600
MemoryMax=134217728
===== SHOW queue-worker =====
MemoryCurrent=1867776
MemoryHigh=1610612736
MemoryMax=2147483648

## L-3a 재캡처 (2026-09-14 14:13 UTC, restart 없음)

factory      MemoryCurrent=124411904 MemoryHigh=1288490188 MemoryMax=1610612736  active
ws           MemoryCurrent=26292224  MemoryHigh=209715200  MemoryMax=268435456   active
async        MemoryCurrent=55189504  MemoryHigh=104857600  MemoryMax=134217728   active
queue-worker MemoryCurrent=95465472  MemoryHigh=1610612736 MemoryMax=2147483648  active

바이트 환산: factory High=1.2G Max=1.5G · ws High=200M Max=256M · async High=100M Max=128M · queue High=1.5G Max=2G

## L-3b 적용 원문 (Done 아님)

crontab:
7 15 * * *  ubuntu  .../bitget.sh --enqueue --scan-futures-ema5-r2

queue-worker: Started 2026-09-13 15:39:47 UTC · "queue worker started"
적용 이후 커널: NO_OOM_SINCE_APPLY
canary 슬롯 UTC 15:07 — 재캡처 시각 14:13 UTC 기준 첫 enqueue 미도래.
24~48h 3종(claim→done / dmesg killed 0 / MemoryCurrent 피크)은 첫 슬롯 이후 재수집.

금지: C-2 / MDD5% / live / ENABLE_REAL_EXECUTION
---CLAUDE---
```

파일 SSOT: 본 파일 상단. 인덱스: `CURSOR_TO_CLAUDE.md`

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 23:13 KST · 1번 승인 반영 (재적용 없음)

디렉터 확정: L-3a 승인 · L-3b A안 · 전체 큐(L-3) 보류.
서버에 **어제 이미 달아 둠**. 896M로 줄이지 않음. 서비스 restart 안 함.

### L-3a 원문 재캡처 (now)

```
factory      MemoryCurrent=124411904 MemoryHigh=1288490188 MemoryMax=1610612736  active
ws           MemoryCurrent=26292224  MemoryHigh=209715200  MemoryMax=268435456   active
async        MemoryCurrent=55189504  MemoryHigh=104857600  MemoryMax=134217728   active
queue-worker MemoryCurrent=95465472  MemoryHigh=1610612736 MemoryMax=2147483648  active
```

### L-3b

crontab: `--enqueue --scan-futures-ema5-r2` 유지.
적용 이후 OOM: `NO_OOM_SINCE_APPLY`.
queue-worker 기동 로그 OK. **canary 슬롯 UTC 15:07은 오늘 14:13 UTC 기준 아직 미도래** → claim→done 완료 조건은 내일 이후.

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 · CAT-L-FENCE-01 구현

**Ask:** Handoff 스펙 일치 OK 여부. L-1/L-2 에스컬레이션 취소 수용.

**엔지니어 분기:** queue-worker Max **2G 유지** + High=1.5G (「없으면 896M」는 해당 없음).

### L-3a 원문 (`ubuntu@43.202.40.136`)

```
===== SHOW factory =====
MemoryCurrent=192512
MemoryHigh=1288490188
MemoryMax=1610612736
===== SHOW ws =====
MemoryCurrent=3289088
MemoryHigh=209715200
MemoryMax=268435456
===== SHOW async =====
MemoryCurrent=3121152
MemoryHigh=104857600
MemoryMax=134217728
===== SHOW queue-worker =====
MemoryCurrent=1867776
MemoryHigh=1610612736
MemoryMax=2147483648
```

(재시작 직후 Current는 작음. 피크는 L-3b 관찰.)

### L-3b

서버 crontab 1줄:
`bitget.sh --enqueue --scan-futures-ema5-r2` (UTC 15:07)
다른 scan_* 는 inline 유지.
**24–48h 잔여:** queue-worker claim→done · `dmesg -T | grep -i killed` 0 · queue MemoryCurrent 피크.

### L-4

`post_deploy_obs_digest_bg.py`: `scan_last_cgroup_by_mode` · `fenced_units_memory_snapshot` · 실패 null+unavailable.
테스트: `test_post_deploy_obs_digest_bg.py` + `test_bitget_staggered_schedule.py` **24 passed**.
**서버 git pull 전엔 일일 텔레그램에 필드 없음.**

**금지 준수:** gates / live / C-2 / MDD5% / ENABLE_REAL_EXECUTION / daily_audit 비접촉.

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 · CAT-L 진단 확정 (코드 없음)

**SSH:** `ubuntu@43.202.40.136` (Stop/Start 후 IP 변경). host `ip-172-26-7-213`.

**오늘 죽음:** 커널 패닉/디스크 풀 아님. 직전 부트 08-17~09-13 14:56 UTC **정상 poweroff**(Lightsail Stop). 그날 저널 OOM/`No space` 없음.

**원인 하나로 좁힘 (재발 구멍):** factory `MemoryMax=1.5G`는 유닛 템플릿에 있음. 그러나 **cron 스캔은 `cron.service` cgroup**이라 그 상한이 안 먹음. 09-07 08:44 UTC `global_oom`이 cron python(RSS≈897M)을 죽임. 지금(리부트 직후)도 auto_pilot(factory) + `scan_futures_ema5_r2`(cron) 동시. crontab 스캔은 **inline**, `--enqueue` 없음.

**기각:** L-1/L-2 미설치 — logrotate `bitget-dante` Aug 2 존재 · journal-vacuum/backup.timer enabled+active · logs 108K · df 38%.

**drop-in:** `dante-bitget-factory.service.d` **없음**. ws/async `MemoryMax=infinity`.

### 요청 Handoff (구현은 그 후)

1. HIST_10 §2.4 MemoryHigh+MemoryMax drop-in (factory + ws + async) + `systemctl show` 원문 캡처를 종료 조건으로
2. cron 스캔을 factory cgroup 또는 `--enqueue`/큐 워커로 넣는 최소 경로 (b-2 canary vs 전면 b-3 — 디렉터 선택)
3. Layer2 digest 3필드 — L-1/L-2는 이미 켜져 있으니 **MemoryMax+cron cgroup**이 🔴 칸이 되게

Layer3 증설(8GB/160GB): 디스크 38%라 급하지 않음. RAM 4G는 cron 겹침이 남으면 여전히 위험 → 디렉터 판단.

C-2/MDD5%/live/`ENABLE_REAL_EXECUTION` 금지. 거래 경로 비접촉.

### Ask 디렉터 (절대규칙 12, 유지)

CAT-L 종료 = 서버 명령 **원문 캡처** 필수. 이번이 그 예시.

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 · CAT-L 크래시 진단 (코드 없음)

**한 줄:** 서버가 왜 죽었는지 지금 확인 중. 로그/메모리 안전장치는 설계는 됐는데 서버에 켜졌는지 확인이 빠졌던 게 유력 원인.

**로컬에서 한 일:** SSOT 대조만. `ssh ubuntu@15.165.236.69` **Connection timed out**. 6블록 원문 **없음**. restart 안 함.

**기록 대조 (체크박스 아님):**
- L-1/L-2: 08-02 Claude OK · `05` 잔여 = 08-17 서버 install 미확인 (08-28까지 체크 안 닫힘)
- MemoryMax drop-in: `00` **미설치 가능** (유닛 템플릿 `factory`는 `MemoryMax=1.5G` — **실측 show 없음**)
- HIST_13 2026-07-04: 로그 무제한 = 1년 방치 시 가장 확실한 서버 파괴 요인
- digest 08-17~19: L-1 ok / L-2 timer later active / overseer running **한 번** — 설치 스크립트 캡처 아님. L-2 `python: command not found`. 08-19 digest overseer `exit=1` 불일치
- cron→큐 b-2/b-3: `infra_next_steps_a_b_plan.md` 운영자 전환 대기 (기본 off) — **후보 2, 미확정**

**금지 준수:** 거래 경로 미수정 · 원인 없이 restart 안 함 · Layer2 digest 필드 선코딩 안 함.

### Ask 1 — 디렉터 (절대규칙 12)

CAT-L은 앞으로 Cursor「구현 완료」만으로 `05` 종료하지 말고, **서버 명령 원문 캡처가 붙어야 종료**로 인정할지 **지금 확정**해 주세요.

### Ask 2 — Claude (6블록 온 뒤에만 Handoff)

지금은 Handoff 확정 금지. 원문 오면 원인 1개로 좁혀 `CLAUDE_TO_CURSOR.md` prepend:
- Layer1: 기존 `install_bitget_logrotate.sh` / `install_bitget_backup.sh` / HIST_10 §2.4 drop-in **설치 + is-enabled/is-active 원문**
- Layer2: digest read-only 3필드 (`logrotate_installed` / `backup_timer_active` / `memorymax_configured`) — 거래 비접촉
- Layer3 증설(8GB/160GB): 디렉터 비용 판단 · Claude/Cursor 임의 결정 금지

C-2 / MDD5% / live / `ENABLE_REAL_EXECUTION` 금지.

---

> 아래는 이전 누적 이력.

> **갱신(이전)**: 2026-08-23  
> **유형**: **UNIVERSE-BT-U0** 구현 완료 · **WAIT_CLAUDE_OK** (전문은 `CURSOR_TO_CLAUDE.md` 미러)

---

## OUTBOX — 2026-08-23 · UNIVERSE-BT-U0 완료 (문서)

**UNIVERSE-BT-U0: 구현 완료** → Claude **OK | 수정 spec** 요청.

| 파일 | 역할 |
|------|------|
| `14_UNIVERSE-BT_구조생존검증.md` | 신규 §1~§5 |
| `00` 말미 | 포인터 1줄 |
| 코드 | **없음** · U1 미착수 |

상세: `CURSOR_TO_CLAUDE.md` 상단.

---

## OUTBOX — 2026-08-23 · Ask · UNIVERSE-BT (전코인 구조생존 백테스트)

**계기(디렉터):** Bitget에 현물·선물로 상장된 코인에 **현재 퀀트 구조를 그대로** 얹어 전수 백테스트 → "구조가 살아남는지" 단서 확보.  
미래(L2 paper·forward)가 주 검증이지만, 히스토리 생존은 **중요한 단서**. Claude와 협업 설계 요청.

### Cursor 엔지니어 브리핑 (2줄)
1. 지금 `time_machine_backtester.py`는 **크래시 구간 MAE/MFE 스트레스**일 뿐 — DNA·gate·scanner 경로 **재현 아님**. "싹 다"를 그 루프에 얹으면 구조 검증이 아니라 가짜 생존률이 나옴.  
2. 전상장 심볼 × 풀스택 리플레이는 4GB·`TIME_MACHINE_MAX_*`·OHLCV 커버리지 한계에 막힘 → **유니버스 스냅샷 → 배치/체크포인트 리플레이 하니스(라이브·config 비접촉)** 가 맞고, 결과는 **IV L0 단서만** (LIVE/B1 승격 금지).

→ **흡수**: U0 Handoff로 로드맵·지표 확정. 이후 이력은 아래 유지.

### 로컬 팩트 (읽기만)

| 자산 | 역할 | 한계 |
|------|------|------|
| `mtf_data_updater.load_dynamic_universe` | 현물/선물 거래량 유니버스 | "상장 전부" ≠ volume floor 통과분 · zombie BL |
| OHLCV `BITGET_SPOT_*` / `BITGET_FUT_*` | 히스토리 바 | 상장 전·갭·신규상장 survivorship |
| `master_scanner` + `signal_engines` + gates + ledger | **실제 퀀트 구조** | 백테스트 전용 리플레이 엔트리 **약함** |
| `time_machine_backtester.py` | 크래시 SL/TP 스트레스 | 구조≠재현 · 테이블 cap |
| `validation/walk_forward_*` | CLOSED trade OOS shadow | **이미 들어온 트레이드**만 · 전유니버스 스캔 아님 |
| 현황판 #14 R&D 샌드박스 | 🟡 | "라이브 분리 연구실 약함" |

### IV / 헌법 (위반 금지)
- 본 작업 산출 = **L0** (`docs/independent_verification` · time_machine/mutant급) → **LIVE·B1「달성」·CAGR 단정 금지**
- R1a paper OPEN 관측·R6 L2와 **혼동 금지**
- `ENABLE_REAL_EXECUTION` · Kelly · MDD tier · execution_safety · deathmatch **live** · WF promotion block **비접촉**
- 주식 루트 `forward/` · `performance_budget_governor` **수정 금지** · Adapter만

### Ask — Claude가 확정할 것

1. **로드맵 자리**: B1-LADDER(R1a 관측)와 **병렬 R&D sub-phase**인가, R2 이후인가, 별도 `UNIVERSE-BT-0x` 트랙인가? (R1a Kill/관측 **차단하지 말 것**)
2. **성공 정의(구조생존)**: 예) 심볼당 hit→gate pass→가상진입 비율 · 크래시 구간 청산률 · 국면별 LONG/SHORT 비대칭 — **연복리%를 성공 계약으로 쓰지 말 것** (B1 계약과 분리)
3. **범위**: "상장 전부" vs `load_dynamic_universe`+OHLCV 보유분 · SPOT/FUT 분리 리포트 여부
4. **sub-phase 분해 초안 요청** (Cursor 제안 — Claude가 ID·순서·Critical 확정):
   - **U0** 문서: 유니버스 스냅샷 정의 · survivorship 고지 · L0 라벨 · Kill(과신 표현)
   - **U1** 코드: read-only 리플레이 하니스 (scanner/engines 경로 재사용, paper DB·config_kv 쓰기 금지, 결과 JSON/SQLite 격리)
   - **U2** 배치: spot→fut 또는 샤드 · 체크포인트 · 메모리 cap 존중
   - **U3** 리포트: 구조생존 표 + Claude 해석 슬롯 (CAGR 승격 문구 템플릿 **금지**)
5. **첫 Handoff**: U0 문서만? U1까지? — `CLAUDE_TO_CURSOR.md`에 CAT·위험도·롤백·테스트 명시

### 디렉터 한 줄 (Claude에 붙여넣기)
```
bitget/docs/work_phases/CURSOR_TO_CLAUDE.md 상단 「UNIVERSE-BT Ask」설계. R1a OBSERVE는 유지. OK면 CLAUDE_TO_CURSOR에 U0(또는 첫 sub) Handoff만 파일로. 채팅 말고 파일.
```

### Cursor 상태
- **코드 미착수** · R1a **관측 유지** 병행
- NEXT_ACTION: R1a=OBSERVE · 본 Ask=`WAIT_CLAUDE_HANDOFF`(설계만)

---

## OUTBOX — 2026-08-23 · B1-LADDER-R1a 완료 (문서) + R0 Claude OK 반영

### Claude OK 수신
**B1-LADDER-R0: OK** (2026-08-23) — 05·CLAUDE_TO_CURSOR 상단 기록 완료.

### R1a 구현
| 파일 | 역할 |
|------|------|
| `13_B1_신뢰사다리.md` | §3 아래 **R1a 판정 절차** 소절 추가 (PASS/관측유지/FAIL a\|b) |
| `CLAUDE_TO_CURSOR.md` | PREPEND(OK+R1a Handoff) 최상단 부착 |
| `09` · `track_b_NEXT_STEP` | Downloads 갱신안 그대로 반영 |
| `05` · `NEXT_ACTION` · `00` | R1a OBSERVE · R0 Claude OK |

### 코드
**없음** (config/gates/Kelly/live 비접촉)

### 이번 판정 (신선 SQL 미수신)
| OPEN | CLOSED | R0 경과 | short_funnel | **판정** |
|------|--------|---------|--------------|----------|
| 0 (직전 SSOT) | 10 | &lt;4주 (앵커 2026-08-23) | 미조회 | **관측 유지** |

디렉터 신선 SQL 오면 동일 표에 숫자만 대입해 재판정.

### Ask
R1a 문서: **OK | 수정 spec** (관측 유지 중에는 주간 숫자만 OUTBOX). FAIL 확정 시에만 R1b Handoff.

---

## OUTBOX — 2026-08-23 · B1-LADDER-R0 완료 (문서)

**B1-LADDER-R0: 구현 완료** → Claude 스펙 일치 검증 요청

### 로컬 스냅샷
| 파일 | 역할 |
|------|------|
| `docs/work_phases/13_B1_신뢰사다리.md` | **신규** §1 성공계약 · §2 렁 R0~R6(+R1a/b·R3~5 승인문구) · §3 Kill · §4 신뢰밴드 · §5 CAT · §6 R1a SQL |
| `00_마스터_로드맵.md` §0.4 말미 | **1줄만** `→ 상세 렁·Kill 기준: 13_B1_신뢰사다리.md` · **표 비변경** |
| `CLAUDE_TO_CURSOR.md` | Handoff 전문 보관 |
| `05` / `00` 용어집 / `09` / `NEXT_*` | 세션 종료 의무 갱신 |

### 스펙 확인
- 성공 = B1만 (12~18% AND MDD≤5% · 6~12개월) · B2/B3/live/G4 비계약
- 순서 `R0→R1→R2→(A06)→R3∥R4→R5→R6` · Kill 표 · 신뢰밴드 35~45→…→80~90
- **코드·config_kv·execution_safety·gates·Kelly·deathmatch live 비접촉**

### R1a 서버 실측
| 출처 | OPEN | CLOSED | 비고 |
|------|------|--------|------|
| **이 세션** | (미조회) | (미조회) | Cursor 환경에서 VPS `BITGET_DB_STORAGE_PATH` **미접속** |
| **직전 SSOT** 2026-08-23 OUTBOX/VPS | **0** | **10** (W2/L8) | 배선 생존 · 신규 진입 정체 후보 · **냉시동 vs 구조막힘 미최종** |

→ 디렉터: §6 SQL로 **신선 실측** 후 숫자 회신. R1b는 R1a FAIL(구조막힘) 확정 전 착수 금지.

### Ask
**B1-LADDER-R0: OK | 수정 spec: …**  
OK면 05에 Claude OK 기록 · 다음 Handoff는 R1a 관측 마감 또는 R1b(조건부).

---

## OUTBOX — 2026-08-23 · Ask · B1 80~90% 신뢰 사다리 (설계 요청)

**계기:** 디렉터 — 시나리오 기준성공 35~45%로는 안 됨. **80~90% 성공률**을 만들 것.  
검증만으로는 부족 → **현실·팩트 완성** + Claude/Cursor 최상의 시나리오·작업 순서.

### Cursor 엔지니어 브리핑 (1줄)
숫자를 희망으로 올리지 말고, **성공 정의를 B1으로 고정**한 뒤 불확실성 렁(R0~R6)을 닫아 **조건부 P(B1|사다리)** 를 80~90%로 설계. B2/B3는 계약 밖.

### 혼동 금지 (팩트)
| 종류 | 의미 | 지금 |
|------|------|------|
| P(성공\|오늘) | MDD 미조임·funding 미반영·OPEN≈0·n≈10 | **35~45%** (솔직) |
| P(B1\|사다리 통과) | 팩트 렁 닫힌 뒤 | **설계 타깃 80~90%** |

→ 디렉터가 원하는 80~90% = **후자**. 전자를 거짓으로 올리는 것은 SSOT/IV 위반.

### 성공 계약 초안 (Claude 확정 요청)
- **성공** = Track B **B1만**: 연복리 **12~18%** AND MDD **≤5%** (B1 시작 후 6~12개월 시계)
- **비계약**: B2 18~25% · B3 25~35% · 실전 LIVE · 상품화 G4 — 스트레치/별도
- **Kill**: 렁 실패 시 목표 하향·롤백·중단 → 남은 경로만 고신뢰 유지 (이게 80~90%를 정직하게 만드는 장치)

### 신뢰 사다리 초안 (Claude가 ID·순서·Critical 승인문구 확정)

| 렁 | 팩트 완성 | 닫는 구멍 | Critical? | 비고 |
|----|-----------|-----------|-----------|------|
| **R0** | 성공=B1 계약 · Kill 기준 문서화 | 목표 과다 | 문서 | `00` §0.4 보완 or 별도 SSOT |
| **R1** | OPEN 처리량 · 퍼널(롱/숏) 진단·복구 | 표본 정체 | 관측→mini Handoff | VPS: OPEN=0 · CLOSED≈10 |
| **R2** | `06` 효과표 2~4주 채움 | 구현≠효과 | 관측 | A/B shadow 유지/롤백 |
| **R3** | MDD 3/4/5% + Kelly↓ + lev≤3 | 5% 미강제 | 🔴 | A-6 / Risk Profile B |
| **R4** | C-2 funding PnL | paper 낙관 | 🔴 | close PnL 오염 해소 |
| **R5** | deathmatch alloc **live** | 패자 자본 | 🔴 Go/No-Go | shadow 4w 후 |
| **R6** | L2: trades≥30 · ≥56일 · rolling MDD≤5% · 페이스 | 통계 과신 | 게이트 | G2 정합 · IV L2 |

**신뢰 밴드(설계):** 오늘 35~45 → R0~R2 후 50~65 → R3~R5 후 70~85 → R6 통과 **80~90**.

### 최상의 시나리오 A+ (작업 축)
1. **0~4주**: R1+R2 (처리량·06) — Critical 손대지 않음  
2. **Go/No-Go**: R3→R4→R5 순차 Handoff (디렉터 Critical 승인 필수)  
3. **6~12개월**: R6 B1 시계 관측 → 통과 시 “조건부 80~90% 달성 경로 입증”

### Ask Claude (설계만 · 이번 라운드 코드 구현 X)
1. 위 **성공 계약** OK? B1만 80~90% 대상으로 고정해도 되나?  
2. 렁 ID·이름·순서 확정 (`B1-CONFIDENCE-LADDER` 가칭) · R1을 어떤 mini Handoff로 쪼갤지 (OPEN 정체 원인: DNA/Cos/게이트/국면)  
3. R3~R5 Critical 각각의 **디렉터 승인 문구** + 의존성 (C-2를 MDD 전/후?)  
4. Kill 기준 표 (렁별 FAIL → 행동) SSOT 위치 (`00` vs `06` vs 신규 `13_B1_신뢰사다리.md`)  
5. 첫 Handoff는 무엇인가? (제안: **R0 문서** 또는 **R1 처리량 진단 전용** — Critical 비접촉)

**금지 유지 (이번 Ask에서 구현 지시 금지):** Kelly 상향 · live · LS-NORTH-STAR 하드캡 분리 · 성급한 R3~R5.

**디렉터:** 이 OUTBOX → Claude. Claude 응답 = `CLAUDE_TO_CURSOR` 설계/Handoff 또는 Mirror. Cursor는 Handoff 전 구현 금지.

---

## Claude OK — LS-GOAL-UX-01 (2026-08-23)

**판정: OK.** position_side 어댑터 · kill-switch 폴백 · Kelly/gates/live 비접촉 · SPOT 숏 각주 정합.  
**다음:** 디렉터 서버 pull · digest/북극성 육안. LONG blocked / LS-NORTH-STAR-01은 후속·🔴 defer.

---

## OUTBOX — 2026-08-23 · LS-GOAL-UX-01 구현 (기록)

**LS-GOAL-UX-01: OK** → Claude OK 2026-08-23 · **DONE**

### 로컬 스냅샷
| 파일 | 역할 |
|------|------|
| `observability/ls_split_summary_bg.py` | **신규** `collect_ls_split_summary` · plain/HTML |
| `north_star_panel_bg.py` | L/S 2열 블록 (쉬운판 4칸 아래) |
| `post_deploy_obs_digest_bg.py` | kid 진행줄 아래 `ls_plain` 1줄 |
| `infra/memory_policy.py` | `POST_DEPLOY_OBS_LS_SPLIT_ENABLED=True` |
| `tests/test_ls_split_summary_bg.py` | **신규** |

### 스펙 확인
- LONG에 `blocked_today` **없음** · SHORT `blocked_today` = short_funnel `blocked_short_total` import
- 목표 MDD/연복리/B0 **미분리** · kill-switch false → 기존 출력(롱 줄 없음)
- SPOT 숏 불가 각주 포함
- 컬럼은 `position_side` (Handoff `side` → 로컬 스키마 맞춤)

### 비접촉
`forward/gates.py` · `gmm_dna_alpha_sync.py` · `dual_north_star_ledger.py` · Kelly · live · short_funnel 버킷 재계산 **없음**

### 테스트
`test_ls_split_summary_bg.py` + `test_north_star_panel_bg` + `test_post_deploy_obs_digest_bg` → **passed**

### Ask
Claude OK / 수정 spec 한 줄. OK면 05 Claude OK 기록.

---

## OUTBOX — 2026-08-23 · Ask · 롱/숏 분리 목표·쉬운판 (LS-GOAL-UX)

**계기:** 디렉터 — 코인은 롱·숏 둘 다 있음 → **목표를 롱/숏으로 나눠** 읽기 쉽고 퀄리티 좋게. 전체 구조에서 L/S 흐름 확인 필요.

### Cursor 로컬 맵 (구현 전 브리핑)

```
스캔 → side(LONG|SHORT) → try_add → OPEN → track → CLOSED
         ↑
spot+SHORT = hard reject (선물만 숏)
dante SHORT = futures-only (SHORT-DANTE-FUT-01)
Cos/funding/BULL = SHORT soft 감점 (임계값 동결)
```

| 이미 있음 | 없음 |
|-----------|------|
| digest **숏 퍼널** (OPEN L/S · 차단 버킷) | 북극성 **롱 목표 vs 숏 목표** 분리 |
| overseer 당일 closed long/short count | 사이드별 MDD/누적/게이트 칸 |
| Track B 북극성 = **통합 장부** | 초등 쉬운판에 「롱 건강 / 숏 건강」 2열 |

**서버 실측(당일):** OPEN=0 · CLOSED 10(W2/L8) — 사이드별 분해는 미조회(Ask 시 SELECT 제안).

### 엔지니어 제안 (Cursor)
표시만 CAT-J: digest/북극성에 **롱 칸 · 숏 칸**(OPEN/CLOSED/당일차단/누적손익 요약).  
MDD5%/연12~25%를 사이드별로 **하드캡 분리**하는 건 Critical·원장 설계 → Claude 판단. 기본안 = **목표 숫자는 Track B 공유 · 진행 칸만 L/S 분리**.

### Ask Claude
1. sub-phase ID 확정? (예: `LS-GOAL-UX-01` 표시만 / `LS-NORTH-STAR-01` 목표 분리)  
2. 스펙: 쉬운판 2열 필드 목록 · 기존 short_funnel과 중복 제거 규칙  
3. Critical 비접촉(Kelly·gate·live) 유지 OK?  
4. SHORT SECTOR 최종 OK와 순서 — 먼저 LS-GOAL-UX?

**디렉터:** 채팅 말고 이 OUTBOX → Claude. Cursor는 Handoff 전 구현 금지.

---

## OUTBOX — 2026-08-23 · AI 감시관「활동 부재」감사 (Cursor 단독 판독)

**계기:** 디렉터 텔레그램 「👁️ Bitget AI 상시 감사관」문제점 = 활동 부재 · 기회 상실.  
**질문:** 의도적으로 막아둔 건지 vs 파이프라인 고장인지.

### Cursor 판정 (코드 SSOT · **서버 DB 실측 반영 2026-08-23**)

**VPS 실측:**
```
CLOSED_LOSS|8
CLOSED_WIN|2
(OPEN 행 없음 → OPEN=0)
```
→ 장부·파이프라인 **과거 배선 OK** (누적 CLOSED=10 = POST_DEPLOY 실측과 일치).  
→ **지금**은 포지션 0 · 신규 진입 대기 국면. DB 단절/전선 절단 ❌.

| 층 | 무엇인가 | 판정 |
|----|----------|------|
| **1. 리포트 문구** | Gemini 자유 서술 | 「활동 부재」= **하드 킬스위치 아님** |
| **2. 팩트 구멍** | facts에 OPEN 미조회 | 「보유 정보 부재」문구는 **과잉** 가능(실제 OPEN=0이면 내용상 맞음) |
| **3. 정책 보수** | kelly 0.006 · HIGH_VOL · B0 · DNA 대기 | **의도적 축소** · 버그 단정 ❌ |
| **4. 현재 상태** | OPEN=0 · 누적 CLOSED=10 | **파이프라인 생존 + 신규 진입 정체** (고장≠전무) |
| **5. POST_DEPLOY** | Cos n≈0 · DNA RANK 재료 대기 | 신규 OPEN이 안 생기는 **주 원인 후보** |

**결론 한 줄:** 배선은 살아 있고(CLOSED 10), 지금은 OPEN이 비어 **관측·게이트·재료 대기** 쪽. 「막아서 활동부재」가 아니라 「들어가지 못해 비어 있음」.

### 디렉터 서버 한 줄(구분용)

```bash
DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
sqlite3 "$DATA/bitget_market_data.sqlite" \
  "SELECT status, COUNT(*) FROM bitget_forward_trades GROUP BY status;"
# + POST_DEPLOY digest / short_funnel 칸
```

### Ask Claude (설계만 · Critical 비접촉)

1. 위 3층 판정 OK?  
2. 다음 mini Handoff 필요? 예: **OVERSEER-FACTS-01** — facts에 OPEN 수·blocked_today 요약 추가(표시만, Kelly/gate 비접촉).  
3. 불필요면 SUB_DONE · 관측 유지.

**작업 방향(디렉터 승인됨):** 본 감사 = Cursor 단독. 팩트 보강 구현만 Claude Handoff 후.

---

## OUTBOX — 2026-08-23 · NS-BG-CRON-ISO-01 주식 북극성 → 코인 채팅

**증상 (디렉터 스크린샷):** 코인 구조 텔레그램에 `📊 주식 북극성 · 일간/주간` + `no such table: forward_trades` + Track A KR/US.  
**추가 보고:** 코인 북극성·POST_DEPLOY_OBS는 **한 통도 안 옴**.

**원인 A (오염):** `update_bitget.sh` → `install_director_digest_cron.sh` → 주식 19:30이 Bot-2에서 REPORT_BOT 발송.  
**원인 B (미수신 · 가설):** 코인 일보 cron(`--post-deploy-obs-digest` UTC 11:00) 미설치·미실행·실패. 주식 cron은 매 업데이트마다 강제 설치되어 왔고, 코인 일보 줄은 예전 crontab에 없으면 **조용히 안 감**. 코인은 **일 20:00만**(주간 없음).

**수정 (bitget/** only):**
| 파일 | 변경 |
|------|------|
| `update_bitget.sh` | director-digest **설치 제거** · 잔여 시 uninstall |
| `uninstall_stock_north_star_cron.sh` | 신규 |
| `diagnose_coin_digest.sh` | 신규 — cron/로그/REPORT vs BITGET 채팅 + `--send` 후 **exit·로그·sent=** 표시 |
| `install_bitget_cron.sh` | post-deploy-obs 줄 **필수** 검증 |
| `audit_bitget_stack.sh` | 주식 cron 있으면 fail · digest 로그 유무 warn |
| `post_deploy_obs_digest_bg.py` | Telegram HTML→plain 재시도 · 길이 분할 · HTTP 실패 로그 |

**디렉터 즉시:**
```bash
git pull && sudo bash bitget/deploy/uninstall_stock_north_star_cron.sh
sudo INSTALL_ROOT=$PWD bash bitget/deploy/install_bitget_cron.sh
bash bitget/deploy/diagnose_coin_digest.sh --send
```

**Ask Claude:** Ops 격리+진단 OK 한 줄. 루트 install_director 주석 Track A 정리 권고.

---

## OUTBOX — 2026-08-21 · SHORT 조건부 OK 3확인 회신 (Cursor)

Claude 요청 형식에 대한 로컬 확인:

### SHORT-DANTE-FUT-01 — blocked_history
**확인:** spot SHORT hard-reject · SHORT Cos reject 모두 `bitget.shadow_tracking.record_blocked_trade` **동일 함수** → 테이블 `bitget_blocked_trade_history` **기존 컬럼만** INSERT. 신규 테이블/컬럼 **없음** (CAT-D 스키마 비접촉).

### CRYPTO-SECTOR-01 — ①②③

| # | 질문 | 결과 |
|---|------|------|
| ① | CAT-MAP Single Writer 표 | **추가함** — `PREDICTED_NEXT_SECTOR` \| `auto_pilot.detect_coin_regime` (+ system_auto_pilot) \| Readers D/J/M · G meta_sync **아님** |
| ② | 맵 입력 | **신규 breadth 공식 없음.** 동일 함수 안 기존 `regime`/`breadth_state` 재사용 → 이미 `CURRENT_REGIME_KEY`·`CRYPTO_BREADTH_STATUS`로 쓰이던 값. (meta_sync `REGIME_ANALYSIS` ensemble 키가 아니라 **coin detect_coin_regime 기존 경로**) |
| ③ | C/F 미소비 | **C 스캐너·signal_engines·F trading/Kelly 모듈: `PREDICTED_NEXT_SECTOR` 미참조.** Reader는 **D ledger `rotation_prebuy`**(기존: Cos×0.85 · `ROTATION_ADVANTAGE_ACTIVE`일 때만 Kelly×2) + J digest + M overseer. digest-only는 아님 · **C/F 직접 소비 아님** → CAT-G 🔴 Critical 재분류 **불필요** (기존 D soft boost 배선만 Writer가 살아남) |

### 요청
- CRYPTO-SECTOR-01 → **최종 OK** 한 줄  
- 4건 Claude OK를 `track_b_05`에 기록해도 되는지 확정

---

Claude 요청 형식에 대한 로컬 확인:

### SHORT-DANTE-FUT-01 — blocked_history
**확인:** spot SHORT hard-reject · SHORT Cos reject 모두 `bitget.shadow_tracking.record_blocked_trade` **동일 함수** → 테이블 `bitget_blocked_trade_history` **기존 컬럼만** INSERT. 신규 테이블/컬럼 **없음** (CAT-D 스키마 비접촉).

### CRYPTO-SECTOR-01 — ①②③

| # | 질문 | 결과 |
|---|------|------|
| ① | CAT-MAP Single Writer 표 | **추가함** — `PREDICTED_NEXT_SECTOR` \| `auto_pilot.detect_coin_regime` (+ system_auto_pilot) \| Readers D/J/M · G meta_sync **아님** |
| ② | 맵 입력 | **신규 breadth 공식 없음.** 동일 함수 안 기존 `regime`/`breadth_state` 재사용 → 이미 `CURRENT_REGIME_KEY`·`CRYPTO_BREADTH_STATUS`로 쓰이던 값. (meta_sync `REGIME_ANALYSIS` ensemble 키가 아니라 **coin detect_coin_regime 기존 경로**) |
| ③ | C/F 미소비 | **C 스캐너·signal_engines·F trading/Kelly 모듈: `PREDICTED_NEXT_SECTOR` 미참조.** Reader는 **D ledger `rotation_prebuy`**(기존: Cos×0.85 · `ROTATION_ADVANTAGE_ACTIVE`일 때만 Kelly×2) + J digest + M overseer. digest-only는 아님 · **C/F 직접 소비 아님** → CAT-G 🔴 Critical 재분류 **불필요** (기존 D soft boost 배선만 Writer가 살아남) |

### 요청
- CRYPTO-SECTOR-01 → **최종 OK** 한 줄  
- 4건 Claude OK를 `track_b_05`에 기록해도 되는지 확정

---

## OUTBOX — 2026-08-21 · SHORT 최상경로 Ask (디렉터 승인 로드맵)

**계기:** 롱만 진입 · 숏 미사용 · `predicted_sector=UNKNOWN` · R&D/청산 정체.  
**디렉터 승인 계획:** Bitget best-path roadmap (B0 paper · Critical 비접촉).

### Ask (Claude Pro)

Track B **B0**. 숏 구조(TV_SHORT / dante / ledger SHORT)는 있으나:

1. SPOT dante → ledger `현물 숏 불가`로 낭비  
2. FUTURES Cos/funding/국면 페널티로 숏 탈락  
3. `PREDICTED_NEXT_SECTOR` writer 없음 → 항상 UNKNOWN  

**요청:** 아래 순서로 Handoff 검토·OK (또는 수정 spec). Cursor는 디렉터 승인 로드맵대로 **이미 구현**했음 → 스펙 일치 검증.

| 순서 | ID | 내용 |
|------|-----|------|
| 1 | SHORT-FUNNEL-01 | 숏 OPEN/CLOSED·차단사유 read-only 집계 |
| 2 | SHORT-DANTE-FUT-01 | SPOT dante no-op (futures-only SHORT) |
| 3 | SHORT-OBS-GATE-01 | funding/국면/Cos 탈락 관측 (임계값 변경 없음) |
| 4 | CRYPTO-SECTOR-01 | `PREDICTED_NEXT_SECTOR` 코인 writer |
| 5 | SHORT-DNA-01 | **defer** — SHORT CLOSED/MFE 충분 시에만 |

**금지:** C-2 · MDD5% tier · B-2/B-3 live · `ENABLE_REAL_EXECUTION` · Cos/funding **임계값 변경**

### 구현 스냅샷 (검증용)
- `bitget/observability/short_funnel_report_bg.py` + digest 연동
- `master_scanner` / scanner_hooks: spot+dante skip
- ledger: SHORT Cos/spot 차단 → blocked_history 기록(관측)
- `auto_pilot` regime: `PREDICTED_NEXT_SECTOR` writer
- 테스트: funnel · schedule/skip · sector

### Claude 응답 요청
- 각 ID OK / 수정 spec 한 줄  
- SHORT-DNA-01 착수 조건(예: TF당 SHORT mfe≥8 ≥N) 제안 환영

---

## OUTBOX — 2026-08-21 · NS-BG-DASH-01 Bitget 북극성 패널

**요청:** 주식 `[쉬운판]` 참조 → Bitget 구조에 맞게 북극성·목표수익률 보이게. 이미 된 건 유지, 빠진 것만 추가.

### 갭
- 원장 `dual_north_star_ledger` Track B(MDD5% · B0~B3 · 게이트) **이미 수집**
- 주식 19:30 일보는 Track A only (의도적 분리 · Track B 미표시)
- Bitget POST_DEPLOY_OBS는 DNA/연습 관측만 · **북극성 목표·누적·게이트 칸 없음**

### 구현 (bitget/** only · 읽기 전용)
- `bitget/observability/north_star_panel_bg.py` — Track B `[쉬운판]` + 상세(목표 MDD/연복리/게이트/기간수익/NAV)
- `post_deploy_obs_digest_bg.py` — 텔레그램 **첫 메시지**로 북극성 발송 · 원장 **쓰기 안 함**(19:30 cron 전용)
- 테스트 `test_north_star_panel_bg.py`
- 문서: `12_듀얼북극성…` · `09_디렉터_쉬운요약`

### Ask
- Claude: 스펙 일치·격리 OK면 한 줄 OK. 다음 Handoff 불필요면 SUB_DONE 유지.
- 금지 유지: C-2 · MDD5% tier · live · ENABLE_REAL_EXECUTION

## OUTBOX — 2026-08-21 · NS-BG-DASH-01b 코인 전용 재분리

**계기:** 디렉터가 주식 북극성(19:30) 붙여넣으며「코인에 KR/US 북극성 올 필요 없음 · 구조만 참조」지적.

### 수정
- `north_star_panel_bg.py`: Track A 스냅샷·OBS_HOLD(n/20)·갈림길·mega_trend 제거
- 제목 `📊 코인 북극성 · Bitget` · 마일스톤=G1 28일 · spot/futures NAV · MDD5%/B0
- 스냅샷에서 tracks/period_returns의 **A 키 strip**
- 테스트: `📊 주식 북극성` / 갈림길 / OBS_HOLD / Track A 부재 assert

### Ask
- Claude: 01b OK면 한 줄. 주식 채널 비접촉 확인.

---

## Claude OK — NS-BG-DASH-01 (2026-08-21)

- 판정: **OK** — 로컬 스냅샷 vs SSOT(00_마스터_로드맵 §0.4 · 12_문서 · CAT-J) 1:1 일치 · 수정 spec 없음
- MDD5%/연12~25%(B0=측정) 값 원본 일치 · 임의 상수 없음
- 원장 read-only 확인 · forward_trades/gates.py/gmm_dna_alpha_sync.py 비접촉
- SPOT/FUT NAV 분리 표시 확인 (CAT-J §4)
- C-2 · MDD5% tier · live · ENABLE_REAL_EXECUTION 미접촉
- 다음: Handoff 불필요 → **SUB_DONE**. 잔여는 디렉터 서버 pull 후 20:00 텔레그램 첫 메시지 육안 1회만.

---

## OUTBOX — 2026-08-20 · Claude 조건부 OK 닫힘

**Claude:** [CAT-J] 조건부 OK · Mirror → `ARCHITECT_MIRROR.md` 상단 기록.  
**Cursor 확인:** enum 정식명 **`DB_PATH_OR_ENV`** (OUTBOX 요약의 `DB_PATH`는 축약 표기만).  
필드: `n_closed_by_tf` · `n_mfe8_by_tf` · `gmm_cluster_n` · `last_error`.  
**잔여:** 디렉터 서버 배포 후 텔레그램 「재료 덜 모였어요」👁️.  
**후속 메모(미착수):** DATA_WAIT streak · 01b/digest 계산 통합.

---

## OUTBOX — 2026-08-20 · POST_DEPLOY_OBS-DNA-UX-01 구현 검증

**요청:** Handoff 스펙 일치 여부 OK/수정 spec. OK면 Claude OK 한 줄 + 09/NEXT_STEP 반영 안내.

### 구현 요약
- `diagnose_dna_state` 순서 고정: DB_PATH → RANK_OK → DATA_WAIT_LOW_MFE → GMM_EMPTY → SYNC_FAIL → UNKNOWN
- Spec2 초등 문구 · Spec3 숫자 메모 · Spec5 paste(DIRECTOR_SSH_CHECK / REPORT_TO_CLAUDE)
- DATA_WAIT → 대시보드 **🟡 missing** (🔴 problem 아님)
- kill-switch `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED` (default true)
- `gmm_min_rows=12` — `data_miner._fit_gmm_templates` 주석 출처 · `GMM_FIT_MIN_ROWS_OBSERVED`
- 테스트 **10 passed** (`test_post_deploy_obs_digest_bg.py`)

### 로컬 구조 스냅샷
- `bitget/observability/post_deploy_obs_digest_bg.py` — diagnose/collect/wire/dashboard/numbers/paste
- `bitget/observability/gmm_dna_alpha_report_bg.py` — `collect_closed_mfe_counts_by_tf` · `count_gmm_template_clusters`
- `bitget/infra/memory_policy.py` — `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED`
- 비접촉: `forward/gates.py` · `evolution/gmm_dna_alpha_sync.py`

### Ask Claude
채팅 말고 파일에 OK 또는 수정 spec. C-2/MDD5%/live 금지 유지.

---

## OUTBOX — 2026-08-20 · Ask: DNA 일일진단 미니 Handoff

### 디렉터 → Claude 붙이기용 (이 블록 전체)

```
Track B · 미니 Handoff 요청 (구현은 Cursor, 설계만 Claude)

목적:
일일 텔레그램「코인 연습 · 오늘 한눈에」DNA 칸이 지금은
RANK1~3 유무만 보고 같은 🔴 문구만 반복한다.
업로드 고장이 아니라 진단력 부족이다.
디렉터가 텔레그램만 보고 (관측유지 / 서버ops / Cursor·Claude 작업) 분기할 수 있게
why 한 줄이 나오게 해 달라.

배경 실측 (2026-08-19 VPS, BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data):
- CLOSED=10 (1H=2, 2H=1, 4H=7)
- n_mfe8=0, n_mfe5=0 전 TF · max_mfe≈3.55
- mine_bitget_dna_templates → 0 templates
- gmm_dna_alpha_sync --force → no_rankable_clusters
- overseer systemd active(running) · L-2 timer active (별건)
- 코드 조건: TF당 mfe≥BITGET_MIN_MFE_FOR_MINING(기본8) · feature dropna 후 ≥12행이어야 GMM

요청물 (Handoff에 넣을 것):
1) DNA 진단 상태 enum (예: RANK_OK / DATA_WAIT_LOW_MFE / GMM_EMPTY /
   SYNC_FAIL / DB_PATH_OR_ENV / UNKNOWN) — 판정 조건 표
2) 각 상태별 초등 문구 plain (텔레그램 kid dashboard 1줄) +
   숫자 메모에 넣을 필드 목록 (예: n_closed, n_mfe8 by TF, gmm_cluster_n, last_error)
3) cursor_action 권고: OBSERVE_HOLD | DIRECTOR_SSH_CHECK | REPORT_TO_CLAUDE | NONE
   (문턱 완화·실전·MDD5%·ENABLE_REAL_EXECUTION 권고 금지)
4) 구현 범위 한정:
   - 수정 허용: bitget/observability/post_deploy_obs_digest_bg.py
     (+ 필요 시 gmm_dna_alpha_report_bg.py 읽기전용 헬퍼, tests)
   - 금지: gates.py · gmm_dna_alpha_sync.py 본체 로직 · execution_safety ·
     BITGET_MIN_MFE 기본값 변경 · C-2/live
5) 테스트: 상태별 fixture 3~5개면 충분
6) sub-phase ID 제안 (예: I-GMM-DNA-DIGEST-01 또는 POST_DEPLOY_OBS-DNA-UX-01)

산출: bitget/docs/work_phases/CLAUDE_TO_CURSOR.md (또는 Track B Handoff 관례 파일)에
CAT-HANDOFF 형식 미니 Handoff 1건. 채팅 장문 말고 파일.

디렉터 승인: DNA「제대로 된 진단」UX — OK. 정책(문턱완화)은 이번 범위 밖.
```

### Cursor 메모 (Claude 답 오기 전)

- status 기대: Claude가 Handoff 쓰면 → `WAIT_CURSOR_IMPL`
- 구현 전 코드 손대지 말 것
- 관련 실측 OUTBOX: 아래「DNA 실측 확정」·「digest JSON」

---

## OUTBOX — 2026-08-19 · 일일 digest JSON (date_kst=08-19)

**스냅샷:** CLOSED=10 🟢 · DNA RANK1~3 false 🔴 · Cos n=0 🟡 · 01b=0 🟡 · L-1/L-2/REPORT_BOT ok · **ai_overseer exit=1 🔴** (당일 오전 OUTBOX「overseer OK」와 불일치 → digest 재수집·프로세스 생존 재확인 권고).  
**Ask:** 구현 Handoff 없음 · DNA는 기존 Ask(A 관측유지 vs B 완화) 유지 · overseer는 서버 `systemctl status`만. C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-19 · DNA 실측 확정 (재료 부족 · 관측 유지)

**DB:** `BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data` · `bitget_market_data.sqlite` ~3.3GB OK.  
**CLOSED=10:** 1H=2 · 2H=1 · 4H=7. **n_mfe8=0 · n_mfe5=0 전 TF.** max_mfe≈3.55 (문턱 8·5 미달).  
**mine→0 templates · sync→`no_rankable_clusters`.** `--force`/재채굴 무의미.  
**잔여 🔴 DNA만** (overseer ✅ · L-2 timer ✅).  
**Ask:** 관측 유지(권장) vs mfe_min/min-rows 완화 Handoff — 디렉터 결정. C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-19 · DNA mine 실측: 0 templates / no_rankable_clusters

**사실:** `BITGET_GMM_DNA_TEMPLATES` 로드 시 None → `mine_bitget_dna_templates()` 실행 → **0 templates** · sync `--force` → `no_rankable_clusters` (더 이상 `no_gmm_templates` 아님 = 구조는 생겼으나 cluster 비어 있음).  
**코드 조건:** TF당 MFE≥`BITGET_MIN_MFE_FOR_MINING`(기본 8) CLOSED가 feature dropna 후 **≥12행**이어야 GMM fit. CLOSED≈10이면 TF별로 부족이 정상.  
**Ask:** (A) 관측 유지·데이터 쌓일 때까지 DNA 🔴 허용 (B) mfe_min/최소행 완화는 **Handoff+디렉터 승인** 후에만. C-2/MDD5%/live 금지. `--force` 반복 무의미.

---

## OUTBOX — 2026-08-19 · overseer 영구 기동 OK

**변화:** `dante-bitget-overseer.service` → `active (running)` + `enabled`.  
원인: VPS는 `.venv` 없음 · **`venv/bin/python`** 이 SSOT. ExecStart를 그 경로로 수정 후 203/EXEC 해소.  
**잔여 🔴:** DNA RANK1~3 false (`no_gmm_templates` — `recover-artifacts-quick`=KMeans만, GMM 미채움).  
**주의:** L-2 timer active이지만 backup 스크립트 `python: command not found` 가능.

**Ask:** DNA는 `mine_bitget_dna_templates` 후 `gmm_dna_alpha_sync --force` — 디렉터 ops vs 미니 Handoff. C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-19 · 일일 관측 (🔴 잔여 2) [superseded by overseer OK]

**변화:** L-2 backup.timer `inactive`→`active` (progress 3/8→4/8).  
**잔여 🔴:** DNA RANK · overseer 203/EXEC (이후 `venv/` 경로로 해소됨).  
**주의:** backup `--test` 시 `python: command not found`.

**Ask:** (1) GMM 템플릿 선행 후 sync (2) overseer `venv/` (3) backup_*.sh PATH — C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-18 · 일일 관측 실측 (🔴)

**스냅샷:** CLOSED=10 🟢 · RANK1~3 전부 false 🔴 · Cos n=0 🟡 · L-1 ok · L-2 backup.timer inactive 🔴 · ai_overseer exit=1 🔴 · 01b weekly=0 🟡 · progress 3/8

**Ask:** 서버 ops 3종(RANK sync --force / backup.timer enable / overseer 기동)을 디렉터 수동으로 할지, CAT-I/L 미니 Handoff가 필요한지. C-2/MDD5%/live 금지 유지.

---

## OUTBOX — 2026-08-18 · kid dashboard on daily digest

- `build_kid_dashboard` + `format_digest_html` 재작성: 진행률 바 · 🟢/🔴/🟡/⬜ 4칸
- 메시지 3분할: 대시보드 → 숫자 메모 → 복붙
- 테스트 3 passed · gates/sync 미접촉
- Ask: 사후 OK · 디렉터 UX 수용 여부

---

## OUTBOX — 2026-08-17 · 일일 관측 실측 (🟡)

**스냅샷:** CLOSED=10(SPOT5+FUT5) 🟢 · Cos sample n=0(journal) 🟡 · DNA RANK1~3 전부 false 🔴 · L-1 ok · L-2 backup.timer inactive 🔴 · ai_overseer exit=1 🔴 · REPORT_BOT ok

**해석(디렉터용):** 장부는 돌아가나 DNA 키가 비어 Cos 표본이 없음. 백업 타이머·감사관 미기동.

**Ask:** Handoff 없이 서버 ops만 할지(RANK sync --force / backup.timer enable / overseer 기동) vs CAT-I 미니 Handoff 필요 여부. C-2/MDD5%/live 금지 유지.

---

## OUTBOX — 2026-08-17 · POST_DEPLOY_OBS daily digest

### 왜
디렉터: 1~2주 관측 항목을 매일 텔레그램으로 받고, Cursor/Claude 복붙 문구 포함.

### 로컬 스냅샷
| 항목 | 내용 |
|------|------|
| 신규 | `observability/post_deploy_obs_digest_bg.py` |
| CLI | `bitget.sh --post-deploy-obs-digest` (락 무접촉) |
| Cron | UTC 11:00 = KST 20:00 daily |
| 전송 | REPORT_BOT direct HTTP (north-star와 동일) |
| 비접촉 | gates.py · gmm_dna_alpha_sync.py |
| 테스트 | `test_post_deploy_obs_digest_bg.py` **3 passed** |

### Ask
- 디렉터 요청 범위로 사후 OK 가능한지 (정식 Handoff 없이 디렉터 지시 구현)
- 복붙 블록 길이·REPORT_BOT 분할 발송 수용 여부

### 금지 준수
C-2 · MDD5% · live · 실전 — 미착수

---

## Claude OK — I-GMM-DNA-01b (2026-08-17)

- 판정: **OK** — Handoff 100% 일치 · 수정 spec 없음 · Adapter 불필요
- Mirror #2 수용: 2주 unavailable → 서버 로그 경로만 (05 잔여 · 선코딩 금지)
- 다음: **디렉터 서버 확인** (POST_DEPLOY_OBS · L-1/L-2/overseer · 01b 1~2주) · C-2/MDD5%/live defer

---

## OUTBOX — 2026-08-17 · I-GMM-DNA-01b 구현

### 로컬 구조 스냅샷
| 항목 | 내용 |
|------|------|
| 신규 | `bitget/observability/gmm_dna_alpha_report_bg.py` |
| Hook | `bitget_pipelines._pipeline_weekly_evolution` — `cost_report` 직후 `gmm_dna_alpha_report` (critical=False) |
| Config | `memory_policy`: `GMM_DNA_ALPHA_REPORT_ENABLED=true` · `WINDOW_DAYS=7` · `LOG_SOURCE=journal` |
| 비접촉 | `forward/gates.py` · `evolution/gmm_dna_alpha_sync.py` — **미수정** |
| 테스트 | `pytest bitget/tests/test_gmm_dna_alpha_report_i01b.py` → **6 passed** |

### 산출 필드
- cos_eff_sample_count / zero_ratio / mean_nonzero(nullable)
- open_count_by_market · closed_count_by_market (B-1 `normalize_market_key`)
- dna_rank_keys_present · shape_source_distribution · log_source_used
- 로그 실패 시 sample null + `unavailable` (추정 금지)

### Ask
- Handoff 스펙 일치 OK 여부
- Mirror #2: 2주 unavailable 시 서버 로그 경로 확인을 05 잔여로 둔 것 수용 여부

### 금지 준수
C-2 · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION` — 미착수

---

## OUTBOX — 2026-08-17 · POST_DEPLOY_OBS (코드 diff 없음)

**디렉터 확인:** I-GMM-DNA-01 포함 Bitget **서버 배포 완료**.  
로컬 `NEXT_ACTION` 등이 “git push + 배포 대기”로 남아 있어 **문서만** 현실에 맞춤. 알파/실전/C-2/MDD5%/live **미착수**.

### 로컬에서 확인 가능한 것
- 코드·테스트·Claude 조건부 OK · R1/R2 반영 이력 (`05` I-GMM)
- 배포 후 **무엇을** 보면 되는지 한 장: `track_b_POST_DEPLOY_OBS_체크리스트.md`
- L-1 / L-2 / ai_overseer+REPORT_BOT = **코드 OK**, 서버 설치·기동 **기록 없음** → 표기 = 미확인

### 서버에서만 확인 가능한 것 (이 세션에서 숫자 없음)
| 항목 | 왜 로컬 불가 |
|------|----------------|
| `bitget_forward_trades` OPEN/CLOSED COUNT | prod SQLite는 `BITGET_DB_STORAGE_PATH` |
| `Cos_eff=0.000` 고정 여부 | journal / BITGET_LOG_DIR |
| `CRYPTO_DNA_ALPHA_RANK*` · `shape_source` | config_kv prod |
| `gmm_dna_alpha_sync --force` 가 **이미** 돌았는지 | RANK 키 존재 여부가 증거. 채팅만으로는 모름 |

### 다음 Handoff 후보 **1개만**
- **I-GMM-DNA-01b** — Cos_eff / OPEN count / `shape_source` **읽기 전용** 관측 미니잡 (ops 로그·주간 숫자). gate/DNA 재배선 아님.
- **하지 말 것**: C-2 funding · 포트폴리오 MDD 5% · B-2 live alloc · `ENABLE_REAL_EXECUTION=true`

**Ask:** 01b Handoff를 쓸지, 아니면 디렉터 48h 관측 숫자 받은 뒤에만 쓸지.

---

## I-GMM-DNA-01 — Claude 조건부 OK (2026-08-12)

**판정:** 조건부 OK → **R1/R2 코드 반영 완료** (8 passed)

| 조건 | Claude 지적 | Cursor 반영 |
|------|-------------|-------------|
| R1 | data_miner `force=True` → manual 덮어쓰기 | `force=False` 기본 · `BITGET_GMM_SYNC_FORCE_ON_MINE` opt-in |
| R2 | score/100 폴백 live 공용 | `ENABLE_REAL_EXECUTION=true` 시 fail-closed (Cos=0) |
| Mirror | shape_source 관측 | `dna["shape_source"]` 태그 추가 |

**paper 배포:** 즉시 진행 가능  
**live 전환 전:** CAT-F Handoff에 폴백 스위치/fail-closed 재확인 예약

---

## I-GMM-DNA-01 — GMM→CRYPTO_DNA_ALPHA 배선 (2026-08-12)

### 증상 (서버)
- `forward_trades` 0건 · 텔레그램 스캔 ~1000건/일
- 로그: `Cos_eff=0.000 < elastic 0.588` (시계열 게이트 100% 거절)
- config: `BITGET_GMM_DNA_TEMPLATES` 있음 · `CRYPTO_DNA_ALPHA_RANK*` 없음

### 근본 원인
- `signal_engines._doppelganger_adjustment` → `CRYPTO_DNA_ALPHA_RANK1..3` (+ shape 20) 만 읽음
- `data_miner` → `BITGET_GMM_DNA_TEMPLATES` 만 채움 (**키 불일치**)
- `sn_score=0` 이 facts에 고정 → `_facts_cos_scalar_01` 이 signal score 폴백 불가

### 구현
| 파일 | 변경 |
|------|------|
| `evolution/gmm_dna_alpha_sync.py` | **신규** — sync SSOT |
| `data_miner.py` | prototype shape + post-mine sync |
| `pipelines/bitget_pipelines.py` | config_bootstrap 훅 |
| `forward/gates.py` | sn_score≈0 시 score/100 폴백 |

### 테스트
`pytest bitget/tests/test_gmm_dna_alpha_sync.py` → **6 passed**

### 서버 배포 후 1회
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
sudo INSTALL_ROOT=$PWD bash bitget/deploy/update_bitget.sh
.venv/bin/python -m bitget.evolution.gmm_dna_alpha_sync --force
sqlite3 /var/lib/quant-bitget/data/bitget_system_config.sqlite \
  "SELECT key FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA%';"
```

### Claude OK Ask
- neutral shape + bounds midpoint DNA가 paper bootstrap에 충분한지
- sn_score=0 폴백 허용 범위 (🟡 리스크 게이트)

---

## D-3 — Claude OK 수신 (2026-08-04)

- cost/fee basis null — SSOT 없음 확인 수용
- `gemini_call_count` llm_call_cache proxy — 수용 (CAT-M/CAT-J 동기화 완료)
- D-3b dormant · pipeline 미배선 재확인
- D-3b 실배선 시 `bitget_real_execution` vs CAT-N interface — P2-5 Handoff 체크 항목 예약
