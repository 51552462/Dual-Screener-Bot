# CLAUDE → CURSOR · B0-SAMPLE-CONTRACT Claude OK
# (append 보관 · 덮어쓰기 금지 · 2026-08-28) · LANE_FASTCHECK

> 작성: Claude Pro · [CAT-F] · 판정: **OK** · sub-phase **DONE**
> 추가 diff 없음 · HIST 비접촉 유지 · 원본 이슈(표본 부족) 미해결·관측

---

# CLAUDE → CURSOR · B0-SAMPLE-CONTRACT Handoff
# (append 보관 · 덮어쓰기 금지 · 2026-08-28) · LANE_FASTCHECK

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]  
> **상태**: Cursor 문서 구현 → **WAIT_CLAUDE_OK**  
> **병행**: LANE_HIST3FIX 비접촉

## [CAT-F] 자본배분&리스크 — B0-SAMPLE-CONTRACT (표본 충분성 계약 문서)

### sub-phase ID
B0-SAMPLE-CONTRACT

### SSOT
- `13_B1_신뢰사다리.md` **§7 신설만** · §1~§6 비변경 · config 없음

### Cursor 지시
- Targeted · 문서만 · 09/NEXT_STEP 이번 라운드 미갱신
- `lanes/LANE_FASTCHECK/CLAUDE_TO_CURSOR.md`에도 append 보관

### 위험도
🟢

---

# CLAUDE → CURSOR · B1-LADDER-R1a-FASTCHECK Claude OK
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> 작성: Claude Pro · [CAT-F] · 판정: OK (1단계 스펙 검증 통과)
> 비차단 확인 2건(R6 페이스 포함여부 / SPOT blocked=0 쿼리vs하드코딩) → 다음 보고 1줄씩
> 배포는 디렉터 승인 후 · 2~4주 가상매매(2단계) 관찰 시작 가능
> FULL-BT(A)는 별도 레인 유지, 본 건과 혼합 금지

---

# CLAUDE → CURSOR · B1-LADDER-R1a-FASTCHECK Handoff
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]
> **상태**: **WAIT_CURSOR_IMPL** → Cursor 구현 후 WAIT_CLAUDE_OK
> **병행**: FULL-BT(A) 좁은 수정 — **별도 트랙 · 비게이팅 · 혼합 금지**

---

## [CAT-F] 자본배분&리스크 — B1-LADDER-R1a-FASTCHECK read-only 주간 집계

### sub-phase ID
B1-LADDER-R1a-FASTCHECK

### SSOT (변경 금지 unless noted)
- 신규: `bitget/observability/b1_ladder_fastcheck_bg.py`
- 수정(추가만): `bitget/docs/work_phases/13_B1_신뢰사다리.md` §6 아래 "R1a FASTCHECK 절차" 소절
- 참조만(비변경): `bitget_forward_trades`, §6 SQL, §3 판정표, `short_funnel_report_bg.py`
- config: `B1_LADDER_FASTCHECK_ENABLED`(config_kv), `B1_LADDER_FASTCHECK_WINDOW_DAYS`(config_kv)

### 변경 Spec
- 함수: `compute_b1_ladder_fastcheck_bg(window_days: int = 7) -> dict[str, dict]`
- market_type(SPOT/FUT)별 open_count·closed_weekly_delta·blocked_short_total·r6_pace_flag → 기존 §3 판정표 대입 → verdict 문자열만
- weekly_evolution 훅 등록(read-only), 기존 훅 순서 비접촉
- 출력: `ops_events` → `b1_ladder_fastcheck_weekly` (mt별 1건)
- SPOT/FUT: 완전 분리, 합산 금지

### Config 변경
| KEY | old | new | default |
|-----|-----|-----|---------|
| `B1_LADDER_FASTCHECK_ENABLED` | — | 신규 | true |
| `B1_LADDER_FASTCHECK_WINDOW_DAYS` | — | 신규 | 7 |

### 인접 CAT 영향
- CAT-C: 없음 (R1b 이름만 예약, 착수 금지 유지)
- CAT-J: 읽기만 (short_funnel 기존 출력)
- CAT-D/N: 비접촉

### 롤백 조건
`B1_LADDER_FASTCHECK_ENABLED=false` → 즉시 비활성, ops_events 신규 기록만 중단

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- FAIL(b) verdict 나와도 R1b 코드 착수 금지 — verdict 산출까지만
- 신규 정책·게이트·Critical·상수 창조 금지
- 테스트: `pytest bitget/tests/test_b1_ladder_fastcheck_bg.py`

### 위험도
🟢 (read-only 집계 · config_kv kill-switch 2개뿐 · gate/Kelly/MDD/live 비접촉)

---

# CLAUDE → CURSOR · FULL-BT-HIST-3-FIX 검증 OK → VPS dry→10×2 + 원문 append
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)
# ⚠️ HIST-3-FIX 좁은 수정 Handoff가 INBOX에 누락됐던 재발(2회째) — 본 검증 응답을 원문으로 소급 append

> **작성**: Claude Pro (Architect) · 2026-08-28 · [CAT-Q]
> **상태**: **FULL-BT-HIST-3-FIX Spec 1~4 = OK** · **VPS dry→10×2 실행 승인** · 전체런 금지
> **caveat**: `_U1_MIN_BARS` 언더스코어 재사용 — 다음 라운드 CAT-CONSTANTS 승격 권고(지금 조치 아님)

---

## [CAT-Q] FULL-BT-HIST-3-FIX 검증 OK → VPS dry→10×2 실행 + 원문 append

### 판정
Spec 1~4 전부 OK. VPS 실행 승인. 전체런 금지 유지.

### Spec 대조 (소급 원문)
1. fetch [start−min_bars, end] Adapter — OK (tail-only 한계 → date-range Adapter, `_load_ohlcv` 비접촉)
2. walk `range(REUSED_MIN_BARS, len)` — OK (walk 1바 단서 대응)
3. warmup 부족 skip(보간 없음) — OK
4. 원본 비접촉 — OK (replay/CAT-C/D/E/batch/report)
- REUSED_MIN_BARS = `bitget.analysis.universe_bt.replay._U1_MIN_BARS` (240)

### 실행 (원문 그대로)
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_full_bt_hist_pilot.sh
BITGET_FULL_BT_MAX_SYMBOLS=10 bash bitget/deploy/run_full_bt_hist_pilot.sh
```

### 보고 (7키)
call_total · outcome_totals · tf_coverage · exception_types · hit/reject ·
fetch_requested vs fetch_loaded · call_total vs walk_bar_expected

### 필수 조치
1. 본 검증 응답 전체를 `CLAUDE_TO_CURSOR.md`에 **append**(덮어쓰기 금지)
2. 롤백: `harness.py` 해당 커밋 revert만 — 결과 스키마·paper·config_kv 무영향

### 세션 종료 의무
- 05: VPS 숫자 + 재판정 · 00 · CURSOR_TO_CLAUDE 7키 · NEXT_ACTION WAIT_CLAUDE_OK · 09/NEXT_STEP

### 위험도
🟢 (read-only 진단 VPS · 실자금/config_kv 미접촉)

---

# CLAUDE → CURSOR · FULL-BT-HIST-3-FIX (warmup fetch-range 교정)
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> **작성**: Claude Pro (Architect) · 2026-08-28 · [CAT-Q]
> **상태**: 디렉터 **(A)** 확정 · **FULL-BT-HIST-3-FIX** Handoff · 🟡
> **금지**: HIST-4 · 열린 lookback · 전체런

---

## [CAT-Q] 진단&레거시 — FULL-BT-HIST-3-FIX: warmup 창 반영 fetch-range 교정

### sub-phase ID
FULL-BT-HIST-3-FIX
(주의: HIST-4 아님 — HIST-3 단일 용의점의 좁은 수정. 디렉터 (A) 확정.)

### SSOT
- 파일: `bitget/full_bt/harness.py` (`run_replay` 내부만)
- `_load_ohlcv` start 오프셋 미지원 시 harness Adapter — `replay.py` 원본 수정 금지
- REUSED_MIN_BARS: 신규 상수 금지 · CAT-C/universe 기존 min-bars import

### Spec
- `fetch_range` / `requested_bar_count = REUSED_MIN_BARS + walk_bar_count`
- walk: `for i in range(REUSED_MIN_BARS, len(window))` — 더 이상 calls=1 고정 아님
- warmup 부족 심볼 skip · 보간 금지

### Cursor
- Targeted diff only · pytest full_bt · dry→10×2 · WAIT_CLAUDE_OK
- 보고: 기존 5키 + requested vs loaded + call vs 기대 walk

---

# CLAUDE → CURSOR · FULL-BT-HIST-3 재판정 = 부분반려 + 디렉터 에스컬레이션
# (append 보관 · 덮어쓰기 금지 · 2026-08-25)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: 원인(1)(3) 배제 **승인** · "에스컬레이션 해당없음" **반려** · **WAIT_DIRECTOR**
> **HOLD**: lookback/HIST-4/신규 코드 착수 금지 · 전체런 금지

---

## 판정 요약
- Ask1: TF·호출경로 배제 OK / 에스컬레이션 해당없음 **반려** (트리거=**미해결 시**)
- Ask2: lookback Handoff **보류** (디렉터 승인 전)
- Ask3: 전체런 금지 **유지**
- 단서 유지: REUSED_MIN_BARS=로더tail(250)→walk 1바 · calls=1

## 디렉터 질문
(A) 용의점만 좁혀 수정 vs (B) FULL-BT 보류·R1a/B1 집중

## Cursor
신규 코드 금지 · NEXT_ACTION=`WAIT_DIRECTOR` · 05에 본 판정 기록

---

# CLAUDE → CURSOR · B0 표본 기아 Ask 답변
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]
> **상태**: Ask 1~5 답변 완료 · 코드 diff 없음 · 코드 Handoff는 디렉터 확정 후
> **병행**: FULL-BT-HIST-3 VPS 유지(비게이팅) · B1-LADDER-R1a OBSERVE 유지

---

## [CAT-F] B0 표본 기아 Ask 답변 — R1a 빠른 판정 경로 제안 (코드 diff 없음)

### SSOT (변경/비변경)
- **비변경**: `13_B1_신뢰사다리.md` §2/§3 렁·Kill 표, `bitget_forward_trades` 스키마, `short_funnel_report_bg.py`, gates/Kelly/execution_safety 전체
- **다음 라운드 추가 예정만(이번엔 미실행)**: `13_B1_신뢰사다리.md` §6에 "R1a FASTCHECK" 절차 소절 — Cursor 확정 후 별도 Handoff

### Spec (Ask 1~5 판정)

**1) 오해/팩트 표 — 수용 OK.** 「누적」= `bitget_forward_trades` CLOSED 사이드별 전 기간 COUNT, 주간 하드쿼터 아님. MAX open positions ~20(CAT-CONSTANTS) 재확인. 배선 생존(CLOSED≈10) + 신규진입 정체(OPEN=0) 동시 성립 인정.

**2) 디렉터 공식 답변 문구** — `09_디렉터_쉬운요약.md` 갱신본 참조.

**3) 우선순위 — (i)도 (ii)도 아님, 제3안.**
`13_` Kill표 R1a는 이미 두 갈래 FAIL 경로: (a) 4주 시간경과, (b) short_funnel 반복 거절 패턴. 4주((a)) 대기도, FAIL 미확정 상태에서 곧장 R1b CAT-C 코드 Handoff((ii))도 Kill표 순서 위반. 대신 **(b) 조건을 기존 계측만으로 지금 판정**. HIST-3(FULL-BT L1)은 다른 트랙(히스토리 리플레이, engine_hit=0 원인규명)이고 R6/B1 근거 사용이 이미 금지(`15_` SSOT) → 완료돼도 디렉터의 진짜 문제(L2 표본 증가 속도)를 풀어주지 않음 → **HIST-3는 병렬·비차단 유지, R1a 판정만 이번 라운드에서 가속.**

**4) 최소 관측 계약** (기존 숫자만 재사용, 신규 상수 없음)

| 신호 | 재사용 출처 | 판정 기여 |
|------|-------------|---------|
| OPEN 재개 | `13_` §6 SQL 그대로 | OPEN>0 → 즉시 PASS 재검토 |
| CLOSED 주간 Δ | 동일 SQL, 기존 timestamp 컬럼 7일 필터만 추가(신규 로직 아님) | 누적치 대신 최근 추이 |
| blocked_short_total 추이 | `short_funnel_report_bg.py` 기존 출력 | 반복되면 FAIL(b) 근거 (LONG 가시성 없음 — 기존 caveat 유지) |
| R6 페이스 환산 | (CLOSED 주간Δ ÷ 7 × 56) vs 기존 30건/56일(`13_` R6, 재발명 아님) | 부족 시 "페이스 부족" flag만, 게이트 변경 아님 |

**5) 다음 Handoff ID 후보:** `B1-LADDER-R1a-FASTCHECK` — 문서(`13_` §6) + 위 4개 값을 조합하는 read-only weekly 집계 1개. 신규 상수·게이트·Critical 없음. R1b(CAT-C)는 FASTCHECK가 FAIL(b) 확정할 때만 별도 라운드 착수.

### SPOT/FUT 분기
공통 — SHORT는 SPOT에서 구조적으로 0(SPOT-FUT 표 기존 각주), FASTCHECK 지표는 market_type별 분리 집계만, 합산 금지.

### 인접 CAT 영향
- CAT-C: 없음 (R1b 미착수, 이번엔 이름만 예약)
- CAT-D/J: 읽기만 (기존 컬럼·기존 short_funnel 출력 재사용)
- CAT-N/F(execution_safety/Kelly/live): 비접촉, 🔴 Critical 아님

### Cursor 지시 (이번 라운드 = 확정 답변 반영만 · 코드 Handoff 아님)
- `09` · `NEXT_STEP` · `NEXT_ACTION`(WAIT_DIRECTOR) · `05` · `00` 현황판 반영
- **다음:** 디렉터가 3)/5) 방향 동의 시, 다음 대화에서 `B1-LADDER-R1a-FASTCHECK` 단일 sub-phase CAT-HANDOFF 발급(CAT-F, 🟢, read-only).
- 그 전까지 HIST-3 VPS dry→10×2 숫자 보고는 별도 트랙 계속.

---

# CLAUDE → CURSOR · FULL-BT-HIST-3 스펙 OK → VPS dry→10×2 실행
# (append 보관 · 덮어쓰기 금지 · 2026-08-25)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: **FULL-BT-HIST-3 검증 = OK (비차단 caveat 1건)** · **VPS dry→10×2 실행 승인** · 코드 diff 종료
> **에스컬레이션**: 3원인 미분리 시 HIST-4 금지 · 디렉터 에스컬레이션만

---

## [CAT-Q] 진단&레거시 — FULL-BT-HIST-3 스펙 OK → VPS dry→10×2 실행

### sub-phase ID
FULL-BT-HIST-3 (VPS 실행 단계)

### SSOT (변경 금지)
- 코드 diff 종료(스펙 OK) — 이번 라운드는 실행만
- full_bt_diag(tf 확장 완료) · CAT-C/B/D 원본 비접촉 유지

### 변경 Spec
- VPS 실행: 회신 원문 명령 그대로(git pull → dry(3) → 10×2), 신규 명령 추가 없음
- 보고 키(5개 숫자만): engine_call_total · engine_call_outcome_totals · tf_ohlcv_coverage · exception_types · HIST-2 hit/reject

### Config 변경
없음

### 인접 CAT 영향
- CAT-C/B/D: 읽기만(재확인) · 🔴 아님

### 롤백 조건
diag 테이블 `tf` 컬럼 무시/삭제만으로 완전 롤백 — 결과 trade 스키마 무영향

### Cursor 지시
- 커밋·푸시 후 VPS: dry(3) → 10×2 순서 그대로. **전체 유니버스 런 금지 유지**
- 보고는 숫자만(해석·재판정은 Claude 몫)
- "call>0 ∧ TF 전부 True ∧ none 지배" 확인되면 → lookback(N bars) 조사는 **별도 Handoff 대기**, 지금 미착수
- 3원인 미분리(여전히 판별 불가) → **HIST-4 설계 금지**, `CURSOR_TO_CLAUDE.md`에 디렉터 에스컬레이션 필요로 명시
- 이번 Handoff 원문 `CLAUDE_TO_CURSOR.md`에 **append 보관**(덮어쓰기 금지, 재발 방지)

### 위험도
🟢 (read-only 진단 VPS 실행 · 실자금/config_kv 미접촉)

### 세션 종료 의무
- 05_진행로그.md HIST-3 VPS 결과 섹션
- 00_전체현황판.md
- CURSOR_TO_CLAUDE.md 4개 숫자 + 3원인 판별 결과
- NEXT_ACTION.md → WAIT_CLAUDE_OK 유지

### caveat (비차단)
HIST-3 원 Handoff 원문 보관 누락 재발 — 이후 append 의무.

---

# CLAUDE → CURSOR · FULL-BT-HIST-3 Handoff (엔진 호출/TF/warmup 분리 계측)
# (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: HIST-2 원인 재판정 **OK**(엔진 미히트) · **FULL-BT-HIST-3** 진단 Handoff · 🟢
> **에스컬레이션**: 동일 trade_count=0 이슈 **3번째** 진단 — HIST-3도 미해결 시 HIST-4 금지·디렉터 에스컬레이션

---

## HIST-2 재판정 요약
- Ask1: 엔진 미히트 재판정 **OK** (hit=0 ∧ reject=0 · dry·10×2 결정론적 0)
- Ask2: **HIST-3** — 후보 (1) TF 갭 (2) warmup/lookback (3) 호출 경로 미실행 — **계측만·수정 아님**
- Ask3: 전체 유니버스 런 **금지 유지**

### Spec (요약)
- `engine_call_count[engine][symbol][mt][tf]` — 함수 진입
- `engine_call_outcome` — candidate / none / exception
- `tf_ohlcv_coverage[mt][tf] -> bool` — 하니스가 로드 가능한 TF
- `full_bt_diag` **확장 우선** · 하니스 wrapper만 · CAT-C/B 원본 금지
- dry(2~3) → 10×2 · pytest full_bt · OUTBOX에 4개 판정 정책 숫자 · WAIT_CLAUDE_OK

---

# CLAUDE → CURSOR · FULL-BT-HIST-2 Claude OK + VPS dry→10×2 실행 승인
# (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: **FULL-BT-HIST-2 검증 = OK (비차단 caveat 2건)** · **VPS dry→10×2 실행 승인** · 추가 코드 변경 없음
> **병행**: B1-LADDER-R1a OBSERVE 유지 (게이팅 없음)

---

## FULL-BT-HIST-2 검증 결과: OK

Spec 1~6 대조 통과. 위험도 🟢 유지. CAT-C/D 원본 비접촉 확인.

**비차단 caveat**
1. 다음 보고 1줄: `bitget_full_bt.sqlite` **FULL-BT 결과 테이블**(`bitget_forward_trades` 클론·report §2) 컬럼 목록 불변 여부 (paper 라이브 원장과 별개로 명시)
2. 기록용: Handoff는 harness-only였으나 batch.py·pilot.sh까지 FULL-BT 스캐폴드 확장 — 🔴 아님. 이후 유사 확장 시 사전 Ask

**실행 승인 (코드 변경 없음)**
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_full_bt_hist_pilot.sh
# 통과 시
BITGET_FULL_BT_MAX_SYMBOLS=10 bash bitget/deploy/run_full_bt_hist_pilot.sh
```
**전체 유니버스 런: 금지 유지**

### 결과 보고 (`CURSOR_TO_CLAUDE.md`)
- spot.diag / futures.diag — engine_hit_total · gate_reject_count
- hit=0 vs reject>0 분리 → HIST-1 병기 원인 재판정
- caveat 1 컬럼 불변 1줄 · paper before=after

---

# [CAT-Q] FULL-BT-HIST-1 파일럿 판정 → FULL-BT-HIST-2 진단 Handoff

> 이 파일은 3개 섹션으로 구성. 각 섹션을 해당 대상 파일에 반영.
> 대상: ① `bitget/docs/work_phases/CLAUDE_TO_CURSOR.md` ② `bitget/docs/work_phases/09_디렉터_쉬운요약.md` ③ `bitget/docs/work_phases/NEXT_STEP.md`

---

## ① CLAUDE_TO_CURSOR.md 반영분

### 판정: trade_count=0 → **미통과 (b) 진단 Handoff 필요**

| # | 항목 | 판정 |
|---|------|------|
| 1 | SPOT/FUT 정량 | 참고만 — 값 0 자체는 무해 |
| 2 | caveat | OK — 빈 테이블, 혼입 관측 없음 |
| 3 | 개선단서 | **FAIL** — step1~10 전부 0, side_asymmetry 전부 0 → 계측 무효 |
| 4 | paper | OK — before=after=10 |
| 5 | 배너 | OK |

**이유 (1줄):** gate_bottleneck 전부 0/N/A는 "거절이 없었다"와 "거절 이벤트가 기록 자체가 안 됐다"를 구분 못 함. Cursor 본인 보고에도 원인이 "엔진 hit 없음 또는 try_add 전량 미통과" 2택 병기 — 상호 배타적 원인을 현재 계측으로 특정 불가. 하니스는 완주했지만 "동작 검증"은 안 된 상태이므로 파일럿 목적(FULL-BT-0) 미충족.

**전체 유니버스 런: 금지 유지** (FULL-BT-0 §6 원칙 그대로).

---

### [CAT-Q] FULL-BT-HIST-2 — 원인 분리 계측 (진단 전용, 정책 변경 없음)

**sub-phase ID:** HIST-2

**SSOT (변경 금지)**
- 5종 엔진 / `forward/shared.py` try_add / `master_scanner.py` — 원본 수정 금지 (FULL-BT-0 비접촉 승계)
- 계측은 FULL-BT-1 하니스(read-only 드라이버) 레벨 Adapter로만 삽입

**Spec**
- `engine_hit_count[engine_name][symbol][market_type]` — 5종 엔진 + master_scanner pre-candidate hook이 candidate 생성한 횟수. 원본 콜 전/후 카운트하는 하니스 wrapper만.
- `gate_reject_count[step][market_type]` — `try_add_virtual_position(...)` 진입점 wrapper에서 반환/예외의 거절 사유를 CAT-D §4 step 1~10에 매핑해 카운트 (11=execution_safety real-only이므로 paper 경로는 그대로 N/A)
- 저장: `bitget_full_bt.sqlite` 신규 진단 전용 테이블(`full_bt_diag`) — 기존 결과 스키마 비접촉

**SPOT/FUT 분기**
공통 로직 + market_type 파라미터로 분리 집계, 합산 금지 (SPOT-FUT 표 원칙 승계)

**인접 CAT 영향**
- CAT-D: 읽기만 — `try_add_virtual_position` 반환/예외 관측, 내부 미수정 → Adapter
- CAT-C: 읽기만 — 엔진 호출 지점 wrapper → Adapter
- 🔴 Critical 아님 (진단 read-only, 실행/게이트 정책 변경 없음)

**롤백 조건**
- wrapper가 하니스 실행시간을 유의미하게 늘리면 제거 후 표본 축소 재시도

**Cursor 지시**
- Targeted diff only — 하니스 드라이버(FULL-BT-1 파일) 내부에만 wrapper 추가
- CAT-C/D 원본 파일 수정 금지
- 소표본(SPOT/FUT 각 2~3심볼) dry 확인 → 통과 시 10×2 재실행
- 테스트: `pytest bitget/tests/full_bt`

**세션 종료 의무**
- `05_진행로그.md` HIST-2 섹션
- `00_전체현황판.md` Phase·SSOT
- `CURSOR_TO_CLAUDE.md` 결과 회신
- `NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

**위험도:** 🟢 (읽기 전용 계측, 정책/실행 로직 변경 없음)

---

---

# CLAUDE → CURSOR · FULL-BT-HIST-1 Claude OK + 파일럿 실런 지시
# (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: **FULL-BT-HIST-1 검증 = OK (비차단 caveat 1건)** · 전체런 전 **파일럿 실런 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## FULL-BT-HIST-1 검증 결과: OK

판정 근거:
- 재사용 소스 `bitget.analysis.universe_bt.replay._load_ohlcv` — 신규 근사·로직 창조 없음(룰5)
- entry/exit **캔들축** 전환 — `15_FULL-BT_전체이식가상매매.md` §6 로드맵 목표("`run_replay` 실제 OHLCV 바 워크, 캔들축")와 정확히 일치
- 산출 3파일(`harness.py` 바 워크, `report.py` Adapter, 테스트) — 신규 파일 범위 내. CAT-C 엔진풀·CAT-D try_add 11단계·CAT-E exit 3파일 원본 로직 재작성 없음(호출만) 재확인
- 비접촉 리스트(`forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·paper·batch/checkpoint) 전부 diff 없음 확인 — FULL-BT-1/2/3 헌법 그대로 승계
- SSOT 충돌(entry/exit 축 전환으로 기존 wall-window report 필터 무효화) → 원본 수정이 아닌 **Adapter(`CANDLE_ENTRY_AXIS`)** 제안 — 룰6 준수
- batch 호환 시그니처 유지(`run_replay(market_type, symbol, engine, start, end, db_path, *, market_db=None)`) — FULL-BT-2 오케스트레이터 재작성 불필요
- 테스트 **14 passed**(기존 10 + 신규 4: multi-bar exit·candle≠wall·SPOT/FUT 소스·pilot resume)
- 루트 주식 경로 무접촉, `bitget/full_bt/` 하위만

**참고(비차단 — 다음 보고에서 1줄 확인 요청)**:
`bitget_full_bt.sqlite`는 `paths.py`상 **공유**(run_id 미포함 물리 파일)다. `CANDLE_ENTRY_AXIS=True`로 wall-window 필터를 끄면 집계는 `checkpoint 완료 심볼 ∩ market_type`만으로 좁혀지는데, **결과 테이블 자체에 run_id 컬럼이 없다면** 같은 심볼을 처리한 서로 다른 run_id(예: 이번 파일럿 vs 이후 전체런)의 트레이드가 같은 결과 테이블에 누적될 때 report가 **다른 run의 트레이드까지 합산**할 위험이 있음 — FULL-BT-3 보완 (A)가 원래 막으려던 것과 동일 클래스 문제. **코드 변경 요청 아님.** 아래 파일럿 실행 결과에 "결과 테이블 run_id 컬럼 존재 여부" + "이번 run 트레이드 건수 vs 결과 테이블 전체 건수" 1줄만 같이 보고.

또한 원본 FULL-BT-HIST-1 Handoff 텍스트가 `CLAUDE_TO_CURSOR.md`에서 검색되지 않음(과거 UTF-8 손상 이력과 유사 — 문서 보관 공백 가능). 이번 OUTBOX 내용은 `15_FULL-BT §6` 로드맵과 자체 정합하므로 검증 차단 사유는 아니나, 다음 세션 종료 시 본 파일 자체를 그대로 `CLAUDE_TO_CURSOR.md`에 보관해 재발 방지 권장(비차단).

**다음**: 전체 유니버스×전체 히스토리 런 전, 아래 **파일럿** 먼저 실행.

---

## 파일럿 실런 지시 (신규 코드 없음 · 실행만)

- **market_type**: SPOT, FUTURES 각 1회 (분리 실행, 합산 실행 금지 — §4)
- **max_symbols**: **10** (U3 VPS 파일럿 재사용값 — 신규 상수 아님, 룰5)
- **run_id**: 신규 생성(pilot 접두, 예: `pilot-{ts}`) — 기존 run_id 재사용 금지(체크포인트 오염 방지)
- **resume**: true (기본값 그대로 — 재개 idempotency 실사용 확인 겸함)

### 실행 후 보고 (`CURSOR_TO_CLAUDE.md`에 1줄씩)
1. SPOT/FUT 각 `trade_count`·`total_return_pct`·`mdd_pct` (`report.py` §2 스키마 그대로, 재계산·해석 금지)
2. 위 caveat 확인: 결과 테이블 run_id 컬럼 유무 + (이번 run 트레이드 수) vs (테이블 전체 행 수) 비교
3. `gate_bottleneck_by_step`·`side_asymmetry` 슬롯 값 채워지는지(§2 개선단서, N/A 고정 아님 확인)
4. paper `bitget_forward_trades`(라이브 원장) before=after 재확인 — FULL-BT 공통 헌법(§5) 불변 재확인
5. 상단 고정 배너("IV L1 전체이식 가상매매...") 원문 그대로 출력되는지

### 비접촉 (재확인)
`forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·`bitget_forward_trades`(라이브 paper)·CAT-B/C/D/E/F/G/N 원본 — 파일럿은 **실행만**, diff 없음.

### 인접 CAT 영향
CAT-D/E: 없음(원본 참조·호출만, 변경 아님 — 룰7 Critical 대상 아님) · CAT-B: 읽기만(OHLCV) · CAT-F/G/N: 비접촉.

### 롤백 조건
파일럿 run_id + 해당 checkpoint/결과 행 삭제만으로 완전 롤백. HIST-1 코드·FULL-BT-1/2/3·paper DB·config_kv·원본 CAT 무영향.

### 위험도
🟢 (read-only 파일럿 · 실자금/config_kv 미접촉 · 결과는 L1 참고용 라벨 고정)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-HIST-1 파일럿 섹션(위 1~5 숫자 그대로)
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

---

*버전 2026-08-25 · FULL-BT-HIST-1 OK + 파일럿 지시 · Architect: Claude Pro · Engineer: Cursor*

---
---

# (룰13) 디렉터 문서 갱신본 — 그대로 덮어쓰기

## `09_디렉터_쉬운요약.md`

```markdown
# 디렉터용 쉬운 요약 (비개발자 OK)

> 갱신: 2026-08-25 · FULL-BT-HIST-1(진짜 시세 연결) 통과 · 소규모 시험(파일럿) 지시

## 지금 한 줄
"과거 진짜 시세로 진짜처럼 돌려보는" 마지막 퍼즐 조각(FULL-BT-HIST-1)이 검증을 통과했습니다.
다만 큰 규모로 돌리기 전에, **코인 10개짜리 소규모 시험(파일럿)**을 먼저 한 번 돌려서
숫자가 이상 없이 나오는지 확인하는 단계입니다. 자동차로 치면 "고속도로 타기 전 동네 한 바퀴 시운전"입니다.

신호등: 🟢 FULL-BT-0~3(골격) · 🟢 FULL-BT-HIST-1(진짜 시세 연결) · 🟡 **소규모 시험(파일럿) 대기** · 🟡 R1a 관측 계속

### 당신이 할 일
1. Cursor에게 이 파일 전달 → **코인 10개 시험 실행** 요청 (SPOT 1번, FUTURES 1번)
2. 결과 숫자(수익률/승률 아님, "참고용" 표) 나오면 Claude에게 다시 검증 요청
3. 매일 텔레그램 OPEN/CLOSED(R1a) 계속 확인
4. 이 시험 결과도 **"수익률 확정"이 아닙니다** — 상단에 항상 "참고용, 실전 증명 아님" 배너가 붙습니다. 진짜 합격 판정은 R6(실거래 56일+)에서만 나옵니다.
```

## `NEXT_STEP.md`

```markdown
# NEXT STEP

> 갱신: 2026-08-25 · FULL-BT-HIST-1 Claude 검증 OK(비차단 확인 1건) · 파일럿 실런 지시 발급

## 지금 상태
FULL-BT-HIST-1(실제 OHLCV 캔들축 바 워크) Claude 검증 **OK**. 전체 유니버스 런 전 **파일럿(max_symbols=10, SPOT/FUT 각 1회)** 먼저 실행하도록 지시. 비차단 확인 1건(결과 테이블 run_id 스코프)은 파일럿 결과 보고에 포함.

## 다음 행동
1. Cursor: 위 파일럿 실런 지시대로 SPOT/FUT 각 1회 실행, 5개 항목 `CURSOR_TO_CLAUDE.md`에 보고
2. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
3. 디렉터: 파일럿 결과 오면 `CURSOR_TO_CLAUDE.md` 검증을 Claude에게 요청(비차단 확인 1건 포함 여부 체크)
4. 파일럿 통과 시에만 전체 유니버스×전체 히스토리 런 Handoff 진행 — 파일럿 생략한 전체런 착수 금지
5. 병행: R1a 매일 관측 유지, 게이팅 없음
6. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지 (전 단계 공통)
```

---

# CLAUDE → CURSOR · FULL-BT-HIST-1 Handoff (bitget/docs/work_phases/CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-24 · [CAT-Q]
> **상태**: Cursor **구현 대기** · `CURSOR_TO_CLAUDE.md` Ask(FULL-BT 실제 히스토리 바 워크) 해소용 Handoff
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## Ask 확인 (CURSOR_TO_CLAUDE.md)
FULL-BT-0~3은 골격(배치·체크포인트·리포트) 완료였으나 `harness.run_replay`가 `_ = (start, end)`로 **미사용**, hist가 **합성 120일 flat**, 청산이 바 워크가 아닌 **즉시 Adapter**라 전 코인×기간 PnL/MDD 방향 검증이 실행 불가하다는 갭 확인. 아래 Handoff로 실데이터 바 워크 전환.

---

## [CAT-Q] 진단&레거시 — FULL-BT-HIST-1 · `run_replay` 실제 OHLCV 바 워크 전환

### sub-phase ID
**FULL-BT-HIST-1** (FULL-BT-0~3 로드맵은 "트랙 종료"로 닫혀 있어 번호 재사용 대신 HIST 접두로 신규 sub-phase 구분 — 15_FULL-BT §6 로드맵 표에 행 추가만, 기존 행 재작성 금지)

### SSOT (변경 금지 unless noted)
- 수정(targeted diff): `bitget/full_bt/harness.py` — `run_replay` 내부만 교체, 시그니처·파일 위치 유지
- 조사 후 재사용(원본 비접촉): `bitget/analysis/universe_bt/` 내 OHLCV 로더(가칭 `_load_ohlcv`) — **정확 경로/시그니처는 Cursor 조사 후 보고**, 신규 로더 발명 금지(룰5)
- 원본 호출만(비변경 — diff 없음): CAT-C 엔진풀(`signal_engines.py`/`master_scanner.py`, FULL-BT-1과 동일 import 경로), CAT-D `forward/shared.py`의 `try_add`(11단계, step11 execution_safety는 real 전용 → paper replay N/A skip 그대로), CAT-E 3파일(`trading/position_manager.py`/`tail_risk_gate.py`/`mega_trend_kill_bg.py`) evaluate
- 변경 없음: `forward/ledger.py`, config_kv, `bitget_forward_trades`(paper), FULL-BT-2 batch/checkpoint 로직, FULL-BT-3 `report.py` 스키마(§2 키 그대로), CAT-B/F/G/N 원본

### 변경 Spec

**함수 시그니처 — 신규 없음, 내부 구현만 실동작화**
```
run_replay(symbol: str, market_type: str, start: int, end: int) -> list[dict]
```

**정책 (Ask 1~6 대응)**
1. **OHLCV 소스**: universe_bt 로더 재사용 우선 조사 → 성공 시 import, 실패/구조 불일치 시 Adapter 제안 후 보고(신규 로더 발명 금지, 룰5). `market_type`별 소스 테이블(`BITGET_SPOT_*`/`BITGET_FUT_*`, `14_UNIVERSE-BT` §1 관례 재사용).
2. **바 워크 루프**: `[start,end]` 구간 각 바에서 — (a) 미보유 심볼 → CAT-C 원본 candidate 생성 → `try_add` 원본 순서 그대로(step11 N/A skip) → 통과 시 격리 DB에 OPEN Adapter write, (b) 보유 심볼 → CAT-E 3파일 evaluate **원본 호출**을 그 바마다 순차 평가 → 트리거 시 CLOSED Adapter write. (현재의 "즉시 Adapter" 방식 폐기 — 실제 경과 바 수만큼 평가)
3. **시간축**: `entry_date`/`exit_date`는 **캔들 타임스탬프**(바 자체 시각) 채택, wall-clock(now()) 아님. 사유 — backtest는 과거 재생이므로 wall-clock을 쓰면 모든 run이 실행 시점 동일 값이 되어 §2 `period_start`/`period_end` 정량표 기간 대조가 무의미해짐. `updated_at`(기록 시각)은 기존처럼 wall-clock 유지, entry_date/exit_date만 분리. Cursor는 FULL-BT-3 `report.py` 공유DB 필터가 현재 어떤 축을 쓰는지 재조사 후 정합 여부 1줄 보고(불일치 시 `report.py` Adapter만, 스키마 재작성 아님).
4. **스코프**: `max_symbols` 파라미터로 소규모 파일럿 먼저(기존 `build_full_bt_shards` 재사용, 신규 파라미터화 최소) → 통과 후 전 유니버스 확장. 메모리 캡은 FULL-BT-2 기 확정값(`TIME_MACHINE_MAX_TABLES`/`TIME_MACHINE_MAX_BARS_PER_TABLE`, `bitget.infra.memory_policy`) 그대로 재사용, 재정의 금지(룰5).
5. **완료 정의**: 격리 DB(`bitget_full_bt.sqlite`)에 실데이터 기반 row 존재 + `generate_full_bt_l1_report(market_type, run_id)` 호출 시 §2 정량표(SPOT/FUT 분리)에 non-flat 실측값 산출. **"합격/달성" 판정 문구 금지**(15_FULL-BT §3 Kill, 배너 원문 그대로 유지).
6. **비접촉**: `forward/ledger.py`·`forward/shared.py`·`signal_engines.py`·exit 3파일 **원본** · config_kv · paper 원장 · CAT-J 미편입(§5 로드맵 그대로).

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지(FULL-BT-0~3과 동일 헌법 승계)

### SPOT/FUT 분기
- `market_type` 파라미터 관통, 공통 바 워크 로직에 하드코딩 금지(`CAT-SPOT-FUT_비대칭표` 인용만)
- OHLCV 소스 테이블만 market_type별 분기(정책1) — 그 외 바 루프/try_add/exit 로직은 공통
- SPOT SHORT는 ledger hard reject로 자연 0건 유지(FULL-BT-1/3 선례 그대로, 신규 분기 불필요)

### 인접 CAT 영향
- **CAT-C**: 없음 — 엔진풀 원본 import·호출만, diff 없음
- **CAT-D**: 없음 — `try_add` 원본 호출만, `forward/shared.py` diff 없음 → **🔴 Critical 아님**(룰7: 원본 "변경" 시에만 대상, 본 Handoff은 읽기/호출 전용)
- **CAT-E**: 없음 — 3파일 evaluate 원본 호출만, diff 없음 → **🔴 Critical 아님**(동일 사유)
- **CAT-B/F/G/N**: 없음, 비접촉
- **CAT-J**: 없음, 미편입 유지(FULL-BT-3 §5 로드맵 그대로)
- **Track B(B1-LADDER)**: 없음, 병렬 독립 · 우선순위 R1a보다 낮음

### 롤백 조건
- `bitget/full_bt/harness.py` diff만 revert(git) — CAT-C/D/E 원본 무영향
- 격리 DB(`bitget_full_bt.sqlite`) 해당 run_id row 삭제만으로 데이터 롤백 — paper/config_kv 무영향
- 파일럿(`max_symbols` 소규모) 단계 이상 발견 시 전체 유니버스 확장 보류(정책4)

### Cursor 지시
- Targeted diff only — `bitget/full_bt/harness.py` 내부 로직만. CAT-C/D/E 신호엔진·try_add·exit 3파일 **원본 diff 금지**, import·호출만.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- universe_bt OHLCV 로더 정확 경로/시그니처 조사 후 `CURSOR_TO_CLAUDE.md`에 `"재사용 소스: {실제 경로/함수명}"` 1줄 보고(임의 명명 금지, 룰5).
- entry_date/exit_date 캔들축 전환과 `report.py` 필터 정합 여부 1줄 보고(정책3).
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/full_bt/`(신규 케이스 — 다중 바 경과 후 exit 트리거 확인 · entry_date≠updated_at 케이스 · SPOT/FUT 소스 분기 · `max_symbols` 파일럿 idempotent resume 필수 포함)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-HIST-1 섹션
- `bitget/docs/work_phases/00_전체현황판.md` SSOT 용어집에 `FULL-BT-HIST-1` 행 추가
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신(재사용 소스 보고 + entry_date 정합 여부)
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `bitget/docs/work_phases/09_디렉터_쉬운요약.md` / `NEXT_STEP.md` — 아래 갱신본 그대로 반영(룰13)

### 위험도
🟡 (신규 실데이터 바 루프 로직 · 결과는 격리 DB만 · CAT-D/E는 원본 호출뿐 diff 없음 · config_kv/paper 무접촉이나 다중바 상태추적 복잡도로 FULL-BT-3 대비 상향)

---

*버전 2026-08-24 · FULL-BT-HIST-1 Handoff · Architect: Claude Pro · Engineer: Cursor*

---
---

# (룰13) 디렉터 문서 갱신본 — 그대로 덮어쓰기

## `09_디렉터_쉬운요약.md`

```markdown
# 디렉터용 쉬운 요약 (비개발자 OK)

> 갱신: 2026-08-24 · FULL-BT 트랙 재가동(진짜 시세 버전) Handoff 발급

## 지금 한 줄
지난번 "과거로 돌려본 결과 보고서"는 사실 진짜 시세가 아니라 평평한 연습용 가짜 데이터로 만든 것이었습니다(모의 훈련용 지도로 길을 그려본 것과 비슷해요). 이번엔 **진짜 과거 시세**로 다시 돌리는 작업을 Cursor에게 요청했습니다.

신호등: 🟡 **FULL-BT-HIST-1 = Cursor 구현 대기(진짜 시세 연결)** · 🟢 이전 골격(FULL-BT-0~3, 배치·저장·보고서 틀)은 그대로 재사용 · 🟡 R1a 관측 계속

## 비유로 설명
- 이전 결과 = 종이 위에 그려본 시뮬레이션 지도
- 이번 결과 = 실제 GPS 기록(진짜 과거 캔들)으로 같은 지도를 다시 그리는 것
- "진짜 참고용" 숫자는 이번 작업이 끝나야 나옵니다. 이전 숫자는 참고조차 되지 않습니다(전부 가짜 평지 데이터였음).

### 당신이 할 일
1. Cursor에게 이 Handoff(FULL-BT-HIST-1) 전달 → 진짜 시세 연결 구현 요청
2. 먼저 코인 몇 개만(파일럿) 돌려보고 이상 없으면 전체로 확대 — 처음부터 전체를 돌리지 않습니다
3. 완료되면 Claude에게 다시 검증 요청 (지금과 같은 방식)
4. 결과가 나와도 여전히 **"수익률 확정 아님"** — 진짜 합격 판정은 실거래 56일+(R6)에서만 나옵니다
5. 매일 텔레그램 OPEN/CLOSED(R1a) 계속 확인
```

## `NEXT_STEP.md`

```markdown
# NEXT STEP

> 갱신: 2026-08-24 · FULL-BT-HIST-1 Handoff 발급(실제 OHLCV 바 워크)

## 지금 상태
FULL-BT-0~3(골격·배치·체크포인트·리포트)은 완료였으나, `harness.run_replay`가 합성(가짜) OHLCV 스모크였음이 확인됨. 실제 히스토리 바 워크로 전환하는 **FULL-BT-HIST-1** Handoff 발급, Cursor 구현 대기.

## 다음 행동
1. Cursor: 위 FULL-BT-HIST-1 Handoff 기준 `bitget/full_bt/harness.py` 실데이터 바 워크 구현
2. Cursor: universe_bt OHLCV 로더 재사용 경로 조사 후 `CURSOR_TO_CLAUDE.md`에 1줄 보고
3. Cursor: `max_symbols` 소규모 파일럿 먼저 → 통과 후 전 유니버스 확장
4. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
5. 디렉터: Cursor 완료 보고 오면 `CURSOR_TO_CLAUDE.md` 검증을 Claude에게 요청
6. 병행: R1a 매일 관측 유지, 게이팅 없음
7. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지(전 단계 공통, 실데이터 전환 후에도 동일)
```

---
---

*본 파일은 `bitget/docs/work_phases/CLAUDE_TO_CURSOR.md`의 기존 최상단(현재 "FULL-BT-3 보완 검증 결과 + 트랙 종료" 블록) 바로 위에 그대로 붙여넣기 위한 prepend 블록입니다. Claude Pro는 실제 리포지토리 파일에 직접 쓰기 권한이 없어(프로젝트 지식은 읽기 전용 미러) 이 파일로 대신합니다 — 디렉터 또는 Cursor가 붙여넣기.*
# CLAUDE → CURSOR · FULL-BT-3 보완 검증 결과 + 트랙 종료

> **작성**: Claude Pro (Architect) · 2026-08-24 · [CAT-Q]
> **상태**: **FULL-BT-3 보완 검증 = OK** · FULL-BT-0~3 로드맵 전체 완료 → **트랙 종료**

## 판정: OK
- (A) 공유 DB 시간창 필터, (B) 미측정 각주 — 원 스펙 항목4/항목3과 상충 없음
- 비접촉 리스트 전부 확인 · 테스트 10 passed
- 신규 코드 요청 없음 · 잔여 하드 격리는 디렉터 별도 Handoff만

## 비차단 확인 (Cursor 이행)
updated_at/entry_date/exit_date wall-clock 동일 축 — OUTBOX 1줄 보고

## 다음 상태
FULL-BT 트랙 **종료** · 우선순위 B1-LADDER R1a OBSERVE 복귀

---
---

# CLAUDE → CURSOR · FULL-BT-3 Handoff (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **FULL-BT-2 검증 = OK** · **FULL-BT-3 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## FULL-BT-2 검증 결과: OK

판정 근거:
- 산출물(`batch.py`/`checkpoint.py`) — Handoff 함수 시그니처 골격(run_full_bt_batch·shards·window batches·checkpoint)과 일치
- 재사용값 `TIME_MACHINE_MAX_TABLES=300`·`TIME_MACHINE_MAX_BARS_PER_TABLE=5000`, 출처 `bitget.infra.memory_policy` — U2 재사용 확인값과 동일, 신규 상수 없음(룰5)
- 정책 승계: `harness.run_replay` 재사용만·TF `['1D','4H','2H','1H']`·funding 미추적·국면 UNKNOWN·step11 N/A skip — FULL-BT-1 OK 판정과 100% 일치, 재조사·확장 없음
- 엔진 5종 확인(`EMA5`/`MASTER`/`NULRIM`/`TV_SHORT_V1`/`TV_SHORT_V2`, `_build_engine_pool` 원본 import) — FULL-BT-1 OK 시 권장했던 선택 보고 항목 이행
- resume idempotency: 최초 `paper_count=2` 유지 → 재실행(동일 run_id) 시 `batches_run=0`·`batches_skipped=n1` — 완료분 skip·중복 삽입 없음 확인(Handoff 정책 5항)
- 비접촉: `forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·paper 원장·FULL-BT-1 harness 로직 재작성 없음 — 전부 확인
- 테스트: resume + paper 케이스 포함 **4 passed** — Handoff 필수 케이스 충족
- 루트 주식 경로 무접촉, `bitget/full_bt/` 하위만

**참고(비차단)**: 결과 격리 테이블의 실제 컬럼명(FULL-BT-1 `paths.py`/`harness.py` 확정본)은 OUTBOX에 별도 명시 안 됨 — FULL-BT-2는 체크포인트 테이블만 다루므로 이번 단계엔 불필요. FULL-BT-3 착수 시 재확인 필요(아래 Handoff에 반영).

**다음:** `05_진행로그.md`에 이 OK 기록 · 아래 FULL-BT-3 Handoff로 진행.

---

## [CAT-Q] 진단&레거시 — FULL-BT-3 리포트 (§2 스키마 · CAT-J 비편입)

### sub-phase ID
FULL-BT-3

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/full_bt/report.py` (기존 `full_bt/` 컨벤션에 맞춰 Cursor 배치)
- 참조만(원본 비접촉): FULL-BT-1 격리 결과 테이블(`bitget_full_bt.sqlite` 내부 — 정확 테이블/컬럼명은 `paths.py`/`harness.py` 확정본 기준, 재조사 후 보고, 임의 명명 금지 룰5) + `bitget_full_bt_checkpoint`(FULL-BT-2, 완료 batch만 집계 대상 판별용), `13_B1_신뢰사다리.md` §1(인용만), `CAT-SPOT-FUT_비대칭표.md`(인용만)
- 변경 없음: `forward/ledger.py`, `forward/shared.py`, `signal_engines.py`, `master_scanner.py`, exit 3파일, config_kv, `bitget_forward_trades`, FULL-BT-1/2 코드 전체, CAT-B/C/D/E/F/G/N 원본

### 변경 Spec

**함수 시그니처 (골격만)**
```
generate_full_bt_l1_report(market_type: str, run_id: str) -> dict
render_full_bt_l1_report_md(report: dict) -> str   # 배너 고정 + §2 정량표/개선단서만
```

**정책**
1. 상단 고정 배너를 `render_full_bt_l1_report_md` 최상단에 원문 그대로 삽입(15_FULL-BT §3, 요약·재작성 금지):
   `"IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지. 공식 B1 판정은 R6(L2 forward 56일+)만."`
2. PnL/MDD 정량표 키(§2) 그대로: `run_id, market_type, symbol_or_agg, period_start, period_end, total_return_pct, mdd_pct, trade_count, b1_reference_band`. `b1_reference_band`는 고정 문자열 `"12~18%/≤5%, 참고용 — 판정 아님"`(13_B1 §1 인용만, 수치 재계산·재해석 금지).
3. 개선 단서 슬롯(§2) 키 그대로: `gate_bottleneck_by_step`(try_add 11단계별 거절 카운트, step11은 N/A 고정), `side_asymmetry`(LONG/SHORT 진입·거절), `symbol_breakdown`(top rejected/entered), `tf_note`(재사용 TF 1줄).
4. 집계 대상은 `bitget_full_bt_checkpoint`상 완료 표시된 (symbol×batch)만 — 미완료 run_id 부분 집계 시 report에 "미완료 run — 부분 결과" 경고 문자열 포함(체크포인트 완료 플래그 그대로 필터, 신규 판정 로직 발명 아님).
5. Kill(§2) 준수: CAGR 과신·승률 단정·연복리 환산 과대표현 금지 — 정량표·개선단서 슬롯 값 그대로만 출력, 해석성 자유서술 삽입 금지.
6. CAT-J 비편입: `reports/` 등 독립 디렉터리 산출물로만 존재, 리포팅 파이프라인 등록·자동 트리거 연결 금지(U3와 동일 원칙, §5 로드맵 그대로).

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지(FULL-BT-1/2와 동일)

### SPOT/FUT 분기
- `market_type` 파라미터 관통, 하드코딩 금지(§4)
- 리포트는 SPOT·FUT **분리 집계 후 나란히 제시** — 합산 금지(§4)
- SPOT: SHORT는 ledger hard reject로 `trade_count=0` 자연 발생(특수분기 불필요), `side_asymmetry`는 U3 `side_asymmetry_ratio` null 처리 선례 준용(신규 정의 금지)

### 인접 CAT 영향
- **CAT-J**: 없음 — 읽기도 아님, 파이프라인 미등록(§5 로드맵 "CAT-J 비편입" 그대로)
- **CAT-B/C/D/E/F/G/N**: 없음 — FULL-BT-1/2와 동일 비접촉 헌법 유지, 결과 테이블 읽기 전용
- **Track B(B1-LADDER)**: 없음, 병렬 독립. `b1_reference_band`는 13_B1 §1 인용 표기일 뿐 B1 판정에 미반영(§3 Kill: R6 대체 금지)

CAT-F/G/N/B/D 관련 — 본 Handoff은 위 CAT들을 변경하지 않고 원본/결과 참조만 하므로 🔴 Critical 아님(룰7: 변경 시에만 Critical 판정 대상).

### 롤백 조건
신규 `report.py` 파일 삭제만으로 완전 롤백. FULL-BT-1/2 하니스·배치·체크포인트·paper DB·config_kv·원본 CAT 코드 무영향.

### Cursor 지시
- Targeted 신규 파일만. FULL-BT-1/2/CAT-B/C/D/E 원본 파일 diff 금지 — 읽기 전용 쿼리만.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- FULL-BT-1 격리 결과 테이블 실제 컬럼명(`paths.py`/`harness.py` 확정본) 재조사 후 `CURSOR_TO_CLAUDE.md`에 "결과 테이블: {실제명}, 컬럼 매핑: {...}" 1줄 보고(임의 컬럼명 금지, 룰5)
- 상단 배너·`b1_reference_band` 문자열은 원문 그대로 복사 — 요약·재계산 금지
- 충돌 시 Adapter 제안 후 디렉터 Ask
- 테스트: `pytest bitget/tests/full_bt/` (신규 — 정량표 키 존재 확인 + SPOT/FUT 분리집계 케이스 + 배너/Kill 문구 고정 확인 + 미완료 run 부분결과 경고 케이스 필수 포함)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-3 섹션
- `bitget/docs/work_phases/00_전체현황판.md` Phase·SSOT
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `bitget/docs/work_phases/09_디렉터_쉬운요약.md` / `NEXT_STEP.md` — 아래 갱신본 그대로 반영(룰13)

### 위험도
🟢 (read-only 리포트 생성 · 원본 코드/DB 쓰기 없음 · CAT-F/G/N Critical 코드 비접촉)

---

*버전 2026-08-23 · FULL-BT-3 Handoff · Architect: Claude Pro · Engineer: Cursor*

---
---

# (룰13) 디렉터 문서 갱신본 — 그대로 덮어쓰기

## `09_디렉터_쉬운요약.md`

```markdown
# 디렉터용 쉬운 요약 (비개발자 OK)

> 갱신: 2026-08-23 · FULL-BT-2 통과 · FULL-BT-3(결과 보고서) Handoff 발급

## 지금 한 줄
전체 코인×기간을 나눠 돌리고 끊기면 이어서 하는 배치(FULL-BT-2)가 검증을 통과했습니다.
이제 마지막 단계, **"과거로 돌려본 결과를 숫자표로 정리하는 보고서(FULL-BT-3)"**를 Cursor가 만들 차례입니다.

신호등: 🟢 FULL-BT-1 통과 · 🟢 FULL-BT-2 통과 · 🟡 **FULL-BT-3 = Cursor 구현 대기** · 🟡 R1a 관측 계속

### 당신이 할 일
1. Cursor에게 이 Handoff 파일 전달 → FULL-BT-3(보고서) 구현 요청
2. 완료되면 Claude에게 다시 검증 요청 (지금과 같은 방식)
3. 매일 텔레그램 OPEN/CLOSED(R1a) 계속 확인
4. 보고서가 나와도 **"수익률 확정"이 아닙니다** — 상단에 항상 "참고용, 실전 증명 아님" 배너가 붙습니다. 진짜 합격 판정은 R6(실거래 56일+)에서만 나옵니다.
```

## `NEXT_STEP.md`

```markdown
# NEXT STEP

> 갱신: 2026-08-23 · FULL-BT-2 Claude 검증 OK · FULL-BT-3 Handoff 발급

## 지금 상태
FULL-BT-2(배치+체크포인트) Claude 검증 **OK**. FULL-BT-3(§2 스키마 리포트 · CAT-J 비편입) Handoff 발급 완료, Cursor 구현 대기.

## 다음 행동
1. Cursor: 위 FULL-BT-3 Handoff 기준 `bitget/full_bt/report.py` 구현
2. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
3. 디렉터: Cursor 완료 보고 오면 `CURSOR_TO_CLAUDE.md` FULL-BT-3 검증을 Claude에게 요청
4. 병행: R1a 매일 관측 유지, 게이팅 없음
5. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지 (전 단계 공통)
```

---

﻿# CLAUDE → CURSOR · FULL-BT-2 Handoff (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **FULL-BT-1 검증 = OK** · **FULL-BT-2 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## FULL-BT-1 검증 결과: OK

판정 근거:
- 엔진 풀: CAT-C `_build_engine_pool` **원본 import** 확인, diff 없음 — 스펙 "signal_engines 원본 그대로" 준수
- candidate→진입: `try_add` **원본 호출**, step11(`execution_safety`, real 전용)은 paper replay에서 자연 N/A skip — 디렉터 원문 "게이트13" 용어 정정(=CAT-N execution_safety, paper 범위 아님) 정확 반영
- 청산: CAT-E 3파일(`position_manager`/`tail_risk_gate`/`mega_trend_kill_bg`) evaluate **원본 import**, CLOSED write만 격리 Adapter — 스펙 일치
- TF 조사: `재사용 TF: ['1D','4H','2H','1H']` — 출처 `master_scanner.TIMEFRAMES` 명시, 임의 확장 없음(룰5)
- funding 조사: (a)/(b)/(c) 모두 확인 후 "추적 없이 진행" 채택, 근사치 창조 없음(룰5), P1-3 미차감 라이브와 동일 승계
- 격리 검증: 격리 DB row 증가 · paper `bitget_forward_trades` **before=after** · config_kv 비접촉 — U1과 동일한 물리적 격리 원칙 충족
- 비접촉 리스트(`forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·paper 원장) 전부 원본 diff 없음 확인
- 테스트 1 passed(smoke) — FULL-BT-1 범위(read-only 하니스 존재 확인)에 충분

**참고(비차단, 기록용)**: OUTBOX가 "5종 엔진 + master_scanner C-1 pre-candidate hook" 커버리지를 항목별로 명시하진 않았음(`_build_engine_pool` import라고만 보고). diff 없음이 확인된 이상 안전성 문제는 아니며, 실제 엔진 커버리지는 FULL-BT-3 리포트의 `symbol_breakdown`/엔진별 집계에서 자연히 드러날 사항이라 이번 read-only 검증 단계의 필수 차단 요건은 아님. FULL-BT-2 세션 종료 보고 시 "관여 엔진 5종 확인" 1줄 추가를 권장(선택).

---

## [CAT-Q] 진단&레거시 — FULL-BT-2 배치·체크포인트 (전체 유니버스×전체 히스토리 확장)

### sub-phase ID
FULL-BT-2

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/full_bt/` 하위 (배치 오케스트레이터 — 기존 디렉토리 컨벤션에 맞춰 Cursor 배치)
- 신규 테이블(격리 DB 내부): `bitget_full_bt_checkpoint` (`bitget_full_bt.sqlite` 전용, paper DB와 무관)
- 참조만(원본 비접촉): FULL-BT-1 `harness.run_replay`(및 그 내부 CAT-C 엔진풀·CAT-D try_add 11단계·CAT-E exit 3파일 import 경로 그대로), `bitget.infra.memory_policy`의 `TIME_MACHINE_MAX_TABLES`/`TIME_MACHINE_MAX_BARS_PER_TABLE`(U2에서 재사용 확인된 값 — **재사용만, 재정의 금지**, 룰5)
- 변경 없음: `forward/ledger.py`, `forward/shared.py`, `signal_engines.py`, `master_scanner.py`, `trading/position_manager.py`/`tail_risk_gate.py`/`mega_trend_kill_bg.py`, config_kv, `bitget_forward_trades`(paper ledger), CAT-B/C/D/E/F/G/N 원본 코드 전체, FULL-BT-1의 국면 처리(UNKNOWN 고정)·funding 미추적 정책

### 변경 Spec

**함수 시그니처 (골격만)**
```
run_full_bt_batch(market_type: str, run_id: str, resume: bool = True) -> None
build_full_bt_shards(symbols: list[str], shard_size: int) -> list[list[str]]
get_full_bt_window_batches(symbol: str, market_type: str, batch_size: int) -> list[tuple[int, int]]
load_full_bt_checkpoint(run_id: str, market_type: str) -> dict | None
save_full_bt_checkpoint(run_id: str, market_type: str, symbol: str, batch_idx: int) -> None
```

**정책**
1. `run_full_bt_batch`는 FULL-BT-1 `harness.run_replay`를 **원본 그대로 재사용**하는 상위 오케스트레이터. 내부 로직(엔진풀 호출·try_add 11단계·exit 3파일 evaluate·CLOSED Adapter write) 재작성·복제 금지 — 배치/체크포인트 Adapter만 추가.
2. 유니버스 스냅샷을 `build_full_bt_shards`로 분할 — `shard_size`는 `TIME_MACHINE_MAX_TABLES`(U2 재사용값=300) 그대로. Cursor가 codebase 재확인 후 "재사용값: {실제값}" 1줄 보고(임의 값 금지, 룰5).
3. 심볼별 `get_full_bt_window_batches`로 시간축 배치 분할 — `batch_size`는 `TIME_MACHINE_MAX_BARS_PER_TABLE`(U2 재사용값=5000) 그대로.
4. 배치(심볼×윈도우) 완료마다 `save_full_bt_checkpoint` 기록 — 대상은 `bitget_full_bt.sqlite` 내부 `bitget_full_bt_checkpoint` 테이블만. config_kv·paper DB(`bitget_forward_trades`) 접촉 금지(FULL-BT-1 원칙 승계).
5. `resume=True` + 동일 `run_id` 체크포인트 존재 시 완료분 skip, 중단 지점부터 재개. 격리 결과 테이블 중복 삽입 방지(unique 제약 또는 사전 skip 로직).
6. 엔진풀(5종+`master_scanner` hook)·try_add 11단계(step11 N/A skip)·exit 3파일·funding 미추적·국면 UNKNOWN 고정 — FULL-BT-1과 **동일 유지**, 본 Handoff 범위 밖(재조사·확장 금지).
7. TF: FULL-BT-1 조사값 `['1D','4H','2H','1H']` 그대로 재사용, 확장 금지.
8. 실행 규모 확대(전체 유니버스×전체 히스토리)에 따라 paper DB(`bitget_forward_trades`) row count 불변 검증을 **배치 실행 전/후 + 샤드마다** 재확인(FULL-BT-1은 1회성 smoke, FULL-BT-2는 노출 시간 증가로 반복 확인 필요 — U2 원칙과 동일).

**신규 테이블 스키마 (키만)**
`bitget_full_bt_checkpoint`: `run_id, market_type, shard_index, completed_symbol, completed_batch_idx, updated_at`

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지 (FULL-BT-1과 동일)

### SPOT/FUT 분기
- `market_type` 파라미터 FULL-BT-1과 동일하게 관통 (하드코딩 금지)
- SPOT: SHORT는 ledger hard reject로 자연 0건(특수분기 불필요)
- FUTURES: LONG/SHORT 모두 기록
- SPOT·FUT 분리 집계 리포트는 FULL-BT-3 범위 — 본 phase는 실행·저장만

### 인접 CAT 영향
- **CAT-B**: 읽기만(OHLCV) — 규모 확대로 읽기량 증가, 쓰기 없음 불변
- **CAT-C**: 읽기만(엔진풀 원본 import 승계), 원본 수정 금지
- **CAT-D**: 참조만 — try_add 11단계 원본 호출 승계, 실 write(`bitget_forward_trades`) 절대 금지, 검증 빈도 상향(위 8항)
- **CAT-E**: 참조만 — exit 3파일 원본 evaluate 승계, CLOSED write는 격리 Adapter만
- **CAT-F/G/N**: 비접촉 — Kelly UNKNOWN cap·regime UNKNOWN·execution_safety step11 N/A skip 그대로 승계(재조사 없음)
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

CAT-F/G/N/B/D 관련 — 본 Handoff은 위 CAT들을 **변경하지 않고 원본 참조만** 하므로 🔴 Critical 아님(룰7: 변경 시에만 Critical 판정 대상).

### 롤백 조건
신규 오케스트레이터 파일 + `bitget_full_bt_checkpoint` 테이블 삭제만으로 완전 롤백. FULL-BT-1 하니스·paper DB·config_kv·원본 CAT 코드 무영향.

### Cursor 지시
- Targeted 신규 파일만. FULL-BT-1/CAT-B/C/D/E 원본 파일 diff 금지 — import만.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- `TIME_MACHINE_MAX_*` 정확 값·위치는 codebase 재조사 후 `CURSOR_TO_CLAUDE.md`에 "재사용값: {실제값}" 1줄 보고(임의 값 사용 금지).
- 하니스 실행 전후 + 샤드마다 paper DB(`bitget_forward_trades`) row count 대조, 세션 종료 보고에 숫자로 기록.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/full_bt/` (신규 — 체크포인트 재개(resume) idempotency 케이스 + paper DB 불변 케이스 필수 포함)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-2 섹션
- `bitget/docs/work_phases/00_전체현황판.md` Phase·SSOT
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

### 위험도
🟡 Medium (원본 CAT 코드 비접촉·paper 격리 유지되나, 실행 규모·노출 시간 증가로 격리 실패 리스크 누적 — 위 8항 반복 검증 필수. 실자금·config_kv 미접촉이라 🔴 Critical 아님)

---

> **NOTE (Cursor 2026-08-23)**: PowerShell 붙여넣기 중 기존 INBOX UTF-8이 손상됨.
> 아래는 `git HEAD`의 직전 커밋본을 복구한 이력 스택이다.
> 미커밋이던 FULL-BT-0/1 Handoff 본문이 필요하면 Downloads에서 재붙여넣기 요청.

---

# CLAUDE → CURSOR · UNIVERSE-BT-U3 OK (상단)

## UNIVERSE-BT-U3 검증 결과: OK

판정 근거:
- 지표 범위 4종(hit/gate_pass/virtual_entry/side_asymmetry) 정확 승계, 지표4는 고정 N/A 문자열로만 표기 — 근사 대입 없음(룰5)
- 고정 배너 + 정량표만, 자유서술 없음 — §3 Kill 준수
- 분모0→null 기존 정책 재사용, 신규 정의 없음
- SPOT/FUT 분리 집계 나란히 제시, side_asymmetry_ratio SPOT=null 각주 준수(§4)
- CAT-J 비편입 확인 — reports/ 별도 디렉터리, 파이프라인 미등록(§5 로드맵 그대로)
- U1/U2·config_kv·paper DB·CAT-B/C/G/F/N/D 원본 무접촉
- 테스트 11 passed(U1 4 + U2 3 + U3 4) — 기존분 보존, 회귀 없음

**다음:** 지표4(`crash_window_forced_exit_rate`)는 규명 미확정 리서치형 문제로 범위 밖 유지. 착수는 디렉터 판단 후 별도 Handoff에서만 진행 — 이번 라운드 미승인.

**U0~U3 로드맵 완료.** UNIVERSE-BT L0 트랙은 지표4 제외 상태로 현재 라운드 종료.

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U2 검증 OK + U3 Handoff · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **U2 검증 = OK** · **U3 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음)
> **범위 밖**: 지표4(`crash_window_forced_exit_rate`) — 별도 Handoff 대기 (regime=UNKNOWN 구간 해석 미확정, 임의 대입 금지)

---

## UNIVERSE-BT-U2 검증 결과: **OK**

판정 근거:
- 재사용 상수 `TIME_MACHINE_MAX_TABLES=300` / `TIME_MACHINE_MAX_BARS_PER_TABLE=5000` — 출처 `bitget.infra.memory_policy` 확인, 신규 상수 없음(룰5)
- U1 원본(`replay_symbol_window` 등) 로직 재작성 없음 확인 — window≤5 우회는 바 단위 재호출(호출 패턴 변경)일 뿐 원본 비접촉
- 테스트 7 passed (U1 4 + U2 3) — 회귀 없음
- 정책 승계 일치: C3 regime=`UNKNOWN` · `exit_trigger=NULL` · 지표4 미재개 — U1 SSOT(§5 로드맵 C3 조건)와 동일
- paper DB before=after=**3**, resume 2회차 `rows_written=0`, result COUNT 불변 — 격리·재개 안전성 확인, U0~U3 공통 비접촉 헌법(paper/config_kv) 위반 없음
- market_type 하드코딩 신규 유입 없음 확인 (§4)

**다음: 아래 U3 Handoff.**

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U3 리포트 (L0 정량표, 지표4 제외)

### sub-phase ID
UNIVERSE-BT-U3

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/analysis/universe_bt/u3_report.py`
- 참조만(읽기 전용, 원본 비접촉): `bitget_universe_bt.sqlite`(U1/U2 산출 — `bitget_universe_bt_results`, `bitget_universe_bt_checkpoint`), `14_UNIVERSE-BT_구조생존검증.md` §2·§3·§4(인용만, 표 재작성 금지)
- 변경 없음: config_kv, paper DB(`bitget_forward_trades`), CAT-C/B/G/F/N/D 원본, CAT-J 리포팅 파이프라인(§5 로드맵 "CAT-J 인접·편입 아님" 그대로)

### 변경 Spec

**함수 시그니처 (골격만)**
```
generate_universe_bt_u3_report(market_type: str, run_id: str) -> dict
render_u3_report_md(report: dict) -> str   # L0 배너 고정 + 정량표만, 자유서술 금지
```

**지표 범위 — §2 5종 중 4종만 (지표4 제외)**
- `hit_rate`
- `gate_pass_rate`
- `virtual_entry_rate`
- `side_asymmetry_ratio` — FUTURES만 산출, SPOT은 §2 각주에 따라 `null` 고정

**지표4(`crash_window_forced_exit_rate`) 범위 제외 (이번 sub-phase 미착수)**
U1/U2 정책 승계상 regime=`UNKNOWN`·`exit_trigger=NULL` 구간이 존재해 분자(SL/MDD 트리거)·분모(해당 구간 가상포지션 수) 모두 신뢰 불가. U3는 이 지표를 계산하지 않고 리포트 내 고정 문자열 `"N/A — 별도 Handoff 대기"`로만 표기. 근사치 대입·임의 재정의 금지(룰5).

**분모 0 처리**: §2 정책 그대로 — 0으로 나누지 않고 `null` (신규 정의 아님, 기존 재사용).

**출력 상단 고정 배너 (그대로 삽입)**
```
L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지
```
배너 + 정량표 외 자유서술 금지 (§3 Kill 준수).

### Config 변경 (있으면)
없음

### SPOT/FUT 분기
- `market_type` 파라미터 관통, 하드코딩 금지 (§4)
- 리포트는 SPOT·FUT **분리 집계 후 나란히 제시** — 합산 금지 (§4)
- `side_asymmetry_ratio`: SPOT은 항상 `null`(각주), FUTURES만 국면별 값 산출

### 인접 CAT 영향
- **CAT-J**: 없음 — 읽기도 아님. §5 로드맵 "CAT-J 인접·편입 아님" 그대로, 리포팅 파이프라인 등록·자동 트리거 연결 금지, 독립 산출물로만 존재
- **CAT-B/C/G/F/N/D**: 없음 (U1/U2와 동일 비접촉 헌법 유지)
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
`u3_report.py` + 산출 리포트 파일 삭제만으로 완전 롤백. `bitget_universe_bt.sqlite`(U1/U2 산출물)·paper DB·config_kv·CAT-C/B/G/F/N/D 원본 무영향.

### Cursor 지시
- Targeted 신규 파일만(`u3_report.py`). U1/U2 파일 diff 금지 — 전체 파일 rewrite 금지.
- **루트 주식 경로 무접촉**, `bitget/` 하위만.
- 리포트 산출 파일은 CAT-J 리포팅 디렉터리 밖에 저장 (예: `bitget/analysis/universe_bt/reports/`) — CAT-J 파이프라인 등록·자동 트리거 연결 금지.
- 지표4는 이번 sub-phase에서 코드·수치 모두 다루지 않음 — `"N/A"` 고정 문자열만 출력.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/universe_bt/` (신규 `test_u3` — denominator=0→null, SPOT `side_asymmetry_ratio`=null, 배너 존재, 지표4 N/A 고정 케이스 포함)

### 위험도
🟢 (읽기 전용 리포트 · 격리 DB만 참조 · 코드/config/paper 비접촉)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U3 착수 + 지표4 제외 사유 1줄
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: **룰13 대상 — 이번 턴은 디렉터 지시(U3 Handoff만 파일로)에 따라 범위 밖. U3 Claude OK 수신 시 별도로 갱신 예정.**

---

*버전 2026-08-23 · UNIVERSE-BT-U3 · Architect: Claude Pro · Engineer: Cursor*

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U1 Claude OK + U2 Handoff · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **UNIVERSE-BT-U1(C3) = OK** · **U2 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음)

---

## UNIVERSE-BT-U1(C3) 검증 결과: **OK**

판정 근거:
- `resolve_historical_regime`를 항상 `UNKNOWN`으로 고정한 선택은 원 spec (c) 문언("현재 라이브 국면 스냅샷 적용")보다 **보수적**임 — 오늘 국면 라벨을 과거 구간에 역투영하는 쪽이 오히려 오정보 위험이 크므로, UNKNOWN + 지표4 null 처리가 U0 §2 "분모 0 → null(가짜 100% 금지)" 원칙에 더 부합. 문언 이탈이나 Kill 위반 아님 — **승인**.
- Adapter 방식: `try_add_virtual_position` 원본 호출 그대로 두고 `DB_PATH`만 scratch로 패치, `save_system_config`/telegram no-op — 원본 미수정 확인(룰6 Adapter 원칙 준수).
- CAT-C/G/D/N/F 원본 파일 diff 없음 · config_kv 쓰기 없음 — spec "변경 없음" 항목과 일치.
- paper DB(`bitget_forward_trades`) row count **before=after=3** 확인 — 물리적 파일 분리(1차 안전장치) 유효 입증.
- SPOT SHORT dry → ledger hard reject(SHORT-DANTE-FUT-01) 확인 — 하니스 특수분기 없이 자연 동작, spec 일치.
- 테스트 4 passed, paper DB 불변 케이스 포함 확인 — spec 필수요건 충족.
- U1 축소 범위(TF=1D·engines=master+ema5·window≤5)는 OUTBOX에 명시적으로 disclosure됨 — 은닉 축소 아님, 승인.

**참고 (수정 요구 아님):** UNKNOWN 고정을 택한 근거를 `05_진행로그.md` 또는 U3 배너 제약문에 한 줄 남겨두면 추후 U1.1/U3에서 판단 근거 추적이 쉬움.

**다음 Handoff 선택 사유:** 지표4(과거 국면 재구성)는 (a)/(b) 조사 모두 불가로 판명된 **리서치형 미해결 문제**라 임의 설계 시 상수/라벨 창조 리스크가 큼(룰5) — 별도 라운드로 분리. 이미 안전성이 검증된 U1 골격을 그대로 재사용해 **규모만 확장**하는 U2가 리스크 대비 진행 가치가 높아 다음 Handoff로 선택.

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U2 배치·샤드·체크포인트

### sub-phase ID
UNIVERSE-BT-U2

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/analysis/universe_bt/` 하위 (U2 오케스트레이터 — 기존 디렉토리 컨벤션에 맞춰 Cursor 배치)
- 신규 테이블(격리 DB 내부): `bitget_universe_bt_checkpoint` (`bitget_universe_bt.sqlite` 전용, paper DB와 무관)
- 참조만(원본 비접촉): U1 `run_universe_bt_u1` / `replay_symbol_window` / `resolve_historical_regime` / `write_bt_results`, 기존 `TIME_MACHINE_MAX_*` 상수(정확 위치는 Cursor 조사 — **재사용만, 재정의 금지**)
- 변경 없음: config_kv, `bitget_forward_trades`(paper ledger), execution_safety, Kelly/Treasury, CAT-C/G/N/D 원본 코드 전체, U1의 국면 처리(UNKNOWN 고정)·exit_trigger(NULL 고정)

### 변경 Spec

**함수 시그니처 (골격만)**
```
run_universe_bt_u2(market_type: str, run_id: str, resume: bool = True) -> None
build_universe_shards(symbols: list[str], shard_size: int) -> list[list[str]]
get_symbol_window_batches(symbol: str, market_type: str, batch_size: int) -> list[tuple[int, int]]
load_checkpoint(run_id: str, market_type: str) -> dict | None
save_checkpoint(run_id: str, market_type: str, symbol: str, batch_idx: int) -> None
```

**정책**
1. `run_universe_bt_u2`는 U1 함수(`replay_symbol_window`/`resolve_historical_regime`/`write_bt_results`)를 **원본 그대로 재사용**하는 상위 오케스트레이터. U1 내부 로직 재작성·복제 금지(Adapter만 추가).
2. 유니버스 스냅샷 `U`(U0 §1 정의 그대로)를 `build_universe_shards`로 분할 — `shard_size`는 기존 `TIME_MACHINE_MAX_*` 값 그대로. Cursor가 codebase에서 정확한 상수명·값을 확인 후 **재사용만, 신규 값 창조 금지**(룰5).
3. 심볼별 `get_symbol_window_batches`로 전체 보유 히스토리를 시간축 배치 분할 — U1의 "심볼당 window ≤5" 임시 상한 제거. 배치 크기도 기존 `TIME_MACHINE_MAX_*` 재사용.
4. 배치(심볼×윈도우) 완료마다 `save_checkpoint` 기록 — 대상은 `bitget_universe_bt.sqlite` 내부 `bitget_universe_bt_checkpoint` 테이블만. config_kv·paper DB 접촉 금지(U1 원칙 승계).
5. `resume=True` + 동일 `run_id` 체크포인트 존재 시 완료분 skip, 중단 지점부터 재개. `write_bt_results`가 `(run_id, market_type, symbol, bar_ts)` 중복 삽입 안 하도록 unique 제약 또는 사전 skip 로직 확인.
6. 국면·지표4: U1 C3 결과 그대로 승계 — `resolve_historical_regime` 항상 `UNKNOWN`, `exit_trigger` 항상 `NULL` 유지. **본 Handoff에서 재개하지 않음**(별도 라운드).
7. engines 풀(`master`+`ema5`)·TF(`1D`)는 U1과 **동일 유지** — 확장은 본 Handoff 범위 밖.
8. 실행 규모 확대(전체 유니버스 × 전체 히스토리)에 따라 paper DB(`bitget_forward_trades`) row count 불변 검증을 **배치 실행 전/후 + 샤드마다** 재확인(U1은 1회 확인, U2는 노출 시간이 길어 반복 확인 필요).

**신규 테이블 스키마 (키만)**
`bitget_universe_bt_checkpoint`: `run_id, market_type, shard_index, completed_symbol, completed_batch_idx, updated_at`

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지 (U1과 동일)

### SPOT/FUT 분기
- `market_type` 파라미터 U1과 동일하게 관통 (하드코딩 금지)
- SPOT: SHORT 자연 0건(U1과 동일, 특수분기 불필요)
- FUTURES: LONG/SHORT 모두 기록

### 인접 CAT 영향
- **CAT-B**: 읽기만(OHLCV) — 규모 확대로 읽기량 증가, 쓰기 없음 불변
- **CAT-C**: 읽기만(원본 import 승계), 원본 수정 금지
- **CAT-G**: 읽기만, UNKNOWN 고정 승계(신규 조사 없음)
- **CAT-D**: 참조만 — 실 write(`bitget_forward_trades`) 절대 금지, 검증 빈도 상향(위 8항)
- **CAT-F/N**: 비접촉
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
신규 파일(U2 오케스트레이터) + `bitget_universe_bt_checkpoint` 테이블 삭제만으로 완전 롤백. U1 하니스·paper DB·config_kv·원본 CAT 코드 무영향.

### Cursor 지시
- Targeted 신규 파일만. U1/CAT-C/G/D 원본 파일 diff 금지 — import만.
- **루트 주식 경로 무접촉**, `bitget/` 하위만.
- `TIME_MACHINE_MAX_*` 정확 값·위치는 codebase 조사 후 `CURSOR_TO_CLAUDE.md`에 "재사용값: {실제값}" 1줄 보고(임의 값 사용 금지).
- 하니스 실행 전후 + 샤드마다 paper DB(`bitget_forward_trades`) row count 대조, 세션 종료 보고에 숫자로 기록.
- 테스트: `pytest bitget/tests/universe_bt/` (신규 — 체크포인트 재개(resume) idempotency 케이스 + paper DB 불변 케이스 필수 포함)

### 위험도
🟡 Medium (원본 CAT 코드 비접촉·paper 격리 유지되나, 실행 규모·노출 시간 증가로 격리 실패 리스크 누적 — 위 8항 반복 검증 필수. CAT-F/G/I/N/B/D **코드 변경 없음**이므로 🔴 Critical 미해당)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U2 착수 + 실사용 `TIME_MACHINE_MAX_*` 값
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: 첨부 갱신본 반영(룰13, 별첨 참고)

---

*버전 2026-08-23 · UNIVERSE-BT-U2 · Architect: Claude Pro · Engineer: Cursor*

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U0 재검증 OK + U1 Handoff · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **U0 재검증 = OK** · **U1 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음)

---

## UNIVERSE-BT-U0 재검증 결과: **OK**

판정 근거:
- `14_UNIVERSE-BT_구조생존검증.md` §2 `crash_window_forced_exit_rate` — `CRASH` 라벨 삭제, `(BEAR ∪ HIGH_VOL)`로 정정 확인
- CAT-CONSTANTS Regime Kelly cap 표 대조: BEAR(~0.010)·HIGH_VOL(~0.012)이 5개 국면 중 최저 리스크 허용치 — "위험 국면" 취지와 일치, 신규 상수 창조 아님(룰5 준수)
- CAT-G SSOT 값 집합 `{BULL,BEAR,CHOP,HIGH_VOL,SIDEWAYS,UNKNOWN}` 재확인 — `CRASH` 미존재 확정
- `gate_pass_rate`/`virtual_entry_rate` 변수명 `gate_passed_candidates`로 통일 확인
- §1·§3·§4·§5, `00_마스터_로드맵.md` 포인터(단일 라인), Track B 병렬 — 비변경 확인
- 코드·config_kv 비접촉 확인 (문서 전용 정정)

**다음: 아래 U1 Handoff.**

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U1 read-only 리플레이 하니스

### sub-phase ID
UNIVERSE-BT-U1

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/analysis/universe_bt/` 하위 (정확 위치는 기존 디렉토리 컨벤션에 맞춰 Cursor 배치)
- 신규 격리 DB: `bitget_universe_bt.sqlite` — **신규 SQLite 파일**, paper DB(`bitget_forward_trades`)와 물리적으로 분리, 커넥션 공유 금지
- 참조만(import/read only, 원본 비접촉): `signal_engines.py`, `master_scanner.py`, `forward/ledger` try_add 게이트 로직, `governance/meta_sync.py`/`meta_consumer.py`, OHLCV `BITGET_SPOT_*`/`BITGET_FUT_*`
- 변경 없음: config_kv, `bitget_forward_trades`(paper ledger), execution_safety, Kelly/Treasury, CAT-C/G/N/D 원본 코드 전체

### 변경 Spec

**함수 시그니처 (골격만)**
```
run_universe_bt_u1(market_type: str) -> None
replay_symbol_window(symbol: str, market_type: str, start_ts: int, end_ts: int) -> list[dict]
resolve_historical_regime(symbol: str, market_type: str, bar_ts: int) -> str
write_bt_results(rows: list[dict]) -> None   # bitget_universe_bt.sqlite 전용, 타 DB 접촉 금지
```

**정책**
1. `run_universe_bt_u1`은 §1 스냅샷(`U = load_dynamic_universe(market_type) ∩ 보유 OHLCV`)을 순회하며 심볼별 `replay_symbol_window` 호출 → `write_bt_results`. 단일 프로세스 순차 실행만 (배치·샤드·체크포인트는 U2 범위 — `TIME_MACHINE_MAX_*` 재사용은 U2에서).
2. `replay_symbol_window`는 CAT-C 후보생성·게이트 로직을 **원본 그대로 import**해 호출 — 로직 재작성·복제 금지. 게이트 판정이 `try_add_virtual_position` 내부에서 DB write와 결합되어 분리 호출이 불가능하면: 원본 함수 **수정 금지**, write 인자만 격리 DB로 주입하는 Adapter로 감싸는 방식을 조사. Adapter로도 안전한 분리가 불가능하면 CURSOR_TO_CLAUDE에 충돌 보고(템플릿 §"구현 충돌") 후 디렉터 Ask.
3. `resolve_historical_regime` — 과거 시점 국면 라벨 소스가 현재 spec상 불명확. Cursor 조사 후 3갈래 중 보고:
   - (a) 국면 이력 로그(`validation/regime_audit.py` 또는 유사)에 시점별 스냅샷이 존재 → 읽기 전용 사용
   - (b) 이력 로그 없음 + `meta_sync.py` 판정이 가격/거래량 히스토리만의 결정적 함수 → 읽기 전용 Adapter로 과거 구간에 재적용 (`meta_sync.py` 원본 수정·config_kv 쓰기 금지)
   - (c) 둘 다 불가 → U1을 "현재 라이브 국면 스냅샷 기준" 한정판으로 축소하고 U3 배너에 제약 명시 — **착수 전 디렉터 Ask 필요**
4. `write_bt_results`는 `bitget_universe_bt.sqlite`에만 연결. paper DB 커넥션과 절대 공유 금지(물리적 파일 분리가 1차 안전장치).

**신규 테이블 스키마 (키만)**
`bitget_universe_bt_results`: `run_id, market_type, symbol, bar_ts, regime_label, candidate_generated, gate_passed, virtual_entry, side, exit_trigger, created_at`

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지

### SPOT/FUT 분기
- `market_type` 파라미터 전체 관통 (하드코딩 금지)
- SPOT: SHORT는 기존 ledger hard reject(SHORT-DANTE-FUT-01)로 자연 0건 — 하니스 내 특수분기 불필요, 있는 그대로 기록
- FUTURES: LONG/SHORT 모두 기록

### 인접 CAT 영향
- **CAT-B**: 읽기만 (OHLCV), 쓰기 없음
- **CAT-C**: 읽기만(원본 import), 원본 수정 금지 — 위 2항 Adapter 조사 필요 시 신규 Adapter 파일만 추가
- **CAT-G**: 읽기만, 이력 소스 불명확 시 Adapter 조사(원본·config_kv 비접촉)
- **CAT-D**: 참조만 — 게이트 로직 재사용하되 실 write(`bitget_forward_trades`) 절대 금지
- **CAT-F/N**: 비접촉 (Kelly/execution_safety 관여 없음 — 가상 리서치, 주문 경로 아님)
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
신규 파일(`bitget/analysis/universe_bt/*`) + `bitget_universe_bt.sqlite` 삭제만으로 완전 롤백. paper DB·config_kv·CAT-C/G/N/D/B 원본 무영향.

### Cursor 지시
- Targeted 신규 파일만. 기존 CAT-C/G/D 원본 파일 diff 금지 — import만.
- **루트 주식 경로 무접촉**, `bitget/` 하위만.
- 위 3항 (a)/(b)/(c) 중 어느 경로로 갔는지, 무슨 이력 소스를 썼는지 `CURSOR_TO_CLAUDE.md`에 먼저 보고 — (c)면 착수 전 디렉터 Ask.
- 하니스 실행 전후 paper DB(`bitget_forward_trades`) row count 불변을 직접 대조해 세션 종료 보고에 숫자로 기록.
- 테스트: `pytest bitget/tests/universe_bt/` (신규 — paper DB 불변 검증 케이스 필수 포함)

### 위험도
🟡 Medium (CAT-C/D 로직 재사용·실거래 경로 비접촉이나, 격리 실패 시 paper 오염 리스크 — 위 격리 검증 필수)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U1 착수 + 국면이력 (a)/(b)/(c) 판단 결과
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: 본 Handoff와 함께 Claude가 갱신(룰13, 별첨 파일 참고)

---

*버전 2026-08-23 · UNIVERSE-BT-U1 · Architect: Claude Pro · Engineer: Cursor*

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U0 수정 spec · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **U0 검증 결과 = 수정 필요** (OK 아님) · **U1 착수 계속 금지**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 정정과 무관)

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U0 §2 지표4 정정 (CAT-G 미존재 라벨 "CRASH" 교정)

### sub-phase ID
UNIVERSE-BT-U0 (수정 라운드 · 재검증 대기)

### SSOT (변경 금지 unless noted)
- 수정(타겟 diff만): `bitget/docs/work_phases/14_UNIVERSE-BT_구조생존검증.md` §2 표 — `crash_window_forced_exit_rate` 행 + 변수명 통일 2곳
- 참조만(비변경): `governance/meta_sync.py` `CURRENT_REGIME_KEY` 값 집합, CAT-CONSTANTS Regime Kelly cap 표
- 변경 없음: `00_마스터_로드맵.md` 포인터, config_kv, CAT-C/B/G/F/N/D 코드 전체

### 변경 Spec

**문제**
§2 표 4번째 지표 `crash_window_forced_exit_rate`가 "CAT-G 국면 라벨 **CRASH**/BEAR 한정"으로 정의되어 있음. 그러나 CAT-G SSOT(`CURRENT_REGIME_KEY`)의 실제 값 집합은:

```
{BULL, BEAR, CHOP, HIGH_VOL, SIDEWAYS, UNKNOWN}
```

`CRASH`는 CAT-G 문서·CAT-CONSTANTS Regime Kelly cap 표 어디에도 없는 값 — 임의 라벨 창조 금지(룰5)에 해당. U1에서 이대로 구현 시 필터가 항상 공집합이 되거나 Cursor가 임의로 재해석하게 됨.

**정정 정의**
```
crash_window_forced_exit_rate =
  (BEAR ∪ HIGH_VOL 구간) SL 또는 MDD 트리거 횟수 / 해당 구간 가상포지션 수
```
지표 ID·의미(하락·고변동 구간 강제청산 비율)는 유지, **라벨만** 실제 SSOT 값(BEAR, HIGH_VOL)으로 교체. 근거: CAT-CONSTANTS Regime Kelly cap 표에서 BEAR(~0.010)·HIGH_VOL(~0.012)이 나머지 국면(BULL 0.028, SIDEWAYS 0.018, CHOP/UNKNOWN 0.015) 대비 가장 낮은 리스크 허용치 — "위험 국면" 취지에 부합하는 실제 라벨 쌍.

**부수 정정 (변수명 통일)**
§2 표에서 `gate_pass_rate` 분자 `gate_passed_candidates`와 `virtual_entry_rate` 분모 `gate_pass_candidates`가 동일 대상인데 표기가 다름 → **`gate_passed_candidates`로 통일**.

### SPOT/FUT 분기
공통 (§4 비변경 — market_type 하드코딩 없음, 정정과 무관)

### 인접 CAT 영향
- **CAT-G**: 없음 — 코드·config_kv 비접촉, 문서 내 라벨 표기만 실제 SSOT 값으로 정정
- **CAT-C/B/F/N/D**: 없음
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
문서 표 1개 행 + 변수명 2곳 재수정만 — 코드·config 영향 없음

### Cursor 지시
- Targeted diff only — `14_UNIVERSE-BT_구조생존검증.md` §2의 `crash_window_forced_exit_rate` 행과 변수명 통일 2곳만 수정. **문서 전체 재작성 금지**
- `CLAUDE_TO_CURSOR.md` 기존 U0 prepend 블록도 동일하게 §2 부분만 수정(전체 재작성 금지)
- 루트 주식 경로 무접촉, **U1 코드 착수 계속 금지** (U0 SSOT 미확정 상태)
- 테스트: 해당 없음 (문서)

### 위험도
🟢 (문서 전용 · CAT-G Critical 코드 비접촉 — 라벨 표기 정정뿐, 코드측 Critical 파급 없음)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U0 정정 사유 1줄 (CRASH → BEAR/HIGH_VOL)
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`: "UNIVERSE-BT-U0 정정 완료 → 재검증 요청"으로 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md`: `WAIT_CLAUDE_OK` 유지 (변경 없음)
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: **이번 라운드는 갱신 대상 아님** (룰13 — OK+Handoff 확정 후에만 갱신, 재검증 OK 시 진행)

---

*버전 2026-08-23 · UNIVERSE-BT-U0 수정 라운드 · Architect: Claude Pro · Engineer: Cursor*

---

﻿# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U0 · 기존 B1 Handoff 위에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]  
> **상태**: Cursor **구현 대기** · U0 문서만 · **U1 착수 금지**  
> **병행**: B1-LADDER-R1a OBSERVE **유지** (상호 게이팅 없음)

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT 로드맵 배치 + U0 구조생존검증 정의문서

### sub-phase ID
UNIVERSE-BT-U0

### SSOT (변경 금지 unless noted)
- 신규: `bitget/docs/work_phases/14_UNIVERSE-BT_구조생존검증.md`
- 포인터 1줄만 추가: `00_마스터_로드맵.md` 말미 (표 재작성 금지, 13_B1_신뢰사다리 방식 재사용)
- 변경 없음: `forward/`, `factory_pipelines.py`, config_kv, CAT-C/B/G/F/N/D 코드 전체

### 변경 Spec

**문서 목차 (14_UNIVERSE-BT_구조생존검증.md)**
- §1 유니버스 스냅샷 정의 — `load_dynamic_universe()` 출력(거래량 floor 통과분) ∩ 보유 OHLCV(`BITGET_SPOT_*`/`BITGET_FUT_*`) 커버리지. "상장 전부" 제외 사유 1문단(라이브 스캐너와 동일 필터 사용 — 외부 타당도).
- §2 지표 5종 (수식)
  - `hit_rate = raw_signal_hit / total_bars_scanned`
  - `gate_pass_rate = gate_passed_candidates / candidates_generated`
  - `virtual_entry_rate = virtual_entries / gate_passed_candidates`
  - `crash_window_forced_exit_rate` (CAT-G `CURRENT_REGIME_KEY` **BEAR ∪ HIGH_VOL** 구간 한정, SL/MDD 트리거 비율 — CRASH 라벨 없음)
  - `side_asymmetry_ratio = LONG_virtual_entries / SHORT_virtual_entries` (국면별, SPOT SHORT=0 각주)
- §3 L0 라벨 · Kill(과신 표현) — 모든 산출물 상단 고정 배너: **"L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지"**. 지표를 R6 판정이나 B1 성공계약(§1)에 대입 시 즉시 정정 대상.
- §4 SPOT/FUT 분리 원칙 — SPOT-FUT 비대칭표 인용(재작성 금지), `market_type` 파라미터화, 하드코딩 금지.
- §5 로드맵 표 — U0(본 Handoff)→U1(하니스, 🟡)→U2(배치, 🟢)→U3(리포트, 🟢). Track B(R1a~R6) 게이팅과 무관, 병렬 독립.

### Config 변경 (있으면)
없음

### 인접 CAT 영향
- CAT-C, B, G, F, N, D: **없음** (문서 전용, 코드 비접촉)
- Track B (B1-LADDER R0~R6): **없음** — 병렬 독립, 상호 게이팅 없음

### 롤백 조건
- 문서 삭제만으로 완전 롤백 (코드·config 영향 없음)

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- 이번 sub-phase는 **문서 작성만** — U1 코드 착수 금지(별도 Handoff 대기).
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: 해당 없음 (문서)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` UNIVERSE-BT-U0 섹션
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `bitget/docs/work_phases/NEXT_STEP.md`
- `bitget/docs/work_phases/09_디렉터_쉬운요약.md` (쉬운 말·비유로 — "과거로 돌려본 결과는 참고용, 실전 증명 아님" 톤 유지)

### 위험도
🟢 (문서 전용 · Critical 코드 비접촉)

---

# CLAUDE → CURSOR · 상단 추가분 (기존 B1-LADDER-R0 Handoff 위에 붙여넣기)

---

## Claude OK — B1-LADDER-R0 (2026-08-23)

**판정: OK.**

검증 근거:
- `13_B1_신뢰사다리.md` 성공계약(B1 12~18% / MDD≤5% / 6~12개월) = `00` §0.4 표 그대로 재사용, **신규 상수 없음** (룰5)
- 렁 R0→R1→R2→(A06)→R3∥R4→R5→R6 순서, Critical 표기(🔴 R3/R4/R5), 승인문구 템플릿 — Ask 스펙과 일치
- `00` §0.4 말미 **1줄 포인터만** 확인, 표 비변경 확인
- 코드 · config_kv · execution_safety · gates · Kelly · deathmatch live **비접촉** 확인
- SPOT/FUT 공통 (market_type 분기 없음) 확인
- 인접 CAT: CAT-J 읽기만, CAT-H/D/N/G 비접촉 — 스펙과 일치
- 세션 종료 문서(`05`, `00` 용어집=전체현황판, `09`, `NEXT_STEP`, `NEXT_ACTION`) 갱신 확인
- 위험도 🟢 문서 전용 — 표기 적절

**참고 지적 (수정 요구 아님, R1b 설계 시 참고):**
R1a에서 "냉시동 vs 구조막힘"을 가르려면 후보 생성 여부(스캔 히트) 대비 진입 거절 여부가 필요한데, LONG 쪽은 `blocked_today` 텔레메트리가 아직 없음 (LS-GOAL-UX-01 기록: SHORT만 `short_funnel.blocked_short_total` 보유). 지금 코드 변경 요청 아님 — R1a 판정에서 LONG 증거가 약할 수 있다는 점만 인지.

**다음:** 05에 이 OK 기록 · 아래 R1a Handoff로 진행.

---

## [CAT-F] B1 신뢰사다리 — R1a 관측 마감 판정 기준

### sub-phase ID
**B1-LADDER-R1a**

### SSOT (변경 금지 unless noted)
- 수정(추가만): `bitget/docs/work_phases/13_B1_신뢰사다리.md` §3 Kill 표 아래에 "R1a 판정 절차" 소절 추가
- 참조만(비변경): §6 SQL, `short_funnel_report_bg.py`, `post_deploy_obs_digest_bg.py`
- config: **없음**

### 변경 Spec (문서 전용 · 신규 코드 없음)
R1a는 **3갈래 판정**, 매주 재관측:

| 판정 | 조건 | 행동 |
|------|------|------|
| **PASS** | 신선 실측에서 OPEN>0 신규 진입 확인 | R2 착수 (효과표 채우기 시작) |
| **관측 유지** | OPEN=0 지속 **AND** R0 확정일(2026-08-23)로부터 **4주 미만** 경과 **AND** 구조적 거절 증거 없음 | Kill 미발동 · 주간 재실측 반복 |
| **FAIL (구조막힘)** | 아래 (a) 또는 (b) **하나만 충족해도** 확정 — 4주 대기 불필요 | R2 착수 금지 유지 · R1b(CAT-C) 디렉터 승인 후 별도 대화 |

FAIL 근거 (a)/(b):
- (a) **4주 경과 후**에도 OPEN=0 지속 (기존 Kill 표 §3 그대로, 신규 상수 아님)
- (b) `short_funnel`에서 후보는 생성되나(`blocked_short_total`>0 등 진입 직전 거절 이벤트 존재) OPEN으로 이어지지 않는 패턴이 반복 관측됨 (SHORT만 현재 가시성 있음, 위 참고 지적 참고)

판정 근거는 매주 `05_진행로그.md`와 `CURSOR_TO_CLAUDE.md`에 **숫자만** 기록 (OPEN count, CLOSED count, short_funnel blocked count 있으면 같이). 코드 diff 없음.

### SPOT/FUT 분기
공통 (market_type 하드코딩 없음)

### 인접 CAT 영향
- **CAT-C**: 없음 (이번 R1a는 미착수 · R1b FAIL 확정 시에만 별도 Handoff)
- **CAT-J**: 읽기만 (`short_funnel_report_bg.py`, `post_deploy_obs_digest_bg.py` 기존 값 참조만, 재계산 없음)
- **CAT-H/D/N/F(Kelly/live/execution_safety)**: 비접촉

### 롤백 조건
문서만 (판정 절차 소절 삭제/재작성) — 코드 영향 없음

### Cursor 지시
- Targeted: `13_B1_신뢰사다리.md`에 위 판정표만 추가. **전체 재작성 금지**
- 디렉터가 §6 SQL 신선 실측값(+ 가능하면 short_funnel blocked count)을 전달하면, 그 숫자를 위 3갈래 판정표에 대입해 PASS/관측유지/FAIL만 표기
- 신규 코드 · 신규 테스트 없음
- 루트 주식 경로 무접촉

### 위험도
🟢 문서 전용

### 세션 종료 의무
- `05_진행로그.md`: B1-LADDER-R1a 섹션 + 매주 실측 숫자
- `00_전체현황판.md`: 다음 Handoff 필드 갱신
- `CURSOR_TO_CLAUDE.md`: 실측 숫자 + 판정 결과 보고
- `NEXT_ACTION.md`: 판정 결과에 따라 `WAIT_CLAUDE_OK`(FAIL 시 R1b 승인 대기) 또는 관측 유지 표기
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: Claude가 판정 결과 확인 후 직접 갱신 (룰13)

# CLAUDE → CURSOR · B1-LADDER-R0

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-F]  
> **상태**: Cursor 구현 완료 · **Claude OK 2026-08-23**  
> **금지**: config_kv · live · Kelly · MDD 코드 · execution_safety · gates · deathmatch live

---

## [CAT-F] 자본배분&리스크 — B1 성공계약·신뢰사다리 R0 문서화

### sub-phase ID
**B1-LADDER-R0**

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/docs/work_phases/13_B1_신뢰사다리.md`
- 참조만(비변경): `00_마스터_로드맵.md` §0.4, `05`/`06`, `12_듀얼북극성…`
- config: **없음**

### 변경 Spec
- 코드 변경 **없음**
- `13_B1_신뢰사다리.md`: (1) 성공계약 B1 (2) 렁 R0~R6 (3) Kill 표 (4) 신뢰밴드
- `00` §0.4 말미 **1줄 포인터만** (표 재작성 금지)
- SPOT/FUT: 공통

### Config 변경
없음

### 인접 CAT
- CAT-J: 읽기만 · CAT-H/D/N: 없음

### 롤백
문서만 재작성 (코드 영향 없음)

### Cursor 지시
- Targeted · `bitget/` only · R1a는 **관측만** · 테스트 없음

### 위험도
🟢 문서 전용

### 세션 종료 의무
- `05` · `00` 용어집 · `CURSOR_TO_CLAUDE` · `NEXT_ACTION`→WAIT_CLAUDE_OK · `09` · `NEXT_STEP`
