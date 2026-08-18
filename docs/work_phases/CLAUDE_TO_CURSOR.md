# CLAUDE → CURSOR (Handoff INBOX)

> ⛓ **세션 SSOT** → [`00_SESSION_SYNC.md`](00_SESSION_SYNC.md) · **Claude는 본 파일 + OUTBOX/CURSOR_TO_CLAUDE만 쓰기**  
> `Downloads/*` 복사본 merge 전까지 **본 경로 우선**.

> **작성**: Claude Pro **만** (디렉터 채팅 중계 · Cursor 랜딩 2026-08-19)  
> **현재**: NS-DIAG-DASH-01 **Claude OK · CLOSED** · OBS-HOLD · 신규 Alpha Handoff **없음** · 앵커 `SYNC-2026-08-19-E`

---

## Claude VERDICT — NS-DIAG-DASH-01 Claude OK (2026-08-19)

> 소스: 디렉터 붙여넣기 `[CAT-J] NS-DIAG-DASH-01 검증 완료 — OK`

**VERDICT: OK · 닫음.** Track A 쉬운판 = Bitget UX 4칸 · `bitget/**` 0 · OPEN=0≠자동🔴 · CAT-J 표시만(Critical 아님).  
**신규 Alpha Handoff: 없음.** OBS-HOLD 유지 · n≥20 재소집 전 대기.

### 후속 (디렉터)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

→ 다음 19:30 `[쉬운판]` 🟢/🔴/🟡/⬜ 육안.

---

## Claude VERDICT — OPS-LIQUIDITY-STALL-01 GATE WORKING Claude OK (2026-08-19)

> 소스: `Downloads/CLAUDE_OK_OPS-LIQUIDITY-STALL-01.md`

**VERDICT: OK · 닫음.** VPS (a~d) n=40 검증 · GATE WORKING 승인.  
**임계 완화 Handoff: 열지 않음.** (c)/(d) 추적 sub 불필요 · 유동성 정책 논의는 디렉터 승인 시에만.

### 검증 근거 (Claude 원문)

1. 수치 정합 — a 7.5% · b 87.5% · c+d 5%&lt;30% · `05`와 일치  
2. 논리 정합 — LIQUIDITY는 DNA 이전 게이트 · cutoff 배제 타당 · (b)=정상 발화  
3. DoD 4/4 · CAT-MAP 경계 위반 없음  

### 후속

- `L-DATA-ALARM-01` 백로그 유지 (sub 미개설)
- OBS-HOLD · n≥20 재소집 · Alpha/임계 완화 금지

---

## Claude VERDICT — OPS-OPEN-STALL-01 진단 OUTBOX Claude OK (2026-08-19)

**VERDICT: OK.** Step 0~2 삼각확인 (LIQUIDITY 100% · DNA_FAIL 0 · scored near-miss 0).  
CLASS **(a)** 채택 · cutoff 가설 이 stall 윈도우에서 **종결**.  
Step 3 계측 공백 = 참고만 · `L-DATA-ALARM-01` 백로그 한 줄 (신규 sub 개설 X).

---

## [CAT-B] OPS-LIQUIDITY-STALL-01 — LIQUIDITY 100% 집중의 원인 분류 (read-only)

### SSOT (변경 금지 unless noted)

- 파일(읽기만): `scanner_funnel.py`(LIQUIDITY 판정 위치), `market_data_fetcher.py` / `data_updater.py`(OHLCV 소스), `scan_funnel_drop_event`(C-FUNNEL-02 테이블)
- config: 리퀴디티 임계 관련 key 전부 — **읽기만, 값 변경 금지**

### 변경 Spec

- 함수/정책: 신규 진단 스크립트 1개 (`scripts/ops_liquidity_stall_01_diagnosis.py`, 이름은 Cursor 재량) — **정책 코드 미수정, 순수 조사**
- 절차:
  1. stall 윈도우(KR/US 각각 `since ≈ 2026-08-17 15:10`, OUTBOX 기준 그대로) `scan_funnel_drop_event` reason=LIQUIDITY 행에서 슬롯당 상위 N=20 종목코드 표본 추출 (cap 없으면 전체 중 앞 N)
  2. 표본 종목 각각 `market_data_fetcher.fetch_market_data`로 해당일 Close/Volume(또는 5일 평균 거래대금) 실측 join
  3. 코드 기준 임계(KR Close&lt;1000 · US Close&lt;0.5 · 5일 평균 거래대금 floor, ~US $300k 환산)와 대조해 4분류:
     - **(a)** 실가격 컷 정상 발화
     - **(b)** 실거래대금 컷 정상 발화
     - **(c)** Volume 필드 결측/0/이상치 — 데이터 파이프라인 의심
     - **(d)** 임계 계산식 자체 오류 의심 (코드가 threshold를 잘못 적용)
  4. (c)+(d) 비중이 표본의 **30% 이상**이면 데이터/코드 결함 가설로 보고, 미만이면 "게이트 정상 작동 + 해당 구간 저유동성 종목 집중"으로 결론
- KR/US: 공통 절차, 마켓별 threshold 상수는 CAT-C 기존 값 그대로 분기 (수정 없음)

### Config 변경 (있으면)

없음 — 이번 sub-phase는 조사 전용, config_kv 쓰기 0건

### 인접 CAT 영향

- CAT-C: 읽기만 (funnel LIQUIDITY 판정 로직 참조, 코드 미수정)
- CAT-B(OHLCV): 읽기만 (fetch_market_data 재사용, schema 미접촉 → Critical 승인 불요)
- CAT-D: 없음 (try_add 미접촉)

### 롤백 조건

- 해당 없음 — read-only 진단, 운영 동작 변경 없음. 스크립트 파일 자체 revert만으로 충분

### Cursor 지시

- Targeted diff only — 신규 진단 스크립트 파일 1개 추가. 기존 `scanner_funnel.py`/`market_data_fetcher.py` **미수정**
- cutoff/config_kv/threshold 값 변경 **절대 금지** (이번 결과가 (c)/(d)로 나와도 즉시 완화 금지 — 별도 Handoff 필요)
- Step 3 계측 공백(drop_event vs snapshot survivors 불일치)은 이 sub-phase 범위 밖 — 발견 시 결과표에 한 줄만 병기, `L-DATA-ALARM-01`로 분리
- 결과는 `CURSOR_TO_CLAUDE.md` OUTBOX에 (a)/(b)/(c)/(d) 표로 append → Claude 재검증
- 테스트: 신규 정책 로직 없음 — 실행 로그 + 표본 대조표로 충분 (unit test 불요)

### 위험도

- 🟡 Medium (CAT-B read-only, schema 비접촉 → Critical 아님, Claude↔Cursor 교차검증만)

**백로그 메모**: OPS-OPEN-STALL Step3 계측 공백 → `L-DATA-ALARM-01` (신규 sub 개설 X).

---

## Claude VERDICT — NS-DIR-DASH-01 DoD 5/5 Claude OK (2026-08-18)

**VERDICT: OK.** DoD 5/5 · 테스트 14 passed · Critical·config_kv·신규 cron·mega_trend·CAGR 확정 로직 비접촉.  
추가 코드 확인 불필요. **디렉터 잔여**: VPS pull · 19:30 `[쉬운판]` 육안.

---

## [CAT-C] OPS-OPEN-STALL-01 — survivors≈0 원인 진단 (read-only)

### SSOT (변경 금지 unless noted)

- 파일: `scan_funnel_drop_event`, `scan_funnel_snapshot` (둘 다 C-FUNNEL-02, 2026-08-09 배포 완료 — 스키마 재사용, 신규 컬럼/ALTER 금지)
- config: 없음 (읽기전용)

### 변경 Spec

- 함수/정책: 신규 read-only 스크립트 `scripts/ops_open_stall_01_diagnosis.py` (이름은 Cursor 자율)
- **Step 0 (선행 확인)**: `scan_funnel_drop_event`가 실제로 적재됐는지 먼저 확인

```sql
  SELECT market, COUNT(*), MIN(ts), MAX(ts),
         COUNT(DISTINCT substr(ts,1,10)) AS days
  FROM scan_funnel_drop_event GROUP BY market;
```

  0행이면 C-FUNNEL-02 결선 재점검(별도 보고, 본 Handoff 범위 밖) — 진행 전 반드시 이 결과부터 첨부.

- **Step 1**: 최근 stall 윈도우(survivors≈0 시작 시점 ~ 오늘)를 `market, reason` GROUP BY COUNT(*) — LIQUIDITY vs DNA_FAIL vs 기타 사유 비중
- **Step 2**: 사유별 near-miss 상위 5건(`|score-cutoff|` 최소) + 해당 시점 `eff_cos_cutoff`/`eff_ml_cutoff` 스냅샷 값 — cutoff가 DEFENSE band에서 비정상적으로 타이트한지 판별
- **Step 3**: 예외 슬롯(2026-08-17 14:15 KR survivors=5)의 drop_event 분포를 같은 쿼리로 대조 — "가끔 통과"와 "상시 차단"의 차이 확인
- KR/US: 공통 (market 컬럼 분리 출력)

### Config 변경 (있으면)

없음 — cutoff/threshold/config_kv 비접촉.

### 인접 CAT 영향

- CAT-D: 없음 (원장 비접촉)
- CAT-F/G (Critical): 없음 — Kelly·notional·국면 판정 로직 비접촉, config_kv 라이브 변경 **금지**
- CAT-L: 없음 (인프라 RED 알람은 별도 트랙, 본 Handoff 범위 밖)

### 롤백 조건

- 해당 없음 (신규 코드 없음, 스크립트 삭제로 완전 롤백)

### Cursor 지시

- Targeted diff only. `scan_funnel_drop_event`/`scan_funnel_snapshot` 스키마 변경 금지.
- 결과를 `CURSOR_TO_CLAUDE.md`에 표로 append하고 3가지 중 하나로 분류해 보고:
  - **(a)** LIQUIDITY 압도적 → 유동성 데이터 정체 재의심(CAT-B)
  - **(b)** DNA_FAIL/cutoff 근접 다수 → DEFENSE band cutoff 과타이트 (정책은 Claude가 후속 결정, 지금은 진단만)
  - **(c)** 절대 카운트 자체가 비정상적으로 적음 → 유니버스 상류(CAT-B) 재조사
- 테스트 불필요(순수 SELECT) — 실행 로그 원문 첨부.

### 위험도

🟢 Low — read-only, 정책/config/Critical 비접촉. OBS-HOLD 유지 상태에서 실행 가능.

**ID**: OPS-OPEN-STALL-01 · CAT-C  
**인프라 RED 알람**: 분리 — 가칭 `L-DATA-ALARM-01` (CAT-L) 후순위.

---

## Claude VERDICT — ROADMAP-SYNC-01 DoD 4/4 Claude OK (2026-08-18)

> **랜딩**: 디렉터 채팅 중계 · Cursor append (2026-08-18).  
> **앵커**: `SYNC-2026-08-18-B` → close `SYNC-2026-08-18-C`

**VERDICT: OK.** 문서 3건(`15_POST_RP1`, `00_전체현황판`, `00_마스터_로드맵`) 라벨링 확인,  
코드 diff 0 확인. 인접 CAT 영향 없음. 롤백 불필요.

**다음**: OBS-HOLD 유지. 재소집 트리거 = VPS daily n≥20 (~2026-09-05).  
Ops 잔여(git pull, 19:30 육안)는 디렉터/Cursor 담당.

---

## Claude VERDICT — ROADMAP-AUDIT-01 · 문서 Must-fix Go + NS-OBS-TG-01 OK (2026-08-18)

> **랜딩**: 디렉터 채팅 중계 · Cursor append (2026-08-18).  
> **앵커**: `SYNC-2026-08-18-A` 기준.

### 감사 결론 (Cursor OUTBOX A~E 회신)

- 카테고리(A~Q)·목표 숫자 — 빠짐없음, 새 CAT 불필요
- 작업순서 문서만 낡음 — 아래 Must-fix 1건으로 충분

### Must-fix Go: ROADMAP-SYNC-01 (문서 3개·코드 0)

## [Meta/문서] ROADMAP-SYNC-01 — 로드맵 문서 상태 라벨 정정 (동결 반영)

### SSOT (변경 금지 unless noted)

- 변경: `15_POST_RP1_단계별로드맵.md` (단계표 2행, §단계2 헤더), `00_전체현황판.md` (Phase표 Phase3 행), `00_마스터_로드맵.md` §0.1 뒤 또는 `00_전체현황판.md` 목표수치 표 아래
- 비접촉: 코드, config_kv, cron, VPS · (본 Handoff 본문의 상태문서 라벨만)

### 변경 Spec

- `15_POST_RP1` 단계표 2행: 상태 `🟡 Claude OK 대기` → `🔴 동결(규칙1)` + 각주 "BULL-RECENCY-01 = 근처놓침 레버 소진 목록 포함, NEXT_ACTION 재접촉 금지 확인"
- `00_전체현황판` Phase표 3행: `다음`/`Handoff 대기` → `동결` + 각주 "C-1-REDUCED INVALID·2단계 No-Go(05 진행로그 참조)"
- 다중 시계 표 삽입 (정확히 아래 4행, 신규 숫자 없음):

| 시계 | 의미 | CAGR "달성" 주장? |
|------|------|-------------------|
| daily n≥20 | 갈림길 재소집만 | ❌ |
| ASG 4주 | L1 조기경보 | ❌ |
| G2(~56d+) | L2 수익 페이스 | △ 주 근거 |
| G4+디렉터 | 상품화/실전 | ✅ LIVE만 |

+ 문장: "RP-1 Pass ≠ 40~70% 달성"

### Config 변경

없음

### 인접 CAT 영향

- 없음 (전 CAT 코드·config 비접촉, 상태문서 라벨링만)

### 롤백 조건

- 텍스트 되돌리기만 — 실행 영향 0이라 사실상 롤백 불필요

### Cursor 지시

- Targeted diff only, 3개 파일 각 수 줄
- 완료 후 `CURSOR_TO_CLAUDE` OUTBOX 1건 · `status: WAIT_CLAUDE_OK`

### 위험도

🟢 Ops-lite

### Defer (n≥20 재소집 시 일괄)

01/02/03 로컬표, 09 스냅샷, 06 효과표, SSOT 배너

### 재소집 3택 옵션 카드 (초안·실행 아님)

| 옵션 | Go 조건 | 지금 금지 | CAT 경계 |
|------|---------|-----------|----------|
| mega_trend (CAT-P) | n≥20 + G1 판정 + 디렉터 승인 | 재소집 전 설계·config 착수 | CAT-P 단독, CAT-F/G 임계값 재정의 금지 |
| 목표하향 | n≥20 + North Star 페이스 실측 + 디렉터 명시 지시 | 임의 CAGR/MDD 수치 변경 | CAT-CONSTANTS 개정 필요(디렉터 지정값만) |
| 관측연장 | n&lt;20 시 기본값(별도 승인 불요) | 없음(현행 유지) | 영향 없음 |

### 신규 Alpha Go

지금 열지 않음 확인 — BULL/SIDE/BEAR/C-1 소진·동결, Phase B/D 미착수 유지.

### NS-OBS-TG-01

DoD 6/6 확인. **VERDICT: OK.**

### 다음

ROADMAP-SYNC-01 완료 후 OUTBOX → Claude 재확인. 실질 Go는 여전히 VPS daily n≥20 (~2026-09-05).

---

## Claude VERDICT — FWD-OBS-HOLD-01 DoD 4/4 Claude OK · OBS-HOLD 확정 (2026-08-17)

> **랜딩**: 디렉터 채팅 중계 · Cursor가 본 파일 최상단 동기화 (2026-08-17).  
> **앵커**: 요청문 `SYNC-2026-08-11-B` 무시 → §3 최신 **`SYNC-2026-08-17-Y`** 기준 (규칙 §5).

### OUTBOX 검증 (`CURSOR_TO_CLAUDE.md` 최상단 FWD-OBS-HOLD-01 DoD 4항)

| # | DoD | 파일 실측 | 판정 |
|---|-----|-----------|------|
| 1 | NEXT_ACTION OBS-HOLD + n≥20 트리거 | status OBS-HOLD · 트리거표 n≥20 · ~2026-09-05 · mega_trend/목표하향 금지 | ✅ |
| 2 | SYNC §3 bump + sub-phase | 앵커 `SYNC-2026-08-17-Y` · 진행 중 = FWD-OBS-HOLD-01 | ✅ |
| 3 | 05 CLOSE Claude OK + OBS-HOLD 섹션 | L265 Claude OK · DoD 5/5(CLOSE) + L268–276 OBS-HOLD | ✅ |
| 4 | 코드 diff 0 | 문서 외 비접촉 · config_kv/cron/VPS 원장 미접촉 | ✅ |

규칙 위반 없음: 규칙4(n=8&lt;20 → 페이스 확정 안 함) · 규칙2(Lookahead v1 — Hold = Pass≠증명 실행) 준수.

**VERDICT: OK.** FWD-OBS-HOLD-01 완결.

### 판단 — 이번 창의 Go = 신규 Handoff 없음

OBS-HOLD 목적 = 재소집 조건 충족 전 신규 착수 차단. BULL/SIDE/BEAR/C-1 소진 · mega_trend·목표하향은 트리거(n≥20, ~2026-09-05) 전 금지 · OPS 관측 중. 다른 sub-phase 개시 = 규칙1·4 위반.

**선택한 Go = 관측유지 지속 · 문서 동기화만.**

### [Sync-only] FWD-OBS-HOLD-01-VERDICT-SYNC

| 항목 | 값 |
|------|-----|
| **범위** | 본 VERDICT를 `CLAUDE_TO_CURSOR.md` 최상단 append만 |
| **금지** | NEXT_ACTION/SYNC/05 재수정 · mega_trend/목표하향 · 코드·config_kv·cron·VPS · 신규 sub-phase |
| **DoD** | 본 VERDICT 반영 1건 |
| **다음 실질 Go** | VPS daily **n≥20** (~2026-09-05) 도달 시 갈림길 재소집 — 그때 3택(mega_trend / 목표하향 / 관측연장)을 VPS 실측으로 재판단 |

---

## Claude VERDICT — FWD-LEDGER-CRON-01-CLOSE DoD 5/5 Claude OK · 갈림길=관측유지 (2026-08-17)

> **랜딩**: 디렉터 채팅 중계 · Cursor가 본 파일 최상단 동기화 (2026-08-17).  
> **앵커**: 요청문 `-08-11-B` 무시 → §3 최신 **`SYNC-2026-08-17-X`** 기준.

### OUTBOX 검증 (CLOSE DoD 5항)

| # | DoD | 판정 |
|---|-----|------|
| 1 | 12 §3 SSOT 각주 | ✅ |
| 2 | 로컬 rename | ✅ `*.LOCAL_DEV_DO_NOT_USE.json` |
| 3 | SYNC bump + CLOSED | ✅ `SYNC-2026-08-17-X` |
| 4 | NEXT_ACTION 20일 문구 | ✅ |
| 5 | 코드 diff 0 | ✅ |

**VERDICT: OK.** CLOSE 완결.

### 판단 — 갈림길 3택은 지금 열 수 없음

VPS daily n=8 · G1(≈28일·종합≥40) 대비 표본 부족. mega_trend·목표하향·단일 composite 4.09 기반 확정은 규칙4 위배. **열 수 있는 건 관측유지뿐** — "결정"이 아니라 **재소집 조건 고정**.

**Go: FWD-OBS-HOLD-01** (아래).

---

## [Ops-lite] FWD-OBS-HOLD-01 — 갈림길 관측유지 · 재소집 트리거 고정

| 항목 | 값 |
|------|-----|
| **sub-phase** | FWD-OBS-HOLD-01 (신규) |
| **발행** | Claude Pro Architect |
| **전제** | FWD-LEDGER-CRON-01-CLOSE Claude OK · VPS 원장 SSOT 고정 |
| **위험도** | 🟢 Ops-lite — 문서만, 코드 0 |

### SSOT (변경 금지 unless noted)

- `NEXT_ACTION.md` · `00_SESSION_SYNC.md` §3 · `05_진행로그.md`
- VPS 원장 — 읽기만, 내용 변경 없음

### 변경 Spec

- `NEXT_ACTION.md`: 상태 **관측유지(OBS-HOLD)**. 트리거 = **daily n≥20** 도달 시 자동 재소집(예상 ~2026-09-05, VPS 실측 재확인 후 확정). 그 전 mega_trend/목표하향 착수 **금지**.
- `00_SESSION_SYNC.md` §3: 앵커 bump **`SYNC-2026-08-17-Y`** · 진행 중 = FWD-OBS-HOLD-01
- `05_진행로그.md`: CLOSE에 `Claude OK: 2026-08-17` + OBS-HOLD-01 섹션 신규

### 금지

- mega_trend 설계/구현 · 목표(CAGR/MDD) 수치 변경 · VPS 원장 재계산/수정 · cron 재접촉 · 코드 변경 전면

### 완료기준 (DoD)

| # | 기준 |
|---|------|
| 1 | NEXT_ACTION OBS-HOLD + n≥20 트리거 |
| 2 | SYNC §3 bump + sub-phase 갱신 |
| 3 | 05 CLOSE Claude OK 라인 + OBS-HOLD-01 섹션 |
| 4 | 코드 diff 0 |

### Timebox

15분.

### Cursor 지시

1. 문서 3개만 · 코드/config_kv/cron 비접촉
2. 완료 후 `CURSOR_TO_CLAUDE` OUTBOX · `status: WAIT_CLAUDE_OK`

---

## Claude VERDICT — FWD-LEDGER-CRON-01 DoD 4/4 Claude OK · SSOT=VPS 고정 (2026-08-17)

> **랜딩**: 디렉터 채팅 중계 · Cursor가 본 파일 최상단 동기화 (2026-08-17).  
> **앵커**: 부팅 문구 `-08-11-B` 무시 → §3 최신 **`SYNC-2026-08-17-W`** 기준.

### OUTBOX 검증 (DoD 4항)

| # | DoD | 판정 |
|---|-----|------|
| 1 | cron/timer 상태 표 | ✅ factory-kr 19:30 daily · 8/9~8/16 연속 |
| 2 | 실행 로그·에러 | ✅ syslog CMD · mail/에러 없음 · mtime=cron |
| 3 | 원인 3분류 | ✅ (a)(b)(c) 해당 없음 — 로컬 정체 전제 기각 |
| 4 | 코드 diff 0 | ✅ |

**VERDICT: OK.** Cursor 질문 회신: **VPS 원장 = North Star SSOT 고정.** 로컬 원장은 dev artifact · 판정 근거 금지.

참고(규정2·4): VPS composite 4.09 · n=8/28 — G1 미달·표본 협소 → 최소 20일 추가 관측 전 페이스 확정 판정 금지. 로컬 "40.0"은 허수.

### 다음 Go

**FWD-LEDGER-CRON-01-CLOSE** (아래).

---

## [Ops-lite] FWD-LEDGER-CRON-01-CLOSE — VPS 원장 SSOT 고정 · 로컬 원장 격리

| 항목 | 값 |
|------|-----|
| **sub-phase** | FWD-LEDGER-CRON-01-CLOSE (신규) |
| **발행** | Claude Pro Architect |
| **전제** | FWD-LEDGER-CRON-01 Claude OK |
| **위험도** | 🟢 Ops-lite — 문서·파일명만, 코드 0 |

### SSOT (변경 금지 unless noted)

- 문서: `00_SESSION_SYNC.md` §3 · `NEXT_ACTION.md` · `05_진행로그.md` · `00_전체현황판.md` · `12_듀얼북극성_진행장부_및_상품화.md` §3
- VPS 원장: `/var/lib/quant-factory/data/dual_north_star_ledger.json` — SSOT 고정 (내용 변경 없음)
- 로컬 원장: `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\dual_north_star_ledger.json` — 격리 대상

### 변경 Spec

- `12_듀얼북극성` §3에 각주: "SSOT=VPS 경로. 로컬 PC 원장은 dev artifact, 갈림길/판정 근거 사용 금지."
- 로컬 원장 **파일명 rename만**: `dual_north_star_ledger.LOCAL_DEV_DO_NOT_USE.json` (`factory_data_dir()` 등 소스 로직 변경 금지)
- KR/US: 공통 (Track A 전체)

### Config 변경

없음 — config_kv 비접촉

### 금지

- cron/timer 재시작 · config_kv/bitget · 소스 코드 변경 · Handoff 재조사

### 완료기준 (DoD)

| # | 기준 |
|---|------|
| 1 | `12_듀얼북극성` §3 SSOT 각주 추가 |
| 2 | 로컬 원장 rename/격리 완료 (ls 결과 첨부) |
| 3 | SYNC §3 앵커 bump(`SYNC-2026-08-17-X`) + FWD-LEDGER-CRON-01 → CLOSED |
| 4 | NEXT_ACTION: 갈림길 재소집 + VPS 실측 기반 · 최소 20일 관측 전 확정 판정 금지 |
| 5 | 코드 diff 0 재확인 |

### Timebox

30분.

### Cursor 지시

1. 코드 diff 0 유지 · 문서 4개+§12 + rename만
2. 완료 후 `CURSOR_TO_CLAUDE` OUTBOX 1건 · `status: WAIT_CLAUDE_OK`

---

## Claude VERDICT — SRV-02 조사 Done Claude OK · 다음 트랙 확정: 포워드 원장 진단 (2026-08-17)

> **랜딩**: `Downloads/CLAUDE_TO_CURSOR_append.md` → Cursor가 본 파일 최상단 동기화 (2026-08-17).  
> **앵커**: `SYNC-2026-08-17-V` (요청문 `-08-11-B` 아님 — §3 당시 `U` 기준 판정 · bump V).

### OUTBOX 검증 (`CURSOR_TO_CLAUDE.md` 최상단 SRV-02 DoD 4항)

| DoD | 결과 |
|-----|------|
| 포워드 원장 표(일수/종합점수/G0·G1 근접) | ✅ n=1/28 · 종합 40.0(하한 충족) · G1 −27일 갭 |
| CAT-P mega_trend 인벤토리 | ✅ kill-chain 7모듈 부분배선(`ENABLE_MEGA_TREND_UNLOCK=1`) · re_evolution 7개 RP-1과 미연결 |
| 근처놓침 동결표 대조 | ✅ `05_진행로그` 라벨과 불일치 없음 (SIDE_03 이중 SSOT는 기존 명시 그대로) |
| OUTBOX 코드 diff 0 | ✅ config_kv/bitget 비접촉 · BULL/SIDE/BEAR/C-1 재계산 없음 |

규칙 위반 없음 확인: period_return_pct 우선(규칙3) · n<20 자동판정 없음(규칙4, 전 구간 n≫20) · MDD tier 기준 사용(규칙5).

**VERDICT: OK.** SRV-02 진단 종결.

### 판단 — 갈림길 3택 중 1

포워드 원장 `updated_at`이 2026-08-11T15:57:50Z 이후 정지 — 조사일(2026-08-17) 기준 **5일+ 무갱신**, `forward_trades_count=0`, daily history 1/28. 이 상태에서는 **목표 하향**도 **mega_trend 신규 투입**도 판단 근거가 없다: Lookahead v1 전제(규칙2, Pass ≠ North Star CAGR 증명)의 증명 경로 자체가 지금 막혀 있어서다. 두 옵션 다 "포워드 관측이 실제로 도는지" 확인 이후에만 의미 있다.

**Go: 포워드 원장 cron 정체 진단 (관측 축).** mega_trend·목표하향은 본 Go 결과가 나올 때까지 **보류** — 병렬 착수 금지(규칙6).

---

## [Ops-lite] FWD-LEDGER-CRON-01 — 포워드 원장 cron 정체 진단 (SRV-02 후속 Go, 2026-08-17)

| 항목 | 값 |
|------|-----|
| **sub-phase** | FWD-LEDGER-CRON-01 (신규) |
| **발행** | Claude Pro Architect |
| **전제** | SRV-02 Claude OK · 근처놓침 레버(BULL/SIDE/BEAR/C-1) 전원 소진·동결 유지 · RP-1 v2.3.3 baseline 불변 |
| **위험도** | 🟢 Ops-lite — 진단 전용, 코드/config_kv 변경 0 |

### 스코프

1. VPS cron/systemd timer 목록 — `dual_north_star_ledger` 일일 갱신 잡 등록 여부·마지막 실행 timestamp·exit code
2. 최근 실행 로그(존재 시) — 2026-08-11 이후 실행 시도 자체가 있었는지, 있었다면 실패 원인(에러 스택 요약)
3. `forward_trades_count=0` 원인 분리 — 페이퍼/라이브 매매 엔진이 신호를 내고 있는지(로그·DB **조회만**, 재실행 아님) vs 신호는 있으나 ledger 기록 단계 실패인지
4. 원인 3분류 중 확정: (a) cron 미등록/미실행 (b) cron 실행되나 매매신호 0건 (c) 신호 있으나 ledger write 실패

### 금지

- cron 재시작·재등록·코드 수정·config_kv 변경 — 원인 확정 전까지 **조회만**
- BULL/SIDE/BEAR/C-1 근처놓침 레버 재접촉 (규칙1, 동결 유지)
- mega_trend 설계/구현 착수 금지 — 별도 Handoff 전까지 보류
- CAGR/MDD 목표 수치 변경 논의는 본 Go 범위 아님 — 디렉터 결정 대기 사안

### 완료기준 (DoD)

| # | 기준 |
|---|------|
| 1 | cron/timer 상태 표 (등록여부·마지막 실행·exit code) |
| 2 | 실행 로그 유무 및 에러 원인(있으면 요약) |
| 3 | 원인 3분류 중 확정 1개 + 근거 |
| 4 | OUTBOX 코드 diff 0 명시 |

### Timebox

1일 (Ops-lite 진단). 초과 시 미달 상태 그대로 OUTBOX — 막힌 지점만 명시하고 원인 강행 확정 금지.

### Cursor 지시

1. 새 세션 1개, 조사 모드 — VPS SSH 조회만, 코드 작성·잡 재시작 금지
2. 결과는 `CURSOR_TO_CLAUDE.md` 최상단 OUTBOX, `status: WAIT_CLAUDE_OK`

---

## Claude VERDICT — C-1-REDUCED 1단계 진단 OK · SECTOR_LEVER_INVALID · 2단계 No-Go (2026-08-17)

> **랜딩**: 디렉터 채팅 중계 · Cursor가 본 파일 최상단 동기화 (2026-08-17).  
> **앵커**: `SYNC-2026-08-17-S` (요청문 `-08-11-B` 아님 — §3 최신 우선).

### OUTBOX 검증 (`CURSOR_TO_CLAUDE.md` C-1-REDUCED DoD 5항)

| DoD | 결과 |
|-----|------|
| 5구간 A/B period_ret 표 | ✅ |
| mdd_B%(tier) ≤10% | ✅ 9.01~9.16 |
| n≥20 | ✅ 16,636~94,458 |
| §7 매핑 | ✅ |
| JSON+05+OUTBOX | ✅ |

**형식 DoD 통과.** Cursor overall **MIXED는 기각** → **SECTOR_LEVER_INVALID** 확정.

| 근거 | 내용 |
|------|------|
| SIDE Δ | +0.0494 / +0.0703pp = 잡음 (06 §C-1 No-Go: baseline 대비 명확한 개선 없음) |
| match_rate | 5.6~6.5% — 레버 희박 발화 |
| BEAR | match 0% 또는 match>0인데 period_ret 불변 → 레버 무효 |
| verdict 라벨 | 전부 NEAR_MISS 유지 — 탈출 실패 |

**VERDICT: OK (진단 종결).** C-1-REDUCED **2단계 실장 No-Go**. 근처놓침 탈출 4연속 소진 (BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1) — 규칙1상 이 라인 재접촉 금지.

### Cursor 질문 회신

물질성 미달 → **MIXED→INVALID**. 인정.

### 다음 Go

**SRV-02** (진단 전용, 코드 0) — 아래 Handoff.

---

## [MASTER] SRV-02 — RP-1-v1 근처놓침 레버 소진 확정 + 다음 트랙 결정 인풋 (진단 전용, 2026-08-17)

| 항목 | 값 |
|------|-----|
| **sub-phase** | SRV-02 (신규) |
| **발행** | Claude Pro Architect |
| **전제** | C-1-REDUCED = SECTOR_LEVER_INVALID (물질성 미달) 확정 · BULL/SIDE/BEAR/C-1 근처놓침 레버 전원 소진 |
| **위험도** | 🟢 진단 전용 — 코드/config_kv 변경 0 |

### SSOT (비접촉)

- BULL_03/05, SIDE_02/03, BEAR_01/03/04, C-1 판정 라벨 — 전부 동결, 재계산 금지
- config_kv, forward 경로, Phase A, S5 — 비접촉

### Spec — 조사 3항목 (읽기 전용)

1. **포워드 원장 스냅샷**: `dual_north_star_ledger.json` 트랙A 최신 — 경과일수·종합점수·G0/G1 근접도
2. **CAT-P mega_trend 인벤토리**: 관련 파일 존재 여부·최종 커밋일·구현 stage(설계만/부분코드/미착수). 신규 코드 작성 금지, 기존 파일 조회만
3. **근처놓침 최종 동결표**: `05_진행로그` 대조 — BULL_05·SIDE_02·SIDE_03·BEAR_01·03·04·C-1섹터 값 재확인만

### 금지

- 신규 코드·테스트·config_kv 작성 금지 (순수 조사)
- BULL/SIDE/BEAR/C-1 재접촉·재계산 금지 (전원 동결)
- mega_trend 설계/구현 **착수 금지** — 본 창은 조사만, 실 Go는 SRV-02 결과 받은 뒤 별도 Handoff

### DoD

| # | 기준 |
|---|------|
| 1 | 포워드 원장 표 (일수/종합점수/G0·G1 근접) |
| 2 | CAT-P 파일 인벤토리 또는 "관련 파일 없음" 명시 |
| 3 | 근처놓침 동결표 대조 확인 |
| 4 | OUTBOX (코드 diff 0) |

### Timebox

1일 (조사 전용). 초과 시 미달 그대로 OUTBOX.

### Cursor 지시

1. **새 세션** 1개만, 조사 모드 — 코드 작성 금지
2. 결과는 `CURSOR_TO_CLAUDE.md` 최상단 OUTBOX

---

## Claude VERDICT — S5-HARNESS-SCOPE-01 VPS 실측 Claude OK · Ops-lite 종료 (2026-08-17)

> **랜딩**: `Downloads/CLAUDE_TO_CURSOR_append (1).md` → Cursor가 본 파일 최상단 동기화 (2026-08-17).

### OUTBOX 검증 (`CURSOR_TO_CLAUDE.md` 최상단 "VPS 실측 Done" 대조)

| 체크 | Handoff 요구 (Ops-lite: 기존 CLI만 실행·코드 0줄) | OUTBOX 실측 | 판정 |
|---|---|---|---|
| 코드 diff | 0줄 | "코드 추가 diff: 0 (관측만)" | ✅ |
| write | 0 (관측만) | DoD#3 write 0 | ✅ |
| n 판정 | n=0 그대로 보고, Pass/Fail 금지(n<20 자동판정 금지) | KR/US n=0 · `numeric_judgment_omitted=true` · notes "표본 부족" | ✅ |
| short_pnl 컬럼 | Adapter 문구 유지 | `short_pnl_column_present=false` 명시 | ✅ |
| 05/OUTBOX 갱신 | 필수 | §S5-HARNESS-SCOPE-01 마지막 항목 갱신됨 | ✅ |

**VERDICT: OK.** S5-HARNESS-SCOPE-01 = 관측 인프라 구축 + VPS 실측 1회로 **종료**(부분 Done 아닌 완결 — 남은 건 향후 트리거뿐). 2026-08-17 window BEAR/HIGH_VOL∩S5 게이트 활성 0·체결 0은 "결과"가 아니라 "관측 가능함"의 확인. 재개 조건은 **VPS 원장 n>0 발생 시 동일 CLI 재실행**뿐 — 신규 코드·신규 Handoff 아님. `05_진행로그` §S5-HARNESS-SCOPE-01에 `Claude OK: 2026-08-17` 한 줄 추가.

### 앵커 불일치 참고

수신 프롬프트가 참조한 앵커 `SYNC-2026-08-11-B`는 `00_SESSION_SYNC.md` §3 현재값 `SYNC-2026-08-17-Q`보다 6일 오래됨. 규칙(§5): §3 최신 스냅샷 우선 — 본 판정은 §3 `SYNC-2026-08-17-Q` 기준으로 진행함. 다음 창 부팅 문구는 최신 앵커로 갱신 권장.

### 로드맵 위치 재확인

RP-1 내부 레버 3종 전부 소진: BULL-RECENCY-01(부분 Done) · SIDE-ALPHA-01(부분 Done, DoD 미달) · BEAR-S5-SIM-01(1단계 Done, 2단계 보류). SRV-01 후보 4개 중 3개 종결 → 남은 1개 **C-1-REDUCED**로 이동.

---

## [CAT-C] C-1-REDUCED — 섹터/스필오버 A/B 진단 (NEAR_MISS 5구간 한정) · 2026-08-17

| 항목 | 값 |
|------|-----|
| **sub-phase** | **C-1-REDUCED** (신규 Go) |
| **발행** | Claude Pro Architect |
| **전제** | RP-1 v2.3.3 baseline 확정 · BULL/SIDE/BEAR-S5 내부 레버 소진(위 VERDICT) · Lookahead v1 — Pass ≠ North Star CAGR 증명 |
| **SSOT 근거** | `14_레짐패널_15구간_목표검증.md` §6~7 원인분석트리("Near-miss + C-1 A/B 개선 → C-1 Handoff Go") |
| **CAT/위험도** | CAT-C 🟡 Medium(Claude↔Cursor 교차검증) — `CAT-MAP` §3: D try_add 내부·F Kelly 접촉 금지, 허용 인터페이스 `try_add_virtual_position(...)`/`sig_type`만 |

### 대상 구간 (NEAR_MISS 5개 한정 — 그 외 전 구간 비대상)

| ID | 현재 판정 | baseline 소스 | 주의 |
|----|-----------|----------------|------|
| SIDE_02 | NEAR_MISS(B) | `rp1_bull_recency_01_20260813.json` (8/13 SSOT) | SIDE_ALPHA_01_EXIT 레버 무효 확정·플래그 OFF — 8/13 수치가 현재 유효 baseline |
| SIDE_03 | NEAR_MISS(B) | 상동 | exit 레버 적용 시 FAIL 회귀했던 수치(`rp1_side_alpha_01_20260817.json`)는 **사용 금지** — 플래그 OFF 상태 수치만 |
| BEAR_01 | NEAR_MISS(B) | 8/13 JSON 우선, 없으면 VPS 재조회 | S5 미배선, 구조적 원인 |
| BEAR_03 | NEAR_MISS(B) | 상동 | 8/13 BEAR n은 BR01 shrink 오염 가능 — **1단계 착수 전 BEAR 3구간만 8/13 JSON 재확인**, 오염 확인되면 클린 재산출 1회 후 diag 진행 |
| BEAR_04 | NEAR_MISS(B) | 상동 | 상동 |

BULL_03(Done)·BULL_05(FAIL, KR레버 동결)·PASS 8구간은 **비대상 — 재실행 금지**.

### Spec — 1단계 (진단 전용, 필수 선행 · 유일 스코프)

- 5구간 한정 **A(현행, 섹터 미적용) vs B(섹터/스필오버 가산 적용)** 페어 백테스트
- B 변형: supernova 진입점수에 섹터 스필오버 가산항만 추가하는 **신규 sandbox 함수** — CAT-C 허용 인터페이스(`try_add_virtual_position(...)`, `sig_type`) 경유만, 스캐너 본체·D/E/F 로직 수정 금지
- Markov 차수·spillover lag: Cursor 재량으로 **값 1개만** 확정해 고정 — 그리드서치/다중 스윕 금지(과적합·lookahead 재발 방지)
- 측정 지표: `period_return_pct`(1순위, 규칙3) · `mdd_pct`(tier 기준, 규칙5) · PF · n

### 금지 (out-of-scope)

- BULL·PASS 8구간 재실행 금지(5구간 한정)
- CAT-D try_add 내부, CAT-F Kelly, CAT-E 청산엔진, Phase A 거버너(Kelly cap·MDD tier) 접촉 금지
- `config_kv` 라이브 반영 금지 — 섹터 가중치는 sandbox 상수로만
- S5/인버스 · BULL_03/05 bounds · SIDE exit 레버(`SIDE_ALPHA_01_EXIT`) 재접촉 금지(각각 동결 유지)
- **2단계(실제 반영) 자동 착수 금지** — 본 Handoff는 1단계까지만. 2단계는 진단 수렴 후 별도 Handoff
- 재현 루프 1회 초과 금지: baseline 재확인(BEAR) 포함 최대 1회 재산출, 이후 결과 그대로 OUTBOX

### 완료 기준 (DoD)

| # | 기준 |
|---|------|
| 1 | 5구간 A vs B `period_return_pct` 비교표 (연환산 CAGR 단독 판정 금지) |
| 2 | 5구간 모두 `mdd_pct` tier 기준 ≤10% 유지(raw 아님) — 위반 구간은 해당 B 즉시 기각 |
| 3 | 5구간 전부 total_trades≥20 (미만 구간은 숫자판정 없이 관찰만 기록) |
| 4 | 판정: `14_레짐패널` §7 매핑 기준 — "A/B 개선"→섹터 레버 유효 / "무효"→구조 아님(mega_trend 또는 목표 하향 Handoff 후보), 구간별 근거 1줄 |
| 5 | JSON(`reports/regime_panel/c1_reduced_diag_{date}.json`) + `05_진행로그` §C-1-REDUCED + `CURSOR_TO_CLAUDE.md` OUTBOX |

### Timebox

**1주** (BEAR baseline 재확인 포함 진단 전용, 조정 없음). 초과 또는 무결론 시 규칙1 준용(계측 버그헌팅 금지) — 미달 그대로 OUTBOX, 재시도 아님.

### Cursor 지시

1. **새 세션** — 본 Handoff 1개만(SIDE-ALPHA-01/BEAR-S5-SIM-01 재개 아님)
2. 1단계 진단만 착수 — 2단계(실장) 자동 착수 금지
3. Targeted diff only · CAT-MAP §3 인터페이스 준수 · 충돌 시 Adapter → 디렉터 Ask
4. BEAR 3구간은 8/13 JSON 오염 여부 먼저 확인 후 진행(§주의 컬럼)

### SRV 재확정 (기록)

| 항목 | 값 |
|------|-----|
| **Go** | C-1-REDUCED (1단계 진단) |
| **종료** | S5-HARNESS-SCOPE-01(Ops 관측 완결) |
| **소진** | BULL-RECENCY-01 · SIDE-ALPHA-01 · BEAR-S5-SIM-01 |
| **후순위** | 없음(SRV-01 후보 4개 전부 처리) — 다음 SRV는 C-1-REDUCED 결과 이후 재소집 |

---

## Claude VERDICT — S5-HARNESS-SCOPE-01 페이퍼 게이트 구현 OK · 로컬 Done (2026-08-17)


> **랜딩**: `Downloads/CLAUDE_TO_CURSOR_append.md` → Cursor가 본 파일 최상단 동기화 (2026-08-17).

### OUTBOX 검증 결과 (`CURSOR_TO_CLAUDE.md` DoD 6항 대조)

| # | 기준 | Cursor 주장 | Claude 판정 |
|---|------|-------------|-------------|
| 1 | write 0 | INSERT/UPDATE/CREATE/ALTER/config_kv 없음 | ✅ 인정 |
| 2 | 기존 시그니처 소비만 | `resolve_defense_arm_weight` 재정의 없음 | ✅ 인정 |
| 3 | 스모크 JSON 산출 | `s5_contribution_20260817.json` (n=0) | ⚠️ **조건부 인정** — 아래 참고 |
| 4 | n<20 무판정 플래그 | `sample_insufficient` + `표본 부족`, Pass/Fail/CAGR 없음 | ✅ 인정 (규칙4 충족) |
| 5 | 라이브 경로 비접촉 | 신규 파일만 | ✅ 인정 |
| 6 | 테스트+05+OUTBOX | 5 passed | ✅ 인정 |

### Cursor 질문 회신

1. **로컬 n=0 스모크를 DoD#3로 인정할지** → **인정한다.** 페이퍼 게이트의 목적은 판정이 아니라 파이프라인 존재 확인이므로 n=0도 유효한 산출물이다. 단, n=0은 "S5가 기여했다/안 했다"를 말해주지 않는다 — **VPS 원장 1회 산출을 별도 완료 조건으로 분리**한다(아래 Go). 이건 재시도가 아니라 같은 코드의 다음 관측 지점이다.
2. **`short_forward_trades` PnL Adapter 유지** → **OK.** 컬럼 강제 추가(스키마 왜곡) 대신 `short_pnl_column_present=false`로 정직하게 플래그한 접근이 규칙에 맞다. 앞으로 산출되는 모든 JSON에 이 플래그가 계속 노출되어야 한다(누락 시 Adapter가 조용히 사라진 것으로 간주해 재확인 요청).

### VERDICT

**OK.** S5-HARNESS-SCOPE-01 **로컬 구현 Done**(Pass/Fail 재판정 아님, 페이퍼 게이트 원칙 유지). Lookahead v1 전제상 이 산출물도 "상한선 추정"이 아니라 **관측 인프라**일 뿐 — North Star 판정과 무관.

| 항목 | 판정 |
|------|------|
| 코드/테스트 | Done |
| 실측(VPS) | **미착수 — 다음 Go** |
| sub-phase 상태 | S5-HARNESS-SCOPE-01 = 부분 Done (구현 Done, 관측 대기) |

---

## [CAT-C / Ops-lite] S5-HARNESS-SCOPE-01 — VPS 실측 기여 로그 산출 (SRV-01 Go, 2026-08-17)

| 항목 | 값 |
|------|-----|
| **sub-phase** | S5-HARNESS-SCOPE-01 (계속, 신규 ID 아님) |
| **발행** | Claude Pro Architect |
| **전제** | 위 VERDICT OK · 로컬 스모크(n=0) 인정 · RP-1 v2.3.3 baseline 재검증 불필요 |
| **위험도** | 🟢 Low — 코드 변경 0줄, 기존 CLI 실행 + 리포트만 |

### SSOT (변경/비변경)

- **변경**: 없음 — 신규 코드·함수·컬럼 **일체 금지**
- **실행**: 기존 `scripts/run_s5_defense_contribution_report.py`를 VPS 실측 DB(`forward_trades`/`short_forward_trades`) 대상으로 재실행
- **산출**: `reports/s5_defense/s5_contribution_{VPS실행일자}.json`
- **비접촉**: `forward/shared.py` · Kelly/게이트 판정식 · config_kv · bitget/ · 라이브 파이프라인 전체 · Phase A

### Spec

1. VPS SSH 가능 세션(OPS-01 키 문제 해결된 채널)에서 기존 CLI 그대로 재실행. `--start`는 BEAR/HIGH_VOL 게이트 활성 구간이 포함되는 날짜로(디렉터 확인).
2. n=0이 재현되면 그대로 보고 — "게이트 미활성" 사실 자체가 유효한 결과. 숫자 조작·추정 금지.
3. n≥20이면 `WR / avg_pnl / gate_active_minutes` 등 기존 필드만 채워서 보고 — **Pass/Fail/CAGR 라벨 삽입 금지**(페이퍼 게이트 원칙, 규칙2 Lookahead 전제 유지).
4. n이 1~19면 규칙4(n<20 자동 숫자 판정 금지)에 따라 `sample_insufficient` 플래그 유지, 숫자는 참고로만 노출.

### 금지 (out-of-scope)

- 코드 수정·리팩터 일체 (함수 시그니처, CLI 옵션 포함)
- config_kv 라이브 반영
- Pass/Fail/CAGR 판정 문구 삽입
- 새 sub-phase 착수 (BULL-RECENCY / SIDE-ALPHA / BEAR-S5-SIM / C-1 접촉 금지 — 전부 소진·동결 상태 유지)

### 완료 기준 (DoD)

| # | 기준 |
|---|------|
| 1 | VPS 실행 로그 + JSON 산출물 경로 명시 |
| 2 | 표본 n 실측값 그대로 보고 (임계 미달이어도 조작 없음) |
| 3 | write 0 재확인 (신규 INSERT/UPDATE/config_kv 없음) |
| 4 | `short_pnl_column_present` 플래그 유지 노출 |
| 5 | `05_진행로그` §S5-HARNESS-SCOPE-01 + `CURSOR_TO_CLAUDE` OUTBOX 갱신 |

### Timebox

2일 (VPS 접속·실행 1일 + 리포트 정리 1일). 초과 또는 SSH 불가 시 **미달 OUTBOX**(OPS-01 SSH 이슈와 동일 패턴으로 즉시 보고) — 재시도 아님.

### Cursor 지시

1. **새 세션 또는 Ops 세션** 1개 — 코드 세션 아님, 실행+리포트만
2. VPS 접속 불가 시 즉시 `WAIT_DIRECTOR`로 OUTBOX (구현 착수 금지)
3. 결과와 무관하게 정직 보고 — n=0이어도 실패가 아니라 관측 그 자체가 목적

---

## [CAT-J/Alpha-방어] S5-HARNESS-SCOPE-01 — 페이퍼 게이트 Go (SRV-lite, 2026-08-17)

> **랜딩**: 디렉터 채팅 중계 · Cursor가 본 파일 최상단 동기화 (2026-08-17). Claude 사본 읽기 전용.

| 항목 | 값 |
|------|-----|
| **sub-phase** | S5-HARNESS-SCOPE-01 (스코프 확정: 페이퍼 게이트) |
| **발행** | Claude Pro Architect |
| **전제** | BEAR-S5-SIM-01 1단계 Claude VERDICT(2026-08-17) — RP-1 내 S5 레버 없음(구조적) 확정. 이번 Handoff는 RP-1 **외부** 읽기 전용 리포트 |
| **위험도** | 🟢 Low — write 경로 없음(신규 테이블/컬럼/주문/config_kv 없음), 전부 기존 SSOT read-only |

### VERDICT (스코프 3안)

| 안 | 판정 | 이유 |
|----|------|------|
| 태그 리플레이 | **기각** | `s5_arm_active` 게이트 차단 아님 이미 확정 · 리플레이해도 PnL 증거 0 · 비용 대비 가치 없음 |
| 풀 슬리브 | **기각** | inverse/blackhole 전용 historical 엔진 = lookahead·고비용 · SRV-lite 초과 |
| **페이퍼 게이트** | **채택** | 순수 read-only 집계 · 신규 주문/테이블/config 없음 · Lookahead 없음 · 🟢 Low |

### SSOT (변경/비변경)

- **신규(read-only 산출물만)**: 리포트 생성 함수 1개 + JSON 출력. 배치 파일은 Cursor 판단(CAT-J 성격)
- **참조(읽기전용, 재정의 금지)**: `forward_trades`(CAT-D, sig_type INVERSE_ETF/BLACKHOLE 필터), `short_forward_trades`(CAT-C blackhole, CAT-MAP §2 Single Writer), `regime_key`/`REGIME_ENSEMBLE`(CAT-G), `s5_arm_active` 판정 — 기존 `resolve_defense_arm_weight`/`ACTION_BY_REGIME` 시그니처 그대로 소비
- **비접촉**: 진입/청산/Kelly/게이트 판정식 전체, config_kv 라이브, `forward/shared.py` 실행경로(신규 hook 없음 — shared.py조차 안 건드림), RP-1/regime_panel_rp1, BULL/SIDE/BEAR 전 sub-phase, Phase A

### 변경 Spec

- 함수(신규, read-only): `compute_s5_defense_contribution_log(start_ts, end_ts, *, market: str | None = None) -> dict`
  - `forward_trades` ∩ `short_forward_trades`를 `regime_key ∈ {BEAR, HIGH_VOL}` **AND** `s5_arm_active=True`(게이트 활성) 구간으로 필터
  - window별: `gate_active_minutes`, `s5_trade_count`, `realized_pnl_sum`, `contributed(bool)` 산출
  - `s5_trade_count < 20`인 window는 **숫자 판정 문구 없이 "표본 부족" 플래그만**(규칙4 — 판정 자동화 금지)
- 산출물: `reports/s5_defense/s5_contribution_{date}.json`
- KR/US: 공통 함수, `market` 파라미터 분기(규칙8 하드코딩 금지)

### 인접 CAT 영향

| CAT | 영향 | Critical |
|-----|------|----------|
| CAT-D | read-only (`forward_trades`) | 🟢 |
| CAT-C | read-only (`short_forward_trades`) | 🟢 |
| CAT-G | read-only (`regime_key`) | 🟢 |
| CAT-F | read-only (`s5_arm_active`/`ACTION_BY_REGIME`) | 🟢 |

전부 read → CAT-MAP §5 🟢 Low — 디렉터 Critical 승인 불필요.

### 금지 (out-of-scope)

- 신규 주문경로·$0 페이퍼 entry hook(F-RETIRE-02 패턴 재사용 아님 — 그보다 더 가벼운 read-only만)
- 태그 리플레이·풀 슬리브 착수 금지
- `s5_arm_active`/게이트 판정식·`resolve_defense_arm_weight` 로직 변경 금지 — 소비만
- config_kv 라이브 반영 금지
- period_return_pct/CAGR 판정 문구 삽입 금지 — **기여 로그(존재 여부·PnL 부호)까지만**, Pass/Fail 재판정은 별도 Handoff

### 완료 기준 (DoD)

| # | 기준 |
|---|------|
| 1 | `compute_s5_defense_contribution_log` — write 0건(신규 테이블/컬럼/config 없음) 정적 확인 |
| 2 | `s5_arm_active` 판정은 기존 함수 시그니처 그대로 호출(재정의 아님) 코드 리뷰 확인 |
| 3 | post_bear_underdog_01 phase 배포 이후(2026-08-17~) 구간 최소 1회 실제 JSON 산출(스모크) |
| 4 | n<20 window는 판정 문구 없이 플래그만 — 규칙4 회귀 테스트 |
| 5 | 라이브 파이프라인(진입/청산/Kelly/게이트) diff 0 — `git diff` 범위가 신규 read-only 파일/함수로 한정 |
| 6 | `tests/test_s5_defense_contribution_report.py` + `05_진행로그` §S5-HARNESS-SCOPE-01 + `CURSOR_TO_CLAUDE` OUTBOX |

### Timebox

3일(구현 1일 + 스모크 1회 산출 1일 + 여유 1일). 초과 시 미완 OUTBOX, 재시도 아님.

### 병렬

OPS-01 관측 cron과 병렬 가능(read-only). BULL/SIDE/BEAR-S5-SIM 전부 동결.

### Cursor 지시

1. **새 세션 — 본 Handoff 1개만**
2. Targeted diff only. 신규 파일/함수 추가 형태 — 기존 파일 rewrite 금지
3. 충돌(스키마·필드 부재 등) 시 Adapter 제안 후 디렉터 Ask
4. 테스트: `tests/test_s5_defense_contribution_report.py` — write-0 · n<20 플래그 · 기존 게이트 함수 mock 소비

### 롤백 조건

산출 스크립트/함수 삭제만으로 완전 롤백 — read-only이므로 라이브 영향 없음.

### 디렉터 3줄

1. 3안 중 **페이퍼 게이트만** SRV-lite 부합 — 태그/풀슬리브 이번 세션 기각.
2. 신규 주문·config_kv 없음 · 🟢 Low — Critical 승인 불필요.
3. 완료돼도 Pass/Fail 재판정 아님 — S5 기여 로그 공백만 메우는 1차 관측 인프라.

---

## Claude VERDICT — BEAR-S5-SIM-01 1단계 Done · 2단계 보류 (2026-08-17)

> **랜딩**: Claude 사본은 읽기 전용 → 디렉터가 채팅으로 전달 · Cursor가 본 파일 최상단에 동기화 (2026-08-17).

### OUTBOX 질문 회신

| # | 질문 | 판정 |
|---|------|------|
| 1 | 표A 프록시 인정? | **인정.** BULL_01 n=97,009 v2.3.3 aggregate bit-match · BR01 OFF · BEAR 비접촉 확인. 원본 `rp1_20260811.json` 확보 시 재검증 조건부 |
| 2 | DoD#1 처리? | **스키마 예외로 1단계 종결.** VPS matrix는 1단계 잔여 아님 — exit_type/KR·US 분해는 2단계 스코핑 선행입력으로 이월 |
| 3 | Go/보류/다른축? | **보류.** 원인 B·S5 미기여 결론 수용, `15_POST_RP1` 우선순위3 권고와 일치하나 **이번 창 자동 착수 금지** |

### 종료 판정

| 항목 | 값 |
|------|-----|
| **sub-phase** | BEAR-S5-SIM-01 = **1단계 Done** · **2단계 미착수** |
| **RP-1 내 레버** | **없음** 확정 |
| **신규 구현 Handoff** | **미발행** (Go 1개 = 없음) — **후속**: 아래가 아니라 **위** S5-HARNESS-SCOPE-01 Handoff (2026-08-17) |
| **RP-1 내 sub-phase** | BULL · SIDE · BEAR **전부 소진** (확인만) |

### 다음 Go 후보 (디렉터 결정 필요)

| 후보 | 내용 |
|------|------|
| S5-HARNESS-SCOPE-01 (신규 SRV-lite) | 태그리플레이 / 풀슬리브 / 페이퍼게이트 스코프 확정 |
| OPS-01 배포 진행 | BEAR-UNDERDOG-01 · L-OBS-02 · F-GATE/F-RETIRE |

### 디렉터 3줄

1. BEAR-S5-SIM-01 1단계 Done — Cursor 진단(원인 B·S5 미배선) 수용, RP-1 내 레버 없음 구조적 확정.
2. 2단계(S5 하네스)는 방향만 합의, 이번 창에서 착수 안 함 — 스코프 확정은 별도 세션.
3. `05_진행로그` §BEAR-S5-SIM-01 · `00_SESSION_SYNC` §3 · `NEXT_ACTION` 갱신은 Cursor 몫.

### Cursor 액션 1줄

`CLAUDE_TO_CURSOR.md` 최상단 VERDICT 확인 후 `05_진행로그` / `00_SESSION_SYNC` / `NEXT_ACTION`만 갱신 — 코드·rerun·2단계 착수 금지.

---

## Claude VERDICT — SIDE-ALPHA-01 2단계 DoD 미달 확정 · 부분 Done 종료 (2026-08-17)

> **앵커**: 프롬프트 `SYNC-2026-08-11-B` ≠ 파일 `SYNC-2026-08-17-D` → **본 폴더 최신 승** (규칙 §5-2). 살아있는 OUTBOX = SIDE-ALPHA-01 2단계 DoD 미달.

### Cursor 질문 대조

| # | 질문 | 판정 |
|---|------|------|
| ① | DoD 미달 = 종료조건? | **인정.** SIDE_03: PF 0.905→0.896(횡보 FAIL 임계 PF<0.9) + period_ret +6.07%→+4.26%(규칙3 우선지표 하락) + avg_pnl 동반악화. DoD#1 하락회귀금지 위반. rerun 1회 소진 → **2단계 종료·재시도 안 함** |
| ② | 다음 레버 | **동결/후순위.** 최소보유 확대 = 추가 15구간 rerun 필요 = 2회차 blind → 규칙 위반. SIDE-ALPHA-01 = **부분 Done** (BULL_05 KR레버와 동일: 코드 삭제 금지·동결·재시도 안 함) |
| ③ | BULL_03 DoD#2 오염 | **레버 무관 제외 인정.** BR01 미적용 matrix(n=40,657 fallthrough) = BULL window 사전 결함 · 규칙1 대상 · 재조사 지시 없음 |

### sub-phase 종료 기록

| 항목 | 값 |
|------|-----|
| **sub-phase** | SIDE-ALPHA-01 = **부분 Done** |
| **SIDE_02** | NEAR_MISS(B) 유지 |
| **SIDE_03** | FAIL(B) 잔존 **동결** |
| **플래그** | `SIDE_ALPHA_01_EXIT` 기본값 **OFF** 유지 (라이브 미반영) |
| **최소보유 레버** | 미착수 · 후순위 보관만 |

로드맵 우선순위 2 종료 → **우선순위 3 BEAR-S5-SIM-01** 로 이동.

---

## [CAT-C / Alpha] BEAR-S5-SIM-01 — BEAR ×3 NEAR_MISS 원인 진단 (SRV Go, 2026-08-17)

| 항목 | 값 |
|------|-----|
| **sub-phase** | **BEAR-S5-SIM-01** (신규, 1단계 진단만) |
| **발행** | Claude Pro Architect |
| **전제** | SIDE-ALPHA-01 2단계 DoD 미달 확정·부분 Done 종료(위 VERDICT) · BULL-RECENCY-01 부분 Done · RP-1 v2.3.3 baseline(`rp1_20260811.json`) BEAR 2P/3NM |
| **SSOT 근거** | `rp1_20260811.json` BEAR ×5 중 NEAR_MISS 3구간 (신규 rerun 없이 기존 JSON에서 추출) |
| **위험도** | 🟢 — read-only 진단, 코드 변경 없음, S1/S5/Phase A 비접촉 |

### Spec — 1단계 (진단, 필수 선행 — 이번 Handoff 범위는 여기까지만)

1. `rp1_20260811.json`에서 BEAR ×5 중 NEAR_MISS 3구간 식별: ID·기간·n·WR·avg_pnl·PF·period_return_pct·mdd_tier_pct
2. 구간별 trade-level breakdown: exit_type(TP/SL/TIME) 비중·평균 보유기간·KR/US avg 분리 (BULL-RECENCY·SIDE-ALPHA 1단계와 동일 절차)
3. **S5 방어 커버리지**: 해당 구간에서 인버스/블랙홀 태그 시그널이 실제 발동했는지 vs 게이트 차단인지 (하락 bucket Pass 기준 = "손실구간 S5 기여 로그")
4. 원인분석트리 분류: A(신호부족)/B(수익부족)/C(MDD초과) + S5 기여/미기여 별도 표기
5. 2단계 실행 가능성 판단: RP-1 하네스 내 파라미터 조정으로 되는지, 별도 S5 시뮬 하네스가 필요한지("RP-1 외" 여부) — 1단계 결과에 명시

### 금지 (out-of-scope)

- 코드 변경 전면 금지 — 이번 Handoff는 **진단만**
- 15구간 rerun 금지 (기존 JSON 재사용만)
- BULL/SIDEWAYS/Phase A/C-1/config_kv 라이브 접촉 금지
- SIDE-ALPHA-01 재개 금지 (동결 확정 — 위 VERDICT)

### 완료 기준 (DoD)

| # | 기준 |
|---|------|
| 1 | BEAR 3구간 trade-level breakdown 표 완성 |
| 2 | 구간별 S5 발동/미발동 + 근거 명시 |
| 3 | 공통원인 vs 개별원인 구분 |
| 4 | 2단계 단일 레버 후보 제시 또는 "레버 없음—구조적 한계" 결론 + 실행가능성(RP-1 내/외) |
| 5 | 전 구간 n≥20 확인 (n<20 시 자동 숫자판정 금지, 정성 기술만) |
| 6 | `05_진행로그` §BEAR-S5-SIM-01 + `CURSOR_TO_CLAUDE.md` OUTBOX (진단만, 채팅 금지) |

### Timebox

3일 (read-only, 코드 없음). 2단계 착수는 **이번 Handoff에 포함 안 됨** — 진단 수렴 후 별도 Claude 판정 필요(SIDE-ALPHA-01처럼 자동 진행 불가, 시뮬 스코프 불확실성 때문).

### 병렬

OPS-01 배포 트랙과 병렬 가능 (파일 충돌 없음).

### Cursor 지시

1. 새 세션 — 본 Handoff 1개만
2. `rp1_20260811.json` 재사용, 신규 rerun 금지
3. 결과는 `CURSOR_TO_CLAUDE.md` OUTBOX에만, 채팅 요약 금지

### SRV 재확정 (기록)

| 항목 | 값 |
|------|-----|
| **Go** | BEAR-S5-SIM-01 (1단계 진단) |
| **종료(부분 Done)** | SIDE-ALPHA-01 (SIDE_02 NEAR 유지 · SIDE_03 FAIL 동결, 코드 삭제 금지·재시도 금지) |
| **후순위** | C-1-REDUCED · SIDE-ALPHA-01 최소보유 레버(미착수, 보관만) |

### 디렉터 3줄

1. SIDE-ALPHA-01 2단계 DoD 미달 확정 — SIDE_03 FAIL 회귀(PF 0.896, period_ret도 하락), rerun 재시도 안 함.
2. sub-phase 동결(부분 Done) — SIDE_02 NEAR 유지 기록, SIDE_03/최소보유 레버는 코드 착수 없이 보관.
3. 다음 Go = BEAR-S5-SIM-01 1단계(진단 전용, read-only, 코드 없음) — 본 Handoff 참조.

### Cursor 액션 1줄

새 세션에서 본 Handoff(BEAR-S5-SIM-01 1단계) 읽고 `rp1_20260811.json` 기반 read-only 진단만 착수 — 코드 변경·rerun 금지.

---

## Claude VERDICT — SIDE-ALPHA-01 1단계 진단 OK + 2단계 Go(CAT-E) (2026-08-17)

### 1단계 완료기준 대조 (Handoff Spec 1단계 vs OUTBOX A+B)

| Spec 요구 | 충족 | 비고 |
|---|---|---|
| WR·avg_pnl | ✅ | 표A 5구간(SIDE_01~05) |
| exit_type 분포(TP/SL/TIME) | ✅ | SL 62.3/66.8 vs PASS 54.8–58.0 스파이크 확인 |
| 평균 보유기간 | △ proxy | 실측 없음 · exit_type로 대체(명시적 caveat 기재) — 결론 안정적, 재요청 불필요 |
| 공통원인 vs 개별원인 | ✅ | 공통 B(SL-heavy edge compression) · 개별(SIDE_02 KR드래그 / SIDE_03 양시장붕괴) 분리 |
| 가설 (i)(ii)(iii) | ✅ | (i)지지 (ii)레버로 기각 (iii)보조지지 |
| 규칙3·4 준수 | ✅ | n 20935/24167 ≫20 · period_ret 우선 명시 |

**VERDICT: 충족.** 보유기간 proxy 대체만으로는 원인 판정을 흔들지 않음 — 1단계 재진단 불필요.

### Go — 2단계 단일 레버: **CAT-E SIDEWAYS exit**

S1 알파 임계값 기각 이유: PASS·NEAR 전 구간 CLUSTER_1 top1 share 100%·Jaccard 1.0 — 진입 신호는 PASS와 동일. 문제 축은 청산(SL 62–67% vs 55–58%, TIME 16–23% vs 22–32%)에 있음 — 진단이 가리키는 축과 알파 임계값 축 불일치. CAT-E가 원인(B)에 직접 대응하는 유일 레버.

---

## [CAT-C / Alpha] SIDE-ALPHA-01 2단계 — CAT-E SIDEWAYS exit 단일 레버 (Go, 2026-08-17)

| 항목 | 값 |
|---|---|
| **sub-phase** | SIDE-ALPHA-01 (계속) — 2단계 |
| **전제** | 1단계 진단 Claude OK(위) · 공통원인 B(SL-heavy edge compression) 확정 |
| **레버** | CAT-E SIDEWAYS 스코프 청산 엔진 — SIDE_02/03(SIDEWAYS 버킷 한정) SL 트리거 완화 **또는** 최소보유 확대 중 **단일 파라미터**만. 둘 동시 조정 금지, Cursor 재량으로 택1 |
| **비접촉** | CLUSTER_1 진입 템플릿/bounds · S1 알파 임계값 · BULL/BEAR/타 레짐 exit · Phase A · config_kv 라이브 |

### Spec

- CAT-E exit 파라미터(TP/SL/TIME) 중 SIDEWAYS 버킷에만 적용되는 단일 값 조정
- 목적: SIDE_02/03 SL%를 PASS 기준선(54.8–58.0%) 근접까지 낮추거나 TIME% 확대로 조기컷 완화
- 조정 후 **15구간 전체 rerun 1회**

### 금지

- 2개 이상 파라미터 동시 조정 (단일 레버 원칙)
- rerun 2회차 (baseline bit-identical/목표 미달 시 즉시 OUTBOX, 재시도 금지)
- BULL_03(NEAR_MISS)·BULL_05(FAIL) 재접촉
- S1 알파 임계값/CLUSTER_1 bounds 접촉

### 완료기준 (DoD) — 원 Handoff #1–5 그대로 적용

| # | 기준 |
|---|------|
| 1 | SIDE_02·03 재판정 ≥ NEAR_MISS (하락 회귀 금지) — **period_return_pct 우선**(규칙3, 아래 해석 참조) |
| 2 | 나머지 13구간 verdict 불변 (BULL_03 NEAR_MISS·BULL_05 FAIL 포함) |
| 3 | mdd_pct(tier) ≤10% · mdd_crosscheck=MDD_OK |
| 4 | 전 구간 total_trades≥20 |
| 5 | JSON + `05_진행로그` §SIDE-ALPHA-01 + `CURSOR_TO_CLAUDE` OUTBOX |

### SIDE_03 period_ret(+)/avg_pnl(−) 해석 — DoD#1 판정 방법

period_return_pct가 규칙3상 판정 SSOT이므로 SIDE_03은 현재도 NEAR_MISS 조건(+6.07%)을 만족하지만, avg_pnl 음수는 일평균/쿼터 시퀀싱 효과로 부풀려진 결과일 가능성을 내포. 2단계 rerun 후 period_ret이 NEAR_MISS를 유지해도 avg_pnl이 여전히 음수면 "회복"이 아니라 "우연한 시퀀싱"으로 간주 — **period_ret 통과는 필요조건이나 avg_pnl 동반개선 없이는 DoD#1을 조건부(잠정) 충족**으로만 처리, Claude 재확인 요청.

### Timebox

원 Handoff 1주 중 잔여분 — 조정·rerun 최대 1회 3일 + 여유 2일 (진단 2일 기완료, 총 timebox 불변).

### 병렬

OPS-01과 병렬 가능 (쓰기 파일 충돌 없음).

### Cursor 지시

1. 동일 세션 유지 가능(1단계 Handoff 연속) — 별도 새 Go 불필요
2. CAT-E 단일 파라미터 조정 → rerun 1회 → OUTBOX
3. 재현 루프 금지 엄수 (2회차 blind 금지, 미달 시 즉시 보고)

### 디렉터 3줄

1. SIDE-ALPHA-01 1단계 진단 OK — 원인 확정(SL-heavy edge compression), 재진단 불필요.
2. 2단계 레버 = CAT-E SIDEWAYS exit 단일 조정 (S1 알파 임계값은 기각 — 진입신호가 PASS와 동일해 무관한 축).
3. SIDE_03 period_ret(+)는 판정엔 유효하나 avg_pnl(−) 미개선 시 "우연한 시퀀싱"으로 보고 조건부 처리.

### Cursor 액션 1줄

CAT-E SIDEWAYS exit 단일 파라미터(SL 완화 또는 최소보유 확대 중 택1) 조정 → 15구간 rerun 1회 → OUTBOX.

---

## Claude VERDICT — BULL-RECENCY-01 8/13 SSOT 확정 · 부분 Done 종료 (2026-08-17)

### OUTBOX 검증 결과 (`rp1_bull_recency_01_20260813.json` 실측 대조)

| 항목 | OUTBOX 주장 | JSON 실측 | 일치 |
|------|-------------|-----------|------|
| generated_at | 2026-08-13 01:26 | 2026-08-13 01:26:26 | ✅ |
| schema | v2.3.4 | regime_panel_rp1.v2.3.4 | ✅ |
| BULL_03 | NEAR_MISS · n=10,276 · period_ret 15.3971% | NEAR_MISS(B) · n=10276 · 15.3971% | ✅ |
| BULL_05 | FAIL · n=9,142 · period_ret −9.0378% | FAIL(B) · n=9142 · −9.0378% | ✅ |
| mdd_crosscheck | MDD_OK | MDD_OK, violating_regimes=[] | ✅ |
| n≥20 (규칙4) | — | 전 구간 min n=1,899 (BEAR_05) | ✅ |
| 15구간 완전성 | — | 15/15 periods 존재 | ✅ |

**VERDICT: OK.** 8/13 JSON을 BULL-RECENCY-01 **iter2 DoD SSOT**로 공식 확정. 재현 full 중지 — 8/14~16 런은 폐기 유지(baseline bit-identical 또는 매칭 붕괴, Cursor 엔지니어 결론 수용).

### 종료 판정 (부분 Done)

| 항목 | 판정 |
|------|------|
| BULL_03 | FAIL→NEAR_MISS(B) — 목표 달성, S1 CLUSTER_1 타이트닝 유효. **Done** |
| BULL_05 | FAIL(B) 잔존. iter3 KR 레버(dyn_rs)는 유효 full 미검증 → **동결 보관**(코드 삭제 금지, 재시도 안 함) |
| 재현 경로 | scope ON→매칭 붕괴 · scope OFF→baseline fallthrough — 규칙1(계측 버그헌팅 종료) 대상 |
| **sub-phase** | BULL-RECENCY-01 = **부분 Done** (2/2 진단 완료, 1/2 목표 달성) |

로드맵 우선순위 1 종료 → **우선순위 2 SIDE-ALPHA-01** 로 이동.

---

## [CAT-C / Alpha] SIDE-ALPHA-01 — SIDE_02/03 NEAR_MISS(B) PF 개선 (SRV-01 Go, 2026-08-17)

| 항목 | 값 |
|------|-----|
| **sub-phase** | **SIDE-ALPHA-01** (신규) |
| **발행** | Claude Pro Architect |
| **전제** | RP-1 v2.3.3 baseline 확정 · BULL-RECENCY-01 부분 Done 종료(위 VERDICT) · 8/13 JSON = 대조 baseline |
| **SSOT 근거** | `rp1_bull_recency_01_20260813.json` SIDE_02/03 실측 |
| **위험도** | 🟡 — SIDEWAYS bucket 알파/청산층만 · BULL/BEAR/Phase A 비접촉 · config_kv 라이브 금지 |

### 대상 구간 실측 (8/13 JSON, 변경 전 baseline)

| ID | 기간 | n | WR% | PF | avg_pnl | period_ret% | mdd_tier% | verdict |
|----|------|---|-----|-----|---------|-------------|-----------|---------|
| SIDE_02_2015횡보 | 2015-06-01~2016-06-30 | 20,935 | 33.58 | 0.989 | −0.0249 | −9.0469 | 9.0469 | NEAR_MISS(B) |
| SIDE_03_2021-22혼조 | 2021-12-01~2022-12-31 | 24,167 | 29.56 | 0.905 | −0.2275 | 6.0714 | 9.1296 | NEAR_MISS(B) |

두 구간 모두 n≫20 · avg_pnl 음수 · PF가 SIDEWAYS 기준선(≥1.0) 근접 미달 — **원인분석트리 B** (신호는 있으나 수익 부족). A(신호부족)·C(MDD초과) 아님.

### SSOT (변경/비변경)

- **변경 후보 (2단계, 진단 확정 후만)**: 청산 엔진(TP/SL/TIME, CAT-E) 또는 S1 알파 임계값 — **선(先)변경 금지**
- **비접촉**: BULL 전 구간(BULL_03/05 freeze) · BEAR · S5/인버스 · C-1 · Phase A · config_kv 라이브

### Spec — 1단계 (진단, 필수 선행)

SIDE_02·SIDE_03 trade-level breakdown:

- WR · avg_pnl · `exit_type` 분포(TP/SL/TIME 비중) · 평균 보유기간
- 공통원인 vs 구간별 개별원인 구분 (BULL-RECENCY와 동일 절차)
- 가설: (i) 청산엔진 손익비 (ii) 특정 템플릿 과다매칭 (iii) 횡보장 회전율/슬리피지

### Spec — 2단계 (조정, 진단 수렴 후만)

- 단일 레버로 수렴할 때만 targeted diff
- 15구간 전체 rerun **최대 1회**로 검증

### 금지 (out-of-scope)

- BULL_03/05 bounds 재접촉 금지 (8/13 동결)
- BEAR·C-1·S5·Phase A 접촉 금지
- config_kv 라이브 반영 금지
- **재현 루프 금지**: 2단계 fix→rerun **최대 1회**. baseline bit-identical 또는 목표 미달 → 즉시 OUTBOX → Claude 판단 (2회차 blind 금지)

### 완료 기준 (DoD)

| # | 기준 |
|---|------|
| 1 | SIDE_02, SIDE_03 재판정 ≥ NEAR_MISS (하락 회귀 금지) — period_return_pct 우선 |
| 2 | 나머지 13구간 verdict 불변 (BULL_03 NEAR_MISS · BULL_05 FAIL 포함 8/13 유지) |
| 3 | mdd_pct_tier ≤10% · mdd_crosscheck=MDD_OK |
| 4 | 전 구간 total_trades≥20 |
| 5 | JSON + `05_진행로그` §SIDE-ALPHA-01 + `CURSOR_TO_CLAUDE` OUTBOX (진단+조정 통합) |

### Timebox

1주 (진단 2일 + 조정·rerun 최대 1회 3일 + 여유 2일). 초과 시 미달 OUTBOX, 재시도 아님.

### 병렬

OPS-01과 병렬 가능 (쓰기 파일 충돌 없음).

### Cursor 지시

1. **새 세션** — 본 Handoff **1개만**
2. **1단계 진단**부터 착수 · 2단계는 진단 확정 후
3. Targeted diff only · 충돌 시 Adapter → 디렉터 Ask
4. 재현 루프 금지 엄수 (BULL-RECENCY 반복 방지 = 프로세스 목적)

### SRV 재확정 (기록)

| 항목 | 값 |
|------|-----|
| **Go** | SIDE-ALPHA-01 |
| **종료(부분 Done)** | BULL-RECENCY-01 |
| **후순위** | BEAR-S5-SIM-01 · C-1-REDUCED |

---

## [CAT-C / Alpha] BULL-RECENCY-01 이터레이션 3 — BULL_05 KR 레버 + DoD 버그픽스 (2026-08-13) *(superseded — KR 레버 동결)*


| 항목 | 값 |
|------|-----|
| **sub-phase** | BULL-RECENCY-01 (계속, 신규 ID 아님) |
| **전제** | shrink=0.45 CLUSTER_1 바운드 패치 결과 확정 — BULL_03 NEAR_MISS(B) 달성, BULL_05 FAIL(B) 잔존 |
| **결론** | shrink 추가 튜닝 기각 — BULL_05 KR-specific 레버로 전환 |

### SSOT (변경/비변경)

- **변경**: BULL_05 KR 시장 한정 `dyn_rs` floor on binding template `260628`
- **비접촉**: shrink=0.45 bounds_after 동결, BULL_03, Phase A, SIDE/BEAR, S5, C-1

### Spec — in-scope

1. BULL_05 KR 레버 (market-conditional gate)
2. DoD 체커 `regime_name` 매칭 버그픽스

### DoD (iter 3)

| # | 기준 |
|---|------|
| 1 | BULL_05 ≥ NEAR_MISS |
| 2 | BULL_03 NEAR_MISS+ 유지 |
| 3 | 나머지 13구간 verdict 불변 |
| 4–6 | MDD_OK · n≥20 · DoD 스크립트 정상 |

### 금지

- shrink/tb/bbe 추가 조정 · BULL_03 bounds 재수정

---

## Claude 판정 — BULL-RECENCY-01 1단계 충족 (2026-08-11)

**VERDICT: 충족.** A+B(trade-level breakdown + 공통/개별원인)가 1단계 스펙 충족.

**S1 2단계 범위** = **CLUSTER_1 bounds 타이트닝 (targeted)** — **전역 DNA threshold 변경 금지** (13구간 회귀 방지, targeted diff 원칙).

**BULL_05**: KR 분기 선제 **아님** — 동일 패치 묶음 15구간 rerun 먼저.

**Cursor**: 별도 Go 문구 없이 2단계(CLUSTER_1 타이트닝) + 15구간 전체 rerun **자체 진행 가능** (ADDENDUM 조항).

---

## [ADDENDUM] BULL-RECENCY-01 — 회신 채널 확정 (2026-08-11)

| 항목 | 내용 |
|------|------|
| **Cursor → Claude** | `CURSOR_TO_CLAUDE.md` **최상단 OUTBOX만** · 채팅/텔레그램/별도 파일 **금지** |
| **필수 포함** | BULL_03/05 trade-level breakdown + 공통원인 vs 개별원인 결론 |
| **Claude 판독** | 다음 창에서 OUTBOX 최상단만 · 채팅 요약 불신뢰 |
| **병행 갱신** | `05_진행로그` · `00_SESSION_SYNC` §3 · `NEXT_ACTION` (파일) |
| **2단계** | OUTBOX 1단계 완료기준 **충족** 판정 시 Cursor **동일 sub-phase 내 자체 진행** (별도 Go 문구 불필요) |

---

## [CAT-C / Alpha] BULL-RECENCY-01 — BULL FAIL 구간 recency drift 진단·S1 조정 (SRV-01 Go, 2026-08-11)

| 항목 | 값 |
|------|-----|
| **sub-phase** | BULL-RECENCY-01 |
| **발행** | Claude Pro Architect |
| **전제** | RP-1 v2.3.3 baseline 확정 — tier replay 계측 재검증 불필요 |
| **Handoff** | 디렉터 채팅 수신 2026-08-11 → Cursor SSOT 기록 |

### SSOT (변경/비변경)

- **변경 후보 (2단계)**: `supernova_hunter` DNA 매칭 threshold · `time_machine_backtester` 템플릿 recency weighting — **진단 후 확정**
- **산출물**: `reports/regime_panel/rp1_bull_recency_01_{date}.json` (schema **v2.3.4** 제안)
- **비접촉**: Phase A (Kelly cap·MDD tier·거버너) · SIDEWAYS/BEAR bucket · S5/인버스 · C-1 섹터 · **config_kv 라이브 반영**

### Spec — 범위 (in-scope)

**대상 구간 (2건 한정)**

| ID | 구간 | 기간 |
|----|------|------|
| BULL_03 | 최근상승 | 2024-10-01 ~ 2025-03-31 |
| BULL_05 | 글로벌리플레이 | 2016-06-01 ~ 2016-11-30 |

**1단계 (진단, 우선)** — trade-level breakdown

- 승률 · 평균손익 · 보유기간 · 진입 트리거 분포
- 가설 검증: **recency drift** (오늘 뇌 템플릿 → 과거 미스매치) vs 개별원인 (narrow breadth · 섹터로테이션 등)
- **공통원인 vs 구간별 별개원인** 결론을 먼저 낼 것

**2단계 (조정)** — 진단 결과 기반 **S1 알파층만**

- `supernova_hunter` DNA 매칭 threshold **또는** time_machine 템플릿 recency weighting

**검증**

- **15구간 전체** metrics-only rerun 필수
- BULL_03/05 **단독 rerun 금지** (회귀 확인 위해)

### 금지 (out-of-scope)

- Phase A(Kelly cap·MDD tier·거버너) 로직 변경 금지 — **S1 알파층만**
- SIDEWAYS/BEAR bucket 파라미터 접촉 금지
- S5/인버스·C-1 섹터 로직 접촉 금지
- **config_kv 실전/라이브 반영 금지** — RP-1 metrics-only backtest 재실행까지만
- 코드 구현 세부설계는 Cursor 재량 — **targeted diff only** (광범위 리팩터 금지)

### 완료 기준 (Definition of Done)

| # | 기준 |
|---|------|
| 1 | BULL_03, BULL_05 재판정 **최소 NEAR_MISS 이상** — `period_return_pct` 우선, **`cagr_pct` 단독 사용 금지** (규칙3) |
| 2 | 나머지 **13구간 verdict 불변** (회귀 없음) — 15구간 전체 rerun |
| 3 | `mdd_pct_tier` 전 구간 ≤10%, `mdd_crosscheck` = **MDD_OK** (tier 기준, raw 아님 — 규칙5) |
| 4 | 전 구간 `total_trades` ≥20 유지 (규칙4) |
| 5 | 산출물 JSON + `05_진행로그` §BULL-RECENCY-01 + `CURSOR_TO_CLAUDE` OUTBOX |

### Timebox

**1주** (진단 2~3일 + 조정·rerun 3~4일) — 2주 Alpha Proof 압축 타임라인 내 완결

### 병렬 허용

**OPS-01** VPS 배포 (F-GATE/F-RETIRE/BEAR-UNDERDOG)와 병렬 가능 — 쓰기 파일 충돌 없음  
(BULL-RECENCY-01: S1 템플릿/reports json vs OPS-01: config_kv/deploy_watch)

### Cursor 지시

1. **새 세션** — 본 Handoff 1개만 · **1단계 진단**(trade-level breakdown)부터 착수
2. Targeted diff only · 충돌 시 Adapter 제안 후 디렉터 Ask
3. 완료 후 metrics-only 15구간 rerun → DoD 1~4 대조

### 위험도

🟡 — S1 알파층만 · Phase A 비접촉 · config_kv 라이브 금지

### SRV-01 STRATEGIC VERDICT (기록)

- **Go**: BULL-RECENCY-01
- **보류**: SIDE-ALPHA-01 · BEAR-S5-SIM-01 · C-1-REDUCED

---

## Claude OK — CAT-E-BARS-01 Reality Audit 검증 (2026-08-09)

결론: **OK** — 6개 질문 모두 코드 실측 정확. exit_type=group-by SSOT(exit_reason 아님) 확인,
로컬 forward_trades 0행 → VPS SQL 필수 판단 동의. RL연장 컬럼 부재·I-2 로그 부재는
negative finding 그대로 인정, 우회추정 기각 타당.

수정 spec 없음. 후속:
- bars×ret: 신규 스크립트 없이 §2 SQL에 버킷 쿼리 (d) 1개만 추가 (본문 Claude 답변 참조)
- I-2 슬롯 병목 계측(F-QUOTA-LOG-01): F-GATE-01/F-RETIRE-02 배포+L-OBS-01 관측 이후로 연기
- RL 연장 식별 컬럼: No-Go (지금은) — 구체적 가설 나오기 전까지 스키마 변경 안 함

### 다음 (디렉터)
1. VPS §2 SQL (a)(b)(c) + 신규 (d) 버킷 쿼리 실행 → 결과 회신
2. F-GATE-01 · F-RETIRE-02 서버 배포 승인 (병렬 가능, 순서 F-GATE-01 먼저는 기존 결정 유지)

---

## 🔴 [CAT-F] F-GATE-01 — Registry State 기반 진입 차단 패치 (디렉터 승인 후 착수)

> **선행 확정 사실 (Cursor 회신 + VPS 실측, 재설계 아님)**: `resolve_group_treasury_mult`가 health 키 없을 때 `(1.0, "default")` 반환 확정. `evaluate_meta_group_entry_gate`는 mult≤0에서만 block — registry `state`(COOLED/RETIRED) 미참조 확정. VPS 교차조회: **COOLED/RETIRED 0건**(현재 활성 인스턴스 없음 — 구조적 결함이지 진행 중인 사고 아님)
> **순서**: 본 Handoff을 **F-RETIRE-02보다 먼저** 착수(사유는 §순서 참조)

### SSOT (변경/비변경)

- **변경**: `meta_treasury_entry_guard.py`(`evaluate_meta_group_entry_gate` — 신규 registry-state 체크 분기 **추가만**, 기존 mult 계산·block 조건식 무변경)
- **참조(읽기전용)**: `strategy_registry_store.load_registry_rows`(또는 governor 사이클 내 이미 로드된 `META_STRATEGY_REGISTRY` — 매 진입평가마다 재쿼리 금지, 아래 §성능 참조), `strategy_promotion_engine.py`(state 값 자체 — 산출 로직 비접촉)
- **비접촉**: `resolve_group_treasury_mult`의 default 반환값 자체(1.0 그대로 유지 — mult 의미 재정의 아님), Kelly sizing/`try_add_virtual_position`

### Spec

**A. 신규 판별 함수**

```text
resolve_registry_state_block(market: str, group_key: str, *, registry_rows: list[dict] | None = None) -> tuple[bool, str]
```

- `registry_rows`에서 `(market, group_key)` 최신 row의 `state`가 `COOLED` 또는 `RETIRED`면 `(True, "registry_state_block")`, 그 외(LIVE/CANDIDATE/OBSERVING 또는 row 없음)는 `(False, "")`
- **F-RETIRE-02 연동**: `state`가 observe_only 재발굴로 `CANDIDATE`로 복귀한 순간부터 자동으로 `(False, "")` — 별도 해제 로직 불필요(동일 SSOT 필드 재사용)

**B. `evaluate_meta_group_entry_gate` 결선**

- 기존 `mult <= 0.0` 체크와 **병렬 병기**(OR 조건, 둘 중 하나만 True여도 block): `state_blocked, state_reason = resolve_registry_state_block(...)` → `state_blocked`면 `block_entry=True`, reason에 기존 `"hard_cut"`류와 구분되는 `"registry_state_block"` 사용(사후 로그 분석 시 원인 구분 목적)
- 기존 mult 기반 block 경로·반환 스키마는 **한 글자도 변경 없음** — 신규 분기 추가만

**C. Kill switch (신규 config, additive)**

```text
ENABLE_REGISTRY_STATE_ENTRY_GATE: bool = True
```

`False` → 즉시 패치 이전 동작(mult-only)으로 완전 복귀

**D. 성능 주의 (Cursor 설계 재량, 스펙 강제 아님)**

진입평가는 고빈도 호출 경로 — `resolve_registry_state_block`이 매 호출마다 DB 재쿼리하지 않도록 governor 사이클 내 이미 로드된 `META_STRATEGY_REGISTRY`(메모리)를 우선 소비하고, 없을 때만 `load_registry_rows` fallback 권장(강제 아님, Cursor가 기존 캐싱 패턴 있으면 그대로 재사용)

### KR/US 분기

없음 — `market` 파라미터 그대로 전달, registry 자체가 이미 시장별 row. 시장별 if-하드코딩 대상 아님(Rule 8 해당 없음, 그대로 통과 원칙만 확인)

### 인접 CAT 영향

| CAT | 영향 | Critical |
|---|---|---|
| CAT-F | **전체 LIVE 진입 경로**에 신규 체크 삽입(주 변경) | 🔴 — 모든 시장·모든 그룹의 진입 평가가 이 함수를 거침. 단 **오늘 기준 COOLED/RETIRED 0건이라 배포 즉시 동작 변화는 없음**(잠재 리스크 차단이 목적, 활성 인시던트 대응 아님) |
| CAT-H | 비접촉 | — |
| CAT-B | 비접촉 | — |
| CAT-G | 비접촉 | — |

### 롤백 조건

`ENABLE_REGISTRY_STATE_ENTRY_GATE=False` → 즉시 패치 이전 상태(mult-only 게이트)로 완전 복귀. additive 분기라 기존 LIVE 그룹(현재 전부 state=LIVE/CANDIDATE로 추정) 진입 흐름에 영향 없음 — COOLED/RETIRED row가 생기는 시점(=F-RETIRE-02 가동 이후)부터 실효.

### Cursor 지시

1. **디렉터 승인 후 착수**(Rule 7 — CAT-F Critical, 전체 진입경로 터치) — 착수 전 디렉터 Go 명시 필요
2. `evaluate_meta_group_entry_gate` 기존 반환 스키마·호출부 시그니처 **무변경** — 내부 분기 추가만
3. §D 성능 권장(캐시 우선) — 강제 스펙 아님, 회귀만 없으면 Cursor 재량
4. 테스트: `tests/test_registry_state_entry_gate_f_gate_01.py`
   - (a) state=LIVE/CANDIDATE — 기존 mult 로직만으로 판정(회귀 없음)
   - (b) state=COOLED/RETIRED — mult=1.0(default)이어도 block 확인(핵심 케이스)
   - (c) F-RETIRE-02 redemption으로 state→CANDIDATE 복귀 시 즉시 unblock
   - (d) `ENABLE_REGISTRY_STATE_ENTRY_GATE=False` — 패치 이전 동작 정확히 재현
   - (e) registry row 자체가 없는 group(신규 discovery 전) — block 아님, 기존 `"empty_group"` 경로 유지 회귀 확인

### 위험도

🔴 **Critical(구조)** · 🟢 **활성 인시던트 없음**(오늘 기준 COOLED/RETIRED 0건) — 두 표현이 동시에 참. 디렉터 승인 후 착수, Cursor 구현·테스트 완료 후 Claude 검증 필수(C-FUNNEL-02와 동일 순서).

---

## 🔴 [CAT-F] 긴급 확인 요청 — capital_mult 미소비 / health 만료 후 실자본 재진입 가능성 (2026-08-09)

> **유형**: **확인 전용 — 구현 아님.** F-RETIRE-02(observe_only)와 별개 이슈, 그보다 **우선** 확인
> **트리거**: Cursor Step 0 회신(`CURSOR_TO_CLAUDE.md` §F-RETIRE-01 항목 1) — `capital_mult`가 진입 경로에서 read 0곳, `evaluate_meta_group_entry_gate`는 registry `state`가 아니라 `META_STRATEGY_HEALTH` mult만 참조

### 확인해야 하는 이유

`strategy_registry.state`(LIVE/COOLED/RETIRED)는 원래 "이 그룹에 실자본을 태워도 되는가"의 SSOT여야 한다. 그런데 Step 0 보고대로면 실제 진입 게이트는 그 `state`를 **한 번도 읽지 않고**, Treasury `health` mult(=최근 lookback 내 실거래 롤링 통계)만 본다. `health`는 슬라이딩 윈도우라 **나쁜 과거 실적이 lookback을 넘기면 자연스럽게 잊힌다** — 이때 해당 group_key가 `health` 딕셔너리에서 아예 사라지고, 소비 측(`evaluate_meta_group_entry_gate` 등)이 "키 없음"을 **default mult=1.0(허용)**으로 처리한다면, RETIRED로 명시적으로 강등된 전략이 **재승인 절차 없이 조용히 실자본을 다시 받을 수 있다.** 이건 observe_only(관측)와 무관하게 **지금 이 순간의 운영 리스크**일 수 있다.

### Cursor 확인 요청 (구현 없이 3가지만)

1. **default 동작 확정** — `forward/shared.py`(또는 `evaluate_meta_group_entry_gate`가 실제 정의된 파일) 코드 실측: `health.get(key)`가 없을 때 mult가 **1.0으로 폴백되는지, 0.0/block으로 폴백되는지** 정확한 분기 인용
2. **VPS 실측 교차조회** — `strategy_registry` 중 `state ∈ (COOLED, RETIRED)`인 group_key 목록 ∩ 현재 `META_STRATEGY_HEALTH` 스냅샷(라이브 `META_STRATEGY_REGISTRY`/`META_STRATEGY_HEALTH` 덤프)에서 **mult=1.0이거나 키 자체가 없는 항목**이 있는지. 있다면 = 스모킹건(현재 진행 중인 실자본 누출 가능성)
3. **최근 실거래 대조** — 1·2에서 위험군이 나오면, 해당 group_key로 `state` 강등 이후 실제 `forward_trades` CLOSED row가 발생했는지(=이미 실자본이 들어갔는지) 날짜 대조

### 회신 형식

- [CAT-F] 결론 3줄
- 위 3건 표
- 위험군 0건이면 "확인 완료, 리스크 없음" / 1건 이상이면 **디렉터 즉시 보고 대상** — Claude가 별도 패치 Handoff(가칭 F-GATE-01) 작성

### 위험도

🔴 **Critical (확인 단계)** — LIVE 자본 배분 게이트의 SSOT 정합성 문제. 패치는 아직 설계하지 않음(사실관계 확인 먼저, Rule 5·6 위반 없이 설계하려면 정확한 default 분기부터 알아야 함). 다중 CAT 파급 가능성(CAT-F 상태기계 + 진입 게이트 공유 경로) — 확인 결과에 따라 디렉터 에스컬레이션 여부 재판단.

---

## [CAT-F] F-RETIRE-02 — COOLED/RETIRED Observe-Only 사후추적 (Go 확정 — 위 🔴 긴급확인 이후 착수)

> **선행조건 (확정)**: VPS 실측 결과 COOLED/RETIRED **0건**으로 스모킹건은 없었으나, 구조적 결함(default mult=1.0, state 미참조) 자체는 확정됨 — **F-GATE-01을 먼저 착수·완료한 뒤 본 Handoff 착수**. observe_only가 COOLED/RETIRED 개체 수를 0→N으로 늘리는 기능인 만큼, 그 개체가 새는 게이트에 노출되는 순서(F-RETIRE-02 먼저)는 피함

### SSOT (변경/비변경)

- **변경**: `strategy_promotion_engine.py`(`run_registry_lifecycle` — COOLED/RETIRED 대상 observe_only 신호 소비 분기 **추가만**. LIVE/CANDIDATE/COOLED **기존 승격·강등 임계값·전이식은 한 줄도 변경 안 함**), `strategy_registry_store.py`(additive 컬럼), **`forward/shared.py`**(신규 — 아래 B' 참조, `evaluate_meta_group_entry_gate`/`RE_EVOL_SHADOW` 인접 지점에 additive 분기만 추가, 기존 LIVE 진입 판정식 비접촉)
- **참조(읽기전용, Adapter 재사용 — 함수 시그니처만 소비, 파일 내부 로직 이관 없음)**: `re_evolution_redemption_gate.py`(`compute_dynamic_shadow_verification_window`, `fetch_shadow_closed_rows`, `compute_shadow_stats`, `passes_redemption_gate`), `forward/shared.py`의 기존 `apply_shadow_entry_zero_notional`(RE_EVOL_SHADOW 전용 — **동일 패턴**을 LIFECYCLE_OBSERVE_ONLY용으로 별도 함수화, 기존 함수 수정 아님), `strategy_lifecycle_config.py`(`alpha_half_life_days` 등 기존 CAT-CONSTANTS — 신규 상수 추가 없음)
- **비접촉**: LIVE/CANDIDATE 승격 임계값(`passes_live_hard_gate` 등), Kelly/자본배분 로직, 기존 `RE_EVOL_SHADOW` 3-Strike 경로(태그 네임스페이스 분리로 간섭 없음)

### Spec

**A. 신규 태그** — `LIFECYCLE_OBSERVE_ONLY`(`forward_trades.sig_type` suffix). `RE_EVOL_SHADOW`와 **문자열 분리**(함수는 재사용, 태그는 분리 — 두 강등 경로의 사후분석이 섞이지 않도록)

**B. 관측 트리거** — `state ∈ {COOLED, RETIRED}` 전환 시점부터 시작:

```text
is_lifecycle_observe_only_row(row: dict) -> bool
```

기존 `is_re_evolution_observing_row`(state=="OBSERVING" 전용)와 동일 판정 패턴을 COOLED/RETIRED로 확장한 **별도 함수**(기존 함수 수정 아님)

**B'. 실행계층 결선 (신규, 필수 — Step 0로 확정된 스코프)**

Step 0 확인: `capital_mult`는 진입 경로 미참조, `RE_EVOL_SHADOW`만 `apply_shadow_entry_zero_notional`로 실제 $0 페이퍼가 `forward_trades`에 적재됨. **동일 지점·동일 패턴**으로 신규 함수를 결선:

```text
apply_lifecycle_observe_only_entry_zero_notional(row: dict, *, market: str, meta, sys_config) -> None
```

`is_lifecycle_observe_only_row(row)`가 True인 group_key에 한해, 기존 `apply_shadow_entry_zero_notional`과 같은 지점에서 호출 — $0 notional 신호를 `forward_trades`에 `sig_type` **`LIFECYCLE_OBSERVE_ONLY`** 태그로 적재. LIVE 그룹의 기존 진입 판정식·notional 계산은 **비접촉**.

**C. 보존창(retention) — 확정**

디렉터 90일(=US 기준) ÷ US half-life(30d) = `RETENTION_MULT = 3.0`으로 역산 확정(Rule 5 — 디렉터 지정값에서 도출, 임의 생성 아님):

| market | alpha_half_life_days | retention_days |
|---|---|---|
| KR | 10 | **30** |
| US | 30 | **90** |
| BG | 21 | 63(참고만, 본 Handoff 구현 대상 아님 — Rule 1) |

**D. 검증창(기존 재사용, 무변경)** — `compute_dynamic_shadow_verification_window`(half_life × 70~100% + HIGH_VOL/BEAR_PANIC ×0.5 dilation). 보존창(C) 소진 전까지 이 짧은 창 단위로 재발굴 시도를 반복

**E. 재발굴 게이트** — 기존 `passes_redemption_gate` 시그니처 그대로 재사용(신규 임계값 없음). 통과 시 `COOLED/RETIRED → CANDIDATE`(**LIVE 직행 아님** — 기존 CANDIDATE→LIVE Hard Gate 정상 재통과 필요)

**F. config 키(신규, additive)**

```text
LIFECYCLE_OBSERVE_ONLY_ENABLED: bool = True
LIFECYCLE_OBSERVE_ONLY_RETENTION_DAYS: {"KR": 30, "US": 90, "BG": 63}   # §C 확정값
```

**G. 함수 시그니처(신규 — `strategy_promotion_engine.py`/`forward/shared.py`/신규 파일, 파일 배치는 Cursor 판단)**

```text
is_lifecycle_observe_only_row(row: dict) -> bool
resolve_observe_only_retention_days(market: str, system_cfg: dict | None) -> int
apply_lifecycle_observe_only_entry_zero_notional(row: dict, *, market, meta, sys_config) -> None
evaluate_lifecycle_observe_only_redemption(row: dict, *, meta, sys_config, forward_db_path, now) -> tuple[bool, dict]
```

### KR/US 분기

공통 함수, `market` 파라미터로 `strategy_lifecycle_config.market_params()` 조회만 — 시장별 if-하드코딩 금지(Rule 8) 그대로 준수. BG는 SSOT상 값만 존재 — **본 Handoff 구현·실행계층 결선 대상 아님**(Rule 1).

### 인접 CAT 영향

| CAT | 영향 | Critical |
|---|---|---|
| CAT-F | 신규 관측 로직 + 실행계층 결선(B') 추가 — LIVE/CANDIDATE/COOLED 기존 임계값·판정식 무변경 | 🟡 (공유 실행파일 `forward/shared.py` 터치로 상향, LIVE 로직 자체는 비접촉) |
| CAT-H | 비접촉(`alpha_half_life_days` read-only 참조만, 재정의 없음) | — |
| CAT-B | 비접촉(신규 테이블 없음, `forward_trades.sig_type` 태그 추가만) | — |
| CAT-G | 비접촉 | — |

🔴 Critical 아님(위 🔴 긴급확인 항목과는 별개 — 그건 기존 게이트의 사실관계 확인, 이건 신규 관측 기능 추가). 단 `forward/shared.py` 공유 파일 터치로 🟢→🟡 상향.

### 롤백 조건

`LIFECYCLE_OBSERVE_ONLY_ENABLED=False` → 즉시 현행(RETIRED 터미널·무추적) 복귀. 태그·컬럼·B' 결선 모두 additive라 기존 LIVE 파이프라인 영향 없음.

### Cursor 지시

1. **선행 필수** — 위 🔴 긴급확인 항목 먼저 회신. "위험군 있음"이면 F-GATE-01(패치) 대기, "리스크 없음"이면 바로 착수
2. B'(실행계층 결선) 구현 시 `apply_shadow_entry_zero_notional` 정의부를 **그대로 복제하지 말고** 공통 헬퍼로 뽑을 수 있으면 제안(강제 아님 — Cursor 판단, 단 RE_EVOL_SHADOW 기존 동작 회귀 없어야 함)
3. §C retention 값 그대로 구현 — 코드 재작성 없이 config 값만 교체 가능하도록 §F 구조 유지
4. 테스트: `tests/test_lifecycle_observe_only_f_retire_02.py`
   - (a) COOLED/RETIRED 진입 시 `LIFECYCLE_OBSERVE_ONLY` 플래그 세팅 + `forward_trades` $0 notional 적재 확인
   - (b) KR 30d / US 90d 경과 후 관측 종료(추가 태깅 중단) 스모크
   - (c) 재발굴 게이트 통과 시 `CANDIDATE` 복귀(LIVE 아님) 회귀
   - (d) `RE_EVOL_SHADOW` 3-Strike 경로와 태그 네임스페이스·`apply_shadow_entry_zero_notional` 기존 동작 미간섭 확인

### 위험도

🟡 Medium(공유 실행파일 `forward/shared.py` 터치, 단 additive 분기·자본 미배분) — 착수 전 위 🔴 긴급확인 완료 필수.

---

## [CAT-C] C-FUNNEL-02 — 스캔 퍼널 탈락 계측 (insert 회귀 수정 + near-miss 이벤트 로그)

### SSOT (변경/비변경)
- **변경**: `scanner_funnel.py`(`ScanFunnelTracker.drop` 시그니처 확장 · near-miss 버퍼 · `finalize` flush), `proprietary_friction_store.py`(루트 파일 — `insert_scan_funnel_snapshot` 복구 + 신규 `insert_scan_funnel_drop_events` + `drops_json`/`scanner` 컬럼)
- **참조(읽기전용)**: 기존 `{market}_REGIME_KEY` / `REGIME_ENSEMBLE.markets.{market}.regime`(A-3와 동일 read 패턴 — CAT-G 로직·계산 변경 없음), `supernova_hunter.py`(`drop()` 호출부에 score 전달 추가만, 판정식 무변경)
- **비접촉**: 스캔 pass/drop 판정 로직 자체(cutoff 비교식) — 계측만 추가, 결과 불변

### Spec

**A. 회귀 수정 (선행, 독립 커밋)**

`insert_scan_funnel_snapshot(ts, market, universe_size, survivors, pass_rate_pct, scanner=None, drops_json=None) -> None`

- `finalize()`에서 `drop_summary`(Counter) → `json.dumps(dict(drop_summary))`로 `drops_json` 전달, 호출 스캐너명 `scanner`에 전달
- 무음 `try/except: pass` 제거 — 실패 시 `logger.warning` 최소 1줄(예외 전파 여부는 Cursor 판단, 무음만 금지)

**B. `scan_funnel_snapshot` 추가 컬럼**

| 컬럼 | 타입 | 비고 |
|------|------|------|
| scanner | TEXT NULL | 신규 — Audit 권장 |
| drops_json | TEXT NULL | 신규 — `{reason: count}` JSON |

**C. 신규 테이블 `scan_funnel_drop_event`**

| 컬럼 | 타입 | 비고 |
|------|------|------|
| id | INTEGER PK AUTOINCREMENT | |
| ts | TEXT(ISO UTC) | |
| market | TEXT | KR/US |
| scanner | TEXT | |
| code | TEXT NULL | 과도기 호환 — 없으면 NULL |
| reason | TEXT | 기존 Counter 키 재사용(신규 사유 문자열 창조 금지) |
| final_score | REAL NULL | |
| eff_cos_cutoff | REAL NULL | |
| eff_ml_cutoff | REAL NULL | |
| regime_key | TEXT NULL | finalize 1회 read로 denormalize |
| rank_in_slot | INTEGER NULL | near-miss 정렬 순위(1=cutoff 최근접) |

**D. `ScanFunnelTracker.drop()` 시그니처 확장** (하위호환 — 신규 인자 전부 keyword-only optional)

```text
drop(self, reason: str, n: int = 1, *, code: str | None = None,
     final_score: float | None = None,
     eff_cos_cutoff: float | None = None,
     eff_ml_cutoff: float | None = None) -> None
```

- 기존 `funnel.drop("DNA_FAIL")` 호출 **무변경 동작**. score 전달은 점진 적용 — **DNA_FAIL·LIQUIDITY 호출부부터** 우선

**E. Near-miss 샘플링 정책**
- 슬롯 = `(scan_date UTC, market, reason)` · **cap = 50/슬롯**
- 정렬 키: `|cutoff - final_score|` 오름차순(0에 근접할수록 우선) — score/cutoff 둘 다 없는 reason은 FIFO 50건 대체
- `finalize()` 1회 flush → `insert_scan_funnel_drop_events(rows: list[dict]) -> None`

**F. Regime denormalize**
- `finalize()`에서 market당 **1회만** `_read_current_regime_key(market: str) -> str | None` 호출 → 버퍼 전체 row에 stamp
- CAT-G 계산 로직 변경 없음 — A-3와 동일 read-only 패턴

**G. Retention**
- `KEEP_DAYS`/`KEEP_LAST` **디렉터 확인 요망** — v1은 pruning 없이 적재만

### KR/US 분기
- 공통 함수·테이블, `market` 컬럼 값만 KR/US 분기. 코드 내 시장별 if-하드코딩 금지(규칙 8)

### 인접 CAT 영향
| CAT | 영향 | Critical |
|-----|------|----------|
| CAT-C | 계측 코드 추가(주 변경) | 🟢 판정 로직 비접촉 |
| CAT-G | read-only 1회 조회만 | 🟢 로직/계산 무변경 |
| CAT-B | 비접촉(OHLCV 조인은 C-FUNNEL-03 별도 Handoff) | — |
| CAT-F | 비접촉 | — |

### 롤백 조건
- 신규 테이블·컬럼은 additive — 기존 파이프라인 영향 없음. 문제 시 `scan_funnel_drop_event` insert만 최소범위 try/except로 스킵, snapshot 경로는 유지
- `drop()` 시그니처는 하위호환 — 별도 롤백 불필요

### Cursor 지시
- Targeted diff only. 판정 로직(cutoff 비교, pass/drop 분기) **한 줄도 건드리지 말 것**
- 순서: **A(회귀 수정) 단독 커밋 먼저** → 검증 후 C~F(near-miss 계측)
- 테스트: `tests/test_scan_funnel_drop_event_c_funnel_02.py`
  - (a) 회귀 수정 후 `scan_funnel_snapshot` insert 성공 스모크
  - (b) `drops_json` 역직렬화 검증
  - (c) near-miss cap=50 초과 시 정렬·컷 스모크
  - (d) `drop()` 신규 인자 없이 호출 시 기존 동작 회귀 없음
  - (e) `regime_key` denormalize 1회 read 확인(mock)

### 위험도
🟢 Low(계측 전용, 판정 로직 비접촉, additive 스키마) — 디렉터 Critical 승인 불필요.

---

## [CAT-C] RP-1 + C-1 병합 — 15구간 레짐패널 baseline (RP-1) → 조건부 섹터부스트 A/B (C-1)

### SSOT (변경 금지 unless noted)
- 파일: `time_machine_backtester.py`(REGIME_PERIODS 확장, run_time_machine_regime_matrix 재사용)
- 참조(읽기전용): `performance_budget_governor.py`(tier 임계값), `meta_governor.py`(ACTION_BY_REGIME), `sector_rotation_store.py`(C-1)

### Stage 1 — RP-1 baseline (필수, 먼저)
- `REGIME_PERIODS` 6→15구간 확장 — 위 15구간 표 그대로. DB 미가용 구간은 백업 리스트로 즉시 치환(순연 금지, 치환 로그 남길 것)
- 시뮬 스택: S1(supernova) + S4(선택, timebox 되면) + S5(태그만) + **Phase A tier overlay**
- **Phase A overlay 스펙**: 라이브 모듈 풀 연동 아님. 백테스트 equity curve의 peak-to-trough 소진율을 계산해 `performance_budget_governor` tier 임계값(40/70/90%)과 동일 기준으로 `KELLY_THROTTLE_MULT`/`POSITION_QUOTA_MULT` **동일 로직으로 replay**만. config_kv 실제 write 없음.
- 시뮬레이션 단위: **KR+US 합산 포트폴리오** (개별 시장 분리 아님)
- 출력: `reports/regime_panel/rp1_{date}.json` — 구간별 CAGR/MDD/PF/n/진입0여부/tier소진로그
- **Lookahead**: v1(오늘 뇌 템플릿) 그대로, 리포트에 "상한선 추정치, Pass≠실전보장" 문구 고정 삽입. v2(point-in-time) 이번 스코프 아님.

### Stage 1 판정 (본 Handoff 규칙대로 자동 계산)
- Pass/Near-miss/Fail 상단 표 그대로 코드화
- Fail 시 원인 카테고리 A/B/C/D 자동 태깅 (§원인분석 트리 규칙 그대로 매핑: 진입n≈0→A, MDD>10%→C, 그 외 저수익→B)

### Stage 2 — C-1 A/B (조건부, Stage 1 결과로 자동 분기)
| Stage 1 결과 | Stage 2 행동 |
|--------------|---------------|
| Fail, 원인=A (신호부족) | **C-1 중단**. `report`에 "C-1 스킵: 원인 A" 명시하고 세션 종료 |
| Fail, 원인=C (MDD구조) | **C-1 중단**. 동일 처리 |
| Fail, 원인=B (수익부족·타이밍) | **C-1 축소 스코프**: sector spillover A/B만 (일반 기능화 아님), 15구간 중 원인B로 태깅된 구간만 재실행 |
| Near-miss (모든 원인) | **C-1 정상 진행** — baseline vs C-1 A/B, 15구간 전체 |
| Pass | C-1 진행(선택) — 이미 목표 달성이므로 우선순위 낮음, 스킵해도 무방 |

### 인접 CAT 영향
- CAT-F: read-only (tier 임계값 replay만, config_kv write 없음)
- CAT-G: read-only (REGIME_PERIODS 라벨 참조만)
- CAT-B: 신규 DB 없음 — JSON 리포트 파일만

### 롤백 조건
- 코드 자체가 배포되는 게 아니라 backtest 리포트 산출물이므로 롤백 대상 없음. 결과가 실망스러워도 **코드 삭제 금지** — 다음 Handoff 판단 자료로 보존.

### Cursor 지시
- Targeted diff only. Stage 1 무결론 2주 → **RP-1도 No-Go** (인프라/데이터 결함으로 태깅, C-1도 자동 스킵)
- 테스트: `tests/test_regime_panel_rp1.py` — 6→15 매핑 smoke, tier overlay 단위, Stage1→Stage2 분기 로직
- n<20 구간 자동판정 금지 로직 필수 (하드코딩 스킵)

### 위험도
🟡 High (목표 직결, 배포 아님) — 디렉터 승인 후 착수, 완료 후 Claude 검증
