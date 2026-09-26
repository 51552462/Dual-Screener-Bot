# 세션 동기화 앵커 (멀티 채널 · 멀티 창 SSOT)

> **새 Claude Pro 창 · 새 Cursor 채팅 · 텔레그램 회신 붙여넣기 전 — 이 파일을 먼저 읽는다.**  
> **갱신**: 2026-09-26 · **앵커 ID**: `SYNC-2026-09-26-DOCS-SYNC-01`

### 상시 HOLD (Cursor·Claude 세션 응답 **마지막 줄**, 해제 전까지)

`HOLD: KRX 일반 ID/PW(비네이버) 재가입되면 .env 후 SMARTMONEY-KRX-CREDENTIAL-PERSIST-01 스모크.`

---

## 1. 한 줄 규칙 (디렉터·AI 공통)

| 규칙 | 내용 |
|------|------|
| **단일 진실** | KR/US 주식 팩토리 실행 SSOT = **`docs/work_phases/`** (본 폴더) **만** |
| **복사본 금지** | `Downloads/*.md`, 채팅에 붙인 예전 요약, `bitget/docs/work_phases/*` → **참고만**. 충돌 시 **본 폴더**가 이긴다 |
| **한 창 = 한 sub-phase** | 동시에 여러 Handoff 구현 금지 (`07_듀얼AI_협업루프.md`) |
| **상태 갱신 순서** | 구현/검증 후 → `05_진행로그` → `00_전체현황판`(필요 시) → **`00_SESSION_SYNC` §3 스냅샷** → `NEXT_ACTION` |
| **채팅 말고 파일** | Claude OK / Cursor 완료 / 조사 결과 → **`CURSOR_TO_CLAUDE` 또는 `CLAUDE_TO_CURSOR`에 append** |

---

## 2. 파일 권한 표 (누가·무엇을 쓰는가)

| 파일 | 역할 | 쓰는 쪽 | 읽는 쪽 |
|------|------|---------|---------|
| **`00_SESSION_SYNC.md`** | **멀티 창 앵커 · 현재 포커스 스냅샷** | Cursor (세션 종료 시 §3) | Claude · Cursor · 디렉터 (창 열 때) |
| `NEXT_ACTION.md` | **지금 디렉터가 할 일 1장** | Claude 또는 Cursor | 디렉터 |
| `05_진행로그.md` | sub-phase별 **이력·잔여** (append) | Cursor | Claude · Cursor |
| `00_전체현황판.md` | Phase 표 · **용어집** (가끔) | Cursor | Claude · Cursor |
| `CLAUDE_TO_CURSOR.md` | Handoff **INBOX** (설계·Go) | **Claude Pro 만** | Cursor |
| `CURSOR_TO_CLAUDE.md` | 검증·조사 **OUTBOX** | **Cursor 만** | Claude Pro |
| `06_검증체크리스트_*.md` | 배포 후 **효과** 검증 (2~4주) | 디렉터·Cursor | Claude |
| **`17_Cursor_세션_부팅_가이드.md`** | **Cursor 첫 메시지·모드 선택 SSOT** | Cursor | 디렉터 |
| **`18_디렉터_VPS_원클릭.md`** | **배포 원클릭 · update_factory 범위** | Cursor (문구 늘어날 때) | **디렉터 (헷갈릴 때)** |
| `bitget/docs/work_phases/` | **Bitget 트랙 미러** | Bitget 세션만 | BG 작업 시 — KR/US와 **혼동 금지** |

**`NEXT_STEP.md` vs `NEXT_ACTION.md`**: 실행 SSOT는 **`NEXT_ACTION.md`만**. `NEXT_STEP`은 레거시·참고.

---

## 3. 현재 포커스 스냅샷 (세션 종료 시 갱신)

| 필드 | 값 |
|------|-----|
| **앵커 ID** | `SYNC-2026-09-26-DOCS-SYNC-01` |
| **마지막 갱신** | 2026-09-26 — ENTRY-ATR **Claude OK(조건부)+DoD4** · DOCS-SYNC-01 · 수급 HOLD |
| **활성 트랙** | **KR/US** |
| **진행 중 sub-phase** | 없음 (ENTRY-ATR 배포 후 관측 · DOCS-SYNC 문서만) |
| **직전 완료** | ENTRY-ATR-OVERLAY-01 Phase1 · DOCS-SYNC-01 |
| **다음** | 2~4주 entry_atr 관측. Phase 2 금지. Bitget 실적은 코인 창 |
| **VPS 배포 SSOT** | KR/US: `18_디렉터_VPS_원클릭.md` |
| **Handoff SSOT** | `docs/work_phases/CLAUDE_TO_CURSOR.md` |
| **North Star 원장 SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **git main** | VPS HEAD **`b7b8913`** (MEGA_TREND) · EXIT-TYPE `810b89e`는 그 이전 |

### 열린 작업 줄기 (꼬이지 않게)

```
[KR/US] TRACKA-NORTHSTAR-AMEND-01 · CLOSED
[KR/US] FWD-OBS-HOLD 목표하향 · AMEND-01로 CLOSED · mega_trend=RP-1대체 스코프 섀도우 WATCH · 발동게이트=AXIS RANK_B/D n>=20 AND PF>0.5 · **SCORING-VALIDITY=NONE(2026-09-25, VPS CLOSED n=462)**
[KR/US] SMARTMONEY-PERSIST-FIX-01 · **CLOSED+재발** · 09-15 DoD#1 PASS → 09-18 재사망 · 06 기록
[KR/US] DIRECTOR-WATCHDOG-FLOW-01 · **WATCHDOG-FUNNEL-VISIBILITY-01에 흡수**(최신일·행수) · 7일 COUNT 스코프는 대체됨
[KR/US] LIVE-REPLAY-HOLD · **지금 만들지 않음** · 월 수급 신호 후에만 「수급 포함 리플레이」재검토
[KR/US] SMARTMONEY-RADAR-SQLITE-01 · backlog **범위확정** · KR/US 라다 JSON→sqlite 병합 · 표시 0픽/적재실패 미구분 · 급하지 않음 · 지금 코드 금지
[KR/US] MAE-ATR-REPLAY-01 · CLOSED · 실전 반려 (스크립트 보존·미배선) · 청산 3가설 기각
[KR/US] PHASEB-SCOPE-02 · CLOSED(문서) · B-4 큐(~09-17) · B-1/B-2 보류(DSR) · B-3 별도
[KR/US] V-2-WFBLOCK-01 · Claude OK · CLOSED · 1주 오탐 추적 (~09-17)
[KR/US] DIRECTOR-WATCHDOG-01 · Claude OK · CLOSED는 19:30 육안 후
[KR/US] FAMILY-SLEEVE-DEMOTE-01 · **정책 B** · A군=차단해제 관찰 0.25 · B군=축소 0.25 · DoD#4 실측 OK(9/12 데스매치 후 effective 0.25) · 코드 0
[KR/US] SIZING-DUAL-LEDGER-01 · backlog 🔴 · 2천만 시드≠NAV $30만 · **실전 전 필수** · 지금 착수 금지
[KR/US] US-COSINE-FUNNEL-DEAD-01 · **CLOSED** · 정상 선별 복구(AXIS 라이브). 침묵 완전 해소 아님. NA/미완봉은 NA-FIX 별 항목
[KR/US] US-COSINE-NA-FIX-01 · 안전장치 유지 · 미완봉→전일 대체 없음
[KR/US] US-COSINE-AXIS-01 · **Claude OK 2026-09-25** · 배포 · 워치독 관찰
[KR/US] SMARTMONEY-CORR-CHECK-01 · **RO 판단보류** · WAIT_CLAUDE_OK
[KR/US] MEGATREND-SCOPE-RO-01 · **RO 견적** · WAIT_CLAUDE_OK
[KR/US] SMARTMONEY-PERSIST-CRON-RECHECK-01 · **Claude OK 2026-09-25** · CLOSED(조회)
[KR/US] SMARTMONEY-NAVER-HTTP-RO · **backlog** · (a)구조변경 vs (b)차단/레이트리밋 · 급하지 않음
[KR/US] MEGATREND-LIVE-IMPACT-AUDIT-01 · **Claude OK 2026-09-25** · 무의미 · 감시만(재점화 시 재감사)
[KR/US] MEGATREND-SCOPE-DEFINE-01 · **Claude OK 2026-09-25** · RP-1 섀도우 WATCH
[KR/US] SCORE-PREDICTIVE-POWER-RO-01 · **Claude OK 2026-09-25** · 없음(사실상 랜덤) · SCORING-VALIDITY=NONE
[KR/US] SCORE-COMPONENT-BREAKDOWN-RO-01 · **RO 판정=전 DNA피처 무의미(새 재료)** · WAIT_CLAUDE_OK · 코드 0
[KR/US] AXIS-RAWMATERIAL-OVERLAP-RO-01 · **RO 판정=B** · tb/bbe 겹침 · z는 부호 안 바꿈 · WAIT_CLAUDE_OK
[KR/US] SCORE-FINAL-GAPS-RO-01 · **RO** · AXIS=완전위험(3축 비신호) · dyn_rs=기각(아티팩트) · WAIT_CLAUDE_OK
[KR/US] MATERIAL-MAP-RO-01 · **RO** · EXCL 유의=SIDEWAYS/국면서열·kelly_invest(내생) · 매크로 미존재 · WAIT_CLAUDE_OK
[KR/US] MACRO-JOIN-SCOPE-RO-01 · **Claude OK 2026-09-26** · asof 동의 · 발견 미확정
[KR/US] DSR-SCOPE-RO-01 · **Claude OK 2026-09-26** · 피처BH 대체 아님
[KR/US] CAT-H-REPAIR-01 · **Claude OK(조건부)·전체회귀 대기** (2026-09-26) · CLOSED 아님
[KR/US] CATH-HTC-LIVE-RO-01 · **RO 2026-09-26** · 토 슬롯은 기동 · 합성 NameError · OOS IndentationError · 승격 0 · GP_MUT 현재 0
[KR/US] DSR-MINIMAL-01 · **구현(로컬)** · VPS HEAD **b7b8913에 없음** · 미배포 · WAIT_CLAUDE_OK
[KR/US] MACRO-SPLIT-DIRECTION-RO-01 · **RO** · KR 방향일치 WATCH · US≈0 · WAIT_CLAUDE_OK
[KR/US] SMARTMONEY-NAVER-FIX-SCOPE-RO-01 · **RO** · iframe **HTTP 410 종료** · 자격 없음 · 9/2전 asof 0 · WAIT_CLAUDE_OK
[KR/US] SMARTMONEY-SOURCE-REVIVE-01 · **1단계 RO** · iframe 대체 HTML **없음** · SPA `/market/stock/kr/trend/foreigner` 등 200 · 2단계 대기
[KR/US] KRX-OPENAPI-SMOKETEST-01 · **RO** · OpenAPI에 투자자별 실적 **없음** · 키≠KRX_ID/PW · WAIT_CLAUDE_OK
[KR/US] KRX-MARKETPLACE-PRICING-RO-01 · **RO** · 유료상품 심사·파일 · **후순위** · WAIT_CLAUDE_OK
[KR/US] SMARTMONEY-KRX-CREDENTIAL-PERSIST-01 · **HOLD** · 네이버SSO≠pykrx · 탈퇴 후 재가입 대기 · 스모크 미완
[KR/US] CORRKELLY-US-PF-RO · **Claude OK: 2026-09-26 (c)판단보류** · 창맞추면 WR갭 소멸 · 회피필터 등록 안 함
[KR/US] ENTRY-ATR-OVERLAY-01 · **Claude OK(조건부): 2026-09-26** · DoD4 111 pass · 2 fail은 무관 pre-exist · 배포
[KR/US] DOCS-SYNC-01 · **문서만 2026-09-26** · 09·IV_03·11·12 날짜 맞춤
[KR/US] US-8월24후-전패구간-RO · **backlog** · 급하지 않음 · 다음 라운드 후보
[KR/US] EXIT-TYPE-LABEL-01 · **Claude OK 2026-09-25** · **배포완료(810b89e)**
[KR/US] REALITY-AUDIT-BADEXIT-01 · **RO CLOSED** · 다음=EXIT-TYPE-LABEL-01
[KR/US] WATCHDOG-AXIS-OBS-TG-01 · **로컬 표시 배관** · 미커밋 · 게이트 없음
[KR/US] ELASTIC-SCOUT-GUARD-TEST · **backlog** · `test_non_scout_never_gated` 실패 원인(급하지 않음, AXIS 무관)
[확인] US-RANK-B-STALL-RO · **CLOSED** · n=30 대기 **폐기** · 워치독 n=7=전생 · 마지막 RANK_B 7/20 · cosine 7/22
[KR/US] US-COSINE-SILENT-A · 확정 · 0.45 임계 기각
[KR/US] US-COSINE-SILENT-B · **확정** · NA→DATA 위장 · 빈 config 폴백 · S1 비공유
[KR/US] SWALLOW-CENSUS-01 · **RO 목록 완료**
[KR/US] SWALLOW-GATE-FO-01 A · **Claude OK 2026-09-19** · 배포 `5ff319d`
[KR/US] SWALLOW-GATE-FO-02 · **Claude OK 2026-09-19** · 배포 · ERROR 소멸 관측
[KR/US] TOXIC-FADE-FO-CHECK · **backlog** · 1848 별 try · 발동 0 · 급하지 않음
[KR/US] EOD-FLUID-STOP-FO-01(A) · **Claude OK 2026-09-20** · 배포 · BEAR 로그 관찰
[KR/US] EOD-DEFENSE-GAP-01 · **backlog** · DEFENSE 시간함수엔 있고 ledger 리스트엔 없음 · 결정 전 코드 금지
[KR/US] SWALLOW-LEFTOVER-BATCH-A-01 · **Claude OK 2026-09-20** · 배포 · 관측 등록
[KR/US] SWALLOW-CENSUS leftover 11곳 · **종결** · 게이트 B · EOD A · leftover A
[KR/US] WATCHDOG-FUNNEL-VISIBILITY-01 · **Claude OK 2026-09-24** · 배포 · 표시 3줄
[KR/US] REGIME-KEYS-RO · **판단 승인** · 통합 금지 · 나중에 4키 표시 XOR CURRENT 덮어쓰기 금지 · 지금 코드 없음
[KR/US] US-COSINE-NA-BOOL-RO · RO 흡수 · NA-FIX-01로 이관
[KR/US] US-COSINE-CUTOFF-RO · **RO 2026-09-24 CLOSED** · tb 노름 지배 · 99%→z 36% · 7/22 재해석
[KR/US] US-LIQ-FLOOR30K-SHADOW · **RO 2026-09-24** · 스펙 `$30k` no-2000 · 단독 라이브 **금지**(컷과 한 Handoff)
[KR/US] US-LIQ-FLOOR-RO · **RO 2026-09-23** · `$300k`=5/28 스캐너 보정 · 사이징 미연동 · 낮출지 디렉터 · 코드 금지
[KR/US] US-COSINE-LIQ-RO · **RO 2026-09-23** · 1153 LIQ = 페니+얇은 ADV 실측 · NA 위장 아님 · 코드 금지
[KR/US] US-COSINE-REGIME-RO · **RO 2026-09-23** · 템플릿-국면 불일치 **미증명** · 코사인 층 미도달 · 코드 금지
[패턴] 예외 삼킴: 전수 목록 OUTBOX · pass-only는 발동 증명 불가
[감시] US_RANK_B 1.35x · US_RANK_D 1.25x — 다음 데스매치 사이클 유지 여부 (액션 아님)
[KR/US] NAV-HOOK-SILENTFAIL-02 · CLOSED (코드+Step B)
[KR/US] KR-LOCKDOWN-STALL-THAW-01 · **VPS ARMED=1** (2026-09-21 OPEN=0) · 관측 · 한국 재오픈 아님
[KR/US] KR-LOCKDOWN-LADDER-01 · HOLD · 회복 후 히스테리시스/N일 (🔴) · STALL-THAW와 혼동 금지
[KR/US] OPS-LIQ-PHASE2-01 · backlog
[IV] V-2-DSR-01 최소=OBSERVE/WARN · LIVE 하드·V-2B 여전히 backlog
[금지] MDD 캡 변경 · 40~70% 삭제 · bitget 미러 · Phase2 자동착수 · **LOCKDOWN 무단 우회**(STALL-THAW는 Handoff+Critical만) · 라이브 리플레이 지금 · **사이징 공식 오늘 개조** · A군 US S1을 임의로 0 재차단(정책 B 번복은 Handoff)
```

**다른 창에서 다른 sub-phase를 열었다면** → 그 창 닫기 전에 §3 이 표만이라도 갱신하거나, 디렉터에게 「앵커 갱신 필요」라고 남긴다.

---

## 4. 새 창 부팅 문구 (복붙용)

### Claude Pro (설계·검증 창)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기(순서):
1) docs/work_phases/00_SESSION_SYNC.md
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md (최상단 OUTBOX)
4) 필요 시 docs/work_phases/CLAUDE_TO_CURSOR.md

트랙: KR/US docs/work_phases/ 만 SSOT. bitget/ Downloads 복사본은 참고만.
충돌 시 00_SESSION_SYNC §3 스냅샷이 최신이면 그걸 따른다.

[여기에 이번 창 목적 한 줄, 예: "CAT-E-BARS-01 OUTBOX 검증"]
```

### Cursor (구현·조사 창)

```text
역할: Cursor Lead Engineer.

자동 적용: .cursorrules + .cursor/rules/ (세션 SSOT·Handoff·트랙)

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CLAUDE_TO_CURSOR.md (Handoff) 또는 CURSOR_TO_CLAUDE (OUTBOX)

부팅 문구 전문: docs/work_phases/17_Cursor_세션_부팅_가이드.md §3

한 세션 = sub-phase 하나. 세션 종료 전: 05 + §3 + NEXT_ACTION + CURSOR_TO_CLAUDE.

[sub-phase ID · 모드: 구현/조사/Ops/상태점검]
```

### Bitget Cursor (Track B)

```text
Track B — bitget/ only.
1) bitget/docs/work_phases/lanes/ACTIVE_LANES.md — 내 레인
2) lanes/<LANE_ID>/NEXT_ACTION.md · CURSOR_TO_CLAUDE.md
3) CLAUDE_TO_CURSOR.md (내 sub-phase Handoff만)
4) 16_멀티창_레인_프로토콜.md — 다른 레인 MD 덮어쓰기 금지
첫 메시지: 「레인: LANE_XXX · sub-phase: …」
```

### 텔레그램 → Cursor / Claude (deploy_watch · IV_OBS)

```text
[DEPLOY_WATCH] 또는 [IV_OBS] 수신 시:
1) ---CURSOR--- 아래 블록 전체를 Cursor 새 채팅 첫 메시지로 붙여넣기
2) Cursor가 cursor_action 해석 → CURSOR_TO_CLAUDE OUTBOX (Claude 검증 필요 시)
3) Claude Pro는 docs/work_phases/ 만 읽음 — 텔레그램 직접 접근 없음

deploy_watch cursor_action SSOT:
  NONE · REPORT_TO_CLAUDE · INVESTIGATE · BLOCK_F_RETIRE_02_DEPLOY
  INVESTIGATE_BEAR_UNDERDOG_TAG · OBSERVE_BEAR_UNDERDOG_L2
```

**VPS 파일 SSOT** (SSH 가능 시): `deploy_watch_latest.json` · `iv_observation_latest.json`

---

## 5. 충돌·중복 발견 시 (AI 행동 규칙)

1. **`NEXT_ACTION`과 `05_진행로그`가 다르면** → `05`의 **해당 sub-phase 최신 섹션** 우선, `NEXT_ACTION`을 맞춘다.  
2. **`CLAUDE_TO_CURSOR` 상단 "현재"와 `00_SESSION_SYNC` §3이 다르면** → **§3을 먼저 디렉터에게 질문** ("어느 쪽이 최신인가?")  
3. **Downloads에 `CURSOR_TO_CLAUDE (1).md` 등** → 레포 `docs/work_phases/`에 merge된 뒤에만 신뢰.  
4. **두 Cursor 창이 Bitget을 동시에** → `bitget/.../16_멀티창_레인_프로토콜.md` · 본문은 `lanes/<ID>/`만 · 루트 MD는 표/인덱스만.  
5. **세션 종료 시** §3의 `앵커 ID`를 `SYNC-YYYY-MM-DD-B`처럼 bump (같은 날 두 번째 창이면 B, C…).

---

## 6. Cursor / Claude 세션 종료 체크리스트 (3줄)

- [ ] `05_진행로그` 해당 sub-phase 섹션 갱신  
- [ ] **`00_SESSION_SYNC.md` §3 스냅샷** 갱신  
- [ ] `NEXT_ACTION.md` 디렉터 할 일 1장 갱신  
- [ ] (구현 시) `CURSOR_TO_CLAUDE` OUTBOX 또는 (설계 시) `CLAUDE_TO_CURSOR` Handoff  
- [ ] 디렉터 응답 **마지막 줄** = 본 파일 상단 HOLD (해제될 때까지)  

---

## 7. Bitget 트랙 분리

| | KR/US (주식) | Bitget |
|--|--------------|--------|
| SSOT 폴더 | `docs/work_phases/` | `bitget/docs/work_phases/` |
| 세션 앵커 | **본 파일** | `bitget/docs/work_phases/README.md` → 주식 앵커 참조 |
| 혼합 금지 | Handoff에 `bitget/` 경로 섞지 않기 (명시적 BG sub-phase 제외) | |
