# CURSOR → CLAUDE (검증 OUTBOX)

> ⛓ **세션 SSOT** → [`00_SESSION_SYNC.md`](00_SESSION_SYNC.md) · Cursor는 본 파일 + `05_진행로그` append  
> `Downloads/*` 복사본은 merge 전까지 **본 경로 우선**.

> **갱신**: 2026-08-20 · **OPS-LIQ-TG-01** Claude OK · CLOSED · 앵커 `SYNC-2026-08-20-C`

---

## OUTBOX — [CAT-J] OPS-LIQ-TG-01 · `[LIQ_BAND]` 패널 구현 · 2026-08-20

| 항목 | 내용 |
|------|------|
| **sub-phase** | **OPS-LIQ-TG-01** |
| **status** | **`CLOSED`** (Claude OK 2026-08-20) |
| **위험도** | 🟢 Low |
| **앵커** | `SYNC-2026-08-20-B` |

### DoD 체크

| # | 기준 | 결과 |
|---|------|------|
| 1 | `[LIQ_BAND]`가 daily digest `[OBS_HOLD]` 아래 | ✅ `format_north_star_digest_html` additive |
| 2 | `scan_funnel_drop_event` read-only · LIQUIDITY 판정/threshold diff 0 | ✅ |
| 3 | n&lt;20 → `NONE`, 숫자판정 문구 없음 | ✅ 테스트 (a) |
| 4 | 5일 연속·과반 0.50 · 표본부족 스킵(리셋아님) | ✅ 테스트 (c)(d) |
| 5 | `LIQ_BAND_ENABLED=False` → 패널 미출력 | ✅ 테스트 (e) |
| 6 | 원장·config_kv 비접촉 · 이력 side-file | ✅ `liq_band_history.json` |
| 7 | 테스트 + 05 + OUTBOX | ✅ |

### 파일

| 파일 | 역할 |
|------|------|
| `reports/liq_band_panel.py` | 4함수 + Phase1 버킷 헬퍼 추출 |
| `dual_north_star_telegram.py` | OBS_HOLD 아래 additive 배선만 |
| `scripts/ops_liq_fork_01_quality_band_phase1.py` | 버킷 헬퍼 import 재사용 |
| `tests/test_liq_band_panel_ops_liq_tg_01.py` | (a)~(f) |

### cursor_action

`NONE` · `OBSERVE_LIQ_BAND` · `PHASE2_CANDIDATE` — Phase2 자동 착수 **없음** (텔레그램 호출만)

### 테스트

`pytest tests/test_liq_band_panel_ops_liq_tg_01.py tests/test_obs_hold_telegram.py` → **20 passed**

### 디렉터 → Claude 한 줄

```text
docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 OPS-LIQ-TG-01 OUTBOX 검증. OK면 CLAUDE_TO_CURSOR.md에. 채팅 말고 파일에.
```

---

[Ask] OPS-LIQ-TG-01 · **텔레그램 Phase2/품질밴드 전용 알람** · 2026-08-20

| 항목 | 내용 |
|------|------|
| **sub-phase** | **OPS-LIQ-TG-01** (가칭 · Claude 확정) |
| **요청자** | 디렉터 직접 — Cursor/Claude는 PC 켤 때만 움직임 → **일일 퀀트 크론이 텔레그램으로 깨워야 함** |
| **status** | `WAIT_CLAUDE_HANDOFF` |
| **위험도** | 🟢 Low (관측·리포트만 · 임계/게이트 **금지**) |

### 디렉터 의도 (쉬운 말)

- Cursor·Claude는 **자동으로 안 알려줌** (채팅 열어야 함)
- 이미 도는 **North Star 19:30** 같은 시스템이 매일 재서  
  **「지금은 지켜보기」 vs 「Phase2 논의하려고 컴퓨터 켜라」** 를 텔레그램으로 보내 달라

### Cursor 엔지니어 제안 (Handoff용 · 구현 전)

| 항목 | 제안 |
|------|------|
| 위치 | 기존 `factory.sh --north-star-digest daily` **19:30**에 패널 추가 (새 cron 금지) |
| 태그 | `[LIQ_BAND]` 또는 `[품질밴드]` (Claude 확정) |
| 계측 | Phase1과 동일 계열 read-only (LIQUIDITY 표본 → dollar-vol 백분위 분포) |
| 출력 | 쉬운 한국어 1~3줄 + `---CURSOR---` 복붙 (기존 OBS_HOLD 패턴 재사용) |
| cursor_action | `OBSERVE_LIQ_BAND` / `PHASE2_CANDIDATE` / `NONE` (이름 Claude) |
| **금지** | LIQUIDITY 임계 변경 · 잡주 완화 · Critical · 임의 정책 컷라인 창조 |
| 분기 숫자 | **Phase2 후보 판정 임계는 디렉터/Claude 지정** — Cursor 임의 금지 (Rule 5) |
| 성능 | 유니버스 전수 스캔이 무거우면 **캐시·샘플·주기(매일/격일)** Claude와 합의 |

### 기존과 차이

| 장치 | 역할 | Phase2 품질밴드? |
|------|------|------------------|
| `[OBS_HOLD]` · n≥20 → `RECALL_FORK` | 북극성 **목표/갈림길** | ❌ 유동성 Phase2 아님 |
| **신규 `[LIQ_BAND]`** | OPEN≈0·LIQUIDITY 품질밴드 | ✅ 이번에 요청 |

### 디렉터 → Claude 한 줄

```text
docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 Ask OPS-LIQ-TG-01 읽고 Handoff를 CLAUDE_TO_CURSOR.md에. 채팅 말고 파일. 임계 변경 금지·텔레그램 일보 패널만.
```

---

## OUTBOX — [CAT-C] OPS-LIQ-FORK-01 Phase1 **VPS 실측 Done · (B) 관측연장** · 2026-08-19

| 항목 | 내용 |
|------|------|
| **sub-phase** | OPS-LIQ-FORK-01 Phase 1 |
| **코드** | `scripts/ops_liq_fork_01_quality_band_phase1.py` |
| **비접촉** | threshold · config_kv · funnel 판정 ✅ |
| **HINT** | **`LOW_TAIL_CONCENTRATION` → (B) observe-extend** |
| **status** | Claude OK 대기 · **Phase2 착수 금지** |

### Percentile 분포표 (VPS · STALL_SINCE=2026-08-17 15:10 · N=20/시장)

| 시장 | universe | low(≤33) | mid | high(≥67) | mid+high | low share |
|------|----------|----------|-----|-----------|----------|-----------|
| **KR** | 2411 | 3 | 15 | 2 | **85.0%** | 15.0% |
| **US** | 6515 | **20** | 0 | 0 | **0.0%** | **100.0%** |
| **합산** | — | **23** | 15 | 2 | — | 합산 HINT = **LOW_TAIL** |

합산 `PHASE1 BRANCH HINT`: `LOW_TAIL_CONCENTRATION -> (B) observe-extend`  
`OBS-HOLD: no config/cutoff/threshold changes applied.`

### 해석 (Cursor · 판정 아님)

- KR만 보면 mid+high 우세이나, **US 표본 100% low** → 합산 HINT는 (B)
- 게이트 “고장” 가설은 이전 Stall OK와 정합 · **잡주 개방형 완화 금지 유지**
- Phase2는 **텔레그램 전용 장치(위 Ask) + 추후 증거/디렉터 소집** 전까지 보류

### 디렉터 3줄

1. Phase1 = **(B) 관측연장** (임계 안 건드림).  
2. Phase2 알람은 **아직 텔레그램에 없음** → 위 Ask로 Claude Handoff 요청.  
3. 잡주 개방 완화 계속 금지.

---

## OUTBOX — [Ask] OPS-LIQ-FORK-01 · OPEN=0 vs 북극성·목표 · **갈림길 Handoff 요청** · 2026-08-19

| 항목 | 내용 |
|------|------|
| **sub-phase** | **OPS-LIQ-FORK-01** (가칭 · Claude 확정) |
| **status** | `WAIT_CLAUDE_HANDOFF` · Cursor **구현 금지** (정책 미정) |
| **앵커** | `SYNC-2026-08-19-F` |
| **프로젝트** | KR/US Dual-Screener Claude Pro (**Bitget 아님**) |

### 한 줄 (Claude에 넘길 핵심)

> **OPEN=0이 매매표본·수익률 증명을 막는다. LIQUIDITY는 정상(OK). 디렉터 유니버스=대형주·정상 유동 종목(작전주·잡주·동전주 제외). 갈림길: (A′)품질밴드 재조정 Handoff vs (B)관측연장 vs (C)추가실측 — 헐겁게 열어 잡주 넣는 완화는 금지.**

### 디렉터 유니버스 헌법 (2026-08-19 추가 · SSOT)

| 선호 | 내용 |
|------|------|
| ✅ 허용·목표 | 삼성·SK하이닉스급 **대형주** + **어느 정도 정상적인 유동성** 종목으로 데이터·수익률 관측 |
| ❌ 금지 방향 | **작전주 · 잡주 · 동전주**를 넣으려고 임계를 헐겁게 푸는 것 |
| 함의 | 현재 LIQUIDITY(저가·거래량 컷)의 **취지(쓰레기 배제)는 유지**. 문제는 “정상 종목까지 과도 차단 → OPEN=0 → 목표 표본 공백” |

### Cursor 엔지니어 브리핑 (최선 아이디어 · 1~2줄)

- **(A) 전면 완화**는 디렉터 의도와 충돌 → **기각 권고**.
- **최선 = (A′) 품질 밴드**: 가격 바닥(동전주 차단)은 **유지/강화 가능**, 거래량 floor만 **정상 중·대형 유동**이 survivors에 남도록 **계측 후 제한 조정** + 샘플에 작전·저가 비중 가드. Critical/Kelly/MDD 비접촉.
- (B)는 목표 페이스를 **의도적으로 미룸** — 디렉터가 수용할 때만.
- (C)는 A′ 전에 “survivors>0인데 try_add만 거절”인지 1회 분리 — 싸게 리스크 줄임.

### 왜 지금 Ask인가

| 층 | 상태 | 북극성·목표에 미치는 영향 |
|----|------|---------------------------|
| daily **n** (일보 스냅샷) | 크론 돌면 쌓일 수 있음 | n≥20 시계는 갈 수 있음 |
| **forward 매매** | OPEN≈0 | CAGR·페이스 **증명 불가** |
| LIQUIDITY | GATE WORKING · OK | 취지 OK · **과도 차단 가능** |
| 디렉터 품질 헌법 | 신규 | 잡주 개방형 완화 **금지** |

### 선행 확정 (뒤집지 말 것)

1. OPS-OPEN-STALL-01 OK — LIQUIDITY 100% · cutoff 배제  
2. OPS-LIQUIDITY-STALL-01 OK — (b) 87.5% · GATE WORKING  
3. NS-DIAG-DASH-01 OK — OPEN=0 ≠ 자동🔴  
4. 「시스템 비상 켈리 0.2%」= 리포트 문구 · 전면차단 아님  

### Claude에게 요청 (택1 + Handoff)

1. **갈림길 VERDICT** (하나만):
   - **(A′)** **품질 밴드** 유동성 재조정 Handoff — 동전주·작전주 유입 가드 명시 · 정상 유동 종목만 survivors↑ · CAT-B/C · Critical 비접촉 · 롤백·관측 KPI(OPEN/주·잡주 비율)  
   - **(B)** 관측연장 — 임계 유지 · 목표 페이스 지연 **수용** 문구  
   - **(C)** A′ 전 read-only 1회 — survivors>0 vs try_add 거절 분리  
   - ~~(A) 전면 완화~~ — **디렉터 헌법상 비권고**
2. 결과 → **`CLAUDE_TO_CURSOR.md`** CAT-HANDOFF  
3. mega_trend·목표하향·CAGR 숫자 변경 = **밖**

### 디렉터 → Claude 복붙 (전문)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (앵커 SYNC-2026-08-19-F)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 OUTBOX (OPS-LIQ-FORK-01)

@CAT-B @CAT-C @CAT-MAP @CAT-HANDOFF_템플릿
@CAT-D
(임계 키 시) @CAT-CONSTANTS

한 줄: OPEN=0이 매매표본·수익률 증명을 막는다. LIQUIDITY는 정상(OK). 유니버스=대형·정상유동(작전·잡·동전주 제외). 갈림길 (A′)품질밴드 재조정 vs (B)관측연장 vs (C)추가실측 — 잡주 개방형 완화 금지.

요청: VERDICT 택1 + CLAUDE_TO_CURSOR.md Handoff. 채팅 말고 파일.
Bitget 프로젝트 쓰지 말 것.
```

### Knowledge / 업로드

| | |
|--|--|
| **프로젝트** | KR/US Dual-Screener (**Bitget ❌**) |
| **CAT 있음** | 업로드 X · `@`만 |
| **없으면** | `CAT-MAP` · `CAT-B` · `CAT-C` · `CAT-HANDOFF` · `CAT-D` · (선택) `CAT-CONSTANTS` |
| **올리지 말 것** | DB · bitget · 전체 zip · 스크린샷 대량 |

### 디렉터 3줄

1. 잡주·동전주 넣자고 푸는 게 **아님** — **정상 종목으로 표본·목표**.  
2. Claude: **(A′)/(B)/(C)** + 파일 Handoff.  
3. Cursor: Handoff 전 임계 **손대지 않음**.

---

## CLOSE — NS-DIAG-DASH-01 **Claude OK** · 2026-08-19

[CAT-J] 검증 OK · bitget 0 · OPEN=0≠자동🔴 · 신규 Alpha Handoff 없음 · OBS-HOLD 유지.  
디렉터: VPS `update_factory.sh` → 19:30 쉬운판 육안.

---

## OUTBOX — NS-DIAG-DASH-01 · Track A 진단형 쉬운판 · 2026-08-19

| 항목 | 내용 |
|------|------|
| **sub-phase** | NS-DIAG-DASH-01 (Ops-lite) |
| **status** | **Claude OK · CLOSED** (was `WAIT_CLAUDE_OK`) |
| **범위** | Track A `[쉬운판]` → Bitget식 4칸(🟢/🔴/🟡/⬜) **건강 진단** · `bitget/**` **0** |
| **코드** | `dual_north_star_ledger.py` (`_forward_book_counts_a` · `read_deploy_watch_health` · `collect_track_a_health`) · `dual_north_star_telegram.py` (`build_goal_dashboard`/`format_goal_dashboard_html`) |
| **규칙** | OPEN=0 ≠ 자동🔴 · CLOSED=0/MDD초과/NAV오류/ledger n=0/watch BREAK=구멍 · gate·obs=🟡 |
| **테스트** | `tests/test_obs_hold_telegram.py` + ledger **19 passed** |
| **문서** | `12_듀얼북극성` §5 NS-DIAG-DASH |
| **금지 유지** | Alpha · mega_trend · 목표하향 · LIQUIDITY 완화 · C-2/실전 |

**Ask:** (해소) Claude OK · VPS 배포만 남음.

---

## CLOSE — OPS-LIQUIDITY-STALL-01 **Claude OK** · 2026-08-19

GATE WORKING 승인 · 임계 완화 Handoff **열지 않음** · OBS-HOLD 유지.  
소스: `Downloads/CLAUDE_OK_OPS-LIQUIDITY-STALL-01.md` → `CLAUDE_TO_CURSOR.md` 랜딩.

---

## OUTBOX — [CAT-B] OPS-LIQUIDITY-STALL-01 **VPS Done · GATE WORKING** · Claude 검증 · 2026-08-19

| 항목 | 내용 |
|------|------|
| **sub-phase** | OPS-LIQUIDITY-STALL-01 |
| **status** | `WAIT_CLAUDE_OK` · OBS-HOLD · **threshold/cutoff 변경 0** |
| **앵커** | `SYNC-2026-08-19-B` |
| **VERDICT** | **GATE WORKING + low-liquidity concentration** (c+d=**5%** &lt; 30%) |
| **DB** | `/var/lib/quant-factory/data/market_data.sqlite` · STALL_SINCE=`2026-08-17 15:10` · N=20/시장 |

### (a)(b)(c)(d) 집계 (KR+US · n=40)

| class | 의미 | n | share |
|-------|------|---|-------|
| **(a)** | 실가격 컷 정상 | 3 | 7.5% |
| **(b)** | 실거래량/대금 컷 정상 | **35** | **87.5%** |
| **(c)** | Volume/필드 이상 | 1 | 2.5% |
| **(d)** | 게이트 통과인데 LIQUIDITY 로그 | 1 | 2.5% |

| market | a | b | c | d | defect (c+d) |
|--------|---|---|---|---|--------------|
| KR | 0 | 19 | 1 (`006380`) | 0 | 5% |
| US | 3 | 16 | 0 | 1 (`BZUN`) | 5% |

### 해석

1. **본체 = (b)** — 저유동성(5일 평균 Volume &lt; floor)로 게이트가 **정상 발화**. 데이터 파이프라인 붕괴 가설 **기각**(30% 미달).
2. **(c)/(d)** 각 1건 — 잡음 수준. 즉시 완화·코드 수정 **금지**(Handoff).
3. OPEN 0 / 청산 0 = **유니버스가 유동성 컷에 걸림**이지 cutoff/DNA/인프라 RED 본체 아님 (선행 CLASS a와 정합).
4. Step3 계측 공백 — `L-DATA-ALARM-01` 백로그만 (본 건 비범위).

### Claude에게 요청

1. 본 OUTBOX DoD 검증 → OPS-LIQUIDITY-STALL-01 **Claude OK**?
2. 후속: **관측유지**(임계 완화 Handoff 없음) vs 별도 “유동성 유니버스/필터 정책” 논의 — OBS-HOLD·Alpha 금지와 충돌 여부.
3. (d) BZUN 1건 / (c) 006380 1건 — 추적 sub 개설 **불필요** 권고(비중 5%).

### 디렉터 → Claude 복붙

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (SYNC-2026-08-19-B)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 (OPS-LIQUIDITY-STALL-01 VERDICT)

@CAT-B @CAT-C @CAT-MAP

요청: VPS (a~d) 표·GATE WORKING VERDICT 검증. OK면 닫기.
임계 완화 Handoff 열지 여부만 한 줄. 채팅 말고 파일에.
```

### 디렉터 3줄

1. **87.5% = 거래량 컷 정상** · 게이트 고장 아님.
2. 결함 가설(c+d) **5%** → 완화/패치 **안 함**.
3. Claude OK 후 OBS-HOLD 유지 · OPEN은 유동성 환경 이슈.

---

## OUTBOX — [CAT-B] OPS-LIQUIDITY-STALL-01 **스크립트 Done · VPS 실행 대기** · 2026-08-19

| 항목 | 내용 |
|------|------|
| **sub-phase** | OPS-LIQUIDITY-STALL-01 |
| **status** | 스크립트 추가 · **VPS (a~d) 실행 잔여** · OBS-HOLD |
| **선행** | OPS-OPEN-STALL-01 **Claude OK** · CLASS (a) |
| **코드** | `scripts/ops_liquidity_stall_01_diagnosis.py` only |
| **비접촉** | scanner_funnel · fetcher · schema · config_kv · threshold |
| **백로그** | Step3 계측 공백 → `L-DATA-ALARM-01` (sub 개설 X) |

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | 신규 진단 스크립트 1개 | ✅ |
| 2 | 기존 정책/fetcher 미수정 | ✅ |
| 3 | VPS (a)(b)(c)(d) 표 + VERDICT | ⏳ |
| 4 | 임계 변경 0 | ✅ (강제) |

### VPS

```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
set -a && source .env && set +a
python3 scripts/ops_liquidity_stall_01_diagnosis.py
```

### 디렉터 3줄

1. OPEN-STALL **Claude OK** · LIQUIDITY 본체 확정.
2. CAT-B 4분류 스크립트 준비 · **VPS 실행만** 남음.
3. (c)/(d)≥30%여도 **즉시 완화 금지** — Claude 후속 Handoff.

---

## OUTBOX — [CAT-C] OPS-OPEN-STALL-01 **VPS 진단 Done · CLASS (a)** · Claude 검증 · 2026-08-18

| 항목 | 내용 |
|------|------|
| **sub-phase** | OPS-OPEN-STALL-01 |
| **status** | `WAIT_CLAUDE_OK` (진단 DoD) · **후속** CAT-B Handoff 대기 · OBS-HOLD · cutoff **비접촉** |
| **앵커** | `SYNC-2026-08-18-G` |
| **분류** | **(a) LIQUIDITY 압도** → CAT-B 유동성/OHLCV 재의심 |
| **비접촉** | schema · config_kv · cutoff · Critical · 정책 0 |

### Step 0 — 적재 OK

| market | n | min_ts | max_ts | days |
|--------|---|--------|--------|------|
| KR | 550 | 2026-08-11 … | 2026-08-18 … | 6 |
| US | 600 | 2026-08-10 … | 2026-08-17 … | 6 |
| **TOTAL** | **1150** | | | |

→ C-FUNNEL-02 결선 **정상** (0행 아님). DB=`/var/lib/quant-factory/data/market_data.sqlite`

### Step 1 — stall window (`since ≈ 2026-08-17 15:10`, contiguous survivors=0)

| market | reason | n | share |
|--------|--------|---|-------|
| KR | **LIQUIDITY** | 150 | **100%** |
| US | **LIQUIDITY** | 100 | **100%** |

DNA_FAIL / 기타 = **0** (이 윈도우의 `drop_event` 기준).

### Step 2 — near-miss (scored)

`(no scored near-miss rows in window)`

→ LIQUIDITY는 DNA/score **이전** 컷이라 `final_score`/`cutoff` 거리 샘플이 없음. **(b) cutoff 과타이트는 이 윈도우 원인으로 배제.**

### Step 3 — 예외 슬롯 KR `2026-08-17 14:15`

`drop_event` **0행** (prefix·hour 모두). snapshot에 survivors=5는 있었으나 near-miss 이벤트 미적재 — 대조 불가(계측 공백, 판정 로직 오류 단정 금지).

### CLASSIFICATION

**(a) LIQUIDITY dominant → CAT-B liquidity/OHLCV recheck**

### Cursor 해석 (엔지니어 1~2줄)

- `drop_event`는 near-miss **샘플**(cap=50/슬롯)이라 전 유니버스 탈락 전수는 아님. 다만 stall 구간 샘플이 **100% LIQUIDITY**이므로 본체 병목은 DNA/cutoff가 아니라 **유동성 게이트** 쪽이 맞음.
- 직전 Ops에서 `KR_005930`·KOSPI는 신선 → **벤치마크·대형주 동결은 아님**. 다수 종목 Volume 미달 / 저가 컷 / Volume 필드 이상 중 무엇인지 CAT-B 진단 Handoff 필요.
- 코드 기준 LIQUIDITY: KR Close&lt;1000 · US Close&lt;0.5 · 또는 5일 평균 Volume &lt; floor (US는 ~$300k 대금 환산).

### Claude에게 요청

1. 본 OUTBOX DoD 검증 → OPS-OPEN-STALL-01 **Claude OK** 여부.
2. 후속 **CAT-B** Handoff 1개 (가칭 `OPS-LIQUIDITY-STALL-01` 또는 Claude 명명) — read-only로 Volume/가격 분포·LIQUIDITY 발화 원인 규명. **cutoff 완화·config_kv 금지** 유지 여부 명시.
3. L-DATA-ALARM-01 · Alpha — 계속 후순위 / OBS-HOLD.

### 디렉터 → Claude 복붙

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (SYNC-2026-08-18-G)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 (OPS-OPEN-STALL-01 CLASS a)

@CAT-C @CAT-B @CAT-MAP @CAT-HANDOFF_템플릿

요청: 진단 OUTBOX 검증(OK?) + CAT-B 후속 Handoff 1개.
cutoff/config 비접촉 유지. 채팅 말고 CLAUDE_TO_CURSOR.md.
```

### 디렉터 3줄

1. Step 0 OK · stall 구간 drop **100% LIQUIDITY** → **(a)**.
2. DNA/cutoff **아님** · 정책 변경 없음.
3. Claude: OK + **CAT-B** 다음 Handoff.

---

## OUTBOX — [CAT-C] OPS-OPEN-STALL-01 **스크립트 Done · VPS 실행 대기** · 2026-08-18

| 항목 | 내용 |
|------|------|
| **sub-phase** | OPS-OPEN-STALL-01 |
| **status** | 로컬 스크립트 추가 · **VPS 실행·분류 (a)(b)(c) 잔여** · `WAIT_CLAUDE_OK` (실행 로그 후) |
| **앵커** | `SYNC-2026-08-18-F` |
| **코드** | `scripts/ops_open_stall_01_diagnosis.py` only |
| **비접촉** | schema · config_kv · cutoff · Critical |
| **NS-DIR-DASH** | **Claude OK 2026-08-18** · VPS pull·19:30 잔여 |

### DoD (Handoff)

| # | 기준 | 결과 |
|---|------|------|
| 1 | Step 0~3 SELECT 스크립트 | ✅ 로컬 추가 |
| 2 | schema ALTER 0 | ✅ |
| 3 | Step 0+ 실행 로그 OUTBOX | ⏳ **VPS만** (로컬 DB에 `scan_funnel_drop_event` 테이블 없음 → exit 2 예상) |
| 4 | (a)(b)(c) 분류 | ⏳ VPS 후 |

### VPS 실행

```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
set -a && source .env && set +a
python3 scripts/ops_open_stall_01_diagnosis.py
```

로그 전체를 본 OUTBOX 아래에 append → Claude 검증.

### 디렉터 3줄

1. Handoff 반영 · 진단 스크립트만 추가 (정책 0).
2. **VPS에서 Step 0부터 실행** — 0행이면 결선 먼저.
3. NS-DIR-DASH Claude OK · pull·19:30만 잔여.

---

## OUTBOX — [Audit] OPS-OPEN-STALL-01 · survivors≈0 → OPEN 0 · **Handoff 요청** · 2026-08-18

| 항목 | 내용 |
|------|------|
| **sub-phase** | **OPS-OPEN-STALL-01** (가칭 · Claude가 ID 확정) |
| **status** | `WAIT_CLAUDE_HANDOFF` · 운영 `OBS-HOLD` 유지 · **코드 구현 금지(Cursor)** |
| **앵커** | `SYNC-2026-08-18-E` |
| **프로젝트** | KR/US Dual-Screener Claude Pro (**Bitget 아님**) |
| **@멘션** | `@CAT-C` `@CAT-MAP` `@CAT-HANDOFF` · (원장 문맥) `@CAT-D` · (알람 분리 시) `@CAT-L` |
| **선행 병렬** | NS-DIR-DASH-01 여전히 `WAIT_CLAUDE_OK` — **본 OUTBOX가 포커스**. 쉬운판 OK는 이어서/별도 |

### 한 줄 결론

- **인프라 RED(캔들)** = `factory.sh --data-refresh` 로 **해소** (일시 복구).
- **본체** = 스캔 `survivors≈0` → LIVE `OPEN=0` → 청산·리포 진입/청산 기록 없음.
- `update_factory` / refresh **반복만으로는 영구 해결 안 됨** → **영구 Handoff 1개** 요청.

### VPS 실측 (2026-08-18 · `/var/lib/quant-factory/data/market_data.sqlite`)

| 단계 | 결과 |
|------|------|
| 배포 직후 health | KR **RED** (캔들 08-14 · lag 2) · US YELLOW → `post_update_data_health` FAIL |
| `--data-refresh` | OK · KR 2368 / US 6335 · exit=0 |
| 벤치마크 | `KR_KOSPI_IDX=2026-08-18` · `US_SPY=2026-08-17` |
| 종목 샘플 | `KR_005930` 최신 **08-18** · Close/Volume 정상 → **종목 OHLCV 정체 아님** |
| health 재검사 | KR/US **YELLOW** · `OK data path + candle freshness` |
| exit 워터마크 | KR **08-05** (lag 9) · US **07-31** (lag 11) — 청산 공백만 |
| `forward_trades` | KR: CLOSED_LOSS 171 · CLOSED_WIN 11 · **OPEN 0** / US: CLOSED_LOSS 111 · CLOSED_WIN 23 · **OPEN_SHADOW 8** · **LIVE OPEN 0** |
| `scan_funnel_snapshot` | 최근 KR/US 슬롯 **survivors=0** · 예외: `2026-08-17 14:15` KR survivors=**5** (pass≈0.20%) |

### 배제 (이전 VPS·코드 대조 · 8/16~)

| 가설 | 판정 |
|------|------|
| LOCKDOWN / GLOBAL_CIRCUIT | **아님** (당시 band=DEFENSE·block=False) |
| 종목 DB 동결 → LIQUIDITY | **기각** (005930 신선) |
| 인버스가 롱 전면차단 | **아님** (스나이퍼 내부 메시지 · LIVE OPEN 0) |
| 리포트 버그로 기록 누락 | **아님** — 원장에 OPEN/CLOSED 신규가 없음 |
| 스캔 텔레그램 = 진입 | **오해** — 알림과 `try_add`/survivors는 별 층 |

### Claude에게 요청

1. **OBS-HOLD / Alpha 금지**와 본 건 충돌 여부 (관측만 vs 퍼널 영구 패치 Go).
2. **영구 sub-phase 1개** Handoff → `CLAUDE_TO_CURSOR.md` (`CAT-HANDOFF` 형식). 범위 예:
   - 퍼널 탈락 사유 SSOT 확정 (LIQUIDITY vs DNA/컷오프 vs try_add 거부)
   - 재발 방지 (관측 알람 / 임계 완화 / 진단 스크립트 — **택1·최소 diff**)
   - Critical(F/G/MDD)·config_kv 라이브 금지 여부 명시
3. **인프라 RED 재발 알람**(`data_refresh` 실패·캔들 lag)은 **별 트랙(CAT-L)** 로 둘지, 같은 Handoff에 넣을지 **분리 권고**.
4. NS-DIR-DASH-01: 본 건과 무관 — OK만 남았으면 한 줄로 닫아도 됨.

### 디렉터 → Claude 복붙 (최소)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (앵커 SYNC-2026-08-18-E)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 OUTBOX (OPS-OPEN-STALL-01)

@CAT-C @CAT-MAP @CAT-HANDOFF_템플릿
(필요 시 @CAT-D · 알람 분리면 @CAT-L)

요청: 최상단 OUTBOX 실측 기준으로 survivors≈0→OPEN 0 영구 Handoff 1개.
채팅 말고 CLAUDE_TO_CURSOR.md에 써 줘.
```

### 디렉터 3줄

1. 시세 RED는 refresh로 끊김 · **본체는 퍼널 survivors≈0**.
2. Claude: 본 OUTBOX → **Handoff 1개** (영구).
3. Cursor는 Handoff 오기 전 **구현 금지**.

---

## OUTBOX — [Ops-lite] NS-DIR-DASH-01 **완료** · Claude 검증 요청 · 2026-08-18

| 항목 | 내용 |
|------|------|
| **sub-phase** | NS-DIR-DASH-01 |
| **status** | `WAIT_CLAUDE_OK` · 운영 `OBS-HOLD` 유지 |
| **앵커** | `SYNC-2026-08-18-D` |
| **범위** | 기존 19:30 North Star 일보 **맨 앞**에 `[쉬운판]` 대시보드 |
| **코드** | `dual_north_star_telegram.py` (`build_goal_dashboard` / `format_goal_dashboard_html`) |
| **비접촉** | Critical · config_kv · 신규 cron · mega_trend/목표하향 · CAGR 확정 로직 |

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | daily HTML 상단에 `[쉬운판]` · ✅/⏳/⚠️/📋 | ✅ |
| 2 | n/20 진행 · 관측문구 · 확정 판정 문구 없음 | ✅ |
| 3 | weekly 패널 생략 | ✅ (daily only) |
| 4 | 테스트 | ✅ **14 passed** |
| 5 | 12·hooks 문서 반영 | ✅ |

### 디렉터 3줄

1. 매일 일보 **맨 위**에 쉬운 체크·대시보드.
2. VPS pull 후 19:30 확인.
3. Claude: 본 OUTBOX 검증.

---

## CLOSE — ROADMAP-SYNC-01 **Claude OK** · 2026-08-18

DoD 4/4 VERDICT OK · OBS-HOLD 유지 · 신규 Handoff 없음.  
다음 실질 Go = VPS daily n≥20.

---

## OUTBOX — [Ops-lite] ROADMAP-SYNC-01 **완료** · Claude 검증 요청 · 2026-08-18

| 항목 | 내용 |
|------|------|
| **sub-phase** | ROADMAP-SYNC-01 |
| **status** | `WAIT_CLAUDE_OK` · 운영 `OBS-HOLD` 유지 |
| **앵커** | `SYNC-2026-08-18-B` |
| **선행** | ROADMAP-AUDIT-01 VERDICT · NS-OBS-TG-01 **Claude OK** |
| **코드 diff** | **0** |

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | `15_POST_RP1` 단계2 → 🔴 동결(규칙1) + 각주 | ✅ 단계표·§단계2·갱신로그 |
| 2 | `00_전체현황판` Phase3 → 동결 + C-1 No-Go 각주 | ✅ |
| 3 | 다중 시계 4행 + 「RP-1 Pass ≠ 40~70%」 | ✅ 마스터 §0.2 + 현황판 목표수치 아래 |
| 4 | 코드·config·cron 비접촉 | ✅ |

### 비고

- NS-OBS-TG-01: Claude OK 반영(현황판). VPS pull·19:30 육안은 디렉터 Ops 잔여.
- Defer/3택 카드/Alpha Go: Handoff대로 **미착수**.

### 디렉터 3줄

1. 문서 3건 라벨 정정 Done — 코드 0.
2. Claude: 본 OUTBOX DoD 확인.
3. 실질 Go는 여전히 n≥20.

---

## OUTBOX — [Audit] ROADMAP-AUDIT-01 · KR/US 카테고리·로드맵·목표 달성 경로 점검 · Claude 상의 요청 · 2026-08-18

| 항목 | 내용 |
|------|------|
| **sub-phase** | ROADMAP-AUDIT-01 (조사·문서 · **코드 0**) |
| **요청** | 디렉터: 「카테고리·마스터로드맵·목표(40~70%/MDD10%) 작업순서가 빠짐없는지 · 보완점 · Claude와 재상의」 |
| **앵커** | `SYNC-2026-08-18-A` |
| **운영 전제** | **OBS-HOLD 유지** · mega_trend/목표하향/소진레버 **착수 금지** · Alpha Handoff **지금 열지 말 것** |
| **병행** | NS-OBS-TG-01 `WAIT_CLAUDE_OK` (일보 복붙) — 본 Audit과 별개 검증 |

### 0. 한 줄 결론 (Cursor 엔지니어)

**목표 숫자는 일관. CAT A~Q는 빠짐없음. 문제는 “다음 코딩 순서” 문서가 현실(OBS-HOLD·레버 소진)과 어긋난 것.** CAGR 달성은 A~D 한 방 패치가 아니라 **L2(forward+G2) 표본 + n≥20 갈림길**이 본체. 지금은 **문서 SSOT 동기화**가 우선, 신규 Alpha Go는 재소집 후.

---

### A. 목표 SSOT — 일관성

| 출처 | 내용 | 판정 |
|------|------|------|
| `00_마스터_로드맵` | MDD **−10%** · CAGR **40~70%** · MDD 우선 | ✅ |
| `00_전체현황판` | 동일 · 가상매매 **3개월+** 후 실매매 | ✅ 숫자 / ⚠ 시계 |
| `12_듀얼북극성` | G2≥56일·avg≥60·fwd>30 · G4=디렉터 수동 | ✅ |
| `15_POST_RP1` | MDD 구조적 가능 · **CAGR 미증명** · 15구간 일관 Pass 아님 | ✅ 정직 |
| IV L0~L3 | L2=주 근거 · L3 미연동 | ✅ |

**보완 제안 (문서 1단락)**: 시계를 한 문장으로 고정.

| 시계 | 의미 | CAGR “달성” 주장? |
|------|------|-------------------|
| daily **n≥20** | 갈림길 재소집만 | ❌ |
| ASG 4주 | L1 조기경보 | ❌ |
| **G2** (~56d+) | L2 수익 페이스 | △ 주 근거 |
| G4+디렉터 | 상품화/실전 | ✅ LIVE만 |

→ RP-1 Pass ≠ 40~70% 달성 (현행 규칙과 일치, 마스터에 명시 권고).

---

### B. 카테고리 (CAT) — 빠짐?

`docs/claude_project/`: **A~Q 전부 존재** (+ MAP · HANDOFF · CONSTANTS · KR-US 비대칭).  
Bitget과 동일 알파벳 · 비대칭 문서만 트랙별 상이 — **누락 카테고리 없음**.

데이터 흐름(MAP): `B→G→C→D→E→F→J` · H→K→C · I→C,F — 목표 경로상 **구조 구멍 없음**.  
빈칸은 카테고리가 아니라 **실행 상태·효과검증(06 3단계 미기록)**.

---

### C. 작업순서 — 낡은/모순 (Must-fix sync 후보)

| 문서 | 문제 | 현실(SYNC/NEXT) |
|------|------|-----------------|
| `00_전체현황판` Phase 3 C | **「다음」·Handoff 대기** | C-1 **No-Go 동결** · OBS-HOLD |
| `15_POST_RP1` | BULL-RECENCY **Claude OK 대기** · “다음 Go” | 레버 **전원 소진·동결** |
| `01_묶음A` 로컬표 | A-4/A-5 **미착수** | A-4 Adapter OK · A-5b OK · Phase1 freeze |
| `03_묶음C` | C-1 **미착수** | C-1-REDUCED **INVALID·동결** |
| `02_묶음B` | B 미착수·A-5 미완 전제 | A-5b OK인데 B는 **의도적 보류** (문구만 구식) |
| `09_쉬운요약` | 08-09 스냅샷 | OBS-HOLD/NS-OBS와 **불일치** |
| `06` 효과표 | A-1~A-3 **3단계 미완료** | Claude OK≠Done — 정상이지만 **공백 장기화** |

**지금 진짜 작업 줄기** (실행 SSOT):

```
OBS-HOLD → VPS daily n≥20 → RECALL_FORK
  3택: mega_trend(CAT-P) / 목표하향 / 관측연장
소진: BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1
금지(~n≥20): 위 3택 외 Alpha 신규 Go
Ops: NS-OBS-TG-01 VPS pull · 19:30 일보
```

---

### D. 40~70% 달성을 위해 — “빠진” 것 (카테고리 추가가 아님)

| 갭 | 근거 | 지금 할 일? |
|----|------|-------------|
| **L2 표본 부족** | composite~4 · G0 · n=8/28 (8/16) | 관측만 (OBS-HOLD) |
| **근처 수익 레버 소진** | NEXT_ACTION 동결 목록 | 재접촉 금지 |
| **Phase B(진화·킬) 보류** | 수익/선별 엔진인데 12주 미착수 | n≥20 후 갈림길과 함께 재평가 |
| **CAT-P mega_trend** | 소진 후 유일 명시 경로 | **재소집 전 금지** |
| **06 효과검증 공백** | A 구현은 됐는데 3단계 미기록 | n 쌓이며 North Star로 대체 관측 가능 |
| **L3/DSR (D-3)** | LIVE 하드게이트 미연동 | G2 전 후순위 유지 OK |
| **실적 스냅샷 공백** | 현황판 “미기록” | 일보/원장이 사실상 대체 — 현황판 링크만 |

→ **새 CAT 문자 추가 불필요.** 필요한 건 **실행 로드맵을 OBS-HOLD+갈림길 중심으로 재정렬**.

---

### E. Claude에게 요청하는 판단 (상의 안건)

1. **Must-fix (문서만 · 코드 0)**  
   - `15_POST_RP1` · `00_전체현황판` Phase표 · (선택) `01`/`03` 상단에 “SSOT=SYNC/현황판” 배너  
   - 목표 **다중 시계** 1단락을 `00_마스터` 또는 현황판에  
   - OK면 Cursor에 **문서-only Handoff** 1개 (예: ROADMAP-SYNC-01)

2. **재소집(n≥20)용 옵션 카드 초안만** (구현 Handoff 금지)  
   - mega_trend vs 목표하향 vs 관측연장 — 각각 Go 조건·금지·CAT 경계 3줄

3. **열지 말 것**  
   - BULL/SIDE/BEAR/C-1 재Go · Phase B/D 착수 · CAGR/MDD 숫자 변경 · Critical F/G rewrite

4. **NS-OBS-TG-01**  
   - 별도 DoD 검증 OK 가능 (본 Audit과 독립)

### Cursor 제안 (엔지니어 1줄)

문서 sync Handoff가 나오면 **targeted markdown only**가 맞음. 코드·config·cron 손대지 말 것.

### 디렉터 3줄

1. 카테고리·목표 숫자는 OK · **작업순서 문서가 낡음**.  
2. 수익률은 지금 코딩이 아니라 **관측→n≥20 갈림길**.  
3. Claude: 본 OUTBOX로 문서 sync Go/Defer 판단.

---

## OUTBOX — [Ops-lite] NS-OBS-TG-01 **완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | NS-OBS-TG-01 |
| **status** | `WAIT_CLAUDE_OK` · 운영 `OBS-HOLD` 유지 |
| **앵커** | `SYNC-2026-08-17-Z` |
| **선행** | FWD-OBS-HOLD-01 **Claude OK** |
| **범위** | 기존 North Star **일간** digest에 `[OBS_HOLD]` 패널 + `---CURSOR---`/`---CLAUDE---` |
| **비접촉** | Critical · config_kv · 신규 cron · mega_trend/목표하향 |

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | `OBS_HOLD_RECALL_N=20` · persist 후 `daily_n`/`cursor_action` | ✅ `dual_north_star_ledger.py` |
| 2 | daily HTML에 OBS 패널 + Cursor/Claude 복붙 | ✅ `dual_north_star_telegram.py` |
| 3 | weekly 패널 생략 · action NONE | ✅ 테스트 |
| 4 | n=8→OBSERVE_HOLD · n=20→RECALL_FORK | ✅ `tests/test_obs_hold_telegram.py` |
| 5 | hooks·12·NEXT_ACTION·§3 동기화 | ✅ |
| 6 | 단위테스트 | ✅ **12 passed** (obs+ledger) |

### 동작 요약

- cron **19:30** 기존 유지 (`factory.sh --north-star-digest daily`)
- n&lt;20: 관측유지 문구 · Alpha 구현 금지
- n≥20: 재소집 · Claude에 `---CLAUDE---` 붙여넣기 유도

### 디렉터 3줄

1. 일보에 OBS_HOLD+복붙 붙임 — 새 cron 없음.
2. VPS `git pull` 후 19:30 확인.
3. Claude: 본 OUTBOX 검증.

---

## OUTBOX — [Ops-lite] FWD-OBS-HOLD-01 **완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | FWD-OBS-HOLD-01 |
| **status** | `WAIT_CLAUDE_OK` · 운영상태 `OBS-HOLD` |
| **앵커** | `SYNC-2026-08-17-Y` |
| **선행** | CLOSE **Claude OK** |
| **코드 diff** | **0** |

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | NEXT_ACTION OBS-HOLD + n≥20 트리거 | ✅ |
| 2 | SYNC §3 bump + sub-phase | ✅ `SYNC-2026-08-17-Y` · FWD-OBS-HOLD-01 |
| 3 | 05 CLOSE Claude OK + OBS-HOLD 섹션 | ✅ |
| 4 | 코드 diff 0 | ✅ |

### 재소집 트리거 (문서 고정 · 판단 없음)

- **조건**: VPS daily history **n≥20**
- **예상**: ~2026-09-05 (8/16 n=8 기준 · **VPS 재확인 후 확정**)
- **그 전**: mega_trend / 목표하향 **착수 금지**

### 디렉터 3줄

1. OBS-HOLD 문서 고정 Done — 갈림길 판단 보류.
2. 재소집 = daily n≥20.
3. Claude: 본 OUTBOX 검증.

---

## OUTBOX — [Ops-lite] FWD-LEDGER-CRON-01-CLOSE **완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | FWD-LEDGER-CRON-01-CLOSE |
| **status** | `WAIT_CLAUDE_OK` |
| **앵커** | `SYNC-2026-08-17-X` |
| **선행** | FWD-LEDGER-CRON-01 **Claude OK** → CLOSED |
| **코드 diff** | **0** (소스/factory_data_dir/config_kv/bitget/cron **비접촉**) |

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | `12_듀얼북극성` §3 SSOT 각주 | ✅ VPS 경로 표 + 로컬 artifact 각주 |
| 2 | 로컬 원장 rename | ✅ 아래 ls |
| 3 | SYNC §3 bump + FWD-LEDGER CLOSED | ✅ `SYNC-2026-08-17-X` |
| 4 | NEXT_ACTION 갈림길+20일 관측 문구 | ✅ |
| 5 | 코드 diff 0 | ✅ |

### 로컬 격리 ls

```
C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\
  dual_north_star_ledger.LOCAL_DEV_DO_NOT_USE.json  7555  2026-08-12
  (dual_north_star_ledger.json — 없음)
```

### SSOT 고정 문구

- **판정/갈림길 근거**: VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` **만**
- **로컬** `*.LOCAL_DEV_DO_NOT_USE.json`: 참고 금지

### 다음 (디렉터·Claude)

갈림길 재소집(mega_trend / 목표하향 / 관측유지) — VPS 실측 composite **4.09** · n=**8/28** 기반. **최소 20일 추가 관측 전 확정 판정 금지.**

### 디렉터 3줄

1. CLOSE Done — VPS=SSOT · 로컬 원장 격리 rename 완료.
2. FWD-LEDGER-CRON-01 CLOSED · 코드 0.
3. Claude: 본 OUTBOX 검증 → 갈림길(20일 관측 전 확정 금지).

---

## OUTBOX — [Ops-lite] FWD-LEDGER-CRON-01 **조사 완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | FWD-LEDGER-CRON-01 |
| **status** | `WAIT_CLAUDE_OK` |
| **앵커** | `SYNC-2026-08-17-W` |
| **SSH** | `ubuntu@52.78.29.151` OK · pem `~/.ssh/LightsailDefaultKey-ap-northeast-2.pem` |
| **VPS HEAD** | **`600c9cd`** |
| **코드 diff** | **0** (조회만 · cron 재시작 0 · config_kv/bitget 비접촉) |

### 엔지니어 1줄

SRV-02의 "원장 5일 정체·fwd=0"은 **로컬 PC 원장** 기준이었다. **VPS SSOT**(`/var/lib/quant-factory/data/dual_north_star_ledger.json`)는 cron·write 모두 정상 — (a)(b)(c) 정체 분류는 **전제 기각**.

### DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | cron/timer 상태 표 | ✅ 아래 §1 |
| 2 | 실행 로그 유무·에러 | ✅ 아래 §2 |
| 3 | 원인 3분류 확정 | ✅ **(a)(b)(c) 해당 없음 — 전제 기각** |
| 4 | 코드 diff 0 | ✅ |

### 1. cron/timer 상태

| 항목 | 값 |
|------|-----|
| **등록** | `/etc/cron.d/dual-screener-factory-kr` — `30 19 * * *` `factory.sh --north-star-digest daily` |
| **별도 director-digest** | `/etc/cron.d/dual-screener-director-digest` **MISSING** (install 스크립트: factory-kr 이미 있으면 skip — 정상) |
| **user crontab** | 없음 |
| **systemd timer** | north-star 매칭 없음 (cron.d만) |
| **마지막 실행** | syslog: **2026-08-16 19:30:01 KST** (조사 시각 8/17 17:48 KST → 당일 19:30 아직 미도래) |
| **exit code** | cron mail/전용 로그 없음. **간접 성공**: ledger mtime **2026-08-16 19:30:08 KST** = cron 직후 write |

### 2. 실행 로그

| 항목 | 값 |
|------|-----|
| **syslog CRON CMD** | daily 관측: 8/9,10,11,12,13,14,15,16 (연속) + weekly 8/15 11:00 |
| **전용 digest 로그 파일** | 없음 (`logs/*north*` 부재 — cron 줄에 redirect 없음) |
| **에러 스택** | 발견 없음 (mail 없음 · write 성공으로 실패 가설 기각) |
| **8/11 이후 시도** | **매일 실행됨** (정체 ≠ 미실행) |

### 3. 원인 분류 (VPS SSOT)

| 분류 | 판정 | 근거 |
|------|------|------|
| **(a) cron 미등록/미실행** | ❌ | factory-kr 등록 · 8/9~16 CMD 전원 |
| **(b) 실행되나 매매신호 0** | ❌ | Track A `forward_trades_count=**324**` · KR n_closed=162 · US=126 · DB `forward_trades` n=324 |
| **(c) 신호 있으나 ledger write 실패** | ❌ | daily history 8장 연속 append · `updated_at=2026-08-16T10:30:07Z` |

**확정: 전제 기각 (로컬≠VPS).** SRV-02가 본 로컬 파일:

| | 로컬 PC | VPS |
|--|---------|-----|
| path | `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\dual_north_star_ledger.json` | `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| updated_at | 2026-08-11T15:57:50Z | **2026-08-16T10:30:07Z** |
| daily n | 1 | **8** |
| fwd count | **0** | **324** |
| composite | 40.0 (로컬) | **4.09** |

### VPS Track A 스냅샷 (latest 2026-08-16)

| 필드 | 값 |
|------|-----|
| gate | G0 |
| composite_score | 4.09 (return_pace 0.0 · mdd_safety 10.21) |
| avg_return_pct | −6.2544 · max_mdd_pct 8.9785 |
| KR | nav≈2.74e8 · ret −8.62% · MDD 8.98% · DEFENSE |
| US | nav≈2.88e5 · ret −3.88% · MDD 3.88% · NORMAL |

참고(분류 밖): daily 8일간 fwd=324·composite=4.09 **수치 고정** — "신규 체결 증가 없음" 가능하나 **cron/원장 write 실패 아님**. 행별 최신 entry 시각 SQL은 본 세션 Auto-review 차단으로 미실행(불필요 — DoD 충족).

### Claude에 질문 1개

갈림길 재소집 시 **VPS 원장(G0·composite 4.09·fwd 324)** 을 North Star SSOT로 고정할지, 로컬 원장은 무시(개발 PC artifact)로 명시할지 — VERDICT에 한 줄.

### 디렉터 3줄

1. VPS north-star cron **정상** — 재시작·재등록 **불필요**.
2. "fwd=0·5일 정체"는 **로컬 원장 오인** — VPS는 daily 8장·fwd 324.
3. 코드 diff 0 · Claude OK 대기 · mega_trend/목표하향은 재갈림길 후.

---

## OUTBOX — [Relay] SRV-02 VERDICT OK · FWD-LEDGER-CRON-01 Handoff 랜딩 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **소스** | `Downloads/CLAUDE_TO_CURSOR_append.md` → `CLAUDE_TO_CURSOR.md` 최상단 |
| **SRV-02** | **Claude OK** · 진단 종결 · 갈림길 = **포워드 원장 cron 정체** |
| **보류** | mega_trend · 목표 하향 (본 Go 결과 후) |
| **다음** | **FWD-LEDGER-CRON-01** · `WAIT_CURSOR_IMPL` · **Ops-lite 새 창** (코드 0 · 잡 재시작 금지) |
| **본 창** | FWD-LEDGER 조사 **안 함** (Handoff: 새 세션 1개만) |
| **앵커** | `SYNC-2026-08-17-V` |

---

## OUTBOX — [CAT-P / Alpha] SRV-02 **조사 완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | SRV-02 · 진단 전용 |
| **status** | `WAIT_CLAUDE_OK` |
| **앵커** | `SYNC-2026-08-17-U` |
| **코드 diff** | **0** (읽기만) |
| **config_kv / bitget/** | **비접촉** |
| **재계산** | BULL/SIDE/BEAR/C-1 **없음** |

### 엔지니어 1줄

CAT-P **kill-chain은 이미 루트에 부분배선**되어 있다. SRV 다음이 mega_trend라면 신규 모듈이 아니라 **별도 Handoff로 역할(RP-1 근처놓침 vs 기존 섹터 킬)**을 먼저 잘라야 한다. 원장 시계는 G0에서 멈춰 있어, 알파 착수와 **포워드 일일 digest 정체**는 별 축이다.

### 1. 포워드 원장 (Track A)

**소스**: 이 PC `factory_data_dir()` = `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\dual_north_star_ledger.json` (스키마 `dual_north_star_ledger.v1`). 레포 내 JSON 없음. **VPS 원장은 본 세션 SSH 미실행.**

| 필드 | Track A |
|------|---------|
| `updated_at` / `latest.ts_utc` | 2026-08-11T15:57:50Z |
| `latest.date_kst` | **2026-08-12** (조사일 2026-08-17 기준 달력 **+5일 정체**) |
| daily history | **1**장 (스냅샷 `meta.daily_snapshot_count` 필드는 0으로 기록됨 — 게이트는 history 길이 사용) |
| `forward_trades_count` | **0** |
| KR/US `n_closed` | 0 / 0 |
| KR NAV / MDD / ret | 300,000,000 · 0% · 0% · band NORMAL |
| US NAV / MDD / ret | 300,000 · 0% · 0% · band NORMAL |
| `aggregate.composite_score` | **40.0** (pace 0 + MDD safety 100 → 0.6/0.4 가중합) |
| `avg_return_pct` / `max_mdd_pct` | 0.0 / 0.0 |
| period_returns A | day/week/month/year = null · total 0.0 |
| `commercialization.A` | **G0** 측정·구조 · `block_reasons=[]` |
| `a06_first_pass` | false |

**G0/G1 근접** (`dual_north_star_ledger.py`: G1 = daily n≥**28** AND 종합 avg≥**40**; n<7이면 무조건 G0)

| 게이트 | 조건 | 현재 | 갭 |
|--------|------|------|-----|
| **G0** | 기본 | **현재** | — |
| **G1** | 28일 + avg≥40 | n=**1**/28 · 종합 **40.0**(하한 충족) | **일수 −27** · 시계 정체 |
| **G2** | 56일 + avg≥60 + forward>30 | n=1 · avg 40 · trades **0** | 일수·점수·체결 전부 미달 |

R1 배너 조건(`show_r1_caveat`): true — 연 목표 대비 참고용 아님. **Pass/CAGR 단정 없음.**

### 2. CAT-P mega_trend 인벤토리 (조회만)

**관련 파일 없음이 아님.** 루트에 kill-chain + re-evolution 모듈 존재. `bitget/` 미열람·미수정.

| 파일 | 최종 커밋 | stage |
|------|-----------|--------|
| `mega_trend_ignition.py` 외 kill 7모듈 + `reports/mega_trend_kill_report_section.py` | **`039cec9` 2026-07-10** | **부분코드·배선됨** — `smart_money_tracker` ignition, `portfolio_risk_overlay` unlock, `factory_pipelines` kill RL evolve, tests 13, `scripts/validate_mega_trend_kill_live.py`. 기본 `ENABLE_MEGA_TREND_UNLOCK=1` |
| `docs/claude_project/CAT-P_MegaTrend_ReEvolution.md` | `fa7e614` 2026-08-01 | 설계 SSOT (역할 문서) |
| `re_evolution_*.py` (7) | 2026-07-09~12 (`6a16582` / `4504989`) | **부분코드** (루트 존재). RP-1 근처놓침 탈출 Handoff와 **미연결** |
| RP-1 대체·mega_trend 알파 신규 Handoff | — | **미착수** (본 창 착수 금지 준수) |

기존 CAT-P는 **섹터 점화→클라이맥스→킬**이지, C-1 실패를 대체하는 **진입 알파 레버가 아님**.

### 3. 근처놓침 동결표 (05 대조 · 재계산 없음)

**라벨 동결 (05)**: BULL_05 FAIL · SIDE_02 NEAR_MISS(B) · SIDE_03 FAIL(B) *after EXIT 실험* · BEAR_01/03/04 NEAR_MISS(B) · C-1 **SECTOR_LEVER_INVALID** (Claude가 JSON MIXED 기각).

**수치 SSOT (재확인만)**

| ID | 라벨 (05 동결) | 수치 소스 | period_ret% | n | MDD_tier% | 비고 |
|----|----------------|-----------|-------------|---|-----------|------|
| BULL_05 | **FAIL(B)** · KR 레버 동결 | 8/13 `rp1_bull_recency_01` | **−9.0378** | 9,142 | 9.04 | 05 요약 −9.04% 일치(반올림) |
| BULL_03 (참고·Done) | NEAR_MISS | 동일 8/13 | **+15.3971** | 10,276 | 9.32 | 05 “15.4% / n=10276” 일치 |
| SIDE_02 | **NEAR_MISS(B)** | 8/13 (플래그 OFF 유효) | **−9.0469** | 20,935 | 9.05 | EXIT ON 실험 −5.77은 **미반영·재시도 금지** |
| SIDE_03 | **FAIL(B)** 동결 (EXIT 실험) / 플래그 OFF 시 8/13은 NEAR_MISS | 8/13 | **+6.0714** | 24,167 | 9.13 | C-1 A는 클린 matrix **+1.2892** n=94,458 — **다른 유니버스**, 재계산 아님 |
| BEAR_01 | NEAR_MISS(B) | 8/13 오염 n | **−9.0273** | 4,585 | 9.03 | C-1 클린 A **−9.0642** n=16,636 match **0%** |
| BEAR_03 | NEAR_MISS(B) | 8/13 오염 n | **−9.0574** | 10,155 | 9.06 | 클린 A **−9.0450** n=39,698 Δ=0 |
| BEAR_04 | NEAR_MISS(B) | 8/13 오염 n | **−1.5444** | 5,691 | 9.01 | 클린 A **+1.7449** n=21,217 Δ=0 |
| C-1 섹터 | **INVALID** · 2단계 No-Go | `c1_reduced_diag_20260817.json` + Claude OK | SIDE Δ +0.0494 / +0.0703pp · BEAR Δ 0 | — | B mdd 9.01~9.16 | JSON `overall_verdict=MIXED` **기각 유지** |

### 4. DoD

| # | 기준 | 결과 |
|---|------|------|
| 1 | 포워드 원장 표 | ✅ 위 §1 |
| 2 | CAT-P 인벤토리 또는 없음 | ✅ 파일 있음 · stage 표 |
| 3 | 동결표 대조 | ✅ 위 §3 · 05와 라벨 불일치 없음 (SIDE_03 이중 SSOT는 05에 이미 명시) |
| 4 | OUTBOX 코드 0 | ✅ |

### Cursor 질문 (Claude)

1. 원장 G0+체결 0+digest 5일 정체를 **다음 Go의 주축**(관측/cron)으로 볼지, RP-1 구조 갈림길(목표 하향 vs mega_trend Handoff)과 **병렬 후순위**로 둘지.
2. mega_trend를 쓰더라도 **기존 kill-chain 확장**인지 **RP-1 진입 알파 신규**인지 역할 절단 — 착수는 별도 Handoff만.

---

## OUTBOX — [Relay] C-1 VERDICT OK · SRV-02 Handoff 랜딩 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **소스** | 디렉터 채팅 VERDICT + SRV-02 Handoff → `CLAUDE_TO_CURSOR.md` 최상단 |
| **C-1** | **Claude OK** · **SECTOR_LEVER_INVALID** · 2단계 **No-Go** · MIXED 기각 |
| **다음** | **SRV-02** 조사 · `WAIT_CURSOR_IMPL` · **Alpha 새 창** (코드 0) |
| **본 창** | SRV-02 조사 **안 함** (Handoff: 새 세션 1개만) |
| **앵커** | `SYNC-2026-08-17-T` |

---

## OUTBOX — [CAT-C / Alpha] C-1-REDUCED **1단계 진단 완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | C-1-REDUCED · **1단계 Done** · **2단계 미착수** |
| **status** | `WAIT_CLAUDE_OK` |
| **앵커** | `SYNC-2026-08-17-S` |
| **산출** | `reports/regime_panel/c1_reduced_diag_20260817.json` (로컬=VPS 사본) |
| **테스트** | `tests/test_c1_reduced_spillover_sandbox.py` **7 passed** |
| **config_kv / D·E·F** | **비접촉** · `stage2_auto_start=false` |

### 엔지니어 1줄

RP-1 트레이드에 `sector` 없고 FDR Industry 컬럼도 소실(`kr_map_n=0`) → **sandbox seed/name/FT 조인 + 기존 Stage2 `apply_c1_sector_boost`**가 try_add 내부 개입보다 안전. 진입점수 보너스 API(`sandbox_spillover_entry_bonus`)는 인터페이스만 준비, 라이브 미배선.

### 0. BEAR 오염 재확인 (선행 DoD)

| ID | n_813 (BR01) | n_clean | drop | contaminated |
|----|--------------|---------|------|--------------|
| BEAR_01 | 4,585 | 16,636 | 72.4% | **true** |
| BEAR_03 | 10,155 | 39,698 | 74.4% | **true** |
| BEAR_04 | 5,691 | 21,217 | 73.2% | **true** |

→ A/B는 **BR01 OFF** matrix `ab52b174…` + SIDE exit OFF · 클린 재산출 1회.

### 1. Frozen knob

`markov_order=1, lag_days=1` (단일 고정 · 그리드 없음) · `C1_SECTOR_BOOST_PCT=+5%` final_ret (기존 RP-1 Stage2) · entry score bonus 상수 5.0 (sandbox only).

### 2. A vs B (`period_return_pct` 1순위 · CAGR 단독 금지)

| ID | A ret% | B ret% | Δ | mdd_B% | n | match_rate | 판정 |
|----|--------|--------|---|--------|---|------------|------|
| SIDE_02 | −8.5181 | −8.4687 | **+0.0494** | 9.11 | 77,430 | 6.5% | SECTOR_LEVER_VALID (미시) |
| SIDE_03 | +1.2892 | +1.3595 | **+0.0703** | 9.16 | 94,458 | 5.6% | SECTOR_LEVER_VALID (미시) |
| BEAR_01 | −9.0642 | −9.0642 | 0 | 9.06 | 16,636 | **0%** | SECTOR_LEVER_INVALID |
| BEAR_03 | −9.0450 | −9.0450 | 0 | 9.05 | 39,698 | 5.6% | SECTOR_LEVER_INVALID |
| BEAR_04 | +1.7449 | +1.7449 | 0 | 9.01 | 21,217 | 5.9% | SECTOR_LEVER_INVALID |

- 전 구간 n≫20 · mdd_tier ≤10% 유지 (B MDD 기각 없음)
- SIDE/BEAR **verdict 라벨 불변** (모두 NEAR_MISS)
- avg_pnl/PF는 boost 전 stats라 A=B (portfolio `period_ret`만 B 반영) — SIDE 미시 Δ만 관측
- BEAR_03/04: match>0인데 period_ret **완전 불변** → Phase A LOCKDOWN 경로가 섹터 +5%에 둔감

### 3. overall / §7 매핑 (Cursor 잠정)

| overall | **MIXED** |
|---------|-----------|
| SIDE | 레버 **미시 유효** (NEAR_MISS 탈출 실패 · Δ≪1pp) |
| BEAR | 레버 **무효** (GFC는 섹터맵 공백 match=0 포함) |
| 목표 관점 | Near-miss 회복용 섹터 레버로는 **설득력 약함** → mega_trend/목표하향 후보와 경합 (Claude 확정) |

### 4. DoD 체크

| # | 기준 | 결과 |
|---|------|------|
| 1 | 5구간 A vs B period_ret 표 | ✅ |
| 2 | mdd ≤10% | ✅ |
| 3 | n≥20 | ✅ |
| 4 | §7 구간별 1줄 | ✅ (표·overall) |
| 5 | JSON + 05 + OUTBOX | ✅ |

### Cursor 질문 (1개)

SIDE Δ=+0.05~0.07pp를 「A/B 개선=섹터 레버 유효」로 인정할지, 아니면 **물질성 미달 → 실질 무효(MIXED→INVALID)** 로 내릴지?

### 금지 준수

- 2단계 실장·config_kv·BULL/PASS 재실행·S5/SIDE exit/BULL bounds **미접촉**
- 코드는 VPS scp 상태 · **origin 미푸시**(커밋 요청 없음)

---

## OUTBOX — [Relay] S5 Claude OK · C-1-REDUCED Go 랜딩 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **소스** | `Downloads/CLAUDE_TO_CURSOR_append (1).md` → `CLAUDE_TO_CURSOR.md` 최상단 |
| **S5** | **Claude OK: 2026-08-17** · 종료 · 재개=n>0 시 동일 CLI |
| **다음** | **C-1-REDUCED** 1단계 진단 · `WAIT_CURSOR_IMPL` · **Alpha 새 창** |
| **본 창** | C-1 구현 **안 함** (Handoff: 새 세션 1개만) |
| **앵커** | `SYNC-2026-08-17-R` |

---

## OUTBOX — [Ops-lite] S5-HARNESS-SCOPE-01 **VPS 실측 Done** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **SSH** | Downloads `LightsailDefaultKey-ap-northeast-2.pem` → `~/.ssh/` · 접속 OK |
| **배포** | `sudo bash update_factory.sh` 완료 (1차 `.git` root 권한 → `chown ubuntu` 후 재실행) |
| **VPS HEAD** | **`600c9cd`** |
| **CLI** | `venv/bin/python scripts/run_s5_defense_contribution_report.py --start 2026-08-17 --as-of 20260817` |
| **산출(VPS)** | `/home/ubuntu/dante_bots/Dual-Screener-Bot/reports/s5_defense/s5_contribution_20260817.json` |
| **로컬 사본** | `reports/s5_defense/s5_contribution_20260817_vps.json` |
| **코드 추가 diff** | 0 (관측만) |
| **status** | `WAIT_CLAUDE_OK` |

### windows 실측 (조작 없음)

| market | n | realized_pnl_sum | gate_active_minutes | sample_insufficient | short_pnl_column_present | contributed |
|--------|---|------------------|---------------------|---------------------|--------------------------|-------------|
| KR | **0** | 0 | 0.0 | true | **false** | false |
| US | **0** | 0 | 0.0 | true | **false** | false |

- `numeric_judgment_omitted=true` · Pass/Fail/CAGR 없음
- notes: `표본 부족` + short PnL Adapter 문구 유지
- 해석: 2026-08-17 당일 window에서 BEAR/HIGH_VOL∩S5 게이트 활성 구간·S5 체결 **관측 0** (실패 아님 · 관측 인프라 동작 확인)

### DoD

| # | 결과 |
|---|------|
| 1 | VPS 로그 + JSON 경로 ✅ |
| 2 | n=0 그대로 보고 ✅ |
| 3 | write 0 (관측만) ✅ |
| 4 | `short_pnl_column_present=false` 노출 ✅ |
| 5 | 05 + 본 OUTBOX ✅ |

---

## OUTBOX — [Ops-lite] S5 VPS 실측 **WAIT_DIRECTOR** · push는 완료 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **git** | 로컬→origin push **OK** · `0efc750` → **`dc90e39`** |
| **커밋** | `feat(s5-harness): add read-only S5 defense contribution paper gate` |
| **SSH** | `ubuntu@52.78.29.151` → **Permission denied (publickey)** (이 워크스테이션) |
| **status** | `WAIT_DIRECTOR` — Cursor가 VPS 실행 불가 · Handoff 패턴대로 미달 보고 |
| **코드 diff** | 이번 단계 추가 수정 **0** (이미 push된 산출물만) |

### 디렉터 — VPS에서 복붙 (SSH 가능 터미널)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
sudo ./update_factory.sh
# 또는 untracked 충돌 시 OPS-01과 동일 정리 후 update_factory

git rev-parse --short HEAD   # 기대: dc90e39

python3 scripts/run_s5_defense_contribution_report.py --start 2026-08-17 --as-of 20260817
```

산출 경로: `reports/s5_defense/s5_contribution_20260817.json`  
확인: `short_pnl_column_present` 필드 존재 · Pass/Fail 문구 없음 · n은 그대로 보고.

JSON(또는 `jq '.windows[] | {market,s5_trade_count,realized_pnl_sum,sample_insufficient,short_pnl_column_present,gate_active_minutes}'` 결과)을 채팅/파일로 주시면 Cursor가 OUTBOX 마감.

---

## OUTBOX — [Relay] S5 Claude OK 랜딩 · VPS 실측 Go · 2026-08-17

| 항목 | 내용 |
|------|------|
| **소스** | `Downloads/CLAUDE_TO_CURSOR_append.md` → `CLAUDE_TO_CURSOR.md` 최상단 |
| **VERDICT** | 로컬 페이퍼 게이트 **OK** · 부분 Done (구현 Done / 관측 대기) |
| **다음** | Ops-lite · VPS CLI 1회 · **코드 diff 금지** |
| **본 Alpha 창** | VPS SSH **미실행** (Handoff: 새 Ops 세션) |
| **status** | `WAIT_CURSOR_IMPL` (Ops-lite) · 앵커 `SYNC-2026-08-17-O` |

---

## OUTBOX — [Alpha] S5-HARNESS-SCOPE-01 **구현 완료** · Claude 검증 요청 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **sub-phase** | S5-HARNESS-SCOPE-01 (페이퍼 게이트) |
| **status** | `WAIT_CLAUDE_OK` (Claude OK 전 Done 아님) |
| **위험도** | 🟢 Low · 라이브 파이프라인 diff 없음 |
| **앵커** | `SYNC-2026-08-17-N` |

### DoD 체크

| # | 기준 | 결과 |
|---|------|------|
| 1 | write 0 (신규 테이블/컬럼/config 없음) | ✅ `compute_s5_defense_contribution_log` 소스에 INSERT/UPDATE/CREATE/ALTER/config_kv 없음 |
| 2 | `s5_arm_active` = 기존 시그니처 소비 | ✅ `resolve_defense_arm_weight(market, regime, sig, cfg)` 호출 · 재정의 없음 (테스트 wraps) |
| 3 | 2026-08-17~ 스모크 JSON | ✅ `reports/s5_defense/s5_contribution_20260817.json` |
| 4 | n<20 판정 문구 없이 플래그 | ✅ `sample_insufficient` + notes `표본 부족` · Pass/Fail/CAGR 없음 |
| 5 | 라이브 경로 비접촉 | ✅ 신규: `reports/s5_defense_contribution.py` · `scripts/run_s5_defense_contribution_report.py` · 테스트 · JSON · work_phases |
| 6 | 테스트 + 05 + OUTBOX | ✅ `tests/test_s5_defense_contribution_report.py` **5 passed** |

### 스모크 요약 (로컬 DB)

- KR/US 각 window: `s5_trade_count=0` · `contributed=false` · `gate_active_minutes=0.0` (`meta_state_log` 소비)
- 숫자 판정 없음. **Pass/Fail 재판정 아님.**

### Adapter (스키마 충돌 — 억지 컬럼 추가 안 함)

`short_forward_trades`에 `final_ret` 없음(후보 원장). 실현손익 합은 `forward_trades.final_ret`만. JSON `short_pnl_column_present=false`.

### 파일

- `reports/s5_defense_contribution.py`
- `scripts/run_s5_defense_contribution_report.py`
- `tests/test_s5_defense_contribution_report.py`
- `reports/s5_defense/s5_contribution_20260817.json`

### Claude에 질문

1. 로컬 n=0 스모크를 DoD#3로 인정할지, VPS 원장 1회 산출을 검증 조건에 넣을지.
2. `short_forward_trades` PnL Adapter 유지 OK인지.

---

## OUTBOX — [OPS/Relay] S5-HARNESS-SCOPE-01 Handoff **파일 랜딩** · 2026-08-17

| 항목 | 내용 |
|------|------|
| **스코프** | **페이퍼 게이트** (태그·풀슬리브 기각) |
| **INBOX** | `CLAUDE_TO_CURSOR.md` 최상단 |
| **status** | `WAIT_CURSOR_IMPL` — **새 Alpha 세션** |
| **본 Ops 창** | 구현 **안 함** (Handoff 지시: 새 세션 1개만) |

---

## OUTBOX — [OPS] OPS-01 **1차 관측 PASS** · 배포 Done · 2026-08-17

| 항목 | 내용 |
|------|------|
| **VPS HEAD** | `0efc750` |
| **phase** | `post_bear_underdog_01` |
| **overall** | **PASS** · `cursor_action=NONE` |
| **telegram** | dry-run · `telegram_sent=false` |
| **SSOT** | `/var/lib/quant-factory/data/deploy_watch_latest.json` |

| check | status | detail |
|-------|--------|--------|
| factory_health | PASS | `dante-factory.service=active` |
| f_gate_01 | **SKIP** | `strategy_registry_missing` (테이블 없음 → COOLED/RETIRED 카운트 불가 · overall PASS 유지) |
| c_funnel_02 | PASS | max_ts 최근 · baseline 2026-07-02 |
| f_retire_02 | PASS | `lifecycle_observe_only_rows=0` |
| c_bear_underdog_01 | PASS | shadow=0 untagged=0 mae=0/0 · pain_repro=false |
| reality_audit | PASS | KR n≈102 bad_et=1 · US n=134 bad_et=0 |

**조치 없음.** 코드 Handoff 불필요. `f_gate_01` SKIP은 메모만 (registry 테이블 경로/생성은 후순위 Ops 조사 — hard gate 아님).

**다음**: 디렉터 → Claude **S5-HARNESS-SCOPE-01** SRV-lite (`NEXT_ACTION` 붙여넣기).

---

## OUTBOX — [OPS] OPS-01 `update_factory` **완료** · 관측 대기 · 2026-08-17

| 항목 | 내용 |
|------|------|
| **VPS HEAD** | **`0efc750`** (디렉터 확인) |
| **배포** | untracked 9 `mv` 후 `update_factory.sh` 재실행 **성공** |
| **남은 일** | ① `DEPLOY_WATCH_PHASE=post_bear_underdog_01` ② 1차 `--deploy-watch` ③ 이상 없으면 OPS-01 체크 → Claude **S5-HARNESS-SCOPE-01** |
| **코드 Handoff** | 불필요 |

```bash
# 영속(예: crontab 앞 또는 /etc/environment — 기존 방식 유지)
export DEPLOY_WATCH_PHASE=post_bear_underdog_01

cd /home/ubuntu/dante_bots/Dual-Screener-Bot
TZ=Asia/Seoul bash ./factory.sh --deploy-watch post_bear_underdog_01 --dry-run --no-telegram
# 이상 없으면 (텔레그램 허용 시):
# TZ=Asia/Seoul bash ./factory.sh --deploy-watch post_bear_underdog_01
```

F-GATE: COOLED/RETIRED **0건**이면 `registry_state_block` 미발화 = 정상.

---

## OUTBOX — [OPS] OPS-01 `update_factory.sh` **[2/7] abort** · 2026-08-17

| 항목 | 내용 |
|------|------|
| **VPS HEAD (시도 전)** | `9f5e3a1` → 목표 `0efc750` (`Updating 9f5e3a1..0efc750`) |
| **[1/7]** | OK — backup `20260817_050751_utc` |
| **[2/7]** | **실패** — untracked가 merge에 덮임. 엔진 교체·재시작 **안 됨** |
| **원인** | SIDE-ALPHA 산출물이 VPS에 untracked로 있고, 같은 경로가 `0efc750`에서 **tracked** |
| **코드 Handoff** | **불필요**. `git clean -fd` **금지** |

디렉터 — VPS에서 9개만 치우고 스크립트 **재실행**:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
STAMP=/var/backups/dante-ops01-untracked-20260817
sudo mkdir -p "$STAMP"
sudo mv -n \
  reports/regime_panel/rp1_bull_recency_01_20260813.json \
  reports/regime_panel/rp1_bull_recency_01_20260813_dod.json \
  reports/regime_panel/rp1_side_alpha_01_20260817.json \
  reports/regime_panel/rp1_side_alpha_01_20260817_dod.json \
  reports/regime_panel/side_alpha_01_trade_diag_20260817.json \
  scripts/run_side_alpha_01_rp1.py \
  scripts/side_alpha_01_trade_diag.py \
  side_alpha_01_exit.py \
  tests/test_side_alpha_01_exit.py \
  "$STAMP/"
sudo bash ./update_factory.sh
git log -1 --oneline   # 기대: 0efc750
```

그다음: `DEPLOY_WATCH_PHASE=post_bear_underdog_01` + `--deploy-watch` 1차 관측.

---

## OUTBOX — [OPS] OPS-01 VPS 배포 **미실행** · `WAIT_DIRECTOR` · 2026-08-17

> **회신 채널**: 본 블록. Alpha 검증 아님. **코드 Handoff 불필요.**

| 항목 | 내용 |
|------|------|
| **sub-phase** | OPS-01 |
| **status** | `WAIT_DIRECTOR` — Cursor 이 창에서 VPS 셸 **불가** |
| **배포 시각** | 없음 (미실행) |
| **phase** | VPS `DEPLOY_WATCH_PHASE` **미확인** (목표: `post_bear_underdog_01`) |
| **이상** | 로컬 `ssh ubuntu@52.78.29.151` → **Permission denied (publickey)** |
| **git (워크스테이션)** | `origin/main` = **`2ecb6d7`** (HEAD 일치) |
| **이미 main에 있는 배포 대상** | `9cf0018` F-GATE-01/F-RETIRE-02/L-OBS-01 · `4906d89` BEAR-UNDERDOG-01 · `201dd74` L-OBS-02 |
| **Alpha 코드** | **diff 없음** · S5/BULL/SIDE **미착수** |
| **금지 준수** | 로컬 dirty 트리 **push 안 함** (SIDE-ALPHA·docs 미커밋 다수) |

### 디렉터 — VPS에서 이어서 (키 있는 셸)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git fetch origin && git log -1 --oneline
git pull
sudo ./update_factory.sh
# cron/env:
#   DEPLOY_WATCH_PHASE=post_bear_underdog_01
# 1차 관측 (텔레그램 없이):
TZ=Asia/Seoul bash ./factory.sh --deploy-watch post_bear_underdog_01 --dry-run --no-telegram
```

F-GATE 1차: COOLED/RETIRED **0건이면** `registry_state_block` 미발화 = 정상. F-RETIRE는 observe_only · 실 notional 계속 블록.

배포 끝나면 `NEXT_ACTION` OPS-01 체크 → Claude에 **S5-HARNESS-SCOPE-01** SRV-lite.

---

## OUTBOX — [CAT-C / Alpha] BEAR-S5-SIM-01 **1단계 진단 완료** (Claude 판독 SSOT) · 2026-08-17

> **회신 채널**: 본 블록 **단독**. 채팅 요약·텔레그램 금지.  
> **코드 변경·15구간 rerun·2단계 착수**: 진단 Claude 판정 **전 금지** (Handoff 엄수 · SIDE-ALPHA처럼 자동 진행 불가).

| 항목 | 내용 |
|------|------|
| **sub-phase** | BEAR-S5-SIM-01 · **1단계 Done** · 2단계 **미착수** |
| **요청 SSOT** | `reports/regime_panel/rp1_20260811.json` (v2.3.3) — **이 워크스테이션에 파일 없음** (`Desktop\rp1_20260811_v233.json`도 부재) |
| **수치 프록시 (BEAR)** | `rp1_side_alpha_01_20260817.json` BEAR 5행 — BR01 **OFF** · SIDEWAYS exit만 오버레이 · **BEAR window 비접촉** · BULL_01 n=**97,009** = v2.3.3 aggregate와 bit-match |
| **오염 참고 (쓰지 말 것)** | `rp1_bull_recency_01_20260813.json` BEAR n은 CLUSTER_1 shrink 0.45 오염 (BULL_01 97,009→25,077 · BEAR_01 16,636→4,585) |
| **매트릭스** | `matrix_ab52b174195da604adc8.pkl` **로컬 부재** — exit_type/KR·US 분해 미실행 (신규 스크립트 = 코드 변경 = 이번 Handoff 금지) |
| **병행 갱신** | `05_진행로그` §BEAR-S5-SIM-01 · `00_SESSION_SYNC` §3 · `NEXT_ACTION.md` |

### 엔지니어 1줄

BEAR ×3 NEAR는 **원인 B(수익 부족)** + **S5 미배선**(게이트 차단 아님). MDD OK는 Phase A **LOCKDOWN**이지 인버스/블랙홀 PnL이 아님. RP-1 안 파라미터로는 S5 기여 로그를 만들 수 없음 → 2단계 = **RP-1 외** S5 시뮬 하네스 (별도 Go).

### 0. SSOT 공백 (정직)

| 경로 | 상태 |
|------|------|
| `reports/regime_panel/rp1_20260811.json` | 로컬 없음 (문서상 VPS) |
| `C:\Users\GoodLife\Desktop\rp1_20260811_v233.json` | 8/11 `bull_recency_01_diag_aggregate` source — **현재 없음** |
| v2.3.3 BULL 행 | aggregate JSON으로 **재확인** (BULL_01 n=97009 등) |
| v2.3.3 BEAR 행 | 원본 JSON 없음 → **아래 프록시** (판정 ID는 8/13과 불변) |

식별(NEAR 3 / PASS 2)은 두 full JSON이 **동일**. 숫자 판정은 프록시 표 A만 사용.

### A. BEAR ×5 식별 + 집계 (DoD #5 n≥20)

> BEAR 판정 규칙 (`regime_panel_rp1.judge_period_verdict`): MDD≤10 → PF≥0.95 **PASS** / PF<0.95 **NEAR_MISS**. 원인 태그: n≥20 · MDD≤10 · CAGR<0 → **B**.

**표 A — 수치 프록시 (v2.3.3에 가장 가까움 · SIDE-ALPHA JSON · BEAR 비접촉)**

| ID | 기간 | verdict | n | WR% | avg_pnl | PF | period_ret% | mdd_tier% | near_miss |
|----|------|---------|---|-----|---------|-----|-------------|-----------|-----------|
| **BEAR_01** 서브프라임GFC | 2008-09-01~2009-03-31 | **NEAR_MISS** | **16,636** | 20.65 | −0.808 | **0.708** | **−9.06** | 9.06 | **B** |
| BEAR_02 COVID폭락 | 2020-02-01~2020-05-31 | PASS | 22,502 | 27.32 | −0.006 | 0.998 | −9.08 | 9.08 | — |
| **BEAR_03** 글로벌금리인상 | 2022-01-01~2022-06-30 | **NEAR_MISS** | **39,698** | 25.09 | −0.729 | **0.714** | **−9.05** | 9.05 | **B** |
| **BEAR_04** 미중무역분쟁 | 2018-09-01~2018-12-31 | **NEAR_MISS** | **21,217** | 24.54 | −0.891 | **0.650** | **+1.74** | 9.01 | **B** |
| BEAR_05 미국신용등급강등 | 2011-08-01~2011-10-31 | PASS | 6,799 | 28.53 | −0.051 | 0.979 | −5.63 | 8.22 | — |

NEAR 3구간: **BEAR_01 · BEAR_03 · BEAR_04**. 전 구간 n≫20. `zero_entries=false` → **원인 A 기각**. `mdd_crosscheck=MDD_OK` · mdd_tier 8.2~9.1% → **원인 C 기각**. quota=8 · `kelly_cap=0.01` (BEAR).

**표 A′ — 8/13 (BR01 shrink 오염 · n/period_ret 비SSOT)**

| ID | n | PF | period_ret% | verdict |
|----|---|-----|-------------|---------|
| BEAR_01 | 4,585 | 0.762 | −9.03 | NEAR(B) |
| BEAR_02 | 5,917 | 0.976 | −5.17 | PASS |
| BEAR_03 | 10,155 | 0.687 | −9.06 | NEAR(B) |
| BEAR_04 | 5,691 | 0.663 | −1.54 | NEAR(B) |
| BEAR_05 | 1,899 | 0.955 | −6.98 | PASS |

라벨(2P/3NM)만 원본과 합의. **n·period_ret는 8/13을 쓰지 말 것.**

### A2. Trade-level breakdown (DoD #1 — 스키마 한계)

RP-1 period row 키에 `exit_type` / `bars_held` / KR·US split / `template` / inverse 태그 **없음**. 로컬 matrix 없음. 기존 `*_trade_diag.py`는 BULL/SIDE `TARGET_WINDOWS` 하드코딩 — BEAR 창 추가는 **코드 변경**이라 이번 Handoff에서 안 함.

| 항목 | 결과 |
|------|------|
| exit_type TP/SL/TIME % | **미계측** (JSON 없음 · matrix 없음) |
| 평균 보유기간 | **미계측** — 기존 절차도 proxy(SL=−3.5/TP=+10/TIME≈15d)뿐 |
| KR/US avg | **미계측** |
| 선행(참고, 본 런 아님) | BULL·SIDE 1단계: top1 `CLUSTER_1_*_폭발형_*` share 100% · Jaccard 1.0 — BEAR도 동일 매칭 풀 개연 **높음**, **이번 숫자 아님** |
| 보유기간 대체 | 집계만: NEAR WR 20.6–25.1% · avg −0.73~−0.89 vs PASS WR 27.3–28.5% · avg ≈0 — **손익 밀도 붕괴**, exit mix는 미분리 |

DoD #1 숫자 표는 **미완**. 원인 B·S5 미배선 결론은 집계+하네스 증거로 안정 (아래). 완성은 VPS matrix read-only 한 번 — **별도 Claude 허용** 필요.

### B. S5 방어 커버리지 (DoD #2)

| 구간 | S5 발동? | 근거 |
|------|----------|------|
| BEAR_01/03/04 NEAR | **미발동 · 미배선** | RP-1 JSON에 inverse/blackhole/S5 필드 0. `regime_panel_rp1.py` / `time_machine_backtester.py`에 S5·inverse·blackhole **PnL 경로 없음** |
| BEAR_02/05 PASS | **동일 미배선** | PASS여도 avg_pnl **음수** · period_ret **음수**(02 −9.08 · 05 −5.63). PF≥0.95만으로 PASS — **S5 기여 로그 없음** |
| 게이트 차단? | **아님** | live `ACTION_BY_REGIME` BEAR `s5_arm_active=True` (A-5b OR이면 허용). RP-1은 `resolve_defense_arm_weight` / `inverse_etf_sniper` / fade를 **호출하지 않음**. runner의 `blackhole_hunter`는 US 티커 리스트만 |
| 실제 방어 | **Phase A tier** | `tier_log_sample`: 초반 후 **LOCKDOWN** · `position_quota_mult=0` · `kelly_throttle_mult=0` → 손실을 mdd_tier≈9%에 **절단**. raw period_ret −52~−77% |
| 문서 SSOT | `14_레짐패널` §4 | S5 = inverse/blackhole **태그만** · RP-1 v1은 진입 시그널 위주 · A-5a/b는 포트폴리오 레벨 **별도** |
| 하락 bucket Pass 기준 | **미충족** | 「손실구간 S5 기여 로그」 — PASS 2구간 포함 **전 BEAR 0건** |

SRV 문장과 동일: **손실 억제 ≠ S5 헤지 증거**.

### C. 원인트리 · 공통 vs 개별 (DoD #3)

| 가설 | 판정 | 근거 |
|------|------|------|
| A 신호 부족 | **기각** | n=16k–40k · zero_entries=false |
| C MDD 초과 | **기각** | mdd_tier≤9.08 · MDD_OK · 위반 0 |
| **공통 B** | **확정** | NEAR PF 0.65–0.71 < 0.95 · avg 동시 악화 · 롱온리 S1을 하락 구간에 그대로 투입 |
| **공통 S5** | **미기여** | 하네스에 arm 없음 (게이트 차단 아님) |
| 개별 BEAR_01 | GFC · WR 최저 20.6% · period_ret **티어 핀** −9.06 | EXTREME_CRASH · raw −76.9% |
| 개별 BEAR_03 | n 최대 · PF 0.714 · 역시 티어 핀 −9.05 | 2022 금리인상 · tpd 348 |
| 개별 BEAR_04 | PF **최악 0.650** 인데 period_ret **+1.74** | SIDE_03형 시퀀싱(일평/쿼터). 규칙3상 period_ret은 NEAR 탈출 아님(판정=PF). 8/13은 같은 구간 −1.54 — shrink 민감 |
| PASS 대조 | PF만 0.95 턱걸이 | 수익 전환 아님. BEAR_02도 period_ret −9% |

### D. 2단계 실행가능성 (DoD #4) — **RP-1 외**

| 후보 | RP-1 내? | 권고 |
|------|----------|------|
| CAT-E BEAR SL/TIME | 내 | **비권고** — SIDE-ALPHA SL 완화 = SIDE_03 FAIL 회귀 선례. S5 기여 로그를 만들지 못함 |
| CLUSTER_1 / S1 타이트닝 | 내 | **비권고** — BULL-RECENCY가 전역 shrink로 BEAR n까지 오염. 하락 엣지 축과 불일치 |
| Phase A kelly/quota | 내 🔴 | **금지** — 이미 LOCKDOWN이 MDD를 붙듦. 방어층 완화=C 위험 |
| **S5/인버스·블랙홀 시뮬 하네스** | **외** | **유일한 2단계 후보**. RP-1 파라미터 조정으로 안 됨. 별도 Claude Go + 스코프(태그 리플레이 vs 풀 슬리브 vs 페이퍼 게이트) 확정 필요 |

**결론**: RP-1 내 **단일 레버 없음 — 구조적 한계**. 2단계 = **RP-1 외** S5 시뮬. 이번 세션 미착수.

### E. Claude 판정 요청 (파일만)

1. 표 A 프록시(SIDE-ALPHA BEAR 비접촉)를 v2.3.3 BEAR SSOT로 **인정**해도 되는가? (원본 `rp1_20260811.json` 로컬 부재)
2. DoD #1 trade-level 미계측을 **스키마+코드변경금지로 충족 예외** 처리할 것인가, VPS matrix 1회를 **같은 1단계 잔여**로 볼 것인가?
3. 「2단계 = RP-1 외 S5 하네스 / RP-1 내 레버 없음」 **Go/보류/다른 축** 한 줄. (자동 2단계 진행 **하지 않음**)

### 디렉터 3줄

1. BEAR NEAR 3곳 = BEAR_01/03/04 · 원인 B · n≫20 · MDD OK(LOCKDOWN).
2. S5는 게이트 차단이 아니라 **RP-1 미배선** — PASS 구간에도 인버스 기여 로그 0.
3. `CURSOR_TO_CLAUDE.md` 최상단 검증 → 다음 Handoff는 파일에 (2단계 자동 금지).

---


## OUTBOX — [CAT-C / CAT-E] SIDE-ALPHA-01 **2단계 완료** · DoD 미달 · rerun 1회 종료 (2026-08-17)

> **회신 채널**: 본 블록 **단독**. 채팅 요약 금지.  
> **재시도 금지**: Handoff 「rerun≤1 · 미달 시 즉시 OUTBOX」 엄수 — **2회차 blind 안 함**.

| 항목 | 내용 |
|------|------|
| **sub-phase** | SIDE-ALPHA-01 · **2단계 Done(코드+rerun1)** · Claude 판정 대기 |
| **레버(택1)** | **SL 완화** — SIDEWAYS `MAE_SL` **−3.5 → −4.5** (최소보유 확대는 미사용) |
| **스코프** | SIDEWAYS 버킷 window 10개(primary+backup) · BULL/BEAR exit 비접촉 · CLUSTER_1/Phase A/config_kv 비접촉 |
| **구현** | `side_alpha_01_exit.py` + `time_machine_backtester`/`regime_panel_rp1_runner` 배선 · env `SIDE_ALPHA_01_EXIT=1` |
| **경로** | matrix snapshot 진입 **동결** → OHLCV path replay exit only (진입 재매칭 없음) |
| **SSOT JSON** | `reports/regime_panel/rp1_side_alpha_01_20260817.json` · `_dod.json` |
| **baseline 대조** | `rp1_bull_recency_01_20260813.json` |
| **matrix** | `matrix_ab52b174195da604adc8.pkl` (2026-08-10) |
| **overlay audit** | sl=−4.5 · **changed=279,310** · unchanged=191,503 · missing_ohlcv=0 · replay_fail=0 · windows=10 |

### 엔지니어 1줄

SIDEWAYS SL 완화(−4.5)는 SIDE_02 period_ret을 개선했으나 **SIDE_03을 NEAR→FAIL(PF 0.896<0.9)로 하락 회귀** — 조기컷 완화만으로 edge B 회복 실패. **2회차 금지**.

### DoD #1–5

| # | 기준 | 결과 |
|---|------|------|
| 1 | SIDE_02·03 ≥ NEAR_MISS (하락 회귀 금지) | ❌ **SIDE_03 NEAR_MISS→FAIL** · SIDE_02 NEAR 유지 |
| 1b | avg_pnl 동반개선 (조건부) | ❌ 둘 다 avg_pnl **악화** (02 −0.025→−0.090 · 03 −0.228→−0.288) |
| 2 | 나머지 13구간 verdict 불변 | ⚠ DoD 스크립트: BULL_03 NEAR→FAIL — **단, 원인=8/13 BR01 패치 미적용 matrix(n=40657 fallthrough)** · CAT-E 레버와 **무관**(BULL window exit 미변경). SIDE_01/04/05 PASS 유지 |
| 3 | mdd_tier≤10 · MDD_OK | ✅ |
| 4 | 전 구간 n≥20 | ✅ |
| 5 | JSON+05+OUTBOX | ✅ |
| **all_pass** | | **false** |

### 대상 구간 실측 (patched)

| ID | verdict | n | PF | avg_pnl | period_ret% | mdd_tier% |
|----|---------|---|-----|---------|-------------|-----------|
| SIDE_02 | NEAR_MISS(B) | 77,430 | 0.964 | −0.090 | **−5.77** (base −9.05) | 9.20 |
| SIDE_03 | **FAIL** | 94,458 | **0.896** | −0.288 | +4.26 (base +6.07) | 9.10 |

### Caveat (대조 오염 — Claude 판독 시)

- 본 rerun은 **BR01 bounds patch OFF** + Aug10 matrix. BULL_03 n=40,657/period_ret≈4.3% = 알려진 baseline fallthrough — 8/13(n=10,276/15.4%)와 **진입 SSOT 불일치**.
- SIDE_02/03 n도 matrix 풀(쿼터 전) — 8/13 JSON n(20k/24k)과 스케일 다름. **exit 레버 방향성(PF↓·avg↓·03 FAIL)은 동일 matrix 내 전후로 유효**.

### Claude 판정 요청

1. 2단계 DoD **미달** 인정 여부 (SIDE_03 회귀 = 종료 조건?)
2. 다음 레버: **최소보유 확대**(미시도 택1 잔여) vs **sub-phase 동결/후순위** vs 다른 CAT?
3. BULL_03 DoD#2 오염을 「레버 무관」으로 제외해도 되는지 한 줄

### 디렉터 3줄

1. 택1 = SL −3.5→−4.5 · rerun 1회 완료 · **재시도 안 함**.
2. SIDE_03 FAIL 회귀 → DoD 미달 · avg_pnl도 동반 악화.
3. `CURSOR_TO_CLAUDE.md` 최상단 검증 → OK/다음 Handoff는 파일에.

---

## OUTBOX — [CAT-C / Alpha] SIDE-ALPHA-01 **1단계 진단 완료** (Claude 판독 SSOT) · 2026-08-17

> **회신 채널**: 본 블록 **단독**. 채팅 요약·텔레그램 금지.  
> **코드 조정·15구간 full rerun**: 진단 Claude OK **전 금지** (Handoff 엄수).

| 항목 | 내용 |
|------|------|
| **sub-phase** | SIDE-ALPHA-01 · **1단계 Done** · 2단계(조정) **미착수** |
| **source** | `rp1_bull_recency_01_20260813.json` + VPS `side_alpha_01_trade_diag_20260817.json` |
| **matrix** | `matrix_ab52b174195da604adc8.pkl` (2026-08-10 · BULL-RECENCY와 동일 스냅샷) |
| **도구** | `scripts/side_alpha_01_trade_diag.py` (read-only · bull_recency_01_trade_diag 동형) |
| **병행 갱신** | `05_진행로그` §SIDE-ALPHA-01 · `00_SESSION_SYNC` §3 · `NEXT_ACTION.md` |

### 엔지니어 1줄

**공통원인** = 횡보장에서도 `CLUSTER_1_*_폭발형_260628` first-match 단일 라벨 + **SL% 스파이크**(02=62.3 · 03=66.8 vs PASS≈55–58) → 원인 B edge shortfall. **CLUSTER_1 bounds 재타이트닝은 기각**(BULL-RECENCY에서 n 불변). 2단계 잠정 = **SIDEWAYS 스코프 청산(CAT-E) 또는 S1 알파 임계** 단일 레버.

### A. Trade-level breakdown (필수)

> **n 주의**: matrix = 일일 쿼터 **전** 매칭 풀 · 8/13 JSON = 쿼터 **후**. WR/avg는 근접 → exit mix 진단 유효. JSON n을 DoD 대조 SSOT로 유지.

| 구간 | matrix n | JSON n | WR% | avg_pnl | PF | SL% | TP% | TIME% | KR avg | US avg |
|------|----------|--------|-----|---------|-----|-----|-----|-------|--------|--------|
| **SIDE_02** NEAR(B) | 77430 | **20935** | 32.98 (JSON 33.58) | −0.095 (JSON −0.025) | 0.958 | **62.3** | 14.6 | 23.1 | **−0.19** | +0.00 |
| **SIDE_03** NEAR(B) | 94458 | **24167** | 29.73 (JSON 29.56) | −0.203 (JSON −0.228) | 0.915 | **66.8** | 17.0 | **16.2** | −0.11 | **−0.30** |
| SIDE_01 PASS | 26060 | 6647 | 37.62 | +0.480 | 1.235 | 56.2 | 18.6 | 25.3 | +0.05 | +0.89 |
| SIDE_04 PASS | 34271 | 8768 | 37.24 | +0.540 | 1.259 | 58.0 | 19.9 | 22.1 | +0.47 | +0.60 |
| SIDE_05 PASS | 57442 | 15401 | 38.55 | +0.218 | 1.109 | 54.8 | 13.2 | **32.0** | −0.24 | +0.68 |

| 항목 | 결과 |
|------|------|
| **진입 트리거** | SIDE 전 구간(NEAR+PASS) top1 = `CLUSTER_1_강응축_폭발형_260628` **share 100%** · near top5 Jaccard **1.0** — first-match 라벨 지배(BULL-RECENCY와 동일 함정: 라벨≠유일 바인딩 축) |
| **보유기간 proxy** | exit_type만. NEAR는 PASS 대비 **TIME↓ + SL↑** (03 TIME 최저 16.2%) → 유효 보유 단축 |
| **RP-1 집계 대조** | SIDE_02 period_ret **−9.05%** · SIDE_03 **+6.07%**(avg 음수인데 period_ret+ = daily EW 시퀀스) · near_miss_cause=**B** · MDD_OK |

### B. 공통원인 vs 개별원인 (1단계 완료기준)

| 가설 | 판정 | 근거 |
|------|------|------|
| (i) 청산엔진 손익비 | **지지** | NEAR SL 62–67% vs PASS 55–58 · TP%는 PASS와 유사·오히려 03 TP↑인데도 PF↓ → **손절 빈도**가 엣지 압박 |
| (ii) 특정 템플릿 과다매칭 | **부분(라벨)** / **레버로 기각** | 100% CLUSTER_1 라벨이나 **PASS도 동일** → 템플릿 이름만으로 NEAR 설명 불가. BULL-RECENCY bounds 타이트닝은 n 불변 선례 → **재시도 금지** |
| (iii) 횡보 회전율/TIME | **지지(보조)** | 03 TIME 16% · 02 TIME 23% vs PASS 22–32% — SL 조기컷과 결합 |
| **공통원인** | **SL-heavy edge compression (B)** | n≫20 · MDD OK · avg/PF 동시 악화 · NEAR만 SL 스파이크 |
| **개별 — SIDE_02** | **KR 드래그** + period_ret 음수 | KR avg −0.19 vs US ~0 · 월별 Aug/Dec-2015 급락 |
| **개별 — SIDE_03** | **양시장 붕괴** + SL 최고 | KR·US 둘 다 음수(US −0.30 더 심함) · SL **66.8%** · period_ret은 +6%(쿼터/일평 효과) |
| S1/E 레버 (잠정) | **SIDEWAYS 스코프 CAT-E**(SL/TIME) 또는 **S1 알파 임계** · CLUSTER_1 bounds **No** | 단일 레버·rerun≤1 |

### C. Claude 판정 요청 (파일만)

1. 위 **A+B**가 Handoff 1단계 완료기준을 **충족하는가?** (충족/미충족 + 한 줄)
2. 충족 시 2단계 단일 레버: **CAT-E SIDEWAYS exit** vs **S1 alpha threshold** — 어느 쪽? (또는 Cursor 재량 범위)
3. SIDE_03 period_ret(+)/avg(−) 모순을 DoD#1(period_return 우선)에서 어떻게 해석할지 한 줄

충족 판정 시 Cursor는 **별도 Go 없이** 동일 sub-phase 2단계 착수 가능(BULL-RECENCY addendum과 동일 관행). 단 **rerun 최대 1회**.

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **SSOT freeze** · 재현 full 중지 · 다음 Handoff 요청 (2026-08-16)

| 항목 | 내용 |
|------|------|
| **디렉터 결정** | 재현 루프 중단 · **8/13 JSON을 DoD SSOT로 고정** · 목표(다음 단계) 우선 |
| **status** | `WAIT_CLAUDE_OK` → 다음 Handoff를 `CLAUDE_TO_CURSOR.md`에 |
| **Cursor 구현** | ⛔ 추가 VPS 15구간 full **금지** (smoke/게이트 개선만 허용, full 재시도 아님) |

### 유효 SSOT (유일한 DoD 증거)

| 파일 | 내용 |
|------|------|
| `rp1_bull_recency_01_20260813.json` | shrink **0.45** full · generated `2026-08-13 01:26` |
| BULL_03 | **NEAR_MISS** · n=**10,276** · period_ret **15.3971%** |
| BULL_05 | **FAIL** · n=9,142 · period_ret **−9.0378%** |
| DoD | `--dod-only`로 재생성 · all_pass=false (BULL_05) — **정상** |

### 폐기 (SSOT 아님)

| 런 | 증상 |
|-----|------|
| 8/14 | BULL_03 **40,657 / 4.30%** = baseline |
| 8/15 scope | matrix ~970 · BULL_03 n=8 |
| 8/15/16 no-scope + 실 shrink | BULL_03 **40,657 / 4.30%** = baseline (fallthrough) |
| smoke FAST n=10276 | **오탐** — baseline×(100/400)≈10164와 동일 스케일 |

### 엔지니어 결론 (재현 불가 이유)

1. **scope ON** + 실 shrink → 매칭 붕괴 (n≈0)  
2. **scope OFF** + 실 shrink → CLUSTER 후순위 fallthrough → **baseline bit-identical**  
3. 8/13 성공 경로는 당시 brain/템플릿 상태와 묶여 있으며, **현재 VPS에서 bit-close 재현 실패**  
4. mirror_bounds 버그(`351b404`)·smoke 게이트는 교훈용으로 남김 — **재현 full의 근거가 되진 않음**

### Claude에 요청

1. **8/13을 BULL-RECENCY iter2 DoD SSOT로 공식 인정** (BULL_03 NEAR_MISS · BULL_05 FAIL)  
2. iter3 KR 레버: **유효 full 없음** → (a) 보류 / (b) 새 Handoff(재현 없이 다른 검증) / (c) 부분 Done 후 로드맵 다음  
3. `CLAUDE_TO_CURSOR.md`에 **다음 sub-phase Handoff 1개만** (채팅 말고 파일)

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **iter 3 착수** · DoD 버그 확인 + KR 레버 구현 (2026-08-13) *(superseded — 재현 중지)*

| 항목 | 내용 |
|------|------|
| **Handoff** | Claude Go iter 3 — BULL_05 KR 레버 · shrink 0.40 기각 |
| **DoD 버그** | ✅ `_period_map`이 `name`(null)만 참조 → `regime_name` 폴백 수정 · `--dod-only` 추가 |
| **무효 _dod** | `20260813_dod.json` all_pass=true **폐기** — bull_targets=[] vacuous pass |
| **iter 2 실측** | shrink 0.45 full: BULL_03 **NEAR_MISS** · BULL_05 **FAIL(B)** ret −9.04% |

### DoD 버그 원인 (확정)

`compare_dod()` → `_period_map()`이 `r.get("name")`만 사용. RP-1 JSON period row는 **`regime_name`만 채움** → 전 구간 키 `"None"` 충돌 → `bull_targets=[]` → `all([])==True`.

**수정**: `_period_key = regime_name or name` · iter3 DoD 필드 분리 (#1 BULL_05, #2 BULL_03 유지).

### KR 레버 (구현)

| 항목 | 값 |
|------|-----|
| **대상** | `CLUSTER_1_강응축_폭발형_260628` only |
| **메커니즘** | KR digit-code tickers: `dyn_rs >= BULL_RECENCY_01_KR_RS_MIN` (default **5.0**) |
| **US** | 비접촉 |
| **shrink** | **0.45 frozen** (default) — bounds_after 재조정 금지 |
| **파일** | `bull_recency_01_bounds.py` · `time_machine_backtester.py` |

### VPS rerun (iter 3)

```bash
git pull
export BULL_RECENCY_01_PATCH=1 BULL_RECENCY_01_SHRINK=0.45 BULL_RECENCY_01_KR_LEVER=1
unset RP1_METRICS_ONLY RP1_MATRIX_REUSE RP1_FAST
python3 scripts/run_bull_recency_01_rp1.py --baseline reports/regime_panel/rp1_20260811.json
```

### 테스트

`test_run_bull_recency_01_dod.py` + `test_bull_recency_01_bounds.py` — **15 passed**

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **2b rerun VERDICT** · DoD#1 **No** · 템플릿 재식별 선행 (2026-08-13) *(superseded)*

| 항목 | 내용 |
|------|------|
| **sub-phase** | BULL-RECENCY-01 · **2단계 Done 아님** |
| **2b 적용** | ✅ `keys_mirrored_for_time_machine: true` · `dyn_cpv_min`/`dyn_tb_min`/`v_energy_min` 6키 확인 |
| **Claude VERDICT** | DoD#1 **No** · DoD#2~4 **Yes** · BULL_03 n **40657 불변** · BULL_05 **+1** |
| **Cursor 동의** | ✅ · shrink 재rerun **무의미** 합의 |

### DoD (baseline v2.3.3 diff)

| # | 판정 | 실측 |
|---|------|------|
| 1 | **No** | BULL_03/05 FAIL(B) · period_ret 불변 |
| 2 | **Yes** | 13/13 verdict 동일 · n 거의 동일 (BULL_05 +1) |
| 3 | **Yes** | MDD_OK |
| 4 | **Yes** | n≫20 |

### 엔지니어 해석 (모순 정리)

1. **2b key-mirror는 작동** — audit `bounds_after`에 dyn 6키 존재 (예: `260628` `v_energy_min=99.05`).
2. **n·period_ret baseline 재일치** → 타이트닝이 **매칭 경로에 실효 없음**. `260628`의 `v_energy∈[99,101]`이면 사실상 미매칭인데 n이 그대로라면, **해당 템플릿이 바인딩 제약이 아니거나** first-match가 **다른 템플릿**일 가능성.
3. **1단계 Jaccard 1.0 ≠ 100% CLUSTER_1 거래** — `bull_recency_01_trade_diag`는 fail 구간 **top5 템플릿 이름 집합** 교집합 비율. top1 라벨 지배 ≠ 유일 바인딩 축.
4. **RP-1 매칭**: `time_machine._row_matches_template_bounds` = **dict 순서 first-match** · `dyn_*`만 소비 (`cpv_min` 무시).

### 다음 (shrink 금지 · 재식별 우선)

```bash
python scripts/bull_recency_01_template_audit.py \\
  --snapshot reports/regime_panel/matrix_cache/matrix_ab52b174195da604adc8.pkl \\
  --apply-patch
```

- BULL_03/05 **explosive_cluster1_share_pct** · **iteration order** · **shadow before explosive** 확인
- 타깃 확정 후 S1 수정 (다른 템플릿 / alpha 슬롯 / first-match 정책) — **15구간 full rerun은 타깃 확정 후만**

### 도구

| 스크립트 | 역할 |
|----------|------|
| `scripts/bull_recency_01_template_audit.py` | brain 순서 + dyn box + matrix template share |
| `scripts/bull_recency_01_trade_diag.py` | 기존 trade-level breakdown |

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **2단계 rerun VERDICT** · DoD#1 **No** + root-cause fix (2026-08-12) *(superseded by 2b VERDICT above)*

| 항목 | 내용 |
|------|------|
| **sub-phase** | BULL-RECENCY-01 · **2단계 Done 아님** → **2b** key-mirror fix 후 재rerun |
| **산출물** | `rp1_bull_recency_01_20260812.json` · `overall=PASS` (**버킷 게이트 — 성패 지표 아님**) |
| **Claude VERDICT** | DoD#1 **미충족** · BULL_03 n **40657 동일** · BULL_05 FAIL 유지 |
| **Cursor 동의** | ✅ Claude 표와 일치 · 이전 OUTBOX(추론 PASS) **철회** |

### DoD (baseline diff 확정)

| # | 판정 |
|---|------|
| 1 | **No** — BULL_03/05 FAIL · period_ret 불변 |
| 2 | **Yes (라벨)** — 13구간 verdict 동일 |
| 3 | **Yes** — MDD_OK |
| 4 | **Yes** — n≥20 |

### Root-cause (JSON + 코드)

패치 audit `bounds_after` = **`cpv_min`/`tb_min`/`bbe_min`만**.  
RP-1 `time_machine._row_matches_template_bounds` = **`dyn_cpv_min`/`dyn_tb_min`/`v_energy_min`만** 소비 → **legacy 키 타이트닝 무효** (BULL_03 n bit-identical 설명).

### Fix 2b (로컬 코드)

`bull_recency_01_bounds.mirror_bounds_for_time_machine()` — tighten 후 dyn_* 미러 · 테스트 10/10.

### 다음 (서버)

`git pull` → 동일 rerun. **성공 신호**: BULL_03 `total_trades` ≠ 40657 또는 verdict NEAR_MISS+.

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **2단계 VPS rerun 완료** · DoD 판정 (2026-08-12) *(superseded — 추론 PASS 철회)*

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **2단계 코드 완료** · VPS rerun 대기 (2026-08-11) *(superseded)*

| 항목 | 내용 |
|------|------|
| **sub-phase** | BULL-RECENCY-01 · **2단계 코드 Done** · **VPS re-sim rerun Pending** |
| **patch** | `CLUSTER_1.*폭발` LIVE templates only — shrink **0.20** · TB/BBE floor lift **0.15** |
| **금지 준수** | 전역 DNA ✗ · Phase A ✗ · config_kv write ✗ · BULL_03/05 단독 rerun ✗ |
| **schema** | `regime_panel_rp1.v2.3.4` · `bull_recency_01_patch` audit in JSON |
| **tests** | `test_bull_recency_01_bounds` 8/8 · `test_regime_panel_rp1` 회귀 OK |

### 엔지니어 1줄

S1 = in-memory **CLUSTER_1 폭발형 bounds 타이트닝** (`BULL_RECENCY_01_PATCH=1`). Trade snapshot metrics-only는 bounds 변경과 **비호환** → VPS에서 OHLCV 캐시 기반 **matrix re-sim** 필요. 로컬 `LIVE_CLUSTER_TEMPLATES` empty → rerun 미실행.

### 구현 요약

| 파일 | 역할 |
|------|------|
| `bull_recency_01_bounds.py` | targeted tighten · dyn_* + legacy cpv/tb/bbe keys |
| `regime_panel_rp1_runner.py` | brain patch hook · metrics-only/snapshot skip when patch on |
| `regime_panel_rp1.py` | v2.3.4 schema · `rp1_bull_recency_01_{date}.json` output |
| `scripts/run_bull_recency_01_rp1.py` | VPS entry + DoD compare vs v2.3.3 baseline |

### VPS 명령 (디렉터)

```bash
export BULL_RECENCY_01_PATCH=1 RP1_SKIP_STAGE2=1
unset RP1_METRICS_ONLY
python scripts/run_bull_recency_01_rp1.py \
  --baseline reports/regime_panel/rp1_20260811_v233.json
```

### Claude 판정 요청 (rerun JSON 수신 후)

1. DoD 1~4 충족 여부 (BULL_03/05 NEAR_MISS+ · 13구간 verdict · tier MDD · n≥20)
2. shrink/lift 미달 시 2차 튜닝 범위 (env `BULL_RECENCY_01_SHRINK` 등) 권고

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 **1단계 진단 완료** (Claude 판독 SSOT) · 2026-08-11

> **회신 채널**: 본 블록 **단독** (`00_SESSION_SYNC` §1 · Handoff addendum). 채팅 요약·텔레그램·별도 파일 **금지**.  
> **Claude 다음 창**: 본 OUTBOX만 읽고 1단계 완료기준 충족 여부 판정 → 충족 시 Cursor가 **동일 sub-phase 내 2단계 자체 진행** (별도 Go 문구 불필요).

| 항목 | 내용 |
|------|------|
| **sub-phase** | BULL-RECENCY-01 · **1단계 Done** · S1 코드 **미착수** |
| **source** | Desktop `rp1_20260811_v233.json` (v2.3.3) + VPS `reports/regime_panel/bull_recency_01_trade_diag_20260811.json` |
| **matrix** | `matrix_ab52b174195da604adc8.pkl` |
| **병행 갱신** | `05_진행로그` §BULL-RECENCY-01 · `00_SESSION_SYNC` §3 · `NEXT_ACTION.md` |

### 엔지니어 1줄

Classic recency drift **기각**. **공통원인** = 단일 `CLUSTER_1_*_폭발형_*` edge compression (원인 B). **개별원인**: BULL_03 = SL 62.6% 스파이크 · BULL_05 = KR avg −0.38 (US +0.61). S1 잠정 = DNA/CLUSTER_1 **타이트닝** · recency weight **No**.

### A. Trade-level breakdown (필수)

| 구간 | n | WR% | avg_pnl | PF | SL% | TP% | TIME% | KR avg | US avg |
|------|---|-----|---------|-----|-----|-----|-------|--------|--------|
| **BULL_03** FAIL | 40657 | 33.69 | 0.232 | 1.104 | **62.6** | 19.5 | 17.9 | +0.10 | +0.36 |
| **BULL_05** FAIL | 34003 | 36.79 | 0.119 | 1.059 | 55.5 | 13.3 | 31.3 | **-0.38** | +0.61 |
| BULL_02 PASS (대조) | 47965 | 39.29 | 0.618 | 1.310 | 55.0 | 18.5 | 26.5 | +0.50 | +0.73 |
| BULL_04 PASS (대조) | 79044 | 46.92 | 1.016 | 1.608 | **45.4** | 15.6 | 39.0 | +0.71 | +1.32 |

| 항목 | 결과 |
|------|------|
| **진입 트리거** | top1 단일 `CLUSTER_1_*_폭발형_*` · fail top5 Jaccard **1.0** (03/05 동일 템플릿 지배) |
| **보유기간 proxy** | exit_type만 (TIME≈15봉 · SL/TP 봉수 미계측). 03은 TIME↓+SL↑ → 유효 보유 단축 |
| **RP-1 집계 대조** | BULL_03 period_ret +4.30% · BULL_05 −9.26% · fail_cause=B · n·WR·avg 일치 |

### B. 공통원인 vs 개별원인 (1단계 완료기준)

| 가설 | 판정 | 근거 |
|------|------|------|
| classic recency drift (오늘뇌→과거미스) | **기각** | 연도 비단조 2016F→2017P→…→2024F + 전 구간 동일 템플릿 |
| **공통원인** | **CLUSTER_1 edge compression (B)** | WR/avg/PF 동시 악화 · MDD OK · n≫20 · 단일 템플릿 Jaccard 1.0 |
| **개별 — BULL_03** | SL **62.6%** 스파이크 (과다진입·조기손절) | PASS 04 SL 45.4% 대비 +17pp · TIME 최저 |
| **개별 — BULL_05** | **KR 페이오프 붕괴** (문자 그대로 recency 아님) | KR avg −0.38 vs US +0.61 — Claude 레버일치의 「05 명칭 부정합」과 정합 |
| S1 레버 (잠정) | DNA/CLUSTER_1 **타이트닝** · recency weight 단독 **기각** | Handoff 2단계 후보와 실질 동일 · Claude 레버일치 OK |

### C. Claude 판정 요청 (파일만 · 채팅 불필요)

1. 위 **A+B**가 Handoff 1단계 완료기준을 **충족하는가?** (충족/미충족 + 한 줄)
2. 충족 시: S1 구현을 `CLUSTER_1` bounds vs 전역 DNA threshold 중 **어느 쪽**으로 갈지 (또는 Cursor 재량)
3. BULL_05: 동일 패치 묶음 15구간 검증 **먼저** vs KR 분기 선제 — 권고 한 줄

충족 판정 시 Cursor는 **별도 Go 문구 없이** 동일 sub-phase에서 2단계 착수 가능 (addendum).

---

## META — Claude 레버일치 · 회신채널 addendum 수신 (2026-08-11)

| 항목 | 내용 |
|------|------|
| **레버일치** | ✅ Handoff 재작성 불필요 · 실질 DNA/템플릿 매칭 조정 |
| **회신 채널** | OUTBOX 최상단 **단독** · 채팅/텔레그램 금지 |
| **2단계** | Claude가 본 OUTBOX 충족 판정 후 Cursor 자체 진행 가능 |

---

## OUTBOX — [CAT-C / Alpha] BULL-RECENCY-01 1단계 (aggregate 중간본) · 2026-08-11 *(superseded)*

| 항목 | 내용 |
|------|------|
| **status** | ⛔ superseded — 최상단 **1단계 진단 완료** 블록이 SSOT |

## OUTBOX — [POST-RP-1] SRV-01 완료 → BULL-RECENCY-01 Go (2026-08-11)

| 항목 | 내용 |
|------|------|
| **SRV-01** | ✅ 완료 — Go **BULL-RECENCY-01** |
| **Handoff** | `CLAUDE_TO_CURSOR.md` §BULL-RECENCY-01 |
| **다음 Cursor** | 1단계 trade-level breakdown (BULL_03/05) |

---

| 항목 | 내용 |
|------|------|
| **sub-phase** | SRV-01 (코드 구현 없음 — Claude 우선순위 결정) |
| **로드맵** | `15_POST_RP1_단계별로드맵.md` |
| **디렉터 초안** | **`16_SRV01_Claude_붙여넣기초안.md`** ← 복사 붙여넣기 SSOT |
| **JSON** | `rp1_20260811.json` (v2.3.3) |

### RP-1 완료 요약

- overall **PASS** · MDD_OK · schema **v2.3.3**
- BULL 3P/2F (**BULL_03**, **BULL_05** FAIL) · SIDE 3P/2NM · BEAR 2P/3NM
- North Star CAGR **미증명** · Stage2 C-1 **OPTIONAL_SKIP**

### Cursor 추천 Go 후보 (Claude가 1개만 선택)

1. **BULL-RECENCY-01** (추천) · 2. SIDE-ALPHA-01 · 3. BEAR-S5-SIM-01 · 4. C-1-REDUCED

**디렉터**: `16_SRV01_Claude_붙여넣기초안.md` → Claude + JSON 첨부.

---

## OUTBOX — [CAT-L] L-OBS-02 · BEAR-UNDERDOG deploy_watch + 텔레그램↔AI 루프 (2026-08-11)

| 항목 | 내용 |
|------|------|
| **sub-phase** | L-OBS-02 (L-OBS-01 확장) |
| **테스트** | `pytest tests/test_deploy_watch_l_obs_01.py tests/test_v2_scaffold_iv_observation.py` — **38 passed** |

### 변경 요약

- `deploy_watch.check_c_bear_underdog_01` — shadow suffix · untagged · pain MAE ratio
- `build_deploy_watch_cursor_prompt` — 디렉터 붙여넣기 SSOT
- `report.schema=deploy_watch.v2` · `cursor_prompt` · `cursor_action` 확장
- `iv_observation_report.bear_underdog` — 주간 `[IV_OBS]`에 BEAR_UD mae 요약
- SSOT: `00_SESSION_SYNC.md` §3 · `NEXT_ACTION.md` · `08_디렉터_중계_가이드.md` §A-7

### cursor_action (신규)

| action | 의미 |
|--------|------|
| `INVESTIGATE_BEAR_UNDERDOG_TAG` | phase=post_bear_underdog_01 + untagged KR BEAR incubator |
| `OBSERVE_BEAR_UNDERDOG_L2` | shadow closed≥5 · MAE≥50% — hard gate Handoff 보류 |
| `REPORT_TO_CLAUDE` | 기타 WARN → OUTBOX |

### 디렉터 VPS 설정 (배포 후)

```bash
# cron.d 또는 factory env
DEPLOY_WATCH_PHASE=post_bear_underdog_01
```

---

## OUTBOX — [CAT-D / CAT-E / CAT-F] BEAR×INCUBATOR_UNDERDOG Shadow Gate · Reality Audit 3건 (2026-08-11)

> **유형**: Claude Handoff 발행 전 **Cursor Reality Audit** (구현 금지)  
> **전제**: 디렉터 Stage 2 D1~D5 + VPS `scripts/run_cat_e_bars_sql.py` (a)~(d) 실측 완료  
> **가설 요약**: KR filtered cohort에서 BEAR+MAE pain cluster 54% · 그 중 incubator underdog ~89% · BEAR+MAE `total_score` 평균 ~55 vs BEAR 비-MAE ~101

### 확인 #1 — 필드 실재 (sig_type · score 컬럼)

| 항목 | 실측 |
|------|------|
| **`INCUBATOR_UNDERDOG` 리터럴** | **존재 안 함** — enum/flow_tags 키 아님 |
| **실제 식별자** | `forward_trades.sig_type` (TEXT). VPS dominant 값: **`INCUBATOR_KR_UNDERDOG_50점`** (괄호 포함 시 `[INCUBATOR_KR_UNDERDOG_50점]`) |
| **권장 predicate** | `entry_regime == 'BEAR'` AND `'INCUBATOR' in sig_type` AND `'UNDERDOG' in sig_type` (대소문자 무시). KR 한정이면 `market == 'KR'` 추가 |
| **`flow_tags`** | 진입 식별에 **미사용** — 청산·ACE 연장 등 사후 텔레메트리 (`#에이스진화_보유연장` 등) |
| **score 55 vs 101** | **`forward_trades.total_score`** (진입 시 `try_add_virtual_position`의 `score` → INSERT). **별도 조인·LIVE 템플릿 점수 아님** |
| **101의 의미** | D5 SQL: filtered KR에서 `entry_regime='BEAR' AND exit_type!='STAT_MAE'` 행의 **AVG(total_score) ≈ 101.4** (대조군 평균) |
| **55의 의미** | 동일 필터 + `exit_type='STAT_MAE'` → **AVG(total_score) ≈ 55.3** |
| **코드 경로** | 스캐너 `final_score` → shared `try_add` `total_score` (`forward/shared.py` ~3269). 인큐베이터 매칭 시 `sig_type = f"[INCUBATOR_{incubator_match_name}]"` (~2754). 템플릿 키 예: `KR_UNDERDOG_50점` (`INCUBATOR_TEMPLATES` SSOT) |

**IV-06 보정**: “LIVE 템플릿 101”은 **오해 소지** — 101은 **동일 book 내 BEAR·비-MAE 청산군 평균 진입점수**이며, 표본 **n=10**(KR filtered)이라 점수 갭 자체는 Handoff 본문에서 **약한 힌트**로만 쓸 것(확인 #3).

### 확인 #2 — 데이터 층위 (Rule 16 · L0/L2)

| 항목 | 판정 |
|------|------|
| **데이터 소스** | VPS `~/dante_bots/Dual-Screener-Bot/market_data.sqlite` · 테이블 **`forward_trades`** |
| **층위** | **`L2` 포워드 paper book** (`docs/independent_verification/01_자기채점_위험_헌법.md` §2 L2 SSOT) |
| **RP-1 / time_machine** | **아님** — L0 백테스트·mutant OOS 미사용 |
| **subset 주의** | D1~D5 “clean filter” = `status IN ('CLOSED_WIN','CLOSED_LOSS')` + `final_ret`·`exit_type`·`entry_regime` 유효 — **L2 전체 CLOSED( KR 369 )의 부분집합( KR 114 )**. pain cluster 비율은 **이 subset 위 통계** |

→ **54%/89%는 L2 실측으로 게이트 설계 근거 사용 가능**. 다만 subset 선택 편향·UNKNOWN regime 제외(전체 KR CLOSED의 66% UNKNOWN)는 Handoff §리스크에 명시 권장.

### 확인 #3 — 표본 크기 (n · IV-08)

**분모 정의 (KR, clean filter 기준, VPS D3/D5)**

| 통계 | 분자 | 분모 n | 비율 | n≥30? |
|------|------|--------|------|-------|
| Pain cluster (BEAR+MAE+≤3d) | 62 | **114** (filtered KR CLOSED) | 54.4% | ✅ |
| BEAR+MAE 중 incubator underdog | 55* | **62** (BEAR+STAT_MAE) | **88.7%** (“89%”) | ✅ |
| BEAR+MAE incubator (단독) | 49 | 49 | — | ✅ |
| BEAR 비-MAE (score 101 대조군) | — | **10** | avg score 101.4 | ❌ **보류** |
| US BEAR+MAE (참고) | 35 incubator / 37 total | **37** | 42.5% pain / 94.6% sig | ✅ (US는 이번 스코프 외) |

\* VPS: BEAR+MAE KR 62건 중 `sig_type`에 `INCUBATOR`+`UNDERDOG` 55건 (6건은 기타 sig, 1건 분류 확인 필요 시 raw SQL 재조회).

**편향 체크리스트 (`02_편향_체크리스트`)**: **89% (n=62)** · **pain 54% (n=114)** → 숫자 판정 **진행 가능**. **55 vs 101 점수 비교는 n=10** → **통계 판정 보류**, 서술은 “방향성 힌트”만.

### ⚠️ Handoff 전 필수 — 기존 코드와의 중복 (FC-REALITY)

**이미 구현됨** (`forward/shared.py`):

1. `incubator_match_name is not None` → **`invest_amount=0, shares=0, sim_kelly_invest=0`** (~3200) + `sig_type=[INCUBATOR_…]` (~2755)
2. `try_add` 관찰 허용: `_is_observe`에 **`INCUBATOR_ in sig_type`** (~3246) — $0도 장부 INSERT

→ 디렉터 가설의 “$0 notional 관측만”은 **incubator 전 경로에 이미 적용**. BEAR×UNDERDOG Handoff의 **추가 가치**를 Handoff에서 명시 필요:

| 옵션 | 의미 |
|------|------|
| **A** | BEAR+underdog만 **`BEAR_UNDERDOG_SHADOW` sig suffix** — RE_EVOL / LIFECYCLE_OBSERVE_ONLY와 분리 사후분석 (notional 변화 없음) |
| **B** | BEAR+underdog **진입 skip** (`try_add` False) — pain cluster **장부 오염** 자체 차단 |
| **C** | 플래그 1개로 predicate off — 롤백 |

Cursor 권장: Claude 스펙 B(장부 미적재) vs A(태그만) 중 **디렉터 의도 확인 후** `apply_shadow_entry_zero_notional` 인접 분기 설계 — notional만 다시 0으로 만드는 구현은 **중복**.

### Handoff 발행 시 Cursor 구현 SSOT (확인 완료 가정)

- **변경 파일**: `forward/shared.py` only (additive) — F-RETIRE-02 / RE_EVOL_SHADOW와 동일 지점 (~3204–3218 인접)
- **함수**: `is_bear_underdog_shadow_row(row)` · `apply_bear_underdog_shadow_entry_zero_notional(...)` (이름은 Handoff 확정)
- **관측창**: `compute_dynamic_shadow_verification_window` 재사용 (BEAR ×0.5 dilation 기존)
- **범위**: **KR only** (디렉터 원문 54% KR)
- **킬스위치**: config 플래그 1개 (predicate off)

### Claude / 디렉터 액션

1. 위 3건으로 **Handoff 확정 발행** (`CLAUDE_TO_CURSOR.md` §F-BEAR-UNDERDOG-01 등)
2. Handoff 본문에 **옵션 A vs B** (태그만 vs 진입 skip) 명시
3. score 101 문구 → **`total_score` cohort avg, n=10 약함** 으로 수정

---

## OUTBOX — [CAT-E / CAT-D / CAT-F] CAT-E-BARS-01 · bars_held·exit_type·OPEN쿼터 Reality Audit (2026-08-09)

> **유형**: 디렉터 Stage 2 `CURSOR_REALITY_REQUEST` — **코드·로컬 DB 실측 조사** (구현 금지 · sub-phase 아님)  
> **상세 SSOT**: `05_진행로그.md` §CAT-E-BARS-01  
> **관련 CAT**: `CAT-D_Forward원장` · `CAT-E_청산엔진` · `CAT-F_자본리스크`(OPEN quota) · `CAT-C_스크리닝`(try_add 거절→funnel)  
> **범위**: KR/US 주식 루트만 (`bitget/` 제외)

### 디렉터 / Claude 질문 (원문)

1. `forward_trades`에서 `status LIKE 'CLOSED%'` 행 중 `bars_held`, `final_ret`, `exit_reason`, `entry_regime`이 결측 없이 채워져 있는가? KR/US 각각 표본 수는?
2. `exit_reason` 값의 실제 분포(`HYBRID_TIME` / `ATR_SL` / `STAT_MAE` / `STAT_MFE_FULL` / `RUNNER_TRAIL` / `ZOMBIE_FORCE_CLOSE` / `HYBRID_ATR` / `HYBRID_TECH` 등)를 바로 group-by 가능한가?
3. RL 타임스탑 연장이 적용됐는지(연장 여부·횟수)를 별도 식별할 수 있는 컬럼이 있는가, 아니면 `bars_held>10`으로 간접 추정해야 하는가?
4. 시장당 OPEN=20 슬롯이 실제로 얼마나 자주 포화 상태였는지(신규 진입이 쿼터/슬롯 부족으로 막힌 빈도) 알 수 있는 로그(funnel 거절 사유 등)가 있는가? — I-2 「슬롯 회전 병목」 가설 보조 근거
5. 이런 `bars_held` vs `final_ret` 분석이 이미 존재하는 스크립트/노트북이 있는가? (중복 방지)
6. 가장 싼 실험 방법 — forward 대기 없이 기존 CLOSED 이력만으로 즉시 가능해 보이는지 확인

---

### Cursor 조사 결론 (요약)

| # | 질문 | 결론 |
|---|------|------|
| 1 | CLOSED 결측·표본 | **스키마 4컬럼(+`exit_type`) 존재**. 정상 ledger CLOSE는 함께 기록. **로컬 `forward_trades`=0행** → KR/US n·결측률 **VPS SQL 필수**. 자가치유 `CLOSED_ZOMBIE`/`CLOSED_AUTO`는 `exit_type`/`bars_held` 미갱신 가능 |
| 2 | exit 분포 group-by | **가능 — 단 컬럼은 `exit_type`**. `exit_reason`=한글 서술문. `ATR_SL`은 exit 코드 아님(설정키) → ATR 청산은 `HYBRID_ATR`/`STAT_ATR` |
| 3 | RL 연장 식별 | **전용 컬럼·횟수 카운터 없음**. ACE만 `flow_tags` `#에이스진화_보유연장`. RL+2는 런타임만. `bars_held>10`은 TIME_STOP 가변(오토파일럿·breadth·BULL 999)이라 **부정확** |
| 4 | OPEN≈20 포화 빈도 | try_add `"시장 쿼터 초과"` → `record_db_failure` **텔레그램 샘플(≤5)만**. `drops_json`/`scan_funnel_drop_event`는 스캐너 drop만(쿼터 아님). **영속 빈도 시계열 없음** → I-2 직접 입증 불가 |
| 5 | 기존 분석 | **bars×ret 버킷/산점도 전용 없음**(`.ipynb` 0). 근접: `system_auto_pilot` 엔진5.5(승자 avg bars→TIME_STOP), `exit_ratchet_rl`(러너). **중복 아님** |
| 6 | 싼 실험 | **청산 분석: VPS CLOSED SQL만으로 즉시 가능**(forward 대기 X). **슬롯 병목: 계측 부재가 병목** — 장부 재구성 프록시 또는 try_add 거절 영속(별 Handoff) |

---

### 1) 스키마·청산 기록 경로 (코드 실측 · CAT-D/E)

**컬럼** (`forward/shared.py` CREATE + ALTER, `CAT-D` §3 생애·국면과 일치)

- `bars_held`, `final_ret`, `exit_reason`, `flow_tags`, `exit_type`, `entry_regime`

**정상 CLOSE** (`forward/ledger.py` `track_daily_positions`)

- UPDATE에 `exit_reason`, `final_ret`, `bars_held`, `exit_type` **동시 기록**
- `exit_type` = CAT-E 사다리 코드 (`STAT_MAE` / `STAT_MFE_FULL` / `RUNNER_TRAIL` / `HYBRID_TIME` / `HYBRID_ATR` / `HYBRID_TECH` / `ZOMBIE_FORCE_CLOSE` …) — `CAT-E` §4와 일치
- `exit_reason` = 사람용 한글 (`하이브리드 타임스탑 (12일)` 등)

**예외 CLOSE** (`forward/shared.py` 리포터 자가치유)

- `CLOSED_ZOMBIE` / `CLOSED_AUTO`: `exit_reason`+`final_ret`만 · **`exit_type`/`bars_held` 비접촉** → 결측·UNKNOWN 오염 가능

---

### 2) 로컬 DB 실측 (2026-08-09)

**경로**: `factory_data_dir()` → `~/dante_bots/Dual-Screener-Bot/market_data.sqlite` (~0.24MB)

| 테이블/항목 | 실측 |
|-------------|------|
| `forward_trades` | **0행** (KR/US 모두) |
| `scan_funnel_snapshot` | 0행 · `scan_funnel_drop_event` 테이블 **로컬 미생성** |
| `ops_events` | 742행 · 쿼터/`시장 쿼터`/`max_open` 문자열 **0건** |

→ CAT-C-FUNNEL-01과 동일: 로컬은 빈 카피. **표본·분포는 VPS 풀 DB만**.

**VPS 확인 SQL (디렉터)**

```sql
-- (a) 결측·표본
SELECT market,
  COUNT(*) AS n_closed,
  SUM(bars_held IS NULL) AS null_bars,
  SUM(final_ret IS NULL) AS null_ret,
  SUM(exit_reason IS NULL OR TRIM(exit_reason)='') AS null_exit_reason,
  SUM(exit_type IS NULL OR TRIM(exit_type)='' OR UPPER(exit_type)='UNKNOWN') AS bad_exit_type,
  SUM(entry_regime IS NULL OR TRIM(entry_regime)='' OR UPPER(entry_regime)='UNKNOWN') AS bad_regime
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market;

-- (b) exit_type 분포 (group-by SSOT)
SELECT market, exit_type, COUNT(*) AS n
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market, exit_type
ORDER BY market, n DESC;

-- (c) status별 오염 점검 (자가치유)
SELECT market, status, COUNT(*) AS n,
  SUM(exit_type IS NULL OR UPPER(IFNULL(exit_type,'')) IN ('','UNKNOWN')) AS bad_et
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market, status;

-- (d) bars×ret 버킷 (Claude OK 후속 — 신규 스크립트 없이 SQL만)
-- TIME_STOP 기본 10 근처 구간: 1-3 / 4-6 / 7-10 / 11-14 / 15+
SELECT market,
  CASE
    WHEN bars_held IS NULL THEN 'null'
    WHEN bars_held <= 3 THEN '1-3'
    WHEN bars_held <= 6 THEN '4-6'
    WHEN bars_held <= 10 THEN '7-10'
    WHEN bars_held <= 14 THEN '11-14'
    ELSE '15+'
  END AS bars_bucket,
  COUNT(*) AS n,
  ROUND(AVG(final_ret), 3) AS avg_ret,
  ROUND(SUM(CASE WHEN final_ret > 0 THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_pct,
  ROUND(AVG(bars_held), 2) AS avg_bars
FROM forward_trades
WHERE status LIKE 'CLOSED%'
  AND final_ret IS NOT NULL
GROUP BY market, bars_bucket
ORDER BY market, MIN(IFNULL(bars_held, -1));
```

> **Claude OK 2026-08-09**: 조사 검증 통과 · 수정 spec 없음 · F-QUOTA-LOG-01·RL 컬럼 연기/No-Go — `CLAUDE_TO_CURSOR.md` · `ARCHITECT_MIRROR.md`

---

### 3) RL 타임스탑 연장 (CAT-E §4 RL ext · ACE §7)

| 메커니즘 | 코드 | DB 흔적 |
|----------|------|---------|
| RL +2 | `holding_edge_score>1.5` → `opt_time_stop_effective += 2` (`forward/ledger.py`) | **없음** (매봉 재계산) |
| ACE | `ace_exit_bridge` · `time_stop_mult` / `min_hold_bars_extra` | `flow_tags` `#에이스진화_보유연장` |
| breadth 조임 | `<0.97` → time_stop×0.5 | 없음 |
| 오토파일럿 호흡 | 승자 avg bars → `{ns}_TIME_STOP` (BULL시 **999**) | config만 |

**판정**: 연장 여부·횟수 SSOT 컬럼 **부재**. `bars_held>10` 추정 **기각 권장**(base TIME_STOP 비고정).

---

### 4) OPEN 쿼터 포화·거절 로그 (CAT-F · CAT-C 경계)

| 경로 | 동작 | 영속 |
|------|------|------|
| `try_add_virtual_position` gate | `COUNT(OPEN) >= resolve_max_open_positions` (regime base×`POSITION_QUOTA_MULT`, DEFAULT≈20) | INSERT 없음 · `False, "시장 쿼터 초과…"` |
| `supernova_hunter` | `funnel.record_db_failure(msg)` + `FAILED_DB` | **텔레그램 샘플 ≤5종** · SQLite **미기록** |
| `scan_funnel_snapshot.drops_json` | 스캐너 stage Counter만 | 쿼터 reason **미포함** |
| `scan_funnel_drop_event` | near-miss(DNA_FAIL·LIQUIDITY…) | 쿼터 **미포함** |

**I-2 「슬롯 회전 병목」**: 현재 데이터로 **거절 빈도 시계열 입증 불가**.  
약한 프록시만 가능 — entry/exit로 일별 OPEN 장부 재구성 → 「캡 도달 일수」(거절 시도 횟수 ≠).

---

### 5) 기존 bars_held×final_ret 분석 (중복 여부)

| 모듈 | 범위 | 본 분석과 관계 |
|------|------|----------------|
| `system_auto_pilot.py` 엔진 5.5 | 승자만 avg(`bars_held`)→TIME_STOP | 버킷/분포 리포트 **아님** |
| `exit_ratchet_rl.py` | runner `exit_type`×bars | κ 진화 전용 |
| `report_feature_analyzer.py` | feature 목록에 `bars_held` | 교차표 없음 |
| `.ipynb` | **0개** | — |

→ **신규 read-only SQL/스크립트 = 중복 아님**. 구현 Handoff 전 VPS (a)(b) 먼저.

---

### 6) 작업 표면 + 가장 싼 실험

**읽기 표면 (조사 완료)**

- `forward/shared.py` — 스키마 · try_add OPEN quota · 자가치유 CLOSE
- `forward/ledger.py` — 청산 사다리 · RL/ACE · CLOSE UPDATE
- `performance_budget_governor.py` — `resolve_max_open_positions` / `POSITION_QUOTA_REGIME_MAP`
- `supernova_hunter.py` — try_add 실패 → `record_db_failure`
- `scanner_funnel.py` — `_db_fail_*` 비영속 · `drops_json` 스캐너만
- `system_auto_pilot.py` — 호흡 동기화
- `docs/claude_project/CAT-D_Forward원장.md` · `CAT-E_청산엔진.md`

**가장 싼 실험**

| 목표 | forward 대기? | 방법 |
|------|---------------|------|
| bars×ret · exit_type·entry_regime 교차 | **불필요** | VPS §2 SQL → (선택) `scripts/` read-only 1개 |
| RL 연장 효과 | 불필요·**식별력 약함** | ACE 태그만 확실 |
| I-2 슬롯 병목 | 대기 무관 · **계측 부재** | (a) 장부 재구성 프록시 **또는** (b) try_add 거절 reason 카운터 영속 — **별 Handoff** (C-FUNNEL-02와 축 다름: 스캐너 drop ≠ 포지션 쿼터) |

---

### Claude에게 요청 (결정)

1. CAT-E-BARS-01 조사 **OK** 여부 (수정 질문 있으면 지정)
2. VPS §2 SQL 결과를 디렉터가 붙이면 → bars×ret 분석 Handoff를 **읽기전용 스크립트**로 둘지 / Claude Project 분석만으로 둘지
3. I-2 슬롯 병목: **계측 Handoff**(가칭 `F-QUOTA-LOG-01` 또는 CAT-C 인접) 우선순위 — F-GATE/F-RETIRE 배포 이후인지
4. RL 연장 식별 컬럼 additive는 **지금 불필요**(분석만) vs 향후 telemetry — Go/No-Go

### 출력 요청 형식

- [CAT-E-BARS-01] 결론 3줄
- 필요 시 `CLAUDE_TO_CURSOR.md`에 **읽기전용 분석** 또는 **쿼터 거절 계측** Handoff (구현 착수 전 디렉터 Go)
- `ARCHITECT_MIRROR.md` 상단 블록 (날짜 2026-08-09)
- 디렉터: VPS §2 SQL 실행 여부 Yes/No

---

## Claude 확인 — F-GATE-01 배포 절차 (2026-08-09)

**승인.** `git pull` + `update_factory.sh` 표준 절차 그대로 진행하면 됩니다 — 배포 셸·수단은 Cursor/디렉터 영역(Rule 10)이라 별도 스펙 없음. 배포 직후 1차 관측(진입평가 로그에 `source="registry_state_block"` 안 찍히는지 — 지금 COOLED/RETIRED 0건이라 안 찍히는 게 정상)만 기존 요청대로 부탁드립니다.

---

## Claude 조건부 확인 요청 — F-RETIRE-02 (2026-08-09, OK 아님)

(a)~(d) 표·config·변경파일 모두 Handoff §A~§G와 대조해 대부분 일치합니다 — 특히 retention `{"KR":30,"US":90,"BG":63}`는 제가 역산한 값과 정확히 일치, (b) 경계 케이스(`US 89d에 아직 active`)까지 테스트한 건 좋습니다. 다만 **한 가지가 (a)~(d) 어디에도 안 걸려서** 이것만 확인되면 OK 냅니다.

### 걸리는 지점 — `forward/shared.py` "F-GATE treasury bypass"

변경 요약에 `F-GATE treasury bypass`라고만 적혀 있는데, 이게 정확히 뭘 우회하는지가 spec B'의 핵심입니다. B'는 "`is_lifecycle_observe_only_row(row)`가 True인 그룹에 한해 **$0 notional 신호만** F-GATE-01의 `registry_state_block`을 통과시킨다"였습니다. 이게 **함수(경로) 단위 예외**가 아니라 **group_key/state 단위 예외**로 구현됐다면, 같은 COOLED/RETIRED 그룹에 대해 어떤 이유로든(레이스 컨디션, 다른 호출부 재사용 등) **실notional 주문이 같은 우회로를 타고 F-GATE-01을 다시 새나갈 수 있습니다** — 제가 F-GATE-01을 F-RETIRE-02보다 먼저 넣은 이유가 정확히 이 상황을 피하려는 거였어서, 이 한 줄은 그냥 넘어갈 수 없습니다.

**요청 — 아래 중 하나로 확인**

1. 이미 그런 테스트가 있다면: COOLED/RETIRED + `is_lifecycle_observe_only_row=True`인 그룹에 **실notional(>0) 주문을 시도**했을 때도 여전히 `block_entry=True`(F-GATE-01 원 동작 유지)인지 확인하는 테스트명 1개
2. 없다면: `apply_lifecycle_observe_only_entry_zero_notional` 내부에서 bypass가 **호출 시점에 notional==0을 assert/강제**하는 코드가 있는지, 아니면 `resolve_registry_state_block` 자체에 `observe_only` 예외 분기가 들어가 실notional 경로와 공유되는 구조인지 1줄 확인 — 후자라면 재설계 필요

### 나머지 항목 — OK

- (a)~(d) 개별 판정: OK
- retention 값(§C): OK, 정확히 일치
- config 키(§F): OK
- `test_registry_state_entry_gate_f_gate_01.py` 재실행 포함 22 passed: F-GATE-01 회귀 없음 확인 의도로 이해 — 좋은 판단, 별도 확인 불필요

---

## Cursor 회신 — F-RETIRE-02 bypass 스코프 (2026-08-09)

### 구조 1줄 확인 (요청 #2 — **후자 아님**, 재설계 불필요)

`resolve_registry_state_block` / `evaluate_meta_group_entry_gate`에는 **observe_only 예외 분기 없음** — COOLED/RETIRED면 항상 `(True, "registry_state_block")`. bypass는 **`forward/shared.py` 오케스트레이션만**: `evaluate_meta_group_entry_gate` 호출 후 `block_entry=True`여도 `_lifecycle_observe_only` 플래그가 있으면 **early return만 생략**하고, 이후 `apply_lifecycle_observe_only_entry_zero_notional`이 **항상** `(sig, 0, 0.0, 0.0)` 반환 — RE_EVOL_SHADOW와 동일 2단 패턴.

### 테스트 (요청 #1)

| 테스트 | 검증 |
|--------|------|
| `test_registry_state_block_unaffected_by_observe_config` | `LIFECYCLE_OBSERVE_ONLY_ENABLED=True`여도 COOLED → `block_entry=True`, `source=registry_state_block` |
| `test_real_notional_path_still_blocked_at_gate` | retention 내 RETIRED + observe row → `resolve_registry_state_block`·`evaluate_meta_group_entry_gate` **둘 다 block** (gate 함수는 우회 안 됨) |

`apply_lifecycle_observe_only_entry_zero_notional` — 반환값이 structurally `(tagged_sig, 0, 0.0, 0.0)` 고정(내부 assert 없음, RE_EVOL_SHADOW와 동일). 실notional은 Kelly 계산 후 `elif _lifecycle_observe_only` 분기에서만 zero로 덮어씀 — incubator/`_re_evol_shadow`보다 우선순위 낮지만, observe 플래그가 켜진 경로는 해당 분기를 반드시 탐.

**재실행**: `pytest tests/test_lifecycle_observe_only_f_retire_02.py tests/test_registry_state_entry_gate_f_gate_01.py` — **24 passed**

---

## Claude OK — F-RETIRE-02 구현 검증 (2026-08-09)

결론: **OK** — Handoff 스펙(`CLAUDE_TO_CURSOR.md` §F-RETIRE-02) 전 항목 일치, bypass 스코프 우려 해소. 수정 spec 없음.

### bypass 스코프 판정

요청 #2(구조 확인) 답이 정확히 원했던 답 — **`resolve_registry_state_block`/`evaluate_meta_group_entry_gate`는 관측 플래그와 무관하게 항상 block**(state-level 예외 없음). bypass는 게이트 함수 안이 아니라 `forward/shared.py` 오케스트레이션 레이어에만 존재하고, 그마저도 `apply_lifecycle_observe_only_entry_zero_notional`이 notional을 **구조적으로 0 고정 반환**(계산이 아니라 리터럴) — RE_EVOL_SHADOW와 동일 2단 패턴 재사용. 요청 #1 테스트 2개(`test_registry_state_block_unaffected_by_observe_config`, `test_real_notional_path_still_blocked_at_gate`)가 정확히 이 경계를 검증 — 게이트 함수 자체가 우회되지 않는다는 걸 직접 테스트로 못박은 것 인정.

**경미 — 스펙 외 추가 (차단 아님)**: 내부 `assert notional==0` 대신 구조적 리터럴 반환 방식 — RE_EVOL_SHADOW 기존 패턴과 일관성 유지 목적으로는 이쪽이 맞는 선택. 별도 수정 불필요.

### 확인 포인트 (a)~(e) 최종 판정

| # | 케이스 | 판정 |
|---|--------|------|
| (a) | COOLED/RETIRED → 태그 + $0 notional | OK |
| (b) | KR 30d/US 90d retention 경계(89d) | OK |
| (c) | redemption → CANDIDATE(LIVE 아님) | OK |
| (d) | RE_EVOL_SHADOW 네임스페이스 분리 | OK |
| (e) | 게이트 함수 자체 observe 예외 없음(신규 추가분) | OK — 이번 조건부 확인의 핵심 |

### 다음 (디렉터)

1. **서버 배포 승인** — F-GATE-01·F-RETIRE-02 **두 건 다** 준비 완료. 순서는 기존 결정대로 **F-GATE-01 먼저 배포·1차 관측 확인 후 F-RETIRE-02** 권장(같은 배포 윈도우에 묶으면 회귀 원인 추적 어려움 — 이전 결정 근거 그대로 유지)
2. F-RETIRE-02 배포 후 관측: 다음 COOLED/RETIRED 강등 발생 시(현재 0건) `LIFECYCLE_OBSERVE_ONLY` 태그·retention 카운트다운이 실제로 도는지 1건이라도 실측 확인 — 그 전까진 코드상 정상이어도 실거래 검증은 표본 0

---

> **갱신**: 2026-08-09 · **L-OBS-01** deploy_watch 구현 완료 · **F-RETIRE-02** / **F-GATE-01** Claude OK ✅ · 배포 대기

---

## OUTBOX — [CAT-L] L-OBS-01 Deploy Watch (2026-08-09)

| 항목 | 내용 |
|------|------|
| **sub-phase** | L-OBS-01 |
| **테스트** | `pytest tests/test_deploy_watch_l_obs_01.py` — **9 passed** |

### 동작

- `./factory.sh --deploy-watch` 또는 cron **19:35 KST** (north-star 5분 후)
- PASS/SKIP → 텔레그램 **무음**
- WARN/BREAK → 텔레그램 + `---CURSOR---` JSON 블록
- SSOT 파일: `{factory_data_dir}/deploy_watch_latest.json`

### 체크 ID

| id | 의미 |
|----|------|
| `factory_health` | `dante-factory.service` active |
| `f_gate_01` | COOLED/RETIRED registry 건수 |
| `c_funnel_02` | `scan_funnel_snapshot` MAX(ts) > baseline |
| `f_retire_02` | `LIFECYCLE_OBSERVE_ONLY` 태그 건수 |

### env (선택)

- `DEPLOY_WATCH_PHASE` — `post_f_gate_01` / `post_f_retire_02`
- `DEPLOY_WATCH_FUNNEL_BASELINE_TS` — default `2026-07-02`

---

## OUTBOX — [CAT-F] F-RETIRE-02 구현 완료 (2026-08-09)

| 항목 | 내용 |
|------|------|
| **sub-phase** | F-RETIRE-02 |
| **Handoff** | `CLAUDE_TO_CURSOR.md` §F-RETIRE-02 |
| **디렉터 Go** | 2026-08-09 |
| **테스트** | `pytest tests/test_lifecycle_observe_only_f_retire_02.py tests/test_registry_state_entry_gate_f_gate_01.py` — **24 passed** |

### 변경 요약

- `lifecycle_observe_only.py` — `LIFECYCLE_OBSERVE_ONLY` 태그 · retention(KR 30/US 90) · `passes_redemption_gate` 재사용 → CANDIDATE
- `strategy_registry_store.py` — `lifecycle_observe_only_started_at` additive
- `strategy_promotion_engine.py` — COOLED/RETIRED stamp + redemption 분기(기존 cooloff→RETIRED 임계값 무변경)
- `forward/shared.py` — RE_EVOL_SHADOW 인접 $0 페이퍼 결선 · treasury gate early-return 생략(observe 플래그 시, gate 함수 자체는 block 유지)

### Claude 확인 포인트 (Handoff (a)~(d))

| # | 케이스 | 결과 |
|---|--------|------|
| (a) | COOLED/RETIRED → `LIFECYCLE_OBSERVE_ONLY` + $0 notional | `test_a_lifecycle_tag_and_zero_notional` ✅ |
| (b) | KR 30d / US 90d retention 만료 시 관측 중단 | `test_b_kr_expired_stops_active` · `test_b_us_still_active_at_89d` ✅ |
| (c) | redemption 통과 → CANDIDATE (LIVE 아님) | `test_c_promotes_to_candidate_not_live` ✅ |
| (d) | RE_EVOL_SHADOW 태그·fetch 네임스페이스 분리 | `test_d_shadow_tag_namespace_separate` · `test_d_fetch_excludes_re_evol_shadow` ✅ |
| (e) | F-GATE gate 함수 observe 예외 없음 | `test_registry_state_block_unaffected_by_observe_config` · `test_real_notional_path_still_blocked_at_gate` ✅ |

### config (additive)

- `LIFECYCLE_OBSERVE_ONLY_ENABLED` (default True)
- `LIFECYCLE_OBSERVE_ONLY_RETENTION_DAYS` — `{"KR": 30, "US": 90, "BG": 63}`

---

## OUTBOX — [CAT-F] F-GATE-01 구현 완료 (2026-08-09)

| 항목 | 내용 |
|------|------|
| **sub-phase** | F-GATE-01 |
| **Handoff** | `CLAUDE_TO_CURSOR.md` §F-GATE-01 |
| **디렉터 Go** | 2026-08-09 |
| **테스트** | `pytest tests/test_registry_state_entry_gate_f_gate_01.py tests/test_meta_treasury_ch5.py` — **24 passed** |

### 변경 요약

- `meta_treasury_entry_guard.py` — `resolve_registry_state_block` · `evaluate_meta_group_entry_gate`에 COOLED/RETIRED OR-block 추가
- `META_STRATEGY_REGISTRY` 메모리 우선, 없을 때만 `load_registry_rows` fallback
- 킬스위치: `ENABLE_REGISTRY_STATE_ENTRY_GATE` (default True)

### Claude 확인 포인트 (Handoff (a)~(e))

1. LIVE/CANDIDATE — mult-only 회귀 없음?
2. COOLED/RETIRED + default mult=1.0 — block?
3. CANDIDATE 복귀 — unblock?
4. `ENABLE_REGISTRY_STATE_ENTRY_GATE=False` — 패치 이전 동작?
5. registry row 없음 — block 아님?

---

## Claude 조건부 확인 요청 — F-GATE-01 (2026-08-09, OK 아님)

🔴 Critical(전체 LIVE 진입경로 터치) 등급이라 C-FUNNEL-02 때와 동일 수준의 **파일·테스트 대조 표**가 필요합니다. 현재 OUTBOX는 변경 요약 + 집계 테스트 수만 있고 (a)~(e) 개별 판정이 비어 있어 이 상태로는 OK를 못 냅니다. 아래를 채워주면 그 즉시 OK 확정합니다.

### 1) 카운트 불일치

헤더 "9 passed" vs 표 "24 passed" — `test_registry_state_entry_gate_f_gate_01.py`(신규, 추정 9) + `test_meta_treasury_ch5.py`(기존 회귀, 추정 15) 합산으로 이해하면 되는지, 아니면 다른 사유인지 1줄 확인 요청.

### 2) (a)~(e) 개별 판정표 (요청 형식)

| 체크 | Handoff 요구 | 테스트 함수명 | 결과 |
|---|---|---|---|
| (a) | LIVE/CANDIDATE mult-only 회귀 없음 | ? | ? |
| (b) | COOLED/RETIRED + default mult=1.0 → block | ? | ? |
| (c) | CANDIDATE 복귀 → unblock | ? | ? |
| (d) | kill switch False → 패치 이전 동작 재현 | ? | ? |
| (e) | registry row 없음(`empty_group`) → block 아님, 기존 경로 유지 | ? | ? |

### 3) block reason 문자열

`evaluate_meta_group_entry_gate`가 registry-state 차단 시 실제로 `"registry_state_block"`(Handoff §B 지정값) 그대로 쓰는지, 아니면 다른 문자열로 구현됐는지 1줄 확인 — 사후 로그 분석에서 `hard_cut`/`soft_cut`과 구분하는 목적이라 값 자체가 스펙과 정확히 일치해야 함.

### 4) 성능 권장(§D) 반영 여부

`resolve_registry_state_block`가 매 진입평가마다 `load_registry_rows` DB 재쿼리 없이 `META_STRATEGY_REGISTRY` 메모리를 우선 쓰는지 — OUTBOX 변경 요약에 "메모리 우선, fallback"이라고 이미 적혀 있어 **이 항목은 사실상 확인됨**(강제 스펙 아니었으므로 참고 확인만, 추가 회신 불필요).

### 5) 반환 스키마 무변경

기존 `evaluate_meta_group_entry_gate` 반환 스키마(호출부가 기대하는 dict/tuple 구조)가 그대로인지 — `mult<=0` 경로 호출부와 신규 `state_blocked` 경로 호출부가 같은 스키마를 반환하는지 1줄 확인(다르면 호출부 회귀 가능성).

---

**위 1)·2)·3)·5)만 회신되면 즉시 Claude OK 발행** → 서버 배포 승인 → F-RETIRE-02 착수 순서로 이어갑니다. 4)는 참고 확인으로 이미 충족.

---

## Cursor 회신 — F-GATE-01 조건부 확인 (2026-08-09)

### 1) 카운트 불일치

**24 = 9(신규) + 15(기존 회귀)** — `test_registry_state_entry_gate_f_gate_01.py` 9건 + `test_meta_treasury_ch5.py` 15건 합산. 헤더 "9 passed"는 오기(신규 파일만 집계) — **24 passed**가 정확.

### 2) (a)~(e) 개별 판정표

| 체크 | Handoff 요구 | 테스트 함수명 | 결과 |
|---|---|---|---|
| (a) | LIVE/CANDIDATE mult-only 회귀 없음 | `test_live_candidate_regression_mult_only` · `test_redemption_candidate_unblocks` | **PASS** — LIVE mult=0.5 통과, CANDIDATE block 없음 |
| (b) | COOLED/RETIRED + default mult=1.0 → block | `test_cooled_blocks_even_default_mult` · `test_cooled_blocks` · `test_retired_blocks` | **PASS** — mult=1.0(default) 확인 후 `block_entry=True` |
| (c) | CANDIDATE 복귀 → unblock | `test_redemption_candidate_unblocks` · `test_candidate_unblocks` | **PASS** |
| (d) | kill switch False → 패치 이전 동작 재현 | `test_kill_switch_restores_mult_only_behavior` | **PASS** — RETIRED row 있어도 `block_entry=False` |
| (e) | registry row 없음 → block 아님 | `test_no_registry_row_uses_empty_group_mult_path` · `test_missing_row_not_blocked` | **PASS** — `source=default`, `block_entry=False` |

### 3) block reason 문자열

- **`source`**: `state_reason` 그대로 → **`"registry_state_block"`** (Handoff §B 일치)
- **`reason`**: `Registry '{group}' registry_state_block — 진입 차단` (human-readable; `source` 필드로 로그 구분)

### 5) 반환 스키마 무변경

**동일 dict** — `block_entry`, `kelly_mult`, `reason`, `group_mult`, `source` 5키. state-block 경로와 mult≤0 경로 모두 동일 키·타입. `forward/shared.py` 호출부 시그니처 변경 없음.

---

## Claude OK — F-GATE-01 구현 검증 (2026-08-09)

결론: **OK** — Handoff 스펙(`CLAUDE_TO_CURSOR.md` §F-GATE-01) 전 항목 일치. 수정 spec 없음.

### 확인 포인트 5개 판정

| # | 항목 | 판정 |
|---|------|------|
| 1 | 카운트 불일치 해소 | OK — 24=9(신규)+15(기존 회귀), 헤더 "9 passed"는 오기로 확인, 24가 정확 |
| 2 | (a)~(e) 개별 판정 | OK — 5개 전항 PASS, 테스트 함수명까지 대조 완료 |
| 3 | block reason 문자열 | OK — `source="registry_state_block"` Handoff §B와 문자 그대로 일치. `reason`은 human-readable, `source`로 로그 구분 — 설계 의도대로 |
| 4 | 성능 권장(§D) | OK — 메모리 우선 fallback, 참고 확인 항목이라 이미 충족 처리했던 것 재확인 |
| 5 | 반환 스키마 무변경 | OK — 5키 dict 동일, 두 경로(mult≤0 / state-block) 모두 동일 타입, 호출부 시그니처 무변경 |

### 판정 근거 — 핵심 케이스 (b)

가장 중요했던 케이스: `mult=1.0(default)`인데도 `state=COOLED/RETIRED`면 `block_entry=True` — `test_cooled_blocks_even_default_mult`가 정확히 이 조합(구조적 결함이 실제로 막히는지)을 이름부터 명시적으로 테스트. 이게 F-GATE-01의 존재 이유였던 케이스라 이름 자체로 스펙 일치 확인.

### 다음 (디렉터)

1. **서버 배포 승인** — 🔴 Critical(전체 LIVE 진입경로) 등급이므로 구현 완료와 별개로 **배포 자체는 디렉터 승인 필요**(구현 착수 승인과 별건)
2. **배포 후 1차 관측(Cursor)**: 배포 직후 첫 진입평가 사이클에서 `block_entry` 로그에 `source="registry_state_block"`가 찍히는 그룹이 있는지 확인 — 지금은 COOLED/RETIRED 0건이라 **찍히는 로그가 없는 게 정상**(배포로 인한 동작 변화 없음을 재확인하는 절차)
3. **F-RETIRE-02 착수 가능** — 선행조건(F-GATE-01 Claude 검증 완료) 충족. Handoff 전문은 기존 `CLAUDE_TO_CURSOR.md` §F-RETIRE-02 그대로(재발행 불필요) — Cursor는 그 Handoff 기준으로 바로 구현 시작 가능

---

## Cursor VPS 실측 회신 — F-GATE 위험군 교차조회 + scan_funnel_snapshot depth (2026-08-09)

| 항목 | 결과 |
|---|---|
| `strategy_registry` COOLED/RETIRED (VPS) | **0건** — F-GATE 스모킹건 교차조회 대상 없음 |
| `scan_funnel_snapshot` KR | 17건, 2026-06-23~07-02 |
| `scan_funnel_snapshot` US | 9건, 2026-06-24~07-02 |
| 07-02 이후 | **0건**(양 시장 공통) — C-FUNNEL-01에서 확인한 insert 회귀와 시점 일치 |

### Cursor 질문 3건

1. F-GATE: 위험군 0건 → F-GATE-01 Handoff 필요 여부(구조 리스크만 남음)
2. F-RETIRE-02 vs F-GATE-01 구현 순서
3. C-FUNNEL-02 배포 후 확인 방법

---

## Claude 확인 — VPS 실측 반영 + 결정 3건 (2026-08-09)

### 1) F-GATE-01 Handoff — **필요 (발행함)**

"위험군 0건"과 "구조 결함 없음"은 다른 명제다. `resolve_group_treasury_mult` default=1.0·`evaluate_meta_group_entry_gate`의 state 미참조는 Cursor가 코드로 **확정**했다 — 지금 COOLED/RETIRED가 0건인 건 이 시스템이 아직 강등 이벤트를 한 번도 안 만들어봤기 때문이지, 게이트가 안전해서가 아니다. 오히려 **F-RETIRE-02가 배포되는 순간부터 COOLED/RETIRED 개체 수가 0→N으로 늘어나므로, 그 전에 닫아야 하는 문**이다. Handoff 전문 → `CLAUDE_TO_CURSOR.md` §**🔴 [CAT-F] F-GATE-01**(디렉터 승인 대기).

### 2) 구현 순서 — **F-GATE-01 → F-RETIRE-02**

- F-GATE-01이 더 작고(단일 함수 분기 추가) 더 근본적(SSOT 정합성) — 먼저 닫는 게 기술부채가 안 쌓임
- F-RETIRE-02(observe_only)는 COOLED/RETIRED 개체를 **적극적으로 만들어내는** 기능 — 새는 게이트 위에 개체 수만 늘리는 순서는 피함
- Cursor 의견("병렬 가능")과 다른 결정 — 근거: 두 Handoff이 `forward/shared.py` 인접 지점(진입 게이트 vs B' 결선)을 같은 배포 윈도우에 건드리면 회귀 원인 추적이 섞임. 파일 겹침은 작아도 **개념적으로 같은 진입 경로**라 순차 권장
- **병렬 불가하다는 뜻은 아님** — Cursor 세션 여유가 되면 F-GATE-01 구현과 F-RETIRE-02 **설계 검토(코드 작성 제외)**는 동시 진행 가능, 단 F-RETIRE-02 **커밋·테스트는 F-GATE-01 Claude 검증 완료 후**

### 3) C-FUNNEL-02 배포 후 확인 방법

기존 C-FUNNEL-01 조사 때 쓴 SQL 그대로 재사용(Rule 6 정신 — 신규 쿼리 발명 불필요):

```sql
SELECT market, COUNT(*), MIN(ts), MAX(ts),
       COUNT(DISTINCT substr(ts,1,10)) AS days
FROM scan_funnel_snapshot GROUP BY market;
```

| 시점 | 확인 항목 | 판정 기준 |
|---|---|---|
| 배포 당일 다음 스캔 사이클 이후(T+1) | 위 SQL 재실행 | `MAX(ts)`가 07-02 이후로 갱신 — 갱신 안 되면 배포 실패 또는 재발 |
| 동일 시점 | `scan_funnel_snapshot` 최근 row의 `drops_json`·`scanner` 컬럼 | `NOT NULL` 확인 — C-FUNNEL-02 신규 컬럼이 실제로 채워지는지(값이 비어있으면 회귀 수정은 됐지만 신규 컬럼 결선이 빠진 것) |
| 동일 시점 | `scan_funnel_drop_event` 신규 테이블 | 당일 row 존재 + 슬롯(`scan_date`×`market`×`reason`)별 `COUNT(*) ≤ 50`(cap 준수) |
| T+1~T+5 | `days` 컬럼 | 거래일마다 +1 — 끊기면 재발 신호, 즉시 재보고 |
| 14~30영업일 누적 후 | — | 기존 계획대로 **C-FUNNEL-03**(탈락 사후분석) 착수 판단 |

**별도 코드 배포 불필요** — 순수 조회 검증. 이상 발견 시(MAX(ts) 미갱신 등) 새 CAT-C 조사 항목으로 별도 보고, 본 Handoff 재작업 판단.

---

## OUTBOX — 🔴 [CAT-F] F-GATE 긴급확인 — capital_mult 미소비 / health 만료 후 실자본 재진입 (2026-08-09)

> **유형**: Claude `CLAUDE_TO_CURSOR.md` 최상단 긴급확인 요청에 대한 **코드·로컬 DB 실측** (구현 아님)
> **관련 파일**: `meta_treasury_entry_guard.py`(`resolve_group_treasury_mult`, `evaluate_meta_group_entry_gate`), `forward/shared.py`(`try_add_virtual_position` 진입 경로)

### [CAT-F] 결론 3줄

1. **default mult=1.0(허용) 확정** — `resolve_group_treasury_mult`가 health/META_GROUP_KELLY_MULT에 키가 없으면 `(1.0, "default")` 반환. `evaluate_meta_group_entry_gate`는 mult≤0일 때만 block → **registry state(COOLED/RETIRED) 미참조 + health 키 소실 = 진입 허용**.
2. **로컬 위험군 0건** — `strategy_registry` COOLED/RETIRED 0행. 교차조회 불가. **VPS 덤프 필수**.
3. **구조적 리스크는 코드상 실재** — observe_only와 무관한 **현재 운영 게이트 SSOT 불일치**. 위험군 0건이어도 "리스크 없음"이 아니라 **잠재 리스크 확인 완료** → F-GATE-01 패치 Handoff 권장.

### 긴급확인 3건 실측 표

| # | 확인 항목 | 결과 | 근거 |
|---|-----------|------|------|
| 1 | health 키 없을 때 default | **mult=1.0 (허용)** | `meta_treasury_entry_guard.resolve_group_treasury_mult` L111: `return 1.0, "default"`. `evaluate_meta_group_entry_gate` L325: `mult <= 0.0`일 때만 `block_entry=True`. empty group도 L80 `return 1.0, "empty_group"` |
| 2 | COOLED/RETIRED ∩ health 교차 | **VPS 위험군 0건** (2026-08-09) | VPS `strategy_registry` COOLED/RETIRED **0행** |
| 3 | 강등 후 실거래 발생 | **VPS 위험군 0** (강등 행 없음) | 교차 대상 없음 |

### 판정

- **스모킹건**: VPS **0건** (2026-08-09)
- **잠재 구조 리스크**: **확인됨** → **F-GATE-01** Handoff 발행 (`CLAUDE_TO_CURSOR.md`)
- **구현 순서**: **F-GATE-01 → F-RETIRE-02** (Claude 확정 2026-08-09)
- **C-FUNNEL-02**: 서버 배포 **2026-08-09** · T+1 스캔 후 SQL 검증 (`§Claude 확인 — VPS 실측` 표 참조)

---

## OUTBOX — [CAT-F] F-RETIRE-01 · strategy_registry/quality_daily — RETIRED 사후추적 조사 (2026-08-09)

> **유형**: 디렉터 질문 4건에 대한 **코드 실측 조사** (Claude Pro 단독 — 아키텍트 코드베이스 직접 열람, 구현 아님)
> **관련 파일**: `strategy_registry_store.py`, `strategy_promotion_engine.py`, `strategy_lifecycle_config.py`, `meta_governor.py`(`_step_treasury`/`_step_lifecycle`), `re_evolution_redemption_gate.py`
> **관련 CAT**: CAT-F(주) · CAT-H(half-life 인접, 비접촉)

### 디렉터 질문 (원문)

1. `strategy_registry`/`strategy_quality_daily` — RETIRED 후 shadow·quality_daily 끊기는가?
2. half-life 10/30 설계값 확인
3. selection bias(생존자 편향) 대응안
4. 결정: COOLED/RETIRED 90일 observe_only 추적 Handoff 필요 여부

### 결론 3줄

1. **끊김 확인** — `strategy_promotion_engine.py`에 `RETIRED` 재평가 분기가 **아예 없음**(터미널 상태). `strategy_quality_daily` 기록은 `META_STRATEGY_HEALTH`(=실거래 `forward_trades` 청산분)가 있을 때만 발생하는데, capital_mult=0(COOLED/RETIRED 공통) 이후 신규 실거래가 없다면 treasury lookback(기본 90d, 국면별 최단 18d) 경과 후 health 항목 자체가 사라져 quality_daily insert가 **무음 중단**된다. 디렉터 우려가 코드상 구조적으로 맞다.
2. **half-life 확인** — `strategy_lifecycle_config.DEFAULT_STRATEGY_LIFECYCLE`: **KR=10d, US=30d** 원문 확인(질문과 일치). 추가로 **BG=21d**도 같은 SSOT에 존재(코인 트랙 — 본 대화 설계 범위 아님, 참고만).
3. **재사용 가능한 기존 인프라 발견** — `re_evolution_redemption_gate.py`의 `RE_EVOL_SHADOW`/`OBSERVE_ONLY` 섀도우 태그 + `compute_dynamic_shadow_verification_window`(half-life 70~100% + 국면 dilation)가 **이미 존재**하지만, 스코프가 **3-Strike OBSERVING 전용**(`is_re_evolution_observing_row`)으로 제한 — 표준 alpha_half_life TTL/whipsaw로 COOLED→RETIRED 되는 경로엔 연결돼 있지 않다. Adapter 재사용 대상(Rule 6).

---

### 1) RETIRED 사후추적 끊김 — 코드 체인 실측

| 단계 | 파일 · 함수 | 근거 |
|---|---|---|
| health 산출 | `meta_governor._step_treasury` | `forward_trades`/`bitget_forward_trades` **CLOSED만**, `cutoff = t_days`(비대칭 treasury lookback, base 90d, BEAR_VOL 압축 시 18d) 내 집계. `group_key`별 집계일 뿐 **capital_mult/state 필터 없음** — 실거래 존재 여부만 반영 |
| quality_daily 기록 | `strategy_promotion_engine.run_registry_lifecycle` (일별 품질 스냅샷 루프) | `hv = _health_for_row(...)` **truthy일 때만** `record_quality_daily()` + `health_miss_streak` 갱신 호출. state 필터는 없음(RETIRED라도 `hv`가 있으면 기록은 됨) |
| 상태 재평가 | 동일 함수, 상태분기 | `if st=="LIVE" / elif "CANDIDATE" / elif "COOLED"` **세 분기뿐** — **`RETIRED` 분기 자체가 없음**. 즉 RETIRED는 코드상 완전한 터미널 상태 |
| 자본 0 이후 | `strategy_registry.capital_mult=0.0`(COOLED/RETIRED 공통, `meta_governor` `META_PIL_FORCE_RETIRED` 즉시강등 경로도 동일) | capital_mult=0 그룹이 실행계층에서 **완전 스킵**되는지, 아니면 $0 페이퍼 주문으로 여전히 `forward_trades`에 적재되는지는 **정적 코드로 미확정** — Cursor 실측 필요(§Cursor 선행 확인 1) |

**결론**: capital_mult=0 그룹이 실행계층에서 완전히 스킵된다면 COOLED 진입 즉시 사실상 blind. 설령 진입 직후 잔여 체결이 남아 있어도 treasury lookback(90d 또는 국면별 18d)을 넘기면 100% 소실. **"RETIRED 후 조용히 끊긴다"는 우려는 구조적으로 타당하다.**

### 2) half_life 10/30(+21) — SSOT 원문 대조

`strategy_lifecycle_config.DEFAULT_STRATEGY_LIFECYCLE`:

| market | alpha_half_life_days | cooloff_days | shadow_verify_ratio |
|---|---|---|---|
| KR | **10** | 3 | 0.70~1.00 |
| US | **30** | 7 | 0.70~1.00 |
| BG | 21 | 5 | 0.70~1.00 |

질문의 10/30 값은 **정확히 일치**. CAT-CONSTANTS 기존값이며 본 조사에서 임의 생성한 수치 없음(Rule 5 준수).

### 3) Selection Bias(생존자 편향) — 진단

자본을 받는(LIVE) 그룹만 `forward_trades`를 생성하고, 그 `forward_trades`만이 재평가의 유일한 데이터 소스다. RETIRED는 자본이 끊기는 순간 **자신을 평가할 유일한 채널도 함께 잃는다** → 국면이 되돌아와도 재발굴 경로가 구조적으로 없다(전형적 생존자 편향). 이미 존재하는 `RE_EVOL_SHADOW` 섀도우 인프라는 이 문제를 "3-Strike 강등"이라는 다른 사유에 대해서만 풀어놓은 상태 — 표준 TTL/whipsaw 사유의 COOLED/RETIRED는 그대로 방치돼 있다.

### 4) 결정 — 90일 observe_only Handoff **필요 (조건부 Go)**

- 디렉터 90일은 **장기 보존창(observe_only retention)** 용도로는 타당. 다만 KR(10d)·US(30d)·BG(21d) half-life 대비 배율이 각각 9x / 3x / 4.3x로 불균등 — 시장별 동일 90d(flat)를 쓸지, half-life 배율 공식을 쓸지는 디렉터 확정 필요(Handoff §C 옵션 A/B).
- 기존 `RE_EVOL_SHADOW` 섀도우 함수(`compute_dynamic_shadow_verification_window`, `fetch_shadow_closed_rows`, `compute_shadow_stats`, `passes_redemption_gate`)를 **Adapter로 재사용** — 신규 SSOT 생성 아님(Rule 6). 단, 태그 네임스페이스는 `LIFECYCLE_OBSERVE_ONLY`로 3-Strike `RE_EVOL_SHADOW`와 분리(사후분석 시 두 강등 사유가 섞이지 않도록).
- Handoff 초안 → 본 파일 하단이 아니라 `CLAUDE_TO_CURSOR.md` §**[CAT-F] F-RETIRE-02** 참조.

### Cursor 선행 확인 요청 (Handoff 착수 전 필수 3건)

1. **capital_mult=0 그룹의 실행계층 동작** — 완전 스킵인지, $0/paper 주문으로 여전히 `forward_trades`에 적재되는지 (스캐너·주문 실행 모듈 실측)
2. **로컬/서버 DB 실측** — `strategy_quality_daily`에서 `state=RETIRED`인 `strategy_id`들의 마지막 row 날짜 vs `last_demoted_at` 대조(실제로 끊기는지 직접 확인)
3. **`RE_EVOL_SHADOW` 태그 청산 row 존재 여부** — 3-Strike 경로가 실전에서 한 번이라도 발동해 섀도우 데이터가 쌓인 적 있는지 depth 확인 (0건이면 Adapter 재사용 시 재검증 필요)

### 출력 요청 형식 (Cursor 회신 시)

- [CAT-F] 결론 3줄
- 위 3건 실측 결과 표
- `CLAUDE_TO_CURSOR.md` §F-RETIRE-02 착수 가능 여부(Go/No-Go) 의견

---

## Cursor Step 0 회신 — F-RETIRE-01 선행확인 3건 (2026-08-09)

### [CAT-F] 결론 3줄

1. **`capital_mult=0`는 실행계층에서 직접 소비되지 않음** — registry write-only. 진입 게이트는 `META_STRATEGY_HEALTH` mult(Treasury) + `RE_EVOL_SHADOW` 전용 $0 페이퍼 경로만 존재. 표준 TTL→COOLED/RETIRED에는 **전용 페이퍼 경로 없음**.
2. **로컬 DB RETIRED 표본 0건** — `strategy_quality_daily`×`last_demoted_at` 교차 불가. VPS SQL 필수.
3. **RE_EVOL_SHADOW 로컬 0행** — 3-Strike Adapter cold-start. F-RETIRE-02는 `forward/shared.py`에 COOLED/RETIRED→`LIFECYCLE_OBSERVE_ONLY` $0 페이퍼 결선이 **필수**(Handoff Cursor 지시 3 해당).

### Step 0 실측 표

| # | 확인 항목 | 결과 | 근거 (파일·함수) |
|---|-----------|------|------------------|
| 1 | `capital_mult=0` 실행계층 | **조건부 — 완전 스킵도 $0 페이퍼도 아님** | `capital_mult`는 전 repo **read 0곳**(진입 경로). `forward/shared.py` `evaluate_meta_group_entry_gate` → health mult≤0 시 block. `RE_EVOL_SHADOW`만 `apply_shadow_entry_zero_notional`→`forward_trades` $0 적재. COOLED/RETIRED `state`는 진입 경로 **미검사** — health 만료 후 default mult=1.0이면 실자본 진입 가능 |
| 2 | RETIRED `quality_daily` 끊김 | **로컬 표본 없음** (구조는 Claude 진단과 일치) | 로컬: `strategy_registry` LIVE/KR 1 · RETIRED 0 · `strategy_quality_daily` 5행 · `forward_trades` 0행. VPS: `state=RETIRED` last `trade_date` vs `last_demoted_at` SQL 필요 |
| 3 | `RE_EVOL_SHADOW` depth | **0건(로컬)** | `forward_trades` WHERE `sig_type LIKE '%RE_EVOL_SHADOW%'` = 0. VPS 재확인 필요 |

### F-RETIRE-02 착수 의견

**조건부 Go** — `strategy_promotion_engine` lifecycle 분기만으로는 데이터가 안 쌓임. Handoff B 이전에 `forward/shared.py`에 `is_lifecycle_observe_only_row` 판별 + `RE_EVOL_SHADOW`와 동일한 $0 페이퍼 결선(`LIFECYCLE_OBSERVE_ONLY` 태그)이 **동일 sub-phase 스코프**에 포함되어야 함. Claude Architect Step 0 결과 재확인 요청.

---

## Claude 확인 — Step 0 회신 반영 + F-RETIRE-02 확정 (2026-08-09)

결론: **Go, forward/shared.py 결선 동일 스코프 포함(Cursor 의견 채택)** · **retention 옵션 B 확정**. Handoff 전문 → `CLAUDE_TO_CURSOR.md` §**[CAT-F] F-RETIRE-02**.

### Go/No-Go — `forward/shared.py` 페이퍼 결선

**Go.** Step 0 #1이 원래 조사보다 더 심각한 사실을 드러냄:

- `capital_mult`는 registry **write-only** — 진입 게이트가 참조하는 건 `META_STRATEGY_HEALTH` mult(Treasury)뿐. `state`(COOLED/RETIRED)는 진입 경로에서 **전혀 체크되지 않음**.
- `RE_EVOL_SHADOW`만 `apply_shadow_entry_zero_notional`로 $0 페이퍼가 실제 적재됨 — 표준 TTL 강등 경로엔 이 결선이 없다는 게 Cursor 확인으로 재확인.
- 따라서 `strategy_promotion_engine`만 고쳐서는 **데이터가 영원히 안 쌓인다** — Cursor 의견 그대로 채택. F-RETIRE-02에 `forward/shared.py` 결선을 **필수 Spec B'**로 병합.
- 단, 공유 실행 파일(`forward/shared.py`)을 건드리는 만큼 위험도를 🟢→🟡로 상향(Handoff §위험도 참조). LIVE 진입 로직 자체는 무변경 — COOLED/RETIRED 전용 신규 분기 추가만.

### Retention — 옵션 B 확정 (RETENTION_MULT=3.0)

디렉터가 제시한 90일을 **임의 폐기하지 않고**, 기존 US half-life(30d, CAT-CONSTANTS)로 나누어 배율을 역산: `90 ÷ 30 = 3.0`. 이 배율을 KR/US 공통 적용(Rule 5 — 신규 상수 창조 아님, 디렉터 지정값에서 도출):

| market | alpha_half_life_days | retention_days (half_life × 3.0) |
|---|---|---|
| KR | 10 | **30** |
| US | 30 | **90** (디렉터 원안 그대로 보존) |
| BG | 21 | 63 (참고만 — 본 설계 범위 아님) |

옵션 A(전 시장 flat 90d)는 KR 기준 half-life의 9배로 과도 — 채택하지 않음. 근거·config 구조는 Handoff §C 참조.

### 🔴 별도 긴급 확인 요청 (F-RETIRE-02와 별개 이슈)

Step 0 #1에서 드러난 "**health 만료 후 default mult=1.0이면 실자본 진입 가능**" 조건절 — 이게 참이면 **현재 운영 중인 RETIRED/COOLED 전략이 재승인 절차 없이 실자본을 재수령할 수 있다는 뜻**. observe_only 기능과 무관하게 그 자체로 🔴. 전문 → `CLAUDE_TO_CURSOR.md` 최상단 별도 항목. **F-RETIRE-02보다 먼저 확인 요망.**

---

## Claude OK — C-FUNNEL-02 구현 검증 (2026-08-09)

결론: **OK** — Handoff 스펙(`CLAUDE_TO_CURSOR.md` §C-FUNNEL-02) 전 항목 일치. 수정 spec 없음.

### 확인 포인트 4개 판정

| # | 항목 | 판정 |
|---|------|------|
| 1 | near-miss cap=50 · `\|cutoff-score\|` 정렬 | OK — Spec E 그대로 |
| 2 | `read_current_regime_key_for_funnel` market당 1회 read | OK — Spec F와 동일 의미 |
| 3 | drop 이벤트 insert 실패 시 snapshot 경로 유지 | OK — 부분 실패 격리 |
| 4 | 판정 로직(cutoff 비교, pass/drop 분기) 미변경 | OK — 계측 3파일만 변경 |

### 파일·테스트 대조

| 항목 | Handoff 지정 | 실제 변경 | 일치 |
|------|-------------|-----------|------|
| SSOT 변경 파일 | `scanner_funnel.py`, `proprietary_friction_store.py` | 동일 + `supernova_hunter.py`(호출부) | ✅ |
| 테스트 케이스 수 | (a)~(e) 5개 요청 | 5 passed | ✅ |
| near-miss cap | 50/슬롯 | 50/슬롯 | ✅ |
| drop() 하위호환 | keyword-only optional | (d) 테스트 확인 | ✅ |

### 경미 — 스펙 외 추가 (차단 아님)

- `insert_regime_friction_event` 회귀도 같이 수정 — 동일 무음 실패 패턴 번들 수정 **인정** (`05` 기록 보강)

### 다음 (디렉터)

1. **서버 배포 승인** — 🟢 Low · Critical 절차 불필요
2. **VPS `scan_funnel_snapshot` depth SQL** — 배포와 무관 병렬 실행
3. 배포일 = **C-FUNNEL-03**(14~30영업일 회고) 카운트다운 시작점

---

## OUTBOX — C-FUNNEL-02 구현 완료 (2026-08-09)

| 항목 | 내용 |
|------|------|
| **sub-phase** | C-FUNNEL-02 |
| **Handoff** | `CLAUDE_TO_CURSOR.md` §C-FUNNEL-02 |
| **테스트** | `pytest tests/test_scan_funnel_drop_event_c_funnel_02.py` — **5 passed** |

### 변경 파일

- `proprietary_friction_store.py` — snapshot insert 회귀 · `scanner`/`drops_json` · `scan_funnel_drop_event`
- `scanner_funnel.py` — `drop()` 확장 · near-miss cap=50 · regime denormalize
- `supernova_hunter.py` — LIQUIDITY/DNA_FAIL 계측 인자
- `tests/test_scan_funnel_drop_event_c_funnel_02.py` (신규)

### Claude 확인 포인트

1. near-miss cap=50 · `|cutoff-score|` 정렬 OK?
2. `read_current_regime_key_for_funnel` — market당 1회 read OK?
3. drop 이벤트 insert 실패 시 snapshot 경로 유지 OK?
4. 판정 로직 미변경 OK?

---

## Claude OK — CAT-C-FUNNEL-01 조사 검증 (2026-08-09)

결론: 조사 **OK** — 수정 spec 없음. 6문항 전부 코드 실측 근거 확인.

| # | 항목 | 판정 |
|---|------|------|
| 1 | 탈락=집계만(Counter), 심볼·컷오프 미기록 | OK |
| 2 | 로컬 0행 · VPS 재측정 필요 | OK — VPS SQL 디렉터 실행 대기 |
| 3 | OHLCV 조인: `market_data_fetcher.fetch_market_data` | OK, 채택 |
| 4 | 국면 조인: 퍼널에 FK 없음, meta asof만 가능 | OK — denormalize로 보완 |
| 5 | 기존 사후분석과 중복 아님(`shadow_performance_tracker`는 다른 경로) | OK |
| 6 | 싼 실험안(per-drop 계측 → 14~30영업일 후 회고) | OK, 세부는 C-FUNNEL-02 Handoff |

### 결정 4가지 (Handoff에 반영 — `CLAUDE_TO_CURSOR.md` §C-FUNNEL-02)

1. **Handoff 범위**: `C-FUNNEL-02` CAT-C sub-phase 공식화. **C-1과 병렬** — 파일 겹침 없음.
2. **로그 스키마**: **near-miss 샘플링, cap=50/슬롯** (슬롯=market×reason×scan_date). Full log 기각.
3. **컷오프 SSOT**: `eff_cos_cutoff`/`eff_ml_cutoff` 스냅샷 **필수**(nullable).
4. **국면**: `regime_key` **denormalize** (finalize 시 market당 1회 read, CAT-G read-only).

### 추가 지시 (Architect)

- `insert_scan_funnel_snapshot` 회귀 — **C-FUNNEL-02와 별개, 즉시 단독 커밋** 가능.
- `drops_json` + `scanner` 컬럼 추가 승인.
- retention — CAT-CONSTANTS 미확인, v1은 pruning 없이 적재만.
- VPS depth SQL — 디렉터 병렬 실행.

---

## OUTBOX — [CAT-C] ScanFunnelTracker 탈락 로그·사후분석 가능성 조사 (2026-08-09)

> **유형**: 디렉터 질문 5건에 대한 **코드·DB 실측 조사** (sub-phase 구현 아님)  
> **상세 SSOT**: `05_진행로그.md` §CAT-C-FUNNEL-01  
> **관련 CAT**: CAT-C (스크리닝) · CAT-B (OHLCV) · CAT-G (국면)

### 디렉터 / Claude 질문 (원문)

1. `scanner_funnel.py` / `ScanFunnelTracker`가 탈락 종목의 심볼·타임스탬프·탈락 사유·컷오프 스코어를 **종목 단위**로 남기는가, 아니면 **집계 카운트**만 남기는가?
2. 위 로그가 실제로 **몇 일치(history depth)** 쌓여 있는가? (표본 수 추정 — KR/US 각각)
3. 탈락 종목의 **사후 가격**을 조인하려면 어떤 OHLCV 테이블/함수로 가능한가? (`data_updater` 계열 재사용 여부)
4. 탈락 시점 **국면 라벨**(BULL/BEAR/SIDEWAYS/HIGH_VOL)을 이 로그와 조인할 **기존 키**가 있는가?
5. 이런 **사후분석 기존 스크립트/노트북**이 이미 존재하는가? (중복 방지)
6. 조사에 필요한 **작업 표면** + **가장 싼 실험** 제안

---

### Cursor 조사 결론 (요약)

| # | 질문 | 결론 |
|---|------|------|
| 1 | 종목 vs 집계 | **탈락 = 집계만** (`Counter`). **합격 생존자만** 런타임 `ScanSurvivor` (심볼·score). 컷오프·탈락 시각·심볼 **미기록** |
| 2 | history depth | 로컬 `market_data.sqlite`: `scan_funnel_snapshot` **0행** (KR/US 0일). `meta_state_log` ~26행·~7일. **VPS 풀 DB 재측정 필요** |
| 3 | OHLCV 조인 | `KR_{code6}` / `US_{ticker}` (`data_updater` 적재). 읽기 SSOT: `market_data_fetcher.fetch_market_data()` 또는 `data_miner._ohlcv_table_name` + SQL. **탈락 심볼 리스트 선행 필요** |
| 4 | 국면 조인 | 퍼널 행에 regime 키 **없음**. `meta_state_log.regime_key` + `updated_at_utc` asof 조인만 가능 (`keep_last=48`) |
| 5 | 기존 분석 | **탈락 false-negative 전용 없음**. 근접: `weekly_proprietary_regime`(집계 pass_rate), `shadow_performance_tracker`(등재 직전 차단 사후 PnL) |
| 6 | 싼 실험 | per-drop 샘플 로그(슬롯당 N건) **forward 적재** → 14~30영업일 후 `fetch_market_data` T+1/T+5 회고. 집계 snapshot insert **복구만으로는 불충분** |

---

### 1) ScanFunnelTracker 데이터 모델 (코드 실측)

**탈락 경로**

- `drop(reason, n=1)` → `_drops: Counter` — **사유별 건수만**
- `supernova_hunter.py` 등에서 `funnel.drop("DNA_FAIL")` 등 호출 — **code 인자 없음**
- `finalize()` → `drop_summary: Tuple[(reason, count), ...]` — 텔레그램·리포트용

**생존 경로**

- `add_final_candidate(code, name, pass_path, final_sig, final_score)` → `_final: Dict[str, ScanSurvivor]`
- Top3·enrolled만 텔레그램 HTML. **DB 미영속**

**영속 스냅샷 (`scan_funnel_snapshot`)**

- 스키마: `ts, market, universe_size, survivors, pass_rate_pct` — **scanner·drops_json·심볼 없음**
- `finalize()`가 `insert_scan_funnel_snapshot(...)` 호출 시도
- **회귀 버그**: 루트 `proprietary_friction_store.py`에 `insert_scan_funnel_snapshot` **함수 없음** (`has_insert=False`) → `try/except: pass`로 **무음 실패** → 로컬 DB 0행
- Bitget: `bitget/infra/proprietary_friction_store_bg.py`에는 insert **존재** (Track B 전용)

**컷오프**

- `DYNAMIC_SUPERNOVA_CUTOFF` / elastic `eff_cos_cutoff` / `eff_ml_cutoff` — `system_config`·`config_snapshots/`에 일별 존재
- **퍼널 로그와 조인 키 없음** (스캔 ts + market 수준 asof만 가능)

`Proprietary_Regime_Audit.md` §1.1과 일치: 퍼널 **런타임 비영속** · 권장 `drops_json` **미구현**.

---

### 2) 로컬 DB 실측 (2026-08-09)

**경로**: `factory_data_dir()` → `~/dante_bots/Dual-Screener-Bot/market_data.sqlite` (~0.24MB)

| 테이블 | KR | US | 비고 |
|--------|----|----|------|
| `scan_funnel_snapshot` | 0행 / 0일 | 0행 / 0일 | 스키마만 |
| `regime_friction_event` | 0 | 0 | |
| `meta_state_log` | GLOBAL 26행 | — | 2026-07-04 ~ 07-11 UTC |
| `KR_*` / `US_*` OHLCV | 테이블 없음 | 테이블 없음 | 로컬 = 빈 카피 가능 |

**설계 retention** (Bitget policy 참고): `SCAN_FUNNEL_KEEP_DAYS=60`, `SCAN_FUNNEL_KEEP_LAST=50_000` — insert 동작 시에만 의미.

**VPS 확인 쿼리 (디렉터)**

```sql
SELECT market, COUNT(*), MIN(ts), MAX(ts),
       COUNT(DISTINCT substr(ts,1,10)) AS days
FROM scan_funnel_snapshot GROUP BY market;
```

---

### 3) OHLCV 사후 가격 조인 경로

| 계층 | SSOT |
|------|------|
| 테이블 명 | `KR_{6자리}` / `US_{티커}` — `data_updater.py` `save_data_safely` |
| 읽기 (권장) | `market_data_fetcher.fetch_market_data(code, market, start, end)` — FDR/YF/SQLite 체인 |
| 직접 SQL | `data_miner._ohlcv_table_name(market, code)` + `Date, O,H,L,C,V` |
| 갱신 | `data_updater.run_daily_db_update` / `run_us_incremental_db_update` |

**전제**: 탈락 `(symbol, ts, reason, score, cutoff)` 행이 **먼저** 있어야 join 가능. 현재 구조로는 **불가**.

---

### 4) 국면 라벨 조인

| 소스 | 키 | 조인 |
|------|-----|------|
| `meta_state_log` | `regime_key`, `updated_at_utc` | 스캔 `as_of_kst` ↔ UTC asof nearest |
| `meta_governor_state.json` / `config_snapshots/` | `META_REGIME_KEY` | 일자 단위 |
| `scan_funnel_snapshot` | — | **regime 컬럼 없음** |

BULL/BEAR/SIDEWAYS/HIGH_VOL — **퍼널과 1:1 FK 없음**. meta asof만.

---

### 5) 기존 사후분석 (중복 여부)

| 모듈 | 범위 | 퍼널 탈락과 관계 |
|------|------|------------------|
| `weekly_proprietary_regime.py` | `pass_rate` 시계열 PRI | 집계만 · 종목 X |
| `shadow_performance_tracker.py` | `blocked_trade_history` 사후 PnL | DOOMSDAY/TOXIC **등재 직전 차단** — DNA_FAIL 유니버스 탈락과 **다름** |
| `Proprietary_Regime_Audit.md` | 맹점 문서화 | drops_json 권장 **미구현** |
| `limit_up_forensics.py` 등 | 별도 forensics | 퍼널 drop 로그 **미사용** |

→ **신규 per-drop 회고 파이프라인 = 중복 아님**. 선행: **계측 Handoff**.

---

### 6) 작업 표면 + 가장 싼 실험

**읽기 표면 (조사 완료)**

- `scanner_funnel.py` — Tracker / finalize / snapshot 호출
- `proprietary_friction_store.py` — insert 회귀
- `supernova_hunter.py` — `funnel.drop` 호출부
- `market_data_fetcher.py`, `data_updater.py`, `data_miner.py` — OHLCV
- `meta_state_market_db.py` — 국면 asof
- `shadow_performance_tracker.py` — 패턴 참고
- `Proprietary_Regime_Audit.md`, `docs/claude_project/CAT-C_스크리닝.md`

**가장 싼 실험 (Claude Handoff 요청)**

1. **C-FUNNEL-02 (계측)**: `drop()` 확장 또는 parallel `scan_funnel_drop_event` 테이블 — `(ts, market, scanner, code, reason, final_score?, cos_cutoff?, ml_cutoff?)` · 슬롯당 cap 50 (DNA_FAIL·LIQUIDITY 우선)
2. **insert 회귀 수정**: `proprietary_friction_store.insert_scan_funnel_snapshot` 복구 + `scanner`·`drops_json` 컬럼 (Audit 권장안)
3. **14~30영업일 적재 후**: `fetch_market_data` T+1/T+5 + `meta_state_log` asof — **회고 스크립트 1개** (`scripts/funnel_dropout_postmortem.py` 등)
4. VPS에서 §2 SQL로 **실제 depth** 먼저 확인 — 0이면 과거 30일 회고 **불가**, forward만 가능

---

### Claude에게 요청 (결정 4가지)

1. **Handoff 범위**: C-FUNNEL-02를 CAT-C sub-phase로 공식화할지, Alpha Proof(C-1) 전 **인프라**로 둘지?
2. **로그 스키마**: 전량 per-drop vs **near-miss 샘플링** (score 상위 탈락 N건/슬롯)?
3. **컷오프 SSOT**: drop 이벤트에 `eff_cos_cutoff`/`eff_ml_cutoff` 스냅샷 **필수** 여부?
4. **국면**: drop row에 `regime_key` **denormalize** vs meta asof join만?

### 출력 요청 형식

- [CAT-C] 결론 3줄
- `CLAUDE_TO_CURSOR.md` Handoff 초안 (C-FUNNEL-02, 있다면)
- `ARCHITECT_MIRROR.md` 상단 블록 (날짜 2026-08-09)
- 디렉터 Yes/No: near-miss 샘플링 vs full log · VPS DB depth 확인 여부

---

## OUTBOX — RP-1 live run RCA (서버 `no_templates`)

### 증상 (디렉터 서버 실측)

| 시도 | 결과 |
|------|------|
| 단일 종목 `_backtest_one_ticker` | ML=12, trades 264/271 ✅ |
| 3종목 `default_run_backtest_for_period` | trades=817 ✅ |
| 400종목 RP-1 live | sum=0, SKIP 15/15, `gate_summary: no_templates` ❌ |

### 근본 원인 (Cursor RCA)

`time_machine_backtester.load_factory_brain_readonly()` 가 **JSON-only** (`system_config.json`) 로만 읽음.  
운영 서버 SSOT는 **`config_manager.load_system_config()` → SQLite `config_kv`** (A-5b 배포 후 템플릿이 KV에만 있을 수 있음).

### 패치 (`5e027e6` 이후 추가 커밋 예정)

1. `load_factory_brain_readonly()` → `load_system_config()` 우선, JSON fallback  
2. `load_rp1_brain_cached()` — 15구간 동안 1회 로드  
3. 템플릿 0이면 live run **즉시 RuntimeError** (조용한 INCONCLUSIVE 방지)

### 패치 v2 (`df0a267` 이후) — KV·JSON 분리

서버: SQLite `config_kv` 비어있지 않지만 **`LIVE_CLUSTER_TEMPLATES`는 JSON/`config_ml.json` 샤드에만 존재** → `load_system_config()` 단독 시 템플릿 0 → RuntimeError.

**수정**: `_backfill_brain_keys_from_legacy()` — KV 로드 후 템플릿·EVOLVED 키만 legacy 병합 뷰에서 read-only backfill.

### Claude 검증 요청

- live run 재실행 후 `sum trades > 0` 확인 시 **결과 재검증** (Pass/Fail/Near-miss)  
- `INCONCLUSIVE` 판정 로직 스펙 일치 여부  
- config 읽기 경로가 CAT-K SSOT와 일치하는지

### 서버 재실행 (디렉터)

```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
python3 -c "from config_manager import load_system_config as l; c=l(); print('ML', len(c.get('LIVE_CLUSTER_TEMPLATES') or {}))"
nohup python3 run_rp1_live.py > rp1_run.log 2>&1 &
```

---

## OUTBOX — [MASTER] RP-1 · 15구간 목표 달성 검증 (디렉터 요청)

> **상세 SSOT**: `14_레짐패널_15구간_목표검증.md`

### 디렉터 요청 (요약)

현재 퀀트 구조로 **상승 5 · 횡보 5 · 하락 5** 역사 구간을 돌려:

1. **40~70% CAGR · MDD 10%** 달성 가능한지  
2. **안 되면** 원인 (신호/방어/과적합/게이트)  
3. **근처면** 무엇이 막았는지 (Near-miss)  
4. **가려면** 수정·보완·추가 항목  
5. **과거 백테에 머무르지 않고** 결과 힌트로 **앞 구조 방향**

### 로컬 실측 (Cursor)

| 항목 | 현황 |
|------|------|
| `time_machine_backtester.REGIME_PERIODS` | **6구간** (붕괴4·상승1·횡보1) — **15 미달** |
| `run_time_machine_regime_matrix()` | 레짐별 PF·승률·n 집계 **이미 존재** |
| Lookahead 경고 | `LOOKAHEAD_BIAS_WARNING` — v1은 **하한 추정**만 |
| Phase A 시뮬 | RP-1 v1에 거버너 포함 여부 **미정** |

### Alpha Proof와의 관계

```
Week 1–2: RP-1 (15구간) + C-1 backtest — 동일 2주 timebox
         무결론 = No-Go
Week 4:   ASG (정성, n<30)
Week 8+:  G2 (상품화, 변경 없음)
```

### Claude에게 결정 요청 (6항)

1. 15구간 캘린더 — KR / US / dual 패널?  
2. bucket Pass 규칙 (상승 ≥3/5 등)  
3. Phase A 거버너 RP-1 v1 포함 여부  
4. Lookahead v1 경고만 vs v2 point-in-time 일정  
5. Near-miss 밴드 정의  
6. RP-1 Fail 시 C-1 중단 vs 축소?

### Handoff 형태 제안

- **안 A**: `RP-1` 단독 Handoff → Cursor가 15구간+리포트 구현  
- **안 B**: `C-1` Handoff에 RP-1 baseline 절차 **병합** (2주 공유)

### 출력 요청 형식

- [MASTER] 결론 3줄  
- 15구간 표 (날짜·시장·bucket) 확정안  
- Pass/Fail/Near-miss 판정 규칙  
- `CLAUDE_TO_CURSOR.md` Handoff 초안 (RP-1 또는 C-1+RP-1)  
- ARCHITECT_MIRROR 블록

---

## Claude OK — RP-1+C-1 harness (2026-08-07)

```
Harness-level OK. 결과 판정은 live run JSON 후 재검증.
```

### Cursor 회신 — 잔여 2건

| # | 요청 | 회신 |
|---|------|------|
| 1 | 15구간 날짜 원문 | `14_레짐패널` §SSOT 표 + `test_regime_periods_dates_ssot_snapshot` |
| 2 | no-write 테스트명 | **`test_rp1_no_config_kv_write`** (`TestRp1NoConfigKvWrite`) |
| 3 | Stage2 5분기 mock | `test_stage2_branch_fail_cause_a_skip` · `_c_skip` · `_b_reduced_ab` · `_near_miss_full_ab` · `_pass_optional_skip` |

**테스트**: 15 passed

### 병렬 진행 (Claude 승인)

| 스트림 | 쓰기 | 충돌 |
|--------|------|------|
| live run | `reports/regime_panel/rp1_*.json` | 없음 |
| A-5b 배포 | config_kv S5 | 없음 |
| north star cron | `dual_north_star_ledger.json` | 없음 |

### live run 주의 (디렉터)

- **1차**: KOSPI 스모크 (KR-only) — 파이프라인 확인용
- **최종본**: **KR+US 합산 유니버스** — North Star RP-1 판정 SSOT. KR 단독으로 Pass/Fail 확정 금지

---

## Claude OK — RP-1+C-1 구현 완료 (2026-08-07) — 검증 요청

```
Handoff 구현: REGIME_PERIODS 15 · tier replay · Stage1→Stage2 분기 · JSON 리포트.
테스트 13 passed. 서버 live fdr run 미실행(로컬).
```

### 확인 포인트

1. 15구간 캘린더·중복 제거 (5 bear 인과 분리) OK?
2. Phase A tier replay only (no config write) OK?
3. Stage2 C-1 분기 규칙 OK?
4. MDD crosscheck 배지 최상단 OK?
5. n<20 SKIP_LOW_N OK?

---

## Claude OK — A-5b + [MASTER] 전략 재편 (2026-08-07)

```
A-5b OK (MASTER). Phase A freeze.
Alpha Proof 압축: 2주 backtest Go/No-Go → 4주 ASG(조기경보) → G2 유지.
다음: C-1 Handoff (backtest timebox 본문 포함).
```

---

## OUTBOX — A-5b (CAT-G) — **처리 완료** ✅

| 항목 | 내용 |
|------|------|
| **sub-phase** | A-5b — BEAR/HIGH_VOL S5 국면 게이트 |
| **Option** | **A (OR)** — `s5_active = regime_allows_s5 OR budget_active` |
| **HIGH_VOL** | `s5_arm_active=True` (crisis_synced KR 조기경보) |
| **킬스위치** | `ENABLE_S5_REGIME_GATE` (default True) — False → A-5a budget-only |
| **독립성** | `ENABLE_WEIGHT_S5_MERGE` (A-5a) 와 **교차 조건 없음** |

### 변경 파일

- `meta_governor.py` — `ACTION_BY_REGIME[*].s5_arm_active` · HIGH_VOL `weight_s5_bounds` `[0.9, 1.55]`
- `meta_governor_consumer.py` — `resolve_defense_arm_weight()` regime OR budget
- `tests/test_s5_regime_gate_a5b.py` (신규)
- `tests/test_kelly_chain_s5_gate.py` — BULL+budget off 회귀 수정

### 테스트

```
test_s5_regime_gate_a5b.py   8 passed
test_kelly_chain_s5_gate.py 10 passed
```

### Claude 확인 포인트

1. Option A (OR) — BEAR 초입 budget 미달 시에도 S5 개방 의도 일치?
2. HIGH_VOL 포함 — crisis_synced KR 인버스 공백 메움 동의?
3. `ENABLE_S5_REGIME_GATE=False` → A-5a budget-only 즉시 복귀 확인?
4. Kelly Step1 수식·순서 무변경 — 게이트 조건만 확장?

---

## Claude OK (A-5a rev.2 · 2026-08-06) — 배포 완료

```
A-5a OK rev.2. S5 sig = INVERSE_ETF + BLACKHOLE only. TOXIC_FADE 단독 제외.
```

서버 `dante-factory.service` **active** · git `aaad40c`

---

## 킬스위치 독립성 (누적)

| sub-phase | 롤백 |
|-----------|------|
| A-5b | `ENABLE_S5_REGIME_GATE=False` |
| A-5a | `ENABLE_WEIGHT_S5_MERGE=False` |
| A-4 | `ENABLE_ASYMMETRIC_HYSTERESIS=False` |

**교차 조건 없음** 확인 유지.

---

## STRATEGIC REVIEW — 디렉터 요청 (2026-08-07)

> **배경**: 디렉터 — "70~80% 구조는 있는데 대중적이고 목표(40~70% CAGR / MDD 10%)에 못 미칠 것 같다. 1년 내 완성 목표. KR/US 총괄 Claude Pro와 재설계 논의 필요."
> **Cursor 역할**: 로컬 코드베이스 실측 기반 솔직한 진단 + Claude Pro 논의 안건 제출 (구현 아님).

### 1. 솔직한 진단 — "70~80%"의 정체

| 층 | 추정 비중 | 상태 | 목표(40~70%) 기여 |
|----|----------|------|-------------------|
| **인프라·오케스트레이션** | ~35% | ✅ 동작 (factory, cron, telegram, DB, config) | 간접 |
| **리스크·자본 OS (Phase A)** | ~25% | ✅ A-1~A-5b 구현·일부 배포 | **MDD 방어** — 상방 제한도 큼 |
| **알파·스캐너 (S1/S4/S5)** | ~25% | ⚠️ 코드 있으나 **효과 미검증** | **핵심 미확인** |
| **진화·선취매·AI CIO (B/C/D)** | ~15% | ❌ 로드맵상 **미착수** | 상방 엔진 — **아직 없음** |

**결론**: "70~80% 완성"은 **플랫폼·방어층** 기준이 맞고, **수익 목표를 증명하는 알파층**은 아직 20~30% 수준이거나 **검증 데이터 0**에 가깝다 (`06_검증체크리스트` 3단계 전부 미완료, `00` 실적 스냅샷 미기록).

### 2. "대중적"인가? — 부분적으로 **맞다**

**누구나 만들 수 있는 패턴 (차별화 약함)**:
- Kelly + regime ACTION_BY_REGIME + MDD tier throttle
- MAB capital allocator, deathmatch lookback kill
- GP 유전자 진화, LinUCB bandit
- VIX/ensemble 국면 판별

**이 레포만의 요소 (차별화 후보)**:
- `supernova_hunter` time-machine DNA + KR/US 듀얼 파이프라인
- KR/US **비대칭표** + `crisis_synced` HIGH_VOL 강제
- `inverse_etf_sniper` + toxic fade → 인버스 브릿지
- CAT-P mega trend kill chain + re-evolution
- `dual_north_star_ledger` G0~G4 상품화 게이트

**문제**: 차별화 후보 모듈은 **많지만**, B/C/D 로드맵이 연결·검증되지 않았고, 최근 2개월 작업은 **전부 CAT-F/G 방어**에 집중. 즉 **"나만의 퀀트"가 아니라 "잘 짜인 리스크 OS 위에 기존 스캐너"** 상태.

### 3. 목표 수치 현실성 (수학)

- **40~70% 연복리 + MDD 10% 하드캡** = 업계 상위 1%급 지속 성과. 방어 중심 Kelly cap(0.6%~2.8%)만으로는 **구조적으로 상방이 막힘**.
- Phase A는 **"안 죽기"**에 최적화 — 목표 달성의 **필수조건**이지 **충분조건 아님**.
- `12_듀얼북극성` G2 조건: `forward_trades>30` + 56일 — **아직 G1도 미판정**. 수익 경로 자체가 측정되지 않음.

### 4. 왜 100년처럼 느껴지는가 (프로세스 병목)

| 병목 | 영향 |
|------|------|
| 한 세션 = sub-phase 하나 + Claude OK 필수 | A만 15+ 세션, B/C/D 미시작 |
| 3단계 완료(2~4주 관측) 미착수 | "구현=완료" 착각, 알파 학습 루프 없음 |
| Phase A→B→C→D **순차 가정** | 상방 엔진(C)이 방어(A) 끝날 때까지 대기 |
| CAT 문서·Mirror·05/00 오버헤드 | 엔지니어링 품질 ↑, 속도 ↓ |
| legacy_archive 스캐너 혼재 | 실전 파이프라인 = supernova + legacy breakout |

### 5. 1년 완성을 위한 **구조 재편** 제안 (Cursor → Claude Pro 논의안)

**원칙**: "플랫폼 완성"과 "엣지 증명"을 **분리·병렬**.

#### Track 1 — Alpha Proof (최우선, 8~12주)
1. **단일 북마크 메트릭**: KR+US 합산 forward NAV, MDD, 월별 페이스 — `dual_north_star_ledger` **매일 채우기** (이미 모듈 있음).
2. **알파 3축만 고정** (나머지 freeze):
   - S1: supernova (공격)
   - S4: pullback/reverse breakout (눌림)
   - S5: inverse_etf + blackhole (방어)
3. **B/C/D 중 1개만** 우선: **C-1 섹터 선취매** 또는 **B-3 데스매치 조기킬** — 둘 다 안 하면 진화는 장식.
4. G2 도달 여부를 **12주 안에** 판정 — 안 되면 전략 가설 폐기·교체 (코드 더 짓지 말 것).

#### Track 2 — Risk OS (현행 유지, 확장 금지)
- A-5b 배포 후 **Phase A freeze** — A-6 이상 신규 방어 레이어 금지.
- 효과 검증만: MDD 소진 tier가 실제로 선제 조임하는지 `06` 표 기록.

#### Track 3 — 로드맵 개정 (Claude Pro 결정 필요)
| 질문 | 옵션 |
|------|------|
| CAGR 목표 | (a) 40~70% 유지 + 소수 고레버 전략 (b) 1년 내 20~30% 현실 목표 + 2년차 확장 |
| MDD | (a) 10% 유지 (b) 개발기 15% 허용 후 실전 10% |
| Phase 순서 | (a) A완→B→C (현행) (b) **C-1∥A**, B는 shadow만 |
| 차별화 베팅 | supernova DNA vs sector spillover vs mega_trend kill — **1개만** |

### 6. Claude Pro KR/US 총괄에 요청할 결정 5가지

1. **Phase A를 지금 freeze해도 되는가?** (A-5c는 필수인가, defer 가능한가)
2. **40~70%를 1년 내 "증명" vs "구조 완성"** 중 어느 쪽이 1차 목표인가?
3. **차별화 단일 베팅** — supernova / sector Markov / mega_trend 중 하나 지정
4. **병렬 트랙 승인** — Alpha Proof Track과 Risk OS Track 분리
5. **실패 기준** — 12주 후 G2 미달 시 롤백 범위 (전략만? 모듈 전체?)

### 7. Cursor 엔지니어 의견 (1~2줄)

Gemini/Claude가 설계한 **방어 헌법(Phase A)은 업계 표준이지만 필수**이고, 디렉터 우려대로 **상방 40~70%를 만드는 고유 알파 루프는 아직 코드보다 문서에 더 많다**. 1년 안에 가려면 **새 sub-phase 추가보다 기존 supernova+forward_trades로 G2 판정부터** 해야 한다.
