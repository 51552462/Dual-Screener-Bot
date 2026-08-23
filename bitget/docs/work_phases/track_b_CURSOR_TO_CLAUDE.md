# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-23  
> **유형**: LS-GOAL-UX-01 · **Claude OK 수신 · DONE**

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
