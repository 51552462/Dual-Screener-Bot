# 전체 현황판 (Bitget · Claude/Cursor 공용 대시보드)

> Phase 완료 또는 중요 변화 있을 때마다 갱신.  
> `05_진행로그.md`의 **요약본 + SSOT 용어집** 역할.  
> **헌법**: `00_마스터_로드맵.md` (주식 MDD 10%와 **별개** · Ubuntu 4GB 격리 운영)  
> **Cursor**: sub-phase 구현 세션 **종료 전** 본 파일 Phase 표·SSOT 용어집 **필수 갱신**.

---

## 목표 수치 (듀얼 북극성 · 2026-08-03)

| 트랙 | MDD 하드캡 | 연복리 북극성 | SSOT |
|------|-----------|--------------|------|
| **A · 주식 KR/US** | **10%** | **40~70%** | 루트 `docs/work_phases/` |
| **B · Bitget** | **5%** | **12~25%** (B1~B2) · B0=측정만 | `00_마스터_로드맵.md` §0.4 |

### Bitget 운영 (Track B · 지금)

| 항목 | 값 | 비고 |
|------|-----|------|
| 운영 모드 | **가상매매 (paper)** | `ENABLE_REAL_EXECUTION=false` |
| Portfolio MDD (코드 1차) | −15/−20/−30% tier | **5% 프로필 튜닝은 `06` 후** |
| Group MDD (legacy) | −30% per group | 5% 달성 시 함께 조임 예정 |
| 실전 전환 | **금지** (P2-5 전) | |
| **B0 단계** | **4-track 관측** · 수익 % 목표 없음 | `06` 2~4주 |
| **다음 Handoff** | **WAIT_CLAUDE_OK** ×2: `R1a-FASTCHECK` · `FULL-BT-HIST-3-FIX` · **혼합 금지** | C-2/MDD5%/live 🔴 defer |
| 마지막 갱신 | 2026-08-28 | FASTCHECK 구현(7 passed) · HIST-3-FIX 병행 |

---

## 목표 대비 잔여 갭 (Track B · 2026-08-04)

> 검증(A `06`·4-track) **유지** · 검증만 기다리지 말고 Layer 1·2를 병렬 진행.  
> SSOT 상세: `05_진행로그.md` · `CURSOR_TO_CLAUDE.md`

| 목표 레버 | 지금 | 갭 | Layer |
|-----------|------|-----|-------|
| MDD **5%** 하드캡 | A-1 tier **15/20/30%** | 5% 프로필 튜닝 미착수 (`06` 후 A-6) | 🔴 3 |
| 연 **12~25%** (B1+) | B0 측정만 | 관측 + C-2 + B-2 live 필요 | 🔴 3 (C-2/alloc) |
| 상품화 G3 | G0 | `06` + C-2 + MDD 4주 (NS-1 R4) | 🔴 3 |
| paper 신호 신뢰 | C-1 **Claude OK** · C-1b 집계 구현 | `06` skip률 관측 (`skip_rate_pct` v1 null) | 🟡 2 |
| paper PnL 현실 | funding 미차감 | **C-2 defer** (close PnL 오염) | 🔴 3 |
| 서버 디스크/백업 | L-1/L-2 코드 OK | **서버 설치 미확인** (I-GMM 배포 ≠ L 설치) | ① 디렉터 |
| 4GB RAM | MemoryMax 가이드만 | drop-in **미설치 가능** | ① 디렉터 |
| cutover | `pipeline_ssot_env=0` FAIL | env 점검 | ① 디렉터 |
| 실전 | OFF | exit·parity·P2-5 | 🔴 금지 |

**병렬 Attribution (확정 운영 규칙)**

| Layer | 내용 | paper 중 |
|-------|------|----------|
| **1** | L-1/L-2 설치 · MemoryMax · paper 배포 확인 · cutover env | I-GMM 배포✅ · L 설치❓ |
| **2** | **C-1** → **D-1~D-3** · P0-6 / P1-7 설계 | D 트랙 ✅ · **POST_DEPLOY_OBS** · overseer❓ |
| **3** | MDD 5% · B-2 live · B-3 block · B-4b · **C-2/C-3** · 실전 | `06` / Go-No-Go **후** |

---

## 디렉터 18카테고리 ↔ CAT-MAP (2026-08-04)

> **원본**: 디렉터 `퀀트 카테고리.txt` (18개 팩토리 계층) · **대조**: `CAT-MAP_의존성경계.md` · **갱신**: Cursor 검토 후 본 표 SSOT  
> **한 줄**: 1~10 = 지금 걷는 길(B0·Layer 1~2) · 11~14 = Layer 3·P2-5 이후 · 15~18 = 스트레치 비전(로드맵 미착수)

### 매핑 표

| # | 디렉터 카테고리 | CAT | Bitget 지금 | Layer |
|---|----------------|-----|-------------|-------|
| 1 | 데이터 파이프라인·전처리 | **B** + **A** | ✅ OHLCV·WS·DB·파이프라인 | 1~2 |
| 2 | 알파·시그널 엔진 | **C** | ✅ 스캐너 · C-1 bad tick | 2 |
| 3 | 리스크·자금 할당 | **F** + **묶음A** | ✅ MDD·tail·lev·gross·config (5% 튜닝=`06` 후) | 2~3 |
| 4 | 매매 실행·청산 | **D** + **E** | ✅ paper 원장·청산 · 실주문 OFF | 2 |
| 5 | 메타·자가 진화 | **H** + **묶음B** | ✅ deathmatch/WF **shadow만** | 2~3 |
| 6 | 인프라·관제·알림 | **L** + **A** + **M** | 🟡 코드 OK · **overseer 기동 미확인** | 1 |
| 7 | 검증·섀도우 | Phase 7 + **B-3** | ✅ parity 도구 · WF shadow · D-3b scaffold | 2 |
| 8 | 매크로·섹터 로테이션 | **G** | ✅ 국면·Meta (주식 비중 큼) | 2 |
| 9 | 리포팅·성과 시각화 | **J** + **D-3a** | ✅ 주간 리포트 · cost 관측 | 2 |
| 10 | 서버·DevOps | **L** | 🟡 systemd·cron SSOT · **L 설치 미확인** · I-GMM 배포✅ | 1 |
| 11 | OMS·슬리피지 | **N** | 🟡 코드 있음 · **실전 꺼짐** (P2-5) | 3 |
| 12 | 포트폴리오·상관·다각화 | **F** 일부 | 🟡 Kelly·gross만 · 상관·캐시드래그 전담 없음 | 3 |
| 13 | 대체데이터·NLP | **M** + sentiment | 🟡 조각 · 독립 “대체데이터 공장” 아님 | 3 |
| 14 | 오프라인 R&D 샌드박스 | validation·backtester | 🟡 WF shadow · **라이브 분리 연구실 약함** | 3 |
| 15 | 꼬리·블랙스완 헤지 | **A-2** tail | 🟡 일상 tail·CB · VIX/옵션/인버스 자동화 **없음** | 스트레치 |
| 16 | 복리·레버 동역학 | **F** + ledger | 🟡 Kelly·피라미딩 조각 · 전담 두뇌 **없음** | 스트레치 |
| 17 | 마켓뉴트럴·롱숏 페어 | **C** (롱/숏 분리) | ❌ stat arb·베타0 **미착수** | 스트레치 |
| 18 | 마찰·세금·수수료 | **D-3a** + **C-2** | 🟡 D-3a 관측만(fee null) · funding **defer** | 3 |

**상태 범례**: ✅ 구현·검증 완료(또는 shadow) · 🟡 부분·defer · ❌ 미착수

### 지금 위치 (한 장)

```
[1~10] 기반·신호·리스크·paper·진화shadow·ops·검증·리포트·DevOps  ← B0 + 묶음A/B/C/D (코드 Done)
[11~14] OMS실전·상관·NLP공장·R&D샌드박스                        ← Layer 3 · 06 / P2-5 후
[15~18] 꼬리헤지·복리가속·페어·세금최적                          ← 스트레치 · 로드맵 미명시
```

### work_phases 묶음과의 관계

| 묶음 | 커버하는 디렉터 # | 비고 |
|------|------------------|------|
| **A** (포트폴리오 리스크) | 3, 15(일부) | 5% 프로필은 `06` 후 |
| **B** (진화) | 5, 7(일부) | live alloc off |
| **C** (데이터·실행품질) | 1, 2, 18(일부) | C-2 funding defer |
| **D** (AI·거버넌스) | 6, 9, 18(관측) | **Done** · overseer 기동 미확인 |
| **L** (인프라) | 6, 10 | 서버 설치 **미확인** |

---

## Phase 진행 현황

| Phase | 묶음 | 상태 | 핵심 산출물 | 로컬 구현 | 가상매매 효과 검증 |
|-------|------|------|-------------|-----------|-------------------|
| 0–8 | 구현 Phase | **완료** | pipelines·OMS·validation | Phase 0–8 docs | architecture PASS |
| 9 | A: 포트폴리오 리스크 | **진행중** | MDD·tail·lev·gross·config | A Claude OK ✅ · Critical **2026-08-02** | paper 배포·`06` 대기 |
| 9′ | **A paper + L/B 병렬** | **진행중** | L-1·L-2 Claude OK · B-1~B-4 OK | L-1/L-2 **서버 설치 미확인** · **I-GMM 배포✅** · **4-track 관측** | A `06` + OPEN/Cos 관측 |
| 10 | B: 진화/deathmatch | **1단계 완료** | B-1~B-4 impl+OK ✅ · prod off | B-1~B-4 Claude OK ✅ | alloc·WF block off · **4-track 관측** · MAB 소비처 없음 |
| 11 | C: 데이터/실행품질 | **C-1b ✅ · I-GMM ✅ · 01b OK** | bad tick · DNA sync · 01b 관측 | C-1 ✅ · C-1b ✅ · I-GMM-01 ✅ · **01b Claude OK** · C-2 defer | paper `06` + 01b 1~2주 |
| 12 | D: AI/거버넌스 | **D-3 전체 ✅** | JSON·gate·poll·cost·parity scaffold | D-1~D-3 Claude OK ✅ · **D-3b 실배선=P2-5 후** | 서버 운영 확인=디렉터 |

**상세 스펙**: `01_묶음A` ~ `04_묶음D` · **기록**: `05_진행로그.md` · **검증**: `06_검증체크리스트_및_실패기록.md`

---

## Phase 간 충돌/의존성 체크리스트

- [ ] A-1 NAV MDD tier가 N execution_safety gate #4와 **동일 threshold** 인가
- [x] A-2 tail fund debit이 A-1 `evaluate_portfolio_mdd_tier` NAV/dd_pct를 **동일 SSOT로 재사용**하는가 (분열 여부 추적 — tail 쪽 완료)
- [x] A-4 gross notional이 A-1 `evaluate_portfolio_mdd_gate` NAV/**nav_current**를 **동일 SSOT로 재사용**하는가 (gross gate 7 — A-4 완료)
- [x] A-5 config write reject가 A-3 `MAX_LEVERAGE=5` 운영 clamp와 **독립**인가 (bound [1,10] ≠ ops cap 5)
- [x] B-2 deathmatch alloc shadow 시 F Kelly chain **충돌 없음** (shadow on/off `sim_kelly_invest` 동일 · prod alloc off)
- [x] B-3 walk-forward shadow 시 registry/config/INCUBATOR **불변** (weekly batch only · factory scan 미통합)
- [x] B-4 lifecycle MAB budget **소비처 없음** — `MAB_EXPLORE_BUDGET_CURRENT` config log only · Kelly/shadow 불변
- [ ] C-2 funding PnL이 D ledger close formula와 **일치**
- [x] C-1 bad tick filter가 E exit trigger **오탐** 없음 (구현 ✅ · `06` 효과 대기)
- [x] D-2 human gate가 K config write path **앞단**에 있는가 (구현 ✅ · Critical 승인 ✅ · poll 배선 ✅ · **서버 기동=디렉터**)
- [x] D-3a cost report가 weekly_evolution에서 **read-only** ops만 쓰는가 (구현 ✅ · basis null · Claude OK 2026-08-04)
- [x] D-3b parity monitor가 파이프라인/cron에 **미배선**인가 (`PARITY_MONITOR_ENABLED` default false · scaffold only)
- [x] L-1 log rotation (P0-1) — deploy SSOT · A~D와 **독립** (`install_bitget_logrotate.sh`)
- [x] L-2 integrity backup (P0-5) — `BITGET_DB_STORAGE_PATH` only · restore drill SSOT (`install_bitget_backup.sh`)
- [x] **NS-1** dual north star ledger — **읽기전용** · config_kv/governor/execution_safety/Kelly **미접촉** (2026-08-03 Claude 조건부 OK)

---

## 최근 실적 스냅샷 (가상매매)

| 항목 | 값 |
|------|-----|
| forward_trades (dev DB) | 0건 (2026-06-14 snapshot) |
| architecture_checks | PASS |
| cutover readiness | FAIL (pipeline_ssot_env=0) |
| 특이사항 | work_phases 협업 구조 **2026-08-01 신설** |

---

## SSOT 용어집 (Bitget 공통)

> 새 모듈/파라미터/상태값 이름 확정 시 **즉시 추가**.

| 이름 | 뜻 | 만든 Phase | 파일 |
|------|-----|-----------|------|
| `14_UNIVERSE-BT_구조생존검증.md` | L0 구조생존 R&D SSOT · U0~U3 · Track B 병렬 독립 | UNIVERSE-BT-U0 | `docs/work_phases/` |
| `UNIVERSE-BT-U0` | 스냅샷·지표5·L0 Kill 문서화 (코드 비접촉) | UNIVERSE-BT-U0 | work_phases |
| `UNIVERSE-BT-U1` | read-only 리플레이 하니스 · C3(지표4 보류) | UNIVERSE-BT-U1 | `analysis/universe_bt/` |
| `UNIVERSE-BT-U2` | 샤드·배치·체크포인트 오케스트레이터 | UNIVERSE-BT-U2 | `analysis/universe_bt/u2.py` |
| `UNIVERSE-BT-U3` | L0 정량 리포트(지표4 제외·CAT-J 비편입) · **1H 폴백 포함 최종 OK** (`live-20260823T121158Z`) | UNIVERSE-BT-U3 | `analysis/universe_bt/u3_report.py` |
| `15_FULL-BT_전체이식가상매매.md` | L1 전체이식 가상매매 SSOT · U-track·Track B 병렬 독립 | FULL-BT-0 | work_phases |
| `FULL-BT-0` | 이식 범위·스키마·Kill·paper·로드맵 문서화 (코드 비접촉) | FULL-BT-0 | work_phases |
| `bitget_full_bt.sqlite` | FULL-BT 격리 DB (paper/config_kv 비접촉) | FULL-BT-0 | FULL-BT-1 paths |
| `FULL-BT-3` / `report.py` | §2 L1 리포트 · CAT-J 비편입 · banner+정량표 · **트랙 골격 Done** | FULL-BT-3 | `bitget/full_bt/report.py` |
| `FULL-BT-HIST-1` | 실제 OHLCV 바 워크 · candle entry/exit · CAT-C/D/E 원본 호출 | FULL-BT-HIST-1 | `bitget/full_bt/harness.py` |
| `FULL-BT-HIST-2` / `full_bt_diag` | engine_hit·gate_reject 진단 계측(하니스 Adapter) · 결과 스키마 비접촉 | FULL-BT-HIST-2 | Claude OK · 엔진 미히트 재판정 |
| `FULL-BT-HIST-3` | engine_call/outcome/tf_coverage · full_bt_diag `tf` 확장 | FULL-BT-HIST-3 | WAIT_CLAUDE_OK · 3회 진단 |
| `13_B1_신뢰사다리.md` | B1 성공계약·렁 R0~R6·Kill·승인문구 SSOT | B1-LADDER-R0 | `docs/work_phases/` |
| `B1-LADDER-R0` | 신뢰사다리 문서화 sub-phase (코드 비접촉) | B1-LADDER-R0 | work_phases |
| `ENABLE_REAL_EXECUTION` | 실주문 마스터 스위치 (default false) | N | config_kv |
| `REAL_EXECUTION_DRY_RUN` | true면 API no-op | N | config_kv |
| `MAX_LEVERAGE` | 선물 레버리지 상한 (default **5**) | A-3 | config_kv, `execution_safety` |
| `resolve_max_leverage` | FUT clamp SSOT — min(requested, MAX_LEVERAGE) | A-3 | `execution_safety.py` |
| `MAX_GROSS_NOTIONAL_PCT` | gross/NAV cap % (default 80) | A-4 | config_kv |
| `GROSS_NOTIONAL_CAP_ENABLED` | gross gate kill-switch (default true) | A-4 | config_kv |
| `evaluate_gross_notional_gate_values` | gate 7 pure eval — gross + nav_current | A-4 | `execution_safety.py` |
| `CONFIG_WRITE_REJECT_BOUNDS` | A-5 write reject table (Kelly, leverage) | A-5 | `config_bounds.py` |
| `CONFIG_WRITE_VALIDATION_ENABLED` | A-5 kill-switch (default true) | A-5 | config_kv / env |
| `TREASURY_SPOT_USDT` | 현물 treasury | D,F | config_kv |
| `TREASURY_FUTURES_USDT` | 선물 treasury | D,F | config_kv |
| `DYNAMIC_KELLY_RISK` | base Kelly risk % | G→F | config_kv |
| `CURRENT_REGIME_KEY` | active regime | G | config_kv |
| `apply_deathmatch_allocation` | deathmatch 자본배분 (현행 **False**) | B | config / pipeline |
| `bitget_forward_trades` | paper book table | D | SQLite |
| `PORTFOLIO_MDD_BREAKER_ENABLED` | portfolio MDD 킬스위치 (default true) | A-1 | config_kv |
| `PORTFOLIO_MDD_REDUCE_PCT` | REDUCE tier (ratio, default 0.15) | A-1 | config_kv |
| `PORTFOLIO_MDD_BLOCK_PCT` | BLOCK tier (ratio, default 0.20) | A-1 | config_kv |
| `PORTFOLIO_MDD_HALT_PCT` | HALT tier (ratio, default 0.30) | A-1 | config_kv |
| `PORTFOLIO_MDD_REDUCE_SIZE_MULT` | REDUCE Kelly mult (default 0.5) | A-1 | config_kv |
| `PORTFOLIO_NAV_PEAK` | treasury NAV monotonic HWM (state) | A-1 | config_kv |
| `PORTFOLIO_MDD_CURRENT_TIER` | NORMAL/REDUCE/BLOCK/HALT (state) | A-1 | config_kv |
| `TAIL_FUND_CONSUMPTION_ENABLED` | tail fund drawdown debit 킬스위치 (default true) | A-2 | config_kv |
| `evaluate_tail_fund_gate` | tail exhausted + BLOCK escalate (auxiliary) | A-2 | `tail_risk_gate.py` |
| `evaluate_portfolio_mdd_tier` | pure tier fn — gate + try_add shared | A-1 | `execution_safety.py` |
| `BITGET_DB_STORAGE_PATH` | prod data root | B/L | env |
| `BITGET_JOURNAL_MAX_USE` | journal vacuum size cap (default 400M) | L-1 | env |
| `BITGET_JOURNAL_MAX_RETENTION` | journal vacuum time cap (default 30d) | L-1 | env |
| `BITGET_STAMPED_LOG_RETENTION_DAYS` | bitget.sh stamped log TTL (default 14) | L-1 | env → `disk_manager` |
| `LOG_ROTATE_MAX_BYTES` | in-process bitget.log rotate (50MB) | L-1 | `memory_policy` |
| `normalize_market_key` | BG→`SPOT`\|`FUT` deathmatch/registry SSOT | B-1 | `evolution/market_key_normalize.py` |
| `DEATHMATCH_KEY_NORMALIZE_ENABLED` | B-1 kill-switch (default true) | B-1 | env / config_kv / `memory_policy` |
| `normalize_bitget_registry_after_lifecycle` | post-lifecycle BG resolve + write-through | B-1 | `registry_lifecycle_bg.py` |
| `load_registry_rows_normalized` | deathmatch registry read path (B-1) | B-1 | `registry_lifecycle_bg.py` |
| `DEATHMATCH_ALLOCATION_SHADOW_ENABLED` | B-2 shadow kill-switch (default true) | B-2 | env / config_kv / `memory_policy` |
| `observe_kelly_chain_shadow` | Kelly hook — log counterfactual, return unchanged | B-2 | `deathmatch_allocation_shadow.py` |
| `bitget_deathmatch_alloc_shadow` | shadow alloc log table (no sizing path) | B-2 | market_data SQLite |
| `WALK_FORWARD_SHADOW_ENABLED` | B-3 shadow kill-switch (default true) | B-3 | env / config_kv / `memory_policy` |
| `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` | B-3 live registry block (default **false**, defer) | B-3 | env / config_kv / `memory_policy` |
| `evaluate_bad_tick` | C-1 ATR×gap pre-try_add 판정 | C-1 | `signal_engines.py` |
| `bad_tick_should_skip_candidate` | C-1 skip + `bad_tick_filtered` ops_events | C-1 | `signal_engines.py` |
| `BAD_TICK_FILTER_ENABLED` | C-1 kill-switch (default true) | C-1 | config_kv |
| `BAD_TICK_ATR_MULT` | C-1 TR/ATR threshold (default 6.0) | C-1 | config_kv |
| `BAD_TICK_GAP_PCT` | C-1 gap ratio threshold (default 0.15) | C-1 | config_kv |
| `BAD_TICK_ACTION` | C-1 v1 action (default skip) | C-1 | config_kv |
| `compute_bad_tick_skip_summary_bg` | C-1b weekly `bad_tick_filtered` group-by aggregate | C-1b | `observability/bad_tick_skip_summary_bg.py` |
| `run_bad_tick_skip_summary_job` | C-1b weekly_evolution hook (read-only) | C-1b | `observability/bad_tick_skip_summary_bg.py` |
| `BAD_TICK_SKIP_SUMMARY_ENABLED` | C-1b kill-switch (default true) | C-1b | config_kv / `memory_policy` |
| `BAD_TICK_SKIP_SUMMARY_WINDOW_DAYS` | C-1b lookback days (default 7) | C-1b | config_kv / `memory_policy` |
| `bad_tick_skip_summary_weekly` | C-1b ops_events summary event | C-1b | `ops_events` |
| `validate_llm_proposal` | D-1 LLM JSON validate + CAT-MAP §6 risk_class | D-1 | `governance/ai_proposal_schema_bg.py` |
| `persist_proposal_bg` | D-1 `bitget_llm_proposals` insert (valid only) | D-1 | `governance/ai_proposal_schema_bg.py` |
| `AI_PROPOSAL_STRUCTURED_ENABLED` | D-1 kill-switch (default true) | D-1 | config_kv / `memory_policy` |
| `bitget_llm_proposals` | D-1 append-only proposal log | D-1 | market_data SQLite |
| `compute_llm_proposal_summary_bg` | D-1b weekly proposal count/risk_class aggregate | D-1b | `observability/llm_proposal_summary_bg.py` |
| `run_llm_proposal_summary_job` | D-1b weekly_evolution hook (read-only) | D-1b | `observability/llm_proposal_summary_bg.py` |
| `AI_PROPOSAL_SUMMARY_ENABLED` | D-1b kill-switch (default true) | D-1b | config_kv / `memory_policy` |
| `AI_PROPOSAL_SUMMARY_WINDOW_DAYS` | D-1b lookback days (default 7) | D-1b | config_kv / `memory_policy` |
| `llm_proposal_summary_weekly` | D-1b ops_events summary event | D-1b | `ops_events` |
| `record_approval_decision` | D-2 append-only approval/reject event | D-2 | `governance/proposal_approval_bg.py` |
| `apply_approved_proposal` | D-2 per-key `set_config_value` (A-5 bounds) | D-2 | `governance/proposal_approval_bg.py` |
| `AI_PROPOSAL_APPROVAL_GATE_ENABLED` | D-2 kill-switch (default true) | D-2 | config_kv / `memory_policy` |
| `bitget_llm_proposal_approvals` | D-2 approval event log (append-only) | D-2 | market_data SQLite |
| `poll_proposal_approval_updates_once` | D-2 poll getUpdates → approve/reject | D-2 poll | `governance/proposal_approval_poll_bg.py` |
| `AI_PROPOSAL_APPROVAL_POLL_ENABLED` | D-2 poll kill-switch (default true) | D-2 poll | config_kv / `memory_policy` |
| `compute_weekly_cost_report_bg` | D-3a weekly Gemini proxy + paper notional rollup | D-3a | `observability/cost_report_bg.py` |
| `run_cost_report_job` | D-3a weekly_evolution hook (D-1b 직후, read-only) | D-3a | `observability/cost_report_bg.py` |
| `COST_REPORT_ENABLED` | D-3a kill-switch (default true) | D-3a | config_kv / `memory_policy` |
| `COST_REPORT_WINDOW_DAYS` | D-3a lookback days (default 7) | D-3a | config_kv / `memory_policy` |
| `cost_report_weekly` | D-3a ops_events summary event | D-3a | `ops_events` |
| `gmm_dna_alpha_report_weekly` | I-GMM-01b Cos/OPEN/DNA 주간 관측 event | I-GMM-DNA-01b | `ops_events` |
| `GMM_DNA_ALPHA_REPORT_ENABLED` | 01b 리포트 kill-switch (default true) | I-GMM-DNA-01b | config_kv / env |
| `GMM_DNA_ALPHA_REPORT_WINDOW_DAYS` | 01b Cos 로그 창 (default 7) | I-GMM-DNA-01b | config_kv / env |
| `GMM_DNA_ALPHA_REPORT_LOG_SOURCE` | journal\|file 우선 (default journal) | I-GMM-DNA-01b | config_kv / env |
| `B1_LADDER_FASTCHECK_ENABLED` | R1a FASTCHECK weekly kill-switch (default true) | B1-LADDER-R1a-FASTCHECK | config_kv / env |
| `B1_LADDER_FASTCHECK_WINDOW_DAYS` | FASTCHECK CLOSED Δ 창 (default 7) | B1-LADDER-R1a-FASTCHECK | config_kv / env |
| `compute_b1_ladder_fastcheck_bg` | SPOT/FUT R1a verdict read-only | B1-LADDER-R1a-FASTCHECK | `observability/b1_ladder_fastcheck_bg.py` |
| `b1_ladder_fastcheck_weekly` | ops_events mt별 1건 | B1-LADDER-R1a-FASTCHECK | `ops_events` |
| `POST_DEPLOY_OBS_DIGEST_ENABLED` | 일일 관측 텔레그램 kill-switch (default true) | POST_DEPLOY_OBS | config_kv / env |
| `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED` | DNA why-진단 (default true · false=이진 RANK 문구) | POST_DEPLOY_OBS-DNA-UX-01 | config_kv / env |
| `POST_DEPLOY_OBS_LS_SPLIT_ENABLED` | 롱/숏 진행 표시 분리 (default true · 목표 숫자는 Track B 공유) | LS-GOAL-UX-01 | config_kv / env |
| `post_deploy_obs_digest_daily` | 일일 관측 digest ops_events | POST_DEPLOY_OBS | `ops_events` |
| `cost_basis` | D-3a USD 단가 없을 때 `no_usd_unit_rate` | D-3a | ops payload |
| `gemini_call_count_source` | D-3a call proxy: `ops_events` 없으면 `llm_call_cache` 행수 (**실 API 호출 수 아님**) | D-3a | ops payload |
| `fee_basis` | D-3a fee SSOT 없을 때 `no_fee_rate_ssot` | D-3a | ops payload |
| `compute_paper_vs_real_parity_bg` | D-3b paper vs real PnL diff (scaffold fn only) | D-3b | `observability/parity_monitor_bg.py` |
| `PARITY_MONITOR_ENABLED` | D-3b kill-switch (default **false**, no hook v1) | D-3b | config_kv / `memory_policy` |
| `walk_forward_bg` | WF pure fn port (no root import) | B-3 | `validation/walk_forward_bg.py` |
| `bitget_walk_forward_shadow` | OOS pass/fail shadow log (no promotion path) | B-3 | market_data SQLite |
| `run_walk_forward_shadow_job` | weekly batch shadow entry | B-3 | `validation/walk_forward_shadow_bg.py` |
| `count_lifecycle_states_bg` | deduped registry lifecycle counts (read-only) | B-4 | `registry_lifecycle_bg.py` |
| `compute_explore_budget_bg` | MAB explore ratio pure fn | B-4 | `registry_lifecycle_bg.py` |
| `MAB_EXPLORE_BUDGET_CURRENT` | lifecycle-derived explore ratio (no consumer yet) | B-4 | config_kv |
| `LIFECYCLE_EXPLORE_BUDGET_ENABLED` | B-4 kill-switch (default true) | B-4 | env / config_kv / `memory_policy` |
| `MAB_EXPLORE_BUDGET_CEILING` | B-4 ratio cap 0.50 — **B-4b 소비 배선 시 재검토** (통상 explore 5~20%) | B-4 | `memory_policy` |
| `BITGET_BACKUP_ENABLED` | L-2 backup kill-switch (default true) | L-2 | env / `memory_policy` |
| `BITGET_BACKUP_RETENTION_DAYS` | daily backup window (default 7) | L-2 | env / `memory_policy` |
| `BITGET_BACKUP_DIR` | backup archive root (별도 파티션 권장) | L-2 | env |
| `run_restore_drill` | L-2 isolated restore + row parity | L-2 | `integrity_backup_l2.py` |
| `dual_north_star_ledger.py` | 듀얼 북극성 진행장부 (Track A/B read-only) | NS-1 | 루트 |
| `dual_north_star_telegram.py` | Track A 주식 북극성 HTML → REPORT_BOT | NS-1 / NS-DIAG | 루트 |
| `north_star_panel_bg.py` | Bitget Track B 북극성 쉬운판 → POST_DEPLOY_OBS 첫 메시지 | NS-BG-DASH-01 | `bitget/observability/` |
| `ls_split_summary_bg.py` | 롱/숏 OPEN·CLOSED 진행 요약 (표시만) | LS-GOAL-UX-01 | `bitget/observability/` |
| `dual_north_star_ledger.json` | 일/주/월/연 스냅샷·게이트 이력 | NS-1 | `factory_data_dir()` |
| `BITGET_NORTH_STAR_PHASE` | B0/B1/B2/B3 — 목표 밴드·리더 모드 | NS-1 | config_kv |
| `A06_CHECKLIST_FIRST_PASS` | G3 Track A 전제 (`06` 1차) | NS-1 | config_kv (read) |
| `C2_FUNDING_PNL_COMPLETE` | G3 Track B 전제 · R3 배너 해제 | NS-1 | config_kv (read, C-2 후) |

---

## 문서 세트 구성

| 파일 | 역할 |
|------|------|
| `bitget/docs/claude_project/` | CAT 구조·SSOT·Handoff |
| `bitget/docs/work_phases/00_전체현황판.md` | 실행·진행·검증 · **18카테고리↔CAT** |
| `bitget/docs/13_institutional_grade_audit_and_roadmap.md` | P0/P1/P2 원본 |
| `bitget/.cursorrules` | Cursor 세션 규칙 |

---

## 루트 프로젝트와 분리

| | 루트 | Bitget |
|---|------|--------|
| claude_project | `docs/claude_project/` | `bitget/docs/claude_project/` |
| work_phases | `docs/work_phases/` | `bitget/docs/work_phases/` |
| 코드 수정 | `forward/`, `factory_*` | `bitget/**` only |
