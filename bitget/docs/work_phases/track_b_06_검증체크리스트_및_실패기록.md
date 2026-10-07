# 검증 체크리스트 & 실패 기록 (Bitget)

> 각 sub-phase·Phase 작업항목이 **"진짜 효과 있었는지"** 판정하는 기준.  
> **협업**: Claude OK = **1단계(구현·스펙)** 통과 · **Done = 3단계 전부** (`07_듀얼AI_협업루프`)

---

## 작업항목별 "완료(Done)" 3단계 (필수)

| 단계 | 내용 | 담당 |
|------|------|------|
| **1. 구현 완료** | Cursor 코드 반영, Claude 스펙 일치 검증 | Cursor → Claude |
| **2. 가상매매 반영** | 최소 **2~4주** paper trading 데이터 | 시스템 자동 |
| **3. 효과 검증** | 변경 전/후 — NAV MDD·복리 페이스 | 디렉터 + Claude |

**`05` 체크박스만으로 Done 금지.**

---

## sub-phase별 최소 검증 항목

### Phase 9 · 묶음A (리스크)

| sub | 구현 검증 (1단계) | 가상매매 (2단계) | 효과 (3단계) |
|-----|------------------|------------------|--------------|
| A-1 | ✅ tier mock NAV → block/reduce/halt | tier transition logs | drawdown 선제 조임 |
| A-2 | ✅ tail fund debit on drawdown | tail balance events | loss absorption |
| A-3 | ✅ leverage > MAX clamped | all resolve paths | no over-leverage opens |
| A-4 | ✅ gross notional block | open count × size | correlated crash survival |
| A-5 | ✅ out-of-range config rejected | invalid write attempts | param drift prevented |

### Phase 10 · 묶음B (진화)

| sub | 구현 | 가상매매 | 효과 |
|-----|------|---------|------|
| B-1 | key consistency test | deathmatch rows | no BG/SPOT split |
| B-2 | alloc flag shadow 4w | Kelly mult losers | bad strategy starvation |
| B-3 | walk-forward dry-run | promotion blocks | OOS fail rate ↓ |
| B-4 | lifecycle counts > 0 | MAB explore events | registry health |

### Phase 11 · 묶음C (품질)

| sub | 구현 | 가상매매 | 효과 |
|-----|------|---------|------|
| C-1 | spike candle blocked | bad tick skips | false signal ↓ |
| C-2 | funding in closed pnl | futures closes | PnL realism ↑ |
| C-3 | correlated cap block | BTC dump scenario | concentration ↓ |

### Phase 12 · 묶음D (거버넌스)

| sub | 구현 | 가상매매 | 효과 |
|-----|------|---------|------|
| D-1 | JSON parse mock | proposal events | free-text 0 |
| D-2 | approve cmd only apply | pending proposals | no silent param drift |
| D-3 | cost line in weekly | API usage | budget visibility |

---

## 효과 검증 기록표

| 항목 | 변경일 | 변경 전 | 변경 후 (2~4주) | 판정 |
|------|--------|---------|----------------|------|
| A-1 NAV MDD tier | 2026-08-01 | _(paper 배포 전)_ | **null** · 소스 없음(ops_events에 tier 이벤트 0). 현재 kv `PORTFOLIO_MDD_CURRENT_TIER=NORMAL` · `NAV_PEAK=100000` (이력 아님) | _(대기 · Claude 3단계)_ |
| A-2 tail fund debit | 2026-08-02 | _(paper 배포 전)_ | **null** · 로그 소스 없음 | _(대기 · Claude 3단계)_ |
| A-3 leverage clamp | 2026-08-02 | _(paper 배포 전)_ | **null** · 로그 소스 없음 (클램프는 logger.info만, ops_events 미기록) | _(대기 · Claude 3단계)_ |
| A-4 gross notional block | 2026-08-02 | _(paper 배포 전)_ | **null** · 로그 소스 없음 | _(대기 · Claude 3단계)_ |
| A-5 config reject | 2026-08-02 | _(paper 배포 전)_ | **null** · 로그 소스 없음 (reject는 logger.warning, ops_events 미기록) | _(대기 · Claude 3단계)_ |
| CAT-L-FENCE-02 cron 슬라이스 펜스 | 2026-09-27 | cron OOM 3회 재발(09-07 · 09-14 · 09-25) | (대기) — 판정 예정 **2026-10-13** (관측 시작=2026-09-29 FENCE_OK 고정 · 09-27~09-29 소실 구간 제외) · 지표: 글로벌 OOM 0건 / slice 내 kill 건수 / (a) 잡 미실행·실패 줄 수 / 3-way 시 slice MemoryCurrent 피크(상한 재확정 근거) | (대기) |
| A5-EVENTLOG-01 A-1~A-5 ops_events 계측 | 2026-09-26 (구현) · 서버 반영 미확인 | A-1~A-5 8주 판정 근거 없음(A-EFFECTVERIFY-01 5개 sub 전부 null) | (대기) — 서버 반영 확인 후 판정일 확정(반영일+28일). 지표: 이벤트 5종 발생 건수. 0건이면 "조건 미발생"과 "계측 미작동"을 구분하는 점검 1회 | (대기) |

**판정**: `유지` / `롤백` / `추가조정`

---

## 실패/롤백 기록

| 시도한 것 | 왜 실패 | 롤백 | 다음 참고 |
|-----------|---------|------|-----------|
| CAT-L Y2(09-29 10:30~10-06 06:27 UTC, ≈164h) | 스코프 184 = ≥5400초 164 + <1800초 20, 중간 0건. 합계 926,969s > 구간 590,243s. 원인 미확정(H1 잔류 / H2 스로틀 / H3 대기) | (해당 없음 — 관측) | CAT-A-LIFECAP-02 1단계(읽기전용) Phase 1 선행. FENCE-02 3단계(10-13) 입력 |
| CAT-L OOM 09-25 14:42 UTC | 커널 기록 확정(cron.service 소속 python, total-vm 2.16GB, 펜스 이전). 09-07·09-14 및 부팅 경계 09-13·09-22는 커널 기록 소실로 원인 미확정 종결. OOM→09-26 Stop 인과는 18h 간격으로 미확정 | (해당 없음 — 관측) | FENCE-02 이후 동종 OOM 0(부팅 0). SECRET-01·BACKUP-01은 별 창 |
| CAT-L-FENCE-02 회귀(2026-09-28 02:24 UTC) | 장애 대응 중 디렉터 `update_bitget.sh`(TTY pts/0) → `deploy_bitget_factory.sh` → root `install_bitget_cron.sh`가 origin 구 생성기(HEAD `1e38166`)로 cron 재생성 → 수동 적용 wrapper 소실. X2: `Sep 28 02:25:01 cron[404]: (*system*dual-screener-bitget) RELOAD`(설치 종료 직후)로 cron 파일 실제 변경 확인. 근본 원인: 라이브 수동 변경이 SSOT(origin)에 없었음 | FENCE-02 재적용(09-29) | CAT-L-FENCE-03(cron 마커 + pull 전 `--diff-live` 가드). 증거 `snapshots/CAT-L-CUTOVER-01_P1PRE_XBLOCK_20261001.md` |

---

## 롤백 원칙

1. **NAV MDD 악화** (전후 2~4주) → **무조건 롤백** 해당 sub config
2. **ENABLE_REAL_EXECUTION** incident → immediate false + postmortem in 본 표
3. 롤백 후 `05` + `00` + `06` 동시 갱신

---

## 인프라 P0 (CAT-L · work_phases 외부 추적)

| ID | 항목 | 3단계 |
|----|------|-------|
| L-1 | ✅ logrotate + journal vacuum deploy | server install + timer | disk stable 30d |
| L-2 | **Cursor ✅ · Claude OK ✅** integrity backup P0-5 | restore drill pass | **서버 install 대기** |

> L-* 완료 시 `05`에 별도 섹션 추가
