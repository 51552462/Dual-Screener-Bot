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
| A-1 NAV MDD tier | 2026-08-01 | _(paper 배포 전)_ | tier 전이 로그·NAV MDD | _(대기)_ |
| A-2 tail fund debit | 2026-08-02 | _(paper 배포 전)_ | debit 이벤트·잔액 추이 | _(대기)_ |
| A-3 leverage clamp | 2026-08-02 | _(paper 배포 전)_ | clamp 발생 빈도 | _(대기)_ |
| A-4 gross notional block | 2026-08-02 | _(paper 배포 전)_ | 차단 이벤트·gross/NAV | _(대기)_ |
| A-5 config reject | 2026-08-02 | _(paper 배포 전)_ | reject 로그·meta_sync | _(대기)_ |

**판정**: `유지` / `롤백` / `추가조정`

---

## 실패/롤백 기록

| 시도한 것 | 왜 실패 | 롤백 | 다음 참고 |
|-----------|---------|------|-----------|
| _(없음 — 첫 기록 대기)_ | | | |

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
