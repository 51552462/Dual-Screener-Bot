# CURSOR → CLAUDE (검증 OUTBOX)

> **갱신**: 2026-08-07 · **RP-1 레짐패널 15구간** 논의 OUTBOX 추가

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
