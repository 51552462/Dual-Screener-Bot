# Architect Mirror — Claude 능동 의견 로그 (KR/US · Track A)

> **역할**: Cursor 로컬 변경을 Claude가 **구조 지도**로 대입해 OK/거절 외 **추가 설계·우선순위·리스크**를 남기는 SSOT.  
> **갱신**: Claude Pro — 검증·Handoff·갭 리뷰 세션 **종료 시 상단에** 날짜 블록 추가.  
> **Cursor**: `CURSOR_TO_CLAUDE.md` §로컬 구조 스냅샷 최신 유지.  
> **Bitget 대응**: `bitget/docs/work_phases/ARCHITECT_MIRROR.md` (Track B, **내용 섞지 않음**)

---

## Mirror — 2026-08-07 · [MASTER] · RP-1 레짐패널 15구간 (디렉터 요청)

### 로컬 구조 이해
- 맞게 반영된 점: `time_machine_backtester.run_time_machine_regime_matrix` + `REGIME_PERIODS`가 RP-1 **골격** — 신규 플랫폼 불필요 · Alpha Proof 2주 timebox와 **병렬 가능**
- 빠거나 불일치: 현재 **6구간**만 등록 · Phase A 거버너·포트폴리오 MDD는 matrix에 **미포함** — RP-1 Pass 정의에 따라 v1/v2 분리 필요

### 추가 제안
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | RP-1을 C-1 **선행 게이트**로 고정 — C-1 backtest 전 baseline 15구간 Fail이면 C-1 No-Go | 🟡 MASTER | 최고 | 코드 추가 전 구조적 상한 확인 |
| 2 | Near-miss 리포트에 **arm별(S1/S4/S5) 기여도** 컬럼 필수 | 🟡 C/F | 높음 | "왜 근처까지 갔는지" 정량화 |
| 3 | Lookahead v1 결과는 **상한선**으로만 쓰고 Pass 문구 금지 | 🟡 MASTER | 최고 | 과거 머무름·과신 방지 |

### 다음 Handoff 후보 재정렬
1. **RP-1** (15구간 확정 + matrix 리포트) — **C-1보다 먼저 또는 동일 Handoff**
2. **C-1** — RP-1 baseline 대비 A/B만

### 디렉터 한 줄
> 15구간은 "증명"이 아니라 **지도**다 — Fail이면 목표·구조 중 하나를 바꾸고, Near-miss면 C-1 한 방에 기대지 말 것.

---

## Mirror — 2026-08-07 · [MASTER] · RP-1 15구간 Pass/Fail 규칙 확정 + C-1 병합 Handoff

### 로컬 구조 이해
- 맞게 반영된 점: `REGIME_PERIODS` 6구간·`run_time_machine_regime_matrix` 기존 인프라로 RP-1 신규 플랫폼 불필요 확인. Fail-원인별 C-1 자동분기 구체화
- 빠졌거나 불일치: 하락 5구간 인과 중복 제거 → BEAR_05 미국신용등급강등(2011) 등으로 교체 반영

### Cursor 구현 (2026-08-07)
- `regime_panel_rp1.py` — tier replay · 판정 · Stage2 분기 · JSON
- `REGIME_PERIODS` 15 + backup 치환
- 테스트 13 passed

### 디렉터 한 줄
> 15구간 확정, 중복 사건 정리 완료. RP-1이 막으면 C-1은 자동으로 서고, RP-1이 열어주면 C-1은 좁게 들어간다.

---

## Mirror — 2026-08-07 · [MASTER] · Alpha Proof 압축 구조 확정

### 로컬 구조 이해
- 맞게 반영된 점: 디렉터 압축 제안은 G2 재정의가 아니라 "fail-fast 체크포인트 삽입"으로 재해석 가능 — 기존 `dual_north_star_ledger` 연속 클록과 충돌 없음
- 빠졌거나 불일치: 4주 ASG를 CAGR 증명으로 오독할 위험 — n<30 통계적 무력함을 문서에 명시 안 하면 "4주 통과=알파 증명" 착각 재발 가능

### 추가 제안
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | ASG 통과 기준을 CAGR/Sharpe 숫자가 아닌 **정성 체크리스트**(작동여부·재앙없음)로 고정 문서화 | 🟡 MASTER | 최고 | 소표본 오판정 방지 |
| 2 | C-1 backtest 2주 timebox — 무결론 시 자동 No-Go 규칙을 Handoff 본문에 명문화 | 🟡 C | 높음 | "코드 더 짓지 말 것" 원칙 실행력 확보 |

### 다음 Handoff 후보 재정렬
1. **C-1** — backtest Go/No-Go 단계 추가된 버전으로 (변경 없음, 절차만 추가)

### 디렉터 한 줄
> G2는 그대로 둔다. 4주는 "증명"이 아니라 "조기경보"다.

---

## Mirror — 2026-08-07 · [MASTER] · 전략 재편 + A-5b 검증

### 로컬 구조 이해
- 맞게 반영된 점: A-5a→A-5b Kelly weight/regime gate 분리 설계 일관 · killswitch 독립성(교차조건 없음) 누적 유지 · Cursor STRATEGIC REVIEW 진단(Phase A=방어 필요조건, 알파층 미검증) 실측 기반 타당
- 빠졌거나 불일치: G1도 미판정 상태에서 A-5b까지 4개 sub-phase(R2~R4, A-4, A-5a, A-5b)가 배포 승인 대기 **누적** — 디렉터 Critical 승인 병목이 실제 freeze 지연 요인

### 추가 제안
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | A-5b + 누적 R-series **일괄 배포 승인** 요청 (개별 승인 대기 병목 해소) | ① 디렉터 | 최고 | freeze 시점이 배포 승인에 묶여 있음 |
| 2 | C-1 Handoff을 "섹터신호 = supernova 진입점수 입력"으로 **스코프 고정** (Markov/스필오버 일반론화 금지) | 🟡 C | 높음 | 신규 빌드를 supernova 베팅에 종속시켜 12주 내 완성 |
| 3 | `dual_north_star_ledger` 일일 기록 cron 등록을 C-1과 **별개로 이번 주 즉시 시작** (코드 변경 없음, cron만) | ① 디렉터 | 최고 | G1/G2 시계가 아직 0일째 — 대기할 이유 없음 |

### 다음 Handoff 후보 재정렬
1. **C-1** 섹터 스필오버 (supernova 진입 보강 스코프 한정)
2. (병렬, 코드 아님) `dual_north_star_ledger` 일일 digest cron 등록 — 디렉터 실행

### 디렉터 한 줄
> Phase A는 A-5b로 사실상 종료 — 이제부터 12주는 supernova+C-1로 G2를 "증명"하는 데만 쓴다. 코드는 늘리지 않는다.

---

## Mirror — 2026-08-06 · [CAT-G] · A-4 검증 완료

### 로컬 구조 이해
- Adapter·6쌍·crisis bypass·killswitch·UNKNOWN fallback 확인 · 16 passed
- 비고(블로킹 아님): 6쌍 명시목록·`HYSTERESIS_MIN_DAYS` 재사용 문서 한 줄 권고

### 다음 Handoff 후보
| # | 후보 | 비고 |
|---|------|------|
| 1 | **A-1-R3** CAT-F | L243 clutch — 즉시 착수 |
| 2 | **A-5** S5 | F+G+C — Rule4 충돌 · 디렉터 (a)/(b) 확인 후 |

### 디렉터 한 줄
> A-4 Done — R3 또는 배포 승인 먼저. A-5는 세션 방식 결정 전 설계 안 함.

---

## Mirror — 2026-08-05 · [CAT-F] · A-1-R2 검증 완료

### 로컬 구조 이해
- 맞게 반영된 점: L234 `resolve_config_float` · 2+7 테스트 · tier/거버너 쓰기 미변경
- grep 부가 등록: L243→**A-1-R3** · L73/L411→🟡 후속 · doomsday→🟢 종결

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | **A-4** CAT-G 히스테리시스 Adapter Handoff (권장 다음) | 🟡 G | 최고 |
| 2 | A-1-R3 L243 clutch mult (디렉터 선택 시 CAT-F 단독) | 🔴 F | 중간 |
| 3 | A-1-R2 **디렉터 Critical 배포 승인** + `update_factory.sh` | ① 디렉터 | 높음 |

### 디렉터 한 줄
> R2 Done — **A-4(G) 새 대화** 권장. R3·배포 승인은 병렬 선택.

---

## Mirror — 2026-08-05 · [CAT-F] · M-R0 + A-3 + A-1-R1 검증 완료

### 로컬 구조 이해
- 맞게 반영된 점: A-3 `POSITION_QUOTA_REGIME_MAP`·곱연산(floor) Handoff 일치 · A-1-R1 `resolve_config_float` SSOT · LOCKDOWN 3중 방어 `00` 명문화 유지 · M-R0 비침투·D-2 분리
- 빠졌거나 불일치: `meta_governor_consumer` L234 `META_GLOBAL_KELLY_MULT` `or 1.0` — A-1-R1 스코프 밖, **A-1-R2 후보**

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | `META_GLOBAL_KELLY_MULT` falsy 0.0 감사 — **A-1-R2** 또는 A-2 정식 편입 | 🔴 F | 높음 |
| 2 | A-4 Handoff — `hysteresis_days_f`(RL) vs 고정 1일/2일 **Adapter** (CAT-G 단독 세션) | 🟡 2 | 높음 |
| 3 | 서버 배포·paper MDD/tier 관측 (`06` 3단계 준비) | ① 디렉터 | 중간 |

### 다음 Handoff 후보 재정렬
1. **A-4** 비대칭 히스테리시스 Adapter (디렉터 확인 후 새 대화)
2. **A-1-R2** META_GLOBAL_KELLY_MULT falsy (A-4 전·후 병렬 가능)

### 디렉터 한 줄
> M-R0·A-3·A-1-R1 Done — **A-4 Adapter Handoff**로 Phase A 후반 재개. R2는 Kelly chain 잔여 falsy.

---

## Mirror — 2026-08-04 · [CAT-M] · M-R0 + factory 긴급 복구

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `llm_provider_core`·`overseer_quality`·degraded audit·watchdog heartbeat thread·`inverse_etf_sniper` 741/797 문법 수정 — 비침투·D-2 분리 유지
- 빠졌거나 불일치: Bitget `overseer_audit_binder` **import 없음**(격리 헌법) — `overseer_audit_contract.py` 계약만. Anthropic 실연동·서버 `.env` 중복 `DB_STORAGE_PATH`는 **운영 잔여**

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | Claude OK: M-R0 + A-3 + A-1-R1 **병렬 검증** (`CURSOR_TO_CLAUDE`) | ① 디렉터 | 최고 |
| 2 | 서버 `.env` — `DB_STORAGE_PATH` **한 줄만** `/var/lib/quant-factory/data` | ① 디렉터 | 최고 |
| 3 | A-4 Handoff 전 RL `hysteresis_days_f` vs `predictive_regime_ensemble` **Adapter 제안** 필수 | 🟡 2 | 높음 |

### 디렉터 한 줄
> factory 살아난 뒤 **문서·Claude OK**로 Phase A 다시 전진. 코인(Bitget) Mirror와 파일만 대칭, 내용은 분리.

---

## Claude가 쓸 블록 형식

```markdown
## Mirror — YYYY-MM-DD · [CAT-X] · {sub-phase}

### 로컬 구조 이해
- 맞게 반영된 점:
- 빠졌거나 불일치:

### 추가 제안 (1~3개)
| # | 제안 | Layer | 우선순위 | 근거 |

### 다음 Handoff 후보 재정렬
1.
2.

### 디렉터 한 줄
>
```

---

## Cursor OUTBOX 스냅샷 (`CURSOR_TO_CLAUDE.md`에 포함)

- 변경 파일·함수·config 키
- 기존 모듈과 **겹침/중복**
- 알려진 부채 (테스트·서버·defer)
- Attribution (🟢/🟡/🔴)

---

## 디렉터 사용법

1. Cursor 구현 후 → `CURSOR_TO_CLAUDE` 복붙  
2. Claude 답변에 **OK + Mirror 블록** 요청 → 본 파일 상단 갱신  
3. `09`·`NEXT_STEP`에 Claude 제안 반영 여부 결정

**금지**: Mirror가 Handoff 없이 🔴 live 변경 지시.
