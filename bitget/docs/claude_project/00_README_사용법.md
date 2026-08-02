# Claude Pro Project Knowledge — Bitget 사용법

> **레포**: Dual-Screener-Bot/bitget · **범위**: Bitget 코인(현물·선물) **전용** · **루트 주식 모듈 절대 수정 금지**  
> **목적**: Claude 사용량(컨텍스트 토큰)을 아끼면서 **정확한** 설계·교차검증

---

## 1. 업로드 전략 (3-Tier)

Claude Pro Project Knowledge에는 **전부 올리되**, 대화 시 **필요한 Tier만 @멘션**한다.

| Tier | 파일 | 언제 켜나 | 예상 역할 |
|------|------|----------|----------|
| **T0 항상** | `00_CUSTOM_INSTRUCTIONS` (Project Instructions에 붙여넣기) | 매 대화 | 행동 규칙 |
| **T0 항상** | `CAT-MAP_의존성경계.md` | 경계·충돌 질문 | ~2k 토큰 |
| **T1 자주** | `CAT-SPOT-FUT_비대칭표.md`, `CAT-CONSTANTS_상수레퍼런스.md` | SPOT/FUT·상수 질문 | ~3k |
| **T2 작업별 1개** | `CAT-A` ~ `CAT-Q` 중 **하나만** | 해당 영역 설계 | ~4–8k each |
| **T3 보조** | `CAT-HANDOFF`, `CAT-Q` | Prompt 작성·진단 | 필요 시 |

### 토큰 절약 규칙

1. **한 대화 = CAT 1개 + T0** — 예: "NAV MDD breaker" → `@CAT-F` + `@CAT-MAP` 만. `@CAT-C` `@CAT-G` 동시 금지.
2. **상수는 CAT-CONSTANTS에만** — 개별 CAT 파일에 상수 반복 없음 → `@CAT-CONSTANTS` 로 조회.
3. **코드 전문 요청 금지** — Claude는 수식·정책·config 키·함수 시그니처만 출력 → Cursor가 구현.
4. **루트 주식 코드 언급·설계 시 거부** — KR/US는 별도 프로젝트(`docs/claude_project/`).
5. **장문 답변 대신 Handoff** — 설계 완료 시 `CAT-HANDOFF` 형식으로 Cursor Prompt만 생성.

---

## 2. 파일 목록

| 파일 | CAT | 위험도 |
|------|-----|--------|
| `CAT-MAP_의존성경계.md` | MAP | — |
| `CAT-SPOT-FUT_비대칭표.md` | SPOT/FUT | — |
| `CAT-CONSTANTS_상수레퍼런스.md` | CONST | — |
| `CAT-HANDOFF_템플릿.md` | HANDOFF | — |
| `CAT-A_오케스트레이션.md` | A | 🟡 |
| `CAT-B_데이터계층.md` | B | 🔴 |
| `CAT-C_스크리닝.md` | C | 🟡 |
| `CAT-D_Forward원장.md` | D | 🔴 |
| `CAT-E_청산엔진.md` | E | 🔴 |
| `CAT-F_자본리스크.md` | F | 🔴 Critical |
| `CAT-G_국면메타.md` | G | 🔴 Critical |
| `CAT-H_진화HTC.md` | H | 🟡 |
| `CAT-I_면역Toxic.md` | I | 🔴 |
| `CAT-J_리포팅.md` | J | 🟢 |
| `CAT-K_설정SSOT.md` | K | 🔴 |
| `CAT-L_인프라배포.md` | L | 🟡 |
| `CAT-M_LLM오버시어.md` | M | 🟢 |
| `CAT-N_FastSafety.md` | N | 🔴 Critical |
| `CAT-O_Practitioner.md` | O | 🟢 |
| `CAT-P_MegaTrend_ReEvolution.md` | P | 🟡 |
| `CAT-Q_진단레거시.md` | Q/R | 🟢 |

---

## 3. 작업 유형 → 로드 매트릭스

| 디렉터 질문 유형 | @멘션 파일 |
|-----------------|-----------|
| 스케줄·cron·데몬·파이프라인 | CAT-A (+ MAP) |
| DB·스키마·OHLCV·WebSocket | CAT-B (+ MAP) |
| 스캔·시그널·유니버스 | CAT-C, CONSTANTS |
| 원장·OPEN/CLOSED·paper PnL | CAT-D (+ MAP) |
| 익절·손절·funding·exit | CAT-E, CONSTANTS |
| Kelly·Treasury·포트폴리오 MDD | CAT-F, CAT-G, CONSTANTS |
| BULL/BEAR/CHOP·Meta sync | CAT-G, SPOT-FUT |
| GP·OOS·deathmatch·INCUBATOR | CAT-H, CAT-K |
| Toxic·ANTI_PATTERNS | CAT-I |
| 일일 audit·Flow 리포트 | CAT-J |
| config 키 추가 | CAT-K (+ MAP) |
| systemd·watchdog·deploy | CAT-L |
| Gemini·감성·ai_overseer | CAT-M |
| 실전 실행·OMS·execution_safety | CAT-N, CAT-F |
| Practitioner rules | CAT-O |
| Mega trend kill | CAT-P |
| Cursor Prompt 작성 | CAT-HANDOFF + 해당 CAT |
| 아키텍처 진단·레거시 | CAT-Q |

---

## 4. Claude ↔ Cursor 워크플로

```
디렉터 → Claude (CAT 1개 + MAP)
       → Handoff Prompt (CAT-HANDOFF 형식)
       → Cursor (bitget/.cursorrules + bitget/ 로컬 코드)
       → 충돌 시 Cursor → Claude (CAT-MAP + 해당 CAT)
       → 🔴 Critical(F/G/N/B/D) → 디렉터 승인
```

**실행 SSOT**: `bitget/docs/work_phases/` — `07_듀얼AI_협업루프.md`, `10_작업함_프로토콜.md`

---

## 5. 상세 depth가 더 필요할 때

로컬 참조 문서 (Claude Knowledge에 **선택적** 업로드):

| 문서 | 용도 |
|------|------|
| `bitget/docs/01_architecture_mapping_and_diagnosis.md` | 주식↔Bitget 매핑 |
| `bitget/docs/13_institutional_grade_audit_and_roadmap.md` | P0/P1/P2 로드맵 |
| `bitget/docs/07_phase8_feasibility_review.md` | Track A/B/C |

너무 큰 파일은 **섹션 지정** 후 Cursor가 해당 부분만 인용.

---

## 6. 루트 프로젝트와의 관계

| | 루트 `docs/claude_project/` | 본 폴더 `bitget/docs/claude_project/` |
|---|---------------------------|--------------------------------------|
| 범위 | KR/US 주식 | Bitget 코인 |
| work_phases | `docs/work_phases/` | `bitget/docs/work_phases/` |
| import | — | 루트 모듈 **읽기 전용** (`practitioner_*`, `reports/*`, `llm_gemini_core`) |
| 격리 | bitget/ 제외 | 루트 `forward/`, `factory_pipelines.py` **수정 금지** |

---

*버전 2026-08-01 · 상위: `bitget/docs/work_phases/README.md`*
