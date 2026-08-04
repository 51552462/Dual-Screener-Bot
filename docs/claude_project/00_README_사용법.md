# Claude Pro Project Knowledge — 사용법

> **레포**: Dual-Screener-Bot · **범위**: KR/US 주식만 · **bitget/ 절대 제외**  
> **목적**: Claude 사용량(컨텍스트 토큰)을 아끼면서 **정확한** 설계·교차검증

---

## 1. 업로드 전략 (3-Tier)

Claude Pro Project Knowledge에는 **전부 올리되**, 대화 시 **필요한 Tier만 @멘션**한다.

| Tier | 파일 | 언제 켜나 | 예상 역할 |
|------|------|----------|----------|
| **T0 항상** | `00_CUSTOM_INSTRUCTIONS` (Project Instructions에 붙여넣기) | 매 대화 | 행동 규칙 |
| **T0 항상** | `CAT-MAP_의존성경계.md` | 경계·충돌 질문 | ~2k 토큰 |
| **T1 자주** | `CAT-KR-US_비대칭표.md`, `CAT-CONSTANTS_상수레퍼런스.md` | KR/US·상수 질문 | ~3k |
| **T2 작업별 1개** | `CAT-A` ~ `CAT-P` 중 **하나만** | 해당 영역 설계 | ~4–8k each |
| **T3 보조** | `CAT-HANDOFF`, `CAT-Q` | Prompt 작성·진단 | 필요 시 |

### 토큰 절약 규칙

1. **한 대화 = CAT 1개 + T0** — 예: "Kelly 변경" → `@CAT-F` + `@CAT-MAP` 만. `@CAT-C` `@CAT-G` 동시 금지.
2. **상수는 CAT-CONSTANTS에만** — 개별 CAT 파일에 상수 반복 없음 → `@CAT-CONSTANTS` 로 조회.
3. **코드 전문 요청 금지** — Claude는 수식·정책·config 키·함수 시그니처만 출력 → Cursor가 구현.
4. **bitget 언급 시 거부** — 코인은 별도 프로젝트.
5. **장문 답변 대신 Handoff** — 설계 완료 시 `CAT-HANDOFF` 형식으로 Cursor Prompt만 생성.

---

## 2. 파일 목록

| 파일 | CAT | 위험도 |
|------|-----|--------|
| `CAT-MAP_의존성경계.md` | MAP | — |
| `CAT-KR-US_비대칭표.md` | KR/US | — |
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
| 스케줄·cron·데몬 | CAT-A (+ MAP) |
| DB·스키마·OHLCV | CAT-B (+ MAP) |
| 초신성·퍼널·DNA | CAT-C, CONSTANTS |
| 원장·OPEN/CLOSED | CAT-D (+ MAP) |
| 익절·손절·러너 | CAT-E, CONSTANTS |
| Kelly·NAV·포지션 쿼터 | CAT-F, CAT-G, CONSTANTS |
| BULL/BEAR·VIX·앙상블 | CAT-G, KR-US |
| GP·OOS·INCUBATOR | CAT-H, CAT-K |
| Toxic·ANTI_PATTERNS | CAT-I |
| 일일 9단계·Flow | CAT-J |
| config 키 추가 | CAT-K (+ MAP) |
| systemd·watchdog | CAT-L |
| Gemini·감성 | CAT-M |
| Fast Safety shadow | CAT-N, CAT-F |
| Mega trend kill | CAT-P |
| Cursor Prompt 작성 | CAT-HANDOFF + 해당 CAT |

---

## 4. Claude ↔ Cursor 워크플로

```
디렉터 → Claude (CAT 1개 + MAP)
       → Handoff Prompt (CAT-HANDOFF 형식)
       → Cursor (.cursorrules + 로컬 코드)
       → 충돌 시 Cursor → Claude (CAT-MAP + 해당 CAT)
       → 🔴 Critical(F/G/N/B) → 디렉터 승인
```

**실행 SSOT**: `docs/work_phases/` — `00_마스터_로드맵.md`, `07`, `10`, `NEXT_STEP.md`, `09`

---

## 6. Bitget 프로젝트와의 관계

| | 본 폴더 | `bitget/docs/claude_project/` |
|---|---------|-------------------------------|
| 범위 | KR/US | Bitget |
| MDD | 10% | 5% |
| work_phases | `docs/work_phases/` | `bitget/docs/work_phases/` |

---

*버전 2026-08-04 · 상위: `docs/work_phases/README.md`*
