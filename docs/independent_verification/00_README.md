# 독립 검증 (Independent Verification) — SSOT

> **갱신**: 2026-08-09  
> **목적**: 채널(Claude Pro / Cursor / Telegram / Desktop)이 여러 개여도 **“시험 본인이 채점”** 위험을 한 곳에서 관리  
> **범위**: Track A (KR/US) + Track B (Bitget) — **듀얼 북 전체**

---

## 이 폴더가 하는 일

| 다른 폴더 | 역할 | 본 폴더와 관계 |
|-----------|------|----------------|
| `docs/claude_project/` | 구조·CAT·경계·상수 (설계 SSOT) | IV는 **“통계적·방법론적 검증”** 전용 — CAT 보완 |
| `bitget/docs/claude_project/` | Bitget 설계 SSOT | 동일 — Track B 검증 항목은 `03_시장별_현황_매트릭스.md` |
| `docs/work_phases/` | 실행 로드맵·sub-phase·진행로그 | IV sub-phase는 **`V-*`** 접두 — `05_진행로그`에 **IV 섹션** 병기 |
| `docs/work_phases/06_검증체크리스트_*.md` | 구현 3단계 Done | IV `06_검증체크리스트.md`는 **방법론 검증** 3단계 |

**한 줄**: `claude_project` = **무엇을 만들지**, `work_phases` = **어떻게 구현·배포할지**, **`independent_verification`** = **그 결과를 믿어도 되는지**.

---

## 채널 엇갈림 방지 규칙

1. **백테스트·RP-1·mutant OOS 숫자로 LIVE 승격 근거를 쓰지 않는다** — `01_자기채점_위험_헌법.md`
2. **승격·목표 달성 판정은 forward paper + G2 클록 + (향후) V-* 하드 게이트만**
3. Claude/Cursor Handoff에 “검증”이 들어가면 **어느 층**(운영 / 통계 / 구조 스크리닝)인지 명시
4. 새 AI 대화 시작 시 **본 폴더 `00_README` + `NEXT_ACTION` + `02_편향_체크리스트`** 먼저 @멘션
5. Track A 질문 → `docs/independent_verification/` · Track B 질문 → 동일 폴더 + `bitget/docs/work_phases/06`

---

## 파일 목록

| 파일 | 내용 |
|------|------|
| **`00_README.md`** | 본 문서 — 진입점 |
| **`01_자기채점_위험_헌법.md`** | 디렉터 의도 · 금지·허용 판정 |
| **`02_편향_체크리스트_전체목록.md`** | 6대 위험 + 추가 14항 — 코드·문서 SSOT |
| **`03_시장별_현황_매트릭스.md`** | KR / US / BG × 항목별 ✅⚠️❌ |
| **`04_코드_SSOT_맵.md`** | 검증 관련 파일·테이블·env 키 |
| **`05_갭_및_로드맵.md`** | V-0~V-4 sub-phase · 우선순위 |
| **`06_검증체크리스트.md`** | IV 전용 3단계 완료 정의 |
| **`NEXT_ACTION.md`** | IV 작업 status SSOT |
| **`CLAUDE_TO_CURSOR.md`** | Claude → Cursor Handoff INBOX (V-*) |
| **`CURSOR_TO_CLAUDE.md`** | Cursor → Claude 검증 OUTBOX |

---

## work_phases 협업 루프 (동일 6단계)

`docs/work_phases/07_듀얼AI_협업루프.md` 와 **동일**. IV sub-phase만 `V-*` 로 구분.

```
① Claude  →  V-* Handoff (본 폴더 CLAUDE_TO_CURSOR)
② 디렉터  →  Cursor에 붙여넣기
③ Cursor  →  구현 + docs/work_phases/05_진행로그 IV 섹션 + 본 폴더 NEXT_ACTION
④ 디렉터  →  CURSOR_TO_CLAUDE 로 Claude 검증
⑤ Claude  →  OK / spec 수정 (OK 전 Done 금지)
⑥ 2~4주   →  06_검증체크리스트 효과 행 갱신
```

**Handoff 형식**: `docs/claude_project/CAT-HANDOFF_템플릿.md` — sub-phase ID만 `V-1` 등으로 교체.

---

## Claude Pro Knowledge 권장

**마스터·총괄 프로젝트** (KR/US + Bitget 목표 검증 질문 시):

1. `@docs/independent_verification/00_README`
2. `@docs/independent_verification/02_편향_체크리스트_전체목록`
3. `@docs/work_phases/14_레짐패널_15구간_목표검증` (RP-1 = 구조 스크리닝만)
4. `@docs/work_phases/06_검증체크리스트_및_실패기록`
5. 해당 Track: `@docs/claude_project/CAT-H` 또는 `bitget/docs/claude_project/CAT-H_진화HTC`

---

## 관련 North Star (목표와 IV의 관계)

| 트랙 | 목표 | IV가 막아야 할 착각 |
|------|------|-------------------|
| **A** (KR+US) | CAGR 40~70%, MDD -10% | RP-1 Pass = 목표 달성 ❌ |
| **B** (Bitget) | CAGR 12~25%, MDD -5%, B0 | paper 미검증(R3) 숫자로 승격 ❌ |
| **공통** | G2 ≥56일 · trades≥30 | 짧은 fast-track만으로 LIVE ❌ |

SSOT: `dual_north_star_ledger.py` · `docs/work_phases/12_듀얼북극성_진행장부_및_상품화.md`

---

*상위: `docs/work_phases/README.md` · 대칭 Bitget: `bitget/docs/work_phases/README.md`*
