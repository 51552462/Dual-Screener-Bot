# Bitget 작업 Phase 문서 — Claude Pro + Cursor + 로컬

> **갱신**: 2026-08-01 — 루트 `docs/work_phases/` 구조를 Bitget에 **미러** · institutional audit P0/P1/P2 반영  
> **범위**: `bitget/` 하위만 · 루트 주식 코드 **수정 금지**

---

## 문서 역할

| 문서 | 출처 | 역할 |
|------|------|------|
| `bitget/docs/claude_project/` | Cursor 카테고리 분해 | 구조·SSOT·경계·상수 (Bitget 전용) |
| **`bitget/docs/work_phases/` (본 폴더)** | Claude Pro + Cursor | **실행 로드맵·작업 스펙·로컬 현황·진행·검증** |
| `bitget/docs/13_institutional_grade_audit_and_roadmap.md` | Cursor/Gemini 감사 | P0/P1/P2 상세 |
| **`00_마스터_로드맵.md`** | Bitget 헌법 | **모든 창 첫 읽기** (주식 `00_마스터_로드맵`과 별개) |

### 루트 `docs/work_phases/` 와의 관계

| 루트 (KR/US) | Bitget (본 폴더) |
|--------------|------------------|
| 묶음A: performance_budget_governor | 묶음A: portfolio NAV MDD·leverage·tail fund |
| 묶음B: GP·deathmatch KR/US | 묶음B: deathmatch key·alloc·walk-forward |
| 묶음C: sector Markov | 묶음C: bad tick·funding·correlation cap |
| 묶음D: AI CIO | 묶음D: LLM JSON·human approval·cost |

**독립 진행** — sub-phase ID 충돌 없음 (Bitget는 A-1부터 새 시퀀스)

---

## 파일 목록

| 파일 | 내용 |
|------|------|
| `00_전체현황판.md` | Phase 상태·SSOT 용어집·의존성 체크 |
| `01_묶음A_작업지시서.md` | A-1~A-5 · P0/P1 리스크 |
| `02_묶음B_작업지시서.md` | B-1~B-4 · 진화/ deathmatch |
| `03_묶음C_작업지시서.md` | C-1~C-3 · 데이터/실행품질 |
| `04_묶음D_작업지시서.md` | D-1~D-3 · AI/거버넌스 |
| `05_진행로그.md` | Phase 간 **유일한 연결고리** |
| `06_검증체크리스트_및_실패기록.md` | 3단계 완료·롤백 원칙 |
| `07_듀얼AI_협업루프.md` | Claude↔Cursor 6단계 SSOT |
| `08_디렉터_중계_가이드.md` | 디렉터 복붙 템플릿 |
| `10_작업함_프로토콜.md` | INBOX/OUTBOX · 연속 작업 SSOT |
| `NEXT_ACTION.md` | 지금 누가/뭘 (AI용) |
| **`NEXT_STEP.md`** | **다음 한 걸음** (디렉터·AI 공용) |
| **`09_디렉터_쉬운요약.md`** | **비개발자용** 작업 끝 요약 (신호등·체크리스트) |
| **`11_협업_효율_점검.md`** | Claude↔Cursor 루프 효율·개선 |
| `CLAUDE_TO_CURSOR.md` | Claude → Cursor Handoff INBOX |
| `CURSOR_TO_CLAUDE.md` | Cursor → Claude 검증 OUTBOX |

---

## sub-phase 총览

```
A-1→A-2→A-3→A-4→A-5 → B-1→B-2→B-3→B-4 → C-1→C-2→C-3 → D-1→D-2→D-3
```

**현재 (2026-08-01)**: Phase 0–8 구현 완료 · **work_phases A-1~ 미착수** · Claude Handoff 대기

---

## Claude Pro — 지금부터 이렇게

**연속 작업이 끊기면** → `10_작업함_프로토콜.md` + **`NEXT_STEP.md`** + **`09_디렉터_쉬운요약.md`** 먼저.

1. Knowledge: `NEXT_STEP`, `09`, `NEXT_ACTION`, `CLAUDE_TO_CURSOR`, `CURSOR_TO_CLAUDE`, `05`, `00`, `10`, `08`, `11`, `01`, `bitget/docs/claude_project/CAT-*`
2. **Claude에게 한 줄**: `NEXT_ACTION.md` status대로
3. **Cursor에게 한 줄**: 「`CLAUDE_TO_CURSOR.md` 구현만, 끝에 CURSOR_TO_CLAUDE·05·00·NEXT_ACTION 갱신」

---

## Claude Pro Knowledge 권장

**묶음A**: `@CAT-F` `@CAT-N` `@CAT-MAP` + `01` + `05` + `06` + `00` + `07` + `08`  
**묶음B**: `@CAT-H` `@CAT-F` + `02` + …  
**묶음C**: `@CAT-C` `@CAT-E` `@CAT-B` + `03` + …  
**묶음D**: `@CAT-M` `@CAT-K` `@CAT-J` + `04` + …  
**진단**: `@CAT-Q` + `13_institutional_grade_audit_and_roadmap.md`

---

## 작업 순서 (= `07_듀얼AI_협업루프` 6단계)

1. Claude Pro → sub-phase Handoff (코드 X)  
2. 디렉터 → Cursor에 Handoff 붙여넣기  
3. Cursor → 구현 + `05`·`00` 갱신 + 3줄 요약  
4. 디렉터 → Claude에 검증 요청  
5. Claude Pro → OK / 수정 spec  
6. (2~4주 후) → `06` 효과 검증

**완료 정의**: Claude OK = 1단계 · **진짜 Done = `06` 3단계 전부**
