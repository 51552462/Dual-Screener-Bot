# 작업 Phase 문서 — 통합본 (Claude Pro + Cursor + 로컬)

> ⛓ **멀티 창 시작점** → [`00_SESSION_SYNC.md`](00_SESSION_SYNC.md) ← **가장 먼저**  
> **갱신**: 2026-08-09

---

## 문서 역할

| 문서 | 출처 | 역할 |
|------|------|------|
| **`17_Cursor_세션_부팅_가이드.md`** | Cursor | **첫 메시지·모드 선택 · `.cursor/rules/` 인덱스** |
| **`.cursor/rules/`** | Cursor | **자동 적용 규칙** (멀티창·Handoff·트랙·RP-1·IV) |
| **`00_SESSION_SYNC.md`** | Cursor+Claude | **멀티 채널 앵커 · 현재 포커스 · 부팅 문구** |
| `docs/claude_project/` | Cursor 카테고리 분해 | 구조·SSOT·경계·상수 (bitget 제외) |
| **`docs/work_phases/` (본 폴더)** | Claude Pro + Cursor | **실행 로드맵·작업 스펙·로컬 현황·진행·검증** |
| **`docs/independent_verification/`** | Claude Pro + Cursor + 디렉터 | **독립 검증 SSOT** — 자기채점 방지·IV 체크리스트·V-* 로드맵 |
| **`00_마스터_로드맵.md`** | Claude Pro + Cursor | **헌법 (MDD 10%)** |

### Desktop `클로드 프로/` 와의 관계

| Desktop 파일 | 레포 통합본 |
|--------------|-------------|
| `01~04_기존묶음*_작업지시서.md` | `01~04_묶음*_작업지시서.md` 내 **「기존묶음 원문」** 섹션 + sub-phase |
| `05_진행로그.md` | `05_진행로그.md` (**2026-07-30 A-1·A-2 이관**) |
| `00-1_전체현황판.md` | `00_전체현황판.md` |
| `08_검증체크리스트_*.md` | `06_검증체크리스트_및_실패기록.md` |
| `07_마스터프로젝트_*` | D-4末尾·마스터 역할 요약 (07 전문은 Desktop 유지 가능) |

---

## 파일 목록

| 파일 | 내용 |
|------|------|
| **`00_SESSION_SYNC.md`** | **멀티 창 앵커** — 부팅 문구·충돌 규칙·§3 스냅샷 |
| `00_전체현황판.md` | Phase 상태·SSOT 용어집·의존성 체크 |
| `01_묶음A_작업지시서.md` | A-1~A-5 + **로컬 A-1·A-2 구현·A-3 부분** |
| `02_묶음B_작업지시서.md` | B-1~B-4 + deathmatch 90/120d 현행 |
| `03_묶음C_작업지시서.md` | C-1~C-3 + bitget Go/No-Go |
| `04_묶음D_작업지시서.md` | D-1~D-3 + 224 files 분할 |
| `05_진행로그.md` | Phase 간 **유일한 연결고리** |
| `06_검증체크리스트_및_실패기록.md` | 3단계 완료·롤백 원칙 |
| `07_듀얼AI_협업루프.md` | **Claude↔Cursor 6단계 SSOT** · Claude Pro 시작 멘트 |
| `08_디렉터_중계_가이드.md` | **디렉터 복붙 템플릿** · **Cursor 새창 시작 멘트** |
| `10_작업함_프로토콜.md` | **INBOX/OUTBOX** · 연속 작업 SSOT |
| `NEXT_ACTION.md` | **디렉터 지금 할 일** SSOT (`NEXT_STEP`은 레거시) |
| **`09_디렉터_쉬운요약.md`** | 비개발 한 장 요약 |
| **`11_협업_효율_점검.md`** | COLLAB_HEALTH |
| `12_듀얼북극성_진행장부_및_상품화.md` | Track A/B 비교 |
| `CLAUDE_TO_CURSOR.md` | Claude → Cursor Handoff INBOX |
| `CURSOR_TO_CLAUDE.md` | Cursor → Claude 검증 OUTBOX |
| **`ARCHITECT_MIRROR.md`** | Claude 능동 구조 제안·Mirror 로그 (bitget 대칭) |
| **`../independent_verification/`** | **독립 검증** — IV-01~25 · V-* · L0~L3 헌법 (`00_README`) |

---

## 포함 관계 (CAT vs work_phases)

| 내용 | claude_project | work_phases |
|------|----------------|-------------|
| Kelly chain, try_add, regime | ✅ CAT-F, G | A-2, A-3, A-4 |
| HTC, OOS, deathmatch 개념 | ✅ CAT-H, F | B-1~B-3 |
| sector, spillover | ✅ CAT-C | C-1 |
| LLM 비침투 | ✅ CAT-M | D-1 경계 |
| MDD 소진율 tier | ❌ | A-1 |
| S5, WEIGHT_S5 | ❌ | A-5, B-4 |
| 국면별 OPEN | ❌ | A-3 |
| 방향별 히스테리시스 | ❌ | A-4 (+ RL 충돌) |
| GP 3계열·λ 마찰 | ❌ | B-1, B-2 |
| 45d / -8% 킬 | ❌ | B-3 |
| bitget 6th shadow | ❌ | C-3 |
| AI CIO JSON·DSR | ❌ | D-1~D-3 |
| **로컬 구현 현황** | ❌ | **각 묶음 + 05** |
| **config 키 SSOT 전체** | ❌ | **A-1 + 00** |
| **3단계 검증·롤백** | ❌ | **06** |
| **독립 검증 (자기채점 방지)** | ❌ | **`../independent_verification/`** |
| **듀얼 AI 6단계 루프** | ❌ | **07** + **01~04 각 파일** |

---

## sub-phase 총览

```
A-1→A-2→A-3→A-4→A-5 → B-1→B-2→B-3→B-4 → C-1→C-2→[C-3] → D-1→D-2→D-3
```

**현재 (2026-08-01)**: A-1~A-3 구현 1단계 · **Claude OK 대기** · A-4~D 미착수

---

## Claude Pro — 지금부터 이렇게 (요약)

**연속 작업이 끊기면** → `10_작업함_프로토콜.md` + **`NEXT_STEP.md`** + **`09_디렉터_쉬운요약.md`** 먼저.

1. Knowledge: `NEXT_STEP`, `09`, `NEXT_ACTION`, `CURSOR_TO_CLAUDE`, **`ARCHITECT_MIRROR`**, `CLAUDE_TO_CURSOR`, `05`, `00`, `10`, `08`, `11`, `01` …
2. **Claude에게 한 줄**: `NEXT_ACTION.md` status대로 — 보통 「CURSOR_TO_CLAUDE.md 검증, OK면 CLAUDE_TO_CURSOR.md에 Handoff」
3. **Cursor에게 한 줄**: 「CLAUDE_TO_CURSOR.md 구현만, 끝에 CURSOR_TO_CLAUDE·05·00·NEXT_ACTION 갱신」

상세·멘트: `10_작업함_프로토콜.md` · 구 템플릿: `08_디렉터_중계_가이드.md`

---

## Claude Pro 프로젝트 Knowledge 권장 (상세)

**Phase A 프로젝트**: `@CAT-F` `@CAT-G` `@CAT-MAP` + `01_묶음A` + `05` + `06` + `00` + **`07`** + **`08`**  
**Phase B**: `@CAT-H` `@CAT-F` + `02` + `05` + `06` + `00` + **`07`**  
**Phase C**: `@CAT-C` `@CAT-B` + `03` + `05` + `06` + `00` + **`07`**  
**Phase D**: `@CAT-M` `@CAT-K` `@CAT-J` `@CAT-Q` + `04` + `05` + `06` + **`07`**  
**마스터**: `00_마스터_로드맵` + `00_전체현황판` + `07` + 4× `05` 사본

---

## 작업 순서 (= `07_듀얼AI_협업루프` 6단계)

1. **Claude Pro** → sub-phase Handoff 프롬프트 (코드 X)  
2. **디렉터** → Cursor에 Handoff 붙여넣기  
3. **Cursor** → 구현 + **`05`·`00` 갱신** + 3줄 요약  
4. **디렉터** → Claude에 검증 요청  
5. **Claude Pro** → OK / 수정 spec (OK 전 Done 금지)  
6. (2~4주 후) → **`06` 효과 검증**

**완료 정의**: Claude OK = 1단계 · **진짜 Done = `06` 3단계 전부**
