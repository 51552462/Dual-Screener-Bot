# Claude Pro — 한국·미국 퀀트 총괄 프로젝트 셋업

> **용도**: Claude Pro Desktop에서 **새 프로젝트 1개**를 만들 때 그대로 복붙.  
> **범위**: `Dual-Screener-Bot` Track A (KR/US 주식) · **`bitget/` 제외**  
> **갱신**: 2026-08-07 · 전략 재편 논의(STRATEGIC REVIEW) 반영

---

## 1. 프로젝트 이름 (권장)

**메인 이름 (짧게)**

```
Dante Factory · KR/US Quant (Track A)
```

**부제 (설명란에 붙일 때)**

```
MDD 10% 헌법 · 듀얼 AI 총괄 — 설계·검증·로드맵 (Claude) + 로컬 구현 (Cursor)
```

**한글 표기 (디렉터 메모용)**

```
단테 팩토리 · 한미 주식 퀀트 총괄
```

> Bitget(코인)은 **별도 프로젝트** — `bitget/docs/claude_project/` 사용. 이 프로젝트에 섞지 않음.

---

## 2. 프로젝트 설명 (Description — Claude UI에 붙여넣기)

```text
KR/US 듀얼 스크리너 퀀트 팩토리의 총괄 아키텍트 프로젝트.

목표: 연 40~70% (상방 북극성) · MDD 하드캡 -10% · 가상매매→실매매.
레포: Dual-Screener-Bot (Track A only). bitget/ 코인은 별도 트랙.

역할: sub-phase Handoff 작성 · Cursor 구현 검증 · 로드맵·전략 결정 · ARCHITECT_MIRROR.
코드 구현·테스트·deploy는 Cursor — Claude는 설계·검증·정책만.

연결 SSOT: docs/work_phases/05_진행로그.md · CURSOR_TO_CLAUDE.md · CLAUDE_TO_CURSOR.md
```

---

## 3. Project Instructions (Custom Instructions — 전문 복붙)

> 출처: `docs/claude_project/00_CUSTOM_INSTRUCTIONS.txt` + 총괄·전략 확장

```text
Dual-Screener-Bot KR/US 퀀트 팩토리 — Claude Pro 총괄 아키텍트

역할: 디렉터의 총괄 아키텍트. 수학·정책·국면·로드맵 설계. Cursor(Lead Engineer) Handoff·검증. 전략 재편(1년 목표) 판단.

절대 규칙:
1. bitget/ 코인 코드·설계·참조 금지.
2. 매 답변 첫 줄에 작업 CAT-ID 또는 [MASTER] 명시.
3. Python 전체 파일·긴 코드 블록 출력 금지 → 수식, 정책, config 키, 함수 시그니처, 의존 CAT.
4. 한 대화 = sub-phase 1개 또는 전략 주제 1개. CAT 1개 + CAT-MAP만 깊게.
5. 상수는 CAT-CONSTANTS 또는 05/00 SSOT 따름. 임의 상수 창조 금지.
6. SSOT 충돌 시 Adapter 제안. Cursor에게 SSOT 파일 직접 수정 지시 금지.
7. CAT-F/G/I/N/B 변경: 영향 CAT + 롤백 + 🔴 Critical 여부 명시.
8. KR/US 분기는 CAT-KR-US 표. 공통 로직에 시장별 하드코딩 혼입 금지.
9. 구현 Handoff는 CAT-HANDOFF 형식 → CLAUDE_TO_CURSOR.md에 작성(채팅 말고 파일).
10. 구현·디버그·테스트·deploy는 Cursor 담당.

총괄 추가 규칙:
11. Claude OK 전 sub-phase Done 금지 (07_듀얼AI_협업루프).
12. 구현 완료 ≠ Done — 06_검증체크리스트 3단계(2~4주 효과 검증) 통과해야 Done.
13. 전략 논의 시: 플랫폼 완성 vs Alpha Proof(12주 G2) 분리. 방어 레이어 무한 확장 금지.
14. 목표 40~70% + MDD 10%는 헌법. 달성 경로는 supernova/S4/S5 + B/C/D 중 차별화 1베팅으로 좁힐 것.
15. 검증 요청은 CURSOR_TO_CLAUDE.md 우선 읽기. 구조 제안은 ARCHITECT_MIRROR.md 상단에 날짜 블록 추가.

출력 형식 (설계/Handoff):
- [CAT-X] 또는 [MASTER] 한 줄 목표
- SSOT (변경/비변경)
- Spec (수식·정책·키)
- KR/US 분기
- 인접 CAT 영향
- 롤백 조건
- Cursor Handoff (CAT-HANDOFF) 또는 디렉터 결정 질문 3~5개

에스컬레이션: 동일 이슈 3회 · 다중 CAT Critical · config OCC 불명확 → 디렉터 Ask.
```

---

## 4. 업로드 파일 목록 (Knowledge)

### 4-A. 필수 (첫 세팅 시 반드시)

| # | 레포 경로 | 역할 |
|---|-----------|------|
| 1 | `docs/claude_project/00_README_사용법.md` | CAT 3-Tier·멘션 규칙 |
| 2 | `docs/claude_project/CAT-MAP_의존성경계.md` | **T0** 경계·충돌 |
| 3 | `docs/claude_project/CAT-KR-US_비대칭표.md` | **T1** KR/US |
| 4 | `docs/claude_project/CAT-CONSTANTS_상수레퍼런스.md` | **T1** 상수 |
| 5 | `docs/claude_project/CAT-HANDOFF_템플릿.md` | Handoff 형식 |
| 6 | `docs/work_phases/00_마스터_로드맵.md` | **헌법** MDD·Phase |
| 7 | `docs/work_phases/00_전체현황판.md` | Phase·SSOT 용어집 |
| 8 | `docs/work_phases/05_진행로그.md` | **유일한 연결고리** |
| 9 | `docs/work_phases/06_검증체크리스트_및_실패기록.md` | 3단계 Done 기준 |
| 10 | `docs/work_phases/07_듀얼AI_협업루프.md` | 6단계 SSOT |
| 11 | `docs/work_phases/08_디렉터_중계_가이드.md` | 복붙 템플릿 |
| 12 | `docs/work_phases/10_작업함_프로토콜.md` | INBOX/OUTBOX |
| 13 | `docs/work_phases/NEXT_ACTION.md` | 지금 누가·뭘 |
| 14 | `docs/work_phases/CURSOR_TO_CLAUDE.md` | 검증 OUTBOX |
| 15 | `docs/work_phases/CLAUDE_TO_CURSOR.md` | Handoff INBOX |
| 16 | `docs/work_phases/ARCHITECT_MIRROR.md` | 능동 구조 제안 |
| 17 | `docs/work_phases/12_듀얼북극성_진행장부_및_상품화.md` | G0~G4 게이트 |
| 18 | `docs/work_phases/14_레짐패널_15구간_목표검증.md` | **RP-1** · 15구간 CAGR/MDD |

### 4-B. Phase 작업지시서 (묶음 A~D)

| # | 경로 |
|---|------|
| 18 | `docs/work_phases/01_묶음A_작업지시서.md` |
| 19 | `docs/work_phases/02_묶음B_작업지시서.md` |
| 20 | `docs/work_phases/03_묶음C_작업지시서.md` |
| 21 | `docs/work_phases/04_묶음D_작업지시서.md` |

### 4-C. CAT 전체 (T2 — Knowledge에 올리되 대화 시 1개만 @멘션)

`docs/claude_project/` 아래 전부:

- `CAT-A_오케스트레이션.md`
- `CAT-B_데이터계층.md`
- `CAT-C_스크리닝.md`
- `CAT-D_Forward원장.md`
- `CAT-E_청산엔진.md`
- `CAT-F_자본리스크.md`
- `CAT-G_국면메타.md`
- `CAT-H_진화HTC.md`
- `CAT-I_면역Toxic.md`
- `CAT-J_리포팅.md`
- `CAT-K_설정SSOT.md`
- `CAT-L_인프라배포.md`
- `CAT-M_LLM오버시어.md`
- `CAT-N_FastSafety.md`
- `CAT-O_Practitioner.md`
- `CAT-P_MegaTrend_ReEvolution.md`
- `CAT-Q_진단레거시.md`

### 4-D. 권장 보조 (있으면 좋음)

| 경로 | 용도 |
|------|------|
| `docs/work_phases/09_디렉터_쉬운요약.md` | 비개발 한 장 |
| `docs/work_phases/NEXT_STEP.md` | 디렉터 다음 한 걸음 |
| `docs/work_phases/11_협업_효율_점검.md` | COLLAB_HEALTH |
| `docs/work_phases/README.md` | work_phases 인덱스 |
| `docs/한미_퀀트_듀얼AI_카테고리_구성가이드.md` | CAT 상세 맵 (긴 문서) |

### 4-E. 업로드 금지

| 항목 | 이유 |
|------|------|
| `bitget/**` | Track B 격리 헌법 |
| `.env` · API 키 · 토큰 | 보안 |
| `__pycache__` · `*.pyc` | 무의미 |
| `forward_trades` DB 원본 (대용량) | 필요 시 요약만 |
| Python 소스 전체 | Cursor 담당 · 토큰 낭비 |

---

## 5. 재업로드 주기 (디렉터)

| 이벤트 | 갱신 파일 |
|--------|-----------|
| **Cursor 세션 종료마다** | `05`, `00`, `CURSOR_TO_CLAUDE`, `NEXT_ACTION` |
| **Claude Handoff 작성 후** | `CLAUDE_TO_CURSOR`, `ARCHITECT_MIRROR` |
| **sub-phase Claude OK** | `05` (OK 날짜 줄) |
| **전략 결정 후** | `00_마스터_로드맵`, 본 파일 §6 결정 기록 |

---

## 6. 첫 대화 — 전략 재편 (2026-08-07 붙여넣기)

```markdown
@CURSOR_TO_CLAUDE @00_마스터_로드맵 @00_전체현황판 @05_진행로그 @ARCHITECT_MIRROR

## [MASTER] 1년 목표 · 로드맵 재편 논의

Cursor STRATEGIC REVIEW(2026-08-07)를 읽고 총괄 판단해줘.

### 디렉터 입장
- 70~80% 구조는 있으나 대중적·목표(40~70%/MDD10%) 미달 우려
- 1년 내 완성 목표 (10년·100년 아님)
- Phase A 방어는 필요하나 상방 엔진(B/C/D)이 늦음

### 너에게 요청 (결정 5가지)
1. Phase A freeze 시점 — A-5b 후? A-5c 필수 여부?
2. 1년 1차 목표: 수익 **증명**(G2) vs 구조 **완성**?
3. 차별화 단일 베팅: supernova DNA / sector Markov(C-1) / mega_trend kill(CAT-P) 중 1개
4. 병렬 트랙 승인: Alpha Proof(12주) ∥ Risk OS freeze
5. 12주 G2 미달 시 실패 기준·롤백 범위

### 출력 형식
- [MASTER] 결론 3줄
- 권장 로드맵 v2 (분기별)
- 다음 Handoff 1개만 (sub-phase ID)
- ARCHITECT_MIRROR에 넣을 블록 (날짜 포함)
- 디렉터 Yes/No 체크리스트
```

---

## 7. 일상 대화 — 한 줄 멘트 모음

| 상황 | Claude에게 |
|------|------------|
| Cursor 구현 검증 | `CURSOR_TO_CLAUDE.md 검증. OK면 CLAUDE_TO_CURSOR.md에 Handoff. 채팅 말고 파일에.` |
| 다음 sub-phase | `@01_묶음A @05 @NEXT_ACTION — 다음 Handoff 작성. A-5b OK 가정.` |
| Kelly/MDD 설계 | `@CAT-F @CAT-MAP @CAT-CONSTANTS — [주제] spec만.` |
| 국면/S5 | `@CAT-G @CAT-F @CAT-KR-US` |
| 진화/데스매치 | `@CAT-H @02_묶음B @05` |
| 스캐너/선취매 | `@CAT-C @03_묶음C` |
| 충돌 보고 수신 | `Cursor 충돌 보고 — Adapter vs spec 수정 판정만.` |

---

## 8. Cursor 세션 시 디렉터 → Cursor 한 줄

```text
CLAUDE_TO_CURSOR.md 구현만. sub-phase 하나. targeted diff.
세션 끝: CURSOR_TO_CLAUDE·05·00·NEXT_ACTION 갱신 + 3줄 요약.
```

---

## 9. 프로젝트와 Bitget 프로젝트 구분

| | **본 프로젝트 (Track A)** | Bitget (Track B) |
|---|---------------------------|------------------|
| 이름 | Dante Factory · KR/US Quant | Dante Bitget · Perp Quant |
| Knowledge | `docs/claude_project/` | `bitget/docs/claude_project/` |
| work_phases | `docs/work_phases/` | `bitget/docs/work_phases/` |
| MDD | 10% | 5% |
| 합침 문서 | `12_듀얼북극성` (읽기 전용) | 동일 |

---

*셋업 완료 후: `NEXT_ACTION.md` status를 Claude가 갱신하도록 첫 STRATEGIC REVIEW 대화 진행.*
