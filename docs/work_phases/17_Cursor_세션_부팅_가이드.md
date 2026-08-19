# 17 · Cursor 세션 부팅 가이드 (SSOT)

> **Claude Pro** = 프로젝트 Instructions + Knowledge 파일  
> **Cursor Pro** = `.cursorrules` + **`.cursor/rules/*.mdc`** (자동) + **첫 메시지**(세션 목적)  
> **갱신**: 2026-08-12

---

## 1. 무엇이 자동이고, 무엇을 붙여넣나?

| 계층 | 위치 | 언제 적용 | 내용 |
|------|------|-----------|------|
| **항상** | `.cursorrules` | 모든 채팅 | 듀얼 AI · targeted diff · 방어 · Handoff 필수 |
| **항상** | `.cursor/rules/00-core-session-ssot.mdc` | 모든 채팅 | SESSION_SYNC 읽기 · 멀티창 · status · 금지 |
| **항상** | `.cursor/rules/03-dual-ai-handoff.mdc` | 모든 채팅 | 작업함 · Claude OK |
| **항상** | `.cursor/rules/04-telegram-ops-hooks.mdc` | 모든 채팅 | DEPLOY_WATCH · IV_OBS |
| **항상** | `.cursor/rules/01-engineering-efficiency.mdc` | 모든 채팅 | 브리핑 · targeted diff · 3회 중단 |
| **항상** | `.cursor/rules/05-risk-and-verification-layers.mdc` | 모든 채팅 | MDD/DEFCON · L0~L3 과신 금지 |
| **조건부** | `.cursor/rules/06-rp1-vps-lab.mdc` | RP-1 파일 편집 시 | metrics-only · VPS |
| **조건부** | `.cursor/rules/02-track-bitget-coin.mdc` | `bitget/**` 편집 시 | Track B 격리 |
| **조건부** | `.cursor/rules/07-independent-verification.mdc` | IV 문서 편집 시 | V-* 헌법 |
| **매 세션** | **첫 메시지** (아래 §3) | 새 채팅 열 때 | 모드·sub-phase ID |

전체 인덱스: `.cursor/rules/README.md`

**첫 메시지 없이도** SSOT 읽기·금지 규칙은 동작한다.  
**첫 메시지**는 「이번에 구현 vs 조사 vs 배포만」을 박는 용도.

**디렉터 VPS 배포 헷갈림** → 항상 [`18_디렉터_VPS_원클릭.md`](18_디렉터_VPS_원클릭.md)  
(평소 = `sudo bash ./update_factory.sh` 한 줄 · 임시 진단만 `NEXT_ACTION`).

---

## 2. 새 채팅 선택 흐름

```
새 Cursor 채팅
  → 00_SESSION_SYNC §3 + NEXT_ACTION status 확인
  → WAIT_CURSOR_IMPL     → §3-A 구현
  → 배포·VPS만           → §3-C Ops
  → 조사·Audit만         → §3-D
  → V-* (IV)             → §3-E
  → [DEPLOY_WATCH]/[IV_OBS] → §3-F 텔레그램
  → bitget/ 작업         → §3-B + Track B 규칙 자동
  → 모름                 → §3-G 상태 점검
  → 세션 끝              → 05 · §3 · NEXT_ACTION · CURSOR_TO_CLAUDE
```

전문·변형: `08_디렉터_중계_가이드.md` §「Cursor 새창 시작 멘트」

---

## 3. 복붙용 부팅 문구

### 3-A. Track A — 구현 (가장 많음)

```text
Track A (KR/US) 구현 세션.

1) docs/work_phases/00_SESSION_SYNC.md §3
2) docs/work_phases/NEXT_ACTION.md — WAIT_CURSOR_IMPL 확인. 아니면 멈추고 보고.
3) docs/work_phases/CLAUDE_TO_CURSOR.md = 유일한 spec.
4) .cursor/rules + .cursorrules 준수. bitget/ 수정 금지.

sub-phase: [ID 붙여넣기]

세션 종료: 05 · 00_SESSION_SYNC §3 · NEXT_ACTION · CURSOR_TO_CLAUDE · 3줄 요약.
```

### 3-B. Track B — Bitget 구현

```text
Track B (Bitget) 구현 세션.

1) bitget/docs/work_phases/NEXT_ACTION.md
2) bitget/docs/work_phases/CLAUDE_TO_CURSOR.md = 유일한 spec
3) bitget/ 하위만 수정. 루트 KR/US 수정 금지.

sub-phase: [ID]

세션 종료: bitget 05 · 00 · CURSOR_TO_CLAUDE · NEXT_ACTION · NEXT_STEP · 09 쉬운요약 · 3줄.
```

### 3-C. Ops — 배포·관측만

```text
Track A — OPS만. Alpha/Handoff 구현 금지.

VPS 배포 SSOT: docs/work_phases/18_디렉터_VPS_원클릭.md
평소: cd .../Dual-Screener-Bot && sudo bash ./update_factory.sh
임시 진단 문구는 NEXT_ACTION.md 만.

코드 diff 필요 시 멈추고 Handoff 요청만 보고.
결과: CURSOR_TO_CLAUDE.md 짧게.
```

### 3-D. 조사·Reality Audit

```text
Track A — 조사만. 구현·리팩터 금지.

CURSOR_TO_CLAUDE.md 최신 OUTBOX 기준 read-only 실측.
산출: OUTBOX 갱신 + 05 한 줄. 코드 diff 없음.
```

### 3-E. Independent Verification (V-*)

```text
Track A — IV (V-*) 구현.

1) docs/independent_verification/NEXT_ACTION.md
2) docs/independent_verification/CLAUDE_TO_CURSOR.md = spec
3) 01_자기채점_위험_헌법.md — L0~L3 위반 금지
RP-1·LIVE 승격 연동 금지.
```

### 3-F. 텔레그램

```text
[DEPLOY_WATCH] 또는 [IV_OBS] 또는 [OBS_HOLD](북극성 일보) 수신 세션.

---CURSOR--- 아래 블록 전체가 첫 메시지.
cursor_action / cursor_prompt 따름. 구현은 action이 요구할 때만.
[OBS_HOLD] OBSERVE_HOLD=관측만 · RECALL_FORK=OUTBOX+Claude 중계(코드 금지).
---CLAUDE--- 블록은 Claude Pro 창에 디렉터가 붙여넣기.
```

### 3-G. 상태 점검 (뭘 할지 모를 때)

```text
Track A — 상태 점검만. 구현 금지.

읽기: 00_SESSION_SYNC §3 · NEXT_ACTION · CLAUDE_TO_CURSOR · CURSOR_TO_CLAUDE
출력: status · 다음 담당 · sub-phase ID 1개 또는 "Handoff 대기"
코드 수정 금지.
```

---

## 4. Claude Pro 대응 (참고)

| Cursor | Claude Pro |
|--------|------------|
| `.cursorrules` + `.cursor/rules/` | Project Instructions |
| `@00_SESSION_SYNC.md` | Knowledge 파일 |
| 첫 메시지 sub-phase | 첫 메시지 SRV/Handoff |

Claude 부팅: `00_SESSION_SYNC.md` §4 · `16_SRV01_Claude_붙여넣기초안.md`

---

## 5. 쓰지 말 것

| 나쁜 시작 | 이유 |
|-----------|------|
| "전체 점검하고 다 고쳐줘" | sub-phase 1개 위반 |
| "Claude가 말한 대로: …" (장문) | `CLAUDE_TO_CURSOR.md`와 불일치 |
| Track A에서 bitget 수정 | 트랙 오염 |
| Handoff 없이 "다음 단계 구현" | 작업함 위반 |

---

*규칙 파일: `.cursor/rules/` · 루트 규칙: `.cursorrules` · 멀티창 앵커: `00_SESSION_SYNC.md`*
