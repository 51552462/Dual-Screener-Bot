# NEXT_ACTION — 지금 누가 / 뭘 (양쪽 첫 읽기)

> **갱신**: Cursor 세션 종료 또는 Claude OK/Handoff 작성 직후  
> **규칙**: 채팅 말고 **이 파일 status**가 SSOT

---

| 필드 | 값 |
|------|-----|
| **sub-phase** | A-3 (국면 쿼터) + A-1-R1 (재검증) |
| **status** | `WAIT_CLAUDE_OK` |
| **다음 담당** | **Claude Pro** — `CURSOR_TO_CLAUDE.md` 검증 |
| **디렉터** | 05·CURSOR_TO_CLAUDE를 Claude Knowledge에 올리고 **한 줄**만 전송 (아래) |

---

## 디렉터 → Claude (한 줄)

```
CURSOR_TO_CLAUDE.md + 05 A-3·A-1-R1 섹션 검증. OK/수정 spec을 파일에. 다음 Handoff는 A-3 try_add 통합테스트 또는 A-4.
```

---

## 대기 중인 결정

| 항목 | 상태 |
|------|------|
| A-3 Claude OK | ❌ |
| A-1-R1 Claude OK | ❌ |
| A-3 try_add gate #6 통합 테스트 | Handoff **미작성** (Claude 다음) |
| A-4 Handoff | ❌ (A-3 OK 후) |

---

## 직전 완료 (Cursor)

- A-3: `performance_budget_governor` 국면×mult 곱연산
- A-1-R1: Kelly+Quota falsy read 수정, 19 tests passed
- **미완**: try_add 9번째/11번째 **진입 거부** 통합 테스트

---

## status 전이 (참고)

```
WAIT_CURSOR_IMPL → (Cursor 끝) → WAIT_CLAUDE_OK
WAIT_CLAUDE_OK → (Claude OK) → WAIT_CLAUDE_HANDOFF 또는 SUB_DONE
WAIT_CLAUDE_HANDOFF → (Claude Handoff 파일 작성) → WAIT_CURSOR_IMPL
```
