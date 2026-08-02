# CAT-HANDOFF · Claude → Cursor Prompt 템플릿 (Bitget)

> **Tier T3** — 설계 완료 후 Cursor에 넘길 때 @멘션.  
> **실행 INBOX**: `bitget/docs/work_phases/CLAUDE_TO_CURSOR.md`

---

## Claude → Cursor (표준)

```markdown
## [CAT-X] {카테고리명} — {한 줄 목표}

### sub-phase ID
{A-1 | B-2 | …}

### SSOT (변경 금지 unless noted)
- 파일: `bitget/{path}`
- config: `{KEY}` (bitget_system_config.sqlite / config_kv)

### 변경 Spec
- 함수/정책: `{name}(...)`
- 수식: `{latex or plain}`
- SPOT/FUT: {공통 | SPOT only | FUT only | 분기 조건}

### Config 변경 (있으면)
| KEY | old | new | default |
|-----|-----|-----|---------|

### 인접 CAT 영향
- CAT-Y: {읽기만 | 없음 | Adapter 필요}

### 롤백 조건
- {metric/flag revert}

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/{hint}`

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` 해당 sub 섹션
- `bitget/docs/work_phases/00_전체현황판.md` Phase·SSOT
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

### 위험도
- {🟢|🟡|🔴|🔴 Critical}
```

---

## Cursor → Claude (충돌 보고)

> **실행 OUTBOX**: `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`

```markdown
## [CAT-X] 구현 충돌

### Claude spec (요약)
{1–3줄}

### 로컬 SSOT
- `bitget/{file:line}` — {현재 동작}

### 충돌
{왜 spec대로 불가}

### 제안
- [ ] Adapter: {설명}
- [ ] Spec 수정: {제안}
- [ ] 범위 축소: {제안}
```

---

## 디렉터 → Claude (작업 시작)

```markdown
CAT: {X}
sub-phase: {A-1}
목표: {한 문장}
제약: {SPOT/FUT, ENABLE_REAL_EXECUTION, 방어계층 여부}
참고: @CAT-{X} @CAT-MAP
```

---

## 토큰 절약 팁

- Handoff에 **코드 블록 30줄 초과 금지**
- 수식은 핵심 1개만
- config는 diff 테이블만
- Cursor가 읽을 파일 경로는 **최대 5개** (bitget/ prefix)
