# CAT-HANDOFF · Claude → Cursor Prompt 템플릿

> **Tier T3** — 설계 완료 후 Cursor에 넘길 때 @멘션.

---

## Claude → Cursor (표준)

```markdown
## [CAT-X] {카테고리명} — {한 줄 목표}

### SSOT (변경 금지 unless noted)
- 파일: `{path}`
- config: `{KEY}`

### 변경 Spec
- 함수/정책: `{name}(...)`
- 수식: `{latex or plain}`
- KR/US: {공통 | KR only | US only | 분기 조건}

### Config 변경 (있으면)
| KEY | old | new | default |
|-----|-----|-----|---------|

### 인접 CAT 영향
- CAT-Y: {읽기만 | 없음 | Adapter 필요}

### 롤백 조건
- {metric/flag revert}

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `{test path hint}`

### 위험도
- {🟢|🟡|🔴|🔴 Critical}
```

---

## Cursor → Claude (충돌 보고)

```markdown
## [CAT-X] 구현 충돌

### Claude spec (요약)
{1–3줄}

### 로컬 SSOT
- `{file:line}` — {현재 동작}

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
목표: {한 문장}
제약: {KR/US, 방어계층 여부}
참고: @CAT-{X} @CAT-MAP
```

---

## 토큰 절약 팁

- Handoff에 **코드 블록 30줄 초과 금지**
- 수식은 핵심 1개만
- config는 diff 테이블만
- Cursor가 읽을 파일 경로는 **최대 5개**
