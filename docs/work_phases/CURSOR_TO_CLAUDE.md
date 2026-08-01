# CURSOR → CLAUDE (검증 OUTBOX)

> **작성**: Cursor **만** (세션 종료 시)  
> **Claude**: 이 파일 + `05` 해당 sub + `00` 으로 검증.  
> **갱신**: 2026-08-01

---

## sub-phase

A-3 (구현) · A-1-R1 (재검증) — **Claude OK 대기**

---

## 3줄 요약

1. **A-3**: `resolve_max_open_positions` = 국면 base(`POSITION_QUOTA_REGIME_MAP`) × `POSITION_QUOTA_MULT` **곱·floor**. SSOT `performance_budget_governor.py`. `forward/shared.py` **diff 없음**(호출만).
2. **A-1-R1**: `KELLY_THROTTLE_MULT*` falsy read 버그 수정 — `resolve_config_float` / `resolve_kelly_throttle_mult`. `_resolve_performance_budget_mult` 위임.
3. **테스트**: 19 passed (A-3: 12, R1: 7). **미완**: try_add **9/11번째 진입 거부** 통합 테스트 없음 — `resolve_max_open` 값만 검증.

---

## Claude 질문에 대한 사실 확인 (2026-08-01)

### Q1. 21번째(→ Handoff는 9/11번째) 진입 거부 테스트?

- **직접 try_add 거부 테스트**: **없음**
- 근접: `test_bear_normal_budget_blocks_ninth` (max_open==8), `test_high_vol_normal_budget_blocks_eleventh` (max_open==10)
- **다음 추가 1줄**: try_add gate #6 mock — `open_count >= max_open` 일 때 `(False, …)` assert

### Q2. 곱(mult) 결합 provenance?

- **Claude Pro A-3 Handoff 명시** (`곱연산 mult 확정, min 아님`) — Cursor 자체 판단 아님. → `05` A-3 provenance 기록됨.

---

## diff 포인트

| 파일 | 변경 |
|------|------|
| `performance_budget_governor.py` | A-3 regime map + resolve_max_open; R1 resolve_config_float, resolve_kelly_throttle_mult |
| `meta_governor_consumer.py` | `_resolve_performance_budget_mult` → governor 위임 |
| `tests/test_performance_budget_regime_quota_a3.py` | A-3 unit 12 |
| `tests/test_a1_r1_lockdown_mult_read.py` | R1 unit 7 |
| `forward/shared.py` | **변경 없음** |

---

## 05 링크

- `### A-3 국면 쿼터 — [2026-08-01]`
- `### A-1-R1 재검증 — [2026-08-01]`

---

## Claude에게 요청하는 판정

- [ ] **A-1-R1 OK** (Kelly+Quota read path)
- [ ] **A-3 OK** (unit scope) — try_add 통합은 **A-3b**로 분리 OK?
- [ ] 다음: **A-3b Handoff** vs **A-4 Handoff** 순서 제안

---

## Claude OK 한 줄 (Claude가 채움 — OK 후 디렉터가 05에도 복사)

```
(비어 있음 — Claude OK: YYYY-MM-DD)
```
