# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | 묶음A (A-1~A-5) |
| **status** | `WAIT_DIRECTOR` (paper 배포) |

---

## 지금 담당: 디렉터

**Critical 승인 5건** — **완료 2026-08-02** ✅

### 다음 액션

1. **paper 배포** — A-1~A-5를 24/7 스캔 사이클에 연결
   - A-5: 배포 직후 `meta_sync` → `config write rejected` 로그 확인
2. **2~4주 후** — `06_검증체크리스트_및_실패기록.md` 효과 기록표 채우기
3. **판정** — 유지 / 롤백 / 추가조정 (NAV MDD 악화 → 무조건 롤백)
4. **통과 시** — `00` Phase 9 완료 → B-1 Handoff (Claude Pro **새 창**)

### 롤백 킬스위치

| sub | env/config |
|-----|------------|
| A-1 | `PORTFOLIO_MDD_BREAKER_ENABLED=false` |
| A-2 | `TAIL_FUND_CONSUMPTION_ENABLED=false` |
| A-4 | `GROSS_NOTIONAL_CAP_ENABLED=false` |
| A-5 | `CONFIG_WRITE_VALIDATION_ENABLED=false` |
