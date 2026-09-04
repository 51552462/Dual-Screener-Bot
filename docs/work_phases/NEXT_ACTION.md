# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **NAV-HOOK-SILENTFAIL-02** |
| **status** | **`WAIT_DIRECTOR` / DoD #3 관측** · Claude 부분 OK (코드+Step A) · Step B **미승인** |
| **직전** | Claude 재검산 Δ 일치 · INBOX 랜딩 · 커밋·푸시 |
| **앵커** | `SYNC-2026-09-04-NAV-HOOK-02` |

---

## 디렉터 — 지금 할 일

1. (완료 예정) 커밋·푸시 후 VPS `update_factory`로 git과 scp 정렬
2. **다음 실청산 1건** 발생 시 `treasury_state.json` mtime/`updated_at`·nav 갱신 확인 → DoD #3
3. DoD #3 통과 후에만 Step B 별도 세션 요청 (지금 백필 금지)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

### Step A (확정 · 쓰기 없음)

| 시장 | PRE→POST(sim) | Δ | n_closed | HWM |
|------|---------------|---|----------|-----|
| KR | ₩268,649,768 → ₩267,987,800 | −661,968 | 204→212 (+8) | 불변 |
| US | $288,738 → $288,126 | −611 | 139→143 (+4) | 불변 |

mdd: 표 11.04%=path-max · 엔드포인트 ≈11.02% (실 governor는 엔드포인트)

### 금지

- Step B / `overwrite_market_state` / HWM 덮어쓰기
- DoD #3 전 완전 Done 처리
- LOCKDOWN 우회
