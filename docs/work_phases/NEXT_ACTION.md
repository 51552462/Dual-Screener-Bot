# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **V-2-WFBLOCK-01** |
| **status** | **CLOSED** · Claude OK 2026-09-10 · 1주 오탐은 추적만 |
| **직전** | 코드/테스트 완전 승인 · 커밋·푸시 후 배포 |
| **앵커** | `SYNC-2026-09-10-WFBLOCK-OK` |

---

## 디렉터 — 지금 할 일

1. **배포** (푸시 완료 후)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

2. 오늘 저녁 `[IV_OBS]` 육안: **V-2 심판 = ON(작동 중)** · readiness `BLOCK_ALREADY_ON`
3. **1주 오탐 관측** (즉시 확인 아님) — 정상 후보가 잘못 막히는지. 워치독이 추적.

### 롤백

```bash
# VPS .env
WALK_FORWARD_PROMOTION_BLOCK_ENABLED=0
```

### 병행 (이번 스코프 아님)

- FAMILY-SLEEVE DoD#4: 다음 데스매치 후 12키 0.25
- DSR / V-2b **금지** (V-2-DSR-01 · V-2B-SNAPSHOT-01 backlog)

### 금지

- MDD/LOCKDOWN/F-GATE 변경
- `evaluate_ledger_deflated_sharpe` 배선 · `OOS_DSR_MIN` 변경
- 승격 완화(조이는 방향만)
