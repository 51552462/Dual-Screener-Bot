# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **TRACK-A-BUGFIX-BATCH-01** |
| **status** | **`WAIT_DIRECTOR`** · Claude OK · 배포 후 US 진입 `entry_regime` 실측 |
| **직전** | 항목1·3·4 CLOSED · 항목2 코드 OK |
| **앵커** | `SYNC-2026-09-05-BUGFIX-OK` |

---

## 디렉터 — 지금 할 일

1. VPS (푸시 확인 후):

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

2. 이후 신규 진입 1건(US 우선)에서 `entry_regime` ≠ UNKNOWN 이면 항목2 CLOSED
3. V-2: **9/6+** `--iv-observation --dry-run`

### 금지

- LOCKDOWN 우회 · `sim_kelly_invest` 값 변경 · `invest_amount` 환율 곱 · Phase 2
