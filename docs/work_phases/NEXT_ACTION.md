# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **KR-LOCKDOWN-STALL-THAW-01** |
| **status** | **Claude OK: 2026-09-21** · 커밋·푸시 후 배포 |
| **직전** | 장치 설치 승인 · **지금 무장 아님** |
| **앵커** | `SYNC-2026-09-21-STALL-THAW-OK` |

---

## 디렉터 — 지금 할 일

푸시 완료 후 VPS:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

배포 후에도 `KR_LOCKDOWN_THAW_ARMED`는 **0**. 슬롯을 열지는 **별도 결정**.

## 금지

- 배포 직후 ARMED=1
- MDD 캡 · 층 통합 · bitget · LADDER-01 혼용
