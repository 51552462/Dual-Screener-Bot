# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **OPS-LIQ-TG-01** (텔레그램 `[LIQ_BAND]` 패널) |
| **status** | **`CLOSED`** / `SUB_DONE` · 신규 코드 구현 없음 |
| **직전** | Claude OK · DoD 7항 일치 · 앵커 `SYNC-2026-08-20-C` |
| **앵커** | `SYNC-2026-08-20-C` |

---

## 디렉터 — 지금 할 일 (배포·관측)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

→ 다음 **19:30** North Star 일보에서 `[LIQ_BAND]` 패널 육안 확인.

### 메모

- Phase1 VERDICT**(B) 관측연장** 유지 · Phase2 자동 착수 없음
- `PHASE2_CANDIDATE` 뜨면 → **새 Claude 세션**에서 OPS-LIQ-FORK-01 갈림길(A′/B/C) 재소집 (이번 OK ≠ Phase2 승인)
- **update_factory**: health RED면 **자동 data-refresh 1회** 후 재검사 (수동 2줄 불필요). 로컬 커밋·푸시 후 VPS 반영
- 신규 Alpha Handoff: **없음**

### 금지

- LIQUIDITY 임계·잡주 완화·자동 Phase2

---

## North Star SSOT (고정)

| 항목 | 값 |
|------|-----|
| **SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **LIQ_BAND 이력** | `factory_data_dir()/liq_band_history.json` (원장 비접촉) |
| **로컬** | `*.LOCAL_DEV_DO_NOT_USE.json` — **사용 금지** |
