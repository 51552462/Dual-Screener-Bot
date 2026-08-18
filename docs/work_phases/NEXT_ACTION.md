# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | — (NS-DIAG-DASH-01 **Claude OK · CLOSED**) |
| **status** | `SUB_DONE` · 운영 **OBS-HOLD** · Alpha **금지** · 신규 Handoff **없음** |
| **직전** | NS-DIAG-DASH-01 **Claude OK** · CAT-J · Critical 아님 |
| **앵커** | `SYNC-2026-08-19-E` |

---

## 디렉터 — 지금 할 일 (1줄)

VPS에 쉬운판 반영:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

→ 다음 **19:30** `[쉬운판]`이 🟢/🔴/🟡/⬜ 4칸인지 육안.  
그다음: **관측유지** (n≥20 재소집 전 Alpha/mega_trend/목표하향/임계 완화 **금지**).

### 재소집 트리거 (고정)

| 항목 | 값 |
|------|-----|
| **트리거** | VPS daily **n≥20** → 갈림길 재소집 |
| **텔레그램** | 19:30 · `[쉬운판]` + `[OBS_HOLD]` + 복붙 |
| **금지** | n&lt;20 페이스·CAGR 확정 · 로컬 원장 근거 · LIQUIDITY 임계 완화 · OPEN=0=고장 단정 |

---

## North Star SSOT (고정)

| 항목 | 값 |
|------|-----|
| **SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **로컬** | `*.LOCAL_DEV_DO_NOT_USE.json` — **사용 금지** |

---

## 근처놓침 레버 — 전원 소진·동결

BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1-REDUCED — 규칙1 재접촉 금지
