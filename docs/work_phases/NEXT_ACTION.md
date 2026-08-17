# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **NS-OBS-TG-01** (Ops-lite · 일보 OBS_HOLD 복붙) |
| **status** | `WAIT_CLAUDE_OK` · 운영상태 `OBS-HOLD` 유지 |
| **직전** | FWD-OBS-HOLD-01 **Claude OK** |
| **앵커** | `SYNC-2026-08-17-Z` |

---

## 디렉터 — 지금 할 일

1. **Claude Pro** — `CURSOR_TO_CLAUDE.md` 최상단 NS-OBS-TG-01 OUTBOX 검증.
2. **VPS** — `git pull` (기존 cron 19:30 유지 · 신규 cron 없음). 다음날 북극성 일보에 `[OBS_HOLD]`·`---CURSOR---`·`---CLAUDE---` 확인.
3. **관측유지** — mega_trend · 목표하향 **착수 금지** (재소집 전).

### 재소집 트리거 (고정)

| 항목 | 값 |
|------|-----|
| **트리거** | VPS `dual_north_star_ledger.json` **daily n≥20** 도달 시 갈림길 자동 재소집 |
| **예상** | ~**2026-09-05** (8/16 기준 n=8 → +12일; **VPS 실측 재확인 후 확정**) |
| **텔레그램** | 매일 19:30 일보 → n&lt;20=`OBSERVE_HOLD` · n≥20=`RECALL_FORK` + 복붙 블록 |
| **금지** | n&lt;20에서 페이스·Pass/Fail·CAGR 확정 판정 · 로컬 원장 근거 |

### Claude 창 부팅 (복붙)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (앵커 SYNC-2026-08-17-Z)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 (NS-OBS-TG-01)

검증: 일보 OBS_HOLD 패널·복붙 DoD. OK면 VPS pull만 남김.
```

---

## North Star SSOT (고정)

| 항목 | 값 |
|------|-----|
| **SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **로컬** | `dual_north_star_ledger.LOCAL_DEV_DO_NOT_USE.json` — **사용 금지** |
| **실측(8/16)** | composite 4.09 · G0 · daily **8**/28 · fwd 324 — **판정 보류** |

---

## 근처놓침 레버 — 전원 소진·동결

BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1-REDUCED — 규칙1 재접촉 금지
