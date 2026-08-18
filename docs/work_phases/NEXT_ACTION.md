# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **NS-DIR-DASH-01** (쉬운판 텔레그램 대시보드) |
| **status** | `WAIT_CLAUDE_OK` · 운영 `OBS-HOLD` 유지 · Alpha **금지** |
| **직전** | ROADMAP-SYNC-01 **Claude OK** |
| **앵커** | `SYNC-2026-08-18-D` |

---

## 디렉터 — 지금 할 일

1. **Claude Pro** — `CURSOR_TO_CLAUDE.md` 최상단 NS-DIR-DASH-01 OUTBOX 검증.
2. **Ops** — VPS `git pull` 후 19:30 일보 **맨 위** `[쉬운판]` 확인.
3. **관측유지** — mega_trend · 목표하향 · 소진 레버 **착수 금지**.

### 재소집 트리거 (고정)

| 항목 | 값 |
|------|-----|
| **트리거** | VPS daily **n≥20** → 갈림길 재소집 |
| **텔레그램** | 19:30 · `[쉬운판]` + `[OBS_HOLD]` + 복붙 |
| **금지** | n&lt;20 페이스·CAGR 확정 · 로컬 원장 근거 |

### Claude 창 부팅 (복붙)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (앵커 SYNC-2026-08-18-D)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 (NS-DIR-DASH-01)

검증: 쉬운판 대시보드 DoD. Alpha/목표숫자 변경 없음 확인.
```

---

## North Star SSOT (고정)

| 항목 | 값 |
|------|-----|
| **SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **로컬** | `*.LOCAL_DEV_DO_NOT_USE.json` — **사용 금지** |

---

## 근처놓침 레버 — 전원 소진·동결

BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1-REDUCED — 규칙1 재접촉 금지
