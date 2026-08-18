# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **OPS-OPEN-STALL-01** (survivors≈0 진단 · read-only) |
| **status** | 스크립트 Done · **VPS 실행 후** `WAIT_CLAUDE_OK` · `OBS-HOLD` · Alpha **금지** |
| **직전** | Claude Handoff 랜딩 · NS-DIR-DASH-01 **Claude OK** |
| **앵커** | `SYNC-2026-08-18-F` |

---

## 디렉터 — 지금 할 일

1. **Cursor** — `scripts/ops_open_stall_01_diagnosis.py` VPS에서 실행 · Step 0부터 OUTBOX 보고.
2. **Ops (잔여)** — NS-DIR-DASH: VPS `git pull` · 19:30 `[쉬운판]` 육안.
3. **관측유지** — mega_trend · 목표하향 · 소진 레버 · cutoff 완화 **금지**.

### 재소집 트리거 (고정)

| 항목 | 값 |
|------|-----|
| **트리거** | VPS daily **n≥20** → 갈림길 재소집 |
| **텔레그램** | 19:30 · `[쉬운판]` + `[OBS_HOLD]` + 복붙 |
| **금지** | n&lt;20 페이스·CAGR 확정 · 로컬 원장 근거 |

### VPS 실행 (복붙)

```bash
cd ~/dante_bots/Dual-Screener-Bot
set -a && source .env && set +a
# 패치 pull 후:
python3 scripts/ops_open_stall_01_diagnosis.py
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
