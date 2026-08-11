# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **BULL-RECENCY-01** |
| **status** | 2단계 **코드 완료** · VPS **15구간 re-sim rerun** 대기 |
| **Handoff** | [`CLAUDE_TO_CURSOR.md`](CLAUDE_TO_CURSOR.md) §BULL-RECENCY-01 + **1단계 충족 판정** |
| **앵커** | `SYNC-2026-08-11-J` |

---

## Cursor — 지금 할 일 (VPS rerun)

### VPS (matrix + brain 서버)

```bash
export BULL_RECENCY_01_PATCH=1
export RP1_SKIP_STAGE2=1
unset RP1_METRICS_ONLY
python scripts/run_bull_recency_01_rp1.py \
  --baseline reports/regime_panel/rp1_20260811_v233.json
```

- OHLCV parquet 캐시 있으면 FDR 없이 re-sim (~1h)
- 산출: `reports/regime_panel/rp1_bull_recency_01_{date}.json` (v2.3.4) + `_dod.json`
- **금지**: `RP1_METRICS_ONLY` (bounds 변경 시 trade snapshot 재사용 불가)

### DoD (rerun 후)

| # | 기준 |
|---|------|
| 1 | BULL_03/05 ≥ NEAR_MISS (`period_return_pct`) |
| 2 | 나머지 13구간 verdict 불변 |
| 3 | tier MDD ≤10% · MDD_OK |
| 4 | n≥20 전 구간 |

### 완료 (2단계 코드 — 2026-08-11)

| 항목 | 경로 |
|------|------|
| bounds patch | `bull_recency_01_bounds.py` |
| RP-1 wiring | `regime_panel_rp1_runner.py` · `regime_panel_rp1.py` |
| runner | `scripts/run_bull_recency_01_rp1.py` |
| tests | `tests/test_bull_recency_01_bounds.py` (8 pass) |

---

## Cursor — 구현 (참고 · 완료)

---

## Cursor 새 채팅 부팅 (복붙)

```text
역할: Cursor Lead Engineer.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CLAUDE_TO_CURSOR.md (BULL-RECENCY-01 Handoff + 1단계 충족 판정)

트랙: docs/work_phases/ SSOT only. bitget/ 제외.
이번 세션: BULL-RECENCY-01 — 2단계 CLUSTER_1 bounds 타이트닝 + 15구간 metrics-only rerun.
금지: 전역 DNA · Phase A · config_kv 라이브 · BULL_03/05 단독 rerun.
```

---

## 완료 (1단계)

| 항목 | 값 |
|------|-----|
| Claude VERDICT | **충족** |
| S1 범위 | CLUSTER_1 bounds targeted |
| BULL_05 | 동일 패치 먼저 |
