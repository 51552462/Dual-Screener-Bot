# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **BULL-RECENCY-01** (이터레이션 3) |
| **status** | `WAIT_CURSOR_IMPL` — KR 레버 + DoD fix · **15구간 rerun 대기** |
| **Handoff** | [`CLAUDE_TO_CURSOR.md`](CLAUDE_TO_CURSOR.md) §이터레이션 3 |
| **앵커** | `SYNC-2026-08-13-B` |

---

## Cursor — 지금 할 일 (iter 3)

1. ✅ DoD `regime_name` 버그픽스 + `--dod-only` (로컬)
2. ✅ KR `dyn_rs` floor on `260628` (코드) — **VPS rerun 필요**
3. VPS full 15구간 rerun (`shrink=0.45` 유지, `BULL_RECENCY_01_KR_LEVER=1`)

```bash
export BULL_RECENCY_01_PATCH=1 BULL_RECENCY_01_SHRINK=0.45 BULL_RECENCY_01_KR_LEVER=1
export RP1_SKIP_STAGE2=1
unset RP1_METRICS_ONLY RP1_MATRIX_REUSE RP1_FAST
python3 scripts/run_bull_recency_01_rp1.py \
  --baseline reports/regime_panel/rp1_20260811.json
```

**iter 2 확정**: BULL_03 NEAR_MISS · BULL_05 FAIL · `_dod.json` all_pass=true **무효**(버그)

---

## 디렉터 — Claude VERDICT 반영 (2026-08-12)

### 완료 (2단계 — 2026-08-12)

| 항목 | 값 |
|------|-----|
| rerun JSON | `rp1_bull_recency_01_20260812.json` v2.3.4 · overall **PASS** |
| _dod.json | 미생성 (baseline 부재) |

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

**전문·모드별 문구**: [`17_Cursor_세션_부팅_가이드.md`](17_Cursor_세션_부팅_가이드.md) §3  
**자동 규칙**: `.cursor/rules/` (첫 메시지 없어도 SSOT·금지 적용)

```text
Track A 구현 — 17 §3-A에 sub-phase ID만 채워서 사용.
현재: BULL-RECENCY-01 — NEXT_ACTION·§3 스냅샷 확인 후 착수.
```

---

## 완료 (1단계)

| 항목 | 값 |
|------|-----|
| Claude VERDICT | **충족** |
| S1 범위 | CLUSTER_1 bounds targeted |
| BULL_05 | 동일 패치 먼저 |
