# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **BULL-RECENCY-01** |
| **status** | DoD#1 **미충족** · **2b rerun 완료** → **템플릿 재식별** 선행 |
| **Handoff** | [`CLAUDE_TO_CURSOR.md`](CLAUDE_TO_CURSOR.md) §BULL-RECENCY-01 |
| **앵커** | `SYNC-2026-08-13-A` |

---

## Cursor — 지금 할 일 (2b 이후)

1. **shrink 재rerun 금지** — Claude·Cursor 합의
2. VPS `scripts/bull_recency_01_template_audit.py --snapshot matrix_*.pkl --apply-patch`
3. BULL_03/05에서 **폭발형 top1 share** vs **first-match order** 대조 → S1 타깃 재확정

**2b 결과**: key-mirror ✅ · BULL_03 n=40657 baseline 동일 · DoD#1 **No**

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
