# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **BULL-RECENCY-01** |
| **status** | `WAIT_CLAUDE_OK` — **8/13 JSON SSOT 고정** · VPS 재현 full **중지** |
| **Handoff** | 다음 = Claude가 `CLAUDE_TO_CURSOR.md`에 발행 |
| **앵커** | `SYNC-2026-08-16-A` |

---

## 디렉터 — 지금 할 일 (1줄)

```text
docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 OUTBOX 검증.
8/13 JSON을 BULL-RECENCY DoD SSOT로 확정(재현 full 중지). OK면 다음 Handoff를 CLAUDE_TO_CURSOR.md에.
채팅 말고 파일에.
```

---

## SSOT 고정 (2026-08-16 디렉터 결정)

| 항목 | 값 |
|------|-----|
| **유효 JSON** | `rp1_bull_recency_01_20260813.json` (+ `--dod-only` 재생성 DoD) |
| **BULL_03** | **NEAR_MISS** · n=**10,276** · period_ret **15.40%** |
| **BULL_05** | **FAIL** · period_ret **−9.04%** (iter2 미충족 — 다음 Handoff 주제) |
| **폐기** | 20260814 · 20260815 · 20260816 full (전부 baseline 또는 붕괴) |
| **금지** | BULL-RECENCY 재현용 VPS 15구간 full **추가 실행** |

### 왜 재현을 멈추는가

- scope ON + 실 shrink → BULL_03 n≈0  
- scope OFF + 실 shrink → BULL_03 = **40,657 / 4.30%** = baseline  
- smoke `n=10276`(FAST)는 8/13이 아니라 **baseline 비율** 오탐이었음  

**iter 2 증거(8/13)는 이미 있음.** 재현 루프는 목표(다음 단계)를 막음 → Claude가 다음 Handoff(예: BULL_05 별도 레버 / Done 부분 인정) 결정.

---

## Cursor — 완료·대기

1. ✅ DoD `regime_name` 버그픽스 + `--dod-only`
2. ✅ KR 레버 코드 (미검증 — 유효 full 없음)
3. ⛔ VPS 재현 full — **중지** (디렉터 결정 2026-08-16)

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
