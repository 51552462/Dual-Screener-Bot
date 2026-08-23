# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-23  
> **유형**: **UNIVERSE-BT L0 VPS 재실행** · 원인 수정 후 듀얼 실측

---

## OUTBOX — 2026-08-23 · VPS 재검증 (`live-20260823T114203Z`)

### 원인 (첫 패스 FUTURES=0)
| 항목 | 실측 |
|------|------|
| FUT_1D BTC | **n=90** (2026-05-26~08-23) |
| SPOT_1D BTC | **n=300** |
| FUT_1H BTC | **n=1000** |
| U1 `min_bars` | 240 → FUT_1D 전 심볼 탈락 (게이트 이전) |

알파벳 밈코인 가설은 SPOT 축소에만 해당. FUTURES 0행의 주원인은 **1D 깊이 부족**.

### 코드 조치 (push `0955088`)
1. `select_run_symbols` — 메이저 우선 + TF 깊이 ≥240  
2. `resolve_run_timeframe` — 1D 부족 시 **1H 폴백** (disclosure 로그)

### 재실측 L0 (`MAX_SYMBOLS=10` · paper delta=0)

| | SPOT (1D) | FUTURES (1H 폴백) |
|--|-----------|-------------------|
| total_bars_scanned | 110 | **110** |
| candidates_generated | 9 | 0 |
| hit_rate | 0.081818 | 0.000000 |
| gate_pass_rate | 0.000000 | null |
| virtual_entries | 0 | 0 |

report: `bitget/analysis/universe_bt/reports/u3_live-20260823T114203Z.md`

### 해석 (L0 단서만 · CAGR 금지)
- 파이프·paper 격리·듀얼 스캔 **정상화**  
- SPOT: 후보 있음 · **게이트 0통과** (구조 사망 단정 아님)  
- FUTURES: 1H로 스캔은 되나 이번 10메이저에서 엔진 후보 0  
- FUT_1D 백필(≥240일) 또는 더 큰 심볼셋은 디렉터 선택

### Ask
- Claude: 1H 폴백 disclosure OK?  
- 디렉터: FUT 1D 히스토리 백필 / MAX_SYMBOLS↑ / 관측 유지
