# FULL-BT · 전체이식 가상매매 (IV L1 · 병렬 독립 트랙)

> **배너 (모든 산출물 고정):** **IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지. 공식 B1 판정은 R6(L2 forward 56일+)만.**  
> **SSOT**: 본 파일 — 이식 범위·산출 스키마·Kill·paper 헌법·로드맵의 **단일 진실**  
> **소유**: CAT-Q (진단&레거시) · **FULL-BT-0 = 문서 전용** (본 파일만으로 코드·config_kv 변경 없음)  
> **작성**: FULL-BT-0 · 2026-08-23 · Claude 설계 · Cursor 문서화  
> **Track B 관계**: B1-LADDER(R0~R6)와 **완전 병렬 독립** — 상호 게이트·Kill 연동 **없음**. 우선순위는 R1a보다 **낮음**.  
> **U-track 관계**: `14_UNIVERSE-BT_구조생존검증.md`와 **분리** — U4로 이어 붙이지 않음 (L0 Kill vs PnL/MDD 요구 충돌).

---

## 1. 이식 범위 (포함/제외 — U-track과 경계)

| 축 | U-track (L0, 기존·불변) | **FULL-BT (L1, 본 트랙)** |
|----|-------------------------|---------------------------|
| 엔진 풀 | master+ema5 2종 | `signal_engines.py` 5종 + `master_scanner.py`(C-1 pre-candidate hook) — CAT-C §2 **원본 import**, 원본 수정 금지 |
| TF | 1D (필요 시 1H 폴백은 U3 한정) | 라이브 스캐너 실사용 TF **재사용만** — FULL-BT-1에서 codebase 조사 후 `재사용 TF: {실제값}` 1줄 보고 · **임의 확장 금지**(룰5) |
| candidate→진입 | U1 Adapter 축소 | `forward/shared.py` **try_add 11단계**(CAT-D §4) 원본 import. 11번 execution_safety는 real 전용 → paper replay에서 **N/A skip**. (원문 “게이트 13”=CAT-N real 명칭 · paper 범위는 try_add 11이 정확) |
| 청산(exit) | 미포함 | `trading/position_manager.py` + `tail_risk_gate.py` + `mega_trend_kill_bg.py` 원본 import(CAT-E) · CLOSED write는 격리 DB Adapter |
| funding | 미포함 | `funding_accum_usdt_est` 재사용 **시도** — 이력 소스 존재는 FULL-BT-1에서 (a)/(b)/(c) 조사 · **신규 근사 금지**. PnL 미차감(P1-3)은 라이브와 동일 승계 · P1-3 선구현 금지 |
| 국면 | UNKNOWN 고정(U1 C3) | **동일 승계 · 재조사 금지**. UNKNOWN Kelly cap(~0.015) 자연 적용 |
| 저장소 | `bitget_universe_bt.sqlite` | **신규** `bitget_full_bt.sqlite` — paper·config_kv 커넥션 분리 |
| 배너 | L0 구조단서 | **L1 전체이식 가상매매** (§3) |

**비포함 (명시):** LIVE · `ENABLE_REAL_EXECUTION` · R6 대체 · U-track 지표와 PnL 혼합 서술 · 지표4 병합 · Track A 비교.

---

## 2. 산출물 스키마 (FULL-BT-3 구현 · FULL-BT-0는 정의만)

### PnL/MDD 정량표 (키)

`run_id`, `market_type`, `symbol_or_agg`, `period_start`, `period_end`, `total_return_pct`, `mdd_pct`, `trade_count`, `b1_reference_band`  
→ `b1_reference_band` 값 예: `"12~18%/≤5%, 참고용 — 판정 아님"` (`13_B1` §1 인용만)

### 개선 단서 슬롯 (키)

`gate_bottleneck_by_step` (try_add 11단계별 거절 카운트), `side_asymmetry` (LONG/SHORT 진입·거절), `symbol_breakdown` (top rejected/entered), `tf_note`

### Kill (서술)

CAGR 과신 · 승률 단정 · 연복리 환산 과대표현 금지 — **정량표 그대로만**.

---

## 3. IV L1 라벨 · Kill

고정 배너: 상단 인용문과 동일.

| 위반 | 행동 |
|------|------|
| FULL-BT 결과를 R6 대체 / B1「달성」 근거로 사용 | 정정 — 공식 B1은 R6 forward 전용 |
| B1 수치를 pass/fail 게이트로 직접 대입 | 정정 — 참고 대조 열만 |
| LIVE 승격 근거로 인용 | 정정 |
| U-track(L0) 지표와 FULL-BT(L1) PnL/MDD를 단일 성공 문구로 혼합 | 정정 — L0/L1/L2 레이어 분리 |

---

## 4. SPOT/FUT 분리

`CAT-SPOT-FUT_비대칭표.md` **인용만**(재작성 금지).  
`market_type` 파라미터화 · SPOT SHORT는 ledger hard reject로 자연 0건 · 리포트는 SPOT·FUT **분리 집계 후 나란히** (합산 금지).

---

## 5. paper 헌법

- 신규 물리 파일: `bitget_full_bt.sqlite` (경로 SSOT는 FULL-BT-1 `paths`에서 확정)
- `bitget_forward_trades` · `config_kv` **비접촉**
- 커넥션 공유 금지 (U1 원칙 승계)

---

## 6. 로드맵 (FULL-BT-0x · Track B·U-track과 게이팅 없음)

| ID | 내용 | 위험 | 상태 |
|----|------|------|------|
| **FULL-BT-0** | 본 SSOT — 이식 범위·스키마·Kill·paper·로드맵 | 🟢 문서 | **✅ Done · Claude OK** |
| **FULL-BT-1** | read-only 하니스 — 엔진풀+try_add 11+청산 Adapter · TF/funding 조사 포함 | 착수 시 재평가 (U1보다 넓음) | **✅ Done · Claude OK** |
| **FULL-BT-2** | 배치·체크포인트 · `TIME_MACHINE_MAX_*` 재사용 | 🟡 | **✅ Done · Claude OK** |
| **FULL-BT-3** | 리포트 — §2 스키마 · CAT-J 비편입 | 🟢 | **✅ Done · Claude OK 2026-08-24** |
| **FULL-BT-HIST-1** | `run_replay` 실제 OHLCV 바 워크 (캔들축) | 🟡 | 파일럿 미통과(계측 무효) → HIST-2 |
| **FULL-BT-HIST-2** | engine_hit / gate_reject 진단 계측 (`full_bt_diag`) | 🟢 | **Claude OK** · VPS dry→10×2 · 전체런 금지 |

**비접촉 (FULL-BT-0~3 공통 헌법):**  
`bitget_forward_trades` · `config_kv` · CAT-B/C/D/E/F/G/N **원본 수정** · `ENABLE_REAL_EXECUTION` · U-track `14_` 본문 재작성.

---

*버전 2026-08-23 · FULL-BT-0 · Architect: Claude Pro · Engineer: Cursor*
