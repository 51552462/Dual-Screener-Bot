# UNIVERSE-BT · 구조생존 검증 (L0 R&D · 병렬 독립 트랙)

> **배너 (모든 산출물 고정):** **L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지**  
> **SSOT**: 본 파일 — 유니버스 스냅샷·지표 5종·Kill·U0~U3 로드맵의 **단일 진실**  
> **소유**: CAT-Q (진단&레거시) · **문서 전용** (본 파일만으로 코드·config_kv 변경 없음)  
> **작성**: UNIVERSE-BT-U0 · 2026-08-23 · Claude 설계 · Cursor 문서화  
> **Track B 관계**: B1-LADDER(R0~R6)와 **완전 병렬 독립** — 상호 게이트·Kill 연동 **없음**. 리소스 우선순위는 R1a보다 **낮음** (디렉터 선택적 병렬).

---

## 1. 유니버스 스냅샷 정의

**포함 집합 (교집합만):**

```text
U = load_dynamic_universe(market_type)   # 거래량 floor 통과분 · zombie BL 제외
  ∩ OHLCV 보유 테이블                    # BITGET_SPOT_* 또는 BITGET_FUT_*
```

| 축 | 정의 | 비고 |
|----|------|------|
| 심볼 소스 | `mtf_data_updater.load_dynamic_universe` 출력 | 라이브 스캐너와 **동일 필터** |
| 바 커버리지 | 해당 `market_type` OHLCV 테이블이 DB에 존재 | 갭·상장 전 구간은 스캔 제외 |
| market_type | `spot` \| `futures` 파라미터 | 하드코딩 금지 (§4) |

### Survivorship · "상장 전부" 제외 (1문단)

**"상장 전부"는 본 트랙 범위에서 제외한다.** 라이브 스캐너가 이미 `load_dynamic_universe`의 거래량 floor·zombie 블랙리스트만 보기 때문이다. floor 밖·좀비·미보유 OHLCV 심볼까지 넣으면 히스토리 결과는 넓어 보여도 **"실제 구조가 보는 유니버스"와 괴리**되어 외부 타당도가 무너진다. 따라서 U = (거래량 통과분) ∩ (보유 OHLCV)만 스냅샷으로 고정하고, 상장 전체 나열은 연구 목표가 아니다. 상장 후 폐지·상폐로 사라진 심볼은 교집합에 없으면 자연 제외되며, 이를 "과거에도 이겼다"는 승률 주장의 근거로 쓰지 않는다 (survivorship 고지).

---

## 2. 성공 정의 — L0 지표 5종만 (CAGR/Sharpe 아님)

구조생존 = 아래 **다섯 비율만**. 연복리%·샤프·승률%를 성공계약처럼 쓰는 표현은 **U3 템플릿부터 금지** (본 §3 Kill).

| ID | 수식 | 의미 (한 줄) |
|----|------|--------------|
| **hit_rate** | `raw_signal_hit / total_bars_scanned` | 스캔 바 대비 원시 시그널 히트 |
| **gate_pass_rate** | `gate_passed_candidates / candidates_generated` | 후보 대비 게이트 통과 |
| **virtual_entry_rate** | `virtual_entries / gate_passed_candidates` | 게이트 통과 대비 가상진입 |
| **crash_window_forced_exit_rate** | `(BEAR ∪ HIGH_VOL 구간) SL 또는 MDD 트리거 횟수 / 해당 구간 가상포지션 수` | CAT-G `CURRENT_REGIME_KEY` **BEAR·HIGH_VOL** 한정 (CRASH 라벨 없음 · 룰5) |
| **side_asymmetry_ratio** | `LONG_virtual_entries / SHORT_virtual_entries` | **국면별** · SPOT 각주 아래 |

**분모 0:** 해당 지표는 `null` (0으로 나누어 가짜 100% 금지).

### SPOT SHORT=0 각주

현물(SPOT)은 구조상 숏 진입 불가(ledger `spot`+`SHORT` hard reject · SHORT-DANTE-FUT-01). 따라서 SPOT에서 `SHORT_virtual_entries = 0`은 **정상**이며, `side_asymmetry_ratio`는 SPOT에서 **정의하지 않거나** `null`로 표기한다 (∞/에러로 과신 해석 금지). FUTURES만 국면별 롱/숏 비대칭을 보고한다.

---

## 3. L0 라벨 · Kill (과신 표현)

### 고정 배너 (모든 산출물 상단)

```text
L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지
```

### Kill (즉시 정정 대상)

| 위반 | 행동 |
|------|------|
| 지표 5종을 **R6** 판정 입력으로 사용 | 정정 · R6은 L2 forward만 |
| 지표를 **B1 성공계약** (연 12~18% · MDD≤5%)에 대입 | 정정 · B1 계약과 **분리** |
| "백테스트 Pass → LIVE 승격" / "CAGR 달성" 단정 | 정정 · IV L0 ≠ 승격 근거 |
| 연복리%·승률%를 U3 성공 문구로 사용 | 템플릿에서 **금지** (정량표만) |

본 트랙 산출 = **IV L0** 단서. paper forward(L2)·B1-LADDER와 **혼동 금지**.

---

## 4. SPOT / FUT 분리 원칙

- **공통 하니스** + `market_type` 파라미터화 (`spot` \| `futures`).
- 비대칭·Treasury·SHORT 정책은 `bitget/docs/claude_project/CAT-SPOT-FUT_비대칭표.md` **인용만** — **표 재작성 금지**.
- `if symbol.endswith("USDT")` 등으로 SPOT/FUT 추론 **금지** — `market_type` SSOT 필드 사용.
- 리포트는 SPOT·FUT **분리 집계** 후 나란히 제시 (합산으로 비대칭 숨기기 금지).

---

## 5. 로드맵 (UNIVERSE-BT-0x · Track B와 게이팅 없음)

| sub | 내용 | 위험도 | 상태 |
|-----|------|--------|------|
| **U0** | 본 SSOT — 스냅샷·지표 5종·L0·Kill·로드맵 | 🟢 문서 | **Claude OK** |
| **U1** | read-only 리플레이 하니스 — CAT-C **원본 import만**(수정 금지), 결과 → 격리 `bitget_universe_bt.sqlite` (paper DB·config_kv **쓰기 금지**) · **C3: `exit_trigger`/지표4 보류** | 🟡 Medium | **Claude OK** |
| **U2** | 배치·샤드·체크포인트 · 기존 `TIME_MACHINE_MAX_*` **재사용** (신규 상수 창조 금지) | 🟢 | **Claude OK** |
| **U3** | 리포트 — L0 배너 고정 · **정량표만** · CAT-J 비편입 · **지표4=N/A** · VPS 1H 폴백 실측 포함 | 🟢 | **Claude OK 2026-08-23** (`live-20260823T121158Z`) |

**Track B (B1-LADDER R1a~R6):** OBSERVE / Kill / PASS에 본 트랙 **게이트 없음**. 본 트랙도 R1a를 **막지 않음**.

**비접촉 (U0~U3 공통 헌법):**  
`master_scanner` / `signal_engines` / gates (CAT-C) 원본 **수정 금지** · OHLCV·`load_dynamic_universe` (CAT-B) 쓰기 금지 · regime writer (CAT-G) · Kelly/Treasury (CAT-F) · `execution_safety` (CAT-N) · `bitget_forward_trades` (CAT-D) · config_kv · 주식 루트 전체.

---

*버전 2026-08-23 · UNIVERSE-BT-U0 · Architect: Claude Pro · Engineer: Cursor*
