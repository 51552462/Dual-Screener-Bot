# 코인 선행 레이더 — `bitget_canary_state.json` 익스포터 설계 & 기대 영향 분석

작성일: 2026-06-29
성격: **설계 제안서(코드 미수정)**. 현재 100% 구조에 어떻게 대입하는지 + 적용 시 국면 판정이 정량적으로 어떻게 바뀌는지.
핵심 철학: 코인(24×7)을 주식의 **선행 변동성 레이더**로 쓰되, 주식 핵심 경로에 **DB 락 0 / 결합도 0**(파일 read-only, try-except)으로 **소프트하게** 얹는다.

---

## 0. TL;DR

| 구성요소 | 위치(실제) | 역할 |
|---|---|---|
| **Producer (코인)** | 신규 `bitget/canary_exporter.py` → `bitget_data_dir()/bitget_canary_state.json` | OI·펀딩·BTC/VIX로 2지표 산출 후 JSON **원자적 1회 기록** |
| **Consumer (주식)** | 기존 `predictive_regime_ensemble.py` `collect_live_snapshots()` + `compute_factor_states()` | JSON을 try-except로 읽어 **VIX 팩터 점수에 소프트 페널티** |
| **결합 방식** | 파일 1개(JSON). 주식은 코인 DB를 **절대 안 만짐** | DB 락 없음·실패 무해(graceful) |

이미 존재하는 **역방향 선례**가 결정적이다: `bitget/doomsday_bridge.py`는 주식 둠스데이 DEFCON을 `bitget_data_dir()/bitget_doomsday_status.json`으로 **파일 미러링**한다(주식→코인). 이번 canary는 그 **거울상(코인→주식)** 이며, 동일 패턴을 그대로 따른다 → 신규 아키텍처가 아니라 **검증된 패턴의 대칭 확장**.

---

## 1. 현재 100% 구조에 어떻게 대입되는가 (아키텍처 맵)

```
[코인 24×7]                                   [주식 KR/US 일일 국면]
bitget data_refresh / scan 파이프라인           predictive_regime_ensemble.run_and_evolve()
  └ (신규) _step_canary_export()                 └ collect_live_snapshots()   ← 여기서 JSON read
        │ ccxt 공개 API + 자기 DB(RO)                  │  _load_crypto_canary() (신규, try-except)
        │ funding_fetcher(기존) + OI(신규)             │
        ▼ 원자적 write                                 ▼ FactorSnapshot(.crypto_*) 주입
   bitget_canary_state.json  ───────파일─────────►  compute_factor_states()
   {crypto_liquidity_stress, macro_contagion_risk}     └ st["vix"] 에 소프트 페널티(이중 게이트)
```

- **Producer 실행 지점**: `bitget/pipelines/bitget_pipelines.py`의 파이프라인에 경량 tail step 1개 추가(예: `data_refresh` 및 `scan_*` 꼬리). 기존 `_step_doomsday_bridge_sync`와 동일한 위치/방식.
- **Consumer 흡수 지점**: `predictive_regime_ensemble.py`의 스냅샷 빌더. 기존에 이미 `_pri_z_by_market()`를 try-except로 외부 상태에서 읽어오는 패턴이 있으므로 **완전히 동형(isomorphic)** 으로 추가.

기존 소비자 코드(수정 대상 지점):

```255:267:predictive_regime_ensemble.py
def compute_factor_states(snap: FactorSnapshot) -> Dict[str, Optional[float]]:
    st: Dict[str, Optional[float]] = {f: None for f in FACTORS}
    if snap.close and snap.ma20 and snap.ma20 > 0:
        st["short_trend"] = _t(((snap.close / snap.ma20) - 1.0) / 0.03)
    if snap.close and snap.ma200 and snap.ma200 > 0:
        st["long_trend"] = _t(((snap.close / snap.ma200) - 1.0) / 0.08)
    if snap.vix is not None:
        st["vix"] = -_t((float(snap.vix) - VIX_MID) / VIX_SCALE)  # 높을수록 약세(-)
    if snap.breadth_ratio is not None:
        st["breadth"] = _t((float(snap.breadth_ratio) - 1.0) / 0.03)
    if snap.pri_z is not None:
        st["pri"] = float(max(-1.0, min(1.0, float(snap.pri_z))))
    return st
```

---

## 2. Producer 설계 — `bitget/canary_exporter.py` (신규)

### 2.1 산출 지표 ①: 암호화폐 유동성 스트레스 지수 (0.0~1.0)

> "단순 가격 하락 배제. OI 급감 **AND** 펀딩 음수 → 스마트머니 극단적 디리스킹."

**입력 (메이저 알트 상위 5: ETH, SOL, XRP, BNB, DOGE — 거래대금 기준 동적 선정 권장)**
- `oi_total_now`, `oi_total_24h_ago` → ΔOI_24h% = (now/ago − 1)
- `avg_funding` = 상위 5 펀딩비 평균 (기존 `bitget/funding_fetcher.fetch_funding_snapshot` 재사용)

**수식 (이중 성분의 기하평균 = "동시 발생" 강제)**
```
oi_drop      = clip( -ΔOI_24h% / OI_DROP_REF , 0, 1 )      # OI_DROP_REF=0.15 → -15%면 1.0
funding_neg  = clip( (FUND_THRESH - avg_funding) / FUND_REF , 0, 1 )
               # FUND_THRESH=-0.0001(-0.01%) 부터 시작, FUND_REF=0.0004
crypto_liquidity_stress = sqrt( oi_drop * funding_neg )    # 둘 다 높아야 0.8↑ (단일 요인 무시)
```
- **기하평균**을 쓰는 이유: OI만 빠지거나(롱청산) 펀딩만 음수인(베이시스) 단일 현상은 흔하다. **둘이 동시에** 극단일 때만 0.8을 넘겨 "진짜 디리스킹"만 포착 → 오탐 최소화.
- OI 24h 비교는 ccxt `fetch_open_interest_history(sym, '1h', limit=24)` 우선, 미지원 시 직전 실행분 OI를 작은 상태파일(`bitget_canary_oi_prev.json`)에 적재해 **스냅샷 차분**(방어적 폴백).

### 2.2 산출 지표 ②: 상관관계 역전 (macro_contagion_risk: bool)

> "BTC 급락 **AND** VIX/둠스데이 상승 동기화일 때만 True. 코인 개별 악재 필터링."

```
btc_ret_3d  = BTC 3일 수익률 (코인 자기 DB RO 또는 ccxt fetch_ohlcv('BTC/USDT','1d',4))
macro_up    = (VIX 3일 상승) OR (DEFCON 악화)   ← 우선: 기존 bitget_doomsday_status.json 읽기
macro_contagion_risk = (btc_ret_3d <= BTC_DROP_THRESH) AND macro_up
                       # BTC_DROP_THRESH 예: -0.07 (3일 -7%)
```
- **VIX 소스(중요·파일 기반 유지)**: 코인 익스포터가 주식 DB를 만지지 않도록, **이미 코인 디렉터리에 미러링된 `bitget_doomsday_status.json`**(둠스데이 브릿지가 기록)의 `metrics`/`signals`를 1순위로 읽는다. 폴백으로만 yfinance `^VIX` 3일을 직접 조회. → 추가 DB 접근 0.
- **의미**: BTC만 빠지고 VIX는 잠잠하면(코인 단독 악재) `False` → 주식에 전이 안 함. BTC·VIX가 **함께** 위험회피로 돌면 `True` → "거시 전염" 신호.

### 2.3 출력 JSON (원자적 기록)

경로: `bitget/infra/data_paths.py`에 `canary_state_path()` 추가 → `bitget_data_dir()/bitget_canary_state.json`
기록: `tmp` 작성 후 `os.replace`(원자적). (참고: `task_orchestrator.touch_worker_heartbeat`가 동일 원자적 패턴)

```json
{
  "schema": "bitget_canary.v1",
  "updated_at": "2026-06-29T06:00:00+00:00",
  "crypto_liquidity_stress": 0.0,
  "macro_contagion_risk": false,
  "components": {
    "oi_total_24h_change_pct": 0.0,
    "avg_funding_rate": 0.0,
    "btc_ret_3d": 0.0,
    "vix_3d_change": 0.0,
    "symbols_used": ["ETH","SOL","XRP","BNB","DOGE"]
  },
  "source": "bitget_canary_exporter"
}
```

### 2.4 실행 스케줄
- 경량(공개 API 5~10콜)이므로 `data_refresh` 파이프라인 **tail** 에 붙여 ~10분 주기 갱신.
- **KR 개장(09:00 KST) 직전·미국 마감 후**가 가장 가치 큼 → 코인이 밤사이 신호를 만들어 두면 다음 주식 국면 산출이 그걸 흡수(= 선행성).

---

## 3. Consumer 설계 — 주식 국면 앙상블의 소프트 흡수

### 3.1 읽기 (try-except, 결합도 0) — `collect_live_snapshots()` 내부

```python
def _load_crypto_canary() -> tuple[float, bool]:
    """코인 canary JSON을 비동기·방어적으로 읽는다. 실패/노후 시 (0.0, False) → 무영향."""
    try:
        import json, os, time
        # 모노레포라 코인 경로 SSOT를 그대로 import (DB 아님, 경로 계산만)
        from bitget.infra.data_paths import bitget_data_dir
        path = os.environ.get("BITGET_CANARY_STATE_PATH") or os.path.join(
            bitget_data_dir(), "bitget_canary_state.json")
        if not os.path.isfile(path):
            return 0.0, False
        # 신선도 게이트: 90분 초과 노후면 무시(코인 다운 시 주식 오염 차단)
        if time.time() - os.path.getmtime(path) > 5400:
            return 0.0, False
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return float(d.get("crypto_liquidity_stress") or 0.0), bool(d.get("macro_contagion_risk"))
    except Exception:
        return 0.0, False
```
→ `FactorSnapshot`에 `crypto_liquidity_stress: float = 0.0`, `macro_contagion_risk: bool = False` 필드 추가 후 US/KR 스냅샷 빌드시 주입(둘 다 글로벌 VIX를 공유하므로 동일 값 주입).

### 3.2 적용 (소프트 페널티) — `compute_factor_states()` 의 `st["vix"]` 직후

```python
    if snap.vix is not None:
        st["vix"] = -_t((float(snap.vix) - VIX_MID) / VIX_SCALE)
        # [코인 선행 레이더] 이중 게이트 통과 시에만 VIX 팩터를 더 약세(-)로 소프트 가중.
        if snap.macro_contagion_risk and snap.crypto_liquidity_stress >= CRYPTO_STRESS_GATE:
            # 0.8→1.0 구간 선형 램프(절벽 없음) × 상한 P_MAX
            ramp = min(1.0, (snap.crypto_liquidity_stress - CRYPTO_STRESS_GATE) / (1.0 - CRYPTO_STRESS_GATE))
            penalty = CRYPTO_VIX_PENALTY_MAX * ramp
            st["vix"] = max(-1.0, st["vix"] - penalty)
```
제안 상수: `CRYPTO_STRESS_GATE = 0.8`, `CRYPTO_VIX_PENALTY_MAX = 0.25`.

### 3.3 "소프트"의 핵심 설계 결정 3가지
1. **팩터 상태(state)에만** 가한다 → 가중합 점수에 비례적으로 녹아든다(연속적). 국면을 직접 덮어쓰지 않음.
2. **`is_vix_crisis()`는 원본 `snap.vix`만 사용** → canary 페널티가 **하드 위기 오버라이드(KR 강제 동기화)를 절대 트리거하지 않음**. (제일 중요한 안전장치)
3. **이중 게이트 + 0.8 램프** → 평시 0, 극단 동기화 위험회피에서만 점진 발동.

---

## 4. "DB 락 없이 파일 기반" 보장 근거

| 경로 | DB 접근 | 락 위험 |
|---|---|---|
| Producer write | 자기 코인 DB는 RO(WAL, busy_timeout=60000 — 이번 #3 적용분)로만 읽고, 산출물은 **JSON 1파일 원자적 write** | 없음(주식 DB 무접촉) |
| Consumer read | **JSON read only**. 주식 `market_data.sqlite` 무관 | 없음 |
| 실패 시 | try-except → `(0.0, False)` → **기존 국면 로직 그대로** | 무해 |

즉 코인이 죽든, 파일이 깨지든, 노후되든 **주식 국면 산출은 1도 안 흔들린다**(graceful degradation). 이는 기존 `cross_market_ssot`의 stale→KR_STANDALONE 폴백 철학과 동일.

---

## 5. 기대 영향 값 (정량)

### 5.1 VIX 팩터 점수 변화
`st["vix"] = -tanh((vix-18)/8)`. 페널티 P는 여기서 추가로 빼는 하방 압력.

| 상황 | 원 st_vix | 적용 P | 페널티 후 st_vix |
|---|---|---|---|
| vix=18(평온) | 0.00 | 0.25 | −0.25 |
| vix=22 | −0.38 | 0.25 | −0.63 |
| vix=26 | −0.76 | 0.25 | −1.00(클램프) |

### 5.2 국면 점수(score) 변화 — Δscore = −(정규화 VIX 가중치) × P
score ∈ [−1,1], 판정 임계 **UP=+0.18 / DOWN=−0.18**.

| VIX 가중치 w_vix | P=0.25 일 때 Δscore |
|---|---|
| 0.10 (저비중) | −0.025 |
| 0.20 (기본 균등) | −0.050 |
| 0.35 (위기에 VIX 스킬↑) | −0.088 |

**해석**: 경계 근처(SIDEWAYS 하단)에서 **−0.05~−0.09**의 하방 틸트 →
- score가 −0.13였다면 → −0.18~−0.22 → **SIDEWAYS→BEAR 전환** 가능(의도된 선제 방어).
- score가 +0.30(견고한 BULL)이면 → +0.25 → **여전히 BULL**(과잉 개입 없음). ✅ 소프트.

### 5.3 2차 효과(켈리 클러치)
1위 국면 softmax 확률이 `CLUTCH_PROB_THRESHOLD=0.60` 밑으로 내려가면 글로벌 켈리가 0.10~0.30배로 축소된다. canary 하방 틸트가 확률을 혼조 쪽으로 밀면 → **포지션 사이즈 선제 축소**(국면이 안 바뀌어도 리스크 다운). 이게 "소프트 흡수"의 진짜 가치.

### 5.4 발동 빈도(중요)
이중 게이트(stress≥0.8 **AND** contagion)는 **연 수 회 수준의 극단 동기화 위험회피**에서만 켜지도록 보정됨 → 평시 영향 0, 신호 대 잡음비 매우 높음. (단일 요인 변동은 기하평균·이중게이트로 자동 무시)

### 5.5 선행성(왜 "레이더"인가)
코인은 24×7라 **KR 개장 전 밤사이** 디리스킹/전염 신호를 먼저 만든다 → 다음 KR 국면 산출이 이를 흡수해 **개장 시점부터** 방어적으로 출발. VIX(미국 종가)만으로는 못 보는 **밤사이 글로벌 위험회피**를 메운다.

---

## 6. 신규 리스크 & 방어

| 리스크 | 내용 | 방어 |
|---|---|---|
| **이중 계산(double counting)** | VIX가 이미 팩터인데 canary가 또 VIX를 누른다 | 이중 게이트 + P 상한 0.25 + 가중치로 희석(Δscore≤~0.09). 평시 미발동 |
| **노후/좀비 신호** | 코인 다운 시 옛 위험신호 고착 | consumer 90분 신선도 게이트 → 무시 |
| **선행성 착시(lead-lag 오염)** | 코인 변동성이 주식과 무관한 날 | contagion 게이트(BTC**와** VIX 동기화)로 코인 단독악재 차단 |
| **OI 데이터 공백** | ccxt OI 히스토리 미지원/공백 | 스냅샷 차분 폴백 + 결측 시 stress=0 |
| **하드 오버라이드 오발** | canary가 위기 강제동기화를 켤까 | `is_vix_crisis`는 원본 vix만 사용 → **구조적으로 불가**(§3.3-2) |
| **파라미터 과적합** | 임계/스케일 임의값 | shadow 기간에 로깅만 하고 사후 보정(§7) |

---

## 7. 단계 롤아웃 (Shadow → Active)

1. **Producer만 가동(Shadow)**: canary JSON을 쓰되 consumer는 **읽되 페널티 미적용**(로그만). 며칠간 stress/contagion 분포와 실제 시장 하락의 정합성 관찰 → 임계 보정.
2. **소프트 활성(env 게이트)**: `CRYPTO_CANARY_PENALTY_ENABLED=1` 같은 플래그로 §3.2 페널티 on. 기본 OFF로 시작.
3. **상수 자가진화(선택)**: 기존 `evolve_weights`의 5일 PnL 보상 루프가 VIX 팩터 스킬을 이미 학습하므로, canary 페널티의 기여도 검증 후 P_MAX를 데이터로 조정.

---

## 8. 결정 필요 / 미구현 항목 (구현 착수 전 확인)
- 상위 5 알트 **고정 목록 vs 거래대금 동적 선정** — 동적 권장(상장폐지/유행 변화 대응).
- 임계 상수 초기값: `OI_DROP_REF=0.15`, `FUND_THRESH=-0.0001`, `FUND_REF=0.0004`, `BTC_DROP_THRESH=-0.07`, `CRYPTO_VIX_PENALTY_MAX=0.25` — shadow로 보정 전제.
- canary 페널티를 **KR/US 동시 적용** vs **KR 우선**(밤사이 선행성은 KR 개장에 더 유효). 기본: 양시장 동일(글로벌 VIX 공유 일관).
- 익스포터를 **bitget 파이프라인 tail** 로 둘지 **독립 cron** 으로 둘지(권장: data_refresh tail — 별도 락/프로세스 불필요).

---

## 9. 요약
- **대입 방식**: 검증된 `doomsday_bridge`(주식→코인) 패턴의 **거울상(코인→주식)**. Producer가 JSON 1개를 원자적 기록, Consumer(`collect_live_snapshots`)가 try-except로 읽어 VIX 팩터에만 소프트 페널티.
- **무DB락**: 주식은 코인 DB 무접촉·JSON read-only·실패 무해. 구조적으로 락 0.
- **기대 영향**: 평시 0, 극단 동기화 위험회피 시 국면점수 −0.05~−0.09 하방 틸트(경계에서 SIDEWAYS→BEAR 가능) + 켈리 클러치로 포지션 선제 축소. 하드 위기 오버라이드는 **절대 안 건드림**.
- **가치**: 코인 24×7이 만든 밤사이 글로벌 디리스킹 신호를 주식 개장 국면에 **선행**으로 소프트 반영.
