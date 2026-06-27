# [1/9] "MetaGovernor 근거" 영어 토큰 완전 해설 + 퀀트 로직 영향 (기관급)

> 리포트의 `🗣️ [MetaGovernor 근거]` 줄에 뜨는 영어/코드(예: `REGIME_ANALYSIS=BULL / BITGET_CURRENT=...
> | VIX>롤링p90 → HIGH_VOL 승격`)는 **"오늘 시장 국면을 무엇으로·왜 판정했는가"의 출처 영수증**이다.
> 이 한 줄이 **켈리 상한(베팅 크기) → 포지션 노출액(NAV×켈리) → 공격/방어 축 가중치**까지 연쇄로
> 바꾸기 때문에, 시스템에서 **실제 돈의 크기를 좌우하는 가장 상위 스위치**다.

---

## 1. 그 영어 줄, 한 토막씩 해석

근거 문자열은 `meta_governor._resolve_regime_from_configs()` 가 조립한다. 구성 요소:

| 화면 토큰 | 뜻 | 출처 |
|-----------|-----|------|
| `REGIME_ANALYSIS=<KEY>` | **주식(거시) 국면 판정기**가 본 현재 국면 | `system_config.REGIME_ANALYSIS.regime_key` (GSPC·KOSPI 지수 분석) |
| `BITGET_CURRENT=<KEY>` | **코인(Bitget) 엔진**이 본 현재 국면(주식 축으로 매핑) | bitget config `CURRENT_REGIME_KEY` |
| `/` | 두 출처를 병합(merge)했다는 구분자 | `_merge_regime_keys` |
| `\| VIX 스킵(오프라인/설정)...` | VIX 데이터를 못 써서 "VIX>p90 → 고변동 승격" 규칙 미적용 | VIX 블록 skipped |
| `\| VIX>롤링p90 → HIGH_VOL 승격` | VIX가 롤링 90퍼센타일 초과 → 국면을 **강제로 HIGH_VOL** 로 격상 | VIX 분위수 비교 |
| `출처 불명 — UNKNOWN 기본` | 두 출처 다 비어 보수적 기본값 적용 | 폴백 |

> 즉 `REGIME_ANALYSIS=BULL / BITGET_CURRENT=BULL` = "주식 판정기도 코인 엔진도 둘 다 강세장으로
> 봤다"는 뜻이고, 뒤에 VIX 노트가 붙으면 "변동성 지표 때문에 판정을 덮어썼다"는 의미다.

---

## 2. 국면(Regime) 5종 사전 — 영어 KEY ↔ 한국어 뜻 ↔ 행동

`meta_governor.ACTION_BY_REGIME` 가 국면별로 **켈리 상한**과 **공격/방어 축 가중 범위**를 못박는다.

| KEY (화면 영어) | 한국어 의미 | 켈리 상한 `kelly_cap` | S1(공격·모멘텀) 범위 | S4(방어·역축) 범위 | 한 줄 정책 |
|-----------------|-------------|------------------------|----------------------|--------------------|-----------|
| **BULL** | 강세장 | **2.8%** (최대 공격) | [1.00, 1.85] (상한 완화) | [0.55, 1.15] (억제) | 모멘텀 풀악셀, 방어 축소 |
| **SIDEWAYS** | 횡보·혼조 | 1.8% (중립) | [0.65, 1.25] | [0.85, 1.45] | 양 축 밸런스 |
| **UNKNOWN** | 데이터 불충분 | 1.5% (보수 기본) | [0.55, 1.35] | [0.75, 1.35] | 안전 기본 맵 |
| **HIGH_VOL** | 고변동 | **1.2%** (축소) | [0.45, 1.05] (억제) | [0.90, 1.55] (방어↑) | 변동성 폭발 → 베팅 축소·방어 가중 |
| **BEAR** | 하락·늪지 | **1.0%** (최소 공격) | [0.35, 0.95] (강한 억제) | [1.05, 1.75] (방어 최대) | 공격 최소·역축/방어 확대 |

- **S1 = 공격(롱·모멘텀) 전략 축**, **S4 = 방어(역추세·숏 성격) 전략 축**. 범위 `[lo, hi]` 는
  스코어링 단계에서 두 축의 가중치를 **이 구간으로 클램프**한다(`meta_governor_consumer` 의
  `weight_s1/s4_bounds` 적용). 강세장이면 공격 축 상한이 1.85까지 열리고, 약세장이면 0.95로 눌린다.

---

## 3. 이게 내 퀀트 로직에 미치는 영향 — 인과 체인 (가장 중요)

```
[VIX·GSPC·KOSPI·Bitget]                      ← 시장 원천
        │  _resolve_regime_from_configs
        ▼
META_REGIME_KEY (BULL/BEAR/HIGH_VOL/...)      ← "근거" 줄의 핵심
        │  ACTION_BY_REGIME[KEY]
        ▼
META_REGIME_ACTION = { kelly_cap, kelly_floor, weight_s1_bounds, weight_s4_bounds, notes }
        │
        ├──► (A) 유효 켈리 산출  effective_kelly = clamp( DYNAMIC_KELLY_RISK × META_GLOBAL_KELLY_MULT,
        │                                                 floor, kelly_cap )
        │            (regime_kelly_failsafe.apply_graceful_kelly_to_effective)
        │
        ├──► (B) 포지션 노출액   Notional = Live NAV × effective_kelly     ← 실제 베팅 금액!
        │            (live_nav_manager.live_notional / row_notional)
        │
        └──► (C) 전략 축 가중    w_S1, w_S4 를 [lo,hi] 로 클램프 → 종목 점수·선정에 반영
                     (meta_governor_consumer.clamp_axis_weights)
```

### (A) 유효 켈리 — "한 판에 자본의 몇 %를 거나"
```200:205:regime_kelly_failsafe.py
    eff = adj_base * g
    if floor is not None:
        eff = max(eff, float(floor))
    if cap is not None:
        eff = min(eff, float(cap))
    eff = max(0.0, eff)
```
- `adj_base` = `DYNAMIC_KELLY_RISK`(시스템 베이스 켈리), `g` = `META_GLOBAL_KELLY_MULT`(메타 글로벌 배수).
- 국면이 BEAR면 `kelly_cap=1.0%` 로 **강제 상한** → 아무리 베이스가 높아도 1%로 깎인다.
- 국면이 BULL이면 상한이 2.8%까지 열려 **같은 자본에도 베팅이 2.8배**까지 커질 수 있다.

### (B) 포지션 노출액 — 리포트 [1/9] Live NAV 와 직결
최근 개편으로 노출액이 `Live NAV × effective_kelly` 다(40만 평면 폴백 폐기). 따라서:
- **국면(근거 줄) → kelly_cap → effective_kelly → 매 거래 베팅 금액 → 복리 NAV** 가 한 줄로 연결된다.
- 예: KR NAV 3억, BULL 국면 eff_k 2.8% → 한 종목 노출 ≈ 840만 원. 같은 NAV라도 BEAR(1.0%)면 ≈ 300만 원.

### (C) 공격/방어 축 가중 — 종목 선정 성향
```107:121:meta_governor_consumer.py
    b1 = ra.get("weight_s1_bounds")
    ...
        out1 = min(max(out1, lo), hi)   # S1(공격) 클램프
    b4 = ra.get("weight_s4_bounds")
    ...
        out4 = min(max(out4, lo), hi)   # S4(방어) 클램프
```
국면에 따라 공격축(S1)을 더 키우거나(BULL) 방어축(S4)을 더 키워(BEAR) **어떤 종류의 종목을
고를지**까지 바뀐다.

---

## 4. MetaGovernor 의 '자가 감속' — 글로벌 켈리 배수(META_GLOBAL_KELLY_MULT)

국면과 별개로, **전략 건강(health)** 이 무너지면 메타가 전체 베팅을 일괄 감속한다.

```1129:1137:meta_governor.py
        actionable = [v for v in health.values() if ... n >= min_trades]
        zeroed = sum(1 for v in actionable if float(v.get("mult",1.0)) <= 0.0)
        if actionable and (zeroed / len(actionable)) >= 0.45:
            self._working["META_GLOBAL_KELLY_MULT"] = round(max(0.5, prior_g * 0.88), 4)
        else:
            self._working["META_GLOBAL_KELLY_MULT"] = prior_g
        self._working["META_TREASURY_MODE"] = "DEFENSE" if zeroed > 0 else "NORMAL"
```
- 표본 충분한 전략군 중 **45% 이상이 '죽음(mult≤0)'** 이면 글로벌 배수를 **×0.88로 감속**(하한 0.5).
  → (A)의 `g` 가 작아지므로 **모든 시장의 베팅이 동시에 줄어든다**(연쇄 손실 방어).
- 하나라도 죽으면 `META_TREASURY_MODE=DEFENSE` 로 전환(방어 태세 신호).

---

## 5. 신뢰도(Meta 신뢰도) 산식 — 화면 괄호 안 숫자

```357:365:meta_governor.py
    if merged == "HIGH_VOL":
        conf = 0.88 if ok_ct >= 1 else 0.55
    elif ok_ct >= 2: conf = 0.82      # GSPC·KOSPI 둘 다 신선
    elif ok_ct == 1: conf = 0.62      # 한 지수만 신선
    else: conf = 0.42 if (rk_main or rk_bg) else 0.25
```
- `ok_ct` = 신선한 지수(GSPC, KOSPI) 개수. 둘 다 OK면 0.82, VIX 격상 HIGH_VOL이면 0.88.
- **신뢰도가 낮다 = 데이터가 부실하다**는 뜻 → 보수적으로 해석해야 한다.

---

## 6. 국면별 '기억력' 차등 — 비대칭 Treasury Lookback

```125:131:meta_governor.py
    if rk in ("BEAR", "HIGH_VOL"):  days = 15~20   # 빠르게 켈리 축소(나쁜 기억 빨리 반영)
    if rk == "BULL":                days = ~120    # 길게 신뢰(좋은 흐름 오래 유지)
    if rk == "SIDEWAYS":            days = ~60
```
- 위험 국면(BEAR/HIGH_VOL)은 **최근 15~20일만** 보고 빠르게 방어로 전환,
- 강세장(BULL)은 **최대 120일**까지 보고 느긋하게 공격 유지. → 손실엔 민감, 수익엔 끈기.

---

## 7. 워크드 예제 — "근거 줄" 하나가 베팅을 어떻게 바꾸나

가정: KR Live NAV = 3억, `DYNAMIC_KELLY_RISK=2.0%`, `META_GLOBAL_KELLY_MULT=1.0`.

| 근거 줄 | 판정 국면 | kelly_cap | effective_kelly | 종목당 노출(NAV×eff) | 성향 |
|---------|-----------|-----------|------------------|----------------------|------|
| `REGIME_ANALYSIS=BULL / BITGET_CURRENT=BULL` | BULL | 2.8% | 2.0% (캡 미도달) | **600만 원** | 공격 |
| `... \| VIX>롤링p90 → HIGH_VOL 승격` | HIGH_VOL | 1.2% | **1.2%** (캡에 깎임) | **360만 원** | 방어 |
| `REGIME_ANALYSIS=BEAR / ...` | BEAR | 1.0% | **1.0%** (캡에 깎임) | **300만 원** | 최소 |
| (health 45% 붕괴 시) `g→0.88` | 위와 동일 | — | eff × 0.88 | 추가 12% 감속 | 자가 방어 |

> 같은 자본·같은 종목이라도 **"근거 줄"이 BULL→HIGH_VOL→BEAR 로 바뀌면 베팅이 600→360→300만 원**
> 으로 자동 축소된다. 이게 MetaGovernor가 "근거"를 통해 내 퀀트에 실제로 행사하는 통제력이다.

---

## 8. 읽는 법 / 건강 체크리스트

- **근거가 비어 있고 "notes 미기록"** 으로 뜨면 → MetaGovernor가 최신 상태를 못 쓴 것.
  서버에서 재구축: `python3 -c "from meta_state_store import rebuild_meta_state; print(rebuild_meta_state(force=True))"`
- **`UNKNOWN` / 신뢰도 ≤ 0.42** 가 계속되면 → 지수(GSPC/KOSPI) 수집·REGIME_ANALYSIS 갱신 점검.
- **`VIX 스킵`** 이 매일 뜨면 → VIX 오프라인. 고변동 자동 격상 규칙이 꺼져 있으니 위험.
- **`META_TREASURY_MODE=DEFENSE`** 또는 글로벌 배수가 0.5에 붙어 있으면 → 다수 전략이 죽어 시스템이
  자가 감속 중. 베팅이 평소보다 작게 나가는 게 정상이다.

---

## 9. 한 줄 요약

> `[MetaGovernor 근거]` 의 영어는 **"오늘 국면을 어떤 데이터(주식 지수/코인/VIX)로, 무엇(BULL·BEAR·
> HIGH_VOL…)으로 판정했는가"의 영수증**이다. 그 국면이 **켈리 상한 → (Live NAV×켈리) 베팅 금액 →
> 공격/방어 축 가중치**를 연쇄로 정하고, 전략 건강이 무너지면 글로벌 배수로 전체를 자가 감속한다.
> 즉 이 한 줄은 **내 시스템이 매일 얼마를·어느 방향으로 베팅할지 결정하는 최상위 통제 신호**다.
