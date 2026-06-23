# Proprietary Regime Audit
## 내부 마찰 데이터(Friction Data) 기반 UP / DOWN / SIDEWAYS 3-State 엔진 — 코드베이스 감사 보고서

**작성 관점:** 수석 데이터 아키텍트 · 코드 오디터  
**대상 독자:** C-Level / 펀드 오너  
**원칙:** 나스닥·비트코인 등 **외부 벤치마크 없이**, 우리 파이프라인이 스스로 생성·축적한 데이터만으로 국면을 3등분  
**감사 일자:** 2026-06-11  
**코드 SSOT:** `forward_trades` · `meta_governor_state` · `system_config.sqlite` · 스캐너 퍼널 · 데스매치 스냅샷

---

## Executive Summary

현재 시스템은 **국면 판정의 공식 SSOT가 여전히 외부 지수에 기대고 있다** (`regime_meta_analyzer.py` → SPX/KOSPI/VIX).  
반면 **내부 마찰 데이터는 이미 풍부하게 생산·저장**되고 있으며, MetaGovernor·Elastic Threshold·PIL·데스매치가 이를 간접적으로 소비한다.

| 구분 | 평가 |
|------|------|
| **당장 구현 가능** | 장부 활력도(MFE/MAE/bars_held), Meta 스트레스(Kelly·Treasury), Elastic 기아지수, 데스매치 DB 스냅샷, flow_tags |
| **부분 가능 (파싱 필요)** | 스캔 퍼널 밀도 (텔레그램·로그만 존재, DB 미적재) |
| **맹점 (로깅 추가 필요)** | 슬롯별 퍼널 시계열, DM-A 발생 이벤트, 정찰병 발동 카운터 SSOT |

**권고:** 외부 `REGIME_ANALYSIS`를 **읽기 전용 레퍼런스**로 격하하고,  
`Proprietary_Regime_Index` (내부 복합 Z-Score)를 Shadow 모드로 병행 산출 → 90일 검증 후 MetaGovernor 입력을 교체한다.

---

## 1. Mission 1 — 내부 데이터 소스 전수 조사

### 1.1 스캐닝 밀도 (Scan Density)

#### 무엇을 측정할 수 있는가
장중 스캐너가 **유니버스 대비 커트라인 통과 종목 수**와 **단계별 탈락률** — 시장이 “신호를 허용하는지”의 1차 프록시.

#### 코드상 추출 경로

| 소스 | 위치 | 추출 가능 지표 | 지속성 |
|------|------|----------------|--------|
| `ScanFunnelTracker` | `scanner_funnel.py` | `universe`, 단계별 `count_surviving`, `drop_summary`, `survivors_final`, `top_n_display` | **런타임만** — `finalize()` 후 텔레그램 발송 |
| `post_scan_funnel_telegram` | `scanner_funnel.py` | 슬롯별 HTML 리포트 | 로그·텔레그램 이력 |
| `supernova_hunter.py` | 초신성 라이브 스캔 | `funnel.drop("DNA_FAIL")` 등 단계별 탈락, `DOOMSDAY_HALT` | 동일 (비영속) |
| `ElasticThreshold.apply_pair` | `elastic_threshold.py` | 당일 effective `cos_cutoff`, `ml_cutoff`, `stretch_factor` | `system_config` 스냅샷·fluid bridge 로그 |
| `DYNAMIC_SUPERNOVA_CUTOFF` | `system_config` / `config_snapshots/` | 일별 커트라인 수준 | **영속** (일별 JSON) |

#### 핵심 수식 (제안)

```
Pass_Rate_t = |survivors_final| / universe
ΔPass_Rate_5d = Pass_Rate_t − median(Pass_Rate_{t−5..t−1})
Funnel_Entropy_t = −Σ p_i log(p_i)   # p_i = drop_reason_i / total_drops
```

- **Pass_Rate 급증** → 시스템이 “좋은 종목”을 많이 허용 = UP 마찰 신호  
- **Pass_Rate ≈ 0 연속 N슬롯** → DOWN 마찰 신호  
- **Universe는 크지만 survivors는 0~1, Entropy 낮음** → SIDEWAYS (필터만 작동, 체결 없음)

#### 맹점
- 퍼널 결과가 **DB/JSON에 구조화 저장되지 않음**.  
- `logs/factory_scan_*.log` 파싱은 가능하나 취약하다.  
- **권장:** `scan_funnel_daily` 테이블 또는 `SCAN_FUNNEL_SSOT.json` (슬롯·시장·pass_rate·drops) 일 1행 append.

---

### 1.2 장부 활력도 (Ledger Vitality)

#### 무엇을 측정할 수 있는가
가상 장부 `forward_trades`가 시장과 싸우며 남긴 **체감 체온** — 보유 기간, 최대 유리/불리 변동, 청산 속도.

#### 스키마 (SSOT: `forward/shared.py` → `init_forward_db`)

| 컬럼 | 의미 | 갱신 주체 |
|------|------|-----------|
| `bars_held` | 보유 일수(봉) | `forward/ledger.py` `track_daily_positions` |
| `max_high`, `min_low` | 보유 중 극값 | 동일 (일별 OHLCV 반영) |
| `mfe` | 청산 시 `(max_high−entry)/entry×100` | 청산 시 확정 |
| `entry_price`, `final_ret` | 진입·결과 | 진입/청산 |
| `flow_tags` | `#빠른슈팅_완벽`, `#슈팅실패_조기소멸` 등 | `ledger.py` 규칙 기반 |
| `market_breadth`, `entry_breadth` | 진입 시점 시장 폭 | 진입 시 박제 |
| `exit_type` | `STAT_MFE`, `STAT_MAE`, `ZOMBIE_FORCE_CLOSE` 등 | 청산 엔진 |
| `status` | `OPEN` / `CLOSED_*` | 생명주기 |

#### 파생 지표 (당장 SQL/Pandas로 계산 가능)

```sql
-- OPEN 포지션 실시간 MFE/MAE (%)
mfe_live = (max_high - entry_price) / entry_price * 100
mae_live = (min_low  - entry_price) / entry_price * 100

-- 활력도 패널 (최근 7일 청산)
avg_mfe_closed_7d = AVG(mfe) WHERE status LIKE 'CLOSED%' AND exit_date >= ...
avg_bars_held_open = AVG(bars_held) WHERE status='OPEN'
bars_held_var_20d  -- system_auto_pilot.py 엔진 5.5에서 이미 사용 중
```

#### 이미 연결된 내부 로직

| 모듈 | 사용 방식 |
|------|-----------|
| `system_auto_pilot.py` | 14일 MAE/MFE → `DYNAMIC_MAE_SL` / `DYNAMIC_MFE_TP` 스무딩 |
| `system_auto_pilot.py` | `bars_held` 분산 급증 + `entry_breadth` 급락 → 알파 반감기 방어 |
| `practitioner_intelligence.py` | `vitality_score`, `stale_hold_ratio`, `turnover_30d`, `is_zombie` |
| `regime_meta_analyzer.py` | `fetch_colosseum_summary()` — 최근 45일 청산 `sum_ret` by logic |

#### flow_tags 기반 서사 (규칙 라벨 → 국면 힌트)

| 태그 패턴 | 해석 |
|-----------|------|
| `#빠른슈팅_완벽`, `#초신성_광기폭발_성공` | UP — 추세가 포지션에 순풍 |
| `#슈팅실패_조기소멸`, `#가짜초신성_수급불발` | DOWN — 신호는 있으나 체결 후 실패 |
| `#지연슈팅_수명연장` + `bars_held`↑ | SIDEWAYS — 방향 없이 시간만 소모 |

---

### 1.3 메타 스트레스 (Meta Stress)

#### 무엇을 측정할 수 있는가
관제탑(MetaGovernor)이 **스스로 방어 모드로 수축하는 속도** — 외부 지수 없이도 “시스템이 스스로 위험하다고 느끼는지”를 읽을 수 있다.

#### 추출 경로

| 지표 | 저장 위치 | 추출 방법 |
|------|-----------|-----------|
| `META_GLOBAL_KELLY_MULT` | `meta_governor_state.json`, `META_GOVERNOR_STATE` KV | 현재값 + `META_CHANGELOG` 시계열 |
| `META_GROUP_KELLY_MULT` | 동일 | 그룹별 `mult` 감쇠 속도 |
| `META_STRATEGY_HEALTH` | meta state | `mult`, `wr`, `mdd`, `n_trades` per group |
| `META_TREASURY_MODE` | meta state | `NORMAL` vs `DEFENSE` |
| `META_PIL_ZOMBIE_STREAK` | meta state | 좀비 연속 일수 |
| `META_RETIRED_STRATEGY_IDS` | meta state | 도태 이벤트 누적 |
| DM-A (청산 0건) | `forward/deathmatch_report_section.py` `_tier_dm_a` | **일일 리포트 시점** `n_closed==0` — 이벤트 DB 없음 |
| 데스매치 arm 스냅샷 | `deathmatch_arm_snapshot` 테이블 | `n_closed`, `mean_ret`, `rank` 일별 |
| `deathmatch_elimination_event` | SQLite | arm 탈락 이벤트 |
| `config_snapshots/system_config_YYYYMMDD.json` | 디스크 | `DYNAMIC_*` 커트라인 역사 |

#### Kelly 감쇠 속도 (제안 수식)

```
Kelly_Velocity_7d = (GKM_t − GKM_{t−7}) / 7
Stress_Score = clip(1 − GKM_t, 0, 1) + 0.3·𝟙[TREASURY_MODE=DEFENSE] + 0.2·Zombie_Rate
```

- `META_GLOBAL_KELLY_MULT`가 1.0 → 0.5로 **가파르게 하락** = DOWN 내부 스트레스  
- `DEFENSE` + 다수 그룹 `mult<1` = DOWN 확인  
- Kelly 유지 + champion `hurdle_passed` = UP

#### DM-A 빈도
`_tier_dm_a`는 롤링 윈도우 내 `df_closed`가 0일 때 발동 (`forward/deathmatch_report_section.py`).  
**당장 가능:** `daily-kr` / `daily-us` 실행 시 플래그를 shadow JSON에 append.  
**맹점:** 과거 DM-A 빈도 역추적은 리포트 로그 파싱 없이는 불가.

---

### 1.4 유동적 진화 압력 (Elastic / Scout)

#### 무엇을 측정할 수 있는가
표본 기아(sample starvation) 시 시스템이 **커트라인을 얼마나 당기고, 정찰병을 얼마나 투입하는지**.

#### 추출 경로

| 지표 | 모듈 | 설명 |
|------|------|------|
| `starvation_index` | `elastic_threshold.py` `compute_starvation_index()` | 0=표본 충분, 1=극심한 기아. 진입/청산 수 vs 주간 목표 |
| `scout_gap`, `cos_cutoff`, `ml_cutoff` | `ElasticThresholdState` | 탄력 커트라인 상태 |
| 정찰병 진입 | `supernova_hunter.py` | `evaluate_scout_candidate` → `trade_source: FLUID_SCOUT` |
| Scout sig 마커 | `evolution/fluid_evolution_bridge.py` | `🔭SCOUT`, `COSINE_SCOUT`, `MLBOX_SCOUT` |
| Forgiveness scout | `toxic_decay_bandit.py` | `forgiveness_scout` 게이트 |

#### starvation_index 내부 구성 (이미 구현됨)

```
ent_gap  = 1 − clip(recent_entries / TARGET_ENTRIES, 0, 1)
cl_gap   = 1 − clip(recent_closed   / TARGET_CLOSED, 0, 1)
stagnation = 0.35 if (n_open>0 and ent==0) else 0
starvation_index = 0.45·ent_gap + 0.45·cl_gap + stagnation
```

#### Scout 카운트 (당장 가능)

```sql
SELECT COUNT(*) FROM forward_trades
WHERE entry_date >= ? AND (
  sig_type LIKE '%SCOUT%' OR trade_source IN ('FLUID_SCOUT','SCOUT')
)
```

**맹점:** Scout **발동 시도 vs 실제 등재**가 분리되어 있지 않음. 퍼널/텔레그램만으로는 “전멸” 카운트가 불완전.

#### 외부 누수 주의
`ElasticThreshold.volatility_proxy()`는 KR `069500` / US `SPY` 수익률 표준편차를 사용 — **내부-only 원칙과 충돌**.  
Proprietary 엔진에서는 `forward_trades` 수익률 분산 또는 OPEN `mfe` 분산으로 대체해야 한다.

---

### 1.5 부록 — 추가 발굴 내부 지표

| 지표 | 소스 | 국면 관련성 |
|------|------|-------------|
| `DOOMSDAY_DEFCON.level` | `system_config` | ≤1이면 스캔 전면 `DOOMSDAY_HALT` — 극단 DOWN |
| `INVERSE_MODE_ACTIVE` | config | 방어 모드 활성 — DOWN |
| `deathmatch_champion` | SQLite | champion 교체 빈도 — regime shift |
| `meta_state_log` | `market_data.sqlite` | `META_REGIME_KEY` 이력 (외부 입력의 결과이긴 하나 내부 타임라인) |
| `SHADOW_PERFORMANCE` | config | 차단 신호 사후 검증 — 품질 프록시 |
| Colosseum `sum_ret` by logic | `regime_meta_analyzer.fetch_colosseum_summary` | 전략 생태계 건강도 |
| Weekly Flow PnL | `weekly_flow_report.py` | 주간 집계 (느린 확인) |

---

## 2. Mission 2 — 3-State (UP / DOWN / SIDEWAYS) 매핑 시나리오

### 2.1 설계 원칙

1. **모든 입력은 상대값(Z-Score / 변화율)** — 절대 임계값은 시장(KR/US)별 프로필로 분리  
2. **복합 점수 1개**로 압축 후 3분위 또는 σ-밴드로 분류  
3. **HIGH_VOL은 별도 플래그**로 두고, 3-State와 직교(orthogonal) 처리 권장 — 내부 `bars_held` 분산·`exit_type=STAT_MAE` 비율로 감지

### 2.2 제안 복합 지표: `Proprietary_Regime_Index` (PRI)

```
PRI_t = w1·Z(ΔPass_Rate) + w2·Z(Avg_MFE_7d) + w3·Z(Avg_MAE_7d)
      + w4·Z(META_GLOBAL_KELLY_MULT) + w5·Z(−starvation_index)
      + w6·Z(Closed_per_week)
```

권장 가중치 (초기 Shadow):  
`w1=0.20, w2=0.25, w3=0.20, w4=0.15, w5=0.10, w6=0.10`

| PRI 구간 (σ 기준) | 국면 | C-Level 한 줄 |
|-------------------|------|----------------|
| PRI > +0.5σ | **UP** | 신호 통과↑, MFE 순풍, Kelly 유지 |
| PRI < −0.5σ | **DOWN** | 통과율 붕괴, MAE 확대, Kelly 감쇠 |
| 그 외 | **SIDEWAYS** | 장부는 살아있으나 방향성·청산 모두 정체 |

---

### 2.3 UP (상승장) — 내부 시그니처

| 차원 | 조건 (예시) | 근거 코드 |
|------|-------------|-----------|
| 스캔 | `Pass_Rate` 5일 이동평균 대비 **+1σ 이상** | `ScanFunnelTracker` |
| 장부 | 최근 7일 청산 `AVG(mfe) > 3%`, `AVG(mae_live) > −2%` | `forward_trades` |
| 메타 | `META_GLOBAL_KELLY_MULT ≥ 0.85`, `TREASURY_MODE=NORMAL` | `meta_governor.py` |
| 데스매치 | ranked arms ≥ 2, champion `mean_ret > 0` | `deathmatch_arm_snapshot` |
| Elastic | `starvation_index < 0.35`, Scout 주간 **≤ 1건** | `elastic_threshold.py` |
| 태그 | `#빠른슈팅_완벽` 비중 ↑ | `flow_tags` |

**내부 서사:** “스캐너가 문을 열고, 장부가 빠르게 수익을 내고, 관제탑이 방어를 걸지 않는다.”

---

### 2.4 DOWN (하락장) — 내부 시그니처

| 차원 | 조건 (예시) | 근거 코드 |
|------|-------------|-----------|
| 스캔 | **연속 3슬롯** `survivors_final=0` 또는 `Pass_Rate < 0.1%` | 퍼널 (로깅 필요) |
| 장부 | OPEN `mae_live` 평균 **< −5%**, `exit_type=STAT_MAE` 비율 ↑ | `ledger.py` |
| 메타 | `Kelly_Velocity_7d < −0.05/일`, `DEFENSE` | `META_CHANGELOG` |
| 데스매치 | **DM-A** (청산 0) 주 2회 이상 | daily report |
| Elastic | `starvation_index > 0.7` 이지만 Scout 전환율 **< 10%** | 장부 + hunter |
| 방어 | `DOOMSDAY_DEFCON ≤ 2` 또는 `INVERSE_MODE_ACTIVE` | config |

**내부 서사:** “신호가 막히거나, 열린 포지션이 MAE로 짓눌리고, 관제탑이 Kelly를 깎으며, 정찰병도 살아남지 못한다.”

---

### 2.5 SIDEWAYS (횡보장) — 내부 시그니처

| 차원 | 조건 (예시) | 근거 코드 |
|------|-------------|-----------|
| 스캔 | `Pass_Rate` 변동 **|Δ| < 0.3σ** — 방향 없음 | 퍼널 |
| 장부 | `AVG(bars_held)` OPEN **상승 추세**, 주간 `CLOSED < 2` | `forward_trades` |
| 메타 | Kelly **횡보** (|ΔGKM_7d| < 0.05), DM-C (관망 arm 다수) | meta + deathmatch |
| Elastic | `starvation_index` 중간(0.4~0.7), stagnation=0.35 플래그 | `compute_starvation_index` |
| 분산 | `bars_held` 분산 20일 **급증** (auto_pilot 엔진 5.5) | `system_auto_pilot.py` |

**내부 서사:** “청산은 거의 없고 보유만 길어지며, 데스매치는 표본 기아로 순위를 못 매긴다.”

---

### 2.6 KR / US 분리

동일 PRI 공식이되 **시장별로 Z-Score 기준창을 분리**한다.

| 시장 | 스캔 SSOT | 장부 필터 | 비고 |
|------|-----------|-----------|------|
| KR | `scan-kr-*` 퍼널 | `market='KR'` | 장중 50분 슬롯 |
| US | `scan-us-*` 퍼널 | `market='US'` | ET cron |

통합 PRI는 `0.5·PRI_KR + 0.5·PRI_US` 또는 운용 비중 가중.

---

## 3. Mission 3 — 구현 가능성 · 맹점 · 로드맵

### 3.1 당장 구현 가능 (코드 변경 최소)

| # | 모듈 | 작업 | 예상 공수 |
|---|------|------|-----------|
| A | `proprietary_regime_engine.py` (신규) | `forward_trades` + meta state → PRI + UP/DOWN/SIDEWAYS | 1~2일 |
| B | `shadow` hook | `daily-kr/us` [9/9] 직후 PRI 블록 append (기존 shadow 패턴) | 0.5일 |
| C | SQL 뷰 | OPEN vitality / 7d MFE·MAE 집계 뷰 | 0.5일 |
| D | `meta_state_log` | `META_GLOBAL_KELLY_MULT` 시계열 export | 0.5일 |

**데이터 의존:** 100% 내부 (단, Elastic vol_proxy 교체 전까지는 B급 외부 누수 1건 존재).

---

### 3.2 맹점 (Blind Spots) — 반드시 로깅 추가

| 맹점 | 현재 상태 | 권장 조치 |
|------|-----------|-----------|
| **스캔 퍼널 시계열** | 텔레그램·메모리만 | `scan_funnel_snapshot` 테이블: `ts, market, scanner, universe, survivors, pass_rate, drops_json` |
| **DM-A 이벤트** | 리포트 문자열만 | `regime_friction_event` 테이블: `date, market, event='DM_A_ZERO_CLOSED'` |
| **Scout 발동/실패** | 부분적으로 `forward_trades` | `scout_event_log`: `ts, market, path, eligible, reason, enrolled` |
| **슬롯별 latency** | `elapsed_min` in report only | 퍼널 snapshot에 포함 |
| **외부 regime 혼선** | `REGIME_ANALYSIS`가 공식 입력 | Shadow PRI와 **AB 라벨** 90일 적재 후 전환 결정 |
| **Elastic vol_proxy** | SPY/069500 사용 | `OPEN mfe` 분산 또는 청산 `final_ret` 롤링 σ로 교체 |

---

### 3.3 기존 외부 의존 맵 (교체 대상)

```
regime_meta_analyzer.analyze_market_regime()
  └─ yfinance: ^GSPC, ^KS11, ^VIX  ← 교체 대상 #1

meta_governor._step_regime()
  └─ REGIME_ANALYSIS.regime_key   ← 위 분석기 출력 소비

elastic_threshold.volatility_proxy()
  └─ SPY / 069500                 ← 교체 대상 #2
```

**내부-only 달성 시:** `_step_regime()`이 `PROPRIETARY_REGIME_SHADOW` KV를 읽도록 분기 (실전 반영 전).

---

### 3.4 C-Level 결단 체크리스트

- [ ] **Shadow 90일:** PRI vs 실현 PnL 상관계수 > 0.3 목표  
- [ ] **퍼널 DB 적재** 착수 — Scan Density 없이는 UP 조기탐지 약함  
- [ ] **DM-A 이벤트 로그** — DOWN 탐지 정밀도 상승  
- [ ] **Elastic vol_proxy 내부화** — 외부 누수 제거  
- [ ] 검증 후 `META_REGIME_KEY` 입력을 PRI로 **단계적 교체** (한 번에 스위치 금지)

---

## 4. 참고 — 핵심 파일 인덱스

| 파일 | 역할 |
|------|------|
| `forward/shared.py` | `forward_trades` 스키마 |
| `forward/ledger.py` | `bars_held`, MFE/MAE, `flow_tags` 갱신 |
| `scanner_funnel.py` | 스캔 퍼널 SSOT (비영속) |
| `elastic_threshold.py` | 기아지수·정찰병·탄력 커트 |
| `supernova_hunter.py` | 라이브 스캔·Scout 등재 |
| `meta_governor.py` | Treasury·Regime·Kelly |
| `forward/deathmatch_report_section.py` | DM-A/B/C 티어 |
| `evolution/deathmatch_store.py` | `deathmatch_arm_snapshot` |
| `practitioner_intelligence.py` | Vitality·Zombie |
| `regime_meta_analyzer.py` | **현행 외부 regime** (교체 대상) |
| `system_auto_pilot.py` | bars_held 분산·breadth 방어 |
| `config_snapshots/` | 일별 커트라인 블랙박스 |

---

## 5. 한 페이지 요약

우리는 이미 **세계급 헤지펀드급 마찰 데이터**를 쌓고 있다.  
다만 그 데이터가 **국면 판정의 주입 파이프**에 연결되지 않고, 텔레그램·Shadow·일부 튜닝 엔진에만 흩어져 있다.

**UP** = 통과율↑ + MFE↑ + Kelly 유지  
**DOWN** = 통과율 붕괴 + MAE↑ + Kelly 감쇠 + DM-A  
**SIDEWAYS** = 청산 기아 + bars_held↑ + 관망 arm 다수  

가장 큰 공백은 **스캔 퍼널의 구조화 저장**이다. 이것만 메우면 외부 지수 없이도 3-State 엔진을 **2주 내 Shadow 가동**할 수 있다.

---

*본 문서는 코드 정적 감사 기준이며, 운영 서버의 실측 분포는 `dry-run` PRI 백필로 검증할 것.*
