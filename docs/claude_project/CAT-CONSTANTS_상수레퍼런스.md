# CAT-CONSTANTS · 핵심 상수 레퍼런스

> **Tier T1** — 숫자·임계값 설계·검증 시 @멘션. 개별 CAT 파일에 상수 중복 없음.

---

## 스케줄 (CAT-A)

| 상수 | 값 |
|------|-----|
| SLOT_INTERVAL_MINUTES | 50 |
| SLOT_START | 10:00 local |
| SCAN_LOCK_WAIT_SEC | 3300 |
| WARMUP_DAYS | 14 |
| lock max-age | 7200s |
| watchdog miss | 100s × 3 → restart |
| CYCLE2_CLOSE_MARGIN | 5min before close |
| US slot match window | ±4min ET |

---

## 스캔 (CAT-C)

| 상수 | 값 |
|------|-----|
| DYNAMIC_SUPERNOVA_CUTOFF | 0.50 |
| DYNAMIC_ML_BOX_CUTOFF | 0.50 |
| TREE_FATAL_CPV | 0.85 |
| anti-toxic cosine | 0.85 |
| synergy min cos mult | 0.72 |
| synergy min ML mult | 0.75 |
| synergy max score bonus | 12.0 |
| rotation pre mult | ×0.85, +3.0 |
| spillover pre mult | ×0.90, +4.0 |
| scan threads | 15 |

---

## 3D DNA (CAT-C)

| 요소 | 정의 |
|------|------|
| cpv | (c−o)/(h−l) |
| tb | (vol/v_ma20)/cpv |
| bbe | (1/bb_width)·vol_mult |

---

## 리스크·Kelly (CAT-F)

| 상수 | 값 |
|------|-----|
| RISK_PCT | 0.02 |
| ACCOUNT_SIZE | 20,000,000 |
| MAX_POSITION_PCT | 0.25 |
| kelly global clamp | [0.002, 0.030] |
| UNKNOWN kelly | [0.005, 0.022] |
| OPEN per market | 20 |
| per logic / day | 4 |
| sector quota | 주도 2 + 차기 2 |
| GLOBAL_CIRCUIT_BREAKER | 일일 −5% |
| MAB_EXPLOIT_RATIO | 0.70 |
| template mult range | [0.10, 2.00] |
| GP explore arm | 0.10 |
| toxic half-life | 45d |
| deathmatch mdd_penalty weight | 0.32 |
| champion alloc mult | ×1.35 |
| US FX | 1350 |

### ACTION_BY_REGIME kelly_cap

| regime | cap |
|--------|-----|
| HIGH_VOL | 0.012 |
| BEAR | 0.010 |
| BULL | 0.028 |
| SIDEWAYS | 0.018 |
| UNKNOWN | 0.015 |

---

## 청산 (CAT-E)

| 상수 | 값 |
|------|-----|
| TIME_STOP | 10 bars |
| ATR_SL | 2.0 |
| MAE_SL | −3.5% |
| MFE_TP | +10.0% |
| breadth emergency | <0.97 → stops×0.5 |
| kappa range | 0.05–0.12 |
| F_out DEF/BULL/CHOP | 0.78 / 0.18 / 0.45 |
| RL eta (ratchet) | 0.04 |
| zombie force | bars ≥ time_stop×2 |
| delisted auto close | −15% after 30d no data |

---

## 국면 앙상블 (CAT-G)

| 상수 | 값 |
|------|-----|
| MACRO_ANCHOR_FLOOR | 0.15 |
| PRI_WEIGHT_CAP | 0.85 |
| UP/DOWN threshold | ±0.18 |
| SOFTMAX_BETA | 6.0 |
| VIX_MID / SCALE / PANIC | 18 / 8 / 30 |
| FWD_EVAL_DAYS | 5 |
| SKILL_EMA_BETA | 0.20 |
| HYSTERESIS_DAYS | 2 |
| meta trust W range | [0.20, 0.80] |
| doomsday γ range | [0.5, 3.0] |
| VIX tail fund trigger | 35 → ×30 payoff |

---

## HTC 진화 (CAT-H)

| 상수 | 값 |
|------|-----|
| GP POPULATION | 1000 |
| MIN_SIGNALS (incubator rank) | 50 |
| synthetic universes | 100×1000d |
| price clip | ±30% |
| OOS min bars / samples | 130 / 100 |
| PROMOTE wr / excess / n | 0.50 / 0.00005 / 30 |
| GP_MUT_MAX_LIVE | 24 |
| immune cap / centroids | 500 / 64 |

---

## Toxic (CAT-I)

| 상수 | 값 |
|------|-----|
| graveyard KR loss | ≤−7% or (STAT_MAE\|ZOMBIE & ≤−4%) |
| bbox match cosine | ≥0.85 |
| decay block_floor | 0.55 |
| forgive_ceil | 0.35 |
| TTL | 90d |

---

## 듀얼암 기본 (system_auto_pilot)

| | 값 |
|---|-----|
| WEIGHT_S1 | 1.0 |
| WEIGHT_S4 | 1.0 |

---

*변경 시: 해당 CAT Handoff에 old→new 명시 + CAT-CONSTANTS 갱신 요청*
