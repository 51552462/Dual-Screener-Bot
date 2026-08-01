# CAT-C · 스크리닝 & 시그널 생성

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: CONSTANTS, KR-US, MAP  
> **outbound**: CAT-D `try_add_virtual_position`

---

## 1. 역할

유니버스 → 퍼널 → 3D DNA 매칭 → 가상 진입 후보. **듀얼 암**: S1(추세) / S4(눌림).

---

## 2. SSOT & 모듈

| 역할 | 파일 |
|------|------|
| 퍼널 | `scanner_funnel.py` |
| 시너지 | `scanner_synergy_engine.py` |
| 국면 hydrate | `scanner_regime_ssot.py` |
| DNA fallback | `scan_resilience.py` |
| 초신성 | `supernova_hunter.py` |
| 블랙홀 (US) | `blackhole_hunter.py` → short DB |
| arm 분류 | `evolution/deathmatch_report.classify_strategy_arm` |
| 레거시 스캐너 | `legacy_archive/scanners/*` |

---

## 3. 전략 암 (deathmatch)

| arm | source |
|-----|--------|
| UD | underdog |
| C (BEAST) | supernova beast |
| B | supernova cosine/mlbox |
| BH | blackhole |
| A | bowl/nulrim/ema5/dante |
| exclude | INCUBATOR, SCOUT |

---

## 4. sig_type 인벤토리 (대표)

`[SUPERNOVA_COSINE]`, `[SUPERNOVA_MLBOX]`, `[UNDERDOG_MLBOX]`, `[SUPERNOVA_BEAST]`, `[🔭SCOUT]`, `💀TOXIC_TRAP`, `🛑[둠스데이 차단]`, `🔥[눌림] S1/S4/S6/S7`, `STANDARD`, `Dante_REVERSE_BREAKOUT`, `BLACKHOLE`, `[OBSERVE_ONLY]`, `#순환매_선취매`, `[🌐스필오버 선취매]`

---

## 5. 퍼널 단계 (supernova)

```
SKIP_POSITION → DATA_FAIL(20봉) → LIQUIDITY → TOXIC_ML_TREE → DNA_FAIL
→ ANTI_TOXIC → DOOMSDAY_HALT → (match) → FINAL_PASS
```

`ScanFunnelTracker` → `scan_funnel_snapshot` + telegram HTML.

---

## 6. 3D DNA & 매칭

| | formula |
|---|---------|
| cpv | (c−o)/(h−l) |
| tb | (vol/v_ma20)/cpv |
| bbe | (1/bb_width)·vol_mult |

**매칭 경로**: cosine(template cutoff) | ML box | underdog box → fail → SCOUT or DNA_FAIL  
**anti-toxic**: bbox cosine ≥0.85 → TOXIC_TRAP (observe only)

**템플릿 로드 순**: `DNA_SUPERNOVA_*` → evolved → `DNA_ALPHA_*` → **`INCUBATOR_TEMPLATES`** → LIVE_CLUSTER → UNDERDOG_CLUSTER

---

## 7. 시너지 (scanner_synergy_engine)

base cutoff × elastic × synergy (하한: cos 0.72, ML 0.75, bonus max 12)

| trigger | effect | tag |
|---------|--------|-----|
| macro synergy | 완화+bonus | MACRO_SYNERGY |
| rotation pre | ×0.85, +3 | ROTATION_PRE |
| spillover pre | ×0.90, +4 | SPILLOVER_PRE |

소비: `PREDICTED_NEXT_SECTOR_*`, `US_SPILLOVER_SECTOR` (CAT-G/C sector refresh)

---

## 8. 눌림목 S1/S4 (legacy nulrim)

- **S1**: EMA448 완전정배열 돌파 (대세) → WEIGHT_S1
- **S4**: 바닥탈출 텐배거 → WEIGHT_S4
- **S6/S7**: bear 바닥 / 112 중기전환
- gates: moneyOk c·v≥1억, priceOk≥1000원
- DTW 도플갱어: DNA_ALPHA ±10 / DNA_TRAP −30

---

## 9. 유니버스

| | KR | US |
|---|----|----|
| list | `krx_list_survival` 4-tier | `us_list_survival` tiered |
| filter | SPAC/우선주/ETF 강함 | benchmark 제외 |
| fetch | FDR | yf 100 batch |

---

## 10. 섹터·스필오버 모듈

`sector_taxonomy`, `sector_rotation_store`, `sector_spillover_refresh`, `us_kr_theme_bridge`, `cross_market_ssot`

흐름: US 고MFE sector → `US_SPILLOVER_SECTOR` → KR hydrate → synergy tags

---

## 11. 미너·보조

| module | output |
|--------|--------|
| underdog_miner | UNDERDOG_CLUSTER_TEMPLATES |
| smart_money_tracker | SMART_MONEY_RADAR ±3% tag |
| limit_up_forensics | LIMIT_UP_COHORT_DNA |
| forensics_pioneer | virtual_trade_history |
| inverse_etf_sniper | defense signal (CAT-F tail fund) |

---

## 12. supernova 실행 흐름

```
execute_supernova_live_scan(market)
→ session gate + SessionDedup (else offline_rnd_sandbox)
→ doomsday_bridge + hydrate_intraday_scanner_config
→ load templates + cutoffs
→ universe → 15-thread process_live_ticker
→ main: try_add_virtual_position → funnel result
→ post_scan telegram
```

---

## 13. Claude 설계

- cutoff/elastic/synergy 곱셈 정책
- SCOUT vs reject tradeoff
- sig_type taxonomy 확장
- KR master / US blackhole 분기

## 14. Cursor 구현

- thread pool, import legacy scanners, yfinance batch

---

## 15. CAT-D 인터페이스

```python
try_add_virtual_position(..., sig_type=..., market=..., sector=..., ...)
# sizing: sig S1/SUPERNOVA → ×w_s1; S4/눌림 → ×w_s4
```

*상수: CAT-CONSTANTS · KR/US: CAT-KR-US*
