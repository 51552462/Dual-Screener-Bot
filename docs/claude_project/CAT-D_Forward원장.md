# CAT-D · Forward 원장 & 거래 생애

> **위험도** 🔴 High · **Tier T2** · **also_load**: MAP, CONSTANTS  
> **pairs**: CAT-E (exit), CAT-F (sizing at entry)

---

## 1. 역할

`forward_trades` SSOT. OPEN→track→CLOSED 상태머신. 무결성·shadow·dual-track.

---

## 2. SSOT

| file | role |
|------|------|
| **`forward/shared.py`** | init DB, try_add, track_daily, Kelly at entry |
| `forward/ledger.py` | daily exit eval orchestration → CAT-E |
| `forward/deep_dive.py` | daily report → CAT-J |
| `forward/forward_trade_identity.py` | code/name backfill |
| `forward/forward_book_integrity.py` | ghost/zombie detect |
| `forward_dual_track_queries.py` | LIVE/HIST/CHAMPION |
| `shadow_tracking.py` | blocked/virtual history |

---

## 3. forward_trades 스키마 (그룹)

**식별**: id, entry_date, market(KR/US), code, name, sector, sig_type, tier, total_score

**DNA fact**: dyn_rs/cpv/tb, is_tenbagger/top_dna/worst/death_combo, v_cpv/yang/rs/energy

**가격**: entry_price (×1.005 slip), entry_atr, marcap_eok

**생애**: max_high(MFE), min_low(MAE), bars_held, status, exit_*, final_ret, mfe

**Kelly**: sim_kelly_risk_pct, sim_kelly_invest, invest_amount(baseline 2%), shares(FX)

**국면**: entry_regime, entry_cos/dtw_score, tier_effective

**partial exit**: scaled_out_frac, realized_partial_ret, free_runner, pyramid_adds, parent_trade_id

**ABC sim**: live_a, cand_b, champ_c, sim_stat/tech/breadth (+ ret/status each)

~40 idempotent ALTER — **DELETE 금지** (soft close only)

---

## 4. 상태머신

```
try_add [gates] → INSERT OPEN
  (max_high=min_low=entry_price, bars_held=0, entry_regime stamped)
→ track_daily_positions (cron)
  OHLCV update; missing >30d → CLOSED_LOSS -15%
  bars_held++, sim update, → ledger exit eval
→ CLOSED_WIN (>0) / CLOSED_LOSS (≤0) / CLOSED_ZOMBIE / CLOSED_AUTO
→ live_nav_manager.record_closure (CAT-F)
→ template_bandit update (CAT-F)
```

**status query**: always `status LIKE 'CLOSED%'`

---

## 5. entry_regime 각인

```
normalize(META_REGIME_KEY) else resolve_config_regime else CURRENT_REGIME_KEY else UNKNOWN
OBSERVE → OBSERVE_ONLY hardcoded
```

---

## 6. Shadow vs Live

- **blocked_trade_history** — gate에서 차단된 후보
- **shadow_performance_tracker** — 차단 거래 사후 PnL → SHADOW_PERFORMANCE
- shadow PnL 지속 negative → gate **자율 해제** (기회비용)

---

## 7. 무결성

| tool | purpose |
|------|---------|
| forward_trade_identity | unknown name/code fill |
| forward_book_integrity | ghost/zombie |
| repair_forward_trades_numeric_corruption | BLOB → CLOSED_LOSS |

---

## 8. Dual-track

`LIVE_TODAY` / `HIST_BASELINE` / `CHAMPION_ROLLING` — 리포트 스테일 배너

---

## 9. Claude 설계

- 새 status/exit_type enum
- partial exit field semantics
- entry_regime policy
- shadow release threshold

## 10. Cursor 구현

- ALTER migrations, track_daily SQL, transaction boundaries

---

## 11. Inbound / Outbound

- **in**: CAT-C try_add
- **out**: CAT-E ledger, CAT-F record_closure, CAT-J queries

*schema detail: docs/한미_퀀트_전체구조 §5*
