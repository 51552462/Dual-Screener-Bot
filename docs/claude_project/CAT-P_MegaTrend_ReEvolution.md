# CAT-P · Mega Trend & Re-Evolution

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: CAT-C (supernova), CAT-E, CAT-J

---

## 1. 역할

Specialized evolution track: mega trend kill chain (ignition→climax→kill) + re-evolution tolerance/strike/redemption — orthogonal to main HTC (CAT-H).

---

## 2. Mega Trend modules

| file | phase |
|------|-------|
| `mega_trend_ignition.py` | entry ignition detect |
| `mega_trend_climax.py` | climax detect |
| `mega_trend_internal_monitor.py` | internal state monitor |
| `mega_trend_internal_kill.py` | internal kill trigger |
| `mega_trend_kill_rl.py` | RL κ/reward for kill |
| `mega_trend_toxic_kill.py` | toxic-pattern kill |
| `mega_trend_trade_filter.py` | trade filter gate |
| `reports/mega_trend_kill_report_section.py` | report [section] |

---

## 3. Re-Evolution modules

| file | role |
|------|------|
| `re_evolution_dynamic_tolerance.py` | dynamic tolerance bands |
| `re_evolution_ev_rampup.py` | EV ramp-up schedule |
| `re_evolution_loser_mutation.py` | loser-side mutation |
| `re_evolution_redemption_gate.py` | redemption after drawdown |
| `re_evolution_strike_guard.py` | strike guard |
| `re_evolution_warm_start.py` | warm start params |
| `re_evolution_zscore_ev.py` | z-score EV gate |

---

## 4. Conceptual flow (Mega Trend)

```
ignition → monitor → climax → kill decision (RL + toxic + filter)
→ affects supernova hold/exit tags
→ report section in daily audit
```

**Claude designs**: RL reward, kill thresholds, redemption gate math  
**Cursor wires**: supernova_hunter hooks, report hydrate

---

## 5. vs CAT-H (HTC)

| | CAT-H | CAT-P |
|---|-------|-------|
| scope | GP/OOS new templates | live mega trend lifecycle |
| output | INCUBATOR_TEMPLATES | kill/filter tags, tolerance |
| schedule | weekend satellite | daily/minute ops |

Do not merge GP promotion (CAT-H) with mega trend kill (CAT-P) in one spec.

---

## 6. Claude 설계

- kill RL reward (giveback, whipsaw analog to CAT-E ratchet)
- redemption gate conditions
- zscore EV thresholds
- tolerance band by regime (coordinate CAT-G)

## 7. Cursor 구현

- supernova integration points, report section, config keys

---

*Scripts: scripts/validate_mega_trend_kill_live.py, mega_trend_kill_live_report.json*
