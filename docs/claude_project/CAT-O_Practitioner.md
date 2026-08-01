# CAT-O · Practitioner Intelligence

> **위험도** 🟢 Low · **Tier T2** · **also_load**: CAT-J, CAT-M

---

## 1. 역할

PIL (Practitioner Intelligence Layer) reports — per-group practitioner context, LLM narrative, penalty bridge, zombie streak analytics.

---

## 2. Modules

| file | role |
|------|------|
| `practitioner_intelligence.py` | core PIL logic |
| `practitioner_llm.py` | LLM narrative (→ CAT-M) |
| `practitioner_market_profiles.py` | KR/US profiles |
| `practitioner_penalty_bridge.py` | penalty linkage |
| `practitioner_zombie_streak.py` | zombie streak stats |
| `reports/practitioner_report_context.py` | report ctx |

---

## 3. Trigger

`factory_pipelines` steps: `_step_pil_kr`, `_step_pil_us`  
Called from daily audit / deep_dive: `send_group_practitioner_reports`

---

## 4. Claude 설계

- PIL section structure per market
- penalty semantics (link to CAT-I toxic / CAT-F bandit)
- zombie streak definition
- what metrics practitioners see vs operator 9-step report

## 5. Cursor 구현

- context builders, telegram send, LLM call via practitioner_llm

---

## 6. Boundaries

- Read forward_trades, config — **no write** to ledger or Kelly
- LLM via CAT-M sanitize rules

*Report pipeline: CAT-J*
