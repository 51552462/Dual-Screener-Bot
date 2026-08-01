# CAT-J · 리포팅 & 알림

> **위험도** 🟢 Low–Medium · **Tier T2** · **also_load**: MAP (read-only to D/F/G)  
> **read policy**: report DB → MAIN forced (CAT-B)

---

## 1. 역할

9-step daily report, weekly Flow master, telegram delivery, executive summary, PIL practitioner reports.

---

## 2. SSOT

| file | role |
|------|------|
| **`forward/deep_dive.py`** | send_comprehensive_daily_report |
| `reports/*` | context, format, tier, staleness, collectors |
| `weekly_flow_report.py` | weekly master |
| `weekly_flow_pnl.py` | realized PnL SSOT |
| `weekly_flow_rollup.py` | DNA/flow tags |
| `weekly_action_plan.py` | Bayesian toxic + Gemini tail |
| `weekend_grand_report.py` | weekend |
| `report_executive_summary.py` | exec summary |
| `report_pipeline_hydrate.py` | pre-flight hydrate |
| `report_date_utils.py` | date SSOT |
| `telegram_*` | queue, HTML, env |
| `async_telegram_daemon.py` | async daemon |

---

## 3. Daily report pre-flight

macro/OHLCV hydrate → rebuild_meta_state → sector spillover → sentiment → doomsday → zombie cleanup

Then loop KR + US — **each section separate telegram**.

---

## 4. Nine sections ([1/9]–[9/9])

| # | title | builder |
|---|-------|---------|
| 1 | 거시 국면 & Live NAV | format_macro_treasury_section_html |
| 2 | 로직별 복리 리더보드 | inline group PnL/WR/PF top-15 |
| 3 | Kelly vs 고정 대결 | CapitalDeathmatchAnalyzer |
| 4 | 섹터 다변화 | VIP fleet, 20-position warn |
| 5 | 티어 & 데스콤보 | filter_tier_80, filter_death_combo |
| 6 | DNA 부검 | build_dna_autopsy_slice |
| 7 | 순환매 & 스필오버 | build_rotation_spillover_section |
| 8 | 메타 & 알파 반감기 | format_lifecycle_section_html |
| 9 | 데스매치 결산 | build_deathmatch_section + shadow macro |

Post: Evolution Δ digest, Executive Summary  
Side: `run_deep_dive_analysis`, `send_group_practitioner_reports` (PIL)

---

## 5. Weekly Flow

`WeeklyFlowMasterSnapshot` 7-day window  
PnL: `row_notional` = sim_kelly_invest → invest_amount → live_notional  
`compute_virtual_equity_curve` compound  
**Never skip** even if 0 trades.

---

## 6. Telegram 3-layer

1. Persistent queue `message_queue.sqlite::msg_queue` (MAIN/PROMO, send_profile)
2. HTML SSOT `telegram_html_delivery` — 400 → plain retry
3. Credentials `telegram_env` — **env only**, never config_kv secrets

Channels: `EQUITY_KR_*` / `EQUITY_US_*` → `*_FACTORY_CHAT_ID`  
Async: `DANTE_ASYNC_TELEGRAM_DAEMON=1`

---

## 7. reports/ package

| module | role |
|--------|------|
| daily_report_context | daily ctx |
| practitioner_report_context | PIL ctx |
| colosseum_report_context | colosseum |
| report_collectors | data gather |
| report_state_binder | state bind |
| report_staleness_gate | stale banner |
| forward_report_tier/scalar | tier metrics |
| mega_trend_kill_report_section | CAT-P section |

---

## 8. Claude 설계

- section KPI definitions
- narrative structure
- KR/US channel policy
- weekly Flow metrics semantics
- when to add/remove sections

## 9. Cursor 구현

- HTML escape, hydrate order, telegram API, collector SQL

---

## 10. Schedule (CAT-A)

| | KR | US |
|---|----|----|
| daily_audit | 18:45 KST M–F | 06:45 KST Tu–Sat |
| weekly_master | Sat 10:05 | included |

*PnL math: CAT-F NAV · regime display: CAT-G*
