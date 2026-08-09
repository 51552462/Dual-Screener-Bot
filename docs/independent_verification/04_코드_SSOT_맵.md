# 04 · 코드 SSOT 맵 (검증·편향 관련)

> **갱신**: 2026-08-09  
> **용도**: Cursor 구현 · Claude 스펙 시 파일 경로 단일 진실

---

## 1. 검증 층별 파일

### L0 — 구조 스크리닝 (과신 금지)

| 파일 | 역할 |
|------|------|
| `time_machine_backtester.py` | RP-1 15구간 · `LOOKAHEAD_BIAS_WARNING_HTML` |
| `regime_panel_rp1.py` | RP-1 리포트 · `RP1_LOOKAHEAD_NOTICE` |
| `mutant_oos_validator.py` | hall_of_fame → 실데이터 OOS (동일 계보) |
| `synthetic_market.sqlite` | 합성 진화 입력 |
| `docs/work_phases/14_레짐패널_15구간_목표검증.md` | RP-1 SSOT 문서 |

### L1 — 운영 무결성

| 파일 | 역할 |
|------|------|
| `deploy_watch.py` | PASS/WARN/BREAK · F-GATE · C-FUNNEL · F-RETIRE |
| `scripts/deploy_watch.py` | CLI · cron 19:35 KST |
| `meta_treasury_entry_guard.py` | COOLED/RETIRED · kill switch |
| `lifecycle_observe_only.py` | F-RETIRE-02 · $0 paper |
| `forward/forward_book_integrity.py` | OPEN book vs reporter |
| `reports/report_staleness_gate.py` | tier-champion RED 차단 |
| `forward_market_guard.py` | KR/US code↔market scrub |
| `market_session_gate.py` | 정규장 외 스캔 차단 |
| `tests/test_registry_state_entry_gate_f_gate_01.py` | F-GATE-01 |
| `tests/test_lifecycle_observe_only_f_retire_02.py` | F-RETIRE-02 |

### L2 — 포워드 실증

| 파일 | 역할 |
|------|------|
| `forward/ledger.py` | KR/US `forward_trades` |
| `forward/shared.py` | virtual entry · friction overlay |
| `bitget/forward/ledger.py` | `bitget_forward_trades` |
| `bitget/forward/shared.py` | Bitget forward SSOT |
| `auto_forward_tester.py` | facade → forward/ |
| `bitget/forward_tester.py` | facade → bitget.forward |
| `strategy_promotion_engine.py` | LIVE Hard Gate · fast-track |
| `dual_north_star_ledger.py` | G1/G2/G3 · Track A/B |
| `evolution/deathmatch_battle_royale.py` | forward 그룹 N-way |
| `evolution/deathmatch_scorecard.py` | composite · oos_bonus ~3% |

### L3 — 통계 독립 (미완·shadow)

| 파일 | 역할 |
|------|------|
| `validation/walk_forward.py` | purged CV · embargo · DSR 함수 |
| `bitget/validation/walk_forward_bg.py` | Bitget port (import 금지) |
| `bitget/validation/walk_forward_shadow_bg.py` | weekly shadow · `bitget_walk_forward_shadow` |
| `bitget/tests/test_walk_forward_shadow_b3.py` | production 격리 테스트 |
| `bitget/evolution/deathmatch_allocation_shadow.py` | B-2 counterfactual shadow |
| `bitget/tests/test_deathmatch_shadow_b2.py` | B-2 격리 테스트 |
| `re_evolution_redemption_gate.py` | shadow redemption alpha |
| `shadow_performance_tracker.py` | blocked-trade counterfactual KR/US |
| `bitget/shadow_performance_tracker.py` | Bitget blocked-trade |

---

## 2. 비용·마찰

| 파일 | 시장 |
|------|------|
| `proprietary_friction_store.py` | KR/US dynamic penalty |
| `forward/shared.py` L3126+ | entry 시 마찰 적용 |
| `mutant_oos_validator.py` | OOS -1.5% 강제 |
| `bitget/trading/slippage_guard.py` | pre-scan · pre-trade · post-trade |
| `bitget/trading/reconciliation.py` | fill slippage audit |
| `scanner_funnel.py` | funnel → friction store |
| `bitget/infra/proprietary_friction_store_bg.py` | BG regime_friction_event |

---

## 3. 생존·데이터 품질

| 파일 | 역할 |
|------|------|
| `forward/ledger.py` | 30d zombie → -15% |
| `bitget/forward/ledger.py` | 14d delist → -100% |
| `legacy_archive/krx_equity_universe.py` | KR junk filter (legacy) |
| `_tmp_reality_audit_bars.py` | CLOSED null audit (**V-1 → `deploy_watch.reality_audit_check`**) |
| `bitget/data/gap_healer.py` | OHLCV gap |
| `bitget/infra/bounded_reads.py` | bounded SQL · PRI windows |

---

## 4. env / config 키 (검증)

| 키 | default | 의미 |
|----|---------|------|
| `WALK_FORWARD_SHADOW_ENABLED` | policy | BG weekly shadow |
| `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` | **false** | LIVE block (미사용) |
| `OOS_DSR_MIN` | **0** | DSR block (0=report only) |
| `OOS_MIN_EXCESS_ALPHA` | 0.00005 | mutant OOS 합격 |
| `LIFECYCLE_OBSERVE_ONLY_ENABLED` | true | F-RETIRE-02 |
| `DEPLOY_WATCH_PHASE` | — | post_f_gate_01 등 |

전체: `docs/claude_project/CAT-CONSTANTS_상수레퍼런스.md` · `bitget/docs/claude_project/CAT-K_설정SSOT.md`

---

## 5. DB 테이블

| 테이블 | 용도 |
|--------|------|
| `forward_trades` | Track A paper SSOT |
| `bitget_forward_trades` | Track B paper SSOT |
| `bitget_walk_forward_shadow` | WF OOS pass/fail log |
| `bitget_deathmatch_allocation_shadow` | B-2 counterfactual |
| `strategy_registry` / quality_daily | lifecycle · promotion |
| `scan_funnel_snapshot` | C-FUNNEL 집계 |

---

## 6. 문서 교차 참조

| 주제 | claude_project | work_phases | independent_verification |
|------|----------------|-------------|-------------------------|
| Forward 원장 | CAT-D | A-* · 12 | L2 · IV-06 |
| 진화·OOS | CAT-H | B-* · 14 | L0/L3 · IV-01,04 |
| 자본·MDD | CAT-F | A-1~A-2 | L1 |
| Bitget WF | CAT-H (bitget) | B-3 | IV-04,25 |
| 3단계 Done | — | 06 | 06 (방법론) |

---

*갭·sub-phase: `05_갭_및_로드맵.md`*
