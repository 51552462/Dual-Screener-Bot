# CAT-M · LLM 오버시어 (Bitget)

> **위험도** 🟢 Low · **Tier T2** · **also_load**: CAT-K, MAP  
> **never_with**: CAT-F Kelly production path (non-invasive only)

---

## 1. 역할

AI overseer daily/weekly, Gemini sentiment, structured proposals, non-invasive audit hooks.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| overseer | `ai_overseer.py` |
| proposal schema (D-1) | `governance/ai_proposal_schema_bg.py` |
| proposal summary (D-1b) | `observability/llm_proposal_summary_bg.py` |
| proposal approval (D-2) | `governance/proposal_approval_bg.py` |
| proposal poll (D-2) | `governance/proposal_approval_poll_bg.py` · hook `ai_overseer` |
| cost report (D-3a) | `observability/cost_report_bg.py` · hook `weekly_evolution` |
| parity scaffold (D-3b) | `observability/parity_monitor_bg.py` · **no hook** (dormant) |
| approval log table | `bitget_llm_proposal_approvals` (append-only) |
| proposal log table | `bitget_llm_proposals` (market_data SQLite) |
| gemini core | root `llm_gemini_core` (read-only import) |
| pipeline step | `daily_audit` overseer step |
| sentinel | `sentinel.py` |

---

## 3. 비침투 원칙 (CAT-MAP)

- LLM **cannot** directly write config_kv
- proposals → human or structured JSON → approval path (P2-4)
- no auto-apply Kelly/regime changes without gate
- **D-1 validate 분기** (2026-08-04):
  - **parse fail** (JSON 있으나 invalid) → `ops_events.llm_proposal_parse_error` + telegram · **미저장**
  - **no JSON block** (free-text 감사만) → **silent skip** — parse fail 아님, ops/telegram 없음

---

## 4. Claude 설계 대상 (work_phases 묶음D)

- D-1: structured JSON proposal schema ✅
- D-1b: weekly proposal count/risk_class + parse_error_rate observability ✅
- D-2: telegram approval command gate ✅ (`proposal_approval_bg.py`)
- D-3a: weekly cost report ✅ (`cost_report_bg.py`) · D-3b parity scaffold ✅ (dormant)

### D-1b rate null 구분 (2026-08-04)

| 필드 | 모듈 | null 의미 |
|------|------|-----------|
| `skip_rate_pct` | C-1b `bad_tick_skip_summary` | ops_events에 **scan-count 분모 없음** (외부 카운터 미기록) |
| `parse_error_rate_pct` | D-1b `llm_proposal_summary` | window 내 **structured parse 시도 0건** (`total_count + parse_error_count = 0`) |

분모가 있으면 각각 계산: C-1b는 scan 이벤트 기반, D-1b는 `성공 persist + parse_error` 합.

### D-3a `gemini_call_count` proxy (2026-08-04)

| 필드 | 의미 |
|------|------|
| `gemini_call_count` | window 내 Gemini 관련 **호출 추정치** (전용 ops 이벤트 없을 때 **캐시 DB 행 수**) |
| `gemini_call_count_source` | `ops_events` \| `llm_call_cache_proxy` \| `none` — **실 API billing 호출 수와 동일하지 않을 수 있음** |
| `gemini_cost_estimate_usd` | `null` + `cost_basis=no_usd_unit_rate` — `LlmResult`에 토큰/단가 SSOT 없음 |
| `exchange_fee_estimate_usd` | `null` + `fee_basis=no_fee_rate_ssot` — ledger fee SSOT 없음 |

---

## 5. Pipeline Hook

`daily_audit`: … → report → **overseer** → reconcile

---

## 6. Root Import

`llm_gemini_core` — read-only. Bitget-specific prompts in `ai_overseer.py`
