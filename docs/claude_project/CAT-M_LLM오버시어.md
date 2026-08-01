# CAT-M · LLM & AI 오버시어

> **위험도** 🟢 Low · **Tier T2** · **비침투**: 거래·Kelly·국면 경로에 LLM 없음

---

## 1. 역할

Gemini API pool, call cache, sentiment mining, overseer narrative, report text — **explain & enrich only**.

---

## 2. SSOT

| file | role |
|------|------|
| `llm_gemini_core.py` | GeminiKeyPool, model, cache, sanitize |
| `gemini_report_cache.py` | report response cache |
| `sentiment_miner.py` | RSS/news → Gemini → news_data.sqlite |
| `ai_overseer.py` | overseer orchestration |
| `overseer_llm_narrative.py` | audit narrative |
| `overseer_audit_binder.py` | audit binding |
| `satellite_intel_brief.py` | intel brief |

---

## 3. llm_gemini_core policy

- **Model**: gemini-2.0-flash
- **GeminiKeyPool**: round-robin multi-key, 429 backoff
- **Cache**: `llm_call_cache.sqlite` dedup
- **sanitize_user_visible_text**: block prompt/formula leak to telegram
- **deterministic_fallback**: if API fail

---

## 4. Consumers (read-only to trading)

| consumer | purpose |
|----------|---------|
| weekly_action_plan | Bayesian toxic + tail narrative |
| practitioner_llm (CAT-O) | PIL text |
| ai_overseer | system commentary |
| sentiment_miner | news_data.sqlite daily score |
| alpha/evolution explain | digest text |

---

## 5. sentiment_miner flow

Naver + BOK/Fed RSS → Gemini → `news_data.sqlite::daily_sentiment`  
Fields: top_keyword_1..3, sentiment_score

---

## 6. Claude 설계

- prompt policy (what data enters LLM)
- fallback narrative templates
- sentiment score semantics (−1..1 or 0..100 — follow existing)
- overseer section content rules
- **never** LLM output → direct trade decision

## 7. Cursor 구현

- API keys in .env only, backoff, cache schema, sanitize regex

---

## 7. Boundaries

| allowed | forbidden |
|---------|-----------|
| report HTML text | kelly_risk_pct |
| weekly action prose | try_add gate |
| sentiment feature | regime key write |

*DB: news_data.sqlite (CAT-B)*
