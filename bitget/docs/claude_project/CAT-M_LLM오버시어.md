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
| gemini core | root `llm_gemini_core` (read-only import) |
| pipeline step | `daily_audit` overseer step |
| sentinel | `sentinel.py` |

---

## 3. 비침투 원칙 (CAT-MAP)

- LLM **cannot** directly write config_kv
- proposals → human or structured JSON → approval path (P2-4)
- no auto-apply Kelly/regime changes without gate

---

## 4. Claude 설계 대상 (work_phases 묶음D)

- D-1: structured JSON proposal schema
- D-2: telegram approval command gate
- D-3: API cost tracking in weekly report (P2-6)

---

## 5. Pipeline Hook

`daily_audit`: … → report → **overseer** → reconcile

---

## 6. Root Import

`llm_gemini_core` — read-only. Bitget-specific prompts in `ai_overseer.py`
