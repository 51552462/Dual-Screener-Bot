# CAT-C · 스크리닝 & 시그널 (Bitget)

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: CONSTANTS, SPOT-FUT, MAP  
> **never_with**: CAT-D try_add internals, CAT-F Kelly chain

---

## 1. 역할

Supernova·**master scanner** = **live scan → candidate → try_add** 경로.  
**blackhole·underdog** = **closed-trade analytics only** — config/ANTI_PATTERNS 학습, **no try_add candidate path** (C-1 hook N/A).

signal_engines, practitioner rules hook, universe filter.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| signal engines (core 5) | `signal_engines.py` — incl. C-1 `evaluate_bad_tick` |
| supernova | `supernova_hunter.py` — C-1 pre-candidate hook |
| blackhole | `blackhole_hunter.py` — analytics only, no candidate |
| underdog | `underdog_miner.py` — analytics only, no candidate |
| master | `master_scanner.py` — C-1 pre-candidate hook (`bse` import OK) |
| practitioner adapter | `forward/practitioner_bitget_adapter.py` |
| scoring / short | signal pipeline in scanners |
| config load | `config_hub.py` → `infra/config_manager` |

---

## 3. Scanner → D 인터페이스

```
scanner → candidate dict (symbol, sig_type, score, market_type)
       → forward/ledger try_add_virtual_position(...)
```

**금지**: try_add 내부 gate 순서 변경 (CAT-F SSOT)

---

## 4. 유니버스

- 거래량·유동성 기준 자동 갱신
- SPOT/FUT 별 scan mode (`scan_spot`, `scan_futures`)
- staggered cron (~27 slots) — overlap skip 정책 (CAT-A)

---

## 5. Practitioner (CAT-O link)

- 루트 `practitioner_intelligence` **read-only import**
- ~30 rules + 5 core engines
- Bitget adapter가 market_type·symbol 정규화

---

## 6. 알려진 이슈

| # | 이슈 | 우선순위 |
|---|------|----------|
| 1 | ~~`master_scanner.py:97` NameError~~ | P0-6 **resolved** (`bse` import) |
| 2 | ~~bad tick / flash crash filter 없음~~ | P1-6 **resolved** (C-1 `evaluate_bad_tick`, 2026-08-04) |
| 3 | regime별 engine 선택 **고정 cron** | P2 |
| — | C-1 vs CAT-N `price_sanity_gate` | 이중 필터 **분리 유지** — threshold 대조만 비차단 후속 |

---

## 7. Claude 설계 대상

- ~~P1-6: N-bar extreme deviation filter (pre try_add)~~ → **C-1 Done**
- P1-2: correlation proxy (BTC beta) for concentration
- scanner output schema versioning
- ENROLLED_SHADOW vs live promotion (CAT-H link)

---

## 8. 상수

→ `@CAT-CONSTANTS` (cutoff, threads)
