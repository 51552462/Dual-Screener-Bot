# CAT-MAP · 의존성 & 교차 수정 경계 (Bitget)

> **Tier T0** — 거의 모든 설계 대화에 @멘션. **루트 주식 코드 수정 금지.**

---

## 1. 데이터 흐름 (한 줄)

`B(OHLCV·WS) → G(국면·Meta) → C(스캔) → D(OPEN) → E(청산·funding) → F(Treasury·Kelly) → J(리포트)`  
`H(진화·deathmatch) → K(config) → C` · `I(toxic) → C,F` · `N(execution_safety) → OMS`

---

## 2. Single Writer (쓰기 SSOT)

| 데이터 | Writer | Readers |
|--------|--------|---------|
| `bitget_forward_trades` | CAT-D | F,G,H,J,N |
| `bitget_system_config.sqlite` config_kv | CAT-K | ALL |
| `CURRENT_REGIME_KEY` / `REGIME_ANALYSIS` | CAT-G (`governance/meta_sync.py`) | C,F,E,J |
| `INCUBATOR_TEMPLATES` (Bitget DB) | CAT-H | C |
| `ANTI_PATTERNS` / TOXIC_ML | CAT-I | C,F |
| `TREASURY_SPOT_USDT` / `TREASURY_FUTURES_USDT` | CAT-D,F | J,N |
| `ops_events` heartbeat | CAT-A/L | L |
| 실주문 (OMS) | CAT-N (`trading/oms_core.py`) | D (reconcile) |

---

## 3. 교차 수정 금지

| 수정 CAT | 건드리면 안 됨 | 허용 인터페이스 |
|----------|---------------|----------------|
| C | D try_add 내부, F Kelly chain | `try_add_virtual_position(...)`, sig_type |
| E | F closure 타이밍 | exit result → ledger UPDATE |
| G | C hydrate 구현 | `CURRENT_REGIME_KEY`, meta_sync hooks |
| H | K INCUBATOR DELETE | `config_manager.set_config_value` |
| I | C scanner 본체 | toxic evaluate hooks |
| N | F production Kelly (shadow only) | `execution_safety` gates, dry_run |
| J | D track/close 로직 | read-only + hydrate hooks |
| F | D schema | kelly_risk_pct, treasury keys |
| B | D schema migration | OHLCV table naming `BITGET_*` |
| L | A pipeline step order | deploy scripts only |

---

## 4. CAT 간 의존 (설계 시 참조 방향)

```
A → C, D, J, G (파이프라인 invoke)
B → C, D, G, J (DB·WS)
K → ALL (config_hub / config_manager)
G → C, F, E (국면·Kelly cap 주입)
C → D (진입)
D → E → F (생애·Treasury)
H → K → C (진화 재투입)
I → C, F (차단)
N ⇢ D,F,G (실전 gate — paper 경로 분리)
O → C (practitioner rules — 루트 import read-only)
```

**설계 원칙**: upstream CAT만 policy 변경. downstream은 consume만.

---

## 5. 루트 import 허용 (읽기 전용)

| 루트 모듈 | Bitget 사용처 | 규칙 |
|-----------|--------------|------|
| `practitioner_intelligence` | CAT-O adapter | import only |
| `reports/*` | CAT-J | import only |
| `llm_gemini_core` | CAT-M | import only |
| `telegram_message_queue` | CAT-L | import only |
| `meta_governor_consumer` | CAT-G (legacy) | **Bitget SSOT는 `governance/meta_consumer.py` 우선** |

**금지**: `forward/shared.py`, `factory_pipelines.py`, `performance_budget_governor.py` 등 주식 SSOT **수정·복사 덮어쓰기**

---

## 6. 위험도 & 승인

| 등급 | CAT | 승인 |
|------|-----|------|
| 🔴 Critical | F, G, N, B(schema), D(book) | 디렉터 + Handoff + Cursor 충돌 보고 |
| 🔴 High | E, I, K | 디렉터 |
| 🟡 Medium | A, C, H(merge), P, L | Claude↔Cursor 교차검증 |
| 🟢 Low | J, M, O, Q | Cursor 자율 |

---

## 7. 듀얼 AI 역할 (한 줄)

| Claude | Cursor |
|--------|--------|
| 수식·정책·config 키·Handoff | bitget/ 코드·SQL·OCC·deploy·테스트 |

**work_phases SSOT**: `bitget/docs/work_phases/07_듀얼AI_협업루프.md`

---

*상수·SPOT/FUT: CAT-CONSTANTS, CAT-SPOT-FUT 참조*
