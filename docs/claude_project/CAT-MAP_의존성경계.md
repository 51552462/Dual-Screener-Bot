# CAT-MAP · 의존성 & 교차 수정 경계

> **Tier T0** — 거의 모든 설계 대화에 @멘션. bitget 제외.

---

## 1. 데이터 흐름 (한 줄)

`B(OHLCV) → G(국면) → C(스캔) → D(OPEN) → E(청산) → F(NAV) → J(리포트)`  
`H(진화) → K(INCUBATOR) → C` · `I(toxic) → C,F`

---

## 2. Single Writer (쓰기 SSOT)

| 데이터 | Writer | Readers |
|--------|--------|---------|
| `forward_trades` | CAT-D | F,G,H,J |
| `config_kv` | CAT-K | ALL |
| `REGIME_ENSEMBLE` / regime keys | CAT-G | C,F,E,J |
| `INCUBATOR_TEMPLATES` | CAT-H | C |
| `ANTI_PATTERNS` / TOXIC_ML | CAT-I | C,F |
| `treasury_state.json` | CAT-F | J |
| `short_forward_trades` | CAT-C (blackhole) | J read |
| `ops_events` heartbeat | CAT-A/L | L |

---

## 3. 교차 수정 금지

| 수정 CAT | 건드리면 안 됨 | 허용 인터페이스 |
|----------|---------------|----------------|
| C | D try_add 내부, F Kelly | `try_add_virtual_position(...)`, sig_type |
| E | F `record_closure` 타이밍 | exit result → ledger UPDATE |
| G | C hydrate 구현 | `CURRENT_REGIME_KEY`, `META_REGIME_KEY` |
| H | K INCUBATOR DELETE | `update_config_value("INCUBATOR_TEMPLATES")` |
| I | C scanner 본체 | `toxic_antipattern_core.evaluate_*` |
| N | F production Kelly | shadow flag, audit queue |
| J | D track/close 로직 | read-only + hydrate hooks |
| F | D schema | kelly_risk_pct, sim_kelly_invest 필드 |

---

## 4. CAT 간 의존 (설계 시 참조 방향)

```
A → C, D, J, G (스케줄이 invoke)
B → C, D, G, J (DB)
K → ALL (config)
G → C, F, E (국면 주입)
C → D (진입)
D → E → F (생애)
H → K → C (진화 재투입)
I → C, F (차단)
N ⇢ C,F,G (shadow only)
```

**설계 원칙**: upstream CAT만 policy 변경. downstream은 consume만.

---

## 5. 위험도 & 승인

| 등급 | CAT | 승인 |
|------|-----|------|
| 🔴 Critical | F, G, N, B(schema) | 디렉터 + Handoff + Cursor 충돌 보고 |
| 🔴 High | D, E, I, K | 디렉터 |
| 🟡 Medium | A, C, H(merge), P, L | Claude↔Cursor 교차검증 |
| 🟢 Low | J, M, O, Q | Cursor 자율 |

---

## 6. 듀얼 AI 역할 (한 줄)

| Claude | Cursor |
|--------|--------|
| 수식·정책·config 키·Handoff | 코드·SQL·OCC·deploy·테스트 |

---

*상수·KR/US: CAT-CONSTANTS, CAT-KR-US 참조*
