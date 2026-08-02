# CAT-G · 국면 & Meta (Bitget)

> **위험도** 🔴 **Critical** · **Tier T2** · **also_load**: MAP, CONSTANTS, SPOT-FUT  
> **never_with**: CAT-C scanner body, CAT-H lifecycle DB

---

## 1. 역할

Regime detection sync, MetaGovernor consumer, Kelly cap injection, config_kv alignment, meta state store.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| meta sync (write) | `governance/meta_sync.py` |
| meta consume (read) | `governance/meta_consumer.py` |
| meta state | `governance/meta_sync.py` + config_kv |
| regime audit | `validation/regime_audit.py` |
| architecture guard | `validation/architecture_checks.py` |

---

## 3. Config Keys (Single Writer: meta_sync)

| key | role |
|-----|------|
| CURRENT_REGIME_KEY | active regime (BULL/BEAR/CHOP/…) |
| REGIME_ANALYSIS | ensemble JSON |
| DYNAMIC_KELLY_RISK | synced kelly cap |
| META_GOVERNOR_STATE | cycle snapshot |

---

## 4. Regime → Action

- Kelly cap floor/ceiling via meta_sync
- Engine selection: **고정 cron** (regime-adaptive engine pick **미구현**)
- doomsday / tail gates read regime

---

## 5. Bitget vs Root Meta

| | Bitget SSOT | Legacy |
|---|-------------|--------|
| consumer | `governance/meta_consumer.py` | root `meta_governor_consumer` |
| DB | `bitget_system_config.sqlite` | 주식 DB **오염 버그 수정됨** (2026-07) |
| forward_db_path | Bitget DB | `ctx.forward_db_path or ctx.bitget_db_path` |

---

## 6. Prelude (CAT-A)

daily/scan 전 `meta_governor_sync` + regime aligned check — misaligned → abort (양호)

---

## 7. Claude 설계 대상

- SPOT/FUT split regime (if needed — new keys)
- REGIME → engine schedule map
- meta cycle failure degraded mode
- sync with root practitioner meta (read-only boundary)

---

## 8. 🔴 Critical

regime key 오염 → 전체 Kelly/scan 오동작. 변경 시 F,N,C 영향 명시.
