# Chapter B0D3A4G — Fast Safety Shadow Activation SSOT Investigation

Read-only investigation completed 2026-07-28. No code, config, DB, or service changes were made.

---

# Executive Summary

- **Current production OFF state:** All four production Supernova call sites pass `fast_safety_shadow_enabled=False` explicitly to `execute_supernova_live_scan_with_fast_safety_ops_audit`. The ops wrapper short-circuits to the lifecycle wrapper with shadow OFF and `fast_safety_audit_sink=None`, so no audit runtime, no ops sink, and no `ops_logger` import on the OFF path (`supernova_hunter.py`, `factory_pipelines.py`; enforced by `test_fast_safety_supernova_ops_writer_integration.py`).

- **Recommended SSOT:** **`config_kv` in `system_config.sqlite` via `config_manager.get_config_value`**, read at **call time** through a **new dedicated activation reader module** (key name **not yet present in codebase — must be chosen in a follow-up chapter**). Storage should follow existing `FAST_SAFETY_*` naming and likely be **market-scoped** (mirroring `FAST_SAFETY_POLICY_KR` / `FAST_SAFETY_POLICY_US`). Exact activation requires `value is True` (Python bool), default **False** when absent or invalid.

- **Recommended alternatives NOT to use:**
  - **Policy document `enabled`** (`FAST_SAFETY_POLICY_KR` / `FAST_SAFETY_POLICY_US`) — slow-plane Kelly policy SSOT; already consumed inside shadow evaluation when runtime shadow is ON; must remain separate from runtime activation (`fast_safety_policy_store.load_fast_safety_policy_snapshot`, `fast_safety_runtime_shadow.prepare_fast_safety_shadow_context`).
  - **Environment variables** at production call sites — forbidden by existing gate tests (`FORBIDDEN_ACTIVATION_PATTERNS` in `test_fast_safety_supernova_ops_writer_integration.py`, `test_fast_safety_supernova_production_off_gate.py`).
  - **Direct `get_config_value` / `os.environ` inside `factory_pipelines.py` or `supernova_hunter.py` call sites** — same test forbids these patterns in production wiring diffs.

- **Restart required?**
  - **Config-only toggle (after reader is implemented):** **No restart** for cron-driven scans (each `factory.sh` invocation is a fresh process). **No restart** for `run_live_sniper_scheduler` if the reader is invoked **each loop iteration** (call-time read).
  - **Code deploy:** Normal process recycle on deploy (systemd/cron picks up new code on next start). **Unknown:** whether any production host still runs `supernova_hunter.py` as a standalone long-lived process (see Open Questions).

- **Next implementation scope:** One new reader module + minimal wiring at the four production call sites + tests. Do **not** modify kernel, shadow adapter, ops sink, audit runtime, or `forward/shared.py`.

---

# Evidence Table

| # | Topic | File | Function / Class | Code evidence | Interpretation | Confidence |
|---|--------|------|------------------|---------------|----------------|------------|
| 1 | Factory KR Supernova entry | `factory_pipelines.py` | `_step_supernova_kr` | Calls `execute_supernova_live_scan_with_fast_safety_ops_audit("KR", fast_safety_shadow_enabled=False)` | Cron KR supernova slot entry | High |
| 2 | Factory US Supernova entry | `factory_pipelines.py` | `_step_supernova_us` | Same pattern with `"US"` | Cron US supernova slot entry | High |
| 3 | Daemon KR/US sniper entry | `supernova_hunter.py` | `run_live_sniper_scheduler` | Two calls to ops wrapper with `fast_safety_shadow_enabled=False` for KR and US branches | Secondary live sniper path (when this function runs) | High |
| 4 | Factory CLI entry | `factory.sh` → `system_auto_pilot.py` | `run_factory_cli` | `python system_auto_pilot.py --mode "$MODE"` | Ubuntu cron one-shot factory entry | High |
| 5 | Pipeline dispatch | `system_auto_pilot.py` | `run_factory_cli` | `dispatch_factory_mode(args.mode, get_pipeline(args.mode), ...)` | Mode → `factory_pipelines.PIPELINE` | High |
| 6 | Staggered scan SSOT | `factory_scan_schedule.py` | `ALL_SCAN_SLOTS`, `_build_market_slots` | Modes like `scan_kr_supernova`, `scan_us_supernova`, `*_r2` | Cron slot names derive from here | High |
| 7 | Ops production wrapper | `supernova_hunter.py` | `execute_supernova_live_scan_with_fast_safety_ops_audit` | `effective_shadow_enabled = fast_safety_shadow_enabled is True`; OFF path skips ops import | Exact-`True` gate at wrapper boundary | High |
| 8 | Lifecycle wrapper | `supernova_hunter.py` | `execute_supernova_live_scan_with_fast_safety_audit` | Creates audit runtime only when shadow ON **and** sink callable | Shadow ON without sink still runs scan but no drain | High |
| 9 | Scan body | `supernova_hunter.py` | `execute_supernova_live_scan` | Prepares shadow context; trade via `aft.try_add_virtual_position` only | Shadow does not reference Kelly decision for orders | High |
| 10 | Policy keys (existing) | `fast_safety_policy_store.py` | `FAST_SAFETY_POLICY_KEYS` | `KR` → `FAST_SAFETY_POLICY_KR`, `US` → `FAST_SAFETY_POLICY_US` | Market-scoped policy SSOT (slow plane) | High |
| 11 | Policy `enabled` semantics | `fast_safety_policy_store.py` | `load_fast_safety_policy_snapshot` | `if enabled is not True: return None` | Policy must be exact bool `True` to load | High |
| 12 | Shadow needs policy when ON | `fast_safety_runtime_shadow.py` | `prepare_fast_safety_shadow_context` | If policy None → `ready=False`, `reason="policy-unavailable"` | Runtime ON + policy OFF/absent = audit-only no-op | High |
| 13 | config_kv read API | `config_manager.py` | `get_config_value` | SQLite `SELECT value_json FROM config_kv WHERE key=?`, JSON decode, `default_value` on miss | Standard point read; no bool helper | High |
| 14 | Runtime config cache | `config_manager.py` | `load_runtime_system_config` | TTL default 60s; `invalidate_runtime_system_config_cache()` | Long-worker pattern; supernova cron is not long-worker | High |
| 15 | Ops writer default | `supernova_hunter.py` | ops wrapper | Lazy `from ops_logger import insert_ops_event` when shadow ON and no injected writer | Production default persistence path | High |
| 16 | Ops insert contract | `ops_logger.py` | `insert_ops_event` | kwargs: `component`, `severity`, `event`, `payload`; returns `bool`; swallows exceptions → `False` | Append-only, non-fatal to caller | High |
| 17 | Ops DB location | `ops_logger.py`, `factory_data_paths.py` | `OPS_EVENTS_DB_PATH` | `{factory_data_dir()}/ops_events.sqlite`, table `ops_events` | WAL append-only telemetry | High |
| 18 | Audit → ops envelope | `fast_safety_ops_sink.py` | `build_fast_safety_ops_envelope` | `component="fast_safety"`, maps `event_type` → `event`, allowlisted payload | Compatible with ops schema | High |
| 19 | Audit event shape | `fast_safety_kernel.py` | `build_audit_event` | `event_type="fast_safety_kelly_decision"`, severities CRITICAL/NORMAL/DEBUG | Kernel audit SSOT | High |
| 20 | Bounded queue drops | `fast_safety_audit_queue.py` | `BoundedAuditEmitter.try_emit` | On `Full`: returns `False`; **no drop counter** | Drops are silent today | High |
| 21 | Production OFF tests | `test_fast_safety_supernova_ops_writer_integration.py` | `test_production_kr_us_use_ops_wrapper_explicit_off` | AST: ≥4 ops wrapper calls, all `fast_safety_shadow_enabled=False` | Current production contract | High |
| 22 | Forbidden activation sources | `test_fast_safety_supernova_ops_writer_integration.py` | `FORBIDDEN_ACTIVATION_PATTERNS` | `get_config_value`, `os.getenv`, `ENABLE_FAST`, `FAST_SAFETY_SHADOW`, `FEATURE_FAST` in production diffs | Reader must live outside forbidden call-site patterns | High |
| 23 | systemd factory daemon | `deploy/systemd/dante-factory.service.in` | ExecStart | `run_factory_daemon.sh` → `system_auto_pilot.py --daemon` | Daemon = autopilot loop, **not** supernova scan cron | High |
| 24 | Cron example | `deploy/factory.kr.crontab.example` | cron line | `factory.sh --scan-kr-supernova` | Primary production supernova path | High |
| 25 | No shadow activation key | repo-wide grep | — | No `FAST_SAFETY_SHADOW*` or runtime activation key defined | Key name is **unconfirmed** | High |
| 26 | `dante-main` vs factory | `deploy/audit_factory_stack.sh` | audit check | `dante-main still active — duplicate daemon` = fail | Production expects `dante-factory`, not legacy main | Medium |
| 27 | Standalone sniper bootstrap | `supernova_hunter.py` | `run_scheduler` / `__main__` | `run_live_sniper_scheduler()` on main thread | Path exists; production use **Unknown** | Medium |

---

# Current Runtime Call Graph

## KR Factory (cron primary)

```
cron (deploy/factory.kr.crontab.example)
  → factory.sh --scan-kr-supernova [--scan-kr-supernova-r2]
    → system_auto_pilot.py --mode scan_kr_supernova[_r2]
      → run_factory_cli()
        → dispatch_factory_mode(mode, pipeline)          [factory_runtime.py]
          → StepSpec prelude (full/light) + supernova step
            → factory_pipelines._step_supernova_kr()
              → execute_supernova_live_scan_with_fast_safety_ops_audit(
                   "KR", fast_safety_shadow_enabled=False)   [supernova_hunter.py — production today]
                → [OFF] execute_supernova_live_scan_with_fast_safety_audit(shadow=False, sink=None)
                  → execute_supernova_live_scan(...)
                    → scan / funnel / try_add_virtual_position (trade path)
```

## US Factory (cron primary)

```
cron (deploy/factory.us.crontab.example / factory_slot_dispatcher)
  → factory.sh --scan-us-supernova [--scan-us-supernova-r2]
    → system_auto_pilot.py --mode scan_us_supernova[_r2]
      → dispatch_factory_mode(...)
        → factory_pipelines._step_supernova_us()
          → execute_supernova_live_scan_with_fast_safety_ops_audit(
               "US", fast_safety_shadow_enabled=False)
            → (same OFF chain as KR)
```

## Daemon KR/US (secondary — code present)

```
[Unknown production trigger — likely manual or legacy]
  supernova_hunter.run_scheduler()
    → run_live_sniper_scheduler()                    [infinite loop]
      → on KR/US wall-clock slots:
          execute_supernova_live_scan_with_fast_safety_ops_audit(
            market, fast_safety_shadow_enabled=False)
        → (same wrapper chain)

  FACTORY_SCAN_OWNER env (default "both"):
    "cron"  → clears daemon target times (daemon defers to cron)
    "daemon" → daemon keeps times; cron may duplicate unless configured
    [supernova_hunter.py:3268-3284]
```

**Note:** `dante-factory.service` runs `system_auto_pilot.py --daemon` → `system_main_loop()` (satellites/maintenance), **not** `run_live_sniper_scheduler`. Primary production Supernova execution is **cron factory**, not the factory daemon.

---

# Existing Configuration Mechanisms

| Mechanism | Exists | Location | Usage pattern | Relevance to shadow activation |
|-----------|--------|----------|---------------|--------------------------------|
| **config_kv** | Yes | `config_manager.py` → `{factory_data_dir()}/system_config.sqlite` | Key/value JSON rows; OCC helpers (`read_config_kv_row`, `update_config_kv_if_match`, …) | **Primary candidate.** Policy already stored here (`FAST_SAFETY_POLICY_*`). |
| **config_manager** | Yes | `get_config_value`, `set_config_value`, `load_system_config`, `load_runtime_system_config(ttl=60)` | Point read or merged dict; errors → default | Reader should use `get_config_value`; long daemons may use TTL cache if needed. |
| **Environment variables** | Yes | `.env`, `factory.sh`, systemd `EnvironmentFile` | Operational toggles: `FACTORY_SCAN_OWNER`, `FACTORY_FORCE_SCAN_OUTSIDE_SESSION`, `META_GOVERNOR_SKIP_VIX`, `DB_STORAGE_PATH` | **Not suitable** for shadow activation at production call sites (gate tests). |
| **File config (legacy JSON)** | Yes | `system_config_atomic.py` → delegates to `config_manager` | `load_config()` / domain shards; bridged to SQLite when KV populated | Supernova scan uses `load_config()` for DNA templates, not shadow flags. |
| **DB policy documents** | Yes | `FAST_SAFETY_POLICY_KR/US` via `fast_safety_policy_store` | Slow-plane Kelly policy; admin CLI apply/rollback (`fast_safety_policy_admin.py`) | **Policy `enabled` ≠ runtime shadow activation.** |

### Feature-flag patterns observed (no dedicated bool SSOT helper)

| Pattern | Example | Bool rule | Import vs call time |
|---------|---------|-----------|---------------------|
| Exact bool | `fast_safety_policy_store`: `enabled is not True` | Strict | Call-time via injected `get_value` |
| Wrapper exact gate | `fast_safety_shadow_enabled is True` | Strict | Parameter at call time |
| Config dict truthy | `system_auto_pilot._meta_governor_skip_vix_from_config`: `True`, `1`, `"true"/"yes"/"on"` | Loose | Call-time after `load_or_create_config()` |
| Env string truthy | `META_GOVERNOR_SKIP_VIX`, `FACTORY_FORCE_SCAN_OUTSIDE_SESSION` | Loose | Read at call site |
| TTL cached dict | `load_runtime_system_config(60.0)` | N/A | Call-time with ≤60s staleness |

**Project standard for Fast Safety:** **exact `is True`** on bool parameters and policy `enabled` (High confidence). Shadow activation should match this, not loose truthy strings.

---

# Recommended Activation SSOT

| Field | Recommendation |
|-------|----------------|
| **Storage location** | `config_kv` table in `{factory_data_dir()}/system_config.sqlite` (same DB as `FAST_SAFETY_POLICY_*`) |
| **Key name** | **Not confirmed in codebase.** Candidates aligned with existing convention (owner must approve in next chapter): |
| | • Per-market: `FAST_SAFETY_SHADOW_KR`, `FAST_SAFETY_SHADOW_US` (mirrors `FAST_SAFETY_POLICY_*`) |
| | • Or nested under a single JSON document key (less aligned with current per-key policy pattern) |
| | **Do not invent and apply a key in this investigation.** |
| **Reader function** | **New module required** (e.g. `fast_safety_shadow_activation.py`): `resolve_fast_safety_shadow_enabled(market: str, *, get_value: Callable \| None = None) -> bool` delegating to `get_config_value` internally — **not** inline in `factory_pipelines.py` / `supernova_hunter.py` (test constraint). |
| **Return type** | `bool` — only `True` activates; everything else → `False` |
| **Default** | `False` when key absent, decode error, wrong type, or any non-`True` value |
| **Exact True rule** | `return raw is True` after read (mirror `load_fast_safety_policy_snapshot` and ops wrapper) |
| **Read timing** | **Call-time** at each production invocation (factory step fn + scheduler loop iteration). Avoid module import-time reads. |
| **KR/US scope** | **Per-market keys recommended** — matches `FAST_SAFETY_POLICY_KEYS`, allows single-market canary. Global single flag is possible but less aligned. |
| **Restart** | **None** for config flip once call-time reader is wired (cron = new process per run). Code change still needs deploy. |

### Why separate from policy `enabled`

| Layer | Controls | Evidence |
|-------|----------|----------|
| **Runtime activation flag** | Whether shadow pipeline, audit emitter, and ops drain run at all | `execute_supernova_live_scan_with_fast_safety_ops_audit`: OFF skips ops import entirely |
| **Policy document `enabled`** | Whether Kelly policy parameters exist for shadow **evaluation** | `load_fast_safety_policy_snapshot`: returns `None` if policy not enabled |
| **Combined behavior** | Runtime ON + policy OFF → scan unchanged, shadow context `policy-unavailable`, no audit emit | `prepare_fast_safety_shadow_context` |

Separating layers allows: (1) instant runtime OFF without policy rollback; (2) policy pre-staged while runtime stays OFF; (3) independent admin/rollback paths (`fast_safety_policy_admin` vs future activation key).

---

# Ops Telemetry Persistence

| Item | Detail |
|------|--------|
| **Writer signature** | `insert_ops_event(*, component: str, severity: str, event: str, payload: dict \| None = None, ts_utc: str \| None = None, max_retries: int = 6) -> bool` |
| **DB / table / path** | `{factory_data_dir()}/ops_events.sqlite` → table `ops_events` (`id`, `ts_utc`, `component`, `severity`, `event`, `payload_json`) |
| **Payload serialization** | `json.dumps(payload)`; truncated at 32k chars; ops sink allowlists scalar fields from audit event |
| **Failure handling** | Writer returns `False`; exceptions swallowed; sink returns `False`; drain counts `failed_count`; **scan continues** |
| **Impact on scan results** | **None** — ops path is post-evaluation drain in `finally`; no feedback into Kelly, shares, or INSERT |
| **Shadow ON query example** | `SELECT ts_utc, severity, payload_json FROM ops_events WHERE component='fast_safety' AND event='fast_safety_kelly_decision' ORDER BY id DESC LIMIT 20;` |
| **Read helper** | `ops_logger.fetch_recent_rows(hours=1.0)` — filter client-side by component/event |

---

# Drop Observability

| Item | Status |
|------|--------|
| **Drop counter exists?** | **No.** `BoundedAuditEmitter.try_emit` returns `False` on `queue.Full` with no accounting (`fast_safety_audit_queue.py:28-29`). |
| **Externally exposed?** | **No** metric, ops event, or drain field for drops. |
| **Drain exposes** | `FastSafetyAuditDrainResult`: `delivered_count`, `failed_count`, `remaining_count` — not enqueue drops (`fast_safety_audit_runtime.py`). |
| **Minimum safe improvement boundary** | **`fast_safety_audit_queue.py`** — add internal drop counter + optional read-only property; optionally surface via drain result or `ops_logger.record_gauge_snapshot` from wrapper (would touch `fast_safety_audit_runtime.py` or ops wrapper — out of scope for activation SSOT chapter). |
| **Test without production wiring** | `test_fast_safety_audit_queue.py` already fills queue to 1024 and asserts `try_emit` returns `False`; extend with counter assertion once implemented. |

---

# Controlled Shadow ON Plan

## 1. Pre-implementation verification

- [ ] Confirm production topology: cron-only vs standalone `supernova_hunter.py` (Open Question).
- [ ] Confirm `FAST_SAFETY_POLICY_{KR|US}` inspect status via `fast_safety_policy_admin_cli.py inspect` (operational — not run in this chapter).
- [ ] Baseline: query `ops_events` for zero `component='fast_safety'` rows during scan window.
- [ ] Baseline: snapshot open `forward_trades` count / recent INSERT rate for target market.

## 2. Code change (next chapter)

- [ ] Add activation reader module (default OFF).
- [ ] Wire four call sites to pass `resolve_...(market)` into ops wrapper.
- [ ] Keep explicit `is True` semantics; no env reads at call sites.

## 3. Local tests

- [ ] Reader unit tests: absent key, `False`, `"true"`, `1`, `True`, per-market isolation.
- [ ] Extend production gate tests for new wiring (still forbid inline config/env in call sites).
- [ ] Shadow ON integration test with injected writer (existing pattern in `test_fast_safety_supernova_ops_writer_integration.py`).
- [ ] Trade boundary invariant tests (`test_fast_safety_supernova_shadow_invariants.py`).

## 4. Detached Ubuntu verification

- [ ] `factory.sh --scan-kr-supernova --dry-run` (pipeline list unchanged).
- [ ] Single scan with activation ON + injected/mock writer or read-only ops DB copy.
- [ ] Confirm no new forward_trades rows attributable to shadow.

## 5. Pre-production config change checklist

- [ ] Policy document enabled + valid for target market (otherwise shadow emits nothing useful).
- [ ] Activation key set to JSON `true` (bool), not string `"true"`.
- [ ] Rollback key/value documented and tested.
- [ ] Ops disk path writable: `{factory_data_dir()}/ops_events.sqlite`.

## 6. Single-market / single-scan canary

- **Feasible** with per-market activation keys and/or enabling only one cron slot (e.g. `scan_kr_supernova` only) while US remains OFF.
- **Daemon path:** `FACTORY_SCAN_OWNER=cron` disables daemon duplicate times; prefer cron canary.

## 7. Ops event confirmation

- Expect rows: `component=fast_safety`, `event=fast_safety_kelly_decision`, severities CRITICAL/NORMAL/DEBUG.
- Confirm `delivered_count > 0` in drain (via debug logging or test hook — not in production today).

## 8. Trade invariance proof

- Compare before/after: `forward_trades` INSERT count, `try_add_virtual_position` call count (mock in tests), Kelly/invest_amount/shares code paths untouched in diff.
- Existing tests: `TRADE_BOUNDARY_SYMBOLS` must not appear in production diffs.

## 9. Immediate OFF rollback

1. Set activation key to `false` or delete key (reader returns False).
2. Next cron scan picks up OFF without restart.
3. If policy was changed independently, use `fast_safety_policy_admin_cli.py apply-disabled` / rollback — separate concern.
4. Verify: no new `fast_safety` ops events; scan telegram/funnel unchanged.

---

# Recommended Next Chapter

## Chapter B0D3A4H — Config-Driven Shadow Activation Reader (recommended single follow-up)

| Item | Specification |
|------|----------------|
| **Chapter name** | **B0D3A4H — Fast Safety Shadow Activation Reader** |
| **Modify allowed** | `factory_pipelines.py` (only `_step_supernova_kr`, `_step_supernova_us`), `supernova_hunter.py` (only `run_live_sniper_scheduler` call sites — **not** wrappers/scan body), new test file(s) |
| **New files** | `fast_safety_shadow_activation.py` — sole owner of config_kv read for runtime flag |
| **Function signature** | `def resolve_fast_safety_shadow_enabled(market: object, *, get_value: Callable[[str, Any], Any] \| None = None) -> bool:` — returns `True` only when stored value `is True` for market-scoped key; else `False` |
| **Default OFF guarantee** | Absent/invalid → `False`; production call sites use reader result but tests may still require explicit fallback documentation; reader never raises |
| **Tests (suggested ~8–12)** | Reader matrix (4–6), wiring AST gate (2), factory step integration mock (2), scheduler mock (2), regression OFF path unchanged (2) |
| **Forbidden** | Touch `fast_safety_kernel.py`, `fast_safety_runtime_shadow.py`, `fast_safety_ops_sink.py`, `fast_safety_audit_runtime.py`, `ops_logger.py`, `forward/shared.py`, wrappers' internal logic, policy admin, production Shadow ON without checklist |
| **Rollback** | Revert reader wiring to literal `False`; remove or set activation key OFF in config_kv |

**Key name finalization** should be a deliverable of B0D3A4H design review (prefer `FAST_SAFETY_SHADOW_{KR|US}` or similar — **subject to owner approval**).

---

# Open Questions

| Question | Status |
|----------|--------|
| Is `supernova_hunter.py` / `run_live_sniper_scheduler` running as a separate production process today? | **Unknown** — not referenced in systemd/cron templates searched; only cron factory paths confirmed. |
| Exact activation `config_kv` key name and owner approval | **Unknown** — no key exists in repository. |
| Current production values of `FAST_SAFETY_POLICY_KR/US` | **Unknown** — requires operational DB inspect (not executed). |
| Whether `FACTORY_SCAN_OWNER` is set to `cron` on production Ubuntu | **Unknown** — env in `.env` on server. |
| Whether `dante-main.service` is fully disabled on production | **Unknown** — audit script expects it inactive. |
| Production `factory_data_dir()` path on Ubuntu | **Unknown** — defaults to `~/dante_bots/Dual-Screener-Bot` or `DB_STORAGE_PATH`. |

---

# Investigation Answers (Q1–Q20 summary)

| Q | Answer |
|---|--------|
| 1 | Production KR/US Supernova: **cron → `factory.sh` → `system_auto_pilot.py --mode scan_{kr\|us}_supernova*`**; secondary **`run_live_sniper_scheduler`** if standalone sniper process runs. |
| 2 | Factory: cron/factory.sh CLI. Live sniper: `run_live_sniper_scheduler` loop in `supernova_hunter.py`. |
| 3 | Shared read mechanism: **`config_manager` / `config_kv`** for system flags; scans also use **`system_config_atomic.load_config`** for DNA templates — **no shared shadow flag today**. |
| 4 | **No** dedicated read-only bool SSOT helper; use **`get_config_value`** + caller enforces `is True`. |
| 5 | Fast Safety uses **exact bool `True`**; elsewhere loose truthy exists — do not reuse loose pattern for shadow. |
| 6 | **Call-time read** is standard for scan steps; **`load_runtime_system_config`** for long workers with 60s TTL. |
| 7 | **Yes** for cron (new process each run) once reader exists; **yes** for daemon loop if read per iteration. |
| 8 | If import-time wiring were used: **`dante-factory`** / standalone sniper would need restart; cron would not. |
| 9 | **Per-market flags** align with `FAST_SAFETY_POLICY_*`; single global flag possible but less consistent. |
| 10 | Validate with **`value is True`** after JSON decode; reject bool-subclass edge cases via strict isinstance if desired. |
| 11 | Policy `enabled` = slow-plane Kelly document; runtime flag = audit path gate — **see Recommended Activation SSOT**. |
| 12 | **`ops_events.sqlite`** / **`ops_events`** via **`insert_ops_event`**. |
| 13 | Signature/returns/exceptions — **see Ops Telemetry Persistence**. |
| 14 | **Compatible** — component/severity/event/payload map cleanly via ops sink. |
| 15 | **Not observable** in production telemetry today. |
| 16 | Minimum boundary: **`BoundedAuditEmitter`** (+ optional drain/gauge exposure). |
| 17 | Tests (invariants, OFF gate, mock writer) + ops SQL + forward_trades count diff. |
| 18 | Set activation key false/absent; optional policy rollback via admin CLI — separate. |
| 19 | **Yes**, reuse **`config_kv` + `get_config_value`**; **no new env var** recommended. |
| 20 | **Chapter B0D3A4H** — see Recommended Next Chapter. |

---

*End of investigation report.*
