# CAT-L-CUTOVER-01 Phase 0c — architecture target dump (local 2026-09-30)

`run_architecture_checks()`:

```
passed: false
failed: ['regime_kelly_audit']
```

regime_kelly_audit message: `regime/kelly audit FAIL: ['regime_keys_known', 'meta_fresh']` — local Windows env, **not** in Phase 0 Bot-2 architecture.failed list.

Phase 0c four targets:

```
pipeline_structure: ok=true  daily_step_count=20  daily_count_ok=true
bitget_shell_daily_audit_guard: ok=true  classification=ok
weekly_evolution_pipeline: ok=true  tail_ok=true
portfolio_nav_risk_ssot: ok=true  failed=[]
```
