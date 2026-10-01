# CAT-L-CUTOVER-01 Phase 0c — Bot-2 `run_architecture_checks()` · 2026-09-30T09:19:08Z

SSH `ubuntu@3.36.90.195`. 설치기/`update_bitget.sh`/`--start-parallel` **0**. 스크립트 CRLF로 마지막 `bash: $'\r'` 경고만(덤프는 `JSON_DUMP_RC=0`).

```
HEAD=c1ffe3f feat(bitget): CAT-L-FENCE-03 cron drift guard with body-sha256 markers
architecture_checks.py mtime: 2026-08-02 05:44 (0c 미반영)
passed: false
failed:
  pipeline_structure
  bitget_shell_daily_audit_guard
  weekly_evolution_pipeline
  portfolio_nav_risk_ssot
```

`regime_kelly_audit`: **ok=true / PASS**. flags: `regime_keys_known=true`, `meta_fresh=true`, `CURRENT_REGIME_KEY=HIGH_VOL`, meta json=`bitget_meta_governor_state.json`.

4건 실패 메시지(구 체크, Phase 0과 동일 계열):
- pipeline: `daily_count_ok=false` (count=20)
- shell: missing `[[ "$pid" -eq "$$" ]]`
- weekly: `tail_ok=false` (끝=`weekly_executive_summary`)
- nav: failed=`execution_safety`, `leverage_manager`, `paper_ledger_gross`

전체 JSON 원문: Bot-2 `/tmp/cutover01_p0c_arch.out` · 이 세션 터미널 캡처.
