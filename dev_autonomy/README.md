# Quant development autonomy — Phase 1

Phase 1 receives the structured data that already exists before Telegram
delivery. It does **not** scrape Telegram and does not call Claude, Cursor,
git push, merge, SSH, deployment, or live trading.

## Intake sources

- Track A: `dual_north_star_ledger.json`
- Track B: latest `post_deploy_obs_digest_daily` row in `bitget_ops_events.sqlite`
- Tracks A/B/IV: repository `NEXT_ACTION` / Handoff SSOT

## Dry-run

```bash
python -m dev_autonomy.control_plane \
  --north-star-ledger /var/lib/quant-factory/data/dual_north_star_ledger.json \
  --bitget-ops-db /var/lib/bitget-factory/data/bitget_ops_events.sqlite \
  --dry-run --json
```

Without `--dry-run`, normalized reports and deterministic decisions are
deduplicated in `data/dev_autonomy/control_plane.sqlite`. The queue is intake
only: every decision has `execution_authorized=false`.

`WAIT_CURSOR_IMPL` can become an implementation *candidate* only while a
valid weekly autonomy envelope is present. The Phase 1 envelope hard-rejects
live, deployment, and merge authority and requires a pull request.

The MDD early-warning route starts at 85% of the existing per-track cap. This
is a review-only alarm; it does not alter the trading cap, leverage, sizing,
or any risk module.
