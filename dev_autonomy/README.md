# Quant development autonomy — Phase 1/2

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

## Weekday runner (Phase 2 bridge)

`weekday_runner` adds three safe bridge functions on top of intake:

1. write one deduplicated JSON job packet for every actionable new report;
2. optionally ask Claude Code for a **read-only** verdict;
3. optionally send a short Korean Telegram digest.

It still does not grant Cursor writes, Git, SSH, deployment, merge, live
trading, leverage, sizing, or risk authority. Cursor jobs in the outbox are
candidates only and keep `execution_authorized=false`.

Dry-run (no DB, job packet, AI call, or Telegram write):

```bash
python -m dev_autonomy.weekday_runner \
  --north-star-ledger /var/lib/quant-factory/data/dual_north_star_ledger.json \
  --bitget-ops-db /var/lib/bitget-factory/data/bitget_ops_events.sqlite \
  --envelope /etc/quant-dev-autonomy/envelope.json \
  --dry-run --json
```

Weekday control cycle after the VPS has Claude Code and Telegram environment
credentials configured:

```bash
python -m dev_autonomy.weekday_runner \
  --north-star-ledger /var/lib/quant-factory/data/dual_north_star_ledger.json \
  --bitget-ops-db /var/lib/bitget-factory/data/bitget_ops_events.sqlite \
  --envelope /etc/quant-dev-autonomy/envelope.json \
  --run-claude-review --notify-telegram --json
```

The Claude adapter uses headless print mode with `dontAsk` and restricts tools
to `Read,Glob,Grep`. The Cursor adapter uses the official `agent` headless CLI,
sandbox mode, and requires an explicit constructor opt-in before it can add
the mutating `--force` flag. The weekday runner intentionally does not turn on
that Cursor mutation path yet. Project-level `.cursor/cli.json` also denies
Git, SSH, network-fetch, secret-file, deployment, workflow, and self-policy
changes. Deny rules are a first wall; the existing post-run diff and test gates
remain mandatory.

`dev_autonomy/weekday_runner.cron.example` is a schedule template. Copy it to
the VPS only after replacing paths and verifying the dry-run. Repeated runs do
not resend unchanged reports because the queue is deduplicated by report ID.

For the supported two-VPS systemd installation, follow
`docs/VPS_DEV_AUTONOMY_DEPLOY.md`. The installer keeps timers disabled until
the non-root Claude/Cursor browser logins are complete, performs a dry-run,
and uses separate `stock` and `bitget` runtime queues.
