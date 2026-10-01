=== TS 2026-09-30T06:49:53Z HEAD=c1ffe3f ===
=== Step 1 bitget.sh --cutover-check ===
[bitget.sh] mode=cutover_check log=/var/lib/quant-bitget/logs/bitget_cutover_check_20260930_154953.log TZ=Asia/Seoul wall_utc=2026-09-30 06:49:53 UTC
CUTOVER_CHECK_RC=124
=== Step 1 JSON dump ===
[BLOCKED] bitget.main is removed. Production SSOT:
  24/7 daemon : python -m bitget.pipelines.bitget_auto_pilot --daemon
              (systemd: dante-bitget-factory)
  cron jobs   : bitget/deploy/bitget.sh --scan-all|--daily-audit|...
See bitget/RUNBOOK.md
[BLOCKED] bitget.factory_launcher is removed. Use:
  systemctl start dante-bitget-factory dante-bitget-dashboard dante-bitget-heatmap
  or: bitget/deploy/bitget.sh --daemon  (→ bitget_auto_pilot)
See bitget/RUNBOOK.md
[BLOCKED] bitget.sentinel is removed. Use systemd:
  dante-bitget-dashboard  (port 8511)
  dante-bitget-heatmap    (port 8512)
See bitget/RUNBOOK.md
{
  "ok": true,
  "passed": false,
  "checks": {
    "pipeline_ssot_env": false,
    "parallel_run_ready": false,
    "no_legacy_main_process": true,
    "async_telegram": true,
    "architecture_ok": false
  },
  "architecture": {
    "ok": true,
    "passed": false,
    "checks": {
      "legacy_entrypoints": {
        "ok": true,
        "details": {
          "bitget.main._blocked": true,
          "bitget.factory_launcher.launch_factory": true,
          "bitget.sentinel.run_sentinel": true,
          "bitget.system_auto_pilot.system_main_loop": true
        },
        "message": "legacy entrypoints blocked"
      },
      "pipeline_structure": {
        "ok": false,
        "daily_audit": [
          "meta_governor_sync",
          "artifact_guard",
          "config_bootstrap",
          "sentiment_mining",
          "doomsday_radar",
          "report_pipeline_hydrate",
          "track_spot",
          "track_futures",
          "deep_dive_spot",
          "deep_dive_futures",
          "doomsday_bridge_sync",
          "reporter_cleanup_zombie_forward_trades",
          "memory_retention_sweep",
          "forward_trade_identity",
          "pil_practitioner_reports",
          "comprehensive_report",
          "executive_summary_daily",
          "genesis_radar_daily",
          "ai_overseer",
          "reconcile"
        ],
        "daily_step_count": 20,
        "scan_spot": [
          "meta_governor_sync_scan",
          "artifact_guard",
          "config_bootstrap",
          "supernova_spot",
          "scan_spot",
          "track_spot"
        ],
        "track_positions": [
          "config_bootstrap",
          "artifact_guard",
          "track_spot",
          "track_futures"
        ],
        "daily_prelude": "ok",
        "scan_prelude": "ok",
        "daily_body_keys": true,
        "daily_count_ok": false,
        "message": "pipeline structure drift"
      },
      "scan_schedule_ssot": {
        "ok": true,
        "n_slots": 27,
        "n_staggered_modes": 27,
        "missing_modes": [],
        "missing_pipelines": [],
        "prelude_ok": true,
        "cron_template": "ok",
        "message": "scan schedule SSOT ok"
      },
      "governance_infra_removed": {
        "ok": true,
        "shim_dir_exists": true,
        "remnants": [],
        "ssot": "bitget/infra",
        "message": "governance/infra shim removed"
      },
      "bitget_shell_daily_audit_guard": {
        "ok": false,
        "missing": [
          "[[ \"$pid\" -eq \"$$\" ]]"
        ],
        "path": "bitget/deploy/bitget.sh",
        "message": "missing: ['[[ \"$pid\" -eq \"$$\" ]]']"
      },
      "weekly_evolution_pipeline": {
        "ok": false,
        "steps": [
          "config_bootstrap",
          "artifact_guard",
          "weekly_evolution",
          "walk_forward_shadow",
          "bad_tick_skip_summary",
          "llm_proposal_summary",
          "cost_report",
          "gmm_dna_alpha_report",
          "b1_ladder_fastcheck",
          "weekly_coin_pri",
          "weekly_coin_regime_archive",
          "regime_deep_archive",
          "weekly_flow_master",
          "genesis_backfill_weekly",
          "weekend_grand_report",
          "weekly_action_plan",
          "weekly_executive_summary"
        ],
        "tail_ok": false,
        "critical_ok": true,
        "message": "weekly pipeline drift"
      },
      "deep_dive_dna_autopsy_ssot": {
        "ok": true,
        "missing": [],
        "inline_dna_leaks": [],
        "message": "deep_dive dna_autopsy SSOT ok"
      },
      "daemon_sniper_policy": {
        "ok": true,
        "missing": [],
        "unconditional_sniper": false,
        "message": "daemon sniper policy ok"
      },
      "daemon_public_ws_policy": {
        "ok": true,
        "missing": [],
        "message": "daemon public WS policy ok"
      },
      "daemon_private_ws_policy": {
        "ok": true,
        "missing": [],
        "message": "daemon private WS policy ok"
      },
      "oms_book_consumer_ssot": {
        "ok": true,
        "failed": [],
        "details": {
          "executor": {
            "ok": true,
            "missing": [],
            "forbidden_present": []
          },
          "account_snapshot": {
            "ok": true,
            "missing": []
          },
          "market_price_snapshot": {
            "ok": true,
            "missing": []
          },
          "leverage_margin_ws": {
            "ok": true,
            "missing": []
          },
          "slippage_inst_id": {
            "ok": true,
            "missing": [],
            "forbidden_present": []
          },
          "dual_plane_stats": {
            "ok": true,
            "missing": []
          },
          "dual_plane_smoke": {
            "ok": true,
            "missing": []
          },
          "daemon_oms_warn": {
            "ok": true,
            "missing": []
          },
          "memory_policy_knobs": {
            "ok": true,
            "missing": []
          }
        },
        "message": "OMS book consumer SSOT ok"
      },
      "portfolio_nav_risk_ssot": {
        "ok": false,
        "failed": [
          "execution_safety",
          "leverage_manager",
          "paper_ledger_gross"
        ],
        "details": {
          "execution_safety": {
            "ok": false,
            "missing": [
              "portfolio_nav_snapshot"
            ]
          },
          "tail_risk_gate": {
            "ok": true,
            "missing": []
          },
          "doomsday_gate": {
            "ok": true,
            "missing": []
          },
          "regime_capital_relay": {
            "ok": true,
            "missing": []
          },
          "concentration_gate": {
            "ok": true,
            "missing": []
          },
          "price_sanity_gate": {
            "ok": true,
            "missing": []
          },
          "executor": {
            "ok": true,
            "missing": []
          },
          "oms_core": {
            "ok": true,
            "missing": []
          },
          "leverage_manager": {
            "ok": false,
            "missing": [
              "max_leverage_cap"
            ]
          },
          "live_nav_manager": {
            "ok": true,
            "missing": []
          },
          "memory_policy": {
            "ok": true,
            "missing": []
          },
          "reconciliation": {
            "ok": true,
            "missing": []
          },
          "bounded_reads_gross": {
            "ok": true,
            "missing": []
          },
          "paper_ledger_gross": {
            "ok": false,
            "missing": [
              "gross_entry_blocked",
              "max_leverage_cap"
            ]
          },
          "meta_kelly_doomsday": {
            "ok": true,
            "missing": []
          },
          "doomsday_radar_score": {
            "ok": true,
            "missing": []
          }
        },
        "message": "NAV risk gate drift: ['execution_safety', 'leverage_manager', 'paper_ledger_gross']"
      },
      "integrity_backup_ssot": {
        "ok": true,
        "failed": [],
        "details": {
          "backup_script": {
            "ok": true,
            "missing": []
          },
          "disk_manager": {
            "ok": true,
            "missing": []
          },
          "memory_policy": {
            "ok": true,
            "missing": []
          },
          "pipeline": {
            "ok": true,
            "missing": []
          },
          "runtime_mode": {
            "ok": true,
            "missing": []
          },
          "bitget_sh": {
            "ok": true,
            "missing": []
          },
          "crontab_gen": {
            "ok": true,
            "missing": []
          }
        },
        "message": "integrity backup SSOT ok"
      },
      "watchdog_restart_matrix_ssot": {
        "ok": true,
        "failed": [],
        "details": {
          "watchdog": {
            "ok": true,
            "missing": []
          },
          "sudoers": {
            "ok": true,
            "missing": []
          },
          "watchdog_timer": {
            "ok": true,
            "missing": []
          },
          "async_ops_patch": {
            "ok": true,
            "missing": []
          }
        },
        "message": "watchdog restart matrix ok"
      },
      "meta_alerts_ssot": {
        "ok": true,
        "message": "meta_alerts + governor cycle ok"
      },
      "no_merge_conflict_markers": {
        "ok": true,
        "offenders": [],
        "n_offenders": 0,
        "message": "no conflict markers"
      },
      "satellite_config_hub": {
        "ok": true,
        "offenders": [],
        "scanned": 13,
        "message": "satellite config_hub only"
      },
      "config_hard_bounds_ssot": {
        "ok": true,
        "failed": [],
        "details": {
          "config_bounds": {
            "ok": true,
            "missing": []
          },
          "config_manager": {
            "ok": true,
            "missing": []
          }
        },
        "message": "config hard bounds SSOT ok"
      },
      "scanner_engine_pool_ssot": {
        "ok": true,
        "failed": [],
        "details": {
          "master_scanner": {
            "ok": true,
            "missing": []
          },
          "signal_engines": {
            "ok": true,
            "missing": []
          }
        },
        "message": "scanner engine pool SSOT ok"
      },
      "exploration_budget_market_ssot": {
        "ok": true,
        "failed": [],
        "details": {
          "market_keys": {
            "ok": true,
            "missing": []
          },
          "exploration_budget": {
            "ok": true,
            "missing": []
          }
        },
        "message": "exploration budget market SSOT ok"
      },
      "regime_kelly_audit": {
        "ok": true,
        "passed": true,
        "message": "regime/kelly audit PASS",
        "audit": {
          "ok": true,
          "passed": true,
          "isolation": {
            "config_db_filename": "bitget_system_config.sqlite",
            "config_db_is_bitget_sqlite": true,
            "config_db_path_contains_bitget": true,
            "meta_json_is_bitget": true,
            "meta_json_path": "/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget_meta_governor_state.json"
          },
          "regime": {
            "CURRENT_REGIME_KEY": "HIGH_VOL",
            "resolve_config_regime_key": "HIGH_VOL",
            "REGIME_ANALYSIS.regime_key": "HIGH_VOL",
            "META_REGIME_KEY": "HIGH_VOL",
            "misaligned": false,
            "meta_degraded": false
          },
          "kelly": {
            "DYNAMIC_KELLY_RISK": 0.006,
            "resolve_trading_kelly_base": 0.006,
            "within_hard_max": true,
            "non_negative": true
          },
          "flags": {
            "regime_keys_known": true,
            "regime_aligned": true,
            "meta_fresh": true,
            "kelly_bounds_ok": true,
            "ssot_isolation_ok": true
          },
          "failed": [],
          "config_db_path": "/var/lib/quant-bitget/data/bitget_system_config.sqlite",
          "message": "regime/kelly audit PASS"
        }
      },
      "watchdog_component": {
        "ok": true,
        "component": "bitget_auto_pilot",
        "recommended": "bitget_auto_pilot",
        "message": "watchdog component ok"
      }
    },
    "failed": [
      "pipeline_structure",
      "bitget_shell_daily_audit_guard",
      "weekly_evolution_pipeline",
      "portfolio_nav_risk_ssot"
    ],
    "message": "failed: ['pipeline_structure', 'bitget_shell_daily_audit_guard', 'weekly_evolution_pipeline', 'portfolio_nav_risk_ssot']"
  },
  "parallel_run": {
    "active": false,
    "elapsed_hours": 0.0,
    "ready_for_cutover": false
  },
  "legacy_main_running": false,
  "message": "cutover ready - set BITGET_PIPELINE_SSOT=1 and complete 48h parallel run",
  "recommendation": "Use systemd dante-bitget-* + bitget.sh cron; deprecate python -m bitget.main"
}
JSON_DUMP_RC=0
=== Step 2 env keys ===
BITGET_ASYNC_TELEGRAM=1
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
BITGET_ASYNC_TELEGRAM=1
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
=== Step 3 processes ===
pgrep_bitget.main=none
pgrep_factory_launcher=none
  dante-bitget-async.service                     loaded    active     running       Bitget async Telegram queue consumer
● dante-bitget-backup.service                    loaded    failed     failed        Bitget SQLite integrity backup (L-2 P0-5)
  dante-bitget-factory.service                   loaded    active     running       Bitget quant factory daemon (auto pilot + satellites)
  dante-bitget-journal-vacuum.service            loaded    inactive   dead          Bitget journal vacuum (dante-bitget-* disk guard P0-1)
  dante-bitget-overseer.service                  loaded    active     running       Bitget AI overseer loop
  dante-bitget-queue-worker.service              loaded    active     running       Bitget task queue worker (single serial drain of bitget_task_queue.sqlite)
● dante-bitget-snapshot.service                  loaded    failed     failed        Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)
  dante-bitget-watchdog.service                  loaded    activating start   start Bitget heartbeat watchdog (ops_events)
  dante-bitget-ws.service                        loaded    active     running       Bitget WebSocket supervisor (public ticker + optional private streams)
=== DONE (no start-parallel, no env write) ===
WROTE /tmp/cutover01_p0.out
