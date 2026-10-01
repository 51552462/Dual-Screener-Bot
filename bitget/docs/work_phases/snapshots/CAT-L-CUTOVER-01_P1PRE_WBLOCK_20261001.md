# CAT-L-CUTOVER-01 Phase 1 선행 · W-블록 출력 전문 · 2026-10-01

Handoff 커밋 `f7f767e`의 §8-1(38줄, sha256 `bbf39d61…0433e5`)과 §8-1b(10줄, sha256 `0e5a8482…00023c`)를 blob에서 바이트 그대로 추출해 Bot-2에서 각 1회 실행(읽기전용). 수정·요약 없음.

## §8-1 W-블록 (START 2026-10-01T10:28:52Z)

```text
== W-BLOCK START 2026-10-01T10:28:52Z user=ubuntu ==
== W1 sudo commands since 2026-09-26 UTC ==
Sep 26 05:26:50 ip-172-26-7-213 sudo[86970]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-async
Sep 26 08:54:37 ip-172-26-7-213 sudo[1019]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/dmesg -T
Sep 26 15:44:23 ip-172-26-7-213 sudo[6599]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 26 15:45:03 ip-172-26-7-213 sudo[6630]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 27 13:30:06 ip-172-26-7-213 sudo[19950]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/crontab -l
Sep 27 14:17:56 ip-172-26-7-213 sudo[20822]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/systemd-run --uid=ubuntu --gid=ubuntu --scope --slice=bitget-cron-heavy.slice -p MemoryMax=1610612736 -p MemoryHigh=1288490188 -p CPUQuota=80% -- sleep 2
Sep 27 14:20:57 ip-172-26-7-213 sudo[21072]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/cp /tmp/bitget-cron-heavy.slice /etc/systemd/system/bitget-cron-heavy.slice
Sep 27 14:20:57 ip-172-26-7-213 sudo[21074]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 27 14:21:01 ip-172-26-7-213 sudo[21103]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/systemctl start bitget-cron-heavy.slice
Sep 27 14:21:01 ip-172-26-7-213 sudo[21106]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/cp /etc/cron.d/dual-screener-bitget /tmp/dual-screener-bitget.pre-fence02
Sep 27 14:21:01 ip-172-26-7-213 sudo[21109]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/tee /etc/cron.d/dual-screener-bitget
Sep 27 14:21:01 ip-172-26-7-213 sudo[21111]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/chmod 644 /etc/cron.d/dual-screener-bitget
Sep 27 14:21:01 ip-172-26-7-213 sudo[21113]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/chown root:root /etc/cron.d/dual-screener-bitget
Sep 27 14:21:02 ip-172-26-7-213 sudo[21120]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/systemd-run --collect --uid=ubuntu --gid=ubuntu --scope --slice=bitget-cron-heavy.slice -p MemoryMax=1610612736 -p MemoryHigh=1288490188 -p CPUQuota=80% -- /bin/bash -c 'sleep 12; echo WRAP_OK'
Sep 27 14:21:30 ip-172-26-7-213 sudo[21184]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/cp /tmp/bitget-cron-heavy.slice /etc/systemd/system/bitget-cron-heavy.slice
Sep 27 14:21:30 ip-172-26-7-213 sudo[21186]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 27 14:21:32 ip-172-26-7-213 sudo[21215]:   ubuntu : PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/systemctl start bitget-cron-heavy.slice
Sep 27 15:44:54 ip-172-26-7-213 sudo[22611]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 27 15:45:03 ip-172-26-7-213 sudo[22646]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 28 01:48:52 ip-172-26-7-213 sudo[29362]:   ubuntu : TTY=pts/0 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh
Sep 28 01:48:52 ip-172-26-7-213 sudo[29374]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/env INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot _BG_BACKUP_DEST=/var/backups/bitget-pre-update/20260928_014852_utc /home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -c '#012import os, shutil, sqlite3, sys#012from bitget.infra.data_paths import bitget_data_dir#012#012dest = os.environ[\'_BG_BACKUP_DEST\']#012data = bitget_data_dir()#012#012if not os.access(dest, os.W_OK):#012    print(f\'backup dest not writable: {dest}\', file=sys.stderr)#012    sys.exit(1)#012#012db_names = (#012    \'bitget_market_data.sqlite\',#012    \'bitget_market_data_snapshot.sqlite\',#012    \'bitget_system_config.sqlite\',#012    \'bitget_ops_events.sqlite\',#012    \'bitget_message_queue.sqlite\',#012)#012present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]#012if not present:#012
Sep 28 01:48:53 ip-172-26-7-213 sudo[29377]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot diff --quiet
Sep 28 01:48:53 ip-172-26-7-213 sudo[29382]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot restore .
Sep 28 01:48:53 ip-172-26-7-213 sudo[29387]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot pull --ff-only
Sep 28 01:54:10 ip-172-26-7-213 sudo[29449]:   ubuntu : TTY=pts/0 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh
Sep 28 01:54:11 ip-172-26-7-213 sudo[29461]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/env INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot _BG_BACKUP_DEST=/var/backups/bitget-pre-update/20260928_015411_utc /home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -c '#012import os, shutil, sqlite3, sys#012from bitget.infra.data_paths import bitget_data_dir#012#012dest = os.environ[\'_BG_BACKUP_DEST\']#012data = bitget_data_dir()#012#012if not os.access(dest, os.W_OK):#012    print(f\'backup dest not writable: {dest}\', file=sys.stderr)#012    sys.exit(1)#012#012db_names = (#012    \'bitget_market_data.sqlite\',#012    \'bitget_market_data_snapshot.sqlite\',#012    \'bitget_system_config.sqlite\',#012    \'bitget_ops_events.sqlite\',#012    \'bitget_message_queue.sqlite\',#012)#012present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]#012if not present:#012
Sep 28 01:54:11 ip-172-26-7-213 sudo[29464]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot diff --quiet
Sep 28 01:54:11 ip-172-26-7-213 sudo[29469]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot pull --ff-only
Sep 28 02:24:09 ip-172-26-7-213 sudo[29911]:   ubuntu : TTY=pts/0 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh
Sep 28 02:24:10 ip-172-26-7-213 sudo[29923]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/env INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot _BG_BACKUP_DEST=/var/backups/bitget-pre-update/20260928_022409_utc /home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -c '#012import os, shutil, sqlite3, sys#012from bitget.infra.data_paths import bitget_data_dir#012#012dest = os.environ[\'_BG_BACKUP_DEST\']#012data = bitget_data_dir()#012#012if not os.access(dest, os.W_OK):#012    print(f\'backup dest not writable: {dest}\', file=sys.stderr)#012    sys.exit(1)#012#012db_names = (#012    \'bitget_market_data.sqlite\',#012    \'bitget_market_data_snapshot.sqlite\',#012    \'bitget_system_config.sqlite\',#012    \'bitget_ops_events.sqlite\',#012    \'bitget_message_queue.sqlite\',#012)#012present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]#012if not present:#012
Sep 28 02:24:10 ip-172-26-7-213 sudo[29926]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot diff --quiet
Sep 28 02:24:10 ip-172-26-7-213 sudo[29932]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot pull --ff-only
Sep 28 02:24:11 ip-172-26-7-213 sudo[29942]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/deploy_bitget_factory.sh
Sep 28 02:24:11 ip-172-26-7-213 sudo[29951]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-async.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29956]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-backup.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29961]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-dashboard.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29966]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-factory.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29971]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-heatmap.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29976]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-journal-vacuum.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29981]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-queue-worker.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29986]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-snapshot.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29991]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-watchdog.service
Sep 28 02:24:13 ip-172-26-7-213 sudo[29996]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-ws.service
Sep 28 02:24:13 ip-172-26-7-213 sudo[29999]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30002]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/update_bitget.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30005]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/deploy_bitget_factory.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30008]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30011]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/deploy/install_director_digest_cron.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30014]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_logrotate.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30017]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_backup.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30020]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30023]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/scripts/bitget_journal_vacuum.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30026]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/reset_bitget_pipeline.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30029]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/master_sync_bitget.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30032]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_async.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_daemon.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_dashboard.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_heatmap.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_queue_worker.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_snapshot.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_ws.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30035]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 28 02:24:17 ip-172-26-7-213 sudo[30065]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl enable dante-bitget-ws.service dante-bitget-factory.service dante-bitget-queue-worker.service dante-bitget-async.service dante-bitget-watchdog.timer dante-bitget-snapshot.timer
Sep 28 02:24:19 ip-172-26-7-213 sudo[30095]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl disable dante-bitget-dashboard.service dante-bitget-heatmap.service
Sep 28 02:24:22 ip-172-26-7-213 sudo[30125]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl reset-failed dante-bitget-dashboard.service dante-bitget-heatmap.service
Sep 28 02:24:22 ip-172-26-7-213 sudo[30128]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 28 15:46:05 ip-172-26-7-213 sudo[39098]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 28 15:46:05 ip-172-26-7-213 sudo[39096]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 29 10:27:37 ip-172-26-7-213 sudo[57910]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 29 10:27:41 ip-172-26-7-213 sudo[57972]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 29 10:27:44 ip-172-26-7-213 sudo[58008]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/install -m 0644 /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/docs/work_phases/snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron /etc/cron.d/dual-screener-bitget
Sep 29 10:27:44 ip-172-26-7-213 sudo[58010]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/rm -f /etc/systemd/system/bitget-cron-heavy.slice
Sep 29 10:27:44 ip-172-26-7-213 sudo[58012]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 29 10:28:32 ip-172-26-7-213 sudo[58155]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 29 10:28:35 ip-172-26-7-213 sudo[58215]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 29 15:45:46 ip-172-26-7-213 sudo[61996]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 29 15:45:47 ip-172-26-7-213 sudo[61999]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 30 06:00:45 ip-172-26-7-213 sudo[70828]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 30 15:45:04 ip-172-26-7-213 sudo[77831]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 30 15:45:06 ip-172-26-7-213 sudo[77836]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
W1_COUNT=74
Sep 26 05:26:50 ip-172-26-7-213 sudo[86970]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-async
Sep 26 05:26:50 ip-172-26-7-213 sudo[86970]: pam_unix(sudo:session): session opened for user root(uid=0) by (uid=1000)
== W2 git reflog ==
8a6da21 HEAD@{2026-09-30T09:42:43+00:00}: pull --ff-only: Fast-forward
c1ffe3f HEAD@{2026-09-30T06:00:40+00:00}: pull --ff-only: Fast-forward
002c612 HEAD@{2026-09-29T09:21:46+00:00}: pull --ff-only: Fast-forward
1e38166 HEAD@{2026-09-28T01:57:45+00:00}: pull --ff-only: Fast-forward
24135f4 HEAD@{2026-09-23T10:10:53+00:00}: pull --ff-only: Fast-forward
40cf631 HEAD@{2026-08-30T15:18:43+00:00}: pull: Fast-forward
6ce3a04 HEAD@{2026-08-30T13:36:49+00:00}: pull: Fast-forward
5cd37b6 HEAD@{2026-08-29T07:02:09+00:00}: pull: Fast-forward
0827e90 HEAD@{2026-08-29T06:21:46+00:00}: pull: Fast-forward
ef455ef HEAD@{2026-08-28T15:49:25+00:00}: pull: Fast-forward
c483cb3 HEAD@{2026-08-28T02:56:31+00:00}: pull --ff-only: Fast-forward
1652687 HEAD@{2026-08-28T01:33:06+00:00}: pull: Fast-forward
7c2d04a HEAD@{2026-08-25T03:21:23+00:00}: pull: Fast-forward
18939bd HEAD@{2026-08-25T02:54:20+00:00}: pull: Fast-forward
e647682 HEAD@{2026-08-25T02:25:51+00:00}: pull: Fast-forward
57d3735 HEAD@{2026-08-23T12:10:29+00:00}: reset: moving to origin/main
7c158f5 HEAD@{2026-08-23T11:03:34+00:00}: pull --ff-only: Fast-forward
07ef4be HEAD@{2026-08-23T09:25:10+00:00}: pull: Fast-forward
3e66e5f HEAD@{2026-08-23T09:21:45+00:00}: pull: Fast-forward
234e353 HEAD@{2026-08-23T06:21:25+00:00}: pull --ff-only: Fast-forward
2276771 HEAD@{2026-08-23T05:23:22+00:00}: pull --ff-only: Fast-forward
ac49da5 HEAD@{2026-08-23T05:07:33+00:00}: pull --ff-only: Fast-forward
e65b66a HEAD@{2026-08-21T02:12:19+00:00}: pull --ff-only: Fast-forward
ca5fb8c HEAD@{2026-08-20T16:10:48+00:00}: pull --ff-only: Fast-forward
3d392f4 HEAD@{2026-08-19T16:37:55+00:00}: pull --ff-only: Fast-forward
a5a83ed HEAD@{2026-08-18T15:19:00+00:00}: pull: Fast-forward
8ce608f HEAD@{2026-08-18T02:13:05+00:00}: pull --ff-only: Fast-forward
b245315 HEAD@{2026-08-17T11:34:24+00:00}: pull --ff-only: Fast-forward
28734e0 HEAD@{2026-08-17T11:18:15+00:00}: pull --ff-only: Fast-forward
d3d2ffd HEAD@{2026-08-12T02:21:49+00:00}: pull --ff-only: Fast-forward
31f0447 HEAD@{2026-08-11T15:52:17+00:00}: pull: Fast-forward
1a04d16 HEAD@{2026-08-11T02:46:46+00:00}: pull: Fast-forward
23ed7f0 HEAD@{2026-08-09T16:31:09+09:00}: pull: Fast-forward
e996d56 HEAD@{2026-08-09T16:23:05+09:00}: pull: Fast-forward
cc3d808 HEAD@{2026-08-09T16:04:59+09:00}: pull: Fast-forward
aaad40c HEAD@{2026-08-06T00:45:52+00:00}: pull: Fast-forward
6d752e6 HEAD@{2026-08-05T16:29:49+00:00}: pull: Fast-forward
2914800 HEAD@{2026-08-05T15:21:55+00:00}: pull: Fast-forward
7ff7855 HEAD@{2026-08-05T00:18:32+00:00}: pull: Fast-forward
40d1a8d HEAD@{2026-08-04T15:31:34+00:00}: pull: Fast-forward
== W3 2026-09-28 01:40-02:40 UTC systemd/cron/sudo ==
Sep 28 01:40:01 ip-172-26-7-213 CRON[29173]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:40:01 ip-172-26-7-213 CRON[29174]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 01:40:05 ip-172-26-7-213 CRON[29173]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:40:05 ip-172-26-7-213 CRON[29173]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 01:44:57 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 01:44:57 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 01:45:01 ip-172-26-7-213 CRON[29202]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:45:01 ip-172-26-7-213 CRON[29201]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:45:01 ip-172-26-7-213 CRON[29204]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --track-positions)
Sep 28 01:45:01 ip-172-26-7-213 CRON[29203]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:45:02 ip-172-26-7-213 CRON[29206]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --canary)
Sep 28 01:45:02 ip-172-26-7-213 CRON[29205]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 01:45:10 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 01:45:10 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 01:45:10 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 4.586s CPU time.
Sep 28 01:45:13 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Deactivated successfully.
Sep 28 01:45:13 ip-172-26-7-213 systemd[1]: Finished Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 01:45:13 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Consumed 5.447s CPU time.
Sep 28 01:45:22 ip-172-26-7-213 CRON[29201]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:45:22 ip-172-26-7-213 CRON[29201]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 01:45:47 ip-172-26-7-213 CRON[29202]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:45:47 ip-172-26-7-213 CRON[29202]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 01:45:50 ip-172-26-7-213 CRON[29203]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:45:50 ip-172-26-7-213 CRON[29203]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 01:47:01 ip-172-26-7-213 CRON[29247]: pam_unix(cron:session): session opened for user root(uid=0) by (uid=0)
Sep 28 01:47:01 ip-172-26-7-213 CRON[29248]: (root) CMD ( /usr/bin/systemd-run --quiet --collect --uid=ubuntu --gid=ubuntu --scope --slice=bitget-cron-heavy.slice -p MemoryMax=1610612736 -p MemoryHigh=1288490188 -p CPUQuota=80% -- /bin/bash -c 'cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim')
Sep 28 01:47:02 ip-172-26-7-213 systemd[1]: Started /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim.
Sep 28 01:48:34 ip-172-26-7-213 systemd[1]: Created slice User Slice of UID 1000.
Sep 28 01:48:34 ip-172-26-7-213 systemd[1]: Starting User Runtime Directory /run/user/1000...
Sep 28 01:48:34 ip-172-26-7-213 systemd[1]: Finished User Runtime Directory /run/user/1000.
Sep 28 01:48:34 ip-172-26-7-213 systemd[1]: Starting User Manager for UID 1000...
Sep 28 01:48:34 ip-172-26-7-213 systemd[29269]: pam_unix(systemd-user:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Queued start job for default target Main User Target.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Created slice User Application Slice.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Reached target Paths.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Reached target Timers.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Starting D-Bus User Message Bus Socket...
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on GnuPG network certificate management daemon.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on GnuPG cryptographic agent and passphrase cache (access for web browsers).
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on GnuPG cryptographic agent and passphrase cache (restricted).
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on GnuPG cryptographic agent (ssh-agent emulation).
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on GnuPG cryptographic agent and passphrase cache.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on debconf communication socket.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on REST API socket for snapd user session agent.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on D-Bus User Message Bus Socket.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Reached target Sockets.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Reached target Basic System.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Reached target Main User Target.
Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Startup finished in 719ms.
Sep 28 01:48:35 ip-172-26-7-213 systemd[1]: Started User Manager for UID 1000.
Sep 28 01:48:52 ip-172-26-7-213 sudo[29362]:   ubuntu : TTY=pts/0 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh
Sep 28 01:48:52 ip-172-26-7-213 sudo[29362]: pam_unix(sudo:session): session opened for user root(uid=0) by ubuntu(uid=1000)
Sep 28 01:48:52 ip-172-26-7-213 sudo[29374]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/env INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot _BG_BACKUP_DEST=/var/backups/bitget-pre-update/20260928_014852_utc /home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -c '#012import os, shutil, sqlite3, sys#012from bitget.infra.data_paths import bitget_data_dir#012#012dest = os.environ[\'_BG_BACKUP_DEST\']#012data = bitget_data_dir()#012#012if not os.access(dest, os.W_OK):#012    print(f\'backup dest not writable: {dest}\', file=sys.stderr)#012    sys.exit(1)#012#012db_names = (#012    \'bitget_market_data.sqlite\',#012    \'bitget_market_data_snapshot.sqlite\',#012    \'bitget_system_config.sqlite\',#012    \'bitget_ops_events.sqlite\',#012    \'bitget_message_queue.sqlite\',#012)#012present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]#012if not present:#012
Sep 28 01:48:52 ip-172-26-7-213 sudo[29374]:     root : (command continued) print(f\'  no bitget sqlite in {data} — first deploy, backup skipped\')#012    sys.exit(0)#012#012def backup_sqlite(src, out_name):#012    out = os.path.join(dest, out_name)#012    try:#012        s = sqlite3.connect(f\'file:{src}?mode=ro\', uri=True, timeout=60)#012        d = sqlite3.connect(out, timeout=60)#012        try:#012            s.backup(d)#012        finally:#012            d.close()#012            s.close()#012    except Exception as e:#012        try:#012            shutil.copy2(src, out)#012        except Exception as e2:#012            print(f\'  backup failed {out_name}: {e}; copy2: {e2}\', file=sys.stderr)#012            raise#012    print(f\'  sqlite: {out_name}\')#012#012for name in db_names:#012    src = os.path.join(data, name)#012    if os.path.isfile(src):#012        backup_sqlite(src, name)#012#012for rel in (\'bitget_system_config.json\', \'bitget_schedule_lock_state.json\'):#012    src =
Sep 28 01:48:52 ip-172-26-7-213 sudo[29374]:     root : (command continued) os.path.join(data, rel)#012    if os.path.isfile(src):#012        shutil.copy2(src, os.path.join(dest, rel))#012        print(f\'  file: {rel}\')#012print(f\'  data_dir={data}\')#012'
Sep 28 01:48:52 ip-172-26-7-213 sudo[29374]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:48:53 ip-172-26-7-213 sudo[29374]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:48:53 ip-172-26-7-213 sudo[29377]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot diff --quiet
Sep 28 01:48:53 ip-172-26-7-213 sudo[29377]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:48:53 ip-172-26-7-213 sudo[29377]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:48:53 ip-172-26-7-213 sudo[29382]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot restore .
Sep 28 01:48:53 ip-172-26-7-213 sudo[29382]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:48:53 ip-172-26-7-213 sudo[29382]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:48:53 ip-172-26-7-213 sudo[29387]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot pull --ff-only
Sep 28 01:48:53 ip-172-26-7-213 sudo[29387]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:48:58 ip-172-26-7-213 sudo[29387]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:48:58 ip-172-26-7-213 sudo[29362]: pam_unix(sudo:session): session closed for user root
Sep 28 01:49:58 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 01:49:58 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 01:50:00 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 01:50:00 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 01:50:00 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 01:50:01 ip-172-26-7-213 CRON[29419]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:50:01 ip-172-26-7-213 CRON[29420]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 01:50:10 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 01:50:10 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 01:50:10 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 5.353s CPU time.
Sep 28 01:50:11 ip-172-26-7-213 CRON[29419]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:50:11 ip-172-26-7-213 CRON[29419]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 01:53:01 ip-172-26-7-213 CRON[29429]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:53:01 ip-172-26-7-213 CRON[29430]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --reconcile)
Sep 28 01:54:10 ip-172-26-7-213 sudo[29449]:   ubuntu : TTY=pts/0 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh
Sep 28 01:54:10 ip-172-26-7-213 sudo[29449]: pam_unix(sudo:session): session opened for user root(uid=0) by ubuntu(uid=1000)
Sep 28 01:54:11 ip-172-26-7-213 sudo[29461]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/env INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot _BG_BACKUP_DEST=/var/backups/bitget-pre-update/20260928_015411_utc /home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -c '#012import os, shutil, sqlite3, sys#012from bitget.infra.data_paths import bitget_data_dir#012#012dest = os.environ[\'_BG_BACKUP_DEST\']#012data = bitget_data_dir()#012#012if not os.access(dest, os.W_OK):#012    print(f\'backup dest not writable: {dest}\', file=sys.stderr)#012    sys.exit(1)#012#012db_names = (#012    \'bitget_market_data.sqlite\',#012    \'bitget_market_data_snapshot.sqlite\',#012    \'bitget_system_config.sqlite\',#012    \'bitget_ops_events.sqlite\',#012    \'bitget_message_queue.sqlite\',#012)#012present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]#012if not present:#012
Sep 28 01:54:11 ip-172-26-7-213 sudo[29461]:     root : (command continued) print(f\'  no bitget sqlite in {data} — first deploy, backup skipped\')#012    sys.exit(0)#012#012def backup_sqlite(src, out_name):#012    out = os.path.join(dest, out_name)#012    try:#012        s = sqlite3.connect(f\'file:{src}?mode=ro\', uri=True, timeout=60)#012        d = sqlite3.connect(out, timeout=60)#012        try:#012            s.backup(d)#012        finally:#012            d.close()#012            s.close()#012    except Exception as e:#012        try:#012            shutil.copy2(src, out)#012        except Exception as e2:#012            print(f\'  backup failed {out_name}: {e}; copy2: {e2}\', file=sys.stderr)#012            raise#012    print(f\'  sqlite: {out_name}\')#012#012for name in db_names:#012    src = os.path.join(data, name)#012    if os.path.isfile(src):#012        backup_sqlite(src, name)#012#012for rel in (\'bitget_system_config.json\', \'bitget_schedule_lock_state.json\'):#012    src =
Sep 28 01:54:11 ip-172-26-7-213 sudo[29461]:     root : (command continued) os.path.join(data, rel)#012    if os.path.isfile(src):#012        shutil.copy2(src, os.path.join(dest, rel))#012        print(f\'  file: {rel}\')#012print(f\'  data_dir={data}\')#012'
Sep 28 01:54:11 ip-172-26-7-213 sudo[29461]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:54:11 ip-172-26-7-213 sudo[29461]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:54:11 ip-172-26-7-213 sudo[29464]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot diff --quiet
Sep 28 01:54:11 ip-172-26-7-213 sudo[29464]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:54:11 ip-172-26-7-213 sudo[29464]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:54:11 ip-172-26-7-213 sudo[29469]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot pull --ff-only
Sep 28 01:54:11 ip-172-26-7-213 sudo[29469]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 01:54:13 ip-172-26-7-213 sudo[29469]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 01:54:13 ip-172-26-7-213 sudo[29449]: pam_unix(sudo:session): session closed for user root
Sep 28 01:54:59 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 01:54:59 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 01:55:00 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 01:55:00 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 01:55:00 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 01:55:01 ip-172-26-7-213 CRON[29499]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 01:55:01 ip-172-26-7-213 CRON[29500]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 01:55:10 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 01:55:10 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 01:55:10 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 4.639s CPU time.
Sep 28 01:55:11 ip-172-26-7-213 CRON[29499]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:55:11 ip-172-26-7-213 CRON[29499]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 01:55:14 ip-172-26-7-213 CRON[29429]: (CRON) info (No MTA installed, discarding output)
Sep 28 01:55:14 ip-172-26-7-213 CRON[29429]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:00:01 ip-172-26-7-213 CRON[29706]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:00:01 ip-172-26-7-213 CRON[29708]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --canary)
Sep 28 02:00:01 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 02:00:01 ip-172-26-7-213 CRON[29704]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:00:01 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 02:00:01 ip-172-26-7-213 CRON[29705]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:00:01 ip-172-26-7-213 CRON[29710]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --track-positions)
Sep 28 02:00:01 ip-172-26-7-213 CRON[29711]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 02:00:04 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 02:00:04 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 02:00:04 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 02:00:13 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 02:00:13 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 02:00:13 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 5.149s CPU time.
Sep 28 02:00:22 ip-172-26-7-213 CRON[29704]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:00:22 ip-172-26-7-213 CRON[29704]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:00:47 ip-172-26-7-213 CRON[29706]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:00:47 ip-172-26-7-213 CRON[29706]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:02:33 ip-172-26-7-213 CRON[29705]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:02:33 ip-172-26-7-213 CRON[29705]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:05:01 ip-172-26-7-213 CRON[29763]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:05:01 ip-172-26-7-213 CRON[29764]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 02:05:01 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 02:05:01 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 02:05:04 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 02:05:04 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 02:05:04 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 02:05:12 ip-172-26-7-213 CRON[29763]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:05:12 ip-172-26-7-213 CRON[29763]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:05:15 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 02:05:15 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 02:05:15 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 4.963s CPU time.
Sep 28 02:10:01 ip-172-26-7-213 CRON[29792]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:10:01 ip-172-26-7-213 CRON[29793]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 02:10:04 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 02:10:04 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 02:10:07 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 02:10:07 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 02:10:07 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 02:10:12 ip-172-26-7-213 CRON[29792]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:10:12 ip-172-26-7-213 CRON[29792]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:10:15 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 02:10:15 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 02:10:15 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 4.153s CPU time.
Sep 28 02:15:02 ip-172-26-7-213 CRON[29823]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:15:02 ip-172-26-7-213 CRON[29822]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:15:02 ip-172-26-7-213 CRON[29825]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 02:15:02 ip-172-26-7-213 CRON[29826]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --track-positions)
Sep 28 02:15:02 ip-172-26-7-213 CRON[29824]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:15:02 ip-172-26-7-213 CRON[29829]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --canary)
Sep 28 02:15:05 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 02:15:05 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 02:15:08 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 02:15:08 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 02:15:08 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 02:15:17 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 02:15:17 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 02:15:17 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 3.952s CPU time.
Sep 28 02:15:23 ip-172-26-7-213 CRON[29822]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:15:23 ip-172-26-7-213 CRON[29822]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:15:55 ip-172-26-7-213 CRON[29824]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:15:55 ip-172-26-7-213 CRON[29824]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:17:01 ip-172-26-7-213 CRON[29880]: pam_unix(cron:session): session opened for user root(uid=0) by (uid=0)
Sep 28 02:17:01 ip-172-26-7-213 CRON[29881]: (root) CMD (   cd / && run-parts --report /etc/cron.hourly)
Sep 28 02:17:01 ip-172-26-7-213 CRON[29880]: pam_unix(cron:session): session closed for user root
Sep 28 02:17:35 ip-172-26-7-213 CRON[29823]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:17:35 ip-172-26-7-213 CRON[29823]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:20:01 ip-172-26-7-213 CRON[29884]: pam_unix(cron:session): session opened for user ubuntu(uid=1000) by (uid=0)
Sep 28 02:20:01 ip-172-26-7-213 CRON[29885]: (ubuntu) CMD ( cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog)
Sep 28 02:20:07 ip-172-26-7-213 CRON[29884]: (CRON) info (No MTA installed, discarding output)
Sep 28 02:20:07 ip-172-26-7-213 CRON[29884]: pam_unix(cron:session): session closed for user ubuntu
Sep 28 02:20:27 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Sep 28 02:20:27 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
Sep 28 02:20:29 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Sep 28 02:20:29 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Sep 28 02:20:29 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Sep 28 02:20:33 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
Sep 28 02:20:33 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
Sep 28 02:20:33 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 4.786s CPU time.
Sep 28 02:24:09 ip-172-26-7-213 sudo[29911]:   ubuntu : TTY=pts/0 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh
Sep 28 02:24:09 ip-172-26-7-213 sudo[29911]: pam_unix(sudo:session): session opened for user root(uid=0) by ubuntu(uid=1000)
Sep 28 02:24:10 ip-172-26-7-213 sudo[29923]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/env INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot _BG_BACKUP_DEST=/var/backups/bitget-pre-update/20260928_022409_utc /home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -c '#012import os, shutil, sqlite3, sys#012from bitget.infra.data_paths import bitget_data_dir#012#012dest = os.environ[\'_BG_BACKUP_DEST\']#012data = bitget_data_dir()#012#012if not os.access(dest, os.W_OK):#012    print(f\'backup dest not writable: {dest}\', file=sys.stderr)#012    sys.exit(1)#012#012db_names = (#012    \'bitget_market_data.sqlite\',#012    \'bitget_market_data_snapshot.sqlite\',#012    \'bitget_system_config.sqlite\',#012    \'bitget_ops_events.sqlite\',#012    \'bitget_message_queue.sqlite\',#012)#012present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]#012if not present:#012
Sep 28 02:24:10 ip-172-26-7-213 sudo[29923]:     root : (command continued) print(f\'  no bitget sqlite in {data} — first deploy, backup skipped\')#012    sys.exit(0)#012#012def backup_sqlite(src, out_name):#012    out = os.path.join(dest, out_name)#012    try:#012        s = sqlite3.connect(f\'file:{src}?mode=ro\', uri=True, timeout=60)#012        d = sqlite3.connect(out, timeout=60)#012        try:#012            s.backup(d)#012        finally:#012            d.close()#012            s.close()#012    except Exception as e:#012        try:#012            shutil.copy2(src, out)#012        except Exception as e2:#012            print(f\'  backup failed {out_name}: {e}; copy2: {e2}\', file=sys.stderr)#012            raise#012    print(f\'  sqlite: {out_name}\')#012#012for name in db_names:#012    src = os.path.join(data, name)#012    if os.path.isfile(src):#012        backup_sqlite(src, name)#012#012for rel in (\'bitget_system_config.json\', \'bitget_schedule_lock_state.json\'):#012    src =
Sep 28 02:24:10 ip-172-26-7-213 sudo[29923]:     root : (command continued) os.path.join(data, rel)#012    if os.path.isfile(src):#012        shutil.copy2(src, os.path.join(dest, rel))#012        print(f\'  file: {rel}\')#012print(f\'  data_dir={data}\')#012'
Sep 28 02:24:10 ip-172-26-7-213 sudo[29923]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 02:24:10 ip-172-26-7-213 sudo[29923]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 02:24:10 ip-172-26-7-213 sudo[29926]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot diff --quiet
Sep 28 02:24:10 ip-172-26-7-213 sudo[29926]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
Sep 28 02:24:10 ip-172-26-7-213 sudo[29926]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 02:24:10 ip-172-26-7-213 sudo[29932]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=ubuntu ; COMMAND=/usr/bin/git -C /home/ubuntu/dante_bots/Dual-Screener-Bot pull --ff-only
Sep 28 02:24:10 ip-172-26-7-213 sudo[29932]: pam_unix(sudo:session): session opened for user ubuntu(uid=1000) by ubuntu(uid=0)
== W4 cutover_check log (2026-09-30 15:49:53 KST) ==
-rw-rw-r-- 1 ubuntu ubuntu 0 2026-09-30 06:49:53.239536112 +0000 /var/lib/quant-bitget/logs/bitget_cutover_check_20260930_154953.log
0 /var/lib/quant-bitget/logs/bitget_cutover_check_20260930_154953.log
--TAIL--
-- logs started 2026-09-30 14:30-15:59 KST --
-rw-rw-r-- 1 ubuntu ubuntu      882 2026-09-30 14:30:43.625213057 +0000 bitget_canary_20260930_143001.log
-rw-rw-r-- 1 ubuntu ubuntu      882 2026-09-30 14:45:44.825091819 +0000 bitget_canary_20260930_144502.log
-rw-rw-r-- 1 ubuntu ubuntu      882 2026-09-30 15:00:39.912931685 +0000 bitget_canary_20260930_150002.log
-rw-rw-r-- 1 ubuntu ubuntu      882 2026-09-30 15:15:54.829899725 +0000 bitget_canary_20260930_151502.log
-rw-rw-r-- 1 ubuntu ubuntu      881 2026-09-30 15:30:51.384758818 +0000 bitget_canary_20260930_153001.log
-rw-rw-r-- 1 ubuntu ubuntu      878 2026-09-30 15:45:36.582587209 +0000 bitget_canary_20260930_154501.log
-rw-rw-r-- 1 ubuntu ubuntu        0 2026-09-30 06:49:53.239536112 +0000 bitget_cutover_check_20260930_154953.log
-rw-rw-r-- 1 ubuntu ubuntu      248 2026-09-30 14:53:10.585001977 +0000 bitget_reconcile_20260930_145301.log
-rw-rw-r-- 1 ubuntu ubuntu      359 2026-09-30 15:53:03.298524905 +0000 bitget_reconcile_20260930_155301.log
-rw-rw-r-- 1 ubuntu ubuntu       78 2026-09-30 15:07:02.984586904 +0000 bitget_scan_futures_ema5_r2_20260930_150702.log
-rw-rw-r-- 1 ubuntu ubuntu      299 2026-09-30 14:32:25.146642292 +0000 bitget_track_positions_20260930_143001.log
-rw-rw-r-- 1 ubuntu ubuntu      299 2026-09-30 14:47:31.143548112 +0000 bitget_track_positions_20260930_144502.log
-rw-rw-r-- 1 ubuntu ubuntu      544 2026-09-30 15:00:33.552904464 +0000 bitget_track_positions_20260930_150002.log
-rw-rw-r-- 1 ubuntu ubuntu      300 2026-09-30 15:17:28.183304408 +0000 bitget_track_positions_20260930_151502.log
-rw-rw-r-- 1 ubuntu ubuntu      301 2026-09-30 15:32:33.543201112 +0000 bitget_track_positions_20260930_153002.log
-rw-rw-r-- 1 ubuntu ubuntu      433 2026-09-30 15:45:30.198559537 +0000 bitget_track_positions_20260930_154501.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 14:30:15.636092037 +0000 bitget_watchdog_20260930_143001.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 05:30:15.386715307 +0000 bitget_watchdog_20260930_143005.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 14:35:07.557343767 +0000 bitget_watchdog_20260930_143501.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 05:35:34.433113238 +0000 bitget_watchdog_20260930_143527.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 14:40:07.627639947 +0000 bitget_watchdog_20260930_144002.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 05:40:57.304518698 +0000 bitget_watchdog_20260930_144051.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 14:45:17.624974797 +0000 bitget_watchdog_20260930_144502.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 05:46:02.354852513 +0000 bitget_watchdog_20260930_144556.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 14:50:07.074214148 +0000 bitget_watchdog_20260930_145001.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 05:51:03.313158114 +0000 bitget_watchdog_20260930_145058.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 14:55:04.669493231 +0000 bitget_watchdog_20260930_145501.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 05:56:11.795507725 +0000 bitget_watchdog_20260930_145606.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 15:00:16.828832992 +0000 bitget_watchdog_20260930_150002.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:01:15.234789665 +0000 bitget_watchdog_20260930_150106.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 15:05:04.843106985 +0000 bitget_watchdog_20260930_150501.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:06:35.157235368 +0000 bitget_watchdog_20260930_150628.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 15:10:08.436391711 +0000 bitget_watchdog_20260930_151001.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:11:57.223635890 +0000 bitget_watchdog_20260930_151151.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 15:15:17.786738520 +0000 bitget_watchdog_20260930_151502.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:16:57.712942996 +0000 bitget_watchdog_20260930_151652.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 15:20:09.392982638 +0000 bitget_watchdog_20260930_152001.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:21:59.552257325 +0000 bitget_watchdog_20260930_152154.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 15:25:08.989283222 +0000 bitget_watchdog_20260930_152501.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:27:01.073571994 +0000 bitget_watchdog_20260930_152655.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 15:30:18.991618976 +0000 bitget_watchdog_20260930_153001.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:32:04.504877071 +0000 bitget_watchdog_20260930_153157.log
-rw-rw-r-- 1 ubuntu ubuntu      170 2026-09-30 15:35:09.716877572 +0000 bitget_watchdog_20260930_153501.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:37:04.279171806 +0000 bitget_watchdog_20260930_153658.log
-rw-rw-r-- 1 ubuntu ubuntu      236 2026-09-30 15:40:08.907170553 +0000 bitget_watchdog_20260930_154001.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:42:10.916503501 +0000 bitget_watchdog_20260930_154205.log
-rw-rw-r-- 1 ubuntu ubuntu      698 2026-09-30 15:46:39.945861965 +0000 bitget_watchdog_20260930_154502.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:47:16.193835981 +0000 bitget_watchdog_20260930_154709.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 15:50:02.686742293 +0000 bitget_watchdog_20260930_155001.log
-rw-r--r-- 1 ubuntu ubuntu      281 2026-09-30 06:53:21.552417370 +0000 bitget_watchdog_20260930_155209.log
-rw-rw-r-- 1 ubuntu ubuntu      171 2026-09-30 15:55:02.447040356 +0000 bitget_watchdog_20260930_155501.log
-rw-r--r-- 1 ubuntu ubuntu      171 2026-09-30 06:57:35.230544365 +0000 bitget_watchdog_20260930_155729.log
== W5 backup / snapshot ==
# /etc/systemd/system/dante-bitget-backup.service
[Unit]
Description=Bitget SQLite integrity backup (L-2 P0-5)
After=network-online.target

[Service]
Type=oneshot
User=root
Group=root
WorkingDirectory=/home/ubuntu/dante_bots/Dual-Screener-Bot
EnvironmentFile=-/home/ubuntu/dante_bots/Dual-Screener-Bot/.env
EnvironmentFile=-/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/.env
ExecStart=/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh

[Install]
WantedBy=multi-user.target

# /etc/systemd/system/dante-bitget-snapshot.service
[Unit]
Description=Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/dante_bots/Dual-Screener-Bot
EnvironmentFile=-/home/ubuntu/dante_bots/Dual-Screener-Bot/.env
EnvironmentFile=-/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/.env
ExecStart=/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_snapshot.sh
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SQLITE3_CLI=yes
Sep 26 09:12:21 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
Sep 26 09:12:21 ip-172-26-7-213 bash[1364]: /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh: line 32: python: command not found
Sep 26 09:12:21 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 26 09:12:21 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 26 09:12:21 ip-172-26-7-213 systemd[1]: Failed to start Bitget SQLite integrity backup (L-2 P0-5).
Sep 27 00:30:01 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
Sep 27 00:30:02 ip-172-26-7-213 bash[11652]: /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh: line 32: python: command not found
Sep 27 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 27 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 27 00:30:02 ip-172-26-7-213 systemd[1]: Failed to start Bitget SQLite integrity backup (L-2 P0-5).
Sep 28 00:30:01 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
Sep 28 00:30:02 ip-172-26-7-213 bash[28536]: /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh: line 32: python: command not found
Sep 28 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 28 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 28 00:30:02 ip-172-26-7-213 systemd[1]: Failed to start Bitget SQLite integrity backup (L-2 P0-5).
Sep 29 00:30:01 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
Sep 29 00:30:01 ip-172-26-7-213 bash[44532]: /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh: line 32: python: command not found
Sep 29 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 29 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 29 00:30:01 ip-172-26-7-213 systemd[1]: Failed to start Bitget SQLite integrity backup (L-2 P0-5).
Sep 30 00:30:02 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
Sep 30 00:30:02 ip-172-26-7-213 bash[66970]: /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh: line 32: python: command not found
Sep 30 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 30 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 30 00:30:02 ip-172-26-7-213 systemd[1]: Failed to start Bitget SQLite integrity backup (L-2 P0-5).
Oct 01 00:30:01 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
Oct 01 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Oct 01 00:30:01 ip-172-26-7-213 bash[83075]: /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh: line 32: python: command not found
Oct 01 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Oct 01 00:30:01 ip-172-26-7-213 systemd[1]: Failed to start Bitget SQLite integrity backup (L-2 P0-5).
Oct 01 10:00:49 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Oct 01 10:00:49 ip-172-26-7-213 bash[89272]: [2026-10-01 19:00:49] [INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131
Oct 01 10:00:49 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Oct 01 10:00:49 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Oct 01 10:00:49 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Oct 01 10:05:49 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Oct 01 10:05:49 ip-172-26-7-213 bash[89300]: [2026-10-01 19:05:49] [INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131
Oct 01 10:05:49 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Oct 01 10:05:49 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Oct 01 10:05:49 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Oct 01 10:10:57 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Oct 01 10:10:58 ip-172-26-7-213 bash[89329]: [2026-10-01 19:10:58] [INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131
Oct 01 10:10:58 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Oct 01 10:10:58 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Oct 01 10:10:58 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Oct 01 10:16:21 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Oct 01 10:16:22 ip-172-26-7-213 bash[89392]: [2026-10-01 19:16:22] [INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131
Oct 01 10:16:22 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Oct 01 10:16:22 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Oct 01 10:16:22 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Oct 01 10:21:27 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Oct 01 10:21:28 ip-172-26-7-213 bash[89424]: [2026-10-01 19:21:28] [INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131
Oct 01 10:21:29 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Oct 01 10:21:29 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Oct 01 10:21:29 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
Oct 01 10:26:47 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
Oct 01 10:26:48 ip-172-26-7-213 bash[89452]: [2026-10-01 19:26:48] [INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131
Oct 01 10:26:49 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Main process exited, code=exited, status=1/FAILURE
Oct 01 10:26:49 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Failed with result 'exit-code'.
Oct 01 10:26:49 ip-172-26-7-213 systemd[1]: Failed to start Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite).
-- backup results since 2026-09-01 --
Sep 23 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 23 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 24 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 24 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 25 00:30:00 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 25 00:30:00 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 26 00:30:00 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 26 00:30:00 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 26 09:12:21 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 26 09:12:21 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 27 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 27 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 28 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 28 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 29 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 29 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Sep 30 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Sep 30 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
Oct 01 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Main process exited, code=exited, status=127/n/a
Oct 01 00:30:01 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
-- snapshot last successes --
Oct 01 09:35:06 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Deactivated successfully.
Oct 01 09:40:23 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Deactivated successfully.
Oct 01 09:45:33 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.service: Deactivated successfully.
SNAPSHOT_FAILS_SINCE_0926=1122
== W6 mode-only changes ==
 mode change 100644 => 100755 bitget/deploy/backup_bitget_db.sh
 mode change 100644 => 100755 bitget/deploy/diagnose_coin_digest.sh
 mode change 100644 => 100755 bitget/deploy/entrypoints/run_bitget_queue_worker.sh
 mode change 100644 => 100755 bitget/deploy/install_bitget_backup.sh
 mode change 100644 => 100755 bitget/deploy/install_bitget_logrotate.sh
 mode change 100644 => 100755 bitget/deploy/master_sync_bitget.sh
 mode change 100644 => 100755 bitget/deploy/reset_bitget_pipeline.sh
 mode change 100644 => 100755 bitget/deploy/scripts/bitget_journal_vacuum.sh
 mode change 100644 => 100755 bitget/deploy/uninstall_stock_north_star_cron.sh
 mode change 100644 => 100755 deploy/install_director_digest_cron.sh
== W7 root-owned in repo (UTC mtime) ==
2026-07-02T13:29 root ./bitget/__pycache__/__init__.cpython-310.pyc
2026-07-02T13:34 root ./__pycache__/low_ram_sqlite_pragmas.cpython-310.pyc
2026-07-02T13:34 root ./__pycache__/sqlite_schema_guard.cpython-310.pyc
2026-07-02T13:34 root ./__pycache__/telegram_env.cpython-310.pyc
2026-07-02T13:34 root ./bitget/__pycache__/env.cpython-310.pyc
2026-07-02T13:34 root ./bitget/__pycache__/symbol_utils.cpython-310.pyc
2026-07-02T13:34 root ./bitget/forward/__pycache__/__init__.cpython-310.pyc
2026-07-02T13:34 root ./bitget/forward/__pycache__/_core.cpython-310.pyc
2026-07-02T13:34 root ./bitget/governance/__pycache__/__init__.cpython-310.pyc
2026-07-02T13:34 root ./bitget/pipelines/__pycache__/__init__.cpython-310.pyc
2026-07-02T13:34 root ./reports/__pycache__/__init__.cpython-310.pyc
2026-08-11T15:52 root ./__pycache__/factory_scan_schedule.cpython-310.pyc
2026-09-23T10:11 root ./bitget/infra/__pycache__/data_paths.cpython-310.pyc
== W-BLOCK END 2026-10-01T10:29:02Z ==

--STDERR--

SSH_RC=0
```

## §8-1b W3b (START 2026-10-01T10:29:09Z)

```text
== W3b START 2026-10-01T10:29:09Z user=ubuntu ==
== W3b-1 kernel 2026-09-27 14:20 ~ 09-28 02:40 UTC ==
KLINES=12
Sep 27 14:20:58 ip-172-26-7-213 systemd-fstab-generator[21090]: Failed to create unit file /run/systemd/generator/swapfile.swap, as it already exists. Duplicate entry in /etc/fstab?
Sep 27 14:21:31 ip-172-26-7-213 systemd-fstab-generator[21202]: Failed to create unit file /run/systemd/generator/swapfile.swap, as it already exists. Duplicate entry in /etc/fstab?
OOM_HITS=0
== W3b-2 bitget long-running units, same window ==
Sep 27 15:46:24 ip-172-26-7-213 systemd[1]: dante-bitget-queue-worker.service: Main process exited, code=killed, status=9/KILL
Sep 27 15:46:24 ip-172-26-7-213 systemd[1]: dante-bitget-queue-worker.service: Failed with result 'timeout'.
== W3b END 2026-10-01T10:29:09Z ==

--STDERR--

SSH_RC=0
```
