# CAT-L-CUTOVER-01 Phase 1 선행 · X-블록 출력 전문 · 2026-10-01

Handoff 커밋 `27a14b0` §8-2 X-블록(48줄, CR 0, sha256 `768a1189…295b21`)을 blob에서 바이트 그대로 추출해 Bot-2에서 1회 실행(읽기전용). SSH_RC 0, 출력 88125 bytes, stderr 0. 수정·요약 없음.

```text
== X-BLOCK START 2026-10-01T11:30:28Z user=ubuntu ==
== X1 boots / kernel OOM since current boot ==
-3 ac6b70aec93a479784a47ed93a81c829 Mon 2026-09-07 08:18:54 UTC—Sun 2026-09-13 14:56:57 UTC
-2 b54245277bd0487f82f7a923696bd87f Sun 2026-09-13 15:03:12 UTC—Tue 2026-09-22 05:49:49 UTC
-1 8f2b44c675f240869b6579e17903f944 Tue 2026-09-22 06:25:59 UTC—Sat 2026-09-26 08:50:08 UTC
 0 9a28ae32173940c0a4284e920c11e727 Sat 2026-09-26 08:52:17 UTC—Thu 2026-10-01 11:30:21 UTC
2026-09-26 08:52:17
KLINES_BOOT0=674
Sep 26 08:52:17 ip-172-26-7-213 kernel: Linux version 6.8.0-1063-aws (buildd@lcy02-amd64-060) (x86_64-linux-gnu-gcc-12 (Ubuntu 12.3.0-1ubuntu1~22.04.3) 12.3.0, GNU ld (GNU Binutils for Ubuntu) 2.38) #66~22.04.1-Ubuntu SMP Fri Aug  7 17:45:18 UTC 2026 (Ubuntu 6.8.0-1063.66~22.04.1-aws 6.8.12)
OOM_HITS_BOOT0=0
== X2 cron.d reload timeline since 2026-09-26 UTC ==
Sep 27 14:21:01 ip-172-26-7-213 cron[404]: (*system*dual-screener-bitget) RELOAD (/etc/cron.d/dual-screener-bitget)
Sep 28 02:25:01 ip-172-26-7-213 cron[404]: (*system*dual-screener-bitget) RELOAD (/etc/cron.d/dual-screener-bitget)
Sep 29 10:28:01 ip-172-26-7-213 cron[404]: (*system*dual-screener-bitget) RELOAD (/etc/cron.d/dual-screener-bitget)
Sep 29 10:29:01 ip-172-26-7-213 cron[404]: (*system*dual-screener-bitget) RELOAD (/etc/cron.d/dual-screener-bitget)
Sep 30 06:01:01 ip-172-26-7-213 cron[404]: (*system*dual-screener-bitget) RELOAD (/etc/cron.d/dual-screener-bitget)
== X2b 2026-09-28 02:24-02:30 UTC systemd/sudo ==
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
Sep 28 02:24:11 ip-172-26-7-213 sudo[29932]: pam_unix(sudo:session): session closed for user ubuntu
Sep 28 02:24:11 ip-172-26-7-213 sudo[29942]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/deploy_bitget_factory.sh
Sep 28 02:24:11 ip-172-26-7-213 sudo[29942]: pam_unix(sudo:session): session opened for user root(uid=0) by ubuntu(uid=0)
Sep 28 02:24:11 ip-172-26-7-213 sudo[29951]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-async.service
Sep 28 02:24:11 ip-172-26-7-213 sudo[29951]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:11 ip-172-26-7-213 sudo[29951]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29956]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-backup.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29956]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29956]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29961]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-dashboard.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29961]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29961]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29966]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-factory.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29966]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29966]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29971]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-heatmap.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29971]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29971]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29976]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-journal-vacuum.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29976]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29976]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29981]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-queue-worker.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29981]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29981]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29986]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-snapshot.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29986]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:12 ip-172-26-7-213 sudo[29986]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:12 ip-172-26-7-213 sudo[29991]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-watchdog.service
Sep 28 02:24:12 ip-172-26-7-213 sudo[29991]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:13 ip-172-26-7-213 sudo[29991]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:13 ip-172-26-7-213 sudo[29996]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/tee /etc/systemd/system/dante-bitget-ws.service
Sep 28 02:24:13 ip-172-26-7-213 sudo[29996]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:13 ip-172-26-7-213 sudo[29996]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:13 ip-172-26-7-213 sudo[29999]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[29999]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:13 ip-172-26-7-213 sudo[29999]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:13 ip-172-26-7-213 sudo[30002]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/update_bitget.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30002]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:13 ip-172-26-7-213 sudo[30002]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:13 ip-172-26-7-213 sudo[30005]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/deploy_bitget_factory.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30005]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:13 ip-172-26-7-213 sudo[30005]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:13 ip-172-26-7-213 sudo[30008]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30008]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:13 ip-172-26-7-213 sudo[30008]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:13 ip-172-26-7-213 sudo[30011]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/deploy/install_director_digest_cron.sh
Sep 28 02:24:13 ip-172-26-7-213 sudo[30011]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30011]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30014]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_logrotate.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30014]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30014]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30017]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_backup.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30017]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30017]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30020]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/backup_bitget_db.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30020]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30020]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30023]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/scripts/bitget_journal_vacuum.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30023]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30023]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30026]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/reset_bitget_pipeline.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30026]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30026]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30029]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/master_sync_bitget.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30029]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30029]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30032]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/chmod +x /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_async.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_daemon.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_dashboard.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_heatmap.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_queue_worker.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_snapshot.sh /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/entrypoints/run_bitget_ws.sh
Sep 28 02:24:14 ip-172-26-7-213 sudo[30032]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 sudo[30032]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:14 ip-172-26-7-213 sudo[30035]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl daemon-reload
Sep 28 02:24:14 ip-172-26-7-213 sudo[30035]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:14 ip-172-26-7-213 systemd[1]: Reloading.
Sep 28 02:24:15 ip-172-26-7-213 systemd[30040]: /usr/lib/systemd/system-generators/systemd-fstab-generator failed with exit status 1.
Sep 28 02:24:16 ip-172-26-7-213 systemd[1]: Configuration file /run/systemd/system/netplan-ovs-cleanup.service is marked world-inaccessible. This has no effect as configuration data is accessible via APIs without restrictions. Proceeding anyway.
Sep 28 02:24:16 ip-172-26-7-213 systemd[1]: /lib/systemd/system/snapd.service:23: Unknown key name 'RestartMode' in section 'Service', ignoring.
Sep 28 02:24:16 ip-172-26-7-213 sudo[30035]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:17 ip-172-26-7-213 sudo[30065]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl enable dante-bitget-ws.service dante-bitget-factory.service dante-bitget-queue-worker.service dante-bitget-async.service dante-bitget-watchdog.timer dante-bitget-snapshot.timer
Sep 28 02:24:17 ip-172-26-7-213 sudo[30065]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:17 ip-172-26-7-213 systemd[1]: Reloading.
Sep 28 02:24:18 ip-172-26-7-213 systemd[30070]: /usr/lib/systemd/system-generators/systemd-fstab-generator failed with exit status 1.
Sep 28 02:24:18 ip-172-26-7-213 systemd[1]: Configuration file /run/systemd/system/netplan-ovs-cleanup.service is marked world-inaccessible. This has no effect as configuration data is accessible via APIs without restrictions. Proceeding anyway.
Sep 28 02:24:18 ip-172-26-7-213 systemd[1]: /lib/systemd/system/snapd.service:23: Unknown key name 'RestartMode' in section 'Service', ignoring.
Sep 28 02:24:19 ip-172-26-7-213 sudo[30065]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:19 ip-172-26-7-213 sudo[30095]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl disable dante-bitget-dashboard.service dante-bitget-heatmap.service
Sep 28 02:24:19 ip-172-26-7-213 sudo[30095]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:19 ip-172-26-7-213 systemd[1]: Reloading.
Sep 28 02:24:20 ip-172-26-7-213 systemd[30100]: /usr/lib/systemd/system-generators/systemd-fstab-generator failed with exit status 1.
Sep 28 02:24:21 ip-172-26-7-213 systemd[1]: Configuration file /run/systemd/system/netplan-ovs-cleanup.service is marked world-inaccessible. This has no effect as configuration data is accessible via APIs without restrictions. Proceeding anyway.
Sep 28 02:24:21 ip-172-26-7-213 systemd[1]: /lib/systemd/system/snapd.service:23: Unknown key name 'RestartMode' in section 'Service', ignoring.
Sep 28 02:24:22 ip-172-26-7-213 sudo[30095]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:22 ip-172-26-7-213 sudo[30125]:     root : TTY=pts/2 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl reset-failed dante-bitget-dashboard.service dante-bitget-heatmap.service
Sep 28 02:24:22 ip-172-26-7-213 sudo[30125]: pam_unix(sudo:session): session opened for user root(uid=0) by root(uid=0)
Sep 28 02:24:22 ip-172-26-7-213 sudo[30125]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:22 ip-172-26-7-213 sudo[29942]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:22 ip-172-26-7-213 sudo[30128]:     root : TTY=pts/1 ; PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; ENV=INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ; COMMAND=/usr/bin/bash /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/install_bitget_cron.sh
Sep 28 02:24:22 ip-172-26-7-213 sudo[30128]: pam_unix(sudo:session): session opened for user root(uid=0) by ubuntu(uid=0)
Sep 28 02:24:24 ip-172-26-7-213 sudo[30128]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopping Bitget quant factory daemon (auto pilot + satellites)...
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: dante-bitget-factory.service: Deactivated successfully.
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopped Bitget quant factory daemon (auto pilot + satellites).
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: dante-bitget-factory.service: Consumed 26.989s CPU time.
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopping Bitget task queue worker (single serial drain of bitget_task_queue.sqlite)...
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: dante-bitget-queue-worker.service: Deactivated successfully.
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopped Bitget task queue worker (single serial drain of bitget_task_queue.sqlite).
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: dante-bitget-queue-worker.service: Consumed 47.117s CPU time.
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopping Bitget WebSocket supervisor (public ticker + optional private streams)...
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: dante-bitget-ws.service: Deactivated successfully.
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopped Bitget WebSocket supervisor (public ticker + optional private streams).
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: dante-bitget-ws.service: Consumed 1h 24min 54.770s CPU time.
Sep 28 02:24:24 ip-172-26-7-213 systemd[1]: Stopping Bitget async Telegram queue consumer...
Sep 28 02:24:25 ip-172-26-7-213 systemd[1]: dante-bitget-async.service: Deactivated successfully.
Sep 28 02:24:25 ip-172-26-7-213 systemd[1]: Stopped Bitget async Telegram queue consumer.
Sep 28 02:24:25 ip-172-26-7-213 systemd[1]: dante-bitget-async.service: Consumed 3h 31min 30.981s CPU time.
Sep 28 02:24:29 ip-172-26-7-213 systemd[1]: Reloading.
Sep 28 02:24:30 ip-172-26-7-213 systemd[30190]: /usr/lib/systemd/system-generators/systemd-fstab-generator failed with exit status 1.
Sep 28 02:24:30 ip-172-26-7-213 systemd[1]: Configuration file /run/systemd/system/netplan-ovs-cleanup.service is marked world-inaccessible. This has no effect as configuration data is accessible via APIs without restrictions. Proceeding anyway.
Sep 28 02:24:30 ip-172-26-7-213 systemd[1]: /lib/systemd/system/snapd.service:23: Unknown key name 'RestartMode' in section 'Service', ignoring.
Sep 28 02:24:31 ip-172-26-7-213 systemd[1]: Starting Message of the Day...
Sep 28 02:24:31 ip-172-26-7-213 systemd[1]: Started Bitget WebSocket supervisor (public ticker + optional private streams).
Sep 28 02:24:33 ip-172-26-7-213 systemd[1]: Started Bitget async Telegram queue consumer.
Sep 28 02:24:33 ip-172-26-7-213 systemd[1]: Started Bitget quant factory daemon (auto pilot + satellites).
Sep 28 02:24:33 ip-172-26-7-213 systemd[1]: Started Bitget task queue worker (single serial drain of bitget_task_queue.sqlite).
Sep 28 02:24:33 ip-172-26-7-213 systemd[1]: Reloading.
Sep 28 02:24:35 ip-172-26-7-213 systemd[30271]: /usr/lib/systemd/system-generators/systemd-fstab-generator failed with exit status 1.
Sep 28 02:24:35 ip-172-26-7-213 systemd[1]: Configuration file /run/systemd/system/netplan-ovs-cleanup.service is marked world-inaccessible. This has no effect as configuration data is accessible via APIs without restrictions. Proceeding anyway.
Sep 28 02:24:35 ip-172-26-7-213 systemd[1]: /lib/systemd/system/snapd.service:23: Unknown key name 'RestartMode' in section 'Service', ignoring.
Sep 28 02:24:36 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.timer: Deactivated successfully.
Sep 28 02:24:36 ip-172-26-7-213 systemd[1]: Stopped Run Bitget watchdog every 5 minutes.
Sep 28 02:24:36 ip-172-26-7-213 systemd[1]: Stopping Run Bitget watchdog every 5 minutes...
Sep 28 02:24:36 ip-172-26-7-213 systemd[1]: Started Run Bitget watchdog every 5 minutes.
Sep 28 02:24:37 ip-172-26-7-213 systemd[1]: dante-bitget-snapshot.timer: Deactivated successfully.
Sep 28 02:24:37 ip-172-26-7-213 sudo[29911]: pam_unix(sudo:session): session closed for user root
Sep 28 02:24:37 ip-172-26-7-213 systemd[1]: Stopped Periodic Bitget CQRS snapshot (market_data -> snapshot).
Sep 28 02:24:37 ip-172-26-7-213 systemd[1]: Stopping Periodic Bitget CQRS snapshot (market_data -> snapshot)...
Sep 28 02:24:37 ip-172-26-7-213 systemd[1]: Started Periodic Bitget CQRS snapshot (market_data -> snapshot).
Sep 28 02:24:45 ip-172-26-7-213 systemd[1]: motd-news.service: Deactivated successfully.
Sep 28 02:24:45 ip-172-26-7-213 systemd[1]: Finished Message of the Day.
Sep 28 02:24:45 ip-172-26-7-213 systemd[1]: motd-news.service: Consumed 2.064s CPU time.
X2b_TOTAL_LINES=162
== X2c pre-update backups 2026-09-28 ==
total 196
drwxr-xr-x  2 ubuntu ubuntu 4096 2026-09-28 01:48:53.001748482 +0000 20260928_014852_utc
drwxr-xr-x  2 ubuntu ubuntu 4096 2026-09-28 01:54:11.321158336 +0000 20260928_015411_utc
drwxr-xr-x  2 ubuntu ubuntu 4096 2026-09-28 02:24:10.353932727 +0000 20260928_022409_utc
-- /var/backups/bitget-pre-update/20260928_014852_utc
total 20
drwxr-xr-x  2 ubuntu ubuntu  4096 Sep 28 01:48 .
drwxr-xr-x 49 root   root    4096 Sep 28 02:24 ..
-rw-r--r--  1 ubuntu ubuntu 12288 Sep 28 01:48 bitget_system_config.sqlite
-- /var/backups/bitget-pre-update/20260928_015411_utc
total 20
drwxr-xr-x  2 ubuntu ubuntu  4096 Sep 28 01:54 .
drwxr-xr-x 49 root   root    4096 Sep 28 02:24 ..
-rw-r--r--  1 ubuntu ubuntu 12288 Sep 28 01:54 bitget_system_config.sqlite
-- /var/backups/bitget-pre-update/20260928_022409_utc
total 20
drwxr-xr-x  2 ubuntu ubuntu  4096 Sep 28 02:24 .
drwxr-xr-x 49 root   root    4096 Sep 28 02:24 ..
-rw-r--r--  1 ubuntu ubuntu 12288 Sep 28 02:24 bitget_system_config.sqlite
== X3 bitget logs by mtime since 2026-09-29 00:00 UTC (lock occupancy input) ==
2026-09-29T00:00:21.0672839520 0 bitget_canary_20260927_001502.log
2026-09-29T00:00:21.0672839520 0 bitget_canary_20260927_003002.log
2026-09-29T00:00:21.0672839520 0 bitget_canary_20260927_004501.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_010002.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_011500.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_013002.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_014502.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_020002.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_021502.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_023001.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_024502.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_030001.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_031501.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_033001.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_034501.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_040001.log
2026-09-29T00:00:21.0682839560 0 bitget_canary_20260927_041501.log
2026-09-29T00:00:21.0692839600 0 bitget_canary_20260927_043002.log
2026-09-29T00:00:21.0692839600 0 bitget_canary_20260927_044502.log
2026-09-29T00:00:21.0692839600 0 bitget_canary_20260927_050001.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_051501.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_053002.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_054501.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_060001.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_061502.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_063002.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_064501.log
2026-09-29T00:00:21.0702839650 0 bitget_canary_20260927_070001.log
2026-09-29T00:00:21.0712839690 0 bitget_canary_20260927_071502.log
2026-09-29T00:00:21.0712839690 0 bitget_canary_20260927_073002.log
2026-09-29T00:00:21.0712839690 0 bitget_canary_20260927_074502.log
2026-09-29T00:00:21.0712839690 0 bitget_canary_20260927_080001.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_081501.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_083001.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_084501.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_090001.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_091501.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_093002.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_094502.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_100001.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_101501.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_103001.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_104502.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_110002.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_111501.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_113001.log
2026-09-29T00:00:21.0722839740 0 bitget_canary_20260927_114501.log
2026-09-29T00:00:21.0732839780 0 bitget_canary_20260927_120002.log
2026-09-29T00:00:21.0732839780 0 bitget_canary_20260927_121501.log
2026-09-29T00:00:21.0732839780 0 bitget_canary_20260927_123002.log
2026-09-29T00:00:21.0732839780 0 bitget_canary_20260927_124501.log
2026-09-29T00:00:21.0732839780 0 bitget_canary_20260927_130001.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_131501.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_133002.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_134501.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_140001.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_141502.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_143002.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_144502.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_150001.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_151501.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_153001.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_154502.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_160001.log
2026-09-29T00:00:21.0742839820 0 bitget_canary_20260927_161501.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_163002.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_164502.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_170001.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_171502.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_173001.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_174501.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_180002.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_181502.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_183001.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_184502.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_190002.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_191502.log
2026-09-29T00:00:21.0752839870 0 bitget_canary_20260927_193002.log
2026-09-29T00:00:21.1422842780 0 bitget_canary_20260927_194502.log
2026-09-29T00:00:21.1422842780 0 bitget_canary_20260927_200001.log
2026-09-29T00:00:21.1422842780 0 bitget_canary_20260927_201501.log
2026-09-29T00:00:21.1422842780 0 bitget_canary_20260927_203001.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_204502.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_210002.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_211502.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_213001.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_214501.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_220002.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_221501.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_223002.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_224501.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_230003.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_231501.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_233002.log
2026-09-29T00:00:21.1432842820 0 bitget_canary_20260927_234502.log
2026-09-29T00:00:21.1432842820 0 bitget_daily_audit_20260927_023001.log
2026-09-29T00:00:21.1432842820 0 bitget_data_refresh_20260927_004301.log
2026-09-29T00:00:21.1432842820 0 bitget_data_refresh_20260927_044301.log
2026-09-29T00:00:21.1442842870 0 bitget_data_refresh_20260927_084301.log
2026-09-29T00:00:21.1442842870 0 bitget_data_refresh_20260927_124301.log
2026-09-29T00:00:21.1442842870 0 bitget_data_refresh_20260927_164301.log
2026-09-29T00:00:21.1452842910 0 bitget_data_refresh_20260927_204302.log
2026-09-29T00:00:21.1452842910 0 bitget_db_backup_20260927_000501.log
2026-09-29T00:00:21.1452842910 0 bitget_health_20260927_001502.log
2026-09-29T00:00:21.1452842910 0 bitget_monthly_grand_20260927_235001.log
2026-09-29T00:00:21.1452842910 0 bitget_post_deploy_obs_20260927_110002.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_005302.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_015301.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_025301.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_035301.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_045301.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_055302.log
2026-09-29T00:00:21.1452842910 0 bitget_reconcile_20260927_065301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_075301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_085301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_095301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_105301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_115302.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_125301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_135301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_145301.log
2026-09-29T00:00:21.1462842950 0 bitget_reconcile_20260927_155301.log
2026-09-29T00:00:21.1472842990 0 bitget_reconcile_20260927_165301.log
2026-09-29T00:00:21.1472842990 0 bitget_reconcile_20260927_175301.log
2026-09-29T00:00:21.1472842990 0 bitget_reconcile_20260927_185301.log
2026-09-29T00:00:21.1472842990 0 bitget_reconcile_20260927_195301.log
2026-09-29T00:00:21.1472842990 0 bitget_reconcile_20260927_205301.log
2026-09-29T00:00:21.1482843030 0 bitget_reconcile_20260927_215302.log
2026-09-29T00:00:21.1482843030 0 bitget_reconcile_20260927_225302.log
2026-09-29T00:00:21.1482843030 0 bitget_reconcile_20260927_235301.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_dante_20260927_042701.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_dante_r2_20260927_132001.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_dante_r3_20260927_202701.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_ema5_20260927_061302.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_ema5_r2_20260927_150701.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_ema5_r3_20260927_221301.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_nulrim_20260927_024001.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_nulrim_r2_20260927_113301.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_nulrim_r3_20260927_184001.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_shadow_20260927_080101.log
2026-09-29T00:00:21.1482843030 0 bitget_scan_futures_supernova_20260927_005401.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_futures_supernova_r2_20260927_094701.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_futures_supernova_r3_20260927_165201.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_dante_20260927_033301.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_dante_r2_20260927_141302.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_dante_r3_20260927_212002.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_ema5_20260927_052002.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_ema5_r2_20260927_160101.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_ema5_r3_20260927_230701.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_master_20260927_070701.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_nulrim_20260927_014701.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_nulrim_r2_20260927_122701.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_nulrim_r3_20260927_193301.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_shadow_20260927_085202.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_supernova_20260927_000201.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_supernova_r2_20260927_104002.log
2026-09-29T00:00:21.1492843080 0 bitget_scan_spot_supernova_r3_20260927_174701.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_001502.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_003002.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_004501.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_010002.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_011500.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_013002.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_014502.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_020002.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_021502.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_023001.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_024502.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_030001.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_031501.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_033001.log
2026-09-29T00:00:21.1502843120 0 bitget_track_positions_20260927_034501.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_040001.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_041501.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_043002.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_044502.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_050001.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_051501.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_053002.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_054501.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_060001.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_061502.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_063002.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_064501.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_070001.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_071502.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_073001.log
2026-09-29T00:00:21.1512843160 0 bitget_track_positions_20260927_074502.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_080001.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_081501.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_083001.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_084501.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_090002.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_091501.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_093002.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_094502.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_100001.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_101501.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_103001.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_104502.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_110002.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_111501.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_113001.log
2026-09-29T00:00:21.1522843200 0 bitget_track_positions_20260927_114501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_120002.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_121501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_123001.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_124501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_130001.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_131501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_133002.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_134501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_140001.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_141502.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_143002.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_144502.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_150001.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_151501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_153002.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_154501.log
2026-09-29T00:00:21.1532843240 0 bitget_track_positions_20260927_160001.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_161501.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_163002.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_164502.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_170001.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_171501.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_173001.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_174501.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_180002.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_181502.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_183002.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_184501.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_190002.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_191502.log
2026-09-29T00:00:21.1542843290 0 bitget_track_positions_20260927_193002.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_194502.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_200001.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_201502.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_203001.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_204502.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_210002.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_211502.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_213001.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_214501.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_220002.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_221501.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_223002.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_224502.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_230002.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_231501.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_233002.log
2026-09-29T00:00:21.1552843330 0 bitget_track_positions_20260927_234502.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_000002.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_000501.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_001001.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_001502.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_002001.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_002501.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_003002.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_003501.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_004001.log
2026-09-29T00:00:21.1562843370 0 bitget_watchdog_20260927_004501.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_005001.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_005501.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_010002.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_010501.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_011001.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_011500.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_012001.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_012501.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_013002.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_013502.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_014002.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_014502.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_015501.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_020002.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_020501.log
2026-09-29T00:00:21.1572843410 0 bitget_watchdog_20260927_021001.log
2026-09-29T00:00:21.1582843450 0 bitget_watchdog_20260927_021502.log
2026-09-29T00:00:21.1582843450 0 bitget_watchdog_20260927_022002.log
2026-09-29T00:00:21.1582843450 0 bitget_watchdog_20260927_022501.log
2026-09-29T00:00:21.1582843450 0 bitget_watchdog_20260927_023001.log
2026-09-29T00:00:21.1582843450 0 bitget_watchdog_20260927_023501.log
2026-09-29T00:00:21.2222846150 0 bitget_watchdog_20260927_024001.log
2026-09-29T00:00:21.2222846150 0 bitget_watchdog_20260927_024502.log
2026-09-29T00:00:21.2222846150 0 bitget_watchdog_20260927_025002.log
2026-09-29T00:00:21.2222846150 0 bitget_watchdog_20260927_025501.log
2026-09-29T00:00:21.2222846150 0 bitget_watchdog_20260927_030001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_030501.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_031001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_031502.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_032002.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_032501.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_033001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_033501.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_034001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_034502.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_035001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_040001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_040502.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_041002.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_041501.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_042001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_042502.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_043002.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_043501.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_044001.log
2026-09-29T00:00:21.2232846190 0 bitget_watchdog_20260927_044501.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_045002.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_045501.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_050001.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_050502.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_051001.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_051501.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_052002.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_052501.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_053002.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_053501.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_054001.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_054501.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_055002.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_055502.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_060001.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_061001.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_061502.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_062002.log
2026-09-29T00:00:21.2242846230 0 bitget_watchdog_20260927_062501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_063002.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_063501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_064002.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_064501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_065001.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_065501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_070001.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_070501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_071001.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_071502.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_072002.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_072501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_073001.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_073501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_074002.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_074501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_075001.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_075501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_080001.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_080501.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_081002.log
2026-09-29T00:00:21.2252846280 0 bitget_watchdog_20260927_081501.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_082001.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_082501.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_083001.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_083501.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_085002.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_085502.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_090002.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_090018.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_090501.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_090547.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_091001.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_091058.log
2026-09-29T00:00:21.2262846320 0 bitget_watchdog_20260927_091501.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_091600.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_092001.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_092124.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_092501.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_092637.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_093001.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_093158.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_093501.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_093711.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_094001.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_094229.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_094502.log
2026-09-29T00:00:21.2272846360 0 bitget_watchdog_20260927_094757.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_095001.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_095301.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_095501.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_095817.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_100001.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_100338.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_100502.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_100847.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_101001.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_101358.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_101501.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_101904.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_102001.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_102427.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_102501.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_102947.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_103001.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_103457.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_103501.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_103957.log
2026-09-29T00:00:21.2282846410 0 bitget_watchdog_20260927_104002.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_104459.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_104502.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_105001.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_105002.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_105501.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_105503.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_110002.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_110013.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_110501.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_110515.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_111002.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_111021.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_111501.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_111547.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_112001.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_112051.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_112502.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_112557.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_113001.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_113058.log
2026-09-29T00:00:21.2292846450 0 bitget_watchdog_20260927_113502.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_113627.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_114001.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_114138.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_114501.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_114640.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_115002.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_115151.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_115502.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_115650.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_120002.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_120157.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_120501.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_120727.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_121001.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_121229.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_121501.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_121740.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_122001.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_122242.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_122502.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_122757.log
2026-09-29T00:00:21.2302846490 0 bitget_watchdog_20260927_123001.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_123301.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_123501.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_123827.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_124001.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_124358.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_124501.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_124911.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_125001.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_125437.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_125501.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_125946.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_130001.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_130457.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_130501.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_130958.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_131001.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_131459.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_131501.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_132001.log
2026-09-29T00:00:21.2312846540 0 bitget_watchdog_20260927_132501.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_133002.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_133502.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_133516.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_134002.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_134028.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_134501.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_134529.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_135001.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_135031.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_135501.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_135532.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_140001.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_140039.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_140501.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_140556.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_141001.log
2026-09-29T00:00:21.2322846580 0 bitget_watchdog_20260927_141057.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_141502.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_141558.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_142002.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_142127.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_142502.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_142651.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_143002.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_143157.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_143501.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_143705.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_144001.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_144213.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_144502.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_144729.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_145002.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_145242.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_145501.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_145749.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_150001.log
2026-09-29T00:00:21.2332846620 0 bitget_watchdog_20260927_150249.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_150501.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_150757.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_151001.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_151258.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_151501.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_151805.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_152001.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_152311.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_152501.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_152812.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_153002.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_153324.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_153501.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_153829.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_154002.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_154332.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_154501.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_154848.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_155002.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_155358.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_155502.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_155907.log
2026-09-29T00:00:21.2342846670 0 bitget_watchdog_20260927_160002.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_160437.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_160501.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_160958.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_161001.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_161459.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_161501.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_162001.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_162002.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_162501.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_162504.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_163002.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_163008.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_163501.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_163532.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_164001.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_164058.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_164502.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_164558.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_165002.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_165100.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_165501.log
2026-09-29T00:00:21.2352846710 0 bitget_watchdog_20260927_165602.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_170001.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_170103.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_170501.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_170614.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_171001.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_171116.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_171501.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_171617.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_172001.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_172119.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_172501.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_172627.log
2026-09-29T00:00:21.2362846750 0 bitget_watchdog_20260927_173001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_173132.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_173501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_173634.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_174001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_174145.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_174501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_174647.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_175001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_175157.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_175501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_175727.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_180002.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_180229.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_180501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_180729.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_181001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_181256.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_181501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_181757.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_182001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_182257.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_182501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_182807.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_183002.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_183312.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_183501.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_183813.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_184001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_184325.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_184502.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_184827.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_185001.log
2026-09-29T00:00:21.3202850330 0 bitget_watchdog_20260927_185328.log
2026-09-29T00:00:21.3822853030 0 bitget_watchdog_20260927_185501.log
2026-09-29T00:00:21.3822853030 0 bitget_watchdog_20260927_185840.log
2026-09-29T00:00:21.3822853030 0 bitget_watchdog_20260927_190002.log
2026-09-29T00:00:21.3822853030 0 bitget_watchdog_20260927_190341.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_190502.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_190847.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_191001.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_191357.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_191502.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_191923.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_192001.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_192439.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_192501.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_192940.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_193002.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_193452.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_193502.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_193958.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_194002.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_194459.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_194501.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_195001.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_195501.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_200001.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_200501.log
2026-09-29T00:00:21.3832853070 0 bitget_watchdog_20260927_200527.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_201001.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_201054.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_201501.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_201555.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_202001.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_202057.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_202501.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_202557.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_203001.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_203127.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_203501.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_203635.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_204002.log
2026-09-29T00:00:21.3842853110 0 bitget_watchdog_20260927_204157.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_204502.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_204716.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_205001.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_205216.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_205501.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_205719.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_210002.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_210224.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_210501.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_210729.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_211001.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_211257.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_211502.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_211806.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_212002.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_212307.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_212501.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_212809.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_213001.log
2026-09-29T00:00:21.3852853150 0 bitget_watchdog_20260927_213338.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_213501.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_213850.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_214001.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_214357.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_214501.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_214859.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_215001.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_215408.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_215502.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_215920.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_220002.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_220427.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_220501.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_220943.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_221001.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_221444.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_221501.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_221946.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_222002.log
2026-09-29T00:00:21.3862853200 0 bitget_watchdog_20260927_222458.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_222501.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_222958.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_223002.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_223459.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_223502.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_224001.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_224002.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_224502.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_224528.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_225001.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_225036.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_225501.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_225557.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_230002.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_230118.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_230501.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_230628.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_231002.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_231133.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_231501.log
2026-09-29T00:00:21.3872853240 0 bitget_watchdog_20260927_231635.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_232001.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_232146.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_232501.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_232647.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_233002.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_233148.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_233501.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_233650.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_234002.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_234156.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_234502.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_234657.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_235001.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_235200.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_235502.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260927_235727.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260928_000002.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260928_000229.log
2026-09-29T00:00:21.3882853280 0 bitget_watchdog_20260928_000742.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_001258.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_001758.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_002323.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_002838.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_003340.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_003848.log
2026-09-29T00:00:21.3892853320 0 bitget_watchdog_20260928_004351.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_004854.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_005354.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_005857.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_010419.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_010921.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_011447.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_011955.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_012456.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_012957.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_013459.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_014001.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_014501.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_015002.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_015518.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_020027.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_020544.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_021057.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_021612.log
2026-09-29T00:00:21.3902853370 0 bitget_watchdog_20260928_022124.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_022647.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_023147.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_023652.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_024157.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_024701.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_025225.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_025729.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_030229.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_030729.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_031239.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_031743.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_032250.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_032757.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_033257.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_033759.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_034301.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_034805.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_035309.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_035837.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_040338.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_040848.log
2026-09-29T00:00:21.3912853410 0 bitget_watchdog_20260928_041358.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_041907.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_042427.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_042932.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_043457.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_043959.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_044501.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_045002.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_045504.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_050015.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_050534.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_051039.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_051547.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_052052.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_052553.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_053058.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_053603.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_054108.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_054610.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_055120.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_055648.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_060154.log
2026-09-29T00:00:21.3922853450 0 bitget_watchdog_20260928_060657.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_061158.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_061700.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_062203.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_062727.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_063229.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_063729.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_064248.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_064758.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_065302.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_065803.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_070304.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_070827.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_071328.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_071840.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_072352.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_072857.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_073358.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_073928.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_074439.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_074940.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_075444.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_075957.log
2026-09-29T00:00:21.3932853490 0 bitget_watchdog_20260928_080459.log
X3_COUNT=3644
== X3b 2026-09-30 06:30-06:56 UTC window ==
2026-09-30T06:30:45.7095252140 883 bitget_canary_20260930_063002.log
2026-09-30T06:32:04.5048770710 171 bitget_watchdog_20260930_153157.log
2026-09-30T06:32:28.9039861420 298 bitget_track_positions_20260930_063001.log
2026-09-30T06:37:04.2791718060 171 bitget_watchdog_20260930_153658.log
2026-09-30T06:40:06.2739577080 171 bitget_watchdog_20260930_064001.log
2026-09-30T06:42:10.9165035010 171 bitget_watchdog_20260930_154205.log
2026-09-30T06:45:15.9503135190 171 bitget_watchdog_20260930_064501.log
2026-09-30T06:45:38.4274116340 891 bitget_canary_20260930_064501.log
2026-09-30T06:47:16.1938359810 171 bitget_watchdog_20260930_154709.log
2026-09-30T06:47:21.3028581580 298 bitget_track_positions_20260930_064501.log
2026-09-30T06:49:53.2395361120 0 bitget_cutover_check_20260930_154953.log
2026-09-30T06:50:06.8335964130 171 bitget_watchdog_20260930_065002.log
2026-09-30T06:52:13.7161357970 589 bitget_scan_spot_ema5_20260930_052001.log
2026-09-30T06:53:21.5524173700 281 bitget_watchdog_20260930_155209.log
2026-09-30T06:53:23.8784274570 248 bitget_reconcile_20260930_065301.log
2026-09-30T06:55:08.0319110960 171 bitget_watchdog_20260930_065501.log
-- bitget_watchdog_20260930_155209.log --
[2026-09-30 15:52:12] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5 pid=70238 age=5517 cap=5400 grace=60
[2026-09-30 15:53:21] [INFO] component='bitget_auto_pilot' watched='bitget_auto_pilot' age=38.8s (stale if >= 600s) db=/var/lib/quant-bitget/data/bitget_ops_events.sqlite
== X4 backup history / targets / sizes ==
BACKUP_FIRST_ENTRY: Sep 08 00:30:00 ip-172-26-7-213 systemd[1]: Starting Bitget SQLite integrity backup (L-2 P0-5)...
BACKUP_SUCCESS_COUNT=0
BACKUP_FAIL_COUNT=27
Sep 08 00:30:02 ip-172-26-7-213 systemd[1]: dante-bitget-backup.service: Failed with result 'exit-code'.
BACKUP_SCRIPT=bitget/deploy/backup_bitget_db.sh
27:if [[ "${BITGET_BACKUP_ENABLED:-true}" == "0" || "${BITGET_BACKUP_ENABLED:-true}" == "false" ]]; then
28:  echo "[backup_bitget_db] BITGET_BACKUP_ENABLED=false — skip"
32:python -m bitget.infra.integrity_backup_l2 --job backup "$@"
total 1904
drwxr-xr-x  4 root root   4096 2026-10-01 00:00:05.067401736 +0000 .
drwxr-xr-x 13 root root   4096 2026-04-10 06:10:08.109285357 +0000 ..
-rw-r--r--  1 root root  51200 2026-09-06 00:00:03.341823140 +0000 alternatives.tar.0
-rw-r--r--  1 root root   2676 2026-09-02 00:00:04.129326632 +0000 alternatives.tar.1.gz
-rw-r--r--  1 root root   2666 2026-08-28 00:00:03.514546900 +0000 alternatives.tar.2.gz
-rw-r--r--  1 root root   2663 2026-08-23 00:00:04.708965215 +0000 alternatives.tar.3.gz
-rw-r--r--  1 root root   2672 2026-07-24 00:00:04.205590718 +0000 alternatives.tar.4.gz
-rw-r--r--  1 root root   2668 2026-07-18 00:00:05.670107759 +0000 alternatives.tar.5.gz
-rw-r--r--  1 root root   2664 2026-07-17 00:00:05.664178070 +0000 alternatives.tar.6.gz
-rw-r--r--  1 root root  35008 2026-09-29 06:44:29.466127551 +0000 apt.extended_states.0
-rw-r--r--  1 root root   3796 2026-08-25 06:50:22.003625981 +0000 apt.extended_states.1.gz
-rw-r--r--  1 root root   3796 2026-08-21 06:44:21.918071241 +0000 apt.extended_states.2.gz
-rw-r--r--  1 root root   3792 2026-07-29 06:41:23.878242455 +0000 apt.extended_states.3.gz
-rw-r--r--  1 root root   3807 2026-07-04 06:54:33.075895363 +0000 apt.extended_states.4.gz
-rw-r--r--  1 root root   3773 2026-07-02 12:46:37.284989406 +0000 apt.extended_states.5.gz
drwxr-xr-x  2 root root   4096 2026-09-30 06:00:51.313686577 +0000 bitget-cron
drwxr-xr-x 49 root root   4096 2026-09-28 02:24:09.946930969 +0000 bitget-pre-update
-rw-r--r--  1 root root      0 2026-10-01 00:00:02.348390045 +0000 dpkg.arch.0
-rw-r--r--  1 root root     32 2026-09-30 00:00:00.531938688 +0000 dpkg.arch.1.gz
-rw-r--r--  1 root root     32 2026-09-28 00:00:02.300396946 +0000 dpkg.arch.2.gz
-rw-r--r--  1 root root     32 2026-09-26 00:00:01.925892979 +0000 dpkg.arch.3.gz
-rw-r--r--  1 root root     32 2026-09-25 00:00:02.323822465 +0000 dpkg.arch.4.gz
-rw-r--r--  1 root root     32 2026-09-24 00:00:01.928167498 +0000 dpkg.arch.5.gz
-rw-r--r--  1 root root     32 2026-09-19 00:00:01.496985236 +0000 dpkg.arch.6.gz
-rw-r--r--  1 root root    268 2026-04-10 06:16:13.520384657 +0000 dpkg.diversions.0
-rw-r--r--  1 root root    140 2026-04-10 06:16:13.520384657 +0000 dpkg.diversions.1.gz
-rw-r--r--  1 root root    140 2026-04-10 06:16:13.520384657 +0000 dpkg.diversions.2.gz
-rw-r--r--  1 root root    140 2026-04-10 06:16:13.520384657 +0000 dpkg.diversions.3.gz
-rw-r--r--  1 root root    140 2026-04-10 06:16:13.520384657 +0000 dpkg.diversions.4.gz
total 2173088
-rw-r--r-- 1 ubuntu ubuntu     12288 2026-10-01 02:30:33.712254540 +0000 bitget_alt_data.sqlite
-rw-r--r-- 1 ubuntu ubuntu    147456 2026-09-27 04:10:34.202149834 +0000 bitget_full_bt.sqlite
-rw-r--r-- 1 ubuntu ubuntu    114688 2026-08-30 13:37:21.496784341 +0000 bitget_fut_depth_staging.sqlite
-rw-r--r-- 1 ubuntu ubuntu     12288 2026-10-01 11:30:19.079690587 +0000 bitget_job_lifetime.sqlite
-rw-r--r-- 1 ubuntu ubuntu 530735104 2026-10-01 11:29:24.572456455 +0000 bitget_market_data.sqlite
-rw-r--r-- 1 ubuntu ubuntu 530735104 2026-10-01 11:28:20.832181986 +0000 bitget_market_data_snapshot.sqlite
-rw-r--r-- 1 ubuntu ubuntu  15851520 2026-10-01 11:30:17.386683311 +0000 bitget_message_queue.sqlite
-rw-r--r-- 1 ubuntu ubuntu 397193216 2026-10-01 11:30:25.793719371 +0000 bitget_ops_events.sqlite
-rw-r--r-- 1 ubuntu ubuntu     61440 2026-10-01 11:30:35.623761507 +0000 bitget_system_config.sqlite
-rw-r--r-- 1 ubuntu ubuntu     16384 2026-10-01 00:00:36.662537521 +0000 bitget_task_queue.sqlite
-rw-r--r-- 1 ubuntu ubuntu    294912 2026-08-23 12:27:30.068163238 +0000 bitget_universe_bt.sqlite
-rw-r--r-- 1 ubuntu ubuntu     53248 2026-08-23 12:26:58.318872429 +0000 bitget_universe_bt_scratch_forward.sqlite
-rw-r--r-- 1 ubuntu ubuntu     16384 2026-09-21 00:33:18.128558980 +0000 regime_task_queue.sqlite
26G	/var/lib/quant-bitget/data
Filesystem      Size  Used Avail Use% Mounted on
/dev/root        78G   36G   42G  46% /
               total        used        free      shared  buff/cache   available
Mem:            3836         864         401           2        2570        2664
Swap:           4095          75        4020
== X4b snapshot success gaps since 2026-09-24 UTC ==
SNAP_SUCCESS_SINCE_0924=519
SNAP_MAX_GAP_SEC=14563 GAP_START_UNIX=1790293708
== X-BLOCK END 2026-10-01T11:30:51Z ==

--STDERR--

SSH_RC=0
```
