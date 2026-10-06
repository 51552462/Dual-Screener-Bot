# CAT-L-CUTOVER-01 Phase 1 선행 · Y-블록 출력 · 2026-10-06

Handoff 커밋 `bcdca77` §7-1 Y-블록(71줄, CR 0, sha256 `b64e3ac2edcd39b39d3c5149102287a7f0457071a0069a5538bcea9bd6ed62e2`)을 blob에서 바이트 그대로 추출해 Bot-2(`ubuntu@52.79.114.70`)에서 1회 실행(읽기전용). SSH_RC 0, START 2026-10-06T06:26:38Z END 06:27:23Z.

**가림:** 원문 journal에 Telegram `getUpdates` URL이 **실제 봇 토큰을 포함**해 커밋하지 않음. `/bot…/` 구간을 `[REDACTED_TELEGRAM_BOT_TOKEN]`으로 치환(해당 줄 12건). 디렉터: 해당 봇 토큰 **즉시 재발급**. 그 외는 원문.

```text
== Y-BLOCK START 2026-10-06T06:26:38Z user=ubuntu ==
== Y0 boot guard ==
-3 ac6b70aec93a479784a47ed93a81c829 Mon 2026-09-07 08:18:54 UTC—Sun 2026-09-13 14:56:48 UTC
-2 b54245277bd0487f82f7a923696bd87f Sun 2026-09-13 15:03:19 UTC—Tue 2026-09-22 05:45:35 UTC
-1 8f2b44c675f240869b6579e17903f944 Tue 2026-09-22 06:26:07 UTC—Sat 2026-09-26 08:50:08 UTC
 0 9a28ae32173940c0a4284e920c11e727 Sat 2026-09-26 08:52:17 UTC—Tue 2026-10-06 06:26:33 UTC
BOOT_GUARD=OK
== Y1 previous boots: kernel distress / hourly volume / pre-shutdown lines ==
---- boot -1 (window since 2026-09-24 20:00:00 UTC) ----
-1 8f2b44c675f240869b6579e17903f944 Tue 2026-09-22 06:26:07 UTC—Sat 2026-09-26 08:50:08 UTC
KLINES=221 OOM_HITS=3 HUNG_HITS=0
-- distress lines: first 3 (of total above) --
Sep 25 14:42:07 ip-172-26-7-213 kernel: snapd invoked oom-killer: gfp_mask=0x140cca(GFP_HIGHUSER_MOVABLE|__GFP_COMP), order=0, oom_score_adj=-900
Sep 25 14:42:07 ip-172-26-7-213 kernel: oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=snapd.service,mems_allowed=0,global_oom,task_memcg=/system.slice/cron.service,task=python,pid=44321,uid=1000
Sep 25 14:42:07 ip-172-26-7-213 kernel: Out of memory: Killed process 44321 (python) total-vm:2162580kB, anon-rss:80708kB, file-rss:2816kB, shmem-rss:0kB, UID:1000 pgtables:3908kB oom_score_adj:0
-- distress lines: last 3 --
Sep 25 14:42:07 ip-172-26-7-213 kernel: snapd invoked oom-killer: gfp_mask=0x140cca(GFP_HIGHUSER_MOVABLE|__GFP_COMP), order=0, oom_score_adj=-900
Sep 25 14:42:07 ip-172-26-7-213 kernel: oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=snapd.service,mems_allowed=0,global_oom,task_memcg=/system.slice/cron.service,task=python,pid=44321,uid=1000
Sep 25 14:42:07 ip-172-26-7-213 kernel: Out of memory: Killed process 44321 (python) total-vm:2162580kB, anon-rss:80708kB, file-rss:2816kB, shmem-rss:0kB, UID:1000 pgtables:3908kB oom_score_adj:0
-- hourly journal line counts (all sources, full window) --
    923 2026-09-24T20
    873 2026-09-24T21
    976 2026-09-24T22
    972 2026-09-24T23
    962 2026-09-25T00
    971 2026-09-25T01
    927 2026-09-25T02
    972 2026-09-25T03
    888 2026-09-25T04
    929 2026-09-25T05
    987 2026-09-25T06
    968 2026-09-25T07
    967 2026-09-25T08
    974 2026-09-25T09
    968 2026-09-25T10
    927 2026-09-25T11
    974 2026-09-25T12
    888 2026-09-25T13
    163 2026-09-25T14
      7              
    533 2026-09-25T14
    409 2026-09-25T15
    414 2026-09-25T16
    404 2026-09-25T17
    411 2026-09-25T18
    399 2026-09-25T19
    400 2026-09-25T20
    387 2026-09-25T21
    401 2026-09-25T22
    391 2026-09-25T23
    433 2026-09-26T00
    373 2026-09-26T01
    387 2026-09-26T02
    382 2026-09-26T03
    401 2026-09-26T04
    386 2026-09-26T05
    399 2026-09-26T06
    399 2026-09-26T07
    661 2026-09-26T08
-- 25 lines before first shutdown marker --
2026-09-26T08:45:10+0000 ip-172-26-7-213 CRON[88804]: (CRON) info (No MTA installed, discarding output)
2026-09-26T08:45:10+0000 ip-172-26-7-213 CRON[88804]: pam_unix(cron:session): session closed for user ubuntu
2026-09-26T08:45:11+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:45:11] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:45:29+0000 ip-172-26-7-213 CRON[88805]: (CRON) info (No MTA installed, discarding output)
2026-09-26T08:45:29+0000 ip-172-26-7-213 CRON[88805]: pam_unix(cron:session): session closed for user ubuntu
2026-09-26T08:45:41+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:45:41] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:45:44+0000 ip-172-26-7-213 CRON[88806]: (CRON) info (No MTA installed, discarding output)
2026-09-26T08:45:44+0000 ip-172-26-7-213 CRON[88806]: pam_unix(cron:session): session closed for user ubuntu
2026-09-26T08:45:59+0000 ip-172-26-7-213 bash[28310]: [2026-09-26 17:45:59] [WARNING] ws reconnect in 60.0s: Cannot connect to host ws.bitget.com:443 ssl:default [DNS server returned general failure]
2026-09-26T08:46:12+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:46:12] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:46:27+0000 ip-172-26-7-213 systemd[1]: Starting Bitget heartbeat watchdog (ops_events)...
2026-09-26T08:46:27+0000 ip-172-26-7-213 bash[88848]: [bitget.sh] mode=watchdog log=/var/lib/quant-bitget/logs/bitget_watchdog_20260926_174627.log TZ=Asia/Seoul wall_utc=2026-09-26 08:46:27 UTC
2026-09-26T08:46:31+0000 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Deactivated successfully.
2026-09-26T08:46:32+0000 ip-172-26-7-213 systemd[1]: Finished Bitget heartbeat watchdog (ops_events).
2026-09-26T08:46:32+0000 ip-172-26-7-213 systemd[1]: dante-bitget-watchdog.service: Consumed 3.992s CPU time.
2026-09-26T08:46:42+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:46:42] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:46:59+0000 ip-172-26-7-213 bash[28310]: [2026-09-26 17:46:59] [WARNING] ws reconnect in 60.0s: Cannot connect to host ws.bitget.com:443 ssl:default [DNS server returned general failure]
2026-09-26T08:47:12+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:47:12] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:47:42+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:47:42] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:47:59+0000 ip-172-26-7-213 bash[28310]: [2026-09-26 17:47:59] [WARNING] ws reconnect in 60.0s: Cannot connect to host ws.bitget.com:443 ssl:default [DNS server returned general failure]
2026-09-26T08:48:12+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:48:12] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:48:42+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:48:42] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:48:59+0000 ip-172-26-7-213 bash[28310]: [2026-09-26 17:48:59] [WARNING] ws reconnect in 60.0s: Cannot connect to host ws.bitget.com:443 ssl:default [DNS server returned general failure]
2026-09-26T08:49:12+0000 ip-172-26-7-213 python[405]: [2026-09-26 17:49:12] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-26T08:49:27+0000 ip-172-26-7-213 systemd[1]: Starting Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)...
2026-09-26T08:49:39+0000 ip-172-26-7-213 systemd-logind[430]: Power key pressed.
-- last 5 lines of boot --
2026-09-26T08:50:08+0000 ip-172-26-7-213 systemd[1]: Reached target System Power Off.
2026-09-26T08:50:08+0000 ip-172-26-7-213 systemd[1]: Shutting down.
2026-09-26T08:50:08+0000 ip-172-26-7-213 systemd-shutdown[1]: Syncing filesystems and block devices.
2026-09-26T08:50:08+0000 ip-172-26-7-213 systemd-shutdown[1]: Sending SIGTERM to remaining processes...
2026-09-26T08:50:08+0000 ip-172-26-7-213 systemd-journald[78367]: Journal stopped
---- boot -2 (window since 2026-09-21 00:00:00 UTC) ----
-2 b54245277bd0487f82f7a923696bd87f Sun 2026-09-13 15:03:19 UTC—Tue 2026-09-22 05:45:35 UTC
KLINES=1 OOM_HITS=0 HUNG_HITS=0
-- distress lines: first 3 (of total above) --
-- distress lines: last 3 --
-- hourly journal line counts (all sources, full window) --
    207 2026-09-21T00
    198 2026-09-21T01
    209 2026-09-21T02
    203 2026-09-21T03
    204 2026-09-21T04
    206 2026-09-21T05
    181 2026-09-21T06
    202 2026-09-21T07
    203 2026-09-21T08
    200 2026-09-21T09
    202 2026-09-21T10
    204 2026-09-21T11
    202 2026-09-21T12
    204 2026-09-21T13
    251 2026-09-21T14
    209 2026-09-21T15
    202 2026-09-21T16
    201 2026-09-21T17
    204 2026-09-21T18
    202 2026-09-21T19
    200 2026-09-21T20
    204 2026-09-21T21
    202 2026-09-21T22
    202 2026-09-21T23
    207 2026-09-22T00
    202 2026-09-22T01
    200 2026-09-22T02
    204 2026-09-22T03
    202 2026-09-22T04
    155 2026-09-22T05
-- 25 lines before first shutdown marker --
NO_SHUTDOWN_MARKER
-- last 5 lines of boot --
2026-09-22T05:44:27+0000 ip-172-26-7-213 bash[1919]: [2026-09-22 14:44:27] [WARNING] ws reconnect in 60.0s: Cannot connect to host ws.bitget.com:443 ssl:default [DNS server returned general failure]
2026-09-22T05:44:35+0000 ip-172-26-7-213 python[406]: [2026-09-22 14:44:35] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-22T05:45:05+0000 ip-172-26-7-213 python[406]: [2026-09-22 14:45:05] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
2026-09-22T05:45:28+0000 ip-172-26-7-213 bash[1919]: [2026-09-22 14:45:28] [WARNING] ws reconnect in 60.0s: Cannot connect to host ws.bitget.com:443 ssl:default [DNS server returned general failure]
2026-09-22T05:45:35+0000 ip-172-26-7-213 python[406]: [2026-09-22 14:45:35] [WARNING] proposal poll getUpdates failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded with url: /bot[REDACTED_TELEGRAM_BOT_TOKEN]/getUpdates?timeout=0 (Caused by NameResolutionError("HTTPSConnection(host='api.telegram.org', port=443): Failed to resolve 'api.telegram.org' ([Errno -3] Temporary failure in name resolution)"))
---- boot -3 (window since 2026-09-12 09:00:00 UTC) ----
-3 ac6b70aec93a479784a47ed93a81c829 Mon 2026-09-07 08:18:54 UTC—Sun 2026-09-13 14:56:48 UTC
KLINES=1 OOM_HITS=0 HUNG_HITS=0
-- distress lines: first 3 (of total above) --
-- distress lines: last 3 --
-- hourly journal line counts (all sources, full window) --
    204 2026-09-12T09
    202 2026-09-12T10
    202 2026-09-12T11
    204 2026-09-12T12
    202 2026-09-12T13
    255 2026-09-12T14
    202 2026-09-12T15
    204 2026-09-12T16
    202 2026-09-12T17
    204 2026-09-12T18
    201 2026-09-12T19
    204 2026-09-12T20
    202 2026-09-12T21
    204 2026-09-12T22
    201 2026-09-12T23
    206 2026-09-13T00
    202 2026-09-13T01
    202 2026-09-13T02
    204 2026-09-13T03
    202 2026-09-13T04
    203 2026-09-13T05
    202 2026-09-13T06
    204 2026-09-13T07
    202 2026-09-13T08
    202 2026-09-13T09
    204 2026-09-13T10
    202 2026-09-13T11
    204 2026-09-13T12
    202 2026-09-13T13
    259 2026-09-13T14
-- 25 lines before first shutdown marker --
NO_SHUTDOWN_MARKER
-- last 5 lines of boot --
2026-09-13T14:56:48+0000 ip-172-26-7-213 systemd[778]: Closed REST API socket for snapd user session agent.
2026-09-13T14:56:48+0000 ip-172-26-7-213 systemd[778]: Removed slice User Application Slice.
2026-09-13T14:56:48+0000 ip-172-26-7-213 systemd[778]: Reached target Shutdown.
2026-09-13T14:56:48+0000 ip-172-26-7-213 systemd[778]: Finished Exit the Session.
2026-09-13T14:56:48+0000 ip-172-26-7-213 systemd[778]: Reached target Exit the Session.
== Y2 transient scope durations since 2026-09-29 10:30 UTC (start_utc dur_sec description) ==
2026-09-29T10:40:01Z 5681 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-09-29T11:33:02Z 5560 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-09-29T12:27:01Z 5516 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-09-29T13:20:01Z 5681 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-09-29T14:13:01Z 5564 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-09-29T16:01:01Z 5608 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-09-29T16:52:01Z 5637 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-09-29T17:47:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-09-29T18:40:01Z 5646 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-09-29T19:33:01Z 5524 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-09-29T20:27:01Z 5645 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-09-29T21:20:01Z 5610 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-09-29T22:13:01Z 5530 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-09-29T23:07:01Z 5586 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-09-30T00:02:01Z 5553 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-09-30T00:54:02Z 5522 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-09-30T02:30:02Z 171 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-09-30T01:47:01Z 5489 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-09-30T02:40:01Z 5661 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-09-30T03:33:01Z 5528 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-09-30T04:27:01Z 5645 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
2026-09-30T05:20:01Z 5592 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
2026-09-30T06:13:01Z 5521 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
2026-09-30T08:01:01Z 22 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
2026-09-30T07:07:01Z 5637 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
2026-09-30T08:52:01Z 1446 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
2026-09-30T09:47:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-09-30T10:40:01Z 5628 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-09-30T11:33:01Z 5526 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-09-30T12:27:01Z 5596 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-09-30T13:20:01Z 5592 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-09-30T14:13:01Z 5522 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-09-30T16:01:01Z 5514 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-09-30T16:52:01Z 5551 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-09-30T17:47:01Z 5639 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-09-30T18:40:01Z 5579 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-09-30T19:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-09-30T20:27:01Z 5595 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-09-30T21:20:01Z 5570 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-09-30T22:13:01Z 5478 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-09-30T23:07:02Z 5586 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-10-01T00:02:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-10-01T00:54:01Z 5522 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-10-01T02:30:02Z 169 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-10-01T01:47:02Z 5584 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-10-01T02:40:01Z 5500 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-10-01T03:33:01Z 5581 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-10-01T04:27:01Z 5598 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
2026-10-01T05:20:02Z 5706 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
2026-10-01T06:13:01Z 5585 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
2026-10-01T08:01:01Z 24 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
2026-10-01T07:07:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
2026-10-01T08:52:01Z 1401 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
2026-10-01T09:47:01Z 5521 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-01T10:40:02Z 5763 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-10-01T11:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-10-01T12:27:01Z 5546 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-10-01T13:20:01Z 5710 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-10-01T14:13:01Z 5586 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-10-01T16:01:01Z 5646 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-10-01T16:52:01Z 5641 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-01T17:47:01Z 5519 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-10-01T18:40:01Z 5711 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-10-01T19:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-10-01T20:27:01Z 5548 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-10-01T21:20:01Z 5764 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-10-01T22:13:01Z 5583 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-10-01T23:07:01Z 5518 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-10-02T00:02:02Z 5589 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-10-02T00:54:01Z 5522 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-10-02T02:30:01Z 171 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-10-02T01:47:01Z 5503 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-10-02T02:40:01Z 5709 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-10-02T03:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-10-02T04:27:01Z 5525 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
2026-10-02T05:20:01Z 5706 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
2026-10-02T06:13:01Z 5532 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
2026-10-02T08:01:01Z 18 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
2026-10-02T07:07:01Z 5641 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
2026-10-02T08:52:02Z 1444 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
2026-10-02T09:47:01Z 5636 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-02T10:40:01Z 5579 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-10-02T11:33:01Z 5525 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-10-02T12:27:01Z 5591 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-10-02T13:20:01Z 5538 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-10-02T14:13:01Z 5588 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-10-02T16:01:01Z 5645 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-10-02T16:52:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-02T17:47:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-10-02T18:40:01Z 5616 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-10-02T19:33:01Z 5499 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-10-02T20:27:01Z 5606 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-10-02T21:20:01Z 5593 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-10-02T22:13:01Z 5491 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-10-02T23:07:01Z 5644 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-10-03T00:02:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-10-03T00:54:01Z 5521 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-10-03T02:30:01Z 188 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-10-03T01:47:01Z 5537 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-10-03T02:40:02Z 5510 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-10-03T03:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-10-03T04:27:01Z 5519 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
2026-10-03T05:20:01Z 5707 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
2026-10-03T06:13:01Z 5531 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
2026-10-03T08:01:01Z 20 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
2026-10-03T07:07:01Z 5641 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
2026-10-03T08:52:02Z 1444 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
2026-10-03T09:47:01Z 5641 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-03T10:40:01Z 5602 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-10-03T11:33:01Z 5503 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-10-03T12:27:01Z 5589 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-10-03T13:20:01Z 5728 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-10-03T14:13:01Z 5584 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-10-03T16:01:02Z 5644 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-10-03T16:52:01Z 5604 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-03T17:47:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-10-03T18:40:01Z 5662 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-10-03T19:33:01Z 5527 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-10-03T20:27:01Z 5635 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-10-03T21:20:02Z 5565 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-10-03T22:13:02Z 5584 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-10-03T23:07:01Z 5585 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-10-04T00:02:01Z 5643 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-10-04T00:54:01Z 5521 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-10-04T02:30:02Z 163 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-10-04T01:47:01Z 5482 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-10-04T02:40:01Z 5671 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-10-04T03:33:01Z 5530 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-10-04T04:27:01Z 5645 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
2026-10-04T05:20:01Z 5624 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
2026-10-04T06:13:01Z 5496 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
2026-10-04T08:01:01Z 17 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
2026-10-04T07:07:01Z 5586 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
2026-10-04T08:52:02Z 1416 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
2026-10-04T09:47:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-04T10:40:02Z 5730 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-10-04T11:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-10-04T12:27:01Z 5509 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-10-04T13:20:01Z 5712 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-10-04T14:13:01Z 5544 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-10-04T16:01:01Z 5561 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-10-04T16:52:01Z 5578 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-04T17:47:01Z 5634 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-10-04T18:40:01Z 5710 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-10-04T19:33:02Z 5581 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-10-04T20:27:01Z 5503 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-10-04T21:20:02Z 5664 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-10-04T22:13:01Z 5529 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-10-05T00:30:02Z 144 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --weekly-evolution
2026-10-04T23:07:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-10-05T00:02:01Z 5480 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-10-05T00:54:01Z 5522 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-10-05T02:30:01Z 172 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-10-05T01:47:01Z 5645 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-10-05T02:40:01Z 5536 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-10-05T03:33:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-10-05T04:27:02Z 5574 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
2026-10-05T05:20:01Z 5708 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
2026-10-05T06:13:02Z 5574 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
2026-10-05T08:01:01Z 23 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
2026-10-05T07:07:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
2026-10-05T08:52:01Z 1446 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
2026-10-05T09:47:01Z 5641 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-05T10:40:01Z 5534 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
2026-10-05T11:33:01Z 5583 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
2026-10-05T12:27:01Z 5541 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
2026-10-05T13:20:01Z 5711 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
2026-10-05T14:13:01Z 5585 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
2026-10-05T16:01:01Z 5623 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
2026-10-05T16:52:01Z 5608 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r
2026-10-05T17:47:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
2026-10-05T18:40:02Z 5589 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
2026-10-05T19:33:01Z 5486 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
2026-10-05T20:27:01Z 5598 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
2026-10-05T21:20:02Z 5495 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
2026-10-05T22:13:01Z 5584 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3
2026-10-05T23:07:01Z 5539 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3
2026-10-06T00:02:01Z 5582 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
2026-10-06T00:54:02Z 5510 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
2026-10-06T02:30:02Z 165 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
2026-10-06T01:47:01Z 5642 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
2026-10-06T02:40:01Z 5637 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
2026-10-06T03:33:02Z 5526 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
2026-10-06T04:27:01Z 5600 /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
OPEN 2026-10-06T05:20:01Z /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
OPEN 2026-10-06T06:13:01Z /bin/bash -c cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
Y2_PAIRS=184 Y2_OPEN=2
== Y2b bitget logs named 20260929..20261002, size>0, excluding watchdog (name size mtime_utc) ==
Y2B_COUNT=0

== Y3 watchdog LIFECAP enforcement (all retained watchdog logs) ==
LIFECAP_COUNT=48
      4 mode=data_refresh
      3 mode=scan_futures_dante
      2 mode=scan_futures_dante_r2
      2 mode=scan_futures_dante_r3
      2 mode=scan_futures_ema5
      1 mode=scan_futures_ema5_r3
      2 mode=scan_futures_nulrim
      1 mode=scan_futures_nulrim_r2
      1 mode=scan_futures_nulrim_r3
      3 mode=scan_futures_supernova
      2 mode=scan_futures_supernova_r2
      2 mode=scan_futures_supernova_r3
      3 mode=scan_spot_dante
      2 mode=scan_spot_dante_r2
      1 mode=scan_spot_dante_r3
      2 mode=scan_spot_ema5
      1 mode=scan_spot_ema5_r2
      3 mode=scan_spot_ema5_r3
      1 mode=scan_spot_master
      3 mode=scan_spot_nulrim
      1 mode=scan_spot_nulrim_r2
      1 mode=scan_spot_nulrim_r3
      3 mode=scan_spot_supernova
      1 mode=scan_spot_supernova_r2
      1 mode=scan_spot_supernova_r3
[2026-10-05 00:40:02] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5_r3 pid=147760 age=5573 cap=5400 grace=60
[2026-10-05 01:15:04] [WARNING] LIFECAP ENFORCE kill mode=data_refresh pid=149149 age=1915 cap=1800 grace=60
[2026-10-05 02:25:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova pid=149271 age=5450 cap=5400 grace=60
[2026-10-05 03:20:05] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_nulrim pid=149732 age=5577 cap=5400 grace=60
[2026-10-05 05:05:02] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_dante pid=151158 age=5513 cap=5400 grace=60
[2026-10-05 06:55:04] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5 pid=152287 age=5693 cap=5400 grace=60
[2026-10-05 07:45:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_ema5 pid=152790 age=5512 cap=5400 grace=60
[2026-10-05 08:40:02] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_master pid=153425 age=5573 cap=5400 grace=60
[2026-10-05 09:15:04] [WARNING] LIFECAP ENFORCE kill mode=data_refresh pid=154344 age=1921 cap=1800 grace=60
[2026-10-05 09:41:00] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5_r3 pid=147760 age=5631 cap=5400 grace=60
[2026-10-05 10:32:21] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_supernova pid=148755 age=5411 cap=5400 grace=60
[2026-10-05 11:20:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova_r2 pid=154923 age=5580 cap=5400 grace=60
[2026-10-05 12:20:05] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_nulrim pid=149732 age=5577 cap=5400 grace=60
[2026-10-05 13:05:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_nulrim_r2 pid=155867 age=5513 cap=5400 grace=60
[2026-10-05 13:11:16] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_nulrim pid=150639 age=5464 cap=5400 grace=60
[2026-10-05 14:55:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante_r2 pid=156816 age=5689 cap=5400 grace=60
[2026-10-05 14:58:55] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante pid=151696 age=5505 cap=5400 grace=60
[2026-10-05 15:45:06] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_dante_r2 pid=157304 age=5520 cap=5400 grace=60
[2026-10-05 15:55:05] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5 pid=152287 age=5694 cap=5400 grace=60
[2026-10-05 16:45:49] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_ema5 pid=152790 age=5558 cap=5400 grace=60
[2026-10-05 18:25:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova_r3 pid=159297 age=5574 cap=5400 grace=60
[2026-10-05 19:20:02] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_supernova_r3 pid=159793 age=5573 cap=5400 grace=60
[2026-10-05 20:20:02] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova_r2 pid=154923 age=5579 cap=5400 grace=60
[2026-10-05 21:11:14] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_supernova_r2 pid=155366 age=5463 cap=5400 grace=60
[2026-10-05 22:00:05] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante_r3 pid=161177 age=5576 cap=5400 grace=60
[2026-10-05 22:58:21] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_nulrim_r2 pid=156329 age=5473 cap=5400 grace=60
[2026-10-05 23:45:04] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_ema5_r3 pid=162325 age=5518 cap=5400 grace=60
[2026-10-05 23:54:29] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante_r2 pid=156816 age=5655 cap=5400 grace=60
[2026-10-06 00:45:59] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_dante_r2 pid=157304 age=5574 cap=5400 grace=60
[2026-10-06 01:15:04] [WARNING] LIFECAP ENFORCE kill mode=data_refresh pid=163780 age=1914 cap=1800 grace=60
[2026-10-06 01:35:01] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_supernova pid=163409 age=5571 cap=5400 grace=60
[2026-10-06 02:25:03] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova pid=163916 age=5447 cap=5400 grace=60
[2026-10-06 02:33:44] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5_r2 pid=158777 age=5560 cap=5400 grace=60
[2026-10-06 03:20:03] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_nulrim pid=164371 age=5574 cap=5400 grace=60
[2026-10-06 03:25:27] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova_r3 pid=159297 age=5599 cap=5400 grace=60
[2026-10-06 05:05:02] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_dante pid=165338 age=5512 cap=5400 grace=60
[2026-10-06 05:12:11] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_nulrim_r3 pid=160233 age=5517 cap=5400 grace=60
[2026-10-06 06:00:07] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante pid=165841 age=5577 cap=5400 grace=60
[2026-10-06 06:03:26] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_nulrim_r3 pid=160731 age=5417 cap=5400 grace=60
[2026-10-06 06:59:45] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante_r3 pid=161177 age=5556 cap=5400 grace=60
[2026-10-06 07:50:37] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_dante_r3 pid=161752 age=5424 cap=5400 grace=60
[2026-10-06 09:38:19] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5_r3 pid=162872 age=5469 cap=5400 grace=60
[2026-10-06 10:14:07] [WARNING] LIFECAP ENFORCE kill mode=data_refresh pid=163780 age=1858 cap=1800 grace=60
[2026-10-06 10:34:47] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_supernova pid=163409 age=5557 cap=5400 grace=60
[2026-10-06 11:25:44] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_supernova pid=163916 age=5488 cap=5400 grace=60
[2026-10-06 13:12:57] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_nulrim pid=164838 age=5566 cap=5400 grace=60
[2026-10-06 14:04:08] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_dante pid=165338 age=5457 cap=5400 grace=60
[2026-10-06 15:00:04] [WARNING] LIFECAP ENFORCE kill mode=scan_futures_dante pid=165841 age=5574 cap=5400 grace=60
WATCHDOG_LOGS_NONEMPTY=716
-- canary 20260930_063002 / 064501 --
== Y4 BITGET_DB_STORAGE_PATH key presence (key counts only, no values) ==
.env KEY_LINES=1
bitget/.env KEY_LINES=1
EnvironmentFiles=/home/ubuntu/dante_bots/Dual-Screener-Bot/.env (ignore_errors=yes)
EnvironmentFiles=/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/.env (ignore_errors=yes)
User=ubuntu
Id=dante-bitget-factory.service

EnvironmentFiles=/home/ubuntu/dante_bots/Dual-Screener-Bot/.env (ignore_errors=yes)
EnvironmentFiles=/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/.env (ignore_errors=yes)
User=root
Id=dante-bitget-backup.service
/home/ubuntu/dante_bots/Dual-Screener-Bot/.env KEY_LINES=1
/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/.env KEY_LINES=1
== Y5 data dir composition ==
DATA_ENTRIES=50
-- largest 20 entries by size (ascending) --
16K	/var/lib/quant-bitget/data/regime_task_queue.sqlite
24K	/var/lib/quant-bitget/data/bitget_task_queue.sqlite
32K	/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite-shm
32K	/var/lib/quant-bitget/data/bitget_market_data_snapshot.sqlite-shm
32K	/var/lib/quant-bitget/data/bitget_universe_bt_scratch_forward.sqlite-shm
44K	/var/lib/quant-bitget/data/Supernova_Flow_Tracking_Master.csv
52K	/var/lib/quant-bitget/data/bitget_universe_bt_scratch_forward.sqlite
52K	/var/lib/quant-bitget/data/watchdog_state
68K	/var/lib/quant-bitget/data/bitget_system_config.sqlite
112K	/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
144K	/var/lib/quant-bitget/data/bitget_full_bt.sqlite
292K	/var/lib/quant-bitget/data/bitget_universe_bt.sqlite
17M	/var/lib/quant-bitget/data/bitget_message_queue.sqlite
250M	/var/lib/quant-bitget/data/bitget_market_data_snapshot.sqlite.tmp.57637
379M	/var/lib/quant-bitget/data/bitget_ops_events.sqlite
451M	/var/lib/quant-bitget/data/bitget_market_data_snapshot.sqlite.tmp.88861
517M	/var/lib/quant-bitget/data/bitget_market_data.sqlite
517M	/var/lib/quant-bitget/data/bitget_market_data_snapshot.sqlite
1.2G	/var/lib/quant-bitget/data/backups
24G	/var/lib/quant-bitget/data/charts
-- sqlite main/wal/shm files --
-rw-r--r-- 1 ubuntu ubuntu     12288 2026-10-06 02:30:31.970073766 +0000 bitget_alt_data.sqlite
-rw-r--r-- 1 ubuntu ubuntu    147456 2026-09-27 04:10:34.202149834 +0000 bitget_full_bt.sqlite
-rw-r--r-- 1 ubuntu ubuntu    114688 2026-08-30 13:37:21.496784341 +0000 bitget_fut_depth_staging.sqlite
-rw-r--r-- 1 ubuntu ubuntu     32768 2026-09-27 04:10:36.749160540 +0000 bitget_fut_depth_staging.sqlite-shm
-rw-r--r-- 1 ubuntu ubuntu         0 2026-09-27 04:09:55.791986534 +0000 bitget_fut_depth_staging.sqlite-wal
-rw-r--r-- 1 ubuntu ubuntu     12288 2026-10-06 06:17:23.110362796 +0000 bitget_job_lifetime.sqlite
-rw-r--r-- 1 ubuntu ubuntu 541679616 2026-10-06 06:13:13.253290855 +0000 bitget_market_data.sqlite
-rw-r--r-- 1 ubuntu ubuntu 541679616 2026-10-06 06:10:37.263627649 +0000 bitget_market_data_snapshot.sqlite
-rw-r--r-- 1 ubuntu ubuntu     32768 2026-10-06 06:13:18.704314147 +0000 bitget_market_data_snapshot.sqlite-shm
-rw-r--r-- 1 ubuntu ubuntu         0 2026-08-23 12:11:57.197630924 +0000 bitget_market_data_snapshot.sqlite-wal
-rw-r--r-- 1 ubuntu ubuntu  17051648 2026-10-06 06:27:10.376893342 +0000 bitget_message_queue.sqlite
-rw-r--r-- 1 ubuntu ubuntu 397193216 2026-10-06 06:27:12.061900605 +0000 bitget_ops_events.sqlite
-rw-r--r-- 1 ubuntu ubuntu     65536 2026-10-06 06:13:13.192290598 +0000 bitget_system_config.sqlite
-rw-r--r-- 1 ubuntu ubuntu     24576 2026-10-06 00:00:32.372529427 +0000 bitget_task_queue.sqlite
-rw-r--r-- 1 ubuntu ubuntu    294912 2026-08-23 12:27:30.068163238 +0000 bitget_universe_bt.sqlite
-rw-r--r-- 1 ubuntu ubuntu     53248 2026-08-23 12:26:58.318872429 +0000 bitget_universe_bt_scratch_forward.sqlite
-rw-r--r-- 1 ubuntu ubuntu     32768 2026-09-13 00:05:16.231893600 +0000 bitget_universe_bt_scratch_forward.sqlite-shm
-rw-r--r-- 1 ubuntu ubuntu         0 2026-09-10 00:05:16.166822506 +0000 bitget_universe_bt_scratch_forward.sqlite-wal
-rw-r--r-- 1 ubuntu ubuntu     16384 2026-09-21 00:33:18.128558980 +0000 regime_task_queue.sqlite
== Y-BLOCK END 2026-10-06T06:27:23Z ==

--STDERR--

SSH_RC=0
```
