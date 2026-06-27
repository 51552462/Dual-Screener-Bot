#!/bin/bash

cd /home/ubuntu/dante_bots/Dual-Screener-Bot

pkill -f bitget_main.py
pkill -f bitget_dashboard.py
pkill -f bitget_heatmap_dashboard.py
pkill -f bitget_factory_launcher.py

sleep 3
nohup python3 bitget_factory_launcher.py > bitget_factory_master.log 2>&1 &

echo "🚀 [Bitget 시스템 재부팅 완료] 팩토리가 성공적으로 가동되었습니다."
