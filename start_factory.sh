#!/bin/bash

# 1. 팩토리 작업 디렉토리로 이동 (경로는 현재 우분투 서버 기준)
cd /home/ubuntu/dante_bots/Dual-Screener-Bot

# 2. 혹시 엉켜있는 파이썬 프로세스가 있다면 전부 깔끔하게 종료
pkill -f main.py
pkill -f dashboard.py
pkill -f heatmap_dashboard.py
pkill -f factory_launcher.py
pkill -f forensics_pioneer.py

# 3. 3초 대기 후 마스터 런처 백그라운드 점화
sleep 3
nohup python3 factory_launcher.py > factory_master.log 2>&1 &

echo "🚀 [시스템 재부팅 완료] 팩토리가 성공적으로 부활했습니다."
