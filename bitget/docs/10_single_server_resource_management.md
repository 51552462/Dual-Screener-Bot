# 10 — 단일 서버 리소스 관리 (주식 + 코인 공존)

> **작성일:** 2026-06-14  
> **전제:** Ubuntu 서버 **1대** — 한국/미국 주식 팩토리(24/7) + Bitget 코인 팩토리 **동시 운영**  
> **목표:** 코인 봇이 RAM을 과점해도 **주식 봇이 OOM으로 같이 죽지 않도록** 리소스 상한을 건다.  
> **성격:** 운영·설계 가이드 (코드 변경 없음)

**관련:** [09_ubuntu_deployment_and_update_guide.md](./09_ubuntu_deployment_and_update_guide.md) · [RUNBOOK.md](../RUNBOOK.md)

---

## 0. Executive Summary

| 오해 | 사실 |
|------|------|
| tmux 세션을 나누면 RAM/CPU가 분리된다 | **아니다.** tmux는 터미널 창만 나눌 뿐, **같은 Linux 커널·같은 RAM** 을 쓴다 |
| tmux `coin_bot` 창을 켜 두어야 봇이 돈다 | **아니다.** systemd 데몬은 SSH·tmux와 **무관하게** 백그라운드에서 돈다 |
| 코인을 tmux에서 수동 실행해야 한다 | 리소스 보호에는 **systemd cgroups**(`MemoryMax`/`MemoryHigh`)가 tmux보다 훨씬 낫다 |

**권장 아키텍처**

```
주식 dante-factory (systemd, 우선순위 높음·메모리 여유 확보)
        +
코인 dante-bitget-* (systemd + MemoryMax/MemoryHigh 로 상한)
        +
Swap 여유 (피크 시간대 버퍼)
```

코인 프로세스가 한도를 넘으면 **코인만** OOM-kill 또는 스로틀되고, 주식은 cgroup 밖(또는 별도 상한)에서 계속 동작할 수 있다.

---

## 1. tmux의 물리적 한계 (왜 “방”이 아닌가)

### 1.1 tmux가 하는 일

tmux는 **터미널 멀티플렉서**다.

- 화면을 여러 “창/세션”으로 나눠 보여 준다
- SSH가 끊겨도 **그 터미널 안에서 돌던 프로세스**가 살아 있을 수 있다

tmux가 **하지 않는** 일:

- RAM 할당량 나누기
- CPU 코어 독점 방지
- OOM 시 “이 세션만 죽이기”

### 1.2 비유

| 비유 | 설명 |
|------|------|
| **같은 아파트 한 칸** | 서버 RAM = 거실 하나. tmux는 거실 안에 **칸막이만** 친 것 |
| **주식 + 코인** | 두 봇 모두 **같은 거실(물리 RAM)** 의 공기(메모리)를 마신다 |
| **OOM** | 거실 공기가 바닥나면 **칸막이와 무관하게** 안에 있는 **모든** 프로세스가 위험해진다 |

### 1.3 예전 워크플로

```bash
tmux attach -t coin_bot
python -m bitget.main   # ❌ 레거시, Phase 1 BLOCKED
```

이 방식은:

1. **리소스 상한 없음** — 코인 스캔·pandas가 RAM을 쓰면 주식도 같이 압박
2. **중복 실행 위험** — systemd `dante-bitget-factory` 와 tmux 수동 실행이 **동시에** 돌면 CPU·DB·텔레그램이 두 배
3. **로그·재시작** — systemd `journalctl`·`Restart=` 보다 운영이 불안정

### 1.4 systemd 데몬이면 tmux가 필요 없는 이유

`dante-bitget-factory` 는 이미:

- SSH 종료 후에도 실행 (`Type=simple`, `Restart=on-failure`)
- 재부팅 후 자동 기동 (`WantedBy=multi-user.target`)
- 로그 영구 보관 (`journalctl`)

**tmux 창을 열어 두지 않아도** 코인 봇은 24/7 돈다.  
tmux는 “로그를 눈으로 보고 싶을 때”만 선택적으로 쓰면 된다:

```bash
# tmux 없이 로그 보기 (권장)
sudo journalctl -u dante-bitget-factory -f
```

### 1.5 그래도 tmux에서 돌리고 싶다면 (차선)

수동 실행이 꼭 필요하면, **tmux 안에서도 cgroup 상한**을 걸 수 있다:

```bash
# tmux coin_bot 창 안에서 — 1GB 하드 캡 예시
systemd-run --user --scope \
  -p MemoryMax=1G \
  -p MemoryHigh=800M \
  bash -lc 'cd /home/ubuntu/Dual-Screener-Bot && source venv/bin/activate && \
  python -m bitget.pipelines.bitget_auto_pilot --daemon'
```

이렇게 하면 “tmux에서 보이지만”, 실제 제한은 **systemd scope(cgroup)** 이 건다.  
**프로덕션 SSOT는 여전히 `dante-bitget-factory.service`** 를 권장한다 (이중 실행 방지).

---

## 2. systemd cgroups — 코인만 메모리 상한 걸기

Linux **cgroups v2** + systemd가 프로세스 그룹별로 `MemoryMax` / `MemoryHigh` 를 강제한다.  
주식 `dante-factory` 와 코인 `dante-bitget-*` 는 **서로 다른 unit** 이므로, 코인 쪽에만 낮은 상한을 두면 **코인이 먼저 제한·종료**된다.

### 2.1 MemoryHigh vs MemoryMax

| 설정 | 동작 | 용도 |
|------|------|------|
| **MemoryHigh** | 한도 근처에서 **스로틀**(할당 속도 제한) | “먼저 느려져라” — 주식에 여유를 줌 |
| **MemoryMax** | 한도 초과 시 **OOM-kill 대상** (해당 unit만) | “절대 이 이상 먹지 마라” — 하드 캡 |

**권장 조합:** `MemoryHigh` (소프트) + `MemoryMax` (하드), `MemoryMax` ≥ `MemoryHigh`.

### 2.2 현재 Bitget 유닛 템플릿 상태

| 유닛 | 템플릿 파일 | 기본 메모리 상한 |
|------|-------------|------------------|
| `dante-bitget-factory` | `bitget/deploy/systemd/dante-bitget-factory.service.in` | `MemoryMax=2G`, `CPUQuota=150%` |
| `dante-bitget-dashboard` | `dante-bitget-dashboard.service.in` | `MemoryMax=768M` |
| `dante-bitget-heatmap` | `dante-bitget-heatmap.service.in` | `MemoryMax=512M` |
| `dante-bitget-ws` | `dante-bitget-ws.service.in` | (미설정 — 아래 권장값 추가) |
| `dante-bitget-async` | `dante-bitget-async.service.in` | (미설정) |

주식 `dante-factory.service` 템플릿에는 **MemoryMax가 없음** → 기본적으로 **남은 RAM을 더 쓸 수 있음** (의도적으로 주식 우선).

### 2.3 서버 RAM별 권장 코인 상한 (예시)

서버 **총 RAM** 을 먼저 확인한다:

```bash
free -h
# 또는
grep MemTotal /proc/meminfo
```

| 서버 총 RAM | 코인 스택 합산 권장 상한 | factory | ws | dashboard | heatmap | async |
|-------------|-------------------------|---------|-----|-----------|---------|-------|
| **8 GB** | ~2.0 GB | 1G / High 768M | 256M | 384M | 256M | 128M |
| **16 GB** | ~3.5 GB | 1.5G / High 1.2G | 384M | 512M | 384M | 192M |
| **32 GB** | ~6 GB | 2G / High 1.5G (현행) | 512M | 768M | 512M | 256M |

**원칙:** `주식 예상 사용 + 코인 상한 합 + 1~2GB(OS·버퍼)` < 총 RAM (+ Swap).

### 2.4 설정 방법 A — drop-in override (운영 중 즉시, 권장)

템플릿을 수정하지 않고 **서버에서만** 상한을 조정한다 (git pull과 무관, 업데이트 후에도 유지).

```bash
# 예: factory를 1GB 하드 캡으로 (8GB 서버)
sudo mkdir -p /etc/systemd/system/dante-bitget-factory.service.d
sudo tee /etc/systemd/system/dante-bitget-factory.service.d/memory.conf <<'EOF'
[Service]
MemoryHigh=800M
MemoryMax=1G
CPUQuota=100%
EOF

# WS에도 상한 (예시)
sudo mkdir -p /etc/systemd/system/dante-bitget-ws.service.d
sudo tee /etc/systemd/system/dante-bitget-ws.service.d/memory.conf <<'EOF'
[Service]
MemoryHigh=200M
MemoryMax=256M
EOF

# async
sudo mkdir -p /etc/systemd/system/dante-bitget-async.service.d
sudo tee /etc/systemd/system/dante-bitget-async.service.d/memory.conf <<'EOF'
[Service]
MemoryHigh=100M
MemoryMax=128M
EOF

sudo systemctl daemon-reload
sudo systemctl restart dante-bitget-ws dante-bitget-async dante-bitget-factory
```

**적용 확인:**

```bash
systemctl show dante-bitget-factory -p MemoryMax -p MemoryHigh -p MemoryCurrent
systemd-cgtop   # 실시간 cgroup 메모리 (q로 종료)
```

### 2.5 설정 방법 B — 템플릿 수정 (저장소 반영)

`bitget/deploy/systemd/*.service.in` 의 `[Service]` 블록에 직접 추가 후:

```bash
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot \
  ./bitget/deploy/deploy_bitget_factory.sh
sudo systemctl restart dante-bitget-ws dante-bitget-factory ...
```

**factory 예시 (8GB 서버용):**

```ini
[Service]
# ... 기존 항목 ...
MemoryHigh=800M
MemoryMax=1G
CPUQuota=100%
```

### 2.6 OOM 시 어떤 프로세스가 죽는가

`MemoryMax` 초과 시 Linux OOM killer는 **해당 cgroup 안의 프로세스**를 우선 종료한다.

- 코인 factory만 1G 초과 → **`dante-bitget-factory` 만 재시작** (`Restart=on-failure`)
- 주식 `dante-factory` 는 **별도 cgroup** → 상한을 넘지 않았다면 **계속 실행**

코인이 죽는 것은 **의도된 격리**다. Watchdog·텔레그램으로 알림 받고 원인(스캔 동시성·메모리 누수)을 조사한다.

### 2.7 애플리케이션 레벨 완화 (메모리 상한과 병행)

`.env` / `bitget/.env`:

```bash
# 스캐너 동시성 낮추기 (RAM 피크 감소)
BITGET_MAX_WORKERS=4          # 기본 8 → 4 (8GB 서버)
BITGET_TELEGRAM_CONCURRENCY=2
```

`master_scanner`·`mtf_data_updater` 가 피크 RAM의 주범일 수 있으므로, **cgroup 상한 + worker 축소** 를 같이 쓴다.

---

## 3. 주식 봇 보호 체크리스트

| # | 조치 | 명령·설정 |
|---|------|-----------|
| 1 | 코인에만 `MemoryMax` | §2.4 drop-in |
| 2 | **이중 실행 금지** | tmux 수동 `bitget.main` ❌ + `systemctl is-active dante-bitget-factory` |
| 3 | 데이터 경로 분리 | `BITGET_DB_STORAGE_PATH` ≠ `DB_STORAGE_PATH` |
| 4 | cron 겹침 완화 | 주식·코인 scan 피크가 같으면 cron 시각 **5~10분 어긋나게** |
| 5 | 주식 상태 모니터 | `systemctl is-active dante-factory` + `journalctl -u dante-factory` |
| 6 | 코인 메모리 모니터 | `systemctl show dante-bitget-factory -p MemoryCurrent` |

### 3.1 이중 실행 진단

```bash
# 레거시 + systemd 동시 기동 여부
pgrep -af 'bitget.main|bitget_auto_pilot|factory_launcher' || true
systemctl is-active dante-bitget-factory dante-factory
```

`bitget_auto_pilot` 이 **두 줄** 이상이면 tmux 수동 실행 + systemd 가 **동시에** 돌고 있을 가능성이 크다.

---

## 4. Swap 메모리 점검 및 증설

Swap은 RAM보다 **느리지만**, 스캔·일일감사가 겹치는 순간 **갑작스런 OOM 전체 서버 다운**을 막는 완충재다.  
**Swap만 믿고 RAM 상한을 안 거는 것은 금물** — Swap + cgroup 상한을 **함께** 쓴다.

### 4.1 현재 Swap 확인

```bash
free -h
swapon --show
cat /proc/swaps
```

`Swap: 0B` 이면 피크 시 **전체 OOM 위험**이 크다.

### 4.2 권장 Swap 크기 (단일 서버, 주식+코인)

| 총 RAM | Swap 권장 (최소) |
|--------|------------------|
| 8 GB | 4 GB |
| 16 GB | 4~8 GB |
| 32 GB | 8 GB |

### 4.3 Swap 파일 추가 (재부팅 후에도 유지)

```bash
# 4GB swap 파일 예시 (서버에 디스크 여유 확인 후)
sudo fallocate -l 4G /swapfile_bitget_buffer
# fallocate 실패 시: sudo dd if=/dev/zero of=/swapfile_bitget_buffer bs=1M count=4096 status=progress

sudo chmod 600 /swapfile_bitget_buffer
sudo mkswap /swapfile_bitget_buffer
sudo swapon /swapfile_bitget_buffer

# 영구 등록
echo '/swapfile_bitget_buffer none swap sw 0 0' | sudo tee -a /etc/fstab

# 확인
free -h
swapon --show
```

### 4.4 Swappiness (선택)

기본 `vm.swappiness=60`. 주식·코인이 RAM을 많이 쓰는 서버에서는 **너무 일찍 Swap으로 밀리지 않게** 10~30 으로 낮추는 경우가 많다:

```bash
cat /proc/sys/vm/swappiness
# 일시 적용
sudo sysctl vm.swappiness=20
# 영구
echo 'vm.swappiness=20' | sudo tee /etc/sysctl.d/99-quant-swappiness.conf
sudo sysctl --system
```

**주의:** swappiness를 0으로 두면 Swap이 거의 안 쓰여 **OOM이 더 빨리** 날 수 있다. 0이 아닌 **낮은 값(10~30)** 권장.

### 4.5 피크 시간대 모니터링

```bash
# 5초마다 RAM/Swap
watch -n 5 free -h

# cgroup별 (코인 factory만)
systemd-cgtop
```

---

## 5. 시나리오별 동작 정리

### 시나리오 A — tmux만 사용, cgroup 없음 (현재 오해에 가까운 방식)

```
코인 RAM 급증 → 전체 서버 메모리 압박 → 주식·코인 동시 저하 또는 전체 OOM
```

### 시나리오 B — systemd + MemoryMax (권장)

```
코인 RAM 급증 → dante-bitget-factory cgroup만 1G 도달 → 코인만 kill/restart
→ 주식 dante-factory 는 별도 cgroup → 정상 유지
```

### 시나리오 C — B + Swap 4GB

```
짧은 피크 → Swap으로 흡수 → 상한 미만이면 둘 다 생존
지속 피크 → 코인 cgroup만 제한 → 주식 우선 보호
```

---

## 6. 적용 순서 (운영자용)

```bash
# 1) RAM/Swap 현황
free -h && swapon --show

# 2) (Swap 0이면) §4.3 Swap 증설

# 3) 코인 unit memory drop-in (§2.4, 서버 RAM에 맞게 숫자 조정)
sudo systemctl daemon-reload
sudo systemctl restart dante-bitget-ws dante-bitget-async dante-bitget-factory \
  dante-bitget-dashboard dante-bitget-heatmap

# 4) .env worker 축소
#    BITGET_MAX_WORKERS=4

# 5) tmux 레거시 coin_bot 중지 (이중 실행 제거)
tmux kill-session -t coin_bot 2>/dev/null || true

# 6) 검증
systemctl is-active dante-factory dante-bitget-factory
systemctl show dante-bitget-factory -p MemoryMax -p MemoryCurrent
./bitget/deploy/bitget.sh --health
```

---

## 7. FAQ

**Q. tmux `coin_bot` 을 꼭 켜 두어야 하나?**  
A. 아니다. systemd가 24/7 돌린다. 로그는 `journalctl` 로 본다.

**Q. tmux를 나누면 주식이 안전한가?**  
A. 아니다. **systemd MemoryMax** 로 코인 상한을 걸어야 주식이 안전하다.

**Q. 코인 factory가 OOM kill 되면?**  
A. `Restart=on-failure` 로 자동 재기동. 반복되면 `BITGET_MAX_WORKERS` 축소·`MemoryMax` 상향·스캔 스케줄 조정.

**Q. 주식에도 MemoryMax를 걸어야 하나?**  
A. 선택. 주식이 **절대 죽으면 안 되면** 주식에는 넉넉한 상한(또는 미설정), 코인에만 **낮은** 상한을 둔다.

---

## 8. 요약

| 항목 | 결론 |
|------|------|
| tmux | 터미널 UI일 뿐, **RAM/CPU 격리 없음** |
| 프로덕션 | **systemd `dante-bitget-*`** — tmux 창 불필요 |
| 주식 보호 | 코인 unit에 **`MemoryHigh` + `MemoryMax`** (drop-in 권장) |
| Swap | `free -h` 확인 후 **4GB+** 완충 |
| 이중 실행 | tmux 수동 + systemd **동시 금지** |

**한 줄:** 같은 서버에서 주식과 코인을 함께 돌리려면 “tmux 방”이 아니라 **“systemd cgroup 방”** 이 필요하다.
