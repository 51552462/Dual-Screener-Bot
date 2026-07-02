# 우분투 서버 구매·분리 결정 가이드 (2026-07)

> **목적:** 재미나이(Gemini)와 Cursor가 **같은 전제**로 서버 스펙·대수를 맞추기 위한 단일 SSOT 문서.  
> **근거:** 2026-07-02 운영 서버 스크린샷 + `Dual-Screener-Bot` 코드베이스 리소스·배포 구조.  
> **독자:** 결제·마이그레이션 담당자.

---

## 0. Executive Summary (결론 먼저)

| 판단 | 내용 |
|------|------|
| **현재 1대(3.7GiB RAM, 디스크 94%)** | 한·미·코인 **동시 100% 가동에 부적합**. RAM·디스크 둘 다 병목. |
| **권장 (비용·안정 균형)** | **서버 2대 분리** — 주식 1대 + 코인 1대 (각 **4GB RAM / 80GB SSD** 이상). |
| **차선 (운영 단순)** | **서버 1대 업그레이드** — **8GB RAM / 160GB SSD** 이상 + `BITGET_YIELD_TO_FACTORY=1` 유지. |
| **비권장** | 현 스펙(4GB급) 유지 + yield OFF + cgroup 4.5G 한도 → **물리 RAM 초과·스왑·디스크 풀** 위험. |

**재미나이와 맞출 때 핵심 숫자:** 주식 트랙 **~3GB 피크**, 코인 트랙 **~2–3GB 피크**, OS·버퍼 **≥1.5GB**, 디스크 **여유 ≥30%**.

---

## 1. 현재 서버 실측 (스크린샷 기준)

호스트명 `ip-172-26-12-53` → AWS **Lightsail** 또는 VPC 내 EC2 유사 인스턴스로 추정.

| 항목 | 실측 | 해석 |
|------|------|------|
| OS | Ubuntu **22.04.5 LTS** | 코드·systemd 템플릿과 호환 ✅ |
| RAM | **3.7 GiB** total, available **~2.5 GiB**, swap **4 GiB** (390 MiB 사용) | 이미 스왑 사용 중 → 메모리 압박 상태 |
| 디스크 `/` | **78 GiB** 중 **73 GiB 사용 (94%)**, 여유 **4.8 GiB** | **P0 위험** — 로그·SQLite·백업 여유 없음 |
| CPU | `lscpu` 미설치 (추정 2 vCPU) | 주식 `CPUQuota=200%` + 코인 `150%` = 동시에 **3.5코어 분** 요구 |

### 1.1 코드에 박힌 cgroup 한도 vs 물리 RAM

| systemd 유닛 (트랙) | MemoryMax (템플릿) | 비고 |
|---------------------|-------------------|------|
| `dante-factory` (주식) | **3G** | `deploy/systemd/dante-factory.service.in` |
| `dante-bitget-factory` | **1.5G** | |
| `dante-bitget-queue-worker` | **2G** | |
| `dante-bitget-dashboard` | **768M** | |
| `dante-bitget-heatmap` | **512M** | |
| `dante-bitget-ws` / `dante-bitget-async` | 미설정 | 피크 시 추가 RAM |

**한 대에 전부 올리면 cgroup 상한 합만 ~7.8G** (동시 피크는 아니어도).  
**물리 RAM 3.7G** → cgroup은 “죽을 때 나눠 죽자”일 뿐, **동시 무거운 스캔을 물리적으로 수용하지 못함**.

### 1.2 원래 설계가 전제한 것

`bitget/bitget_scan_schedule.py` · `bitget/docs/10_single_server_resource_management.md`:

1. **크론 분(minute) 분리** — 주식(:00/:10/…) vs 코인(:07/:23/…)
2. **`BITGET_YIELD_TO_FACTORY=1`** — 시간대 겹침 시 코인 무거운 잡 **스킵**(4GB OOM 방지)
3. **`MAX_WORKERS=1`** (주식 `.env` 예시) — 4GB 보수값

최근 Two-Track 인프라(yield 기본 OFF, 락/큐 분리)는 **“겹치면 서로 안 죽게”**이지 **RAM을 늘리지 않음**.  
**4GB 단일 서버 + yield OFF = 원 설계와 충돌.**

---

## 2. 코드베이스 구조 — 무엇이 어디에 붙는가

### 2.1 주식 트랙 (KR + US) — Server A 후보

| 구성 | 경로·유닛 |
|------|-----------|
| 데몬 | `dante-factory`, `dante-async`, `dante-dashboard` |
| 타이머 | `dante-watchdog`, `dante-snapshot`, `dante-backup` |
| 크론 | `/etc/cron.d/dual-screener-factory-{kr,us}` → `factory.sh` |
| 락 | `DB_STORAGE_PATH/.factory_runtime.lock` |
| 큐 | `task_queue.sqlite` (`factory_data_paths`) |
| DB | `market_data.sqlite`, `forward_trades`, `treasury_state.json` |
| 설치 | `deploy_quant_factory.sh`, `update_factory.sh` |

**피크 부하:** KR/US 스캔·`daily_audit`·`data_updater`·리포트. **장중·장후 집중.**

### 2.2 코인 트랙 (Bitget) — Server B 후보

| 구성 | 경로·유닛 |
|------|-----------|
| 데몬 | `dante-bitget-factory`, `dante-bitget-ws`, `dante-bitget-queue-worker`, `dante-bitget-async` |
| UI | `dante-bitget-dashboard` (:8511), `dante-bitget-heatmap` (:8512) |
| 타이머 | `dante-bitget-watchdog`, `dante-bitget-snapshot` |
| 크론 | `/etc/cron.d/dual-screener-bitget` → `bitget.sh` |
| 락 | `BITGET_DB_STORAGE_PATH/.bitget_runtime.lock` |
| 큐 | `bitget_task_queue.sqlite` |
| DB | `bitget_market_data.sqlite`, `bitget_forward_trades` |
| 설치 | `bitget/deploy/deploy_bitget_factory.sh`, `bitget/deploy/update_bitget.sh` |

**피크 부하:** 24/7 WS + 스캔 10종×2마켓(SPOT/FUT) + `data_refresh`. **시간대 무관 상시.**

### 2.3 분리 시에만 남는 “교차 의존” (주의)

| 기능 | 분리 영향 | 권장 |
|------|-----------|------|
| **코인→주식 canary** (`BITGET_CANARY_STATE_PATH`) | 파일이 코인 서버에만 있으면 주식이 못 읽음 | ① S3/rsync 15분 동기화 ② 또는 주식 `.env`에 `CRYPTO_CANARY_PENALTY_ENABLED=0` (단기) |
| **Git 저장소** | 양쪽 동일 repo clone 가능 | 각 서버 `git pull` + 해당 `update_*.sh` 만 |
| **텔레그램** | 이미 봇/채널 분리 설계 | 주식 `telegram_env` / 코인 `BITGET_BOT_*` |

**매매 로직·스캐너 코드는 분리해도 수정 불필요.** 환경변수·cron·systemd만 트랙별로 깎으면 됨.

---

## 3. 구매 옵션 비교 (비용 효율)

가격은 리전·환율에 따라 변동. **AWS Lightsail USD 기준 2026년 상반기 대략치** (재미나이가 결제 화면에서 재확인).

| 옵션 | 스펙 예시 | 월 비용(대략) | 한·미·코인 100% | 운영 난이도 | Cursor 의견 |
|------|-----------|---------------|-----------------|-------------|-------------|
| **A. 2대 분리** ⭐ | 주식 **4GB/80GB** + 코인 **4GB/80GB** | **~$20+$20≈$40** | ✅ **가장 안전** | 중 (서버 2개) | **1순위 권장** |
| **B. 1대 업그레이드** | **8GB/160GB** | **~$40–48** | △ 가능 (yield·크론 준수 필요) | 낮 | 예산·운영 단순 시 |
| **C. 1대 16GB** | 16GB/320GB | **~$80+** | ✅ 여유 | 낮 | 지금은 과투자 |
| **D. 현행 유지** | 4GB/78GB 94% | ~$20 | ❌ | — | **즉시 중단 권고** |

### 3.1 왜 2대×4GB가 “한 대 8GB”와 비슷한 돈으로 더 낫나

- **물리 RAM 격리** — 코인 OOM이 주식 프로세스를 kernel 레벨에서 덜 건드림 (cgroup보다 강함).
- **디스크 분산** — 현재 **73GB/78GB** → 단일 디스크 풀 시 백업·WAL·journal과 경쟁. 분리 시 주식 DB·코인 DB 각각 여유 확보.
- **CPU 경합 감소** — 코인 24/7 WS가 주식 `daily_audit`와 같은 vCPU를 덜 잠음.
- **배포 독립** — `update_bitget.sh`가 주식 유닛을 안 건드리는 설계와 **하드웨어가 일치**.

### 3.2 한 대 8GB를 고를 때 필수 조건

1. 디스크 **≥160GB** (또는 데이터 루트를 볼륨 확장).
2. `.env`: `BITGET_YIELD_TO_FACTORY=1` (4GB 설계의 2차 방어 복원).
3. `MAX_WORKERS=1`, `BITGET_MAX_WORKERS=4` 이하로 보수 튜닝.
4. 디스크 94% → **마이그레이션 전 정리** (§6).

---

## 4. 권장 스펙 SSOT (재미나이 교차검증용)

### 4.1 옵션 A — 서버 2대 (권장)

#### Server A: `dante-equity` (한국·미국)

| 항목 | 최소 | 권장 |
|------|------|------|
| vCPU | 2 | 2 |
| RAM | 4 GiB | **4–8 GiB** (DB 크면 8) |
| SSD | 80 GiB | **120 GiB** (`market_data.sqlite` 성장) |
| OS | Ubuntu 22.04 LTS | 동일 |
| cgroup | `MemoryMax=3G` on `dante-factory` | drop-in으로 dashboard/async 상한 추가 가능 |

**올릴 것:** `dante-factory`, `dante-async`, `dante-dashboard`, timers, `dual-screener-factory-{kr,us}` cron.  
**올리지 말 것:** `dante-bitget-*` 전부.

#### Server B: `dante-bitget` (코인)

| 항목 | 최소 | 권장 |
|------|------|------|
| vCPU | 2 | 2 |
| RAM | 4 GiB | 4 GiB |
| SSD | 80 GiB | 80–120 GiB |
| OS | Ubuntu 22.04 LTS | 동일 |
| cgroup | factory 1.5G + queue-worker 2G (템플릿) | dashboard/heatmap은 필요 시만 enable |

**올릴 것:** `dante-bitget-*`, `dual-screener-bitget` cron.  
**올리지 말 것:** `dante-factory`, 주식 cron.

`.env` (코인 서버):

```bash
BITGET_YIELD_TO_FACTORY=0          # 물리 분리 후 yield 불필요
BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_MAX_WORKERS=4               # 4GB에서 8은 과함
```

`.env` (주식 서버):

```bash
DB_STORAGE_PATH=/var/lib/quant-factory/data
MAX_WORKERS=1
# canary 쓸 경우만 — 코인 서버에서 rsync된 경로
# BITGET_CANARY_STATE_PATH=/var/lib/quant-factory/canary/bitget_canary_state.json
```

### 4.2 옵션 B — 서버 1대

| 항목 | 스펙 |
|------|------|
| vCPU | **4** (2코어면 CPUQuota 합 350%에 병목) |
| RAM | **8 GiB** |
| SSD | **160 GiB** |
| OS | Ubuntu 22.04 LTS |

`.env` 추가:

```bash
BITGET_YIELD_TO_FACTORY=1
MAX_WORKERS=1
BITGET_MAX_WORKERS=4
```

---

## 5. 클라우드 상품 매핑 (재미나이가 결제 UI에서 고를 때)

아래는 **동급 스펙** 찾기용 키워드. 정확한 SKU명·가격은 결제 시점에 재확인.

### AWS Lightsail

| 용도 | 인스턴스 플랜 (참고) |
|------|---------------------|
| 옵션 A 주식 | **$20/mo** — 2 vCPU, 4 GB RAM, 80 GB SSD |
| 옵션 A 코인 | **$20/mo** — 동일 |
| 옵션 B 통합 | **$40/mo** — 2 vCPU, 8 GB RAM, 160 GB SSD |

### AWS EC2 (온디맨드 대안)

| 용도 | 인스턴스 타입 (참고) |
|------|---------------------|
| 4GB급 | `t3.medium` (2 vCPU, 4 GiB) + gp3 **80–120GB** |
| 8GB급 | `t3.large` (2 vCPU, 8 GiB) + gp3 **160GB** |

### 재미나이에게 맡길 확인 체크리스트

- [ ] 리전: **서울(ap-northeast-2)** vs 버지니아 (한국장 지연 vs Bitget API 지연 — 보통 **서울** 또는 **싱가포르**)
- [ ] Lightsail vs EC2: 고정 월요금 vs 유연 스케일 (운영 단순 → **Lightsail**)
- [ ] 옵션 A(2대) vs B(1대 8GB): 본 문서 **§0 결론**과 일치하는지
- [ ] 디스크: **신규 서버는 사용률 30% 이하로 시작** (마이그레이션 여유)
- [ ] 스냅샷/백업 비용 포함 여부

---

## 6. 마이그레이션 전 현 서버에서 할 일 (디스크 94%)

```bash
# 용량 큰 디렉터리
sudo du -xh / --max-depth=3 2>/dev/null | sort -h | tail -30

# journal 로그 상한 (이미 deploy_quant_factory.sh에 2G 설정 있음)
sudo journalctl --disk-usage

# 오래된 백업·pytest_cache·__pycache__ 정리 (데이터 DB는 삭제 금지)
```

**이전 대상 (대략):**

| 자산 | 주식 서버 | 코인 서버 |
|------|-----------|-----------|
| `market_data.sqlite` (+ snapshot) | ✅ | — |
| `treasury_state.json`, `system_config.json` | ✅ | — |
| `bitget_market_data.sqlite` | — | ✅ |
| `bitget_system_config.json` | — | ✅ |
| `.env` / `bitget/.env` | 주식 키만 | 코인 키만 |

**이전 후 설치 (요약):**

```bash
# Server A (주식)
sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ./deploy_quant_factory.sh
sudo ./update_factory.sh

# Server B (코인)
sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ./bitget/deploy/deploy_bitget_factory.sh
sudo INSTALL_ROOT=$INSTALL_ROOT ./bitget/deploy/update_bitget.sh
sudo INSTALL_ROOT=$INSTALL_ROOT bash bitget/deploy/install_bitget_cron.sh
```

---

## 7. 가동 검증 (양쪽 100%)

```bash
# Server A
systemctl is-active dante-factory dante-async dante-dashboard
ls /etc/cron.d/dual-screener-factory-kr /etc/cron.d/dual-screener-factory-us
bash deploy/audit_factory_stack.sh

# Server B
systemctl is-active dante-bitget-factory dante-bitget-ws dante-bitget-queue-worker dante-bitget-async
ls /etc/cron.d/dual-screener-bitget
./bitget/deploy/bitget.sh --health
```

---

## 8. Cursor ↔ 재미나이 합의 문장 (복붙용)

> **현재 Ubuntu 서버는 RAM 3.7GiB·디스크 94%로 한·미·코인 동시 100% 가동에 부적합하다.**  
> **코드베이스는 이미 주식(`dante-*`)과 코인(`dante-bitget-*`) 트랙 분리를 전제로 한다.**  
> **비용 대비 안정성 1순위는 Lightsail(또는 동급) 4GB×2대(주식+코인, 각 80GB+ SSD, 월 약 $40 합계).**  
> **운영 단순화가 우선이면 8GB/160GB 1대(월 약 $40–48) + `BITGET_YIELD_TO_FACTORY=1` + 디스크 확장.**  
> **현 4GB 단일 서버 유지 + yield OFF는 권장하지 않는다.**

---

## 9. 관련 문서

| 파일 | 내용 |
|------|------|
| `bitget/docs/10_single_server_resource_management.md` | cgroup·4GB/8GB RAM 표 |
| `bitget/docs/14_tri_factory_single_server_sovereign_masterplan.md` | 단일 서버 한계 진단 |
| `docs/한미코인_100퍼센트가동_점검_및_수정필요사항.md` | canary·큐·4GB 리스크 |
| `RUNBOOK.md` / `bitget/RUNBOOK.md` | 설치·업데이트 명령 |
| `deploy/ubuntu/factory_resource_limits.env.example` | 주식 보수 튜닝 |
| `bitget/deploy/bitget_resource_limits.env.example` | 코인 튜닝 |

---

*작성: Cursor (코드베이스 감사 기반). 재미나이는 §5 체크리스트·결제 UI 가격으로 최종 SKU 확정.*
