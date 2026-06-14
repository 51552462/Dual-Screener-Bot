# 주간 Flow 총결산 「표본 0건」진단 보고서

**대상 리포트:** `[V100.0 퀀트 팩토리 주간 흐름(Flow) 총결산]`  
**표시 기간:** 2026-06-06 ~ 2026-06-13  
**증상:** KR/US 「이번 주 청산 없음」· Universal DNA 「표본 0건」· Flow 태그 「표본 없음」  
**배경:** 최근 ~13일 시스템 정지 · Ubuntu에서 `exit_date` NULL 백필·동기화 **완료** (사용자 진술)

---

## 1. 최종 진단 결론

| 판정 | 설명 |
|------|------|
| **1차 원인: True Zero (실제 데이터 부재)** | 13일간 `track_daily_positions`·스캐너·일일 파이프라인이 멈추면 **2026-06-06 이후 `exit_date`를 가진 신규 CLOSED 행이 생성되지 않음**. `exit_date` NULL 백필은 **기존 CLOSED 행**에만 적용되며, 대부분 **5월 이전/중순 `entry_date`** 를 가지므로 주간 창 `[2026-06-06, 2026-06-13]`에 들어오지 않음. |
| **2차 요인: False Zero 위험 (로직 불일치, 수정함)** | 구버전 `weekly_flow_report._load_week_closed_df`는 SQL `exit_date >= week_start`만 사용해 **일일 리포트(`closed_event_dates`·`in_date_window`)와 불일치**. `exit_date` NULL·비표준 포맷·`trade_date`만 채워진 행은 **DB에 있어도 주간만 0건**이 될 수 있었음. **본 진단에서 해당 로직을 수정 반영.** |

**요약 한 줄:** 현재 백지는 **거의 확실히 True Zero**이나, 백필 후에도 0이면 **False Zero 가능성**이 있어 주간 로더를 일일 리포트와 동일 SSOT로 맞춤. Ubuntu에서 §3 검증 스크립트로 최종 확정할 것.

---

## 2. DB 데이터 팩트 체크 (Cross-Validation)

### 2.1 DB 경로 SSOT

| 항목 | 모듈 | 경로 |
|------|------|------|
| 메인 DB | `market_db_paths.MARKET_DATA_DB_PATH` | `{factory_data_dir()}/market_data.sqlite` |
| 데이터 루트 | `factory_data_paths.factory_data_dir()` | `DB_STORAGE_PATH` → 없으면 `~/dante_bots/Dual-Screener-Bot` |
| 주간 리포트 읽기 | `system_auto_pilot.DB_PATH` | `market_data_db_path()` (메인과 동일) |

**로컬 워크스페이스:** `market_data.sqlite` **없음** — 아래 쿼리는 **Ubuntu 서버**에서 실행 필요.

### 2.2 검증 쿼리 (Ubuntu)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot   # INSTALL_ROOT
source .env && source venv/bin/activate
python scripts/weekly_zero_diag.py
```

### 2.3 수동 SQL (동일 검증)

```sql
-- DB 경로 확인
-- python -c "from market_db_paths import MARKET_DATA_DB_PATH; print(MARKET_DATA_DB_PATH)"

-- [A] 구버전 주간 SQL (legacy — exit_date >= 시작일만)
SELECT market, COUNT(*) AS legacy_cnt
FROM forward_trades
WHERE status LIKE 'CLOSED%'
  AND exit_date >= '2026-06-06'
  AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
  AND final_ret IS NOT NULL
GROUP BY market;

-- [B] 주간 창 엄격 (exit_date 문자열 BETWEEN — 상한 포함)
SELECT market, COUNT(*) AS strict_exit_between
FROM forward_trades
WHERE status LIKE 'CLOSED%'
  AND substr(trim(exit_date),1,10) BETWEEN '2026-06-06' AND '2026-06-13'
  AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
  AND final_ret IS NOT NULL
GROUP BY market;

-- [C] Coalesce ground truth (exit → entry → trade_date)
SELECT market, COUNT(*) AS truth_cnt
FROM forward_trades
WHERE status LIKE 'CLOSED%'
  AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
  AND final_ret IS NOT NULL
  AND date(substr(
        COALESCE(
          NULLIF(trim(exit_date),''),
          NULLIF(trim(trade_date),''),
          NULLIF(trim(entry_date),'')
        ), 1, 10)) BETWEEN '2026-06-06' AND '2026-06-13'
GROUP BY market;

-- [D] 전체 워터마크 (주간 밖 데이터 존재 여부)
SELECT market,
       MAX(substr(COALESCE(NULLIF(trim(exit_date),''), entry_date),1,10)) AS max_exit,
       COUNT(*) AS closed_total
FROM forward_trades
WHERE status LIKE 'CLOSED%' AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
GROUP BY market;

-- [E] NULL exit_date 잔존
SELECT market, COUNT(*) FROM forward_trades
WHERE status LIKE 'CLOSED%' AND (exit_date IS NULL OR trim(exit_date)='')
GROUP BY market;
```

### 2.4 결과 해석 매트릭스

| [B] strict | [C] truth | [D] max_exit | 진단 |
|------------|-----------|--------------|------|
| 0 | 0 | ≤ 2026-05-26 근처 | **True Zero** — 주간 창에 청산 없음 (정상 Fallback) |
| 0 | >0 | any | **False Zero** — coalesce에는 있으나 strict SQL 누락 → **패치 후 재실행** |
| >0 | >0 | ≥ 2026-06-06 | 데이터 있음 — 리포트 재실행 시 숫자 채워져야 함 |
| any | any | OK but [E]>0 | 백필 미완 — `force_data_sync.sh` 재실행 |

### 2.5 예상 결과 (13일 정지 시나리오)

동기화·NULL 백필만 하고 **6월에 실제 청산 이벤트가 없었다면:**

- `[B]` = `[C]` = **0** (KR/US)
- `[D].max_exit` ≈ **2026-05-26** (이전 워터마크 고착과 일치)
- 리포트 「이번 주 청산 없음」= **버그가 아닌 정확한 Fallback**

신규 청산을 만들려면: `track_daily_positions` + OPEN 포지션 존재 또는 스캐너 신규 장부 적재 후 **6월 `exit_date`** 가 기록되어야 함.

---

## 3. 주간 리포트 로직 점검

### 3.1 파이프라인

```
factory.sh --weekly
  → factory_pipelines._step_weekly_master()
  → weekly_flow_report.send_weekly_flow_master_report()
  → build_weekly_flow_snapshot()
       → _load_week_closed_df()   ← 주간 표본 SSOT
       → build_weekly_market_snapshot() / build_weekly_dna_rollup() / build_weekly_flow_tag_rollup()
  → format_weekly_flow_report_html()
```

### 3.2 구버전 버그 (False Zero 후보)

**파일:** `weekly_flow_report.py` — `_load_week_closed_df` (수정 전)

```python
# 문제점
WHERE exit_date >= ?          # ① NULL exit_date 전량 제외 (SQL에서 NULL 비교 실패)
                              # ② week_end 상한 없음 (과다 집계만, 0건 원인 아님)
                              # ③ trade_date / entry_date 폴백 없음
                              # ④ report_date_utils.normalize 미사용
AND final_ret IS NOT NULL     # ⑤ SQL 단계 필터 (pandas 정규화 전)
```

**일일 리포트** (`reports/daily_report_context.load_market_slice`)는:

- SQL: market + INCUBATOR 제외만
- Pandas: `closed_event_dates()` + `in_date_window(rolling_cutoff, session_anchor)`

→ **주간만 SQL raw `exit_date`에 의존** → 일일·주간 불일치.

### 3.3 수정 내용 (2026-06-13 반영)

**파일:** `weekly_flow_report.py`

- `_load_week_closed_df(conn, market, week_start, week_end)` 시그니처 확장
- SQL: market + CLOSED% + INCUBATOR 제외만 로드
- Pandas: `closed_event_dates` → `trade_date` 폴백 → `in_date_window(week_start, week_end)`
- `final_ret` 숫자 변환·`dropna`는 **윈도우 필터 후** 적용

```python
closed_day = closed_event_dates(df)
if "trade_date" in df.columns:
    trade_d = normalize_date_series(df["trade_date"])
    closed_day = closed_day.where(closed_day != "", trade_d)
window_mask = in_date_window(closed_day, week_start, week_end)
```

`weekly_flow_rollup.py`의 DNA/Flow 태그는 **이미** 빈 DataFrame 시 `_empty_*_rollup()` Fallback — 버그 없음.

### 3.4 빈 데이터 시 동작 (의도된 설계)

| 단계 | empty 시 |
|------|----------|
| `build_weekly_market_snapshot` | `None` → 「이번 주 청산 없음」 |
| `build_weekly_dna_rollup` | `n_total=0` → 「표본 0건」 |
| `build_weekly_flow_tag_rollup` | 「flow_tags 표본 없음」 |
| `send_weekly_flow_master_report` | **예외 없이 텔레그램 발송** (L596–599) |

→ 크래시 없이 백지 = **데이터 0건 Fallback**, 정상.

---

## 4. Ubuntu 실행 쿼리 결과 (워크스페이스)

| 항목 | 결과 |
|------|------|
| 로컬 `market_data.sqlite` | **없음** (Git 워크스페이스에 DB 미포함) |
| 코드 정적 분석 | True Zero + False Zero 로직 리스크 **확인** |
| 자동 교차검증 | `scripts/weekly_zero_diag.py` — **서버에서 실행 필요** |

**서버에서 한 줄 판정:**

```bash
python scripts/weekly_zero_diag.py
# TRUE_ZERO / FALSE_ZERO / DATA_PRESENT 출력
```

---

## 5. 주간 리포트 정상화 절차

### 5.1 True Zero인 경우 (예상)

1. 데이터 동기화는 완료됨 — **6월 주간 CLOSED가 없으면 리포트는 계속 0이 맞음**
2. 정상 숫자를 보려면:
   - `track_daily_positions('KR'/'US')`로 OPEN 청산 발생, 또는
   - 스캐너·가상매매로 **6월 `exit_date` CLOSED** 적재
3. 그 후:

```bash
cd $INSTALL_ROOT && source venv/bin/activate
git pull   # weekly_flow_report.py 패치 반영
./factory.sh --weekly
```

### 5.2 False Zero였던 경우 (패치 후)

```bash
python scripts/weekly_zero_diag.py   # truth_cnt > 0 확인
./factory.sh --weekly
```

`KR청산=N US청산=M` (N,M>0) 로그 · 텔레그램 본문에 일자별 PnL·DNA 표본 표시.

### 5.3 dry-run

```bash
python weekly_flow_report.py
# ./_weekly_dry_run.html + stdout KR/US closed counts
```

---

## 6. 관련 파일

| 파일 | 역할 |
|------|------|
| `weekly_flow_report.py` | `_load_week_closed_df` · `send_weekly_flow_master_report` (**수정됨**) |
| `weekly_flow_rollup.py` | DNA · Flow 태그 롤업 |
| `report_date_utils.py` | `closed_event_dates` · `in_date_window` |
| `market_db_paths.py` | DB 경로 |
| `scripts/weekly_zero_diag.py` | True/False Zero 교차검증 |
| `WEEKLY_FLOW_REPORT_RECOVERY.md` | 운영 재발송 가이드 |

---

## 7. 결론 체크리스트

- [ ] Ubuntu `weekly_zero_diag.py` 실행 → `VERDICT` 확인  
- [ ] `truth_cnt` = 0 이면 **True Zero** — 파이프라인 복구·신규 청산 필요  
- [ ] `truth_cnt` > 0 인데 리포트 0이면 **패치 배포 후 `--weekly` 재실행**  
- [ ] 복구 후 `FACTORY_FORCE_SCAN_OUTSIDE_SESSION` 등 임시 env 제거  

**현 시점 코드베이스 판단:** 13일 정지 + 5/26 워터마크 고착 맥락에서는 **True Zero가 주원인**이며, 주간 SQL-only 필터는 **잠재적 False Zero 버그**로 분류되어 **수정 완료**.
