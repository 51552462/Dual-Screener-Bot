# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-25  
> **유형**: **FULL-BT-HIST-1 Claude OK 수신** · **파일럿 실런 시도** · **데이터 블로커** · **WAIT_DIRECTOR** (OHLCV 경로)

---

## HIST-1 코드 판정
Claude OK(비차단 caveat 1건) 수령 · Handoff 파일 `CLAUDE_TO_CURSOR.md` 최상단 보관 완료.

## 파일럿 실런 결과 (로컬 PC)

| 항목 | SPOT | FUTURES |
|------|------|---------|
| run_id | `pilot-spot-20260825T021558Z` | `pilot-fut-20260825T021558Z` |
| max_symbols | 10 | 10 |
| symbol_count / batches_run | **0 / 0** | **0 / 0** |
| trade_count / total_return_pct / mdd_pct | N/A (집계 대상 없음) | N/A |

**원인 (팩트):** `market_db_read_path()` → `bitget/bitget_market_data.sqlite` (32KB). 테이블은 `strategy_registry` 등만 존재, **`BITGET_SPOT_*_1D` / `BITGET_FUT_*_1D` OHLCV = 0**. live∩ohlcv n=0 → `select_run_symbols` eligible=0. 코드/Handoff 문제가 아니라 **로컬에 히스토리 시세 DB가 없음**.

### 지시 5항 보고
1. SPOT/FUT 정량표: 실행 가능 심볼 0 → trade_count 미산출 (가짜 숫자 생성 안 함, 룰5)
2. caveat: 결과 테이블 `run_id` 컬럼 **없음**(스키마 클론). 이번 런 트레이드 0 · 테이블 전체도 신규/빈 수준 — 혼입 관측 불가(데이터 없어 미발생)
3. gate_bottleneck / side_asymmetry: 집계 대상 0이라 슬롯 기본값만 (실측 미발생)
4. paper before=after: **0=0** (불변 확인)
5. 배너: report 호출 시 L1 배너 원문 유지(코드 경로) — 실측 row 없음

## Ask (디렉터)
파일럿을 완료하려면 **OHLCV가 있는 market DB**가 필요합니다.
- VPS에서 파일럿 실행, 또는
- `BITGET_DB_STORAGE_PATH`(또는 해당 market sqlite)를 OHLCV 포함 경로로 지정

경로 확정 후 Cursor가 **동일 파라미터**(max_symbols=10, SPOT/FUT 각 1회, pilot-{ts})로 재실행·5항 보고하겠습니다. **신규 코드/스키마 선확장 없음.**

## 비접촉
파일럿은 실행만 · HIST-1/원본 CAT diff 없음.
