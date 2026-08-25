# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-25  
> **유형**: **FULL-BT-HIST-2 Claude OK 수신** · **VPS dry→10×2 대기** (푸시 후 실행)  
> **선행**: HIST-2 스펙 OK (비차단 caveat 2건)

---

## Claude OK 수령 · 실행 대기

스펙 검증 OK. 코드 추가 변경 없음. VPS dry→10×2 승인됨 — **커밋/푸시 후** coin VPS에서 실행.

### caveat 1 (선확인 · VPS 보고에도 재기재)
HIST-2 diff = `full_bt_diag` **신규 CREATE만**. `bitget_full_bt.sqlite` 결과 테이블(`bitget_forward_trades` 클론) · `bitget_full_bt_checkpoint` · report §2 **컬럼 ALTER 없음** → FULL-BT 결과 스키마 불변.

### caveat 2 (기록 수용)
Handoff harness-only 대비 batch.py·pilot.sh 확장 — FULL-BT 스캐폴드 내부·🔴 아님. 이후 유사 시 사전 Ask.

### VPS 보고 예정 항목 (실행 후 본 파일 갱신)
1. spot.diag / futures.diag — `engine_hit_total` · `gate_reject_count`
2. hit=0 vs reject>0 분리 여부
3. caveat 1 컬럼 불변 재확인 1줄
4. paper `bitget_forward_trades` before=after

**전체 유니버스 런: 금지 유지**
