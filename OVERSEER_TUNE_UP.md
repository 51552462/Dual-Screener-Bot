# OVERSEER_TUNE_UP — Overseer 오탐 제거·LLM 지연 완화

## Action 1) SIGNAL_MISMATCH 기준 현실화

수정 파일: `overseer_audit_binder.py`

- `OVERSEER_KELLY_LOW_THRESHOLD` 기본값: `0.05` → `0.02`
- `OVERSEER_KELLY_CRIT_THRESHOLD` 기본값: `0.02` → `0.015`
- `SIGNAL_MISMATCH` 트리거 조건 보강:
  - 기존: BULL + Kelly 낮음 + (청산 0 또는 청산/진입 0/0)
  - 변경: BULL + Kelly 낮음 + **청산/진입 0/0** + `META_GLOBAL_KELLY_MULT < OVERSEER_META_MULT_CLAMP`

결과:
- BULL에서 `유효 Kelly=2.38% (0.0238)`는 기본 임계(`0.02`)를 상회하므로 WARN/CRITICAL 오탐이 더 이상 발생하지 않음.
- 단순히 당일 매매 0건이라는 이유만으로 경고를 내지 않고, 실제 글로벌 승수 클램프 근거가 있을 때만 `SIGNAL_MISMATCH`를 발생시킴.

## Action 2) LLM 해석 타임아웃 연장

수정 파일: `ai_overseer.py`

- `LlmCallSpec.timeout_sec`: `45.0` → `75.0`
- `generate_text_sync(..., max_wait_sec=...)`: `90.0` → `180.0`
- 재시도 강화: `max_attempts=2` 추가

결과:
- “LLM 해석 지연” 조기 fallback 빈도를 낮추고, 응답 지연 구간에서 더 우아하게 대기 후 해석 본문을 수신하도록 조정됨.

## 배포 안내

로컬(저장소 루트)에서:

```bash
git add . && git commit -m "Tune overseer thresholds and timeout" && git push origin main
```

서버에서:

```bash
sudo bash update_bot.sh
```
