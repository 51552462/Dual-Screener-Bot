"""
스필오버 관측(Observe-only) 배너 — 엔진 점수·가중치에 영향 없음.

`auto_forward_tester` 가 V28 리포트에서 갱신하는 `SPILLOVER_OBSERVE_SSOT` 를 읽어
콘솔에 가상 배수만 표시한다. master / nulrim 의 score 연산에는 사용하지 않는다.
"""
from __future__ import annotations

from datetime import datetime

import pytz

_last_observe_log_day: str | None = None


def log_spillover_observe_banner() -> None:
    """한국장 스캔 루프당 1일 1회: 관측 SSOT 가 있으면 가상배수 로그."""
    global _last_observe_log_day
    kr_tz = pytz.timezone("Asia/Seoul")
    day = datetime.now(kr_tz).strftime("%Y-%m-%d")
    if _last_observe_log_day == day:
        return
    try:
        from system_config_atomic import load_system_config

        cfg = load_system_config()
    except Exception:
        return
    blob = cfg.get("SPILLOVER_OBSERVE_SSOT") if isinstance(cfg, dict) else None
    if not isinstance(blob, dict):
        print(
            "[관측] 스필오버 SSOT 없음 — 심층 리포트(V28) 실행 후 `SPILLOVER_OBSERVE_SSOT` 가 채워집니다. "
            "(엔진 점수·가중치 미연동)"
        )
        _last_observe_log_day = day
        return
    try:
        mult = float(blob.get("observe_multiplier") or 1.0)
    except (TypeError, ValueError):
        mult = 1.0
    try:
        a3 = int(blob.get("align_3d") or 0)
    except (TypeError, ValueError):
        a3 = 0
    print(
        f"[관측] 현재 시너지 가상배수: {mult:.1f}x (미적용) | 최근3일(KST) 한·미 섹터 일치: {a3}회 "
        f"— 엔진 점수·가중치 변경 없음"
    )
    _last_observe_log_day = day
