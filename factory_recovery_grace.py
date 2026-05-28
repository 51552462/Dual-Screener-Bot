"""
Factory recovery grace — 리포트·가상매매·PIL 복구 모드 SSOT.

환경변수 FACTORY_RECOVERY_GRACE 또는 REPORT_STALENESS_RECOVERY_GRACE 가
1/true/on 이면 하드 차단을 완화한다.
"""
from __future__ import annotations

import os


def factory_recovery_grace() -> bool:
    for key in ("FACTORY_RECOVERY_GRACE", "REPORT_STALENESS_RECOVERY_GRACE"):
        v = str(os.environ.get(key, "0")).strip().lower()
        if v in ("1", "true", "yes", "on"):
            return True
    return False


def practitioner_penalties_relaxed(sys_config: object = None) -> bool:
    """복구 기간: PIL Kelly=0·RETIRED 미적용 (리포트·가상매매 정상화)."""
    if factory_recovery_grace():
        return True
    if isinstance(sys_config, dict):
        v = sys_config.get("PRACTITIONER_PENALTIES_RELAXED", 1)
        return str(v).strip().lower() not in ("0", "false", "no", "off")
    return True
