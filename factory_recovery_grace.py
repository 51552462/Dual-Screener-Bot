"""
Factory recovery grace — 리포트·가상매매·PIL 복구 모드 SSOT.

환경변수 FACTORY_RECOVERY_GRACE 또는 REPORT_STALENESS_RECOVERY_GRACE 가
1/true/on 이면 하드 차단을 완화한다.

P0-4: META_REGIME_KEY 가 BEAR 이면 유예(grace)는 **절대** 적용되지 않는다
(ZOMBIE Kelly=0 · RETIRED 정상 집행).
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def factory_recovery_grace() -> bool:
    """환경변수만 검사 — 국면 무관(raw env flag)."""
    for key in ("FACTORY_RECOVERY_GRACE", "REPORT_STALENESS_RECOVERY_GRACE"):
        v = str(os.environ.get(key, "0")).strip().lower()
        if v in ("1", "true", "yes", "on"):
            return True
    return False


def practitioner_penalties_relaxed(sys_config: object = None) -> bool:
    """
    복구 기간: PIL Kelly=0·RETIRED 미적용(완화) 여부.

    P0-4: BEAR 국면이면 env/config 와 무관하게 **항상 False** (페널티 집행).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}

    try:
        from bear_defense_booster_guard import (
            recovery_grace_blocked_by_regime,
            resolve_meta_regime_key,
        )

        rk = resolve_meta_regime_key(cfg)
        if recovery_grace_blocked_by_regime(rk):
            logger.info(
                "P0-4 recovery grace blocked: regime=%s — PIL penalties enforced",
                rk,
            )
            return False
    except Exception:
        pass

    if factory_recovery_grace():
        return True
    if isinstance(cfg, dict):
        v = cfg.get("PRACTITIONER_PENALTIES_RELAXED", 1)
        return str(v).strip().lower() not in ("0", "false", "no", "off")
    return True
