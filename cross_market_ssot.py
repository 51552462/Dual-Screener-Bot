"""
CROSS_MARKET_SSOT — US 마감 스냅샷 publish · KR graceful load.

- US publish: scan-us / daily-us / sector_spillover_refresh 후 (강제 scan 금지)
- KR stale/missing: KR_STANDALONE_MOMENTUM (US 가중 0%, KR 모멘텀 100%)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pytz

from us_kr_theme_bridge import build_cross_market_mapping_payload, is_valid_sector_label

logger = logging.getLogger(__name__)

CROSS_MARKET_SSOT_KEY = "CROSS_MARKET_SSOT"
MODE_US_ONLINE = "US_ONLINE"
MODE_KR_STANDALONE = "KR_STANDALONE_MOMENTUM"

_DEFAULT_STALE_HOURS = 36.0

_DDL = """
CREATE TABLE IF NOT EXISTS cross_market_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of_kst TEXT NOT NULL,
    us_session TEXT,
    us_sector_raw TEXT,
    us_sector_std TEXT,
    kr_sector_std TEXT,
    kr_play_targets_json TEXT,
    sector_weights_json TEXT,
    source TEXT,
    confidence REAL,
    mapping_confidence REAL,
    mode TEXT,
    published_at TEXT,
    payload_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_cross_market_asof ON cross_market_snapshot(as_of_kst DESC);
"""


def _db_path() -> str:
    from market_db_paths import market_db_read_path

    return market_db_read_path()


def _kst_now() -> datetime:
    return datetime.now(pytz.timezone("Asia/Seoul"))


def _kst_today() -> str:
    return _kst_now().strftime("%Y-%m-%d")


def ensure_cross_market_schema(db_path: Optional[str] = None) -> None:
    path = db_path or _db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.executescript(_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("cross_market_snapshot DDL skip: %s", ex)


def _invalid_sector(s: str) -> bool:
    return not is_valid_sector_label(s)


def _stale_hours(cfg: Dict[str, Any]) -> float:
    try:
        return float(cfg.get("CROSS_MARKET_STALE_HOURS", _DEFAULT_STALE_HOURS))
    except (TypeError, ValueError):
        return _DEFAULT_STALE_HOURS


def _parse_published_at(ssot: Dict[str, Any]) -> Optional[datetime]:
    raw = ssot.get("published_at") or ssot.get("as_of_kst")
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(str(raw)[:19], fmt)
            return pytz.timezone("Asia/Seoul").localize(dt)
        except ValueError:
            continue
    return None


def _age_hours(ssot: Dict[str, Any]) -> Optional[float]:
    pub = _parse_published_at(ssot)
    if pub is None:
        return None
    return (_kst_now() - pub).total_seconds() / 3600.0


def _default_ssot(mode: str = MODE_KR_STANDALONE) -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "mode": mode,
        "as_of_kst": _kst_today(),
        "published_at": _kst_now().strftime("%Y-%m-%d %H:%M:%S"),
        "us_sector_raw": "",
        "us_sector_std": "",
        "kr_sector_std": "",
        "kr_play_targets": [],
        "confidence": 0.0,
        "mapping_confidence": 0.0,
        "spillover_weight_us": 0.0,
        "spillover_weight_kr": 1.0,
        "source": "none",
        "age_hours": None,
        "degraded_reason": "",
    }


def load_cross_market_ssot(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    raw = cfg.get(CROSS_MARKET_SSOT_KEY)
    if isinstance(raw, dict) and raw.get("mode"):
        ssot = dict(raw)
    else:
        try:
            from config_manager import get_config_value

            disk = get_config_value(CROSS_MARKET_SSOT_KEY)
            ssot = dict(disk) if isinstance(disk, dict) else _default_ssot()
        except Exception:
            ssot = _default_ssot()

    age = _age_hours(ssot)
    ssot["age_hours"] = round(age, 2) if age is not None else None
    stale = age is None or age > _stale_hours(cfg)
    us_raw = str(ssot.get("us_sector_raw") or ssot.get("us_dominant_raw") or "").strip()

    if stale or _invalid_sector(us_raw):
        ssot = _apply_kr_standalone_mode(ssot, reason=stale and "stale_or_missing" or "invalid_us_sector")
    elif ssot.get("mode") != MODE_US_ONLINE:
        ssot = _apply_us_online_mode(ssot)
    return ssot


def _apply_us_online_mode(ssot: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(ssot)
    out["mode"] = MODE_US_ONLINE
    out["spillover_weight_us"] = 1.0
    out["spillover_weight_kr"] = 0.0
    out["degraded_reason"] = ""
    return out


def _apply_kr_standalone_mode(ssot: Dict[str, Any], *, reason: str) -> Dict[str, Any]:
    out = dict(ssot)
    out["mode"] = MODE_KR_STANDALONE
    out["spillover_weight_us"] = 0.0
    out["spillover_weight_kr"] = 1.0
    out["degraded_reason"] = reason
    return out


def append_snapshot_row(payload: Dict[str, Any], db_path: Optional[str] = None) -> None:
    path = db_path or _db_path()
    if not path:
        return
    ensure_cross_market_schema(path)
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.execute(
                """
                INSERT INTO cross_market_snapshot (
                    as_of_kst, us_session, us_sector_raw, us_sector_std, kr_sector_std,
                    kr_play_targets_json, sector_weights_json, source, confidence,
                    mapping_confidence, mode, published_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.get("as_of_kst") or "")[:10],
                    str(payload.get("us_session") or "post_close"),
                    str(payload.get("us_sector_raw") or ""),
                    str(payload.get("us_sector_std") or ""),
                    str(payload.get("kr_sector_std") or ""),
                    json.dumps(payload.get("kr_play_targets") or [], ensure_ascii=False),
                    json.dumps(payload.get("sector_weights") or {}, ensure_ascii=False),
                    str(payload.get("source") or "merged"),
                    float(payload.get("confidence") or 0),
                    float(payload.get("mapping_confidence") or 0),
                    str(payload.get("mode") or MODE_US_ONLINE),
                    str(payload.get("published_at") or ""),
                    json.dumps(payload, ensure_ascii=False, default=str),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("cross_market_snapshot insert failed: %s", ex)


def persist_cross_market_ssot(ssot: Dict[str, Any], *, save_config: bool = True) -> bool:
    if not isinstance(ssot, dict):
        return False
    try:
        from config_manager import load_system_config, save_system_config, set_config_value

        set_config_value(CROSS_MARKET_SSOT_KEY, ssot)
        if save_config:
            cfg = load_system_config() or {}
            cfg[CROSS_MARKET_SSOT_KEY] = ssot
            # 레거시 키 동기화
            us_raw = str(ssot.get("us_sector_raw") or "").strip()
            if is_valid_sector_label(us_raw):
                cfg["US_SPILLOVER_SECTOR"] = us_raw
                cfg["US_SPILLOVER_SECTOR_LAST_GOOD"] = us_raw
                cfg["US_SPILLOVER_SECTOR_AS_OF"] = str(ssot.get("as_of_kst") or _kst_today())[:10]
            cfg["SPILLOVER_RUNTIME_MODE"] = str(ssot.get("mode") or MODE_KR_STANDALONE)
            save_system_config(cfg)
        return True
    except Exception as ex:
        logger.error("persist_cross_market_ssot failed: %s", ex)
        return False


def publish_us_market_snapshot(
    cfg: Optional[Dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
    source: str = "merged",
    save: bool = True,
) -> Dict[str, Any]:
    """
    US 스냅샷 publish — forward_trades MFE 기반 + 테마 매핑.
    scan-us 를 깨우지 않음; 이미 수집된 DB·config 만 사용.
    """
    from config_manager import load_system_config, save_system_config
    from sector_spillover_refresh import refresh_us_spillover_from_db

    config = dict(cfg) if isinstance(cfg, dict) else load_system_config() or {}
    spill = refresh_us_spillover_from_db(config, db_path)

    us_raw = str(config.get("US_SPILLOVER_SECTOR") or config.get("US_SPILLOVER_SECTOR_LAST_GOOD") or "").strip()
    mapping = build_cross_market_mapping_payload(us_raw) if is_valid_sector_label(us_raw) else {}

    conf = 0.5
    if spill.get("updated") and spill.get("reason") == "ok":
        conf = 0.82
    elif is_valid_sector_label(us_raw):
        conf = 0.65

    now = _kst_now()
    ssot: Dict[str, Any] = {
        "schema_version": 1,
        "mode": MODE_US_ONLINE if is_valid_sector_label(us_raw) else MODE_KR_STANDALONE,
        "as_of_kst": _kst_today(),
        "published_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "us_sector_raw": us_raw,
        "us_sector_std": mapping.get("us_sector_std", ""),
        "kr_sector_std": mapping.get("kr_sector_std", ""),
        "kr_play_targets": mapping.get("kr_play_targets", []),
        "mapping_confidence": mapping.get("mapping_confidence", 0.0),
        "confidence": conf,
        "source": source,
        "spillover_refresh": spill,
        "sector_weights": {us_raw: 1.0} if us_raw else {},
    }

    if ssot["mode"] == MODE_US_ONLINE:
        ssot = _apply_us_online_mode(ssot)
    else:
        ssot = _apply_kr_standalone_mode(ssot, reason="no_valid_us_sector_at_publish")

    append_snapshot_row(ssot, db_path)
    persist_cross_market_ssot(ssot, save_config=save)
    if save:
        save_system_config(config)
    return ssot


def publish_us_snapshot_after_pipeline() -> Dict[str, Any]:
    """factory scan-us / daily-us tail — non-blocking publish."""
    try:
        out = publish_us_market_snapshot(source="pipeline_publish", save=True)
        print(f"🌐 [CrossMarket] US snapshot published mode={out.get('mode')} kr={out.get('kr_sector_std')}")
        return out
    except Exception as ex:
        logger.warning("publish_us_snapshot_after_pipeline: %s", ex)
        return _default_ssot(MODE_KR_STANDALONE)


def hydrate_kr_runtime_from_ssot(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    KR scan/daily 직전 — SSOT 로드만 (US 스캔 절대 호출 안 함).
    """
    try:
        from config_manager import load_system_config, save_system_config

        cfg = load_system_config() if sys_config is None else dict(sys_config)
        ssot = load_cross_market_ssot(cfg)
        # stale 고착 해제: KR hydrate 시점에 US 섹터가 비고 standalone이면 1회 재산출 시도
        if (
            str(ssot.get("mode") or "") == MODE_KR_STANDALONE
            and not str(ssot.get("us_sector_raw") or "").strip()
        ):
            try:
                republished = publish_us_market_snapshot(
                    cfg=cfg, source="kr_hydrate_republish", save=True
                )
                ssot = load_cross_market_ssot(cfg)
                ssot["republish_mode"] = republished.get("mode")
            except Exception as rex:
                logger.warning("hydrate_kr_runtime_from_ssot republish: %s", rex)
        cfg[CROSS_MARKET_SSOT_KEY] = ssot
        cfg["SPILLOVER_RUNTIME_MODE"] = ssot.get("mode")
        save_system_config(cfg)
        print(
            f"🌐 [CrossMarket] KR hydrate mode={ssot.get('mode')} "
            f"us_w={ssot.get('spillover_weight_us')} kr_w={ssot.get('spillover_weight_kr')}"
        )
        return ssot
    except Exception as ex:
        logger.warning("hydrate_kr_runtime_from_ssot: %s", ex)
        return _default_ssot(MODE_KR_STANDALONE)


def format_kr_spillover_telegram_line(sys_config: Optional[Dict[str, Any]] = None) -> str:
    """[7/9] 한미 스필오버 한 줄 — graceful degradation."""
    ssot = load_cross_market_ssot(sys_config)
    mode = str(ssot.get("mode") or MODE_KR_STANDALONE)

    if mode == MODE_KR_STANDALONE:
        age = ssot.get("age_hours")
        age_s = f"{age:.0f}h" if isinstance(age, (int, float)) else "—"
        return (
            "\n🌐 <b>한미 스필오버:</b> "
            "<i>US 오프라인</i> (스냅샷 없음/만료 "
            f"{age_s}) ➔ <b>KR 단독 모멘텀 100%</b> 적용\n"
        )

    us_raw = str(ssot.get("us_sector_raw") or "—")
    kr_std = str(ssot.get("kr_sector_std") or "—")
    targets = ssot.get("kr_play_targets") or []
    tgt_s = ", ".join(str(t) for t in targets[:4]) if targets else kr_std
    conf = ssot.get("confidence")
    conf_s = f"{float(conf):.0%}" if conf is not None else "—"
    asof = str(ssot.get("as_of_kst") or "")[:10]
    return (
        f"\n🌐 <b>한미 스필오버 연동:</b> 🇺🇸 <b>{us_raw}</b> "
        f"➔ 🇰🇷 <b>{kr_std}</b> ({tgt_s}) · 신뢰 {conf_s} · {asof}\n"
    )


def resolve_us_spillover_display_v2(cfg: Dict[str, Any]) -> str:
    """sector_spillover_refresh.resolve_us_spillover_display 대체·래핑."""
    ssot = load_cross_market_ssot(cfg)
    if ssot.get("mode") == MODE_KR_STANDALONE:
        return "US 오프라인 (KR 단독 모멘텀)"
    us_raw = str(ssot.get("us_sector_raw") or "").strip()
    if not is_valid_sector_label(us_raw):
        lg = str(cfg.get("US_SPILLOVER_SECTOR_LAST_GOOD") or "").strip()
        if is_valid_sector_label(lg):
            kr_std = str(build_cross_market_mapping_payload(lg).get("kr_sector_std") or "")
            return f"{lg} ➔ KR {kr_std}"
        return "US 오프라인 (KR 단독 모멘텀)"
    kr_std = str(ssot.get("kr_sector_std") or "")
    return f"{us_raw} ➔ KR {kr_std}" if kr_std else us_raw


def build_kr_spillover_prompt_block(sys_config: Optional[Dict[str, Any]] = None) -> str:
    """KR 스캐너·LLM FactPack용 (P3)."""
    ssot = load_cross_market_ssot(sys_config)
    return json.dumps(
        {
            "spillover_mode": ssot.get("mode"),
            "us_sector": ssot.get("us_sector_raw"),
            "kr_sector_std": ssot.get("kr_sector_std"),
            "kr_play_targets": ssot.get("kr_play_targets"),
            "weight_us": ssot.get("spillover_weight_us"),
            "weight_kr": ssot.get("spillover_weight_kr"),
        },
        ensure_ascii=False,
    )


def resolve_kr_spillover_target_std(sys_config: Optional[Dict[str, Any]] = None) -> str:
    """
    KR 가상매매·스캐너가 비교해야 할 표준 섹터 (US raw ≠ KR std 버그 방지).
    CROSS_MARKET_SSOT.kr_sector_std 우선, 없으면 us_kr_theme_bridge 매핑.
    """
    ssot = load_cross_market_ssot(sys_config)
    kr = str(ssot.get("kr_sector_std") or "").strip()
    if kr and kr != "기타/혼합" and is_valid_sector_label(kr):
        return kr
    cfg: Dict[str, Any]
    if isinstance(sys_config, dict):
        cfg = sys_config
    else:
        try:
            from config_manager import load_system_config

            cfg = load_system_config() or {}
        except Exception:
            cfg = {}
    us_raw = str(
        cfg.get("US_SPILLOVER_SECTOR") or cfg.get("US_SPILLOVER_SECTOR_LAST_GOOD") or ""
    ).strip()
    if is_valid_sector_label(us_raw):
        from us_kr_theme_bridge import map_us_sector_to_kr_std

        kr_std, _conf = map_us_sector_to_kr_std(us_raw)
        if kr_std and kr_std != "기타/혼합":
            return kr_std
    return ""


def kr_stock_matches_spillover(
    stock_sector: Any,
    sys_config: Optional[Dict[str, Any]] = None,
) -> bool:
    """KR 종목 섹터가 US→KR 스필오버 타깃(표준축·playbook)과 정렬되는지."""
    target = resolve_kr_spillover_target_std(sys_config)
    if not target:
        return False
    from sector_spillover_refresh import map_standard_sector

    std = map_standard_sector(stock_sector)
    if std == target:
        return True
    ssot = load_cross_market_ssot(sys_config)
    blob = f"{std} {stock_sector}".lower()
    for play in ssot.get("kr_play_targets") or []:
        p = str(play or "").strip()
        if len(p) >= 2 and p.lower() in blob:
            return True
    return False
