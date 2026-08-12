"""
BITGET_GMM_DNA_TEMPLATES → CRYPTO_DNA_ALPHA_RANK1..3 동기화.

signal_engines._doppelganger_adjustment 는 CRYPTO_DNA_ALPHA_RANK* (+ shape 20) 만 읽는다.
data_miner 는 BITGET_GMM_DNA_TEMPLATES 만 채우므로, 본 모듈이 두 SSOT 를 연결한다.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import OHLCV_SIGNAL_BAR_LIMIT
from bitget.infra.shared_db_connector import get_connection

import memory_bounds

logger = get_logger("bitget.evolution.gmm_dna_alpha_sync")

GMM_TO_DNA_FIELDS: Tuple[Tuple[str, str, str, str], ...] = (
    ("dyn_cpv", "dyn_cpv_min", "dyn_cpv_max", "cpv"),
    ("dyn_tb", "dyn_tb_min", "dyn_tb_max", "tb"),
    ("v_energy", "v_energy_min", "v_energy_max", "bbe"),
    ("dyn_rs", "dyn_rs_min", "dyn_rs_max", "rs"),
)

DEFAULT_NEUTRAL_SHAPE: List[float] = [round(0.45 + 0.01 * i, 4) for i in range(20)]


def _bounds_mid(cluster: dict, min_key: str, max_key: str, default: float = 0.0) -> float:
    try:
        lo = float(cluster.get(min_key))
        hi = float(cluster.get(max_key))
        return float((lo + hi) / 2.0)
    except (TypeError, ValueError):
        return float(default)


def compute_shape20_from_closes(close_arr) -> Optional[List[float]]:
    """signal_engines._calc_shape20 과 동일 규칙 (의존 순환 방지용 로컬 복제)."""
    if close_arr is None:
        return None
    c = np.asarray(close_arr, dtype=float)
    if c.size < 40:
        return None
    c = c[-300:] if c.size > 300 else c
    c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
    shape = np.array([np.mean(x) for x in np.array_split(c_norm, 20)], dtype=float)
    return [float(x) for x in shape]


def _ohlcv_table_name(market_type: str, symbol: str, timeframe: str) -> str:
    mkt = str(market_type).strip().lower()
    prefix = "BITGET_FUT" if mkt == "futures" else "BITGET_SPOT"
    sym = str(symbol).strip().upper()
    tf = str(timeframe).strip().upper()
    return f"{prefix}_{sym}_{tf}"


def load_shape20_from_db(
    db_path: str,
    market_type: str,
    symbol: str,
    timeframe: str,
) -> Optional[List[float]]:
    tbl = _ohlcv_table_name(market_type, symbol, timeframe)
    try:
        conn = get_connection(db_path, read_only=True)
        try:
            df = pd.read_sql(
                f'SELECT Close FROM "{tbl}"'
                f"{memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_SIGNAL_BAR_LIMIT)}",
                conn,
            )
        finally:
            conn.close()
        if df.empty:
            return None
        closes = pd.to_numeric(df["Close"], errors="coerce").dropna().values
        return compute_shape20_from_closes(closes)
    except Exception as exc:
        logger.debug("shape load skip %s: %s", tbl, exc)
        return None


def gmm_cluster_to_dna_template(cluster: dict, *, name: str) -> Optional[Dict[str, Any]]:
    if not isinstance(cluster, dict):
        return None
    has_bounds = any(min_k in cluster for _f, min_k, _max_k, _out in GMM_TO_DNA_FIELDS)
    if not has_bounds:
        return None

    dna: Dict[str, Any] = {
        "name": str(name),
        "source": "BITGET_GMM",
        "mean_mfe": cluster.get("mean_mfe"),
        "sample_size": cluster.get("sample_size"),
    }
    for _feat, min_k, max_k, out_k in GMM_TO_DNA_FIELDS:
        dna[out_k] = round(_bounds_mid(cluster, min_k, max_k), 6)

    shape = cluster.get("shape")
    if isinstance(shape, (list, tuple)) and len(shape) == 20:
        dna["shape"] = [float(x) for x in shape]
    else:
        dna["shape"] = list(DEFAULT_NEUTRAL_SHAPE)
    return dna


def rank_gmm_clusters(all_templates: dict, *, top_n: int = 3) -> List[Tuple[str, str, dict, float]]:
    ranked: List[Tuple[str, str, dict, float]] = []
    if not isinstance(all_templates, dict):
        return ranked
    for tf_key, tf_blob in all_templates.items():
        if not isinstance(tf_blob, dict):
            continue
        inner = tf_blob.get("templates")
        if not isinstance(inner, dict) or not inner:
            continue
        for cname, cluster in inner.items():
            if not isinstance(cluster, dict):
                continue
            try:
                mfe = float(cluster.get("mean_mfe") or 0.0)
            except (TypeError, ValueError):
                mfe = 0.0
            try:
                sample = int(cluster.get("sample_size") or 0)
            except (TypeError, ValueError):
                sample = 0
            if sample <= 0 and mfe <= 0.0:
                continue
            ranked.append((str(tf_key), str(cname), cluster, mfe))
    ranked.sort(key=lambda row: (-row[3], -int(row[2].get("sample_size") or 0), row[0], row[1]))
    return ranked[: max(1, int(top_n))]


def resolve_cluster_shape(cluster: dict, db_path: str) -> Tuple[List[float], str]:
    shape = cluster.get("shape")
    if isinstance(shape, (list, tuple)) and len(shape) == 20:
        src = str(cluster.get("shape_source") or "gmm_cluster_stored")
        return [float(x) for x in shape], src
    market = str(cluster.get("prototype_market") or "").strip().lower()
    symbol = str(cluster.get("prototype_symbol") or "").strip()
    tf = str(cluster.get("prototype_timeframe") or "1D").strip().upper()
    if market in ("spot", "futures") and symbol:
        loaded = load_shape20_from_db(db_path, market, symbol, tf)
        if loaded:
            return loaded, "prototype_ohlcv"
    return list(DEFAULT_NEUTRAL_SHAPE), "neutral_fallback"


def _should_skip_sync(cfg: dict, *, force: bool) -> Optional[str]:
    if force:
        return None
    gmm_at = str(cfg.get("BITGET_GMM_DNA_UPDATED_AT") or "").strip()
    sync_at = str(cfg.get("CRYPTO_DNA_ALPHA_SYNCED_AT") or "").strip()
    ranks_ok = all(
        isinstance(cfg.get(f"CRYPTO_DNA_ALPHA_RANK{i}"), dict)
        and cfg.get(f"CRYPTO_DNA_ALPHA_RANK{i}", {}).get("shape")
        for i in (1, 2, 3)
    )
    if ranks_ok and gmm_at and sync_at and sync_at >= gmm_at:
        return "stale_ok"
    return None


def sync_gmm_to_crypto_dna_alpha(
    cfg: dict,
    *,
    force: bool = False,
    db_path: Optional[str] = None,
    top_n: int = 3,
) -> Dict[str, Any]:
    """
    cfg 를 in-place 갱신. CRYPTO_DNA_ALPHA_RANK1..top_n 주입.
    manual source 랭크는 force=False 일 때 덮어쓰지 않음.
    """
    if not isinstance(cfg, dict):
        return {"ok": False, "updated": False, "error": "cfg_not_dict"}

    skip = _should_skip_sync(cfg, force=force)
    if skip:
        return {"ok": True, "updated": False, "skipped_reason": skip, "ranks": []}

    gmm = cfg.get("BITGET_GMM_DNA_TEMPLATES")
    if not isinstance(gmm, dict) or not gmm:
        return {"ok": False, "updated": False, "error": "no_gmm_templates", "ranks": []}

    ranked = rank_gmm_clusters(gmm, top_n=top_n)
    if not ranked:
        return {"ok": False, "updated": False, "error": "no_rankable_clusters", "ranks": []}

    db = str(db_path or market_data_db_path())
    written: List[str] = []
    for i, (tf_key, cname, cluster, mfe) in enumerate(ranked, start=1):
        key = f"CRYPTO_DNA_ALPHA_RANK{i}"
        existing = cfg.get(key)
        if (
            not force
            and isinstance(existing, dict)
            and str(existing.get("source") or "").lower() == "manual"
        ):
            logger.info("skip %s — manual source preserved", key)
            continue
        dna = gmm_cluster_to_dna_template(cluster, name=f"{tf_key}/{cname}")
        if not dna:
            continue
        shape_vals, shape_src = resolve_cluster_shape(cluster, db)
        dna["shape"] = shape_vals
        dna["shape_source"] = shape_src
        dna["synced_from"] = {
            "tf": tf_key,
            "cluster": cname,
            "mean_mfe": mfe,
            "gmm_updated_at": cfg.get("BITGET_GMM_DNA_UPDATED_AT"),
        }
        cfg[key] = dna
        written.append(key)

    if not written:
        return {"ok": False, "updated": False, "error": "nothing_written", "ranks": []}

    cfg["CRYPTO_DNA_ALPHA_SYNCED_AT"] = utc_datetime_str()
    cfg["CRYPTO_DNA_ALPHA_SYNC_SOURCE"] = "BITGET_GMM"
    logger.info(
        "GMM→CRYPTO_DNA_ALPHA sync: %s (top clusters=%s)",
        ", ".join(written),
        len(ranked),
    )
    return {"ok": True, "updated": True, "ranks": written, "n_clusters": len(ranked)}


def sync_gmm_dna_alpha_if_stale(*, force: bool = False) -> Dict[str, Any]:
    """config_bootstrap 훅 — SQLite config 로드·저장."""
    from bitget.config_hub import load_config, save_config_atomic
    from bitget.infra.config_manager import invalidate_runtime_system_config_cache

    cfg = load_config()
    result = sync_gmm_to_crypto_dna_alpha(cfg, force=force)
    if result.get("updated"):
        save_config_atomic(cfg)
        invalidate_runtime_system_config_cache()
    return result


if __name__ == "__main__":
    import json
    import sys

    force = "--force" in sys.argv
    print(json.dumps(sync_gmm_dna_alpha_if_stale(force=force), ensure_ascii=False, indent=2))


__all__ = [
    "compute_shape20_from_closes",
    "gmm_cluster_to_dna_template",
    "load_shape20_from_db",
    "rank_gmm_clusters",
    "resolve_cluster_shape",
    "sync_gmm_dna_alpha_if_stale",
    "sync_gmm_to_crypto_dna_alpha",
]
