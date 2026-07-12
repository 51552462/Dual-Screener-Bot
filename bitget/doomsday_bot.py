from bitget.config_hub import load_config, save_config
from bitget.infra.clock import utc_hm_key
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.network_retry import http_get

logger = get_logger("bitget.doomsday_bot")


def _fetch_global_crypto():
    url = "https://api.coingecko.com/api/v3/global"
    res = http_get(
        url,
        op="doomsday.coingecko.global",
        throttle_key="http.coingecko.global.doom",
        throttle_interval_sec=0.35,
        timeout=15.0,
        swallow=False,
    )
    data = res.json().get("data", {})
    return {
        "btc_dominance": float(data.get("market_cap_percentage", {}).get("btc", 0.0) or 0.0),
        "market_cap_change_24h": float(data.get("market_cap_change_percentage_24h_usd", 0.0) or 0.0),
    }


def _fetch_eth_btc_ratio():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    res = http_get(
        url,
        op="doomsday.coingecko.simple_price",
        throttle_key="http.coingecko.simple_price.doom",
        throttle_interval_sec=0.35,
        timeout=15.0,
        swallow=False,
    )
    j = res.json()
    btc = float(j.get("bitcoin", {}).get("usd", 0.0) or 0.0)
    eth = float(j.get("ethereum", {}).get("usd", 0.0) or 0.0)
    return (eth / btc) if btc > 0 else 0.0


def run_doomsday_radar():
    logger.info("[doomsday radar] scanning BTC dominance / alt weakness / mcap collapse")

    btc_dominance = 0.0
    market_cap_change_24h = 0.0
    eth_btc_ratio = 0.0
    try:
        g = _fetch_global_crypto()
        btc_dominance = float(g["btc_dominance"])
        market_cap_change_24h = float(g["market_cap_change_24h"])
    except Exception as e:
        log_exception(logger, "global crypto macro load failed: %s", e)
    try:
        eth_btc_ratio = float(_fetch_eth_btc_ratio())
    except Exception as e:
        log_exception(logger, "ETH/BTC ratio load failed: %s", e)

    # 코인 전용 리스크 신호
    dominance_spike = btc_dominance >= 58.0
    alt_weakness = eth_btc_ratio > 0 and eth_btc_ratio < 0.045
    market_cap_crash = market_cap_change_24h <= -6.0

    defcon_level = 5
    risk_factors = sum([dominance_spike, alt_weakness, market_cap_crash])
    if risk_factors == 1:
        defcon_level = 4
    elif risk_factors == 2:
        defcon_level = 2
    elif risk_factors >= 3:
        defcon_level = 1

    # Contagion score → shared doomsday_dampener (Kelly + live size mult)
    from bitget.trading.doomsday_gate import crypto_contagion_score, floor_score_for_defcon

    raw_score = crypto_contagion_score(btc_dominance, eth_btc_ratio, market_cap_change_24h)
    contagion_score = floor_score_for_defcon(defcon_level, raw_score)

    config = load_config()
    config["DOOMSDAY_DEFCON"] = {
        "level": defcon_level,
        "updated_at": utc_hm_key(),
        "signals": {
            "btc_dominance_spike": dominance_spike,
            "alt_weakness_eth_btc": alt_weakness,
            "crypto_market_cap_crash": market_cap_crash,
        },
        "metrics": {
            "btc_dominance": round(float(btc_dominance), 3),
            "eth_btc_ratio": round(float(eth_btc_ratio), 6),
            "market_cap_change_24h_usd_pct": round(float(market_cap_change_24h), 3),
        },
        "scores": {
            "Global_Contagion_Score": float(contagion_score),
            "raw_crypto_contagion": float(raw_score),
        },
    }
    save_config(config)

    logger.info(
        "doomsday radar complete: DEFCON %s contagion=%.1f",
        defcon_level,
        contagion_score,
    )
    if defcon_level <= 2:
        logger.warning(
            "[URGENT] DEFCON %s — new LONG entries blocked (no flatten); "
            "contagion=%.1f — reduce size / defensive mode",
            defcon_level,
            contagion_score,
        )


if __name__ == "__main__":
    run_doomsday_radar()
