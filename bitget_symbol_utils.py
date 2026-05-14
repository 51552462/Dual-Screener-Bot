def normalize_symbol(symbol):
    s = str(symbol or "").strip().upper()
    s = s.replace("_", "/")
    if ":" in s:
        s = s.split(":")[0]
    return s


def normalize_market_symbol(symbol, market_type="futures"):
    base = normalize_symbol(symbol)
    if market_type == "futures" and base.endswith("/USDT"):
        return f"{base}:USDT"
    return base


def normalize_table_symbol(symbol):
    return normalize_symbol(symbol).replace("/", "_").replace(":", "_")
