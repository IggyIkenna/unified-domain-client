"""Lookback and timeframe constants for date validation.

Tier 2 compliance: Local definitions, no unified-trading-library dependency.
"""

# Timeframe to seconds mapping
TIMEFRAME_TO_SECONDS: dict[str, int] = {
    "15s": 15,
    "30s": 30,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "24h": 86400,
    "1d": 86400,
}

# Alias for compatibility
TIMEFRAME_SECONDS = TIMEFRAME_TO_SECONDS

# Feature group max lookback periods
FEATURE_GROUP_LOOKBACK: dict[str, int] = {
    "technical_indicators": 50,
    "moving_averages": 200,
    "oscillators": 14,
    "volatility_realized": 100,
    "momentum": 50,
    "volume_analysis": 120,
    "vwap": 48,
    "candlestick_patterns": 10,
    "market_structure": 200,
    "returns": 100,
    "round_numbers": 10,
    "streaks": 20,
    "microstructure": 50,
    "funding_oi": 48,
    "liquidations": 24,
    "temporal": 0,
    "economic_events": 0,
    "targets": 50,
}

# Max lookback days by timeframe (for date validation fallback)
MAX_LOOKBACK_DAYS_BY_TIMEFRAME: dict[str, int] = {
    "15s": 30,
    "1m": 30,
    "5m": 60,
    "15m": 90,
    "1h": 120,
    "4h": 150,
    "24h": 200,
}
