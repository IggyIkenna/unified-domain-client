"""ID naming validation for strategy_id and config_id.

Tier 2 compliance: Local implementation, no unified-config-interface dependency.
Validates per unified-trading-codex conventions.
"""

import re

_CATEGORIES = frozenset(["CEFI", "TRADFI", "DEFI"])
_MODES = frozenset(["SCE", "HUF"])
_TIMEFRAMES = frozenset(["1M", "5M", "15M", "30M", "1H", "4H", "8H", "24H"])

_STRATEGY_ID_PATTERN = re.compile(r"^([A-Z]+)_([A-Z]+)_([a-z0-9-]+)_([A-Z]+)_(\d+[MH])_V(\d+)$")


def validate_strategy_id(strategy_id: str) -> bool:
    """Validate strategy_id format. Returns True if valid, False otherwise."""
    try:
        match = _STRATEGY_ID_PATTERN.match(strategy_id)
        if not match:
            return False
        category, mode, timeframe = match.group(1), match.group(4), match.group(5)
        return category in _CATEGORIES and mode in _MODES and timeframe.upper() in _TIMEFRAMES
    except (ValueError, IndexError):
        return False
