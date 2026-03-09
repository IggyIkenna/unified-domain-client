# Moved to unified-config-interface. Re-exported for backward compatibility.
from unified_config_interface.id_conventions import (  # noqa: F401
    validate_config_id,
    validate_strategy_id,
)

__all__ = ["validate_config_id", "validate_strategy_id"]
