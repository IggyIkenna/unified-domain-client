"""Config schema and validation - re-exported from unified-config-interface.

Canonical implementation lives in unified-config-interface. This module
provides legacy API compatibility for callers importing from unified_domain_client.

Re-exports all config schema symbols from UCI so domain services and downstream
callers can import from unified_domain_client.schemas.config_schema if needed.
"""

from unified_api_contracts import (
    CLOB_VENUES,
    CONFIG_SCHEMA,
    DEX_VENUES,
    INSTRUMENT_TYPE_FOLDER_MAP,
    OPTIONAL_CONFIG_FIELDS,
    REQUIRED_CONFIG_FIELDS,
    VALID_ALGORITHMS,
    VALID_INSTRUCTION_TYPES,
    VENUE_CATEGORY_MAP,
    ZERO_ALPHA_VENUES,
)
from unified_config_interface import (
    ConfigValidationError,
    ConfigValidator,
    validate_config,
    validate_config_file,
)

__all__ = [
    "CLOB_VENUES",
    "CONFIG_SCHEMA",
    "ConfigValidationError",
    "ConfigValidator",
    "DEX_VENUES",
    "INSTRUMENT_TYPE_FOLDER_MAP",
    "OPTIONAL_CONFIG_FIELDS",
    "REQUIRED_CONFIG_FIELDS",
    "VALID_ALGORITHMS",
    "VALID_INSTRUCTION_TYPES",
    "VENUE_CATEGORY_MAP",
    "ZERO_ALPHA_VENUES",
    "validate_config",
    "validate_config_file",
]
