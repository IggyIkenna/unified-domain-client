"""Config schema and validation - re-exported from unified-config-interface.

Canonical implementation lives in unified-config-interface. This module
provides backward compatibility for callers importing from unified_domain_services.

Re-exports all config schema symbols from UCI so domain services and downstream
callers can import from unified_domain_services.schemas.config_schema if needed.
"""

from unified_config_interface import (
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
    ConfigValidationError,
    ConfigValidator,
    validate_config,
    validate_config_file,
)

__all__ = [
    "CLOB_VENUES",
    "CONFIG_SCHEMA",
    "DEX_VENUES",
    "INSTRUMENT_TYPE_FOLDER_MAP",
    "OPTIONAL_CONFIG_FIELDS",
    "REQUIRED_CONFIG_FIELDS",
    "VALID_ALGORITHMS",
    "VALID_INSTRUCTION_TYPES",
    "VENUE_CATEGORY_MAP",
    "ZERO_ALPHA_VENUES",
    "ConfigValidationError",
    "ConfigValidator",
    "validate_config",
    "validate_config_file",
]
