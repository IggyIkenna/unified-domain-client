"""Domain validation schemas - config, instruction, instrument key."""

# pyright: reportUnknownVariableType=false
# Re-exports inherit types from source modules; instruction_schema has pyarrow/pandas types.

from unified_domain_services import (
    CLOB_VENUES,
    CONFIG_SCHEMA,
    DEX_VENUES,
    INSTRUCTION_COLUMNS,
    INSTRUCTION_SCHEMA,
    INSTRUMENT_TYPE_FOLDER_MAP,
    OPTIONAL_CONFIG_FIELDS,
    REQUIRED_CONFIG_FIELDS,
    VALID_ALGORITHMS,
    VALID_INSTRUCTION_TYPES,
    VENUE_CATEGORY_MAP,
    ZERO_ALPHA_VENUES,
    ConfigValidationError,
    ConfigValidator,
    InstructionValidationError,
    InstructionValidator,
    InstrumentKey,
    validate_config,
    validate_config_file,
    validate_instruction_dataframe,
    validate_instruction_parquet,
)

__all__ = [
    "CLOB_VENUES",
    "CONFIG_SCHEMA",
    "DEX_VENUES",
    "INSTRUCTION_COLUMNS",
    "INSTRUCTION_SCHEMA",
    "INSTRUMENT_TYPE_FOLDER_MAP",
    "OPTIONAL_CONFIG_FIELDS",
    "REQUIRED_CONFIG_FIELDS",
    "VALID_ALGORITHMS",
    "VALID_INSTRUCTION_TYPES",
    "VENUE_CATEGORY_MAP",
    "ZERO_ALPHA_VENUES",
    "ConfigValidationError",
    "ConfigValidator",
    "InstructionValidationError",
    "InstructionValidator",
    "InstrumentKey",
    "validate_config",
    "validate_config_file",
    "validate_instruction_dataframe",
    "validate_instruction_parquet",
]
