"""
InstrumentKey — re-exported from unified_internal_contracts (UIC SSOT).

UIC owns the canonical InstrumentKey definition (str-typed fields,
parse_for_tardis included). This module re-exports it so that existing
imports via ``unified_domain_client.schemas.instrument_key`` continue
to work without changes.

The ``Venue`` and ``InstrumentType`` enum imports from
``unified_config_interface`` are retained for callers that need
enum-typed validation alongside InstrumentKey.
"""

from unified_config_interface import InstrumentType, Venue
from unified_internal_contracts import InstrumentKey

__all__ = ["InstrumentKey", "InstrumentType", "Venue"]
