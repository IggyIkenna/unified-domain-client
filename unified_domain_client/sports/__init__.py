"""Sports domain clients — typed access to sports betting datasets."""

from __future__ import annotations

from .features_client import (
    SportsFeaturesDomainClient,
)
from .fixtures_client import (
    SportsFixturesDomainClient,
)
from .mappings_client import (
    SportsMappingsDomainClient,
)
from .odds_client import SportsOddsDomainClient
from .tick_data_client import (
    SportsTickDataDomainClient,
)

__all__ = [
    "SportsFeaturesDomainClient",
    "SportsFixturesDomainClient",
    "SportsMappingsDomainClient",
    "SportsOddsDomainClient",
    "SportsTickDataDomainClient",
]
