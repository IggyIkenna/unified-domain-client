"""Sports domain clients — typed access to sports betting datasets."""

from __future__ import annotations

from unified_domain_client.sports.features_client import (  # noqa: deep-import
    SportsFeaturesDomainClient,
)
from unified_domain_client.sports.fixtures_client import (  # noqa: deep-import
    SportsFixturesDomainClient,
)
from unified_domain_client.sports.mappings_client import (  # noqa: deep-import
    SportsMappingsDomainClient,
)
from unified_domain_client.sports.odds_client import SportsOddsDomainClient  # noqa: deep-import
from unified_domain_client.sports.tick_data_client import (  # noqa: deep-import
    SportsTickDataDomainClient,
)

__all__ = [
    "SportsFeaturesDomainClient",
    "SportsFixturesDomainClient",
    "SportsMappingsDomainClient",
    "SportsOddsDomainClient",
    "SportsTickDataDomainClient",
]
