"""Sports domain clients — typed access to sports betting datasets."""

from __future__ import annotations

from unified_domain_client.sports.features_client import SportsFeaturesDomainClient
from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient
from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient
from unified_domain_client.sports.odds_client import SportsOddsDomainClient
from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient

__all__ = [
    "SportsFeaturesDomainClient",
    "SportsFixturesDomainClient",
    "SportsMappingsDomainClient",
    "SportsOddsDomainClient",
    "SportsTickDataDomainClient",
]
