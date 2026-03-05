# unified-domain-client

Trading domain services library: clients, validation, cloud service wrappers, and `StandardizedDomainCloudService`.

## Install

```bash
# From Artifact Registry (Cloud Build)
pip install unified-domain-client

# Local development
uv pip install -e ".[dev]"
```

## Dependencies

- unified-trading-services>=1.5.0

## Usage

```python
from unified_domain_client import (
    StandardizedDomainCloudService,
    create_market_data_cloud_service,
    validate_timestamp_date_alignment,
    DateValidator,
)
```

## Sports Domain Clients

Sports domain clients are available in `unified_domain_client/sports/`:

- **SportsFeaturesDomainClient** — sports feature vectors
- **SportsFixturesDomainClient** — fixture and match data
- **SportsOddsDomainClient** — raw odds from bookmakers
- **SportsMappingsDomainClient** — cross-provider entity mappings
- **SportsTickDataDomainClient** — sports tick data access

Five `DataSetSpec` entries: `sports_features`, `sports_fixtures`, `sports_raw_odds`, `sports_mappings`, `sports_tick_data`.

## Quality Gates

```bash
bash scripts/quality-gates.sh
bash scripts/quality-gates.sh --no-fix
```

## Quick Merge

```bash
bash scripts/quickmerge.sh "commit message"
```

## Dependencies

See [docs/DEPENDENCIES.md](docs/DEPENDENCIES.md) for library deps, install order, and public API.

## Cloud Build

Push to `main` triggers Cloud Build: lint → build wheel → publish to Artifact Registry (`unified-libraries`).
