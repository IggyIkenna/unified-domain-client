# unified-domain-services

Domain clients **canonical** library: InstrumentsDomainClient, ExecutionDomainClient, validation, and cloud service wrappers.

**Scope:** Domain clients are the single source of truth. **InstrumentsDomainClient** is the only canonical reader for instruments (see `unified-trading-codex/02-data/instruments-and-api-keys-standard.md`). Depends on UCS, UCI, UEI.

## Install

```bash
# From Artifact Registry (Cloud Build)
pip install unified-domain-services

# Local development
uv pip install -e ".[dev]"
```

## Dependencies

- unified-cloud-services>=1.5.0

## Usage

```python
from unified_domain_services import (
    InstrumentsDomainClient,
    ExecutionDomainClient,
    StandardizedDomainCloudService,
    create_market_data_cloud_service,
    validate_timestamp_date_alignment,
    DateValidator,
)
```

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
