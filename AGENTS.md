# AGENTS.md

## Setup

```bash
uv sync --extra dev
source .venv/bin/activate
```

## Quality Gates

```bash
bash scripts/quality-gates.sh
```

## Type Checking

```bash
timeout 120 basedpyright unified_domain_client/
```

## Key Entry Points

This is a **Python library** providing the cloud storage I/O layer — reads/writes GCS/S3 domain data. Provides venue adapters and domain ABCs.

## Notes

- Tier 3 library (arch_tier=3) — depends on T0 (UCI, UEI, UCLI) and T1 (UTL) libraries
- Requires GCP credentials: `gcloud auth application-default login`
- Initialize events with `from unified_events_interface import setup_events`
- arch_tier corrected from 2 to 3 on 2026-02-28 (canonical DAG)
