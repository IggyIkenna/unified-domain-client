# unified-domain-client — Configuration

## Config

UDC inherits all cloud configuration from `unified-cloud-interface`. There is no UDC-specific config file. Domain clients read project IDs, bucket names, and region from UCI's bootstrap layer — all resolved at construction time via UCI factory functions or passed explicitly.

## Environment Variables

UDC itself does not read `os.environ` directly. All env var reading is delegated to UCI. The relevant variables for UDC usage are:

| Variable            | Source | Description                                        |
| ------------------- | ------ | -------------------------------------------------- |
| `CLOUD_PROVIDER`    | UCI    | `gcp`, `aws`, or `local` — selects storage backend |
| `GCP_PROJECT_ID`    | UCI    | GCP project ID (required for GCP)                  |
| `AWS_ACCOUNT_ID`    | UCI    | AWS account ID (required for AWS)                  |
| `GCS_REGION`        | UCI    | GCS region (default: `asia-northeast1`)            |
| `AWS_REGION`        | UCI    | AWS region (default: `us-east-1`)                  |
| `BIGQUERY_LOCATION` | UCI    | BigQuery dataset location                          |

## Bucket Naming

Domain clients resolve bucket names via UCI's `get_bucket_name()` convention. Override buckets per domain using UCI env vars:

| Domain              | Override Variable                | GCP Default Pattern                          |
| ------------------- | -------------------------------- | -------------------------------------------- |
| Instruments         | `INSTRUMENTS_GCS_BUCKET`         | `instruments-store-{category}-{project_id}`  |
| Market data         | `MARKET_DATA_GCS_BUCKET`         | `market-data-tick-{category}-{project_id}`   |
| Features delta-one  | `FEATURES_DELTA_ONE_GCS_BUCKET`  | `features-delta-one-{category}-{project_id}` |
| Features calendar   | `FEATURES_CALENDAR_GCS_BUCKET`   | `features-calendar-{project_id}`             |
| Features onchain    | `FEATURES_ONCHAIN_GCS_BUCKET`    | `features-onchain-{project_id}`              |
| Features volatility | `FEATURES_VOLATILITY_GCS_BUCKET` | `features-volatility-{project_id}`           |
| ML models           | `ML_MODELS_GCS_BUCKET`           | `ml-models-store-{project_id}`               |
| ML predictions      | `ML_PREDICTIONS_GCS_BUCKET`      | `ml-predictions-store-{project_id}`          |
| Strategy            | `STRATEGY_GCS_BUCKET`            | `strategy-store-{project_id}`                |
| Execution           | `EXECUTION_GCS_BUCKET`           | `execution-store-{project_id}`               |

Category suffixes (`CEFI`, `TRADFI`, `DEFI`) apply to instruments, market data, and features-delta-one buckets. Execution, strategy, calendar, onchain, and ML buckets use unified (no-category) naming.

## Passing Configuration to Domain Clients

Most domain clients accept optional `project_id`, `storage_bucket`, and `analytics_dataset` parameters. If omitted, they resolve from UCI env vars:

```python
from unified_domain_client import MarketCandleDomainClient

# Use env vars (recommended for deployed services)
client = MarketCandleDomainClient()

# Explicit override (useful in tests or multi-project scenarios)
client = MarketCandleDomainClient(
    project_id="my-project",
    storage_bucket="my-custom-bucket",
    analytics_dataset="trading_candles",
)
```

## CloudTarget Dataclass

`CloudTarget` is a dataclass for explicit configuration of a cloud target, used in some legacy clients and in service orchestration:

```python
from unified_domain_client.cloud_target import CloudTarget

target = CloudTarget(
    project_id="my-project",
    storage_bucket="instruments-store-cefi-my-project",
    analytics_dataset="instruments",
    region="us-central1",
    bigquery_location="asia-northeast1",
)
```

`project_id`, `storage_bucket`, and `analytics_dataset` are all required. `region` defaults to `us-central1`; `bigquery_location` defaults to `asia-northeast1`.

## Lookback Constants

Lookback windows are defined as constants (not env vars) in `lookback_constants.py`:

- `TIMEFRAME_SECONDS` — maps timeframe labels to seconds: `{"1min": 60, "5min": 300, "1h": 3600, "1d": 86400, ...}`
- `FEATURE_GROUP_LOOKBACK` — per feature group lookback in days (e.g. `"momentum": 90`, `"volatility": 252`)
- `MAX_LOOKBACK_DAYS_BY_TIMEFRAME` — max lookback per timeframe for sliding window calculations

## Validation Date Rules

`DateValidator` enforces earliest-available dates per instrument and venue. These are loaded from the instruments catalog at runtime — not from env vars. Override earliest dates by passing a custom `DateValidator` instance:

```python
from unified_domain_client import DateValidator

validator = DateValidator(
    earliest_dates={"BTC-USD:binance": "2020-01-01"},
)
```

## Local Development / Tests

Set `CLOUD_PROVIDER=local` and UDC's path/bucket construction still works, but all storage operations use the in-memory `LocalStorageProvider` from UCI. No cloud credentials required.

```bash
export CLOUD_PROVIDER=local
export GCP_PROJECT_ID=test-project  # Still required for bucket name construction
```
