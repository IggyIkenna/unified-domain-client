"""PathRegistry for standardized cloud storage paths."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DataSetSpec:
    name: str
    bucket_template: str
    path_template: str
    partition_keys: list[str]
    file_template: str
    multi_file: bool = False
    extra_files: list[str] = field(default_factory=list)


PATH_REGISTRY: dict[str, DataSetSpec] = {
    "raw_tick_data": DataSetSpec(
        name="raw_tick_data",
        bucket_template="market-data-tick-{category}-{project_id}",
        path_template="raw_tick_data/by_date/day={date}/data_type={data_type}/instrument_type={instrument_type}/venue={venue}/",
        partition_keys=["date", "data_type", "instrument_type", "venue"],
        file_template="{instrument_key}.parquet",
    ),
    "processed_candles": DataSetSpec(
        name="processed_candles",
        bucket_template="market-data-tick-{category}-{project_id}",
        path_template="processed_candles/by_date/day={date}/timeframe={timeframe}/data_type={data_type}/instrument_type={instrument_type}/venue={venue}/",
        partition_keys=["date", "timeframe", "data_type", "instrument_type", "venue"],
        file_template="{instrument_id}.parquet",
    ),
    "instruments": DataSetSpec(
        name="instruments",
        bucket_template="instruments-store-{category}-{project_id}",
        path_template="instrument_availability/by_date/day={date}/venue={venue}/",
        partition_keys=["date", "venue"],
        file_template="instruments.parquet",
    ),
    "corporate_actions": DataSetSpec(
        name="corporate_actions",
        bucket_template="instruments-store-tradfi-{project_id}",
        path_template="corporate_actions/by_date/day={date}/",
        partition_keys=["date"],
        file_template="dividends.parquet",
        multi_file=True,
        extra_files=["splits.parquet", "earnings.parquet"],
    ),
    "delta_one_features": DataSetSpec(
        name="delta_one_features",
        bucket_template="features-delta-one-{category}-{project_id}",
        path_template="by_date/day={date}/feature_group={feature_group}/timeframe={timeframe}/",
        partition_keys=["date", "feature_group", "timeframe"],
        file_template="{instrument_id}.parquet",
    ),
    "calendar_features": DataSetSpec(
        name="calendar_features",
        bucket_template="features-calendar-{project_id}",
        path_template="calendar/{category}/by_date/day={date}/",
        partition_keys=["category", "date"],
        file_template="features.parquet",
    ),
    "onchain_features": DataSetSpec(
        name="onchain_features",
        bucket_template="features-onchain-{project_id}",
        path_template="by_date/day={date}/feature_group={feature_group}/",
        partition_keys=["date", "feature_group"],
        file_template="features.parquet",
    ),
    "volatility_features": DataSetSpec(
        name="volatility_features",
        bucket_template="features-volatility-{category}-{project_id}",
        path_template="by_date/day={date}/feature_group={feature_group}/",
        partition_keys=["date", "feature_group"],
        file_template="{underlying}.parquet",
    ),
    "ml_models": DataSetSpec(
        name="ml_models",
        bucket_template="ml-models-store-{project_id}",
        path_template="models/{model_id}/training-period={training_period}/",
        partition_keys=["model_id", "training_period"],
        file_template="model.joblib",
    ),
    "ml_model_metadata": DataSetSpec(
        name="ml_model_metadata",
        bucket_template="ml-models-store-{project_id}",
        path_template="model_registry/metadata/{model_id}/training-period={training_period}/",
        partition_keys=["model_id", "training_period"],
        file_template="metadata.json",
    ),
    "ml_predictions": DataSetSpec(
        name="ml_predictions",
        bucket_template="ml-predictions-store-{project_id}",
        path_template="predictions/by_date/day={date}/mode={mode}/",
        partition_keys=["date", "mode"],
        file_template="{event_id}.json",
        multi_file=True,
        extra_files=["batch_{timestamp}.parquet"],
    ),
    "ml_training_artifacts": DataSetSpec(
        name="ml_training_artifacts",
        bucket_template="ml-training-artifacts-{project_id}",
        path_template="stage1-preselection/model-{model_id}/training-period={training_period}/",
        partition_keys=["model_id", "training_period"],
        file_template="artifacts.tar.gz",
    ),
    "strategy_orders": DataSetSpec(
        name="strategy_orders",
        bucket_template="strategy-store-{project_id}",
        path_template="strategy_orders/by_date/day={date}/strategy_id={strategy_id}/",
        partition_keys=["date", "strategy_id"],
        file_template="orders.parquet",
    ),
    "strategy_instructions": DataSetSpec(
        name="strategy_instructions",
        bucket_template="strategy-store-{project_id}",
        path_template="strategy_instructions/strategy_id={strategy_id}/day={date}/",
        partition_keys=["strategy_id", "date"],
        file_template="instructions.parquet",
    ),
    "backtest_results": DataSetSpec(
        name="backtest_results",
        bucket_template="strategy-store-{project_id}",
        path_template="backtest_results/strategy_id={strategy_id}/run_id={run_id}/",
        partition_keys=["strategy_id", "run_id"],
        file_template="instructions.parquet",
        multi_file=True,
        extra_files=["positions.parquet", "pnl_attribution.parquet", "summary.json"],
    ),
    "execution_fills": DataSetSpec(
        name="execution_fills",
        bucket_template="execution-store-{category}-{project_id}",
        path_template="execution/by_date/day={date}/",
        partition_keys=["date"],
        file_template="fills.parquet",
        multi_file=True,
        extra_files=["orders.parquet"],
    ),
    "positions": DataSetSpec(
        name="positions",
        bucket_template="positions-store-{project_id}",
        path_template="by_date/day={date}/account={account_key}/snapshot_type={snapshot_type}/",
        partition_keys=["date", "account_key", "snapshot_type"],
        file_template="positions.parquet",
    ),
    "pnl_attribution": DataSetSpec(
        name="pnl_attribution",
        bucket_template="pnl-attribution-store-{project_id}",
        path_template="by_date/day={date}/strategy_id={strategy_id}/",
        partition_keys=["date", "strategy_id"],
        file_template="pnl_attribution.parquet",
    ),
    "risk_metrics": DataSetSpec(
        name="risk_metrics",
        bucket_template="risk-metrics-store-{project_id}",
        path_template="by_date/day={date}/risk_type={risk_type}/",
        partition_keys=["date", "risk_type"],
        file_template="risk_metrics.parquet",
    ),
    "nautilus_catalog": DataSetSpec(
        name="nautilus_catalog",
        bucket_template="execution-store-{category}-{project_id}",
        path_template="nautilus-catalog-cache/data/trade_tick/{instrument_id}/",
        partition_keys=["instrument_id"],
        file_template="data.parquet",
    ),
    "sports_features": DataSetSpec(
        name="sports_features",
        bucket_template="features-sports-{project_id}",
        path_template="features/horizon={horizon}/date={date}/league={league}/",
        partition_keys=["horizon", "date", "league"],
        file_template="features.parquet",
    ),
    "sports_fixtures": DataSetSpec(
        name="sports_fixtures",
        bucket_template="features-sports-{project_id}",
        path_template="fixtures/season={season}/league={league}/date={date}/",
        partition_keys=["season", "league", "date"],
        file_template="fixtures.parquet",
    ),
    "sports_raw_odds": DataSetSpec(
        name="sports_raw_odds",
        bucket_template="features-sports-{project_id}",
        path_template="raw_odds/provider={provider}/league={league}/date={date}/",
        partition_keys=["provider", "league", "date"],
        file_template="odds.parquet",
    ),
    "sports_mappings": DataSetSpec(
        name="sports_mappings",
        bucket_template="features-sports-{project_id}",
        path_template="mappings/entity_type={entity_type}/",
        partition_keys=["entity_type"],
        file_template="mappings.parquet",
    ),
    "sports_tick_data": DataSetSpec(
        name="sports_tick_data",
        bucket_template="market-tick-data-{project_id}",
        path_template="sports/venue={venue}/date={date}/",
        partition_keys=["venue", "date"],
        file_template="ticks.parquet",
    ),
    "l2_book_checkpoints": DataSetSpec(
        name="l2_book_checkpoints",
        bucket_template="market-data-tick-{category}-{project_id}",
        path_template="l2_book_checkpoints/by_date/day={date}/venue={venue}/",
        partition_keys=["date", "venue"],
        file_template="instrument_key={instrument_key}.parquet",
    ),
    "liquidation_clusters": DataSetSpec(
        name="liquidation_clusters",
        bucket_template="market-data-tick-{category}-{project_id}",
        path_template="liquidation_clusters/by_date/day={date}/source={source}/venue={venue}/",
        partition_keys=["date", "source", "venue"],
        file_template="instrument_key={instrument_key}.parquet",
    ),
    "liquidity_features_1m": DataSetSpec(
        name="liquidity_features_1m",
        bucket_template="market-data-features-{category}-{project_id}",
        path_template="liquidity_features_1m/by_date/day={date}/venue={venue}/",
        partition_keys=["date", "venue"],
        file_template="instrument_key={instrument_key}.parquet",
    ),
}


def get_spec(name: str) -> DataSetSpec:
    if name not in PATH_REGISTRY:
        raise KeyError(f"Dataset '{name}' not in PATH_REGISTRY. Known: {sorted(PATH_REGISTRY)}")
    return PATH_REGISTRY[name]


def build_bucket(name: str, *, project_id: str, category: str = "") -> str:
    spec = get_spec(name)
    return spec.bucket_template.format(project_id=project_id, category=category)


def build_path(name: str, **partition_values: str) -> str:
    spec = get_spec(name)
    return spec.path_template.format(**partition_values)


def build_full_uri(
    name: str, *, project_id: str, category: str = "", **partition_values: str
) -> str:
    bucket = build_bucket(name, project_id=project_id, category=category)
    path = build_path(name, **partition_values)
    return f"gs://{bucket}/{path}"


class PathRegistry:
    """Central registry of GCS path patterns for all datasets in the system.

    Thin wrapper around PATH_REGISTRY for backward compatibility.
    Prefer using PATH_REGISTRY, get_spec(), build_bucket(), build_path(), build_full_uri() directly.
    """

    # Legacy path template strings for backward-compatible readers/writers.
    # Used with PathRegistry.format() which substitutes {instrument}, {date}, {timeframe}.
    MARKET_TICK_RAW: str = "raw_tick_data/by_date/day={date}/instrument={instrument}.parquet"
    MARKET_CANDLE_1M: str = (
        "processed_candles/by_date/day={date}/timeframe=1m/instrument={instrument}.parquet"
    )
    MARKET_CANDLE_1H: str = (
        "processed_candles/by_date/day={date}/timeframe=1h/instrument={instrument}.parquet"
    )
    MARKET_CANDLE_24H: str = (
        "processed_candles/by_date/day={date}/timeframe=24h/instrument={instrument}.parquet"
    )
    MARKET_CANDLES: str = (
        "processed_candles/by_date/day={date}/timeframe={timeframe}/instrument={instrument}.parquet"
    )
    FEATURES_DELTA_ONE: str = "delta_one_features/by_date/day={date}/timeframe={timeframe}/instrument={instrument}.parquet"  # noqa: E501
    ML_PREDICTIONS: str = (
        "ml_predictions/by_date/day={date}/timeframe={timeframe}/instrument={instrument}.parquet"
    )

    @classmethod
    def all_patterns(cls) -> dict[str, DataSetSpec]:
        return dict(PATH_REGISTRY)

    @classmethod
    def format(cls, pattern: str, **kwargs: str) -> str:
        """Format a path pattern string with the given keyword arguments.

        ``pattern`` is treated as a raw format string (not a PATH_REGISTRY key).
        This method exists for backward compatibility with readers/writers that
        store path template strings in the class-level constants above.
        """
        return pattern.format(**kwargs)
