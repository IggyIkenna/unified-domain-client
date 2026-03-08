"""T3 STEP B contract tests.

Covers three ic-* todos from phase2_library_tier_hardening:
  - ic-deprecated-withdraw-cleanup  (verified via instruction_schema tests too)
  - ic-trad-fi-datasource-tag        (DataSourceConstraint + InstrumentRecord)
  - ic-onchain-freshness-contract    (OnchainDataFreshnessConfig per chain)
"""

import pytest
from unified_internal_contracts import (
    AssetClass,
    DataSourceConstraint,
    InstrumentRecord,
    InstrumentType,
    OnchainDataFreshnessConfig,
)
from unified_internal_contracts.reference.onchain_freshness import CHAIN_FRESHNESS_DEFAULTS

# ---------------------------------------------------------------------------
# ic-trad-fi-datasource-tag
# ---------------------------------------------------------------------------


class TestDataSourceConstraintEnum:
    """DataSourceConstraint enum is exported and correct."""

    def test_databento_only_value(self) -> None:
        assert DataSourceConstraint.DATABENTO_ONLY == "DATABENTO_ONLY"

    def test_any_value(self) -> None:
        assert DataSourceConstraint.ANY == "ANY"

    def test_is_str_enum(self) -> None:
        from enum import StrEnum

        assert issubclass(DataSourceConstraint, StrEnum)


class TestInstrumentRecordDataSourceConstraint:
    """InstrumentRecord.data_source_constraint field and helper."""

    def _make_record(self, asset_class: AssetClass) -> InstrumentRecord:
        return InstrumentRecord(
            instrument_key="TEST:SPOT:AAPL",
            venue="NYSE",
            asset_class=asset_class,
            instrument_type=InstrumentType.SPOT,
        )

    def test_default_constraint_is_any_for_crypto(self) -> None:
        record = self._make_record(AssetClass.CRYPTO)
        assert record.data_source_constraint == DataSourceConstraint.ANY

    def test_tradfi_constraint_helper_equity(self) -> None:
        constraint = InstrumentRecord.tradfi_datasource_constraint(AssetClass.EQUITY)
        assert constraint == DataSourceConstraint.DATABENTO_ONLY

    def test_tradfi_constraint_helper_fx(self) -> None:
        constraint = InstrumentRecord.tradfi_datasource_constraint(AssetClass.FX)
        assert constraint == DataSourceConstraint.DATABENTO_ONLY

    def test_tradfi_constraint_helper_commodity(self) -> None:
        constraint = InstrumentRecord.tradfi_datasource_constraint(AssetClass.COMMODITY)
        assert constraint == DataSourceConstraint.DATABENTO_ONLY

    def test_tradfi_constraint_helper_fixed_income(self) -> None:
        constraint = InstrumentRecord.tradfi_datasource_constraint(AssetClass.FIXED_INCOME)
        assert constraint == DataSourceConstraint.DATABENTO_ONLY

    def test_tradfi_constraint_helper_crypto_is_any(self) -> None:
        constraint = InstrumentRecord.tradfi_datasource_constraint(AssetClass.CRYPTO)
        assert constraint == DataSourceConstraint.ANY

    def test_instrument_record_can_be_tagged_databento_only(self) -> None:
        """Adapters can explicitly set data_source_constraint to DATABENTO_ONLY."""
        record = InstrumentRecord(
            instrument_key="NYSE:SPOT:AAPL",
            venue="NYSE",
            asset_class=AssetClass.EQUITY,
            instrument_type=InstrumentType.SPOT,
            data_source_constraint=DataSourceConstraint.DATABENTO_ONLY,
        )
        assert record.data_source_constraint == DataSourceConstraint.DATABENTO_ONLY

    def test_all_tradfi_asset_classes_map_to_databento_only(self) -> None:
        tradfi_classes = [
            AssetClass.EQUITY,
            AssetClass.FX,
            AssetClass.COMMODITY,
            AssetClass.FIXED_INCOME,
        ]
        for ac in tradfi_classes:
            constraint = InstrumentRecord.tradfi_datasource_constraint(ac)
            assert constraint == DataSourceConstraint.DATABENTO_ONLY, (
                f"{ac} should be DATABENTO_ONLY"
            )


# ---------------------------------------------------------------------------
# ic-onchain-freshness-contract
# ---------------------------------------------------------------------------


class TestOnchainDataFreshnessConfig:
    """OnchainDataFreshnessConfig model is correct and usable."""

    def test_basic_construction(self) -> None:
        config = OnchainDataFreshnessConfig(
            chain_id="ethereum",
            max_age_seconds=60,
            block_time_seconds=12.0,
        )
        assert config.chain_id == "ethereum"
        assert config.max_age_seconds == 60
        assert config.block_time_seconds == 12.0

    def test_max_age_seconds_must_be_positive(self) -> None:
        with pytest.raises(Exception):
            OnchainDataFreshnessConfig(
                chain_id="ethereum",
                max_age_seconds=0,
                block_time_seconds=12.0,
            )

    def test_block_time_seconds_must_be_positive(self) -> None:
        with pytest.raises(Exception):
            OnchainDataFreshnessConfig(
                chain_id="ethereum",
                max_age_seconds=60,
                block_time_seconds=0.0,
            )

    def test_max_blocks_behind_property(self) -> None:
        config = OnchainDataFreshnessConfig(
            chain_id="ethereum",
            max_age_seconds=60,
            block_time_seconds=12.0,
        )
        assert config.max_blocks_behind == pytest.approx(5.0)

    def test_optional_warn_age_seconds_defaults_none(self) -> None:
        config = OnchainDataFreshnessConfig(
            chain_id="ethereum",
            max_age_seconds=60,
            block_time_seconds=12.0,
        )
        assert config.warn_age_seconds is None

    def test_optional_source_defaults_empty(self) -> None:
        config = OnchainDataFreshnessConfig(
            chain_id="ethereum",
            max_age_seconds=60,
            block_time_seconds=12.0,
        )
        assert config.source == ""

    def test_with_all_fields(self) -> None:
        config = OnchainDataFreshnessConfig(
            chain_id="arbitrum",
            max_age_seconds=10,
            block_time_seconds=0.25,
            source="alchemy",
            warn_age_seconds=5,
        )
        assert config.chain_id == "arbitrum"
        assert config.source == "alchemy"
        assert config.warn_age_seconds == 5

    def test_arbitrum_max_blocks_behind(self) -> None:
        config = OnchainDataFreshnessConfig(
            chain_id="arbitrum",
            max_age_seconds=10,
            block_time_seconds=0.25,
        )
        assert config.max_blocks_behind == pytest.approx(40.0)


class TestChainFreshnessDefaults:
    """CHAIN_FRESHNESS_DEFAULTS covers all expected chains."""

    def test_ethereum_is_present(self) -> None:
        assert "ethereum" in CHAIN_FRESHNESS_DEFAULTS

    def test_arbitrum_is_present(self) -> None:
        assert "arbitrum" in CHAIN_FRESHNESS_DEFAULTS

    def test_base_is_present(self) -> None:
        assert "base" in CHAIN_FRESHNESS_DEFAULTS

    def test_polygon_is_present(self) -> None:
        assert "polygon" in CHAIN_FRESHNESS_DEFAULTS

    def test_solana_is_present(self) -> None:
        assert "solana" in CHAIN_FRESHNESS_DEFAULTS

    def test_bsc_is_present(self) -> None:
        assert "bsc" in CHAIN_FRESHNESS_DEFAULTS

    def test_all_defaults_are_valid_configs(self) -> None:
        for chain_id, config in CHAIN_FRESHNESS_DEFAULTS.items():
            assert isinstance(config, OnchainDataFreshnessConfig), (
                f"{chain_id} default is not OnchainDataFreshnessConfig"
            )
            assert config.max_age_seconds > 0
            assert config.block_time_seconds > 0

    def test_ethereum_max_age_is_60s(self) -> None:
        cfg = CHAIN_FRESHNESS_DEFAULTS["ethereum"]
        assert cfg.max_age_seconds == 60

    def test_arbitrum_is_faster_than_ethereum(self) -> None:
        eth = CHAIN_FRESHNESS_DEFAULTS["ethereum"]
        arb = CHAIN_FRESHNESS_DEFAULTS["arbitrum"]
        assert arb.max_age_seconds < eth.max_age_seconds

    def test_all_defaults_have_warn_below_max(self) -> None:
        for chain_id, config in CHAIN_FRESHNESS_DEFAULTS.items():
            if config.warn_age_seconds is not None:
                assert config.warn_age_seconds < config.max_age_seconds, (
                    f"{chain_id}: warn_age_seconds must be < max_age_seconds"
                )
