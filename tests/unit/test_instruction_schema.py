"""Unit tests for instruction schema validation."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from unified_domain_client.schemas.instruction_schema import (
    INSTRUCTION_SCHEMA,
    InstructionValidationError,
    InstructionValidator,
    get_instruction_pyarrow_schema,
    migrate_legacy_dataframe,
    validate_instruction_dataframe,
    validate_instruction_parquet,
)


def _minimal_valid_df() -> pd.DataFrame:
    """Create minimal valid instruction DataFrame."""
    return pd.DataFrame(
        {
            "timestamp": [1704067200000000000],
            "instruction_id": ["inst-001"],
            "instruction_type": ["TRADE"],
            "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
            "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
            "quantity": [0.1],
            "benchmark_price": [50000.0],
            "direction": [1],
        }
    )


class TestInstructionValidator:
    """Test InstructionValidator."""

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_rejects_non_dataframe(self, mock_validate: MagicMock):
        """Test validate rejects non-DataFrame input."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        result = validator.validate([1, 2, 3])
        assert result == ["Input must be a pandas DataFrame or pyarrow Table"]

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_rejects_missing_required_columns(self, mock_validate: MagicMock):
        """Test validate rejects missing required columns."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame({"timestamp": [1], "instruction_id": ["x"]})
        result = validator.validate(df)
        assert any("Missing required column" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_rejects_invalid_instruction_type(self, mock_validate: MagicMock):
        """Test validate rejects invalid instruction_type."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["instruction_type"] = "INVALID"
        result = validator.validate(df)
        assert any("Invalid instruction_type" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_rejects_zero_quantity(self, mock_validate: MagicMock):
        """Test validate rejects quantity <= 0."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["quantity"] = 0
        result = validator.validate(df)
        assert any("quantity" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_rejects_zero_benchmark_price(self, mock_validate: MagicMock):
        """Test validate rejects benchmark_price <= 0."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["benchmark_price"] = 0.0
        result = validator.validate(df)
        assert any("benchmark_price" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_accepts_valid_trade(self, mock_validate: MagicMock):
        """Test validate accepts valid TRADE instruction."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        result = validator.validate(df)
        assert result == []

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_accepts_pyarrow_table(self, mock_validate: MagicMock):
        """Test validate accepts pyarrow Table."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        table = pa.Table.from_pandas(df)
        result = validator.validate(table)
        assert result == []

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_validate_or_raise_raises_on_errors(self, mock_validate: MagicMock):
        """Test validate_or_raise raises InstructionValidationError."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame()
        with pytest.raises(InstructionValidationError) as exc_info:
            validator.validate_or_raise(df)
        assert len(exc_info.value.errors) > 0


class TestValidateInstructionDataframe:
    """Test validate_instruction_dataframe function."""

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_convenience_function(self, mock_validate: MagicMock):
        """Test validate_instruction_dataframe convenience function."""
        mock_validate.return_value = True
        df = _minimal_valid_df()
        result = validate_instruction_dataframe(df)
        assert result == []


class TestMigrateLegacyDataframe:
    """Test migrate_legacy_dataframe."""

    def test_migrates_price_to_price_cap(self):
        """Test price is migrated to price_cap."""
        df = pd.DataFrame(
            {
                "timestamp": [1],
                "instruction_id": ["i1"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["B:P:X"],
                "strategy_id": ["S1"],
                "quantity": [1.0],
                "benchmark_price": [100.0],
                "price": [99.0],
            }
        )
        result = migrate_legacy_dataframe(df)
        assert "price_cap" in result.columns
        assert result["price_cap"].iloc[0] == 99.0

    def test_accepts_pyarrow_table(self):
        """Test migrate_legacy_dataframe accepts pyarrow Table."""
        df = pd.DataFrame(
            {
                "timestamp": [1],
                "instruction_id": ["i1"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["B:P:X"],
                "strategy_id": ["S1"],
                "quantity": [1.0],
                "benchmark_price": [100.0],
            }
        )
        table = pa.Table.from_pandas(df)
        result = migrate_legacy_dataframe(table)
        assert isinstance(result, pd.DataFrame)
        assert "instruction_id" in result.columns


class TestSchemaExports:
    """Test schema exports."""

    def test_get_instruction_pyarrow_schema(self):
        """Test get_instruction_pyarrow_schema returns schema."""
        schema = get_instruction_pyarrow_schema()
        assert schema == INSTRUCTION_SCHEMA
        assert "timestamp" in schema.names
        assert "instruction_id" in schema.names


class TestValidateInstructionParquet:
    """Test validate_instruction_parquet."""

    def test_returns_error_for_missing_file(self):
        """Test validate_instruction_parquet returns error for missing file."""
        result = validate_instruction_parquet(Path("/nonexistent/path.parquet"))
        assert len(result) == 1
        assert "not found" in result[0].lower() or "exist" in result[0].lower()


class TestDeprecatedWithdrawRemoved:
    """Tests verifying WITHDRAW instruction type and signal_id are fully removed."""

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_withdraw_not_in_valid_instruction_types(self, mock_validate: MagicMock):
        """WITHDRAW must not appear in VALID_INSTRUCTION_TYPES after cleanup."""
        from unified_domain_client.schemas.instruction_schema import (
            VALID_INSTRUCTION_TYPES,
        )

        assert "WITHDRAW" not in VALID_INSTRUCTION_TYPES

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_withdraw_not_in_atomic_compatible_types(self, mock_validate: MagicMock):
        """WITHDRAW must not appear in ATOMIC_COMPATIBLE_TYPES after cleanup."""
        from unified_domain_client.schemas.instruction_schema import (
            ATOMIC_COMPATIBLE_TYPES,
        )

        assert "WITHDRAW" not in ATOMIC_COMPATIBLE_TYPES

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_withdraw_rejected_as_invalid_type(self, mock_validate: MagicMock):
        """Validator must reject WITHDRAW as an invalid instruction_type."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["WITHDRAW"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
            }
        )
        result = validator.validate(df)
        assert any("Invalid instruction_type" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_signal_id_no_longer_exported(self, mock_validate: MagicMock):
        """LEGACY_SIGNAL_ID_COLUMN must not be importable from instruction_schema."""
        import unified_domain_client.schemas.instruction_schema as schema_mod

        assert not hasattr(schema_mod, "LEGACY_SIGNAL_ID_COLUMN")

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_signal_id_column_fails_required_check(self, mock_validate: MagicMock):
        """DataFrame with only signal_id (no instruction_id) must fail required column check."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "signal_id": ["sig-001"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "direction": [1],
            }
        )
        result = validator.validate(df)
        assert any("instruction_id" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_unstake_is_valid_replacement_for_withdraw(self, mock_validate: MagicMock):
        """UNSTAKE must be accepted as the replacement for the removed WITHDRAW type."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["UNSTAKE"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
            }
        )
        result = validator.validate(df)
        assert not any("Invalid instruction_type" in e for e in result)


class TestInstructionValidatorDirectionValidation:
    """Test direction validation in detail."""

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_missing_direction_column_for_trade(self, mock_validate: MagicMock):
        """Test that missing direction column is flagged for TRADE instructions."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                # No direction column
            }
        )
        result = validator.validate(df)
        assert any("direction" in e.lower() for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_null_direction_for_trade(self, mock_validate: MagicMock):
        """Test that null direction is flagged for TRADE instructions."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "direction": [None],
            }
        )
        result = validator.validate(df)
        assert any("direction" in e.lower() for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_invalid_direction_zero_for_trade(self, mock_validate: MagicMock):
        """Test that direction=0 is rejected for TRADE instructions."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "direction": [0],
            }
        )
        result = validator.validate(df)
        assert any("direction" in e.lower() for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_direction_not_required_for_heartbeat(self, mock_validate: MagicMock):
        """Test that direction is not required for HEARTBEAT instructions."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["HEARTBEAT"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
            }
        )
        result = validator.validate(df)
        assert result == []


class TestInstructionValidatorAtomicValidation:
    """Test ATOMIC instruction validation."""

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_atomic_requires_nested_instructions_column(self, mock_validate: MagicMock):
        """Test that ATOMIC instructions require nested_instructions column."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["ATOMIC"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
            }
        )
        result = validator.validate(df)
        assert any("nested_instructions" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_atomic_empty_nested_instructions(self, mock_validate: MagicMock):
        """Test that ATOMIC with empty nested_instructions is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["ATOMIC"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "nested_instructions": [None],
            }
        )
        result = validator.validate(df)
        assert any("empty nested_instructions" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_atomic_valid_nested_instructions(self, mock_validate: MagicMock):
        """Test valid ATOMIC instruction with valid nested types."""
        import json

        mock_validate.return_value = True
        validator = InstructionValidator()
        nested = json.dumps([{"instruction_type": "SWAP", "quantity": 1.0}])
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["ATOMIC"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "nested_instructions": [nested],
            }
        )
        result = validator.validate(df)
        assert result == []

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_atomic_invalid_nested_type_trade(self, mock_validate: MagicMock):
        """Test ATOMIC with TRADE nested type is rejected."""
        import json

        mock_validate.return_value = True
        validator = InstructionValidator()
        # TRADE is not allowed in ATOMIC
        nested = json.dumps([{"instruction_type": "TRADE", "quantity": 1.0}])
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["ATOMIC"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "nested_instructions": [nested],
            }
        )
        result = validator.validate(df)
        assert any("ATOMIC" in e and "TRADE" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_atomic_invalid_json(self, mock_validate: MagicMock):
        """Test ATOMIC with invalid JSON nested_instructions."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["ATOMIC"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
                "nested_instructions": ["{not valid json}"],
            }
        )
        result = validator.validate(df)
        assert any("invalid JSON" in e for e in result)


class TestInstructionValidatorOptionalFields:
    """Test optional field validation."""

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_confidence_out_of_range(self, mock_validate: MagicMock):
        """Test that confidence outside [0, 1] is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["confidence"] = 1.5  # out of range
        result = validator.validate(df)
        assert any("confidence" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_urgency_out_of_range(self, mock_validate: MagicMock):
        """Test that urgency outside [0, 1] is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["urgency"] = -0.1  # out of range
        result = validator.validate(df)
        assert any("urgency" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_price_cap_less_than_price_floor(self, mock_validate: MagicMock):
        """Test that price_cap < price_floor is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["price_cap"] = 100.0
        df["price_floor"] = 200.0
        result = validator.validate(df)
        assert any("price_cap" in e and "price_floor" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_chain_id_without_chain_sequence(self, mock_validate: MagicMock):
        """Test that chain_id without chain_sequence is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["chain_id"] = "chain-001"
        df["chain_sequence"] = None
        result = validator.validate(df)
        assert any("chain_sequence" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_chain_sequence_negative(self, mock_validate: MagicMock):
        """Test that negative chain_sequence is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["chain_id"] = "chain-001"
        df["chain_sequence"] = -1
        result = validator.validate(df)
        assert any("chain_sequence" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_duplicate_instruction_ids_flagged(self, mock_validate: MagicMock):
        """Test that duplicate instruction_ids are flagged in strict mode."""
        mock_validate.return_value = True
        validator = InstructionValidator(strict=True)
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000, 1704067200000000000], dtype="int64"),
                "instruction_id": ["i1", "i1"],  # duplicate!
                "instruction_type": ["TRADE", "TRADE"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT", "BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1", "CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1, 0.1],
                "benchmark_price": [50000.0, 50000.0],
                "direction": [1, 1],
            }
        )
        result = validator.validate(df)
        assert any("duplicate" in e.lower() for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_timestamp_non_int_flagged(self, mock_validate: MagicMock):
        """Test that non-int64 timestamp is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["timestamp"] = df["timestamp"].astype(float)  # float instead of int
        result = validator.validate(df)
        assert any("timestamp" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_invalid_instrument_id_format(self, mock_validate: MagicMock):
        """Test that instrument_id without two colons is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["instrument_id"] = "BINANCE-BTC-USDT"  # missing colons
        result = validator.validate(df)
        assert any("instrument_id" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_invalid_strategy_id_format(self, mock_validate: MagicMock):
        """Test that invalid strategy_id format is flagged."""
        mock_validate.return_value = False  # simulate invalid format
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["strategy_id"] = "invalid_strategy_id"
        result = validator.validate(df)
        assert any("strategy_id" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_benchmark_price_negative(self, mock_validate: MagicMock):
        """Test that negative benchmark_price is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["benchmark_price"] = -100.0
        result = validator.validate(df)
        assert any("benchmark_price" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_benchmark_price_nan(self, mock_validate: MagicMock):
        """Test that NaN benchmark_price is flagged."""

        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["benchmark_price"] = float("nan")
        result = validator.validate(df)
        assert any("benchmark_price" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_null_instruction_id(self, mock_validate: MagicMock):
        """Test that null instruction_id is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["instruction_id"] = None
        result = validator.validate(df)
        assert any("instruction_id" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_null_instruction_type(self, mock_validate: MagicMock):
        """Test that null instruction_type is flagged."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = _minimal_valid_df()
        df["instruction_type"] = None
        result = validator.validate(df)
        assert any("instruction_type" in e for e in result)

    @patch("unified_domain_client.schemas.instruction_schema.validate_strategy_id")
    def test_withdraw_is_now_invalid_instruction_type(self, mock_validate: MagicMock):
        """Test that WITHDRAW is rejected as invalid after deprecation removal."""
        mock_validate.return_value = True
        validator = InstructionValidator()
        df = pd.DataFrame(
            {
                "timestamp": pd.array([1704067200000000000], dtype="int64"),
                "instruction_id": ["i1"],
                "instruction_type": ["WITHDRAW"],
                "instrument_id": ["BINANCE:PERPETUAL:BTC-USDT"],
                "strategy_id": ["CEFI_BTC_momentum_LIVE_1h_V1"],
                "quantity": [0.1],
                "benchmark_price": [50000.0],
            }
        )
        result = validator.validate(df)
        # WITHDRAW removed — must be rejected as invalid
        assert any("Invalid instruction_type" in e for e in result)
