"""Unit tests for instruction schema validation."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from unified_domain_client.schemas.instruction_schema import (
    INSTRUCTION_SCHEMA,
    LEGACY_SIGNAL_ID_COLUMN,
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

    def test_migrates_signal_id_to_instruction_id(self):
        """Test signal_id is renamed to instruction_id."""
        df = pd.DataFrame(
            {
                "timestamp": [1],
                LEGACY_SIGNAL_ID_COLUMN: ["sig-1"],
                "instruction_type": ["TRADE"],
                "instrument_id": ["B:P:X"],
                "strategy_id": ["S1"],
                "quantity": [1.0],
                "benchmark_price": [100.0],
            }
        )
        result = migrate_legacy_dataframe(df)
        assert "instruction_id" in result.columns
        assert LEGACY_SIGNAL_ID_COLUMN not in result.columns
        assert result["instruction_id"].iloc[0] == "sig-1"

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
