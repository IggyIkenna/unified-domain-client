"""Unit tests for InstrumentKey schema."""

import pytest
from unified_config_interface import InstrumentType, Venue

from unified_domain_services import InstrumentKey


class TestInstrumentKey:
    """Test InstrumentKey class."""

    def test_init_basic(self):
        """Test InstrumentKey initialization with basic fields."""
        key = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")

        assert key.venue == Venue.BINANCE_FUTURES
        assert key.instrument_type == InstrumentType.PERPETUAL
        assert key.symbol == "BTC-USDT"
        assert key.expiry is None
        assert key.option_type is None

    def test_init_with_expiry_and_option(self):
        """Test InstrumentKey initialization with expiry and option_type."""
        key = InstrumentKey(
            venue=Venue.DERIBIT,
            instrument_type=InstrumentType.OPTION,
            symbol="ETH-USDC",
            expiry="251027",
            option_type="C",
        )

        assert key.venue == Venue.DERIBIT
        assert key.instrument_type == InstrumentType.OPTION
        assert key.symbol == "ETH-USDC"
        assert key.expiry == "251027"
        assert key.option_type == "C"

    def test_str_basic(self):
        """Test string representation without optional fields."""
        key = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")

        result = str(key)
        assert result == "BINANCE-FUTURES:PERPETUAL:BTC-USDT"

    def test_str_with_expiry(self):
        """Test string representation with expiry."""
        key = InstrumentKey(
            venue=Venue.DERIBIT, instrument_type=InstrumentType.FUTURE, symbol="BTC-USD", expiry="251227"
        )

        result = str(key)
        assert result == "DERIBIT:FUTURE:BTC-USD:251227"

    def test_str_with_expiry_and_option(self):
        """Test string representation with expiry and option type."""
        key = InstrumentKey(
            venue=Venue.DERIBIT,
            instrument_type=InstrumentType.OPTION,
            symbol="ETH-USD",
            expiry="251027",
            option_type="P",
        )

        result = str(key)
        assert result == "DERIBIT:OPTION:ETH-USD:251027:P"

    def test_from_string_basic(self):
        """Test parsing from basic string format."""
        instrument_str = "BINANCE-FUTURES:PERPETUAL:BTC-USDT"
        key = InstrumentKey.from_string(instrument_str)

        assert key.venue == Venue.BINANCE_FUTURES
        assert key.instrument_type == InstrumentType.PERPETUAL
        assert key.symbol == "BTC-USDT"
        assert key.expiry is None
        assert key.option_type is None

    def test_from_string_with_expiry(self):
        """Test parsing from string with expiry."""
        instrument_str = "DERIBIT:FUTURE:BTC-USD:251227"
        key = InstrumentKey.from_string(instrument_str)

        assert key.venue == Venue.DERIBIT
        assert key.instrument_type == InstrumentType.FUTURE
        assert key.symbol == "BTC-USD"
        assert key.expiry == "251227"
        assert key.option_type is None

    def test_from_string_with_option(self):
        """Test parsing from string with option type."""
        instrument_str = "DERIBIT:OPTION:ETH-USD:251027:C"
        key = InstrumentKey.from_string(instrument_str)

        assert key.venue == Venue.DERIBIT
        assert key.instrument_type == InstrumentType.OPTION
        assert key.symbol == "ETH-USD"
        assert key.expiry == "251027"
        assert key.option_type == "C"

    def test_from_string_invalid_format(self):
        """Test parsing invalid string format raises error."""
        with pytest.raises(ValueError, match="Invalid instrument string format"):
            InstrumentKey.from_string("INVALID")

    def test_from_string_invalid_venue(self):
        """Test parsing invalid venue raises error."""
        with pytest.raises(ValueError, match="Invalid venue"):
            InstrumentKey.from_string("INVALID_VENUE:PERPETUAL:BTC-USDT")

    def test_from_string_invalid_instrument_type(self):
        """Test parsing invalid instrument type raises error."""
        with pytest.raises(ValueError, match="Invalid instrument type"):
            InstrumentKey.from_string("BINANCE-FUTURES:INVALID_TYPE:BTC-USDT")

    def test_equality(self):
        """Test InstrumentKey equality comparison."""
        key1 = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")
        key2 = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")
        key3 = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.SPOT_PAIR, symbol="BTC-USDT")

        assert key1 == key2
        assert key1 != key3

    def test_hash(self):
        """Test InstrumentKey can be used as dictionary key."""
        key1 = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")
        key2 = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")

        # Should be able to use as dict keys
        test_dict = {key1: "value1"}
        test_dict[key2] = "value2"

        # Should be the same key due to equality
        assert len(test_dict) == 1
        assert test_dict[key1] == "value2"

    def test_repr(self):
        """Test InstrumentKey representation."""
        key = InstrumentKey(venue=Venue.BINANCE_FUTURES, instrument_type=InstrumentType.PERPETUAL, symbol="BTC-USDT")

        repr_str = repr(key)
        assert "InstrumentKey" in repr_str
        assert "BINANCE-FUTURES:PERPETUAL:BTC-USDT" in repr_str
