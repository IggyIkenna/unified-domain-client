"""
InstrumentKey - Canonical instrument identification schema.

Domain schema for VENUE:INSTRUMENT_TYPE:SYMBOL format.
Uses Venue and InstrumentType from unified-config-interface.
"""

from dataclasses import dataclass

from unified_config_interface import InstrumentType, Venue

# Venue → Tardis exchange name mapping (Tardis API allowed values)
_VENUE_TO_TARDIS: dict[str, str] = {
    "BINANCE-SPOT": "binance",
    "BINANCE-FUTURES": "binance-futures",
    "DERIBIT": "deribit",
    "BYBIT": "bybit",
    "OKX": "okex",
    "OKX-FUTURES": "okex-futures",
    "UPBIT": "upbit",
    "COINBASE": "coinbase",
}


@dataclass
class InstrumentKey:
    """
    Instrument key following VENUE:INSTRUMENT_TYPE:SYMBOL format.

    Used for canonical instrument identification across all services.

    Examples:
        - BINANCE-FUTURES:PERPETUAL:BTC-USDT
        - DERIBIT:OPTION:ETH-USDC-251027-3500-CALL
        - CME:FUTURE:ES.FUT
    """

    venue: Venue
    instrument_type: InstrumentType
    symbol: str
    expiry: str | None = None  # For futures/options (YYMMDD format)
    option_type: str | None = None  # C or P for options

    def __str__(self) -> str:
        """Format: venue:type:symbol:expiry:option_type"""
        parts = [self.venue.value, self.instrument_type.value, self.symbol]
        if self.expiry:
            parts.append(self.expiry)
        if self.option_type:
            parts.append(self.option_type)
        return ":".join(parts)

    @classmethod
    def from_string(cls, instrument_key_str: str) -> "InstrumentKey":
        """Parse instrument key from string"""
        parts = instrument_key_str.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid instrument key format: {instrument_key_str}")

        venue = Venue(parts[0])
        instrument_type = InstrumentType(parts[1])
        symbol = parts[2]
        expiry = parts[3] if len(parts) > 3 else None
        option_type = parts[4] if len(parts) > 4 else None

        return cls(
            venue=venue,
            instrument_type=instrument_type,
            symbol=symbol,
            expiry=expiry,
            option_type=option_type,
        )

    @classmethod
    def _format_tardis_symbol(cls, symbol: str, tardis_exchange: str) -> str:
        """Convert canonical symbol to Tardis exchange-specific format."""
        if tardis_exchange in ("binance", "binance-futures"):
            return symbol.replace("-", "").lower()
        if tardis_exchange == "deribit":
            return symbol.lower()
        if tardis_exchange == "upbit":
            parts = symbol.split("-")
            return f"{parts[1]}-{parts[0]}" if len(parts) == 2 else symbol
        if tardis_exchange == "coinbase":
            return symbol.upper()
        return symbol.lower()

    @classmethod
    def parse_for_tardis(cls, instrument_key_str: str) -> dict[str, str]:
        """Parse instrument key and return venue/symbol for Tardis API.

        Converts VENUE:INSTRUMENT_TYPE:SYMBOL → venue + symbol for streaming architecture compatibility.

        Args:
            instrument_key_str: Instrument key in format VENUE:INSTRUMENT_TYPE:SYMBOL

        Returns:
            Dict with venue, symbol, tardis_exchange, tardis_symbol, instrument_type
        """
        parts = instrument_key_str.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid instrument key format: {instrument_key_str}")

        venue = parts[0]
        instrument_type = parts[1]
        symbol = ":".join(parts[2:])
        tardis_exchange = _VENUE_TO_TARDIS.get(venue, venue.lower())
        tardis_symbol = cls._format_tardis_symbol(symbol, tardis_exchange)

        return {
            "venue": venue,
            "symbol": symbol,
            "tardis_exchange": tardis_exchange,
            "tardis_symbol": tardis_symbol,
            "instrument_type": instrument_type,
        }
