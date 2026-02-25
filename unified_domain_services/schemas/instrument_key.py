"""
InstrumentKey - Canonical instrument identification schema.

Domain schema for VENUE:INSTRUMENT_TYPE:SYMBOL format.
Uses Venue and InstrumentType from unified-config-interface.
"""

from dataclasses import dataclass

from unified_config_interface import InstrumentType, Venue


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
    def parse_for_tardis(cls, instrument_key_str: str) -> dict[str, str]:
        """
        Parse instrument key and return venue/symbol for Tardis API and post-validation storage.

        Converts VENUE:INSTRUMENT_TYPE:SYMBOL → venue + symbol for streaming architecture compatibility.

        Args:
            instrument_key_str: Instrument key in format VENUE:INSTRUMENT_TYPE:SYMBOL

        Returns:
            Dict with venue, symbol, exchange (for Tardis), tardis_symbol
        """
        parts = instrument_key_str.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid instrument key format: {instrument_key_str}")

        venue = parts[0]  # BINANCE, DERIBIT, etc.
        instrument_type = parts[1]  # SPOT_PAIR, PERPETUAL, etc.
        symbol = ":".join(parts[2:])  # BTC-USDT, BTC-USD-50000-241225-CALL, etc.

        # Map venue to Tardis exchange (corrected mapping based on Tardis API allowed values)
        venue_to_tardis = {
            "BINANCE-SPOT": "binance",
            "BINANCE-FUTURES": "binance-futures",
            "DERIBIT": "deribit",
            "BYBIT": "bybit",
            "OKX": "okex",  # Corrected: okx → okex
            "OKX-FUTURES": "okex-futures",
            "UPBIT": "upbit",  # Korean exchange (spot only)
            "COINBASE": "coinbase",  # Coinbase (spot only)
        }

        tardis_exchange = venue_to_tardis.get(venue, venue.lower())

        # Convert symbol to proper Tardis format based on exchange
        if tardis_exchange in ["binance", "binance-futures"]:
            # Binance format: SOL-USDT → solusdt (lowercase, no dash)
            tardis_symbol_formatted = symbol.replace("-", "").lower()
        elif tardis_exchange == "deribit":
            # Deribit format: keep original but lowercase
            tardis_symbol_formatted = symbol.lower()
        elif tardis_exchange == "upbit":
            # Upbit format: Our canonical key has BASE-QUOTE (VET-KRW) but Tardis expects QUOTE-BASE (KRW-VET) uppercase
            symbol_parts = symbol.split("-")
            tardis_symbol_formatted = (
                f"{symbol_parts[1]}-{symbol_parts[0]}" if len(symbol_parts) == 2 else symbol
            )  # VET-KRW → KRW-VET
        elif tardis_exchange == "coinbase":
            # Coinbase format: Our canonical key has BASE-QUOTE (SOL-USD), Tardis expects same format uppercase
            tardis_symbol_formatted = symbol.upper()  # SOL-USD stays SOL-USD
        else:
            # Default: lowercase
            tardis_symbol_formatted = symbol.lower()

        return {
            "venue": venue,  # For post-validation storage (streaming compatible)
            "symbol": symbol,  # For post-validation storage (canonical format)
            "tardis_exchange": tardis_exchange,  # For Tardis API call
            "tardis_symbol": tardis_symbol_formatted,  # For Tardis API call (exchange-specific formatting)
            "instrument_type": instrument_type,
        }
