"""
Unit tests for unified_domain_client.instrument_date_filter module.

Tests DateFilterService, _parse_iso_datetime, _to_utc.
"""

from __future__ import annotations

import datetime as _dtmod
from datetime import UTC, datetime, timezone

from unified_domain_client import DateFilterService
from unified_domain_client.instrument_date_filter import (
    _parse_iso_datetime,
    _to_utc,
)


class TestInstrumentDateFilter:
    def setup_method(self):
        self.svc = DateFilterService()

    def test_get_protocol_default_date_known(self):
        result = self.svc.get_protocol_default_date("uniswap_v3")
        assert result == "2021-05-05T00:00:00Z"

    def test_get_protocol_default_date_unknown(self):
        result = self.svc.get_protocol_default_date("nonexistent_protocol")
        assert result is None

    def test_get_protocol_default_date_unknown_key(self):
        result = self.svc.get_protocol_default_date("uniswap_v3", key="available_to")
        assert result is None

    def test_set_protocol_default_date_new_protocol(self):
        self.svc.set_protocol_default_date("my_protocol", "available_from", "2024-01-01T00:00:00Z")
        result = self.svc.get_protocol_default_date("my_protocol")
        assert result == "2024-01-01T00:00:00Z"

    def test_set_protocol_default_date_existing_protocol(self):
        self.svc.set_protocol_default_date("uniswap_v3", "available_from", "2020-01-01T00:00:00Z")
        result = self.svc.get_protocol_default_date("uniswap_v3")
        assert result == "2020-01-01T00:00:00Z"

    def test_filter_instruments_no_available_from_no_protocol(self):
        instruments = {"inst1": {"name": "Token A"}}
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_instruments_available_from_future(self):
        instruments = {"inst1": {"available_from_datetime": "2030-01-01T00:00:00Z"}}
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" not in result

    def test_filter_instruments_available_to_past(self):
        instruments = {
            "inst1": {
                "available_from_datetime": "2020-01-01T00:00:00Z",
                "available_to_datetime": "2021-01-01T00:00:00Z",
            }
        }
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" not in result

    def test_filter_instruments_available_from_protocol_default(self):
        instruments = {"inst1": {"name": "Token A"}}
        target = datetime(2020, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target, protocol="uniswap_v3")
        assert "inst1" not in result

    def test_filter_instruments_protocol_default_passes(self):
        instruments = {"inst1": {"name": "Token A"}}
        target = datetime(2022, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target, protocol="uniswap_v3")
        assert "inst1" in result

    def test_filter_instruments_naive_datetime(self):
        instruments = {"inst1": {"name": "Token A"}}
        target = datetime(2023, 1, 1)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_instruments_empty_available_from(self):
        instruments = {"inst1": {"available_from_datetime": ""}}
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_with_available_to_empty_string(self):
        instruments = {
            "inst1": {
                "available_from_datetime": "2020-01-01T00:00:00Z",
                "available_to_datetime": "",
            }
        }
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_with_available_to_none_value(self):
        instruments = {
            "inst1": {
                "available_from_datetime": "2020-01-01T00:00:00Z",
                "available_to_datetime": None,
            }
        }
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_instruments_non_string_available_from(self):
        instruments = {"inst1": {"available_from_datetime": None}}
        target = datetime(2023, 1, 1, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_target_exactly_at_available_from(self):
        instruments = {"inst1": {"available_from_datetime": "2023-01-01T00:00:00Z"}}
        target = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_filter_target_exactly_at_available_to(self):
        instruments = {
            "inst1": {
                "available_from_datetime": "2020-01-01T00:00:00Z",
                "available_to_datetime": "2023-01-01T00:00:00Z",
            }
        }
        target = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        result = self.svc.filter_instruments_by_date(instruments, target)
        assert "inst1" in result

    def test_set_then_get_protocol_unknown_key(self):
        self.svc.set_protocol_default_date("curve", "available_to", "2030-01-01T00:00:00Z")
        result = self.svc.get_protocol_default_date("curve", "available_to")
        assert result == "2030-01-01T00:00:00Z"

    def test_filter_empty_instruments(self):
        result = self.svc.filter_instruments_by_date({}, datetime(2023, 1, 1, tzinfo=UTC))
        assert result == {}


class TestParseIsoDatetime:
    def test_parse_iso_datetime_with_z(self):
        result = _parse_iso_datetime("2023-01-01T00:00:00Z")
        assert result is not None
        assert result.tzinfo is not None

    def test_parse_iso_datetime_invalid(self):
        result = _parse_iso_datetime("not-a-date")
        assert result is None

    def test_parse_iso_datetime_none(self):
        result = _parse_iso_datetime(None)
        assert result is None

    def test_parse_iso_datetime_empty(self):
        result = _parse_iso_datetime("   ")
        assert result is None


class TestToUtc:
    def test_to_utc_naive(self):
        naive = datetime(2023, 6, 1)
        result = _to_utc(naive)
        assert result.tzinfo == UTC

    def test_to_utc_aware(self):
        aware = datetime(2023, 6, 1, tzinfo=timezone(_dtmod.timedelta(hours=5)))
        result = _to_utc(aware)
        assert result.tzinfo is not None
