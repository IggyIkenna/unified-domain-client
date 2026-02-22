"""Test package imports and basic exports."""


def test_import_unified_domain_services():
    """Test that unified_domain_services can be imported (lightweight exports)."""
    import unified_domain_services

    # Lightweight exports - always available (no gcs_operations)
    assert hasattr(unified_domain_services, "validate_timestamp_date_alignment")
    assert hasattr(unified_domain_services, "DateFilterService")
    assert hasattr(unified_domain_services, "validate_config")
    assert hasattr(unified_domain_services, "InstrumentKey")

    # Heavy exports (clients, standardized_service) require gcs_operations - skip if unavailable
    try:
        _ = getattr(unified_domain_services, "StandardizedDomainCloudService")
        _ = getattr(unified_domain_services, "create_market_data_cloud_service")
    except ModuleNotFoundError:
        pass  # gcs_operations not in UCS - lazy load fails when accessed


def test_date_validator_import():
    """Test DateValidator and related exports."""
    from unified_domain_services import DateValidator, get_validator, should_skip_date

    assert DateValidator is not None
    assert callable(get_validator)
    assert callable(should_skip_date)
