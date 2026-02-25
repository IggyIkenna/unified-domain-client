"""
Standardized Domain Cloud Service

Re-exports from unified_cloud_services.domain.standardized_service.
StandardizedDomainCloudService now uses cloud-agnostic clients directly,
removing the UnifiedCloudService dependency.
"""

from unified_cloud_services import StandardizedDomainCloudService


# Legacy factory function removed - use StandardizedDomainCloudService directly
def create_domain_cloud_service(domain: str):
    """DEPRECATED: Use StandardizedDomainCloudService(domain=domain) instead."""
    raise NotImplementedError(
        "create_domain_cloud_service has been removed. Use StandardizedDomainCloudService(domain=domain) instead."
    )


__all__ = ["StandardizedDomainCloudService", "create_domain_cloud_service"]
