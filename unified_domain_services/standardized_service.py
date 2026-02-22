"""
Standardized Domain Cloud Service

Re-exports from unified_cloud_services.domain.standardized_service.
DomainCloudOperations was removed from UCS; StandardizedDomainCloudService
now wraps UnifiedCloudService (cloud-agnostic).
"""

from unified_cloud_services.domain.standardized_service import (
    StandardizedDomainCloudService,
    create_domain_cloud_service,
)

__all__ = ["StandardizedDomainCloudService", "create_domain_cloud_service"]
