"""
unified_domain_services — compatibility re-export alias for unified_domain_client.

This package provides the NEW canonical import path for all consumers. During the
rename transition (T3 STEP C: lib-phase2-rename-step2), all 14 services and T2 libs
will update their imports to use ``unified_domain_services`` instead of
``unified_domain_client``. Once the cascade is complete, the ``unified_domain_client``
package directory will be removed.

Usage (new canonical path):
    from unified_domain_services import InstrumentsDomainClient, PathRegistry

Old path (deprecated — do NOT add new imports using this):
    from unified_domain_client import InstrumentsDomainClient, PathRegistry

STEP 1 (this file): Add alias package — both paths work simultaneously.
STEP 2 (T3 STEP C):  Update all 14 services + T2 libs; remove unified_domain_client/.
"""

# Re-export the entire unified_domain_client public API under the new package name.
# Star-import is intentional here: this module IS the alias shim and must expose
# every symbol that unified_domain_client exposes.
from unified_domain_client import *  # noqa: F401, F403
from unified_domain_client import __all__ as __all__  # explicit re-export for type-checkers
from unified_domain_client import __version__  # noqa: F401
