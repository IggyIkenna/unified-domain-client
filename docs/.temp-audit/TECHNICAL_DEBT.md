# Technical Debt Inventory - Unified Domain Services

*Generated: 2026-02-25*

## Executive Summary

This technical debt audit of `unified-domain-services` identifies areas for improvement across lazy imports, backwards compatibility patterns, code complexity, and maintenance burden. The codebase shows good overall structure but contains several areas of technical debt that should be addressed.

## 1. Lazy Imports Analysis

### ✅ Approved Patterns
**Location**: `/unified_domain_services/__init__.py`
- **Status**: COMPLIANT - All lazy imports are for performance optimization, not optional dependencies
- **Pattern**: Uses `__getattr__` for client-heavy modules (`clients.py`, `cloud_data_provider.py`, `factories.py`)
- **Justification**: These modules import heavy dependencies (pandas, storage clients) that shouldn't load on import

### 🔍 Findings
| Location | Lines | Pattern | Severity | Notes |
|----------|-------|---------|----------|--------|
| `__init__.py:79-144` | 66 lines | Complex `__getattr__` implementation | Medium | Multiple conditional branches, could be simplified |
| `__init__.py:54-76` | 23 lines | Large `_LAZY_NAMES` set | Low | Well-documented but may indicate too many lazy imports |

## 2. Backwards Compatibility Patterns

### 🚨 High Technical Debt

| Issue Type | Location | Description | Severity | Remediation |
|------------|----------|-------------|----------|-------------|
| Deprecated Functions | `factories.py:24-56` | 5 deprecated factory functions that raise `NotImplementedError` | High | **Remove entirely** - These are dead code |
| Deprecated Client | `clients.py:1015-1029` | `MarketDataDomainClient` marked deprecated | High | **Remove in next major version** |
| Legacy Signal ID | `schemas/instruction_schema.py:228-319` | Extensive backwards compatibility for `signal_id` → `instruction_id` | Medium | **Phase out after migration period** |
| Deprecated Instructions | `schemas/instruction_schema.py:59-60` | `DEPRECATED_INSTRUCTION_TYPES = ["WITHDRAW"]` | Medium | **Remove deprecated types** |

### 📋 Backwards Compatibility Debt Details

#### 2.1 Factory Functions (`factories.py`)
```python
# Lines 24-56: Dead code - all factory functions raise NotImplementedError
def create_market_data_cloud_service():
    """DEPRECATED: Use StandardizedDomainCloudService(domain='market_data') instead."""
    raise NotImplementedError(...)
```
**Impact**: 33 lines of dead code, confusing API surface
**Remediation**: Delete entire file and remove from `__init__.py` exports

#### 2.2 Legacy Column Migration (`instruction_schema.py`)
```python
# Lines 305-319: Complex legacy signal_id handling
if not has_instruction_id and has_legacy_signal_id:
    if self.allow_legacy_signal_id:
        df = df.rename(columns={LEGACY_SIGNAL_ID_COLUMN: "instruction_id"})
```
**Impact**: Adds complexity to validation pipeline
**Remediation**: Set migration deadline, then remove compatibility layer

#### 2.3 Deprecated Client (`clients.py`)
```python
# Lines 1015-1029: Deprecated MarketDataDomainClient
class MarketDataDomainClient(MarketCandleDataDomainClient):
    """⚠️ DEPRECATED: Use MarketCandleDataDomainClient or MarketTickDataDomainClient instead."""
```
**Impact**: Maintains inheritance for backwards compatibility
**Remediation**: Remove in next major version with migration guide

## 3. Empty Fallbacks Analysis

### ✅ Clean - No Issues Found
- No `.get("key", "")` patterns found for required configuration
- No empty `except:` blocks found
- Error handling follows proper patterns with specific exception types

## 4. Circular Import Workarounds

### 🔍 Acceptable Patterns Found
| Location | Pattern | Justification | Severity |
|----------|---------|---------------|----------|
| `__init__.py:84-144` | Imports inside `__getattr__` | Lazy loading for performance | Low |
| `schemas/instruction_schema.py:476-481` | Optional import in try/except | Optional dependency graceful degradation | Low |
| `schemas/instruction_schema.py:25,31` | TYPE_CHECKING import | Standard typing pattern | Low |

## 5. Re-exports and Indirection

### 🔍 Findings
| Issue Type | Location | Description | Severity | Remediation |
|------------|----------|-------------|----------|-------------|
| Cross-package Re-export | `__init__.py:9-18` | Re-exports from `unified_cloud_services.domain` | Low | **Acceptable** - Provides unified API |
| Standardized Service Re-export | `standardized_service.py:9-11` | Simple re-export wrapper | Low | **Consider removing** - Minimal value added |
| Complex Schema Re-exports | `schemas/__init__.py` | 29 re-exported symbols | Medium | **Simplify** - Too many exports |

## 6. Code Complexity Issues

### 🚨 Large Files (>900 lines)
| File | Lines | Classes | Functions | Severity | Remediation |
|------|-------|---------|-----------|----------|-------------|
| `clients.py` | 1,387 | 6 clients + 1 deprecated | 17 functions | **High** | **Split into separate client modules** |

### ⚠️ Large Functions (>100 lines)
| Function | File | Lines | Severity | Remediation |
|----------|------|-------|----------|-------------|
| `get_tick_data()` | `clients.py:856-969` | 113 | **High** | **Extract query logic and error handling** |

### 📊 File Size Analysis
```
clients.py:                1,387 lines ⚠️  EXCEEDS 900-line threshold
instruction_schema.py:        637 lines ✓  Within limits  
validation.py:               512 lines ✓  Within limits
cloud_data_provider.py:      464 lines ✓  Within limits
```

## 7. Technical Debt Priorities

### 🔥 Critical (Address Immediately)
1. **Remove Dead Code**: Delete `factories.py` entirely - 5 functions that only raise errors
2. **Split Large File**: Break down `clients.py` (1,387 lines) into separate modules per client type

### ⚠️ High Priority (Next Sprint)  
3. **Simplify Large Function**: Refactor `get_tick_data()` (113 lines) into smaller functions
4. **Remove Deprecated Client**: Eliminate `MarketDataDomainClient` with proper migration notice
5. **Set Migration Deadline**: Plan removal of legacy `signal_id` compatibility (currently ~91 lines of compatibility code)

### 📋 Medium Priority (Next Quarter)
6. **Simplify Lazy Loading**: Refactor `__getattr__` into smaller, more focused functions
7. **Reduce Schema Exports**: Simplify `schemas/__init__.py` to essential exports only
8. **Plan Deprecation Removal**: Set timeline for removing `DEPRECATED_INSTRUCTION_TYPES`

### 🔍 Low Priority (Monitoring)
9. **Optional Import Pattern**: Current try/except import in `instruction_schema.py` is acceptable but monitor usage
10. **Re-export Strategy**: Evaluate if `standardized_service.py` wrapper adds value

## 8. Remediation Plan

### Phase 1: Dead Code Removal (1 week)
- [ ] Delete `unified_domain_services/factories.py`
- [ ] Remove factory function exports from `__init__.py`
- [ ] Update documentation to use `StandardizedDomainCloudService` directly
- [ ] Verify no internal usage of deprecated factories

### Phase 2: File Size Reduction (2 weeks)
- [ ] Split `clients.py` into separate modules:
  - `clients/instruments.py` → `InstrumentsDomainClient`
  - `clients/market_data.py` → `MarketCandleDataDomainClient`, `MarketTickDataDomainClient`
  - `clients/execution.py` → `ExecutionDomainClient`  
  - `clients/features.py` → `FeaturesDomainClient`
  - `clients/__init__.py` → factory functions and re-exports
- [ ] Refactor `get_tick_data()` function:
  - Extract path building logic
  - Extract query parameter validation
  - Extract data loading and concatenation

### Phase 3: Deprecation Cleanup (1 month)
- [ ] Remove `MarketDataDomainClient` (breaking change)
- [ ] Provide migration guide for users
- [ ] Set firm deadline for legacy `signal_id` support removal
- [ ] Plan communication strategy for deprecated instruction types

### Phase 4: Architecture Cleanup (3 months)
- [ ] Simplify lazy loading patterns
- [ ] Consolidate schema exports  
- [ ] Evaluate re-export strategy across modules
- [ ] Add complexity monitoring to CI/CD

## 9. Monitoring and Prevention

### Automated Checks
- **File Size**: Fail CI if any file exceeds 900 lines
- **Function Size**: Warn on functions over 50 lines, fail over 100 lines  
- **Deprecation Tracking**: Automated scanning for deprecated patterns
- **Import Analysis**: Monitor for new circular dependencies

### Code Quality Gates
- **Complexity Metrics**: Track cyclomatic complexity per function
- **Dead Code Detection**: Regular scans for unreachable code
- **Backwards Compatibility**: Versioned compatibility matrix

---

## Summary Metrics

| Metric | Count | Threshold | Status |
|--------|-------|-----------|---------|
| Files > 900 lines | 1 | 0 | ❌ EXCEEDS |
| Functions > 100 lines | 1 | 0 | ❌ EXCEEDS |  
| Deprecated functions | 6 | 0 | ❌ EXCEEDS |
| TODO/FIXME comments | 0 | <5 | ✅ CLEAN |
| Empty except blocks | 0 | 0 | ✅ CLEAN |
| Circular import workarounds | 3 | <5 | ✅ ACCEPTABLE |

**Overall Technical Debt Score: MEDIUM** 
Primary issues are file size and deprecated code rather than architectural problems.