# Library Production Hardening Prompt Template

**Purpose:** Comprehensive audit and hardening prompt for central shared libraries in the unified trading system.

**Target Libraries:** unified-cloud-services, unified-config-interface, unified-events-interface, unified-domain-services, unified-market-interface, unified-trade-execution-interface, unified-ml-interface, execution-algo-library

---

## PROMPT TEMPLATE (Replace `unified-domain-services` with actual library)

**New library:** If this is a new library, document that and any onboarding steps.


```
You are an AI agent whose sole responsibility is to audit and harden the **unified-domain-services** repository to production-ready standards with 70%+ test coverage.

---

## CRITICAL CONTEXT: This is a Central Shared Library

**unified-domain-services** is a CENTRAL LIBRARY used by 10-30+ downstream services/repos. Every function, class, and pattern you implement MUST be:
- ✅ Used by at least one downstream consumer (no dead code)
- ✅ Used consistently across ALL consumers (no version A in service X, version B in service Y)
- ✅ Enforced via unit tests that validate proper usage patterns
- ✅ Documented with clear examples and type hints
- ✅ Published to Google Artifact Registry (versioned releases)

**If functionality exists in unified-domain-services that NO service uses → Flag for my approval (remove or mandate adoption)**
**If functionality exists in MULTIPLE libraries → Flag for consolidation (one canonical implementation)**
**If services implement their own version instead of using unified-domain-services → Flag as violation (must use library)**

---

## YOUR MISSION

### Phase 1: Comprehensive Audit (Start Here)

Run a strict audit of unified-domain-services according to:
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/AUDIT_INSTRUCTIONS.md`
- All cursor rules in `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/.cursor/rules/`
- All coding standards in `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/unified-trading-codex/06-coding-standards/`

**Audit Dimensions:**

1. **Downstream Adoption Audit**
   - For EVERY exported function/class in unified-domain-services:
     - [ ] Is it used by at least one downstream service?
     - [ ] Which services use it? (list them)
     - [ ] Are there services that SHOULD use it but don't? (flag violations)
   - **Output:** `ADOPTION_AUDIT.md` with usage matrix

2. **Implementation Consistency Audit**
   - Check if services implement their own versions of unified-domain-services functionality:
     - [ ] Search for duplicate implementations (e.g., custom config loaders when UCI exists)
     - [ ] Search for different patterns (e.g., some use `get_storage_client()`, others use `storage.Client()`)
     - [ ] Search for outdated patterns (e.g., `os.getenv()` instead of config classes)
   - **Output:** `CONSISTENCY_VIOLATIONS.md` with service-by-service breakdown

3. **Dead Code Audit**
   - Identify unused exports in unified-domain-services:
     - [ ] Functions/classes with zero downstream usage
     - [ ] Deprecated patterns still in codebase
     - [ ] Backwards compatibility code that can be removed
   - **Output:** `DEAD_CODE_REPORT.md` for my approval (remove or mandate adoption)

4. **Duplication Audit**
   - Check if functionality exists in multiple libraries:
     - [ ] Same feature in unified-domain-services and another library (e.g., error handling in both UCS and UEI)
     - [ ] Different implementations of same concept (e.g., two retry decorators)
   - **Output:** `DUPLICATION_REPORT.md` for consolidation decisions

5. **Technical Debt Audit**
   - [ ] Backwards compatibility patterns (flag for removal)
   - [ ] Fallback imports (e.g., `try: from X except: from Y`)
   - [ ] Empty fallbacks (e.g., `.get("key", "")`)
   - [ ] Lazy imports (except for optional deps like web3, databento)
   - [ ] Circular import workarounds
   - **Output:** `TECHNICAL_DEBT.md` with removal plan

6. **Type Safety Audit**
   - [ ] Any types (must be exceptional, documented)
   - [ ] Missing type hints
   - [ ] Incomplete Protocol definitions
   - [ ] reportAny/reportUnknown* violations
   - **Output:** Type errors must be ZERO after hardening

7. **Test Coverage Audit**
   - [ ] Current coverage percentage (must reach 70%+ for central libraries)
   - [ ] Untested critical paths
   - [ ] Missing edge case tests
   - [ ] Test quality (synthetic fixtures, no real GCS data)
   - **Output:** `COVERAGE_PLAN.md` with path to 70%+

8. **Dependency Audit**
   - [ ] All dependencies in `pyproject.toml` are actually used
   - [ ] Version alignment with codex standards (pytest>=9.0.1, ruff==0.15.0, etc.)
   - [ ] No path dependencies (must use versioned releases)
   - [ ] Published to Google Artifact Registry
   - **Output:** Dependency violations fixed

9. **Import Standards Audit**
   - [ ] All imports at top of file (no lazy imports except documented exceptions)
   - [ ] No circular imports
   - [ ] Clean `__init__.py` exports (consumers import from top-level, not nested modules)
   - [ ] External import standards followed (see `.cursor/rules/external-import-standards.mdc`)
   - **Output:** Import violations fixed

10. **Quality Gates Audit**
    - [ ] `scripts/quality-gates.sh` exists and passes
    - [ ] Ruff version matches codex (==0.15.0)
    - [ ] basedpyright passes (blocking, no exceptions)
    - [ ] pytest-xdist for parallel execution
    - [ ] Coverage threshold enforced (70% for libraries)
    - **Output:** Quality gates pass in <3 minutes

11. **Artifact Registry Audit**
    - [ ] Library published to Google Artifact Registry
    - [ ] Version bumps before code changes (see `.cursor/rules/library-versioning.mdc`)
    - [ ] Cloud Build validates version uniqueness
    - [ ] Downstream services can install from registry
    - **Output:** Publishing workflow verified

---

### Phase 2: Downstream Enforcement

For EVERY service that should use unified-domain-services:

1. **Verify Usage**
   - [ ] Service declares unified-domain-services in `pyproject.toml` dependencies
   - [ ] Service imports from unified-domain-services (not custom implementations)
   - [ ] Service uses canonical patterns (e.g., `UnifiedCloudConfig`, not `os.getenv()`)

2. **Unit Test Enforcement**
   - [ ] Service has unit tests validating proper unified-domain-services usage
   - [ ] Tests fail if service bypasses unified-domain-services (e.g., uses `os.getenv()` instead of config)
   - [ ] Tests enforce canonical patterns

3. **Standardization**
   - Take **instruments-service** as the role model for:
     - Dependency declaration patterns
     - Import patterns
     - Usage patterns
     - Test patterns
   - All services should match instruments-service patterns (unless documented deviation)

**Output:** `DOWNSTREAM_ENFORCEMENT.md` with per-service compliance matrix

---

### Phase 3: Hardening Execution

**Goals:**
- 70%+ test coverage (libraries are critical infrastructure)
- Zero type errors (basedpyright passes)
- Zero quality gate failures
- All functions <50-100 lines
- All files <900 lines (split if larger)
- Zero dead code (everything used by at least one service)
- Zero duplication (one canonical implementation)
- Zero technical debt (no backwards compatibility, no fallbacks)

**Constraints:**
- ❌ NO empty fallbacks (`.get("key", "")` for required config)
- ❌ NO backwards compatibility code (clean cutover, not gradual)
- ❌ NO lazy imports (except optional deps: web3, databento, or TYPE_CHECKING)
- ❌ NO circular imports (fix architecture, not workarounds)
- ❌ NO `Any` types (use Protocol, TypedDict, TypeVar)
- ❌ NO hardcoded project IDs (use config)
- ❌ NO broad `except Exception` (use decorators or specific exceptions)
- ❌ NO skipping tests (fix or remove irrelevant tests)
- ❌ NO duplicate implementations across libraries
- ❌ NO functionality that zero services use

**Execution Strategy:**
- Use parallel agents for independent work (max 4 concurrent)
- Break work into <1 hour tasks per agent
- Commit after every major block of work
- Run quality gates before every commit
- Document all architectural decisions in codex

---

## REFERENCE DOCUMENTATION

### Cursor Rules (MUST Follow All)
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/.cursorrules` (workspace rules)
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/.cursor/rules/*.mdc` (all rule files)

**Key Rules:**
- `service-structure-standards.mdc` - Library structure (adapt for libraries)
- `external-import-standards.mdc` - Top-level imports only
- `no-type-any-use-specific.mdc` - Specific types, not Any
- `hardening-standards.mdc` - Fail fast, no defensive programming
- `quality-gates-hardening.mdc` - E501 enforced, basedpyright blocking
- `library-versioning.mdc` - Version bump before code changes
- `uv-package-manager.mdc` - UV for all installs
- `python-version-consistency.mdc` - Python 3.13+ everywhere

### Codex Standards (Source of Truth)
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/unified-trading-codex/06-coding-standards/` (all files)

**Key Standards:**
- `quality-gates.md` - 3-stage consistency, <3 min execution
- `dependency-management.md` - Version alignment, artifact registry
- `testing.md` - 4-tier structure, 70% coverage for libraries
- `validation-patterns.md` - Pre-flight vs in-flight validation
- `type-hints-guide.md` - Specific types, no Any
- `file-splitting-guide.md` - SRP, <900 lines per file

### Epics (Context for Library Purpose)
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/unified-trading-codex/11-project-management/epics/`

**Relevant Epics:**
- `unified-libraries-refactor-epic.md` - Library split rationale
- `library-dependency-alignment-epic.md` - Version alignment
- `quality-gates-consistency-epic.md` - 3-stage consistency

### API Contracts (Schema Standards)
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/api-contracts/`

**For libraries with external API integrations:**
- Test schemas against actual API responses
- Use api-contracts as source of truth
- Run local tests with real credentials to validate schemas

### Deployment Configs (Usage Validation)
- `/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/unified-trading-deployment-v3/configs/`

**Check:**
- `checklist.{service}.yaml` - Service capabilities
- `data-catalogue.{service}.yaml` - Data expectations
- `sharding.{service}.yaml` - Sharding dimensions

**Ensure unified-domain-services supports all dimensions services need**

---

## ROLE MODEL: instruments-service

Use **instruments-service** as the reference implementation for:

1. **Dependency Declaration**
   - How to declare unified libraries in `pyproject.toml`
   - Version ranges and constraints
   - Dev dependencies (pytest>=9.0.1, ruff==0.15.0)

2. **Import Patterns**
   - Top-level imports from libraries
   - No nested module imports
   - Clean separation of concerns

3. **Usage Patterns**
   - How to use UnifiedCloudConfig
   - How to use event logging (setup_events, log_event)
   - How to use domain clients (InstrumentsDomainClient)
   - How to use storage abstractions (get_storage_client)

4. **Test Patterns**
   - Synthetic fixtures (5-20 rows, no real GCS data)
   - Unit test structure
   - Coverage targets (35% min, 50% recommended, 70% for libraries)
   - Test quality standards

5. **Quality Gates**
   - `scripts/quality-gates.sh` structure
   - Ruff configuration
   - basedpyright configuration
   - Coverage enforcement

**Deviations from instruments-service patterns MUST be documented**

---

## VALIDATION CHECKLIST

Before declaring unified-domain-services production-ready:

### Coverage & Quality
- [ ] 70%+ test coverage (libraries are critical)
- [ ] All tests pass in <3 minutes (pytest-xdist parallel execution)
- [ ] Zero skipped tests (fix or remove)
- [ ] Quality gates pass (ruff, basedpyright, pytest)
- [ ] All files <900 lines
- [ ] All functions <50-100 lines

### Type Safety
- [ ] Zero Any types (or documented exceptions)
- [ ] basedpyright passes (reportAny=error)
- [ ] All public APIs have type hints
- [ ] Protocol definitions complete

### Downstream Adoption
- [ ] Every exported function/class used by at least one service
- [ ] All services that should use unified-domain-services actually do
- [ ] No services implement their own versions of unified-domain-services functionality
- [ ] Usage patterns consistent across all consumers
- [ ] Unit tests enforce proper usage in downstream services

### Technical Debt
- [ ] Zero backwards compatibility code
- [ ] Zero empty fallbacks
- [ ] Zero lazy imports (except optional deps)
- [ ] Zero circular imports
- [ ] Zero dead code

### Duplication
- [ ] No duplicate implementations across libraries
- [ ] One canonical implementation per feature
- [ ] Clear ownership (which library owns what)

### Artifact Registry
- [ ] Published to Google Artifact Registry
- [ ] Version bumps before code changes
- [ ] Cloud Build validates version uniqueness
- [ ] Downstream services can install from registry

### Documentation
- [ ] README with clear usage examples
- [ ] All public APIs documented
- [ ] Architecture decisions in codex
- [ ] Cursor rules updated if patterns changed

---

## OUTPUTS REQUIRED

Create these documents for my review:

1. **ADOPTION_AUDIT.md**
   - Usage matrix (which services use which functions)
   - Services that should use unified-domain-services but don't
   - Dead code (functions with zero usage)

2. **CONSISTENCY_VIOLATIONS.md**
   - Services implementing their own versions
   - Different patterns across services
   - Outdated patterns (e.g., os.getenv instead of config)

3. **DUPLICATION_REPORT.md**
   - Functionality duplicated across libraries
   - Recommendation for consolidation
   - Migration plan if consolidation needed

4. **TECHNICAL_DEBT.md**
   - Backwards compatibility code to remove
   - Fallback patterns to eliminate
   - Lazy imports to fix
   - Circular imports to resolve

5. **COVERAGE_PLAN.md**
   - Current coverage: X%
   - Target coverage: 70%+
   - Untested critical paths
   - Test implementation plan (broken into <1 hour tasks)

6. **DOWNSTREAM_ENFORCEMENT.md**
   - Per-service compliance matrix
   - Services needing updates
   - Unit tests to add for enforcement
   - Migration tasks (broken into <1 hour tasks)

7. **PRODUCTION_READINESS_CHECKLIST.md**
   - Where we are (current state)
   - Where we're going (target state)
   - Detailed tasks to get there (each <1 hour)
   - Priority order (P0, P1, P2)
   - Agent assignment strategy (which tasks can run in parallel)

---

## EXECUTION RULES

### What You WILL Do
- ✅ Audit comprehensively (all 11 dimensions)
- ✅ Flag dead code for my approval
- ✅ Flag duplication for consolidation decisions
- ✅ Fix technical debt (backwards compatibility, fallbacks, lazy imports)
- ✅ Achieve 70%+ coverage
- ✅ Enforce downstream adoption
- ✅ Run quality gates before every commit
- ✅ Use parallel agents (max 4) for independent work
- ✅ Break work into <1 hour tasks
- ✅ Commit after every major block of work
- ✅ Validate against instruments-service patterns

### What You Will NOT Do
- ❌ NO quickmerge/push to repo (you can commit locally though)
- ❌ NO skipping tests (fix or remove irrelevant tests)
- ❌ NO empty fallbacks (fail loud with clear errors)
- ❌ NO backwards compatibility (clean cutover)
- ❌ NO lazy imports (except optional deps)
- ❌ NO Any types (use specific types)
- ❌ NO duplicate implementations (one canonical version)
- ❌ NO dead code (everything must be used)

### Resource Management
- Follow `.cursor/rules/service-structure-standards.mdc` to avoid killing resources
- Use workspace groups for epic work (see `.cursor/workspace-configs/WORKSPACE-RECOMMENDATIONS.md`)
- Kill zombie basedpyright processes before running (see `.cursor/rules/basedpyright-safety.mdc`)

---

## ALIGNMENT VALIDATION

### Venues & Data Types
Check that unified-domain-services supports all venues and data types in our universe:
- CEFI: Binance, Coinbase, Kraken, etc. (see `unified-trading-deployment-v3/configs/venues.yaml`)
- DEFI: Uniswap, Aave, etc.
- TRADFI: NYSE, NASDAQ, etc.

**Validation:**
- Check `unified-trading-deployment-v3/configs/checklist.{service}.yaml` for capability requirements
- Check `unified-trading-deployment-v3/configs/data-catalogue.{service}.yaml` for data expectations
- Check `unified-trading-deployment-v3/configs/sharding.{service}.yaml` for sharding dimensions

**Ensure unified-domain-services supports ALL dimensions services need**

### Environment Alignment
- Local testing: Uses path dependencies via `[tool.uv.sources]`
- GitHub Actions: Installs from Artifact Registry via `pip.conf`
- Cloud Build: Installs from Artifact Registry via `pip.conf`

**Proper auth required for all three environments**

---

## SAMPLE SIZE & CSV DUMPING

unified-domain-services should facilitate:
- CSV sample results (using UCS patterns)
- Sample size controlled via env var (e.g., `CSV_MAX_LINES`)
- Default: 0 in prod (disabled), configurable in dev (e.g., 1000)

**Pattern:**
```python
def dump_to_csv(data, max_lines: int | None = None):
    max_lines = max_lines or int(os.getenv("CSV_MAX_LINES", "0"))
    if max_lines == 0:
        return  # Disabled in prod
    # ... dump logic
```

---

## BATCH-LIVE SYMMETRY (If Applicable)

If unified-domain-services supports both batch and live modes:
- 90% code shared (mode-agnostic engine)
- Only 4 seams differ: data source, data sink, persistence thread, trigger
- Clearly document what live deviates on
- See `unified-trading-codex/04-architecture/batch-live-symmetry.md`

---

## FINAL DELIVERABLE

A production-ready unified-domain-services with:
- ✅ 70%+ test coverage
- ✅ Zero quality gate failures
- ✅ Zero type errors
- ✅ Zero dead code
- ✅ Zero duplication across libraries
- ✅ 100% downstream adoption (all services use it properly)
- ✅ Unit tests enforce proper usage
- ✅ Published to Artifact Registry
- ✅ Ready to build on GitHub Actions, then Cloud Build, then deploy with UTDv3

**Are we clear? Ready for the audit of where we are and where we're going?**

**Write me the 7 required output documents (ADOPTION_AUDIT.md, CONSISTENCY_VIOLATIONS.md, DUPLICATION_REPORT.md, TECHNICAL_DEBT.md, COVERAGE_PLAN.md, DOWNSTREAM_ENFORCEMENT.md, PRODUCTION_READINESS_CHECKLIST.md) with detailed tasks. No task should be >1 hour of wall time with 1 agent. In practice you will run parallel agents. It's all going to be done by AI (you) so take that into account in your time estimates.**
```

---

## USAGE INSTRUCTIONS

1. **Copy this template**
2. **Replace `unified-domain-services` with actual library:**
   - unified-cloud-services
   - unified-config-interface
   - unified-events-interface
   - unified-domain-services
   - unified-market-interface
   - unified-trade-execution-interface
   - unified-ml-interface
   - execution-algo-library

3. **Adjust library-specific context:**
   - For UCS: Focus on cloud abstractions, storage, secrets
   - For UCI: Focus on configuration patterns, validation
   - For UEI: Focus on event logging, lifecycle events
   - For UDS: Focus on domain clients (instruments, market data)
   - For UMI: Focus on market data interfaces, venue adapters
   - For UOI: Focus on order execution, trade execution
   - For unified-ml-interface: Focus on ML training/inference coordination
   - For execution-algo-library: Focus on execution algorithms (TWAP, VWAP, etc.)

4. **Run the prompt with Claude Code or Cursor Agent**

5. **Review the 7 output documents**

6. **Make decisions on:**
   - Dead code (remove or mandate adoption)
   - Duplication (consolidate into which library)
   - Backwards compatibility (remove or keep)

7. **Execute hardening with parallel agents**

---

## KEY DIFFERENCES FROM SERVICE PROMPT

| Aspect | Service Prompt | Library Prompt |
|--------|---------------|----------------|
| **Coverage Target** | 35% min, 50% recommended | 70% min (libraries are critical) |
| **Downstream Enforcement** | N/A | CRITICAL - all services must use library properly |
| **Dead Code** | Remove | Flag for approval (might need to mandate adoption) |
| **Duplication** | N/A | CRITICAL - one canonical implementation per feature |
| **Artifact Registry** | N/A | REQUIRED - versioned releases |
| **Usage Validation** | N/A | Unit tests enforce proper usage in downstream services |
| **Role Model** | instruments-service | instruments-service (for dependency/import patterns) |

---

## EXAMPLE: unified-cloud-services

**Adoption Audit Questions:**
- Which services use `get_storage_client()`? (should be ALL services with GCS I/O)
- Which services use `get_secret_client()`? (should be ALL services with secrets)
- Which services use `UnifiedCloudConfig`? (should be ALL services)
- Which services still use `os.getenv()` instead? (flag as violations)
- Which services use `storage.Client()` directly? (flag as violations)

**Consistency Violations:**
- instruments-service uses `get_storage_client()` ✅
- market-tick-data-handler uses `storage.Client()` ❌ (must fix)
- features-calendar uses `get_storage_client()` ✅
- ml-training uses custom GCS wrapper ❌ (must migrate to UCS)

**Dead Code:**
- `GCSFuseHelper` class (0 services use it) → Flag for approval
- `SecurityAuditLogger` class (0 services use it) → Flag for approval
- Old `BaseGCSLoader` (deprecated, replaced by `CloudStorageLoader`) → Remove

**Duplication:**
- Error handling decorators in both UCS and UEI → Consolidate into UEI
- Resource monitoring in both UCS and UEI → Consolidate into one library

**Technical Debt:**
- Backwards compatibility for old config patterns → Remove (clean cutover)
- Fallback imports for split libraries → Remove (all services migrated)
- Empty fallbacks in config loading → Remove (fail loud)

---

## NOTES

- This prompt is comprehensive and opinionated based on workspace standards
- It enforces fail-fast principles, no backwards compatibility, no technical debt
- It requires 70%+ coverage for libraries (higher than services)
- It mandates downstream adoption enforcement (no dead code, no duplication)
- It uses instruments-service as the role model for patterns
- It breaks work into <1 hour tasks for parallel agent execution
- It produces 7 output documents for review before execution

**This prompt will result in production-ready libraries that are:**
- ✅ Fully adopted by all downstream services
- ✅ Consistently used (no version A in service X, version B in service Y)
- ✅ Enforced via unit tests
- ✅ Zero dead code
- ✅ Zero duplication
- ✅ Zero technical debt
- ✅ 70%+ test coverage
- ✅ Published to Artifact Registry
- ✅ Ready for production deployment
