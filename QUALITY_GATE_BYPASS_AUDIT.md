# Quality Gate Bypass Audit — unified-domain-services

Inventory of all exceptions, exclusions, and handling that bypass or relax quality gate checks. Use this to decide which to keep, fix, or remove.

**Status:** Comprehensive audit. All bypasses documented and justified. Type fixes completed Feb 2026 — 5 previously excluded files now pass basedpyright.

**CRITICAL — Only Audited Exceptions May Pass:** Quality gates (basedpyright) must pass. Allowed: (1) inline bypasses in sections 2.1, 2.2, 2.3; (2) path exclusions in section 1.1. All other type errors must be fixed — no relaxations, no baseline files, no downgrading rules to warning.

---

## 1. Quality Gate Script Exclusions (quality-gates.sh)

### 1.1 Path/Glob Exclusions (checks never run on these paths)

| Check | Excluded Paths | Rationale |
|-------|----------------|-----------|
| **basedpyright** | None | All 5 previously excluded files (clients.py, date_validation.py, schemas/__init__.py, schemas/instruction_schema.py, validation.py) now pass — exclusions removed Feb 2026 |

**Note:** PR #4 had excluded these 5 files from pyrightconfig.json. Those exclusions were removed; all files now pass basedpyright.

---

## 2. Inline Code Bypasses (unified_domain_services/)

### 2.1 type: ignore / pyright: ignore — Third-party stub limitations

| File | Line | Code | Purpose |
|------|------|------|---------|
| `clients.py` | 12 | `# pyright: reportAny=false, reportExplicitAny=false` | Storage client `list_blobs` iterator `.prefixes` and pandas `Series.to_dict()` have incomplete stubs |
| `clients.py` | 232 | `list(raw_prefixes)  # type: ignore[arg-type]` | GCS iterator `prefixes` attribute type not in stubs |
| `clients.py` | 455 | `counts.to_dict().items()  # type: ignore[reportAny]` | pandas `Series.to_dict()` returns `dict[Hashable, Any]`; iteration needs Any |
| `clients.py` | 1023 | `super().__init__(*args, **kwargs)  # pyright: ignore[reportCallIssue]` | TypedDict unpacking with `**kwargs: Unpack[ClientConfig]`; Protocol variance limitation |
| date_validation.py | — | *(Removed Feb 2026)* | Replaced Any with dict[str, object], cast(object, yaml.safe_load()), and cast(dict[str, object], x) after isinstance; file now passes strict basedpyright without override |
| `validation.py` | 388 | `timestamps.dt.time == ...  # type: ignore` | Pandas datetime accessor `.dt.time` — pandas-stubs incomplete |
| `schemas/instruction_schema.py` | 18 | `# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportAny=false` | Polars/pandas schema validation; dynamic types from JSON |
| `schemas/instruction_schema.py` | 297 | `df.to_pandas()  # pyright: ignore[reportCallIssue]` | Polars `.to_pandas()` — polars type stubs incomplete |
| `schemas/instruction_schema.py` | 613 | `df.to_pandas()  # pyright: ignore[reportCallIssue]` | Polars `.to_pandas()` — polars type stubs incomplete |
| `__init__.py` | 7 | `# pyright: reportUnsupportedDunderAll=false, reportUnknownVariableType=false` | Re-exports; __all__ dynamic |
| `schemas/__init__.py` | 2 | `# pyright: reportUnknownVariableType=false` | Re-export type inference |

**Justification:**
- **clients.py**: GCS `list_blobs` iterator and pandas `Series.to_dict()` have incomplete type stubs. TypedDict unpacking with `super().__init__` triggers reportCallIssue due to Protocol variance.
- **date_validation.py**: (Fixed Feb 2026) No longer uses file-level override; uses dict[str, object], cast(object, yaml.safe_load()), and cast(dict[str, object], x) after isinstance for YAML config.
- **validation.py**: Pandas `.dt.time` accessor — pandas-stubs don't fully type datetime accessors.
- **instruction_schema.py**: Polars `.to_pandas()` — polars type stubs incomplete. Schema validation uses dynamic types from JSON.
- **__init__.py files**: Re-exports cause reportUnknownVariableType; __all__ triggers reportUnsupportedDunderAll.

**Removal Plan:**
- Monitor pandas-stubs, polars, google-cloud-storage for improved type stubs
- Review in Q2 2026

---

## 3. Summary Counts

| Category | Count |
|----------|-------|
| **File-level pyright overrides** | 4 (clients, instruction_schema, __init__, schemas/__init__) |
| **Inline type: ignore** | 3 (clients 232, 455; validation 388) |
| **Inline pyright: ignore** | 3 (clients 1023; instruction_schema 297, 613) |
| **Third-party stub issues** | 6 (GCS iterator, pandas Series.to_dict, pandas .dt.time, Polars .to_pandas×2, TypedDict unpacking) |

---

## 4. Related Documentation

- `.cursor/rules/strict-type-checking.mdc` — Type checking standards
- `.cursor/rules/quality-gates-audit-factors.mdc` — Audit factors
- `unified-trading-codex/06-coding-standards/quality-gates.md` — Quality gates

---

---

## 5. Lazy Import Exceptions (acceptable patterns)

### 5.1 Optional Dependencies with Graceful Degradation

| File | Line | Code | Purpose |
|------|------|------|---------|
| `schemas/instruction_schema.py` | 476-481 | `try: from unified_cloud_services.utils.id_conventions import validate_strategy_id except ImportError: ...` | Optional validation dependency; gracefully degrades if unified_cloud_services not available in some environments |

**Justification:** This is an acceptable pattern for optional dependencies where the feature can degrade gracefully. The import is protected by try/except and provides fallback behavior.

## 6. File Size Exceptions

### 6.1 Large Files (>1200 lines - Warning)

| File | Lines | Justification | Action Plan |
|------|-------|---------------|-------------|
| `clients.py` | 1394 | Contains multiple domain client classes and factory functions. Each client provides domain-specific convenience methods. | Consider splitting into separate modules per domain (instruments, market_data, features) in Q2 2026 refactor |

**Justification:** The file contains logically related client classes that provide domain-specific access patterns. While large, splitting would reduce cohesion without significant benefit currently.

---

**Last Updated:** 2026-02-24
**Next Review:** Q2 2026 (April 2026)
**Audit Status:** ✅ Comprehensive — All bypasses documented and justified
**Type Checking:** basedpyright — 0 errors, 0 warnings
