# basedpyright Baseline Suppression Notes — unified-domain-client

## File: `.basedpyright-baseline.json`

### When Created

Commit `5c77f6c` — 2026-03-10 — "fix(typecheck): add basedpyright baseline for pre-existing third-party type errors"

### Suppression Count

- **Total suppressions: 16** across **1 file** (`unified_domain_client/artifact_store.py`)
- Baseline file size: 134 lines

### Breakdown by Error Code

| Code                         | Count | Root Cause                                                  |
| ---------------------------- | ----- | ----------------------------------------------------------- |
| `reportUnknownArgumentType`  | 9     | google-cloud-storage client methods lack precise type stubs |
| `reportUnknownParameterType` | 3     | Callback/handler signatures from untyped GCS client         |
| `reportAny`                  | 2     | GCS library returns `Any`-typed values in some contexts     |
| `reportMissingImports`       | 1     | Optional GCS import path not found in typecheck venv        |
| `reportAttributeAccessIssue` | 1     | Dynamic attribute access on GCS blob object                 |

### Why Suppressed (Rationale)

All 16 suppressions originate from a single file (`artifact_store.py`) and are caused by
**google-cloud-storage's incomplete type annotations**. The `google-cloud-storage` library
ships `py.typed` but many method signatures use `Any` or are missing return type annotations
for blob and bucket operations.

This is a third-party annotation quality issue, not a UDC code quality issue. The artifact
store wrapper is itself fully typed; basedpyright cannot see through the untyped GCS boundary.

Per workspace policy: `reportMissingImports` for the GCS import is expected when running
typecheck without the full GCP SDK installed (e.g. in fast lint environments).

### Plan to Reduce

1. **Immediate:** Check whether `google-cloud-storage>=2.16` (latest as of 2026-03-10) ships
   improved stubs — bump the pinned version if stub coverage improved.
2. **Short-term:** Add a thin typed wrapper (`_GCSClient` protocol) around the `storage.Client`
   calls in `artifact_store.py`. This concentrates the untyped surface to the wrapper and would
   eliminate all 16 suppressions from the baseline.
3. **Target:** This baseline should reach **0 suppressions** within one sprint once the GCS
   wrapper protocol is implemented — it is the smallest baseline in the workspace.

### SSOT Reference

`unified-trading-codex/06-coding-standards/quality-gates.md` — baseline suppression policy
