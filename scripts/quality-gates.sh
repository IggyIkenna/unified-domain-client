#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-library.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-library-template.sh
#
# Instructions for a new library:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SOURCE_DIR, PACKAGE_NAME, and MIN_COVERAGE are set automatically by rollout (floor=80)
#   3. Add LOCAL_DEPS entries if your library has local editable deps
PACKAGE_NAME="unified-domain-client"
SOURCE_DIR="unified_domain_client"
MIN_COVERAGE=82
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# artifact_store.py uses lazy `import joblib` inside save/load methods because
# joblib is an optional heavy dependency only needed for model serialization.
# Importing at module level forces all UDC consumers to install joblib even when
# they never use artifact storage. Documented in QUALITY_GATE_BYPASS_AUDIT.md.
INSIDE_EXTRA_EXCLUDES=(
    "unified_domain_client/artifact_store.py"
)
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-library.sh"
