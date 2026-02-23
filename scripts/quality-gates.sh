#!/bin/bash
# Quality Gates - Format, Lint, Test
# Three-stage consistency: Local, GitHub Actions, Cloud Build (all use ruff==0.15.0)

set -e

NO_FIX=false
QUICK=false

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --no-fix) NO_FIX=true ;;
    --quick) QUICK=true ;;
    *) echo "Unknown parameter: $1"; exit 1 ;;
  esac
  shift
done

echo "======================================================================"
echo "UNIFIED-DOMAIN-SERVICES QUALITY GATES"
echo "======================================================================"

# Track overall status
LINT_STATUS=0
TEST_STATUS=0
PYRIGHT_STATUS=0
CODEX_VIOLATIONS=0

# Step 0: Bootstrap UV if needed
if ! command -v uv &> /dev/null; then
  echo "Installing UV..."
  pip install uv --quiet
fi

# Step 0.5: Create/activate venv (use Python 3.13 per codex)
if [ ! -d ".venv" ]; then
  echo "Creating venv with Python 3.13..."
  uv venv --python 3.13 .venv
fi
source .venv/bin/activate

# Step 1: Install dependencies
# Use [tool.uv.sources] path deps when sibling repos exist (../unified-*); else use deps/ for CI
if [ -d "../unified-config-interface" ] && [ -d "../unified-cloud-services" ]; then
  echo "Installing with path deps (sibling repos)..."
elif [ -d "../unified-trading-codex" ]; then
  # CI: create deps symlinks if not present
  if [ ! -d "deps/unified-cloud-services" ]; then
    mkdir -p deps
    [ -d "../unified-cloud-services" ] && ln -sf ../../unified-cloud-services deps/unified-cloud-services 2>/dev/null || true
    [ -d "../unified-config-interface" ] && ln -sf ../../unified-config-interface deps/unified-config-interface 2>/dev/null || true
    [ -d "../unified-events-interface" ] && ln -sf ../../unified-events-interface deps/unified-events-interface 2>/dev/null || true
  fi
  if [ -d "deps/unified-config-interface" ] && [ -d "deps/unified-cloud-services" ]; then
    echo "Installing from deps..."
    uv pip install -e deps/unified-config-interface --quiet
    uv pip install -e deps/unified-cloud-services --quiet
    [ -d "deps/unified-events-interface" ] && uv pip install -e deps/unified-events-interface --quiet || true
  fi
fi

# Step 2: Install self (uses [tool.uv.sources] when siblings exist)
uv pip install -e ".[dev]" --quiet

# Step 3: Format
if [ "$NO_FIX" = false ]; then
  ruff format .
else
  ruff format --check .
fi

# Step 4: Lint
if [ "$NO_FIX" = false ]; then
  ruff check --fix .
else
  ruff check .
fi

# ============================================================================
# Step 2b: TYPE CHECKING (pyright)
# ============================================================================
echo ""
echo "🔍 TYPE CHECKING (pyright)"
echo "----------------------------------------------------------------------"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Ensure pyright or basedpyright is available
if ! command -v pyright &> /dev/null && ! python3 -c "import pyright" &> /dev/null && ! python3 -c "import basedpyright" &> /dev/null; then
    echo -e "${YELLOW}Installing pyright...${NC}"
    command -v uv >/dev/null 2>&1 || pip install uv --quiet
    uv pip install "pyright>=1.1.390" --quiet
fi

echo -n "Running type checker (basedpyright)... "
if python3 -m basedpyright --version &> /dev/null 2>&1; then
    PYRIGHT_CMD="python3 -m basedpyright"
elif command -v pyright &> /dev/null; then
    PYRIGHT_CMD="pyright"
elif command -v basedpyright &> /dev/null; then
    PYRIGHT_CMD="basedpyright"
else
    PYRIGHT_CMD="python3 -m pyright"
fi

if $PYRIGHT_CMD unified_domain_services/ --level warning 2>&1 | tee /tmp/pyright.log | grep -qE "0 errors, 0 warnings"; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC}"
    echo -e "${RED}Type errors found:${NC}"
    grep -E "error|warning" /tmp/pyright.log | head -10
    PYRIGHT_STATUS=1
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
fi

# ============================================================================
# Step 3: TESTS (pytest)
# ============================================================================
echo ""
echo "🧪 TESTS (pytest)"
echo "----------------------------------------------------------------------"

SOURCE_DIR="${SOURCE_DIR:-unified_domain_services}"
MIN_COVERAGE=35
if [ "$QUICK" = false ]; then
  echo "Running: pytest -n auto -v --tb=short --cov=$SOURCE_DIR --cov-report=term-missing --cov-fail-under=$MIN_COVERAGE --timeout=180"
  if pytest -n auto -v --tb=short --cov="${SOURCE_DIR}" --cov-report=term-missing --cov-fail-under="${MIN_COVERAGE}" --timeout=180; then
    echo -e "${GREEN}✅ Tests PASSED${NC}"
    TEST_STATUS=0
  else
    echo -e "${RED}❌ Tests FAILED${NC}"
    TEST_STATUS=1
  fi
else
  echo "Running: pytest -n auto -v --tb=short --cov=$SOURCE_DIR --cov-report=term-missing --cov-fail-under=$MIN_COVERAGE --maxfail=1 -x --timeout=180 (quick mode)"
  if pytest -n auto -v --tb=short --cov="${SOURCE_DIR}" --cov-report=term-missing --cov-fail-under="${MIN_COVERAGE}" --maxfail=1 -x --timeout=180; then
    echo -e "${GREEN}✅ Quick tests PASSED${NC}"
    TEST_STATUS=0
  else
    echo -e "${RED}❌ Quick tests FAILED${NC}"
    TEST_STATUS=1
  fi
fi

# ============================================================================
# Step 6: CI/CD VALIDATORS (BLOCKING - Libraries Only)
# ============================================================================
echo ""
echo "======================================================================"
echo "CI/CD VALIDATORS (BLOCKING)"
echo "======================================================================"

# Colors for validator output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Get repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
CODEX_ROOT="${REPO_ROOT}/../unified-trading-codex"

if [ -f "${CODEX_ROOT}/validators/run_validators.py" ]; then
    echo "Running BLOCKING CI/CD validators for libraries..."

    # CI-02: Local importability (BLOCKING) - verify this repo only (validator checks all libs)
    if (cd "$REPO_ROOT" && uv run python -c "import unified_domain_services; print('OK')" 2>/dev/null) | grep -q OK; then
        echo -e "${GREEN}✅ CI-02 (Local Import) PASSED${NC}"
    else
        echo -e "${RED}❌ CI-02 FAILED: unified-domain-services not locally importable!${NC}"
        exit 1
    fi

    # CI-03: GitHub installability (BLOCKING)
    if python3 "${CODEX_ROOT}/validators/run_validators.py" --validator CI-03 --workspace "${REPO_ROOT}/.."; then
        echo -e "${GREEN}✅ CI-03 (GitHub Install) PASSED${NC}"
    else
        echo -e "${RED}❌ CI-03 FAILED: Library not GitHub installable!${NC}"
        exit 1
    fi

    # CI-04: Artifact Registry (BLOCKING)
    if python3 "${CODEX_ROOT}/validators/run_validators.py" --validator CI-04 --workspace "${REPO_ROOT}/.."; then
        echo -e "${GREEN}✅ CI-04 (Artifact Registry) PASSED${NC}"
    else
        echo -e "${RED}❌ CI-04 FAILED: Library not AR-ready!${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️  Validators not available (unified-trading-codex not found)${NC}"
fi

# ============================================================================
# FINAL SUMMARY
# ============================================================================
echo ""
echo "======================================================================"
echo "QUALITY GATES SUMMARY"
echo "======================================================================"

OVERALL_STATUS=0

if [ $PYRIGHT_STATUS -eq 0 ]; then
    echo -e "Pyright:  ${GREEN}✅ PASSED${NC}"
else
    echo -e "Pyright:  ${RED}❌ FAILED${NC}"
    OVERALL_STATUS=1
fi

if [ $TEST_STATUS -eq 0 ]; then
    echo -e "Tests:    ${GREEN}✅ PASSED${NC}"
else
    echo -e "Tests:    ${RED}❌ FAILED${NC}"
    OVERALL_STATUS=1
fi

if [ $CODEX_VIOLATIONS -eq 0 ]; then
    echo -e "Codex:    ${GREEN}✅ PASSED${NC}"
else
    echo -e "Codex:    ${RED}❌ FAILED (${CODEX_VIOLATIONS} violations)${NC}"
    OVERALL_STATUS=1
fi

echo "======================================================================"

if [ $OVERALL_STATUS -eq 0 ]; then
    echo -e "\n${GREEN}✅ ALL QUALITY GATES PASSED - Safe to push!${NC}\n"
else
    echo -e "\n${RED}❌ QUALITY GATES FAILED - Fix issues before pushing${NC}\n"
fi

exit $OVERALL_STATUS
