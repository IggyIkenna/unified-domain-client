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
  ruff format --line-length 120 .
else
  ruff format --check --line-length 120 .
fi

# Step 4: Lint
if [ "$NO_FIX" = false ]; then
  ruff check --fix --line-length 120 .
else
  ruff check --line-length 120 .
fi

# ============================================================================
# Step 2b: TYPE CHECKING (basedpyright)
# ============================================================================
echo ""
echo "🔍 TYPE CHECKING (basedpyright)"
echo "----------------------------------------------------------------------"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Ensure basedpyright is available
if command -v basedpyright &> /dev/null; then
    TYPE_CHECKER="basedpyright"
elif python3 -c "import basedpyright" &> /dev/null; then
    TYPE_CHECKER="python3 -m basedpyright"
else
    uv pip install "basedpyright>=1.20.0" --quiet
    TYPE_CHECKER="basedpyright"
fi

echo -n "Running type checker (basedpyright)... "
if $TYPE_CHECKER unified_domain_services/ --level warning 2>&1 | tee /tmp/basedpyright.log | grep -qE "0 errors, 0 warnings"; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC}"
    echo -e "${RED}Type errors found:${NC}"
    grep -E "error|warning" /tmp/basedpyright.log | head -10
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
# Step 5.5: CODEX COMPLIANCE (empty fallbacks, imports, Any)
# ============================================================================
echo ""
echo "======================================================================"
echo "CODEX COMPLIANCE"
echo "======================================================================"

if command -v rg &> /dev/null; then
  echo -n "Checking for empty fallback patterns... "
  # Whitelist: cloud_data_provider.py os.environ.get("_", "") for pytest detection
  EMPTY_FALLBACKS=$(rg '\.get\([^)]*,\s*""\)' --type py --glob "!tests/**" --glob "!scripts/**" --glob "!**/cloud_data_provider.py" . 2>/dev/null || true)
  ENV_EMPTY=$(rg 'os\.environ\.get\([^)]*,\s*""\)' --type py --glob "!tests/**" --glob "!scripts/**" --glob "!**/cloud_data_provider.py" . 2>/dev/null || true)
  if [ -n "$EMPTY_FALLBACKS" ] || [ -n "$ENV_EMPTY" ]; then
    echo -e "${RED}FAIL${NC}"
    [ -n "$EMPTY_FALLBACKS" ] && echo "$EMPTY_FALLBACKS" | head -5
    [ -n "$ENV_EMPTY" ] && echo "$ENV_EMPTY" | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
  else
    echo -e "${GREEN}PASS${NC}"
  fi

  echo -n "Checking for imports inside functions... "
  # Whitelist: __init__.py (circular import prevention), clients.py, cloud_data_provider.py, date_validation.py, instruction_schema.py
  violations=$(rg "^[[:space:]]+import |^[[:space:]]+from .* import" --type py --glob "!tests/**" --glob "!scripts/**" \
    --glob "!**/__init__.py" --glob "!**/clients.py" --glob "!**/cloud_data_provider.py" \
    --glob "!**/date_validation.py" --glob "!**/instruction_schema.py" . 2>/dev/null || true)
  if [ -n "$violations" ]; then
    echo -e "${RED}FAIL${NC}"
    echo "$violations" | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
  else
    echo -e "${GREEN}PASS${NC}"
  fi

  echo -n "Checking for Any type usage... "
  ANY_USAGE=$(rg ": Any|-> Any|\[Any\]" --type py --glob "!tests/**" --glob "!scripts/**" . 2>/dev/null | wc -l | tr -d " " || echo "0")
  if [ "${ANY_USAGE:-0}" -gt 0 ]; then
    echo -e "${RED}FAIL${NC}"
    rg ": Any|-> Any|\[Any\]" --type py --glob "!tests/**" --glob "!scripts/**" . | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
  else
    echo -e "${GREEN}PASS${NC}"
  fi
else
  echo -e "${YELLOW}ripgrep not found - skipping codex checks${NC}"
fi

# Check 8f: .gitignore must NOT allow credential JSON (no negation like !central-element-*.json)
echo -n "Checking .gitignore for credential file negation... "
if [[ -f ".gitignore" ]] && rg "!central-element|!.*credentials.*\.json" .gitignore --no-heading --no-line-number > /dev/null 2>&1; then
    echo -e "${RED}FAIL${NC}"
    echo -e "${YELLOW}Found credential file negation in .gitignore (remove !central-element-*.json; credential JSON must never be committed):${NC}"
    rg "!central-element|!.*credentials.*\.json" .gitignore --no-heading --line-number
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 8g: No hardcoded project ID in tests (use test-project placeholder)
echo -n "Checking for hardcoded project ID in tests... "
HARDCODED_PROJECT=$(rg "central-element-323112|get_config.*central-element" tests/ 2>/dev/null || true)
if [[ -n "$HARDCODED_PROJECT" ]]; then
    echo -e "${RED}FAIL${NC}"
    echo -e "${YELLOW}Found real project ID in tests (use test-project placeholder):${NC}"
    rg "central-element-323112|get_config.*central-element" tests/ --no-heading --line-number 2>/dev/null | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 8h: No broad except Exception in production (use specific exceptions or @handle_api_errors)
# Whitelist: cloud_data_provider.py check_if_exists() intentionally catches all exceptions
echo -n "Checking for broad except Exception in production... "
PROD_SOURCE_DIR=$(echo "$SOURCE_DIRS" | awk '{print $1}')
[ -z "$PROD_SOURCE_DIR" ] && PROD_SOURCE_DIR="unified_domain_services/"
BROAD_EXCEPT=$(rg "except Exception:" --type py --glob "!tests/**" --glob "!**/cloud_data_provider.py" $PROD_SOURCE_DIR 2>/dev/null || true)
if [[ -n "$BROAD_EXCEPT" ]]; then
    echo -e "${RED}FAIL${NC}"
    echo -e "${YELLOW}Found broad except Exception (use @handle_api_errors or specific exceptions):${NC}"
    echo "$BROAD_EXCEPT" | head -5
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi

# Check 9: File size limit (COD-SIZE: max 1500 lines per file)
echo -n "Checking file size (max 1500 lines)... "
SIZE_VIOLATIONS=""
SIZE_WARNINGS=""
for f in $(find . -name "*.py" ! -path "./.venv/*" ! -path "./deps/*" ! -path "./.git/*" ! -path "./build/*" ! -path "./scripts/*" 2>/dev/null); do
    lines=$(wc -l < "$f" 2>/dev/null || echo 0)
    if [[ "$lines" -gt 1500 ]]; then
        SIZE_VIOLATIONS="${SIZE_VIOLATIONS}\n  $f: $lines lines (max 1500)"
    elif [[ "$lines" -gt 1200 ]]; then
        SIZE_WARNINGS="${SIZE_WARNINGS}\n  $f: $lines lines (plan split before 1500)"
    fi
done
if [[ -n "$SIZE_VIOLATIONS" ]]; then
    echo -e "${RED}FAIL${NC}"
    echo -e "${YELLOW}Files exceed 1500-line limit (split by SRP per file-splitting-guide.md):${NC}"
    echo -e "$SIZE_VIOLATIONS"
    CODEX_VIOLATIONS=$((CODEX_VIOLATIONS + 1))
else
    echo -e "${GREEN}PASS${NC}"
fi
if [[ -n "$SIZE_WARNINGS" ]]; then
    echo -e "${YELLOW}Files near limit (plan split):${NC}"
    echo -e "$SIZE_WARNINGS"
fi

# Check 10: pip-audit (dependency vulnerability scan) — non-blocking if not installed
echo -n "Checking dependency vulnerabilities (pip-audit)... "
if command -v pip-audit &> /dev/null; then
    if pip-audit 2>/dev/null; then
        echo -e "${GREEN}PASS${NC}"
    else
        echo -e "${YELLOW}pip-audit found vulnerabilities (review and update deps)${NC}"
    fi
elif python3 -m pip_audit 2>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${YELLOW}pip-audit not installed (add to dev deps: uv pip install pip-audit)${NC}"
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
    echo -e "basedpyright:  ${GREEN}✅ PASSED${NC}"
else
    echo -e "basedpyright:  ${RED}❌ FAILED${NC}"
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
