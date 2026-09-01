#!/bin/bash

# ─────────────────────────────────────────────
# Test Runner with Coverage
# Usage: ./run_tests.sh
# ─────────────────────────────────────────────

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASS=0
FAIL=0
ERROR=0
SKIPPED=()
TOTAL_STMTS=0
TOTAL_MISS=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ── Ensure conftest.py exists at project root ──
cat > "$PROJECT_ROOT/conftest.py" << 'PYEOF'
import sys
import os

root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root)
sys.path.insert(0, os.path.join(root, 'crm'))
sys.path.insert(0, os.path.join(root, 'usage_api'))
sys.path.insert(0, os.path.join(root, 'persistence_layer'))
PYEOF

# All test files - add/remove as needed
TEST_FILES=(
    "crm/tests/test_kafka.py"
    "crm/tests/test_pipeline.py"
    "persistence_layer/tests/test_persistence_validation.py"
    "test/test_csv_parser.py"
    "test/test_redpanda.py"
    "test/test_usage_api.py"
    "test/test_crm.py"
    # Slow tests (import cdr/main.py which generates 150k faker records)
    # Uncomment below if you have time to wait (~5 mins)
    # "test/test_cdr_generator.py"
    # "test/test_validation.py"
)

echo ""
echo -e "${CYAN}================================================${NC}"
echo -e "${CYAN}        Running Tests with Coverage             ${NC}"
echo -e "${CYAN}================================================${NC}"
echo ""

for TEST_FILE in "${TEST_FILES[@]}"; do
    FULL_PATH="$PROJECT_ROOT/$TEST_FILE"

    # Skip if file doesn't exist
    if [ ! -f "$FULL_PATH" ]; then
        echo -e "${YELLOW}[SKIP]${NC} $TEST_FILE (file not found)"
        SKIPPED+=("$TEST_FILE")
        continue
    fi

    echo -e "${CYAN}── Running: $TEST_FILE ${NC}"

    # Run pytest with coverage for this file
    OUTPUT=$(cd "$PROJECT_ROOT" && pytest "$FULL_PATH" \
        --cov="$PROJECT_ROOT" \
        --cov-report=term-missing \
        --ignore=volumes \
        -q 2>&1)

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}[PASS]${NC} $TEST_FILE"
        PASS=$((PASS + 1))
    elif echo "$OUTPUT" | grep -q "error"; then
        echo -e "${RED}[ERROR]${NC} $TEST_FILE"
        echo "$OUTPUT" | grep -E "ERROR|ModuleNotFoundError|ImportError" | head -5
        ERROR=$((ERROR + 1))
    else
        echo -e "${RED}[FAIL]${NC} $TEST_FILE"
        echo "$OUTPUT" | grep -E "FAILED|AssertionError" | head -5
        FAIL=$((FAIL + 1))
    fi

    # Print coverage summary line for this file
    echo "$OUTPUT" | grep -E "TOTAL|%" | tail -3

    # Accumulate total statements and missed for overall coverage
    TOTAL_LINE=$(echo "$OUTPUT" | grep "^TOTAL")
    if [ -n "$TOTAL_LINE" ]; then
        STMTS=$(echo "$TOTAL_LINE" | awk '{print $2}')
        MISS=$(echo "$TOTAL_LINE" | awk '{print $3}')
        TOTAL_STMTS=$((TOTAL_STMTS + STMTS))
        TOTAL_MISS=$((TOTAL_MISS + MISS))
    fi
    echo ""
done

# ── Final Summary ──────────────────────────────
# Calculate overall coverage percentage
if [ $TOTAL_STMTS -gt 0 ]; then
    COVERED=$((TOTAL_STMTS - TOTAL_MISS))
    TOTAL_PCT=$(awk "BEGIN {printf \"%.0f\", ($COVERED / $TOTAL_STMTS) * 100}")
else
    TOTAL_PCT=0
fi

echo -e "${CYAN}================================================${NC}"
echo -e "${CYAN}                  SUMMARY                      ${NC}"
echo -e "${CYAN}================================================${NC}"
echo -e "${GREEN}  PASSED   : $PASS${NC}"
echo -e "${RED}  FAILED   : $FAIL${NC}"
echo -e "${RED}  ERRORS   : $ERROR${NC}"
if [ ${#SKIPPED[@]} -gt 0 ]; then
    echo -e "${YELLOW}  SKIPPED  : ${#SKIPPED[@]}${NC}"
    for s in "${SKIPPED[@]}"; do
        echo -e "${YELLOW}    - $s${NC}"
    done
fi
echo -e "${CYAN}────────────────────────────────────────────────${NC}"
if [ $TOTAL_PCT -ge 80 ]; then
    echo -e "${GREEN}  COVERAGE : ${TOTAL_PCT}%${NC}"
elif [ $TOTAL_PCT -ge 60 ]; then
    echo -e "${YELLOW}  COVERAGE : ${TOTAL_PCT}%${NC}"
else
    echo -e "${RED}  COVERAGE : ${TOTAL_PCT}%${NC}"
fi
echo -e "${CYAN}================================================${NC}"

# Generate combined HTML coverage report
echo ""
echo -e "${CYAN}Generating combined HTML coverage report...${NC}"
cd "$PROJECT_ROOT" && pytest \
    crm/tests/ \
    persistence_layer/tests/ \
    test/test_csv_parser.py \
    test/test_redpanda.py \
    test/test_usage_api.py \
    test/test_crm.py \
    --cov="$PROJECT_ROOT" \
    --cov-report=html \
    --ignore=volumes \
    -q 2>&1 | tail -5

echo -e "${GREEN}HTML report saved to: htmlcov/index.html${NC}"
echo ""

# Exit with failure if any tests failed or errored
if [ $FAIL -gt 0 ] || [ $ERROR -gt 0 ]; then
    exit 1
fi
exit 0