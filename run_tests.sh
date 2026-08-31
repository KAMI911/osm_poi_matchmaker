#!/bin/bash
# Run all unit tests with coverage report

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

echo "=== OSM POI Matchmaker Test Suite ==="
echo "Repository: $REPO_ROOT"
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "ERROR: pytest not found. Install with: pip install pytest pytest-cov"
    exit 1
fi

# Check if coverage is installed
if ! command -v coverage &> /dev/null; then
    echo "WARNING: coverage not found. Install with: pip install coverage"
    SKIP_COVERAGE=1
fi

# Run tests with coverage
echo "Running unit tests with coverage..."
echo ""

TEST_DIR="osm_poi_matchmaker/test"

if [ -z "$SKIP_COVERAGE" ]; then
    coverage run --source=osm_poi_matchmaker -m pytest \
        $TEST_DIR \
        -v \
        --tb=short \
        --color=yes \
        2>&1 | tee test_output.log

    echo ""
    echo "=== Coverage Report ==="
    coverage report -m

    echo ""
    echo "=== Coverage HTML Report ==="
    coverage html
    echo "HTML report generated: htmlcov/index.html"
else
    pytest \
        $TEST_DIR \
        -v \
        --tb=short \
        --color=yes \
        2>&1 | tee test_output.log
fi

echo ""
echo "=== Test Summary ==="
if grep -q "passed" test_output.log; then
    echo "✓ Tests completed"
    grep "passed" test_output.log | tail -1
else
    echo "✗ No test results found"
    exit 1
fi
