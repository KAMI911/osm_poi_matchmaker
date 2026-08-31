.PHONY: test test-all test-verbose test-coverage test-quick test-fixed clean install help

help:
	@echo "OSM POI Matchmaker Test Suite"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Main targets:"
	@echo "  test              Run all tests with coverage"
	@echo "  test-quick        Run tests without coverage (fast)"
	@echo "  test-verbose      Run tests with verbose output"
	@echo "  test-coverage     Show coverage report in terminal"
	@echo "  test-html         Generate HTML coverage report"
	@echo ""
	@echo "New module tests (3.0 framework):"
	@echo "  test-conflict     Conflict resolution tests (16 tests)"
	@echo "  test-validation   Data validation tests (24 tests)"
	@echo "  test-metrics      Quality metrics tests (26 tests)"
	@echo ""
	@echo "Fixed module tests (regression):"
	@echo "  test-fixed        All fixed module tests (63 tests)"
	@echo "  test-opening-hours Opening hours nan-nan fix (6 tests)"
	@echo "  test-address      Postcode validation fix (18 tests)"
	@echo "  test-geo          Boundary checking fix (24 tests)"
	@echo "  test-file-output  CSV quoting fix (15 tests)"
	@echo ""
	@echo "Utilities:"
	@echo "  clean             Remove test artifacts"
	@echo "  install-deps      Install test dependencies"
	@echo ""

install-deps:
	pip install pytest pytest-cov coverage

test: install-deps
	bash run_tests.sh

test-quick:
	pytest test osm_poi_matchmaker/test/ -v --tb=short

test-verbose:
	pytest test osm_poi_matchmaker/test/ -vv --tb=long

test-coverage:
	coverage run --source=osm_poi_matchmaker -m pytest test osm_poi_matchmaker/test/ -v
	coverage report -m

test-html:
	coverage run --source=osm_poi_matchmaker -m pytest test osm_poi_matchmaker/test/ -q
	coverage html
	@echo "HTML report generated at: htmlcov/index.html"

test-conflict:
	pytest osm_poi_matchmaker/test/test_match_conflict_resolution.py -v

test-validation:
	pytest osm_poi_matchmaker/test/test_data_validation.py -v

test-metrics:
	pytest osm_poi_matchmaker/test/test_quality_metrics.py -v

test-fixed:
	pytest osm_poi_matchmaker/test/test_*_fixed.py -v

test-opening-hours:
	pytest osm_poi_matchmaker/test/test_opening_hours_fixed.py -v

test-address:
	pytest osm_poi_matchmaker/test/test_address_fixed.py -v

test-geo:
	pytest osm_poi_matchmaker/test/test_geo_fixed.py -v

test-file-output:
	pytest osm_poi_matchmaker/test/test_file_output_fixed.py -v

clean:
	rm -rf .coverage htmlcov/ __pycache__ *.pyc test_output.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

test-stats:
	@echo "=== Test Suite Statistics ==="
	@echo ""
	@echo "Test files:"
	@find test osm_poi_matchmaker/test -name "test_*.py" -type f | wc -l | xargs -I {} echo "  Total: {} files"
	@echo ""
	@echo "Test cases (legacy):"
	@grep -r "def test_" test/ 2>/dev/null | wc -l | xargs -I {} echo "  Legacy: {} tests"
	@echo ""
	@echo "Test cases (new framework):"
	@grep -r "def test_" osm_poi_matchmaker/test/ 2>/dev/null | wc -l | xargs -I {} echo "  New: {} tests"
	@echo ""
	@echo "Combined:"
	@(grep -r "def test_" test/ osm_poi_matchmaker/test/ 2>/dev/null | wc -l) | xargs -I {} echo "  Total: {} test methods"

.PHONY: test-all
test-all: test
	@echo ""
	@echo "All tests completed!"
