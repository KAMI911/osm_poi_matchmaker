.PHONY: test test-all test-verbose test-coverage test-quick test-fixed clean install help

help:
	@echo "OSM POI Matchmaker Test Suite (182 tests consolidated)"
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
	@echo "New framework tests (129 tests):"
	@echo "  test-conflict     Conflict resolution (16)"
	@echo "  test-validation   Data validation (24)"
	@echo "  test-metrics      Quality metrics (26)"
	@echo "  test-fixed        All regression tests (63)"
	@echo ""
	@echo "Regression test subsets:"
	@echo "  test-opening-hours Opening hours fix (6)"
	@echo "  test-address      Address/postcode fix (18)"
	@echo "  test-geo          Geo boundary fix (24)"
	@echo "  test-file-output  CSV quoting fix (15)"
	@echo ""
	@echo "Utilities:"
	@echo "  test-stats        Show test suite statistics"
	@echo "  clean             Remove test artifacts"
	@echo "  install-deps      Install test dependencies"
	@echo ""

install-deps:
	python3 -m pip install --break-system-packages -q pytest pytest-cov coverage

test: install-deps
	bash run_tests.sh

test-quick:
	pytest osm_poi_matchmaker/test/ -v --tb=short

test-verbose:
	pytest osm_poi_matchmaker/test/ -vv --tb=long

test-coverage:
	coverage run --source=osm_poi_matchmaker -m pytest osm_poi_matchmaker/test/ -v
	coverage report -m

test-html:
	coverage run --source=osm_poi_matchmaker -m pytest osm_poi_matchmaker/test/ -q
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
	@find osm_poi_matchmaker/test -name "test_*.py" -type f | wc -l | xargs -I {} echo "  Total: {} files"
	@echo ""
	@echo "Test cases:"
	@grep -r "def test_" osm_poi_matchmaker/test/ 2>/dev/null | wc -l | xargs -I {} echo "  Total: {} test methods"
	@echo ""
	@echo "Breakdown by file:"
	@for f in osm_poi_matchmaker/test/test_*.py; do count=$$(grep -c "def test_" "$$f" 2>/dev/null || echo 0); echo "  $$(basename $$f): $$count tests"; done | sort -t: -k2 -rn

.PHONY: test-all
test-all: test
	@echo ""
	@echo "All tests completed!"
