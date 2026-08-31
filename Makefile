.PHONY: test test-all test-verbose test-coverage test-quick clean install help

help:
	@echo "OSM POI Matchmaker Test Suite"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  test              Run all tests with coverage"
	@echo "  test-quick        Run tests without coverage (fast)"
	@echo "  test-verbose      Run tests with verbose output"
	@echo "  test-coverage     Show coverage report in terminal"
	@echo "  test-html         Generate HTML coverage report"
	@echo "  test-conflict     Run only conflict resolution tests"
	@echo "  test-validation   Run only data validation tests"
	@echo "  test-metrics      Run only quality metrics tests"
	@echo "  clean             Remove test artifacts"
	@echo "  install-deps      Install test dependencies"
	@echo ""

install-deps:
	pip install pytest pytest-cov coverage

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

clean:
	rm -rf .coverage htmlcov/ __pycache__ *.pyc test_output.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

.PHONY: test-all
test-all: test
	@echo ""
	@echo "All tests completed!"
