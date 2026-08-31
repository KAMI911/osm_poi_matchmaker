# Testing Guide

## Quick Start

### Using Make (Recommended)

```bash
# Run all tests with coverage
make test

# Quick test without coverage
make test-quick

# Show coverage report
make test-coverage

# Generate HTML coverage report
make test-html
```

### Using Bash Script

```bash
# Make executable
chmod +x run_tests.sh

# Run with coverage
./run_tests.sh
```

### Using pytest directly

```bash
# Run all tests
pytest osm_poi_matchmaker/test -v

# Run specific test file
pytest osm_poi_matchmaker/test/test_data_validation.py -v

# Run specific test class
pytest osm_poi_matchmaker/test/test_quality_metrics.py::TestQualityMetricsCalculate -v

# Run specific test method
pytest osm_poi_matchmaker/test/test_quality_metrics.py::TestQualityMetricsCalculate::test_calculate_metrics -v

# Run with coverage
coverage run --source=osm_poi_matchmaker -m pytest osm_poi_matchmaker/test
coverage report -m
```

---

## Test Suite Overview

All tests consolidated in `osm_poi_matchmaker/test/` (19 files, 182 tests)

### New Framework Tests

| File | Tests | Purpose |
|------|-------|---------|
| `test_match_conflict_resolution.py` | 16 | OSM ID conflict resolution (haversine, conflict finding/resolving) |
| `test_data_validation.py` | 24 | Data validation rules (coordinates, postcode, OSM ID, opening hours) |
| `test_quality_metrics.py` | 26 | Quality metrics calculation (valid records, duplicates, errors) |
| `test_opening_hours_fixed.py` | 6 | Opening hours nan-nan fix testing |
| `test_address_fixed.py` | 18 | Postcode validation fix testing |
| `test_geo_fixed.py` | 24 | Geographic boundary fix testing |
| `test_file_output_fixed.py` | 15 | CSV quoting fix testing |

**New tests: 129 test cases**

### Legacy/Migrated Tests

| File | Tests | Purpose |
|------|-------|---------|
| `test_address.py` | 14 | Address module tests |
| `test_address_extended.py` | 9 | Extended address tests |
| `test_poi_patch.py` | 19 | POI patch functionality |
| `test_osm_extended.py` | 5 | OSM module extended tests |
| `test_file_output_helper.py` | 1 | File output helper |
| `test_opening_hours.py` | 1 | Opening hours legacy |
| `test_online_poi_matching.py` | 1 | Online POI matching |
| `test_osm.py` | 1 | OSM module |
| `test_poi_dataset.py` | 1 | POI dataset |
| `test_timing.py` | 1 | Timing tests |
| `test_create_db.py` | 0 | DB creation (empty) |
| `test_opening_hours_data.py` | 0 | Opening hours data (empty) |

**Legacy tests: 53 test cases**

**Total: 182 test cases**

### Test Coverage Areas

#### Conflict Resolution (`test_match_conflict_resolution.py`)
- ✅ Haversine distance: zero distance, known distance (Budapest-Szeged), edge cases
- ✅ Find conflicts: basic duplicates, no duplicates, ignoring None values
- ✅ Resolve single conflict: duplicate removal, single-conflict edge case
- ✅ Full workflow: large conflict groups, preserve single matches, mixed conflicts

#### Data Validation (`test_data_validation.py`)
- ✅ Required fields: missing coordinates, missing lat/lon
- ✅ Geographic bounds: outside Hungary (lat/lon), edge cases (min/max valid)
- ✅ Postcode format: valid 4-digit, too long/short, non-numeric, missing/empty OK
- ✅ OSM ID format: positive integers, negative/zero invalid, missing OK
- ✅ Opening hours: valid formats, missing OK
- ✅ Batch validation: multiple errors, strict mode, summary generation

#### Quality Metrics (`test_quality_metrics.py`)
- ✅ Initialization: with data, empty DataFrame
- ✅ Metric calculation: all metrics, total/valid/missing counts
- ✅ Calculations: error rate %, completion rate %
- ✅ Summary generation: formatted output with percentages
- ✅ Export: JSON export, auto-calculate, logging
- ✅ Robustness: missing DataFrame columns, zero-division protection

---

## Dependencies

```bash
# Install test requirements
pip install pytest pytest-cov coverage

# Or use make
make install-deps
```

---

## Coverage Goals

- **Target:** 80%+ coverage for core modules
- **Priority:** data_validation.py, quality_metrics.py, match_conflict_resolution.py

### Check Coverage

```bash
# Terminal report
make test-coverage

# HTML report (browse htmlcov/index.html)
make test-html
```

---

## Running Specific Tests

```bash
# Run only conflict resolution tests
make test-conflict

# Run only data validation tests
make test-validation

# Run only quality metrics tests
make test-metrics
```

---

## Continuous Testing

```bash
# Watch mode (requires pytest-watch)
pip install pytest-watch
ptw osm_poi_matchmaker/test/ -- -v
```

---

## Debugging Tests

```bash
# Show print statements
pytest osm_poi_matchmaker/test/ -v -s

# Stop at first failure
pytest osm_poi_matchmaker/test/ -x

# Show local variables on failure
pytest osm_poi_matchmaker/test/ -l

# Drop into debugger on failure (requires ipdb)
pytest osm_poi_matchmaker/test/ --pdb
```

---

## Test Output Example

```
=== OSM POI Matchmaker Test Suite ===
Repository: /home/kami/git/kami911/osmpoi/osm_poi_matchmaker

Running unit tests with coverage...

osm_poi_matchmaker/test/test_data_validation.py::TestPOIDataValidator::test_valid_dataset PASSED
osm_poi_matchmaker/test/test_data_validation.py::TestPOIDataValidator::test_missing_coordinates PASSED
...

=== Coverage Report ===
Name                                                      Stmts   Miss  Cover
-----------------------------------------------------------------------
osm_poi_matchmaker/libs/data_validation.py                  120    10   91%
osm_poi_matchmaker/libs/quality_metrics.py                   95     8   92%
osm_poi_matchmaker/libs/match_conflict_resolution.py        45     3   93%
-----------------------------------------------------------------------
TOTAL                                                       260    21   92%

✓ Tests completed
66 passed in 1.23s
```

---

## Troubleshooting

### pytest not found

```bash
pip install pytest pytest-cov coverage
```

### Import errors

```bash
# Ensure the repo is in PYTHONPATH
cd /home/kami/git/kami911/osmpoi/osm_poi_matchmaker
export PYTHONPATH="${PWD}:$PYTHONPATH"
pytest osm_poi_matchmaker/test/
```

### Permission denied on run_tests.sh

```bash
chmod +x run_tests.sh
./run_tests.sh
```

---

## Adding New Tests

### Template

```python
import unittest
from osm_poi_matchmaker.libs.your_module import YourClass

class TestYourClass(unittest.TestCase):
    """Test YourClass functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.obj = YourClass()

    def test_something(self):
        """Test description."""
        result = self.obj.method()
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
```

### Naming Conventions

- **Test classes:** `Test<ClassName>`
- **Test methods:** `test_<what_is_being_tested>`
- **Fixtures:** Use `setUp()` for shared test data

### Best Practices

- One assertion per test (when possible)
- Descriptive test names
- Use `setUp()` for initialization
- Test both happy path and edge cases
- Test error conditions (invalid input, missing data)

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - run: pip install -r requirements-test.txt
      - run: make test
```

---

## Performance

| Suite | Time | Tests |
|-------|------|-------|
| Conflict Resolution | ~50ms | 16 |
| Data Validation | ~150ms | 24 |
| Quality Metrics | ~200ms | 26 |
| **Total** | **~400ms** | **66** |

Target: Keep full suite under 1 second.
