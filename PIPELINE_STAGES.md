# OSM POI Matchmaker Pipeline Stages

## Overview

The pipeline processes POI data through 10 sequential stages, from initial data import to final export. Each stage has specific inputs, processing logic, and outputs.

---

## STAGE 0: Import Basic Data

**Duration:** ~30 seconds
**Type:** Single-threaded

Import static reference datasets needed before harvesting:

- **poi_patch.tsv** → POI patches/corrections
- **country.tsv** → Country reference data
- **Hungarian Post XML feeds** → Zip codes, street types

**Output:**
- `city` table with postal codes
- `street_type` table
- `country` table

---

## STAGE 1: Load POI Type Definitions

**Duration:** ~5 seconds
**Type:** Single-threaded

Load POI type/tag definitions from database:

- Shops, banks, fuel stations, etc.
- OSM tag mappings
- Search distance configurations

**Output:**
- `poi_common` table with poi_type definitions

---

## STAGE 2: POI Data Harvest

**Duration:** ~10-15 minutes (parallel)
**Type:** Multiprocessing (N worker pools)

Harvest raw POI data from multiple data providers:

- 40+ data providers (Aldi, CBA, Mobiliti, OMV, etc.)
- Download from APIs, web scrapers, CSV imports
- Extract address, coordinates, opening hours

**Validation Rules:**
- Coordinates must be present (lat/lon)
- Postcode format: 4 digits only (Hungarian)
- Opening hours: valid OSM format

**Quality Metrics:**
- Harvested records per provider
- Harvest errors (API failures, parsing errors)
- Database insertion success rate

**Output:**
- `poi_address_raw` table with harvested data
- Per-provider statistics

---

## STAGE 3: Address Processing

**Duration:** ~2 minutes
**Type:** Single-threaded

Clean and normalize address data:

- Street name extraction
- House number parsing
- Postal code validation
- City name normalization

**Validation Rules:**
- Street name: max 128 characters
- House number: max 16 characters
- City: must exist in city reference table

**Output:**
- Updated `poi_address_raw` with normalized addresses

---

## STAGE 4: Opening Hours Processing

**Duration:** ~1 minute
**Type:** Single-threaded

Process and validate opening hours:

- Parse opening_hours format (Mo-Su HH:MM-HH:MM)
- Validate time format
- Remove invalid entries (nan-nan)
- Handle lunch breaks

**Validation Rules:**
- Time format: HH:MM (24-hour)
- Days: Mo, Tu, We, Th, Fr, Sa, Su
- No invalid time values (NaN, "nan")

**Output:**
- Cleaned opening_hours strings

---

## STAGE 5: Geometry Validation

**Duration:** ~1 minute
**Type:** Single-threaded

Validate and correct coordinates:

- Check latitude/longitude within Hungary bounds
- Detect and swap reversed coordinates
- Fix missing decimal points
- Snap coordinates to nearest building

**Validation Rules:**
- Latitude: 45.5 ≤ lat ≤ 48.6
- Longitude: 16.1 ≤ lon ≤ 22.9
- Must be valid float values

**Output:**
- Corrected geometry column

---

## STAGE 6: Postcode Enhancement

**Duration:** ~3 minutes
**Type:** Single-threaded

Enhance postcode data via reverse geocoding:

- Query OSM database for missing postcodes
- Validate postcode format (4 digits)
- Clean float values (1016.0 → 1016)
- Remove non-Hungarian identifiers

**Validation Rules:**
- Hungarian postcode: exactly 4 digits
- No float formatting (no ".0")
- No complex formats ("10003 - Mobiliti")

**Output:**
- Complete `poi_postcode` column

---

## STAGE 7: POI Patches

**Duration:** ~30 seconds
**Type:** Single-threaded

Apply manual corrections and overrides:

- Load from `poi_patch.tsv`
- Fix known data quality issues
- Override incorrect classifications

**Output:**
- Updated POI data with applied patches

---

## STAGE 8: Online POI Matching

**Duration:** ~20-30 minutes (parallel)
**Type:** Multiprocessing (N worker pools)

Match harvested POIs against live OSM data:

1. Query OSM database by name/type/address/distance
2. If match found:
   - Copy OSM ID, version, changeset, timestamp
   - Download live tags from OSM API
   - Cache in `POI_OSM_cache`
   - Refine address/postcode from OSM
3. If no match:
   - Mark as `poi_new = True`
   - Try to snap to nearby building

**Validation Rules:**
- osm_id: positive integer (if present)
- osm_version: positive integer
- osm_timestamp: valid ISO format

**Quality Metrics:**
- Matched: % with osm_id
- New: % marked poi_new
- Errors: % with match_error

**Output:**
- Matched POI data with OSM IDs
- Match statistics (new vs. matched vs. errors)

---

## STAGE 9: OSM ID Conflict Resolution ⭐

**Duration:** ~50ms (in-memory)
**Type:** Single-threaded (iterative)

Resolve conflicts where multiple POIs match the same OSM element:

1. Find all osm_id duplicates
2. For each conflict group:
   - Calculate distance (Haversine) from each POI to OSM element
   - Reassign farthest POI's osm_id to None
3. Iterate until 0 conflicts remain (max 10 iterations)

**Validation Rules:**
- Each osm_id must be unique (max 1 POI per OSM element)
- Distance calculation: in-memory haversine (fast)

**Quality Metrics:**
- Initial conflicts: number of duplicate osm_ids
- Resolved: how many reassigned to None
- Unresolved: remaining conflicts after max iterations

**Output:**
- POI data with unique osm_ids (no duplicates)
- Conflict resolution statistics

**Example:**
```
Initial: POI A → OSM #123, POI B → OSM #123 (CONFLICT)
         Distance: A=500m, B=100m
Action:  Reassign A (farthest): osm_id=None
Result:  POI A → None, POI B → OSM #123 (RESOLVED)
```

---

## STAGE 10: Data Export

**Duration:** ~1-2 minutes
**Type:** Multiprocessing (per poi_code)

Export matched POI data to multiple formats:

**Formats:**
- CSV: `poi_address.csv`, `poi_common.csv`
- GeoJSON: `poi_address.geojson`
- OSM XML: `poi_address.osm` (for import to OpenStreetMap)

**Export by poi_code:**
- Separate file per provider (e.g., `poi_address_hualdi.csv`)
- Tagged with dataset metadata

**Validation Rules:**
- CSV quoting: `QUOTE_ALL` (handles embedded commas)
- GeoJSON: valid FeatureCollection
- OSM XML: valid XML structure

**Output:**
- CSV files in output directory
- GeoJSON files for mapping/visualization
- OSM XML files for potential import

---

## Quality Metrics Summary

### Per-Stage Metrics

| Stage | Metric | Target | Alert |
|-------|--------|--------|-------|
| 2 | Harvest error rate | < 1% | > 5% |
| 8 | Matching success | 85-95% | < 80% |
| 9 | Conflict resolution | 100% | > 0 unresolved |
| Export | CSV validity | 100% | Invalid format |

### Data Quality Thresholds

- **Missing coordinates:** < 1%
- **Missing postcode:** < 5%
- **Duplicate osm_ids:** 0 (after Stage 9)
- **Invalid postcodes:** 0 (4-digit only)
- **Out-of-bounds coordinates:** 0 (Hungary only)

---

## Error Handling

### Recovery Strategy

| Failure | Stage | Action |
|---------|-------|--------|
| API timeout | 2 (harvest) | Retry 3x, skip provider |
| No OSM match | 8 (matcher) | Mark as new, continue |
| Postcode missing | 6 (enhancement) | Leave empty, log warning |
| Duplicate osm_id | 9 (conflict) | Reassign farthest, retry |
| Export failed | 10 (export) | Retry 3x, alert operator |

---

## Performance Optimization

### Memory-Efficient Processing

- **STAGE 8**: Chunk processing to avoid loading full dataset
- **STAGE 9**: In-memory haversine (not DB queries) for speed
- **Export**: Per-poi_code parallel writing

### Bottlenecks

1. **STAGE 8 (matcher)**: 20-30 min (OSM database queries)
2. **STAGE 2 (harvest)**: 10-15 min (external API calls)
3. **Export**: 1-2 min (I/O operations)

---

## Next Steps / Future Improvements

- [ ] Unit tests for all stages
- [ ] Data lineage tracking (which data source for each record)
- [ ] Monitoring & alerting dashboard
- [ ] Configuration file for thresholds
- [ ] Automated rollback on critical failures
