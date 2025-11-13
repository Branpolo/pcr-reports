# New Features - Report Filtering and Exclusion System

## Overview

This document describes the new filtering and exclusion features added to the unified JSON extractor (`unified_json_extractor.py`). These features provide fine-grained control over which errors appear in different report sections and how data is filtered.

**Date Added**: October 2025
**Last Updated**: 2025-10-21

---

## Feature 1: Flexible Error Code Exclusion with Wildcard Support

### Description
Allows custom exclusion of specific error codes from SOP sample reports and/or control reports using exact matches or wildcard patterns.

### Command-Line Arguments

#### `--exclude-from-sop ERROR_CODE [ERROR_CODE ...]`
Exclude specified error codes from the SOP sample report only. These errors will still appear in control and discrepancy reports if applicable.

**Supports wildcards**: Use `%` as a wildcard (SQL LIKE syntax)

**Examples**:
```bash
# Exclude any error code containing "SIGMOID"
--exclude-from-sop "%SIGMOID%"

# Exclude multiple specific error codes
--exclude-from-sop "CONTROL_SIGMOID_FAILURE" "SAMPLE_SIGMOID_ERROR"

# Combine exact and wildcard patterns
--exclude-from-sop "EXACT_ERROR" "%WILDCARD%"
```

#### `--exclude-from-control ERROR_CODE [ERROR_CODE ...]`
Exclude specified error codes from BOTH the control report AND affected samples. This completely removes these error types from the pipeline.

**Supports wildcards**: Use `%` as a wildcard (SQL LIKE syntax)

**Examples**:
```bash
# Completely remove SIGMOID errors from control report and affected samples
--exclude-from-control "%SIGMOID%"

# Exclude multiple types
--exclude-from-control "%SIGMOID%" "%CURVE_FIT%"
```

### Default Auto-Exclusions

Some error codes are automatically excluded from the SOP sample report to prevent double-counting:

**Auto-excluded from SOP sample report**:
- `CLSDISC_WELL` - Classification discrepancy (well level)
- `CONTROL_CLSDISC_WELL` - Control classification discrepancy (well level)
- `CONTROL_CLSDISC_TARGET` - Control classification discrepancy (target level)
- `RQ_CLS` - Classification RQ error

**Note**: CT discrepancy errors (CTDISC_WELL, etc.) are NOT auto-excluded. They will appear in the SOP report unless explicitly excluded.

### Implementation Details

- **Wildcard matching**: Uses SQL `LIKE` operator with `%` wildcards
- **Case-sensitive**: Error code matching is case-sensitive
- **Cumulative**: Default exclusions + custom exclusions are combined
- **SQL-based**: Exclusions are applied directly in SQL queries for efficiency

---

## Feature 2: Suppress Unaffected Control Errors

### Description
Filters out control errors that have no associated affected samples, reducing noise in control reports while maintaining focus on controls that actually impacted sample results.

### Command-Line Argument

#### `--suppress-unaffected-controls`
Flag to enable suppression of control errors with no affected samples.

**Example**:
```bash
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest_prod_aug2025.db \
  --output output.json \
  --sample-since-date 2024-06-01 \
  --control-since-date 2024-06-01 \
  --suppress-unaffected-controls
```

### Behavior

**Without flag**:
- All control errors are included in the report
- Many controls may have no associated affected samples

**With flag**:
- Only control errors that have affected samples are included
- Affected samples are those with:
  - `INHERITED_CONTROL_FAILURE`
  - `INHERITED_EXTRACTION_FAILURE`
  - Associated with the control via `control_well_id`

**Console Output**:
When suppression occurs, you'll see output like:
```
Suppressed 942 control errors with no affected samples
```

### Use Cases

1. **Focused reports**: Show only controls that actually impacted results
2. **Root cause analysis**: Identify which control failures propagated to samples
3. **Cleaner output**: Remove "noise" from controls that failed but didn't affect any samples

---

## Feature 3: Site Filtering

### Description
Filter entire dataset by site ID(s), limiting results to specific laboratory locations.

### Command-Line Argument

#### `--site-ids SITE_ID [SITE_ID ...]`
Filter by one or more site IDs. Only include wells from these sites.

**Examples**:
```bash
# Single site
--site-ids "9959fde1-21bf-454b-9da4-a6b7c7368986"

# Multiple sites
--site-ids "9959fde1-21bf-454b-9da4-a6b7c7368986" "other-site-id"
```

### Implementation Details

- **Applied globally**: Site filter is applied to all SQL queries (sample, control, discrepancy, valid results)
- **SQL IN clause**: Uses efficient `IN` clause for multiple site IDs
- **Table aliases**: Properly handles different table aliases (`w.site_id`, `pw.site_id`, etc.)

### Finding Site IDs

To find site IDs in your database:

```sql
-- Quest database (qst)
SELECT DISTINCT site_id, site_name
FROM wells
WHERE site_name LIKE '%search term%';

-- Example: Find San Juan Capistrano site
SELECT DISTINCT site_id, site_name
FROM wells
WHERE site_name LIKE '%San Juan Capistrano%';
-- Result: 9959fde1-21bf-454b-9da4-a6b7c7368986
```

---

## Feature 4: UNKNOWN_MIX Auto-Exclusion

### Description
`UNKNOWN_MIX` errors are now automatically excluded from both sample and control reports as they typically represent data quality issues rather than actionable errors.

### Implementation
- Added `UNKNOWN_MIX` to default exclusion lists for all three database types (QST, Notts, Vira)
- Applied to both SOP sample exclusions AND control exclusions
- Cannot be overridden (hard-coded exclusion)

---

## Complete Usage Examples

### Example 1: Comprehensive Filtering for Specific Site

Generate a report for San Juan Capistrano site, excluding SIGMOID errors and suppressing unaffected controls:

```bash
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest_prod_aug2025.db \
  --output output_data/san_juan_report.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31 \
  --exclude-from-sop "%SIGMOID%" \
  --exclude-from-control "%SIGMOID%" \
  --suppress-unaffected-controls \
  --site-ids "9959fde1-21bf-454b-9da4-a6b7c7368986"
```

### Example 2: Clean Control Report

Generate a control report excluding multiple error patterns and unaffected controls:

```bash
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest_prod_aug2025.db \
  --output output_data/clean_controls.json \
  --control-since-date 2024-06-01 \
  --exclude-from-control "%SIGMOID%" "%CURVE_FIT%" \
  --suppress-unaffected-controls
```

### Example 3: SOP Report with Custom Exclusions

Generate SOP sample report excluding specific error types:

```bash
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest_prod_aug2025.db \
  --output output_data/sop_custom.json \
  --sample-since-date 2024-06-01 \
  --exclude-from-sop "CONTROL_SIGMOID_FAILURE" "SAMPLE_LOW_VOLUME" \
  --limit 1000
```

### Example 4: Generate All Output Formats

After generating JSON, create HTML and XLSX reports:

```bash
# 1. Generate JSON with filters
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest_prod_aug2025.db \
  --output output_data/report.json \
  --sample-since-date 2024-06-01 \
  --control-since-date 2024-06-01 \
  --discrepancy-since-date 2024-06-01 \
  --exclude-from-sop "%SIGMOID%" \
  --suppress-unaffected-controls

# 2. Generate combined HTML report
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/report.json \
  --output output_data/report_combined.html

# 3. Generate summary HTML report with pie charts
python3 -m reports.generate_summary_report \
  --json output_data/report.json \
  --output output_data/report_summary.html

# 4. Generate XLSX spreadsheet
python3 -m reports.generate_xlsx_from_json \
  --json output_data/report.json \
  --output output_data/report.xlsx
```

---

## Integration Test Results

A comprehensive integration test was performed on 2025-10-21 with the following parameters:

**Test Configuration**:
- Database: Quest production (June 2024 - May 2025)
- Site: EZ - San Juan Capistrano CA
- Exclusions: %SIGMOID% from both SOP and control
- Suppress unaffected controls: Enabled
- Limit: 500 errors per category

**Results**:
```
=== Site Filtering ===
✓ Found 38 mixes (vs thousands in full dataset)
✓ Total samples: 101,229

=== Control Error Suppression ===
✓ Suppressed 942 control errors with no affected samples
✓ Kept 62 control errors with affected samples
✓ Samples affected by control errors: 7,863

=== Error Breakdown ===
✓ SOP errors: 1,500 (1,000 affected, 500 ignored)
✓ Control errors: 62 (all affected)
✓ Classification errors: 1,218 (718 affected, 500 ignored)

=== Output Files Generated ===
✓ JSON: output_data/integration_test/qst_integration.json
✓ Combined HTML: output_data/integration_test/qst_integration_combined.html
✓ Summary HTML: output_data/integration_test/qst_integration_summary.html
✓ XLSX: output_data/integration_test/qst_integration.xlsx
```

---

## Technical Implementation Notes

### SQL Injection Prevention
- All site IDs are properly quoted in SQL IN clauses
- Wildcard patterns are used with parameterized queries where possible
- User input is validated before SQL generation

### Performance Considerations
- Site filtering is applied at the SQL level for efficiency
- Wildcard exclusions use SQL LIKE operator (indexed when possible)
- Control suppression is performed post-query to maintain relationship integrity

### Database Compatibility
All features are implemented for all three database types:
- **QST** (Quest)
- **Notts** (Nottingham)
- **Vira** (Viracore)

Each database has its own LIMS pattern detection and schema handling, but exclusion and filtering logic is consistent across all types.

---

## Troubleshooting

### Issue: No data returned after site filtering
**Solution**: Verify the site_id exists in your database using:
```sql
SELECT DISTINCT site_id, site_name FROM wells LIMIT 100;
```

### Issue: Wildcard exclusion not working
**Solution**:
- Ensure you're using `%` wildcards (not `*`)
- Error codes are case-sensitive
- Verify error code naming in your database

### Issue: Suppressed control count seems high
**Solution**: This is normal. Many control failures don't propagate to samples due to:
- Control failures caught before sample processing
- Controls for mixes with no samples
- Classification-only control issues

---

## See Also

- **Main Documentation**: `reports/instructions.md`
- **Database Configuration**: `database_configs.py`
- **Report Helpers**: `reports/utils/report_helpers.py`
- **JSON Extractor**: `reports/unified_json_extractor.py`

---

## Version History

### v2.0 (October 2025)
- Added wildcard pattern support for error exclusions
- Added `--exclude-from-sop` flag
- Added `--exclude-from-control` flag
- Added `--suppress-unaffected-controls` flag
- Added `--site-ids` flag for site filtering
- Fixed CTDISC auto-exclusion bug (now only CLSDISC is auto-excluded)
- Added UNKNOWN_MIX to default exclusions
- Implemented site filtering across all 13 SQL queries
- Added comprehensive integration testing

### v1.0 (Prior)
- Initial unified JSON extractor implementation
- Basic date filtering
- Database type support (QST, Notts, Vira)
