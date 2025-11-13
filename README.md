# PCR Reports - Multi-Database Error Report Generation

Error report generation system for Quest/Notts/Vira PCR laboratory databases with CSV-driven categorization.

## Overview

This repository contains tools for generating comprehensive error reports from PCR laboratory databases. The system uses **client-specific CSV mappings** to categorize errors across multiple laboratory databases, providing flexible, maintainable categorization without hardcoding rules.

**Key Features:**
- **Multi-database support**: QST (Quest), Notts (Nottingham), Vira (Viracor) with database-specific CSV mappings
- **Hybrid categorization**: CSV lookups + runtime classification checks
- **O(1) performance**: Fast dictionary lookups for categorization
- **Multiple export formats**: JSON, HTML (with interactive graphs), XLSX
- **Executive summaries**: High-level insights with interactive pie charts
- **Client-customizable**: Update CSV files without changing code
- **Production-tested**: Handles 100,000+ wells across three production databases

## Directory Structure

```
reports/
├── extract_report_with_curves.py       # Universal JSON extractor (sample / control / discrepancy / combined)
├── generate_report_from_json_with_graphs.py  # Universal HTML renderer
├── generate_xlsx_from_json.py          # XLSX export (four sheets with mix summary)
├── generate_summary_report.py          # Executive summary with pie charts
├── generate_full_report.py             # Wrapper for complete workflow
├── non_inverted_sigmoid_gen_html.py    # Sigmoid filtering HTML
├── import_qst_data.py                  # Quest CSV → SQLite loader
├── utils/report_helpers.py             # Shared helpers (DB, curve extraction, control mapping)
└── docs/                               # Reporting-specific documentation
```

Legacy scripts remain available under `reports/archive/2025-10-10/` if you need to reference the old per-report generators.

## Quick Start

### 1. Generate Complete Report Bundle

**Unified date filtering (applies to samples, controls, and discrepancies):**

```bash
# Simple: single date range for all reports
python3 -m reports.generate_full_report \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --since-date 2024-06-01 \
  --until-date 2025-01-31 \
  --output output_data/qst_production
```

This creates:
- `qst_production.json` - Raw data
- `qst_production.html` - Full interactive report
- `qst_production.xlsx` - Excel export with summary sheet
- `qst_production_summary.html` - Executive summary with charts

**Note:** The new `--since-date` and `--until-date` flags apply to **ALL** reports (samples, controls, AND discrepancies), making it easy to extract a consistent date range across all report types.

### 2. Date Range Filtering

The reporting system supports two approaches to date filtering:

#### Unified Date Filtering (Recommended)

Use `--since-date` and `--until-date` to apply the same date range to **all** report types:

```bash
python3 -m reports.generate_full_report \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --since-date 2024-06-01 \
  --until-date 2025-01-31 \
  --output output_data/qst_report
```

This applies the date range to:
- ✅ Sample errors
- ✅ Control errors
- ✅ Classification discrepancies

#### Fine-Grained Date Filtering

Use report-specific flags to set different dates for each report type:

```bash
python3 -m reports.generate_full_report \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --sample-since-date 2024-06-01 \
  --control-since-date 2024-06-01 \
  --discrepancy-since-date 2024-01-01 \
  --output output_data/qst_report
```

**Note:** Fine-grained filters override unified filters when both are specified.

### 3. Step-by-Step Workflow

#### Extract Data to JSON

```bash
python3 -m reports.extract_report_with_curves combined \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --output output_data/combined_report.json \
  --since-date 2024-06-01 \
  --until-date 2025-01-31
```

#### Generate HTML Report

```bash
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/combined_report.json \
  --output output_data/unified_report.html \
  --max-per-category 0
```

#### Generate Executive Summary

```bash
python3 -m reports.generate_summary_report \
  --json output_data/combined_report.json \
  --output output_data/summary_report.html
```

#### Generate Excel Export

```bash
python3 -m reports.generate_xlsx_from_json \
  --json output_data/combined_report.json \
  --output output_data/combined_report.xlsx
```

## Unified JSON Extractor

The JSON extractor supports both unified and fine-grained date filtering. Run with module syntax so imports resolve correctly:

```bash
python3 -m reports.extract_report_with_curves <subcommand> [options]
```

### Subcommands & Key Options

| Subcommand | Purpose | Key Flags |
| --- | --- | --- |
| `sample` | Sample error report JSON | `--db PATH`, `--since-date YYYY-MM-DD`, `--until-date YYYY-MM-DD`, `--include-label-errors`, `--max-controls` |
| `control` | Control error report JSON | `--db PATH`, `--since-date YYYY-MM-DD`, `--until-date YYYY-MM-DD`, `--suppress-unaffected-controls`, `--max-controls` |
| `discrepancy` | QST discrepancy JSON | `--db PATH`, `--since-date YYYY-MM-DD`, `--until-date YYYY-MM-DD`, `--date-field {upload,extraction}`, `--max-controls` |
| `combined` | Builds sample+control+discrepancy payload in a single JSON | All date filters plus `--exclude-from-sop`, `--exclude-from-control` |

### Example: Sample-Only Report with Unified Dates

```bash
python3 -m reports.extract_report_with_curves sample \
  --db ~/dbs/quest_prod_aug2025.db \
  --output output_data/sample_report.json \
  --since-date 2024-06-01 \
  --until-date 2025-01-31 \
  --max-controls 3
```

### Example: Combined JSON with Fine-Grained Dates

```bash
python3 -m reports.extract_report_with_curves combined \
  --db ~/dbs/quest_prod_aug2025.db \
  --output output_data/combined_report.json \
  --sample-since-date 2024-06-01 --sample-until-date 2025-01-31 \
  --control-since-date 2024-06-01 --control-until-date 2025-01-31 \
  --discrepancy-since-date 2024-01-01 --discrepancy-date-field extraction \
  --exclude-from-control "%SIGMOID%" "%WESTGARDS%" \
  --suppress-unaffected-controls
```

### Date Filtering Reference

**Unified filters** (apply to all reports):
- `--since-date YYYY-MM-DD` - Start date for all reports
- `--until-date YYYY-MM-DD` - End date for all reports

**Fine-grained filters** (override unified filters):
- `--sample-since-date` / `--sample-until-date` - Sample-specific dates
- `--control-since-date` / `--control-until-date` - Control-specific dates
- `--discrepancy-since-date` / `--discrepancy-until-date` - Discrepancy-specific dates
- `--discrepancy-date-field {upload,extraction}` - Which date to filter on (default: upload)

### Date Fields Used (Important!)

Different report types use different SQL date columns:

| Report | Date Used | Column | Meaning |
|--------|-----------|--------|---------|
| **Sample** | Extraction | `w.created_at` | When the well was created/processed |
| **Control** | Extraction | `w.created_at` (or `pw.created_at` for parent) | When the control well was created |
| **Discrepancy** | Configurable | `r.created_at` (upload, default) OR `w.created_at` (extraction) | Upload date or extraction date |

**Important:** Discrepancies default to **upload date** while samples/controls use **extraction date**. This means results uploaded on 2024-06-01 but extracted on 2024-05-01 will appear in Discrepancy reports but NOT in Sample/Control reports.

**Recommendation:** For consistent reporting across all types, use:
```bash
--since-date 2024-06-01 --until-date 2025-01-31 \
--discrepancy-date-field extraction
```

This makes all three reports filter by extraction date.

## Processing Options

### Error Exclusion Filters

Exclude specific error codes from reports using pattern matching (supports wildcards):

```bash
python3 -m reports.generate_full_report \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --since-date 2024-06-01 --until-date 2025-01-31 \
  --exclude-from-sop "%SIGMOID%" \
  --exclude-from-control "%SIGMOID%" "%WESTGARDS%" \
  --output output_data/qst_report
```

**Flags:**
- `--exclude-from-sop ERROR_CODE [ERROR_CODE ...]` - Exclude patterns from sample SOP report
- `--exclude-from-control ERROR_CODE [ERROR_CODE ...]` - Exclude patterns from control report and affected samples

**Examples:**
- `"%SIGMOID%"` - Excludes any error code containing "SIGMOID"
- `"WESTGARDS_FAILED"` - Excludes exact error code match

### Control Report Optimization

```bash
--suppress-unaffected-controls
```

Excludes control errors that have no associated affected samples. This reduces clutter in control reports when controls pass but have minor configuration issues.

### Sample Error Options

```bash
--sample-include-label-errors
```

Include label/setup errors in the sample report (excluded by default as they typically affect entire plates, not individual samples).

### Performance and Testing

```bash
--max-controls 5              # Limit control curves extracted per target (default: 3)
--limit 100                   # Limit total records processed (useful for testing)
--test                        # Test mode: limits to 100 records automatically
```

## HTML Renderer Features

- **Collapsible sections** when rendering combined payloads (Sample / Control / Discrepancy)
- **Interactive graphs** for each well with control overlays
- **Responsive auto-resize**: embedded reports notify the wrapper whenever content expands
- **Updated controls**: per-mix "Show Controls" buttons with green highlight
- **Table of Contents**: Quick navigation to specific mixes
- **Appendix**: Control errors with affected samples

```bash
# Render a single-report JSON
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/sample_report_since20240601.json \
  --report-type sample \
  --output output_data/sample_report_since20240601_unlimited.html \
  --max-per-category 0
```

## XLSX Export

Turns a combined JSON into an Excel workbook with four sheets:

1. **Sample SOP Errors** - All sample errors with metadata, CT values, and comments
2. **Control Errors** - All control errors with metadata, CT values, and comments
3. **Classification Errors** - Classification discrepancies with machine/final classifications
4. **Valid Results Summary** - By-mix summary with:
   - Valid sample counts (Detected/Not Detected)
   - Control pass rates
   - Error statistics (SOP, Control, Classification errors with affected vs ignored breakdowns)
   - Samples affected by control errors

```bash
python3 -m reports.generate_xlsx_from_json \
  --json output_data/unified_report_since20240601.json \
  --output output_data/unified_report_since20240601.xlsx
```

## Executive Summary Report

Generate a standalone executive summary with interactive pie charts for quick insights:

```bash
python3 -m reports.generate_summary_report \
  --json output_data/combined_report.json \
  --output output_data/summary_report.html
```

**Features:**
- **Overall Sample Summary** - Total samples breakdown with controls context
- **Error Type Summaries** - SOP, Control, and Classification errors with pie charts
- **Mix Family Analysis** - Individual pie charts for each mix family (ADV, BKV, CMV, COVID, EBV, etc.)
- **Clean Design** - Professional cards, percentages, and interactive Chart.js visualizations
- **Fast Generation** - Instant from existing JSON (no database queries)

**Use Case:** Share the executive summary with stakeholders for high-level insights, then provide the detailed unified report and XLSX for deep-dive analysis.

## CSV-Driven Multi-Database Categorization

### Overview

The reporting system uses **client-specific CSV mappings** to categorize errors across multiple laboratory databases.

### Database-Specific CSV Mappings

Each database has its own categorization CSV that defines how to classify wells based on error patterns:

| Database | CSV File | Records | Wells Categorized |
|----------|----------|---------|-------------------|
| **QST** (Quest) | `output_data/qst_category_mapping_v4_ac.csv` | 179 | 456,665 |
| **Notts** (Nottingham) | `output_data/notts_category_mapping_v1_ac.csv` | 103 | 43,619 |
| **Vira** (Viracor) | `output_data/vira_category_mapping_v1_ac.csv` | 156 | 55,673 |

**CSV Structure:**
```csv
WELL_TYPE,ERROR_CODE,ERROR_MESSAGE,RESOLUTION_CODES,WELL_LIMS_STATUS,OCCURRENCE_COUNT,CATEGORY,NOTES
SAMPLE,,,[],NOT DETECTED,280100,VALID_NOT_DETECTED,No error - NOT DETECTED
CONTROL,FAILED_POS_WELL,The positive control has failed.,[],,32,SOP_UNRESOLVED,FAILED_POS_WELL - Unresolved
SAMPLE,CLSDISC_WELL,Classification discrepancy,[BLA],DETECTED,1250,DISCREP_IGNORED,BLA pattern - error ignored
```

**Lookup Key:** `(WELL_TYPE, ERROR_CODE, RESOLUTION_CODES, WELL_LIMS_STATUS)`

### Category Taxonomy

**Classification Discrepancies (3+1):**
- `DISCREP_IN_ERROR` - Unresolved classification discrepancy
- `DISCREP_IGNORED` - Classification error ignored, result reported
- `DISCREP_RESULT_CHANGED` - Classification error, test repeated
- `DISCREP_NEEDS_CLS_DATA` - BLA resolution, needs machine_cls/final_cls comparison

**SOP Errors (3):**
- `SOP_UNRESOLVED` - Sample/control error unresolved
- `SOP_IGNORED` - Error ignored, result reported
- `SOP_REPEATED` - Test repeated/excluded

**Valid Results (4):**
- `VALID_DETECTED` - No error, DETECTED result
- `VALID_NOT_DETECTED` - No error, NOT DETECTED result
- `VALID_CONTROL` - No error, control passed
- `VALID_OTHER` - No error, other LIMS status

**Special Categories (2):**
- `CONTROL_AFFECTED_SAMPLE` - INHERITED_*_FAILURE (for Controls appendix)
- `IGNORE_WELL` - Wells to ignore (MIX_MISSING, UNKNOWN_MIX, etc.)

### Using CSV-Driven Categorization

Specify the database type when running report extraction:

```bash
# QST/Quest database with automatic CSV mapping
python3 -m reports.extract_report_with_curves combined \
  --db-type qst \
  --db ~/dbs/quest_prod.db \
  --output output_data/qst_report.json \
  --sample-since-date 2024-06-01

# Nottingham database with automatic CSV mapping
python3 -m reports.extract_report_with_curves combined \
  --db-type notts \
  --db ~/dbs/notts.db \
  --output output_data/notts_report.json \
  --sample-since-date 2024-06-01

# Viracor database with automatic CSV mapping
python3 -m reports.extract_report_with_curves combined \
  --db-type vira \
  --db ~/dbs/vira.db \
  --output output_data/vira_report.json \
  --sample-since-date 2024-06-01
```

The `--db-type` parameter automatically loads the correct CSV mapping file.

### Hybrid Discrepancy Categorization

Discrepancies require **hybrid categorization** because CSV lookups alone cannot determine if a classification result actually changed.

**Two-Stage Process:**

1. **CSV Lookup** - Get initial category based on error pattern
2. **Runtime Classification Check** - For BLA patterns, compare machine_cls vs final_cls to determine if result actually changed

**Why Hybrid?**
- CSV provides error pattern context (BLA, CLSDISC_WELL, etc.)
- Runtime check determines if classification actually changed
- Handles database-specific classification data not in CSV

### Updating CSV Mappings

To update categorization for a specific database:

1. **Generate new template** (analyzes all error patterns in database):
   ```bash
   python3 analyze_categories.py \
     --db-type qst \
     --db ~/dbs/quest_prod.db \
     --output output_data/qst_category_mapping_TEMPLATE.csv
   ```

2. **Review and categorize** - Edit the TEMPLATE file:
   - Assign `CATEGORY` for each error pattern
   - Add `NOTES` to document decisions
   - Mark unclear patterns as `NEEDS_REVIEW`

3. **Apply corrections** (optional):
   ```bash
   python3 recategorize_categories.py \
     --input output_data/qst_category_mapping_TEMPLATE.csv \
     --output output_data/qst_category_mapping_v5.csv \
     --corrections-csv output_data/qst_category_mapping_v4_ac.csv
   ```

4. **Test with new CSV**:
   ```bash
   python3 -m reports.extract_report_with_curves combined \
     --db-type qst \
     --db ~/dbs/quest_prod.db \
     --output output_data/qst_test.json \
     --sample-since-date 2024-06-01
   ```

### Performance

CSV-driven categorization provides **O(1) lookups** with minimal overhead:

- **Load time**: ~10ms to load CSV into dictionary
- **Lookup time**: <1μs per well
- **Memory**: ~500KB per CSV file (in-memory dictionary)
- **Production**: Processes 100,000+ wells in ~3 minutes (I/O bound, not CPU)

## Data Loading

Use this when you need to refresh the QST discrepancy database from CSV extracts:

```bash
python3 -m reports.import_qst_data \
  --csv ~/dbs/qst_prod-discreps-newcolumns.csv \
  --db qst_discreps.db \
  --reset
```

The script normalises JSON readings into discrete columns and creates supporting tables (`qst_readings`, `qst_controls`, `qst_other_observations`).

## Non-Inverted Sigmoid Utilities

- `reports/non_inverted_sigmoid_gen_html.py`: Generates detailed HTML summary with per-run cards, control counts, and target breakdowns for filtering out inverted sigmoid Parvo/HHV6 curves.

```bash
python3 -m reports.non_inverted_sigmoid_gen_html \
  --db ~/dbs/quest_prod_aug2025.db \
  --output output_data/non_inverted_sigmoid_report.html
```

## Dependencies

- Python 3.x
- SQLite3
- NumPy
- openpyxl (for XLSX export)
- argparse (command line parsing)

## Documentation

- `reports/docs/` - Combined reporting plans, findings, tasks, and resolution code references
- `CSV_DRIVEN_CATEGORIZATION_PLAN.md` - Implementation plan and technical details
- `OPTIMAL_IMPLEMENTATION_PLAN.md` - Reports implementation strategy
- `DISCREPANCY_FIX_PLAN.md` - Reports fix planning
- `REORGANIZATION_SUMMARY.md` - Documents 2025-10-10 reorganization

## Files Reference

**Implementation:**
- `reports/extract_report_with_curves.py` - CSV loading and hybrid categorization logic
- `reports/utils/report_helpers.py` - Shared CSV lookup utilities

**CSV Mappings:**
- `output_data/qst_category_mapping_v4_ac.csv` - Quest production mappings
- `output_data/notts_category_mapping_v1_ac.csv` - Nottingham production mappings
- `output_data/vira_category_mapping_v1_ac.csv` - Viracor production mappings

**Tools:**
- `analyze_categories.py` - Generate CSV templates from database analysis
- `recategorize_categories.py` - Apply corrections and validate CSV mappings
- `compare_reference_tables.py` - Compare reference data across databases
- `compare_test_results.py` - Compare test results
- `check_*.py` - Various validation scripts
- `category_lookup.py` - Category lookup class
- `database_configs.py` - Database configuration

## License

[Add your license information here]
