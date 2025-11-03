# Quest/Notts/Vira Report Generation Instructions

This document provides step-by-step instructions for generating complete report suites for Quest, Notts, and Vira databases.

## 🆕 New Features (October 2025)

For detailed documentation on new filtering and exclusion features, see **[NEW_FEATURES.md](NEW_FEATURES.md)**.

**Quick reference**:
- `--exclude-from-sop "%PATTERN%"` - Exclude error codes from SOP sample report (supports wildcards)
- `--exclude-from-control "%PATTERN%"` - Exclude error codes from control report and affected samples
- `--suppress-unaffected-controls` - Hide control errors with no affected samples
- `--site-ids "site-id"` - Filter entire dataset by site location(s)

## Overview

The reports system generates comprehensive error analysis reports in multiple formats:
- **JSON**: Complete data payload with all errors, curves, and statistics
- **XLSX**: Excel workbook with organized error data
- **Unified HTML**: Interactive HTML with detailed error records and curve visualizations
- **Summary HTML**: Executive summary with pie charts and overall statistics

All three formats are generated from a single JSON source file using the scripts in this directory.

## Database Locations

- **Quest**: `/home/azureuser/code/wssvc-flow-codex/input/quest.db`
- **Notts**: `/home/azureuser/code/wssvc-flow-codex/input/notts.db`
- **Vira**: `/home/azureuser/code/wssvc-flow-codex/input/vira.db`

## ⚡ ONE-COMMAND PIPELINE (Recommended)

The **`generate_full_report.py`** script runs the entire pipeline in one command, generating JSON, HTML, and XLSX files automatically:

```bash
cd /home/azureuser/code/wssvc-flow-codex

# Notts report (auto-generates output_data/notts_report.{json,html,xlsx})
python3 -m reports.generate_full_report \
  --db-type notts \
  --db input/notts.db \
  --limit 100

# Quest report with base output path and filters
python3 -m reports.generate_full_report \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-01-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-01-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-01-31 \
  --exclude-from-sop "%SIGMOID%" \
  --exclude-from-control "%SIGMOID%" \
  --suppress-unaffected-controls

# Vira report with individual output paths (fine control)
python3 -m reports.generate_full_report \
  --db-type vira \
  --db input/vira.db \
  --json output_data/vira_custom.json \
  --html output_data/vira_custom.html \
  --xlsx output_data/vira_custom.xlsx
```

**What it does:**
1. Generates JSON data using `unified_json_extractor.py`
2. Generates detailed HTML report using `generate_report_from_json_with_graphs.py`
3. Generates XLSX export using `generate_xlsx_from_json.py`
4. Generates executive summary HTML using `generate_summary_report.py`

**Output options:**
- Use `--output <base_path>` to set a base path (generates `.json`, `.html`, `.xlsx`, `_summary.html` files)
- Use individual `--json`, `--html`, `--xlsx`, `--summary` flags for fine-grained control
- If no output flags specified, auto-generates: `output_data/<db_name>_report.{json,html,xlsx}` and `_summary.html`

**All JSON extractor flags are supported:**
- Date filters: `--sample-since-date`, `--control-since-date`, `--discrepancy-since-date`, etc.
- Exclusions: `--exclude-from-sop`, `--exclude-from-control`, `--suppress-unaffected-controls`
- Filtering: `--site-ids`, `--limit`, `--max-controls`
- Options: `--sample-include-label-errors`, `--test`

If you need more control over individual steps, see the step-by-step instructions below.

---

## Quick Start: Generate Combined Report (Step-by-Step)

### Step 1: Generate JSON Report

```bash
cd /home/azureuser/code/wssvc-flow-codex

python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31
```

### Step 2: Generate HTML and XLSX Reports from JSON

```bash
# Generate unified HTML report
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_unified.html

# Generate XLSX report
python3 -m reports.generate_xlsx_from_json \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_unified.xlsx

# Generate executive summary HTML
python3 -m reports.generate_summary_report \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_summary.html
```

This generates four files:
- `qst_unified.json` - Complete JSON report with all errors and curve data
- `qst_unified.html` - Interactive unified HTML report with detailed errors
- `qst_unified.xlsx` - Excel workbook with organized data
- `qst_summary.html` - Executive summary with pie charts and statistics

## Full Database-Specific Examples

### Quest Database (Complete Workflow)

```bash
cd /home/azureuser/code/wssvc-flow-codex

# Step 1: Generate JSON
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31

# Step 2: Generate all outputs from JSON
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_unified.html

python3 -m reports.generate_xlsx_from_json \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_unified.xlsx

python3 -m reports.generate_summary_report \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_summary.html
```

### Notts Database (Example)

```bash
cd /home/azureuser/code/wssvc-flow-codex

python3 -m reports.unified_json_extractor \
  --db-type notts \
  --db input/notts.db \
  --output output_data/final/notts_unified.json \
  --sample-since-date 2024-01-01 \
  --discrepancy-since-date 2024-01-01

# Then generate HTML/XLSX as shown above
```

### Vira Database (Example)

```bash
cd /home/azureuser/code/wssvc-flow-codex

python3 -m reports.unified_json_extractor \
  --db-type vira \
  --db input/vira.db \
  --output output_data/final/vira_unified.json \
  --sample-since-date 2024-01-01 \
  --discrepancy-since-date 2024-01-01

# Then generate HTML/XLSX as shown above
```

## Detailed Parameter Explanations

### JSON Extraction (`unified_json_extractor`)

#### Required Parameters

- `--db-type {qst,notts,vira}`: Specifies which database type and categorization rules to use (default: qst)
- `--db DB`: Path to the SQLite database file
- `--output OUTPUT`: Path for the generated JSON report

#### Date Range Parameters

- `--sample-since-date YYYY-MM-DD`: Include samples extracted on or after this date
- `--sample-until-date YYYY-MM-DD`: Include samples extracted on or before this date
- `--control-since-date YYYY-MM-DD`: Include controls extracted on or after this date
- `--control-until-date YYYY-MM-DD`: Include controls extracted on or before this date
- `--discrepancy-since-date YYYY-MM-DD`: Include discrepancies on or after this date (default: 2024-01-01)
- `--discrepancy-until-date YYYY-MM-DD`: Include discrepancies on or before this date

#### Advanced Parameters

- `--limit LIMIT`: Limit number of records processed (useful for testing)
- `--max-controls MAX_CONTROLS`: Maximum controls per target (default: 3)
- `--sample-include-label-errors`: Include label/setup errors in sample report
- `--exclude-from-sop ERROR_CODE [ERROR_CODE ...]`: Additional error codes to exclude from SOP sample report (supports wildcards like `%SIGMOID%`). These errors will still appear in control and discrepancy reports if applicable.
- `--exclude-from-control ERROR_CODE [ERROR_CODE ...]`: Error codes to exclude from control report AND affected samples (supports wildcards like `%SIGMOID%`). Use this to completely remove specific error types from both control errors and their propagated effects.
- `--suppress-unaffected-controls`: Hide control errors that have no affected samples
- `--site-ids SITE_ID [SITE_ID ...]`: Filter entire dataset by site location(s)
- `--discrepancy-date-field {upload,extraction}`: Date field used for discrepancy filtering (default: upload)
- `--test`: Test mode (limits to 100 records)

**Note on Auto-Exclusions**: Classification discrepancy errors (CLSDISC_WELL, CONTROL_CLSDISC_WELL, CONTROL_CLSDISC_TARGET, RQ_CLS) are automatically excluded from the SOP sample report to prevent double-counting with the discrepancy report. Internal Control (IC) discrepancies are automatically excluded from classification error reports as they are no longer relevant.

### HTML Report Generation

Generate an interactive HTML report from JSON:

```bash
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_unified.html \
  --max-per-category 100
```

#### Parameters

- `--json JSON`: Path to the combined JSON report (required)
- `--output OUTPUT`: Output HTML file path (optional, default: auto-generated from JSON filename)
- `--max-per-category MAX`: Maximum records per category to display (default: 100, 0 for unlimited)

### XLSX Report Generation

Generate an Excel workbook from JSON:

```bash
python3 -m reports.generate_xlsx_from_json \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_unified.xlsx
```

#### Parameters

- `--json JSON`: Path to the combined JSON report (required)
- `--output OUTPUT`: Output XLSX file path (optional, default: auto-generated from JSON filename)

### Summary Report Generation

Generate an executive summary with pie charts:

```bash
python3 -m reports.generate_summary_report \
  --json output_data/final/qst_unified.json \
  --output output_data/final/qst_summary.html
```

#### Parameters

- `--json JSON`: Path to the combined JSON report (required)
- `--output OUTPUT`: Output HTML file path (optional, default: auto-generated from JSON filename)

## Output Files

After running the commands above, you'll have four files in `output_data/final/`:

| File | Purpose | Size | Format |
|------|---------|------|--------|
| `{db}_unified.json` | Complete data with errors, curves, statistics | Large (~35MB) | JSON |
| `{db}_unified.xlsx` | Organized error data in spreadsheet format | Medium (~1MB) | Excel |
| `{db}_unified.html` | Interactive HTML with detailed records and curves | Large (~15MB) | HTML |
| `{db}_summary.html` | Executive summary with pie charts | Small (~100KB) | HTML |

## Understanding the Reports

### JSON Report Structure

The JSON file contains:
- `generated_at`: Timestamp when report was generated
- `database`: Database file used
- `valid_results`: Summary of valid results per mix
- `error_statistics`: Error counts and affected samples per mix
- `reports`: Three main sections:
  - `sample`: Sample well errors
  - `control`: Control well errors
  - `discrepancy`: Classification discrepancies

### Unified HTML Report

Interactive report showing:
- All sample, control, and discrepancy errors
- Curve visualizations for each error
- Collapsible error details
- Filterable by category and mix type
- Target selectors for multiplex assays

### Summary HTML Report

Executive summary showing:
- Overall sample statistics
- Error type breakdowns (SOP, Control, Classification)
- Pie charts for each error category
- Per-mix-family summaries

## Filtering and Limiting

### Generate Report for Recent Data Only

```bash
# Only samples/controls extracted in the last 30 days
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_recent.json \
  --sample-since-date 2025-09-17 \
  --sample-until-date 2025-10-17 \
  --control-since-date 2025-09-17 \
  --control-until-date 2025-10-17 \
  --discrepancy-since-date 2025-09-17 \
  --discrepancy-until-date 2025-10-17
```

### Exclude Specific Error Codes from Reports

#### Exclude from SOP Sample Report Only

```bash
# Exclude specific error code from SOP sample report (exact match)
# (will still appear in control/discrepancy reports if applicable)
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31 \
  --exclude-from-sop INCORRECT_SIGMOID
```

#### Exclude Using Wildcard Patterns

```bash
# Exclude all SIGMOID-related errors from SOP sample report using wildcard
# This will match INCORRECT_SIGMOID, INCORRECT_POSITIVE_SIGMOID, etc.
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31 \
  --exclude-from-sop "%SIGMOID%"
```

#### Exclude from Control Report and Affected Samples

```bash
# Exclude from control report AND affected samples
# This completely removes the error type from both control errors and propagated effects
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31 \
  --exclude-from-control "%SIGMOID%"
```

#### Combine Multiple Exclusions with Control Suppression

```bash
# Recommended: Exclude errors and suppress unaffected controls
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-01-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-01-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-01-31 \
  --exclude-from-sop "%SIGMOID%" \
  --exclude-from-control "%SIGMOID%" \
  --suppress-unaffected-controls
```

**Use Case**: These options are useful when you want to:
- Suppress certain errors from the SOP sample report to avoid double-counting or to focus on specific error types
- Completely remove specific control errors and their downstream effects from all reports
- Hide control errors that didn't affect any samples (reduces noise in reports)
- Use wildcard patterns to match multiple related error codes (e.g., all SIGMOID-related errors)

### Generate Report with Limited Records (Testing)

```bash
# Test mode: generates only first 100 records
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_test.json \
  --test
```

## Discrepancy Report Details

The discrepancy report categorizes wells into three groups:

1. **Acted Upon**: Wells where clinical category was changed based on re-extraction
2. **Samples Repeated**: Wells where samples were repeated due to discrepancy
3. **Ignored**: Wells where errors were acknowledged but no action taken

> Note: Internal Control (IC) discrepancies are automatically excluded from classification error reports as they are no longer relevant after recent system changes.

## Troubleshooting

### Missing Database

```bash
# Verify database exists
ls -lh /home/azureuser/code/wssvc-flow-codex/input/quest.db
```

### Report Generation is Slow

This is normal for large databases. The JSON generation can take 5-10 minutes for a year of data. Use `--test` flag for quick validation:

```bash
python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_test.json \
  --test
```

### Output Directory Issues

Ensure output directory exists:

```bash
mkdir -p /home/azureuser/code/wssvc-flow-codex/output_data/final
```

### Invalid Date Format

Dates must be in YYYY-MM-DD format. Examples:
- ✓ `2024-06-01`
- ✗ `06/01/2024`
- ✗ `2024-6-1`

## Report Generation Timeline

Typical timings for full database reports:

| Stage | Duration |
|-------|----------|
| JSON extraction | 3-5 minutes |
| HTML generation | 1-2 minutes |
| XLSX generation | <1 minute |
| Summary HTML | <1 minute |
| **Total** | ~5-8 minutes |

## Performance Tips

1. **Use date ranges**: Restrict to recent data when testing changes
2. **Background execution**: Run long reports in background with `nohup`

```bash
nohup python3 -m reports.unified_json_extractor \
  --db-type qst \
  --db input/quest.db \
  --output output_data/final/qst_unified.json \
  --sample-since-date 2024-06-01 \
  --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 \
  --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 \
  --discrepancy-until-date 2025-05-31 \
  --exclude-from-sop "%SIGMOID%" \
  --exclude-from-control "%SIGMOID%" \
  --suppress-unaffected-controls > report_generation.log 2>&1 &
```

## Quality Checks

After report generation, verify the outputs:

```bash
# Check JSON file exists and is valid
python3 -c "import json; json.load(open('output_data/final/qst_unified.json'))"

# Check file sizes make sense
ls -lh output_data/final/qst_*

# Quick validation: view report summary
python3 << 'EOF'
import json
data = json.load(open('output_data/final/qst_unified.json'))
print(f"Sample errors: {len(data['reports']['sample']['errors'])}")
print(f"Control errors: {len(data['reports']['control']['errors'])}")
print(f"Discrepancy errors: {len(data['reports']['discrepancy']['errors'])}")
EOF
```

## Contact & Support

For issues or questions about report generation:
- Check database file paths are correct
- Verify date formats are YYYY-MM-DD
- Review error messages in console output
- Check output directory permissions
