# Claude Code Memory - PCR Reports Project

## Important Reminders

### User Instructions
- **NEVER start implementation without all clarification questions answered** - Always wait for explicit user confirmation before proceeding with implementation
- When asking clarification questions, wait for complete user responses before beginning work
- Always put output files from py or other scripts in `/output_data`, otherwise finding and Git are complicated.

### Documentation Updates
- **ALWAYS update README.md when adding new features or parameters** - Document any new command-line flags, parameters, or functionality
- Update both the parameter descriptions and usage examples when applicable
- Add usage examples for new features to help users understand how to use them

## Project Overview

This repository contains error report generation tools for Quest/Notts/Vira PCR laboratory databases.

**Purpose**: Error report generation from Quest/Notts/Vira databases
- **Active scripts**: `extract_report_with_curves.py`, `generate_report_from_json_with_graphs.py`, `generate_xlsx_from_json.py`, `generate_summary_report.py`
- **Databases**: Quest/Notts/Vira production databases in `~/dbs/` (e.g., `~/dbs/quest_prod_aug2025.db`)
- **Output**: JSON, HTML, XLSX error reports
- **Documentation**: `reports/docs/`

## Reports Project Guidelines

### When Working on Reports
- Use Python module syntax: `python3 -m reports.extract_report_with_curves`
- Import from reports utils: `from .utils.report_helpers import ...`
- Put outputs in `/output_data/` (shared output directory)
- Focus on data extraction, visualization, and export
- No mathematical analysis (CUSUM, curve flattening) in this project

### Active Report Scripts
- `reports/extract_report_with_curves.py` - Universal JSON extractor (sample/control/discrepancy)
- `reports/generate_report_from_json_with_graphs.py` - Universal HTML generator
- `reports/generate_xlsx_from_json.py` - Excel export generator
- `reports/generate_summary_report.py` - Executive summary with pie charts
- `reports/generate_full_report.py` - Wrapper for complete workflow
- `reports/non_inverted_sigmoid_gen_html.py` - Sigmoid filtering HTML
- `reports/import_qst_data.py` - QST data import utility

## CSV-Driven Categorization System

### Overview
The reporting system uses **client-specific CSV mappings** to categorize errors across multiple laboratory databases. This allows flexible, maintainable categorization without hardcoding rules for each client's workflow.

### Database-Specific CSV Mappings
Each database has its own categorization CSV:
- **QST** (Quest): `output_data/qst_category_mapping_v4_ac.csv`
- **Notts** (Nottingham): `output_data/notts_category_mapping_v1_ac.csv`
- **Vira** (Viracor): `output_data/vira_category_mapping_v1_ac.csv`

### Category Taxonomy
**Classification Discrepancies**: DISCREP_IN_ERROR, DISCREP_IGNORED, DISCREP_RESULT_CHANGED, DISCREP_NEEDS_CLS_DATA
**SOP Errors**: SOP_UNRESOLVED, SOP_IGNORED, SOP_REPEATED
**Valid Results**: VALID_DETECTED, VALID_NOT_DETECTED, VALID_CONTROL, VALID_OTHER
**Special**: CONTROL_AFFECTED_SAMPLE, IGNORE_WELL

### When Updating CSV Mappings
1. Generate new template: `python3 analyze_categories.py --db-type qst --db /path/to/db --output template.csv`
2. Review and categorize patterns in the template
3. Apply corrections: `python3 recategorize_categories.py --input template.csv --output final.csv`
4. Test with new CSV: Verify report statistics match expectations

## Workflow Guidelines

### Standard Workflow
1. **Extract data to JSON** using `reports.extract_report_with_curves combined`
2. **Generate HTML** using `reports.generate_report_from_json_with_graphs`
3. **Generate summary** using `reports.generate_summary_report`
4. **Generate Excel** using `reports.generate_xlsx_from_json`

### Quick Workflow
Use `reports.generate_full_report` to generate all outputs in one command.

## Code Style and Best Practices

### Import Statements
```python
# For scripts in reports/
from .utils.report_helpers import load_csv_categories, extract_curve_data, get_control_mapping

# For root-level categorization scripts
from database_configs import get_config
```

### Database Connections
```python
# Always use database_configs for multi-database support
from database_configs import get_config

config = get_config('qst')  # or 'notts', 'vira'
conn = sqlite3.connect(config['db_path'])
```

### Error Handling
- Always validate database paths exist before processing
- Handle missing CSV mapping files gracefully
- Provide clear error messages for missing columns or invalid data
- Log warnings for unrecognized error patterns

### Output Files
- Always write to `output_data/` directory
- Use descriptive filenames with date ranges: `qst_report_20240601_20250531.json`
- Generate both JSON (for programmatic access) and HTML (for human review)

## Testing and Verification

### Before Committing Changes
1. **Test with all database types**: QST, Notts, Vira
2. **Verify report statistics**: Check sample counts, error counts match expectations
3. **Check HTML rendering**: Ensure graphs display correctly, controls toggle properly
4. **Validate Excel export**: Confirm all sheets populate correctly

### Common Test Commands
```bash
# Test QST report extraction
python3 -m reports.extract_report_with_curves combined \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --output output_data/test_qst.json \
  --sample-since-date 2024-06-01 \
  --sample-limit 100

# Test HTML generation
python3 -m reports.generate_report_from_json_with_graphs \
  --json output_data/test_qst.json \
  --output output_data/test_qst.html \
  --max-per-category 10

# Test summary generation
python3 -m reports.generate_summary_report \
  --json output_data/test_qst.json \
  --output output_data/test_summary.html
```

## Common Issues and Solutions

### Issue: Missing CSV Mapping File
**Solution**: Ensure CSV mapping file exists in `output_data/` or specify custom path with `--csv-mapping` flag

### Issue: Unrecognized Error Patterns
**Solution**: Generate new CSV template with `analyze_categories.py`, categorize new patterns, and update mapping

### Issue: HTML Graphs Not Displaying
**Solution**: Check that `well_curves` data is present in JSON, verify control mapping is correct

### Issue: Excel Export Missing Data
**Solution**: Verify JSON contains all required fields, check for None values in metadata

## Documentation Files

- `CSV_DRIVEN_CATEGORIZATION_PLAN.md` - Implementation plan and technical details
- `OPTIMAL_IMPLEMENTATION_PLAN.md` - Reports implementation strategy
- `DISCREPANCY_FIX_PLAN.md` - Reports fix planning
- `REORGANIZATION_SUMMARY.md` - Documents 2025-10-10 reorganization
- `reports/docs/NOTTS_ISSUES_AND_FIXES.md` - Nottingham database specific issues
- `reports/docs/resolution_code_meanings.md` - Resolution code reference
- `reports/docs/unified_report_plan.md` - Unified reporting architecture
- `reports/docs/unify_extractors_findings.md` - Extractor unification analysis
- `reports/docs/unify_extractors_tasks.md` - Extractor unification tasks

## Archive Reference

Legacy scripts remain available under `reports/archive/2025-10-10/` for historical reference. These scripts were superseded by the unified reporting system but may contain useful implementation details.
