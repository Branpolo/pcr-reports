# Project Reorganization Summary (2025-10-10)

## New Directory Structure

```
wssvc-flow-codex/
├── reports/                          # Active error report generation
│   ├── extract_report_with_curves.py # Universal JSON extractor
│   ├── generate_report_from_json_with_graphs.py # Universal HTML generator
│   ├── generate_xlsx_from_json.py    # XLSX export generator
│   ├── import_qst_data.py            # QST data import utility
│   ├── utils/
│   │   └── report_helpers.py         # Report utility functions
│   ├── docs/                         # Reports documentation
│   │   ├── unified_report_plan.md
│   │   ├── unify_extractors_findings.md
│   │   ├── unify_extractors_tasks.md
│   │   └── resolution_code_meanings.md
│   └── archive/2025-10-10/           # Archived old report generators
│       ├── [14 old extract_*.py scripts]
│       └── [8 old generate_*.py scripts]
├── flatten/                          # Curve flattening & CUSUM
│   ├── apply_corrected_cusum_all.py
│   ├── compare_k_parameters.py
│   ├── create_flattened_database_fast.py
│   ├── generate_flattened_cusum_html.py
│   ├── generate_database_flattened_html_fixed.py
│   ├── generate_pcrai_from_db.py
│   ├── manage_example_ids.py         # Example dataset management
│   ├── utils/
│   │   ├── algorithms.py             # CUSUM/smoothing algorithms
│   │   ├── visualization.py          # SVG generation
│   │   └── database.py               # Database utilities
│   └── docs/                         # Flatten documentation
│       ├── flags_table.md
│       ├── flags_applicability_table.md
│       ├── command_line_flags_analysis.md
│       └── curve_flattening_edge_cases_analysis.md
├── utils/                            # Undecided utilities
│   └── export_database_to_csv.py
├── docs/                             # General documentation
│   ├── AGENTS.md
│   ├── PRD.md
│   └── REFACTORING_TEST_PLAN.md
├── README.md                         # Main documentation
├── CLAUDE.md                         # Claude Code memory
├── REORGANIZATION_SUMMARY.md         # This file
├── tasks.md                          # Active tasks
└── [existing directories: output_data/, input_data/, archive/, backup/, .claude/, .playwright-mcp/, __pycache__/]
```

## Active Report Generation Files (reports/)

### Main Scripts
1. **extract_report_with_curves.py** - Universal JSON extractor supporting:
   - Combined mode (all 3 reports in one JSON)
   - Sample error reports
   - Control error reports
   - Classification discrepancy reports

2. **generate_report_from_json_with_graphs.py** - Universal HTML generator with:
   - Interactive graphs with target selection
   - Control overlay toggles
   - Responsive design
   - Comments display
   - Date-filtered reports

3. **generate_xlsx_from_json.py** - XLSX export generator:
   - Three sheets (Sample SOP Errors, Control Errors, Classification Errors)
   - All metadata fields
   - Pathogen target CT values
   - Comments
   - No curves/readings data

4. **import_qst_data.py** - QST data import utility:
   - Imports QST CSV data to SQLite database
   - Parses JSON readings into individual columns

### Running Report Scripts
```bash
# From project root, use module syntax:
python3 -m reports.extract_report_with_curves combined \
  --db input_data/quest_prod_aug2025.db \
  --output output_data/combined_report.json \
  --html-output output_data/unified_report.html \
  --sample-since-date 2024-06-01 \
  --control-since-date 2024-06-01 \
  --discrepancy-since-date 2024-06-01

# Generate XLSX from JSON
python3 -m reports.generate_xlsx_from_json \
  --json output_data/combined_report.json \
  --output output_data/combined_report.xlsx
```

## Undecided Files

### utils/export_database_to_csv.py
- Purpose unclear, may be used by either flatten or reports
- Left in utils/ pending decision

## Documentation Organization

### Root-level docs
- `README.md` - Main project documentation
- `CLAUDE.md` - Claude Code memory and guidelines
- `REORGANIZATION_SUMMARY.md` - This file
- `tasks.md` - Active task tracking

### Project-specific docs
- `reports/docs/` - Reports project documentation
  - unified_report_plan.md
  - unify_extractors_findings.md
  - unify_extractors_tasks.md
  - resolution_code_meanings.md

- `flatten/docs/` - Flatten project documentation
  - flags_table.md
  - flags_applicability_table.md
  - command_line_flags_analysis.md
  - curve_flattening_edge_cases_analysis.md

### General docs
- `docs/` - General project documentation
  - AGENTS.md
  - PRD.md
  - REFACTORING_TEST_PLAN.md

## Archived Files (reports/archive/2025-10-10/)

All old/unused report generators moved to dated archive:
- 14 extract_*.py scripts (superseded by extract_report_with_curves.py)
- 8 generate_*.py scripts (superseded by generate_report_from_json_with_graphs.py)

## Dependencies Updated

Import paths updated in active scripts:
- `from utils.report_helpers import ...` → `from .utils.report_helpers import ...`
- `from generate_report_from_json_with_graphs import ...` → `from .generate_report_from_json_with_graphs import ...`

## Next Steps

1. Update README.md Part 2 (Reports section)
2. Decide on utils/export_database_to_csv.py placement
3. Consider if manage_example_ids.py and import_qst_data.py should move to flatten/
