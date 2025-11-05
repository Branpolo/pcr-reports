# CSV-Driven Categorization Implementation Plan

## Current State Summary

### Completed Materials

#### 1. Category Mapping CSVs (v1)
All three databases have complete category mappings with zero `NEEDS_REVIEW` rows:

- **QST**: `output_data/qst_category_mapping_v1.csv`
  - 179 data rows (208 total with headers)
  - 456,665 wells categorized
  - Uses unified SOP_ categories

- **Notts**: `output_data/notts_category_mapping_v1.csv`
  - 103 data rows (131 total with headers)
  - 43,619 wells categorized
  - 23 AC corrections applied

- **Vira**: `output_data/vira_category_mapping_v1.csv`
  - 156 data rows (184 total with headers)
  - 55,673 wells categorized
  - 25 AC corrections applied

**CSV Structure:**
```csv
WELL_TYPE,ERROR_CODE,ERROR_MESSAGE,RESOLUTION_CODES,WELL_LIMS_STATUS,OCCURRENCE_COUNT,CATEGORY,NOTES
SAMPLE,,,[],NOT DETECTED,280100,VALID_NOT_DETECTED,No error - NOT DETECTED
CONTROL,FAILED_POS_WELL,The positive control has failed.,[],,32,SOP_UNRESOLVED,FAILED_POS_WELL - Unresolved
```

**Lookup Key:** `(WELL_TYPE, ERROR_CODE, RESOLUTION_CODES, WELL_LIMS_STATUS)`

#### 2. Category Taxonomy

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
- `IGNORE_WELL` - Wells to ignore (MIX_MISSING, UNKNOWN_MIX, standalone REAMP/REXCT)

#### 3. Python Scripts

**JSON Extractor (hardcoded categorization):**
- File: `extract_report_with_curves.py`
- Lines: ~2000+
- Key functions:
  - `fetch_unresolved_errors()` - Lines ~500-700
  - `fetch_resolved_errors()` - Lines ~700-900
  - `fetch_resolved_with_new()` - Lines ~900-1100
  - `fetch_valid_results()` - Lines 1331-1450
  - `fetch_discrepancy_data()` - Lines ~1100-1300
  - Main execution: Lines 1700+

**HTML Report Generators:**
- `generate_report_from_json_with_graphs.py` - Detailed error reports with curves
- `generate_summary_report.py` - Executive summary with pie charts

#### 4. Documentation
- `CSV_TO_JSON_EXTRACTOR_MAPPING.md` - Conceptual mapping between CSV categories and JSON sections
- Database-specific control detection logic documented in `analyze_categories.py`

---

## The Technical Problem

### Objective
Replace ~13 hardcoded categorization rules in `extract_report_with_curves.py` with CSV-driven lookups.

### Current Architecture (Hardcoded)

```python
# Example from fetch_unresolved_errors():
if error_code and not has_meaningful_resolution(resolution_codes):
    category = 'unresolved'
elif error_code == 'CLSDISC_WELL':
    category = 'unresolved'  # Classification discrepancy
# ... 11+ more rules
```

### Target Architecture (CSV-Driven)

```python
# Load CSV once at startup
category_lookup = load_category_csv('output_data/qst_category_mapping_v1.csv')

# Lookup during processing
key = (well_type, error_code, resolution_codes, lims_status)
category = category_lookup.get(key, 'UNKNOWN')

# Map category to JSON section
if category in ['SOP_UNRESOLVED', 'DISCREP_IN_ERROR']:
    add_to_unresolved_section()
elif category in ['SOP_IGNORED', 'DISCREP_IGNORED']:
    add_to_resolved_error_ignored_section()
# ... etc
```

---

## CRITICAL: CSV Categorization vs. Discrepancy Detection

### ⚠️ Important Distinction

**The CSV does NOT replace discrepancy detection logic - it only categorizes other or related issues.**

#### What the CSV Provides
- **Categorization of known patterns**: `(well_type, error_code, resolution_codes, lims_status) → category`
- Example: "If you find a well with CLSDISC_WELL error and no resolution, categorize as DISCREP_IN_ERROR"
- Example: "If you find a well with BLA resolution + DETECTED, it needs cls data to categorize"

#### What the CSV Does NOT Provide
- **Discrepancy detection**: Which wells have `machine_cls != final_cls`
- **Classification data**: The actual machine_cls, final_cls, dx_ai_cls values
- **BLA resolution without error_code**: These wells need pcr_ai_classifications JOIN to detect if discrepancy exists

### Discrepancy Detection Logic MUST Continue

The `fetch_discrepancy_data()` function in `extract_report_with_curves.py` must continue to:

1. **Query pcr_ai_classifications table** for machine_cls
2. **Query wells table** for final_cls
3. **Compare machine_cls vs final_cls** to detect discrepancies
4. **Join with error_codes** to get error_code (CLSDISC_WELL, CTDISC_WELL, etc.)
5. **THEN apply CSV categorization** based on (well_type, error_code, resolution_codes, lims_status)

### Two-Stage Process

```python
# STAGE 1: DETECT discrepancies (NOT replaced by CSV)
query = """
    SELECT w.*,
           c.machine_cls,
           w.final_cls,
           c.dx_ai_cls
    FROM wells w
    JOIN pcr_ai_classifications c ON w.id = c.well_id
    WHERE w.final_cls != c.machine_cls  -- DISCREPANCY DETECTED
       OR w.error_code_id IN (SELECT id FROM error_codes WHERE error_code IN ('CLSDISC_WELL', 'CTDISC_WELL'))
       OR w.resolution_codes LIKE '%BLA%'
"""

# STAGE 2: CATEGORIZE detected discrepancies (CSV-driven)
for well in detected_discrepancies:
    key = (well['well_type'], well['error_code'], well['resolution_codes'], well['lims_status'])
    category = category_lookup.get(key)

    if category == 'DISCREP_NEEDS_CLS_DATA':
        # Use detected machine_cls/final_cls to subcategorize
        if well['final_cls'] != well['machine_cls']:
            if well['lims_status'] in ['REAMP', 'REXCT']:
                category = 'DISCREP_RESULT_CHANGED'
            else:
                category = 'DISCREP_IGNORED'

    # Route to appropriate JSON section based on category
    if category == 'DISCREP_IN_ERROR':
        add_to_unresolved_discrepancies()
    elif category == 'DISCREP_IGNORED':
        add_to_error_ignored_discrepancies()
    elif category == 'DISCREP_RESULT_CHANGED':
        add_to_result_changed_discrepancies()
```

### Why This Matters

**Without discrepancy detection:**
- We can't populate the discrepancy report at all
- We don't know which wells have machine_cls != final_cls
- BLA patterns without explicit error codes would be missed
- The "Classification Discrepancies" section would be empty

**CSV categorization alone would give us:**
- "If CLSDISC_WELL, categorize as DISCREP_IN_ERROR" ✓
- But NOT "here are all the wells where machine_cls != final_cls" ✗

### Impact on Implementation

**DO use CSV for:**
- Categorizing SOP errors (error_code + resolution + lims)
- Categorizing valid results (no error + lims)
- Categorizing control errors
- Categorizing KNOWN discrepancies (CLSDISC_WELL, CTDISC_WELL error codes)

**DO NOT replace with CSV:**
- `pcr_ai_classifications` table JOINs
- `machine_cls != final_cls` comparisons
- `dx_ai_cls` retrieval for display in reports
- Discrepancy detection queries

**Hybrid approach:**
```python
# Detection (keep as-is)
discrepancy_wells = fetch_wells_with_cls_mismatch()  # Queries pcr_ai_classifications

# Categorization (CSV-driven)
for well in discrepancy_wells:
    key = (well_type, error_code, resolution_codes, lims_status)
    category = category_lookup.get(key, 'DISCREP_IN_ERROR')  # Default if not in CSV

    # Special handling for DISCREP_NEEDS_CLS_DATA
    if category == 'DISCREP_NEEDS_CLS_DATA':
        category = determine_discrep_subcategory(well['machine_cls'], well['final_cls'], well['lims_status'])

    route_to_json_section(well, category)
```

### Testing Impact

When comparing baseline vs CSV-driven reports, the discrepancy section should have:
- **Same wells detected** (because detection logic unchanged)
- **Same/similar categorization** (because CSV mirrors existing logic)
- **Same machine_cls/final_cls/dx_ai_cls display** (because classification data still queried)

---

## Category → JSON Section Mapping

Based on `CSV_TO_JSON_EXTRACTOR_MAPPING.md` and actual JSON output analysis:

| CSV Category | JSON Section | Query Function | HTML Tab |
|--------------|--------------|----------------|----------|
| `SOP_UNRESOLVED` | `unresolved` | `fetch_unresolved_errors()` | "Unresolved Errors" |
| `SOP_IGNORED` | `resolved` with `clinical_category='error_ignored'` | `fetch_resolved_errors()` | "Errors Ignored" |
| `SOP_REPEATED` | `resolved_with_new` with `clinical_category='test_repeated'` | `fetch_resolved_with_new()` | "Test Repeated" |
| `DISCREP_IN_ERROR` | `discrepancy` unresolved | `fetch_discrepancy_data()` | Discrepancy "Unresolved" |
| `DISCREP_IGNORED` | `discrepancy` resolved with `clinical_category='error_ignored'` | `fetch_discrepancy_data()` | Discrepancy "Ignored" |
| `DISCREP_RESULT_CHANGED` | `discrepancy` resolved with `clinical_category='result_changed'` | `fetch_discrepancy_data()` | Discrepancy "Result Changed" |
| `DISCREP_NEEDS_CLS_DATA` | `discrepancy` (requires machine_cls/final_cls data) | `fetch_discrepancy_data()` | Discrepancy tabs (needs cls data to categorize) |
| `VALID_DETECTED` | `valid_results.samples_detected` | `fetch_valid_results()` | Summary stats |
| `VALID_NOT_DETECTED` | `valid_results.samples_not_detected` | `fetch_valid_results()` | Summary stats |
| `VALID_CONTROL` | `valid_results.controls_passed` | `fetch_valid_results()` | Summary stats |
| `VALID_OTHER` | `valid_results.samples_other_status` | `fetch_valid_results()` | Summary stats |
| `CONTROL_AFFECTED_SAMPLE` | `controls` appendix | `fetch_control_data()` | Controls Appendix |
| `IGNORE_WELL` | Skip/exclude from all queries | N/A | Not shown |

**Important Notes:**
- `DISCREP_NEEDS_CLS_DATA` requires machine_cls/final_cls comparison to determine if goes to `error_ignored` or `result_changed`
- BLA patterns are caught by discrepancy extractor first
- Controls use same SOP_ categories as samples; `WELL_TYPE` field distinguishes them

---

## Testing & Verification Strategy

### 1. Baseline Generation (Before Changes)

Generate baseline reports using current hardcoded logic:

```bash
# QST baseline
python3 extract_report_with_curves.py \
  --db ~/dbs/readings.db \
  --output output_data/qst_baseline_report.json \
  --since 2024-06-01

# Notts baseline
python3 extract_report_with_curves.py \
  --db ~/dbs/notts.db \
  --output output_data/notts_baseline_report.json \
  --since 2024-06-01

# Vira baseline
python3 extract_report_with_curves.py \
  --db ~/dbs/vira.db \
  --output output_data/vira_baseline_report.json \
  --since 2024-06-01
```

**Generate HTML reports from baseline:**

```bash
# QST HTML
python3 generate_report_from_json_with_graphs.py \
  --input output_data/qst_baseline_report.json \
  --output output_data/qst_baseline_report.html

# QST Summary
python3 generate_summary_report.py \
  --input output_data/qst_baseline_report.json \
  --output output_data/qst_baseline_summary.html

# Repeat for Notts and Vira
```

### 2. Validation Checks

**Extract key metrics from baseline JSON:**

```bash
python3 -c "
import json
with open('output_data/qst_baseline_report.json') as f:
    data = json.load(f)

print('=== QST Baseline Metrics ===')
print(f\"Valid samples detected: {data['valid_results']['samples_detected']}\")
print(f\"Valid samples not detected: {data['valid_results']['samples_not_detected']}\")
print(f\"Valid controls passed: {data['valid_results']['controls_passed']}\")

print(f\"\\nError Statistics:\")
for mix, stats in data['error_statistics'].items():
    print(f\"  {mix}: {stats['total_errors']} errors\")

print(f\"\\nReport Sections:\")
for section in ['sample', 'control', 'discrepancy']:
    if section in data['reports']:
        report = data['reports'][section]
        print(f\"  {section.upper()}:\")
        print(f\"    Unresolved: {len(report.get('unresolved', []))}\")
        print(f\"    Resolved: {len(report.get('resolved', []))}\")
        print(f\"    Resolved with new: {len(report.get('resolved_with_new', []))}\")
"
```

Save this output as `output_data/qst_baseline_metrics.txt` for comparison.

### 3. Post-Implementation Testing

After implementing CSV-driven categorization:

```bash
# Generate new reports with same parameters
python3 extract_report_with_curves.py \
  --db ~/dbs/readings.db \
  --output output_data/qst_csv_driven_report.json \
  --since 2024-06-01

# Extract metrics from new JSON
python3 -c "
import json
with open('output_data/qst_csv_driven_report.json') as f:
    data = json.load(f)
# ... same metrics extraction as above
"
```

**Compare metrics:**
```bash
diff output_data/qst_baseline_metrics.txt output_data/qst_csv_driven_metrics.txt
```

**Expected result:** No differences (or only expected differences in categories that were corrected)

### 4. Visual Verification

Open both HTML reports side-by-side in browser:
- `output_data/qst_baseline_report.html`
- `output_data/qst_csv_driven_report.html`

**Check:**
- Tab counts match (Unresolved Errors, Errors Ignored, Test Repeated)
- Well counts in each section match
- Discrepancy tabs show same wells
- Controls appendix has same affected samples
- Summary statistics match

### 5. Database Comparison

For each database, verify category distribution matches CSV:

```bash
python3 -c "
import csv
import json

# Load CSV
csv_categories = {}
with open('output_data/qst_category_mapping_v1.csv') as f:
    for line in f:
        if not line.startswith('#'):
            break
    reader = csv.DictReader([line] + list(f))
    for row in reader:
        csv_categories[row['CATEGORY']] = csv_categories.get(row['CATEGORY'], 0) + int(row['OCCURRENCE_COUNT'])

print('Expected from CSV:')
for cat, count in sorted(csv_categories.items()):
    print(f'  {cat:30} {count:>10,}')

# Compare with JSON output
# ... (count categories in JSON report)
"
```

---

## Implementation Steps

### Phase 1: CSV Loader

Create `load_category_csv()` function in `extract_report_with_curves.py`:

```python
def load_category_csv(csv_path):
    """
    Load category mappings from CSV

    Returns:
        dict: {(well_type, error_code, resolution_codes, lims_status): category}
    """
    category_lookup = {}

    with open(csv_path, 'r', encoding='utf-8') as f:
        # Skip header comment lines
        for line in f:
            if not line.startswith('#'):
                break

        reader = csv.DictReader([line] + f)

        for row in reader:
            key = (
                row['WELL_TYPE'],
                row['ERROR_CODE'],
                row['RESOLUTION_CODES'],
                row['WELL_LIMS_STATUS']
            )
            category_lookup[key] = row['CATEGORY']

    return category_lookup
```

### Phase 2: Add CSV Parameter

Add `--category-csv` parameter to argument parser:

```python
parser.add_argument('--category-csv',
                   help='Path to category mapping CSV (e.g., output_data/qst_category_mapping_v1.csv)')
```

### Phase 3: Modify Query Functions

For each query function (`fetch_unresolved_errors`, `fetch_resolved_errors`, etc.):

**Before (hardcoded):**
```python
if error_code and not has_meaningful_resolution(resolution_codes):
    # Include in unresolved
    wells.append(well_data)
```

**After (CSV-driven):**
```python
key = (well_type, error_code, resolution_codes, lims_status)
category = category_lookup.get(key)

if category in ['SOP_UNRESOLVED', 'DISCREP_IN_ERROR']:
    # Include in unresolved
    wells.append(well_data)
elif category == 'IGNORE_WELL':
    continue  # Skip this well
```

### Phase 4: Category Mapping Logic

Create a mapping function to route categories to appropriate sections:

```python
def get_json_section_for_category(category):
    """Map CSV category to JSON section"""

    if category in ['SOP_UNRESOLVED', 'DISCREP_IN_ERROR']:
        return 'unresolved'
    elif category in ['SOP_IGNORED', 'DISCREP_IGNORED']:
        return 'resolved', 'error_ignored'
    elif category in ['SOP_REPEATED', 'DISCREP_RESULT_CHANGED']:
        return 'resolved_with_new', 'test_repeated'
    elif category.startswith('VALID_'):
        return 'valid_results'
    elif category == 'CONTROL_AFFECTED_SAMPLE':
        return 'controls_appendix'
    elif category == 'IGNORE_WELL':
        return None  # Skip
    elif category == 'DISCREP_NEEDS_CLS_DATA':
        return 'discrepancy'  # Requires further classification
    else:
        return 'unknown'  # Log warning
```

---

## Rollback Strategy

If something goes wrong during implementation:

### 1. Git Revert

```bash
# Check current changes
git status
git diff extract_report_with_curves.py

# If needed, revert to last commit
git checkout extract_report_with_curves.py

# Or revert to specific commit
git log --oneline -10  # Find commit hash
git checkout <commit_hash> extract_report_with_curves.py
```

### 2. Backup Strategy

Before making changes:

```bash
# Create backup of current working version
cp extract_report_with_curves.py extract_report_with_curves.py.backup_YYYY-MM-DD

# Restore from backup if needed
cp extract_report_with_curves.py.backup_YYYY-MM-DD extract_report_with_curves.py
```

### 3. Baseline Reports

Keep baseline reports as reference:
- `output_data/*_baseline_report.json`
- `output_data/*_baseline_report.html`
- `output_data/*_baseline_summary.html`
- `output_data/*_baseline_metrics.txt`

These can be regenerated at any time from the backup script.

---

## Expected Challenges

### 1. Key Normalization

**Issue:** CSV has exact strings; database may have variations

**Solution:** Normalize before lookup:
```python
def normalize_resolution_codes(res_codes):
    """Normalize resolution codes for lookup"""
    if not res_codes:
        return ''
    # Handle different formats: "SKIP,WG12S" vs "SKIP, WG12S"
    codes = [c.strip() for c in res_codes.split(',')]
    return ','.join(sorted(codes))
```

### 2. Missing Keys

**Issue:** Database may have combinations not in CSV

**Solution:** Log missing keys and use default category:
```python
if key not in category_lookup:
    logger.warning(f"Missing category for: {key}")
    category = 'SOP_UNRESOLVED'  # Safe default
```

### 3. LIMS Status Variants

**Issue:** Database may have LIMS variants not normalized in CSV

**Solution:** Apply same LIMS normalization used in `recategorize_categories.py`:
```python
LIMS_MAPPINGS = {
    'qst': {
        'MPX & OPX DETECTED': 'DETECTED',
        'HSV1_DETECTED': 'DETECTED',
        # ...
    },
    # ...
}
```

### 4. BLA/DISCREP_NEEDS_CLS_DATA

**Issue:** Some categories require machine_cls/final_cls data not in CSV

**Solution:** For `DISCREP_NEEDS_CLS_DATA`, query machine_cls/final_cls from DB:
```python
if category == 'DISCREP_NEEDS_CLS_DATA':
    # Query pcr_ai_classifications table
    machine_cls, final_cls = fetch_classifications(well_id)
    if final_cls != machine_cls:
        category = 'DISCREP_RESULT_CHANGED'
    else:
        category = 'DISCREP_IGNORED'
```

---

## Success Criteria

✅ All three databases (QST, Notts, Vira) generate reports with CSV-driven categorization

✅ Baseline vs CSV-driven metrics match (within expected differences)

✅ HTML reports visually identical (same tab counts, well distributions)

✅ No `UNKNOWN` categories in generated JSON

✅ All special cases handled (IGNORE_WELL, CONTROL_AFFECTED_SAMPLE)

✅ Summary statistics match baseline

✅ Error rates in pie charts match baseline

---

## Files to Modify

### Primary
- `extract_report_with_curves.py` - Add CSV loading and category-driven queries

### Supporting (if needed)
- `generate_report_from_json_with_graphs.py` - May need updates if JSON structure changes
- `generate_summary_report.py` - Should work unchanged (uses valid_results/error_statistics)

### Do NOT Modify
- All three `*_category_mapping_v1.csv` files - These are now the source of truth
- `recategorize_categories.py` - Database-agnostic categorizer (already working)
- `analyze_categories.py` - Template generator (already working)

---

## Current Git State

Branch: `codex`

Recent commits:
```
6eb4cfc seems unified generators now working
cf2dab7 unified gen working :)
8eb38ba unified things but sample report not producing curves
```

Modified files:
```
M extract_report_with_curves.py
M generate_report_from_json_with_graphs.py
```

---

## Contact Points / Decision Points

If implementation reveals issues:

1. **Category not in CSV** → Update CSV with `analyze_categories.py` to regenerate TEMPLATE, then recategorize
2. **JSON structure needs change** → Update both extractor and HTML generators
3. **Performance issues** → Consider caching lookups or indexing
4. **Database-specific quirks** → May need per-database logic (already have in analyze_categories.py)

---

## References

- **Category Mapping Documentation:** `CSV_TO_JSON_EXTRACTOR_MAPPING.md`
- **Agent Guidelines:** `AGENTS.md`
- **Baseline JSON:** `output_data/unified_report_since20240601.json` (QST production example)
- **Control Detection Logic:** See `DB_CONFIGS` in `analyze_categories.py` lines 30-90
