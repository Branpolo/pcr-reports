# Unified Report Generation Plan

## Overview
Create a unified report generation system that:
1. Extracts data from SQL into JSON files
2. Uses a single HTML generator (based on `generate_control_report_working.py`) to render from JSON

## Phase 1: SQL Query Extraction

### 1. Control Report Queries (from generate_control_report_working.py)

#### Query 1A: Unresolved Control Errors
```sql
SELECT DISTINCT
    w.id as well_id,
    w.sample_name,
    w.well_number,
    ec.error_code,
    ec.error_message,
    m.mix_name,
    r.run_name,
    r.id as run_id,
    w.lims_status,
    'unresolved' as category
FROM wells w
JOIN error_codes ec ON w.error_code_id = ec.id
JOIN runs r ON w.run_id = r.id
JOIN run_mixes rm ON w.run_mix_id = rm.id
JOIN mixes m ON rm.mix_id = m.id
WHERE w.error_code_id IS NOT NULL
AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
AND ec.error_type != 0
AND w.role_alias IS NOT NULL
AND w.role_alias != 'Patient'
AND (w.role_alias LIKE '%PC%' 
     OR w.role_alias LIKE '%NC%' 
     OR w.role_alias LIKE '%CONTROL%'
     OR w.role_alias LIKE '%NEGATIVE%'
     OR w.role_alias LIKE '%POSITIVE%'
     OR w.role_alias LIKE '%NTC%'
     OR w.role_alias LIKE '%PTC%')
ORDER BY m.mix_name, ec.error_code, w.sample_name
```

#### Query 1B: Resolved Control Errors
```sql
SELECT DISTINCT
    w.id as well_id,
    w.sample_name,
    w.well_number,
    COALESCE(w.resolution_codes, ec.error_code) as error_code,
    COALESCE(ec.error_message, 'Resolved') as error_message,
    m.mix_name,
    r.run_name,
    r.id as run_id,
    w.lims_status,
    'resolved' as category
FROM wells w
LEFT JOIN error_codes ec ON w.error_code_id = ec.id
JOIN runs r ON w.run_id = r.id
JOIN run_mixes rm ON w.run_mix_id = rm.id
JOIN mixes m ON rm.mix_id = m.id
WHERE w.resolution_codes IS NOT NULL 
AND w.resolution_codes <> ''
AND (ec.error_type IS NULL OR ec.error_type != 0)
AND w.role_alias IS NOT NULL
AND w.role_alias != 'Patient'
AND (w.role_alias LIKE '%PC%' 
     OR w.role_alias LIKE '%NC%' 
     OR w.role_alias LIKE '%CONTROL%'
     OR w.role_alias LIKE '%NEGATIVE%'
     OR w.role_alias LIKE '%POSITIVE%'
     OR w.role_alias LIKE '%NTC%'
     OR w.role_alias LIKE '%PTC%')
ORDER BY m.mix_name, w.sample_name
```

**Post-processing**: Categorize resolved into 'error_ignored' or 'test_repeated' based on resolution codes containing RP/RX/TN

#### Query 1C: Affected Samples - INHERITED Errors
```sql
SELECT DISTINCT
    pw.id as well_id,
    pw.sample_name,
    pw.well_number,
    pec.error_code,
    pec.error_message,
    pm.mix_name,
    pr.run_name,
    pw.lims_status,
    pw.resolution_codes,
    cw.id as control_well_id,
    cw.sample_name as control_name,
    cw.well_number as control_well,
    cm.mix_name as control_mix,
    cw.resolution_codes as control_resolution
FROM wells pw
JOIN error_codes pec ON pw.error_code_id = pec.id
JOIN runs pr ON pw.run_id = pr.id
JOIN run_mixes prm ON pw.run_mix_id = prm.id
JOIN mixes pm ON prm.mix_id = pm.id
JOIN wells cw ON cw.run_id = pw.run_id
JOIN run_mixes crm ON cw.run_mix_id = crm.id
JOIN mixes cm ON crm.mix_id = cm.id
WHERE pw.error_code_id IN (
    '937829a3-a630-4a86-939d-c2b1ec229c9d',  -- INHERITED_CONTROL_FAILURE
    '937829a3-aa88-44cf-bbd5-deade616cff5',  -- INHERITED_EXTRACTION_FAILURE
    '995a530f-1da9-457d-9217-5afdac6ca59f',  -- INHERITED_CONTROL_FAILURE
    '995a530f-2239-4007-80f9-4102b5826ee5'   -- INHERITED_EXTRACTION_FAILURE
)
AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
AND cw.role_alias IS NOT NULL
AND cw.role_alias != 'Patient'
AND (cw.error_code_id IS NOT NULL OR cw.resolution_codes IS NOT NULL)
```

#### Query 1D: Affected Samples - REPEATED
```sql
SELECT DISTINCT
    pw.id as well_id,
    pw.sample_name,
    pw.well_number,
    '' as error_code,
    'Repeated due to control' as error_message,
    pm.mix_name,
    pr.run_name,
    pw.lims_status,
    pw.resolution_codes,
    cw.id as control_well_id,
    cw.sample_name as control_name,
    cw.well_number as control_well,
    cm.mix_name as control_mix,
    cw.resolution_codes as control_resolution
FROM wells pw
JOIN runs pr ON pw.run_id = pr.id
JOIN run_mixes prm ON pw.run_mix_id = prm.id
JOIN mixes pm ON prm.mix_id = pm.id
JOIN wells cw ON cw.run_id = pw.run_id
JOIN run_mixes crm ON cw.run_mix_id = crm.id
JOIN mixes cm ON crm.mix_id = cm.id
WHERE pw.lims_status IN ('REAMP','REXCT','RPT','RXT','TNP')
AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
AND cw.role_alias IS NOT NULL
AND cw.role_alias != 'Patient'
AND (cw.resolution_codes LIKE '%RP%' 
     OR cw.resolution_codes LIKE '%RX%' 
     OR cw.resolution_codes LIKE '%TN%')
```

#### Query 1E: Control Curves (for each error)
For each control error, we need to fetch curve data for visualization:

```sql
-- Get main target readings for the error well
SELECT 
    t.target_name,
    t.ct,
    t.is_ic,
    t.readings0, t.readings1, ..., t.readings49
FROM targets t
WHERE t.well_id = ? 
ORDER BY t.is_ic, t.target_name

-- Get control curves for context (up to 5 controls)
SELECT 
    w.id,
    w.sample_name,
    w.role_alias,
    t.target_name,
    t.ct,
    t.is_ic,
    t.readings0, t.readings1, ..., t.readings49
FROM wells w
JOIN targets t ON w.id = t.well_id
WHERE w.run_id = ?
AND t.target_name IN (?) -- Related target names
AND w.role_alias IS NOT NULL
AND w.role_alias != 'Patient'
ORDER BY 
    t.is_ic,  -- Non-IC first
    CASE WHEN w.role_alias LIKE '%NC%' THEN 0
         WHEN w.role_alias LIKE '%PC%' THEN 1
         ELSE 2 END,
    t.ct
LIMIT 5
```

**Data needed for visualization**:
- Main well's target readings (50 data points)
- Control wells' readings for comparison
- CT values for each curve
- Role (NC/PC) to determine curve style (dotted/dashed)
- Target name to group related curves

### 2. Sample Report Queries (from generate_error_report_interactive_fixed.py)

#### Query 2A: Unresolved Sample Errors
```sql
-- INCLUDED_ERROR_TYPES from generate_error_report_interactive_fixed.py:
-- 'INH_WELL', 'ADJ_CT', 'DO_NOT_EXPORT', 'INCONCLUSIVE_WELL',
-- 'CTDISC_WELL', 'BICQUAL_WELL', 'BAD_CT_DELTA', 'LOW_FLUORESCENCE_WELL'

-- SETUP_ERROR_TYPES (optional, based on --include-label-errors flag):
-- 'MIX_MISSING', 'UNKNOWN_MIX', 'ACCESSION_MISSING', 'INVALID_ACCESSION',
-- 'UNKNOWN_ROLE', 'CONTROL_FAILURE', 'MISSING_CONTROL', 'INHERITED_CONTROL_FAILURE'

SELECT DISTINCT
    w.id as well_id,
    w.sample_name,
    w.well_number,
    ec.error_code,
    ec.error_message,
    m.mix_name,
    r.run_name,
    r.id as run_id,
    w.lims_status,
    'unresolved' as category
FROM wells w
JOIN error_codes ec ON w.error_code_id = ec.id
JOIN runs r ON w.run_id = r.id
JOIN run_mixes rm ON w.run_mix_id = rm.id
JOIN mixes m ON rm.mix_id = m.id
WHERE w.error_code_id IS NOT NULL
AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
AND ec.error_type != 0
AND ec.error_code IN (...)  -- Either INCLUDED_ERROR_TYPES or INCLUDED+SETUP based on flag
AND (w.role_alias IS NULL OR w.role_alias = 'Patient')
ORDER BY m.mix_name, ec.error_code, w.sample_name
```

#### Query 2B: Resolved Sample Errors
```sql
SELECT DISTINCT
    w.id as well_id,
    w.sample_name,
    w.well_number,
    COALESCE(w.resolution_codes, ec.error_code) as error_code,
    COALESCE(ec.error_message, 'Resolved') as error_message,
    m.mix_name,
    r.run_name,
    r.id as run_id,
    w.lims_status,
    'resolved' as category
FROM wells w
LEFT JOIN error_codes ec ON w.error_code_id = ec.id
JOIN runs r ON w.run_id = r.id
JOIN run_mixes rm ON w.run_mix_id = rm.id
JOIN mixes m ON rm.mix_id = m.id
WHERE w.resolution_codes IS NOT NULL 
AND w.resolution_codes <> ''
AND (ec.error_type IS NULL OR ec.error_type != 0)
AND (w.role_alias IS NULL OR w.role_alias = 'Patient')
ORDER BY m.mix_name, w.sample_name
```

**Post-processing**: 
- Check LIMS status after resolution:
  - If lims_status IN ('DETECTED', 'NOT DETECTED') → 'error_ignored' (error was ignored, valid result)
  - Otherwise → 'test_repeated' (sample was repeated)
- Alternative check by resolution codes:
  - If resolution contains 'BLA' or 'SKIP' → 'error_ignored'
  - If resolution contains 'RP', 'RX', 'TN', 'TP' → 'test_repeated'
  - Otherwise → Check lims_status as above

### 3. Discrepancy Report Queries (from generate_qst_report_interactive_v2.py)

#### Query 3A: Main QST Readings
```sql
SELECT 
    id,
    sample_label,
    well_number,
    lims_status,
    error_code,
    error_message,
    resolution_codes,
    extraction_date,
    machine_cls,
    dxai_cls,
    final_cls,
    manual_cls,
    machine_ct,
    dxai_ct,
    readings0, readings1, ..., readings49,
    target_name,
    mix_name,
    run_id
FROM qst_readings
ORDER BY mix_name, target_name, sample_label
```

**Post-processing categorization**:
- If machine_cls != final_cls AND lims_status IN ('DETECTED', 'NOT DETECTED'):
  - If final_cls == 1 → 'discrepancy_positive' (Section 1: Acted Upon)
  - Else → 'discrepancy_negative' (Section 1: Acted Upon)
- Else if error_code exists → 'has_error' (Section 2: Samples Repeated)
- Else if lims_status NOT IN ('DETECTED', 'NOT DETECTED') → 'lims_other' (Section 2: Samples Repeated)
- Else if machine_cls == final_cls AND lims_status IN ('DETECTED', 'NOT DETECTED'):
  - If lims_status == 'DETECTED' → 'agreement_detected' (Section 3: Ignored)
  - Else → 'agreement_not_detected' (Section 3: Ignored)

#### Query 3B: QST Controls (for curve visualization)
```sql
-- Gets control curves to display alongside main sample curve
-- Similar to Query 1E but for QST database structure
SELECT 
    role_alias,
    control_label,
    well_number,
    machine_ct,
    readings0, readings1, ..., readings49
FROM qst_controls
WHERE run_id = ? AND target_name = ?
```
**Purpose**: Display control curves (NC/PC) for comparison with the discrepancy sample

#### Query 3C: QST Other Observations (additional targets)
```sql
-- Gets other target curves from the same well
-- Shows IC curves or other multiplexed targets
SELECT 
    target,
    readings0, readings1, ..., readings49
FROM qst_other_observations
WHERE discrepancy_obs_id = ?
```
**Purpose**: Display additional target curves (e.g., IC) from the same well

## Phase 2: Unified JSON Schema

### Core Structure
```json
{
  "report_type": "control|sample|discrepancy",
  "generated_at": "2024-09-11T09:00:00",
  "database": "input_data/quest_prod_aug2025.db",
  "summary": {
    "total_errors": 3161,
    "unresolved": 932,
    "error_ignored": 1942,
    "test_repeated": 287
  },
  "errors": [
    {
      "well_id": "abc123",
      "sample_name": "CONTROL_001",
      "well_number": "A01",
      "error_code": "THRESHOLD_WRONG",
      "error_message": "Control threshold issue",
      "mix_name": "HSV",
      "run_name": "RUN_001",
      "run_id": "run123",
      "lims_status": "DETECTED",
      "clinical_category": "unresolved",
      "target_name": "HSV1",
      "ct_value": 25.5,
      "readings": [0.1, 0.2, ...],
      "controls": [
        {
          "role": "NC",
          "label": "HSVNC",
          "sample_name": "HSVNC",
          "readings": [0.1, 0.2, ...],
          "type": "negative"
        }
      ]
    }
  ],
  "affected_samples": {
    "RUN_001_HSV": {
      "run_name": "RUN_001",
      "control_mix": "HSV",
      "controls": {
        "ctrl123": {
          "control_name": "HSVNC",
          "control_well": "A01",
          "resolution": null
        }
      },
      "affected_samples_error": {
        "sample123": {
          "well_id": "sample123",
          "sample_name": "PATIENT_001",
          "well_number": "B01",
          "error_code": "INHERITED_CONTROL_FAILURE",
          "error_message": "Inherited from control failure",
          "mix_name": "HSV",
          "run_name": "RUN_001",
          "lims_status": null,
          "resolution_codes": null
        }
      },
      "affected_samples_repeat": {}
    }
  }
}
```

### Key Differences by Report Type

#### Control Report
- Has `affected_samples` section
- `clinical_category`: unresolved, error_ignored, test_repeated
- Filters for control wells only
- Includes control curves data

#### Sample Report
- No `affected_samples` section
- Same categories as control report
- Filters for patient samples only
- May include label errors based on flag

#### Discrepancy Report
- Different database schema (qst_discreps.db)
- Different categorization logic
- Categories map to sections:
  - Section 1: acted_upon
  - Section 2: samples_repeated
  - Section 3: ignored
- Has additional fields: machine_cls, final_cls, dxai_cls

## Phase 3: Implementation Steps

### Step 1: Create SQL to JSON extractors
- `extract_control_data.py` - Runs control queries, saves JSON
- `extract_sample_data.py` - Runs sample queries, saves JSON
- `extract_discrepancy_data.py` - Runs QST queries, saves JSON

### Step 2: Create test JSON files
- Limited to 20 records per mix for testing
- Include all sections and categories
- Validate JSON schema consistency

### Step 3: Adapt HTML generator
- Copy `generate_control_report_working.py` as base
- Modify to read from JSON instead of database
- Handle all three report types with same renderer
- Preserve all existing functionality:
  - Expandable/collapsible sections
  - Tabbed categories
  - Curve visualizations
  - Appendix for affected samples (control only)

## Phase 4: Testing Steps

### Step 1: Test SQL Queries
For each report type:
1. Run each SQL query independently
2. Verify row counts match expectations:
   - **Control Report**: 
     - 932 unresolved + 2229 resolved (1942 ignored + 287 repeated) = 3161 total
     - Affected: 8816 ERROR + 3936 REPEATS
   - **Sample Report**: 
     - 11390 unresolved + 8264 resolved (1892 ignored + 6372 repeated) = 19654 total
   - **Discrepancy Report**: 
     - 374 acted upon + 1307 repeated + 3906 ignored = 5587 displayed (387 suppressed)
3. Verify categorization logic:
   - Control: RP/RX/TN → test_repeated, others → error_ignored
   - Sample: Check lims_status for categorization
4. Save sample results to verify structure

### Step 2: Test JSON Generation
1. Create small test JSON files (5-10 records per category)
2. Validate JSON structure matches schema
3. Ensure all required fields present
4. Test with limited data first (--limit 100)
5. Generate full JSON files for each report type:
   - `control_data_test.json`
   - `sample_data_test.json`
   - `discrepancy_data_test.json`

### Step 3: Test HTML Generation
Using Playwright MCP tools for visual comparison:

```bash
# Step 1: Open original report
mcp__playwright__browser_navigate --url "file:///path/control_report_working.html"
mcp__playwright__browser_take_screenshot --filename "original_summary.png"

# Step 2: Capture key elements from original
mcp__playwright__browser_evaluate --function "() => {
    const stats = document.querySelector('.summary-stats');
    return {
        total: stats.querySelector('.stat-value').textContent,
        unresolved: stats.querySelectorAll('.stat-value')[1].textContent,
        ignored: stats.querySelectorAll('.stat-value')[2].textContent,
        repeated: stats.querySelectorAll('.stat-value')[3].textContent
    }
}"

# Step 3: Open new report  
mcp__playwright__browser_navigate --url "file:///path/control_report_from_json.html"
mcp__playwright__browser_take_screenshot --filename "new_summary.png"

# Step 4: Compare specific sections
mcp__playwright__browser_click --element "Mix: HSV section" --ref "mix-HSV"
mcp__playwright__browser_take_screenshot --filename "hsv_section_comparison.png"

# Step 5: Test interactions
mcp__playwright__browser_click --element "Unresolved tab" --ref "tab-unresolved"
mcp__playwright__browser_click --element "View Affected Samples link" --ref "affected-link"
```

**Automated Visual Checks**:
1. Screenshot summary stats for pixel comparison
2. Extract text values for exact matching
3. Expand specific mix sections and compare
4. Test tab switching functionality
5. Verify affected samples links work
6. Check curve rendering in specific error cards

**Files to compare**:
- Original: `output_data/control_report_working.html`
- New: `output_data/control_report_from_json.html`
- Sample baseline: `output_data/sample_report_baseline.html`
- Discrepancy baseline: `output_data/discrepancy_report_baseline.html`

**Key validations using MCP**:
1. Summary stats match exactly
2. Mix groupings are identical
3. Category tabs show same counts
4. Affected samples appendix matches (control only)
5. Curves display correctly
6. Expand/collapse functionality works
7. Links from controls to affected samples work

### Step 4: Integration Testing
1. Run full pipeline: SQL → JSON → HTML
2. Test all command-line options:
   - `--include-label-errors` for sample report
   - `--limit` for partial data
   - `--max-per-category` for display limits
3. Compare performance (JSON should be faster than direct SQL)

## Phase 5: Final Validation

### Requirements to Preserve
1. **Control Report**:
   - Must show 3,161 total errors (932 unresolved, 1,942 ignored, 287 repeated)
   - Must show 8,816 ERROR affected samples, 3,936 REPEATS
   - Must have working links from controls to affected samples

2. **Sample Report**:
   - Must show patient sample errors correctly
   - Must handle optional label errors flag

3. **Discrepancy Report**:
   - Must categorize into 3 sections correctly
   - Must handle QST database schema

### Files to Generate
1. `control_data.json` - Full control report data
2. `sample_data.json` - Full sample report data  
3. `discrepancy_data.json` - Full discrepancy data
4. `generate_html_from_json.py` - Based on generate_control_report_working.py

## Issues to Avoid
1. Don't start from scratch - use existing working code
2. Preserve exact SQL queries and categorization logic
3. Keep all HTML/CSS/JS functionality from working version
4. Test incrementally - one report type at a time
5. Validate counts match original reports exactly

## Next Steps
1. Review this plan for accuracy
2. Create SQL extractors one at a time
3. Generate test JSON files
4. Adapt HTML generator from working code
5. Test and validate each report type