# Discrepancy Report Fix - Implementation Plan for Quest DB

## Problem Summary
The discrepancy report categorization is broken, producing incorrect counts:
- **acted_upon**: 219 (expected ~221) ✅ 99% accurate
- **samples_repeated**: 4,510 (expected ~2,690) ❌ 1,820 too many
- **ignored**: 4,981 (expected ~3,729) ❌ 1,252 too many

**Root Cause**: Control-affected samples (INHERITED_CONTROL_FAILURE, INHERITED_EXTRACTION_FAILURE, EXTRACTION_CONTROLS_MISSING) are incorrectly being included in the discrepancy report when they should be suppressed (already in control report).

## Phase 1: Update CSV Category Mappings

**File**: `/home/azureuser/code/wssvc-flow-codex/output_data/qst_category_mapping_v1.csv`

### Changes:

1. **Check if INHERITED_CONTROL_FAILURE exists in CSV**:
   - If present: Change category to `CONTROL_AFFECTED_SAMPLE`
   - If not present: Skip (not in Quest DB data)

2. **Update EXTRACTION_CONTROLS_MISSING** (currently line 44):
   - Change category from `SOP_UNRESOLVED` to `NO_CONTROLS`
   - Rationale: This error means missing controls (not failed controls), so can't be in control report appendix
   - Update notes to clarify this distinction

3. **Verify INHERITED_EXTRACTION_FAILURE** (line 33):
   - Already has category `CONTROL_AFFECTED_SAMPLE` ✅
   - No change needed

## Phase 2: Fix discrepancy_enrich() Function

**File**: `/home/azureuser/code/wssvc-flow-codex/reports/extract_report_with_curves.py`

### Changes:

#### 2.1 Remove hardcoded control error codes (lines 1131-1137)
Delete the entire `control_extraction_error_codes` set - we'll use CSV instead.

#### 2.2 Load excluded errors from CSV (add after line 1126)
Add code to build set of error codes to exclude from discrepancy report:

```python
# Load error codes that should be excluded from discrepancy report
# (control-affected samples and samples with no controls)
excluded_error_codes = set()
for error_code, category in args.category_lookup.lookup.values():
    if category in ('CONTROL_AFFECTED_SAMPLE', 'NO_CONTROLS'):
        # Extract error_code from the lookup key
        # lookup key format: (well_type, error_code, resolution_codes, lims_status)
        # We need to iterate through the actual keys to get error codes
        pass  # Implementation will extract error codes from CSV
```

**Better approach**: Iterate through CSV lookup keys to find matching categories:
```python
excluded_error_codes = set()
for key, category in args.category_lookup.lookup.items():
    well_type, error_code, resolution_codes, lims_status = key
    if well_type == 'SAMPLE' and category in ('CONTROL_AFFECTED_SAMPLE', 'NO_CONTROLS'):
        if error_code:  # Skip empty error codes
            excluded_error_codes.add(error_code)
```

#### 2.3 Add early suppression for excluded errors (add before Section 1 check, ~line 1194)
```python
# Skip control-affected samples (already in control report)
# and samples with no controls (can't be analyzed)
if error_code in excluded_error_codes:
    continue
```

#### 2.4 Simplify Section 2 logic (line 1211)
Change:
```python
# OLD (line 1211):
elif (not has_detected_lims) or (error_code and error_code not in control_extraction_error_codes):
```

To:
```python
# NEW:
elif (not has_detected_lims) or error_code:
```

Rationale: The control-affected exclusion is now handled in the early suppression check, so we don't need it here.

## Phase 3: Verify and Test

### 3.1 Run Quest Report Generation
```bash
python3 -m reports.extract_report_with_curves combined \
  --db-type qst \
  --db ~/dbs/quest_prod_aug2025.db \
  --output output_data/final/qst_full_csv.json \
  --sample-since-date 2024-06-01 --sample-until-date 2025-05-31 \
  --control-since-date 2024-06-01 --control-until-date 2025-05-31 \
  --discrepancy-since-date 2024-06-01 --discrepancy-until-date 2025-05-31
```

### 3.2 Check Category Breakdown
```bash
python3 check_disc_cats.py
```

### 3.3 Expected Results
| Category         | Current | Expected | Status           |
|------------------|---------|----------|------------------|
| acted_upon       | 219     | ~221     | ✅ 99% accurate  |
| samples_repeated | 4,510   | ~2,690   | ❌ Should drop   |
| ignored          | 4,981   | ~3,729   | ❌ Should drop   |
| **Total**        | 9,710   | ~6,640   | ❌ Should drop   |

After fix, expect:
- **acted_upon**: ~221
- **samples_repeated**: ~2,690
- **ignored**: ~3,729
- **Total**: ~6,640

## Important Notes

### Current Code Analysis

**Line 1191** - has_detected_lims check:
```python
has_detected_lims = 'DETECTED' in lims_status if lims_status else False
```
✅ This is CORRECT - matches SQL `LIKE '%detected%'` pattern (includes both 'DETECTED' and 'NOT DETECTED')

**Lines 1203-1207** - Section 1 (acted_upon):
```python
if (has_changed_result and has_bla_resolution and has_detected_lims):
    record['clinical_category'] = 'acted_upon'
```
✅ This is CORRECT - matches reference SQL Section 1

**Line 1211** - Section 2 (samples_repeated):
❌ Includes control-affected samples - needs the fix above

### CSV Category Mapping Structure

CSV categories relevant to discrepancy report:
- **CONTROL_AFFECTED_SAMPLE**: Samples affected by failed controls (excluded from discrepancy report)
- **NO_CONTROLS**: Samples missing controls entirely (excluded from discrepancy report)
- **VALID_DETECTED / VALID_NOT_DETECTED**: Valid results (used for discrepancy categorization to correct section ONLY)
- **SOP_UNRESOLVED / SOP_REPEATED / SOP_IGNORED**: SOP errors (used for discrepancy categorization to correct section ONLY)

### Discrepancy Categorization Logic

**BASE CONDITION**: All records already have `machine_cls != dxai_cls` (fetched by initial query)

Within these discrepancy records, categorization is:

**VALID RESULT** = Wells with:
- `lims_status LIKE '%detected%'` (valid LIMS output: DETECTED, NOT DETECTED, HSV_DETECTED, etc.)
- `resolution_codes LIKE '%bla%'` (BLA present)
- No SOP error code (or only control-affected errors that get excluded)

**SOP ERROR** = Wells with:
- `lims_status NOT LIKE '%detected%'` (error LIMS: REAMP, REXCT, RPT, TNP, EXCLUDE, INCONCLUSIVE)
- OR `error_code IS NOT NULL` (has an SOP error code, excluding control-affected)

**Then the three sections are:**

1. **Section 1 (acted_upon)**:
   - VALID RESULT + `final_cls != machine_cls` (result was changed)
   - Has BLA + detected LIMS + result changed

2. **Section 2 (samples_repeated)**:
   - SOP ERROR (within discrepancy records where `machine_cls != dxai_cls`)
   - Test was repeated due to error or has non-valid LIMS status

3. **Section 3 (ignored)**:
   - VALID RESULT + `final_cls = machine_cls` (discrepancy acknowledged but result unchanged)
   - Has BLA + detected LIMS + result NOT changed

**CSV is used ONLY for**: Identifying which error codes to exclude (CONTROL_AFFECTED_SAMPLE and NO_CONTROLS categories).

## Success Criteria

1. ✅ CSV updated with correct categories
2. ✅ Hardcoded error lists removed
3. ✅ CSV-driven exclusion implemented
4. ✅ Category counts match expected values (~221, ~2,690, ~3,729)
5. ✅ Code is portable across databases (no hardcoded UUIDs or Quest-specific logic)
