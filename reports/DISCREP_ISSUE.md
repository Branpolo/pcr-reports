# Discrepancy Report SQL Query Mismatch

## Problem
Python code returns different counts than user's SQL queries for Notts database discrepancy categorization.

## User's Expected Results (from SQL)
- **Acted upon**: 23 results
- **Ignored**: 171 results
- **Samples repeated**: 380 results
- **Total**: 574 results (but UNION returns only 542 unique wells)

## Current Python Results
- **Acted upon**: 126
- **Ignored**: 40
- **Samples repeated**: 184
- **Total**: 350

## Database Facts
- 2036 total wells with `machine_cls != dxai_cls`
- 2271 total observations (targets) with discrepancies
- Average ~1.1 observations per well

## User's SQL Queries

###1. Acted Upon (23 results)
```sql
SELECT * FROM wells, observations WHERE
  observations.well_id = wells.id
  AND observations.machine_cls != observations.dxai_cls
  AND observations.final_cls = observations.dxai_cls
  AND (wells.lims_status LIKE '%detected%' OR wells.lims_status LIKE '%1500%')
  AND wells.resolution_codes LIKE '%bla%';
```

### 2. Ignored (171 results)
```sql
SELECT * FROM wells, observations WHERE
  observations.well_id = wells.id
  AND observations.machine_cls != observations.dxai_cls
  AND observations.final_cls = observations.machine_cls
  AND wells.lims_status LIKE '%detected%'
  AND wells.resolution_codes LIKE '%bla%';
```

### 3. Samples Repeated (380 results)
```sql
SELECT * FROM wells, observations WHERE
  observations.well_id = wells.id
  AND observations.machine_cls != observations.dxai_cls
  AND (wells.lims_status NOT LIKE '%detected%' AND wells.lims_status NOT LIKE '%1500%' OR wells.lims_status IS NULL)
  AND wells.error_code_id NOT IN ('9a404521-fba9-48e4-bbf2-82e80204952a')  -- INHERITED_EXTRACTION_FAILURE
  AND wells.role_alias LIKE 'Patient';
```

## Key Questions

1. **Per-Observation vs Per-Well Logic**: The SQL joins wells to observations, meaning it checks each observation (target) separately. Should categorization be per-observation or per-well?

2. **Multi-Target Wells**: If a well has 3 targets where 1 has `final==dxai` and 2 have `final==machine`, should it be:
   - In "acted_upon" (because at least one target was changed)?
   - In "ignored" (because at least one target was NOT changed)?
   - In both categories (one row per observation)?
   - In the dominant category only?

3. **Row Count vs Well Count**: User says "23 results" but unclear if this means 23 rows (observations) or 23 wells. Verification shows it's 23 unique wells with 23 observations.

## Discrepancies to Investigate

1. **Why 574 total but 542 unique wells?**: Some wells appear in multiple categories (32 overlap)
2. **Python getting too many acted_upon**: 126 vs expected 23
3. **Python getting too few ignored**: 40 vs expected 171
4. **Python getting too few repeated**: 184 vs expected 380

## Next Steps

**NEED USER CLARIFICATION**:
- Should categorization be per-observation (target) or per-well?
- If a well has mixed results across targets, which category should it go in?
- Are the SQL queries the definitive source of truth?
