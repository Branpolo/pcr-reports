# Discrepancy Report SQL Filtering Logic

## Overview
This document describes the SQL logic for extracting classification discrepancies from the Quest production database, replacing the need for a separate QST database.

## Key Filters

### 1. Base Filters
- **Role**: `role_alias = 'Patient'` (excludes controls)
- **Error/Resolution**: Wells must have `error_code_id in {classification discrepancy code} OR resolution_codes IS NOT NULL`

### 2. Discrepancy Identification
Wells must have observations where:
- `machine_cls != dxai_cls`
- `dxai_cls IS NOT NULL`

### 3. Target Exclusions
Exclude Internal Control (IC) targets:
```sql
AND t.target_name NOT LIKE '%IC%'
AND t.target_name NOT LIKE '%IPC%'
AND t.target_name NOT IN ('IC', 'IPC', 'QIPC', 'QIC')
```

### 4. Well-Level Exclusions
Exclude wells where:
- **ONLY IC targets have discrepancies** (non-IC targets must have discrepancies)
- **Resolution contains SKIP but not BLA** (ambiguous resolutions)
- **Error code is control-related** (not classification discrepancy)

### 5. Classification-Only Error Codes
Include ONLY wells with classification discrepancy error codes:
```sql
-- Classification discrepancy error codes (examples):
'937829a3-00d6-4015-be4b-02af64083857' -- "There are one or more classification discrepancies"
-- And other similar classification error codes

-- EXCLUDE control-related errors like:
'937829a3-aa88-44cf-bbd5-deade616cff5' -- "An associated extraction control has failed"
'98b5395c-97be-4dbd-b185-9a57a25a31ca' -- "This well is missing required extraction controls"
```

## Categorization Logic

### Category 1: "Error Ignored"
- Resolution contains `'BLA'`
- ALL non-IC observations have `final_cls = machine_cls`
- Error has been reviewed and accepted

### Category 2: "Result changed"
- Resolution contains `'BLA'`
- NOT all non-IC observations have `final_cls = machine_cls`
- Partially resolved discrepancy

### Category 3: "Well excluded"
- Unresolved classification error

## SQL Implementation Pattern

```sql
-- Base query for classification discrepancies
SELECT DISTINCT w.*, o.*
FROM wells w
JOIN observations o ON o.well_id = w.id
JOIN targets t ON o.target_id = t.id
WHERE 
    -- Base filters
    w.extraction_date >= '2024-01-01'
    AND w.role_alias = 'Patient'
    AND (w.error_code_id IS NOT NULL OR w.resolution_codes IS NOT NULL)
    
    -- Classification discrepancy
    AND o.machine_cls != o.dxai_cls
    AND o.dxai_cls IS NOT NULL
    
    -- Exclude IC targets
    AND t.target_name NOT LIKE '%IC%'
    AND t.target_name NOT LIKE '%IPC%'
    AND t.target_name NOT IN ('IC', 'IPC', 'QIPC', 'QIC')
    
    -- Classification error codes only
    AND (w.error_code_id IN (
        SELECT id FROM error_codes 
        WHERE error_message_id in {classification_discrepancy_ids})
```

## Statistics (2024+ Data)
- **Total classification discrepancies**: ~6,508 wells (6,080 unique samples)
- **Category 1 (Error Ignored)**: ~4,122 wells (63%)
- **Category 2 (BLA Partial)**: ~357 wells (5%)
- **Category 3 (Classification Error)**: ~2,029 wells (31%)

## Comparison with Legacy QST Database
- QST has 5,418 unique samples (2024+)
- Our approach finds 6,080 unique samples
- Difference: We include 662 more samples (mostly unresolved errors)
- QST strongly favors BLA-resolved discrepancies (99-100% inclusion)
- QST excludes most unresolved errors (only ~15% included)