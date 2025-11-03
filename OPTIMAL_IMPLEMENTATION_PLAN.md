# Optimal Implementation Plan: CSV-Driven Multi-Database Reporting

## Executive Summary

**Goal:** Generate unified HTML reports (detailed + summary) for all three databases (QST, Notts, Vira) using:
1. Single JSON extractor with CSV-driven categorization
2. Single detailed HTML generator (reused)
3. Single summary HTML generator (reused)

**Key Insight:** The existing HTML generators are already database-agnostic. We only need to make the JSON extractor database-agnostic.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATABASE CONFIG SYSTEM                        │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │   QST    │    │  NOTTS   │    │   VIRA   │                  │
│  │ Config   │    │ Config   │    │ Config   │                  │
│  └──────────┘    └──────────┘    └──────────┘                  │
│  - Category CSV  - Category CSV  - Category CSV                 │
│  - Control SQL   - Control SQL   - Control SQL                  │
│  - LIMS mapping  - LIMS mapping  - LIMS mapping                 │
│  - DB path       - DB path       - DB path                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│            UNIFIED JSON EXTRACTOR (CSV-DRIVEN)                   │
│                extract_report_with_curves.py                     │
│                                                                   │
│  Input: --db-type {qst|notts|vira} --since YYYY-MM-DD          │
│                                                                   │
│  Process:                                                         │
│  1. Load database config                                         │
│  2. Load category CSV                                            │
│  3. Query database with database-specific SQL                    │
│  4. Normalize LIMS values                                        │
│  5. Lookup category from CSV                                     │
│  6. Build JSON structure                                         │
│                                                                   │
│  Output: {db_type}_report_YYYYMMDD.json                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
         ┌────────────────────┴────────────────────┐
         ↓                                          ↓
┌─────────────────────┐                  ┌─────────────────────┐
│  DETAILED HTML      │                  │  SUMMARY HTML       │
│  GENERATOR          │                  │  GENERATOR          │
│  (UNCHANGED)        │                  │  (UNCHANGED)        │
│                     │                  │                     │
│  Input: JSON        │                  │  Input: JSON        │
│  Output: HTML       │                  │  Output: HTML       │
│  with curves        │                  │  with pie charts    │
└─────────────────────┘                  └─────────────────────┘
```

---

## Critical Gap Analysis

### What the Current Plan MISSES:

1. **❌ No Database Config System**
   - Current plan: Pass --category-csv manually
   - Problem: User must know which CSV for which database
   - Optimal: `--db-type notts` auto-selects `notts_category_mapping_v1.csv`

2. **❌ Control Detection Not Addressed**
   - Current plan: Mentions in "challenges" but no implementation
   - Problem: QST, Notts, Vira have different role_alias patterns
   - Optimal: Database config includes control SQL patterns

3. **❌ LIMS Normalization Not Integrated**
   - Current plan: Mentioned in challenges, not in implementation steps
   - Problem: `HSV_1_DETECTED` (Notts) needs to become `DETECTED` for CSV lookup
   - Optimal: Normalize before CSV lookup, using database-specific mapping

4. **❌ No CSV Coverage Validation**
   - Current plan: Assumes CSV covers all database patterns
   - Problem: New error combinations may not be in CSV
   - Optimal: Validation script to check coverage before running

5. **❌ No Incremental Migration Path**
   - Current plan: All-or-nothing replacement
   - Problem: Risky, hard to debug
   - Optimal: Hybrid mode (CSV + fallback to hardcoded)

---

## Optimal Implementation Strategy

### Phase 0: Database Configuration System (NEW)

**Create `database_configs.py`:**

```python
import os

DB_CONFIGS = {
    'qst': {
        'name': 'QST Production',
        'db_path': 'input_data/readings.db',
        'category_csv': 'output_data/qst_category_mapping_v1.csv',

        # Control detection SQL (injected into WHERE clauses)
        'control_where': """
            (w.role_alias LIKE '%PC' OR w.role_alias LIKE '%NC'
             OR w.role_alias LIKE 'PC%' OR w.role_alias LIKE 'NC%')
        """,

        # LIMS status normalization
        'lims_mapping': {
            'MPX & OPX DETECTED': 'DETECTED',
            'MPX & OPX NOT DETECTED': 'NOT DETECTED',
            'HSV1_DETECTED': 'DETECTED',
            'HSV2_DETECTED': 'DETECTED',
            'HSV_NOT_DETECTED': 'NOT DETECTED',
        },

        # Has pcr_ai_classifications table?
        'has_classifications': True,

        # Default date range
        'default_since': '2024-06-01',
    },

    'notts': {
        'name': 'Nottingham',
        'db_path': 'input_data/notts.db',
        'category_csv': 'output_data/notts_category_mapping_v1.csv',

        'control_where': """
            (w.role_alias LIKE '%NEG%' OR w.role_alias LIKE '%NTC%'
             OR w.role_alias LIKE '%QS%' OR w.role_alias LIKE '%Neg%'
             OR w.role_alias LIKE '%neg%')
        """,

        'lims_mapping': {
            'HSV_1_DETECTED': 'DETECTED',
            'HSV_2_DETECTED': 'DETECTED',
            'HSV_1_2_DETECTED': 'DETECTED',
            'ADENOVIRUS_DETECTED': 'DETECTED',
            'BKV_DETECTED': 'DETECTED',
            'VZV_DETECTED': 'DETECTED',
            'HSV_1_VZV_DETECTED': 'DETECTED',
            '<1500': 'DETECTED',  # Quantified detection
            'Detected <500IU/ml': 'DETECTED',
            'Detected_btw_loq_lod': 'DETECTED',
        },

        'has_classifications': True,
        'default_since': '2024-01-01',
    },

    'vira': {
        'name': 'Vira',
        'db_path': 'input_data/vira.db',
        'category_csv': 'output_data/vira_category_mapping_v1.csv',

        'control_where': """
            w.role_alias IN ('CC1', 'CC2', 'POS', 'NEC', 'NTC', 'S#')
        """,

        'lims_mapping': {
            'DETECTED_QUANT': 'DETECTED',
            'DETECTED_LOQ': 'DETECTED',
            'DETECTED_HIQ': 'DETECTED',
        },

        'has_classifications': True,
        'default_since': '2024-01-01',
    },
}

def get_config(db_type):
    """Get configuration for specified database type"""
    if db_type not in DB_CONFIGS:
        raise ValueError(f"Unknown database type: {db_type}. Must be one of {list(DB_CONFIGS.keys())}")
    return DB_CONFIGS[db_type]
```

**Why this is critical:**
- Single source of truth for all database-specific logic
- Easy to add new databases
- Testable independently
- Clear documentation of differences

### Phase 1: CSV Coverage Validation (NEW)

**Create `validate_csv_coverage.py`:**

```python
#!/usr/bin/env python3
"""
Validate that category mapping CSV covers all patterns in the database
"""

import sqlite3
import csv
import sys
from database_configs import get_config

def validate_coverage(db_type):
    """Check if CSV covers all error/resolution/lims combinations in database"""

    config = get_config(db_type)

    # Load CSV patterns
    csv_patterns = set()
    with open(config['category_csv'], 'r') as f:
        for line in f:
            if not line.startswith('#'):
                break
        reader = csv.DictReader([line] + list(f))
        for row in reader:
            key = (
                row['WELL_TYPE'],
                row['ERROR_CODE'],
                row['RESOLUTION_CODES'],
                row['WELL_LIMS_STATUS']
            )
            csv_patterns.add(key)

    # Query unique patterns from database
    conn = sqlite3.connect(config['db_path'])

    # Samples
    query = f"""
        SELECT DISTINCT
            'SAMPLE' as well_type,
            COALESCE(ec.error_code, '') as error_code,
            COALESCE(w.resolution_codes, '') as resolution_codes,
            COALESCE(w.lims_status, '') as lims_status,
            COUNT(*) as count
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE NOT ({config['control_where']})
        GROUP BY well_type, error_code, resolution_codes, lims_status
    """

    sample_patterns = set()
    missing_patterns = []

    for row in conn.execute(query):
        db_key = (row[0], row[1], row[2], row[3])

        # Normalize LIMS before checking
        normalized_lims = config['lims_mapping'].get(row[3], row[3])
        normalized_key = (row[0], row[1], row[2], normalized_lims)

        if normalized_key not in csv_patterns and db_key not in csv_patterns:
            missing_patterns.append({
                'well_type': row[0],
                'error_code': row[1],
                'resolution_codes': row[2],
                'lims_status': row[3],
                'count': row[4]
            })

    # Repeat for controls...

    conn.close()

    # Report
    if missing_patterns:
        print(f"⚠️  {db_type.upper()}: {len(missing_patterns)} patterns NOT in CSV:")
        for p in missing_patterns[:10]:  # Show first 10
            print(f"   {p['well_type']:8} {p['error_code']:30} {p['resolution_codes']:30} {p['lims_status']:20} ({p['count']} wells)")
        if len(missing_patterns) > 10:
            print(f"   ... and {len(missing_patterns) - 10} more")
        return False
    else:
        print(f"✓ {db_type.upper()}: All database patterns covered by CSV")
        return True

if __name__ == '__main__':
    all_valid = True
    for db_type in ['qst', 'notts', 'vira']:
        valid = validate_coverage(db_type)
        all_valid = all_valid and valid
        print()

    sys.exit(0 if all_valid else 1)
```

**Why this is critical:**
- Catches missing CSV entries BEFORE running reports
- Prevents "UNKNOWN category" errors
- Shows exactly which patterns need to be added to CSV

### Phase 2: Modify JSON Extractor

**Key changes to `extract_report_with_curves.py`:**

```python
import csv
from database_configs import get_config, DB_CONFIGS

# Add at top of file
class CategoryLookup:
    """Thread-safe category lookup with LIMS normalization"""

    def __init__(self, csv_path, lims_mapping):
        self.lookup = {}
        self.lims_mapping = lims_mapping
        self.load_csv(csv_path)

    def load_csv(self, csv_path):
        """Load category mappings from CSV"""
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Skip header comments
            for line in f:
                if not line.startswith('#'):
                    break

            reader = csv.DictReader([line] + f.readlines())

            for row in reader:
                key = (
                    row['WELL_TYPE'],
                    row['ERROR_CODE'],
                    row['RESOLUTION_CODES'],
                    row['WELL_LIMS_STATUS']
                )
                self.lookup[key] = row['CATEGORY']

        print(f"Loaded {len(self.lookup)} category mappings from {csv_path}")

    def get_category(self, well_type, error_code, resolution_codes, lims_status):
        """Get category with LIMS normalization"""

        # Normalize empty values
        error_code = error_code or ''
        resolution_codes = resolution_codes or ''
        lims_status = lims_status or ''

        # Try direct lookup first
        key = (well_type, error_code, resolution_codes, lims_status)
        if key in self.lookup:
            return self.lookup[key]

        # Try with normalized LIMS
        normalized_lims = self.lims_mapping.get(lims_status, lims_status)
        if normalized_lims != lims_status:
            normalized_key = (well_type, error_code, resolution_codes, normalized_lims)
            if normalized_key in self.lookup:
                return self.lookup[normalized_key]

        # Not found - log warning
        print(f"⚠️  Missing category for: well_type={well_type}, error_code={error_code}, "
              f"resolution={resolution_codes}, lims={lims_status}")

        # Return safe default
        return 'SOP_UNRESOLVED' if error_code else 'IGNORE_WELL'


def main():
    parser = argparse.ArgumentParser(description='Extract unified report with curves')

    # NEW: Database type parameter
    parser.add_argument('--db-type',
                       choices=list(DB_CONFIGS.keys()),
                       required=True,
                       help='Database type (qst, notts, or vira)')

    # Optional overrides
    parser.add_argument('--db',
                       help='Database path (overrides config default)')
    parser.add_argument('--category-csv',
                       help='Category CSV path (overrides config default)')
    parser.add_argument('--since',
                       help='Start date YYYY-MM-DD (overrides config default)')
    parser.add_argument('--output',
                       help='Output JSON path (default: auto-generated)')

    args = parser.parse_args()

    # Load configuration
    config = get_config(args.db_type)

    # Apply overrides or use defaults
    db_path = args.db or config['db_path']
    category_csv = args.category_csv or config['category_csv']
    since_date = args.since or config['default_since']
    output_path = args.output or f"output_data/{args.db_type}_report_{since_date.replace('-', '')}.json"

    print(f"=== Extracting {config['name']} Report ===")
    print(f"Database: {db_path}")
    print(f"Category CSV: {category_csv}")
    print(f"Since: {since_date}")
    print(f"Output: {output_path}")
    print()

    # Initialize category lookup
    category_lookup = CategoryLookup(category_csv, config['lims_mapping'])

    # Connect to database
    conn = sqlite3.connect(db_path)

    # Pass config and category_lookup to all fetch functions
    report_data = {
        'metadata': {
            'database': args.db_type,
            'database_name': config['name'],
            'generated_date': datetime.now().isoformat(),
            'since_date': since_date,
        },
        'valid_results': fetch_valid_results(conn, config, category_lookup, since_date),
        'error_statistics': calculate_error_statistics(conn, config, since_date),
        'reports': {
            'sample': fetch_sample_report(conn, config, category_lookup, since_date),
            'control': fetch_control_report(conn, config, category_lookup, since_date),
            'discrepancy': fetch_discrepancy_report(conn, config, category_lookup, since_date),
        }
    }

    # Write JSON
    with open(output_path, 'w') as f:
        json.dump(report_data, f, indent=2)

    print(f"\n✓ Report written to: {output_path}")


def fetch_valid_results(conn, config, category_lookup, since_date):
    """Fetch valid results using CSV categorization"""

    # Query all wells (samples)
    query = f"""
        SELECT
            w.id,
            COALESCE(ec.error_code, '') as error_code,
            COALESCE(w.resolution_codes, '') as resolution_codes,
            COALESCE(w.lims_status, '') as lims_status
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.extraction_date >= ?
          AND NOT ({config['control_where']})
    """

    samples_detected = 0
    samples_not_detected = 0
    samples_other = 0

    for row in conn.execute(query, (since_date,)):
        category = category_lookup.get_category('SAMPLE', row[1], row[2], row[3])

        if category == 'VALID_DETECTED':
            samples_detected += 1
        elif category == 'VALID_NOT_DETECTED':
            samples_not_detected += 1
        elif category == 'VALID_OTHER':
            samples_other += 1
        # Ignore other categories (errors, etc.)

    # Query controls
    query = f"""
        SELECT
            w.id,
            COALESCE(ec.error_code, '') as error_code,
            COALESCE(w.resolution_codes, '') as resolution_codes,
            COALESCE(w.lims_status, '') as lims_status
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.extraction_date >= ?
          AND ({config['control_where']})
    """

    controls_passed = 0

    for row in conn.execute(query, (since_date,)):
        category = category_lookup.get_category('CONTROL', row[1], row[2], row[3])

        if category == 'VALID_CONTROL':
            controls_passed += 1

    return {
        'samples_detected': samples_detected,
        'samples_not_detected': samples_not_detected,
        'samples_other_status': samples_other,
        'controls_passed': controls_passed,
    }


def fetch_unresolved_errors(conn, config, category_lookup, since_date, well_type):
    """Fetch unresolved errors using CSV categorization"""

    is_control = (well_type == 'CONTROL')
    where_clause = config['control_where'] if is_control else f"NOT ({config['control_where']})"

    query = f"""
        SELECT
            w.*,
            ec.error_code,
            ec.error_message
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.extraction_date >= ?
          AND {where_clause}
    """

    unresolved_wells = []

    for row in conn.execute(query, (since_date,)):
        category = category_lookup.get_category(
            well_type,
            row['error_code'] or '',
            row['resolution_codes'] or '',
            row['lims_status'] or ''
        )

        if category in ['SOP_UNRESOLVED', 'DISCREP_IN_ERROR']:
            # Fetch well curves...
            well_data = build_well_data(row, conn)
            unresolved_wells.append(well_data)
        elif category == 'IGNORE_WELL':
            continue  # Skip
        # Other categories handled by other functions

    return unresolved_wells

# Similar modifications for:
# - fetch_resolved_errors()
# - fetch_resolved_with_new()
# - fetch_discrepancy_report()
```

### Phase 3: Command Line Interface

**Simple, intuitive commands:**

```bash
# Generate all three database reports
python3 extract_report_with_curves.py --db-type qst --since 2024-06-01
python3 extract_report_with_curves.py --db-type notts --since 2024-01-01
python3 extract_report_with_curves.py --db-type vira --since 2024-01-01

# Override defaults if needed
python3 extract_report_with_curves.py --db-type qst --db /path/to/custom.db --output custom_output.json

# Generate HTML reports (same command for all databases)
python3 generate_report_from_json_with_graphs.py --input output_data/notts_report_20240101.json --output output_data/notts_report.html
python3 generate_summary_report.py --input output_data/notts_report_20240101.json --output output_data/notts_summary.html
```

---

## Testing Strategy

### 1. Pre-Implementation Validation

```bash
# Validate CSV coverage BEFORE changing code
python3 validate_csv_coverage.py

# Expected output:
# ✓ QST: All database patterns covered by CSV
# ✓ NOTTS: All database patterns covered by CSV
# ✓ VIRA: All database patterns covered by CSV
```

### 2. Baseline Generation (All Databases)

```bash
# Generate baselines with current (pre-CSV) code
for db_type in qst notts vira; do
    python3 extract_report_with_curves.py --db-type $db_type --output output_data/${db_type}_baseline.json
    python3 generate_report_from_json_with_graphs.py --input output_data/${db_type}_baseline.json --output output_data/${db_type}_baseline.html
    python3 generate_summary_report.py --input output_data/${db_type}_baseline.json --output output_data/${db_type}_baseline_summary.html
done
```

### 3. Post-Implementation Testing

```bash
# Generate new reports with CSV-driven code
for db_type in qst notts vira; do
    python3 extract_report_with_curves.py --db-type $db_type --output output_data/${db_type}_csv_driven.json
done

# Compare metrics
for db_type in qst notts vira; do
    echo "=== $db_type ==="
    python3 compare_reports.py output_data/${db_type}_baseline.json output_data/${db_type}_csv_driven.json
done
```

### 4. Visual Verification

Open reports in browser:
- QST baseline vs CSV-driven
- Notts baseline vs CSV-driven
- Vira baseline vs CSV-driven

Check:
- Same tab counts
- Same well distributions
- Same error categories
- Same summary statistics

---

## Implementation Timeline

### Week 1: Foundation
- ✅ Day 1-2: Create database_configs.py
- ✅ Day 2-3: Create validate_csv_coverage.py
- ✅ Day 3-4: Run validation, update CSVs if gaps found
- ✅ Day 4-5: Generate all baselines

### Week 2: Implementation
- 📝 Day 1-2: Add CategoryLookup class to extractor
- 📝 Day 2-3: Modify fetch_valid_results()
- 📝 Day 3-4: Modify fetch_unresolved/resolved/resolved_with_new()
- 📝 Day 4-5: Modify fetch_discrepancy_report()

### Week 3: Testing
- 🧪 Day 1-2: Test QST (largest database)
- 🧪 Day 2-3: Test Notts
- 🧪 Day 3-4: Test Vira
- 🧪 Day 4-5: Cross-database validation

### Week 4: Finalization
- 📄 Day 1-2: Generate production reports for all databases
- 📊 Day 2-3: Create comparison documentation
- ✅ Day 3-4: Code review and cleanup
- 🚀 Day 4-5: Deploy and document

---

## Success Metrics

✅ **Coverage:** All three CSVs cover 100% of database patterns (validated)

✅ **Parity:** Baseline vs CSV-driven metrics match within 1%

✅ **Reusability:** Same HTML generators work for all three databases (no changes)

✅ **Maintainability:** Adding new database requires only new config entry

✅ **Performance:** Report generation time < 5 minutes per database

✅ **Completeness:** All sections populated (unresolved, resolved, discrepancy, controls, summary)

---

## Key Improvements Over Original Plan

| Aspect | Original Plan | Optimal Plan |
|--------|---------------|--------------|
| **Database Config** | Manual CSV path | Auto-selected by --db-type |
| **Control Detection** | Mentioned in challenges | Integrated in config |
| **LIMS Normalization** | Mentioned in challenges | Built into CategoryLookup |
| **Coverage Validation** | Not included | Pre-flight validation script |
| **Command Line** | Multiple parameters | Single --db-type parameter |
| **Testing** | Ad-hoc | Structured 3-database matrix |
| **Migration** | All-or-nothing | Incremental with fallback |

---

## Risk Mitigation

**Risk 1: CSV Missing Patterns**
- Mitigation: validate_csv_coverage.py catches before runtime
- Fallback: CategoryLookup returns safe default (SOP_UNRESOLVED or IGNORE_WELL)

**Risk 2: LIMS Variants**
- Mitigation: Database config includes all known mappings
- Fallback: Use original LIMS value if no mapping found

**Risk 3: Control Detection Errors**
- Mitigation: Test control counts match baseline
- Fallback: Config can be updated without code changes

**Risk 4: Performance Degradation**
- Mitigation: CSV lookup is O(1) hash lookup
- Fallback: Can cache lookups if needed

**Risk 5: JSON Structure Changes**
- Mitigation: Keep same JSON structure, only change categorization logic
- Fallback: HTML generators remain unchanged

---

## Next Steps

1. ✅ Review this optimal plan
2. 📝 Create database_configs.py
3. 📝 Create validate_csv_coverage.py
4. 🧪 Run validation on all three databases
5. 📊 Update CSVs if gaps found
6. 🚀 Begin Phase 2 implementation

---

## Summary

**The optimal approach is:**
1. **Single unified extractor** with database config system
2. **CSV-driven categorization** with LIMS normalization
3. **Validation first** to ensure CSV coverage
4. **Reuse existing HTML generators** (no changes needed)
5. **Simple CLI** (--db-type auto-selects everything)

This approach:
- ✅ Maximizes code reuse
- ✅ Minimizes changes to working code
- ✅ Validates before implementing
- ✅ Easy to test and debug
- ✅ Easy to add new databases
- ✅ Clear separation of concerns

**Result:** Unified reporting for all three databases with minimal risk and maximum maintainability.
