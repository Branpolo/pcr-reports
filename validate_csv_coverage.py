#!/usr/bin/env python3
"""
Validate that category mapping CSVs cover all patterns in the databases

This script checks if the category CSVs have entries for all unique
(well_type, error_code, resolution_codes, lims_status) combinations
found in each database.

Run this BEFORE implementing CSV-driven categorization to ensure
no missing patterns that would cause "UNKNOWN" categories.
"""

import sqlite3
import csv
import sys
from database_configs import get_config, list_databases


def normalize_empty_value(value):
    """
    Normalize empty value representations

    CSV uses '[]' for empty values, database uses ''
    Normalize both to empty string for comparison
    """
    if value in ('[]', None, ''):
        return ''
    return value


def load_csv_patterns(csv_path, lims_mapping):
    """
    Load all patterns from category CSV

    Returns:
        set: Set of (well_type, error_code, resolution_codes, lims_status) tuples
    """
    csv_patterns = set()

    with open(csv_path, 'r', encoding='utf-8') as f:
        # Skip header comment lines
        for line in f:
            if not line.startswith('#'):
                break

        reader = csv.DictReader([line] + f.readlines())

        for row in reader:
            # Normalize empty values ([] -> '')
            key = (
                row['WELL_TYPE'],
                normalize_empty_value(row['ERROR_CODE']),
                normalize_empty_value(row['RESOLUTION_CODES']),
                normalize_empty_value(row['WELL_LIMS_STATUS'])
            )
            csv_patterns.add(key)

            # Also add normalized LIMS variant if applicable
            original_lims = normalize_empty_value(row['WELL_LIMS_STATUS'])
            normalized_lims = lims_mapping.get(original_lims, original_lims)
            if normalized_lims != original_lims:
                normalized_key = (
                    row['WELL_TYPE'],
                    normalize_empty_value(row['ERROR_CODE']),
                    normalize_empty_value(row['RESOLUTION_CODES']),
                    normalized_lims
                )
                csv_patterns.add(normalized_key)

    return csv_patterns


def get_database_patterns(db_path, control_where, well_type):
    """
    Get all unique patterns from database for given well type

    Args:
        db_path: Path to database
        control_where: SQL WHERE clause for control detection
        well_type: 'SAMPLE' or 'CONTROL'

    Returns:
        list: List of dicts with pattern and count
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Determine WHERE clause based on well type
    if well_type == 'CONTROL':
        type_where = control_where
    else:  # SAMPLE
        type_where = f"NOT ({control_where})"

    query = f"""
        SELECT DISTINCT
            '{well_type}' as well_type,
            COALESCE(ec.error_code, '') as error_code,
            COALESCE(w.resolution_codes, '') as resolution_codes,
            COALESCE(w.lims_status, '') as lims_status,
            COUNT(*) as count
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE {type_where}
        GROUP BY ec.error_code, w.resolution_codes, w.lims_status
        ORDER BY count DESC
    """

    patterns = []
    for row in conn.execute(query):
        patterns.append({
            'well_type': row['well_type'],
            'error_code': row['error_code'],
            'resolution_codes': row['resolution_codes'],
            'lims_status': row['lims_status'],
            'count': row['count']
        })

    conn.close()
    return patterns


def normalize_lims(lims_status, lims_mapping):
    """Normalize LIMS status using mapping"""
    return lims_mapping.get(lims_status, lims_status)


def validate_coverage(db_type, verbose=False):
    """
    Validate that CSV covers all patterns in database

    Args:
        db_type: Database type ('qst', 'notts', 'vira')
        verbose: Show all patterns if True

    Returns:
        tuple: (is_valid, missing_count, total_wells_missing)
    """
    config = get_config(db_type)

    print(f"=== {config['name']} ({db_type.upper()}) ===\n")

    # Load CSV patterns
    csv_patterns = load_csv_patterns(config['category_csv'], config['lims_mapping'])
    print(f"CSV has {len(csv_patterns)} unique patterns")

    # Get database patterns for samples and controls
    sample_patterns = get_database_patterns(
        config['db_path'],
        config['control_where'],
        'SAMPLE'
    )
    control_patterns = get_database_patterns(
        config['db_path'],
        config['control_where'],
        'CONTROL'
    )

    all_patterns = sample_patterns + control_patterns
    print(f"Database has {len(all_patterns)} unique patterns")
    print()

    # Find missing patterns
    missing_patterns = []
    total_wells_missing = 0

    for pattern in all_patterns:
        # Normalize database values to match CSV format
        db_key = (
            pattern['well_type'],
            normalize_empty_value(pattern['error_code']),
            normalize_empty_value(pattern['resolution_codes']),
            normalize_empty_value(pattern['lims_status'])
        )

        # Try normalized LIMS match
        normalized_lims = normalize_lims(pattern['lims_status'], config['lims_mapping'])
        normalized_key = (
            pattern['well_type'],
            normalize_empty_value(pattern['error_code']),
            normalize_empty_value(pattern['resolution_codes']),
            normalize_empty_value(normalized_lims)
        )

        # Check if either key exists in CSV
        if db_key not in csv_patterns and normalized_key not in csv_patterns:
            missing_patterns.append(pattern)
            total_wells_missing += pattern['count']

    # Report results
    if missing_patterns:
        print(f"⚠️  MISSING: {len(missing_patterns)} patterns NOT in CSV")
        print(f"⚠️  Affects {total_wells_missing:,} wells\n")

        # Show top missing patterns
        show_count = 20 if verbose else 10
        print(f"Top {min(show_count, len(missing_patterns))} missing patterns:")
        for i, p in enumerate(missing_patterns[:show_count], 1):
            print(f"  {i:2}. {p['well_type']:8} | "
                  f"EC:{p['error_code'] or '(none)':30} | "
                  f"RES:{p['resolution_codes'] or '(none)':30} | "
                  f"LIMS:{p['lims_status'] or '(none)':20} | "
                  f"{p['count']:>6,} wells")

        if len(missing_patterns) > show_count:
            remaining = len(missing_patterns) - show_count
            remaining_wells = sum(p['count'] for p in missing_patterns[show_count:])
            print(f"  ... and {remaining} more patterns ({remaining_wells:,} wells)")

        return False, len(missing_patterns), total_wells_missing

    else:
        print(f"✓ SUCCESS: All database patterns covered by CSV")
        total_wells = sum(p['count'] for p in all_patterns)
        print(f"✓ Coverage: {len(all_patterns)} patterns, {total_wells:,} wells")
        return True, 0, 0


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Validate category CSV coverage for databases',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all databases
  python validate_csv_coverage.py

  # Validate specific database with verbose output
  python validate_csv_coverage.py --db-type qst --verbose

  # Validate all and exit with error code if gaps found
  python validate_csv_coverage.py --strict
"""
    )

    parser.add_argument('--db-type',
                       choices=list_databases(),
                       help='Validate specific database only')
    parser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Show more missing patterns')
    parser.add_argument('--strict',
                       action='store_true',
                       help='Exit with error code if any gaps found')

    args = parser.parse_args()

    # Determine which databases to validate
    if args.db_type:
        databases = [args.db_type]
    else:
        databases = list_databases()

    # Validate each database
    all_valid = True
    total_missing = 0
    total_wells_affected = 0

    for db_type in databases:
        valid, missing_count, wells_affected = validate_coverage(db_type, args.verbose)
        all_valid = all_valid and valid
        total_missing += missing_count
        total_wells_affected += wells_affected
        print()

    # Summary
    print("=" * 80)
    if all_valid:
        print("✓ ALL DATABASES: CSV coverage complete")
        print(f"✓ Ready to implement CSV-driven categorization")
        sys.exit(0)
    else:
        print(f"⚠️  GAPS FOUND: {total_missing} missing patterns across databases")
        print(f"⚠️  Affects {total_wells_affected:,} wells total")
        print()
        print("ACTION REQUIRED:")
        print("  1. Run analyze_categories.py to regenerate TEMPLATE CSVs")
        print("  2. Run recategorize_categories.py to create new v1 CSVs")
        print("  3. Re-run this validation script")

        if args.strict:
            sys.exit(1)
        else:
            sys.exit(0)


if __name__ == '__main__':
    main()
