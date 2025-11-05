#!/usr/bin/env python3
"""
Database-agnostic script to analyze result categories and create mapping CSV template.
Supports QST, Notts, and Vira databases with different control detection logic.
"""
import sqlite3
import csv
import argparse
from collections import defaultdict


# Database-specific configurations
DB_CONFIGS = {
    'qst': {
        'name': 'QST',
        'control_where_clause': """(w.role_alias LIKE '%PC' OR w.role_alias LIKE '%NC'
                                    OR w.role_alias LIKE 'PC%' OR w.role_alias LIKE 'NC%')""",
        'sample_where_clause': """(w.role_alias IS NULL OR
                                   (w.role_alias NOT LIKE '%PC' AND w.role_alias NOT LIKE '%NC'
                                    AND w.role_alias NOT LIKE 'PC%' AND w.role_alias NOT LIKE 'NC%'))""",
        'lims_variants': ['DETECTED', 'NOT DETECTED', 'MPX & OPX DETECTED', 'MPX & OPX NOT DETECTED',
                         'HSV_DETECTED', 'HSV_NOT_DETECTED', 'HSV1_DETECTED', 'HSV2_DETECTED',
                         'ADENOVIRUS_DETECTED', 'BKV_DETECTED', 'VZV_DETECTED'],
        'default_db': '/home/azureuser/code/wssvc-flow/input_data/quest_prod_aug2025.db',
    },
    'notts': {
        'name': 'Notts',
        'control_where_clause': """(w.role_alias LIKE '%NEG%' OR w.role_alias LIKE '%Neg%' OR w.role_alias LIKE '%neg%'
                                    OR w.role_alias LIKE '%NTC%' OR w.role_alias LIKE '%QS%' OR w.role_alias LIKE '%PC%'
                                    OR w.role_alias = 'NIBSC')""",
        'sample_where_clause': """(w.role_alias = 'Patient' OR w.role_alias IS NULL)""",
        'lims_variants': ['DETECTED', 'NOT DETECTED', '<1500',
                         'HSV_1_DETECTED', 'HSV_2_DETECTED', 'HSV_1_2_DETECTED', 'HSV_1_VZV_DETECTED',
                         'BKV_DETECTED', 'ADENOVIRUS_DETECTED', 'VZV_DETECTED',
                         'Detected <500IU/ml', 'Detected_btw_loq_lod'],
        'default_db': '/home/azureuser/code/wssvc-flow-codex/input/notts.db',
    },
    'vira': {
        'name': 'Vira',
        'control_where_clause': """w.role_alias IN ('CC1', 'CC2', 'POS', 'NEC', 'NTC', 'S#')""",
        'sample_where_clause': """w.role_alias NOT IN ('CC1', 'CC2', 'POS', 'NEC', 'NTC', 'S#')""",
        'lims_variants': ['DETECTED', 'NOT DETECTED',
                         'DETECTED_QUANT', 'DETECTED_LOQ', 'DETECTED_HIQ'],
        'default_db': '/home/azureuser/code/wssvc-flow-codex/input/vira.db',
    },
}


def discover_control_patterns(db_path):
    """Discover control detection patterns by examining role_alias values"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("\n=== Discovering Control Patterns ===")

    # Get all unique role_alias values
    cursor = conn.execute("""
        SELECT DISTINCT w.role_alias, COUNT(*) as cnt
        FROM wells w
        GROUP BY w.role_alias
        ORDER BY cnt DESC
        LIMIT 50
    """)

    roles = cursor.fetchall()
    print(f"\nFound {len(roles)} unique role_alias values (showing top 50):")
    for row in roles:
        role = row['role_alias'] or 'NULL'
        print(f"  {role:30} | {row['cnt']:,} wells")

    conn.close()

    print("\n⚠️  Manual review required:")
    print("  - Review the role_alias values above")
    print("  - Identify which values represent controls (PC, NC, etc.)")
    print("  - Update DB_CONFIGS in this script with appropriate WHERE clause")

    return roles


def discover_lims_variants(db_path):
    """Discover all LIMS status variants in the database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("\n=== Discovering LIMS Status Variants ===")

    cursor = conn.execute("""
        SELECT DISTINCT w.lims_status, COUNT(*) as cnt
        FROM wells w
        WHERE w.lims_status IS NOT NULL
        GROUP BY w.lims_status
        ORDER BY cnt DESC
    """)

    statuses = cursor.fetchall()
    print(f"\nFound {len(statuses)} unique lims_status values:")
    for row in statuses:
        print(f"  {row['lims_status']:40} | {row['cnt']:,} wells")

    conn.close()

    print("\n⚠️  Manual review required:")
    print("  - Review the lims_status values above")
    print("  - Identify which represent valid results (DETECTED/NOT DETECTED variants)")
    print("  - Update DB_CONFIGS in this script with appropriate list")

    return statuses


def analyze_result_categories(db_path, db_config):
    """Analyze actual data patterns in database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    db_name = db_config['name']
    print(f"\nAnalyzing result categories in {db_name} database...\n")

    # Check if control/sample detection is configured
    if not db_config['control_where_clause']:
        print("⚠️  Control detection not configured!")
        print("   Run with --discover-controls first to identify control patterns")
        conn.close()
        return None

    if not db_config['lims_variants']:
        print("⚠️  LIMS variants not configured!")
        print("   Run with --discover-lims first to identify LIMS status variants")
        conn.close()
        return None

    # Build LIMS IN clause
    lims_list = "'" + "', '".join(db_config['lims_variants']) + "'"

    # 1. Get valid results (no errors)
    print("=== Valid Results (No Errors) ===")
    query = f"""
        SELECT DISTINCT w.lims_status, COUNT(*) as cnt
        FROM wells w
        WHERE w.error_code_id IS NULL
          AND {db_config['sample_where_clause']}
          AND w.lims_status IN ({lims_list})
        GROUP BY w.lims_status
        ORDER BY cnt DESC
    """
    cursor = conn.execute(query)
    valid_results = cursor.fetchall()
    for row in valid_results:
        print(f"  {row['lims_status']}: {row['cnt']:,} wells")

    # 2. Get sample errors with resolution patterns
    print("\n=== Sample SOP Errors ===")
    query = f"""
        SELECT
            ec.error_code,
            ec.error_message,
            w.lims_status,
            w.resolution_codes,
            COUNT(*) as cnt
        FROM wells w
        JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE {db_config['sample_where_clause']}
        GROUP BY ec.error_code, w.lims_status, w.resolution_codes
        ORDER BY cnt DESC
        LIMIT 30
    """
    cursor = conn.execute(query)
    sample_errors = cursor.fetchall()
    print("  Top 30 error_code + lims_status + resolution combinations:")
    for row in sample_errors:
        lims = row['lims_status'] or 'NULL'
        res_codes = row['resolution_codes'] or '[]'
        if len(res_codes) > 40:
            res_codes = res_codes[:37] + '...'
        print(f"  {row['error_code']:30} | {lims:20} | {res_codes:40} | {row['cnt']:,}")

    # 3. Get control errors
    print("\n=== Control Errors ===")
    query = f"""
        SELECT
            ec.error_code,
            ec.error_message,
            w.lims_status,
            w.resolution_codes,
            COUNT(*) as cnt
        FROM wells w
        JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE {db_config['control_where_clause']}
        GROUP BY ec.error_code, w.lims_status, w.resolution_codes
        ORDER BY cnt DESC
        LIMIT 30
    """
    cursor = conn.execute(query)
    control_errors = cursor.fetchall()
    print("  Top 30 control error_code + lims_status + resolution combinations:")
    for row in control_errors:
        lims = row['lims_status'] or 'NULL'
        res_codes = row['resolution_codes'] or '[]'
        if len(res_codes) > 40:
            res_codes = res_codes[:37] + '...'
        print(f"  {row['error_code']:30} | {lims:20} | {res_codes:40} | {row['cnt']:,}")

    # 4. Check classification discrepancies
    print("\n=== Classification Discrepancies ===")
    try:
        cursor = conn.execute("""
            SELECT
                co.machine_cls,
                co.final_cls,
                co.resolution_codes,
                COUNT(*) as cnt
            FROM combined_outcomes co
            WHERE co.machine_cls != co.final_cls
              AND co.machine_cls IS NOT NULL
              AND co.final_cls IS NOT NULL
            GROUP BY co.machine_cls, co.final_cls, co.resolution_codes
            ORDER BY cnt DESC
            LIMIT 20
        """)
        discrepancies = cursor.fetchall()
        print("  Top 20 machine_cls -> final_cls + resolution patterns:")
        for row in discrepancies:
            machine = 'POS' if row['machine_cls'] == 1 else 'NEG'
            final = 'POS' if row['final_cls'] == 1 else 'NEG'
            res = row['resolution_codes'] or 'NULL'
            print(f"  {machine} -> {final:3} | {res:30} | {row['cnt']:,}")
    except sqlite3.OperationalError:
        print("  (combined_outcomes table not available - classification discrepancies tracked differently)")

    conn.close()

    return {
        'valid_results': valid_results,
        'sample_errors': sample_errors,
        'control_errors': control_errors,
    }


def extract_unique_combinations(db_path, db_config):
    """Extract all unique error_code/resolution/lims_status combinations"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print(f"\n\n=== Extracting All Unique Combinations ===\n")

    # Check if control/sample detection is configured
    if not db_config['control_where_clause']:
        print("⚠️  Control detection not configured - cannot extract combinations")
        conn.close()
        return None

    # Get all unique combinations from wells
    query = f"""
        SELECT
            ec.error_code,
            ec.error_message,
            w.resolution_codes,
            w.lims_status as well_lims_status,
            CASE
                WHEN {db_config['control_where_clause']}
                THEN 'CONTROL'
                ELSE 'SAMPLE'
            END as well_type,
            COUNT(*) as occurrence_count
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        GROUP BY
            ec.error_code, ec.error_message,
            w.resolution_codes,
            w.lims_status, well_type
        ORDER BY occurrence_count DESC
    """

    cursor = conn.execute(query)
    combinations = cursor.fetchall()
    conn.close()

    print(f"Found {len(combinations)} unique combinations")
    sample_combos = [c for c in combinations if c['well_type'] == 'SAMPLE']
    control_combos = [c for c in combinations if c['well_type'] == 'CONTROL']
    print(f"  - {len(sample_combos)} sample combinations")
    print(f"  - {len(control_combos)} control combinations")

    return combinations


def create_category_mapping_csv(combinations, output_file, db_name):
    """Create CSV with combinations and empty category column for manual filling"""

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write header with categories documentation
        writer.writerow([f'# Category Mapping Template for {db_name}'])
        writer.writerow(['#'])
        writer.writerow(['# CLASSIFICATION DISCREPANCIES (mutually exclusive):'])
        writer.writerow(['#   DISCREP_IN_ERROR - Discrepancy unresolved'])
        writer.writerow(['#   DISCREP_IGNORED - final_cls = machine_cls (AI suggestion ignored)'])
        writer.writerow(['#   DISCREP_RESULT_CHANGED - final_cls != machine_cls (result changed)'])
        writer.writerow(['#   DISCREP_NEEDS_CLS_DATA - BLA resolution, needs machine_cls/final_cls comparison'])
        writer.writerow(['#'])
        writer.writerow(['# SAMPLE/CONTROL SOP ERRORS:'])
        writer.writerow(['#   SOP_UNRESOLVED - Error unresolved'])
        writer.writerow(['#   SOP_IGNORED - Error resolved, result reported (DETECTED/NOT DETECTED)'])
        writer.writerow(['#   SOP_REPEATED - Error resolved, test repeated/extracted/excluded'])
        writer.writerow(['#'])
        writer.writerow(['# VALID RESULTS:'])
        writer.writerow(['#   VALID_DETECTED - No error, DETECTED result'])
        writer.writerow(['#   VALID_NOT_DETECTED - No error, NOT DETECTED result'])
        writer.writerow(['#   VALID_CONTROL - No error, control passed'])
        writer.writerow(['#'])
        writer.writerow(['# SPECIAL CATEGORIES:'])
        writer.writerow(['#   CONTROL_AFFECTED_SAMPLE - INHERITED_*_FAILURE (for Controls appendix)'])
        writer.writerow(['#   IGNORE_WELL - Wells to ignore (MIX_MISSING, UNKNOWN_MIX, etc.)'])
        writer.writerow(['#   NEEDS_REVIEW - Unclear, needs manual review'])
        writer.writerow(['#'])

        # Write data header
        writer.writerow([
            'WELL_TYPE',
            'ERROR_CODE',
            'ERROR_MESSAGE',
            'RESOLUTION_CODES',
            'WELL_LIMS_STATUS',
            'OCCURRENCE_COUNT',
            'CATEGORY',  # Empty column to fill
            'NOTES',  # For any additional notes
        ])

        # Write data rows
        for combo in combinations:
            writer.writerow([
                combo['well_type'] or '',
                combo['error_code'] or '',
                (combo['error_message'] or '')[:150],  # Truncate long messages
                combo['resolution_codes'] or '[]',
                combo['well_lims_status'] or '',
                combo['occurrence_count'],
                '',  # Empty CATEGORY - to be filled
                '',  # Empty NOTES
            ])

    print(f"\nCSV written to: {output_file}")
    print(f"Total rows: {len(combinations)}")
    print("\nNext steps:")
    print("1. Open the CSV and review the unique combinations")
    print("2. Use recategorize_*_v1.py to automatically fill CATEGORY column")
    print("3. Review any NEEDS_REVIEW categories manually")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze database to extract unique error/resolution/LIMS combinations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover control patterns in Notts database
  python analyze_categories.py --db-type notts --discover-controls

  # Discover LIMS variants in Vira database
  python analyze_categories.py --db-type vira --discover-lims

  # Generate QST template CSV
  python analyze_categories.py --db-type qst --output output_data/qst_category_mapping_TEMPLATE.csv

  # Generate Notts template CSV (after configuring control detection)
  python analyze_categories.py --db-type notts --output output_data/notts_category_mapping_TEMPLATE.csv
        """
    )

    parser.add_argument('--db-type', required=True, choices=['qst', 'notts', 'vira'],
                       help='Database type (qst, notts, or vira)')
    parser.add_argument('--db', help='Path to database file (overrides default)')
    parser.add_argument('--output', help='Output CSV file path')
    parser.add_argument('--discover-controls', action='store_true',
                       help='Discover control detection patterns (role_alias values)')
    parser.add_argument('--discover-lims', action='store_true',
                       help='Discover LIMS status variants')

    args = parser.parse_args()

    # Get database configuration
    db_config = DB_CONFIGS[args.db_type]
    db_path = args.db or db_config['default_db']

    print(f"Database: {db_path}")
    print(f"Type: {db_config['name']}")

    # Discovery modes
    if args.discover_controls:
        discover_control_patterns(db_path)
        return

    if args.discover_lims:
        discover_lims_variants(db_path)
        return

    # Generate template CSV
    if not args.output:
        args.output = f"output_data/{args.db_type}_category_mapping_TEMPLATE.csv"

    # Analyze patterns
    analysis = analyze_result_categories(db_path, db_config)
    if analysis is None:
        print("\n❌ Analysis failed - missing configuration")
        print("   Run with --discover-controls and --discover-lims first")
        return

    # Extract all unique combinations
    combinations = extract_unique_combinations(db_path, db_config)
    if combinations is None:
        print("\n❌ Extraction failed - missing configuration")
        return

    # Create CSV template
    create_category_mapping_csv(combinations, args.output, db_config['name'])


if __name__ == '__main__':
    main()
