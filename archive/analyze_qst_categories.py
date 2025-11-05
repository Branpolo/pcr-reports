#!/usr/bin/env python3
"""
Analyze QST production database to identify result categories and create mapping CSV
"""
import sqlite3
import csv
from collections import defaultdict


def analyze_result_categories(db_path):
    """Analyze actual data patterns in QST database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("Analyzing result categories in QST database...\n")

    # 1. Get valid results (no errors)
    print("=== Valid Results (No Errors) ===")
    cursor = conn.execute("""
        SELECT DISTINCT w.lims_status, COUNT(*) as cnt
        FROM wells w
        WHERE w.error_code_id IS NULL
          AND (w.role_alias IS NULL OR w.role_alias NOT LIKE '%PC' AND w.role_alias NOT LIKE '%NC'
               AND w.role_alias NOT LIKE 'PC%' AND w.role_alias NOT LIKE 'NC%')
          AND w.lims_status IN ('DETECTED', 'NOT DETECTED')
        GROUP BY w.lims_status
        ORDER BY cnt DESC
    """)
    valid_results = cursor.fetchall()
    for row in valid_results:
        print(f"  {row['lims_status']}: {row['cnt']:,} wells")

    # 2. Get sample errors with resolution patterns
    print("\n=== Sample SOP Errors ===")
    cursor = conn.execute("""
        SELECT
            ec.error_code,
            ec.error_message,
            w.lims_status,
            w.resolution_codes,
            COUNT(*) as cnt
        FROM wells w
        JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE (w.role_alias IS NULL OR w.role_alias NOT LIKE '%PC' AND w.role_alias NOT LIKE '%NC'
               AND w.role_alias NOT LIKE 'PC%' AND w.role_alias NOT LIKE 'NC%')
        GROUP BY ec.error_code, w.lims_status, w.resolution_codes
        ORDER BY cnt DESC
        LIMIT 30
    """)
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
    cursor = conn.execute("""
        SELECT
            ec.error_code,
            ec.error_message,
            w.lims_status,
            w.resolution_codes,
            COUNT(*) as cnt
        FROM wells w
        JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE (w.role_alias LIKE '%PC' OR w.role_alias LIKE '%NC'
               OR w.role_alias LIKE 'PC%' OR w.role_alias LIKE 'NC%')
        GROUP BY ec.error_code, w.lims_status, w.resolution_codes
        ORDER BY cnt DESC
        LIMIT 30
    """)
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


def extract_unique_combinations(db_path):
    """Extract all unique error_code/resolution/lims_status combinations"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("\n\n=== Extracting All Unique Combinations ===\n")

    # Get all unique combinations from wells
    cursor = conn.execute("""
        SELECT
            ec.error_code,
            ec.error_message,
            w.resolution_codes,
            w.lims_status as well_lims_status,
            CASE
                WHEN w.role_alias LIKE '%PC' OR w.role_alias LIKE '%NC'
                     OR w.role_alias LIKE 'PC%' OR w.role_alias LIKE 'NC%'
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
    """)

    combinations = cursor.fetchall()
    conn.close()

    print(f"Found {len(combinations)} unique combinations")
    print(f"Samples breakdown:")
    sample_combos = [c for c in combinations if c['well_type'] == 'SAMPLE']
    control_combos = [c for c in combinations if c['well_type'] == 'CONTROL']
    print(f"  - {len(sample_combos)} sample combinations")
    print(f"  - {len(control_combos)} control combinations")

    return combinations


def create_category_mapping_csv(combinations, output_file):
    """Create CSV with combinations and empty category column for manual filling"""

    # Define result categories based on report structure
    categories = [
        'VALID_RESULT_REPORTED',
        'SOP_ERROR_AFFECTED_RESULT',
        'SOP_ERROR_IGNORED',
        'CONTROL_ERROR_AFFECTED_RESULT',
        'CONTROL_ERROR_IGNORED',
        'CONTROL_ERROR_SAMPLES_AFFECTED',
        'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT',
        'CLASSIFICATION_DISCREPANCY_IGNORED',
        'EXCLUDE',  # Wells excluded from analysis
        'OTHER',
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write header with categories list
        writer.writerow(['# Result Categories:'])
        for cat in categories:
            writer.writerow([f'#   {cat}'])
        writer.writerow(['#'])
        writer.writerow(['# Instructions: Fill the CATEGORY column based on the error/resolution pattern'])
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
    print("1. Open the CSV and fill in the CATEGORY column")
    print("2. Use the categories listed at the top of the file")
    print("3. Look at OCCURRENCE_COUNT to prioritize common patterns")


def main():
    db_path = '/home/azureuser/code/wssvc-flow/input_data/quest_prod_aug2025.db'
    output_file = 'output_data/qst_category_mapping_TEMPLATE.csv'

    # Analyze patterns
    analyze_result_categories(db_path)

    # Extract all unique combinations
    combinations = extract_unique_combinations(db_path)

    # Create CSV template
    create_category_mapping_csv(combinations, output_file)


if __name__ == '__main__':
    main()
