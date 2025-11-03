#!/usr/bin/env python3
"""
Create category mappings for Notts and Vira client databases
"""
import sqlite3
import csv
import sys


def extract_unique_combinations(db_path, db_name):
    """Extract all unique error_code/resolution/lims_status combinations from a database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print(f"\n=== Extracting combinations from {db_name} ===\n")

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
    sample_combos = [c for c in combinations if c['well_type'] == 'SAMPLE']
    control_combos = [c for c in combinations if c['well_type'] == 'CONTROL']
    print(f"  - {len(sample_combos)} sample combinations")
    print(f"  - {len(control_combos)} control combinations")

    return [dict(row) for row in combinations]


def create_template_csv(combinations, output_file, db_name):
    """Create CSV template for manual categorization"""

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write header with instructions
        writer.writerow([f'# Category Mapping Template for {db_name}'])
        writer.writerow(['#'])
        writer.writerow(['# CLASSIFICATION DISCREPANCIES (mutually exclusive):'])
        writer.writerow(['#   DISCREP_IN_ERROR - Discrepancy unresolved'])
        writer.writerow(['#   DISCREP_IGNORED - final_cls = machine_cls (AI suggestion ignored)'])
        writer.writerow(['#   DISCREP_RESULT_CHANGED - final_cls != machine_cls (result changed)'])
        writer.writerow(['#'])
        writer.writerow(['# SAMPLE SOP ERRORS:'])
        writer.writerow(['#   SOP_UNRESOLVED - Error unresolved'])
        writer.writerow(['#   SOP_IGNORED - Error resolved, result reported (DETECTED/NOT DETECTED)'])
        writer.writerow(['#   SOP_REPEATED - Error resolved, test repeated/extracted/excluded'])
        writer.writerow(['#'])
        writer.writerow(['# CONTROL ERRORS:'])
        writer.writerow(['#   CONTROL_UNRESOLVED - Control error unresolved (may affect samples)'])
        writer.writerow(['#   CONTROL_IGNORED - Control error resolved, deemed acceptable'])
        writer.writerow(['#   CONTROL_REPEATED - Control repeated/excluded (samples may be affected)'])
        writer.writerow(['#'])
        writer.writerow(['# VALID RESULTS:'])
        writer.writerow(['#   VALID_DETECTED, VALID_NOT_DETECTED, VALID_EXCLUDED, etc.'])
        writer.writerow(['#'])
        writer.writerow(['# Instructions:'])
        writer.writerow(['#   1. Fill in the CATEGORY column based on error/resolution pattern'])
        writer.writerow(['#   2. Use QST mapping as reference'])
        writer.writerow(['#   3. Map equivalent error codes (may have different names)'])
        writer.writerow(['#'])

        # Write data header
        writer.writerow([
            'WELL_TYPE',
            'ERROR_CODE',
            'ERROR_MESSAGE',
            'RESOLUTION_CODES',
            'WELL_LIMS_STATUS',
            'OCCURRENCE_COUNT',
            'CATEGORY',  # Empty - to be filled
            'QST_EQUIVALENT',  # For reference
            'NOTES',
        ])

        # Write data rows
        for combo in combinations:
            writer.writerow([
                combo['well_type'] or '',
                combo['error_code'] or '',
                (combo['error_message'] or '')[:150],
                combo['resolution_codes'] or '[]',
                combo['well_lims_status'] or '',
                combo['occurrence_count'],
                '',  # Empty CATEGORY
                '',  # Empty QST_EQUIVALENT
                '',  # Empty NOTES
            ])

    print(f"\nCSV template written to: {output_file}")
    print(f"Total rows: {len(combinations)}")
    print("\nTop 20 combinations by frequency:")
    for i, combo in enumerate(combinations[:20], 1):
        print(f"  {i:2}. {combo['well_type']:8} | {(combo['error_code'] or ''):30} | {(combo['resolution_codes'] or '[]'):20} | {(combo['well_lims_status'] or ''):15} | {combo['occurrence_count']:,}")


def main():
    databases = [
        ('input/notts.db', 'Notts', 'output_data/notts_category_mapping_TEMPLATE.csv'),
        ('input/vira.db', 'Vira', 'output_data/vira_category_mapping_TEMPLATE.csv'),
    ]

    for db_path, db_name, output_file in databases:
        try:
            combinations = extract_unique_combinations(db_path, db_name)
            create_template_csv(combinations, output_file, db_name)
        except Exception as e:
            print(f"\nError processing {db_name}: {e}")
            import traceback
            traceback.print_exc()

    print("\n\n=== Summary ===")
    print("Created template CSVs for Notts and Vira databases")
    print("\nNext steps:")
    print("1. Review output_data/notts_category_mapping_TEMPLATE.csv")
    print("2. Review output_data/vira_category_mapping_TEMPLATE.csv")
    print("3. Fill in CATEGORY column using QST mapping as reference")
    print("4. Map equivalent error codes between databases")


if __name__ == '__main__':
    main()
