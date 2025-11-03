#!/usr/bin/env python3
"""
Convert QST v3 from CONTROL_ categories to unified SOP_ categories,
then apply v3_ac corrections
"""

import csv
import sys


def convert_control_to_sop_categories(input_file, output_file):
    """Convert CONTROL_ categories to SOP_ categories for consistency"""

    rows = []
    header_lines = []
    conversions = 0

    with open(input_file, 'r', encoding='utf-8') as f:
        # Read header comment lines
        line = f.readline()
        while line.startswith('#') or line.strip() == '':
            header_lines.append(line)
            line = f.readline()

        # line now contains the column headers
        reader = csv.DictReader([line] + f.readlines())

        for row in reader:
            old_category = row['CATEGORY']

            # Convert CONTROL_ categories to SOP_ categories
            if row['CATEGORY'] == 'CONTROL_UNRESOLVED':
                row['CATEGORY'] = 'SOP_UNRESOLVED'
                conversions += 1
                print(f"  {row['WELL_TYPE']:8} {row['ERROR_CODE']:30} → CONTROL_UNRESOLVED => SOP_UNRESOLVED")
            elif row['CATEGORY'] == 'CONTROL_IGNORED':
                row['CATEGORY'] = 'SOP_IGNORED'
                conversions += 1
                print(f"  {row['WELL_TYPE']:8} {row['RESOLUTION_CODES']:30} → CONTROL_IGNORED => SOP_IGNORED")
            elif row['CATEGORY'] == 'CONTROL_REPEATED':
                row['CATEGORY'] = 'SOP_REPEATED'
                conversions += 1
                print(f"  {row['WELL_TYPE']:8} {row['RESOLUTION_CODES']:30} → CONTROL_REPEATED => SOP_REPEATED")

            rows.append(row)

    # Update header to reflect SOP_ category usage
    updated_header_lines = []
    for line in header_lines:
        if 'CONTROL_UNRESOLVED' in line:
            line = line.replace('CONTROL_UNRESOLVED - Unresolved', 'Use same SOP_ categories (WELL_TYPE distinguishes)')
        elif 'CONTROL_IGNORED' in line:
            continue  # Skip this line
        elif 'CONTROL_REPEATED' in line:
            continue  # Skip this line
        updated_header_lines.append(line)

    # Write output
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        # Write updated header
        for line in updated_header_lines:
            f.write(line)

        # Write CSV data
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return conversions, len(rows)


def apply_ac_corrections(input_file, ac_file, output_file):
    """Apply AC corrections from v3_ac file to the converted file"""

    # Read AC corrections
    ac_corrections = {}
    with open(ac_file, 'r', encoding='utf-8') as f:
        # Skip header lines
        for line in f:
            if not line.startswith('#'):
                break

        reader = csv.DictReader([line] + f.readlines())
        for row in reader:
            if row.get('user notes', '').strip():
                # Create key from well_type, error_code, resolution_codes, lims_status
                key = (
                    row['WELL_TYPE'],
                    row['ERROR_CODE'],
                    row['RESOLUTION_CODES'],
                    row['WELL_LIMS_STATUS']
                )
                ac_corrections[key] = row['user notes'].strip()

    print(f"\nLoaded {len(ac_corrections)} AC corrections from {ac_file}")

    # Apply corrections
    rows = []
    header_lines = []
    corrections_applied = 0

    with open(input_file, 'r', encoding='utf-8') as f:
        # Read header comment lines
        line = f.readline()
        while line.startswith('#') or line.strip() == '':
            header_lines.append(line)
            line = f.readline()

        # line now contains the column headers
        reader = csv.DictReader([line] + f.readlines())

        for row in reader:
            key = (
                row['WELL_TYPE'],
                row['ERROR_CODE'],
                row['RESOLUTION_CODES'],
                row['WELL_LIMS_STATUS']
            )

            if key in ac_corrections:
                user_note = ac_corrections[key]
                old_category = row['CATEGORY']

                # Parse user note to determine new category
                # Based on patterns seen in v3_ac and v4_ac files
                if user_note in ['control_repeated', 'CONTROL_REPEATED']:
                    row['CATEGORY'] = 'SOP_REPEATED'
                elif 'depends on machine vs final cls' in user_note.lower():
                    row['CATEGORY'] = 'DISCREP_NEEDS_CLS_DATA'
                elif user_note == 'CONTROL_IGNORED':
                    row['CATEGORY'] = 'SOP_IGNORED'
                # Add more mappings as needed

                if old_category != row['CATEGORY']:
                    corrections_applied += 1
                    print(f"  {row['WELL_TYPE']:8} {row['ERROR_CODE']:30} {row['RESOLUTION_CODES']:20} → {old_category:25} => {row['CATEGORY']}")

            rows.append(row)

    # Write output
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        # Write header comment lines
        for line in header_lines:
            f.write(line)

        # Write CSV data
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return corrections_applied, len(rows)


if __name__ == '__main__':
    print("Step 1: Converting CONTROL_ categories to SOP_ categories")
    print("=" * 60)
    conversions, total = convert_control_to_sop_categories(
        'output_data/qst_category_mapping_v3.csv',
        '/tmp/qst_v3_sop_categories.csv'
    )
    print(f"\n✓ Converted {conversions} categories out of {total} total rows")
    print(f"✓ Written to: /tmp/qst_v3_sop_categories.csv")

    print("\n\nStep 2: Applying AC corrections from v3_ac")
    print("=" * 60)
    corrections, total = apply_ac_corrections(
        '/tmp/qst_v3_sop_categories.csv',
        'output_data/qst_category_mapping_v3_ac.csv',
        'output_data/qst_category_mapping_v1.csv'
    )
    print(f"\n✓ Applied {corrections} AC corrections")
    print(f"✓ Final output: output_data/qst_category_mapping_v1.csv")
