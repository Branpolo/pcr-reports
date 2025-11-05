#!/usr/bin/env python3
"""
Apply AC (corrected category) column to CATEGORY column in _ac CSVs
"""

import csv
import sys

def apply_ac_corrections(input_file, output_file):
    """Read CSV, copy AC to CATEGORY where AC is filled, write to output"""

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
            # If AC column exists and has a value, copy it to CATEGORY
            if 'AC' in row and row['AC'] and row['AC'].strip():
                old_category = row['CATEGORY']
                row['CATEGORY'] = row['AC'].strip()
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
    if len(sys.argv) != 3:
        print("Usage: python apply_ac_corrections.py input_ac.csv output.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f"Processing: {input_file}")
    corrections, total = apply_ac_corrections(input_file, output_file)

    print(f"\n✓ Applied {corrections} corrections out of {total} total rows")
    print(f"✓ Written to: {output_file}")
