#!/usr/bin/env python3
"""
Auto-categorize QST mapping based on existing report logic
"""
import csv
import json


def categorize_combination(well_type, error_code, resolution_codes, well_lims_status):
    """
    Categorize based on extract_report_with_curves.py logic:

    Sample logic (lines 273-282):
    - No resolution_codes ([] or NULL) → unresolved → AFFECTED
    - Has resolution_codes:
      - lims_status in ['DETECTED', 'NOT DETECTED'] → error_ignored → NOT AFFECTED
      - lims_status in ['INCONCLUSIVE', 'EXCLUDE', 'REXCT', 'REAMP', 'RXT', 'RPT', 'TNP'] → test_repeated → AFFECTED
      - Otherwise → test_repeated → AFFECTED

    Special cases:
    - CLSDISC_WELL = Classification discrepancy
    - CTDISC_WELL = CT discrepancy
    - Control errors affect samples on same run
    """

    # Parse resolution_codes
    try:
        if resolution_codes and resolution_codes not in ['[]', '']:
            res_list = json.loads(resolution_codes) if resolution_codes.startswith('[') else [resolution_codes]
            has_resolution = len(res_list) > 0 and res_list != ['']
        else:
            has_resolution = False
    except:
        has_resolution = False if resolution_codes in ['[]', '', 'NULL'] else True

    lims = (well_lims_status or '').upper()

    # No error code = valid result
    if not error_code or error_code == '':
        if lims == 'DETECTED':
            return 'VALID_RESULT_DETECTED', 'No error, reported as detected'
        elif lims == 'NOT DETECTED':
            return 'VALID_RESULT_NOT_DETECTED', 'No error, reported as not detected'
        else:
            return 'VALID_RESULT_OTHER', f'No error, lims_status: {lims}'

    # Classification discrepancies
    if 'CLSDISC' in error_code:
        if well_type == 'CONTROL':
            if has_resolution:
                if lims in ['DETECTED', 'NOT DETECTED']:
                    return 'CLASSIFICATION_DISCREPANCY_IGNORED', 'Classification discrepancy but result reported'
                else:
                    return 'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT', 'Classification discrepancy - test repeated/excluded'
            else:
                return 'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT', 'Classification discrepancy - unresolved'
        else:  # SAMPLE
            if has_resolution:
                if lims in ['DETECTED', 'NOT DETECTED']:
                    return 'CLASSIFICATION_DISCREPANCY_IGNORED', 'Classification discrepancy but result reported'
                else:
                    return 'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT', 'Classification discrepancy - test repeated/excluded'
            else:
                return 'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT', 'Classification discrepancy - unresolved'

    # CT discrepancies
    if 'CTDISC' in error_code:
        if has_resolution:
            if lims in ['DETECTED', 'NOT DETECTED']:
                return 'CLASSIFICATION_DISCREPANCY_IGNORED', 'CT discrepancy but result reported'
            else:
                return 'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT', 'CT discrepancy - test repeated/excluded'
        else:
            return 'CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT', 'CT discrepancy - unresolved'

    # Control errors
    if well_type == 'CONTROL':
        if has_resolution:
            if lims in ['DETECTED', 'NOT DETECTED']:
                return 'CONTROL_ERROR_IGNORED', 'Control error but marked as acceptable'
            else:
                return 'CONTROL_ERROR_AFFECTED_RESULT', 'Control error - control repeated/excluded, samples may be affected'
        else:
            return 'CONTROL_ERROR_AFFECTED_RESULT', 'Control error - unresolved, samples may be affected'

    # Sample SOP errors
    if well_type == 'SAMPLE':
        # Special case: EXCLUDE means well excluded
        if 'EXCLUDE' in lims or 'EXCLUDE' in (resolution_codes or ''):
            return 'SOP_ERROR_EXCLUDED', 'Sample excluded from analysis'

        if not has_resolution:
            return 'SOP_ERROR_AFFECTED_RESULT', 'Sample error - unresolved'
        else:
            if lims in ['DETECTED', 'NOT DETECTED']:
                return 'SOP_ERROR_IGNORED', 'Sample error but result reported'
            else:
                return 'SOP_ERROR_AFFECTED_RESULT', 'Sample error - test repeated/excluded'

    # Fallback
    return 'OTHER', 'Unclear categorization'


def auto_categorize_csv(input_file, output_file):
    """Read template CSV and auto-fill CATEGORY column"""

    rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        # Skip comment lines
        lines = []
        for line in f:
            if not line.startswith('#'):
                lines.append(line)

    # Parse CSV from non-comment lines
    import io
    csv_text = ''.join(lines)
    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        if row.get('WELL_TYPE'):  # Skip empty rows
            category, note = categorize_combination(
                row['WELL_TYPE'],
                row['ERROR_CODE'],
                row['RESOLUTION_CODES'],
                row['WELL_LIMS_STATUS']
            )
            row['CATEGORY'] = category
            row['NOTES'] = note
            rows.append(row)

    # Write categorized CSV
    if rows:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())

            # Write header with categories
            f.write('# Auto-categorized based on extract_report_with_curves.py logic\n')
            f.write('# Categories used:\n')
            f.write('#   VALID_RESULT_DETECTED - No error, reported as detected\n')
            f.write('#   VALID_RESULT_NOT_DETECTED - No error, reported as not detected\n')
            f.write('#   SOP_ERROR_AFFECTED_RESULT - Sample error caused repeat/exclusion\n')
            f.write('#   SOP_ERROR_IGNORED - Sample error but result still reported\n')
            f.write('#   SOP_ERROR_EXCLUDED - Sample explicitly excluded\n')
            f.write('#   CONTROL_ERROR_AFFECTED_RESULT - Control failure, samples may be affected\n')
            f.write('#   CONTROL_ERROR_IGNORED - Control error but deemed acceptable\n')
            f.write('#   CLASSIFICATION_DISCREPANCY_AFFECTED_RESULT - Classification mismatch caused repeat\n')
            f.write('#   CLASSIFICATION_DISCREPANCY_IGNORED - Classification mismatch but result reported\n')
            f.write('#   OTHER - Unclear categorization\n')
            f.write('#\n')

            writer.writeheader()
            writer.writerows(rows)

    print(f"Auto-categorized CSV written to: {output_file}")
    print(f"Total rows: {len(rows)}")

    # Print category breakdown
    category_counts = {}
    for row in rows:
        cat = row['CATEGORY']
        category_counts[cat] = category_counts.get(cat, 0) + int(row['OCCURRENCE_COUNT'])

    print("\nCategory Breakdown (by occurrence count):")
    for cat, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat:50} {count:,}")


def main():
    input_file = 'output_data/qst_category_mapping_TEMPLATE.csv'
    output_file = 'output_data/qst_category_mapping.csv'

    auto_categorize_csv(input_file, output_file)


if __name__ == '__main__':
    main()
