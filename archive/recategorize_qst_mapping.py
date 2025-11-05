#!/usr/bin/env python3
"""
Re-categorize QST mapping with correct 3+3+3 category logic
"""
import csv
import json
import io


def parse_resolution_codes(resolution_codes):
    """Parse resolution_codes field into list"""
    if not resolution_codes or resolution_codes in ['[]', '', 'NULL']:
        return []
    try:
        if resolution_codes.startswith('['):
            res_list = json.loads(resolution_codes)
            return [r for r in res_list if r]  # Filter empty strings
        else:
            return [resolution_codes]
    except:
        return [resolution_codes] if resolution_codes else []


def has_meaningful_resolution(resolution_codes):
    """Check if resolution is meaningful (not empty, not just BLA)"""
    res_list = parse_resolution_codes(resolution_codes)
    if not res_list:
        return False
    # BLA alone is not a meaningful resolution
    if res_list == ['BLA'] or res_list == ['BLA|BLA']:
        return False
    return True


def categorize_classification_discrepancy(error_code, resolution_codes, well_lims_status, machine_cls=None, final_cls=None):
    """
    Classification Discrepancies (3 categories):
    1. In Error (not resolved) - Has discrepancy, no resolution
    2. Ignored - machine_cls != dxai_cls BUT final_cls = machine_cls (sided with machine)
    3. Result Changed - machine_cls != final_cls AND has resolution

    Note: For wells table, we don't have machine_cls/final_cls, so we infer from resolution
    """
    has_resolution = has_meaningful_resolution(resolution_codes)

    if not has_resolution:
        return 'DISCREP_IN_ERROR', 'Classification discrepancy - unresolved'

    # If we have machine_cls and final_cls data (from combined_outcomes)
    if machine_cls is not None and final_cls is not None:
        if machine_cls == final_cls:
            return 'DISCREP_IGNORED', 'Classification discrepancy - final result matches machine (AI suggestion ignored)'
        else:
            return 'DISCREP_RESULT_CHANGED', 'Classification discrepancy - final result differs from machine'

    # For wells without machine_cls/final_cls, infer from resolution + lims_status
    # If resolved with DETECTED/NOT DETECTED, likely ignored (result matched machine)
    lims = (well_lims_status or '').upper()
    if lims in ['DETECTED', 'NOT DETECTED']:
        return 'DISCREP_IGNORED', 'Classification discrepancy - result reported (likely sided with machine)'
    else:
        return 'DISCREP_RESULT_CHANGED', 'Classification discrepancy - result changed (likely test repeated/changed)'


def categorize_sample_sop_error(error_code, resolution_codes, well_lims_status):
    """
    Sample SOP Errors (3 categories):
    1. Unresolved - Has error code, no meaningful resolution
    2. Ignored - Has resolution (not BLA), AND lims_status = DETECTED/NOT DETECTED
    3. Repeated - Has resolution, lims_status = RXT/RPT/TNP/REXCT/REAMP/EXCLUDE/etc.
    """
    has_resolution = has_meaningful_resolution(resolution_codes)

    if not has_resolution:
        return 'SOP_UNRESOLVED', 'Sample error - unresolved'

    lims = (well_lims_status or '').upper()

    # Ignored: Result reported despite error
    if lims in ['DETECTED', 'NOT DETECTED']:
        return 'SOP_IGNORED', 'Sample error - resolved, result reported'

    # Repeated: Test action taken (repeat, extract, exclude, etc.)
    repeat_statuses = ['RXT', 'RPT', 'TNP', 'REXCT', 'REAMP', 'EXCLUDE', 'INCONCLUSIVE',
                      'RETEST', 'REPEAT', 'RE-EXTRACT']
    if lims in repeat_statuses or any(status in lims for status in repeat_statuses):
        return 'SOP_REPEATED', 'Sample error - test repeated/extracted/excluded'

    # Default to repeated if has resolution but unclear status
    return 'SOP_REPEATED', f'Sample error - resolved with status: {lims}'


def categorize_control_error(error_code, resolution_codes, well_lims_status):
    """
    Control Errors (3 categories):
    1. Unresolved - Has error code, no meaningful resolution
    2. Ignored - Has resolution, resolved successfully
    3. Repeated - Has resolution, control was repeated
    """
    has_resolution = has_meaningful_resolution(resolution_codes)

    if not has_resolution:
        return 'CONTROL_UNRESOLVED', 'Control error - unresolved (may affect samples)'

    lims = (well_lims_status or '').upper()

    # Similar logic to samples
    if lims in ['DETECTED', 'NOT DETECTED', 'NORMAL', 'Normal']:
        return 'CONTROL_IGNORED', 'Control error - resolved, deemed acceptable'

    repeat_statuses = ['RXT', 'RPT', 'TNP', 'REXCT', 'REAMP', 'EXCLUDE', 'INCONCLUSIVE',
                      'RETEST', 'REPEAT', 'RE-EXTRACT']
    if lims in repeat_statuses or any(status in lims for status in repeat_statuses):
        return 'CONTROL_REPEATED', 'Control error - control repeated/excluded (samples may be affected)'

    return 'CONTROL_REPEATED', f'Control error - resolved with status: {lims}'


def categorize_combination(well_type, error_code, resolution_codes, well_lims_status):
    """
    Main categorization logic

    Priority:
    1. Check lims_status for repeat/exclude actions (applies even without error_code)
    2. Check if classification discrepancy (CLSDISC or CTDISC in error_code)
    3. Check if control error
    4. Check if sample SOP error
    5. Valid result (no error + DETECTED/NOT DETECTED only)
    """

    lims = (well_lims_status or '').upper()

    # IMPORTANT: ONLY DETECTED/NOT DETECTED are valid results
    # All repeat statuses go to "test repeated" category even without error_code
    repeat_statuses = ['EXCLUDE', 'EXCLUDED', 'REAMP', 'REXCT', 'TNP', 'RXT', 'RPT',
                      'INCONCLUSIVE', 'RETEST', 'REPEAT', 'RE-EXTRACT']

    # If lims_status indicates action taken (repeat/exclude), classify accordingly
    if lims and any(status in lims for status in repeat_statuses):
        if not error_code or error_code == '':
            # No error code but has repeat status - still action taken
            if 'EXCLUDE' in lims:
                return 'SAMPLE_EXCLUDED_NO_ERROR', f'Excluded (no error code) - {lims}'
            else:
                return 'SAMPLE_REPEATED_NO_ERROR', f'Repeated/action taken (no error code) - {lims}'
        # If has error code, will be handled below in SOP error logic

    # No error = valid result (ONLY DETECTED or NOT DETECTED)
    if not error_code or error_code == '':
        if lims == 'DETECTED':
            return 'VALID_DETECTED', 'No error - DETECTED'
        elif lims == 'NOT DETECTED':
            return 'VALID_NOT_DETECTED', 'No error - NOT DETECTED'
        elif not lims:
            if well_type == 'CONTROL':
                return 'VALID_CONTROL', 'No error - Control well'
            else:
                return 'VALID_OTHER', 'No error - No lims_status'
        else:
            # Any other lims_status without error_code
            return 'VALID_OTHER', f'No error - {lims}'

    # Classification discrepancies (highest priority - mutually exclusive)
    if 'CLSDISC' in error_code or 'CTDISC' in error_code:
        return categorize_classification_discrepancy(error_code, resolution_codes, well_lims_status)

    # Control errors
    if well_type == 'CONTROL':
        return categorize_control_error(error_code, resolution_codes, well_lims_status)

    # Sample SOP errors
    if well_type == 'SAMPLE':
        return categorize_sample_sop_error(error_code, resolution_codes, well_lims_status)

    # Fallback
    return 'OTHER', 'Unclear categorization'


def recategorize_csv(input_file, output_file):
    """Re-categorize CSV with new 3+3+3 logic"""

    rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        # Skip comment lines
        lines = []
        for line in f:
            if not line.startswith('#'):
                lines.append(line)

    # Parse CSV
    csv_text = ''.join(lines)
    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        if row.get('WELL_TYPE'):
            category, note = categorize_combination(
                row['WELL_TYPE'],
                row['ERROR_CODE'],
                row['RESOLUTION_CODES'],
                row['WELL_LIMS_STATUS']
            )
            row['CATEGORY'] = category
            row['NOTES'] = note
            rows.append(row)

    # Write re-categorized CSV
    if rows:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            # Write header with new categories
            f.write('# QST Category Mapping - Updated with 3+3+3 logic\n')
            f.write('#\n')
            f.write('# CLASSIFICATION DISCREPANCIES (mutually exclusive):\n')
            f.write('#   DISCREP_IN_ERROR - Discrepancy unresolved\n')
            f.write('#   DISCREP_IGNORED - final_cls = machine_cls (AI suggestion ignored)\n')
            f.write('#   DISCREP_RESULT_CHANGED - final_cls != machine_cls (result changed)\n')
            f.write('#\n')
            f.write('# SAMPLE SOP ERRORS:\n')
            f.write('#   SOP_UNRESOLVED - Error unresolved\n')
            f.write('#   SOP_IGNORED - Error resolved, result reported (DETECTED/NOT DETECTED)\n')
            f.write('#   SOP_REPEATED - Error resolved, test repeated/extracted/excluded\n')
            f.write('#\n')
            f.write('# CONTROL ERRORS:\n')
            f.write('#   CONTROL_UNRESOLVED - Control error unresolved (may affect samples)\n')
            f.write('#   CONTROL_IGNORED - Control error resolved, deemed acceptable\n')
            f.write('#   CONTROL_REPEATED - Control repeated/excluded (samples may be affected)\n')
            f.write('#\n')
            f.write('# VALID RESULTS (ONLY DETECTED/NOT DETECTED):\n')
            f.write('#   VALID_DETECTED - No error, DETECTED\n')
            f.write('#   VALID_NOT_DETECTED - No error, NOT DETECTED\n')
            f.write('#   VALID_CONTROL - No error, control well\n')
            f.write('#   VALID_OTHER - No error, other status\n')
            f.write('#\n')
            f.write('# ACTIONS TAKEN (even without error code):\n')
            f.write('#   SAMPLE_REPEATED_NO_ERROR - Repeat status without error code (REAMP/REXCT/TNP/RXT/RPT)\n')
            f.write('#   SAMPLE_EXCLUDED_NO_ERROR - Excluded without error code\n')
            f.write('#\n')

            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    print(f"Re-categorized CSV written to: {output_file}")
    print(f"Total rows: {len(rows)}")

    # Category breakdown
    category_counts = {}
    for row in rows:
        cat = row['CATEGORY']
        category_counts[cat] = category_counts.get(cat, 0) + int(row['OCCURRENCE_COUNT'])

    print("\n=== Category Breakdown (by occurrence count) ===\n")

    # Group by type
    discrep = {k: v for k, v in category_counts.items() if k.startswith('DISCREP_')}
    sop = {k: v for k, v in category_counts.items() if k.startswith('SOP_')}
    control = {k: v for k, v in category_counts.items() if k.startswith('CONTROL_')}
    valid = {k: v for k, v in category_counts.items() if k.startswith('VALID_')}
    other = {k: v for k, v in category_counts.items() if not any(k.startswith(p) for p in ['DISCREP_', 'SOP_', 'CONTROL_', 'VALID_'])}

    print("CLASSIFICATION DISCREPANCIES:")
    for cat, count in sorted(discrep.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat:50} {count:,}")
    print(f"  TOTAL: {sum(discrep.values()):,}\n")

    print("SAMPLE SOP ERRORS:")
    for cat, count in sorted(sop.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat:50} {count:,}")
    print(f"  TOTAL: {sum(sop.values()):,}\n")

    print("CONTROL ERRORS:")
    for cat, count in sorted(control.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat:50} {count:,}")
    print(f"  TOTAL: {sum(control.values()):,}\n")

    print("VALID RESULTS:")
    for cat, count in sorted(valid.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat:50} {count:,}")
    print(f"  TOTAL: {sum(valid.values()):,}\n")

    if other:
        print("OTHER:")
        for cat, count in sorted(other.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cat:50} {count:,}")


def main():
    input_file = 'output_data/qst_category_mapping_TEMPLATE.csv'
    output_file = 'output_data/qst_category_mapping_v2.csv'

    recategorize_csv(input_file, output_file)


if __name__ == '__main__':
    main()
