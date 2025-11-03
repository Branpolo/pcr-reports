#!/usr/bin/env python3
"""
Re-categorize QST mapping based on user feedback patterns
"""
import csv
import json
import io


# Global rules derived from user feedback
IGNORE_ERROR_CODES = [
    'MIX_MISSING',
    'UNKNOWN_MIX',
    'UNKNOWN_ROLE',
]

CONTROL_AFFECTED_ERROR_CODES = [
    'INHERITED_EXTRACTION_FAILURE',
    'INHERITED_CONTROL_FAILURE',
]

# Map variant LIMS statuses to standard categories
LIMS_MAPPING = {
    # Detected variants
    'MPX & OPX DETECTED': 'DETECTED',
    'MPX DETECTED': 'DETECTED',
    'HSV_DETECTED': 'DETECTED',
    'HSV1_DETECTED': 'DETECTED',
    'HSV2_DETECTED': 'DETECTED',
    'HSV_1_DETECTED': 'DETECTED',
    'HSV_2_DETECTED': 'DETECTED',
    'HSV_1_2_DETECTED': 'DETECTED',
    'ADENOVIRUS_DETECTED': 'DETECTED',
    'BKV_DETECTED': 'DETECTED',
    'VZV_DETECTED': 'DETECTED',

    # Not detected variants
    'MPX & OPX NOT DETECTED': 'NOT DETECTED',
    'MPX NOT DETECTED': 'NOT DETECTED',
    'HSV_NOT_DETECTED': 'NOT DETECTED',
}


def normalize_lims_status(lims_status):
    """Normalize variant LIMS statuses to standard DETECTED/NOT DETECTED"""
    if not lims_status:
        return lims_status
    return LIMS_MAPPING.get(lims_status, lims_status)


def parse_resolution_codes(resolution_codes):
    """Parse resolution_codes field into list"""
    if not resolution_codes or resolution_codes in ['[]', '', 'NULL']:
        return []
    try:
        if resolution_codes.startswith('['):
            res_list = json.loads(resolution_codes)
            return [r for r in res_list if r]
        else:
            return [resolution_codes]
    except:
        return [resolution_codes] if resolution_codes else []


def has_bla(resolution_codes):
    """Check if resolution contains BLA"""
    res_list = parse_resolution_codes(resolution_codes)
    res_str = '|'.join(res_list).upper()
    return 'BLA' in res_str


def has_wdcls_wdct(resolution_codes):
    """Check if resolution contains WDCLS or WDCT (classification error indicators)"""
    res_list = parse_resolution_codes(resolution_codes)
    res_str = '|'.join(res_list).upper()
    return 'WDCLS' in res_str or 'WDCT' in res_str


def has_skip(resolution_codes):
    """Check if resolution contains SKIP"""
    res_list = parse_resolution_codes(resolution_codes)
    res_str = '|'.join(res_list).upper()
    return 'SKIP' in res_str


def has_meaningful_resolution(resolution_codes):
    """Check if resolution is meaningful (not empty, not just BLA alone)"""
    res_list = parse_resolution_codes(resolution_codes)
    if not res_list:
        return False

    # Check if only BLA (no other resolutions)
    non_bla = [r for r in res_list if r.upper() not in ['BLA', '']]
    if not non_bla:
        return False  # Only BLA means no meaningful resolution

    return True


def categorize_combination(well_type, error_code, resolution_codes, well_lims_status):
    """
    Categorization logic based on user feedback patterns
    """

    lims = (well_lims_status or '').upper()
    lims_normalized = normalize_lims_status(lims)

    # Rule 1: IGNORE certain error codes
    if error_code in IGNORE_ERROR_CODES:
        return 'IGNORE_WELL', f'{error_code} - Ignore such wells'

    # Rule 2: INHERITED_EXTRACTION/CONTROL_FAILURE = Control affected sample
    if error_code in CONTROL_AFFECTED_ERROR_CODES:
        return 'CONTROL_AFFECTED_SAMPLE', f'{error_code} - Affected sample for control (Controls appendix only)'

    # Rule 3: Check for SKIP,WG patterns (Westgard control rules)
    if (not error_code or error_code == '') and not lims and has_skip(resolution_codes):
        res_str = '|'.join(parse_resolution_codes(resolution_codes)).upper()
        if well_type == 'CONTROL' and 'WG' in res_str:
            return 'SOP_IGNORED', f'SKIP + Westgard pattern - Control error ignored'

    # Rule 4: No error code + no lims_status = IGNORE or VALID_CONTROL
    if (not error_code or error_code == '') and not lims:
        if well_type == 'CONTROL' and not parse_resolution_codes(resolution_codes):
            # Passed controls: no error, no lims, no resolution
            return 'VALID_CONTROL', 'Passed control - No error, no lims, no resolution'
        return 'IGNORE_WELL', 'No error, no lims_status - Ignore such wells'

    # Rule 5: No error code + standalone repeat status (no resolution) = IGNORE
    repeat_statuses = ['REAMP', 'REXCT', 'RXT', 'RPT']
    if (not error_code or error_code == '') and lims in repeat_statuses:
        res_list = parse_resolution_codes(resolution_codes)
        if not res_list or res_list == ['']:
            return 'IGNORE_WELL', f'No error code, standalone {lims} - Ignore such wells'

    # Rule 6: EXCLUDE status = always SOP_REPEATED (resolved as error)
    if 'EXCLUDE' in lims:
        if error_code and error_code != '':
            return 'SOP_REPEATED', f'{error_code} - Excluded wells are repeated'
        else:
            return 'SOP_REPEATED', 'Excluded (no error code) - Counts as resolved error'

    # Rule 7: INCONCLUSIVE status = always SOP_REPEATED (resolved as error)
    if 'INCONCLUSIVE' in lims:
        if error_code and error_code != '':
            return 'SOP_REPEATED', f'{error_code} - Inconclusive wells are repeated'
        else:
            return 'SOP_REPEATED', 'Inconclusive (no error code) - Counts as resolved error'

    # Rule 8: TNP status = always SOP_REPEATED (resolved as error)
    if lims == 'TNP':
        if error_code and error_code != '':
            return 'SOP_REPEATED', f'{error_code} - TNP = resolved as error'
        else:
            return 'SOP_REPEATED', 'TNP (no error code) - Resolved as error'

    # Rule 9: BLA in resolution → Classification discrepancy (handled by discrepancy extractor)
    # BLA patterns are pulled by discrepancy extractor first, sample report excludes them
    if has_bla(resolution_codes):
        if error_code and ('CLSDISC' in error_code or 'CTDISC' in error_code):
            # Explicit classification discrepancy error code
            return 'DISCREP_IN_ERROR', f'{error_code} - Classification discrepancy'
        elif error_code and error_code != '':
            # Error with BLA - treat as unresolved (BLA alone is not meaningful)
            return 'SOP_UNRESOLVED', f'{error_code} - BLA alone not meaningful resolution'
        else:
            # No error + BLA = classification discrepancy (needs machine_cls/final_cls to determine outcome)
            if lims_normalized in ['DETECTED', 'NOT DETECTED']:
                # Result reported - could be ignored or result changed, need cls data
                return 'DISCREP_NEEDS_CLS_DATA', f'BLA + {lims_normalized} - Classification discrepancy (need machine_cls/final_cls)'
            elif lims in ['REAMP', 'REXCT', 'RXT', 'RPT', 'TNP', 'EXCLUDE', 'INCONCLUSIVE']:
                # Test repeated due to classification issue
                return 'DISCREP_IN_ERROR', f'BLA + {lims} - Classification discrepancy, test repeated'
            elif not lims:
                # BLA but no lims (typically controls) = ignore
                return 'IGNORE_WELL', 'BLA resolution without error/lims - Ignore'
            else:
                return 'NEEDS_REVIEW', 'BLA without error code - needs review'

    # Rule 10: WDCLS/WDCT in resolution → Classification error indicators
    # NOTE: WDCLS/WDCT with REAMP/REXCT = test repeated due to classification error
    # This is still an error (DISCREP_IN_ERROR), not a result change
    if has_wdcls_wdct(resolution_codes):
        if lims in ['REAMP', 'REXCT', 'RXT', 'RPT', 'TNP']:
            if error_code and error_code != '':
                return 'DISCREP_IN_ERROR', f'{error_code} - WDCLS/WDCT + {lims} = classification error, test repeated'
            else:
                return 'DISCREP_IN_ERROR', f'WDCLS/WDCT + {lims} = classification error, test repeated'
        elif lims_normalized in ['DETECTED', 'NOT DETECTED']:
            return 'DISCREP_IGNORED', f'WDCLS/WDCT + {lims_normalized} = classification error ignored'
        elif not lims:
            # WDCLS/WDCT without lims = ignore
            return 'IGNORE_WELL', 'WDCLS/WDCT pattern without lims - Ignore'

    # Rule 11: Classification discrepancies (error code)
    # NOTE: DISCREP_RESULT_CHANGED requires machine_cls vs final_cls comparison
    # Without that data, all discrepancies are treated as DISCREP_IN_ERROR
    if error_code and ('CLSDISC' in error_code or 'CTDISC' in error_code):
        # All classification discrepancies are errors unless explicitly resolved
        # (Resolution by repeat/retest still counts as error)
        return 'DISCREP_IN_ERROR', f'{error_code} - Classification discrepancy (requires machine_cls/final_cls to determine if result changed)'

    # Rule 12: SOP errors (all wells with error codes)
    if error_code and error_code != '':
        has_resolution = has_meaningful_resolution(resolution_codes)

        if not has_resolution:
            return 'SOP_UNRESOLVED', f'{error_code} - Unresolved'
        else:
            # Has resolution
            if lims_normalized in ['DETECTED', 'NOT DETECTED']:
                return 'SOP_IGNORED', f'{error_code} - Resolved, result reported'
            elif lims in ['REAMP', 'REXCT', 'RXT', 'RPT', 'TNP', 'EXCLUDE', 'INCONCLUSIVE']:
                return 'SOP_REPEATED', f'{error_code} - Test repeated/excluded'
            elif has_skip(resolution_codes):
                return 'SOP_IGNORED', f'{error_code} - SKIP resolution, error ignored'
            else:
                return 'SOP_REPEATED', f'{error_code} - Resolved with action'

    # Rule 13: No error code - check for implicit errors via resolution codes
    # Resolution codes without error_code indicate error was suppressed on reanalysis
    if not error_code or error_code == '':
        res_list = parse_resolution_codes(resolution_codes)
        # Exclude BLA (already handled in Rule 9) and empty strings
        non_bla_res = [r for r in res_list if r and r.upper() != 'BLA']

        if non_bla_res:
            # Has resolution code → implicit error was suppressed on reanalysis
            res_str = '|'.join(non_bla_res)
            if lims_normalized == 'DETECTED':
                return 'SOP_IGNORED', f'{res_str} + DETECTED - Error suppressed, result reported'
            elif lims_normalized == 'NOT DETECTED':
                return 'SOP_IGNORED', f'{res_str} + NOT DETECTED - Error suppressed, result reported'
            elif lims in ['REAMP', 'REXCT', 'RXT', 'RPT', 'TNP', 'EXCLUDE', 'INCONCLUSIVE']:
                return 'SOP_REPEATED', f'{res_str} + {lims} - Test repeated due to suppressed error'
            elif not lims:
                # Resolution without lims (typically controls)
                return 'SOP_REPEATED', f'{res_str} without lims - Test repeated due to suppressed error'
            else:
                # Other LIMS status with resolution
                return 'SOP_REPEATED', f'{res_str} + {lims} - Test repeated due to suppressed error'

        # No resolution codes → truly valid results (no error at all)
        if lims_normalized == 'DETECTED':
            return 'VALID_DETECTED', 'No error - DETECTED'
        elif lims_normalized == 'NOT DETECTED':
            return 'VALID_NOT_DETECTED', 'No error - NOT DETECTED'
        elif not lims:
            # Controls with no error/lims/resolution = passed controls
            if well_type == 'CONTROL' and not res_list:
                return 'VALID_CONTROL', 'Passed control - No error, no lims, no resolution'
            return 'VALID_OTHER', 'No error - No lims_status'
        else:
            return 'VALID_OTHER', f'No error - {lims}'

    # Fallback
    return 'NEEDS_REVIEW', 'Unclear categorization - needs manual review'


def recategorize_csv(input_file, output_file):
    """Re-categorize CSV with updated logic"""

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
            # Write header
            f.write('# QST Category Mapping - v3 (Updated based on user feedback)\n')
            f.write('#\n')
            f.write('# CLASSIFICATION DISCREPANCIES:\n')
            f.write('#   DISCREP_IN_ERROR - Unresolved\n')
            f.write('#   DISCREP_IGNORED - Error ignored, result reported\n')
            f.write('#   DISCREP_RESULT_CHANGED - Result changed (test repeated)\n')
            f.write('#   DISCREP_NEEDS_CLS_DATA - BLA resolution, needs machine_cls/final_cls data\n')
            f.write('#\n')
            f.write('# SAMPLE SOP ERRORS:\n')
            f.write('#   SOP_UNRESOLVED - Unresolved\n')
            f.write('#   SOP_IGNORED - Error ignored, result reported\n')
            f.write('#   SOP_REPEATED - Test repeated/excluded\n')
            f.write('#\n')
            f.write('# CONTROL ERRORS:\n')
            f.write('#   CONTROL_UNRESOLVED - Unresolved\n')
            f.write('#   CONTROL_IGNORED - Error ignored, deemed acceptable\n')
            f.write('#   CONTROL_REPEATED - Control repeated/excluded\n')
            f.write('#   CONTROL_AFFECTED_SAMPLE - INHERITED_*_FAILURE (for Controls appendix)\n')
            f.write('#\n')
            f.write('# VALID RESULTS:\n')
            f.write('#   VALID_DETECTED - No error, DETECTED\n')
            f.write('#   VALID_NOT_DETECTED - No error, NOT DETECTED\n')
            f.write('#   VALID_CONTROL - No error, control well\n')
            f.write('#   VALID_OTHER - No error, other status\n')
            f.write('#\n')
            f.write('# SPECIAL CATEGORIES:\n')
            f.write('#   IGNORE_WELL - Wells to ignore (MIX_MISSING, UNKNOWN_MIX, etc.)\n')
            f.write('#   NEEDS_REVIEW - Unclear, needs manual review\n')
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

    print("\n=== Category Breakdown ===\n")
    for cat in sorted(category_counts.keys()):
        count = category_counts[cat]
        pct = (count / sum(category_counts.values())) * 100
        print(f"  {cat:40} {count:>10,} ({pct:>5.1f}%)")
    print(f"\n  {'TOTAL':40} {sum(category_counts.values()):>10,}")


def main():
    input_file = 'output_data/qst_category_mapping_TEMPLATE.csv'
    output_file = 'output_data/qst_category_mapping_v3.csv'

    recategorize_csv(input_file, output_file)


if __name__ == '__main__':
    main()
