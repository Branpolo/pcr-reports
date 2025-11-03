#!/usr/bin/env python3
"""
Extract sample report data from database to JSON
Based on generate_error_report_interactive_fixed.py
"""

import sqlite3
import json
import argparse
from datetime import datetime
from collections import defaultdict

# Error types from generate_error_report_interactive_fixed.py
INCLUDED_ERROR_TYPES = [
    'INH_WELL',
    'ADJ_CT',
    'DO_NOT_EXPORT',
    'INCONCLUSIVE_WELL',
    'CTDISC_WELL',
    'BICQUAL_WELL',
    'BAD_CT_DELTA',
    'LOW_FLUORESCENCE_WELL'
]

SETUP_ERROR_TYPES = [
    'MIX_MISSING',
    'UNKNOWN_MIX',
    'ACCESSION_MISSING',
    'INVALID_ACCESSION',
    'UNKNOWN_ROLE',
    'CONTROL_FAILURE',
    'MISSING_CONTROL',
    'INHERITED_CONTROL_FAILURE'
]

def fetch_sample_errors(conn, include_label_errors=False, limit=None):
    """Fetch patient sample errors"""
    cursor = conn.cursor()
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Build error type list
    error_types = INCLUDED_ERROR_TYPES.copy()
    if include_label_errors:
        error_types.extend(SETUP_ERROR_TYPES)
    
    error_types_str = "','".join(error_types)
    
    # Excluded error types from generate_error_report_interactive_fixed.py
    excluded_types = [
        'MIX_MISSING', 'UNKNOWN_MIX', 'ACCESSION_MISSING', 'INVALID_ACCESSION',
        'UNKNOWN_ROLE', 'CONTROL_FAILURE', 'MISSING_CONTROL',
        'INHERITED_CONTROL_FAILURE', 'WG_ERROR', 'BLA'
    ]
    excluded_str = "','".join(excluded_types)
    
    # Unresolved query - matches generate_error_report_interactive_fixed.py
    unresolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        ec.error_code,
        ec.error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        'unresolved' as category
    FROM wells w
    JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.error_code_id IS NOT NULL
    AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
    AND ec.error_code IN ('{error_types_str}')
    AND ec.error_code NOT IN ('{excluded_str}')
    -- Only patient wells (exclude controls)
    AND (w.role_alias IS NULL 
         OR (w.role_alias NOT LIKE '%CONTROL%' 
             AND w.role_alias NOT LIKE '%NC%'
             AND w.role_alias NOT LIKE '%PC%'
             AND w.role_alias NOT LIKE '%NTC%'
             AND w.role_alias NOT LIKE '%PTC%'
             AND w.role_alias NOT LIKE '%HPC%'
             AND w.role_alias NOT LIKE '%LPC%'
             AND w.role_alias NOT LIKE '%NEGATIVE%'
             AND w.role_alias NOT IN ('NC', 'PC', 'HPC', 'LPC', 'WG', 'QUANT')))
    -- Exclude classification discrepancy wells
    AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations 
        WHERE machine_cls <> dxai_cls 
        OR (machine_cls IS NULL AND dxai_cls IS NOT NULL) 
        OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
    )
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """
    
    # Resolved query - matches generate_error_report_interactive_fixed.py
    resolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        w.resolution_codes as error_code,
        'Resolved' as error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        'resolved' as category
    FROM wells w
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL 
    AND w.resolution_codes <> ''
    AND w.error_code_id IS NULL
    -- Exclude BLA resolution code (IC discrepancy) - any variant
    AND w.resolution_codes NOT LIKE '%BLA%'
    -- Only patient wells
    AND (w.role_alias IS NULL 
         OR (w.role_alias NOT LIKE '%CONTROL%' 
             AND w.role_alias NOT LIKE '%NC%'
             AND w.role_alias NOT LIKE '%PC%'
             AND w.role_alias NOT LIKE '%NTC%'
             AND w.role_alias NOT LIKE '%PTC%'
             AND w.role_alias NOT LIKE '%HPC%'
             AND w.role_alias NOT LIKE '%LPC%'
             AND w.role_alias NOT LIKE '%NEG%'
             AND w.role_alias NOT LIKE '%NEGATIVE%'
             AND w.role_alias NOT LIKE '00-%'
             AND w.role_alias NOT IN ('NC', 'PC', 'HPC', 'LPC', 'WG', 'QUANT')))
    -- Exclude classification discrepancy wells
    AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations 
        WHERE machine_cls <> dxai_cls 
        OR (machine_cls IS NULL AND dxai_cls IS NOT NULL) 
        OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
    )
    ORDER BY m.mix_name, w.sample_name
    {limit_clause}
    """
    
    # Add query for resolved with new error - from generate_error_report_interactive_fixed.py
    resolved_with_new_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        ec.error_code,
        ec.error_message || ' (was: ' || w.resolution_codes || ')' as error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        'resolved_with_new' as category
    FROM wells w
    JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL 
    AND w.resolution_codes <> ''
    AND w.error_code_id IS NOT NULL
    AND ec.error_code IN ('{error_types_str}')
    AND ec.error_code NOT IN ('{excluded_str}')
    -- Exclude BLA resolution
    AND w.resolution_codes NOT IN ('BLA')
    -- Only patient wells
    AND (w.role_alias IS NULL 
         OR (w.role_alias NOT LIKE '%CONTROL%' 
             AND w.role_alias NOT LIKE '%NC%'
             AND w.role_alias NOT LIKE '%PC%'
             AND w.role_alias NOT LIKE '%NTC%'
             AND w.role_alias NOT LIKE '%PTC%'
             AND w.role_alias NOT LIKE '%HPC%'
             AND w.role_alias NOT LIKE '%LPC%'
             AND w.role_alias NOT LIKE '%NEG%'
             AND w.role_alias NOT LIKE '%NEGATIVE%'
             AND w.role_alias NOT LIKE '00-%'
             AND w.role_alias NOT IN ('NC', 'PC', 'HPC', 'LPC', 'WG', 'QUANT')))
    -- Exclude classification discrepancy wells
    AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations 
        WHERE machine_cls <> dxai_cls 
        OR (machine_cls IS NULL AND dxai_cls IS NOT NULL) 
        OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
    )
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """
    
    all_errors = []
    
    # Fetch unresolved
    print("  Fetching unresolved errors...")
    cursor.execute(unresolved_query)
    unresolved = cursor.fetchall()
    unresolved_list = []
    for row in unresolved:
        error = dict(row)
        error['clinical_category'] = 'unresolved'
        unresolved_list.append(error)
    all_errors.extend(unresolved_list)
    print(f"    Found {len(unresolved)} unresolved errors")
    
    # Fetch resolved and categorize
    print("  Fetching resolved errors...")
    cursor.execute(resolved_query)
    resolved = cursor.fetchall()
    error_ignored_list = []
    test_repeated_list = []
    
    for row in resolved:
        error = dict(row)
        # Categorize based on LIMS status - exact logic from generate_error_report_interactive_fixed.py
        lims_status = (error.get('lims_status') or '').upper()
        if lims_status in ['DETECTED', 'NOT DETECTED']:
            error['clinical_category'] = 'error_ignored'
            error_ignored_list.append(error)
        elif lims_status in ['INCONCLUSIVE', 'EXCLUDE', 'REXCT', 'REAMP', 'RXT', 'RPT', 'TNP'] or not lims_status:
            error['clinical_category'] = 'test_repeated'
            test_repeated_list.append(error)
        else:
            # Default to test_repeated for other statuses
            error['clinical_category'] = 'test_repeated'
            test_repeated_list.append(error)
    
    all_errors.extend(error_ignored_list)
    all_errors.extend(test_repeated_list)
    print(f"    Found {len(resolved)} resolved errors ({len(error_ignored_list)} ignored, {len(test_repeated_list)} repeated)")
    
    # Fetch resolved with new error
    print("  Fetching resolved with new errors...")
    cursor.execute(resolved_with_new_query)
    resolved_new = cursor.fetchall()
    resolved_new_list = []
    for row in resolved_new:
        error = dict(row)
        # These are all test repeated since they have new errors
        error['clinical_category'] = 'test_repeated'
        resolved_new_list.append(error)
    all_errors.extend(resolved_new_list)
    print(f"    Found {len(resolved_new)} resolved with new errors")
    
    return all_errors

def get_summary_stats(errors):
    """Calculate summary statistics"""
    counts = defaultdict(int)
    for error in errors:
        counts[error['clinical_category']] += 1
    
    return {
        'total_errors': len(errors),
        'unresolved': counts['unresolved'],
        'error_ignored': counts['error_ignored'],
        'test_repeated': counts['test_repeated']
    }

def main():
    parser = argparse.ArgumentParser(description='Extract sample report data to JSON')
    parser.add_argument('--db', default='input_data/quest_prod_aug2025.db',
                       help='Path to database')
    parser.add_argument('--output', default='sample_data.json',
                       help='Output JSON file')
    parser.add_argument('--include-label-errors', action='store_true',
                       help='Include label/setup errors')
    parser.add_argument('--limit', type=int,
                       help='Limit number of records')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - limit to 100 records')
    
    args = parser.parse_args()
    
    if args.test:
        args.limit = 100
        args.output = 'sample_data_test.json'
    
    print(f"Connecting to database: {args.db}")
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    
    try:
        # Fetch data
        print("\nFetching sample error data...")
        errors = fetch_sample_errors(conn, args.include_label_errors, args.limit)
        
        # Calculate summary
        summary = get_summary_stats(errors)
        
        # Build JSON structure
        data = {
            'report_type': 'sample',
            'generated_at': datetime.now().isoformat(),
            'database': args.db,
            'include_label_errors': args.include_label_errors,
            'summary': summary,
            'errors': errors
        }
        
        # Save to file
        with open(args.output, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"\n=== SUMMARY ===")
        print(f"Total errors: {summary['total_errors']}")
        print(f"  Unresolved: {summary['unresolved']}")
        print(f"  Error Ignored: {summary['error_ignored']}")
        print(f"  Test Repeated: {summary['test_repeated']}")
        print(f"\nData saved to: {args.output}")
        
        # Validate expected counts (from baseline run)
        print(f"\n=== VALIDATION ===")
        expected = {
            'total': 19654,
            'unresolved': 11390,
            'error_ignored': 1892,
            'test_repeated': 6372
        }
        
        if not args.test and not args.include_label_errors:
            if summary['total_errors'] == expected['total']:
                print(f"✓ Total errors match: {summary['total_errors']}")
            else:
                print(f"✗ Total errors mismatch: {summary['total_errors']} != {expected['total']}")
            
            if summary['unresolved'] == expected['unresolved']:
                print(f"✓ Unresolved match: {summary['unresolved']}")
            else:
                print(f"✗ Unresolved mismatch: {summary['unresolved']} != {expected['unresolved']}")
            
            if summary['error_ignored'] == expected['error_ignored']:
                print(f"✓ Error ignored match: {summary['error_ignored']}")
            else:
                print(f"✗ Error ignored mismatch: {summary['error_ignored']} != {expected['error_ignored']}")
            
            if summary['test_repeated'] == expected['test_repeated']:
                print(f"✓ Test repeated match: {summary['test_repeated']}")
            else:
                print(f"✗ Test repeated mismatch: {summary['test_repeated']} != {expected['test_repeated']}")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()