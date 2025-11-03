#!/usr/bin/env python3
"""
Extract control report data from database to JSON
Based on generate_control_report_working.py
"""

import sqlite3
import json
import argparse
from datetime import datetime
from collections import defaultdict

def fetch_control_errors(conn, limit=None):
    """Fetch control wells with errors or resolutions"""
    cursor = conn.cursor()
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Unresolved query - exact copy from generate_control_report_working.py
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
    AND ec.error_type != 0
    AND w.role_alias IS NOT NULL
    AND w.role_alias != 'Patient'
    AND (w.role_alias LIKE '%PC%' 
         OR w.role_alias LIKE '%NC%' 
         OR w.role_alias LIKE '%CONTROL%'
         OR w.role_alias LIKE '%NEGATIVE%'
         OR w.role_alias LIKE '%POSITIVE%'
         OR w.role_alias LIKE '%NTC%'
         OR w.role_alias LIKE '%PTC%')
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """
    
    # Resolved query
    resolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        COALESCE(w.resolution_codes, ec.error_code) as error_code,
        COALESCE(ec.error_message, 'Resolved') as error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        'resolved' as category
    FROM wells w
    LEFT JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL 
    AND w.resolution_codes <> ''
    AND (ec.error_type IS NULL OR ec.error_type != 0)
    AND w.role_alias IS NOT NULL
    AND w.role_alias != 'Patient'
    AND (w.role_alias LIKE '%PC%' 
         OR w.role_alias LIKE '%NC%' 
         OR w.role_alias LIKE '%CONTROL%'
         OR w.role_alias LIKE '%NEGATIVE%'
         OR w.role_alias LIKE '%POSITIVE%'
         OR w.role_alias LIKE '%NTC%'
         OR w.role_alias LIKE '%PTC%')
    ORDER BY m.mix_name, w.sample_name
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
        resolution_code = (error.get('error_code') or '').upper()
        
        # Categorization logic from generate_control_report_working.py
        if 'RP' in resolution_code or 'RX' in resolution_code or 'TN' in resolution_code:
            error['clinical_category'] = 'test_repeated'
            test_repeated_list.append(error)
        else:
            error['clinical_category'] = 'error_ignored'
            error_ignored_list.append(error)
    
    all_errors.extend(error_ignored_list)
    all_errors.extend(test_repeated_list)
    print(f"    Found {len(resolved)} resolved errors ({len(error_ignored_list)} ignored, {len(test_repeated_list)} repeated)")
    
    return all_errors

def fetch_affected_samples(conn):
    """Fetch patient samples affected by failed controls"""
    cursor = conn.cursor()
    
    # INHERITED errors query
    inherited_query = """
    SELECT DISTINCT
        pw.id as well_id,
        pw.sample_name,
        pw.well_number,
        pec.error_code,
        pec.error_message,
        pm.mix_name,
        pr.run_name,
        pw.lims_status,
        pw.resolution_codes,
        cw.id as control_well_id,
        cw.sample_name as control_name,
        cw.well_number as control_well,
        cm.mix_name as control_mix,
        cw.resolution_codes as control_resolution
    FROM wells pw
    JOIN error_codes pec ON pw.error_code_id = pec.id
    JOIN runs pr ON pw.run_id = pr.id
    JOIN run_mixes prm ON pw.run_mix_id = prm.id
    JOIN mixes pm ON prm.mix_id = pm.id
    JOIN wells cw ON cw.run_id = pw.run_id
    JOIN run_mixes crm ON cw.run_mix_id = crm.id
    JOIN mixes cm ON crm.mix_id = cm.id
    WHERE pw.error_code_id IN (
        '937829a3-a630-4a86-939d-c2b1ec229c9d',
        '937829a3-aa88-44cf-bbd5-deade616cff5',
        '995a530f-1da9-457d-9217-5afdac6ca59f',
        '995a530f-2239-4007-80f9-4102b5826ee5'
    )
    AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
    AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
    AND cw.role_alias IS NOT NULL
    AND cw.role_alias != 'Patient'
    AND (cw.error_code_id IS NOT NULL OR cw.resolution_codes IS NOT NULL)
    """
    
    # REPEATED samples query
    repeated_query = """
    SELECT DISTINCT
        pw.id as well_id,
        pw.sample_name,
        pw.well_number,
        '' as error_code,
        'Repeated due to control' as error_message,
        pm.mix_name,
        pr.run_name,
        pw.lims_status,
        pw.resolution_codes,
        cw.id as control_well_id,
        cw.sample_name as control_name,
        cw.well_number as control_well,
        cm.mix_name as control_mix,
        cw.resolution_codes as control_resolution
    FROM wells pw
    JOIN runs pr ON pw.run_id = pr.id
    JOIN run_mixes prm ON pw.run_mix_id = prm.id
    JOIN mixes pm ON prm.mix_id = pm.id
    JOIN wells cw ON cw.run_id = pw.run_id
    JOIN run_mixes crm ON cw.run_mix_id = crm.id
    JOIN mixes cm ON crm.mix_id = cm.id
    WHERE pw.lims_status IN ('REAMP','REXCT','RPT','RXT','TNP')
    AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
    AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
    AND cw.role_alias IS NOT NULL
    AND cw.role_alias != 'Patient'
    AND (cw.resolution_codes LIKE '%RP%' 
         OR cw.resolution_codes LIKE '%RX%' 
         OR cw.resolution_codes LIKE '%TN%')
    """
    
    print("  Fetching affected samples...")
    cursor.execute(inherited_query)
    inherited_results = cursor.fetchall()
    
    cursor.execute(repeated_query)
    repeated_results = cursor.fetchall()
    
    # Count unique samples
    unique_inherited = set()
    for row in inherited_results:
        unique_inherited.add(row['well_id'])
    
    unique_repeated = set()
    for row in repeated_results:
        unique_repeated.add(row['well_id'])
    
    print(f"    Found {len(inherited_results)} rows with {len(unique_inherited)} unique INHERITED affected samples")
    print(f"    Found {len(repeated_results)} rows with {len(unique_repeated)} unique REPEATED affected samples")
    
    # Group by control set
    grouped = {}
    for row in list(inherited_results) + list(repeated_results):
        group_key = f"{row['run_name']}_{row['control_mix']}"
        
        if group_key not in grouped:
            grouped[group_key] = {
                'run_name': row['run_name'],
                'control_mix': row['control_mix'],
                'controls': {},
                'affected_samples_error': {},
                'affected_samples_repeat': {}
            }
        
        # Add control info
        control_id = row['control_well_id']
        if control_id not in grouped[group_key]['controls']:
            grouped[group_key]['controls'][control_id] = {
                'control_name': row['control_name'],
                'control_well': row['control_well'],
                'resolution': row['control_resolution']
            }
        
        # Categorize sample
        lims_status = row['lims_status']
        is_repeated_sample = lims_status in ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
        
        sample_data = {
            'well_id': row['well_id'],
            'sample_name': row['sample_name'],
            'well_number': row['well_number'],
            'error_code': row['error_code'],
            'error_message': row['error_message'],
            'mix_name': row['mix_name'],
            'run_name': row['run_name'],
            'lims_status': lims_status,
            'resolution_codes': row['resolution_codes']
        }
        
        if is_repeated_sample:
            grouped[group_key]['affected_samples_repeat'][row['well_id']] = sample_data
        else:
            grouped[group_key]['affected_samples_error'][row['well_id']] = sample_data
    
    return grouped, len(unique_inherited), len(unique_repeated)

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
    parser = argparse.ArgumentParser(description='Extract control report data to JSON')
    parser.add_argument('--db', default='input_data/quest_prod_aug2025.db',
                       help='Path to database')
    parser.add_argument('--output', default='control_data.json',
                       help='Output JSON file')
    parser.add_argument('--limit', type=int,
                       help='Limit number of records')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - limit to 100 records')
    
    args = parser.parse_args()
    
    if args.test:
        args.limit = 100
        args.output = 'control_data_test.json'
    
    print(f"Connecting to database: {args.db}")
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    
    try:
        # Fetch data
        print("\nFetching control error data...")
        errors = fetch_control_errors(conn, args.limit)
        
        print("\nFetching affected samples...")
        affected_samples, error_count, repeat_count = fetch_affected_samples(conn)
        
        # Calculate summary
        summary = get_summary_stats(errors)
        summary['affected_error_count'] = error_count
        summary['affected_repeat_count'] = repeat_count
        
        # Build JSON structure
        data = {
            'report_type': 'control',
            'generated_at': datetime.now().isoformat(),
            'database': args.db,
            'summary': summary,
            'errors': errors,
            'affected_samples': affected_samples
        }
        
        # Save to file
        with open(args.output, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"\n=== SUMMARY ===")
        print(f"Total errors: {summary['total_errors']}")
        print(f"  Unresolved: {summary['unresolved']}")
        print(f"  Error Ignored: {summary['error_ignored']}")
        print(f"  Test Repeated: {summary['test_repeated']}")
        print(f"Affected samples:")
        print(f"  ERROR: {error_count}")
        print(f"  REPEATS: {repeat_count}")
        print(f"\nData saved to: {args.output}")
        
        # Validate expected counts
        print(f"\n=== VALIDATION ===")
        expected = {
            'total': 3161,
            'unresolved': 932,
            'error_ignored': 1942,
            'test_repeated': 287,
            'affected_error': 8816,
            'affected_repeat': 3936
        }
        
        if not args.test:
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
            
            if error_count == expected['affected_error']:
                print(f"✓ Affected ERROR match: {error_count}")
            else:
                print(f"✗ Affected ERROR mismatch: {error_count} != {expected['affected_error']}")
            
            if repeat_count == expected['affected_repeat']:
                print(f"✓ Affected REPEATS match: {repeat_count}")
            else:
                print(f"✗ Affected REPEATS mismatch: {repeat_count} != {expected['affected_repeat']}")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()