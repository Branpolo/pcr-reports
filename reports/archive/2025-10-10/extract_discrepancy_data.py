#!/usr/bin/env python3
"""
Extract discrepancy (QST) report data from database to JSON
Based on generate_qst_report_interactive_v2.py
"""

import sqlite3
import json
import argparse
from datetime import datetime
from collections import defaultdict

def categorize_record(row):
    """Categorize record based on classification discrepancies - from generate_qst_report_interactive_v2.py"""
    machine_cls = row['machine_cls']
    final_cls = row['final_cls']
    lims_status = row['lims_status']
    error_code = row['error_code']
    
    # Check suppression condition first
    if not lims_status and not error_code:
        return ('suppressed', None, 0)  # Will be filtered out
    
    # Section 1: Discrepancies Acted Upon (machine != final AND LIMS is DETECTED/NOT DETECTED)
    if machine_cls != final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
        if final_cls == 1:
            return ('discrepancy_positive', '#90EE90', 1)  # Green - False Negative corrected
        else:
            return ('discrepancy_negative', '#FF6B6B', 1)  # Red - False Positive corrected
    
    # Section 2: Samples Repeated (error codes OR LIMS other)
    if error_code:
        return ('has_error', '#FFB6C1', 2)  # Pink - Has error codes
    if lims_status and lims_status not in ('DETECTED', 'NOT DETECTED'):
        return ('lims_other', '#FFD700', 2)  # Yellow - LIMS other status
    
    # Section 3: Discrepancies Ignored (machine = final AND LIMS is DETECTED/NOT DETECTED)
    if machine_cls == final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
        if lims_status == 'DETECTED':
            return ('agreement_detected', '#E8F5E9', 3)  # Pale green
        else:
            return ('agreement_not_detected', '#FCE4EC', 3)  # Pale pink
    
    return ('unknown', '#F5F5F5', 3)

def fetch_discrepancy_data(conn, limit=None):
    """Fetch QST discrepancy data"""
    cursor = conn.cursor()
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Main query - matches generate_qst_report_interactive_v2.py
    query = f"""
    SELECT 
        id,
        sample_label as sample_name,
        well_number,
        lims_status,
        error_code,
        error_message,
        resolution_codes,
        extraction_date,
        machine_cls,
        dxai_cls,
        final_cls,
        manual_cls,
        machine_ct as ct,
        dxai_ct,
        target_name,
        mix_name,
        run_id,
        readings0, readings1, readings2, readings3, readings4,
        readings5, readings6, readings7, readings8, readings9,
        readings10, readings11, readings12, readings13, readings14,
        readings15, readings16, readings17, readings18, readings19,
        readings20, readings21, readings22, readings23, readings24,
        readings25, readings26, readings27, readings28, readings29,
        readings30, readings31, readings32, readings33, readings34,
        readings35, readings36, readings37, readings38, readings39,
        readings40, readings41, readings42, readings43, readings44,
        readings45, readings46, readings47, readings48, readings49
    FROM qst_readings
    WHERE in_use = 1
    AND UPPER(target_name) NOT LIKE '%IPC%'
    AND UPPER(target_name) NOT LIKE '%QIPC%'
    AND UPPER(mix_name) NOT LIKE '%IPC%'
    AND NOT (machine_cls = final_cls AND UPPER(resolution_codes) LIKE '%BLA|%')
    ORDER BY mix_name, target_name, sample_label
    {limit_clause}
    """
    
    print("  Fetching discrepancy records...")
    cursor.execute(query)
    rows = cursor.fetchall()
    
    all_records = []
    suppressed_count = 0
    section_counts = {1: 0, 2: 0, 3: 0}
    category_counts = defaultdict(int)
    
    for row in rows:
        record = dict(row)
        
        # Categorize the record
        category, color, section = categorize_record(record)
        
        # Skip suppressed records
        if section == 0:
            suppressed_count += 1
            continue
        
        # Add categorization info
        record['category'] = category
        record['color'] = color
        record['section'] = section
        
        # Map sections to clinical categories for consistency
        if section == 1:
            record['clinical_category'] = 'acted_upon'
        elif section == 2:
            record['clinical_category'] = 'samples_repeated'
        elif section == 3:
            record['clinical_category'] = 'ignored'
        
        # Extract readings as list
        readings = []
        for i in range(50):
            val = record.get(f'readings{i}')
            if val is not None:
                readings.append(val)
        record['readings'] = readings
        
        # Remove individual reading fields to reduce JSON size
        for i in range(50):
            if f'readings{i}' in record:
                del record[f'readings{i}']
        
        # Add compatibility fields
        record['well_id'] = record['id']
        record['run_name'] = record['run_id']
        record['well_number'] = record.get('well_number', '')
        record['error_message'] = record.get('error_message', record.get('error_code', ''))
        
        all_records.append(record)
        section_counts[section] += 1
        category_counts[record['clinical_category']] += 1
    
    print(f"    Found {len(rows)} total records")
    print(f"    Displayed: {len(all_records)}, Suppressed: {suppressed_count}")
    print(f"    Section 1 (Acted Upon): {section_counts[1]}")
    print(f"    Section 2 (Repeated): {section_counts[2]}")
    print(f"    Section 3 (Ignored): {section_counts[3]}")
    
    return all_records, category_counts

def get_summary_stats(category_counts):
    """Calculate summary statistics"""
    return {
        'total_displayed': sum(category_counts.values()),
        'acted_upon': category_counts['acted_upon'],
        'samples_repeated': category_counts['samples_repeated'],
        'ignored': category_counts['ignored']
    }

def main():
    parser = argparse.ArgumentParser(description='Extract discrepancy report data to JSON')
    parser.add_argument('--db', default='qst_discreps.db',
                       help='Path to QST database')
    parser.add_argument('--output', default='discrepancy_data.json',
                       help='Output JSON file')
    parser.add_argument('--limit', type=int,
                       help='Limit number of records')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - limit to 100 records')
    
    args = parser.parse_args()
    
    if args.test:
        args.limit = 100
        args.output = 'discrepancy_data_test.json'
    
    print(f"Connecting to database: {args.db}")
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    
    try:
        # Fetch data
        print("\nFetching discrepancy data...")
        errors, category_counts = fetch_discrepancy_data(conn, args.limit)
        
        # Calculate summary
        summary = get_summary_stats(category_counts)
        
        # Build JSON structure
        data = {
            'report_type': 'discrepancy',
            'generated_at': datetime.now().isoformat(),
            'database': args.db,
            'summary': summary,
            'errors': errors
        }
        
        # Save to file
        with open(args.output, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"\n=== SUMMARY ===")
        print(f"Total displayed: {summary['total_displayed']}")
        print(f"  Acted Upon: {summary['acted_upon']}")
        print(f"  Samples Repeated: {summary['samples_repeated']}")
        print(f"  Ignored: {summary['ignored']}")
        print(f"\nData saved to: {args.output}")
        
        # Validate expected counts
        print(f"\n=== VALIDATION ===")
        expected = {
            'total': 5587,
            'acted_upon': 374,
            'samples_repeated': 1307,
            'ignored': 3906
        }
        
        if not args.test:
            if summary['total_displayed'] == expected['total']:
                print(f"✓ Total displayed match: {summary['total_displayed']}")
            else:
                print(f"✗ Total displayed mismatch: {summary['total_displayed']} != {expected['total']}")
            
            if summary['acted_upon'] == expected['acted_upon']:
                print(f"✓ Acted upon match: {summary['acted_upon']}")
            else:
                print(f"✗ Acted upon mismatch: {summary['acted_upon']} != {expected['acted_upon']}")
            
            if summary['samples_repeated'] == expected['samples_repeated']:
                print(f"✓ Samples repeated match: {summary['samples_repeated']}")
            else:
                print(f"✗ Samples repeated mismatch: {summary['samples_repeated']} != {expected['samples_repeated']}")
            
            if summary['ignored'] == expected['ignored']:
                print(f"✓ Ignored match: {summary['ignored']}")
            else:
                print(f"✗ Ignored mismatch: {summary['ignored']} != {expected['ignored']}")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()