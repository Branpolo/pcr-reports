#!/usr/bin/env python3
"""
Analyze resolved errors to find unique resolution codes and LIMS statuses
"""

import sqlite3
from collections import Counter

def analyze_resolved_errors():
    conn = sqlite3.connect('input_data/quest_prod_aug2025.db')
    conn.row_factory = sqlite3.Row
    
    # Query for resolved errors (no current error code)
    query = """
    SELECT DISTINCT
        w.resolution_codes,
        w.lims_status,
        w.sample_name
    FROM wells w
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL 
    AND w.resolution_codes <> ''
    AND w.error_code_id IS NULL
    -- Exclude BLA resolution code (IC discrepancy)
    AND w.resolution_codes NOT IN ('BLA')
    -- Only patient wells
    AND (w.role_alias IS NULL OR w.role_alias NOT IN ('NC', 'PC', 'HPC', 'LPC', 'WG', 'QUANT'))
    -- Exclude classification discrepancy wells
    AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations 
        WHERE machine_cls <> dxai_cls 
        OR (machine_cls IS NULL AND dxai_cls IS NOT NULL) 
        OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
    )
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"Total resolved (non-discrepancy) errors: {len(results)}\n")
    
    # Collect unique resolution codes and LIMS statuses
    resolution_codes = Counter()
    lims_statuses = Counter()
    
    for row in results:
        if row['resolution_codes']:
            # Some resolution codes might have multiple values separated by |
            codes = row['resolution_codes'].split('|')
            for code in codes:
                code = code.strip()
                if code:
                    resolution_codes[code] += 1
        
        if row['lims_status']:
            lims_statuses[row['lims_status']] += 1
        else:
            lims_statuses['(NULL/Empty)'] += 1
    
    # Print results
    print("=" * 60)
    print("A. UNIQUE RESOLUTION CODES FOR RESOLVED ERRORS:")
    print("=" * 60)
    for code, count in sorted(resolution_codes.items(), key=lambda x: -x[1]):
        print(f"  {code}: {count:,} occurrences")
    
    print(f"\nTotal unique resolution codes: {len(resolution_codes)}")
    
    print("\n" + "=" * 60)
    print("B. UNIQUE LIMS EXPORT STATUSES FOR RESOLVED ERRORS:")
    print("=" * 60)
    for status, count in sorted(lims_statuses.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count:,} occurrences")
    
    print(f"\nTotal unique LIMS statuses: {len(lims_statuses)}")
    
    # Now check discrepancy-specific resolution codes for comparison
    print("\n" + "=" * 60)
    print("BONUS: RESOLUTION CODES FOR DISCREPANCY WELLS (for reference):")
    print("=" * 60)
    
    disc_query = """
    SELECT DISTINCT
        w.resolution_codes
    FROM wells w
    WHERE w.resolution_codes IS NOT NULL 
    AND w.resolution_codes <> ''
    AND w.id IN (
        SELECT DISTINCT well_id FROM observations 
        WHERE machine_cls <> dxai_cls 
        OR (machine_cls IS NULL AND dxai_cls IS NOT NULL) 
        OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
    )
    """
    
    cursor.execute(disc_query)
    disc_results = cursor.fetchall()
    
    disc_codes = Counter()
    for row in disc_results:
        if row['resolution_codes']:
            codes = row['resolution_codes'].split('|')
            for code in codes:
                code = code.strip()
                if code:
                    disc_codes[code] += 1
    
    for code, count in sorted(disc_codes.items(), key=lambda x: -x[1])[:10]:
        print(f"  {code}: {count:,} occurrences")
    
    if len(disc_codes) > 10:
        print(f"  ... and {len(disc_codes) - 10} more codes")
    
    conn.close()

if __name__ == '__main__':
    analyze_resolved_errors()