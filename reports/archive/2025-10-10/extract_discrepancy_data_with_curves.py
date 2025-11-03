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

def get_well_data_with_targets(conn, well_id):
    """Get all targets for a well with their readings"""
    cursor = conn.cursor()
    
    # Query adapted from generate_qst_report_interactive_v2.py
    query = """
    SELECT 
        target_name,
        machine_ct as ct,
        readings0, readings1, readings2, readings3, readings4,
        readings5, readings6, readings7, readings8, readings9,
        readings10, readings11, readings12, readings13, readings14,
        readings15, readings16, readings17, readings18, readings19,
        readings20, readings21, readings22, readings23, readings24,
        readings25, readings26, readings27, readings28, readings29,
        readings30, readings31, readings32, readings33, readings34,
        readings35, readings36, readings37, readings38, readings39,
        readings40, readings41, readings42, readings43, readings44,
        readings45, readings46, readings47, readings48, readings49,
        CASE 
            WHEN UPPER(target_name) LIKE '%IPC%' OR UPPER(target_name) = 'IC' OR UPPER(target_name) = 'IPC' THEN 1
            ELSE 0
        END as is_ic
    FROM qst_readings
    WHERE id = ?
    """
    
    cursor.execute(query, (well_id,))
    result = cursor.fetchone()
    
    if not result:
        return None
    
    # Extract readings from individual columns
    readings = []
    for i in range(50):
        val = result[f'readings{i}']
        if val is not None:
            readings.append(val)
    
    # Convert to list format with single target (QST has one target per row)
    target_data = {
        'target_name': result['target_name'],
        'readings': readings,
        'machine_ct': result['ct'],
        'is_ic': result['is_ic']
    }
    
    return [target_data]

def get_control_curves_from_quest(qconn, run_id, mix_name, target_name, limit=3):
    """Fetch control curves from Quest DB for a given run+target, prioritizing same-mix controls.
    Returns up to `limit` items: {type, readings, ct}.
    """
    cur = qconn.cursor()
    query = """
    SELECT 
        w.role_alias,
        o.readings,
        o.machine_ct,
        m.mix_name
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.run_id = ?
      AND t.target_name = ?
      AND w.role_alias IS NOT NULL
      AND w.role_alias != 'Patient'
      AND (
        w.role_alias LIKE '%NC' OR w.role_alias LIKE '%PC' OR w.role_alias LIKE '%HPC' OR w.role_alias LIKE '%LPC' OR
        w.role_alias = 'NEGATIVE' OR w.role_alias = 'PC' OR w.role_alias = 'NC'
      )
      AND t.is_passive = 0
      AND o.readings IS NOT NULL
    ORDER BY 
      CASE WHEN m.mix_name = ? THEN 0 ELSE 1 END,
      CASE 
        WHEN w.role_alias LIKE '%NC%' OR w.role_alias = 'NC' THEN 0
        WHEN w.role_alias LIKE '%PC%' OR w.role_alias = 'PC' OR w.role_alias LIKE '%HPC%' OR w.role_alias LIKE '%LPC%' THEN 1
        ELSE 2
      END
    LIMIT ?
    """
    cur.execute(query, (run_id, target_name, mix_name, limit * 4))
    rows = cur.fetchall()
    controls = []
    for role_alias, readings_json, ct, ctrl_mix in rows:
        try:
            readings = json.loads(readings_json)
        except Exception:
            continue
        ru = (role_alias or '').upper()
        if 'NC' in ru or ru == 'NEGATIVE' or 'NTC' in ru:
            ctype = 'negative'
        elif 'PC' in ru or 'HPC' in ru or 'LPC' in ru or 'POS' in ru:
            ctype = 'positive'
        else:
            ctype = 'control'
        controls.append({'type': ctype, 'readings': readings, 'ct': ct})
    # Balance selection
    neg = [c for c in controls if c['type'] == 'negative']
    pos = [c for c in controls if c['type'] == 'positive']
    other = [c for c in controls if c['type'] not in ('negative','positive')]
    out = []
    if neg:
        out.extend(neg[:min(2, len(neg))])
    rem = limit - len(out)
    if rem > 0 and pos:
        out.extend(pos[:rem])
    rem = limit - len(out)
    if rem > 0 and other:
        out.extend(other[:rem])
    return out

def get_comments_from_quest(qconn, run_id, well_numbers):
    """Fetch system-generated comments for a single run across multiple wells.
    Returns mapping: well_number -> [ {text, is_system, created_at}, ... ]
    """
    if not well_numbers:
        return {}
    cur = qconn.cursor()
    placeholders = ','.join(['?'] * len(well_numbers))
    query = f"""
    SELECT w.well_number, c.text, c.is_system_generated, c.created_at
    FROM wells w
    JOIN comments c ON c.commentable_id = w.id
    WHERE w.run_id = ?
      AND w.well_number IN ({placeholders})
    ORDER BY w.well_number, c.created_at DESC
    """
    cur.execute(query, (run_id, *well_numbers))
    out = {}
    for well_number, text, is_sys, created_at in cur.fetchall():
        out.setdefault(well_number, []).append({
            'text': text,
            'is_system': is_sys,
            'created_at': created_at
        })
    return out

def get_control_curves_limited(conn, mix_name, target_name, max_controls=3):
    """Get limited number of control curves for a specific mix and target"""
    cursor = conn.cursor()
    
    # For QST database, we need to find control wells
    # Positive controls
    pos_query = """
    SELECT DISTINCT
        machine_ct as ct,
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
    WHERE UPPER(mix_name) = UPPER(?)
    AND UPPER(target_name) = UPPER(?)
    AND (
        sample_label LIKE '%PC%'
        OR sample_label LIKE '%POS%'
        OR sample_label LIKE '%PTC%'
        OR sample_label LIKE '%HPC%'
    )
    AND machine_ct IS NOT NULL
    AND machine_ct > 0
    ORDER BY machine_ct
    LIMIT ?
    """
    
    cursor.execute(pos_query, (mix_name, target_name, max_controls))
    pos_controls = cursor.fetchall()
    
    # Negative controls
    neg_query = """
    SELECT DISTINCT
        machine_ct as ct,
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
    WHERE UPPER(mix_name) = UPPER(?)
    AND UPPER(target_name) = UPPER(?)
    AND (sample_label IN ('NC', 'NTC', 'NEG', 'NEGATIVE') OR sample_label LIKE '%NEG%')
    ORDER BY machine_ct
    LIMIT ?
    """
    
    cursor.execute(neg_query, (mix_name, target_name, max_controls))
    neg_controls = cursor.fetchall()
    
    controls = []
    for control in pos_controls:
        # Extract readings from individual columns
        readings = []
        for i in range(50):
            val = control[f'readings{i}']
            if val is not None:
                readings.append(val)
        
        if readings:
            controls.append({
                'readings': readings,
                'machine_ct': control['ct'],
                'control_type': 'PC'
            })
    
    for control in neg_controls:
        # Extract readings from individual columns
        readings = []
        for i in range(50):
            val = control[f'readings{i}']
            if val is not None:
                readings.append(val)
        
        if readings:
            controls.append({
                'readings': readings,
                'machine_ct': control['ct'],
                'control_type': 'NC'
            })
    
    return controls

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
    parser = argparse.ArgumentParser(description='Extract discrepancy report data to JSON with curve data')
    parser.add_argument('--db', default='qst_discreps.db',
                       help='Path to QST database')
    parser.add_argument('--quest-db', default='input_data/quest_prod_aug2025.db',
                       help='Path to Quest production database (for control curves)')
    parser.add_argument('--output', default='discrepancy_data_with_curves.json',
                       help='Output JSON file')
    parser.add_argument('--limit', type=int,
                       help='Limit number of records')
    parser.add_argument('--test', action='store_true',
                       help='Test mode - limit to 100 records')
    
    args = parser.parse_args()
    
    if args.test:
        args.limit = 100
        args.output = 'discrepancy_data_with_curves_test.json'
    
    print(f"Connecting to database: {args.db}")
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA journal_mode=OFF")
    except Exception:
        pass
    # Quest DB for controls
    print(f"Connecting to Quest database for controls: {args.quest_db}")
    qconn = sqlite3.connect(args.quest_db)
    qconn.row_factory = sqlite3.Row
    
    try:
        # Fetch data
        print("\nFetching discrepancy data...")
        errors, category_counts = fetch_discrepancy_data(conn, args.limit)
        
        # Fetch well curve data for each error
        print("\nFetching well curve data...")
        well_curves = {}
        control_curves_cache = {}
        processed_wells = set()
        
        # Track well_id -> (run_id, well_number) for comments
        well_key_map = {}
        
        for error in errors:
            well_id = error['well_id']
            if well_id not in processed_wells:
                # Get well data with all targets
                well_data = get_well_data_with_targets(conn, well_id)
                if well_data:
                    # Get control curves for each target
                    for target_data in well_data:
                        target_name = target_data['target_name']
                        mix_name = error['mix_name']
                        
                        # Create cache key for control curves
                        cache_key = (error['run_id'], mix_name, target_name)
                        if cache_key not in control_curves_cache:
                            control_curves_cache[cache_key] = get_control_curves_from_quest(
                                qconn, error['run_id'], mix_name, target_name, limit=3
                            )
                        target_data['control_curves'] = control_curves_cache[cache_key]
                    
                    well_curves[well_id] = {
                        'sample_name': error['sample_name'],
                        'mix_name': error['mix_name'],
                        'targets': well_data
                    }
                    processed_wells.add(well_id)
                    # map for comments
                    well_key_map[well_id] = (error['run_id'], error.get('well_number'))
        
        # Attach comments from Quest DB, grouped by run
        if well_key_map:
            from collections import defaultdict as _dd
            runs = _dd(list)
            for wid, (rid, wnum) in well_key_map.items():
                if wnum is not None:
                    runs[rid].append((wid, wnum))
            for rid, items in runs.items():
                wid_list, wnums = zip(*items)
                comments_by_wellnum = get_comments_from_quest(qconn, rid, list(wnums))
                for wid, wnum in items:
                    coms = comments_by_wellnum.get(wnum)
                    if coms and wid in well_curves:
                        well_curves[wid]['comments'] = coms
        
        print(f"  Extracted {len(well_curves)} well curves")
        
        # Calculate summary
        summary = get_summary_stats(category_counts)
        
        # Build JSON structure
        data = {
            'report_type': 'discrepancy',
            'generated_at': datetime.now().isoformat(),
            'database': args.db,
            'summary': summary,
            'errors': errors,
            'well_curves': well_curves
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
        try:
            qconn.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
