#!/usr/bin/env python3
"""
Generate interactive error report for quality issues only
Excludes classification discrepancies, focuses on well quality errors
Optimized version with proper limit handling
"""

import sqlite3
import json
import argparse
import os
from datetime import datetime
from collections import defaultdict

# Control-specific error types to INCLUDE in control report
INCLUDED_ERROR_TYPES = [
    'THRESHOLD_WRONG',           # Control threshold issue
    'CONTROL_CLSDISC_WELL',      # Control classification discrepancy
    'FAILED_POS_WELL',           # Failed positive control
    'BICQUAL_WELL',              # IC quality issue
    'CNTRL_HAS_ACS',            # Control has ACS
    'WG13S_HIGH_WELL',          # Westgard 13S high
    'NEGATIVE_FAILURE_WELL',     # Negative control failure
    'WG_IN_ERROR_WELL',         # Westgard error
    'WG12S_HIGH_WELL',          # Westgard 12S high  
    'INCORRECT_SIGMOID',         # Sigmoid issue
    'WG13S_LOW_WELL',           # Westgard 13S low
    'WG12S_LOW_WELL',           # Westgard 12S low
    'CONTROL_CTDISC_WELL',      # Control CT discrepancy
    'WG14S_LOW_WELL',           # Westgard 14S low
    'LOW_FLUORESCENCE_WELL',    # Low fluorescence
    'WG14S_HIGH_WELL',          # Westgard 14S high
    'WESTGARDS_MISSED'          # Westgard missed
]

# Setup/import error types (separate section) - these go in a different section
SETUP_ERROR_TYPES = [
    'MIX_MISSING',
    'UNKNOWN_MIX', 
    'ACCESSION_MISSING',
    'INVALID_ACCESSION',
    'UNKNOWN_ROLE',
    'CONTROL_FAILURE',  # Control specific failures
    'MISSING_CONTROL',  # Missing controls
    'INHERITED_CONTROL_FAILURE'  # Inherited control failures
]

# Error types to explicitly EXCLUDE from control report (setup/admin errors)
EXCLUDED_ERROR_TYPES = [
    'MIX_MISSING',
    'UNKNOWN_MIX',
    'ACCESSION_MISSING', 
    'INVALID_ACCESSION',
    'UNKNOWN_ROLE',
    'EXTRACTION_INSTRUMENT_MISSING',
    'BLA'  # IC discrepancy resolution code, not an error
]

def get_resolution_message(resolution_code):
    """Convert resolution codes to human-readable messages"""
    if not resolution_code:
        return ""
    
    # Parse the resolution code
    codes = resolution_code.split(',')
    messages = []
    
    for code in codes:
        code = code.strip().upper()
        
        # Handle compound codes
        if '|' in code:
            # Handle cases like BLA|SKIP
            parts = code.split('|')
            for part in parts:
                msg = get_single_code_message(part.strip())
                if msg and msg not in messages:
                    messages.append(msg)
        else:
            msg = get_single_code_message(code)
            if msg:
                messages.append(msg)
    
    return ', '.join(messages) if messages else resolution_code

def get_single_code_message(code):
    """Get message for a single resolution code"""
    # Main resolution codes
    if code == 'SKIP':
        return 'Ignore issue'
    elif code == 'WDCLS':
        return 'Ignore Cls discrepancy'
    elif code == 'WDCLSC':
        return 'Ignore Cls discrepancy (confirmed)'
    elif code == 'WDCT':
        return 'Ignore CT discrepancy'
    elif code == 'WDCTC':
        return 'Ignore CT discrepancy (confirmed)'
    elif code.startswith('RX'):
        return 'Re-extract'
    elif code.startswith('RP') or code.startswith('TP'):
        return 'Repeat test'
    elif code == 'SETPOS':
        return 'Manual override: Positive'
    elif code == 'SETNEG':
        return 'Manual override: Negative'
    elif code.startswith('WG'):
        # Extract well group number if present
        if len(code) > 2:
            return f'Well group {code[2:]} action'
        return 'Well group action'
    elif code == 'BLA':
        return 'IC discrepancy'
    elif code == 'BPEC':
        return 'Special case'
    else:
        return None

def get_well_data_with_targets(conn, well_id):
    """Get all targets for a well with their readings"""
    cursor = conn.cursor()
    
    # Get all non-passive targets
    query = """
    SELECT 
        t.target_name,
        o.readings,
        o.machine_ct,
        t.is_passive,
        CASE 
            WHEN UPPER(t.target_name) LIKE '%IPC%' OR UPPER(t.target_name) = 'IC' OR UPPER(t.target_name) = 'IPC' THEN 1
            ELSE 0
        END as is_ic
    FROM observations o
    JOIN targets t ON o.target_id = t.id
    WHERE o.well_id = ?
    AND t.is_passive = 0
    ORDER BY 
        is_ic,  -- Non-IC first
        t.target_name
    """
    
    cursor.execute(query, (well_id,))
    results = cursor.fetchall()
    
    if not results:
        return None
    
    targets_data = {}
    main_target = None
    
    for row in results:
        target_name = row[0]
        readings_json = row[1]
        ct = row[2]
        is_ic = row[4]
        
        if readings_json:
            try:
                readings = json.loads(readings_json)
                targets_data[target_name] = {
                    'readings': readings,
                    'ct': ct,
                    'is_ic': bool(is_ic)
                }
                
                # First non-IC target becomes the main target
                if not main_target and not is_ic:
                    main_target = target_name
            except:
                pass
    
    if not targets_data:
        return None
    
    return {
        'main_target': main_target,
        'targets': targets_data
    }

def get_backup_control_mapping():
    """Get backup control mappings from CSV file"""
    import csv
    backup_map = {}
    
    try:
        with open('input_data/backup-controls.csv', 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                role = row['ROLE']
                role_type = row['ROLE TYPE']
                backup_mixes = row.get('BACKUP MIXES', '').split(' | ') if row.get('BACKUP MIXES') else []
                
                if role_type in ['PC', 'NC'] and backup_mixes:
                    for mix in backup_mixes:
                        mix = mix.strip()
                        if mix:
                            if mix not in backup_map:
                                backup_map[mix] = {'PC': [], 'NC': []}
                            if role not in backup_map[mix][role_type]:
                                backup_map[mix][role_type].append(role)
    except:
        # If CSV file not found or error, return empty mapping
        pass
    
    return backup_map

def get_control_curves_with_backup(conn, run_id, target_name, mix_name, limit=5):
    """Get control curves, using backup controls if primary ones aren't available"""
    cursor = conn.cursor()
    
    # For backup controls, we need to match on related targets
    # E.g., BKV uses QBK target, BKVQ uses QBKQ target
    # Build a list of related target names
    related_targets = [target_name]
    
    # Add related target patterns - handle various naming conventions
    base_target = target_name.replace('Q2', '').replace('BL', '').replace('PL', '').replace('SE', '').replace('UR', '')
    
    if 'BK' in base_target:
        # BKV family
        related_targets.extend(['QBK', 'QBKQ', 'QBKQUR', 'QBKQSE', 'QBKQPL', 'QBKQBL', 'QBKQU'])
    elif 'CMV' in base_target:
        # CMV family - includes QCMV, QCMVQ2, QCMVQ2PL, etc.
        related_targets.extend(['QCMV', 'QCMVQ', 'QCMVQ2', 'QCMVQ2BL', 'QCMVQ2PL', 'QCMVQ2SE', 'CMVQ'])
    elif 'EBV' in base_target:
        # EBV family
        related_targets.extend(['QEBV', 'QEBVQ', 'QEBVQPL', 'QEBVQBL', 'QEBVQSE', 'EBVQ'])
    elif 'VZV' in base_target:
        # VZV family
        related_targets.extend(['QVZV', 'QVZVQ', 'QVZVQBL', 'QVZVQC'])
    elif 'ADV' in base_target:
        # ADV family
        related_targets.extend(['QADV', 'QADVQ', 'QADVQSE', 'QADVQPL', 'QADVQBL', 'QADVQRE', 'QADVQU'])
    elif 'HHV6' in base_target:
        # HHV6 family
        related_targets.extend(['QHHV6', 'QHHV6Q'])
    elif 'HSV' in base_target:
        # HSV family
        related_targets.extend(['QHSV', 'QHSVQ'])
    elif 'PARV' in base_target:
        # Parvo family
        related_targets.extend(['QPARV', 'QPARVOQ'])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_targets = []
    for t in related_targets:
        if t not in seen:
            seen.add(t)
            unique_targets.append(t)
    related_targets = unique_targets
    
    # Build target condition for SQL
    target_placeholders = ','.join(['?' for _ in related_targets])
    
    # First try to get primary controls for this run and target
    query = f"""
    SELECT 
        w.role_alias,
        w.sample_label,
        o.readings,
        m.mix_name,
        t.target_name,
        w.sample_name
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.run_id = ?
    AND t.target_name IN ({target_placeholders})
    AND w.role_alias IS NOT NULL
    AND w.role_alias != 'Patient'
    AND (
        w.role_alias LIKE '%NC' OR 
        w.role_alias LIKE '%PC' OR 
        w.role_alias LIKE '%HPC' OR 
        w.role_alias LIKE '%LPC' OR
        w.role_alias = 'NEGATIVE' OR
        w.role_alias = 'PC' OR
        w.role_alias = 'NC'
    )
    AND t.is_passive = 0
    ORDER BY 
        CASE WHEN m.mix_name = ? THEN 0 ELSE 1 END,  -- Prioritize same mix
        CASE WHEN t.target_name = ? THEN 0 ELSE 1 END,  -- Prioritize exact target match
        CASE 
            WHEN w.role_alias LIKE '%NC%' OR w.role_alias = 'NC' THEN 0  -- Negative controls first
            WHEN w.role_alias LIKE '%PC%' OR w.role_alias = 'PC' THEN 1  -- Then positive controls
            ELSE 2  -- Then others
        END,
        w.role_alias
    LIMIT ?
    """
    
    cursor.execute(query, (run_id, *related_targets, mix_name, target_name, limit * 3))  # Get more to ensure mix of controls
    results = cursor.fetchall()
    
    # Separate controls by same mix vs backup
    same_mix_controls = []
    backup_controls = []
    backup_map = get_backup_control_mapping()
    
    for row in results:
        role = row[0]
        label = row[1]
        readings_json = row[2]
        control_mix = row[3]
        control_target = row[4]
        sample_name = row[5]
        
        # Parse the readings
        if readings_json:
            try:
                readings = json.loads(readings_json)
                
                # Categorize control type based on role pattern
                role_upper = role.upper() if role else ''
                if 'NC' in role_upper or 'NEGATIVE' in role_upper or 'NTC' in role_upper:
                    ctrl_type = 'negative'
                elif 'PC' in role_upper or 'HPC' in role_upper or 'LPC' in role_upper or 'POSITIVE' in role_upper:
                    ctrl_type = 'positive'
                else:
                    ctrl_type = 'other'
                
                control_obj = {
                    'role': role,
                    'label': label,
                    'sample_name': sample_name,
                    'readings': readings,
                    'type': ctrl_type
                }
                
                if control_mix == mix_name:
                    # Same mix control - use directly
                    same_mix_controls.append(control_obj)
                elif control_mix.startswith(mix_name) or mix_name.startswith(control_mix):
                    # Related mix (e.g., BKVQ for BKV, or CMVQ2 for CMVQ2PL) - use as backup
                    backup_controls.append(control_obj)
                elif mix_name.replace('Q2', 'Q').replace('PL', '').replace('BL', '').replace('SE', '').replace('UR', '') == \
                     control_mix.replace('Q2', 'Q').replace('PL', '').replace('BL', '').replace('SE', '').replace('UR', ''):
                    # Same base mix with different variants (e.g., CMVQ2 and CMVQ2PL)
                    backup_controls.append(control_obj)
                elif mix_name in backup_map:
                    # Check if this control can serve as backup for our mix
                    # Need to check both exact match and pattern match
                    for control_type in ['PC', 'NC']:
                        # Check exact match first
                        if role in backup_map[mix_name][control_type]:
                            backup_controls.append(control_obj)
                            break
                        # Check if role matches any pattern (e.g., QBKNC matches pattern for NECB or QPECB)
                        # For BKV: QBKNC/QBKLPC/QBKHPC should match QPECB/NECB patterns
                        for backup_role in backup_map[mix_name][control_type]:
                            # Match patterns like QBK* for QPECB, or BK* for NECB
                            if 'QPECB' in backup_role and 'QBK' in role and 'PC' in role:
                                backup_controls.append(control_obj)
                                break
                            elif 'NECB' in backup_role and ('QBK' in role or 'BK' in role) and 'NC' in role:
                                backup_controls.append(control_obj)
                                break
                            elif 'QPECC' in backup_role and 'CMV' in role and 'PC' in role:
                                backup_controls.append(control_obj)
                                break
                            elif 'NECC' in backup_role and 'CMV' in role and 'NC' in role:
                                backup_controls.append(control_obj)
                                break
                            elif 'QPECE' in backup_role and 'EBV' in role and 'PC' in role:
                                backup_controls.append(control_obj)
                                break
                            elif 'NECE' in backup_role and 'EBV' in role and 'NC' in role:
                                backup_controls.append(control_obj)
                                break
                        if control_obj in backup_controls:
                            break
            except:
                pass
    
    # Prefer same-mix controls, fall back to backup controls
    # Try to get a balanced mix of positive and negative controls
    controls_to_use = same_mix_controls if same_mix_controls else backup_controls
    
    # Separate by type
    negative_controls = [c for c in controls_to_use if c.get('type') == 'negative']
    positive_controls = [c for c in controls_to_use if c.get('type') == 'positive']
    other_controls = [c for c in controls_to_use if c.get('type') == 'other']
    
    # Build balanced list - try to include at least 1 negative and some positive
    final_controls = []
    
    # Add negative controls first (at least 1 if available, up to 2)
    if negative_controls:
        final_controls.extend(negative_controls[:min(2, len(negative_controls))])
    
    # Add positive controls (remaining slots)
    remaining_slots = limit - len(final_controls)
    if positive_controls and remaining_slots > 0:
        final_controls.extend(positive_controls[:remaining_slots])
    
    # If still have slots, add other controls
    remaining_slots = limit - len(final_controls)
    if other_controls and remaining_slots > 0:
        final_controls.extend(other_controls[:remaining_slots])
    
    return final_controls

def get_control_curves_limited(conn, run_id, target_name, limit=5):
    """Get limited control curves for comparison (legacy function kept for compatibility)"""
    cursor = conn.cursor()
    
    # Controls are wells with control-like role_alias in the same run
    # Looking for patterns like *NC, *PC, *HPC, *LPC, NEGATIVE, etc.
    query = """
    SELECT 
        w.role_alias,
        w.sample_label,
        o.readings
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE w.run_id = ?
    AND t.target_name = ?
    AND w.role_alias IS NOT NULL
    AND w.role_alias != 'Patient'
    AND (
        w.role_alias LIKE '%NC' OR 
        w.role_alias LIKE '%PC' OR 
        w.role_alias LIKE '%HPC' OR 
        w.role_alias LIKE '%LPC' OR
        w.role_alias = 'NEGATIVE' OR
        w.role_alias = 'PC' OR
        w.role_alias = 'NC'
    )
    AND t.is_passive = 0
    LIMIT ?
    """
    
    cursor.execute(query, (run_id, target_name, limit))
    results = cursor.fetchall()
    
    controls = []
    for row in results:
        role = row[0]
        label = row[1]
        readings_json = row[2]
        
        if readings_json:
            try:
                readings = json.loads(readings_json)
                
                # Categorize control type based on role pattern
                role_upper = role.upper() if role else ''
                if 'NC' in role_upper or 'NEGATIVE' in role_upper or 'NTC' in role_upper:
                    ctrl_type = 'negative'
                elif 'PC' in role_upper or 'HPC' in role_upper or 'LPC' in role_upper or 'POSITIVE' in role_upper:
                    ctrl_type = 'positive'
                else:
                    ctrl_type = 'other'
                
                controls.append({
                    'role': role,
                    'label': label,
                    'readings': readings,
                    'type': ctrl_type
                })
            except:
                pass
    
    return controls

def get_well_comments(conn, well_id):
    """Get comments for a well, especially system-generated ones with resolution details"""
    cursor = conn.cursor()
    query = """
    SELECT 
        c.text,
        c.is_system_generated,
        c.created_at
    FROM comments c
    WHERE c.commentable_id = ?
    ORDER BY c.created_at DESC
    """
    cursor.execute(query, (well_id,))
    results = cursor.fetchall()
    
    comments = []
    for row in results:
        comments.append({
            'text': row[0],
            'is_system': row[1],
            'created_at': row[2]
        })
    
    return comments

def get_wells_comments_batch(conn, well_ids):
    """Get comments for multiple wells in a single query for efficiency"""
    if not well_ids:
        return {}
    
    cursor = conn.cursor()
    placeholders = ','.join(['?' for _ in well_ids])
    query = f"""
    SELECT 
        c.commentable_id,
        c.text,
        c.is_system_generated,
        c.created_at
    FROM comments c
    WHERE c.commentable_id IN ({placeholders})
    AND c.is_system_generated = 1
    ORDER BY c.commentable_id, c.created_at DESC
    """
    cursor.execute(query, well_ids)
    results = cursor.fetchall()
    
    # Group comments by well_id
    comments_by_well = {}
    for row in results:
        well_id = row[0]
        if well_id not in comments_by_well:
            comments_by_well[well_id] = []
        comments_by_well[well_id].append({
            'text': row[1],
            'is_system': row[2],
            'created_at': row[3]
        })
    
    return comments_by_well

def generate_svg_curve(readings, width=300, height=150, color="#2196F3", title=""):
    """Generate SVG curve for readings"""
    if not readings:
        return f'<svg width="{width}" height="{height}"><text x="{width//2}" y="{height//2}" text-anchor="middle" fill="#999">No data</text></svg>'
    
    # Filter valid readings
    valid_readings = [(i, r) for i, r in enumerate(readings) if r is not None]
    if len(valid_readings) < 2:
        return f'<svg width="{width}" height="{height}"><text x="{width//2}" y="{height//2}" text-anchor="middle" fill="#999">Insufficient data</text></svg>'
    
    indices = [i for i, _ in valid_readings]
    values = [v for _, v in valid_readings]
    
    min_val = min(values)
    max_val = max(values)
    if min_val == max_val:
        max_val = min_val + 1
    
    margin_top = 20 if title else 10
    margin = 10
    plot_width = width - 2 * margin
    plot_height = height - margin_top - margin
    
    # Generate path
    points = []
    for idx, val in valid_readings:
        x = margin + (idx * plot_width / (len(readings) - 1))
        y = margin_top + plot_height - ((val - min_val) / (max_val - min_val) * plot_height)
        points.append(f"{x:.1f},{y:.1f}")
    
    path = "M " + " L ".join(points)
    
    svg = f'''<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
        <rect width="{width}" height="{height}" fill="white"/>'''
    
    if title:
        svg += f'<text x="{width//2}" y="15" text-anchor="middle" font-size="11" fill="#666">{title}</text>'
    
    svg += f'''<rect x="{margin}" y="{margin_top}" width="{plot_width}" height="{plot_height}" 
              fill="none" stroke="#e0e0e0" stroke-width="1"/>
        <path d="{path}" fill="none" stroke="{color}" stroke-width="2"/>
    </svg>'''
    
    return svg

def get_all_control_errors(conn, limit=None, mix_filter=None):
    """Get all control wells with errors or resolutions"""
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    mix_clause = f"AND m.mix_name = '{mix_filter}'" if mix_filter else ""
    
    # Get unresolved control errors - controls with error_code_id and no resolution
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
    AND ec.error_type != 0  -- Exclude error_type 0
    -- Only control wells
    AND w.role_alias IS NOT NULL
    AND w.role_alias != 'Patient'
    AND (w.role_alias LIKE '%PC%' 
         OR w.role_alias LIKE '%NC%' 
         OR w.role_alias LIKE '%CONTROL%'
         OR w.role_alias LIKE '%NEGATIVE%'
         OR w.role_alias LIKE '%POSITIVE%'
         OR w.role_alias LIKE '%NTC%'
         OR w.role_alias LIKE '%PTC%')
    {mix_clause}
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """
    
    # Get resolved controls - those with resolution codes (could have error_code too)
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
    AND (ec.error_type IS NULL OR ec.error_type != 0)  -- Exclude error_type 0 (NULL OK for resolved without error_code)
    -- Only control wells  
    AND w.role_alias IS NOT NULL
    AND w.role_alias != 'Patient'
    AND (w.role_alias LIKE '%PC%' 
         OR w.role_alias LIKE '%NC%' 
         OR w.role_alias LIKE '%CONTROL%'
         OR w.role_alias LIKE '%NEGATIVE%'
         OR w.role_alias LIKE '%POSITIVE%'
         OR w.role_alias LIKE '%NTC%'
         OR w.role_alias LIKE '%PTC%')
    {mix_clause}
    ORDER BY m.mix_name, w.sample_name
    {limit_clause}
    """
    
    
    cursor = conn.cursor()
    
    all_errors = []
    
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
    
    print("  Fetching resolved errors...")
    cursor.execute(resolved_query)
    resolved = cursor.fetchall()
    error_ignored_list = []
    test_repeated_list = []
    for row in resolved:
        error = dict(row)
        # Categorize based on resolution code
        resolution_code = (error.get('error_code') or '').upper()  # resolution_codes is in error_code field
        
        # Check for repeat codes (RP, RX, TN) - if ANY of these appear ANYWHERE in the string
        # it's a repeat (which will have linked samples in appendix)
        if 'RP' in resolution_code or 'RX' in resolution_code or 'TN' in resolution_code:
            error['clinical_category'] = 'test_repeated'
            test_repeated_list.append(error)
        else:
            # All other resolution codes are ignored errors (no affected samples)
            error['clinical_category'] = 'error_ignored'
            error_ignored_list.append(error)
    all_errors.extend(error_ignored_list)
    all_errors.extend(test_repeated_list)
    print(f"    Found {len(resolved)} resolved errors ({len(error_ignored_list)} ignored, {len(test_repeated_list)} repeated)")
    
    
    return all_errors

def fetch_affected_samples(conn, control_well_ids):
    """Fetch patient samples affected by failed controls, grouped by run/mix"""
    # Note: control_well_ids is passed but we'll query ALL affected samples
    # to ensure we don't miss any
    
    cursor = conn.cursor()
    
    # QUERY 1: Get affected samples with INHERITED errors (ERROR section)
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
    -- Join to ALL failed controls on same run (will handle mix matching in app logic)
    JOIN wells cw ON cw.run_id = pw.run_id
    JOIN run_mixes crm ON cw.run_mix_id = crm.id
    JOIN mixes cm ON crm.mix_id = cm.id
    WHERE 
        -- Patient samples with INHERITED errors
        pw.error_code_id IN (
            '937829a3-a630-4a86-939d-c2b1ec229c9d',  -- INHERITED_CONTROL_FAILURE
            '937829a3-aa88-44cf-bbd5-deade616cff5',  -- INHERITED_EXTRACTION_FAILURE
            '995a530f-1da9-457d-9217-5afdac6ca59f',  -- INHERITED_CONTROL_FAILURE
            '995a530f-2239-4007-80f9-4102b5826ee5'   -- INHERITED_EXTRACTION_FAILURE
        )
        AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
        -- Only unresolved patient samples
        AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
        -- Control must be a failed control (don't filter by specific IDs)
        AND cw.role_alias IS NOT NULL
        AND cw.role_alias != 'Patient'
        AND (cw.error_code_id IS NOT NULL OR cw.resolution_codes IS NOT NULL)
    ORDER BY pr.run_name, cm.mix_name, cw.sample_name, pm.mix_name, pw.sample_name
    """
    
    cursor.execute(inherited_query)
    inherited_results = cursor.fetchall()
    
    # Debug: count unique inherited samples
    unique_inherited = set()
    for row in inherited_results:
        unique_inherited.add(row[0])  # well_id is first column
    print(f"  Found {len(inherited_results)} rows with {len(unique_inherited)} unique INHERITED affected samples")
    
    # QUERY 2: Get repeated samples with REAMP/REXCT/RPT/RXT/TNP status from control resolutions
    repeated_query = """
    SELECT DISTINCT
        pw.id as well_id,
        pw.sample_name,
        pw.well_number,
        'REPEATED' as error_code,
        'Sample repeated due to control resolution' as error_message,
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
    AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
    AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
    AND cw.role_alias IS NOT NULL
    AND cw.role_alias != 'Patient'
    AND cw.resolution_codes IS NOT NULL
    AND (cw.resolution_codes LIKE '%RP%' 
         OR cw.resolution_codes LIKE '%RX%' 
         OR cw.resolution_codes LIKE '%TN%')
    AND (
        cm.mix_name = pm.mix_name
        OR (pm.mix_name LIKE '%BKV%' AND cm.mix_name LIKE '%BKV%')
        OR (pm.mix_name LIKE '%CMV%' AND cm.mix_name LIKE '%CMV%')
        OR (pm.mix_name LIKE '%EBV%' AND cm.mix_name LIKE '%EBV%')
        OR (pm.mix_name LIKE '%ADV%' AND cm.mix_name LIKE '%ADV%')
        OR (pm.mix_name LIKE '%COVID%' AND cm.mix_name LIKE '%COVID%')
        OR (pm.mix_name LIKE '%HSV%' AND cm.mix_name LIKE '%HSV%')
        OR (pm.mix_name LIKE '%VZV%' AND cm.mix_name LIKE '%VZV%')
        OR (pm.mix_name LIKE '%HHV6%' AND cm.mix_name LIKE '%HHV6%')
        OR (pm.mix_name LIKE '%PARV%' AND cm.mix_name LIKE '%PARV%')
        OR (pm.mix_name LIKE '%MPX%' AND cm.mix_name LIKE '%MPX%')
        OR (pm.mix_name = 'RP' AND cm.mix_name = 'RP')
        OR SUBSTR(pm.mix_name, 1, 3) = SUBSTR(cm.mix_name, 1, 3)
    )
    ORDER BY pr.run_name, cm.mix_name, cw.sample_name, pm.mix_name, pw.sample_name
    """
    
    cursor.execute(repeated_query)
    repeated_results = cursor.fetchall()
    
    # Debug: count unique repeated samples
    unique_repeated = set()
    for row in repeated_results:
        unique_repeated.add(row[0])  # well_id is first column
    print(f"  Found {len(repeated_results)} rows with {len(unique_repeated)} unique REPEATED affected samples")
    
    # Combine both result sets
    all_results = inherited_results + repeated_results
    
    # Group by control set (run + control mix combination)
    grouped = {}
    for row in all_results:
        well_id = row[0]
        control_id = row[9]
        control_resolution = row[13]
        run_name = row[6]
        control_mix = row[12]
        
        # Create group key
        group_key = f"{run_name}_{control_mix}"
        
        if group_key not in grouped:
            grouped[group_key] = {
                'run_name': run_name,
                'control_mix': control_mix,
                'controls': {},
                'affected_samples_error': {},
                'affected_samples_repeat': {}
            }
        
        # Add control info
        if control_id not in grouped[group_key]['controls']:
            grouped[group_key]['controls'][control_id] = {
                'control_name': row[10],
                'control_well': row[11],
                'resolution': control_resolution
            }
        
        # Determine if this goes to Error or Repeat section
        # Samples with REAMP/REXCT/RPT/RXT/TNP status go to REPEATS
        # All others (INHERITED errors) go to ERROR
        lims_status = row[7]
        is_repeated_sample = lims_status in ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
        
        sample_data = {
            'well_id': well_id,
            'sample_name': row[1],
            'well_number': row[2],
            'error_code': row[3],
            'error_message': row[4],
            'mix_name': row[5],
            'run_name': run_name,
            'lims_status': lims_status,
            'resolution_codes': row[8]
        }
        
        if is_repeated_sample:
            grouped[group_key]['affected_samples_repeat'][well_id] = sample_data
        else:
            grouped[group_key]['affected_samples_error'][well_id] = sample_data
    
    return grouped

def generate_interactive_html(errors, conn, output_file, max_per_category=100):
    """Generate interactive HTML with JavaScript controls"""
    
    # Group by mix and clinical category
    mix_groups = defaultdict(lambda: defaultdict(list))
    for error in errors:
        clinical_cat = error.get('clinical_category', error['category'])
        mix_groups[error['mix_name']][clinical_cat].append(error)
    
    # Prepare affected samples data and control-to-group mapping
    print("Fetching affected samples...")
    control_well_ids = set()
    control_to_group_map = {}  # Map control well IDs to their appendix anchor
    
    # Only fetch affected samples for controls that were repeated (have RP/RX/TN in resolution)
    # Ignored errors don't have affected samples
    for error in errors:
        if error.get('well_id') and error.get('clinical_category') in ['unresolved', 'test_repeated']:
            control_well_ids.add(error['well_id'])
    
    affected_groups = {}
    if control_well_ids:
        affected_groups = fetch_affected_samples(conn, list(control_well_ids))
        
        # Build the control-to-group mapping
        for group_key, group_data in affected_groups.items():
            anchor_id = f"affected-group-{group_key}"
            for control_id in group_data['controls'].keys():
                control_to_group_map[control_id] = anchor_id
    
    # Start HTML
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Control Error Report - Interactive</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 10px;
            background-color: #f5f5f5;
        }
        
        .header {
            text-align: center;
            margin: 20px 0;
        }
        
        .stats {
            background: white;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            display: flex;
            justify-content: center;
            gap: 30px;
        }
        
        .stat-item {
            text-align: center;
        }
        
        .stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #2196F3;
        }
        
        .stat-label {
            font-size: 12px;
            color: #666;
            margin-top: 5px;
        }
        
        .mix-section {
            margin: 20px 0;
            border: 1px solid #ddd;
            border-radius: 5px;
            background: white;
        }
        
        .mix-header {
            font-size: 18px;
            font-weight: bold;
            padding: 15px;
            color: #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            background: #f8f9fa;
            border-bottom: 1px solid #ddd;
        }
        
        .mix-header:hover {
            background: #e9ecef;
        }
        
        .mix-content {
            padding: 15px;
            display: none;
        }
        
        .mix-section.expanded .mix-content {
            display: block;
        }
        
        .expand-icon {
            font-size: 16px;
            transition: transform 0.3s;
        }
        
        .mix-section.expanded .expand-icon {
            transform: rotate(90deg);
        }
        
        .category-tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
            border-bottom: 2px solid #e0e0e0;
        }
        
        .category-tab {
            padding: 8px 16px;
            cursor: pointer;
            border: none;
            background: none;
            font-size: 14px;
            color: #666;
            position: relative;
        }
        
        .category-tab.active {
            color: #2196F3;
            font-weight: bold;
        }
        
        .category-tab.active::after {
            content: '';
            position: absolute;
            bottom: -2px;
            left: 0;
            right: 0;
            height: 2px;
            background: #2196F3;
        }
        
        .category-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 10px;
            font-size: 11px;
            margin-left: 5px;
            background: #e0e0e0;
            color: #666;
        }
        
        .subcategory-tabs {
            display: flex;
            gap: 8px;
            margin: 10px 0;
            padding: 8px;
            background: #f8f8f8;
            border-radius: 4px;
            justify-content: center;
        }
        
        .subcategory-tab {
            padding: 6px 12px;
            cursor: pointer;
            border: 1px solid #ddd;
            background: white;
            font-size: 13px;
            color: #555;
            border-radius: 4px;
            transition: all 0.2s;
        }
        
        .subcategory-tab:hover {
            background: #f0f0f0;
        }
        
        .subcategory-tab.active {
            background: #2196F3;
            color: white;
            border-color: #1976D2;
        }
        
        .subcategory-badge {
            display: inline-block;
            padding: 1px 4px;
            border-radius: 8px;
            font-size: 11px;
            margin-left: 4px;
            background: rgba(255,255,255,0.5);
            color: #333;
        }
        
        /* Color coding for LIMS status results */
        .card.valid-detected {
            background: #c8e6c9 !important; /* Light green for DETECTED */
        }
        
        .card.valid-not-detected {
            background: #e1f5fe !important; /* Light blue for NOT DETECTED */
        }
        
        .card.excluded-reprocess {
            background: #fff9c4 !important; /* Light yellow for excluded/reprocess */
        }
        
        .card.other-status {
            background: #f5f5f5 !important; /* Light gray for other/null */
        }
        
        .container {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 15px;
        }
        
        .card {
            border: 1px solid #ddd;
            padding: 12px;
            border-radius: 5px;
            background: white;
        }
        
        .card.resolved {
            background: #e8f5e9;
        }
        
        .card.unresolved {
            background: #ffebee;
        }
        
        /* Color coding for resolved based on LIMS status */
        .card.resolved-detected {
            background: #c8e6c9; /* Green - DETECTED */
        }
        
        .card.resolved-not-detected {
            background: #e1f5fe; /* Blue - NOT DETECTED */
        }
        
        .card.resolved-excluded {
            background: #fff9c4; /* Yellow - Excluded/Reprocess */
        }
        
        .card.resolved-other {
            background: #f5f5f5; /* Gray - Other/NULL */
        }
        
        .card.resolved-with-new {
            background: #fff3e0;
        }
        
        .card.setup {
            background: #f3e5f5;
        }
        
        .card-header {
            font-weight: bold;
            margin-bottom: 10px;
            color: #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .target-selector {
            margin: 5px 0;
            font-size: 12px;
        }
        
        .target-select {
            padding: 2px 5px;
            font-size: 12px;
            border: 1px solid #ddd;
            border-radius: 3px;
        }
        
        .svg-container {
            margin: 10px 0;
            min-height: 150px;
        }
        
        .card-details {
            font-size: 12px;
            color: #666;
            margin-top: 10px;
        }
        
        .error-badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 11px;
            margin-top: 5px;
        }
        
        .error-badge.unresolved {
            background: #ff5252;
            color: white;
        }
        
        .error-badge.resolved {
            background: #4caf50;
            color: white;
        }
        
        .error-badge.resolved-with-new {
            background: #ff9800;
            color: white;
        }
        
        .error-badge.setup {
            background: #9c27b0;
            color: white;
        }
        
        .control-toggles {
            margin: 5px 0;
            font-size: 12px;
        }
        
        .control-toggles label {
            margin-right: 10px;
            cursor: pointer;
        }
        
        .control-toggles input[type="checkbox"] {
            margin-right: 3px;
        }
        
        svg {
            border: 1px solid #eee;
        }
    </style>
    <script>
        // Store all curve data
        const curveData = {};
        const recordTargets = {};
        let currentTargets = {};
        let controlsVisible = {}; // Track control visibility per well
        let sectionControlsVisible = {}; // Track control visibility per section
        
        // Expand/collapse functions
        function toggleSection(mixAnchor) {
            const section = document.getElementById('mix-' + mixAnchor);
            section.classList.toggle('expanded');
        }
        
        function expandAll() {
            document.querySelectorAll('.mix-section').forEach(section => {
                section.classList.add('expanded');
            });
        }
        
        function collapseAll() {
            document.querySelectorAll('.mix-section').forEach(section => {
                section.classList.remove('expanded');
            });
        }
        
        // Auto-expand section when navigating from TOC
        function navigateToSection(mixAnchor) {
            const section = document.getElementById('mix-' + mixAnchor);
            if (section && !section.classList.contains('expanded')) {
                section.classList.add('expanded');
            }
        }
        
        // Toggle controls for entire section
        function toggleControlsForSection(mixAnchor) {
            const section = document.getElementById('mix-' + mixAnchor);
            const isVisible = sectionControlsVisible[mixAnchor] !== false; // Default true
            sectionControlsVisible[mixAnchor] = !isVisible;
            
            // Update button text
            const btnText = document.getElementById('ctrl-text-' + mixAnchor);
            if (btnText) {
                btnText.textContent = sectionControlsVisible[mixAnchor] ? 'Hide Controls' : 'Show Controls';
            }
            
            // Update all wells in this section
            section.querySelectorAll('.svg-container[data-record-id]').forEach(container => {
                const wellId = container.getAttribute('data-record-id');
                if (wellId && curveData[wellId]) {
                    controlsVisible[wellId] = sectionControlsVisible[mixAnchor];
                    const targetName = currentTargets[wellId] || curveData[wellId].main_target;
                    if (targetName) {
                        container.innerHTML = generateSVGWithControls(wellId, sectionControlsVisible[mixAnchor]);
                    }
                }
            });
        }
        
        function generateSVG(readings, width=300, height=150, color='#2196F3', title='') {
            if (!readings || readings.length === 0) {
                return `<svg width="${width}" height="${height}"><text x="${width/2}" y="${height/2}" text-anchor="middle" fill="#999">No data</text></svg>`;
            }
            
            // Filter valid readings
            const validReadings = [];
            readings.forEach((val, idx) => {
                if (val !== null && val !== undefined) {
                    validReadings.push({idx: idx, val: val});
                }
            });
            
            if (validReadings.length < 2) {
                return `<svg width="${width}" height="${height}"><text x="${width/2}" y="${height/2}" text-anchor="middle" fill="#999">Insufficient data</text></svg>`;
            }
            
            const values = validReadings.map(d => d.val);
            const minVal = Math.min(...values);
            const maxVal = Math.max(...values);
            const range = maxVal - minVal || 1;
            
            const marginTop = title ? 20 : 10;
            const margin = 10;
            const plotWidth = width - 2 * margin;
            const plotHeight = height - marginTop - margin;
            
            // Generate path
            const points = validReadings.map(d => {
                const x = margin + (d.idx * plotWidth / (readings.length - 1));
                const y = marginTop + plotHeight - ((d.val - minVal) / range * plotHeight);
                return `${x.toFixed(1)},${y.toFixed(1)}`;
            });
            
            const path = 'M ' + points.join(' L ');
            
            let svg = `<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">`;
            svg += `<rect width="${width}" height="${height}" fill="white"/>`;
            
            if (title) {
                svg += `<text x="${width/2}" y="15" text-anchor="middle" font-size="11" fill="#666">${title}</text>`;
            }
            
            svg += `<rect x="${margin}" y="${marginTop}" width="${plotWidth}" height="${plotHeight}" fill="none" stroke="#e0e0e0" stroke-width="1"/>`;
            svg += `<path d="${path}" fill="none" stroke="${color}" stroke-width="2"/>`;
            svg += `</svg>`;
            
            return svg;
        }
        
        function updateTargetForRecord(wellId, targetName) {
            currentTargets[wellId] = targetName;
            const container = document.querySelector(`.svg-container[data-record-id="${wellId}"]`);
            if (container && curveData[wellId] && curveData[wellId].targets[targetName]) {
                // Find which section this well belongs to
                const section = container.closest('.mix-section');
                const mixAnchor = section ? section.id.replace('mix-', '') : null;
                const showControls = mixAnchor ? (sectionControlsVisible[mixAnchor] !== false) : true;
                
                // Update with controls based on section setting
                controlsVisible[wellId] = showControls;
                container.innerHTML = generateSVGWithControls(wellId, showControls);
            }
        }
        
        function showCategory(mixAnchor, category) {
            // Find the mix section by anchor
            const mixSection = document.getElementById(`mix-${mixAnchor}`);
            if (!mixSection) return;
            
            // Update tab styles
            const tabs = mixSection.querySelectorAll('.category-tab');
            tabs.forEach(tab => {
                if (tab.dataset.category === category) {
                    tab.classList.add('active');
                } else {
                    tab.classList.remove('active');
                }
            });
            
            // Show/hide content
            const contents = mixSection.querySelectorAll('.category-content');
            contents.forEach(content => {
                if (content.dataset.category === category) {
                    content.style.display = 'block';
                    
                    // If showing resolved, show 'all' subcategory by default
                    if (category === 'resolved') {
                        const allTab = content.querySelector('.subcategory-tab[data-subcategory="all"]');
                        if (allTab) {
                            showSubcategory(mixAnchor, 'all');
                        }
                    }
                } else {
                    content.style.display = 'none';
                }
            });
        }
        
        function generateSVGWithControls(wellId, showControls = true) {
            const data = curveData[wellId];
            if (!data || !data.targets) return '<svg width="300" height="150"><text x="150" y="75" text-anchor="middle" fill="#999">No data</text></svg>';
            
            const currentTarget = currentTargets[wellId] || data.main_target;
            const targetData = data.targets[currentTarget];
            if (!targetData || !targetData.readings) return '<svg width="300" height="150"><text x="150" y="75" text-anchor="middle" fill="#999">No target data</text></svg>';
            
            const width = 300;
            const height = 150;
            const marginLeft = 25;
            const marginRight = 15;
            const marginTop = 15;
            const marginBottom = 15;
            const plotWidth = width - marginLeft - marginRight;
            const plotHeight = height - marginTop - marginBottom;
            
            // Collect all readings for scaling
            let allReadings = [];
            
            // Add main curve readings
            const mainReadings = targetData.readings.filter(r => r !== null && r !== undefined);
            allReadings = allReadings.concat(mainReadings);
            
            // Add control readings if visible
            if (showControls && data.controls) {
                data.controls.forEach(ctrl => {
                    if (ctrl.readings) {
                        const validReadings = ctrl.readings.filter(r => r !== null && r !== undefined);
                        allReadings = allReadings.concat(validReadings);
                    }
                });
            }
            
            if (allReadings.length === 0) {
                return '<svg width="' + width + '" height="' + height + '"><text x="150" y="75" text-anchor="middle" fill="#999">No valid data</text></svg>';
            }
            
            // Calculate min/max with padding
            let minVal = Math.min(...allReadings);
            let maxVal = Math.max(...allReadings);
            let range = maxVal - minVal;
            
            // Add 5% padding
            if (range > 0) {
                const padding = range * 0.05;
                minVal -= padding;
                maxVal += padding;
                range = maxVal - minVal;
            } else {
                minVal -= 0.5;
                maxVal += 0.5;
                range = 1;
            }
            
            // Generate path function
            function generatePath(readings) {
                const validReadings = [];
                readings.forEach((reading, i) => {
                    if (reading !== null && reading !== undefined) {
                        validReadings.push({idx: i, val: reading});
                    }
                });
                
                if (validReadings.length === 0) return '';
                
                const points = validReadings.map(item => {
                    const x = marginLeft + (item.idx * plotWidth / (readings.length - 1 || 1));
                    const y = marginTop + plotHeight - ((item.val - minVal) / range * plotHeight);
                    return x.toFixed(1) + ',' + y.toFixed(1);
                });
                
                return 'M ' + points.join(' L ');
            }
            
            // Build SVG
            let svg = '<svg width="' + width + '" height="' + height + '">';
            
            // White background
            svg += '<rect width="' + width + '" height="' + height + '" fill="white"/>';
            
            // Plot area border
            svg += '<rect x="' + marginLeft + '" y="' + marginTop + '" width="' + plotWidth + '" height="' + plotHeight + '" fill="none" stroke="#ddd" stroke-width="1"/>';
            
            // Draw control curves if visible (underneath main curve)
            if (showControls && data.controls) {
                data.controls.forEach(ctrl => {
                    if (ctrl.readings) {
                        const path = generatePath(ctrl.readings);
                        if (path) {
                            if (ctrl.type === 'negative') {
                                // Red dotted line for negative controls
                                svg += '<path d="' + path + '" fill="none" stroke="red" stroke-width="1" stroke-dasharray="2,2" opacity="0.7"/>';
                            } else if (ctrl.type === 'positive') {
                                // Green dashed line for positive controls
                                svg += '<path d="' + path + '" fill="none" stroke="green" stroke-width="1" stroke-dasharray="5,3" opacity="0.7"/>';
                            } else {
                                // Grey for other controls
                                svg += '<path d="' + path + '" fill="none" stroke="#666" stroke-width="1" stroke-dasharray="4,4" opacity="0.6"/>';
                            }
                        }
                    }
                });
            }
            
            // Draw main curve on top
            const mainPath = generatePath(targetData.readings);
            if (mainPath) {
                svg += '<path d="' + mainPath + '" fill="none" stroke="#2196F3" stroke-width="2"/>';
            }
            
            // Y-axis labels
            svg += '<text x="' + (marginLeft-3) + '" y="' + (marginTop+5) + '" text-anchor="end" font-size="9" fill="#666">' + maxVal.toFixed(1) + '</text>';
            svg += '<text x="' + (marginLeft-3) + '" y="' + (height-marginBottom+3) + '" text-anchor="end" font-size="9" fill="#666">' + minVal.toFixed(1) + '</text>';
            
            svg += '</svg>';
            
            return svg;
        }
        
        
        // Function to auto-expand parent section when jumping to anchor
        function autoExpandForAnchor() {
            const hash = window.location.hash;
            if (hash && hash.startsWith('#affected-group-')) {
                // Find the anchor element
                const target = document.querySelector(hash);
                if (target) {
                    // Find parent mix-section
                    let parent = target.closest('.mix-section');
                    if (parent) {
                        // Extract section id
                        const sectionId = parent.id.replace('mix-', '');
                        // Expand the section if it's collapsed
                        const content = parent.querySelector('.mix-content');
                        const icon = parent.querySelector('.expand-icon');
                        if (content && content.style.display === 'none') {
                            content.style.display = 'block';
                            if (icon) icon.textContent = '▼';
                        }
                    }
                    // Scroll to the element after a short delay
                    setTimeout(() => {
                        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    }, 100);
                }
            }
        }
        
        // Initialize all SVGs on page load
        document.addEventListener('DOMContentLoaded', function() {
            // Initialize all section control states to true (showing controls)
            document.querySelectorAll('.mix-section').forEach(section => {
                const mixAnchor = section.id.replace('mix-', '');
                sectionControlsVisible[mixAnchor] = true;
            });
            
            // Generate SVGs with controls visible by default
            const containers = document.querySelectorAll('.svg-container[data-record-id]');
            containers.forEach(container => {
                const wellId = container.getAttribute('data-record-id');
                if (wellId && curveData[wellId]) {
                    controlsVisible[wellId] = true;
                    container.innerHTML = generateSVGWithControls(wellId, true);
                }
            });
            
            // Auto-expand for initial hash
            autoExpandForAnchor();
        });
        
        // Auto-expand on hash change
        window.addEventListener('hashchange', autoExpandForAnchor);
    </script>
</head>
<body>
    <div class="header">
        <h1>Control Error Report with Affected Samples</h1>
        <p>Generated: ''' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '''</p>
    </div>
    
    <div style="text-align: center; margin: 20px 0;">
        <button onclick="expandAll()" style="padding: 10px 20px; margin: 0 5px; background: #2196F3; color: white; border: none; border-radius: 5px; cursor: pointer;">Expand All</button>
        <button onclick="collapseAll()" style="padding: 10px 20px; margin: 0 5px; background: #2196F3; color: white; border: none; border-radius: 5px; cursor: pointer;">Collapse All</button>
    </div>
    
    <div class="stats">
        <div class="stat-item">
            <div class="stat-value">''' + str(len(errors)) + '''</div>
            <div class="stat-label">Total Errors</div>
        </div>
        <div class="stat-item">
            <div class="stat-value" style="color: #d32f2f;">''' + str(sum(1 for e in errors if e.get('clinical_category') == 'unresolved')) + '''</div>
            <div class="stat-label">Unresolved</div>
        </div>
        <div class="stat-item">
            <div class="stat-value" style="color: #388e3c;">''' + str(sum(1 for e in errors if e.get('clinical_category') == 'error_ignored')) + '''</div>
            <div class="stat-label">Error Ignored</div>
        </div>
        <div class="stat-item">
            <div class="stat-value" style="color: #f57c00;">''' + str(sum(1 for e in errors if e.get('clinical_category') == 'test_repeated')) + '''</div>
            <div class="stat-label">Test Repeated</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">''' + str(len(mix_groups)) + '''</div>
            <div class="stat-label">Affected Mixes</div>
        </div>
    </div>
    
    <!-- Curve Legend -->
    <div style="background: white; padding: 10px; margin: 15px auto; border-radius: 5px; max-width: 800px;">
        <div style="text-align: center; font-weight: bold; margin-bottom: 10px;">Curve Types</div>
        <div style="display: flex; justify-content: center; gap: 20px; flex-wrap: wrap; font-size: 12px;">
            <div style="display: flex; align-items: center; gap: 5px;">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="#2196F3" stroke-width="2"/></svg>
                <span>Main Curve (Blue solid)</span>
            </div>
            <div style="display: flex; align-items: center; gap: 5px;">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="red" stroke-width="1" stroke-dasharray="2,2"/></svg>
                <span>Negative Controls (Red dotted)</span>
            </div>
            <div style="display: flex; align-items: center; gap: 5px;">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="green" stroke-width="1" stroke-dasharray="5,3"/></svg>
                <span>Positive Controls (Green dashed)</span>
            </div>
        </div>
    </div>
    
    <!-- Table of Contents -->
    <div style="background: white; padding: 15px; margin: 20px 0; border-radius: 5px;">
        <h2 style="margin-top: 0;">Table of Contents</h2>
        <ul style="list-style: none; padding: 0;">'''
    
    # Add TOC entries with detailed counts
    for mix_name, categories in sorted(mix_groups.items()):
        total_mix_errors = sum(len(records) for records in categories.values())
        unresolved_count = len(categories.get('unresolved', []))
        ignored_count = len(categories.get('error_ignored', []))
        repeated_count = len(categories.get('test_repeated', []))
        
        mix_anchor = mix_name.replace(" ", "_").replace("/", "_")
        html += f'''
            <li style="margin: 8px 0;">
                <a href="#mix-{mix_anchor}" onclick="navigateToSection('{mix_anchor}')" style="text-decoration: none; color: #2196F3; display: block;">
                    <div style="display: flex; justify-content: space-between; align-items: center; position: relative;">
                        <span style="background: white; padding-right: 10px; z-index: 1; position: relative;">{mix_name}</span>
                        <div style="position: absolute; left: 0; right: 0; top: 50%; border-bottom: 1px dotted #ccc; z-index: 0;"></div>
                        <span style="font-size: 12px; color: #666; background: white; padding-left: 10px; z-index: 1; position: relative; white-space: nowrap;">
                            Total: {total_mix_errors} | 
                            <span style="color: #d32f2f;">Unresolved: {unresolved_count}</span> | 
                            <span style="color: #388e3c;">Ignored: {ignored_count}</span> | 
                            <span style="color: #f57c00;">Repeated: {repeated_count}</span>
                        </span>
                    </div>
                </a>
            </li>'''
    
    # Calculate appendix totals - count unique samples across all groups
    error_samples_total = 0
    repeat_samples_total = 0
    unique_error_samples = set()
    unique_repeat_samples = set()
    
    if affected_groups:
        for group_data in affected_groups.values():
            unique_error_samples.update(group_data.get('affected_samples_error', {}).keys())
            unique_repeat_samples.update(group_data.get('affected_samples_repeat', {}).keys())
    
    error_samples_total = len(unique_error_samples)
    repeat_samples_total = len(unique_repeat_samples)
    
    html += f'''
            <li style="margin: 15px 0; padding-top: 15px; border-top: 2px solid #e0e0e0;">
                <a href="#appendix" style="text-decoration: none; color: #2196F3; font-weight: bold; display: block;">
                    APPENDIX: Affected Patient Samples
                </a>
                <ul style="list-style: none; padding-left: 20px; margin-top: 10px;">
                    <li style="margin: 5px 0;">
                        <a href="#mix-appendix-error" onclick="navigateToSection('appendix-error')" style="text-decoration: none; color: #2196F3;">
                            → ERROR - Active Failed Samples ({error_samples_total})
                        </a>
                    </li>
                    <li style="margin: 5px 0;">
                        <a href="#mix-appendix-repeats" onclick="navigateToSection('appendix-repeats')" style="text-decoration: none; color: #2196F3;">
                            → REPEATS - Resolved Samples ({repeat_samples_total})
                        </a>
                    </li>
                </ul>
            </li>
        </ul>
    </div>
'''
    
    # Process each mix
    mix_id = 0
    for mix_name, categories in sorted(mix_groups.items()):
        mix_id += 1
        total_mix_errors = sum(len(records) for records in categories.values())
        
        mix_anchor = mix_name.replace(" ", "_").replace("/", "_")
        html += f'''
        <div class="mix-section{' expanded' if mix_id == 1 else ''}" id="mix-{mix_anchor}">
            <div class="mix-header">
                <div style="flex: 1; display: flex; align-items: center; gap: 15px;">
                    <div style="cursor: pointer; flex: 1;" onclick="toggleSection('{mix_anchor}')">
                        <span>Mix: {mix_name}</span>
                        <span style="font-size: 14px; color: #666; margin-left: 10px;">Total errors: {total_mix_errors}</span>
                    </div>
                    <button onclick="toggleControlsForSection('{mix_anchor}')" style="padding: 4px 10px; font-size: 12px; background: #f0f0f0; border: 1px solid #ddd; border-radius: 3px; cursor: pointer;">
                        <span id="ctrl-text-{mix_anchor}">Hide Controls</span>
                    </button>
                </div>
                <span class="expand-icon" style="cursor: pointer;" onclick="toggleSection('{mix_anchor}')">▶</span>
            </div>
            <div class="mix-content">
            
            <div class="category-tabs">
        '''
        
        # Add category tabs for clinical categories
        first_category = True
        clinical_categories = [
            ('unresolved', 'Unresolved'),
            ('error_ignored', 'Error Ignored'),
            ('test_repeated', 'Test Repeated')
        ]
        
        for cat_key, cat_label in clinical_categories:
            if cat_key in categories:
                count = len(categories[cat_key])
                active_class = 'active' if first_category else ''
                html += f'''
                <button class="category-tab {active_class}" data-category="{cat_key}" 
                        onclick="showCategory('{mix_anchor}', '{cat_key}')">
                    {cat_label}
                    <span class="category-badge">{count}</span>
                </button>
                '''
                first_category = False
        
        html += '''
            </div>
        '''
        
        # Add category content
        first_category = True
        for category, records in categories.items():
            display = 'block' if first_category else 'none'
            showing_text = f" (showing {min(len(records), max_per_category)} of {len(records)})" if len(records) > max_per_category else ""
            
            category_labels = {
                'unresolved': 'Unresolved Errors',
                'error_ignored': 'Error Ignored (Valid Results)',
                'test_repeated': 'Test Repeated'
            }
            category_label = category_labels.get(category, category.replace('_', ' ').title())
            
            html += f'''
            <div class="category-content" data-category="{category}" style="display: {display};">
                <div style="text-align: center; color: #666; font-size: 12px; margin: 5px 0;">
                    {category_label}{showing_text}
                </div>'''
            
            html += '''
                <div class="container">
            '''
            
            # Process records (limited for performance if needed)
            # Sort for visual grouping
            if category == 'unresolved':
                # Sort by error code for unresolved
                records = sorted(records, key=lambda r: (r.get('error_code', ''), r.get('sample_name', '')))
            elif category in ['error_ignored', 'test_repeated']:
                # Sort by LIMS export status for resolved (handle None values)
                records = sorted(records, key=lambda r: (r.get('lims_status') or '', r.get('sample_name', '')))
            
            records_to_show = records[:max_per_category] if max_per_category > 0 else records
            
            # Pre-fetch all comments for this batch of wells for efficiency
            well_ids_to_fetch = [r['well_id'] for r in records_to_show]
            comments_batch = get_wells_comments_batch(conn, well_ids_to_fetch)
            
            for record in records_to_show:
                well_id = record['well_id']
                
                # Get well data with all targets
                well_data = get_well_data_with_targets(conn, well_id)
                
                if well_data:
                    main_target = well_data.get('main_target')
                    if not main_target and well_data['targets']:
                        # If no main target (all IC), use first target
                        main_target = list(well_data['targets'].keys())[0]
                    
                    if main_target:
                        target_data = well_data['targets'][main_target]
                        
                        # Get limited control curves (with backup support)
                        mix_name = record.get('mix_name', '')
                        controls = get_control_curves_with_backup(conn, record['run_id'], main_target, mix_name, 3)
                    
                        # Prepare JavaScript data
                        js_data = {
                            'main_target': main_target,
                            'targets': well_data['targets'],
                            'controls': controls
                        }
                        
                        # SVG will be generated on page load by JavaScript
                        
                        # Build target selector with proper sorting:
                        # 1. Pathogen targets first (alphabetically if multiple)
                        # 2. Internal control targets last
                        pathogen_targets = sorted([t for t in well_data['targets'].keys() 
                                                  if not well_data['targets'][t]['is_ic']])
                        ic_targets = sorted([t for t in well_data['targets'].keys() 
                                           if well_data['targets'][t]['is_ic']])
                        target_options = pathogen_targets + ic_targets
                        
                        html += f'''
                        <script>
                        curveData["{well_id}"] = {json.dumps(js_data)};
                        currentTargets["{well_id}"] = "{main_target}";
                        </script>
                        '''
                    
                        # Determine card color class based on clinical category and LIMS status
                        card_class = category
                        if category == 'error_ignored':
                            lims_status = record.get('lims_status', '')
                            if lims_status == 'DETECTED':
                                card_class = 'resolved-detected'
                            elif lims_status == 'NOT DETECTED':
                                card_class = 'resolved-not-detected'
                            else:
                                card_class = 'resolved'
                        elif category == 'test_repeated':
                            card_class = 'resolved-excluded'
                        
                        html += f'''
                        <div class="card {card_class}">
                            <div class="card-header">
                                <span>{record['sample_name']} - Well {record['well_number']}</span>
                            </div>
                        '''
                        
                        # Combined target selector and control toggle
                        html += f'''
                        <div class="target-selector" style="display: flex; align-items: center; gap: 10px; margin: 5px 0;">
                        '''
                    
                        if len(target_options) > 1:
                            html += f'''
                            <div style="flex: 1;">
                                Target: 
                                <select class="target-select" onchange="updateTargetForRecord('{well_id}', this.value)">
                            '''
                            for target in target_options:
                                selected = 'selected' if target == main_target else ''
                                ct_val = well_data['targets'][target]['ct']
                                ct_str = f" (CT: {ct_val:.1f})" if ct_val else ""
                                html += f'<option value="{target}" {selected}>{target}{ct_str}</option>'
                            html += '''
                                </select>
                            </div>
                            '''
                        else:
                            html += f'<div style="flex: 1;">Target: {main_target}</div>'
                    
                        html += '''
                        </div>
                        '''
                        
                        html += f'''
                            <div class="svg-container" data-record-id="{well_id}">
                                <!-- SVG will be generated by JavaScript -->
                            </div>
                            <div class="card-details">
                                Run: {record['run_name']}<br>
                                Error: {record.get('error_message', record['error_code'])}'''
                        
                        # Add link to affected samples if this control has affected samples
                        if well_id in control_to_group_map:
                            anchor = control_to_group_map[well_id]
                            html += f'''<br>
                                <a href="#{anchor}" style="color: #2196F3; text-decoration: none; font-size: 11px;">→ View Affected Samples</a>'''
                        
                        if category in ['error_ignored', 'test_repeated'] and record.get('lims_status'):
                            html += f'<br>LIMS: <strong>{record["lims_status"]}</strong>'
                        
                        # Display resolution code with message for resolved items
                        if category in ['error_ignored', 'test_repeated']:
                            resolution_code = record.get('error_code', '')
                            resolution_message = get_resolution_message(resolution_code)
                            html += f'''
                                <div style="margin-top: 5px;">
                                    <span style="color: #666; font-size: 11px; font-weight: bold;">User Resolution: </span>
                                    <span style="color: #666; font-size: 11px;">{resolution_message}</span>
                                    <div class="error-badge {category}">{resolution_code}</div>
                                </div>'''
                            
                            # Add comments for all resolved wells (both ignored and repeated)
                            if category in ['error_ignored', 'test_repeated']:
                                comments = comments_batch.get(well_id, [])
                                if comments:
                                    html += '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #e0e0e0;">'
                                    html += '<div style="font-size: 11px; color: #666; font-weight: bold; margin-bottom: 4px;">Resolution Details:</div>'
                                    for comment in comments[:2]:  # Show max 2 most recent
                                        # Format the comment text for display - handle both escaped and actual newlines
                                        comment_text = comment['text'].replace('\\n', '<br>').replace('\n', '<br>')
                                        html += f'<div style="font-size: 10px; color: #666; background: #f5f5f5; padding: 4px 6px; margin: 4px 0; border-radius: 3px;">{comment_text}</div>'
                                    html += '</div>'
                        else:
                            html += f'''
                                <div class="error-badge {category}">{record['error_code']}</div>'''
                            
                            # Add comments for unresolved wells too (if any exist)
                            if category == 'unresolved':
                                comments = comments_batch.get(well_id, [])
                                if comments:
                                    html += '<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #e0e0e0;">'
                                    html += '<div style="font-size: 11px; color: #666; font-weight: bold; margin-bottom: 4px;">Comments:</div>'
                                    for comment in comments[:2]:  # Show max 2 most recent
                                        # Format the comment text for display - handle both escaped and actual newlines
                                        comment_text = comment['text'].replace('\\n', '<br>').replace('\n', '<br>')
                                        html += f'<div style="font-size: 10px; color: #666; background: #f5f5f5; padding: 4px 6px; margin: 4px 0; border-radius: 3px;">{comment_text}</div>'
                                    html += '</div>'
                        
                        html += '''
                            </div>
                        </div>
                        '''
                else:
                    # No data available
                    
                    html += f'''
                    <div class="card {category}">
                        <div class="card-header">
                            <span>{record['sample_name']} - Well {record['well_number']}</span>
                        </div>
                        <div class="svg-container" data-record-id="{well_id}-nodata">
                            <svg width="300" height="150"><text x="150" y="75" text-anchor="middle" fill="#999">No data available</text></svg>
                        </div>
                        <div class="card-details">
                            Run: {record['run_name']}<br>
                            Error: {record.get('error_message', record['error_code'])}'''
                    
                    if category == 'resolved' and record.get('lims_status'):
                        html += f'<br>LIMS: <strong>{record["lims_status"]}</strong>'
                    
                    html += f'''
                            <div class="error-badge {category}">{record['error_code']}</div>
                        </div>
                    </div>
                    '''
            
            html += '''
                </div>
            </div>
            '''
            first_category = False
        
        html += '''
            </div>
        </div>
        '''
    
    # Add APPENDIX for affected samples
    if affected_groups:
            html += '''
        <div style="margin-top: 40px; padding: 20px 0; border-top: 3px solid #2196F3;">
            <h2 id="appendix" style="text-align: center; color: #1976D2;">APPENDIX: Affected Patient Samples</h2>
            <p style="text-align: center; color: #666;">Patient samples that inherited errors from failed controls</p>
        </div>
        '''
            
            # Collect Error groups (those without repeat resolutions)
            error_groups = {}
            
            for group_key, group_data in affected_groups.items():
                if group_data['affected_samples_error']:
                    # Format control info
                    control_info = ', '.join([f"{ctrl['control_name']}-Well {ctrl['control_well']}" 
                                            for ctrl in group_data['controls'].values()])
                    
                    error_groups[group_key] = {
                        'run_name': group_data['run_name'],
                        'mix_name': group_data['control_mix'],
                        'control_info': control_info,
                        'control_ids': list(group_data['controls'].keys()),
                        'samples': list(group_data['affected_samples_error'].values())
                    }
            
            if error_groups:
                # Count unique samples across all groups
                unique_error_wells = set()
                for g in error_groups.values():
                    unique_error_wells.update(s['well_id'] for s in g['samples'])
                total_error_samples = len(unique_error_wells)
                html += f'''
        <div class="mix-section" id="mix-appendix-error">
            <div class="mix-header">
                <div style="flex: 1; display: flex; align-items: center; gap: 15px;">
                    <div style="cursor: pointer; flex: 1;" onclick="toggleSection('appendix-error')">
                        <span>ERROR - Active Failed Samples</span>
                        <span style="font-size: 14px; color: #666; margin-left: 10px;">Total samples: {total_error_samples}</span>
                    </div>
                </div>
                <span class="expand-icon" style="cursor: pointer;" onclick="toggleSection('appendix-error')">▶</span>
            </div>
            <div class="mix-content">
        '''
                
                for group_key, group_data in sorted(error_groups.items()):
                    samples = group_data['samples']  # Show ALL samples
                    control_info = group_data['control_info']
                    
                    # Create anchor ID from group key
                    anchor_id = f"affected-group-{group_key}"
                    
                    html += f'''
                <div id="{anchor_id}" style="margin: 20px 0; padding: 15px; background: #fff3e0; border-left: 4px solid #ff9800;">
                    <h4 style="margin: 0 0 10px 0;">Failed Control(s): {control_info}</h4>
                    <div style="color: #666; font-size: 12px; margin-bottom: 10px;">
                        Run: {group_data['run_name']} | Mix: {group_data['mix_name']} | Samples: {len(samples)}
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px;">
        '''
                    
                    for sample in samples:
                        lims_status = sample.get('lims_status', 'UNKNOWN')
                        status_color = '#d32f2f' if lims_status in ['REAMP', 'REXCT', 'TNP', 'EXCLUDE'] else '#666'
                        
                        html += f'''
                        <div style="padding: 8px; background: white; border: 1px solid #ddd; border-radius: 4px;">
                            <div style="font-weight: bold; font-size: 12px;">{sample['sample_name']}</div>
                            <div style="font-size: 11px; color: #666;">Well: {sample['well_number']}</div>
                            <div style="font-size: 11px; color: {status_color};">Status: {lims_status}</div>
                        </div>
        '''
                    
                    html += '''
                    </div>
                </div>
        '''
                
                html += '''
            </div>
        </div>
        '''
            
            # Collect Repeat groups (those with repeat resolutions)
            repeat_groups = {}
            for group_key, group_data in affected_groups.items():
                if group_data['affected_samples_repeat']:
                    # Format control info
                    control_info = ', '.join([f"{ctrl['control_name']}-Well {ctrl['control_well']}" 
                                            for ctrl in group_data['controls'].values()])
                    
                    repeat_groups[group_key] = {
                        'run_name': group_data['run_name'],
                        'mix_name': group_data['control_mix'],
                        'control_info': control_info,
                        'control_ids': list(group_data['controls'].keys()),
                        'samples': list(group_data['affected_samples_repeat'].values())
                    }
            
            if repeat_groups:
                # Count unique samples across all groups
                unique_repeat_wells = set()
                for g in repeat_groups.values():
                    unique_repeat_wells.update(s['well_id'] for s in g['samples'])
                total_repeat_samples = len(unique_repeat_wells)
                html += f'''
        <div class="mix-section" id="mix-appendix-repeats">
            <div class="mix-header">
                <div style="flex: 1; display: flex; align-items: center; gap: 15px;">
                    <div style="cursor: pointer; flex: 1;" onclick="toggleSection('appendix-repeats')">
                        <span>REPEATS - Resolved Samples</span>
                        <span style="font-size: 14px; color: #666; margin-left: 10px;">Total samples: {total_repeat_samples}</span>
                    </div>
                </div>
                <span class="expand-icon" style="cursor: pointer;" onclick="toggleSection('appendix-repeats')">▶</span>
            </div>
            <div class="mix-content">
        '''
                
                for group_key, group_data in sorted(repeat_groups.items()):
                    samples = group_data['samples']  # Show ALL samples
                    control_info = group_data['control_info']
                    
                    # Create anchor ID from group key
                    anchor_id = f"affected-group-{group_key}"
                    
                    html += f'''
                <div id="{anchor_id}" style="margin: 20px 0; padding: 15px; background: #fff3e0; border-left: 4px solid #ff9800;">
                    <h4 style="margin: 0 0 10px 0;">Resolved Control(s): {control_info}</h4>
                    <div style="color: #666; font-size: 12px; margin-bottom: 10px;">
                        Run: {group_data['run_name']} | Mix: {group_data['mix_name']} | Samples: {len(samples)}
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px;">
        '''
                    
                    for sample in samples:
                        lims_status = sample.get('lims_status', 'UNKNOWN')
                        status_color = '#f57c00' if lims_status in ['REAMP', 'REXCT', 'TNP', 'EXCLUDE'] else '#666'
                        
                        html += f'''
                        <div style="padding: 8px; background: white; border: 1px solid #ddd; border-radius: 4px;">
                            <div style="font-weight: bold; font-size: 12px;">{sample['sample_name']}</div>
                            <div style="font-size: 11px; color: #666;">Well: {sample['well_number']}</div>
                            <div style="font-size: 11px; color: {status_color};">Status: {lims_status}</div>
                        </div>
        '''
                    
                    html += '''
                    </div>
                </div>
        '''
                
                html += '''
            </div>
        </div>
        '''
    
    html += '''
</body>
</html>
'''
    
    # Save report
    with open(output_file, 'w') as f:
        f.write(html)
    
    return len(errors)

def main():
    parser = argparse.ArgumentParser(description='Generate interactive error analysis report')
    parser.add_argument('--quest-db', default='input_data/quest_prod_aug2025.db',
                       help='Path to Quest database')
    parser.add_argument('--output', default='output_data/control_report_working.html',
                       help='Output HTML file path')
    parser.add_argument('--limit', type=int, help='Limit total number of records to process')
    parser.add_argument('--max-per-category', type=int, default=100,
                       help='Maximum records to show per category (default: 100)')
    parser.add_argument('--mix-filter', type=str, 
                       help='Filter to specific mix name (e.g., BKVQ)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.quest_db):
        print(f"Error: Database not found at {args.quest_db}")
        return
    
    print(f"Connecting to database: {args.quest_db}")
    conn = sqlite3.connect(args.quest_db)
    conn.row_factory = sqlite3.Row
    
    print("\nFetching error data...")
    if args.mix_filter:
        print(f"Filtering to mix: {args.mix_filter}")
    all_errors = get_all_control_errors(conn, args.limit, args.mix_filter)
    
    if not all_errors:
        print("No errors found matching criteria")
        conn.close()
        return
    
    print(f"\nTotal errors found: {len(all_errors)}")
    
    print("\nGenerating interactive HTML report...")
    print(f"Maximum records per category: {args.max_per_category}")
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    total = generate_interactive_html(all_errors, conn, args.output, args.max_per_category)
    
    print(f"\nReport generated successfully:")
    print(f"  Output file: {args.output}")
    print(f"  Total errors: {total}")
    
    conn.close()

if __name__ == '__main__':
    main()