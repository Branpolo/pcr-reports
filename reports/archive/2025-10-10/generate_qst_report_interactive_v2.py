#!/usr/bin/env python3
"""
Generate interactive HTML report for QST discrepancies with controls and other observations
Version 2: Dynamic rescaling based on visible curves
"""

import sqlite3
import argparse
import os
from datetime import datetime
import sys
import json
import base64
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.database import bytes_to_float

def categorize_record(row):
    """Categorize record based on classification discrepancies and assign color/section
    
    Returns: (category_name, color_code, section, priority_in_section)
    Sections: 1=acted upon, 2=repeated, 3=ignored
    """
    machine_cls = row['machine_cls']
    final_cls = row['final_cls']
    lims_status = row['lims_status']
    error_code = row['error_code']
    
    # Check suppression condition first
    if not lims_status and not error_code:
        return ('suppressed', None, 0, 999)  # Will be filtered out
    
    # Section 1: Discrepancies Acted Upon (machine != final AND LIMS is DETECTED/NOT DETECTED)
    if machine_cls != final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
        if final_cls == 1:
            return ('discrepancy_positive', '#90EE90', 1, 1)  # Green - False Negative corrected
        else:
            return ('discrepancy_negative', '#FF6B6B', 1, 2)  # Red - False Positive corrected
    
    # Section 2: Samples Repeated (error codes OR LIMS other)
    if error_code:
        return ('has_error', '#FFB6C1', 2, 2)  # Pink - Has error codes
    if lims_status and lims_status not in ('DETECTED', 'NOT DETECTED'):
        return ('lims_other', '#FFD700', 2, 1)  # Yellow - LIMS other status
    
    # Section 3: Discrepancies Ignored (machine = final AND LIMS is DETECTED/NOT DETECTED)
    if machine_cls == final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
        if lims_status == 'DETECTED':
            return ('agreement_detected', '#E8F5E9', 3, 1)  # Pale green
        else:
            return ('agreement_not_detected', '#FCE4EC', 3, 2)  # Pale pink
    
    # Fallback for edge cases
    return ('other', '#F0F0F0', 4, 1)  # Light gray

def format_resolution_code(resolution_code, machine_cls, final_cls, lims_status=None):
    """Convert resolution codes to human-readable text"""
    if not resolution_code:
        return 'None'
    
    res_upper = resolution_code.upper()
    
    # Check for BLA (baseline)
    if 'BLA' in res_upper:
        if machine_cls != final_cls:
            return 'Change result'
        else:
            return 'Keep original result'
    
    # Check for SETPOS/SETNEG (like BLA)
    if 'SETPOS' in res_upper or 'SETNEG' in res_upper:
        if machine_cls != final_cls:
            return 'Change result'
        else:
            return 'Keep original result'
    
    # Check for SKIP patterns
    if 'SKIP' in res_upper:
        # SKIP with pipe - check if LIMS is DETECTED/NOT DETECTED
        if '|' in res_upper:
            # If LIMS is DETECTED or NOT DETECTED, it's not actually a repeat
            if lims_status in ('DETECTED', 'NOT DETECTED'):
                return resolution_code[:15] + '...' if len(resolution_code) > 15 else resolution_code
            else:
                return 'Repeat'
        # SKIP without pipe - simple repeat
        else:
            return 'Repeat'
    
    # Return original if no match (for analysis)
    # Truncate if too long
    if len(resolution_code) > 15:
        return resolution_code[:15] + '...'
    return resolution_code

def get_controls_for_record(conn, run_id, target_name):
    """Get control curves for a specific run-target combination"""
    cursor = conn.cursor()
    query = """
    SELECT role_alias, control_label, 
           readings0, readings1, readings2, readings3, readings4, readings5, readings6, readings7, readings8, readings9,
           readings10, readings11, readings12, readings13, readings14, readings15, readings16, readings17, readings18, readings19,
           readings20, readings21, readings22, readings23, readings24, readings25, readings26, readings27, readings28, readings29,
           readings30, readings31, readings32, readings33, readings34, readings35, readings36, readings37, readings38, readings39,
           readings40, readings41, readings42, readings43, readings44, readings45, readings46, readings47, readings48, readings49
    FROM qst_controls
    WHERE run_id = ? AND target_name = ?
    """
    cursor.execute(query, (run_id, target_name))
    
    controls = []
    for row in cursor.fetchall():
        role = row[0]
        label = row[1]
        readings = [row[i+2] for i in range(50) if row[i+2] is not None]
        
        # Categorize control type
        role_upper = role.upper() if role else ''
        if 'NC' in role_upper or 'NEGATIVE' in role_upper or 'BLANK' in role_upper:
            control_type = 'negative'
        elif 'PC' in role_upper or 'HPC' in role_upper or 'LPC' in role_upper or 'QUANT' in role_upper:
            control_type = 'positive'
        else:
            control_type = 'other'
        
        controls.append({
            'role': role,
            'label': label,
            'readings': readings,
            'type': control_type
        })
    
    return controls

def get_other_observations_for_record(conn, disc_id, sample_label):
    """Get other observation curves for the same well"""
    cursor = conn.cursor()
    query = """
    SELECT target_name,
           readings0, readings1, readings2, readings3, readings4, readings5, readings6, readings7, readings8, readings9,
           readings10, readings11, readings12, readings13, readings14, readings15, readings16, readings17, readings18, readings19,
           readings20, readings21, readings22, readings23, readings24, readings25, readings26, readings27, readings28, readings29,
           readings30, readings31, readings32, readings33, readings34, readings35, readings36, readings37, readings38, readings39,
           readings40, readings41, readings42, readings43, readings44, readings45, readings46, readings47, readings48, readings49
    FROM qst_other_observations
    WHERE discrepancy_obs_id = ?
    """
    cursor.execute(query, (disc_id,))
    
    observations = []
    for row in cursor.fetchall():
        target = row[0]
        readings = [row[i+1] for i in range(50) if row[i+1] is not None]
        
        # Categorize observation type
        target_upper = target.upper() if target else ''
        if 'IPC' in target_upper or target_upper.endswith('IC'):
            obs_type = 'ic'
        else:
            obs_type = 'other'
        
        observations.append({
            'target': target,
            'readings': readings,
            'type': obs_type
        })
    
    return observations

def prepare_curve_data_json(main_readings, controls=None, other_obs=None):
    """Prepare all curve data as JSON for JavaScript processing"""
    data = {
        'main': main_readings,
        'controls': [],
        'other': []
    }
    
    if controls:
        for ctrl in controls:
            data['controls'].append({
                'type': ctrl['type'],
                'role': ctrl['role'],
                'readings': ctrl['readings']
            })
    
    if other_obs:
        for obs in other_obs:
            data['other'].append({
                'type': obs['type'],
                'target': obs['target'],
                'readings': obs['readings']
            })
    
    return json.dumps(data)

def generate_html_report(db_path, output_path, limit=None):
    """Generate interactive HTML report with dynamic rescaling"""
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Build query with sorting - exclude IPC/QIPC targets, COVID-IPC mixes, and BLA resolution codes where machine=final
    query = """
    SELECT * FROM qst_readings 
    WHERE in_use = 1
    AND UPPER(target_name) NOT LIKE '%IPC%'
    AND UPPER(target_name) NOT LIKE '%QIPC%'
    AND UPPER(mix_name) NOT LIKE '%IPC%'
    AND NOT (machine_cls = final_cls AND UPPER(resolution_codes) LIKE '%BLA|%')
    ORDER BY mix_name, target_name, id
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    all_records = cursor.fetchall()
    
    # Categorize records into sections
    sections = {
        1: {'name': 'Discrepancies Acted Upon', 'desc': 'Results where the discrepancy was detected and the exported result was changed', 'records': []},
        2: {'name': 'Samples Repeated', 'desc': 'Results where the sample was repeated due to errors or unusual LIMS status', 'records': []},
        3: {'name': 'Discrepancies Ignored', 'desc': 'Results where a discrepancy was detected but the original result was kept', 'records': []},
        4: {'name': 'Other', 'desc': 'Edge cases not fitting other categories', 'records': []}
    }
    
    stats = {
        'total': 0,
        'suppressed': 0,
        'discrepancy_positive': 0,
        'discrepancy_negative': 0,
        'lims_other': 0,
        'has_error': 0,
        'agreement_detected': 0,
        'agreement_not_detected': 0,
        'other': 0
    }
    
    for row in all_records:
        category, color, section, priority = categorize_record(row)
        stats[category] += 1
        stats['total'] += 1
        
        if category != 'suppressed':
            # Get readings
            readings = []
            for i in range(50):
                val = row[f'readings{i}']
                if val is not None:
                    readings.append(bytes_to_float(val) if isinstance(val, bytes) else val)
            
            # Get controls and other observations
            controls = get_controls_for_record(conn, row['run_id'], row['target_name'])
            other_obs = get_other_observations_for_record(conn, row['id'], row['sample_label'])
            
            sections[section]['records'].append({
                'row': row,
                'category': category,
                'color': color,
                'priority': priority,
                'readings': readings,
                'controls': controls,
                'other_obs': other_obs,
                'curve_data_json': prepare_curve_data_json(readings, controls, other_obs)
            })
    
    # Sort records within each section
    for section in sections.values():
        section['records'].sort(key=lambda x: (
            x['row']['mix_name'] or '',
            x['row']['target_name'] or '',
            x['priority'],
            x['row']['id']
        ))
    
    # First pass - collect all targets per section for TOC
    toc_data = {}
    for section_num in [1, 2, 3]:
        section = sections[section_num]
        if not section['records']:
            continue
        
        toc_data[section_num] = {
            'name': section['name'],
            'count': len(section['records']),
            'targets': []
        }
        
        seen_targets = set()
        for record_data in section['records']:
            target = record_data['row']['target_name']
            if target and target not in seen_targets:
                seen_targets.add(target)
                toc_data[section_num]['targets'].append(target)
    
    # Generate HTML with enhanced JavaScript
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>QST Discrepancies Report - Interactive v2</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 10px;
            background-color: #f5f5f5;
        }}
        .header {{
            text-align: center;
            margin: 20px 0;
        }}
        .stats {{
            background: white;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            text-align: center;
        }}
        .legend {{
            background: white;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 20px;
            text-align: center;
            font-size: 12px;
        }}
        .legend-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
            margin-top: 10px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            font-size: 11px;
        }}
        .color-box {{
            width: 20px;
            height: 20px;
            border: 1px solid #999;
            border-radius: 3px;
        }}
        .section-header {{
            background: #1976D2;
            color: white;
            padding: 15px;
            margin-top: 40px;
            margin-bottom: 10px;
            border-radius: 5px;
            font-size: 20px;
            font-weight: bold;
            text-align: center;
        }}
        .section-controls {{
            background: white;
            padding: 10px;
            margin-bottom: 15px;
            border-radius: 5px;
            text-align: center;
        }}
        .view-selector {{
            display: inline-flex;
            gap: 15px;
            align-items: center;
            padding: 5px 10px;
            background: #f0f0f0;
            border-radius: 5px;
        }}
        .view-selector label {{
            cursor: pointer;
            padding: 5px 10px;
        }}
        .view-selector input[type="radio"] {{
            cursor: pointer;
        }}
        .section-desc {{
            background: white;
            padding: 10px 15px;
            margin-bottom: 15px;
            border-radius: 5px;
            text-align: center;
            font-style: italic;
            color: #555;
        }}
        .section-stats {{
            background: #E3F2FD;
            padding: 8px 15px;
            margin-bottom: 20px;
            border-radius: 5px;
            text-align: center;
            font-size: 14px;
        }}
        .mix-header {{
            background: #2196F3;
            color: white;
            padding: 10px 15px;
            margin-top: 30px;
            margin-bottom: 15px;
            border-radius: 5px;
            font-size: 18px;
            font-weight: bold;
            text-align: center;
        }}
        .target-header {{
            background: #607D8B;
            color: white;
            padding: 8px 15px;
            margin-top: 20px;
            margin-bottom: 5px;
            border-radius: 5px;
            font-size: 16px;
            font-weight: bold;
            text-align: center;
        }}
        .target-controls {{
            background: white;
            padding: 8px;
            margin-bottom: 10px;
            border-radius: 4px;
            text-align: center;
            border: 1px solid #ddd;
        }}
        .target-view-selector {{
            display: inline-flex;
            gap: 10px;
            align-items: center;
            font-size: 12px;
        }}
        .target-view-selector label {{
            cursor: pointer;
            padding: 3px 8px;
            background: #f8f8f8;
            border-radius: 3px;
        }}
        .target-view-selector label:hover {{
            background: #e8e8e8;
        }}
        .target-view-selector input[type="radio"] {{
            cursor: pointer;
            margin-right: 3px;
        }}
        .container {{
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 8px;
            max-width: 1400px;
            margin: 0 auto;
        }}
        .graph-container {{
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 8px;
            text-align: center;
        }}
        .graph-header {{
            font-size: 11px;
            font-weight: bold;
            margin-bottom: 4px;
            color: #333;
        }}
        .graph-details {{
            font-size: 10px;
            margin-top: 4px;
            line-height: 1.3;
        }}
        .detail-row {{
            margin: 1px 0;
        }}
        .svg-container {{
            position: relative;
            width: 240px;
            height: 120px;
            margin: 0 auto;
        }}
        .toc-container {{
            background: white;
            border: 2px solid #1976D2;
            border-radius: 8px;
            padding: 20px;
            margin: 20px auto;
            max-width: 800px;
        }}
        .toc-title {{
            font-size: 18px;
            font-weight: bold;
            color: #1976D2;
            margin-bottom: 15px;
            text-align: center;
        }}
        .toc-section {{
            margin-bottom: 20px;
            border-left: 3px solid #2196F3;
            padding-left: 15px;
        }}
        .toc-section-header {{
            font-size: 16px;
            font-weight: bold;
            color: #333;
            margin-bottom: 8px;
        }}
        .toc-section-header a {{
            color: #1976D2;
            text-decoration: none;
        }}
        .toc-section-header a:hover {{
            text-decoration: underline;
        }}
        .toc-targets {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 8px;
        }}
        .toc-target {{
            background: #E3F2FD;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 12px;
            color: #1976D2;
            text-decoration: none;
            transition: background 0.2s;
        }}
        .toc-target:hover {{
            background: #90CAF9;
            color: #0D47A1;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>QST Discrepancies Report - Interactive Curves v2</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <div class="stats">
            <strong>Total Records:</strong> {stats['total']} | 
            <strong>Displayed:</strong> {stats['total'] - stats['suppressed']} | 
            <strong>Suppressed:</strong> {stats['suppressed']}<br>
            <span style="background-color: #90EE90; padding: 2px 5px;">Changed to Positive: {stats['discrepancy_positive']}</span> | 
            <span style="background-color: #FF6B6B; padding: 2px 5px;">Changed to Negative: {stats['discrepancy_negative']}</span> | 
            <span style="background-color: #FFD700; padding: 2px 5px;">LIMS Other: {stats['lims_other']}</span> | 
            <span style="background-color: #FFB6C1; padding: 2px 5px;">Has Error: {stats['has_error']}</span> | 
            <span style="background-color: #E8F5E9; padding: 2px 5px;">Kept Detected: {stats['agreement_detected']}</span> | 
            <span style="background-color: #FCE4EC; padding: 2px 5px;">Kept Not Detected: {stats['agreement_not_detected']}</span>
        </div>
    </div>
    
    <div class="legend">
        <h3>Curve Types</h3>
        <div class="legend-grid">
            <div class="legend-item">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="#2196F3" stroke-width="2"/></svg>
                <span>Main Discrepancy (Blue solid)</span>
            </div>
            <div class="legend-item">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="red" stroke-width="1" stroke-dasharray="2,2"/></svg>
                <span>Negative Controls (Red dotted)</span>
            </div>
            <div class="legend-item">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="green" stroke-width="1" stroke-dasharray="5,3"/></svg>
                <span>Positive Controls (Green dashed)</span>
            </div>
            <div class="legend-item">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="orange" stroke-width="1.2"/></svg>
                <span>IC Curves (Orange solid)</span>
            </div>
            <div class="legend-item">
                <svg width="30" height="10"><line x1="0" y1="5" x2="30" y2="5" stroke="#666" stroke-width="1" stroke-dasharray="8,2"/></svg>
                <span>Other Targets (Grey varied)</span>
            </div>
        </div>
    </div>
    
"""
    
    # Generate Table of Contents
    html += """
    <div class="toc-container">
        <div class="toc-title">📑 Table of Contents</div>
"""
    
    for section_num, section_info in sorted(toc_data.items()):
        html += f"""
        <div class="toc-section">
            <div class="toc-section-header">
                <a href="#section-{section_num}">Section {section_num}: {section_info['name']}</a> ({section_info['count']} records)
            </div>
            <div class="toc-targets">
"""
        for target in sorted(section_info['targets']):
            # Create a safe ID for the target
            target_id = f"target-{section_num}-{target.replace(' ', '_').replace('/', '_')}"
            html += f'                <a href="#{target_id}" class="toc-target">{target}</a>\n'
        
        html += """            </div>
        </div>
"""
    
    html += """    </div>
    
    <script>
    // Store all curve data for each record
    const curveData = {};
    
    function generateSVG(recordId, viewType) {
        const data = curveData[recordId];
        if (!data) return '';
        
        const width = 240;
        const height = 120;
        const marginLeft = 25;
        const marginRight = 15;
        const marginTop = 15;
        const marginBottom = 15;
        const plotWidth = width - marginLeft - marginRight;
        const plotHeight = height - marginTop - marginBottom;
        
        // Determine which curves to show and collect all readings for scaling
        let allReadings = [];
        let curvesToDraw = [];
        
        // Always include main curve
        if (data.main && data.main.length > 0) {
            allReadings = allReadings.concat(data.main.filter(r => r !== null));
            curvesToDraw.push({type: 'main', readings: data.main});
        }
        
        // Add other curves based on view type
        if (viewType === 'controls' || viewType === 'all') {
            data.controls.forEach(ctrl => {
                if (ctrl.readings && ctrl.readings.length > 0) {
                    allReadings = allReadings.concat(ctrl.readings.filter(r => r !== null));
                    curvesToDraw.push({type: 'control', subtype: ctrl.type, readings: ctrl.readings, role: ctrl.role});
                }
            });
        }
        
        if (viewType === 'other' || viewType === 'all') {
            data.other.forEach(obs => {
                if (obs.readings && obs.readings.length > 0) {
                    allReadings = allReadings.concat(obs.readings.filter(r => r !== null));
                    curvesToDraw.push({type: 'other', subtype: obs.type, readings: obs.readings, target: obs.target});
                }
            });
        }
        
        if (allReadings.length === 0) {
            return '<svg width="' + width + '" height="' + height + '"><rect width="' + width + '" height="' + height + '" fill="white"/><text x="' + (width/2) + '" y="' + (height/2) + '" text-anchor="middle" font-size="12" fill="#999">No data</text></svg>';
        }
        
        // Calculate min/max with some padding to avoid cramping
        let minVal = Math.min(...allReadings);
        let maxVal = Math.max(...allReadings);
        let range = maxVal - minVal;
        
        // Add 5% padding to avoid curves touching edges
        if (range > 0) {
            const padding = range * 0.05;
            minVal -= padding;
            maxVal += padding;
            range = maxVal - minVal;
        } else {
            // Handle flat line case
            minVal -= 0.5;
            maxVal += 0.5;
            range = 1;
        }
        
        // Generate path function
        function generatePath(readings) {
            const validReadings = readings.filter(r => r !== null);
            if (validReadings.length === 0) return '';
            
            const points = validReadings.map((reading, i) => {
                const x = marginLeft + (i * plotWidth / (validReadings.length - 1 || 1));
                const y = marginTop + plotHeight - ((reading - minVal) / range * plotHeight);
                return x.toFixed(1) + ',' + y.toFixed(1);
            });
            
            return 'M ' + points.join(' L ');
        }
        
        // Build SVG
        let svg = '<svg width="' + width + '" height="' + height + '" viewBox="0 0 ' + width + ' ' + height + '">';
        
        // White background
        svg += '<rect width="' + width + '" height="' + height + '" fill="white"/>';
        
        // Plot area border
        svg += '<rect x="' + marginLeft + '" y="' + marginTop + '" width="' + plotWidth + '" height="' + plotHeight + '" fill="none" stroke="#ddd" stroke-width="1"/>';
        
        // Draw curves (controls and other first, main on top)
        const dashPatterns = ["8,2", "4,4", "2,1,2,1", "6,2,2,2"];
        const greyShades = ["#666", "#888", "#555", "#777"];
        let otherCount = 0;
        
        curvesToDraw.forEach(curve => {
            if (curve.type === 'main') return; // Draw main last
            
            const path = generatePath(curve.readings);
            if (!path) return;
            
            if (curve.type === 'control') {
                if (curve.subtype === 'negative') {
                    svg += '<path d="' + path + '" fill="none" stroke="red" stroke-width="1" stroke-dasharray="2,2" opacity="0.7"/>';
                } else if (curve.subtype === 'positive') {
                    svg += '<path d="' + path + '" fill="none" stroke="green" stroke-width="1" stroke-dasharray="5,3" opacity="0.7"/>';
                }
            } else if (curve.type === 'other') {
                if (curve.subtype === 'ic') {
                    svg += '<path d="' + path + '" fill="none" stroke="orange" stroke-width="1.2" opacity="0.8"/>';
                } else {
                    const shade = greyShades[otherCount % greyShades.length];
                    const dash = dashPatterns[otherCount % dashPatterns.length];
                    svg += '<path d="' + path + '" fill="none" stroke="' + shade + '" stroke-width="1" stroke-dasharray="' + dash + '" opacity="0.7"/>';
                    otherCount++;
                }
            }
        });
        
        // Draw main curve on top
        const mainCurve = curvesToDraw.find(c => c.type === 'main');
        if (mainCurve) {
            const path = generatePath(mainCurve.readings);
            if (path) {
                svg += '<path d="' + path + '" fill="none" stroke="#2196F3" stroke-width="1.5"/>';
            }
        }
        
        // Y-axis labels
        svg += '<text x="' + (marginLeft-3) + '" y="' + (marginTop+5) + '" text-anchor="end" font-size="9" fill="#666">' + maxVal.toFixed(1) + '</text>';
        svg += '<text x="' + (marginLeft-3) + '" y="' + (height-marginBottom+3) + '" text-anchor="end" font-size="9" fill="#666">' + minVal.toFixed(1) + '</text>';
        
        svg += '</svg>';
        
        return svg;
    }
    
    function updateCurveVisibility(sectionNum, viewType) {
        const section = document.getElementById('section-' + sectionNum);
        if (!section) return;
        
        const containers = section.querySelectorAll('.svg-container');
        
        containers.forEach(container => {
            const recordId = container.getAttribute('data-record-id');
            if (recordId && curveData[recordId]) {
                container.innerHTML = generateSVG(recordId, viewType);
            }
        });
    }
    
    function updateTargetCurves(targetId, viewType) {
        const targetContainer = document.getElementById('target-container-' + targetId);
        if (!targetContainer) return;
        
        const containers = targetContainer.querySelectorAll('.svg-container');
        
        containers.forEach(container => {
            const recordId = container.getAttribute('data-record-id');
            if (recordId && curveData[recordId]) {
                container.innerHTML = generateSVG(recordId, viewType);
            }
        });
        
        // Update section radio to match if needed
        const section = targetContainer.closest('[id^="section-"]');
        if (section) {
            const sectionNum = section.id.split('-')[1];
            const radio = document.querySelector(`input[name="view-section-${sectionNum}"][value="${viewType}"]`);
            if (radio) radio.checked = true;
        }
    }
    
    // Initialize all sections to controls view on load
    document.addEventListener('DOMContentLoaded', function() {
        const sections = [1, 2, 3];
        sections.forEach(num => {
            const radio = document.querySelector(`input[name="view-section-${num}"][value="controls"]`);
            if (radio) {
                radio.checked = true;
                updateCurveVisibility(num, 'controls');
            }
        });
    });
    </script>
"""
    
    # Generate each section
    for section_num in [1, 2, 3]:  # Skip section 4 if empty
        section = sections[section_num]
        if not section['records']:
            continue
            
        # Count records by category in this section
        section_counts = {}
        for record in section['records']:
            cat = record['category']
            section_counts[cat] = section_counts.get(cat, 0) + 1
        
        html += f"""
    <div id="section-{section_num}">
        <div class="section-header">Section {section_num}: {section['name']}</div>
        <div class="section-controls">
            <div class="view-selector">
                <strong>Show curves:</strong>
                <label><input type="radio" name="view-section-{section_num}" value="controls" onclick="updateCurveVisibility({section_num}, 'controls')" checked> With Controls</label>
                <label><input type="radio" name="view-section-{section_num}" value="other" onclick="updateCurveVisibility({section_num}, 'other')"> With Other Well Curves</label>
                <label><input type="radio" name="view-section-{section_num}" value="all" onclick="updateCurveVisibility({section_num}, 'all')"> All</label>
                <label><input type="radio" name="view-section-{section_num}" value="none" onclick="updateCurveVisibility({section_num}, 'none')"> None</label>
            </div>
        </div>
        <div class="section-desc">{section['desc']}</div>
        <div class="section-stats">
            <strong>Records in section:</strong> {len(section['records'])} | """
        
        # Add category counts for this section
        if 'discrepancy_positive' in section_counts:
            html += f"Changed to Positive: {section_counts['discrepancy_positive']} | "
        if 'discrepancy_negative' in section_counts:
            html += f"Changed to Negative: {section_counts['discrepancy_negative']} | "
        if 'lims_other' in section_counts:
            html += f"LIMS Other: {section_counts['lims_other']} | "
        if 'has_error' in section_counts:
            html += f"Has Error: {section_counts['has_error']} | "
        if 'agreement_detected' in section_counts:
            html += f"Kept Detected: {section_counts['agreement_detected']} | "
        if 'agreement_not_detected' in section_counts:
            html += f"Kept Not Detected: {section_counts['agreement_not_detected']} | "
        
        html = html.rstrip(' | ') + "</div>\n"
        
        # Generate records in grid layout
        current_mix = None
        current_target = None
        grid_open = False
        target_counter = 0  # Counter for unique target IDs
        
        for record_data in section['records']:
            row = record_data['row']
            
            # Check if we need a new mix header
            if row['mix_name'] != current_mix:
                if grid_open:
                    html += '</div></div>\n'  # Close previous grid and target container
                    grid_open = False
                current_mix = row['mix_name']
                current_target = None  # Reset target when mix changes
                html += f'<div class="mix-header">Mix: {current_mix or "Unknown"}</div>\n'
            
            # Check if we need a new target header
            if row['target_name'] != current_target:
                if grid_open:
                    html += '</div></div>\n'  # Close previous grid and target container
                    grid_open = False
                current_target = row['target_name']
                target_counter += 1
                target_id = f"{section_num}-{target_counter}"
                # Create anchor ID for TOC linking
                anchor_id = f"target-{section_num}-{(current_target or 'Unknown').replace(' ', '_').replace('/', '_')}"
                
                html += f'<div class="target-header" id="{anchor_id}">Target: {current_target or "Unknown"}</div>\n'
                html += f"""
                <div class="target-controls">
                    <div class="target-view-selector">
                        <strong>View:</strong>
                        <label><input type="radio" name="view-target-{target_id}" value="controls" onclick="updateTargetCurves('{target_id}', 'controls')" checked> Controls</label>
                        <label><input type="radio" name="view-target-{target_id}" value="other" onclick="updateTargetCurves('{target_id}', 'other')"> Other Wells</label>
                        <label><input type="radio" name="view-target-{target_id}" value="all" onclick="updateTargetCurves('{target_id}', 'all')"> All</label>
                        <label><input type="radio" name="view-target-{target_id}" value="none" onclick="updateTargetCurves('{target_id}', 'none')"> Main Only</label>
                    </div>
                </div>
                """
                html += f'<div id="target-container-{target_id}">\n'
                html += '<div class="container">\n'  # Start new grid
                grid_open = True
            
            # Add curve data to JavaScript
            html += f"""
        <script>
        curveData[{row['id']}] = {record_data['curve_data_json']};
        </script>
"""
            
            # Generate record HTML as a grid item
            dxai_ct_text = ''
            if row['dxai_cls'] == 1 and row['dxai_ct']:
                dxai_ct_text = f"<div class='detail-row'><strong>DXAI CT:</strong> {row['dxai_ct']:.2f}</div>"
            
            # Format extraction date
            extraction_date = row['extraction_date'] if row['extraction_date'] else 'N/A'
            if extraction_date != 'N/A':
                # Parse and format date nicely (2021-07-24 05:00:26 -> 24-Jul-2021)
                try:
                    from datetime import datetime as dt
                    date_obj = dt.strptime(extraction_date[:10], '%Y-%m-%d')
                    extraction_date = date_obj.strftime('%d-%b-%Y')
                except:
                    extraction_date = extraction_date[:10]  # Just show YYYY-MM-DD if parsing fails
            
            # Build detail fields based on conditions
            detail_fields = f"""
                <div class="detail-row"><strong>Date:</strong> {extraction_date}</div>
                <div class="detail-row"><strong>Final:</strong> {row['final_cls']} | <strong>Machine:</strong> {row['machine_cls']}</div>
                <div class="detail-row"><strong>Machine CT:</strong> {f"{row['machine_ct']:.2f}" if row['machine_ct'] else 'N/A'}</div>
                {dxai_ct_text}"""
            
            # Handle LIMS vs Error display based on conditions
            error_code = row['error_code']
            lims_status = row['lims_status']
            resolution = format_resolution_code(row['resolution_codes'], row['machine_cls'], row['final_cls'], lims_status)
            
            if error_code:
                # When there's an error, don't show LIMS (it would be None)
                # Show error first, then resolution, with renamed headers
                detail_fields += f"""
                <div class="detail-row"><strong>Final Error:</strong> {error_code}</div>
                <div class="detail-row"><strong>Original Resolution:</strong> {resolution}</div>"""
            elif lims_status and lims_status != 'None':
                # When there's LIMS status, don't show error (it would be None)
                # Show LIMS and resolution normally
                detail_fields += f"""
                <div class="detail-row"><strong>LIMS:</strong> {lims_status}</div>
                <div class="detail-row"><strong>Resolution:</strong> {resolution}</div>"""
            else:
                # Neither error nor LIMS - just show resolution
                detail_fields += f"""
                <div class="detail-row"><strong>Resolution:</strong> {resolution}</div>"""
            
            html += f"""
        <div class="graph-container" style="background-color: {record_data['color']};">
            <div class="graph-header">
                ID {row['id']} | {row['sample_label'][:20] if len(row['sample_label']) > 20 else row['sample_label']}
            </div>
            <div class="svg-container" data-record-id="{row['id']}">
                <!-- SVG will be generated by JavaScript -->
            </div>
            <div class="graph-details">
                {detail_fields}
            </div>
        </div>
"""
        
        # Close final grid and target container if open
        if grid_open:
            html += '</div></div>\n'  # Close both grid container and target container
        
        html += '</div>\n'  # Close section div
    
    html += """
</body>
</html>
"""
    
    # Write HTML file
    with open(output_path, 'w') as f:
        f.write(html)
    
    conn.close()
    
    print(f"\nInteractive report v2 generated: {output_path}")
    print(f"Total records processed: {stats['total']}")
    print(f"Records displayed: {stats['total'] - stats['suppressed']}")
    print(f"Records suppressed: {stats['suppressed']}")
    print(f"\nSection breakdown:")
    print(f"  Section 1 (Acted Upon): {len(sections[1]['records'])} records")
    print(f"  Section 2 (Repeated): {len(sections[2]['records'])} records")
    print(f"  Section 3 (Ignored): {len(sections[3]['records'])} records")
    if sections[4]['records']:
        print(f"  Section 4 (Other): {len(sections[4]['records'])} records")
    
    return stats

def main():
    parser = argparse.ArgumentParser(description='Generate interactive HTML report v2 for QST discrepancies')
    parser.add_argument('--db', type=str, default='qst_discreps.db',
                       help='Path to QST database')
    parser.add_argument('--output', type=str, default='output_data/qst_report_interactive_v2.html',
                       help='Output HTML file path')
    parser.add_argument('--limit', type=int,
                       help='Limit number of records to process')
    
    args = parser.parse_args()
    
    # Check database exists
    if not os.path.exists(args.db):
        print(f"Error: Database not found: {args.db}")
        return
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate report
    generate_html_report(args.db, args.output, args.limit)

if __name__ == "__main__":
    main()