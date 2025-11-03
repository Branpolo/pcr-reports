#!/usr/bin/env python3
"""
Generate color-coded HTML report for QST discrepancies data with three analytical sections
"""

import sqlite3
import argparse
import os
from datetime import datetime
import sys
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

def generate_svg_graph(readings, width=240, height=120):
    """Generate SVG graph for readings"""
    if not readings:
        return f'<svg width="{width}" height="{height}"><rect width="{width}" height="{height}" fill="white"/><text x="{width/2}" y="{height/2}" text-anchor="middle" font-size="12" fill="#999">No data</text></svg>'
    
    # Filter out None values
    valid_readings = [r for r in readings if r is not None]
    if not valid_readings:
        return f'<svg width="{width}" height="{height}"><rect width="{width}" height="{height}" fill="white"/><text x="{width/2}" y="{height/2}" text-anchor="middle" font-size="12" fill="#999">No valid data</text></svg>'
    
    # Calculate graph dimensions
    margin_left = 25
    margin_right = 15
    margin_top = 15
    margin_bottom = 15
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    
    # Get min/max for scaling
    min_val = min(valid_readings)
    max_val = max(valid_readings)
    value_range = max_val - min_val if max_val != min_val else 1
    
    # Generate path
    points = []
    for i, reading in enumerate(valid_readings):
        x = margin_left + (i * plot_width / (len(valid_readings) - 1 if len(valid_readings) > 1 else 1))
        y = margin_top + plot_height - ((reading - min_val) / value_range * plot_height)
        points.append(f"{x:.1f},{y:.1f}")
    
    path = "M " + " L ".join(points)
    
    svg = f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">
        <!-- White background for graph area -->
        <rect width="{width}" height="{height}" fill="white"/>
        
        <!-- Plot area border -->
        <rect x="{margin_left}" y="{margin_top}" width="{plot_width}" height="{plot_height}" 
              fill="none" stroke="#ddd" stroke-width="1"/>
        
        <!-- Data line -->
        <path d="{path}" fill="none" stroke="#2196F3" stroke-width="1.5"/>
        
        <!-- Y-axis labels -->
        <text x="{margin_left-3}" y="{margin_top+5}" text-anchor="end" font-size="9" fill="#666">{max_val:.1f}</text>
        <text x="{margin_left-3}" y="{height-margin_bottom+3}" text-anchor="end" font-size="9" fill="#666">{min_val:.1f}</text>
    </svg>"""
    
    return svg

def generate_html_report(db_path, output_path, limit=None):
    """Generate HTML report with color-coded categorization in three sections"""
    
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
            
            sections[section]['records'].append({
                'row': row,
                'category': category,
                'color': color,
                'priority': priority,
                'readings': readings
            })
    
    # Sort records within each section
    for section in sections.values():
        section['records'].sort(key=lambda x: (
            x['row']['mix_name'] or '',
            x['row']['target_name'] or '',
            x['priority'],
            x['row']['id']
        ))
    
    # Generate HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>QST Discrepancies Report - Analytical View</title>
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
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 10px;
            margin-top: 10px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
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
            margin-bottom: 15px;
            border-radius: 5px;
            font-size: 16px;
            font-weight: bold;
            text-align: center;
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
    </style>
</head>
<body>
    <div class="header">
        <h1>QST Discrepancies Report - Analytical View</h1>
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
        <h3>Color Legend by Section</h3>
        <div class="legend-grid">
            <div class="legend-item">
                <div class="color-box" style="background-color: #90EE90;"></div>
                <span>Section 1: Changed to Positive (Green)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #FF6B6B;"></div>
                <span>Section 1: Changed to Negative (Red)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #FFD700;"></div>
                <span>Section 2: LIMS Other Status (Yellow)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #FFB6C1;"></div>
                <span>Section 2: Has Error Code (Pink)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #E8F5E9;"></div>
                <span>Section 3: Kept as Detected (Pale Green)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #FCE4EC;"></div>
                <span>Section 3: Kept as Not Detected (Pale Pink)</span>
            </div>
        </div>
    </div>
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
    <div class="section-header">Section {section_num}: {section['name']}</div>
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
        
        for record_data in section['records']:
            row = record_data['row']
            
            # Check if we need a new mix header
            if row['mix_name'] != current_mix:
                if grid_open:
                    html += '</div>\n'  # Close previous grid
                    grid_open = False
                current_mix = row['mix_name']
                current_target = None  # Reset target when mix changes
                html += f'<div class="mix-header">Mix: {current_mix or "Unknown"}</div>\n'
            
            # Check if we need a new target header
            if row['target_name'] != current_target:
                if grid_open:
                    html += '</div>\n'  # Close previous grid
                    grid_open = False
                current_target = row['target_name']
                html += f'<div class="target-header">Target: {current_target or "Unknown"}</div>\n'
                html += '<div class="container">\n'  # Start new grid
                grid_open = True
            
            # Generate record HTML as a grid item
            dxai_ct_text = ''
            if row['dxai_cls'] == 1 and row['dxai_ct']:
                dxai_ct_text = f"<div class='detail-row'><strong>DXAI CT:</strong> {row['dxai_ct']:.2f}</div>"
            
            html += f"""
        <div class="graph-container" style="background-color: {record_data['color']};">
            <div class="graph-header">
                ID {row['id']} | {row['sample_label'][:20] if len(row['sample_label']) > 20 else row['sample_label']}
            </div>
            {generate_svg_graph(record_data['readings'])}
            <div class="graph-details">
                <div class="detail-row"><strong>Final:</strong> {row['final_cls']} | <strong>Machine:</strong> {row['machine_cls']}</div>
                <div class="detail-row"><strong>Machine CT:</strong> {f"{row['machine_ct']:.2f}" if row['machine_ct'] else 'N/A'}</div>
                {dxai_ct_text}
                <div class="detail-row"><strong>LIMS:</strong> {row['lims_status'] or 'None'}</div>
                <div class="detail-row"><strong>Error:</strong> {row['error_code'] or 'None'}</div>
                <div class="detail-row"><strong>Resolution:</strong> {format_resolution_code(row['resolution_codes'], row['machine_cls'], row['final_cls'], row['lims_status'])}</div>
            </div>
        </div>
"""
        
        # Close final grid if open
        if grid_open:
            html += '</div>\n'
    
    html += """
</body>
</html>
"""
    
    # Write HTML file
    with open(output_path, 'w') as f:
        f.write(html)
    
    conn.close()
    
    print(f"\nReport generated: {output_path}")
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
    parser = argparse.ArgumentParser(description='Generate sectioned HTML report for QST discrepancies')
    parser.add_argument('--db', type=str, default='qst_discreps.db',
                       help='Path to QST database')
    parser.add_argument('--output', type=str, default='output_data/qst_report_sections.html',
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