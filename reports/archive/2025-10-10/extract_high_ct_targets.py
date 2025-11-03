#!/usr/bin/env python3
"""
Extract Parvo and HHV6 results with machine CT > 33 from quest_prod database
Generate PCRAI files per run and create HTML report
"""

import sqlite3
import argparse
import os
import json
from datetime import datetime
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.database import bytes_to_float

def get_high_ct_results(quest_conn, ct_threshold=33):
    """Get Parvo and HHV6 results with CT > threshold"""
    cursor = quest_conn.cursor()
    
    query = """
    SELECT DISTINCT
        r.id as run_id,
        r.run_name,
        r.created_at as run_date,
        w.id as well_id,
        w.well_number as well_position,
        w.role_alias,
        w.sample_name,
        o.id as obs_id,
        o.machine_cls as machine_result,
        o.machine_ct,
        o.final_cls as final_result,
        o.final_ct,
        t.target_name,
        o.readings
    FROM runs r
    JOIN wells w ON r.id = w.run_id
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE o.machine_ct > ?
    AND o.machine_cls = 1
    AND (
        UPPER(t.target_name) LIKE '%PARVO%'
        OR UPPER(t.target_name) LIKE '%HHV6%'
        OR UPPER(t.target_name) LIKE '%HHV-6%'
    )
    AND w.role_alias NOT LIKE '%CONTROL%'
    AND w.role_alias != 'NC'
    AND w.role_alias != 'PC'
    ORDER BY r.run_name, w.well_number
    """
    
    cursor.execute(query, (ct_threshold,))
    results = []
    for row in cursor.fetchall():
        results.append({
            'run_id': row[0],
            'run_name': row[1],
            'run_date': row[2],
            'well_id': row[3],
            'well_position': row[4],
            'role_alias': row[5],
            'sample_name': row[6],
            'obs_id': row[7],
            'machine_result': row[8],
            'machine_ct': row[9],
            'final_result': row[10],
            'final_ct': row[11],
            'target_name': row[12],
            'readings': row[13]
        })
    
    return results

def get_run_controls(quest_conn, run_ids):
    """Get all control wells for specified runs"""
    cursor = quest_conn.cursor()
    
    placeholders = ','.join('?' * len(run_ids))
    query = f"""
    SELECT 
        r.id as run_id,
        r.run_name,
        w.id as well_id,
        w.well_number as well_position,
        w.role_alias,
        w.sample_name,
        o.id as obs_id,
        o.machine_cls as machine_result,
        o.machine_ct,
        o.final_cls as final_result,
        o.final_ct,
        t.target_name,
        o.readings
    FROM runs r
    JOIN wells w ON r.id = w.run_id
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE r.id IN ({placeholders})
    AND (
        w.role_alias LIKE '%CONTROL%'
        OR w.role_alias = 'NC'
        OR w.role_alias = 'PC'
        OR w.role_alias = 'HPC'
        OR w.role_alias = 'LPC'
        OR w.role_alias LIKE '%BLANK%'
    )
    ORDER BY r.run_name, w.well_number
    """
    
    cursor.execute(query, run_ids)
    controls = {}
    for row in cursor.fetchall():
        run_id = row[0]
        if run_id not in controls:
            controls[run_id] = []
        controls[run_id].append({
            'run_name': row[1],
            'well_id': row[2],
            'well_position': row[3],
            'role_alias': row[4],
            'sample_name': row[5],
            'obs_id': row[6],
            'machine_result': row[7],
            'machine_ct': row[8],
            'final_result': row[9],
            'final_ct': row[10],
            'target_name': row[11],
            'readings': row[12]
        })
    
    return controls

def parse_readings(readings_blob):
    """Parse readings blob into list of floats"""
    if not readings_blob:
        return []
    
    try:
        # Try parsing as JSON first
        if isinstance(readings_blob, str):
            return json.loads(readings_blob)
        # If it's bytes, try to decode
        elif isinstance(readings_blob, bytes):
            try:
                # Try as JSON string
                return json.loads(readings_blob.decode('utf-8'))
            except:
                # Try as packed floats (50 floats * 8 bytes each = 400 bytes)
                import struct
                if len(readings_blob) == 400:
                    return list(struct.unpack('50d', readings_blob))
                else:
                    return []
    except:
        return []
    
    return []

def generate_pcrai_file(run_data, controls, output_dir):
    """Generate a PCRAI file for a run"""
    run_name = run_data[0]['run_name']
    run_id = run_data[0]['run_id']
    
    # Create PCRAI structure
    pcrai_data = {
        'run_name': run_name,
        'run_id': run_id,
        'run_date': run_data[0]['run_date'],
        'generated': datetime.now().isoformat(),
        'high_ct_samples': [],
        'controls': []
    }
    
    # Add high CT samples
    for sample in run_data:
        readings = parse_readings(sample['readings'])
        pcrai_data['high_ct_samples'].append({
            'well_position': sample['well_position'],
            'sample_name': sample['sample_name'],
            'target': sample['target_name'],
            'machine_ct': sample['machine_ct'],
            'final_ct': sample['final_ct'],
            'machine_result': sample['machine_result'],
            'final_result': sample['final_result'],
            'readings': readings
        })
    
    # Add controls
    if run_id in controls:
        for control in controls[run_id]:
            readings = parse_readings(control['readings'])
            pcrai_data['controls'].append({
                'well_position': control['well_position'],
                'role': control['role_alias'],
                'sample_name': control['sample_name'],
                'target': control['target_name'],
                'machine_ct': control['machine_ct'],
                'final_ct': control['final_ct'],
                'machine_result': control['machine_result'],
                'final_result': control['final_result'],
                'readings': readings
            })
    
    # Save PCRAI file
    safe_run_name = run_name.replace('/', '_').replace('\\', '_')
    filename = f"{safe_run_name}_{run_id}.pcrai"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(pcrai_data, f, indent=2)
    
    return filename

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

def generate_html_report(results_by_run, controls, output_path):
    """Generate HTML report following generate_qst_report_sections.py template"""
    
    # Count statistics
    total_parvo = sum(1 for samples in results_by_run.values() for s in samples if 'PARVO' in s['target_name'].upper())
    total_hhv6 = sum(1 for samples in results_by_run.values() for s in samples if 'HHV' in s['target_name'].upper())
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>High CT Parvo/HHV6 Results Report</title>
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
        .run-header {{
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
        .controls-section {{
            background: #E3F2FD;
            padding: 10px;
            margin-top: 20px;
            border-radius: 5px;
        }}
        .controls-title {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
            text-align: center;
        }}
        .controls-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 5px;
            font-size: 11px;
        }}
        .control-item {{
            padding: 3px;
            background: white;
            border-radius: 2px;
        }}
        .pcrai-note {{
            text-align: center;
            color: #666;
            font-size: 12px;
            margin-top: 10px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>High CT (>33) Results for Parvo and HHV6 Targets</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <div class="stats">
            <strong>Total Runs:</strong> {len(results_by_run)} | 
            <strong>Total High CT Results:</strong> {sum(len(samples) for samples in results_by_run.values())} | 
            <span style="background-color: #FFE4E1; padding: 2px 5px;">Parvo Results: {total_parvo}</span> | 
            <span style="background-color: #E0F2F1; padding: 2px 5px;">HHV6 Results: {total_hhv6}</span>
        </div>
    </div>
    
    <div class="legend">
        <h3>Target Color Legend</h3>
        <div class="legend-grid">
            <div class="legend-item">
                <div class="color-box" style="background-color: #FFE4E1;"></div>
                <span>Parvo Targets (Light Pink)</span>
            </div>
            <div class="legend-item">
                <div class="color-box" style="background-color: #E0F2F1;"></div>
                <span>HHV6 Targets (Light Teal)</span>
            </div>
        </div>
    </div>
"""
    
    # Sort runs and process each
    for run_id in sorted(results_by_run.keys(), key=lambda x: results_by_run[x][0]['run_name']):
        samples = results_by_run[run_id]
        run_name = samples[0]['run_name']
        run_date = samples[0]['run_date']
        
        # Group samples by target
        samples_by_target = {}
        for sample in samples:
            target = sample['target_name']
            if target not in samples_by_target:
                samples_by_target[target] = []
            samples_by_target[target].append(sample)
        
        # Generate PCRAI filename
        safe_run_name = run_name.replace('/', '_').replace('\\', '_')
        pcrai_filename = f"{safe_run_name}_{run_id}.pcrai"
        
        # Run header
        html += f"""
    <div class="run-header">
        Run: {run_name}
        <div style="font-size: 14px; font-weight: normal; margin-top: 5px;">
            Date: {run_date} | High CT Samples: {len(samples)}
        </div>
    </div>
"""
        
        # Process each target in sorted order
        for target_name in sorted(samples_by_target.keys()):
            target_samples = samples_by_target[target_name]
            
            # Determine background color based on target
            if 'PARVO' in target_name.upper():
                bg_color = '#FFE4E1'  # Light pink for Parvo
            else:
                bg_color = '#E0F2F1'  # Light teal for HHV6
            
            html += f'    <div class="target-header">Target: {target_name}</div>\n'
            html += '    <div class="container">\n'
            
            # Add each sample
            for sample in target_samples:
                readings = parse_readings(sample['readings'])
                
                html += f"""
        <div class="graph-container" style="background-color: {bg_color};">
            <div class="graph-header">
                {sample['well_position']}: {sample['sample_name'][:20] if len(sample['sample_name']) > 20 else sample['sample_name']}
            </div>
            {generate_svg_graph(readings)}
            <div class="graph-details">
                <div class="detail-row"><strong>Machine CT:</strong> {sample['machine_ct']:.2f}</div>
                <div class="detail-row"><strong>Final CT:</strong> {f"{sample['final_ct']:.2f}" if sample['final_ct'] else 'N/A'}</div>
                <div class="detail-row"><strong>Machine:</strong> {'DET' if sample['machine_result'] == 1 else 'ND'} | <strong>Final:</strong> {'DET' if sample['final_result'] == 1 else 'ND'}</div>
            </div>
        </div>
"""
            
            html += '    </div>\n'
        
        # Add controls section if available
        if run_id in controls and controls[run_id]:
            html += """
    <div class="controls-section">
        <div class="controls-title">Control Wells in This Run</div>
        <div class="controls-grid">
"""
            # Group controls by target for better organization
            controls_by_target = {}
            for control in controls[run_id]:
                target = control['target_name']
                if target not in controls_by_target:
                    controls_by_target[target] = []
                controls_by_target[target].append(control)
            
            for target in sorted(controls_by_target.keys()):
                for control in controls_by_target[target]:
                    html += f"""            <div class="control-item">
                <strong>{control['well_position']}</strong>: {control['role_alias']} | {target[:20]} | 
                CT: {f"{control['machine_ct']:.2f}" if control['machine_ct'] else 'N/A'}
            </div>
"""
            
            html += """        </div>
    </div>
"""
        
        # Add PCRAI download note
        html += f"""
    <div class="pcrai-note">
        PCRAI file: <a href="pcrai_files/{pcrai_filename}" style="color: #1976D2;">{pcrai_filename}</a>
    </div>
"""
    
    html += """
</body>
</html>
"""
    
    # Write HTML file
    with open(output_path, 'w') as f:
        f.write(html)
    
    return len(results_by_run)

def main():
    parser = argparse.ArgumentParser(description='Extract high CT Parvo/HHV6 results and generate PCRAI files')
    parser.add_argument('--quest-db', type=str, default='input_data/quest_prod_aug2025.db',
                       help='Path to Quest database')
    parser.add_argument('--ct-threshold', type=float, default=33,
                       help='CT threshold (default: 33)')
    parser.add_argument('--output-dir', type=str, default='output_data/pcrai_files',
                       help='Output directory for PCRAI files')
    parser.add_argument('--html-output', type=str, default='output_data/high_ct_report.html',
                       help='Output HTML report path')
    
    args = parser.parse_args()
    
    # Check database exists
    if not os.path.exists(args.quest_db):
        print(f"Error: Database not found: {args.quest_db}")
        return
    
    # Create output directory if needed
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    html_dir = os.path.dirname(args.html_output)
    if html_dir and not os.path.exists(html_dir):
        os.makedirs(html_dir)
    
    # Connect to database
    quest_conn = sqlite3.connect(args.quest_db)
    
    print(f"Extracting Parvo/HHV6 results with CT > {args.ct_threshold}...")
    
    # Get high CT results
    results = get_high_ct_results(quest_conn, args.ct_threshold)
    
    if not results:
        print("No results found with specified criteria")
        return
    
    print(f"Found {len(results)} high CT results")
    
    # Group by run
    results_by_run = {}
    for result in results:
        run_id = result['run_id']
        if run_id not in results_by_run:
            results_by_run[run_id] = []
        results_by_run[run_id].append(result)
    
    print(f"Results span {len(results_by_run)} runs")
    
    # Get controls for these runs
    run_ids = list(results_by_run.keys())
    print(f"Fetching controls for {len(run_ids)} runs...")
    controls = get_run_controls(quest_conn, run_ids)
    
    # Generate PCRAI files
    print(f"Generating PCRAI files in {args.output_dir}...")
    for run_id, run_data in results_by_run.items():
        filename = generate_pcrai_file(run_data, controls, args.output_dir)
        print(f"  Generated: {filename}")
    
    # Generate HTML report
    print(f"Generating HTML report: {args.html_output}")
    num_runs = generate_html_report(results_by_run, controls, args.html_output)
    
    quest_conn.close()
    
    print(f"\nSummary:")
    print(f"  Total runs processed: {num_runs}")
    print(f"  Total high CT results: {len(results)}")
    print(f"  PCRAI files generated in: {args.output_dir}")
    print(f"  HTML report: {args.html_output}")

if __name__ == "__main__":
    main()