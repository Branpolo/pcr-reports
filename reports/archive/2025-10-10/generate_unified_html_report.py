#!/usr/bin/env python3
"""
Unified HTML report generator from JSON data
Based on generate_control_report_working.py
Handles control, sample, and discrepancy reports
"""

import json
import argparse
from datetime import datetime
from collections import defaultdict

def load_json_data(json_file):
    """Load data from JSON file"""
    with open(json_file, 'r') as f:
        return json.load(f)

def generate_interactive_html(data, output_file):
    """Generate interactive HTML report from JSON data"""
    
    report_type = data.get('report_type', 'unknown')
    errors = data.get('errors', [])
    summary = data.get('summary', {})
    
    # Group by mix and clinical category
    mix_groups = defaultdict(lambda: defaultdict(list))
    for error in errors:
        clinical_cat = error.get('clinical_category', error.get('category', 'unknown'))
        mix_name = error.get('mix_name', 'Unknown Mix')
        mix_groups[mix_name][clinical_cat].append(error)
    
    # Start HTML
    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{report_type.title()} Report - Interactive</title>
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
            display: flex;
            justify-content: center;
            gap: 30px;
        }}
        
        .stat-item {{
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 24px;
            font-weight: bold;
            color: #2196F3;
        }}
        
        .stat-label {{
            font-size: 12px;
            color: #666;
            margin-top: 5px;
        }}
        
        .mix-section {{
            margin: 20px 0;
            background: white;
            border-radius: 4px;
            overflow: hidden;
        }}
        
        .mix-header {{
            background: #2196F3;
            color: white;
            padding: 10px 15px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .mix-header:hover {{
            background: #1976D2;
        }}
        
        .mix-content {{
            padding: 15px;
            display: none;
        }}
        
        .mix-content.active {{
            display: block;
        }}
        
        .category-tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
            border-bottom: 2px solid #e0e0e0;
        }}
        
        .tab {{
            padding: 8px 16px;
            cursor: pointer;
            border-bottom: 3px solid transparent;
            transition: all 0.3s;
        }}
        
        .tab:hover {{
            background: #f5f5f5;
        }}
        
        .tab.active {{
            border-bottom-color: #2196F3;
            color: #2196F3;
        }}
        
        .tab-content {{
            display: none;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        .error-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }}
        
        .error-table th {{
            background: #f0f0f0;
            padding: 8px;
            text-align: left;
            font-weight: normal;
            color: #666;
            border-bottom: 1px solid #ddd;
        }}
        
        .error-table td {{
            padding: 6px 8px;
            border-bottom: 1px solid #eee;
        }}
        
        .error-table tr:hover {{
            background: #f9f9f9;
        }}
        
        .unresolved {{ color: #FF5722; }}
        .error_ignored {{ color: #9E9E9E; }}
        .test_repeated {{ color: #FF9800; }}
        .acted_upon {{ color: #4CAF50; }}
        .samples_repeated {{ color: #FF9800; }}
        .ignored {{ color: #9E9E9E; }}
        
        .truncated-warning {{
            background: #FFF3E0;
            border: 1px solid #FFB74D;
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
            color: #E65100;
        }}
        
        .show-more {{
            background: #2196F3;
            color: white;
            border: none;
            padding: 8px 16px;
            cursor: pointer;
            border-radius: 4px;
            margin-top: 10px;
        }}
        
        .show-more:hover {{
            background: #1976D2;
        }}
        
        .controls {{
            margin: 20px 0;
            padding: 15px;
            background: white;
            border-radius: 4px;
            display: flex;
            gap: 10px;
            align-items: center;
        }}
        
        .controls button {{
            padding: 8px 16px;
            cursor: pointer;
            border: 1px solid #ddd;
            background: white;
            border-radius: 4px;
        }}
        
        .controls button:hover {{
            background: #f5f5f5;
        }}
        
        .controls button.active {{
            background: #2196F3;
            color: white;
            border-color: #2196F3;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{report_type.title()} Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="stats">'''
    
    # Add stats based on report type
    if report_type == 'control':
        html += f'''
        <div class="stat-item">
            <div class="stat-value">{summary.get('total_errors', 0):,}</div>
            <div class="stat-label">Total Control Errors</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('unresolved', 0):,}</div>
            <div class="stat-label">Unresolved</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('error_ignored', 0):,}</div>
            <div class="stat-label">Error Ignored</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('test_repeated', 0):,}</div>
            <div class="stat-label">Test Repeated</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('affected_error_count', 0):,}</div>
            <div class="stat-label">Affected Samples (Error)</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('affected_repeat_count', 0):,}</div>
            <div class="stat-label">Affected Samples (Repeat)</div>
        </div>'''
    elif report_type == 'sample':
        html += f'''
        <div class="stat-item">
            <div class="stat-value">{summary.get('total_errors', 0):,}</div>
            <div class="stat-label">Total Sample Errors</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('unresolved', 0):,}</div>
            <div class="stat-label">Unresolved</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('error_ignored', 0):,}</div>
            <div class="stat-label">Error Ignored</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('test_repeated', 0):,}</div>
            <div class="stat-label">Test Repeated</div>
        </div>'''
    elif report_type == 'discrepancy':
        html += f'''
        <div class="stat-item">
            <div class="stat-value">{summary.get('total_displayed', 0):,}</div>
            <div class="stat-label">Total Discrepancies</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('acted_upon', 0):,}</div>
            <div class="stat-label">Acted Upon</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('samples_repeated', 0):,}</div>
            <div class="stat-label">Samples Repeated</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{summary.get('ignored', 0):,}</div>
            <div class="stat-label">Ignored</div>
        </div>'''
    
    html += '''
    </div>
    
    <div class="controls">
        <button onclick="expandAll()">Expand All</button>
        <button onclick="collapseAll()">Collapse All</button>
    </div>
    '''
    
    # Add mix sections
    for mix_name in sorted(mix_groups.keys()):
        categories = mix_groups[mix_name]
        total_in_mix = sum(len(errors) for errors in categories.values())
        
        html += f'''
    <div class="mix-section">
        <div class="mix-header" onclick="toggleMix(this)">
            <span>{mix_name}</span>
            <span>{total_in_mix:,} errors</span>
        </div>
        <div class="mix-content">
            <div class="category-tabs">'''
        
        # Add tabs for each category
        tab_idx = 0
        for category in sorted(categories.keys()):
            active = 'active' if tab_idx == 0 else ''
            count = len(categories[category])
            html += f'''
                <div class="tab {active}" onclick="switchTab(this, '{mix_name}_{category}')">{category.replace('_', ' ').title()} ({count})</div>'''
            tab_idx += 1
        
        html += '''
            </div>'''
        
        # Add tab content
        tab_idx = 0
        for category in sorted(categories.keys()):
            active = 'active' if tab_idx == 0 else ''
            errors_list = categories[category][:100]  # Limit to 100 per category
            truncated = len(categories[category]) > 100
            
            html += f'''
            <div class="tab-content {active}" id="{mix_name}_{category}">'''
            
            if truncated:
                html += f'''
                <div class="truncated-warning">
                    Showing first 100 of {len(categories[category])} errors
                </div>'''
            
            html += '''
                <table class="error-table">
                    <thead>
                        <tr>
                            <th>Sample</th>
                            <th>Well</th>
                            <th>Error Code</th>
                            <th>Message</th>
                            <th>Run</th>'''
            
            if report_type == 'discrepancy':
                html += '''
                            <th>Machine</th>
                            <th>Final</th>
                            <th>CT</th>'''
            
            html += '''
                            <th>LIMS Status</th>
                        </tr>
                    </thead>
                    <tbody>'''
            
            for error in errors_list:
                clinical_cat = error.get('clinical_category', category)
                html += f'''
                        <tr>
                            <td>{error.get('sample_name', '')}</td>
                            <td>{error.get('well_number', '')}</td>
                            <td>{error.get('error_code', '')}</td>
                            <td>{error.get('error_message', '')}</td>
                            <td>{error.get('run_name', '')}</td>'''
                
                if report_type == 'discrepancy':
                    html += f'''
                            <td>{error.get('machine_cls', '')}</td>
                            <td>{error.get('final_cls', '')}</td>
                            <td>{error.get('ct', '')}</td>'''
                
                html += f'''
                            <td class="{clinical_cat}">{error.get('lims_status', '')}</td>
                        </tr>'''
            
            html += '''
                    </tbody>
                </table>
            </div>'''
            tab_idx += 1
        
        html += '''
        </div>
    </div>'''
    
    # Add affected samples section for control report
    if report_type == 'control' and 'affected_samples' in data:
        html += '''
    <div class="mix-section">
        <div class="mix-header" onclick="toggleMix(this)">
            <span>Affected Patient Samples</span>
            <span>Due to Control Failures</span>
        </div>
        <div class="mix-content">'''
        
        affected = data['affected_samples']
        for group_key in sorted(affected.keys()):
            group = affected[group_key]
            error_count = len(group.get('affected_samples_error', {}))
            repeat_count = len(group.get('affected_samples_repeat', {}))
            
            if error_count > 0 or repeat_count > 0:
                html += f'''
            <h3>{group['run_name']} - {group['control_mix']}</h3>
            <p>Failed Controls: {len(group.get('controls', {}))} | Affected Errors: {error_count} | Affected Repeats: {repeat_count}</p>'''
    
        html += '''
        </div>
    </div>'''
    
    # Add JavaScript
    html += '''
    <script>
        function toggleMix(header) {
            const content = header.nextElementSibling;
            content.classList.toggle('active');
        }
        
        function switchTab(tab, contentId) {
            // Remove active from all tabs in this group
            const tabs = tab.parentElement.querySelectorAll('.tab');
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            
            // Hide all content in this mix
            const mixContent = tab.parentElement.parentElement;
            const contents = mixContent.querySelectorAll('.tab-content');
            contents.forEach(c => c.classList.remove('active'));
            
            // Show selected content
            const content = document.getElementById(contentId);
            if (content) {
                content.classList.add('active');
            }
        }
        
        function expandAll() {
            document.querySelectorAll('.mix-content').forEach(c => c.classList.add('active'));
        }
        
        function collapseAll() {
            document.querySelectorAll('.mix-content').forEach(c => c.classList.remove('active'));
        }
    </script>
</body>
</html>'''
    
    # Write HTML file
    with open(output_file, 'w') as f:
        f.write(html)
    
    print(f"Report generated: {output_file}")
    
    # Print summary
    print(f"\n=== {report_type.title()} Report Summary ===")
    for key, value in summary.items():
        if isinstance(value, (int, float)):
            print(f"  {key.replace('_', ' ').title()}: {value:,}")
        else:
            print(f"  {key.replace('_', ' ').title()}: {value}")

def main():
    parser = argparse.ArgumentParser(description='Generate unified HTML report from JSON data')
    parser.add_argument('json_file', 
                       help='Input JSON file (control_data.json, sample_data.json, or discrepancy_data.json)')
    parser.add_argument('--output', 
                       help='Output HTML file (default: auto-generated based on input)')
    
    args = parser.parse_args()
    
    # Auto-generate output filename if not provided
    if not args.output:
        if 'control' in args.json_file:
            args.output = 'control_report.html'
        elif 'sample' in args.json_file:
            args.output = 'sample_report.html'
        elif 'discrepancy' in args.json_file:
            args.output = 'discrepancy_report.html'
        else:
            args.output = 'report.html'
    
    # Load JSON data
    print(f"Loading data from: {args.json_file}")
    data = load_json_data(args.json_file)
    
    # Generate HTML
    generate_interactive_html(data, args.output)

if __name__ == '__main__':
    main()