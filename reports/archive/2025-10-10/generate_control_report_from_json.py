#!/usr/bin/env python3
"""
Generate control report HTML from JSON data
Reuses HTML generation code from generate_control_report_working.py
"""

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

def generate_interactive_html(all_errors, affected_samples, output_file, max_per_category=100):
    """Generate the interactive HTML report - copied from original"""
    
    # Group errors by mix and error_code
    errors_by_mix = defaultdict(lambda: defaultdict(list))
    
    for error in all_errors:
        mix_name = error['mix_name']
        error_code = error['error_code']
        errors_by_mix[mix_name][error_code].append(error)
    
    # Count by category
    unresolved_count = sum(1 for e in all_errors if e['clinical_category'] == 'unresolved')
    error_ignored_count = sum(1 for e in all_errors if e['clinical_category'] == 'error_ignored')
    test_repeated_count = sum(1 for e in all_errors if e['clinical_category'] == 'test_repeated')
    
    # Count affected samples
    affected_error_count = 0
    affected_repeat_count = 0
    for group in affected_samples.values():
        if 'affected_samples_error' in group:
            affected_error_count += len(group['affected_samples_error'])
        if 'affected_samples_repeat' in group:
            affected_repeat_count += len(group['affected_samples_repeat'])
    
    # Map control well IDs to their affected sample groups for linking
    control_to_group_map = {}
    for group_key, group_data in affected_samples.items():
        if 'controls' in group_data:
            anchor_id = f"affected-group-{group_key}"
            for control_id in group_data['controls'].keys():
                control_to_group_map[control_id] = anchor_id
    
    # Start HTML - this is the exact HTML structure from the original
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
        
        .control-toggle {
            margin: 0 10px 0 0;
        }
        
        .search-box {
            margin: 15px 0;
            padding: 8px;
            width: 300px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        
        .subcategory-content {
            display: none;
        }
        
        .subcategory-content.active {
            display: block;
        }
        
        .resolution-message {
            padding: 4px 8px;
            background: #e3f2fd;
            border-radius: 3px;
            font-size: 11px;
            color: #1565c0;
            margin-top: 5px;
            display: inline-block;
        }
        
        .note {
            background: #fff3e0;
            border-left: 4px solid #ff9800;
            padding: 10px;
            margin: 10px 0;
            font-size: 13px;
        }
        
        .toggle-all {
            background: #2196F3;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            margin: 10px 0;
        }
        
        .toggle-all:hover {
            background: #1976D2;
        }
        
        .summary-message {
            background: #e3f2fd;
            border: 1px solid #2196F3;
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
            text-align: center;
        }
        
        .affected-section {
            margin-top: 40px;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 5px;
        }
        
        .affected-header {
            font-size: 20px;
            font-weight: bold;
            margin-bottom: 15px;
            color: #333;
        }
        
        .affected-group {
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            margin: 15px 0;
            padding: 15px;
        }
        
        .affected-group-header {
            font-weight: bold;
            color: #2196F3;
            margin-bottom: 10px;
        }
        
        .affected-controls {
            background: #f0f0f0;
            padding: 10px;
            border-radius: 3px;
            margin: 10px 0;
        }
        
        .affected-samples {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 10px;
            margin-top: 10px;
        }
        
        .affected-sample-card {
            background: #fff;
            border: 1px solid #e0e0e0;
            padding: 8px;
            border-radius: 3px;
            font-size: 12px;
        }
        
        .link-to-affected {
            display: inline-block;
            padding: 2px 6px;
            background: #2196F3;
            color: white;
            border-radius: 3px;
            font-size: 11px;
            text-decoration: none;
            margin-left: 5px;
        }
        
        .link-to-affected:hover {
            background: #1976D2;
        }
    </style>
</head>
<body>
'''
    
    # Add header with stats
    html += f'''
    <div class="header">
        <h1>Control Error Report</h1>
        <p style="color: #666;">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="stats">
        <div class="stat-item">
            <div class="stat-value">{len(all_errors)}</div>
            <div class="stat-label">Total Control Errors</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{unresolved_count}</div>
            <div class="stat-label">Unresolved</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{error_ignored_count}</div>
            <div class="stat-label">Error Ignored</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">{test_repeated_count}</div>
            <div class="stat-label">Test Repeated</div>
        </div>
    </div>
    '''
    
    # Add toggle all button
    html += '''
    <button class="toggle-all" onclick="toggleAllMixes()">Expand/Collapse All</button>
    '''
    
    # Process each mix
    for mix_name in sorted(errors_by_mix.keys()):
        mix_errors = errors_by_mix[mix_name]
        
        # Count errors in this mix by category
        mix_all_errors = []
        for error_list in mix_errors.values():
            mix_all_errors.extend(error_list)
        
        mix_unresolved = sum(1 for e in mix_all_errors if e['clinical_category'] == 'unresolved')
        mix_ignored = sum(1 for e in mix_all_errors if e['clinical_category'] == 'error_ignored')
        mix_repeated = sum(1 for e in mix_all_errors if e['clinical_category'] == 'test_repeated')
        
        # Generate unique ID for this mix section
        mix_id = f"mix_{mix_name.replace(' ', '_')}"
        
        html += f'''
        <div class="mix-section" id="{mix_id}">
            <div class="mix-header" onclick="toggleMix('{mix_id}')">
                <span>
                    {mix_name} 
                    <span style="font-size: 14px; color: #666;">
                        ({len(mix_all_errors)} errors: {mix_unresolved} unresolved, {mix_ignored} ignored, {mix_repeated} repeated)
                    </span>
                </span>
                <span class="expand-icon">▶</span>
            </div>
            <div class="mix-content">
        '''
        
        # Add category tabs
        html += f'''
        <div class="category-tabs">
            <button class="category-tab active" onclick="showCategory('{mix_id}', 'all')">
                All<span class="category-badge">{len(mix_all_errors)}</span>
            </button>
            <button class="category-tab" onclick="showCategory('{mix_id}', 'unresolved')">
                Unresolved<span class="category-badge">{mix_unresolved}</span>
            </button>
            <button class="category-tab" onclick="showCategory('{mix_id}', 'error_ignored')">
                Error Ignored<span class="category-badge">{mix_ignored}</span>
            </button>
            <button class="category-tab" onclick="showCategory('{mix_id}', 'test_repeated')">
                Test Repeated<span class="category-badge">{mix_repeated}</span>
            </button>
        </div>
        '''
        
        # Process errors for each category
        categories = ['all', 'unresolved', 'error_ignored', 'test_repeated']
        
        for category in categories:
            if category == 'all':
                category_errors = mix_all_errors[:max_per_category]
            else:
                category_errors = [e for e in mix_all_errors if e['clinical_category'] == category][:max_per_category]
            
            display_style = '' if category == 'all' else 'display: none;'
            
            html += f'''
            <div class="category-content" data-category="{category}" style="{display_style}">
                <div class="container">
            '''
            
            # Add cards for each error
            for error in category_errors:
                well_id = error['well_id']
                error_code = error.get('error_code', '')
                
                # Determine card class based on clinical category
                card_class = 'card'
                if error['clinical_category'] == 'unresolved':
                    card_class += ' unresolved'
                elif error['clinical_category'] == 'error_ignored':
                    card_class += ' resolved'
                elif error['clinical_category'] == 'test_repeated':
                    card_class += ' resolved-with-new'
                
                # Add link to affected samples if this is a repeated test
                affected_link = ''
                if error['clinical_category'] == 'test_repeated' and well_id in control_to_group_map:
                    affected_link = f'''
                    <a href="#{control_to_group_map[well_id]}" class="link-to-affected">
                        View Affected Samples ↓
                    </a>
                    '''
                
                html += f'''
                <div class="{card_class}">
                    <div class="card-header">
                        {error['sample_name']} - Well {error['well_number']}
                        <span class="error-badge {error['clinical_category'].replace('_', '-')}">
                            {error['clinical_category'].replace('_', ' ').title()}
                        </span>
                    </div>
                    
                    <div class="card-details">
                        <div><strong>Error:</strong> {error_code}</div>
                        <div><strong>Message:</strong> {error.get('error_message', 'N/A')}</div>
                        <div><strong>Run:</strong> {error['run_name']}</div>
                        <div><strong>LIMS Status:</strong> {error.get('lims_status') or 'None'}</div>
                '''
                
                # Add resolution message if resolved
                if error['clinical_category'] in ['error_ignored', 'test_repeated']:
                    resolution_msg = get_resolution_message(error_code)
                    if resolution_msg:
                        html += f'''
                        <div class="resolution-message">
                            Resolution: {resolution_msg}
                        </div>
                        '''
                
                # Add link to affected samples
                if affected_link:
                    html += affected_link
                
                html += '''
                    </div>
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
    
    # Add affected samples section
    if affected_samples:
        html += '''
        <div class="affected-section">
            <div class="affected-header">Affected Patient Samples</div>
        '''
        
        for group_key, group_data in affected_samples.items():
            anchor_id = f"affected-group-{group_key}"
            
            html += f'''
            <div class="affected-group" id="{anchor_id}">
                <div class="affected-group-header">
                    Run: {group_data.get('run_name', 'Unknown')} | Mix: {group_data.get('control_mix', 'Unknown')}
                </div>
                
                <div class="affected-controls">
                    <strong>Failed Controls:</strong>
            '''
            
            # List the failed controls
            for control_id, control_info in group_data.get('controls', {}).items():
                html += f'''
                    <div style="margin-left: 20px;">
                        • {control_info.get('control_name', 'Unknown')} (Well {control_info.get('control_well', 'Unknown')}) - 
                        Resolution: {control_info.get('resolution') or 'None'}
                    </div>
                '''
            
            # Get all affected samples (both error and repeat)
            all_affected = []
            if 'affected_samples_error' in group_data:
                all_affected.extend(group_data['affected_samples_error'].values())
            if 'affected_samples_repeat' in group_data:
                all_affected.extend(group_data['affected_samples_repeat'].values())
            
            html += f'''
                </div>
                
                <div style="margin-top: 10px;">
                    <strong>Affected Patient Samples ({len(all_affected)} samples):</strong>
                    <div class="affected-samples">
            '''
            
            # Add affected sample cards
            for sample in all_affected:
                html += f'''
                    <div class="affected-sample-card">
                        <div><strong>{sample['sample_name']}</strong></div>
                        <div>Well: {sample['well_number']}</div>
                        <div>Status: {sample.get('lims_status') or 'None'}</div>
                    </div>
                '''
            
            html += '''
                    </div>
                </div>
            </div>
            '''
        
        html += '''
        </div>
        '''
    
    # Add JavaScript
    html += '''
    <script>
        function toggleMix(mixId) {
            const mixSection = document.getElementById(mixId);
            mixSection.classList.toggle('expanded');
        }
        
        function toggleAllMixes() {
            const mixSections = document.querySelectorAll('.mix-section');
            const allExpanded = Array.from(mixSections).every(s => s.classList.contains('expanded'));
            
            mixSections.forEach(section => {
                if (allExpanded) {
                    section.classList.remove('expanded');
                } else {
                    section.classList.add('expanded');
                }
            });
        }
        
        function showCategory(mixId, category) {
            const mixSection = document.getElementById(mixId);
            
            // Update active tab
            const tabs = mixSection.querySelectorAll('.category-tab');
            tabs.forEach(tab => {
                if (tab.textContent.toLowerCase().includes(category.replace('_', ' '))) {
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
                } else {
                    content.style.display = 'none';
                }
            });
        }
        
        // Auto-expand first mix on load
        document.addEventListener('DOMContentLoaded', function() {
            const firstMix = document.querySelector('.mix-section');
            if (firstMix) {
                firstMix.classList.add('expanded');
            }
        });
    </script>
</body>
</html>
    '''
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    
    return len(all_errors)

def main():
    parser = argparse.ArgumentParser(description='Generate control report HTML from JSON data')
    parser.add_argument('--json', required=True, help='Path to JSON data file')
    parser.add_argument('--output', default='output_data/control_report_from_json.html',
                       help='Output HTML file path')
    parser.add_argument('--max-per-category', type=int, default=100,
                       help='Maximum records to show per category (default: 100)')
    
    args = parser.parse_args()
    
    # Load JSON data
    print(f"Loading JSON data from: {args.json}")
    with open(args.json, 'r') as f:
        data = json.load(f)
    
    # Extract errors and affected samples
    all_errors = data.get('errors', [])
    affected_samples = data.get('affected_samples', {})
    
    if not all_errors:
        print("No errors found in JSON data")
        return
    
    print(f"Loaded {len(all_errors)} errors")
    print(f"Found {len(affected_samples)} affected sample groups")
    
    # Generate HTML report
    print("\nGenerating interactive HTML report...")
    print(f"Maximum records per category: {args.max_per_category}")
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    total = generate_interactive_html(all_errors, affected_samples, args.output, args.max_per_category)
    
    print(f"\nReport generated successfully:")
    print(f"  Output file: {args.output}")
    print(f"  Total errors: {total}")

if __name__ == '__main__':
    main()