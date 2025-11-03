#!/usr/bin/env python3
"""
Generate control report HTML from JSON data - Complete version
Maintains exact same HTML structure as generate_control_report_working.py
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

def generate_interactive_html(errors, affected_groups, well_curves, output_file, max_per_category=100):
    """Generate interactive HTML with JavaScript controls - with real curve data"""
    
    # Group by mix and clinical category
    mix_groups = defaultdict(lambda: defaultdict(list))
    for error in errors:
        clinical_cat = error.get('clinical_category', error.get('category', 'unresolved'))
        mix_groups[error['mix_name']][clinical_cat].append(error)
    
    # Prepare control-to-group mapping for affected samples
    control_to_group_map = {}
    for group_key, group_data in affected_groups.items():
        anchor_id = f"affected-group-{group_key}"
        for control_id in group_data.get('controls', {}).keys():
            control_to_group_map[control_id] = anchor_id
    
    # Start HTML - exact copy of CSS and JavaScript from original
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
        const currentTargets = {};
        
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
                } else {
                    content.style.display = 'none';
                }
            });
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
                        // Expand the section if it's collapsed
                        parent.classList.add('expanded');
                    }
                    // Scroll to the element after a short delay
                    setTimeout(() => {
                        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    }, 100);
                }
            }
        }
        
        function updateTargetForRecord(wellId, targetName) {
            currentTargets[wellId] = targetName;
            const container = document.querySelector(`.svg-container[data-record-id="${wellId}"]`);
            if (container && curveData[wellId]) {
                container.innerHTML = generateSVGWithControls(wellId);
            }
        }
        
        function generateSVGWithControls(wellId) {
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
            
            // Add control readings
            if (data.controls) {
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
            
            // Draw control curves (underneath main curve)
            if (data.controls) {
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
        
        // Initialize on page load
        document.addEventListener('DOMContentLoaded', function() {
            // Generate all SVGs
            const containers = document.querySelectorAll('.svg-container[data-record-id]');
            containers.forEach(container => {
                const wellId = container.getAttribute('data-record-id');
                if (wellId && curveData[wellId]) {
                    container.innerHTML = generateSVGWithControls(wellId);
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
    
    # Calculate appendix totals
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
                records = sorted(records, key=lambda r: (r.get('error_code', ''), r.get('sample_name', '')))
            elif category in ['error_ignored', 'test_repeated']:
                records = sorted(records, key=lambda r: (r.get('lims_status') or '', r.get('sample_name', '')))
            
            records_to_show = records[:max_per_category] if max_per_category > 0 else records
            
            for record in records_to_show:
                well_id = record['well_id']
                
                # Get well curve data
                well_data = well_curves.get(well_id)
                
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
                
                # Add graph with real data if available
                if well_data and well_data.get('targets'):
                    main_target = well_data.get('main_target')
                    targets = well_data.get('targets', {})
                    controls = well_data.get('controls', [])
                    
                    # Store data in JavaScript for this well
                    js_data = json.dumps({
                        'main_target': main_target,
                        'targets': targets,
                        'controls': controls
                    })
                    
                    html += f'''
                    <script>
                    curveData["{well_id}"] = {js_data};
                    currentTargets["{well_id}"] = "{main_target or ''}";
                    </script>
                    '''
                    
                    # Add target selector if multiple targets
                    if len(targets) > 1:
                        html += f'''
                        <div class="target-selector">
                            Target: 
                            <select class="target-select" onchange="updateTargetForRecord('{well_id}', this.value)">
                        '''
                        for target_name, target_data in targets.items():
                            selected = 'selected' if target_name == main_target else ''
                            ct_val = target_data.get('ct')
                            ct_str = f" (CT: {ct_val:.1f})" if ct_val else ""
                            html += f'<option value="{target_name}" {selected}>{target_name}{ct_str}</option>'
                        html += '''
                            </select>
                        </div>
                        '''
                    
                    html += f'''
                    <div class="svg-container" data-record-id="{well_id}">
                        <!-- SVG will be generated by JavaScript -->
                    </div>
                    '''
                else:
                    # No curve data available, show placeholder
                    html += f'''
                    <div class="svg-container" data-record-id="{well_id}">
                        <svg width="300" height="150">
                            <rect width="300" height="150" fill="white" stroke="#eee"/>
                            <text x="150" y="75" text-anchor="middle" fill="#999">No curve data</text>
                        </svg>
                    </div>
                    '''
                
                html += f'''
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
                else:
                    html += f'''
                        <div class="error-badge {category}">{record['error_code']}</div>'''
                
                html += '''
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
            if group_data.get('affected_samples_error'):
                # Format control info
                control_info = ', '.join([f"{ctrl.get('control_name', 'Unknown')}-Well {ctrl.get('control_well', 'Unknown')}" 
                                        for ctrl in group_data.get('controls', {}).values()])
                
                error_groups[group_key] = {
                    'run_name': group_data.get('run_name', 'Unknown'),
                    'mix_name': group_data.get('control_mix', 'Unknown'),
                    'control_info': control_info,
                    'control_ids': list(group_data.get('controls', {}).keys()),
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
                samples = group_data['samples']
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
            if group_data.get('affected_samples_repeat'):
                # Format control info
                control_info = ', '.join([f"{ctrl.get('control_name', 'Unknown')}-Well {ctrl.get('control_well', 'Unknown')}" 
                                        for ctrl in group_data.get('controls', {}).values()])
                
                repeat_groups[group_key] = {
                    'run_name': group_data.get('run_name', 'Unknown'),
                    'mix_name': group_data.get('control_mix', 'Unknown'),
                    'control_info': control_info,
                    'control_ids': list(group_data.get('controls', {}).keys()),
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
                samples = group_data['samples']
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
    parser = argparse.ArgumentParser(description='Generate control report HTML from JSON data')
    parser.add_argument('--json', required=True, help='Path to JSON data file')
    parser.add_argument('--output', default='output_data/control_report_from_json_complete.html',
                       help='Output HTML file path')
    parser.add_argument('--max-per-category', type=int, default=100,
                       help='Maximum records to show per category (default: 100)')
    
    args = parser.parse_args()
    
    # Load JSON data
    print(f"Loading JSON data from: {args.json}")
    with open(args.json, 'r') as f:
        data = json.load(f)
    
    # Extract errors, affected samples, and well curves
    all_errors = data.get('errors', [])
    affected_samples = data.get('affected_samples', {})
    well_curves = data.get('well_curves', {})
    
    if not all_errors:
        print("No errors found in JSON data")
        return
    
    print(f"Loaded {len(all_errors)} errors")
    print(f"Found {len(affected_samples)} affected sample groups")
    print(f"Found {len(well_curves)} well curves")
    
    # Generate HTML report
    print("\nGenerating interactive HTML report...")
    print(f"Maximum records per category: {args.max_per_category}")
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    total = generate_interactive_html(all_errors, affected_samples, well_curves, args.output, args.max_per_category)
    
    print(f"\nReport generated successfully:")
    print(f"  Output file: {args.output}")
    print(f"  Total errors: {total}")

if __name__ == '__main__':
    main()