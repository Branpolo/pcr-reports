#!/usr/bin/env python3
"""
Generate HTML reports from JSON data - Supports both control and discrepancy reports
Maintains exact same HTML structure as original report generators
"""

import json
import argparse
import os
import tempfile
from datetime import datetime
from collections import defaultdict
import html as html_std

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

REPORT_TITLE_MAP = {
    'control': 'Control Errors',
    'sample': 'Sample SOP Errors',
    'discrepancy': 'Classification Errors',
}

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

def generate_interactive_html(
    errors,
    affected_groups,
    well_curves,
    output_file,
    report_type='control',
    max_per_category=100,
    metadata=None,
    embed=False,
):
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
    since_date = None
    date_field = None
    if metadata:
        since_date = metadata.get('since_date')
        date_field = metadata.get('date_field')

    title_text = REPORT_TITLE_MAP.get(report_type, report_type.title())
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>''' + title_text + '''</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 10px;
            background-color: #f5f5f5;
        }

        body.embedded-report {
            margin: 0;
            background-color: transparent;
        }

        body.embedded-report .header {
            display: none;
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
        /* Also auto-show a section when a descendant is the :target (CSS-based fallback) */
        .mix-section:has(:target) .mix-content {
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

        .control-toggle-btn {
            padding: 6px 14px;
            font-size: 12px;
            background: #2e7d32;
            color: white;
            border: 1px solid #1b5e20;
            border-radius: 4px;
            cursor: pointer;
        }

        .control-toggle-btn:hover {
            background: #1b5e20;
        }

        svg {
            border: 1px solid #eee;
        }
    </style>
    <script>
        // Store all curve data
        const curveData = {};
        const currentTargets = {};
        // Track control visibility per well and per section
        let controlsVisible = {};
        let sectionControlsVisible = {};

        function notifyParentResize() {
            if (window.parent && window.parent !== window && window.parent.postMessage) {
                window.parent.postMessage({ type: 'nested-report-resize' }, '*');
            }
        }
        
        // Expand/collapse functions
        function toggleSection(mixAnchor) {
            const section = document.getElementById('mix-' + mixAnchor);
            const willExpand = !section.classList.contains('expanded');
            section.classList.toggle('expanded');
            if (willExpand) {
                const firstTab = section.querySelector('.category-tab');
                if (firstTab && firstTab.dataset && firstTab.dataset.category) {
                    showCategory(mixAnchor, firstTab.dataset.category);
                }
            }
            notifyParentResize();
        }

        function expandAll() {
            document.querySelectorAll('.mix-section').forEach(section => {
                section.classList.add('expanded');
            });
            notifyParentResize();
        }

        function collapseAll() {
            document.querySelectorAll('.mix-section').forEach(section => {
                section.classList.remove('expanded');
            });
            notifyParentResize();
        }
        
        // Auto-expand section when navigating from TOC
        function navigateToSection(mixAnchor, event) {
            if (event && event.preventDefault) {
                event.preventDefault();
            }
            const section = document.getElementById('mix-' + mixAnchor);
            if (section) {
                const wasExpanded = section.classList.contains('expanded');
                if (!wasExpanded) {
                    section.classList.add('expanded');
                    // Force layout recalculation, then scroll
                    void section.offsetHeight;
                    setTimeout(() => {
                        section.scrollIntoView({ behavior: 'smooth', block: 'start' });
                        notifyParentResize();
                    }, 350);
                } else {
                    // Already expanded, scroll immediately
                    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    notifyParentResize();
                }
            }
            return false;
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
            notifyParentResize();
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
                        notifyParentResize();
                    }, 100);
                }
            }
        }
        
        function updateTargetForRecord(wellId, targetName) {
            currentTargets[wellId] = targetName;
            const container = document.querySelector(`.svg-container[data-record-id="${wellId}"]`);
            if (container && curveData[wellId]) {
                const showCtrls = controlsVisible[wellId] === true;
                container.innerHTML = generateSVGWithControls(wellId, showCtrls);
                notifyParentResize();
            }
        }

        function generateSVGWithControls(wellId, showControls=false) {
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
            
            // Collect all readings for scaling (respect control visibility)
            let allReadings = [];
            // Add main curve readings
            const mainReadings = targetData.readings.filter(r => r !== null && r !== undefined);
            allReadings = allReadings.concat(mainReadings);
            // Add control readings only if visible (use per-target controls if available, else top-level)
            if (showControls) {
                const controlsToUse = targetData.controls || data.controls;
                if (controlsToUse) {
                    controlsToUse.forEach(ctrl => {
                        if (ctrl.readings) {
                            const validReadings = ctrl.readings.filter(r => r !== null && r !== undefined);
                            allReadings = allReadings.concat(validReadings);
                        }
                    });
                }
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

            // Add vertical gridlines and x-axis labels every 10 cycles
            const totalCycles = targetData.readings.length;
            // Add label for cycle 0
            svg += '<text x="' + marginLeft + '" y="' + (height - marginBottom + 12) + '" text-anchor="middle" font-size="8" fill="#999">0</text>';
            // Add gridlines and labels for cycles 10, 20, 30, etc.
            for (let cycle = 10; cycle < totalCycles; cycle += 10) {
                const x = marginLeft + (cycle * plotWidth / (totalCycles - 1 || 1));
                // Vertical gridline (faint)
                svg += '<line x1="' + x + '" y1="' + marginTop + '" x2="' + x + '" y2="' + (marginTop + plotHeight) + '" stroke="#e0e0e0" stroke-width="1" opacity="0.5"/>';
                // X-axis label
                svg += '<text x="' + x + '" y="' + (height - marginBottom + 12) + '" text-anchor="middle" font-size="8" fill="#999">' + cycle + '</text>';
            }

            // Draw control curves (underneath main curve) - use per-target controls if available, else top-level
            if (showControls) {
                const controlsToUse = targetData.controls || data.controls;
                if (controlsToUse) {
                    controlsToUse.forEach(ctrl => {
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
            // Initialize section control states to hidden
            document.querySelectorAll('.mix-section').forEach(section => {
                const mixAnchor = section.id.replace('mix-', '');
                sectionControlsVisible[mixAnchor] = false;
                const btnText = document.getElementById('ctrl-text-' + mixAnchor);
                if (btnText) {
                    btnText.textContent = 'Show Controls';
                }
            });
            // Generate all SVGs with controls hidden by default
            const containers = document.querySelectorAll('.svg-container[data-record-id]');
            containers.forEach(container => {
                const wellId = container.getAttribute('data-record-id');
                if (wellId && curveData[wellId]) {
                    controlsVisible[wellId] = false;
                    container.innerHTML = generateSVGWithControls(wellId, false);
                }
            });
            
            // Auto-expand for initial hash
            autoExpandForAnchor();
            notifyParentResize();
        });
        
        // Toggle controls for entire section
        function toggleControlsForSection(mixAnchor) {
            const section = document.getElementById('mix-' + mixAnchor);
            const isVisible = sectionControlsVisible[mixAnchor] === true;
            sectionControlsVisible[mixAnchor] = !isVisible;
            const btnText = document.getElementById('ctrl-text-' + mixAnchor);
            if (btnText) {
                btnText.textContent = sectionControlsVisible[mixAnchor] ? 'Hide Controls' : 'Show Controls';
            }
            section.querySelectorAll('.svg-container[data-record-id]').forEach(container => {
                const wellId = container.getAttribute('data-record-id');
                if (wellId && curveData[wellId]) {
                    controlsVisible[wellId] = sectionControlsVisible[mixAnchor];
                    container.innerHTML = generateSVGWithControls(wellId, sectionControlsVisible[mixAnchor]);
                }
            });
            notifyParentResize();
        }

        // Ensure target's parent section is expanded (used by affected samples links)
        function ensureVisibleAnchor(anchorId, event) {
            if (event && event.preventDefault) {
                event.preventDefault();
            }
            const target = document.getElementById(anchorId);
            if (!target) return false;
            const parent = target.closest('.mix-section');
            const wasExpanded = parent ? parent.classList.contains('expanded') : true;
            if (parent && !wasExpanded) {
                parent.classList.add('expanded');
                // Force layout recalculation, then scroll
                void parent.offsetHeight;
                setTimeout(() => {
                    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    notifyParentResize();
                }, 350);
            } else {
                // Already expanded, scroll immediately
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
                notifyParentResize();
            }
            return false;
        }
        // Auto-expand on hash change
        window.addEventListener('hashchange', function() {
            autoExpandForAnchor();
            setTimeout(notifyParentResize, 200);
        });
    </script>
</head>
<body''' + (' class="embedded-report"' if embed else '') + '''>
    <div class="header">
        <h1>''' + REPORT_TITLE_MAP.get(report_type, report_type.title()) + '''</h1>
        <p>Generated: ''' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '''</p>'''
    if since_date:
        label = 'extraction date'
        if date_field == 'upload':
            label = 'upload date'
        elif date_field == 'extraction':
            label = 'extraction date'
        html += f'''\n        <p>Filtered since {since_date} ({label})</p>'''
    html += '''
    </div>
    
    <div style="text-align: center; margin: 20px 0;">
        <button onclick="expandAll()" style="padding: 10px 20px; margin: 0 5px; background: #2196F3; color: white; border: none; border-radius: 5px; cursor: pointer;">Expand All</button>
        <button onclick="collapseAll()" style="padding: 10px 20px; margin: 0 5px; background: #2196F3; color: white; border: none; border-radius: 5px; cursor: pointer;">Collapse All</button>
    </div>
    '''
    
    # Different stats for different report types
    if report_type == 'discrepancy':
        html += '''
    <div class="stats">
        <div class="stat-item">
            <div class="stat-value">''' + str(len(errors)) + '''</div>
            <div class="stat-label">Total Discrepancies</div>
        </div>
        <div class="stat-item">
            <div class="stat-value" style="color: #388e3c;">''' + str(sum(1 for e in errors if e.get('clinical_category') == 'acted_upon')) + '''</div>
            <div class="stat-label">Changed Results</div>
        </div>
        <div class="stat-item">
            <div class="stat-value" style="color: #f57c00;">''' + str(sum(1 for e in errors if e.get('clinical_category') == 'samples_repeated')) + '''</div>
            <div class="stat-label">Samples Repeated</div>
        </div>
        <div class="stat-item">
            <div class="stat-value" style="color: #666;">''' + str(sum(1 for e in errors if e.get('clinical_category') == 'ignored')) + '''</div>
            <div class="stat-label">Error Ignored</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">''' + str(len(mix_groups)) + '''</div>
            <div class="stat-label">Affected Mixes</div>
        </div>
    </div>'''
    else:
        html += '''
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
    </div>'''
    
    html += '''
    <!-- Table of Contents -->
    <div style="background: white; padding: 15px; margin: 20px 0; border-radius: 5px;">
        <h2 style="margin-top: 0;">Table of Contents</h2>
        <ul style="list-style: none; padding: 0;">'''
    
    # Add TOC entries with detailed counts
    for mix_name, categories in sorted(mix_groups.items()):
        total_mix_errors = sum(len(records) for records in categories.values())
        
        if report_type == 'discrepancy':
            acted_count = len(categories.get('acted_upon', []))
            repeated_count = len(categories.get('samples_repeated', []))
            ignored_count = len(categories.get('ignored', []))
        else:
            unresolved_count = len(categories.get('unresolved', []))
            ignored_count = len(categories.get('error_ignored', []))
            repeated_count = len(categories.get('test_repeated', []))
        
        mix_anchor = mix_name.replace(" ", "_").replace("/", "_")
        html += f'''
            <li style="margin: 8px 0;">
                            <a href="#mix-{mix_anchor}" onclick="return navigateToSection('{mix_anchor}', event)" style="text-decoration: none; color: #2196F3; display: block;">
                    <div style="display: flex; justify-content: space-between; align-items: center; position: relative;">
                        <span style="background: white; padding-right: 10px; z-index: 1; position: relative;">{mix_name}</span>
                        <div style="position: absolute; left: 0; right: 0; top: 50%; border-bottom: 1px dotted #ccc; z-index: 0;"></div>
                        <span style="font-size: 12px; color: #666; background: white; padding-left: 10px; z-index: 1; position: relative; white-space: nowrap;">
                            Total: {total_mix_errors} | '''
        
        if report_type == 'discrepancy':
            html += f'''
                            <span style="color: #388e3c;">Changed: {acted_count}</span> | 
                            <span style="color: #f57c00;">Repeated: {repeated_count}</span> | 
                            <span style="color: #666;">Ignored: {ignored_count}</span>'''
        else:
            html += f'''
                            <span style="color: #d32f2f;">Unresolved: {unresolved_count}</span> | 
                            <span style="color: #388e3c;">Ignored: {ignored_count}</span> | 
                            <span style="color: #f57c00;">Repeated: {repeated_count}</span>'''
        
        html += '''
                        </span>
                    </div>
                </a>
            </li>'''
    
    # Calculate appendix totals (only for control reports)
    if report_type == 'control' and affected_groups:
        error_samples_total = 0
        repeat_samples_total = 0
        unique_error_samples = set()
        unique_repeat_samples = set()
        
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
                            <a href="#mix-appendix-error" onclick="return navigateToSection('appendix-error', event)" style="text-decoration: none; color: #2196F3;">
                                &rarr; ERROR - Active Failed Samples ({error_samples_total})
                            </a>
                        </li>
                        <li style="margin: 5px 0;">
                            <a href="#mix-appendix-repeats" onclick="return navigateToSection('appendix-repeats', event)" style="text-decoration: none; color: #2196F3;">
                                &rarr; REPEATS - Resolved Samples ({repeat_samples_total})
                            </a>
                        </li>
                    </ul>
                </li>'''
    
    html += '''
        </ul>
    </div>
'''
    
    # Process each mix
    mix_id = 0
    for mix_name, categories in sorted(mix_groups.items()):
        mix_id += 1
        total_mix_errors = sum(len(records) for records in categories.values())

        # Determine if this section has any control overlays available
        section_has_controls = False
        try:
            for recs in categories.values():
                for r in recs:
                    wid = r.get('well_id')
                    wd = well_curves.get(str(wid)) or well_curves.get(wid)
                    if not wd:
                        continue
                    # Direct controls (control reports)
                    if wd.get('controls'):
                        section_has_controls = True
                        break
                    # Targets with control_curves (sample/discrepancy)
                    tgs = wd.get('targets')
                    if isinstance(tgs, list):
                        for t in tgs:
                            if t.get('control_curves'):
                                section_has_controls = True
                                break
                        if section_has_controls:
                            break
                if section_has_controls:
                    break
        except Exception:
            section_has_controls = False

        mix_anchor = mix_name.replace(" ", "_").replace("/", "_")
        html += f'''
        <div class="mix-section{' expanded' if mix_id == 1 else ''}" id="mix-{mix_anchor}">
            <div class="mix-header">
                <div style="flex: 1; display: flex; align-items: center; gap: 15px;">
                    <div style="cursor: pointer; flex: 1;" onclick="toggleSection('{mix_anchor}')">
                        <span>Mix: {mix_name}</span>
                        <span style="font-size: 14px; color: #666; margin-left: 10px;">Total errors: {total_mix_errors}</span>
                    </div>
                    {(
                        f"<button class=\"control-toggle-btn\" type=\"button\" onclick=\"toggleControlsForSection('{mix_anchor}')\">"
                        f"<span id=\"ctrl-text-{mix_anchor}\">Show Controls</span>"
                        '</button>'
                    ) if section_has_controls else '<span style="font-size: 11px; color: #999;">No control overlays</span>'}
                </div>
                <span class="expand-icon" style="cursor: pointer;" onclick="toggleSection('{mix_anchor}')">&#9654;</span>
            </div>
            <div class="mix-content">
            
            <div class="category-tabs">
        '''
        
        # Add category tabs for clinical categories
        first_category = True
        if report_type == 'discrepancy':
            clinical_categories = [
                ('acted_upon', 'Changed Results'),
                ('samples_repeated', 'Samples Repeated'),
                ('ignored', 'Error Ignored')
            ]
        else:
            clinical_categories = [
                ('test_repeated', 'Test Repeated'),
                ('unresolved', 'Unresolved'),
                ('error_ignored', 'Error Ignored')
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
        
        # Add category content in a stable order matching tab order
        first_category = True
        ordered_keys = []
        for cat_key, _ in clinical_categories:
            if cat_key in categories:
                ordered_keys.append(cat_key)
        for category in ordered_keys:
            records = categories[category]
            display = 'block' if first_category else 'none'
            showing_text = f" (showing {min(len(records), max_per_category)} of {len(records)})" if len(records) > max_per_category else ""
            
            if report_type == 'discrepancy':
                category_labels = {
                    'acted_upon': 'Changed Results (Discrepancies Acted Upon)',
                    'samples_repeated': 'Samples Repeated',
                    'ignored': 'Error Ignored (Discrepancies Not Acted Upon)'
                }
            else:
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
            
            # When rendering, optionally group by subkeys within discrepancy report
            group_by_lims = (report_type == 'discrepancy' and category in ['samples_repeated', 'ignored'])
            group_by_final = (report_type == 'discrepancy' and category == 'acted_upon')

            # Container wrapper (used when not grouping)
            if not (group_by_lims or group_by_final):
                html += '''
                <div class="container">
                '''

            # Process records (limited for performance if needed)
            # Sort for visual grouping
            if category == 'unresolved':
                records = sorted(records, key=lambda r: (r.get('error_code') or '', r.get('sample_name') or ''))
            elif category in ['error_ignored', 'test_repeated']:
                records = sorted(records, key=lambda r: (r.get('lims_status') or '', r.get('sample_name') or ''))
            elif group_by_lims:
                # For discrepancy Samples Repeated, sort by LIMS status then sample name
                records = sorted(records, key=lambda r: ((r.get('lims_status') or 'UNKNOWN'), r.get('sample_name', '')))
            elif group_by_final:
                # For discrepancy Changed Results, sort by final class then sample name
                records = sorted(records, key=lambda r: (r.get('final_cls', -1), r.get('sample_name', '')))

            records_to_show = records if max_per_category == 0 else records[:max_per_category]

            # Optionally group by LIMS output for discrepancy repeated/ignored samples
            if group_by_lims:
                buckets = defaultdict(list)
                for rec in records_to_show:
                    status = (rec.get('lims_status') or 'UNKNOWN').upper()
                    buckets[status].append(rec)

                # Stable order: DETECTED/NOT DETECTED, then others alpha
                ordered_keys = []
                for k in ['DETECTED', 'NOT DETECTED']:
                    if k in buckets:
                        ordered_keys.append(k)
                other_keys = sorted([k for k in buckets.keys() if k not in ('DETECTED', 'NOT DETECTED')])
                ordered_keys.extend(other_keys)

                for key in ordered_keys:
                    subset = buckets[key]
                    html += f'''<div style="margin: 6px 0 6px 0; color: #555; font-size: 12px; font-weight: bold;">LIMS Output: {key} ({len(subset)})</div>'''
                    html += '''<div class="container">'''
                    for record in subset:
                        well_id = record['well_id']
                        
                        # Get well curve data - try both string and int keys
                        well_data = well_curves.get(str(well_id)) or well_curves.get(well_id)
                        
                        # Determine card color class
                        if category == 'samples_repeated':
                            card_class = 'resolved-excluded'
                        else:
                            # ignored: color by LIMS
                            ls = (record.get('lims_status') or '').upper()
                            if ls == 'DETECTED':
                                card_class = 'resolved-detected'
                            elif ls == 'NOT DETECTED':
                                card_class = 'resolved-not-detected'
                            else:
                                card_class = 'resolved-other'
                        
                        html += f'''
                        <div class="card {card_class}">
                            <div class="card-header">
                                <span>{record['sample_name']} - Well {record['well_number']}</span>
                            </div>
                        '''
                        
                        # Add graph with real data if available
                        if well_data and well_data.get('targets'):
                            targets = well_data.get('targets', {})
                            controls = well_data.get('controls', [])

                            if isinstance(targets, list):
                                # Try to get main_target from well_data first
                                main_target = well_data.get('main_target')

                                targets_dict = {}
                                fallback_main_target = None
                                for target in targets:
                                    target_name = target.get('target_name', 'Unknown')
                                    target_controls = target.get('control_curves', [])
                                    is_ic = target.get('is_ic', 0)
                                    formatted_controls = []
                                    for ctrl in target_controls:
                                        ctype_raw = ctrl.get('control_type') or ctrl.get('type')
                                        ctype_up = ctype_raw.upper() if isinstance(ctype_raw, str) else ''
                                        mapped_type = 'control'
                                        if ctype_up in ('PC', 'POSITIVE'):
                                            mapped_type = 'positive'
                                        elif ctype_up in ('NC', 'NEGATIVE', 'NTC'):
                                            mapped_type = 'negative'
                                        # For IC, negative controls are actually positive (no sample interference)
                                        if is_ic and mapped_type == 'negative':
                                            mapped_type = 'positive'
                                        formatted_controls.append({
                                            'readings': ctrl.get('readings', []),
                                            'type': mapped_type,
                                            'ct': ctrl.get('machine_ct') or ctrl.get('ct')
                                        })
                                    targets_dict[target_name] = {
                                        'readings': target.get('readings', []),
                                        'ct': target.get('machine_ct') or target.get('ct'),
                                        'is_ic': is_ic,
                                        'controls': formatted_controls
                                    }
                                    if fallback_main_target is None and not is_ic:
                                        fallback_main_target = target_name

                                # Use fallback only if main_target wasn't provided
                                if main_target is None:
                                    main_target = fallback_main_target or (list(targets_dict.keys())[0] if targets_dict else None)
                                targets = targets_dict
                            else:
                                main_target = well_data.get('main_target')
                                if not isinstance(targets, dict):
                                    targets = {}
                            # Include top-level controls for control reports (backward compatibility)
                            js_data = json.dumps({'main_target': main_target, 'targets': targets, 'controls': controls})
                            html += f'''<script>curveData["{well_id}"] = {js_data}; currentTargets["{well_id}"] = "{main_target or ''}";</script>'''

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

                            html += f'''<div class="svg-container" data-record-id="{well_id}"></div>'''

                            # Display passive normalization status
                            passive_status = well_data.get('passive_status')
                            if passive_status == 'normalized':
                                html += '<div style="font-size: 9px; color: #0066cc; background: #e6f2ff; padding: 3px 6px; margin: 4px 0; border-radius: 3px; border-left: 3px solid #0066cc;">✓ Normalized with passive dye</div>'
                            elif passive_status == 'expected_but_missing':
                                html += '<div style="font-size: 9px; color: #cc6600; background: #fff3e6; padding: 3px 6px; margin: 4px 0; border-radius: 3px; border-left: 3px solid #cc6600;">⚠ Passive dye normalization expected but passive target missing/failed</div>'

                            comments = (well_data.get('comments') or [])[:3]
                            if comments:
                                html += '<div style="margin-top: 6px;">'
                                for c in comments:
                                    ctext = (c.get('text') or '').replace('\n', '<br>')
                                    html += f'<div style="font-size: 10px; color: #666; background: #f5f5f5; padding: 4px 6px; margin: 4px 0; border-radius: 3px;">{ctext}</div>'
                                html += '</div>'
                        else:
                            html += f'''<div class="svg-container" data-record-id="{well_id}"><svg width="300" height="150"><rect width="300" height="150" fill="white" stroke="#eee"/><text x="150" y="75" text-anchor="middle" fill="#999">No curve data</text></svg></div>'''

                        # Details: emphasize LIMS output; for ignored also include classification
                        lims = (record.get('lims_status') or 'UNKNOWN')
                        html += f'''<div class="card-details">Run: {record.get('run_name', record.get('run_id', 'Unknown'))}<br>Date: {record.get('extraction_date') or 'N/A'}<br>LIMS Output: <strong>{lims}</strong>'''
                        if category == 'ignored':
                            machine_cls = record.get('machine_cls', 'N/A')
                            final_cls = record.get('final_cls', 'N/A')
                            if machine_cls != 'N/A' and final_cls != 'N/A':
                                machine_result = 'POS' if machine_cls == 1 else 'NEG'
                                final_result = 'POS' if final_cls == 1 else 'NEG'
                                html += f'<br>Machine: {machine_result} &rarr; Final: {final_result}'
                        if record.get('error_code'):
                            html += f'<br>Error: {record.get("error_message", record["error_code"])}'

                        html += '</div></div>'  # close card-details and card
                    html += '</div>'  # close container for this LIMS bucket

            # Optionally group by final classification for discrepancy changed results
            elif group_by_final:
                buckets = defaultdict(list)
                for rec in records_to_show:
                    fc = rec.get('final_cls')
                    if fc == 1:
                        key = 'POS'
                    elif fc == 0:
                        key = 'NEG'
                    else:
                        key = 'UNKNOWN'
                    buckets[key].append(rec)

                # Order POS, NEG, then UNKNOWN
                for key in [k for k in ['POS', 'NEG', 'UNKNOWN'] if k in buckets]:
                    subset = buckets[key]
                    html += f'''<div style="margin: 6px 0 6px 0; color: #555; font-size: 12px; font-weight: bold;">Final: {key} ({len(subset)})</div>'''
                    html += '''<div class="container">'''
                    for record in subset:
                        well_id = record['well_id']

                        well_data = well_curves.get(str(well_id)) or well_curves.get(well_id)

                        # Card color based on final classification
                        card_class = 'resolved-detected' if key == 'POS' else 'resolved-not-detected' if key == 'NEG' else 'resolved-other'

                        html += f'''
                        <div class="card {card_class}">
                            <div class="card-header">
                                <span>{record['sample_name']} - Well {record['well_number']}</span>
                            </div>
                        '''

                        # Graph content
                        if well_data and well_data.get('targets'):
                            targets = well_data.get('targets', {})
                            controls = well_data.get('controls', [])
                            if isinstance(targets, list):
                                # Try to get main_target from well_data first
                                main_target = well_data.get('main_target')

                                targets_dict = {}
                                fallback_main_target = None
                                for target in targets:
                                    target_name = target.get('target_name', 'Unknown')
                                    target_controls = target.get('control_curves', [])
                                    is_ic = target.get('is_ic', 0)
                                    formatted_controls = []
                                    for ctrl in target_controls:
                                        ctype_raw = ctrl.get('control_type') or ctrl.get('type')
                                        ctype_up = ctype_raw.upper() if isinstance(ctype_raw, str) else ''
                                        mapped_type = 'control'
                                        if ctype_up in ('PC', 'POSITIVE'):
                                            mapped_type = 'positive'
                                        elif ctype_up in ('NC', 'NEGATIVE', 'NTC'):
                                            mapped_type = 'negative'
                                        # For IC, negative controls are actually positive (no sample interference)
                                        if is_ic and mapped_type == 'negative':
                                            mapped_type = 'positive'
                                        formatted_controls.append({
                                            'readings': ctrl.get('readings', []),
                                            'type': mapped_type,
                                            'ct': ctrl.get('machine_ct') or ctrl.get('ct')
                                        })
                                    targets_dict[target_name] = {
                                        'readings': target.get('readings', []),
                                        'ct': target.get('machine_ct') or target.get('ct'),
                                        'is_ic': is_ic,
                                        'controls': formatted_controls
                                    }
                                    if fallback_main_target is None and not is_ic:
                                        fallback_main_target = target_name

                                # Use fallback only if main_target wasn't provided
                                if main_target is None:
                                    main_target = fallback_main_target or (list(targets_dict.keys())[0] if targets_dict else None)
                                targets = targets_dict
                            else:
                                main_target = well_data.get('main_target')
                                if not isinstance(targets, dict):
                                    targets = {}
                            # Include top-level controls for control reports (backward compatibility)
                            js_data = json.dumps({'main_target': main_target, 'targets': targets, 'controls': controls})
                            html += f'''<script>curveData["{well_id}"] = {js_data}; currentTargets["{well_id}"] = "{main_target or ''}";</script>'''

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

                            html += f'''<div class="svg-container" data-record-id="{well_id}"></div>'''

                            # Display passive normalization status
                            passive_status = well_data.get('passive_status')
                            if passive_status == 'normalized':
                                html += '<div style="font-size: 9px; color: #0066cc; background: #e6f2ff; padding: 3px 6px; margin: 4px 0; border-radius: 3px; border-left: 3px solid #0066cc;">✓ Normalized with passive dye</div>'
                            elif passive_status == 'expected_but_missing':
                                html += '<div style="font-size: 9px; color: #cc6600; background: #fff3e6; padding: 3px 6px; margin: 4px 0; border-radius: 3px; border-left: 3px solid #cc6600;">⚠ Passive dye normalization expected but passive target missing/failed</div>'

                            comments = (well_data.get('comments') or [])[:3]
                            if comments:
                                html += '<div style="margin-top: 6px;">'
                                for c in comments:
                                    ctext = (c.get('text') or '').replace('\n', '<br>')
                                    html += f'<div style="font-size: 10px; color: #666; background: #f5f5f5; padding: 4px 6px; margin: 4px 0; border-radius: 3px;">{ctext}</div>'
                                html += '</div>'
                        else:
                            html += f'''<div class="svg-container" data-record-id="{well_id}"><svg width="300" height="150"><rect width="300" height="150" fill="white" stroke="#eee"/><text x="150" y="75" text-anchor="middle" fill="#999">No curve data</text></svg></div>'''

                        # Details: classification and LIMS
                        machine_cls = record.get('machine_cls', 'N/A')
                        final_cls = record.get('final_cls', 'N/A')
                        ct = record.get('ct', 'N/A')
                        lims = (record.get('lims_status') or 'UNKNOWN')
                        if machine_cls != 'N/A' and final_cls != 'N/A':
                            machine_result = 'POS' if machine_cls == 1 else 'NEG'
                            final_result = 'POS' if final_cls == 1 else 'NEG'
                            html += f'''<div class="card-details">Run: {record.get('run_name', record.get('run_id', 'Unknown'))}<br>Date: {record.get('extraction_date') or 'N/A'}<br>Machine: {machine_result} &rarr; Final: {final_result}<br>CT: {ct if ct != 'N/A' and ct is not None else 'N/A'}<br>LIMS Output: <strong>{lims}</strong>'''
                        else:
                            html += f'''<div class="card-details">Run: {record.get('run_name', record.get('run_id', 'Unknown'))}<br>Date: {record.get('extraction_date') or 'N/A'}<br>LIMS Output: <strong>{lims}</strong>'''
                        if record.get('error_code'):
                            html += f'<br>Error: {record.get("error_message", record["error_code"])}'
                        html += '</div></div>'
                    html += '</div>'  # close container

            else:
                for idx, record in enumerate(records_to_show):
                    if idx < 3:
                        print(f'RENDER sample mix {mix_name} cat {category} idx {idx}')
                    well_id = record['well_id']

                    # Get well curve data - try both string and int keys
                    well_data = well_curves.get(str(well_id)) or well_curves.get(well_id)

                    # Determine card color class based on clinical category and LIMS status
                    if report_type == 'discrepancy':
                        # For discrepancy report, use different color coding
                        if category == 'acted_upon':
                            # Changed results - use green/red based on final classification
                            final_cls = record.get('final_cls', 0)
                            card_class = 'resolved-detected' if final_cls == 1 else 'resolved-not-detected'
                        elif category == 'samples_repeated':
                            card_class = 'resolved-excluded'
                        else:  # ignored
                            lims_status = record.get('lims_status', '')
                            if lims_status == 'DETECTED':
                                card_class = 'resolved-detected'
                            elif lims_status == 'NOT DETECTED':
                                card_class = 'resolved-not-detected'
                            else:
                                card_class = 'resolved-other'
                    else:
                        # Original control/sample report color coding
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
                        targets = well_data.get('targets', {})
                        controls = well_data.get('controls', [])

                        # Convert targets to dict format if it's a list
                        if isinstance(targets, list):
                            # Try to get main_target from well_data first (for discrepancy reports)
                            main_target = well_data.get('main_target')

                            targets_dict = {}
                            fallback_main_target = None
                            for target in targets:
                                target_name = target.get('target_name', 'Unknown')
                                target_controls = target.get('control_curves', [])
                                is_ic = target.get('is_ic', 0)

                                # Convert control format if needed
                                formatted_controls = []
                                for ctrl in target_controls:
                                    ctype_raw = ctrl.get('control_type') or ctrl.get('type')
                                    if isinstance(ctype_raw, str):
                                        ctype_up = ctype_raw.upper()
                                    else:
                                        ctype_up = ''
                                    mapped_type = 'control'
                                    if ctype_up in ('PC', 'POSITIVE'):
                                        mapped_type = 'positive'
                                    elif ctype_up in ('NC', 'NEGATIVE', 'NTC'):
                                        mapped_type = 'negative'
                                    # For IC, negative controls are actually positive (no sample interference)
                                    if is_ic and mapped_type == 'negative':
                                        mapped_type = 'positive'
                                    formatted_controls.append({
                                        'readings': ctrl.get('readings', []),
                                        'type': mapped_type,
                                        'ct': ctrl.get('machine_ct') or ctrl.get('ct')
                                    })

                                targets_dict[target_name] = {
                                    'readings': target.get('readings', []),
                                    'ct': target.get('machine_ct') or target.get('ct'),
                                    'is_ic': is_ic,
                                    'controls': formatted_controls
                                }

                                # Set first non-IC target as fallback, or first target if all are IC
                                if fallback_main_target is None and not is_ic:
                                    fallback_main_target = target_name

                            # Use fallback only if main_target wasn't provided
                            if main_target is None:
                                main_target = fallback_main_target or (list(targets_dict.keys())[0] if targets_dict else None)
                            targets = targets_dict
                        else:
                            main_target = well_data.get('main_target')
                            # Ensure targets is dict format
                            if not isinstance(targets, dict):
                                targets = {}

                        # Store data in JavaScript for this well
                        # For control reports, need to format controls to match expected structure
                        formatted_top_level_controls = []
                        if controls and isinstance(controls, list):
                            for ctrl in controls:
                                ctype_raw = ctrl.get('control_type') or ctrl.get('type')
                                ctype_up = ctype_raw.upper() if isinstance(ctype_raw, str) else ''
                                mapped_type = 'control'
                                if ctype_up in ('PC', 'POSITIVE'):
                                    mapped_type = 'positive'
                                elif ctype_up in ('NC', 'NEGATIVE', 'NTC'):
                                    mapped_type = 'negative'
                                formatted_top_level_controls.append({
                                    'readings': ctrl.get('readings', []),
                                    'type': mapped_type,
                                    'ct': ctrl.get('machine_ct') or ctrl.get('ct')
                                })

                        js_data = json.dumps({
                            'main_target': main_target,
                            'targets': targets,
                            'controls': formatted_top_level_controls
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

                        # Display passive normalization status
                        passive_status = well_data.get('passive_status')
                        if passive_status == 'normalized':
                            html += '<div style="font-size: 9px; color: #0066cc; background: #e6f2ff; padding: 3px 6px; margin: 4px 0; border-radius: 3px; border-left: 3px solid #0066cc;">✓ Normalized with passive dye</div>'
                        elif passive_status == 'expected_but_missing':
                            html += '<div style="font-size: 9px; color: #cc6600; background: #fff3e6; padding: 3px 6px; margin: 4px 0; border-radius: 3px; border-left: 3px solid #cc6600;">⚠ Passive dye normalization expected but passive target missing/failed</div>'

                        # Include up to two system comments if provided in JSON
                        comments = (well_data.get('comments') or [])[:3]
                        if comments:
                            html += '<div style="margin-top: 6px;">'
                            for c in comments:
                                ctext = (c.get('text') or '').replace('\\n', '<br>')
                                html += f'<div style="font-size: 10px; color: #666; background: #f5f5f5; padding: 4px 6px; margin: 4px 0; border-radius: 3px;">{ctext}</div>'
                            html += '</div>'
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

                    # Show different details for discrepancy vs control reports
                    if report_type == 'discrepancy':
                        html += f'''
                    <div class="card-details">
                        Run: {record.get('run_name', record.get('run_id', 'Unknown'))}<br>
                        Date: {record.get('extraction_date') or 'N/A'}<br>'''

                        # For Samples Repeated, emphasize LIMS output instead of Machine→Final
                        if category == 'samples_repeated':
                            lims = (record.get('lims_status') or 'UNKNOWN')
                            html += f'''LIMS Output: <strong>{lims}</strong>'''
                        else:
                            # Show classification details for discrepancy report
                            machine_cls = record.get('machine_cls', 'N/A')
                            final_cls = record.get('final_cls', 'N/A')
                            ct = record.get('ct', 'N/A')
                            if machine_cls != 'N/A' and final_cls != 'N/A':
                                machine_result = 'POS' if machine_cls == 1 else 'NEG'
                                final_result = 'POS' if final_cls == 1 else 'NEG'
                                html += f'''Machine: {machine_result} &rarr; Final: {final_result}<br>CT: {ct if ct != 'N/A' and ct is not None else 'N/A'}'''
                            # Always include LIMS for ignored and acted_upon in discrepancy
                            if category in ['ignored', 'acted_upon'] and record.get('lims_status'):
                                html += f'''<br>LIMS Output: <strong>{record.get('lims_status')}</strong>'''

                        if record.get('error_code'):
                            html += f'<br>Error: {record.get("error_message", record["error_code"])}'
                    else:
                        html += f'''
                    <div class="card-details">
                        Run: {record['run_name']}<br>
                        Date: {record.get('extraction_date') or 'N/A'}<br>
                        Error: {record.get('error_message', record['error_code'])}'''

                    # Add link to affected samples if this control has affected samples
                    if well_id in control_to_group_map:
                        anchor = control_to_group_map[well_id]
                        html += f'''<br>
                        <a href="#{anchor}" onclick="return ensureVisibleAnchor('{anchor}', event)" style="color: #2196F3; text-decoration: none; font-size: 11px;">&rarr; View Affected Samples</a>'''

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
                        <div class="error-badge {category}">{record.get('error_message', record.get('error_code', 'Unknown'))}</div>'''

                    html += '''
                    </div>
                </div>
                '''
            
            if not (group_by_lims or group_by_final):
                html += '''
                </div>
            </div>
                '''
            else:
                # Close only the category-content wrapper when grouping is used
                html += '''
            </div>
                '''
            first_category = False
        
        html += '''
            </div>
        </div>
        '''
    
    # Add APPENDIX for affected samples (only for control reports)
    if report_type == 'control' and affected_groups:
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
                <span class="expand-icon" style="cursor: pointer;" onclick="toggleSection('appendix-error')">&#9654;</span>
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
                <span class="expand-icon" style="cursor: pointer;" onclick="toggleSection('appendix-repeats')">&#9654;</span>
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


def generate_combined_html(combined_data, output_file, max_per_category):
    reports = combined_data.get('reports', {})
    if not reports:
        raise ValueError('Combined data must include a "reports" object with per-report payloads')

    order = ['sample', 'control', 'discrepancy']
    sections = []
    totals = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        for key in order:
            payload = reports.get(key)
            if not payload:
                continue

            report_type = payload.get('report_type', key)
            errors = payload.get('errors', [])
            affected = payload.get('affected_samples', {})
            well_curves = payload.get('well_curves', {})

            print(f"\nRendering {report_type} section: {len(errors)} errors")
            temp_path = os.path.join(tmpdir, f'{key}_report.html')
            total = generate_interactive_html(
                errors,
                affected,
                well_curves,
                temp_path,
                report_type=report_type,
                max_per_category=max_per_category,
                metadata=payload,
                embed=True,
            )
            with open(temp_path, 'r', encoding='utf-8') as handle:
                section_html = handle.read()

            sections.append((key, payload, section_html))
            totals[key] = total

    generated_at = combined_data.get('generated_at', datetime.now().isoformat())
    database = combined_data.get('database')

    # Calculate date range for header
    since_dates = [payload.get('since_date') for key, payload, _ in sections if payload.get('since_date')]
    until_dates = [payload.get('until_date') for key, payload, _ in sections if payload.get('until_date')]
    date_range_str = ''
    if since_dates:
        earliest_date = min(since_dates)
        try:
            start_dt = datetime.strptime(earliest_date, '%Y-%m-%d')
            # Use until_date if available, otherwise fall back to generated_at
            if until_dates:
                latest_date = max(until_dates)
                end_dt = datetime.strptime(latest_date, '%Y-%m-%d')
            else:
                end_dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00')) if 'T' in generated_at else datetime.now()
            date_range_str = f" ({start_dt.strftime('%-d %b %y')} - {end_dt.strftime('%-d %b %y')})"
        except:
            pass

    html_parts = [
        '<!DOCTYPE html>',
        '<html>',
        '<head>',
        '    <meta charset="UTF-8">',
        '    <title>Unified Error Reports</title>',
        '    <style>',
        '        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }',
        '        h1 { text-align: center; margin-bottom: 10px; }',
        '        .meta { text-align: center; color: #555; margin-bottom: 30px; }',
        '        section.report-block { margin-bottom: 24px; background: white; padding: 18px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); transition: box-shadow 0.2s; }',
        '        section.report-block.collapsed { cursor: pointer; }',
        '        section.report-block.collapsed:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.12); }',
        '        .block-header { display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; }',
        '        .block-header h2 { margin: 0; }',
        '        .block-header .subhead { color: #666; margin-top: 4px; }',
        '        .block-actions { display: flex; align-items: center; gap: 8px; }',
        '        .toggle-btn { background: #1976D2; color: white; border: none; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-size: 13px; }',
        '        .toggle-btn:hover { background: #115293; }',
        '        .block-content { display: none; margin-top: 16px; }',
        '        iframe.report-frame { width: 100%; height: 70vh; max-height: 1000px; overflow: auto; border: 1px solid #ccc; border-radius: 6px; background: white; display: block; }',
        '    </style>',
        '</head>',
        '<body>',
        f'    <h1>Unified Error Reports{date_range_str}</h1>',
    ]

    meta_lines = [f'Generated at {generated_at}']
    if database:
        meta_lines.append(f'Database: {database}')
    html_parts.append(f"    <div class=\"meta\">{' &mdash; '.join(meta_lines)}</div>")

    for key, payload, section_html in sections:
        title = payload.get('title') or REPORT_TITLE_MAP.get(key) or key.title()
        since_date = payload.get('since_date')
        date_field = payload.get('date_field')
        subtitle = ''
        if since_date:
            if date_field == 'upload':
                label = 'upload date'
            elif date_field == 'extraction':
                label = 'extraction date'
            else:
                if key in ('sample', 'control'):
                    label = 'extraction date'
                else:
                    label = date_field or 'date'
            subtitle = f'Filtered since {since_date} ({label})'

        escaped = html_std.escape(section_html, quote=True)
        html_parts.append('    <section class="report-block collapsed" id="section-{}" onclick="expandIfCollapsed(\'{}\', event)">'.format(key, key))
        html_parts.append('        <div class="block-header">')
        html_parts.append('            <div>')
        html_parts.append(f'                <h2>{html_std.escape(title)}</h2>')
        if subtitle:
            html_parts.append(f'                <div class="subhead">{html_std.escape(subtitle)}</div>')
        html_parts.append('            </div>')
        html_parts.append(f'            <div class="block-actions"><button class="toggle-btn" type="button" onclick="toggleCombinedSection(\'{key}\', this, event)">Expand</button></div>')
        html_parts.append('        </div>')
        html_parts.append('        <div class="block-content" style="display:none;">')
        html_parts.append(
            f"            <iframe class=\"report-frame\" scrolling=\"auto\" loading=\"lazy\" data-section=\"{key}\" title=\"{html_std.escape(title)}\" srcdoc='{escaped}'></iframe>"
        )
        html_parts.append('        </div>')
        html_parts.append('    </section>')

    html_parts.extend([
        '    <script>',
        '        function expandIfCollapsed(key, event) {',
        '            const section = document.getElementById("section-" + key);',
        '            if (!section) { return; }',
        '            // Only expand if currently collapsed',
        '            if (section.classList.contains("collapsed")) {',
        '                const content = section.querySelector(".block-content");',
        '                const button = section.querySelector(".toggle-btn");',
        '                if (content) { content.style.display = "block"; }',
        '                if (button) { button.textContent = "Collapse"; }',
        '                section.classList.remove("collapsed");',
        '            }',
        '        }',
        '        function toggleCombinedSection(key, button, event) {',
        '            // Stop propagation to prevent section click handler',
        '            if (event) { event.stopPropagation(); }',
        '            const section = document.getElementById("section-" + key);',
        '            if (!section) { return; }',
        '            const content = section.querySelector(".block-content");',
        '            const isHidden = !content || content.style.display === "none";',
        '            if (isHidden) {',
        '                if (content) { content.style.display = "block"; }',
        '                if (button) { button.textContent = "Collapse"; }',
        '                section.classList.remove("collapsed");',
        '            } else {',
        '                if (content) { content.style.display = "none"; }',
        '                if (button) { button.textContent = "Expand"; }',
        '                section.classList.add("collapsed");',
        '            }',
        '        }',
        '    </script>',
        '</body>',
        '</html>',
    ])

    with open(output_file, 'w', encoding='utf-8') as handle:
        handle.write('\n'.join(html_parts))

    return totals

def main():
    parser = argparse.ArgumentParser(description='Generate HTML report from JSON data')
    parser.add_argument('--json', required=True, help='Path to JSON data file')
    parser.add_argument('--output', help='Output HTML file path')
    parser.add_argument('--report-type', choices=['control', 'discrepancy', 'sample'], default='control',
                       help='Type of report to generate (default: control)')
    parser.add_argument('--max-per-category', type=int, default=100,
                       help='Maximum records to show per category (default: 100)')
    
    args = parser.parse_args()
    
    # Set default output if not provided
    if not args.output:
        if args.report_type == 'discrepancy':
            args.output = 'output_data/discrepancy_report_from_json.html'
        elif args.report_type == 'sample':
            args.output = 'output_data/sample_report_from_json.html'
        else:
            args.output = 'output_data/control_report_from_json.html'
    
    # Load JSON data
    print(f"Loading JSON data from: {args.json}")
    with open(args.json, 'r') as f:
        data = json.load(f)
    
    # Combined payload support
    if 'reports' in data:
        report_keys = list(data['reports'].keys())
        print(f"Detected combined payload with sections: {', '.join(report_keys)}")
        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        totals = generate_combined_html(data, args.output, args.max_per_category)
        print(f"\nCombined HTML written to {args.output}")
        for key, total in totals.items():
            print(f"  {key}: {total} errors rendered")
        return

    # Detect report type from JSON if not specified
    if 'report_type' in data:
        detected_type = data['report_type']
        if detected_type != args.report_type:
            print(f"Note: JSON indicates report type '{detected_type}', using --report-type '{args.report_type}'")
    
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
    print(f"\nGenerating interactive {args.report_type} HTML report...")
    print(f"Maximum records per category: {args.max_per_category}")
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    total = generate_interactive_html(
        all_errors,
        affected_samples,
        well_curves,
        args.output,
        report_type=args.report_type,
        max_per_category=args.max_per_category,
        metadata=data,
    )
    
    print(f"\nReport generated successfully:")
    print(f"  Output file: {args.output}")
    print(f"  Total errors: {total}")

if __name__ == '__main__':
    main()
