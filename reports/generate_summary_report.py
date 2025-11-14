#!/usr/bin/env python3
"""
Generate executive summary report with pie charts from combined JSON report.
"""

import json
import argparse
from collections import defaultdict
from datetime import datetime


def group_mix_by_family(mix_name):
    """Group mix into family (ADV*, BKV*, CMV*, etc.)"""
    mix_upper = mix_name.upper()

    # Define mix families
    families = {
        'ADV': ['ADV', 'ADVQ', 'ADVQBL', 'ADVQSE', 'ADVQU'],
        'BKV': ['BKV', 'BKVQ', 'BKVQBL', 'BKVQPL', 'BKVQSE', 'BKVQU', 'BKVQUR'],
        'CMV': ['CMV', 'CMVQ2', 'CMVQ2BL', 'CMVQ2PL', 'CMVQ2SE'],
        'COVID': ['COVID-IPC'],
        'EBV': ['EBV', 'EBVQ', 'EBVQBL', 'EBVQPL', 'EBVQSE', 'EBVQU'],
        'HHV6': ['HHV6Q'],
        'HSV': ['HSV', 'HSVQ', 'HSVQBL', 'HSVQPL'],
        'MPX': ['MPX'],
        'NOROV': ['NOROV'],
        'PARVO': ['PARVO', 'PARVOQ'],
        'PCOVID': ['PCOVID'],
        'PJ': ['PJ', 'QPJ'],
        'QCOVID': ['QCOVID'],
        'RP': ['RP'],
        'VZV': ['VZV', 'VZVQ', 'VZVQBL'],
    }

    # Find which family this mix belongs to
    for family, mixes in families.items():
        if mix_upper in mixes:
            return family

    # Check if starts with any family prefix
    for family in families.keys():
        if mix_upper.startswith(family):
            return family

    return 'OTHER'


def calculate_summary_statistics(data):
    """Calculate statistics for all summary charts"""

    valid_results = data.get('valid_results', {})
    error_stats = data.get('error_statistics', {})

    # Overall sample summary
    total_detected = sum(v.get('samples_detected', 0) for v in valid_results.values())
    total_not_detected = sum(v.get('samples_not_detected', 0) for v in valid_results.values())
    total_reported = total_detected + total_not_detected

    # Total samples (ALL samples including those with errors and NULL LIMS)
    total_samples = sum(v.get('total_samples', 0) for v in valid_results.values())
    # Fallback if total_samples not present (older JSON format)
    if total_samples == 0:
        total_samples = total_reported

    # Control summary
    total_controls = sum(v.get('controls_total', 0) for v in valid_results.values())
    total_controls_passed = sum(v.get('controls_passed', 0) for v in valid_results.values())

    # Total samples affected by each error type (sum across all mixes)
    total_sop_affected = sum(v.get('sop_errors_affected', 0) for v in error_stats.values())
    total_control_affected_results = sum(v.get('control_errors_affected', 0) for v in error_stats.values())
    total_samples_affected_by_controls = sum(v.get('samples_affected_by_controls', 0) for v in error_stats.values())
    total_classification_affected = sum(v.get('classification_errors_affected', 0) for v in error_stats.values())

    # Use displayed cohorts as the denominator so the cards add up cleanly
    displayed_total = (
        total_reported
        + total_sop_affected
        + total_samples_affected_by_controls
        + total_classification_affected
    )

    overall_summary = {
        'reported': total_reported,
        'sop_affected': total_sop_affected,
        'control_affected': total_samples_affected_by_controls,
        'classification_affected': total_classification_affected,
        'total': displayed_total,
        'controls_total': total_controls,
        'controls_passed': total_controls_passed,
    }

    # Error type summaries
    total_sop_errors = sum(v.get('sop_errors', 0) for v in error_stats.values())
    total_sop_ignored = total_sop_errors - total_sop_affected

    total_control_errors = sum(v.get('control_errors', 0) for v in error_stats.values())
    total_control_ignored = total_control_errors - total_control_affected_results

    total_classification_errors = sum(v.get('classification_errors', 0) for v in error_stats.values())
    total_classification_ignored = total_classification_errors - total_classification_affected

    error_summaries = {
        'sop': {
            'affected': total_sop_affected,
            'ignored': total_sop_ignored,
            'total': total_sop_errors,
        },
        'control': {
            'affected': total_control_affected_results,
            'ignored': total_control_ignored,
            'samples_affected': total_samples_affected_by_controls,
            'total': total_control_errors,
        },
        'classification': {
            'affected': total_classification_affected,
            'ignored': total_classification_ignored,
            'total': total_classification_errors,
        },
    }

    # Mix family summaries
    family_summaries = defaultdict(lambda: {
        'reported': 0,
        'sop_affected': 0,
        'control_affected': 0,
        'classification_affected': 0,
        'total': 0,
    })

    # Group by family
    for mix_name in set(valid_results.keys()) | set(error_stats.keys()):
        family = group_mix_by_family(mix_name)

        valid = valid_results.get(mix_name, {})
        errors = error_stats.get(mix_name, {})

        reported = valid.get('samples_detected', 0) + valid.get('samples_not_detected', 0)
        mix_total = valid.get('total_samples', reported)  # Use total_samples if available
        sop_aff = errors.get('sop_errors_affected', 0)
        ctrl_aff = errors.get('samples_affected_by_controls', 0)
        class_aff = errors.get('classification_errors_affected', 0)

        family_summaries[family]['reported'] += reported
        family_summaries[family]['sop_affected'] += sop_aff
        family_summaries[family]['control_affected'] += ctrl_aff
        family_summaries[family]['classification_affected'] += class_aff
        family_summaries[family]['total'] += mix_total  # Use actual total instead of sum

    # Sort families by total
    sorted_families = sorted(family_summaries.items(), key=lambda x: x[1]['total'], reverse=True)

    return {
        'overall': overall_summary,
        'errors': error_summaries,
        'families': sorted_families,
    }


def generate_html_summary(data, output_path):
    """Generate HTML summary report with pie charts"""

    stats = calculate_summary_statistics(data)
    generated_at = data.get('generated_at', datetime.now().isoformat())

    # Get date range from reports
    reports = data.get('reports', {})
    since_dates = []
    until_dates = []
    for report_type in ['sample', 'control', 'discrepancy']:
        report_data = reports.get(report_type, {})
        if report_data.get('since_date'):
            since_dates.append(report_data['since_date'])
        if report_data.get('until_date'):
            until_dates.append(report_data['until_date'])

    date_range_str = ''
    if since_dates and until_dates:
        start = min(since_dates)
        end = max(until_dates)
        try:
            start_dt = datetime.strptime(start, '%Y-%m-%d')
            end_dt = datetime.strptime(end, '%Y-%m-%d')
            date_range_str = f" ({start_dt.strftime('%-d %b %y')} - {end_dt.strftime('%-d %b %y')})"
        except:
            pass

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Executive Summary Report{date_range_str}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: #f5f7fa;
            padding: 20px;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            color: #2c3e50;
        }}

        .subtitle {{
            color: #7f8c8d;
            font-size: 1.1em;
            margin-bottom: 30px;
        }}

        .section {{
            background: white;
            border-radius: 8px;
            padding: 25px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .section h2 {{
            font-size: 1.8em;
            margin-bottom: 20px;
            color: #34495e;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}

        .chart-container {{
            position: relative;
            height: 300px;
            margin: 20px 0;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}

        .stat-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            border-left: 4px solid #3498db;
        }}

        .stat-label {{
            font-size: 0.9em;
            color: #7f8c8d;
            margin-bottom: 5px;
        }}

        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: #2c3e50;
        }}

        .stat-percent {{
            font-size: 0.9em;
            color: #95a5a6;
            margin-left: 5px;
        }}

        .family-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}

        .family-card {{
            background: white;
            border: 1px solid #e1e8ed;
            border-radius: 8px;
            padding: 20px;
        }}

        .family-card h3 {{
            font-size: 1.3em;
            margin-bottom: 15px;
            color: #2c3e50;
        }}

        .family-chart {{
            position: relative;
            height: 250px;
        }}

        .color-reported {{ color: #27ae60; }}
        .color-sop {{ color: #e74c3c; }}
        .color-control {{ color: #f39c12; }}
        .color-classification {{ color: #9b59b6; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Executive Summary Report{date_range_str}</h1>
        <p class="subtitle">Generated {generated_at[:19].replace('T', ' ')}</p>
"""

    # Overall Sample Summary
    overall = stats['overall']
    html += f"""
        <div class="section">
            <h2>Overall Sample Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Total Samples</div>
                    <div class="stat-value">{overall['total']:,}</div>
                </div>
                <div class="stat-card" style="border-left-color: #3498db;">
                    <div class="stat-label">Total Controls</div>
                    <div class="stat-value">{overall['controls_total']:,}</div>
                    <div class="stat-percent">({overall['controls_passed']:,} passed{', ' + str(round(100 * overall['controls_passed'] / overall['controls_total'], 1)) + '%' if overall['controls_total'] > 0 else ''})</div>
                </div>
                <div class="stat-card" style="border-left-color: #27ae60;">
                    <div class="stat-label">Reported (Valid Results)</div>
                    <div class="stat-value">{overall['reported']:,} <span class="stat-percent">({100 * overall['reported'] / overall['total']:.1f}%)</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #e74c3c;">
                    <div class="stat-label">Affected by SOP Errors</div>
                    <div class="stat-value">{overall['sop_affected']:,} <span class="stat-percent">({100 * overall['sop_affected'] / overall['total']:.1f}%)</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #f39c12;">
                    <div class="stat-label">Affected by Control Errors</div>
                    <div class="stat-value">{overall['control_affected']:,} <span class="stat-percent">({100 * overall['control_affected'] / overall['total']:.1f}%)</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #9b59b6;">
                    <div class="stat-label">Affected by Classification Errors</div>
                    <div class="stat-value">{overall['classification_affected']:,} <span class="stat-percent">({100 * overall['classification_affected'] / overall['total']:.1f}%)</span></div>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="overallChart"></canvas>
            </div>
        </div>
"""

    # Error Summaries
    errors = stats['errors']

    # SOP Errors
    sop = errors['sop']
    html += f"""
        <div class="section">
            <h2>SOP Error Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Total SOP Errors</div>
                    <div class="stat-value">{sop['total']:,}</div>
                </div>
                <div class="stat-card" style="border-left-color: #e74c3c;">
                    <div class="stat-label">Affected Results</div>
                    <div class="stat-value">{sop['affected']:,} <span class="stat-percent">({100 * sop['affected'] / sop['total']:.1f}%)</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #95a5a6;">
                    <div class="stat-label">Error Ignored</div>
                    <div class="stat-value">{sop['ignored']:,} <span class="stat-percent">({100 * sop['ignored'] / sop['total']:.1f}%)</span></div>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="sopChart"></canvas>
            </div>
        </div>
"""

    # Control Errors
    ctrl = errors['control']
    html += f"""
        <div class="section">
            <h2>Control Error Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Total Control Errors</div>
                    <div class="stat-value">{ctrl['total']:,}</div>
                </div>
                <div class="stat-card" style="border-left-color: #f39c12;">
                    <div class="stat-label">Affected Results</div>
                    <div class="stat-value">{ctrl['affected']:,} <span class="stat-percent">({'(' + str(round(100 * ctrl['affected'] / ctrl['total'], 1)) + '%)' if ctrl['total'] > 0 else ''})</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #95a5a6;">
                    <div class="stat-label">Error Ignored</div>
                    <div class="stat-value">{ctrl['ignored']:,} <span class="stat-percent">({'(' + str(round(100 * ctrl['ignored'] / ctrl['total'], 1)) + '%)' if ctrl['total'] > 0 else ''})</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #e67e22;">
                    <div class="stat-label">Samples Affected by Control Errors</div>
                    <div class="stat-value">{ctrl['samples_affected']:,}</div>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="controlChart"></canvas>
            </div>
        </div>
"""

    # Classification Errors
    cls = errors['classification']
    html += f"""
        <div class="section">
            <h2>Classification Error Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Total Classification Errors</div>
                    <div class="stat-value">{cls['total']:,}</div>
                </div>
                <div class="stat-card" style="border-left-color: #9b59b6;">
                    <div class="stat-label">Affected Results</div>
                    <div class="stat-value">{cls['affected']:,} <span class="stat-percent">({100 * cls['affected'] / cls['total']:.1f}%)</span></div>
                </div>
                <div class="stat-card" style="border-left-color: #95a5a6;">
                    <div class="stat-label">Error Ignored</div>
                    <div class="stat-value">{cls['ignored']:,} <span class="stat-percent">({100 * cls['ignored'] / cls['total']:.1f}%)</span></div>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="classificationChart"></canvas>
            </div>
        </div>
"""

    # Mix Family Summary
    html += """
        <div class="section">
            <h2>Mix Family Summaries</h2>
            <div class="family-grid">
"""

    for family, family_stats in stats['families']:
        if family == 'OTHER' or family_stats['total'] == 0:
            continue

        html += f"""
                <div class="family-card">
                    <h3>{family}</h3>
                    <div style="font-size: 0.9em; color: #7f8c8d; margin-bottom: 10px;">
                        Total: {family_stats['total']:,} samples
                    </div>
                    <div class="family-chart">
                        <canvas id="family{family}"></canvas>
                    </div>
                </div>
"""

    html += """
            </div>
        </div>
    </div>

    <script>
        Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';

        const colors = {
            reported: '#27ae60',
            sop: '#e74c3c',
            control: '#f39c12',
            classification: '#9b59b6',
            affected: '#e74c3c',
            ignored: '#95a5a6',
        };

        function createPieChart(canvasId, data, labels, backgroundColors) {
            new Chart(document.getElementById(canvasId), {
                type: 'pie',
                data: {
                    labels: labels,
                    datasets: [{
                        data: data,
                        backgroundColor: backgroundColors,
                        borderWidth: 2,
                        borderColor: '#fff',
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 15,
                                font: { size: 12 }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    const label = context.label || '';
                                    const value = context.parsed || 0;
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const percent = ((value / total) * 100).toFixed(1);
                                    return label + ': ' + value.toLocaleString() + ' (' + percent + '%)';
                                }
                            }
                        }
                    }
                }
            });
        }
"""

    # Overall chart
    html += f"""
        createPieChart('overallChart',
            [{overall['reported']}, {overall['sop_affected']}, {overall['control_affected']}, {overall['classification_affected']}],
            ['Reported (Valid Results)', 'Affected by SOP Errors', 'Affected by Control Errors', 'Affected by Classification Errors'],
            [colors.reported, colors.sop, colors.control, colors.classification]
        );
"""

    # SOP chart
    html += f"""
        createPieChart('sopChart',
            [{sop['affected']}, {sop['ignored']}],
            ['Affected Results', 'Error Ignored'],
            [colors.affected, colors.ignored]
        );
"""

    # Control chart
    html += f"""
        createPieChart('controlChart',
            [{ctrl['affected']}, {ctrl['ignored']}],
            ['Affected Results', 'Error Ignored'],
            [colors.affected, colors.ignored]
        );
"""

    # Classification chart
    html += f"""
        createPieChart('classificationChart',
            [{cls['affected']}, {cls['ignored']}],
            ['Affected Results', 'Error Ignored'],
            [colors.affected, colors.ignored]
        );
"""

    # Family charts
    for family, family_stats in stats['families']:
        if family == 'OTHER' or family_stats['total'] == 0:
            continue

        html += f"""
        createPieChart('family{family}',
            [{family_stats['reported']}, {family_stats['sop_affected']}, {family_stats['control_affected']}, {family_stats['classification_affected']}],
            ['Reported', 'SOP Errors', 'Control Errors', 'Classification Errors'],
            [colors.reported, colors.sop, colors.control, colors.classification]
        );
"""

    html += """
    </script>
</body>
</html>
"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Summary report written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Generate executive summary report from combined JSON')
    parser.add_argument('--json', required=True, help='Path to combined JSON report')
    parser.add_argument('--output', help='Output HTML file path (default: auto-generated)')

    args = parser.parse_args()

    # Generate output filename if not specified
    if args.output:
        output_file = args.output
    else:
        output_file = args.json.replace('.json', '_summary.html')

    # Load JSON data
    with open(args.json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    generate_html_summary(data, output_file)


if __name__ == '__main__':
    main()
