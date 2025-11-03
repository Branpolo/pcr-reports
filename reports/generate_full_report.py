#!/usr/bin/env python3
"""
Full report pipeline - generates JSON, HTML, and XLSX in one command.

This script is a convenience wrapper that runs:
1. unified_json_extractor.py (generate JSON)
2. generate_report_from_json_with_graphs.py (generate detailed HTML)
3. generate_xlsx_from_json.py (generate XLSX)
4. generate_summary_report.py (generate executive summary HTML)

Accepts all flags from unified_json_extractor, plus optional output path overrides.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description='Generate complete report suite (JSON + HTML + XLSX) in one command',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Notts report with default outputs in output_data/
  python3 -m reports.generate_full_report --db-type notts --db input/notts.db --limit 100

  # Quest report with base output path (generates .json, .html, .xlsx, _summary.html)
  python3 -m reports.generate_full_report --db-type qst --db input/quest.db \\
      --output output_data/final/qst_unified \\
      --sample-since-date 2024-06-01 \\
      --exclude-from-sop "%SIGMOID%" \\
      --suppress-unaffected-controls

  # Vira report with individual output paths
  python3 -m reports.generate_full_report --db-type vira --db input/vira.db \\
      --json output_data/vira.json \\
      --html output_data/vira.html \\
      --xlsx output_data/vira.xlsx
        """
    )

    # Database configuration
    parser.add_argument('--db-type', choices=['qst', 'notts', 'vira'],
                       help='Database type (determines control patterns and LIMS mappings)')
    parser.add_argument('--db', required=True,
                       help='Path to SQLite database file')

    # Date filters
    parser.add_argument('--sample-since-date',
                       help='Sample report start date (YYYY-MM-DD)')
    parser.add_argument('--sample-until-date',
                       help='Sample report end date (YYYY-MM-DD)')
    parser.add_argument('--control-since-date',
                       help='Control report start date (YYYY-MM-DD)')
    parser.add_argument('--control-until-date',
                       help='Control report end date (YYYY-MM-DD)')
    parser.add_argument('--discrepancy-since-date',
                       help='Discrepancy report start date (YYYY-MM-DD)')
    parser.add_argument('--discrepancy-until-date',
                       help='Discrepancy report end date (YYYY-MM-DD)')
    parser.add_argument('--discrepancy-date-field',
                       choices=['upload', 'extraction'],
                       help='Date field for discrepancy filtering')

    # Processing options
    parser.add_argument('--limit', type=int,
                       help='Limit records per category (for testing)')
    parser.add_argument('--max-controls', type=int,
                       help='Maximum control curves per target')
    parser.add_argument('--sample-include-label-errors', action='store_true',
                       help='Include label/setup errors in sample report')
    parser.add_argument('--exclude-from-sop', nargs='+', metavar='ERROR_CODE',
                       help='Error codes to exclude from SOP report')
    parser.add_argument('--exclude-from-control', nargs='+', metavar='ERROR_CODE',
                       help='Error codes to exclude from control report')
    parser.add_argument('--suppress-unaffected-controls', action='store_true',
                       help='Exclude control errors with no affected samples')
    parser.add_argument('--site-ids', nargs='+', metavar='SITE_ID',
                       help='Filter by site IDs')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: limit=10')

    # Output paths (optional - auto-generated if not provided)
    parser.add_argument('--output',
                       help='Base output path (generates .json, .html, .xlsx, _summary.html with this base)')
    parser.add_argument('--json',
                       help='Output path for JSON file (overrides --output)')
    parser.add_argument('--html',
                       help='Output path for HTML file (overrides --output)')
    parser.add_argument('--xlsx',
                       help='Output path for XLSX file (overrides --output)')
    parser.add_argument('--summary',
                       help='Output path for summary HTML file (overrides --output)')

    # HTML-specific options
    parser.add_argument('--max-per-category', type=int, default=0,
                       help='Maximum records per category in HTML (0=unlimited)')

    args = parser.parse_args()

    # Generate default output paths if not provided
    db_path = Path(args.db)
    db_name = db_path.stem  # e.g., "notts" from "input/notts.db"

    # Priority: individual flags > --output base > auto-generated
    if args.output:
        # Use --output as base path
        base_path = args.output
        json_output = args.json or f"{base_path}.json"
        html_output = args.html or f"{base_path}.html"
        xlsx_output = args.xlsx or f"{base_path}.xlsx"
        summary_output = args.summary or f"{base_path}_summary.html"
    else:
        # Auto-generate or use individual flags
        json_output = args.json or f"output_data/{db_name}_report.json"
        html_output = args.html or f"output_data/{db_name}_report.html"
        xlsx_output = args.xlsx or f"output_data/{db_name}_report.xlsx"
        summary_output = args.summary or f"output_data/{db_name}_report_summary.html"

    # Ensure output directories exist
    for output_file in [json_output, html_output, xlsx_output, summary_output]:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("FULL REPORT PIPELINE")
    print("=" * 70)
    print(f"Database: {args.db}")
    print(f"Output JSON:    {json_output}")
    print(f"Output HTML:    {html_output}")
    print(f"Output XLSX:    {xlsx_output}")
    print(f"Output Summary: {summary_output}")
    print("=" * 70)

    # Step 1: Generate JSON
    print("\n[1/4] Generating JSON data...")
    json_cmd = [
        sys.executable, '-m', 'reports.unified_json_extractor',
        '--db', args.db,
        '--output', json_output,
    ]

    # Add optional db-type
    if args.db_type:
        json_cmd.extend(['--db-type', args.db_type])

    # Add date filters
    if args.sample_since_date:
        json_cmd.extend(['--sample-since-date', args.sample_since_date])
    if args.sample_until_date:
        json_cmd.extend(['--sample-until-date', args.sample_until_date])
    if args.control_since_date:
        json_cmd.extend(['--control-since-date', args.control_since_date])
    if args.control_until_date:
        json_cmd.extend(['--control-until-date', args.control_until_date])
    if args.discrepancy_since_date:
        json_cmd.extend(['--discrepancy-since-date', args.discrepancy_since_date])
    if args.discrepancy_until_date:
        json_cmd.extend(['--discrepancy-until-date', args.discrepancy_until_date])
    if args.discrepancy_date_field:
        json_cmd.extend(['--discrepancy-date-field', args.discrepancy_date_field])

    # Add processing options
    if args.limit:
        json_cmd.extend(['--limit', str(args.limit)])
    if args.max_controls:
        json_cmd.extend(['--max-controls', str(args.max_controls)])
    if args.sample_include_label_errors:
        json_cmd.append('--sample-include-label-errors')
    if args.exclude_from_sop:
        json_cmd.extend(['--exclude-from-sop'] + args.exclude_from_sop)
    if args.exclude_from_control:
        json_cmd.extend(['--exclude-from-control'] + args.exclude_from_control)
    if args.suppress_unaffected_controls:
        json_cmd.append('--suppress-unaffected-controls')
    if args.site_ids:
        json_cmd.extend(['--site-ids'] + args.site_ids)
    if args.test:
        json_cmd.append('--test')

    result = subprocess.run(json_cmd)
    if result.returncode != 0:
        print("\n❌ JSON generation failed!")
        sys.exit(1)

    # Step 2: Generate HTML
    print("\n[2/4] Generating detailed HTML report...")
    html_cmd = [
        sys.executable, '-m', 'reports.generate_report_from_json_with_graphs',
        '--json', json_output,
        '--output', html_output,
        '--max-per-category', str(args.max_per_category),
    ]

    result = subprocess.run(html_cmd)
    if result.returncode != 0:
        print("\n❌ HTML generation failed!")
        sys.exit(1)

    # Step 3: Generate XLSX
    print("\n[3/4] Generating XLSX export...")
    xlsx_cmd = [
        sys.executable, '-m', 'reports.generate_xlsx_from_json',
        '--json', json_output,
        '--output', xlsx_output,
    ]

    result = subprocess.run(xlsx_cmd)
    if result.returncode != 0:
        print("\n❌ XLSX generation failed!")
        sys.exit(1)

    # Step 4: Generate Summary HTML
    print("\n[4/4] Generating executive summary HTML...")
    summary_cmd = [
        sys.executable, '-m', 'reports.generate_summary_report',
        '--json', json_output,
        '--output', summary_output,
    ]

    result = subprocess.run(summary_cmd)
    if result.returncode != 0:
        print("\n❌ Summary generation failed!")
        sys.exit(1)

    # Success summary
    print("\n" + "=" * 70)
    print("✅ PIPELINE COMPLETE!")
    print("=" * 70)
    print(f"📄 JSON:     {json_output}")
    print(f"🌐 HTML:     {html_output}")
    print(f"📊 XLSX:     {xlsx_output}")
    print(f"📈 Summary:  {summary_output}")
    print("=" * 70)


if __name__ == '__main__':
    main()
