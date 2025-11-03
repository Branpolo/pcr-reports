#!/usr/bin/env python3
"""Generate a discrepancy diff report between two JSON exports."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def load_report(path):
    with open(path, 'r') as handle:
        return json.load(handle)


def map_records(errors):
    mapping = {}
    for record in errors:
        well_id = record.get('well_id') or record.get('id')
        if well_id is None:
            continue
        mapping[str(well_id)] = record
    return mapping


def build_diff(a_path, b_path, label_a, label_b):
    data_a = load_report(a_path)
    data_b = load_report(b_path)

    errors_a = data_a.get('errors', [])
    errors_b = data_b.get('errors', [])

    map_a = map_records(errors_a)
    map_b = map_records(errors_b)

    only_a_ids = sorted(set(map_a.keys()) - set(map_b.keys()))
    only_b_ids = sorted(set(map_b.keys()) - set(map_a.keys()))

    diff_errors = []
    diff_curves = {}

    def add_records(ids, source_label, source_map, curves_source):
        for well_id in ids:
            record = dict(source_map[well_id])
            note = f"[Only in {source_label}]"
            msg = record.get('error_message') or record.get('error_code') or ''
            if note not in msg:
                record['error_message'] = f"{msg} {note}".strip()
            record['diff_source'] = source_label
            diff_errors.append(record)

            curves = curves_source.get(well_id) or curves_source.get(int(well_id))
            if curves:
                diff_curves[well_id] = curves

    add_records(only_a_ids, label_a, map_a, data_a.get('well_curves', {}))
    add_records(only_b_ids, label_b, map_b, data_b.get('well_curves', {}))

    summary = {
        'total_wells': len(diff_errors),
        f'only_in_{label_a}': len(only_a_ids),
        f'only_in_{label_b}': len(only_b_ids),
    }

    diff_data = {
        'report_type': data_a.get('report_type', data_b.get('report_type', 'discrepancy')),
        'generated_from': {
            'dataset_a': str(a_path),
            'dataset_b': str(b_path),
            'label_a': label_a,
            'label_b': label_b,
        },
        'summary': summary,
        'errors': diff_errors,
        'well_curves': diff_curves,
    }

    return diff_data


def maybe_render_html(json_path, html_path, report_type):
    script = Path(__file__).parent / 'generate_report_from_json_with_graphs.py'
    if not script.exists():
        print('Unable to locate generator script for HTML output.', file=sys.stderr)
        return

    cmd = [
        sys.executable,
        str(script),
        '--json',
        str(json_path),
        '--report-type',
        report_type,
        '--output',
        str(html_path),
        '--max-per-category',
        '0',
    ]
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description='Diff two discrepancy report JSON files and emit a diff report.')
    parser.add_argument('--json-a', required=True, help='Baseline JSON file')
    parser.add_argument('--json-b', required=True, help='Comparison JSON file')
    parser.add_argument('--label-a', default='dataset_a', help='Label for baseline dataset')
    parser.add_argument('--label-b', default='dataset_b', help='Label for comparison dataset')
    parser.add_argument('--output-json', default='classification_discrepancies_diff.json', help='Output diff JSON path')
    parser.add_argument('--output-html', help='Optional output HTML path')
    parser.add_argument('--report-type', default='discrepancy', help='Report type to pass to HTML generator')

    args = parser.parse_args()

    diff_data = build_diff(Path(args.json_a), Path(args.json_b), args.label_a, args.label_b)

    with open(args.output_json, 'w') as handle:
        json.dump(diff_data, handle, indent=2)

    print(f"Diff JSON written to {args.output_json} (total wells: {diff_data['summary']['total_wells']})")

    if args.output_html:
        maybe_render_html(args.output_json, args.output_html, args.report_type)


if __name__ == '__main__':
    main()

