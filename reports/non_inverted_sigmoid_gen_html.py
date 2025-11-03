#!/usr/bin/env python3
"""Generate detailed HTML report for non-inverted sigmoid Parvo/HHV6 runs."""

import argparse
import json
import os
import sqlite3
from collections import defaultdict
from datetime import datetime
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))

for path in (CURRENT_DIR, PROJECT_ROOT):
    if path not in sys.path:
        sys.path.append(path)

from utils.database import bytes_to_float  # type: ignore


def is_inverted_sigmoid(readings):
    """Return True when readings show an inverted (downward) sigmoid pattern."""
    valid = []
    for value in readings:
        if value is None:
            continue
        if isinstance(value, bytes):
            valid.append(bytes_to_float(value))
        else:
            valid.append(float(value))

    if len(valid) <= 3:
        return None

    count = len(valid)
    middle_index = (count // 2) - 1 if count % 2 == 0 else round(count / 2) - 1
    penultimate_index = count - 1
    return valid[middle_index] > valid[penultimate_index]


def get_parvo_hhv6_samples(quest_conn):
    """Return Parvo and HHV6 patient wells grouped by run."""
    cursor = quest_conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT
            r.id AS run_id,
            r.run_name,
            r.created_at AS run_date,
            w.id AS well_id,
            w.well_number,
            w.sample_label,
            w.role_alias,
            t.target_name,
            o.machine_cls,
            o.final_cls,
            o.machine_ct,
            o.dxai_cls,
            o.dxai_ct,
            o.readings,
            o.id AS observation_id
        FROM runs r
        JOIN wells w ON r.id = w.run_id
        JOIN observations o ON w.id = o.well_id
        JOIN targets t ON o.target_id = t.id
        WHERE (
            UPPER(t.target_name) LIKE '%PARVO%'
            OR UPPER(t.target_name) LIKE '%HHV6%'
            OR UPPER(t.target_name) LIKE '%HHV-6%'
        )
        AND (w.role_alias = 'Patient' OR w.role_alias = '' OR w.role_alias IS NULL)
        ORDER BY r.id, t.target_name, w.well_number
        """
    )

    runs = defaultdict(lambda: {"parvo": [], "hhv6": [], "run_info": None})

    for row in cursor.fetchall():
        run_id = row[0]
        target_name = row[7].upper()

        if not runs[run_id]["run_info"]:
            runs[run_id]["run_info"] = {
                "run_id": run_id,
                "run_name": row[1],
                "run_date": row[2],
            }

        try:
            readings = json.loads(row[13]) if row[13] else []
        except Exception:
            readings = []

        sample = {
            "well_id": row[3],
            "well_number": row[4],
            "sample_label": row[5],
            "target_name": row[7],
            "machine_cls": row[8],
            "final_cls": row[9],
            "machine_ct": row[10],
            "dxai_cls": row[11],
            "dxai_ct": row[12],
            "readings": readings,
            "observation_id": row[14],
        }

        sample["is_inverted_sigmoid"] = is_inverted_sigmoid(readings)

        if "PARVO" in target_name:
            runs[run_id]["parvo"].append(sample)
        elif "HHV" in target_name or "HHV-6" in target_name:
            runs[run_id]["hhv6"].append(sample)

    return runs


def get_controls_for_run(quest_conn, run_id, target_patterns):
    """Return control wells for the supplied target patterns."""
    if not target_patterns:
        return []

    cursor = quest_conn.cursor()
    where_targets = " OR ".join(
        f"UPPER(t.target_name) LIKE '%{pattern}%'" for pattern in target_patterns
    )

    cursor.execute(
        f"""
        SELECT
            w.id AS well_id,
            w.well_number,
            w.sample_label,
            w.role_alias,
            t.target_name,
            o.machine_cls,
            o.final_cls,
            o.machine_ct,
            o.dxai_cls,
            o.dxai_ct,
            o.readings,
            o.id AS observation_id
        FROM wells w
        JOIN observations o ON w.id = o.well_id
        JOIN targets t ON o.target_id = t.id
        WHERE w.run_id = ?
        AND w.role_alias NOT IN ('Patient', '')
        AND w.role_alias IS NOT NULL
        AND ({where_targets})
        ORDER BY t.target_name, w.well_number
        """,
        (run_id,),
    )

    controls = []
    for row in cursor.fetchall():
        try:
            readings = json.loads(row[10]) if row[10] else []
        except Exception:
            readings = []

        controls.append(
            {
                "well_id": row[0],
                "well_number": row[1],
                "sample_label": row[2],
                "role_alias": row[3],
                "target_name": row[4],
                "machine_cls": row[5],
                "final_cls": row[6],
                "machine_ct": row[7],
                "dxai_cls": row[8],
                "dxai_ct": row[9],
                "readings": readings,
                "observation_id": row[11],
                "is_control": True,
            }
        )

    return controls


def generate_html_report(processed_runs, output_path):
    """Write the detailed HTML report to disk."""
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Non-Inverted Sigmoid Extraction Report</title>
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
        }}
        .run-section {{
            background: white;
            margin: 20px 0;
            padding: 15px;
            border-radius: 4px;
        }}
        .run-header {{
            background: #2196F3;
            color: white;
            padding: 10px;
            margin: -15px -15px 15px;
            border-radius: 4px 4px 0 0;
        }}
        .target-section {{
            margin: 15px 0;
        }}
        .target-header {{
            background: #607D8B;
            color: white;
            padding: 8px;
            margin: 10px 0;
            border-radius: 4px;
        }}
        .samples-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
            margin: 10px 0;
        }}
        .sample-card {{
            border: 1px solid #ddd;
            padding: 8px;
            border-radius: 4px;
            font-size: 12px;
        }}
        .sample-card.control {{
            background: #E8F5E9;
        }}
        .sample-card.inverted {{
            background: #FFEBEE;
            opacity: 0.6;
        }}
        .sample-card.valid {{
            background: #E3F2FD;
        }}
        .summary-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        .summary-table th, .summary-table td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        .summary-table th {{
            background: #f0f0f0;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Non-Inverted Sigmoid Extraction Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
"""

    total_runs = len(processed_runs)
    total_samples = sum(run["valid_samples"] for run in processed_runs)
    total_excluded = sum(run["inverted_samples"] for run in processed_runs)
    total_controls = sum(run["controls_count"] for run in processed_runs)

    html += f"""
    <div class="stats">
        <h2>Summary Statistics</h2>
        <table class="summary-table">
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Total Runs Processed</td><td>{total_runs}</td></tr>
            <tr><td>Valid Samples (Non-Inverted)</td><td>{total_samples}</td></tr>
            <tr><td>Excluded Samples (Inverted Sigmoid)</td><td>{total_excluded}</td></tr>
            <tr><td>Control Samples Included</td><td>{total_controls}</td></tr>
        </table>
    </div>
"""

    for run in sorted(processed_runs, key=lambda item: item["run_id"]):
        html += f"""
    <div class="run-section">
        <div class="run-header">
            <h3>Run {run['run_id']}: {run['run_name']}</h3>
            <p>Date: {run['run_date']} | Valid Samples: {run['valid_samples']} |
               Excluded: {run['inverted_samples']} | Controls: {run['controls_count']}</p>
        </div>
"""

        for target_type in ["parvo", "hhv6"]:
            if run[f"{target_type}_valid"] <= 0:
                continue

            html += f"""
        <div class="target-section">
            <div class="target-header">
                {target_type.upper()} Targets - Valid: {run[f'{target_type}_valid']}, Excluded: {run[f'{target_type}_inverted']}
            </div>
            <div class="samples-grid">
"""

            for sample in run[f"{target_type}_samples"]:
                if sample.get("is_inverted_sigmoid", False):
                    continue
                ct_text = (
                    f"CT: {sample['machine_ct']:.1f}"
                    if isinstance(sample.get("machine_ct"), (int, float))
                    else "CT: N/A"
                )
                html += f"""
                <div class="sample-card valid">
                    <strong>{sample['sample_label'][:20]}</strong><br>
                    Well: {sample['well_number']}<br>
                    {sample['target_name']}<br>
                    {ct_text}<br>
                    Class: {sample['machine_cls']}
                </div>
"""

            html += """
            </div>
        </div>
"""

        html += """
    </div>
"""

    html += """
</body>
</html>
"""

    with open(output_path, "w") as handle:
        handle.write(html)

    print(f"HTML report generated: {output_path}")


def collect_processed_runs(quest_conn):
    """Collect run-level statistics needed for the HTML report."""
    runs_data = get_parvo_hhv6_samples(quest_conn)

    processed_runs = []
    for run_id, run_data in runs_data.items():
        run_info = run_data["run_info"]
        if not run_info:
            continue

        parvo_valid = sum(
            1 for sample in run_data["parvo"] if not sample.get("is_inverted_sigmoid", True)
        )
        parvo_inverted = sum(
            1 for sample in run_data["parvo"] if sample.get("is_inverted_sigmoid", False)
        )
        hhv6_valid = sum(
            1 for sample in run_data["hhv6"] if not sample.get("is_inverted_sigmoid", True)
        )
        hhv6_inverted = sum(
            1 for sample in run_data["hhv6"] if sample.get("is_inverted_sigmoid", False)
        )

        if parvo_valid == 0 and hhv6_valid == 0:
            continue

        target_patterns = []
        if parvo_valid > 0:
            target_patterns.append("PARVO")
        if hhv6_valid > 0:
            target_patterns.extend(["HHV6", "HHV-6"])

        controls = get_controls_for_run(quest_conn, run_id, target_patterns)
        valid_samples = [
            sample
            for group in ("parvo", "hhv6")
            for sample in run_data[group]
            if not sample.get("is_inverted_sigmoid", True)
        ]

        processed_runs.append(
            {
                "run_id": run_id,
                "run_name": run_info["run_name"],
                "run_date": run_info["run_date"],
                "valid_samples": len(valid_samples),
                "inverted_samples": parvo_inverted + hhv6_inverted,
                "controls_count": len(controls),
                "parvo_valid": parvo_valid,
                "parvo_inverted": parvo_inverted,
                "hhv6_valid": hhv6_valid,
                "hhv6_inverted": hhv6_inverted,
                "parvo_samples": run_data["parvo"],
                "hhv6_samples": run_data["hhv6"],
            }
        )

    return processed_runs


def main():
    parser = argparse.ArgumentParser(
        description="Generate the detailed HTML report for non-inverted sigmoid Parvo/HHV6 runs"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="input_data/quest_prod_aug2025.db",
        help="Path to the Quest production database",
    )
    parser.add_argument(
        "--report",
        type=str,
        default="output_data/non_inverted_sigmoid_report.html",
        help="Output HTML report path",
    )

    args = parser.parse_args()

    report_parent = os.path.dirname(args.report) or "."
    os.makedirs(report_parent, exist_ok=True)

    quest_conn = sqlite3.connect(args.db)
    try:
        processed_runs = collect_processed_runs(quest_conn)
    finally:
        quest_conn.close()

    if not processed_runs:
        print("No runs found with non-inverted sigmoid Parvo/HHV6 samples.")
        return

    generate_html_report(processed_runs, args.report)


if __name__ == "__main__":
    main()
