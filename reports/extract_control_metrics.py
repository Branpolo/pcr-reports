#!/usr/bin/env python3
"""
Extract control and repeat metrics for specified assays and date ranges.

Metrics:
1. Failed negative controls (Ct_value >= 40)
2. Number of repeats (ct >= 40)
"""

import sqlite3
import argparse
import os
from datetime import datetime
from reports.utils.database_configs import get_config


def get_db_connection(db_path):
    """Create database connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def extract_control_repeats_with_high_ct(conn, assay_pattern, start_date, end_date):
    """
    Extract control wells marked for repeat with Ct >= 40.

    Args:
        conn: Database connection
        assay_pattern: Target name pattern (e.g., '%CMV%', '%BK%')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Count of control wells marked for repeat with Ct >= 40
    """
    query = """
    SELECT COUNT(DISTINCT w.id) as count
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE (w.role_alias LIKE '%NC' OR w.role_alias LIKE '%NC%')
      AND t.target_name NOT LIKE '%IPC%'
      AND t.target_name NOT LIKE '%REFERENCE%'
      AND t.target_name LIKE ?
      AND w.lims_status IN ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
      AND o.machine_ct >= 40
      AND DATE(w.created_at) >= ?
      AND DATE(w.created_at) <= ?
    """

    cursor = conn.cursor()
    cursor.execute(query, (assay_pattern, start_date, end_date))
    result = cursor.fetchone()
    return result['count'] if result else 0


def extract_sample_repeats_with_high_ct(conn, assay_pattern, start_date, end_date):
    """
    Extract sample wells marked for repeat with Ct >= 40.

    Args:
        conn: Database connection
        assay_pattern: Target name pattern (e.g., '%CMV%', '%BK%')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Count of sample wells marked for repeat with Ct >= 40
    """
    query = """
    SELECT COUNT(DISTINCT w.id) as count
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE w.role_alias = 'Patient'
      AND t.target_name NOT LIKE '%IPC%'
      AND t.target_name NOT LIKE '%REFERENCE%'
      AND t.target_name LIKE ?
      AND w.lims_status IN ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
      AND o.machine_ct >= 40
      AND DATE(w.created_at) >= ?
      AND DATE(w.created_at) <= ?
    """

    cursor = conn.cursor()
    cursor.execute(query, (assay_pattern, start_date, end_date))
    result = cursor.fetchone()
    return result['count'] if result else 0


def extract_control_repeats_mid_ct(conn, assay_pattern, start_date, end_date):
    """
    Extract control wells marked for repeat with 38 <= Ct < 40.

    Args:
        conn: Database connection
        assay_pattern: Target name pattern (e.g., '%CMV%', '%BK%')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Count of control wells marked for repeat with 38 <= Ct < 40
    """
    query = """
    SELECT COUNT(DISTINCT w.id) as count
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE (w.role_alias LIKE '%NC' OR w.role_alias LIKE '%NC%')
      AND t.target_name NOT LIKE '%IPC%'
      AND t.target_name NOT LIKE '%REFERENCE%'
      AND t.target_name LIKE ?
      AND w.lims_status IN ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
      AND o.machine_ct >= 38
      AND o.machine_ct < 40
      AND DATE(w.created_at) >= ?
      AND DATE(w.created_at) <= ?
    """

    cursor = conn.cursor()
    cursor.execute(query, (assay_pattern, start_date, end_date))
    result = cursor.fetchone()
    return result['count'] if result else 0


def extract_sample_repeats_mid_ct(conn, assay_pattern, start_date, end_date):
    """
    Extract sample wells marked for repeat with 38 <= Ct < 40.

    Args:
        conn: Database connection
        assay_pattern: Target name pattern (e.g., '%CMV%', '%BK%')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Count of sample wells marked for repeat with 38 <= Ct < 40
    """
    query = """
    SELECT COUNT(DISTINCT w.id) as count
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE w.role_alias = 'Patient'
      AND t.target_name NOT LIKE '%IPC%'
      AND t.target_name NOT LIKE '%REFERENCE%'
      AND t.target_name LIKE ?
      AND w.lims_status IN ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
      AND o.machine_ct >= 38
      AND o.machine_ct < 40
      AND DATE(w.created_at) >= ?
      AND DATE(w.created_at) <= ?
    """

    cursor = conn.cursor()
    cursor.execute(query, (assay_pattern, start_date, end_date))
    result = cursor.fetchone()
    return result['count'] if result else 0


def main():
    parser = argparse.ArgumentParser(
        description='Extract control and repeat metrics for specified assays and date ranges'
    )
    parser.add_argument(
        '--db-type',
        default='qst',
        choices=['qst', 'notts', 'vira'],
        help='Database type'
    )
    parser.add_argument(
        '--output',
        default='output_data/control_metrics.csv',
        help='Output CSV file path'
    )

    args = parser.parse_args()

    # Get database configuration
    config = get_config(args.db_type)
    db_path = config['db_path']

    # Define assays and date ranges - grouped by table/period
    # Each assay has specific date ranges per the email requirements
    assay_groups = {
        'Table 1: BPP & MYCOPLASMA (4 months)': {
            'assays': {
                'BPP': {
                    'pattern': '%BPP%',
                    'periods': [
                        ('2025-02-16', '2025-06-16'),
                        ('2025-06-17', '2025-10-17'),
                    ]
                },
                'MYCOPLASMA': {
                    'pattern': '%MYCOPLASMA%',
                    'periods': [
                        ('2025-02-16', '2025-06-16'),
                        ('2025-06-17', '2025-10-17'),
                    ]
                },
            }
        },
        'Table 2: PJ (6 months)': {
            'assays': {
                'PJ': {
                    'pattern': '%PJ%',
                    'periods': [
                        ('2024-08-17', '2025-02-17'),
                        ('2025-02-18', '2025-08-18'),
                    ]
                },
            }
        },
        'Table 3: BKV, CMV, EBV (6 months)': {
            'assays': {
                'BKV': {
                    'pattern': '%BKQ%',  # BKV uses BKQ in Quest
                    'periods': [
                        ('2024-04-13', '2024-10-15'),
                        ('2024-10-16', '2025-04-13'),
                    ]
                },
                'CMV': {
                    'pattern': '%CMV%',
                    'periods': [
                        ('2024-04-13', '2024-10-15'),
                        ('2024-10-16', '2025-04-13'),
                    ]
                },
                'EBV': {
                    'pattern': '%EBV%',
                    'periods': [
                        ('2024-04-13', '2024-10-15'),
                        ('2024-10-16', '2025-04-13'),
                    ]
                },
            }
        },
    }

    conn = None
    try:
        conn = get_db_connection(db_path)

        # Collect all results by group
        all_results = {}

        for group_name, group_data in assay_groups.items():
            table_results = []
            assays = group_data['assays']

            for assay, config in assays.items():
                pattern = config['pattern']
                periods = config['periods']

                # Extract metrics for both periods
                period1_start, period1_end = periods[0]
                period2_start, period2_end = periods[1]

                p1_controls_gte40 = extract_control_repeats_with_high_ct(conn, pattern, period1_start, period1_end)
                p1_controls_mid = extract_control_repeats_mid_ct(conn, pattern, period1_start, period1_end)
                p1_samples_gte40 = extract_sample_repeats_with_high_ct(conn, pattern, period1_start, period1_end)
                p1_samples_mid = extract_sample_repeats_mid_ct(conn, pattern, period1_start, period1_end)

                p2_controls_gte40 = extract_control_repeats_with_high_ct(conn, pattern, period2_start, period2_end)
                p2_controls_mid = extract_control_repeats_mid_ct(conn, pattern, period2_start, period2_end)
                p2_samples_gte40 = extract_sample_repeats_with_high_ct(conn, pattern, period2_start, period2_end)
                p2_samples_mid = extract_sample_repeats_mid_ct(conn, pattern, period2_start, period2_end)

                table_results.append({
                    'assay': assay,
                    'p1_start': period1_start,
                    'p1_end': period1_end,
                    'p1_controls_gte40': p1_controls_gte40,
                    'p1_controls_mid': p1_controls_mid,
                    'p1_samples_gte40': p1_samples_gte40,
                    'p1_samples_mid': p1_samples_mid,
                    'p2_start': period2_start,
                    'p2_end': period2_end,
                    'p2_controls_gte40': p2_controls_gte40,
                    'p2_controls_mid': p2_controls_mid,
                    'p2_samples_gte40': p2_samples_gte40,
                    'p2_samples_mid': p2_samples_mid,
                })

            all_results[group_name] = table_results

        # Write CSV
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)

        with open(args.output, 'w') as f:
            f.write('Group,Assay,P1 Start,P1 End,P1 Ctrl >=40,P1 Ctrl 38-40,P1 Samp >=40,P1 Samp 38-40,P2 Start,P2 End,P2 Ctrl >=40,P2 Ctrl 38-40,P2 Samp >=40,P2 Samp 38-40\n')
            for group_name, results in all_results.items():
                for row in results:
                    f.write(f"{group_name},{row['assay']},{row['p1_start']},{row['p1_end']},{row['p1_controls_gte40']},{row['p1_controls_mid']},{row['p1_samples_gte40']},{row['p1_samples_mid']},{row['p2_start']},{row['p2_end']},{row['p2_controls_gte40']},{row['p2_controls_mid']},{row['p2_samples_gte40']},{row['p2_samples_mid']}\n")

        print(f"Results written to {args.output}\n")

        # Print three separate tables
        table_num = 1
        for group_name, results in all_results.items():
            print(f"Table {table_num}: Wells Marked for Repeat\n")

            if results:
                # Get dates from first assay in this group
                first = results[0]
                p1_start = first['p1_start']
                p1_end = first['p1_end']
                p2_start = first['p2_start']
                p2_end = first['p2_end']

                # Print header with dates and CT ranges
                print(f"| ASSAY          | P1 Ctrl ≥40 | P1 Ctrl 38-40 | P1 Samp ≥40 | P1 Samp 38-40 | P2 Ctrl ≥40 | P2 Ctrl 38-40 | P2 Samp ≥40 | P2 Samp 38-40 |")
                print(f"|                | ({p1_start}→{p1_end}) | ({p1_start}→{p1_end}) | ({p1_start}→{p1_end}) | ({p1_start}→{p1_end}) | ({p2_start}→{p2_end}) | ({p2_start}→{p2_end}) | ({p2_start}→{p2_end}) | ({p2_start}→{p2_end}) |")
                print("|----------------|-------------|---------------|-------------|---------------|-------------|---------------|-------------|---------------|")

                for row in results:
                    print(f"| {row['assay']:<14} | {row['p1_controls_gte40']:>11} | {row['p1_controls_mid']:>13} | {row['p1_samples_gte40']:>11} | {row['p1_samples_mid']:>13} | {row['p2_controls_gte40']:>11} | {row['p2_controls_mid']:>13} | {row['p2_samples_gte40']:>11} | {row['p2_samples_mid']:>13} |")

            print()
            table_num += 1

    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
