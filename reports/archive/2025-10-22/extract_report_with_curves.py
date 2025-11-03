#!/usr/bin/env python3
"""Unified JSON extractor for control, sample, and discrepancy reports."""

from __future__ import annotations

import argparse
import json
import sys
import copy
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

import sqlite3

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from category_lookup import CategoryLookup
from database_configs import get_config

from .utils.report_helpers import (
    classify_control_role,
    connect_sqlite,
    decode_readings,
    fetch_comments_batch,
    fetch_targets_for_well,
    load_backup_control_mapping,
    normalize_mix_name,
    related_target_names,
)


# ---------------------------------------------------------------------------
# Date helper functions

def get_next_day(date_str: str) -> str:
    """Convert YYYY-MM-DD to the next day for inclusive date range queries.

    When filtering by created_at <= 'YYYY-MM-DD', SQLite interprets this as
    <= 'YYYY-MM-DD 00:00:00', which excludes the entire day. To include
    the full day, we use < 'YYYY-MM-DD+1'.
    """
    from datetime import datetime, timedelta
    dt = datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)
    return dt.strftime('%Y-%m-%d')

def get_prev_day(date_str: str) -> str:
    """Convert YYYY-MM-DD to the previous day for inclusive date range queries.

    When filtering by created_at >= 'YYYY-MM-DD', SQLite interprets this as
    >= 'YYYY-MM-DD 00:00:00', which excludes records on the previous day
    with timestamps after midnight. To include the full day, we use > 'YYYY-MM-DD-1'.
    """
    from datetime import datetime, timedelta
    dt = datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
    return dt.strftime('%Y-%m-%d')

# ---------------------------------------------------------------------------
# Shared dataclass for configuration


@dataclass
class ReportConfig:
    report_type: str
    add_arguments: Callable[[argparse.ArgumentParser], None]
    fetch_records: Callable[[sqlite3.Connection, argparse.Namespace], Dict[str, object]]
    enrich_records: Callable[[sqlite3.Connection, Dict[str, object], argparse.Namespace], Dict[str, object]]
    compute_summary: Callable[[Dict[str, object], argparse.Namespace], Dict[str, int]]
    build_payload: Callable[[Dict[str, object], Dict[str, object], argparse.Namespace], Dict[str, object]]
    test_limit: Optional[int] = None
    test_output: Optional[str] = None


# ---------------------------------------------------------------------------
# Sample report implementation


# Sample-specific helpers ----------------------------------------------------


INCLUDED_ERROR_TYPES = [
    'INH_WELL',
    'ADJ_CT',
    'DO_NOT_EXPORT',
    'INCONCLUSIVE_WELL',
    'CTDISC_WELL',
    'BICQUAL_WELL',
    'BAD_CT_DELTA',
    'LOW_FLUORESCENCE_WELL',
]

SETUP_ERROR_TYPES = [
    'MIX_MISSING',
    'UNKNOWN_MIX',
    'ACCESSION_MISSING',
    'INVALID_ACCESSION',
    'UNKNOWN_ROLE',
    'CONTROL_FAILURE',
    'MISSING_CONTROL',
    'INHERITED_CONTROL_FAILURE',
]


def fetch_sample_errors(
    conn: sqlite3.Connection,
    include_label_errors: bool,
    limit: Optional[int],
    category_lookup: CategoryLookup,
    *,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
    db_config: Optional[Dict[str, object]] = None,
) -> List[Dict[str, object]]:
    cursor = conn.cursor()
    limit_clause = f"LIMIT {limit}" if limit else ""

    error_types = INCLUDED_ERROR_TYPES.copy()
    if include_label_errors:
        error_types.extend(SETUP_ERROR_TYPES)

    error_types_str = "','".join(error_types)
    excluded_types = [
        'MIX_MISSING',
        'UNKNOWN_MIX',
        'ACCESSION_MISSING',
        'INVALID_ACCESSION',
        'UNKNOWN_ROLE',
        'CONTROL_FAILURE',
        'MISSING_CONTROL',
        'INHERITED_CONTROL_FAILURE',
        'WG_ERROR',
        'BLA',
    ]
    excluded_str = "','".join(excluded_types)

    unresolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        ec.error_code,
        ec.error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        w.created_at,
        'unresolved' as category
    FROM wells w
    JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.error_code_id IS NOT NULL
      AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
      AND ec.error_code IN ('{error_types_str}')
      AND ec.error_code NOT IN ('{excluded_str}')
      AND w.role_alias = 'Patient'
      AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations
        WHERE machine_cls <> dxai_cls
          OR (machine_cls IS NULL AND dxai_cls IS NOT NULL)
          OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
      )
    {f"AND w.created_at > '{get_prev_day(since_date)}'" if since_date else ''}
    {f"AND w.created_at < '{get_next_day(until_date)}'" if until_date else ''}
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """

    resolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        w.resolution_codes as error_code,
        'Resolved' as error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        w.resolution_codes,
        '' as actual_error_code,
        w.created_at,
        'resolved' as category
    FROM wells w
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL
      AND w.resolution_codes <> ''
      AND w.error_code_id IS NULL
      AND w.resolution_codes NOT LIKE '%BLA%'
      AND w.role_alias = 'Patient'
      AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations
        WHERE machine_cls <> dxai_cls
          OR (machine_cls IS NULL AND dxai_cls IS NOT NULL)
          OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
      )
    {f"AND w.created_at > '{get_prev_day(since_date)}'" if since_date else ''}
    {f"AND w.created_at < '{get_next_day(until_date)}'" if until_date else ''}
    ORDER BY m.mix_name, w.sample_name
    {limit_clause}
    """

    resolved_with_new_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        ec.error_code,
        ec.error_message || ' (was: ' || w.resolution_codes || ')' as error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        w.created_at,
        'resolved_with_new' as category
    FROM wells w
    JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL
      AND w.resolution_codes <> ''
      AND w.error_code_id IS NOT NULL
      AND ec.error_code IN ('{error_types_str}')
      AND ec.error_code NOT IN ('{excluded_str}')
      AND w.resolution_codes NOT IN ('BLA')
      AND w.role_alias = 'Patient'
      AND w.id NOT IN (
        SELECT DISTINCT well_id FROM observations
        WHERE machine_cls <> dxai_cls
          OR (machine_cls IS NULL AND dxai_cls IS NOT NULL)
          OR (machine_cls IS NOT NULL AND dxai_cls IS NULL)
      )
    {f"AND w.created_at > '{get_prev_day(since_date)}'" if since_date else ''}
    {f"AND w.created_at < '{get_next_day(until_date)}'" if until_date else ''}
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """

    def row_list(query: str) -> List[Dict[str, object]]:
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    print("  Fetching unresolved errors...")
    unresolved = row_list(unresolved_query)
    for record in unresolved:
        if record.get('created_at'):
            record['created_at'] = str(record['created_at'])[:10]
        record['clinical_category'] = 'unresolved'
    print(f"    Found {len(unresolved)} unresolved errors")

    print("  Fetching resolved errors...")
    resolved_rows = row_list(resolved_query)
    error_ignored: List[Dict[str, object]] = []
    test_repeated: List[Dict[str, object]] = []
    for record in resolved_rows:
        if record.get('created_at'):
            record['created_at'] = str(record['created_at'])[:10]

        # CSV-driven categorization
        csv_category = category_lookup.get_category(
            'SAMPLE',
            record.get('actual_error_code', ''),
            record.get('resolution_codes', ''),
            record.get('lims_status', '')
        )
        clinical_category = csv_to_clinical_category(csv_category)
        record['clinical_category'] = clinical_category

        # Group by clinical category for counts
        if clinical_category == 'error_ignored':
            error_ignored.append(record)
        elif clinical_category == 'test_repeated':
            test_repeated.append(record)
        else:
            # Default to test_repeated for unknown categories
            test_repeated.append(record)

    print(
        f"    Found {len(resolved_rows)} resolved errors ({len(error_ignored)} ignored, {len(test_repeated)} repeated)"
    )

    print("  Fetching resolved with new errors...")
    resolved_new = row_list(resolved_with_new_query)
    for record in resolved_new:
        if record.get('created_at'):
            record['created_at'] = str(record['created_at'])[:10]
        record['clinical_category'] = 'test_repeated'
    print(f"    Found {len(resolved_new)} resolved with new errors")

    return unresolved + error_ignored + test_repeated + resolved_new


def sample_enrich(
    conn: sqlite3.Connection,
    data: Dict[str, object],
    args: argparse.Namespace,
) -> Dict[str, object]:
    errors: List[Dict[str, object]] = data['errors']  # type: ignore[assignment]
    well_curves: Dict[str, Dict[str, object]] = {}
    control_cache: Dict[Tuple[str, str, str], List[Dict[str, object]]] = {}
    processed_wells: set[str] = set()
    comment_batch: List[str] = []

    for record in errors:
        well_id = str(record['well_id'])
        if well_id not in processed_wells:
            raw_targets = fetch_targets_for_well(conn, well_id)
            if raw_targets:
                targets: List[Dict[str, object]] = []
                for target in raw_targets:
                    entry = {
                        'target_name': target['target_name'],
                        'readings': target['readings'],
                        'machine_ct': target['machine_ct'],
                        'is_passive': target['is_passive'],
                        'is_ic': target['is_ic'],
                    }
                    cache_key = (record['run_id'], record['mix_name'], entry['target_name'])
                    if cache_key not in control_cache:
                        # Sample runs often store controls under the Quest (Q*) mix names,
                        # so enable backup matching to fall back across those aliases.
                        control_cache[cache_key] = get_run_control_curves(
                            conn,
                            record['run_id'],
                            record['mix_name'],
                            entry['target_name'],
                            args.max_controls,
                            allow_backup=True,
                        )
                    entry['control_curves'] = control_cache[cache_key]
                    targets.append(entry)

                # For sample reports, use first non-IC target as main_target
                main_target = None
                for t in targets:
                    if not t.get('is_ic'):
                        main_target = t['target_name']
                        break
                # If all targets are IC, use first target
                if main_target is None and targets:
                    main_target = targets[0]['target_name']

                well_curves[well_id] = {
                    'sample_name': record['sample_name'],
                    'mix_name': record['mix_name'],
                    'main_target': main_target,
                    'targets': targets,
                }
                processed_wells.add(well_id)
                comment_batch.append(well_id)

        if len(comment_batch) >= 200:
            comments = fetch_comments_batch(conn, comment_batch)
            for wid, items in comments.items():
                if wid in well_curves:
                    well_curves[wid]['comments'] = items
            comment_batch = []

    if comment_batch:
        comments = fetch_comments_batch(conn, comment_batch)
        for wid, items in comments.items():
            if wid in well_curves:
                well_curves[wid]['comments'] = items

    print(f"  Extracted {len(well_curves)} well curves")
    return {'well_curves': well_curves}


def sample_summary(data: Dict[str, object], _: argparse.Namespace) -> Dict[str, int]:
    counts = {}
    errors: List[Dict[str, object]] = data['errors']  # type: ignore
    for record in errors:
        category = record.get('clinical_category', 'unresolved')
        counts[category] = counts.get(category, 0) + 1

    return {
        'total_errors': len(errors),
        'unresolved': counts.get('unresolved', 0),
        'error_ignored': counts.get('error_ignored', 0) + counts.get('ignored', 0),  # Handle both
        'test_repeated': counts.get('test_repeated', 0),
    }


def sample_payload(
    data: Dict[str, object],
    enrichment: Dict[str, object],
    args: argparse.Namespace,
) -> Dict[str, object]:
    payload = {
        'include_label_errors': args.include_label_errors,
    }
    if args.since_date:
        payload['since_date'] = args.since_date
        payload['date_field'] = 'extraction'
    if getattr(args, 'until_date', None):
        payload['until_date'] = args.until_date
    payload.update({k: v for k, v in enrichment.items() if k != 'well_curves'})
    return payload


def sample_fetch_wrapper(conn: sqlite3.Connection, args: argparse.Namespace) -> Dict[str, object]:
    return {
        'errors': fetch_sample_errors(
            conn,
            args.include_label_errors,
            args.limit,
            args.category_lookup,
            since_date=args.since_date,
            until_date=getattr(args, 'until_date', None),
            db_config=getattr(args, 'db_config', None),
        )
    }


# ---------------------------------------------------------------------------
# Control report implementation


def fetch_control_errors(
    conn: sqlite3.Connection,
    limit: Optional[int],
    category_lookup: CategoryLookup,
    *,
    since_date: Optional[str] = None,
    until_date: Optional[str] = None,
) -> List[Dict[str, object]]:
    cursor = conn.cursor()
    limit_clause = f"LIMIT {limit}" if limit else ""

    unresolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        ec.error_code,
        ec.error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        w.created_at,
        'unresolved' as category
    FROM wells w
    JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.error_code_id IS NOT NULL
      AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
      AND ec.error_type != 0
      AND w.role_alias IS NOT NULL
      AND w.role_alias != 'Patient'
      AND (
            w.role_alias LIKE '%PC%'
            OR w.role_alias LIKE '%NC%'
            OR w.role_alias LIKE '%CONTROL%'
            OR w.role_alias LIKE '%NEGATIVE%'
            OR w.role_alias LIKE '%POSITIVE%'
            OR w.role_alias LIKE '%NTC%'
            OR w.role_alias LIKE '%PTC%'
      )
    {f"AND w.created_at > '{get_prev_day(since_date)}'" if since_date else ''}
    {f"AND w.created_at < '{get_next_day(until_date)}'" if until_date else ''}
    ORDER BY m.mix_name, ec.error_code, w.sample_name
    {limit_clause}
    """

    resolved_query = f"""
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.well_number,
        COALESCE(w.resolution_codes, ec.error_code) as error_code,
        COALESCE(ec.error_message, 'Resolved') as error_message,
        m.mix_name,
        r.run_name,
        r.id as run_id,
        w.lims_status,
        w.resolution_codes,
        COALESCE(ec.error_code, '') as actual_error_code,
        w.created_at,
        'resolved' as category
    FROM wells w
    LEFT JOIN error_codes ec ON w.error_code_id = ec.id
    JOIN runs r ON w.run_id = r.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.resolution_codes IS NOT NULL
      AND w.resolution_codes <> ''
      AND (ec.error_type IS NULL OR ec.error_type != 0)
      AND w.role_alias IS NOT NULL
      AND w.role_alias != 'Patient'
      AND (
            w.role_alias LIKE '%PC%'
            OR w.role_alias LIKE '%NC%'
            OR w.role_alias LIKE '%CONTROL%'
            OR w.role_alias LIKE '%NEGATIVE%'
            OR w.role_alias LIKE '%POSITIVE%'
            OR w.role_alias LIKE '%NTC%'
            OR w.role_alias LIKE '%PTC%'
      )
    {f"AND w.created_at > '{get_prev_day(since_date)}'" if since_date else ''}
    {f"AND w.created_at < '{get_next_day(until_date)}'" if until_date else ''}
    ORDER BY m.mix_name, w.sample_name
    {limit_clause}
    """

    def row_list(query: str) -> List[Dict[str, object]]:
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    print("  Fetching unresolved control errors...")
    unresolved = row_list(unresolved_query)
    for record in unresolved:
        if record.get('created_at'):
            record['created_at'] = str(record['created_at'])[:10]
        record['clinical_category'] = 'unresolved'
    print(f"    Found {len(unresolved)} unresolved control errors")

    print("  Fetching resolved control errors...")
    resolved = row_list(resolved_query)
    ignored: List[Dict[str, object]] = []
    repeated: List[Dict[str, object]] = []
    for record in resolved:
        if record.get('created_at'):
            record['created_at'] = str(record['created_at'])[:10]

        # CSV-driven categorization
        csv_category = category_lookup.get_category(
            'CONTROL',
            record.get('actual_error_code', ''),
            record.get('resolution_codes', ''),
            record.get('lims_status', '')
        )
        clinical_category = csv_to_clinical_category(csv_category)
        record['clinical_category'] = clinical_category

        # Group by clinical category for counts
        if clinical_category == 'test_repeated':
            repeated.append(record)
        else:
            # Default to error_ignored for other categories
            ignored.append(record)

    print(f"    Found {len(resolved)} resolved control errors ({len(ignored)} ignored, {len(repeated)} repeated)")

    return unresolved + ignored + repeated


def fetch_control_report_controls(
    conn: sqlite3.Connection,
    run_id: str,
    target_name: str,
    limit: int,
) -> List[Dict[str, object]]:
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT
        w.id as well_id,
        w.sample_name,
        w.role_alias,
        o.readings
    FROM wells w
    JOIN observations o ON w.id = o.well_id
    JOIN targets t ON o.target_id = t.id
    WHERE w.run_id = ?
      AND t.target_name = ?
      AND w.role_alias IS NOT NULL
      AND w.role_alias != 'Patient'
      AND (
            w.role_alias LIKE '%PC%'
            OR w.role_alias LIKE '%NC%'
            OR w.role_alias LIKE '%CONTROL%'
            OR w.role_alias LIKE '%NEGATIVE%'
            OR w.role_alias LIKE '%POSITIVE%'
      )
      AND o.readings IS NOT NULL
    LIMIT ?
    """

    cursor.execute(query, (run_id, target_name, limit))
    results = []
    for row in cursor.fetchall():
        control_type = classify_control_role(row['role_alias'])
        results.append(
            {
                'well_id': row['well_id'],
                'name': row['sample_name'],
                'type': control_type,
                'readings': decode_readings(row['readings']),
            }
        )
    return results


def fetch_control_well_curves(
    conn: sqlite3.Connection,
    errors: List[Dict[str, object]],
    args: argparse.Namespace,
) -> Dict[str, object]:
    well_curves: Dict[str, Dict[str, object]] = {}
    comment_batch: List[str] = []
    control_cache: Dict[Tuple[str, str, str], List[Dict[str, object]]] = {}

    for error in errors:
        well_id = str(error['well_id'])
        if well_id in well_curves:
            continue

        targets = fetch_targets_for_well(conn, well_id)
        if not targets:
            continue

        target_map: Dict[str, Dict[str, object]] = {}
        main_target: Optional[str] = None
        for target in targets:
            target_map[target['target_name']] = {
                'readings': target['readings'],
                'ct': target['machine_ct'],
                'is_ic': bool(target['is_ic']),
            }
            if main_target is None and not target['is_ic']:
                main_target = target['target_name']

        controls = []
        if main_target:
            cache_key = (error['run_id'], main_target)
            if cache_key not in control_cache:
                control_cache[cache_key] = fetch_control_report_controls(
                    conn,
                    error['run_id'],
                    main_target,
                    args.max_controls,
                )
            controls = control_cache[cache_key]

        well_curves[well_id] = {
            'main_target': main_target,
            'targets': target_map,
            'controls': controls,
        }

        comment_batch.append(well_id)
        if len(comment_batch) >= 200:
            comments = fetch_comments_batch(conn, comment_batch)
            for wid, items in comments.items():
                if wid in well_curves:
                    well_curves[wid]['comments'] = items
            comment_batch = []

    if comment_batch:
        comments = fetch_comments_batch(conn, comment_batch)
        for wid, items in comments.items():
            if wid in well_curves:
                well_curves[wid]['comments'] = items

    print(f"  Fetched curves for {len(well_curves)} control wells")
    return well_curves


def fetch_affected_samples(conn: sqlite3.Connection, category_lookup: CategoryLookup) -> Tuple[Dict[str, object], int, int]:
    cursor = conn.cursor()

    # Build list of error codes that are categorized as CONTROL_AFFECTED_SAMPLE in the CSV
    # This makes it database-agnostic and follows the categorization logic
    control_affected_codes = set()
    for key, category in category_lookup.lookup.items():
        well_type, error_code, resolution_codes, lims_status = key
        if well_type == 'SAMPLE' and category == 'CONTROL_AFFECTED_SAMPLE':
            if error_code:  # Skip empty error codes
                control_affected_codes.add(error_code)

    if not control_affected_codes:
        # Fallback if no codes found (shouldn't happen with proper CSV)
        return {}, 0, 0

    error_codes_str = "','".join(control_affected_codes)

    inherited_query = f"""
    SELECT DISTINCT
        pw.id as well_id,
        pw.sample_name,
        pw.well_number,
        pec.error_code,
        pec.error_message,
        pm.mix_name,
        pr.run_name,
        pw.lims_status,
        pw.resolution_codes,
        cw.id as control_well_id,
        cw.sample_name as control_name,
        cw.well_number as control_well,
        cm.mix_name as control_mix,
        cw.resolution_codes as control_resolution
    FROM wells pw
    JOIN error_codes pec ON pw.error_code_id = pec.id
    JOIN runs pr ON pw.run_id = pr.id
    JOIN run_mixes prm ON pw.run_mix_id = prm.id
    JOIN mixes pm ON prm.mix_id = pm.id
    JOIN wells cw ON cw.run_id = pw.run_id
    JOIN run_mixes crm ON cw.run_mix_id = crm.id
    JOIN mixes cm ON crm.mix_id = cm.id
    WHERE pec.error_code IN ('{error_codes_str}')
      AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
      AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
      AND cw.role_alias IS NOT NULL
      AND cw.role_alias != 'Patient'
      AND (cw.error_code_id IS NOT NULL OR cw.resolution_codes IS NOT NULL)
    """

    repeated_query = """
    SELECT DISTINCT
        pw.id as well_id,
        pw.sample_name,
        pw.well_number,
        '' as error_code,
        'Repeated due to control' as error_message,
        pm.mix_name,
        pr.run_name,
        pw.lims_status,
        pw.resolution_codes,
        cw.id as control_well_id,
        cw.sample_name as control_name,
        cw.well_number as control_well,
        cm.mix_name as control_mix,
        cw.resolution_codes as control_resolution
    FROM wells pw
    JOIN runs pr ON pw.run_id = pr.id
    JOIN run_mixes prm ON pw.run_mix_id = prm.id
    JOIN mixes pm ON prm.mix_id = pm.id
    JOIN wells cw ON cw.run_id = pw.run_id
    JOIN run_mixes crm ON cw.run_mix_id = crm.id
    JOIN mixes cm ON crm.mix_id = cm.id
    WHERE pw.lims_status IN ('REAMP','REXCT','RPT','RXT','TNP')
      AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
      AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
      AND cw.role_alias IS NOT NULL
      AND cw.role_alias != 'Patient'
      AND (
            cw.resolution_codes LIKE '%RP%'
            OR cw.resolution_codes LIKE '%RX%'
            OR cw.resolution_codes LIKE '%TN%'
      )
    """

    print("  Fetching affected samples...")
    cursor.execute(inherited_query)
    inherited_rows = cursor.fetchall()
    cursor.execute(repeated_query)
    repeated_rows = cursor.fetchall()

    unique_inherited = {row['well_id'] for row in inherited_rows}
    unique_repeated = {row['well_id'] for row in repeated_rows}

    grouped: Dict[str, Dict[str, object]] = {}
    for row in list(inherited_rows) + list(repeated_rows):
        group_key = f"{row['run_name']}_{row['control_mix']}"
        if group_key not in grouped:
            grouped[group_key] = {
                'run_name': row['run_name'],
                'control_mix': row['control_mix'],
                'controls': {},
                'affected_samples_error': {},
                'affected_samples_repeat': {},
            }

        control_id = row['control_well_id']
        controls = grouped[group_key]['controls']
        if control_id not in controls:
            controls[control_id] = {
                'control_name': row['control_name'],
                'control_well': row['control_well'],
                'resolution': row['control_resolution'],
            }

        sample_data = {
            'well_id': row['well_id'],
            'sample_name': row['sample_name'],
            'well_number': row['well_number'],
            'error_code': row['error_code'],
            'error_message': row['error_message'],
            'mix_name': row['mix_name'],
            'run_name': row['run_name'],
            'lims_status': row['lims_status'],
            'resolution_codes': row['resolution_codes'],
        }

        is_repeated = row['lims_status'] in ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
        if is_repeated:
            grouped[group_key]['affected_samples_repeat'][row['well_id']] = sample_data
        else:
            grouped[group_key]['affected_samples_error'][row['well_id']] = sample_data

    print(
        f"    Found {len(inherited_rows)} rows with {len(unique_inherited)} unique INHERITED affected samples"
    )
    print(
        f"    Found {len(repeated_rows)} rows with {len(unique_repeated)} unique REPEATED affected samples"
    )

    return grouped, len(unique_inherited), len(unique_repeated)


def control_enrich(
    conn: sqlite3.Connection,
    data: Dict[str, object],
    args: argparse.Namespace,
) -> Dict[str, object]:
    errors: List[Dict[str, object]] = data['errors']  # type: ignore
    if getattr(args, 'no_curves', False):
        well_curves = {}
    else:
        well_curves = fetch_control_well_curves(conn, errors, args)
    affected_samples, error_count, repeat_count = fetch_affected_samples(conn, args.category_lookup)
    print(
        f"  Affected samples: {error_count} errors, {repeat_count} repeats across {len(affected_samples)} groups"
    )
    counts_info = {
        'error': error_count,
        'repeat': repeat_count,
    }
    data['affected_counts'] = counts_info
    return {
        'well_curves': well_curves,
        'affected_samples': affected_samples,
        'affected_counts': counts_info,
    }


def control_summary(data: Dict[str, object], _: argparse.Namespace) -> Dict[str, int]:
    counts = {}
    errors: List[Dict[str, object]] = data['errors']  # type: ignore
    for record in errors:
        category = record.get('clinical_category', 'unresolved')
        counts[category] = counts.get(category, 0) + 1

    summary = {
        'total_errors': len(errors),
        'unresolved': counts.get('unresolved', 0),
        'error_ignored': counts.get('error_ignored', 0) + counts.get('ignored', 0),  # Handle both 'error_ignored' and 'ignored'
        'test_repeated': counts.get('test_repeated', 0),
    }
    counts_info = data.get('affected_counts')  # type: ignore[arg-type]
    if isinstance(counts_info, dict):
        summary['affected_error_count'] = counts_info.get('error', 0)
        summary['affected_repeat_count'] = counts_info.get('repeat', 0)
    return summary


def control_payload(
    data: Dict[str, object],
    enrichment: Dict[str, object],
    args: argparse.Namespace,
) -> Dict[str, object]:
    payload = {
        'affected_samples': enrichment.get('affected_samples', {}),
    }
    if args.since_date:
        payload['since_date'] = args.since_date
        payload['date_field'] = 'extraction'
    if getattr(args, 'until_date', None):
        payload['until_date'] = args.until_date
    return payload


def control_fetch_wrapper(conn: sqlite3.Connection, args: argparse.Namespace) -> Dict[str, object]:
    return {
        'errors': fetch_control_errors(
            conn,
            args.limit,
            args.category_lookup,
            since_date=args.since_date,
            until_date=getattr(args, 'until_date', None),
        )
    }


# ---------------------------------------------------------------------------
# Discrepancy report implementation


def get_run_control_curves(
    conn: sqlite3.Connection,
    run_id: str,
    mix_name: str,
    target_name: str,
    max_controls: int,
    *,
    allow_backup: bool,
) -> List[Dict[str, object]]:
    related = related_target_names(target_name) if allow_backup else [target_name]
    if not related:
        return []

    placeholders = ','.join(['?'] * len(related))
    query = f"""
    SELECT
        w.role_alias,
        w.sample_label,
        o.readings,
        o.machine_ct,
        m.mix_name,
        t.target_name
    FROM wells w
    JOIN observations o ON o.well_id = w.id
    JOIN targets t ON o.target_id = t.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.run_id = ?
      AND t.target_name IN ({placeholders})
      AND w.role_alias IS NOT NULL
      AND w.role_alias != 'Patient'
      AND (
            w.role_alias LIKE '%NC' OR
            w.role_alias LIKE '%PC' OR
            w.role_alias LIKE '%HPC' OR
            w.role_alias LIKE '%LPC' OR
            w.role_alias LIKE '%NEG' OR
            w.role_alias = 'NEGATIVE' OR
            w.role_alias = 'PC' OR
            w.role_alias = 'NC'
      )
      AND t.is_passive = 0
      AND o.readings IS NOT NULL
    ORDER BY
      CASE WHEN m.mix_name = ? THEN 0 ELSE 1 END,
      CASE WHEN UPPER(t.target_name) = UPPER(?) THEN 0 ELSE 1 END,
      w.role_alias
    LIMIT ?
    """

    cursor = conn.cursor()
    params = (run_id, *related, mix_name, target_name, max_controls * 5)
    cursor.execute(query, params)

    same_mix: List[Dict[str, object]] = []
    fallback: List[Dict[str, object]] = []
    backup_map = load_backup_control_mapping() if allow_backup else {}
    normalized_mix = normalize_mix_name(mix_name)

    for row in cursor.fetchall():
        readings = decode_readings(row['readings'])
        if not readings:
            continue

        ctrl_type = classify_control_role(row['role_alias'])
        control = {
            'role_alias': row['role_alias'],
            'label': row['sample_label'],
            'readings': readings,
            'machine_ct': row['machine_ct'],
            'mix_name': row['mix_name'],
            'target_name': row['target_name'],
            'control_type': ctrl_type,
        }

        if row['mix_name'] == mix_name:
            same_mix.append(control)
            continue

        if allow_backup:
            control_norm = normalize_mix_name(row['mix_name'])
            if (
                row['mix_name'].startswith(mix_name)
                or mix_name.startswith(row['mix_name'])
                or control_norm == normalized_mix
            ):
                fallback.append(control)
                continue

            mapping = backup_map.get(mix_name)
            if mapping:
                if ctrl_type == 'positive' and row['role_alias'] in mapping['PC']:
                    fallback.append(control)
                elif ctrl_type == 'negative' and row['role_alias'] in mapping['NC']:
                    fallback.append(control)

    def balance(controls: List[Dict[str, object]]) -> List[Dict[str, object]]:
        negatives = [c for c in controls if c['control_type'] == 'negative']
        positives = [c for c in controls if c['control_type'] == 'positive']
        others = [c for c in controls if c['control_type'] not in {'negative', 'positive'}]

        result: List[Dict[str, object]] = []
        if negatives:
            result.extend(negatives[: min(2, len(negatives))])
        remaining = max_controls - len(result)
        if remaining > 0 and positives:
            result.extend(positives[:remaining])
        remaining = max_controls - len(result)
        if remaining > 0 and others:
            result.extend(others[:remaining])
        return result

    final_controls = balance(same_mix)

    if allow_backup:
        has_negative = any(c['control_type'] == 'negative' for c in final_controls)
        has_positive = any(c['control_type'] == 'positive' for c in final_controls)

        if len(final_controls) < max_controls:
            for control in balance(fallback):
                if control in final_controls:
                    continue
                if final_controls and has_negative and has_positive:
                    break
                if control['control_type'] == 'negative' and not has_negative:
                    final_controls.append(control)
                    has_negative = True
                elif control['control_type'] == 'positive' and not has_positive:
                    final_controls.append(control)
                    has_positive = True
                elif not final_controls:
                    final_controls.append(control)
                if len(final_controls) >= max_controls:
                    break

        if not final_controls and fallback:
            final_controls = balance(fallback)

    output: List[Dict[str, object]] = []
    for control in final_controls:
        if control['control_type'] == 'negative':
            ctrl_code = 'NC'
        elif control['control_type'] == 'positive':
            ctrl_code = 'PC'
        else:
            ctrl_code = 'CTRL'
        output.append(
            {
                'readings': control['readings'],
                'machine_ct': control['machine_ct'],
                'control_type': ctrl_code,
            }
        )

    return output


def fetch_discrepancy_records(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
) -> Dict[str, object]:
    cursor = conn.cursor()
    limit_clause = f"LIMIT {int(args.limit)}" if args.limit else ""
    date_column = 'r.created_at' if args.date_field == 'upload' else 'w.created_at'
    date_clause = f" AND {date_column} > '{get_prev_day(args.since_date)}'" if args.since_date else ""
    date_clause += f" AND {date_column} < '{get_next_day(args.until_date)}'" if getattr(args, 'until_date', None) else ""

    # Build excluded error codes from CSV (control-affected samples and no-controls)
    excluded_error_codes = set()
    for key, category in args.category_lookup.lookup.items():
        well_type, error_code, resolution_codes, lims_status = key
        if well_type == 'SAMPLE' and category in ('CONTROL_AFFECTED_SAMPLE', 'NO_CONTROLS'):
            if error_code:  # Skip empty error codes
                excluded_error_codes.add(error_code)

    # Build error code exclusion filter
    error_exclusion = ""
    if excluded_error_codes:
        excluded_str = "','".join(excluded_error_codes)
        error_exclusion = f" AND (ec.error_code NOT IN ('{excluded_str}') OR ec.error_code IS NULL)"

    # Early filtering query - match user's SQL query patterns exactly
    # Exclude IC-only discrepancies: only include wells with at least one non-IC discrepancy
    query = f"""
    WITH discrepancy_wells AS (
        SELECT DISTINCT w.id
        FROM wells w
        JOIN observations o ON o.well_id = w.id
        JOIN runs r ON w.run_id = r.id
        JOIN targets t ON o.target_id = t.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias = 'Patient'
          AND o.dxai_cls IS NOT NULL
          AND o.machine_cls <> o.dxai_cls
          AND (t.type IS NULL OR t.type != 1)  -- Exclude IC target types (type=1), include regular targets (type=NULL)
          -- Filter excluded error codes (control-affected samples)
          {error_exclusion}
          -- Only fetch categorizable wells (match user's SQL patterns)
          AND (
              -- Has BLA (for acted_upon/ignored categories)
              (w.resolution_codes LIKE '%bla%' OR w.resolution_codes LIKE '%BLA%')
              OR
              -- No valid LIMS (for samples_repeated category)
              (w.lims_status IS NULL
               OR (w.lims_status NOT LIKE '%detected%' AND w.lims_status NOT LIKE '%1500%'))
          )
          {date_clause}
    )
    SELECT
        w.id AS well_id,
        COALESCE(w.sample_label, w.sample_name) AS sample_name,
        w.sample_name AS sample_name_lims,
        w.well_number,
        w.lims_status,
        w.resolution_codes,
        w.error_code_id,
        ec.error_code,
        ec.error_message,
        m.mix_name,
        r.run_name,
        r.id AS run_id,
        w.created_at
    FROM wells w
    JOIN discrepancy_wells dw ON dw.id = w.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    JOIN runs r ON w.run_id = r.id
    LEFT JOIN error_codes ec ON ec.id = w.error_code_id
    ORDER BY m.mix_name, w.sample_label, w.well_number
    {limit_clause}
    """

    cursor.execute(query)
    errors = [dict(row) for row in cursor.fetchall()]
    return {'errors': errors}


def discrepancy_enrich(
    conn: sqlite3.Connection,
    data: Dict[str, object],
    args: argparse.Namespace,
) -> Dict[str, object]:
    errors: List[Dict[str, object]] = data['errors']  # type: ignore
    well_curves: Dict[str, Dict[str, object]] = {}
    control_cache: Dict[Tuple[str, str, str], List[Dict[str, object]]] = {}
    comment_batch: List[str] = []

    for record in errors:
        well_id = str(record['well_id'])
        targets = fetch_targets_for_well(conn, well_id)
        if not targets:
            continue

        for target in targets:
            cache_key = (record['run_id'], record['mix_name'], target['target_name'])
            if cache_key not in control_cache:
                control_cache[cache_key] = get_run_control_curves(
                    conn,
                    record['run_id'],
                    record['mix_name'],
                    target['target_name'],
                    args.max_controls,
                    allow_backup=True,
                )
            target['control_curves'] = control_cache[cache_key]

        # User's SQL queries check ALL observations (including IC) for categorization
        # We only filter IC when displaying (primary target selection)

        # Skip records where NO target has complete classification data
        has_complete_cls_data = any(
            t.get('machine_cls') is not None
            and t.get('dxai_cls') is not None
            and t.get('final_cls') is not None
            for t in targets
        )
        if not has_complete_cls_data:
            continue

        # Priority-based categorization using ALL observations (including IC)
        # Priority 1: acted_upon (ANY obs has final==dxai + valid LIMS + BLA)
        # Priority 2: samples_repeated (NO valid LIMS)
        # Priority 3: ignored (ANY obs has final==machine + valid LIMS + BLA)

        lims_status = record.get('lims_status', '') or ''
        resolution_codes = record.get('resolution_codes', '') or ''

        lims_lower = lims_status.lower()
        has_bla = 'bla' in resolution_codes.lower()
        has_detected = 'detected' in lims_lower
        has_1500 = '1500' in lims_lower
        has_valid_lims = has_detected or has_1500

        # Check ALL observations (including IC) that HAVE DISCREPANCIES
        has_acted_upon_obs = False
        has_ignored_obs = False

        for obs in targets:
            machine_cls = obs.get('machine_cls')
            dxai_cls = obs.get('dxai_cls')
            final_cls = obs.get('final_cls')

            # Only check observations with actual discrepancies
            if machine_cls == dxai_cls:
                continue  # No discrepancy in this observation

            if final_cls == dxai_cls and has_valid_lims and has_bla:
                has_acted_upon_obs = True
            if final_cls == machine_cls and has_valid_lims and has_bla:
                has_ignored_obs = True

        # Keep non-IC targets for display
        non_ic_targets = [t for t in targets if not t['is_ic']]

        # Apply priority logic
        if has_acted_upon_obs:
            # Priority 1: At least one observation was acted upon
            record['clinical_category'] = 'acted_upon'
            record['category_detail'] = 'result_changed'
        elif not has_valid_lims:
            # Priority 2: No valid LIMS status = sample repeated
            record['clinical_category'] = 'samples_repeated'
            record['category_detail'] = 'unresolved_discrepancy'
        elif has_ignored_obs:
            # Priority 3: At least one observation was ignored (kept as machine)
            record['clinical_category'] = 'ignored'
            record['category_detail'] = 'discrepancy_acknowledged'
        else:
            # Doesn't match any category - suppress
            continue

        # Get primary target for display
        # For discrepancy reports, prioritize the target with the actual discrepancy
        primary_target = None
        display_targets = non_ic_targets if non_ic_targets else targets

        # First, try to find a discrepant target (machine_cls != dxai_cls)
        for candidate in display_targets:
            if (candidate.get('machine_cls') is not None
                and candidate.get('dxai_cls') is not None
                and candidate.get('machine_cls') != candidate.get('dxai_cls')):
                primary_target = candidate
                break

        # If no discrepant target found, fall back to first target with complete data
        if primary_target is None:
            for candidate in display_targets:
                if candidate.get('machine_cls') is not None or candidate.get('final_cls') is not None:
                    primary_target = candidate
                    break

        # Last resort: use first available target
        if primary_target is None:
            primary_target = display_targets[0] if display_targets else targets[0]

        record['target_name'] = primary_target.get('target_name')
        record['machine_cls'] = primary_target.get('machine_cls')
        record['dxai_cls'] = primary_target.get('dxai_cls')
        record['final_cls'] = primary_target.get('final_cls')
        record['machine_ct'] = primary_target.get('machine_ct')
        record['targets_reviewed'] = [
            {
                'target_name': t['target_name'],
                'machine_cls': t['machine_cls'],
                'dxai_cls': t['dxai_cls'],
                'final_cls': t['final_cls'],
            }
            for t in display_targets
        ]
        if not record.get('error_message'):
            record['error_message'] = record.get('error_code')

        well_curves[well_id] = {
            'sample_name': record['sample_name'],
            'mix_name': record['mix_name'],
            'run_name': record['run_name'],
            'main_target': primary_target.get('target_name'),
            'targets': targets,
        }
        comment_batch.append(well_id)

        if len(comment_batch) >= 200:
            comments = fetch_comments_batch(conn, comment_batch)
            for wid, items in comments.items():
                if wid in well_curves:
                    well_curves[wid]['comments'] = items
            comment_batch = []

    if comment_batch:
        comments = fetch_comments_batch(conn, comment_batch)
        for wid, items in comments.items():
            if wid in well_curves:
                well_curves[wid]['comments'] = items

    return {'well_curves': well_curves}


def discrepancy_summary(data: Dict[str, object], _: argparse.Namespace) -> Dict[str, int]:
    counts = {}
    errors: List[Dict[str, object]] = data['errors']  # type: ignore
    # Only count records that have been categorized (suppressed/discarded records have None category)
    categorized_errors = [e for e in errors if e.get('clinical_category') is not None]

    for record in categorized_errors:
        category = record.get('clinical_category')
        counts[category] = counts.get(category, 0) + 1

    unique_samples = len({record['sample_name'] for record in categorized_errors})
    return {
        'total_wells': len(categorized_errors),
        'unique_samples': unique_samples,
        'acted_upon': counts.get('acted_upon', 0),
        'samples_repeated': counts.get('samples_repeated', 0),
        'ignored': counts.get('ignored', 0) + counts.get('error_ignored', 0),  # Handle both
    }


def discrepancy_payload(
    data: Dict[str, object],
    enrichment: Dict[str, object],
    args: argparse.Namespace,
) -> Dict[str, object]:
    payload = {
        'since_date': args.since_date,
        'date_field': args.date_field,
    }
    if getattr(args, 'until_date', None):
        payload['until_date'] = args.until_date
    return payload


# ---------------------------------------------------------------------------
# CLI orchestration


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument('--db-type', choices=['qst', 'notts', 'vira'], default='qst',
                       help='Database type for CSV-driven categorization (default: qst)')
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    parser.add_argument('--output', required=True, help='Output JSON file')
    parser.add_argument('--limit', type=int, help='Limit number of records processed')
    parser.add_argument('--max-controls', type=int, default=3, help='Maximum controls per target')
    parser.add_argument('--test', action='store_true', help='Test mode (implies --limit=100)')
    parser.add_argument('--legacy-output', help='Optional path to write a second copy for diffing')
    parser.add_argument('--compat-mode', action='store_true', help='Enable strict legacy compatibility toggles')
    parser.add_argument('--generated-at', help='Override generated_at timestamp for reproducible diffs')


def sample_add_arguments(parser: argparse.ArgumentParser) -> None:
    add_common_arguments(parser)
    parser.add_argument('--include-label-errors', action='store_true', help='Include label/setup errors')
    parser.add_argument('--since-date', help='Filter wells on/after this extraction date (YYYY-MM-DD)')


def control_add_arguments(parser: argparse.ArgumentParser) -> None:
    add_common_arguments(parser)
    parser.add_argument('--no-curves', action='store_true', help='Skip extracting well curve data')
    parser.add_argument('--since-date', help='Filter wells on/after this extraction date (YYYY-MM-DD)')


def discrepancy_add_arguments(parser: argparse.ArgumentParser) -> None:
    add_common_arguments(parser)
    parser.add_argument('--since-date', default='2024-01-01', help="Filter runs on/after this date (default: 2024-01-01)")
    parser.add_argument('--until-date', help='Filter runs on/before this date (YYYY-MM-DD)')
    parser.add_argument('--date-field', choices=['upload', 'extraction'], default='upload', help='Date field for since-date filtering')


SAMPLE_CONFIG = ReportConfig(
    report_type='sample',
    add_arguments=sample_add_arguments,
    fetch_records=sample_fetch_wrapper,
    enrich_records=sample_enrich,
    compute_summary=sample_summary,
    build_payload=sample_payload,
    test_limit=100,
    test_output='sample_data_with_curves_test.json',
)

CONTROL_CONFIG = ReportConfig(
    report_type='control',
    add_arguments=control_add_arguments,
    fetch_records=control_fetch_wrapper,
    enrich_records=control_enrich,
    compute_summary=control_summary,
    build_payload=control_payload,
    test_limit=10,
    test_output='control_data_test_curves.json',
)

DISCREPANCY_CONFIG = ReportConfig(
    report_type='discrepancy',
    add_arguments=discrepancy_add_arguments,
    fetch_records=fetch_discrepancy_records,
    enrich_records=discrepancy_enrich,
    compute_summary=discrepancy_summary,
    build_payload=discrepancy_payload,
    test_limit=100,
    test_output='classification_discrepancies_with_curves_test.json',
)


CONFIG_MAP = {
    'sample': SAMPLE_CONFIG,
    'control': CONTROL_CONFIG,
    'discrepancy': DISCREPANCY_CONFIG,
}


def csv_to_clinical_category(csv_category: str) -> str:
    """
    Map CSV category to clinical_category used in JSON reports.

    CSV categories are more granular, while clinical_category groups them
    into broader categories for the report UI.
    """
    mapping = {
        'SOP_UNRESOLVED': 'unresolved',
        'SOP_IGNORED': 'error_ignored',
        'SOP_REPEATED': 'test_repeated',
        'DISCREP_RESULT_CHANGED': 'acted_upon',
        'DISCREP_IGNORED': 'ignored',
        'DISCREP_IN_ERROR': 'samples_repeated',
        'DISCREP_NEEDS_CLS_DATA': 'needs_review',
        'VALID_DETECTED': 'valid',
        'VALID_NOT_DETECTED': 'valid',
        'CONTROL_PASS': 'control_pass',
        'CONTROL_FAIL': 'control_fail',
        'CONTROL_REPEATED': 'test_repeated',
        'IGNORE_WELL': 'ignored',
    }
    return mapping.get(csv_category, 'unresolved')


def init_category_lookup(args: argparse.Namespace) -> None:
    """
    Initialize CategoryLookup based on db_type and attach to args.

    This allows all fetch/enrich functions to access the category lookup
    via args.category_lookup.
    """
    if not hasattr(args, 'category_lookup') or args.category_lookup is None:
        db_type = getattr(args, 'db_type', 'qst')
        config = get_config(db_type)
        args.category_lookup = CategoryLookup(config['category_csv'], config['lims_mapping'])
        args.db_config = config


def build_report_payload(config: ReportConfig, args: argparse.Namespace) -> Dict[str, object]:
    args_copy = copy.deepcopy(args)

    # Initialize CategoryLookup for CSV-driven categorization
    init_category_lookup(args_copy)

    if getattr(args_copy, 'test', False):
        if config.test_limit is not None:
            args_copy.limit = min(args_copy.limit or config.test_limit, config.test_limit)
        elif args_copy.limit is None or args_copy.limit > 100:
            args_copy.limit = 100

    conn = connect_sqlite(args_copy.db)
    try:
        data = config.fetch_records(conn, args_copy)
        enrichment = config.enrich_records(conn, data, args_copy)
        summary = config.compute_summary(data, args_copy)
    finally:
        conn.close()

    generated_at = getattr(args_copy, 'generated_at', None) or datetime.now().isoformat()

    payload: Dict[str, object] = {
        'report_type': config.report_type,
        'generated_at': generated_at,
        'database': args_copy.db,
    }

    extra = config.build_payload(data, enrichment, args_copy)
    if 'include_label_errors' in extra:
        payload['include_label_errors'] = extra.pop('include_label_errors')
    if 'since_date' in extra:
        payload['since_date'] = extra.pop('since_date')
    if 'date_field' in extra:
        payload['date_field'] = extra.pop('date_field')

    payload['summary'] = summary
    # For discrepancy reports, filter out uncategorized/suppressed records
    if config.report_type == 'discrepancy':
        payload['errors'] = [e for e in data['errors'] if e.get('clinical_category') is not None]
    else:
        payload['errors'] = data['errors']
    payload['well_curves'] = enrichment.get('well_curves', {})
    payload.update(extra)

    return payload


def run_report(config: ReportConfig, args: argparse.Namespace) -> None:
    if getattr(args, 'test', False) and config.test_output:
        args.output = config.test_output

    payload = build_report_payload(config, args)

    with open(args.output, 'w') as handle:
        json.dump(payload, handle, indent=2, default=str)

    print(f"Report written to {args.output}")
    print(f"  Total errors: {len(payload['errors'])}")

    if args.legacy_output:
        with open(args.legacy_output, 'w') as handle:
            json.dump(payload, handle, indent=2, default=str)
        print(f"Legacy copy written to {args.legacy_output}")


def fetch_valid_results(
    conn: sqlite3.Connection,
    sample_since_date: Optional[str],
    control_since_date: Optional[str],
    sample_until_date: Optional[str] = None,
    control_until_date: Optional[str] = None,
    db_type: str = 'qst',
) -> Dict[str, Dict[str, int]]:
    """
    Fetch valid results summary by mix:
    - Samples: DETECTED vs NOT DETECTED counts
    - Controls: passed vs total counts

    Returns dict with structure:
    {
        'mix_name': {
            'samples_detected': int,
            'samples_not_detected': int,
            'controls_passed': int,
            'controls_total': int,
        },
        ...
    }
    """
    # Database-specific control detection patterns
    control_patterns = {
        'qst': """(
            w.role_alias LIKE '%PC'
            OR w.role_alias LIKE '%NC'
            OR w.role_alias IN ('PC', 'NC', 'HPC', 'LPC')
        )""",
        'notts': """(
            w.role_alias LIKE '%QS%'
            OR w.role_alias LIKE '%PC%'
            OR w.role_alias LIKE '%NC%'
            OR w.role_alias LIKE '%NTC%'
            OR w.role_alias LIKE '%NEG%'
            OR w.role_alias LIKE '%Neg%'
            OR w.role_alias LIKE '%neg%'
            OR w.role_alias LIKE '%NIBSC%'
        )""",
        'vira': """(
            w.role_alias = 'NEC'
            OR w.role_alias LIKE '%POS%'
            OR w.role_alias IN ('CC1', 'CC2', 'NTC', 'S2', 'S4', 'S6')
        )""",
    }

    # Database-specific LIMS status variants for valid results
    lims_variants = {
        'qst': ['DETECTED', 'NOT DETECTED'],
        'notts': [
            'DETECTED', 'NOT DETECTED', '<1500',
            'HSV_1_DETECTED', 'HSV_2_DETECTED', 'HSV_1_2_DETECTED', 'HSV_1_VZV_DETECTED',
            'BKV_DETECTED', 'ADENOVIRUS_DETECTED', 'VZV_DETECTED',
            'Detected <500IU/ml', 'Detected_btw_loq_lod'
        ],
        'vira': [
            'DETECTED', 'NOT DETECTED',
            'DETECTED_QUANT', 'DETECTED_LOQ', 'DETECTED_HIQ'
        ],
    }

    control_where = control_patterns.get(db_type.lower(), control_patterns['qst'])
    lims_list = lims_variants.get(db_type.lower(), lims_variants['qst'])

    # Build LIMS IN clause
    lims_in_clause = '(' + ', '.join(f"'{status}'" for status in lims_list) + ')'

    cursor = conn.cursor()

    # Query samples grouped by mix (valid LIMS statuses only, excluding controls)
    sample_query = f"""
    SELECT
        m.mix_name,
        w.lims_status,
        COUNT(*) as cnt
    FROM wells w
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE w.lims_status IN {lims_in_clause}
      AND w.role_alias = 'Patient'
    """
    if sample_since_date:
        sample_query += f" AND w.created_at > '{get_prev_day(sample_since_date)}'"
    if sample_until_date:
        sample_query += f" AND w.created_at < '{get_next_day(sample_until_date)}'"
    sample_query += " GROUP BY m.mix_name, w.lims_status ORDER BY m.mix_name"

    # Query controls grouped by mix (passed = no error and null/normal lims_status)
    control_query = f"""
    SELECT
        m.mix_name,
        COUNT(*) as total,
        SUM(CASE WHEN w.error_code_id IS NULL
                  AND (w.lims_status IS NULL OR w.lims_status = 'Normal')
             THEN 1 ELSE 0 END) as passed
    FROM wells w
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE {control_where}
    """
    if control_since_date:
        control_query += f" AND w.created_at > '{get_prev_day(control_since_date)}'"
    if control_until_date:
        control_query += f" AND w.created_at < '{get_next_day(control_until_date)}'"
    control_query += " GROUP BY m.mix_name ORDER BY m.mix_name"

    # Query for total sample counts (ALL samples, not just those with recognized LIMS)
    # This includes samples with errors and NULL LIMS status
    total_samples_query = f"""
    SELECT
        m.mix_name,
        COUNT(*) as total_samples
    FROM wells w
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE NOT {control_where}
      AND w.role_alias = 'Patient'
    """
    if sample_since_date:
        total_samples_query += f" AND w.created_at > '{get_prev_day(sample_since_date)}'"
    if sample_until_date:
        total_samples_query += f" AND w.created_at < '{get_next_day(sample_until_date)}'"
    total_samples_query += " GROUP BY m.mix_name ORDER BY m.mix_name"

    # Build results dictionary
    results = {}

    # Process sample results (with recognized LIMS status)
    cursor.execute(sample_query)
    for row in cursor.fetchall():
        mix_name = row['mix_name']
        lims_status = row['lims_status']
        cnt = row['cnt']

        if mix_name not in results:
            results[mix_name] = {
                'samples_detected': 0,
                'samples_not_detected': 0,
                'controls_passed': 0,
                'controls_total': 0,
                'total_samples': 0,
            }

        # Categorize LIMS status as detected or not detected
        # Variants ending in _DETECTED or containing 'Detected' are detected results
        # '<1500' and 'NOT DETECTED' are not detected results
        if lims_status == 'NOT DETECTED' or lims_status == '<1500':
            results[mix_name]['samples_not_detected'] += cnt
        elif 'DETECTED' in lims_status or 'Detected' in lims_status:
            results[mix_name]['samples_detected'] += cnt

    # Process total sample counts (ALL samples including errors with NULL LIMS)
    cursor.execute(total_samples_query)
    for row in cursor.fetchall():
        mix_name = row['mix_name']
        total_samples = row['total_samples']

        if mix_name not in results:
            results[mix_name] = {
                'samples_detected': 0,
                'samples_not_detected': 0,
                'controls_passed': 0,
                'controls_total': 0,
                'total_samples': 0,
            }

        results[mix_name]['total_samples'] = total_samples

    # Process control results
    cursor.execute(control_query)
    for row in cursor.fetchall():
        mix_name = row['mix_name']
        total = row['total']
        passed = row['passed']

        if mix_name not in results:
            results[mix_name] = {
                'samples_detected': 0,
                'samples_not_detected': 0,
                'controls_passed': 0,
                'controls_total': 0,
            }

        results[mix_name]['controls_total'] = total
        results[mix_name]['controls_passed'] = passed

    return results


def calculate_error_statistics(
    sample_payload: Dict[str, object],
    control_payload: Dict[str, object],
    discrepancy_payload: Dict[str, object],
) -> Dict[str, Dict[str, int]]:
    """
    Calculate error statistics by mix for all report types.

    Returns dict with structure:
    {
        'mix_name': {
            'sop_errors': int,
            'sop_errors_affected': int,  # unresolved + test_repeated
            'control_errors': int,
            'control_errors_affected': int,  # unresolved + test_repeated
            'samples_affected_by_controls': int,  # from affected_samples
            'classification_errors': int,
            'classification_errors_affected': int,  # acted_upon + samples_repeated
        },
        ...
    }
    """
    stats = {}

    # Process sample errors
    sample_errors = sample_payload.get('errors', [])
    for error in sample_errors:
        mix_name = error.get('mix_name', 'Unknown')
        clinical_category = error.get('clinical_category', '')

        if mix_name not in stats:
            stats[mix_name] = {
                'sop_errors': 0,
                'sop_errors_affected': 0,
                'control_errors': 0,
                'control_errors_affected': 0,
                'samples_affected_by_controls': 0,
                'classification_errors': 0,
                'classification_errors_affected': 0,
            }

        stats[mix_name]['sop_errors'] += 1
        if clinical_category in ('unresolved', 'test_repeated'):
            stats[mix_name]['sop_errors_affected'] += 1

    # Process control errors
    control_errors = control_payload.get('errors', [])
    for error in control_errors:
        mix_name = error.get('mix_name', 'Unknown')
        clinical_category = error.get('clinical_category', '')

        if mix_name not in stats:
            stats[mix_name] = {
                'sop_errors': 0,
                'sop_errors_affected': 0,
                'control_errors': 0,
                'control_errors_affected': 0,
                'samples_affected_by_controls': 0,
                'classification_errors': 0,
                'classification_errors_affected': 0,
            }

        stats[mix_name]['control_errors'] += 1
        if clinical_category in ('unresolved', 'test_repeated'):
            stats[mix_name]['control_errors_affected'] += 1

    # Process affected samples from control errors
    # affected_samples is keyed by run_name, but contains control_mix for actual mix name
    affected_samples = control_payload.get('affected_samples', {})
    affected_by_mix = {}  # Group by actual mix name

    for run_name, affected_data in affected_samples.items():
        # Get the actual mix name from control_mix field
        mix_name = affected_data.get('control_mix', 'Unknown')

        # Count affected samples (both error and repeat are dicts with well_id keys)
        error_count = len(affected_data.get('affected_samples_error', {}))
        repeat_count = len(affected_data.get('affected_samples_repeat', {}))
        total_affected = error_count + repeat_count

        if mix_name not in affected_by_mix:
            affected_by_mix[mix_name] = 0
        affected_by_mix[mix_name] += total_affected

    # Add affected samples counts to stats
    for mix_name, count in affected_by_mix.items():
        if mix_name not in stats:
            stats[mix_name] = {
                'sop_errors': 0,
                'sop_errors_affected': 0,
                'control_errors': 0,
                'control_errors_affected': 0,
                'samples_affected_by_controls': 0,
                'classification_errors': 0,
                'classification_errors_affected': 0,
            }
        stats[mix_name]['samples_affected_by_controls'] = count

    # Process discrepancy errors
    discrepancy_errors = discrepancy_payload.get('errors', [])
    for error in discrepancy_errors:
        # Skip uncategorized/suppressed records (those without a clinical_category)
        clinical_category = error.get('clinical_category')
        if clinical_category is None:
            continue

        mix_name = error.get('mix_name', 'Unknown')

        if mix_name not in stats:
            stats[mix_name] = {
                'sop_errors': 0,
                'sop_errors_affected': 0,
                'control_errors': 0,
                'control_errors_affected': 0,
                'samples_affected_by_controls': 0,
                'classification_errors': 0,
                'classification_errors_affected': 0,
            }

        stats[mix_name]['classification_errors'] += 1
        if clinical_category in ('acted_upon', 'samples_repeated'):
            stats[mix_name]['classification_errors_affected'] += 1

    return stats


def run_combined_report(args: argparse.Namespace) -> None:
    """Generate all 4 report formats using unified_json_extractor.py with unlimited rendering"""
    import subprocess
    import sys

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Build command for unified_json_extractor with all parameters
    cmd = [
        sys.executable, '-m', 'reports.unified_json_extractor',
        '--db-type', getattr(args, 'db_type', 'qst'),
        '--db', args.db,
        '--output', args.output,
    ]

    # Add date parameters
    if args.sample_since_date:
        cmd.extend(['--sample-since-date', args.sample_since_date])
    if getattr(args, 'sample_until_date', None):
        cmd.extend(['--sample-until-date', args.sample_until_date])
    if args.control_since_date:
        cmd.extend(['--control-since-date', args.control_since_date])
    if getattr(args, 'control_until_date', None):
        cmd.extend(['--control-until-date', args.control_until_date])
    if args.discrepancy_since_date:
        cmd.extend(['--discrepancy-since-date', args.discrepancy_since_date])
    if getattr(args, 'discrepancy_until_date', None):
        cmd.extend(['--discrepancy-until-date', args.discrepancy_until_date])
    if getattr(args, 'discrepancy_date_field', None):
        cmd.extend(['--discrepancy-date-field', args.discrepancy_date_field])

    # Add new filtering parameters
    if getattr(args, 'exclude_from_sop', None):
        cmd.append('--exclude-from-sop')
        cmd.extend(args.exclude_from_sop)
    if getattr(args, 'exclude_from_control', None):
        cmd.append('--exclude-from-control')
        cmd.extend(args.exclude_from_control)
    if getattr(args, 'suppress_unaffected_controls', False):
        cmd.append('--suppress-unaffected-controls')
    if getattr(args, 'site_ids', None):
        cmd.append('--site-ids')
        cmd.extend(args.site_ids)

    # Add other parameters
    if getattr(args, 'sample_include_label_errors', False):
        cmd.append('--sample-include-label-errors')

    print("\n=== Generating combined JSON report using unified_json_extractor ===")
    result = subprocess.run(cmd, check=True)

    if result.returncode != 0:
        raise RuntimeError(f"JSON extraction failed with code {result.returncode}")

    print(f"\n✅ JSON report written to {args.output}")

    # Load the generated JSON
    with open(args.output, 'r') as f:
        combined_payload = json.load(f)

    # Auto-generate all output formats with UNLIMITED rendering
    base_name = args.output.replace('.json', '')

    # Generate combined HTML (UNLIMITED - max_per_category=0)
    if getattr(args, 'html_output', None):
        html_output = args.html_output
    else:
        html_output = f"{base_name}_combined.html"

    print(f"\n=== Generating combined HTML report (UNLIMITED) ===")
    from .generate_report_from_json_with_graphs import generate_combined_html
    html_dir = os.path.dirname(html_output)
    if html_dir:
        os.makedirs(html_dir, exist_ok=True)
    totals = generate_combined_html(
        combined_payload,
        html_output,
        max_per_category=0,  # UNLIMITED
    )
    print(f"✅ Combined HTML written to {html_output}")
    for key, total in totals.items():
        print(f"  {key}: {total} errors rendered")

    # Generate summary HTML
    if getattr(args, 'summary_output', None):
        summary_output = args.summary_output
    else:
        summary_output = f"{base_name}_summary.html"

    print(f"\n=== Generating summary HTML report ===")
    from .generate_summary_report import generate_html_summary
    summary_dir = os.path.dirname(summary_output)
    if summary_dir:
        os.makedirs(summary_dir, exist_ok=True)
    generate_html_summary(combined_payload, summary_output)
    print(f"✅ Summary HTML written to {summary_output}")

    # Generate XLSX
    if getattr(args, 'xlsx_output', None):
        xlsx_output = args.xlsx_output
    else:
        xlsx_output = f"{base_name}.xlsx"

    print(f"\n=== Generating XLSX report ===")
    from .generate_xlsx_from_json import generate_xlsx_from_json
    xlsx_dir = os.path.dirname(xlsx_output)
    if xlsx_dir:
        os.makedirs(xlsx_dir, exist_ok=True)
    generate_xlsx_from_json(args.output, xlsx_output)
    print(f"✅ XLSX report written to {xlsx_output}")

    print(f"\n🎉 All 4 report formats generated successfully!")
    print(f"  📄 JSON: {args.output}")
    print(f"  📊 Combined HTML (unlimited): {html_output}")
    print(f"  📈 Summary HTML: {summary_output}")
    print(f"  📋 XLSX: {xlsx_output}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Unified report JSON extractor with curve data.')
    subparsers = parser.add_subparsers(dest='report', required=True)

    for name, config in CONFIG_MAP.items():
        sub = subparsers.add_parser(name, help=f'Generate {name} report JSON')
        config.add_arguments(sub)
        sub.set_defaults(report=name)

    combined = subparsers.add_parser('combined', help='Generate combined JSON for sample, control, and discrepancy reports')
    add_common_arguments(combined)
    combined.add_argument('--sample-since-date', help='Sample wells on/after this extraction date (YYYY-MM-DD)')
    combined.add_argument('--sample-until-date', help='Sample wells on/before this extraction date (YYYY-MM-DD)')
    combined.add_argument('--sample-include-label-errors', action='store_true', help='Include label/setup errors in sample report')
    combined.add_argument('--exclude-from-sop', nargs='+', metavar='ERROR_CODE', help='Additional error codes to exclude from SOP sample report (supports wildcards like %%SIGMOID%%)')
    combined.add_argument('--exclude-from-control', nargs='+', metavar='ERROR_CODE', help='Additional error codes to exclude from Control report and affected samples (supports wildcards)')
    combined.add_argument('--suppress-unaffected-controls', action='store_true', help='Suppress control errors with no affected samples')
    combined.add_argument('--site-ids', nargs='+', metavar='SITE_ID', help='Filter by site IDs (only include wells from these sites)')
    combined.add_argument('--control-since-date', help='Control wells on/after this extraction date (YYYY-MM-DD)')
    combined.add_argument('--control-until-date', help='Control wells on/before this extraction date (YYYY-MM-DD)')
    combined.add_argument('--discrepancy-since-date', default='2024-01-01', help='Discrepancy runs on/after this date (default: 2024-01-01)')
    combined.add_argument('--discrepancy-until-date', help='Discrepancy runs on/before this date (YYYY-MM-DD)')
    combined.add_argument('--discrepancy-date-field', choices=['upload', 'extraction'], default='upload', help='Date field used for discrepancy filtering')
    combined.add_argument('--html-output', help='Optional path to also write a combined HTML report')
    combined.add_argument('--html-max-per-category', type=int, default=0, help='Maximum records per category when rendering combined HTML (0 for unlimited)')
    combined.add_argument('--xlsx-output', help='Optional path to also write a combined XLSX report')
    combined.add_argument('--summary-output', help='Optional path to also write a summary HTML report with pie charts')
    combined.set_defaults(report='combined')

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.report == 'combined':
        run_combined_report(args)
        return

    config = CONFIG_MAP[args.report]
    run_report(config, args)


if __name__ == '__main__':
    main()
