#!/usr/bin/env python3
"""
Extract discrepancy (QST) report data directly from Quest DB to JSON, with curve data

Removes dependency on qst_discreps.db by:
- Selecting eligible targets from `targets` that have calibration_file_path
  AND are mapped to the WDCLS rule for Patient role via `rule_mappings`/`rules`/`roles`.
- Selecting wells with either an error_code or resolution_codes present
  AND at least one observation for an eligible target where dxai_cls != machine_cls
  AND dxai_cls IS NOT NULL.

Outputs the same JSON schema used by extract_discrepancy_data_with_curves.py:
{
  report_type: 'discrepancy',
  generated_at, database,
  summary: { total_displayed, acted_upon, samples_repeated, ignored },
  errors: [...records...],
  well_curves: { well_id: { sample_name, mix_name, targets: [...] } }
}
"""

import sqlite3
import json
import argparse
from datetime import datetime
from collections import defaultdict


def fetch_eligible_target_ids(conn):
    """Return a set of target IDs eligible for discrepancy reporting.

    Criteria:
    - targets.calibration_file_path IS NOT NULL
    - rules.programmatic_rule_name = 'WDCLS' (case-insensitive)
    - rule_mappings maps that rule to the target for Patient role
    - Exclude IPC targets by name
    """
    q = """
    SELECT DISTINCT t.id
    FROM targets t
    JOIN rule_mappings rm ON rm.target_id = t.id
    JOIN rules r ON r.id = rm.rule_id
    JOIN roles ro ON ro.id = rm.role_id
    WHERE t.calibration_file_path IS NOT NULL
      AND LOWER(r.programmatic_rule_name) = 'wdcls'
      AND ro.role_name = 'Patient'
      AND (UPPER(t.target_name) NOT LIKE '%IPC%')
    """
    cur = conn.cursor()
    rows = cur.execute(q).fetchall()
    return set(row[0] for row in rows)


def fetch_discrepancies(conn, eligible_target_ids, limit=None, since_date=None, class_only=False, exclude_skip_without_bla=False):
    """Fetch discrepancy records from Quest DB using observations where dxai_cls != machine_cls."""
    cur = conn.cursor()
    limit_clause = f"LIMIT {int(limit)}" if limit else ""

    # To avoid extremely long IN clauses, materialize eligible target IDs in a temp table
    cur.execute("DROP TABLE IF EXISTS _eligible_targets")
    cur.execute("CREATE TEMP TABLE _eligible_targets (id TEXT PRIMARY KEY)")
    cur.executemany("INSERT INTO _eligible_targets(id) VALUES (?)", [(tid,) for tid in eligible_target_ids])

    # Optional temp table for classification-only error codes
    class_only_join = ""
    if class_only:
        cur.execute("DROP TABLE IF EXISTS _class_errs")
        cur.execute("CREATE TEMP TABLE _class_errs (id TEXT PRIMARY KEY)")
        # Heuristic: error_message mentions classification discrep
        cur.execute("""
            INSERT INTO _class_errs(id)
            SELECT id FROM error_codes
            WHERE LOWER(error_message) LIKE '%classification%discrep%'
        """)
        class_only_join = " AND (w.error_code_id IN (SELECT id FROM _class_errs) OR w.resolution_codes IS NOT NULL)"

    date_clause = f" AND w.extraction_date >= '{since_date}'" if since_date else ""
    skip_clause = ""
    if exclude_skip_without_bla:
        skip_clause = " AND NOT (w.resolution_codes LIKE '%SKIP%' AND w.resolution_codes NOT LIKE '%BLA%')"

    q = f"""
    SELECT 
        w.id AS well_id,
        w.sample_label AS sample_name,
        w.well_number,
        w.lims_status,
        ec.error_code,
        ec.error_message,
        w.resolution_codes,
        w.extraction_date,
        o.machine_cls,
        o.dxai_cls,
        o.final_cls,
        o.manual_cls,
        o.machine_ct AS ct,
        o.dxai_ct,
        t.target_name,
        m.mix_name,
        r.run_name AS run_id
    FROM observations o
    JOIN wells w ON o.well_id = w.id
    JOIN targets t ON o.target_id = t.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    JOIN runs r ON w.run_id = r.id
    LEFT JOIN error_codes ec ON w.error_code_id = ec.id
    WHERE o.dxai_cls IS NOT NULL
      AND o.machine_cls != o.dxai_cls
      AND (w.resolution_codes IS NOT NULL OR w.error_code_id IS NOT NULL)
      AND t.id IN (SELECT id FROM _eligible_targets)
      AND (w.role_alias IS NULL OR w.role_alias = 'Patient')
      {date_clause}
      {class_only_join}
      {skip_clause}
    ORDER BY m.mix_name, t.target_name, w.sample_label
    {limit_clause}
    """

    rows = cur.execute(q).fetchall()
    records = []
    for row in rows:
        rec = {
            'id': row[0],  # alias for well_id
            'well_id': row[0],
            'sample_name': row[1],
            'well_number': row[2],
            'lims_status': row[3],
            'error_code': row[4],
            'error_message': row[5],
            'resolution_codes': row[6],
            'extraction_date': row[7],
            'machine_cls': row[8],
            'dxai_cls': row[9],
            'final_cls': row[10],
            'manual_cls': row[11],
            'ct': row[12],
            'dxai_ct': row[13],
            'target_name': row[14],
            'mix_name': row[15],
            'run_id': row[16],
        }

        # Categorization compatible with the previous extractor
        category, section = categorize_record(rec)
        if section == 0:
            # do not suppress; selection already excludes null dxai but keep for symmetry
            pass
        rec['category'] = category
        rec['clinical_category'] = (
            'acted_upon' if section == 1 else
            'samples_repeated' if section == 2 else
            'ignored'
        )
        rec['run_name'] = rec['run_id']  # compatibility field name
        rec['error_message'] = rec.get('error_message') or rec.get('error_code', '')
        records.append(rec)

    return records


def categorize_record(row):
    """Categorize record based on classification discrepancies (machine vs final) and LIMS status.
    Returns (category_string, section_number).
    Section 1: acted upon; 2: repeated; 3: ignored; 0: suppressed.
    """
    machine_cls = row.get('machine_cls')
    final_cls = row.get('final_cls')
    lims_status = row.get('lims_status')
    error_code = row.get('error_code')

    # Section 1: Discrepancies Acted Upon
    if machine_cls != final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
        if final_cls == 1:
            return ('discrepancy_positive', 1)
        else:
            return ('discrepancy_negative', 1)

    # Section 2: Samples Repeated
    if error_code:
        return ('has_error', 2)
    if lims_status and lims_status not in ('DETECTED', 'NOT DETECTED'):
        return ('lims_other', 2)

    # Section 3: Discrepancies Ignored
    if machine_cls == final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
        if lims_status == 'DETECTED':
            return ('agreement_detected', 3)
        else:
            return ('agreement_not_detected', 3)

    return ('unknown', 3)


def get_well_data_with_targets(conn, well_id):
    """Get all non-passive targets and readings for a well (Quest DB)."""
    cur = conn.cursor()
    q = """
    SELECT 
        t.target_name,
        o.readings,
        o.machine_ct,
        t.is_passive,
        CASE 
            WHEN UPPER(t.target_name) LIKE '%IPC%' OR UPPER(t.target_name) = 'IC' OR UPPER(t.target_name) = 'IPC' THEN 1
            ELSE 0
        END as is_ic
    FROM observations o
    JOIN targets t ON o.target_id = t.id
    WHERE o.well_id = ?
      AND t.is_passive = 0
    ORDER BY is_ic, t.target_name
    """
    rows = cur.execute(q, (well_id,)).fetchall()
    if not rows:
        return None
    targets = []
    for r in rows:
        readings_json = r[1]
        try:
            readings = json.loads(readings_json) if isinstance(readings_json, str) else readings_json
        except Exception:
            readings = []
        targets.append({
            'target_name': r[0],
            'readings': readings,
            'machine_ct': r[2],
            'is_passive': r[3],
            'is_ic': r[4]
        })
    return targets


def get_control_curves_limited(conn, mix_name, target_name, max_controls=3):
    """Fetch up to max_controls positive and negative control curves for a mix/target from Quest DB."""
    cur = conn.cursor()
    pos_q = """
    SELECT DISTINCT o.readings, o.machine_ct, 'PC' AS control_type
    FROM observations o
    JOIN targets t ON o.target_id = t.id
    JOIN wells w ON o.well_id = w.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE UPPER(m.mix_name) = UPPER(?)
      AND UPPER(t.target_name) = UPPER(?)
      AND w.role_alias IN ('PC', 'PTC', 'HPC')
      AND o.machine_ct IS NOT NULL AND o.machine_ct > 0
      AND o.readings IS NOT NULL
    ORDER BY o.machine_ct
    LIMIT ?
    """
    neg_q = """
    SELECT DISTINCT o.readings, o.machine_ct, 'NC' AS control_type
    FROM observations o
    JOIN targets t ON o.target_id = t.id
    JOIN wells w ON o.well_id = w.id
    JOIN run_mixes rm ON w.run_mix_id = rm.id
    JOIN mixes m ON rm.mix_id = m.id
    WHERE UPPER(m.mix_name) = UPPER(?)
      AND UPPER(t.target_name) = UPPER(?)
      AND (w.role_alias IN ('NC', 'NTC', 'NEG', 'NEGATIVE') OR w.role_alias LIKE '%NEG%')
      AND o.readings IS NOT NULL
    ORDER BY o.machine_ct
    LIMIT ?
    """
    pos = cur.execute(pos_q, (mix_name, target_name, max_controls)).fetchall()
    neg = cur.execute(neg_q, (mix_name, target_name, max_controls)).fetchall()
    out = []
    for row in pos + neg:
        try:
            readings = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        except Exception:
            readings = []
        out.append({
            'readings': readings,
            'machine_ct': row[1],
            'control_type': row[2]
        })
    return out


def build_well_curves(conn, records):
    """Build well_curves mapping with controls per target, de-duplicated per well."""
    well_curves = {}
    processed = set()
    cache_controls = {}
    for rec in records:
        wid = rec['well_id']
        if wid in processed:
            continue
        targets = get_well_data_with_targets(conn, wid)
        if targets:
            # Attach per-target control curves with simple cache on (mix, target)
            for t in targets:
                key = (rec['mix_name'], t['target_name'])
                if key not in cache_controls:
                    cache_controls[key] = get_control_curves_limited(conn, rec['mix_name'], t['target_name'], 3)
                t['control_curves'] = cache_controls[key]
            well_curves[wid] = {
                'sample_name': rec['sample_name'],
                'mix_name': rec['mix_name'],
                'targets': targets
            }
        processed.add(wid)
    return well_curves


def get_summary(records):
    counts = defaultdict(int)
    for r in records:
        counts[r['clinical_category']] += 1
    return {
        'total_displayed': len(records),
        'acted_upon': counts['acted_upon'],
        'samples_repeated': counts['samples_repeated'],
        'ignored': counts['ignored']
    }


def main():
    p = argparse.ArgumentParser(description='Extract discrepancy data directly from Quest DB with curves')
    p.add_argument('--db', default='../wssvc-flow/input_data/quest_prod_aug2025.db', help='Path to Quest DB')
    p.add_argument('--output', default='output_data/discrepancy_data_from_quest.json', help='Output JSON file')
    p.add_argument('--limit', type=int, help='Optional limit')
    p.add_argument('--since', help="Optional extraction_date lower bound, e.g., 2024-01-01")
    p.add_argument('--class-only', action='store_true', help='Limit to classification discrepancy error codes or any resolution present')
    p.add_argument('--exclude-skip-without-bla', action='store_true', help="Exclude resolutions containing SKIP unless they also contain BLA")
    args = p.parse_args()

    print(f"Connecting to database: {args.db}")
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        print("\nDetermining eligible targets (WDCLS mapped to Patient with calibration)…")
        eligible = fetch_eligible_target_ids(conn)
        print(f"  Eligible targets: {len(eligible)}")

        print("\nSelecting discrepancy observations (dxai_cls != machine_cls)…")
        records = fetch_discrepancies(
            conn,
            eligible,
            limit=args.limit,
            since_date=args.since,
            class_only=args.class_only,
            exclude_skip_without_bla=args.exclude_skip_without_bla,
        )
        print(f"  Found {len(records)} discrepancy records")

        print("\nFetching well curves and control overlays…")
        well_curves = build_well_curves(conn, records)
        print(f"  Built curves for {len(well_curves)} wells")

        summary = get_summary(records)
        data = {
            'report_type': 'discrepancy',
            'generated_at': datetime.now().isoformat(),
            'database': args.db,
            'summary': summary,
            'errors': records,
            'well_curves': well_curves
        }

        print(f"\n=== SUMMARY ===")
        print(f"Total displayed: {summary['total_displayed']}")
        print(f"  Acted Upon: {summary['acted_upon']}")
        print(f"  Samples Repeated: {summary['samples_repeated']}")
        print(f"  Ignored: {summary['ignored']}")

        with open(args.output, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"\nData saved to: {args.output}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
