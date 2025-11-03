"""Shared helper functions for report extractors."""

from __future__ import annotations

import csv
import json
import sqlite3
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    """Create a SQLite connection with common pragma tweaks."""

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA journal_mode=OFF")
    except sqlite3.DatabaseError:
        pass
    return conn


def fetch_comments_batch(
    conn: sqlite3.Connection,
    well_ids: Sequence[str],
    system_only: bool = True,
) -> Dict[str, List[Dict[str, object]]]:
    """Fetch comments for a batch of well IDs."""

    if not well_ids:
        return {}

    placeholders = ','.join(['?' for _ in well_ids])
    clause = " AND c.is_system_generated = 1" if system_only else ""

    query = f"""
    SELECT
        c.commentable_id,
        c.text,
        c.is_system_generated,
        c.created_at
    FROM comments c
    WHERE c.commentable_id IN ({placeholders})
    {clause}
    ORDER BY c.commentable_id, c.created_at DESC
    """

    cursor = conn.cursor()
    cursor.execute(query, tuple(well_ids))

    grouped: Dict[str, List[Dict[str, object]]] = {}
    for row in cursor.fetchall():
        well_id = str(row['commentable_id'])
        grouped.setdefault(well_id, []).append(
            {
                'text': row['text'],
                'is_system': row['is_system_generated'],
                'created_at': row['created_at'],
            }
        )
    return grouped


def decode_readings(value: object) -> List[float]:
    """Ensure readings payload is returned as a list."""

    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def fetch_passive_normalization_data(
    conn: sqlite3.Connection,
    well_id: str,
    mix_name: str,
) -> tuple[bool, Optional[List[float]]]:
    """
    Check if mix uses passive dye and fetch passive readings for normalization.

    Returns:
        (should_normalize, passive_readings) where:
        - should_normalize: True if mix.use_passive_dye = 1
        - passive_readings: List of passive readings if available, None otherwise
    """
    # Check if mix uses passive dye
    cursor = conn.cursor()
    cursor.execute("""
        SELECT use_passive_dye FROM mixes WHERE mix_name = ?
    """, (mix_name,))

    row = cursor.fetchone()
    if not row or not row['use_passive_dye']:
        return False, None

    # Mix uses passive dye - fetch passive target readings
    cursor.execute("""
        SELECT o.readings
        FROM observations o
        JOIN targets t ON o.target_id = t.id
        WHERE o.well_id = ?
          AND t.is_passive = 1
        LIMIT 1
    """, (well_id,))

    passive_row = cursor.fetchone()
    if not passive_row:
        return True, None  # Should normalize but passive missing

    passive_readings = decode_readings(passive_row['readings'])
    return True, passive_readings


def normalize_readings_with_passive(
    readings: List[float],
    passive_readings: List[float]
) -> List[float]:
    """
    Normalize target readings by dividing by passive dye readings.
    Handles divide-by-zero by keeping original reading.
    """
    if len(readings) != len(passive_readings):
        # Length mismatch - return original
        return readings

    normalized = []
    for i, (reading, passive) in enumerate(zip(readings, passive_readings)):
        if passive == 0 or passive is None:
            # Avoid divide by zero - keep original
            normalized.append(reading)
        else:
            normalized.append(reading / passive)

    return normalized


def fetch_targets_for_well(
    conn: sqlite3.Connection,
    well_id: str,
) -> List[Dict[str, object]]:
    """Retrieve all non-passive targets for a well with classification metadata."""

    query = """
    SELECT
        t.target_name,
        o.readings,
        o.machine_ct,
        o.machine_cls,
        o.dxai_cls,
        o.final_cls,
        t.is_passive,
        CASE
            WHEN (UPPER(t.target_name) IN ('IC', 'IPC', 'QRICK')
                  OR UPPER(t.target_name) LIKE '%CONTROL%') THEN 1
            ELSE 0
        END AS is_ic
    FROM observations o
    JOIN targets t ON o.target_id = t.id
    WHERE o.well_id = ?
      AND t.is_passive = 0
    ORDER BY is_ic, t.target_name
    """

    cursor = conn.cursor()
    cursor.execute(query, (well_id,))

    targets: List[Dict[str, object]] = []
    for row in cursor.fetchall():
        targets.append(
            {
                'target_name': row['target_name'],
                'readings': decode_readings(row['readings']),
                'machine_ct': row['machine_ct'],
                'machine_cls': row['machine_cls'],
                'dxai_cls': row['dxai_cls'],
                'final_cls': row['final_cls'],
                'is_passive': row['is_passive'],
                'is_ic': row['is_ic'],
            }
        )

    return targets


_BACKUP_CONTROL_MAP: Optional[Dict[str, Dict[str, List[str]]]] = None


def load_backup_control_mapping(path: str = 'input_data/backup-controls.csv') -> Dict[str, Dict[str, List[str]]]:
    """Load backup control relationships from CSV (cached)."""

    global _BACKUP_CONTROL_MAP
    if _BACKUP_CONTROL_MAP is not None:
        return _BACKUP_CONTROL_MAP

    mapping: Dict[str, Dict[str, List[str]]] = {}
    try:
        with open(path, 'r', encoding='utf-8-sig') as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                role = (row.get('ROLE') or '').strip()
                role_type = (row.get('ROLE TYPE') or '').strip().upper()
                backups = (row.get('BACKUP MIXES') or '').split(' | ')
                if role_type not in {'PC', 'NC'} or not role:
                    continue
                for mix in backups:
                    mix = mix.strip()
                    if not mix:
                        continue
                    mapping.setdefault(mix, {'PC': [], 'NC': []})
                    bucket = mapping[mix][role_type]
                    if role not in bucket:
                        bucket.append(role)
    except FileNotFoundError:
        mapping = {}

    _BACKUP_CONTROL_MAP = mapping
    return mapping


def normalize_mix_name(name: str) -> str:
    """Normalise mix name to compare variants."""

    if not name:
        return ''
    result = name.upper()
    for token in ('Q2', 'PL', 'BL', 'SE', 'UR', 'QC'):
        result = result.replace(token, '')
    return result


def related_target_names(target_name: str) -> List[str]:
    """Return related target aliases for backup lookup."""

    target = (target_name or '').upper()
    if not target:
        return []

    names = {target}

    def extend(options: Iterable[str]):
        for opt in options:
            names.add(opt)

    if 'BK' in target:
        extend(['QBK', 'QBKQ', 'QBKQUR', 'QBKQSE', 'QBKQPL', 'QBKQBL', 'QBKQU'])
    elif 'CMV' in target:
        extend(['QCMV', 'QCMVQ', 'QCMVQ2', 'QCMVQ2BL', 'QCMVQ2PL', 'QCMVQ2SE', 'CMVQ'])
    elif 'EBV' in target:
        extend(['QEBV', 'QEBVQ', 'QEBVQPL', 'QEBVQBL', 'QEBVQSE', 'EBVQ'])
    elif 'VZV' in target:
        extend(['QVZV', 'QVZVQ', 'QVZVQBL', 'QVZVQC'])
    elif 'ADV' in target:
        extend(['QADV', 'QADVQ', 'QADVQSE', 'QADVQPL', 'QADVQBL', 'QADVQRE', 'QADVQU'])
    elif 'HHV6' in target:
        extend(['QHHV6', 'QHHV6Q'])
    elif 'HSV' in target:
        extend(['QHSV', 'QHSVQ'])
    elif 'PARV' in target:
        extend(['QPARV', 'QPARVOQ'])

    return list(names)


def classify_control_role(role_alias: Optional[str]) -> str:
    """Map control role alias to category."""

    role = (role_alias or '').upper()
    if any(token in role for token in ('NC', 'NEGATIVE', 'NTC')):
        return 'negative'
    if any(token in role for token in ('PC', 'POSITIVE', 'HPC', 'LPC', 'PTC')):
        return 'positive'
    return 'other'


def fetch_control_curves(
    conn: sqlite3.Connection,
    run_id: str,
    mix_name: str,
    target_name: str,
    *,
    max_controls: int = 3,
    allow_backup: bool = False,
) -> List[Dict[str, object]]:
    """Fetch control curves for a run/target, optionally using backup mix logic."""

    related_targets = [target_name]
    if allow_backup:
        related_targets = related_target_names(target_name)

    placeholders = ','.join(['?'] * len(related_targets))

    query = f"""
    SELECT
        w.id AS well_id,
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
            w.role_alias LIKE '%NEG%' OR
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
    params: Tuple[object, ...] = (run_id, *related_targets, mix_name, target_name, max_controls * 5)
    cursor.execute(query, params)

    backup_map = load_backup_control_mapping()
    normalized_mix = normalize_mix_name(mix_name)

    same_mix: List[Dict[str, object]] = []
    backups: List[Dict[str, object]] = []

    for row in cursor.fetchall():
        readings = decode_readings(row['readings'])
        if not readings:
            continue

        # Check if this control well's mix uses passive dye normalization
        control_well_id = row['well_id']
        control_mix_name = row['mix_name']
        should_normalize, passive_readings = fetch_passive_normalization_data(
            conn, control_well_id, control_mix_name
        )

        # Apply passive normalization to control readings if needed
        if should_normalize and passive_readings:
            readings = normalize_readings_with_passive(readings, passive_readings)

        control = {
            'well_id': row['well_id'],
            'role_alias': row['role_alias'],
            'label': row['sample_label'],
            'readings': readings,
            'machine_ct': row['machine_ct'],
            'mix_name': row['mix_name'],
            'target_name': row['target_name'],
            'category': classify_control_role(row['role_alias']),
        }

        if row['mix_name'] == mix_name:
            same_mix.append(control)
            continue

        if not allow_backup:
            continue

        control_norm = normalize_mix_name(row['mix_name'])
        if row['mix_name'].startswith(mix_name) or mix_name.startswith(row['mix_name']) or control_norm == normalized_mix:
            backups.append(control)
            continue

        mapping = backup_map.get(mix_name)
        if mapping:
            if control['category'] == 'positive' and control['role_alias'] in mapping['PC']:
                backups.append(control)
            elif control['category'] == 'negative' and control['role_alias'] in mapping['NC']:
                backups.append(control)

    def balance(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
        negatives = [c for c in items if c['category'] == 'negative']
        positives = [c for c in items if c['category'] == 'positive']
        others = [c for c in items if c['category'] not in {'negative', 'positive'}]

        chosen: List[Dict[str, object]] = []
        if negatives:
            chosen.extend(negatives[: min(2, len(negatives))])
        remaining = max_controls - len(chosen)
        if remaining > 0 and positives:
            chosen.extend(positives[:remaining])
        remaining = max_controls - len(chosen)
        if remaining > 0 and others:
            chosen.extend(others[:remaining])
        return chosen

    selected = balance(same_mix)
    if allow_backup and len(selected) < max_controls:
        for candidate in balance(backups):
            if candidate not in selected:
                selected.append(candidate)
            if len(selected) >= max_controls:
                break

    return selected
