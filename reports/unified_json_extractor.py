#!/usr/bin/env python3
"""
Unified JSON Extractor for Multi-Database Reporting System

Uses SQL-based categorization where categories are determined directly
in WHERE clauses, not through CSV lookup afterward.
"""

import argparse
import json
import sqlite3
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reports.utils.database_configs import get_config
from reports.utils.report_helpers import (
    connect_sqlite,
    decode_readings,
    fetch_comments_batch,
    fetch_targets_for_well,
    fetch_passive_normalization_data,
    normalize_readings_with_passive,
    classify_control_role,
)


@dataclass
class ExtractorConfig:
    """Configuration for the unified extractor"""
    db_path: str
    db_type: str

    # Date ranges
    sample_since_date: Optional[str] = None
    sample_until_date: Optional[str] = None
    control_since_date: Optional[str] = None
    control_until_date: Optional[str] = None
    discrepancy_since_date: Optional[str] = None
    discrepancy_until_date: Optional[str] = None
    discrepancy_date_field: str = 'upload'  # or 'extraction'

    # Options
    max_controls: int = 3
    limit: Optional[int] = None
    include_label_errors: bool = False
    suppress_unaffected_controls: bool = False  # Suppress control errors with no affected samples
    site_ids: Optional[List[str]] = None  # Filter by site IDs

    # Database-specific patterns (loaded from config)
    valid_lims_pattern: Optional[str] = None
    control_error_codes: Optional[List[str]] = None  # For discrepancy exclusion
    sample_exclusion_error_codes: Optional[List[str]] = None  # For SOP sample exclusion
    control_exclusion_error_codes: Optional[List[str]] = None  # For Control report exclusion

    # User-specified exclusions
    custom_sop_exclusions: Optional[List[str]] = None  # Additional error codes to exclude from SOP
    custom_control_exclusions: Optional[List[str]] = None  # Additional error codes to exclude from Control


class UnifiedJSONExtractor:
    """Main extractor class using SQL-based categorization"""

    def __init__(self, config: ExtractorConfig):
        self.config = config
        self._load_db_config()
        self.conn = connect_sqlite(config.db_path)

    def _load_db_config(self):
        """Load database-specific configuration"""
        db_config = get_config(self.config.db_type)

        # Set valid LIMS patterns based on database type
        if self.config.db_type == 'qst':
            self.config.valid_lims_pattern = "(w.lims_status LIKE '%detected%' OR w.lims_status LIKE '%1500%')"
            # Control/setup errors for discrepancy exclusion
            self.config.control_error_codes = [
                'MIX_MISSING',
                'EXTRACTION_INSTRUMENT_MISSING',
                'INHERITED_EXTRACTION_FAILURE',
                'EXTRACTION_CONTROLS_MISSING'
            ]
            # All errors to exclude from SOP sample report (control + classification discrepancies)
            self.config.sample_exclusion_error_codes = [
                'MIX_MISSING',
                'EXTRACTION_INSTRUMENT_MISSING',
                'INHERITED_EXTRACTION_FAILURE',
                'EXTRACTION_CONTROLS_MISSING',
                'UNKNOWN_MIX',  # Samples never analyzed
                # Only classification discrepancies, NOT CT discrepancies
                'CLSDISC_WELL',
                'CONTROL_CLSDISC_WELL',
                'CONTROL_CLSDISC_TARGET',
                'RQ_CLS'
            ]
            # Errors to exclude from Control report
            self.config.control_exclusion_error_codes = [
                'UNKNOWN_MIX'  # Controls never analyzed
            ]
        elif self.config.db_type == 'notts':
            self.config.valid_lims_pattern = "(w.lims_status LIKE '%detected%' OR w.lims_status LIKE '%1500%' OR w.lims_status LIKE '%<500IU%')"
            # Control/setup errors for discrepancy exclusion
            self.config.control_error_codes = [
                'INHERITED_CONTROL_FAILURE',
                'MISSING_CONTROL',
                'CONTROL_FAILURE',
                'INHERITED_EXTRACTION_FAILURE',
                'MIX_MISSING',
                'UNKNOWN_MIX',
                'ACCESSION_MISSING',
                'INVALID_ACCESSION',
                'UNKNOWN_ROLE',
                'WG_ERROR',
                'BLA'
            ]
            # All errors to exclude from SOP sample report (control + classification discrepancies)
            self.config.sample_exclusion_error_codes = [
                'INHERITED_CONTROL_FAILURE',
                'MISSING_CONTROL',
                'CONTROL_FAILURE',
                'INHERITED_EXTRACTION_FAILURE',
                'MIX_MISSING',
                'UNKNOWN_MIX',
                'ACCESSION_MISSING',
                'INVALID_ACCESSION',
                'UNKNOWN_ROLE',
                'WG_ERROR',
                'BLA',
                # Only classification discrepancies, NOT CT discrepancies
                'CLSDISC_WELL',
                'CONTROL_CLSDISC_WELL',
                'CONTROL_CLSDISC_TARGET',
                'RQ_CLS'
            ]
            # Errors to exclude from Control report
            self.config.control_exclusion_error_codes = [
                'UNKNOWN_MIX'  # Controls never analyzed
            ]
        elif self.config.db_type == 'vira':
            self.config.valid_lims_pattern = "(w.lims_status LIKE '%DETECTED%' OR w.lims_status LIKE '%LOQ%' OR w.lims_status LIKE '%HIQ%')"
            # Control/setup errors for discrepancy exclusion
            self.config.control_error_codes = [
                'INHERITED_CONTROL_FAILURE',
                'MISSING_CONTROL',
                'CONTROL_FAILURE',
                'INHERITED_EXTRACTION_FAILURE',
                'MIX_MISSING',
                'UNKNOWN_MIX',
                'ACCESSION_MISSING',
                'INVALID_ACCESSION',
                'UNKNOWN_ROLE',
                'WG_ERROR',
                'BLA'
            ]
            # All errors to exclude from SOP sample report (control + classification discrepancies)
            self.config.sample_exclusion_error_codes = [
                'INHERITED_CONTROL_FAILURE',
                'MISSING_CONTROL',
                'CONTROL_FAILURE',
                'INHERITED_EXTRACTION_FAILURE',
                'MIX_MISSING',
                'UNKNOWN_MIX',
                'ACCESSION_MISSING',
                'INVALID_ACCESSION',
                'UNKNOWN_ROLE',
                'WG_ERROR',
                'BLA',
                # Only classification discrepancies, NOT CT discrepancies
                'CLSDISC_WELL',
                'CONTROL_CLSDISC_WELL',
                'CONTROL_CLSDISC_TARGET',
                'RQ_CLS'
            ]
            # Errors to exclude from Control report
            self.config.control_exclusion_error_codes = [
                'UNKNOWN_MIX'  # Controls never analyzed
            ]

    def extract_combined_report(self) -> Dict:
        """Extract all report types and combine into unified JSON"""

        print("\n=== Generating sample report payload ===")
        sample_payload = self._extract_sample_report()

        print("\n=== Generating control report payload ===")
        control_payload = self._extract_control_report()

        print("\n=== Generating discrepancy report payload ===")
        discrepancy_payload = self._extract_discrepancy_report()

        print("\n=== Fetching valid results summary ===")
        valid_results = self._fetch_valid_results()

        print("\n=== Calculating error statistics ===")
        error_statistics = self._calculate_error_statistics(
            sample_payload, control_payload, discrepancy_payload
        )

        # Combine into final structure
        combined = {
            'generated_at': datetime.now().isoformat(),
            'database': self.config.db_path,
            'valid_results': valid_results,
            'error_statistics': error_statistics,
            'reports': {
                'sample': sample_payload,
                'control': control_payload,
                'discrepancy': discrepancy_payload
            }
        }

        return combined

    # =========================================================================
    # SAMPLE REPORT EXTRACTION
    # =========================================================================

    def _extract_sample_report(self) -> Dict:
        """Extract sample (SOP) report data"""

        # Get error codes to exclude from sample report (control + classification discrepancies)
        sample_exclusion_error_ids = self._get_sample_exclusion_error_ids()

        # Fetch categorized errors
        print("  Fetching SOP unresolved errors...")
        unresolved = self._fetch_sop_unresolved(sample_exclusion_error_ids)
        print(f"    Found {len(unresolved)} unresolved errors")

        print("  Fetching SOP repeated errors...")
        repeated = self._fetch_sop_repeated(sample_exclusion_error_ids)
        print(f"    Found {len(repeated)} test repeated errors")

        print("  Fetching SOP ignored errors...")
        ignored = self._fetch_sop_ignored(sample_exclusion_error_ids)
        print(f"    Found {len(ignored)} error ignored")

        # Combine all errors
        all_errors = unresolved + repeated + ignored

        # Enrich with curves and comments
        well_curves = self._enrich_with_curves(all_errors, 'sample')
        print(f"  Extracted {len(well_curves)} well curves")

        # Calculate summary
        summary = {
            'total_errors': len(all_errors),
            'unresolved': len(unresolved),
            'error_ignored': len(ignored),
            'test_repeated': len(repeated)
        }

        return {
            'report_type': 'sample',
            'generated_at': datetime.now().isoformat(),
            'database': self.config.db_path,
            'since_date': self.config.sample_since_date,
            'until_date': self.config.sample_until_date,
            'date_field': 'extraction',
            'include_label_errors': self.config.include_label_errors,
            'summary': summary,
            'errors': all_errors,
            'well_curves': well_curves
        }

    def _fetch_sop_unresolved(self, control_error_ids: str) -> List[Dict]:
        """Fetch unresolved SOP errors"""
        query = f"""
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
            w.resolution_codes,
            w.created_at,
            'unresolved' as clinical_category
        FROM wells w
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias = 'Patient'
          AND (w.resolution_codes NOT LIKE '%bla%' OR w.resolution_codes IS NULL)
          AND w.lims_status IS NULL
          AND w.error_code_id IS NOT NULL
          AND w.error_code_id NOT IN ({control_error_ids})
          {self._get_date_filter('w.created_at', 'sample')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_error_record(row) for row in cursor.fetchall()]

    def _fetch_sop_repeated(self, control_error_ids: str) -> List[Dict]:
        """Fetch repeated SOP errors"""
        query = f"""
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
            w.resolution_codes,
            w.created_at,
            'test_repeated' as clinical_category
        FROM wells w
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias = 'Patient'
          AND w.resolution_codes NOT LIKE '%bla%'
          AND (NOT {self.config.valid_lims_pattern}
               OR w.lims_status IS NULL)
          AND (w.error_code_id NOT IN ({control_error_ids})
               OR w.error_code_id IS NULL)
          {self._get_date_filter('w.created_at', 'sample')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_error_record(row) for row in cursor.fetchall()]

    def _fetch_sop_ignored(self, control_error_ids: str) -> List[Dict]:
        """Fetch ignored SOP errors"""
        query = f"""
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
            w.resolution_codes,
            w.created_at,
            'error_ignored' as clinical_category
        FROM wells w
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias = 'Patient'
          AND w.resolution_codes NOT LIKE '%bla%'
          AND {self.config.valid_lims_pattern}
          {self._get_date_filter('w.created_at', 'sample')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_error_record(row) for row in cursor.fetchall()]

    # =========================================================================
    # CONTROL REPORT EXTRACTION
    # =========================================================================

    def _extract_control_report(self) -> Dict:
        """Extract control report data"""

        # Get control exclusion error IDs
        control_exclusion_ids = self._get_control_exclusion_error_ids()

        # Fetch categorized control errors
        print("  Fetching control unresolved errors...")
        unresolved = self._fetch_control_unresolved(control_exclusion_ids)
        print(f"    Found {len(unresolved)} unresolved control errors")

        print("  Fetching control repeated errors...")
        repeated = self._fetch_control_repeated(control_exclusion_ids)
        print(f"    Found {len(repeated)} control repeated errors")

        print("  Fetching control ignored errors...")
        ignored = self._fetch_control_ignored(control_exclusion_ids)
        print(f"    Found {len(ignored)} control ignored errors")

        # Combine all errors
        all_errors = unresolved + repeated + ignored

        # Fetch affected samples (excluding custom control exclusions)
        print("  Fetching affected samples...")
        affected_samples, affected_counts = self._fetch_affected_samples(control_exclusion_ids)
        print(f"    Found {affected_counts['error']} error, {affected_counts['repeat']} repeat affected samples")

        # Filter out controls with no affected samples if requested
        if self.config.suppress_unaffected_controls:
            # Build set of control well IDs that have affected samples
            affected_control_ids = set()
            for group_data in affected_samples.values():
                affected_control_ids.update(group_data['controls'].keys())

            # Filter all_errors to only include controls with affected samples
            # BUT keep all "error_ignored" controls (they were resolved before affecting samples)
            original_count = len(all_errors)
            all_errors = [e for e in all_errors
                         if e['clinical_category'] == 'error_ignored'  # Keep all ignored
                         or str(e['well_id']) in affected_control_ids]  # Or has affected samples
            suppressed_count = original_count - len(all_errors)
            print(f"    Suppressed {suppressed_count} unresolved/repeated control errors with no affected samples")

        # Enrich with curves
        well_curves = self._enrich_with_curves(all_errors, 'control')
        print(f"  Fetched curves for {len(well_curves)} control wells")

        # Calculate summary
        summary = {
            'total_errors': len(all_errors),
            'unresolved': len(unresolved),
            'error_ignored': len(ignored),
            'test_repeated': len(repeated),
            'affected_error_count': affected_counts['error'],
            'affected_repeat_count': affected_counts['repeat']
        }

        return {
            'report_type': 'control',
            'generated_at': datetime.now().isoformat(),
            'database': self.config.db_path,
            'since_date': self.config.control_since_date,
            'until_date': self.config.control_until_date,
            'date_field': 'extraction',
            'summary': summary,
            'errors': all_errors,
            'well_curves': well_curves,
            'affected_samples': affected_samples,
            'affected_counts': affected_counts
        }

    def _fetch_control_unresolved(self, exclusion_ids: str = "''") -> List[Dict]:
        """Fetch unresolved control errors - excludes type 3 and type 2 with valid lims"""
        exclusion_clause = f"AND (w.error_code_id NOT IN ({exclusion_ids}) OR w.error_code_id IS NULL)" if exclusion_ids != "''" else ""

        query = f"""
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
            w.resolution_codes,
            w.created_at,
            'unresolved' as clinical_category
        FROM wells w
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias != 'Patient'
          AND w.resolution_codes IS NULL
          AND (w.lims_status IS NOT NULL OR w.error_code_id IS NOT NULL)
          AND (ec.error_type IS NULL OR ec.error_type = 1
               OR (ec.error_type = 2
                   AND ec.lims_status IS NOT NULL
                   AND UPPER(ec.lims_status) NOT IN ('DETECTED', 'NOT DETECTED', 'INHN', 'INHP', 'NOTISSUE')))
          {exclusion_clause}
          {self._get_date_filter('w.created_at', 'control')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_error_record(row) for row in cursor.fetchall()]

    def _fetch_control_repeated(self, exclusion_ids: str = "''") -> List[Dict]:
        """Fetch repeated control errors - excludes type 3 and type 2 with valid/null lims"""
        exclusion_clause = f"AND (w.error_code_id NOT IN ({exclusion_ids}) OR w.error_code_id IS NULL)" if exclusion_ids != "''" else ""

        query = f"""
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
            w.resolution_codes,
            w.created_at,
            'test_repeated' as clinical_category
        FROM wells w
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias != 'Patient'
          AND w.resolution_codes IS NOT NULL
          AND (w.lims_status IS NOT NULL OR w.error_code_id IS NOT NULL)
          AND (ec.error_type IS NULL OR ec.error_type = 1
               OR (ec.error_type = 2
                   AND ec.lims_status IS NOT NULL
                   AND UPPER(ec.lims_status) NOT IN ('DETECTED', 'NOT DETECTED', 'INHN', 'INHP', 'NOTISSUE')))
          {exclusion_clause}
          {self._get_date_filter('w.created_at', 'control')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_error_record(row) for row in cursor.fetchall()]

    def _fetch_control_ignored(self, exclusion_ids: str = "''") -> List[Dict]:
        """Fetch ignored control errors"""
        exclusion_clause = f"AND w.error_code_id NOT IN ({exclusion_ids})" if exclusion_ids != "''" else ""

        query = f"""
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
            w.resolution_codes,
            w.created_at,
            'error_ignored' as clinical_category
        FROM wells w
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE w.role_alias != 'Patient'
          AND w.resolution_codes IS NOT NULL
          AND w.lims_status IS NULL
          AND w.error_code_id IS NULL
          {self._get_date_filter('w.created_at', 'control')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_error_record(row) for row in cursor.fetchall()]

    def _fetch_affected_samples(self, control_exclusion_ids: str = "''") -> Tuple[Dict, Dict[str, int]]:
        """Fetch samples affected by control failures"""

        # Only get IDs for truly inherited errors (not setup/config errors)
        inherited_error_codes = ['INHERITED_CONTROL_FAILURE', 'INHERITED_EXTRACTION_FAILURE']
        placeholders = ','.join(['?' for _ in inherited_error_codes])
        query = f"SELECT id FROM error_codes WHERE error_code IN ({placeholders})"

        cursor = self.conn.cursor()
        cursor.execute(query, inherited_error_codes)
        inherited_ids = [f"'{row['id']}'" for row in cursor.fetchall()]
        inherited_error_ids = ','.join(inherited_ids) if inherited_ids else "''"

        # Build control exclusion clause
        control_exclusion_clause = f"AND cw.error_code_id NOT IN ({control_exclusion_ids})" if control_exclusion_ids != "''" else ""

        # Fetch error-affected samples (inherited control failures)
        error_query = f"""
        SELECT
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
        WHERE pw.role_alias = 'Patient'
          AND pw.error_code_id IN ({inherited_error_ids})
          AND cw.role_alias != 'Patient'
          AND (cw.error_code_id IS NOT NULL OR cw.resolution_codes IS NOT NULL)
          AND (
            pm.mix_name = cm.mix_name
            OR (INSTR(UPPER(cm.mix_name), 'CMV') > 0 AND INSTR(UPPER(pm.mix_name), 'CMV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'BK') > 0 AND INSTR(UPPER(pm.mix_name), 'BK') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'EBV') > 0 AND INSTR(UPPER(pm.mix_name), 'EBV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ADV') > 0 AND INSTR(UPPER(pm.mix_name), 'ADV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'VZV') > 0 AND INSTR(UPPER(pm.mix_name), 'VZV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HSV') > 0 AND INSTR(UPPER(pm.mix_name), 'HSV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HHV6') > 0 AND INSTR(UPPER(pm.mix_name), 'HHV6') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'PARV') > 0 AND INSTR(UPPER(pm.mix_name), 'PARV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ENT') > 0 AND INSTR(UPPER(pm.mix_name), 'ENT') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HEV') > 0 AND INSTR(UPPER(pm.mix_name), 'HEV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HDV') > 0 AND INSTR(UPPER(pm.mix_name), 'HDV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ZIKV') > 0 AND INSTR(UPPER(pm.mix_name), 'ZIKV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ZIK') > 0 AND INSTR(UPPER(pm.mix_name), 'ZIK') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'MUCO') > 0 AND INSTR(UPPER(pm.mix_name), 'MUCO') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'NOR') > 0 AND INSTR(UPPER(pm.mix_name), 'NOR') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'COVID') > 0 AND INSTR(UPPER(pm.mix_name), 'COVID') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'RP') > 0 AND INSTR(UPPER(pm.mix_name), 'RP') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'MPX') > 0 AND INSTR(UPPER(pm.mix_name), 'MPX') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'PJ') > 0 AND INSTR(UPPER(pm.mix_name), 'PJ') > 0)
          )
          {control_exclusion_clause}
          {self._get_date_filter('pw.created_at', 'control')}
          {self._get_site_filter('pw')}
        """

        # Fetch repeat-affected samples
        repeat_query = f"""
        SELECT
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
        WHERE pw.role_alias = 'Patient'
          AND pw.lims_status IN ('REAMP','REXCT','RPT','RXT','TNP')
          AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
          AND cw.role_alias != 'Patient'
          AND (cw.resolution_codes LIKE '%RP%'
               OR cw.resolution_codes LIKE '%RX%'
               OR cw.resolution_codes LIKE '%TN%')
          AND (
            pm.mix_name = cm.mix_name
            OR (INSTR(UPPER(cm.mix_name), 'CMV') > 0 AND INSTR(UPPER(pm.mix_name), 'CMV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'BK') > 0 AND INSTR(UPPER(pm.mix_name), 'BK') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'EBV') > 0 AND INSTR(UPPER(pm.mix_name), 'EBV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ADV') > 0 AND INSTR(UPPER(pm.mix_name), 'ADV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'VZV') > 0 AND INSTR(UPPER(pm.mix_name), 'VZV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HSV') > 0 AND INSTR(UPPER(pm.mix_name), 'HSV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HHV6') > 0 AND INSTR(UPPER(pm.mix_name), 'HHV6') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'PARV') > 0 AND INSTR(UPPER(pm.mix_name), 'PARV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ENT') > 0 AND INSTR(UPPER(pm.mix_name), 'ENT') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HEV') > 0 AND INSTR(UPPER(pm.mix_name), 'HEV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'HDV') > 0 AND INSTR(UPPER(pm.mix_name), 'HDV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ZIKV') > 0 AND INSTR(UPPER(pm.mix_name), 'ZIKV') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'ZIK') > 0 AND INSTR(UPPER(pm.mix_name), 'ZIK') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'MUCO') > 0 AND INSTR(UPPER(pm.mix_name), 'MUCO') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'NOR') > 0 AND INSTR(UPPER(pm.mix_name), 'NOR') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'COVID') > 0 AND INSTR(UPPER(pm.mix_name), 'COVID') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'RP') > 0 AND INSTR(UPPER(pm.mix_name), 'RP') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'MPX') > 0 AND INSTR(UPPER(pm.mix_name), 'MPX') > 0)
            OR (INSTR(UPPER(cm.mix_name), 'PJ') > 0 AND INSTR(UPPER(pm.mix_name), 'PJ') > 0)
          )
          {self._get_date_filter('pw.created_at', 'control')}
          {self._get_site_filter('pw')}
        """

        cursor = self.conn.cursor()

        # Process error-affected samples
        cursor.execute(error_query)
        error_rows = cursor.fetchall()

        # Process repeat-affected samples
        cursor.execute(repeat_query)
        repeat_rows = cursor.fetchall()

        # Group by run and control mix
        grouped = defaultdict(lambda: {
            'run_name': '',
            'control_mix': '',
            'controls': {},
            'affected_samples_error': {},
            'affected_samples_repeat': {}
        })

        for row in error_rows:
            group_key = f"{row['run_name']}_{row['control_mix']}"
            grouped[group_key]['run_name'] = row['run_name']
            grouped[group_key]['control_mix'] = row['control_mix']

            # Add control
            control_id = str(row['control_well_id'])
            if control_id not in grouped[group_key]['controls']:
                grouped[group_key]['controls'][control_id] = {
                    'control_name': row['control_name'],
                    'control_well': row['control_well'],
                    'resolution': row['control_resolution']
                }

            # Add affected sample
            well_id = str(row['well_id'])
            grouped[group_key]['affected_samples_error'][well_id] = {
                'well_id': row['well_id'],
                'sample_name': row['sample_name'],
                'well_number': row['well_number'],
                'error_code': row['error_code'],
                'error_message': row['error_message'],
                'mix_name': row['mix_name'],
                'run_name': row['run_name'],
                'lims_status': row['lims_status'],
                'resolution_codes': row['resolution_codes']
            }

        for row in repeat_rows:
            group_key = f"{row['run_name']}_{row['control_mix']}"
            grouped[group_key]['run_name'] = row['run_name']
            grouped[group_key]['control_mix'] = row['control_mix']

            # Add control
            control_id = str(row['control_well_id'])
            if control_id not in grouped[group_key]['controls']:
                grouped[group_key]['controls'][control_id] = {
                    'control_name': row['control_name'],
                    'control_well': row['control_well'],
                    'resolution': row['control_resolution']
                }

            # Add affected sample
            well_id = str(row['well_id'])
            grouped[group_key]['affected_samples_repeat'][well_id] = {
                'well_id': row['well_id'],
                'sample_name': row['sample_name'],
                'well_number': row['well_number'],
                'error_code': row['error_code'],
                'error_message': row['error_message'],
                'mix_name': row['mix_name'],
                'run_name': row['run_name'],
                'lims_status': row['lims_status'],
                'resolution_codes': row['resolution_codes']
            }

        # Count unique affected samples
        unique_error = len(set(row['well_id'] for row in error_rows))
        unique_repeat = len(set(row['well_id'] for row in repeat_rows))

        counts = {
            'error': unique_error,
            'repeat': unique_repeat
        }

        return dict(grouped), counts

    # =========================================================================
    # DISCREPANCY REPORT EXTRACTION
    # =========================================================================

    def _extract_discrepancy_report(self) -> Dict:
        """Extract discrepancy report data"""

        # Determine date column based on field selection
        date_col = 'r.created_at' if self.config.discrepancy_date_field == 'upload' else 'w.created_at'

        # Fetch categorized discrepancies
        print("  Fetching acted upon discrepancies...")
        acted_upon = self._fetch_discrepancy_acted_upon(date_col)
        print(f"    Found {len(acted_upon)} acted upon")

        print("  Fetching ignored discrepancies...")
        ignored = self._fetch_discrepancy_ignored(date_col)
        print(f"    Found {len(ignored)} ignored")

        print("  Fetching repeated discrepancies...")
        repeated = self._fetch_discrepancy_repeated(date_col)
        print(f"    Found {len(repeated)} samples repeated")

        # Deduplicate wells across categories using priority order
        acted_upon, repeated, ignored, dedupe_stats = self._deduplicate_discrepancies(
            acted_upon, repeated, ignored
        )
        total_deduped = sum(dedupe_stats.values())
        if total_deduped:
            print(
                f"    Deduplicated {total_deduped} discrepancies across categories "
                f"(acted: {dedupe_stats['acted_upon']}, repeated: {dedupe_stats['samples_repeated']}, "
                f"ignored: {dedupe_stats['ignored']})"
            )

        # Combine all errors
        all_errors = acted_upon + ignored + repeated

        # Enrich with curves
        well_curves = self._enrich_with_curves(all_errors, 'discrepancy')

        # Calculate unique samples
        unique_samples = len(set(e['sample_name'] for e in all_errors))

        # Calculate summary
        summary = {
            'total_wells': len(all_errors),
            'unique_samples': unique_samples,
            'acted_upon': len(acted_upon),
            'samples_repeated': len(repeated),
            'ignored': len(ignored)
        }

        return {
            'report_type': 'discrepancy',
            'generated_at': datetime.now().isoformat(),
            'database': self.config.db_path,
            'since_date': self.config.discrepancy_since_date,
            'until_date': self.config.discrepancy_until_date,
            'date_field': self.config.discrepancy_date_field,
            'summary': summary,
            'errors': all_errors,
            'well_curves': well_curves
        }

    def _deduplicate_discrepancies(
        self,
        acted_upon: List[Dict],
        repeated: List[Dict],
        ignored: List[Dict]
    ) -> Tuple[List[Dict], List[Dict], List[Dict], Dict[str, int]]:
        """Ensure each well appears in at most one discrepancy category.

        Priority order (highest first): acted_upon -> samples_repeated -> ignored.
        """

        priority = [
            ('acted_upon', acted_upon),
            ('samples_repeated', repeated),
            ('ignored', ignored)
        ]

        seen_wells = set()
        deduped = {
            'acted_upon': [],
            'samples_repeated': [],
            'ignored': []
        }
        dropped_counts = {k: 0 for k, _ in priority}

        for category, records in priority:
            for record in records:
                well_id = str(record['well_id'])
                if well_id in seen_wells:
                    dropped_counts[category] += 1
                    continue

                deduped[category].append(record)
                seen_wells.add(well_id)

        return (
            deduped['acted_upon'],
            deduped['samples_repeated'],
            deduped['ignored'],
            dropped_counts
        )

    def _fetch_discrepancy_acted_upon(self, date_col: str) -> List[Dict]:
        """Fetch acted upon discrepancies (result changed)"""

        # Use subquery to pick one observation per well (avoids duplication)
        query = f"""
        SELECT
            w.id as well_id,
            w.sample_name,
            w.well_number,
            ec.error_code,
            ec.error_message,
            m.mix_name,
            r.run_name,
            r.id as run_id,
            w.lims_status,
            w.resolution_codes,
            w.created_at,
            o.machine_cls,
            o.dxai_cls,
            o.final_cls,
            o.machine_ct,
            t.target_name,
            'acted_upon' as clinical_category,
            'result_changed' as category_detail
        FROM wells w
        JOIN (
            SELECT o.well_id, MIN(o.id) as obs_id
            FROM observations o
            JOIN targets t ON o.target_id = t.id
            WHERE o.machine_cls != o.dxai_cls AND o.final_cls = o.dxai_cls
              AND (t.type IS NULL OR t.type != 1)  -- Exclude IC targets
            GROUP BY o.well_id
        ) obs_filter ON obs_filter.well_id = w.id
        JOIN observations o ON o.id = obs_filter.obs_id
        JOIN targets t ON o.target_id = t.id
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE {self.config.valid_lims_pattern}
          AND w.resolution_codes LIKE '%bla%'
          AND w.role_alias = 'Patient'
          {self._get_date_filter(date_col, 'discrepancy')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_discrepancy_record(row) for row in cursor.fetchall()]

    def _fetch_discrepancy_ignored(self, date_col: str) -> List[Dict]:
        """Fetch ignored discrepancies"""

        # Use subquery to pick one observation per well (avoids duplication)
        query = f"""
        SELECT
            w.id as well_id,
            w.sample_name,
            w.well_number,
            ec.error_code,
            ec.error_message,
            m.mix_name,
            r.run_name,
            r.id as run_id,
            w.lims_status,
            w.resolution_codes,
            w.created_at,
            o.machine_cls,
            o.dxai_cls,
            o.final_cls,
            o.machine_ct,
            t.target_name,
            'ignored' as clinical_category,
            'discrepancy_acknowledged' as category_detail
        FROM wells w
        JOIN (
            SELECT o.well_id, MIN(o.id) as obs_id
            FROM observations o
            JOIN targets t ON o.target_id = t.id
            WHERE o.machine_cls != o.dxai_cls AND o.final_cls = o.machine_cls
              AND (t.type IS NULL OR t.type != 1)  -- Exclude IC targets
            GROUP BY o.well_id
        ) obs_filter ON obs_filter.well_id = w.id
        JOIN observations o ON o.id = obs_filter.obs_id
        JOIN targets t ON o.target_id = t.id
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE {self.config.valid_lims_pattern}
          AND w.resolution_codes LIKE '%bla%'
          AND w.role_alias = 'Patient'
          {self._get_date_filter(date_col, 'discrepancy')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor = self.conn.cursor()
        cursor.execute(query)
        return [self._format_discrepancy_record(row) for row in cursor.fetchall()]

    def _fetch_discrepancy_repeated(self, date_col: str) -> List[Dict]:
        """Fetch repeated discrepancies"""

        control_error_ids = self._get_control_error_ids()

        # Get CLSDISC_WELL error codes for this database
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM error_codes WHERE error_code = 'CLSDISC_WELL'")
        clsdisc_ids = [f"'{row['id']}'" for row in cursor.fetchall()]
        clsdisc_error_ids = ','.join(clsdisc_ids) if clsdisc_ids else "''"

        # Use subquery to pick one observation per well (avoids duplication)
        query = f"""
        SELECT
            w.id as well_id,
            w.sample_name,
            w.well_number,
            ec.error_code,
            ec.error_message,
            m.mix_name,
            r.run_name,
            r.id as run_id,
            w.lims_status,
            w.resolution_codes,
            w.created_at,
            o.machine_cls,
            o.dxai_cls,
            o.final_cls,
            o.machine_ct,
            t.target_name,
            'samples_repeated' as clinical_category,
            'unresolved_discrepancy' as category_detail
        FROM wells w
        JOIN (
            SELECT o.well_id, MIN(o.id) as obs_id
            FROM observations o
            JOIN targets t ON o.target_id = t.id
            WHERE o.machine_cls != o.dxai_cls
              AND (t.type IS NULL OR t.type != 1)  -- Exclude IC targets
            GROUP BY o.well_id
        ) obs_filter ON obs_filter.well_id = w.id
        JOIN observations o ON o.id = obs_filter.obs_id
        JOIN targets t ON o.target_id = t.id
        LEFT JOIN runs r ON w.run_id = r.id
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        WHERE (
            (
                -- Has BLA resolution but still invalid LIMS (discrepancy is the main issue)
                ((NOT {self.config.valid_lims_pattern}) OR w.lims_status IS NULL)
                AND w.resolution_codes LIKE '%bla%'
            )
            OR
            -- Has classification discrepancy error code
            w.error_code_id IN ({clsdisc_error_ids})
          )
          AND w.role_alias = 'Patient'
          AND (w.error_code_id NOT IN ({control_error_ids}) OR w.error_code_id IS NULL)
          {self._get_date_filter(date_col, 'discrepancy')}
          {self._get_site_filter('w')}
        ORDER BY m.mix_name, w.sample_name
        {self._get_limit()}
        """

        cursor.execute(query)
        return [self._format_discrepancy_record(row) for row in cursor.fetchall()]

    # =========================================================================
    # VALID RESULTS & STATISTICS
    # =========================================================================

    def _fetch_valid_results(self) -> Dict[str, Dict]:
        """Fetch valid results summary by mix"""

        # Valid patients
        patient_query = f"""
        SELECT
            m.mix_name,
            w.lims_status,
            COUNT(*) as cnt
        FROM wells w
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        WHERE w.role_alias = 'Patient'
          AND w.resolution_codes IS NULL
          AND {self.config.valid_lims_pattern}
          {self._get_date_filter('w.created_at', 'sample')}
          {self._get_site_filter('w')}
        GROUP BY m.mix_name, w.lims_status
        """

        # Valid controls
        control_query = f"""
        SELECT
            m.mix_name,
            COUNT(*) as total,
            SUM(CASE WHEN w.error_code_id IS NULL THEN 1 ELSE 0 END) as passed
        FROM wells w
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        WHERE w.role_alias != 'Patient'
          AND w.resolution_codes IS NULL
          AND w.lims_status IS NULL
          {self._get_date_filter('w.created_at', 'control')}
          {self._get_site_filter('w')}
        GROUP BY m.mix_name
        """

        # Total samples (including those with errors, invalid LIMS, etc.)
        total_samples_query = f"""
        SELECT
            m.mix_name,
            COUNT(*) as total_count
        FROM wells w
        LEFT JOIN run_mixes rm ON w.run_mix_id = rm.id
        LEFT JOIN mixes m ON rm.mix_id = m.id
        WHERE w.role_alias = 'Patient'
          AND w.resolution_codes IS NULL
          {self._get_date_filter('w.created_at', 'sample')}
          {self._get_site_filter('w')}
        GROUP BY m.mix_name
        """

        cursor = self.conn.cursor()
        results = {}

        # Process patient results
        cursor.execute(patient_query)
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
                    'total_samples': 0
                }

            if 'NOT DETECTED' in (lims_status or '').upper():
                results[mix_name]['samples_not_detected'] += cnt
            elif 'DETECTED' in (lims_status or '').upper():
                results[mix_name]['samples_detected'] += cnt

            results[mix_name]['total_samples'] += cnt

        # Process control results
        cursor.execute(control_query)
        for row in cursor.fetchall():
            mix_name = row['mix_name']
            if mix_name not in results:
                results[mix_name] = {
                    'samples_detected': 0,
                    'samples_not_detected': 0,
                    'controls_passed': 0,
                    'controls_total': 0,
                    'total_samples': 0
                }

            results[mix_name]['controls_total'] = row['total']
            results[mix_name]['controls_passed'] = row['passed'] or 0

        # Process total samples (overwrite total_samples with actual count including errors)
        cursor.execute(total_samples_query)
        for row in cursor.fetchall():
            mix_name = row['mix_name']
            if mix_name in results:
                # Update with actual total (includes samples with errors, invalid LIMS, etc.)
                results[mix_name]['total_samples'] = row['total_count']

        print(f"  Found {len(results)} mixes with valid results")
        return results

    def _calculate_error_statistics(self, sample_payload, control_payload, discrepancy_payload) -> Dict:
        """Calculate error statistics by mix"""

        stats = defaultdict(lambda: {
            'sop_errors': 0,
            'sop_errors_affected': 0,
            'control_errors': 0,
            'control_errors_affected': 0,
            'samples_affected_by_controls': 0,
            'classification_errors': 0,
            'classification_errors_affected': 0
        })

        # Process sample errors
        for error in sample_payload['errors']:
            mix_name = error['mix_name']
            stats[mix_name]['sop_errors'] += 1
            if error['clinical_category'] in ('unresolved', 'test_repeated'):
                stats[mix_name]['sop_errors_affected'] += 1

        # Process control errors
        for error in control_payload['errors']:
            mix_name = error['mix_name']
            stats[mix_name]['control_errors'] += 1
            if error['clinical_category'] in ('unresolved', 'test_repeated'):
                stats[mix_name]['control_errors_affected'] += 1

        # Process affected samples
        for group_data in control_payload['affected_samples'].values():
            mix_name = group_data['control_mix']
            error_count = len(group_data.get('affected_samples_error', {}))
            repeat_count = len(group_data.get('affected_samples_repeat', {}))
            stats[mix_name]['samples_affected_by_controls'] += error_count + repeat_count

        # Process discrepancy errors
        for error in discrepancy_payload['errors']:
            mix_name = error['mix_name']
            stats[mix_name]['classification_errors'] += 1
            if error['clinical_category'] in ('acted_upon', 'samples_repeated'):
                stats[mix_name]['classification_errors_affected'] += 1

        print(f"  Calculated statistics for {len(stats)} mixes")
        return dict(stats)

    # =========================================================================
    # ENRICHMENT FUNCTIONS
    # =========================================================================

    def _enrich_with_curves(self, errors: List[Dict], report_type: str) -> Dict:
        """Enrich errors with curve data and comments"""

        well_curves = {}
        control_cache = {}
        comment_batch = []

        for error in errors:
            well_id = str(error['well_id'])
            if well_id in well_curves:
                continue

            # Fetch targets
            targets = fetch_targets_for_well(self.conn, well_id)
            if not targets:
                continue

            # Check if passive dye normalization is needed
            should_normalize, passive_readings = fetch_passive_normalization_data(
                self.conn, well_id, error['mix_name']
            )

            # Apply passive normalization to all targets if needed
            passive_status = None
            if should_normalize:
                if passive_readings:
                    # Apply normalization
                    for target in targets:
                        target['readings'] = normalize_readings_with_passive(
                            target['readings'], passive_readings
                        )
                    passive_status = 'normalized'
                else:
                    # Expected but missing
                    passive_status = 'expected_but_missing'

            # Fetch control curves for each target
            if report_type in ('sample', 'discrepancy'):
                target_list = []
                for target in targets:
                    # Fetch control curves with fallback
                    cache_key = (error['run_id'], error['mix_name'], target['target_name'])
                    if cache_key not in control_cache:
                        control_cache[cache_key] = self._fetch_control_curves(
                            error['run_id'],
                            error['mix_name'],
                            target['target_name']
                        )

                    target_data = {
                        'target_name': target['target_name'],
                        'readings': target['readings'],
                        'machine_ct': target['machine_ct'],
                        'is_passive': target['is_passive'],
                        'is_ic': target['is_ic'],
                        'control_curves': control_cache[cache_key]
                    }

                    if report_type == 'discrepancy':
                        target_data.update({
                            'machine_cls': target.get('machine_cls'),
                            'dxai_cls': target.get('dxai_cls'),
                            'final_cls': target.get('final_cls')
                        })

                    target_list.append(target_data)

                # Set main_target intelligently
                main_target = None
                if report_type == 'discrepancy' and 'target_name' in error:
                    # For discrepancy, use the discrepant target
                    main_target = error['target_name']
                else:
                    # For sample reports, use first non-IC target
                    for target in target_list:
                        if not target.get('is_ic'):
                            main_target = target['target_name']
                            break

                well_curves[well_id] = {
                    'sample_name': error['sample_name'],
                    'mix_name': error['mix_name'],
                    'main_target': main_target,
                    'targets': target_list,
                    'passive_status': passive_status  # normalized, expected_but_missing, or None
                }

            else:  # control report
                # Different structure for control report
                targets_dict = {}
                main_target = None

                for target in targets:
                    targets_dict[target['target_name']] = {
                        'readings': target['readings'],
                        'ct': target['machine_ct'],
                        'is_ic': target['is_ic']
                    }
                    if main_target is None and not target['is_ic']:
                        main_target = target['target_name']

                # Fetch controls for this control well
                if main_target:
                    controls = self._fetch_control_well_controls(
                        error['run_id'],
                        main_target,
                        error['mix_name']  # Pass mix_name for fallback logic
                    )
                else:
                    controls = []

                well_curves[well_id] = {
                    'main_target': main_target,
                    'targets': targets_dict,
                    'controls': controls,
                    'passive_status': passive_status  # normalized, expected_but_missing, or None
                }

            comment_batch.append(well_id)

            # Batch fetch comments
            if len(comment_batch) >= 200:
                comments = fetch_comments_batch(self.conn, comment_batch)
                for wid, items in comments.items():
                    if wid in well_curves:
                        well_curves[wid]['comments'] = items
                comment_batch = []

        # Fetch remaining comments
        if comment_batch:
            comments = fetch_comments_batch(self.conn, comment_batch)
            for wid, items in comments.items():
                if wid in well_curves:
                    well_curves[wid]['comments'] = items

        return well_curves

    def _fetch_control_curves(self, run_id: str, mix_name: str, target_name: str) -> List[Dict]:
        """Fetch control curves with fallback logic"""

        # First try exact mix match
        query = f"""
        SELECT
            w.id AS well_id,
            w.role_alias,
            w.sample_label,
            o.readings,
            o.machine_ct,
            m.mix_name
        FROM wells w
        JOIN observations o ON o.well_id = w.id
        JOIN targets t ON o.target_id = t.id
        JOIN run_mixes rm ON w.run_mix_id = rm.id
        JOIN mixes m ON rm.mix_id = m.id
        WHERE w.run_id = ?
          AND t.target_name = ?
          AND m.mix_name = ?
          AND w.role_alias != 'Patient'
          AND o.readings IS NOT NULL
        LIMIT {self.config.max_controls * 2}
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (run_id, target_name, mix_name))
        controls = []

        for row in cursor.fetchall():
            # Normalize control readings with passive dye if needed
            readings = decode_readings(row['readings'])
            should_normalize, passive_readings = fetch_passive_normalization_data(
                self.conn, row['well_id'], row['mix_name']
            )
            if should_normalize and passive_readings:
                readings = normalize_readings_with_passive(readings, passive_readings)

            ctrl_type = classify_control_role(row['role_alias'])
            controls.append({
                'readings': readings,
                'machine_ct': row['machine_ct'],
                'control_type': 'NC' if ctrl_type == 'negative' else 'PC' if ctrl_type == 'positive' else 'CTRL'
            })

        # If insufficient controls, try fallback with pattern matching
        if len(controls) < self.config.max_controls:
            # Extract assay pattern from mix name (e.g., "QCMVQ2" -> "CMV")
            pattern = self._extract_assay_pattern(mix_name)
            if pattern:
                fallback_query = f"""
                SELECT
                    w.id AS well_id,
                    w.role_alias,
                    w.sample_label,
                    o.readings,
                    o.machine_ct,
                    m.mix_name
                FROM wells w
                JOIN observations o ON o.well_id = w.id
                JOIN targets t ON o.target_id = t.id
                JOIN run_mixes rm ON w.run_mix_id = rm.id
                JOIN mixes m ON rm.mix_id = m.id
                WHERE w.run_id = ?
                  AND t.target_name LIKE ?
                  AND m.mix_name LIKE ?
                  AND m.mix_name != ?
                  AND w.role_alias != 'Patient'
                  AND o.readings IS NOT NULL
                LIMIT {self.config.max_controls * 2}
                """

                cursor.execute(fallback_query, (run_id, f'%{pattern}%', f'%{pattern}%', mix_name))

                for row in cursor.fetchall():
                    if len(controls) >= self.config.max_controls:
                        break

                    # Normalize control readings with passive dye if needed
                    readings = decode_readings(row['readings'])
                    should_normalize, passive_readings = fetch_passive_normalization_data(
                        self.conn, row['well_id'], row['mix_name']
                    )
                    if should_normalize and passive_readings:
                        readings = normalize_readings_with_passive(readings, passive_readings)

                    ctrl_type = classify_control_role(row['role_alias'])
                    controls.append({
                        'readings': readings,
                        'machine_ct': row['machine_ct'],
                        'control_type': 'NC' if ctrl_type == 'negative' else 'PC' if ctrl_type == 'positive' else 'CTRL'
                    })

        # Balance controls (prefer 2 NC, then PC)
        return self._balance_controls(controls)

    def _fetch_control_well_controls(self, run_id: str, target_name: str, mix_name: str = None) -> List[Dict]:
        """Fetch controls for control report with fallback to related mixes"""

        controls = []

        # If mix_name provided, try exact mix match first
        if mix_name:
            query = f"""
            SELECT
                w.id as well_id,
                w.sample_name,
                w.role_alias,
                o.readings,
                m.mix_name
            FROM wells w
            JOIN observations o ON o.well_id = w.id
            JOIN targets t ON o.target_id = t.id
            JOIN run_mixes rm ON w.run_mix_id = rm.id
            JOIN mixes m ON rm.mix_id = m.id
            WHERE w.run_id = ?
              AND t.target_name = ?
              AND m.mix_name = ?
              AND w.role_alias != 'Patient'
              AND o.readings IS NOT NULL
            LIMIT {self.config.max_controls * 2}
            """

            cursor = self.conn.cursor()
            cursor.execute(query, (run_id, target_name, mix_name))

            for row in cursor.fetchall():
                # Normalize control readings with passive dye if needed
                readings = decode_readings(row['readings'])
                should_normalize, passive_readings = fetch_passive_normalization_data(
                    self.conn, row['well_id'], row['mix_name']
                )
                if should_normalize and passive_readings:
                    readings = normalize_readings_with_passive(readings, passive_readings)

                ctrl_type = classify_control_role(row['role_alias'])
                controls.append({
                    'well_id': row['well_id'],
                    'name': row['sample_name'],
                    'type': ctrl_type,
                    'control_type': 'NC' if ctrl_type == 'negative' else 'PC' if ctrl_type == 'positive' else 'CTRL',
                    'readings': readings
                })

            # If insufficient controls, try fallback with related mixes
            if len(controls) < self.config.max_controls:
                pattern = self._extract_assay_pattern(mix_name)
                if pattern:
                    fallback_query = f"""
                    SELECT
                        w.id as well_id,
                        w.sample_name,
                        w.role_alias,
                        o.readings,
                        m.mix_name
                    FROM wells w
                    JOIN observations o ON o.well_id = w.id
                    JOIN targets t ON o.target_id = t.id
                    JOIN run_mixes rm ON w.run_mix_id = rm.id
                    JOIN mixes m ON rm.mix_id = m.id
                    WHERE w.run_id = ?
                      AND t.target_name LIKE ?
                      AND m.mix_name LIKE ?
                      AND m.mix_name != ?
                      AND w.role_alias != 'Patient'
                      AND o.readings IS NOT NULL
                    LIMIT {self.config.max_controls * 2}
                    """

                    cursor.execute(fallback_query, (run_id, f'%{pattern}%', f'%{pattern}%', mix_name))

                    for row in cursor.fetchall():
                        if len(controls) >= self.config.max_controls * 2:
                            break

                        # Normalize control readings with passive dye if needed
                        readings = decode_readings(row['readings'])
                        should_normalize, passive_readings = fetch_passive_normalization_data(
                            self.conn, row['well_id'], row['mix_name']
                        )
                        if should_normalize and passive_readings:
                            readings = normalize_readings_with_passive(readings, passive_readings)

                        ctrl_type = classify_control_role(row['role_alias'])
                        controls.append({
                            'well_id': row['well_id'],
                            'name': row['sample_name'],
                            'type': ctrl_type,
                            'control_type': 'NC' if ctrl_type == 'negative' else 'PC' if ctrl_type == 'positive' else 'CTRL',
                            'readings': readings
                        })
        else:
            # No mix provided, just fetch all controls for this target
            query = f"""
            SELECT
                w.id as well_id,
                w.sample_name,
                w.role_alias,
                o.readings,
                m.mix_name
            FROM wells w
            JOIN observations o ON o.well_id = w.id
            JOIN targets t ON o.target_id = t.id
            JOIN run_mixes rm ON w.run_mix_id = rm.id
            JOIN mixes m ON rm.mix_id = m.id
            WHERE w.run_id = ?
              AND t.target_name = ?
              AND w.role_alias != 'Patient'
              AND o.readings IS NOT NULL
            LIMIT {self.config.max_controls * 2}
            """

            cursor = self.conn.cursor()
            cursor.execute(query, (run_id, target_name))

            for row in cursor.fetchall():
                # Normalize control readings with passive dye if needed
                readings = decode_readings(row['readings'])
                should_normalize, passive_readings = fetch_passive_normalization_data(
                    self.conn, row['well_id'], row['mix_name']
                )
                if should_normalize and passive_readings:
                    readings = normalize_readings_with_passive(readings, passive_readings)

                ctrl_type = classify_control_role(row['role_alias'])
                controls.append({
                    'well_id': row['well_id'],
                    'name': row['sample_name'],
                    'type': ctrl_type,
                    'control_type': 'NC' if ctrl_type == 'negative' else 'PC' if ctrl_type == 'positive' else 'CTRL',
                    'readings': readings
                })

        # Balance controls to ensure at least one positive and one negative
        return self._balance_controls(controls)

    def _balance_controls(self, controls: List[Dict]) -> List[Dict]:
        """Balance controls to prefer 2 NC, then PC"""
        negatives = [c for c in controls if c['control_type'] == 'NC']
        positives = [c for c in controls if c['control_type'] == 'PC']
        others = [c for c in controls if c['control_type'] == 'CTRL']

        result = []
        # Add up to 2 negatives first
        result.extend(negatives[:min(2, len(negatives))])

        # Fill remaining with positives
        remaining = self.config.max_controls - len(result)
        if remaining > 0:
            result.extend(positives[:remaining])

        # Fill any remaining with others
        remaining = self.config.max_controls - len(result)
        if remaining > 0:
            result.extend(others[:remaining])

        return result

    # =========================================================================
    # HELPER FUNCTIONS
    # =========================================================================

    def _get_control_error_ids(self) -> str:
        """Get control-related error IDs for exclusion (used in discrepancy queries)"""
        if not self.config.control_error_codes:
            return "''"

        placeholders = ','.join(['?' for _ in self.config.control_error_codes])
        query = f"SELECT id FROM error_codes WHERE error_code IN ({placeholders})"

        cursor = self.conn.cursor()
        cursor.execute(query, self.config.control_error_codes)
        ids = [f"'{row['id']}'" for row in cursor.fetchall()]

        return ','.join(ids) if ids else "''"

    def _get_sample_exclusion_error_ids(self) -> str:
        """Get all error IDs to exclude from SOP sample report (control + classification discrepancies + custom)"""
        return self._get_exclusion_error_ids(
            self.config.sample_exclusion_error_codes,
            self.config.custom_sop_exclusions
        )

    def _get_control_exclusion_error_ids(self) -> str:
        """Get error IDs to exclude from Control report (default + custom exclusions)"""
        return self._get_exclusion_error_ids(
            self.config.control_exclusion_error_codes,  # Default control exclusions (e.g., UNKNOWN_MIX)
            self.config.custom_control_exclusions
        )

    def _get_exclusion_error_ids(self, default_codes: Optional[List[str]], custom_codes: Optional[List[str]]) -> str:
        """
        Get error IDs for exclusion (supports wildcards like %SIGMOID%)

        Args:
            default_codes: Default error codes to exclude
            custom_codes: Custom error codes (may include wildcards)

        Returns:
            Comma-separated quoted UUIDs
        """
        # Build query parts
        exact_matches = []
        wildcard_conditions = []
        params = []

        # Add default exact matches
        if default_codes:
            exact_matches.extend(default_codes)

        # Process custom codes (may include wildcards)
        if custom_codes:
            for code in custom_codes:
                if '%' in code:
                    # Wildcard pattern
                    wildcard_conditions.append("error_code LIKE ?")
                    params.append(code)
                else:
                    # Exact match
                    exact_matches.append(code)

        if not exact_matches and not wildcard_conditions:
            return "''"

        # Build query
        where_parts = []
        if exact_matches:
            placeholders = ','.join(['?' for _ in exact_matches])
            where_parts.append(f"error_code IN ({placeholders})")
            params = exact_matches + params

        if wildcard_conditions:
            where_parts.append('(' + ' OR '.join(wildcard_conditions) + ')')

        query = f"SELECT id FROM error_codes WHERE {' OR '.join(where_parts)}"

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        ids = [f"'{row['id']}'" for row in cursor.fetchall()]

        return ','.join(ids) if ids else "''"

    def _get_date_filter(self, column: str, report_type: str) -> str:
        """Get date filter SQL for specified report type"""
        filters = []

        if report_type == 'sample':
            if self.config.sample_since_date:
                filters.append(f"AND {column} >= '{self.config.sample_since_date}'")
            if self.config.sample_until_date:
                filters.append(f"AND {column} <= '{self.config.sample_until_date} 23:59:59'")
        elif report_type == 'control':
            if self.config.control_since_date:
                filters.append(f"AND {column} >= '{self.config.control_since_date}'")
            if self.config.control_until_date:
                filters.append(f"AND {column} <= '{self.config.control_until_date} 23:59:59'")
        elif report_type == 'discrepancy':
            if self.config.discrepancy_since_date:
                filters.append(f"AND {column} >= '{self.config.discrepancy_since_date}'")
            if self.config.discrepancy_until_date:
                filters.append(f"AND {column} <= '{self.config.discrepancy_until_date} 23:59:59'")

        return ' '.join(filters)

    def _get_site_filter(self, table_alias: str = 'w') -> str:
        """Get site filter SQL if configured"""
        if not self.config.site_ids:
            return ""

        # Create IN clause with quoted site IDs
        site_list = ', '.join([f"'{site_id}'" for site_id in self.config.site_ids])
        return f"AND {table_alias}.site_id IN ({site_list})"

    def _get_limit(self) -> str:
        """Get LIMIT clause if configured"""
        return f"LIMIT {self.config.limit}" if self.config.limit else ""

    def _extract_assay_pattern(self, mix_name: str) -> Optional[str]:
        """Extract assay pattern from mix name for fallback matching"""
        # Common patterns to extract
        patterns = {
            'CMV': ['CMV', 'CYTOMEG'],
            'BK': ['BKV', 'BK'],
            'EBV': ['EBV', 'EPSTEIN'],
            'ADV': ['ADV', 'ADENO'],
            'VZV': ['VZV', 'VARICELLA'],
            'HSV': ['HSV', 'HERPES'],
            'HHV6': ['HHV6', 'HHV-6'],
            'PARV': ['PARV', 'PARVO']
        }

        mix_upper = mix_name.upper()
        for key, variants in patterns.items():
            for variant in variants:
                if variant in mix_upper:
                    return key

        return None

    def _format_error_record(self, row: sqlite3.Row) -> Dict:
        """Format standard error record from database row"""
        return {
            'well_id': row['well_id'],
            'sample_name': row['sample_name'] or 'Unknown',
            'well_number': row['well_number'] or 'Unknown',
            'error_code': row['error_code'] or '',
            'error_message': row['error_message'] or row['error_code'] or '',
            'mix_name': row['mix_name'] or 'Unknown',
            'run_name': row['run_name'] or 'Unknown',
            'run_id': row['run_id'],
            'lims_status': row['lims_status'] or '',
            'resolution_codes': row['resolution_codes'] or '',
            'clinical_category': row['clinical_category'],
            'created_at': str(row['created_at'])[:10] if row['created_at'] else ''
        }

    def _format_discrepancy_record(self, row: sqlite3.Row) -> Dict:
        """Format discrepancy record with additional fields"""
        record = self._format_error_record(row)
        record.update({
            'machine_cls': row['machine_cls'],
            'dxai_cls': row['dxai_cls'],
            'final_cls': row['final_cls'],
            'machine_ct': row['machine_ct'],
            'target_name': row['target_name'],
            'category_detail': row['category_detail'] if 'category_detail' in row.keys() else ''
        })
        return record

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """Main entry point for the unified JSON extractor"""

    parser = argparse.ArgumentParser(description='Unified JSON Extractor for Multi-Database Reporting')

    # Database arguments
    parser.add_argument('--db-type', choices=['qst', 'notts', 'vira'], default='qst',
                       help='Database type (default: qst)')
    parser.add_argument('--db', required=True, help='Path to SQLite database')
    parser.add_argument('--output', required=True, help='Output JSON file path')

    # Unified date range (applies to all reports)
    parser.add_argument('--since-date', help='Unified start date for all reports (YYYY-MM-DD). Overridden by report-specific filters.')
    parser.add_argument('--until-date', help='Unified end date for all reports (YYYY-MM-DD). Overridden by report-specific filters.')

    # Fine-grained date range arguments (override unified --since-date and --until-date)
    parser.add_argument('--sample-since-date', help='Sample wells on/after this date (YYYY-MM-DD). Overrides --since-date.')
    parser.add_argument('--sample-until-date', help='Sample wells on/before this date (YYYY-MM-DD). Overrides --until-date.')
    parser.add_argument('--control-since-date', help='Control wells on/after this date (YYYY-MM-DD). Overrides --since-date.')
    parser.add_argument('--control-until-date', help='Control wells on/before this date (YYYY-MM-DD). Overrides --until-date.')
    parser.add_argument('--discrepancy-since-date',
                       help='Discrepancy wells on/after this date (YYYY-MM-DD). Overrides --since-date. Defaults to 2024-01-01.')
    parser.add_argument('--discrepancy-until-date', help='Discrepancy wells on/before this date (YYYY-MM-DD). Overrides --until-date.')
    parser.add_argument('--discrepancy-date-field', choices=['upload', 'extraction'], default='upload',
                       help='Date field for discrepancy filtering (default: upload)')

    # Options
    parser.add_argument('--limit', type=int, help='Limit number of records processed')
    parser.add_argument('--max-controls', type=int, default=3,
                       help='Maximum control curves per target (default: 3)')
    parser.add_argument('--sample-include-label-errors', action='store_true',
                       help='Include label/setup errors in sample report')
    parser.add_argument('--exclude-from-sop', nargs='+', metavar='ERROR_CODE',
                       help='Additional error codes to exclude from SOP sample report (supports wildcards: %%SIGMOID%%)')
    parser.add_argument('--exclude-from-control', nargs='+', metavar='ERROR_CODE',
                       help='Additional error codes to exclude from Control report and affected samples (supports wildcards: %%SIGMOID%%)')
    parser.add_argument('--suppress-unaffected-controls', action='store_true',
                       help='Suppress control errors that have no associated affected samples')
    parser.add_argument('--site-ids', nargs='+', metavar='SITE_ID',
                       help='Filter by site IDs (only include wells from these sites)')
    parser.add_argument('--test', action='store_true',
                       help='Test mode (limits to 100 records)')

    args = parser.parse_args()

    # Apply test mode limit
    if args.test:
        args.limit = min(args.limit or 100, 100)

    # Handle unified vs fine-grained date filters
    # Fine-grained filters override unified filters
    sample_since = args.sample_since_date or args.since_date
    sample_until = args.sample_until_date or args.until_date
    control_since = args.control_since_date or args.since_date
    control_until = args.control_until_date or args.until_date
    # Discrepancy has a default of 2024-01-01 if not specified
    discrepancy_since = args.discrepancy_since_date or args.since_date or '2024-01-01'
    discrepancy_until = args.discrepancy_until_date or args.until_date

    # Create configuration
    config = ExtractorConfig(
        db_path=args.db,
        db_type=args.db_type,
        sample_since_date=sample_since,
        sample_until_date=sample_until,
        control_since_date=control_since,
        control_until_date=control_until,
        discrepancy_since_date=discrepancy_since,
        discrepancy_until_date=discrepancy_until,
        discrepancy_date_field=args.discrepancy_date_field,
        max_controls=args.max_controls,
        limit=args.limit,
        include_label_errors=args.sample_include_label_errors,
        suppress_unaffected_controls=args.suppress_unaffected_controls,
        site_ids=args.site_ids,
        custom_sop_exclusions=args.exclude_from_sop,
        custom_control_exclusions=args.exclude_from_control
    )

    # Create extractor and run
    extractor = UnifiedJSONExtractor(config)

    try:
        # Extract combined report
        result = extractor.extract_combined_report()

        # Write output
        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)

        print(f"\nCombined report written to {args.output}")

        # Print summary
        total_sample = len(result['reports']['sample']['errors'])
        total_control = len(result['reports']['control']['errors'])
        total_discrepancy = len(result['reports']['discrepancy']['errors'])

        print(f"  Sample errors: {total_sample}")
        print(f"  Control errors: {total_control}")
        print(f"  Discrepancy errors: {total_discrepancy}")

    finally:
        extractor.close()


if __name__ == '__main__':
    main()
