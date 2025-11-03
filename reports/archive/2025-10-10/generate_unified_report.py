#!/usr/bin/env python3
"""
Unified Report Generator for Control, Sample, and Discrepancy Reports
Consolidates three report types into a single configurable script
"""

import sqlite3
import argparse
import os
import json
from datetime import datetime
from collections import defaultdict
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.database import bytes_to_float

# Report type configurations
REPORT_CONFIGS = {
    'control': {
        'default_db': 'input_data/quest_prod_aug2025.db',
        'schema': 'quest',
        'title': 'Control Error Report with Affected Samples',
        'categories': [
            ('unresolved', 'Unresolved'),
            ('error_ignored', 'Error Ignored'),
            ('test_repeated', 'Test Repeated')
        ],
        'included_error_types': [
            'THRESHOLD_WRONG',
            'CONTROL_CLSDISC_WELL',
            'FAILED_POS_WELL',
            'BICQUAL_WELL',
            'CNTRL_HAS_ACS',
            'WG13S_HIGH_WELL',
            'NEGATIVE_FAILURE_WELL',
            'WG_IN_ERROR_WELL',
            'WG12S_HIGH_WELL',
            'INCORRECT_SIGMOID',
            'WG13S_LOW_WELL',
            'WG12S_LOW_WELL',
            'CONTROL_CTDISC_WELL',
            'WG14S_LOW_WELL',
            'LOW_FLUORESCENCE_WELL',
            'WG14S_HIGH_WELL',
            'WESTGARDS_MISSED'
        ],
        'has_appendix': True,
        'has_curves': True
    },
    'sample': {
        'default_db': 'input_data/quest_prod_aug2025.db',
        'schema': 'quest',
        'title': 'Sample Error Report',
        'categories': [
            ('unresolved', 'Unresolved'),
            ('error_ignored', 'Error Ignored'),
            ('test_repeated', 'Test Repeated')
        ],
        'included_error_types': [
            'INH_WELL',
            'ADJ_CT',
            'DO_NOT_EXPORT',
            'INCONCLUSIVE_WELL',
            'CTDISC_WELL',
            'BICQUAL_WELL',
            'BAD_CT_DELTA',
            'LOW_FLUORESCENCE_WELL'
        ],
        'setup_error_types': [
            'MIX_MISSING',
            'UNKNOWN_MIX',
            'ACCESSION_MISSING',
            'INVALID_ACCESSION',
            'UNKNOWN_ROLE',
            'CONTROL_FAILURE',
            'MISSING_CONTROL',
            'INHERITED_CONTROL_FAILURE'
        ],
        'has_appendix': False,
        'has_curves': True
    },
    'discrepancy': {
        'default_db': 'qst_discreps.db',
        'schema': 'qst',
        'title': 'QST Discrepancy Report',
        'categories': [
            ('acted_upon', 'Discrepancies Acted Upon'),
            ('samples_repeated', 'Samples Repeated'),
            ('ignored', 'Discrepancies Ignored')
        ],
        'has_appendix': False,
        'has_curves': True
    }
}

class UnifiedReportGenerator:
    """Main report generator class"""
    
    def __init__(self, report_type, db_path=None, output_path=None, **options):
        self.report_type = report_type
        self.config = REPORT_CONFIGS[report_type]
        self.db_path = db_path or self.config['default_db']
        self.output_path = output_path or f'output_data/{report_type}_report.html'
        self.options = options
        self.conn = None
        
    def connect_db(self):
        """Connect to the database"""
        print(f"\nConnecting to database: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            
    def generate_report(self):
        """Main method to generate the report"""
        try:
            self.connect_db()
            
            # Fetch data based on report type
            print(f"\nFetching {self.report_type} data...")
            if self.report_type == 'control':
                data = self.fetch_control_data()
            elif self.report_type == 'sample':
                data = self.fetch_sample_data()
            elif self.report_type == 'discrepancy':
                data = self.fetch_discrepancy_data()
            else:
                raise ValueError(f"Unknown report type: {self.report_type}")
            
            # Generate HTML
            print(f"Generating interactive HTML report...")
            html = self.generate_html(data)
            
            # Write output
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with open(self.output_path, 'w', encoding='utf-8') as f:
                f.write(html)
            
            print(f"\nReport generated successfully:")
            print(f"  Output file: {self.output_path}")
            print(f"  Total records: {len(data['errors']) if 'errors' in data else len(data)}")
            
        finally:
            self.close_db()
    
    def fetch_control_data(self):
        """Fetch control error data"""
        handler = ControlReportHandler(self.conn, self.config, self.options)
        return handler.fetch_all_data()
    
    def fetch_sample_data(self):
        """Fetch sample error data"""
        handler = SampleReportHandler(self.conn, self.config, self.options)
        return handler.fetch_all_data()
    
    def fetch_discrepancy_data(self):
        """Fetch QST discrepancy data"""
        handler = DiscrepancyReportHandler(self.conn, self.config, self.options)
        return handler.fetch_all_data()
    
    def generate_html(self, data):
        """Generate the HTML report"""
        generator = HTMLGenerator(self.report_type, self.config, self.options)
        return generator.generate(data)


class ControlReportHandler:
    """Handler for control error reports"""
    
    def __init__(self, conn, config, options):
        self.conn = conn
        self.config = config
        self.options = options
        self.exclude_error_type_zero = options.get('exclude_error_type_zero', True)
        
    def fetch_all_data(self):
        """Fetch all control error data"""
        errors = self.fetch_control_errors()
        affected_samples = {}
        
        if self.config['has_appendix']:
            # Only fetch affected samples for unresolved and test_repeated
            control_ids = [e['well_id'] for e in errors 
                          if e.get('clinical_category') in ['unresolved', 'test_repeated']]
            if control_ids:
                affected_samples = self.fetch_affected_samples(control_ids)
        
        return {
            'errors': errors,
            'affected_samples': affected_samples
        }
    
    def fetch_control_errors(self):
        """Fetch control wells with errors or resolutions"""
        cursor = self.conn.cursor()
        
        # Build error type filter
        error_type_filter = "AND ec.error_type != 0" if self.exclude_error_type_zero else ""
        error_type_filter_resolved = "AND (ec.error_type IS NULL OR ec.error_type != 0)" if self.exclude_error_type_zero else ""
        
        # Unresolved query
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
            'unresolved' as category
        FROM wells w
        JOIN error_codes ec ON w.error_code_id = ec.id
        JOIN runs r ON w.run_id = r.id
        JOIN run_mixes rm ON w.run_mix_id = rm.id
        JOIN mixes m ON rm.mix_id = m.id
        WHERE w.error_code_id IS NOT NULL
        AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
        {error_type_filter}
        AND w.role_alias IS NOT NULL
        AND w.role_alias != 'Patient'
        AND (w.role_alias LIKE '%PC%' 
             OR w.role_alias LIKE '%NC%' 
             OR w.role_alias LIKE '%CONTROL%'
             OR w.role_alias LIKE '%NEGATIVE%'
             OR w.role_alias LIKE '%POSITIVE%'
             OR w.role_alias LIKE '%NTC%'
             OR w.role_alias LIKE '%PTC%')
        """
        
        # Resolved query
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
            'resolved' as category
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        JOIN runs r ON w.run_id = r.id
        JOIN run_mixes rm ON w.run_mix_id = rm.id
        JOIN mixes m ON rm.mix_id = m.id
        WHERE w.resolution_codes IS NOT NULL 
        AND w.resolution_codes <> ''
        {error_type_filter_resolved}
        AND w.role_alias IS NOT NULL
        AND w.role_alias != 'Patient'
        AND (w.role_alias LIKE '%PC%' 
             OR w.role_alias LIKE '%NC%' 
             OR w.role_alias LIKE '%CONTROL%'
             OR w.role_alias LIKE '%NEGATIVE%'
             OR w.role_alias LIKE '%POSITIVE%'
             OR w.role_alias LIKE '%NTC%'
             OR w.role_alias LIKE '%PTC%')
        """
        
        all_errors = []
        
        # Fetch unresolved
        print("  Fetching unresolved errors...")
        cursor.execute(unresolved_query)
        unresolved = cursor.fetchall()
        for row in unresolved:
            error = dict(row)
            error['clinical_category'] = 'unresolved'
            all_errors.append(error)
        print(f"    Found {len(unresolved)} unresolved errors")
        
        # Fetch resolved and categorize
        print("  Fetching resolved errors...")
        cursor.execute(resolved_query)
        resolved = cursor.fetchall()
        error_ignored_count = 0
        test_repeated_count = 0
        
        for row in resolved:
            error = dict(row)
            resolution_code = (error.get('error_code') or '').upper()
            
            # Check for repeat codes (RP, RX, TN)
            if 'RP' in resolution_code or 'RX' in resolution_code or 'TN' in resolution_code:
                error['clinical_category'] = 'test_repeated'
                test_repeated_count += 1
            else:
                error['clinical_category'] = 'error_ignored'
                error_ignored_count += 1
            
            all_errors.append(error)
        
        print(f"    Found {len(resolved)} resolved errors ({error_ignored_count} ignored, {test_repeated_count} repeated)")
        
        return all_errors
    
    def fetch_affected_samples(self, control_well_ids):
        """Fetch patient samples affected by failed controls"""
        cursor = self.conn.cursor()
        
        # Query for INHERITED errors
        inherited_query = """
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
        WHERE pw.error_code_id IN (
            '937829a3-a630-4a86-939d-c2b1ec229c9d',
            '937829a3-aa88-44cf-bbd5-deade616cff5',
            '995a530f-1da9-457d-9217-5afdac6ca59f',
            '995a530f-2239-4007-80f9-4102b5826ee5'
        )
        AND (pw.role_alias IS NULL OR pw.role_alias = 'Patient')
        AND (pw.resolution_codes IS NULL OR pw.resolution_codes = '')
        AND cw.role_alias IS NOT NULL
        AND cw.role_alias != 'Patient'
        AND (cw.error_code_id IS NOT NULL OR cw.resolution_codes IS NOT NULL)
        """
        
        # Query for REPEATED samples
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
        AND (cw.resolution_codes LIKE '%RP%' 
             OR cw.resolution_codes LIKE '%RX%' 
             OR cw.resolution_codes LIKE '%TN%')
        """
        
        cursor.execute(inherited_query)
        inherited_results = cursor.fetchall()
        
        cursor.execute(repeated_query)
        repeated_results = cursor.fetchall()
        
        # Count unique samples
        unique_inherited = set(row['well_id'] for row in inherited_results)
        unique_repeated = set(row['well_id'] for row in repeated_results)
        
        print(f"  Found {len(inherited_results)} rows with {len(unique_inherited)} unique INHERITED affected samples")
        print(f"  Found {len(repeated_results)} rows with {len(unique_repeated)} unique REPEATED affected samples")
        
        # Group by control set
        grouped = {}
        for row in inherited_results + repeated_results:
            group_key = f"{row['run_name']}_{row['control_mix']}"
            
            if group_key not in grouped:
                grouped[group_key] = {
                    'run_name': row['run_name'],
                    'control_mix': row['control_mix'],
                    'controls': {},
                    'affected_samples_error': {},
                    'affected_samples_repeat': {}
                }
            
            # Add control info
            control_id = row['control_well_id']
            if control_id not in grouped[group_key]['controls']:
                grouped[group_key]['controls'][control_id] = {
                    'control_name': row['control_name'],
                    'control_well': row['control_well'],
                    'resolution': row['control_resolution']
                }
            
            # Categorize sample
            lims_status = row['lims_status']
            is_repeated_sample = lims_status in ('REAMP', 'REXCT', 'RPT', 'RXT', 'TNP')
            
            sample_data = {
                'well_id': row['well_id'],
                'sample_name': row['sample_name'],
                'well_number': row['well_number'],
                'error_code': row['error_code'],
                'error_message': row['error_message'],
                'mix_name': row['mix_name'],
                'run_name': row['run_name'],
                'lims_status': lims_status,
                'resolution_codes': row['resolution_codes']
            }
            
            if is_repeated_sample:
                grouped[group_key]['affected_samples_repeat'][row['well_id']] = sample_data
            else:
                grouped[group_key]['affected_samples_error'][row['well_id']] = sample_data
        
        return grouped


class SampleReportHandler:
    """Handler for sample error reports"""
    
    def __init__(self, conn, config, options):
        self.conn = conn
        self.config = config
        self.options = options
        self.include_label_errors = options.get('include_label_errors', False)
        self.exclude_error_type_zero = options.get('exclude_error_type_zero', True)
        
    def fetch_all_data(self):
        """Fetch all sample error data"""
        errors = self.fetch_sample_errors()
        return {'errors': errors}
    
    def fetch_sample_errors(self):
        """Fetch patient sample errors"""
        cursor = self.conn.cursor()
        
        # Build error type lists
        included_types = self.config['included_error_types'].copy()
        if self.include_label_errors:
            included_types.extend(self.config.get('setup_error_types', []))
        
        # Build error type filter
        error_type_filter = "AND ec.error_type != 0" if self.exclude_error_type_zero else ""
        error_type_filter_resolved = "AND (ec.error_type IS NULL OR ec.error_type != 0)" if self.exclude_error_type_zero else ""
        
        # Build error code filter
        if included_types:
            error_codes_str = "','".join(included_types)
            error_code_filter = f"AND ec.error_code IN ('{error_codes_str}')"
        else:
            error_code_filter = ""
        
        # Unresolved query
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
            'unresolved' as category
        FROM wells w
        JOIN error_codes ec ON w.error_code_id = ec.id
        JOIN runs r ON w.run_id = r.id
        JOIN run_mixes rm ON w.run_mix_id = rm.id
        JOIN mixes m ON rm.mix_id = m.id
        WHERE w.error_code_id IS NOT NULL
        AND (w.resolution_codes IS NULL OR w.resolution_codes = '')
        {error_type_filter}
        {error_code_filter}
        AND (w.role_alias IS NULL OR w.role_alias = 'Patient')
        """
        
        # Resolved query
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
            'resolved' as category
        FROM wells w
        LEFT JOIN error_codes ec ON w.error_code_id = ec.id
        JOIN runs r ON w.run_id = r.id
        JOIN run_mixes rm ON w.run_mix_id = rm.id
        JOIN mixes m ON rm.mix_id = m.id
        WHERE w.resolution_codes IS NOT NULL 
        AND w.resolution_codes <> ''
        {error_type_filter_resolved}
        AND (w.role_alias IS NULL OR w.role_alias = 'Patient')
        """
        
        all_errors = []
        
        # Fetch unresolved
        print("  Fetching unresolved errors...")
        cursor.execute(unresolved_query)
        unresolved = cursor.fetchall()
        for row in unresolved:
            error = dict(row)
            error['clinical_category'] = 'unresolved'
            all_errors.append(error)
        print(f"    Found {len(unresolved)} unresolved errors")
        
        # Fetch resolved and categorize
        print("  Fetching resolved errors...")
        cursor.execute(resolved_query)
        resolved = cursor.fetchall()
        error_ignored_count = 0
        test_repeated_count = 0
        
        for row in resolved:
            error = dict(row)
            resolution_code = (error.get('error_code') or '').upper()
            
            # Check for ignore codes
            if 'BLA' in resolution_code or 'SKIP' in resolution_code:
                error['clinical_category'] = 'error_ignored'
                error_ignored_count += 1
            elif any(code in resolution_code for code in ['RP', 'RX', 'TN', 'TP']):
                error['clinical_category'] = 'test_repeated'
                test_repeated_count += 1
            else:
                error['clinical_category'] = 'test_repeated'
                test_repeated_count += 1
            
            all_errors.append(error)
        
        print(f"    Found {len(resolved)} resolved errors ({error_ignored_count} ignored, {test_repeated_count} repeated)")
        
        return all_errors


class DiscrepancyReportHandler:
    """Handler for QST discrepancy reports"""
    
    def __init__(self, conn, config, options):
        self.conn = conn
        self.config = config
        self.options = options
        
    def fetch_all_data(self):
        """Fetch all discrepancy data"""
        cursor = self.conn.cursor()
        
        # Query QST database
        query = """
        SELECT 
            id,
            run_id as run,
            well_number as well,
            sample_label as sample_name,
            mix_name as mix,
            target_name as target,
            machine_ct as ct,
            machine_cls,
            machine_cls as machine_conf,
            final_cls,
            lims_status,
            error_code
        FROM qst_readings
        ORDER BY mix_name, target_name, sample_label
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        all_records = []
        for row in rows:
            record = dict(row)
            
            # Categorize the record
            category, color, section = self.categorize_record(record)
            
            # Skip suppressed records
            if section == 0:
                continue
            
            record['category'] = category
            record['color'] = color
            record['section'] = section
            
            # Map sections to clinical categories for unified handling
            if section == 1:
                record['clinical_category'] = 'acted_upon'
            elif section == 2:
                record['clinical_category'] = 'samples_repeated'
            elif section == 3:
                record['clinical_category'] = 'ignored'
            
            # Add compatibility fields
            record['well_id'] = record['id']
            record['mix_name'] = record['mix']
            record['run_name'] = record['run']
            record['well_number'] = record['well']
            record['error_message'] = record.get('error_code', '')
            
            # Fetch control and observation data
            record['controls'] = self.fetch_controls(cursor, record['id'])
            record['observations'] = self.fetch_observations(cursor, record['id'])
            
            all_records.append(record)
        
        print(f"  Found {len(all_records)} discrepancy records")
        return {'errors': all_records}
    
    def categorize_record(self, row):
        """Categorize QST record based on classification discrepancies"""
        machine_cls = row['machine_cls']
        final_cls = row['final_cls']
        lims_status = row['lims_status']
        error_code = row['error_code']
        
        # Check suppression condition
        if not lims_status and not error_code:
            return ('suppressed', None, 0)
        
        # Section 1: Discrepancies Acted Upon
        if machine_cls != final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
            if final_cls == 1:
                return ('discrepancy_positive', '#90EE90', 1)  # Green
            else:
                return ('discrepancy_negative', '#FF6B6B', 1)  # Red
        
        # Section 2: Samples Repeated
        if error_code:
            return ('has_error', '#FFB6C1', 2)  # Pink
        if lims_status and lims_status not in ('DETECTED', 'NOT DETECTED'):
            return ('lims_other', '#FFD700', 2)  # Yellow
        
        # Section 3: Discrepancies Ignored
        if machine_cls == final_cls and lims_status in ('DETECTED', 'NOT DETECTED'):
            if lims_status == 'DETECTED':
                return ('agreement_detected', '#E8F5E9', 3)  # Pale green
            else:
                return ('agreement_not_detected', '#FCE4EC', 3)  # Pale pink
        
        return ('unknown', '#F5F5F5', 3)
    
    def fetch_controls(self, cursor, disc_id):
        """Fetch control data for a discrepancy record"""
        # For QST database, controls are stored differently
        # We'll return empty for now as the structure is different
        return []
    
    def fetch_observations(self, cursor, disc_id):
        """Fetch other observations for a discrepancy record"""
        # QST database has different structure for observations
        # Return empty for now
        return []


class HTMLGenerator:
    """Generates HTML output for reports"""
    
    def __init__(self, report_type, config, options):
        self.report_type = report_type
        self.config = config
        self.options = options
        self.max_per_category = options.get('max_per_category', 100)
        
    def generate(self, data):
        """Generate the complete HTML report"""
        # Group errors by mix and category
        mix_groups = self.group_by_mix_and_category(data['errors'])
        
        # Build HTML sections
        html = self.generate_header()
        html += self.generate_styles()
        html += self.generate_summary_stats(data['errors'])
        html += self.generate_table_of_contents(mix_groups, data.get('affected_samples', {}))
        
        # Generate mix sections
        for mix_name, categories in sorted(mix_groups.items()):
            html += self.generate_mix_section(mix_name, categories, data)
        
        # Generate appendix if needed
        if self.config['has_appendix'] and data.get('affected_samples'):
            html += self.generate_appendix(data['affected_samples'])
        
        html += self.generate_javascript(data)
        html += self.generate_footer()
        
        return html
    
    def group_by_mix_and_category(self, errors):
        """Group errors by mix and clinical category"""
        mix_groups = defaultdict(lambda: defaultdict(list))
        for error in errors:
            clinical_cat = error.get('clinical_category', error.get('category', 'unresolved'))
            mix_groups[error['mix_name']][clinical_cat].append(error)
        return mix_groups
    
    def generate_header(self):
        """Generate HTML header"""
        return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{self.config['title']}</title>'''
    
    def generate_styles(self):
        """Generate CSS styles"""
        return '''
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 10px;
            background-color: #f5f5f5;
        }
        
        .header {
            text-align: center;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 10px;
            margin-bottom: 20px;
        }
        
        .summary-stats {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin: 20px 0;
            padding: 20px;
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .stat-item {
            text-align: center;
        }
        
        .stat-value {
            font-size: 36px;
            font-weight: bold;
            margin-bottom: 5px;
        }
        
        .stat-label {
            font-size: 14px;
            color: #666;
            text-transform: uppercase;
        }
        
        .toc {
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .mix-section {
            background: white;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .mix-header {
            padding: 15px 20px;
            background: linear-gradient(to right, #f5f5f5, #e0e0e0);
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .mix-content {
            padding: 20px;
            display: none;
        }
        
        .mix-section.expanded .mix-content {
            display: block;
        }
        
        .category-tabs {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            border-bottom: 2px solid #e0e0e0;
        }
        
        .category-tab {
            padding: 10px 20px;
            background: #f5f5f5;
            border: none;
            cursor: pointer;
            border-radius: 5px 5px 0 0;
            transition: all 0.3s;
        }
        
        .category-tab.active {
            background: white;
            border-bottom: 2px solid #2196F3;
        }
        
        .category-content {
            display: none;
        }
        
        .category-content.active {
            display: block;
        }
        
        .error-card {
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            background: white;
        }
        
        .error-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            text-transform: uppercase;
            margin-left: 10px;
        }
        
        .error-badge.unresolved {
            background: #ffebee;
            color: #d32f2f;
        }
        
        .error-badge.error_ignored {
            background: #e8f5e9;
            color: #388e3c;
        }
        
        .error-badge.test_repeated {
            background: #fff3e0;
            color: #f57c00;
        }
        
        .expand-icon {
            font-size: 20px;
            transition: transform 0.3s;
        }
        
        .mix-section.expanded .expand-icon {
            transform: rotate(90deg);
        }
    </style>
</head>
<body>'''
    
    def generate_summary_stats(self, errors):
        """Generate summary statistics section"""
        # Count by category
        counts = defaultdict(int)
        for error in errors:
            cat = error.get('clinical_category', 'unresolved')
            counts[cat] += 1
        
        html = f'''
    <div class="header">
        <h1>{self.config['title']}</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="summary-stats">'''
        
        # Add total
        html += f'''
        <div class="stat-item">
            <div class="stat-value">{len(errors)}</div>
            <div class="stat-label">Total Errors</div>
        </div>'''
        
        # Add category counts
        for cat_key, cat_label in self.config['categories']:
            count = counts.get(cat_key, 0)
            color = '#d32f2f' if cat_key == 'unresolved' else '#388e3c' if 'ignored' in cat_key else '#f57c00'
            html += f'''
        <div class="stat-item">
            <div class="stat-value" style="color: {color};">{count}</div>
            <div class="stat-label">{cat_label}</div>
        </div>'''
        
        html += '''
    </div>'''
        
        return html
    
    def generate_table_of_contents(self, mix_groups, affected_samples):
        """Generate table of contents"""
        html = '''
    <div class="toc">
        <h2>Table of Contents</h2>
        <ul>'''
        
        for mix_name, categories in sorted(mix_groups.items()):
            total = sum(len(records) for records in categories.values())
            counts = []
            for cat_key, _ in self.config['categories']:
                if cat_key in categories:
                    counts.append(f"{cat_key.replace('_', ' ').title()}: {len(categories[cat_key])}")
            
            mix_anchor = mix_name.replace(" ", "_").replace("/", "_")
            html += f'''
            <li>
                <a href="#mix-{mix_anchor}">{mix_name}</a>
                <span style="color: #666; font-size: 12px;"> - Total: {total} | {' | '.join(counts)}</span>
            </li>'''
        
        # Add appendix link if applicable
        if self.config['has_appendix'] and affected_samples:
            error_count = sum(len(g.get('affected_samples_error', {})) for g in affected_samples.values())
            repeat_count = sum(len(g.get('affected_samples_repeat', {})) for g in affected_samples.values())
            html += f'''
            <li style="margin-top: 15px; padding-top: 15px; border-top: 2px solid #e0e0e0;">
                <a href="#appendix"><strong>APPENDIX: Affected Patient Samples</strong></a>
                <ul>
                    <li><a href="#appendix-error">ERROR - Active Failed Samples ({error_count})</a></li>
                    <li><a href="#appendix-repeats">REPEATS - Resolved Samples ({repeat_count})</a></li>
                </ul>
            </li>'''
        
        html += '''
        </ul>
    </div>'''
        
        return html
    
    def generate_mix_section(self, mix_name, categories, data):
        """Generate section for a single mix"""
        mix_anchor = mix_name.replace(" ", "_").replace("/", "_")
        total_errors = sum(len(records) for records in categories.values())
        
        html = f'''
    <div class="mix-section" id="mix-{mix_anchor}">
        <div class="mix-header" onclick="toggleSection('{mix_anchor}')">
            <div>
                <span style="font-weight: bold; font-size: 18px;">Mix: {mix_name}</span>
                <span style="color: #666; margin-left: 10px;">Total errors: {total_errors}</span>
            </div>
            <span class="expand-icon">▶</span>
        </div>
        <div class="mix-content">
            <div class="category-tabs">'''
        
        # Add tabs
        for idx, (cat_key, cat_label) in enumerate(self.config['categories']):
            if cat_key in categories:
                active_class = 'active' if idx == 0 else ''
                count = len(categories[cat_key])
                html += f'''
                <button class="category-tab {active_class}" onclick="showCategory('{mix_anchor}', '{cat_key}')">
                    {cat_label} <span style="color: #666;">({count})</span>
                </button>'''
        
        html += '''
            </div>'''
        
        # Add category content
        for idx, (cat_key, cat_label) in enumerate(self.config['categories']):
            if cat_key in categories:
                active_class = 'active' if idx == 0 else ''
                html += f'''
            <div class="category-content {active_class}" id="{mix_anchor}-{cat_key}">
                <h3>{cat_label}</h3>'''
                
                # Add error cards (limited)
                records = categories[cat_key][:self.max_per_category]
                for record in records:
                    html += self.generate_error_card(record, data)
                
                if len(categories[cat_key]) > self.max_per_category:
                    html += f'''
                <p style="text-align: center; color: #666; font-style: italic;">
                    Showing {self.max_per_category} of {len(categories[cat_key])} records
                </p>'''
                
                html += '''
            </div>'''
        
        html += '''
        </div>
    </div>'''
        
        return html
    
    def generate_error_card(self, record, data):
        """Generate a single error card"""
        well_id = record.get('well_id', '')
        sample_name = record.get('sample_name', '')
        well_number = record.get('well_number', '')
        error_code = record.get('error_code', '')
        error_message = record.get('error_message', '')
        run_name = record.get('run_name', '')
        category = record.get('clinical_category', 'unresolved')
        
        html = f'''
        <div class="error-card">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>{sample_name}</strong> - Well {well_number}
                    <span class="error-badge {category}">{error_code}</span>
                </div>
            </div>
            <div style="margin-top: 10px; color: #666; font-size: 14px;">
                <div>Run: {run_name}</div>
                <div>Error: {error_message}</div>'''
        
        # Add link to affected samples for control report
        if self.report_type == 'control' and category in ['unresolved', 'test_repeated']:
            affected = data.get('affected_samples', {})
            for group_key, group_data in affected.items():
                if well_id in group_data.get('controls', {}):
                    html += f'''
                <div style="margin-top: 5px;">
                    <a href="#affected-{group_key}" style="color: #2196F3;">
                        → View Affected Samples
                    </a>
                </div>'''
                    break
        
        html += '''
            </div>
        </div>'''
        
        return html
    
    def generate_appendix(self, affected_samples):
        """Generate appendix for affected samples"""
        html = '''
    <div class="mix-section" id="appendix">
        <div class="header" style="margin-top: 40px;">
            <h2>APPENDIX: Affected Patient Samples</h2>
            <p>Patient samples that inherited errors from failed controls</p>
        </div>'''
        
        # ERROR section
        error_samples = []
        for group in affected_samples.values():
            error_samples.extend(group.get('affected_samples_error', {}).values())
        
        html += f'''
        <div class="mix-section">
            <div class="mix-header" onclick="toggleSection('appendix-error')">
                <div>
                    <span style="font-weight: bold;">ERROR - Active Failed Samples</span>
                    <span style="color: #666; margin-left: 10px;">Total: {len(error_samples)}</span>
                </div>
                <span class="expand-icon">▶</span>
            </div>
            <div class="mix-content" id="appendix-error-content">'''
        
        # Group ERROR samples by run/control
        for group_key, group_data in affected_samples.items():
            if group_data['affected_samples_error']:
                html += f'''
                <div id="affected-{group_key}" style="margin-bottom: 20px;">
                    <h4>{group_data['run_name']} - {group_data['control_mix']}</h4>
                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background: #f5f5f5;">
                                <th style="padding: 8px; text-align: left;">Sample Name</th>
                                <th style="padding: 8px; text-align: left;">Well</th>
                                <th style="padding: 8px; text-align: left;">Mix</th>
                                <th style="padding: 8px; text-align: left;">Error</th>
                                <th style="padding: 8px; text-align: left;">LIMS Status</th>
                            </tr>
                        </thead>
                        <tbody>'''
                
                for sample in group_data['affected_samples_error'].values():
                    html += f'''
                            <tr>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['sample_name']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['well_number']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['mix_name']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['error_code']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['lims_status'] or '-'}</td>
                            </tr>'''
                
                html += '''
                        </tbody>
                    </table>
                </div>'''
        
        html += '''
            </div>
        </div>'''
        
        # REPEATS section
        repeat_samples = []
        for group in affected_samples.values():
            repeat_samples.extend(group.get('affected_samples_repeat', {}).values())
        
        html += f'''
        <div class="mix-section">
            <div class="mix-header" onclick="toggleSection('appendix-repeats')">
                <div>
                    <span style="font-weight: bold;">REPEATS - Resolved Samples</span>
                    <span style="color: #666; margin-left: 10px;">Total: {len(repeat_samples)}</span>
                </div>
                <span class="expand-icon">▶</span>
            </div>
            <div class="mix-content" id="appendix-repeats-content">'''
        
        # Group REPEAT samples by run/control
        for group_key, group_data in affected_samples.items():
            if group_data['affected_samples_repeat']:
                html += f'''
                <div style="margin-bottom: 20px;">
                    <h4>{group_data['run_name']} - {group_data['control_mix']}</h4>
                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background: #f5f5f5;">
                                <th style="padding: 8px; text-align: left;">Sample Name</th>
                                <th style="padding: 8px; text-align: left;">Well</th>
                                <th style="padding: 8px; text-align: left;">Mix</th>
                                <th style="padding: 8px; text-align: left;">LIMS Status</th>
                            </tr>
                        </thead>
                        <tbody>'''
                
                for sample in group_data['affected_samples_repeat'].values():
                    html += f'''
                            <tr>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['sample_name']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['well_number']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['mix_name']}</td>
                                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;">{sample['lims_status']}</td>
                            </tr>'''
                
                html += '''
                        </tbody>
                    </table>
                </div>'''
        
        html += '''
            </div>
        </div>
    </div>'''
        
        return html
    
    def generate_javascript(self, data):
        """Generate JavaScript for interactivity"""
        return '''
    <script>
        function toggleSection(mixId) {
            const section = document.getElementById('mix-' + mixId);
            section.classList.toggle('expanded');
        }
        
        function showCategory(mixId, category) {
            // Hide all category contents for this mix
            const contents = document.querySelectorAll(`#mix-${mixId} .category-content`);
            contents.forEach(content => content.classList.remove('active'));
            
            // Remove active from all tabs
            const tabs = document.querySelectorAll(`#mix-${mixId} .category-tab`);
            tabs.forEach(tab => tab.classList.remove('active'));
            
            // Show selected category
            const selectedContent = document.getElementById(`${mixId}-${category}`);
            if (selectedContent) {
                selectedContent.classList.add('active');
            }
            
            // Activate selected tab
            event.target.classList.add('active');
        }
        
        // Expand/Collapse All functionality
        function expandAll() {
            document.querySelectorAll('.mix-section').forEach(section => {
                section.classList.add('expanded');
            });
        }
        
        function collapseAll() {
            document.querySelectorAll('.mix-section').forEach(section => {
                section.classList.remove('expanded');
            });
        }
    </script>'''
    
    def generate_footer(self):
        """Generate HTML footer"""
        return '''
</body>
</html>'''


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Unified Report Generator')
    parser.add_argument('--report-type', 
                       choices=['control', 'sample', 'discrepancy'],
                       required=True,
                       help='Type of report to generate')
    parser.add_argument('--db', 
                       help='Path to database (defaults based on report type)')
    parser.add_argument('--output', 
                       help='Output HTML file path')
    parser.add_argument('--limit', 
                       type=int,
                       help='Limit number of records to process')
    parser.add_argument('--max-per-category', 
                       type=int,
                       default=100,
                       help='Maximum records per category tab (default: 100)')
    parser.add_argument('--include-label-errors', 
                       action='store_true',
                       help='Include label/setup errors (sample report only)')
    parser.add_argument('--exclude-error-type-zero', 
                       action='store_true',
                       default=True,
                       help='Exclude errors with error_type = 0 (default: True)')
    
    args = parser.parse_args()
    
    # Create generator
    generator = UnifiedReportGenerator(
        report_type=args.report_type,
        db_path=args.db,
        output_path=args.output,
        limit=args.limit,
        max_per_category=args.max_per_category,
        include_label_errors=args.include_label_errors,
        exclude_error_type_zero=args.exclude_error_type_zero
    )
    
    # Generate report
    generator.generate_report()


if __name__ == '__main__':
    main()