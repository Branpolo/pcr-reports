#!/usr/bin/env python3
"""
Database configuration system for multi-database reporting

Provides unified interface for QST, Notts, and Vira databases with:
- Database paths
- Category CSV mappings
- Control detection SQL patterns
- LIMS status normalization
- Default parameters
"""

import os

DB_CONFIGS = {
    'qst': {
        'name': 'QST Production',
        'db_path': 'input/quest_prod_aug2025.db',
        'category_csv': 'input/qst_category_mapping_v3.csv',

        # Control detection SQL (injected into WHERE clauses)
        # QST controls have role_alias patterns: PC, NC, %PC, %NC
        'control_where': """
            (w.role_alias LIKE '%PC' OR w.role_alias LIKE '%NC'
             OR w.role_alias LIKE 'PC%' OR w.role_alias LIKE 'NC%')
        """,

        # LIMS status normalization mapping
        # Normalize variant LIMS statuses to canonical values for CSV lookup
        'lims_mapping': {
            'MPX & OPX DETECTED': 'DETECTED',
            'MPX & OPX NOT DETECTED': 'NOT DETECTED',
            'HSV1_DETECTED': 'DETECTED',
            'HSV2_DETECTED': 'DETECTED',
            'HSV_NOT_DETECTED': 'NOT DETECTED',
        },

        # Database schema features
        'has_classifications': True,  # Has pcr_ai_classifications table

        # Default parameters
        'default_since': '2024-06-01',
    },

    'notts': {
        'name': 'Nottingham',
        'db_path': 'input/notts.db',
        'category_csv': 'input/notts_category_mapping_v1.csv',

        # Control detection SQL
        # Notts controls have role_alias patterns: NEG, NTC, QS, NIBSC
        # Use word-boundary patterns to avoid false positives
        # Match: " QS1 |", " QS2 |", "NEG", "NTC", "NIBSC", "Neg", etc.
        'control_where': """
            (w.role_alias LIKE '% NEG%' OR w.role_alias LIKE '% NTC%'
             OR w.role_alias LIKE '% QS%' OR w.role_alias LIKE '% Neg%'
             OR w.role_alias LIKE '% neg%'
             OR w.role_alias = 'NEG' OR w.role_alias = 'NTC'
             OR w.role_alias = 'NIBSC' OR w.role_alias = 'Neg'
             OR w.role_alias = 'neg')
        """,

        # LIMS status normalization mapping
        'lims_mapping': {
            'HSV_1_DETECTED': 'DETECTED',
            'HSV_2_DETECTED': 'DETECTED',
            'HSV_1_2_DETECTED': 'DETECTED',
            'HSV_1_VZV_DETECTED': 'DETECTED',
            'ADENOVIRUS_DETECTED': 'DETECTED',
            'BKV_DETECTED': 'DETECTED',
            'VZV_DETECTED': 'DETECTED',
            '<1500': 'DETECTED',  # Quantified detection
            'Detected <500IU/ml': 'DETECTED',
            'Detected_btw_loq_lod': 'DETECTED',
        },

        'has_classifications': True,
        'default_since': '2024-01-01',
    },

    'vira': {
        'name': 'Vira',
        'db_path': 'input/vira.db',
        'category_csv': 'input/vira_category_mapping_v1.csv',

        # Control detection SQL
        # Vira controls have specific role_alias values
        'control_where': """
            w.role_alias IN ('CC1', 'CC2', 'POS', 'NEC', 'NTC', 'S#')
        """,

        # LIMS status normalization mapping
        'lims_mapping': {
            'DETECTED_QUANT': 'DETECTED',
            'DETECTED_LOQ': 'DETECTED',
            'DETECTED_HIQ': 'DETECTED',
        },

        'has_classifications': True,
        'default_since': '2024-01-01',
    },
}


def get_config(db_type):
    """
    Get configuration for specified database type

    Args:
        db_type (str): Database type ('qst', 'notts', or 'vira')

    Returns:
        dict: Configuration dictionary for the database

    Raises:
        ValueError: If database type is unknown
    """
    db_type = db_type.lower()

    if db_type not in DB_CONFIGS:
        available = ', '.join(DB_CONFIGS.keys())
        raise ValueError(
            f"Unknown database type: '{db_type}'. "
            f"Must be one of: {available}"
        )

    return DB_CONFIGS[db_type]


def list_databases():
    """
    Get list of available database types

    Returns:
        list: List of database type strings
    """
    return list(DB_CONFIGS.keys())


def get_all_configs():
    """
    Get all database configurations

    Returns:
        dict: All database configurations
    """
    return DB_CONFIGS


if __name__ == '__main__':
    # Test/demo the configuration system
    print("Available databases:")
    for db_type in list_databases():
        config = get_config(db_type)
        print(f"\n{db_type.upper()}:")
        print(f"  Name: {config['name']}")
        print(f"  Database: {config['db_path']}")
        print(f"  Category CSV: {config['category_csv']}")
        print(f"  LIMS mappings: {len(config['lims_mapping'])} variants")
        print(f"  Default since: {config['default_since']}")
