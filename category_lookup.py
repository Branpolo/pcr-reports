#!/usr/bin/env python3
"""
CategoryLookup class for CSV-driven well categorization

Provides fast, thread-safe lookups of well categories based on:
- well_type (SAMPLE or CONTROL)
- error_code
- resolution_codes
- lims_status

Features:
- Automatic normalization of empty values ([] -> '')
- LIMS status normalization using database-specific mappings
- Safe defaults for missing patterns
- Logging of missing patterns for debugging
"""

import csv
import logging

logger = logging.getLogger(__name__)


def normalize_empty_value(value):
    """
    Normalize empty value representations

    CSV uses '[]' for empty values, database uses '' or None
    Normalize all to empty string for comparison
    """
    if value in ('[]', None, ''):
        return ''
    return value


class CategoryLookup:
    """
    Fast lookup of well categories from CSV mapping

    Thread-safe, caches all patterns in memory for O(1) lookups
    """

    def __init__(self, csv_path, lims_mapping):
        """
        Initialize category lookup

        Args:
            csv_path (str): Path to category mapping CSV
            lims_mapping (dict): Database-specific LIMS normalization mapping
        """
        self.csv_path = csv_path
        self.lims_mapping = lims_mapping
        self.lookup = {}
        self.missing_patterns = set()  # Track missing patterns for logging
        self._load_csv()

    def _load_csv(self):
        """Load category mappings from CSV"""
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            # Skip header comment lines
            for line in f:
                if not line.startswith('#'):
                    break

            reader = csv.DictReader([line] + f.readlines())

            for row in reader:
                # Normalize empty values ([] -> '')
                key = (
                    row['WELL_TYPE'],
                    normalize_empty_value(row['ERROR_CODE']),
                    normalize_empty_value(row['RESOLUTION_CODES']),
                    normalize_empty_value(row['WELL_LIMS_STATUS'])
                )
                self.lookup[key] = row['CATEGORY']

                # Also add normalized LIMS variant if applicable
                original_lims = normalize_empty_value(row['WELL_LIMS_STATUS'])
                normalized_lims = self.lims_mapping.get(original_lims, original_lims)
                if normalized_lims != original_lims:
                    normalized_key = (
                        row['WELL_TYPE'],
                        normalize_empty_value(row['ERROR_CODE']),
                        normalize_empty_value(row['RESOLUTION_CODES']),
                        normalized_lims
                    )
                    self.lookup[normalized_key] = row['CATEGORY']

        logger.info(f"Loaded {len(self.lookup)} category mappings from {self.csv_path}")

    def get_category(self, well_type, error_code, resolution_codes, lims_status):
        """
        Get category for well pattern

        Args:
            well_type (str): 'SAMPLE' or 'CONTROL'
            error_code (str): Error code or empty string
            resolution_codes (str): Resolution codes or empty string
            lims_status (str): LIMS status or empty string

        Returns:
            str: Category name (e.g., 'SOP_UNRESOLVED', 'VALID_DETECTED')
        """
        # Normalize all inputs
        well_type = well_type or 'SAMPLE'
        error_code = normalize_empty_value(error_code)
        resolution_codes = normalize_empty_value(resolution_codes)
        lims_status = normalize_empty_value(lims_status)

        # Try direct lookup
        key = (well_type, error_code, resolution_codes, lims_status)
        if key in self.lookup:
            return self.lookup[key]

        # Try with normalized LIMS
        normalized_lims = self.lims_mapping.get(lims_status, lims_status)
        if normalized_lims != lims_status:
            normalized_key = (well_type, error_code, resolution_codes, normalized_lims)
            if normalized_key in self.lookup:
                return self.lookup[normalized_key]

        # Pattern not found - log once per unique pattern
        if key not in self.missing_patterns:
            self.missing_patterns.add(key)
            logger.warning(
                f"Missing category for pattern: "
                f"well_type={well_type}, "
                f"error_code='{error_code or '(none)'}', "
                f"resolution='{resolution_codes or '(none)'}', "
                f"lims='{lims_status or '(none)'}'"
            )

        # Return safe default based on error_code presence
        if error_code:
            return 'SOP_UNRESOLVED'  # Has error code -> unresolved error
        else:
            return 'IGNORE_WELL'  # No error code -> ignore

    def get_missing_patterns_count(self):
        """Get count of unique patterns that were missing during lookups"""
        return len(self.missing_patterns)

    def get_missing_patterns(self):
        """Get set of missing patterns for analysis"""
        return self.missing_patterns.copy()


if __name__ == '__main__':
    # Test/demo the CategoryLookup class
    from database_configs import get_config

    print("Testing CategoryLookup class:\n")

    for db_type in ['qst', 'notts', 'vira']:
        config = get_config(db_type)
        print(f"=== {config['name']} ===")

        lookup = CategoryLookup(config['category_csv'], config['lims_mapping'])

        # Test some patterns
        test_patterns = [
            ('SAMPLE', '', '', 'DETECTED'),
            ('SAMPLE', '', '', 'NOT DETECTED'),
            ('CONTROL', '', '', ''),
            ('SAMPLE', 'THRESHOLD_WRONG', '', ''),
            ('SAMPLE', '', 'SKIP', 'DETECTED'),
            ('SAMPLE', '', 'BLA', 'DETECTED'),
        ]

        for well_type, error_code, resolution, lims in test_patterns:
            category = lookup.get_category(well_type, error_code, resolution, lims)
            print(f"  {well_type:8} EC:{error_code or '(none)':20} "
                  f"RES:{resolution or '(none)':10} "
                  f"LIMS:{lims or '(none)':15} → {category}")

        print()
