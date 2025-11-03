 # JSON Extractor Requirements Document

  1. OVERVIEW

  The JSON extractor must produce a single unified JSON output containing data for four report types:
  1. Sample Report - Patient well SOP errors
  2. Control Report - Control well failures and their affected samples
  3. Discrepancy Report - Classification mismatches (machine vs AI vs final)
  4. Summary/Valid Results - Statistics of valid (non-error) results

  2. COMBINED JSON STRUCTURE

  {
    "generated_at": "2024-10-20T11:48:53Z",
    "database": "/path/to/database.db",

    "valid_results": {
      "MIX_NAME": {
        "samples_detected": 1500,
        "samples_not_detected": 2000,
        "controls_passed": 180,
        "controls_total": 200,
        "total_samples": 3500
      }
    },

    "error_statistics": {
      "MIX_NAME": {
        "sop_errors": 150,
        "sop_errors_affected": 100,  // unresolved + test_repeated
        "control_errors": 20,
        "control_errors_affected": 15,
        "samples_affected_by_controls": 250,
        "classification_errors": 300,
        "classification_errors_affected": 200  // acted_upon + samples_repeated
      }
    },

    "reports": {
      "sample": { /* Sample Report Payload */ },
      "control": { /* Control Report Payload */ },
      "discrepancy": { /* Discrepancy Report Payload */ }
    }
  }

  3. INDIVIDUAL REPORT SCHEMAS

  3.1 Sample Report Schema

  {
    "report_type": "sample",
    "generated_at": "2024-10-20T11:48:53Z",
    "database": "/path/to/database.db",
    "since_date": "2024-06-01",
    "until_date": "2025-05-31",
    "date_field": "extraction",
    "include_label_errors": false,

    "summary": {
      "total_errors": 5318,
      "unresolved": 761,
      "error_ignored": 808,
      "test_repeated": 3749
    },

    "errors": [
      {
        "well_id": 12345,
        "sample_name": "SAMPLE-001",
        "well_number": "A01",
        "error_code": "THRESHOLD_WRONG",
        "error_message": "Threshold incorrect",
        "mix_name": "QCMVQ2",
        "run_name": "RUN-2024-001",
        "run_id": 567,
        "lims_status": "DETECTED",
        "resolution_codes": "SKIP",
        "clinical_category": "error_ignored",  // From CSV lookup
        "created_at": "2024-06-15"
      }
    ],

    "well_curves": {
      "12345": {
        "sample_name": "SAMPLE-001",
        "mix_name": "QCMVQ2",
        "targets": [
          {
            "target_name": "QCMV",
            "readings": [1.2, 1.3, 1.4, /* ... 40-45 values */],
            "machine_ct": 28.5,
            "is_passive": false,
            "is_ic": false,
            "control_curves": [
              {
                "readings": [1.1, 1.2, /* ... */],
                "machine_ct": null,
                "control_type": "NC"  // or "PC" or "CTRL"
              }
            ]
          }
        ],
        "comments": [
          {
            "text": "System generated comment",
            "is_system": true,
            "created_at": "2024-06-15T10:30:00Z"
          }
        ]
      }
    }
  }

  3.2 Control Report Schema

  {
    "report_type": "control",
    // ... same metadata fields ...

    "summary": {
      "total_errors": 1423,
      "unresolved": 472,
      "error_ignored": 792,
      "test_repeated": 159,
      "affected_error_count": 8816,
      "affected_repeat_count": 3936
    },

    "errors": [/* Same as sample errors */],

    "well_curves": {
      // Similar to sample, but with different control structure:
      "789": {
        "main_target": "QCMV",
        "targets": {
          "QCMV": {
            "readings": [/* ... */],
            "ct": 28.5,
            "is_ic": false
          }
        },
        "controls": [  // Top-level controls for this well
          {
            "well_id": 999,
            "name": "NC-001",
            "type": "negative",
            "readings": [/* ... */]
          }
        ],
        "comments": [/* ... */]
      }
    },

    "affected_samples": {
      "RUN-001_QCMVQ2": {
        "run_name": "RUN-001",
        "control_mix": "QCMVQ2",
        "controls": {
          "789": {
            "control_name": "NC-001",
            "control_well": "H12",
            "resolution": "RPT"
          }
        },
        "affected_samples_error": {
          "123": {
            "well_id": 123,
            "sample_name": "SAMPLE-002",
            "well_number": "A02",
            "error_code": "INHERITED_CONTROL_FAILURE",
            "error_message": "Control failure inherited",
            "mix_name": "QCMVQ2",
            "run_name": "RUN-001",
            "lims_status": "EXCLUDE",
            "resolution_codes": ""
          }
        },
        "affected_samples_repeat": {
          "456": {/* Same structure, but LIMS status like RPT/REAMP */}
        }
      }
    },

    "affected_counts": {
      "error": 8816,
      "repeat": 3936
    }
  }

  3.3 Discrepancy Report Schema

  {
    "report_type": "discrepancy",
    // ... same metadata fields ...
    "since_date": "2024-01-01",
    "date_field": "upload",  // or "extraction"

    "summary": {
      "total_wells": 9710,
      "unique_samples": 8500,
      "acted_upon": 221,
      "samples_repeated": 2528,
      "ignored": 6961
    },

    "errors": [
      {
        // All standard error fields PLUS:
        "machine_cls": 1,  // 0=NEG, 1=POS
        "dxai_cls": 0,
        "final_cls": 1,
        "machine_ct": 28.5,
        "target_name": "QCMV",  // Primary target
        "clinical_category": "acted_upon",  // or "samples_repeated" or "ignored"
        "category_detail": "result_changed",
        "targets_reviewed": [
          {
            "target_name": "QCMV",
            "machine_cls": 1,
            "dxai_cls": 0,
            "final_cls": 1
          }
        ]
      }
    ],

    "well_curves": {/* Same as sample report */}
  }

  4. KEY EXTRACTION REQUIREMENTS

  4.1 Control Curve Fetching Logic

  For each target in a well:
  1. Try to fetch controls from SAME mix and run
  2. If insufficient controls found:
     a. Extract assay pattern from mix name (e.g., "QCMVQ2" → "CMV")
     b. Find controls from mixes LIKE '%{pattern}%' in same run
     c. Prioritize: 2 NC, then PC to fill max_controls (default: 3)

  4.2 SQL-Based Categorization

  - Categories determined directly in SQL WHERE clauses:
    - Discrepancy: Based on machine_cls/dxai_cls/final_cls comparisons
    - SOP: Based on resolution_codes, lims_status, error_code_id
    - Control: Based on role_alias and error/resolution combinations
  - Database-specific patterns:
    - Valid LIMS: '%detected%' or '%1500%' (varies by database)
    - Control roles: Varies by database (PC/NC for QST, etc.)
    - Error codes to exclude: Control-related errors

  4.3 Date Filtering

  - Support since_date and until_date for all report types
  - Support date_field choice: "extraction" or "upload"
  - Apply to created_at column for samples/controls
  - Apply to specified field for discrepancies

  4.4 Database Support

  - Support three databases: QST, Notts, Vira
  - Each has specific:
    - Control detection patterns
    - LIMS status mappings
    - Category CSV file

  4.5 Comments

  - Fetch system-generated comments only
  - Batch fetch (200 wells at a time)
  - Include up to 3 most recent per well

  5. PROCESSING FLOW

  1. FETCH CATEGORIZED ERRORS (via SQL queries with WHERE clauses)
     - Each query returns pre-categorized records
     - Clinical category determined by SQL logic
     ↓
  2. ENRICH WITH CURVES
     - Fetch targets and readings
     - Fetch control curves with fallback
     ↓
  3. BATCH FETCH COMMENTS
     ↓
  4. CALCULATE SUMMARIES
     - Error counts by category
     - Valid results statistics
     - Error statistics by mix
     ↓
  5. COMBINE INTO FINAL PAYLOAD

  6. REQUIRED FEATURES

  Must Have:

  - ✅ All three report types in combined mode
  - ✅ Control curve fetching with fallback
  - ✅ SQL-based categorization (via WHERE clauses)
  - ✅ Valid results summary
  - ✅ Error statistics by mix
  - ✅ Affected samples tracking
  - ✅ Date range filtering
  - ✅ Multi-database support

  Should Have:

  - ✅ Comment batching for performance
  - ✅ Control caching to avoid duplicate queries
  - ✅ Max records limiting for testing
  - ✅ LIMS status normalization

  Won't Have (handled by generators):

  - ❌ HTML generation
  - ❌ Excel generation
  - ❌ Pie charts
  - ❌ UI interactions

  7. SUCCESS CRITERIA

  The JSON extractor is successful when:
  1. All error records include proper categorization
  2. All well curves include control overlays where available
  3. Valid results match database counts
  4. Affected samples correctly link to failed controls
  5. Date filtering properly includes/excludes records
  6. All three databases produce valid output
