# Product Requirements Document (PRD)
# WSSVC-Flow Analysis and Reporting System

## Executive Summary

The WSSVC-Flow Analysis and Reporting System is a comprehensive data analysis platform designed for processing time-series laboratory data, detecting trends using CUSUM algorithms, and generating actionable reports for quality control and decision-making. The system consists of two major components: Mathematical Analysis Tools for curve flattening and slope detection, and Data Integration & Reporting Tools for discrepancy analysis and external data extraction.

## 1. Product Overview

### 1.1 Vision
Provide a robust, automated system for analyzing laboratory fluorescence data to detect downward trends, correct anomalies, and generate comprehensive reports for quality assurance and regulatory compliance.

### 1.2 Scope
- **Mathematical Analysis**: CUSUM-based trend detection and curve flattening algorithms
- **Data Processing**: SQLite database management with batch processing capabilities
- **Visualization**: Interactive HTML reports with SVG-based curve visualizations
- **Integration**: PCRAI file generation for downstream platform compatibility
- **Quality Control**: Discrepancy analysis between machine and final classifications
- **External Data Extraction**: High CT target identification and extraction from production databases

### 1.3 Key Users
- Laboratory technicians performing data analysis
- Quality assurance personnel reviewing classification discrepancies
- Data scientists optimizing detection algorithms
- System administrators managing data pipelines

## 2. Business Context & Problem Statement

### 2.1 Problem Definition
Laboratory fluorescence readings often exhibit downward trends that can lead to false negative results. Manual review of thousands of curves is time-consuming and prone to human error. Additionally, discrepancies between machine classifications and final determinations need systematic tracking and analysis.

### 2.2 Business Objectives
- Automate detection of significant downward trends in fluorescence data
- Reduce false negatives by flattening curves with detected trends
- Provide clear visualization for validation and quality control
- Track and categorize classification discrepancies for process improvement
- Extract high CT value samples for specialized analysis
- Generate standardized PCRAI files for platform interoperability

### 2.3 Success Criteria
- Process 19,000+ records efficiently (current database size)
- Detect and flatten ~55% of curves with significant trends (10,534 of 19,120)
- Generate interactive reports with <5 second load time
- Export PCRAI files compatible with downstream platforms
- Identify 100% of high CT targets above specified thresholds

## 3. System Architecture

### 3.1 Database Architecture

#### Primary Databases
1. **readings.db** (Main Analysis Database)
   - Size: ~19,120 records
   - Tables: readings, flatten, example_ids
   - Purpose: Store original and processed fluorescence data

2. **qst_discreps.db** (Discrepancy Database)
   - Size: ~6,615 records
   - Tables: qst_discrepancies, qst_controls, qst_other_observations
   - Purpose: Track classification discrepancies and control data

3. **quest_prod_aug2025.db** (Production Database)
   - Location: input_data/quest_prod_aug2025.db
   - Purpose: External data source for high CT extraction and control curves
   - Contains: Production run data, control wells, observation results

### 3.2 Processing Pipeline

```
Input Data → Database Import → CUSUM Analysis → Curve Flattening → Visualization/Export
                                       ↓
                            Discrepancy Analysis → Interactive Reports
                                       ↓
                            High CT Extraction → PCRAI Generation
```

### 3.3 Technology Stack
- **Language**: Python 3.x
- **Database**: SQLite3
- **Visualization**: HTML5, SVG, JavaScript
- **Libraries**: NumPy, SciPy, tqdm, argparse
- **Output Formats**: HTML, PCRAI (JSON), CSV

## 4. Functional Requirements

### 4.1 Part 1: Mathematical Analysis Tools

#### 4.1.1 CUSUM Analysis Engine
**Component**: `apply_corrected_cusum_all.py`

**Requirements**:
- FR-1.1: Convert raw readings to SVG coordinate system (0-400 pixel range)
- FR-1.2: Apply data inversion using max(y_vals) - y_vals formula
- FR-1.3: Smooth data using 5-point rolling window
- FR-1.4: Compute negative CUSUM with configurable k parameter (default: 0.0)
- FR-1.5: Store CUSUM values for all 44 reading cycles in database
- FR-1.6: Calculate and store minimum CUSUM value for trend detection

**Algorithm Specifications**:
```python
CUSUM[0] = 0
CUSUM[i] = min(0, CUSUM[i-1] + (y[i] - y[i-1] - k))
```

#### 4.1.2 Curve Flattening System
**Component**: `create_flattened_database_fast.py`

**Requirements**:
- FR-2.1: Identify curves with CUSUM minimum ≤ threshold (default: -80)
- FR-2.2: Determine flattening point at CUSUM minimum index
- FR-2.3: Calculate target value from readings after minimum point
- FR-2.4: Apply flattening with ±2% random noise for realism
- FR-2.5: Create/update flatten table with modified readings
- FR-2.6: Support sanity checks to prevent false positives
  - Slope-based check: Compare minimum point with early cycle average
  - Line of Best Fit (LOB): Calculate gradient to verify downward trend

**Flattening Logic**:
- Target value = average of readings from (min_index + 5) onwards
- Flattened readings[0:min_index] = target ± random(0.02)
- Original readings preserved from min_index onwards

#### 4.1.3 Visualization Generator
**Component**: `generate_flattened_cusum_html.py`

**Requirements**:
- FR-3.1: Generate HTML reports with SVG curve visualizations
- FR-3.2: Display original (blue), flattened (green), and CUSUM (red) curves
- FR-3.3: Support multiple dataset options (all, example, specific IDs)
- FR-3.4: Enable parameter testing with different k values
- FR-3.5: Sort results by CUSUM value or record ID
- FR-3.6: Show curve metadata (ID, filename, target, mix, well position)
- FR-3.7: Implement "only-failed" filtering for specific analysis

**Visualization Features**:
- 800x400 pixel SVG graphs
- Dual Y-axis (readings and CUSUM values)
- Grid lines for reference
- Interactive tooltips with exact values
- Threshold line visualization

#### 4.1.4 Parameter Comparison Tool
**Component**: `compare_k_parameters.py`

**Requirements**:
- FR-4.1: Compare two different k tolerance values
- FR-4.2: Identify curves where flattening decision changes
- FR-4.3: Support CUSUM vs derivative analysis comparison
- FR-4.4: Display side-by-side analysis values
- FR-4.5: Color-code changes (red: lost flattening, green: gained)
- FR-4.6: Generate comparison HTML reports

#### 4.1.5 PCRAI Export System
**Component**: `generate_pcrai_from_db.py`

**Requirements**:
- FR-5.1: Export database records to PCRAI JSON format
- FR-5.2: Process by filename (one PCRAI per unique filename)
- FR-5.3: Detect mix configurations dynamically
- FR-5.4: Include flattened readings when available
- FR-5.5: Map target names to standardized format (CMV, HSV1, etc.)
- FR-5.6: Generate ~174MB of PCRAI data for 100+ files

**PCRAI Structure**:
```json
{
  "filename": "exp4E042354",
  "mixes": ["Mix1", "Mix2"],
  "wells": {
    "A1": {
      "target": "CMV",
      "readings": [7.1, 7.2, ...],
      "flattened": [7.5, 7.5, ...]
    }
  }
}
```

#### 4.1.6 Example Dataset Manager
**Component**: `manage_example_ids.py`

**Requirements**:
- FR-6.1: Maintain curated example dataset (34 IDs)
- FR-6.2: Add/remove IDs with validation
- FR-6.3: List current example IDs
- FR-6.4: Verify ID existence in readings table
- FR-6.5: Store in example_ids table

### 4.2 Part 2: Data Integration & Reporting Tools

#### 4.2.1 QST Data Import
**Component**: `import_qst_data.py`

**Requirements**:
- FR-7.1: Import CSV data with 50 reading columns
- FR-7.2: Parse JSON readings into individual columns
- FR-7.3: Handle duplicate columns and missing values
- FR-7.4: Create indexed database for efficient queries
- FR-7.5: Support database reset option
- FR-7.6: Process 6,615 discrepancy records

#### 4.2.2 Interactive Discrepancy Reports
**Component**: `generate_qst_report_interactive_v2.py`

**Requirements**:
- FR-8.1: Generate JavaScript-enabled interactive HTML reports
- FR-8.2: Implement 4 viewing modes:
  - "With Controls": Show target curve with control curves
  - "With Other Well Curves": Show target with other observations
  - "All": Display all curves together
  - "None": Hide all curves
- FR-8.3: Dynamic Y-axis rescaling based on visible curves
- FR-8.4: Per-target toggle controls without scrolling
- FR-8.5: Table of Contents with clickable navigation
- FR-8.6: Display extraction date (DD-Mon-YYYY format)
- FR-8.7: Smart field suppression (hide "None" values)
- FR-8.8: Color coding by discrepancy type:
  - Green: False Negative (Machine ≠ Final & Final = 1)
  - Red: False Positive (Machine ≠ Final & Final = 0)
  - Yellow: LIMS status issues
  - Pink: Error codes present

#### 4.2.3 Sectioned Analysis Reports
**Component**: `generate_qst_report_sections.py`

**Requirements**:
- FR-9.1: Categorize discrepancies into three sections:
  - Discrepancies Acted Upon (result changed)
  - Samples Repeated (error codes/unusual LIMS)
  - Discrepancies Ignored (result kept)
- FR-9.2: Grid layout with 5 columns
- FR-9.3: Mix and target headers for organization
- FR-9.4: SVG curve visualization for each record
- FR-9.5: Display metadata (CT values, classifications, LIMS status)

#### 4.2.4 Control Data Extraction
**Component**: `create_qst_additional_tables_optimized.py`

**Requirements**:
- FR-10.1: Extract control curves from Quest production database
- FR-10.2: Create qst_controls table (8,550+ records)
- FR-10.3: Create qst_other_observations table (5,400+ records)
- FR-10.4: Exclude IPC-related data
- FR-10.5: Use batched inserts for performance
- FR-10.6: Process ~14,000 total records efficiently

#### 4.2.5 High CT Target Extraction
**Component**: `extract_high_ct_targets.py`

**Requirements**:
- FR-11.1: Extract Parvo and HHV6 results with CT > threshold
- FR-11.2: Support all target variations (PARVO, PARVOQ, QPARVOQ, HHV6, HHV-6)
- FR-11.3: Generate one PCRAI file per run with all samples
- FR-11.4: Include all control wells from identified runs
- FR-11.5: Create HTML report with standard template
- FR-11.6: Group samples by target within runs
- FR-11.7: Color code by target type (pink for Parvo, teal for HHV6)
- FR-11.8: Generate download links for PCRAI files

**High CT PCRAI Structure**:
```json
{
  "run_name": "RT10 101124_20009.sds",
  "run_id": "9d7d3c24-d81f-4962-994b-ed3a55782e0b",
  "run_date": "2024-11-14 19:20:55",
  "high_ct_samples": [...],
  "controls": [...]
}
```

## 5. Technical Requirements

### 5.1 Performance Requirements
- TR-1: Process 19,120 records in < 2 minutes for CUSUM calculation
- TR-2: Generate HTML reports with 1,000 curves in < 10 seconds
- TR-3: Export 100 PCRAI files in < 30 seconds
- TR-4: Support batch operations for 10,000+ record updates
- TR-5: Handle SQLite databases up to 500MB

### 5.2 Data Requirements
- TR-6: Support 44-50 reading cycles per record
- TR-7: Handle floating-point precision for CT values
- TR-8: Maintain data integrity during flattening operations
- TR-9: Preserve original data alongside processed results
- TR-10: Support NULL values in reading columns

### 5.3 Scalability Requirements
- TR-11: Linear performance scaling up to 100,000 records
- TR-12: Support concurrent read operations
- TR-13: Efficient memory usage (< 2GB for full processing)
- TR-14: Incremental processing capability

### 5.4 Compatibility Requirements
- TR-15: Python 3.x compatibility
- TR-16: Cross-platform operation (Linux/macOS/Windows)
- TR-17: PCRAI format compatibility with downstream platforms
- TR-18: HTML5/CSS3 standard compliance
- TR-19: SQLite3 database format

## 6. Parameter Configuration

### 6.1 Standard Parameters
All tools support a standardized parameter set for consistency:

#### Core Parameters
- `--db`: Database path (default: readings.db)
- `--output`: Output directory/file path
- `--ids`: Specific record IDs (comma-separated)
- `--example-dataset`: Use curated example set
- `--all`: Process all records
- `--limit`: Limit number of records

#### Algorithm Parameters
- `--k`: CUSUM tolerance (0.0-1.0, default: 0.0)
- `--threshold`: Flattening threshold (default: -80)
- `--cusum-limit`: Alias for threshold
- `--sanity-check-slope`: Enable sanity checking
- `--sanity-lob`: Use Line of Best Fit checking

#### Display Parameters
- `--sort-by`: Sort criteria (cusum/id/db-cusum)
- `--sort-order`: Sort direction (up/down)
- `--only-failed`: Filter to specific failures

### 6.2 Recommended Parameter Values
- **K Parameter**: 0.1-0.3 for typical analysis
- **Threshold**: -80 (standard), -100 (selective), -50 (aggressive)
- **Window Size**: 5 points for smoothing
- **CT Threshold**: 33 for high CT extraction

## 7. User Interface Requirements

### 7.1 HTML Report Requirements
- UI-1: Responsive design for various screen sizes
- UI-2: Clear visual hierarchy with headers and sections
- UI-3: Color-coded status indicators
- UI-4: Interactive controls for curve toggling
- UI-5: Clickable navigation elements
- UI-6: Download links for PCRAI files

### 7.2 Command Line Interface
- UI-7: Clear help documentation (--help)
- UI-8: Progress bars for long operations
- UI-9: Informative error messages
- UI-10: Confirmation prompts for destructive operations

### 7.3 Visualization Standards
- UI-11: Consistent color scheme across reports
- UI-12: SVG graphs with 800x400 pixel standard
- UI-13: Dual-axis support for different scales
- UI-14: Grid lines and reference markers
- UI-15: Legend for curve identification

## 8. Data Flow & Workflows

### 8.1 Standard Analysis Workflow
1. Import raw data → `readings.db`
2. Apply CUSUM analysis → `apply_corrected_cusum_all.py`
3. Create flattened database → `create_flattened_database_fast.py`
4. Generate visualizations → `generate_flattened_cusum_html.py`
5. Export PCRAI files → `generate_pcrai_from_db.py`

### 8.2 Discrepancy Analysis Workflow
1. Import QST CSV → `import_qst_data.py`
2. Extract control data → `create_qst_additional_tables_optimized.py`
3. Generate interactive reports → `generate_qst_report_interactive_v2.py`
4. Create sectioned analysis → `generate_qst_report_sections.py`

### 8.3 High CT Extraction Workflow
1. Connect to Quest production database
2. Query for high CT targets → `extract_high_ct_targets.py`
3. Generate PCRAI files per run
4. Create HTML summary report
5. Provide download links

## 9. Success Metrics

### 9.1 Accuracy Metrics
- Detection rate of true downward trends: > 95%
- False positive rate for flattening: < 5%
- Correct PCRAI file generation: 100%

### 9.2 Performance Metrics
- Average processing time per 1000 records: < 10 seconds
- HTML report generation time: < 5 seconds
- Database query response time: < 100ms

### 9.3 Quality Metrics
- Data integrity after processing: 100%
- Successful PCRAI imports downstream: > 99%
- User-reported issues per month: < 5

### 9.4 Usage Metrics
- Daily active users
- Records processed per day
- Reports generated per week
- PCRAI files exported per month

## 10. Future Considerations

### 10.1 Planned Enhancements
- Machine learning integration for trend detection
- Real-time processing capabilities
- Multi-user concurrent access
- Cloud database support
- Advanced statistical analysis options
- Automated parameter optimization
- API endpoints for external integration

### 10.2 Potential Integrations
- LIMS system direct connection
- Automated email report distribution
- Dashboard for monitoring metrics
- Version control for processed data
- Audit trail functionality

### 10.3 Scalability Improvements
- Distributed processing for large datasets
- Caching layer for frequently accessed data
- Asynchronous report generation
- Streaming data processing
- Database partitioning strategies

## 11. Risk Mitigation

### 11.1 Data Integrity Risks
- **Risk**: Data corruption during flattening
- **Mitigation**: Maintain separate flatten table, preserve originals

### 11.2 Performance Risks
- **Risk**: Slow processing for large datasets
- **Mitigation**: Batch processing, indexed databases, optimized queries

### 11.3 Compatibility Risks
- **Risk**: PCRAI format changes
- **Mitigation**: Versioned export formats, validation tests

### 11.4 User Error Risks
- **Risk**: Incorrect parameter usage
- **Mitigation**: Default safe values, validation checks, clear documentation

## 12. Appendices

### Appendix A: Database Schemas

#### readings table
- id (INTEGER PRIMARY KEY)
- filename (TEXT)
- mix_name (TEXT)
- target_name (TEXT)
- well_position (TEXT)
- readings0-43 (REAL)
- cusum0-43 (REAL)
- cusum_min_correct (REAL)
- in_use (INTEGER)

#### qst_discrepancies table
- id (INTEGER PRIMARY KEY)
- run_id (TEXT)
- mix_name (TEXT)
- target_name (TEXT)
- well_position (TEXT)
- sample_name (TEXT)
- machine_ct (REAL)
- final_ct (REAL)
- machine_cls (INTEGER)
- final_cls (INTEGER)
- dxai_ct (REAL)
- dxai_cls (INTEGER)
- lims_status (TEXT)
- error_code (TEXT)
- resolution_code (TEXT)
- extraction_date (TEXT)
- readings0-49 (REAL)

### Appendix B: File Structure
```
/home/azureuser/code/wssvc-flow/
├── Core Scripts (Part 1: Mathematical Analysis)
│   ├── apply_corrected_cusum_all.py
│   ├── create_flattened_database_fast.py
│   ├── generate_flattened_cusum_html.py
│   ├── generate_database_flattened_html_fixed.py
│   ├── generate_pcrai_from_db.py
│   ├── compare_k_parameters.py
│   └── manage_example_ids.py
│
├── Integration Scripts (Part 2: Reporting)
│   ├── import_qst_data.py
│   ├── generate_qst_report_interactive_v2.py
│   ├── generate_qst_report_sections.py
│   ├── create_qst_additional_tables_optimized.py
│   └── extract_high_ct_targets.py
│
├── Databases
│   ├── readings.db (19,120 records)
│   ├── qst_discreps.db (6,615 records)
│   └── input_data/quest_prod_aug2025.db (production)
│
├── Output Directories
│   ├── output_data/ (HTML reports, PCRAI files)
│   ├── plots/ (SVG visualizations)
│   └── feedback_plots/ (example references)
│
└── Documentation
    ├── README.md
    ├── CLAUDE.md
    ├── PRD.md (this document)
    └── flags_table.md
```

### Appendix C: Algorithm Specifications

#### CUSUM Algorithm
```
1. Scale readings to SVG coordinates (0-400 range)
2. Invert: y_inv = max(y) - y
3. Smooth with 5-point window
4. Calculate CUSUM:
   S[0] = 0
   S[i] = min(0, S[i-1] + (y[i] - y[i-1] - k))
5. Find minimum CUSUM value
6. If min <= threshold, flatten before minimum index
```

#### Flattening Algorithm
```
1. Identify CUSUM minimum index
2. Calculate target = average(readings[min_index+5:])
3. For i in [0, min_index):
   flattened[i] = target * random(0.98, 1.02)
4. For i >= min_index:
   flattened[i] = original[i]
```

---

*Document Version: 1.0*
*Last Updated: 2025-09-04*
*Status: Latest v0.5*