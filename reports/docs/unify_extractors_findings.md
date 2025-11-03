# JSON Extractor Unification Findings

> **Status**: ✅ **COMPLETED** - This plan has been implemented in `unified_json_extractor.py`, which is now the active script. The old extractors mentioned in this document have been archived.

## Overview
Reviewed the three primary SQL→JSON extractors that back our HTML reports:

1. `extract_sample_data_with_curves.py`
2. `extract_control_data_with_curves.py`
3. `extract_classification_discrepancies_with_curves.py`

The goal is to reuse shared orchestration (connection setup, batched enrichment, JSON emission) while keeping type-specific logic (SQL, categorisation, auxiliary payloads) isolated via simple configuration objects. Below is a summary of commonalities, key differences, and candidate building blocks for reuse.

## Shared Building Blocks
- **SQLite connection orchestration**: each script establishes a connection, applies light PRAGMAs, and wraps work in `try/finally` with `conn.close()`.
- **Batched well comment lookup**: sample & discrepancy scripts already use identical `get_wells_comments_batch`; control has a nearly identical helper (same SQL, no exception guard).
- **Curve extraction per well**: all scripts gather non-passive targets, attach control curves, and cache repeated lookups, differing mainly in how controls are sourced.
- **JSON envelope**: consistent top-level keys `report_type`, `generated_at`, `database`, `summary`, `errors`, `well_curves` (control also emits `affected_samples`).
- **CLI defaults**: all scripts accept `--db`, `--output`, optional `--limit`/`--test`, and write human-readable progress logs.

## Script-Specific Highlights

### `extract_sample_data_with_curves.py`
- **Domains**: patient wells with predefined error-code lists (`INCLUDED_ERROR_TYPES` plus optional setup errors).
- **Queries**: three SQL blocks (unresolved, resolved, resolved_with_new) sharing similar WHERE clauses; classification discrepancies explicitly excluded.
- **Categorisation**: `clinical_category` values `unresolved`, `error_ignored`, `test_repeated` derived from LIMS status.
- **Extras**: optional `--include-label-errors`, validation printout for expected counts, simpler control selection (mix+target, no backup map).
- **Curves**: `well_curves[well_id]` stores `targets` list including `control_curves` arrays with `'control_type': 'PC'/'NC'`.

### `extract_control_data_with_curves.py`
- **Domains**: non-patient control wells, includes both error wells and linked affected patient wells.
- **Queries**: unresolved/resolved control queries plus two additional queries for affected samples (INHERITED / repeated). Uses more permissive role filters.
- **Categorisation**: `clinical_category` uses resolution code patterns (`RP`, `RX`, `TN`, etc.).
- **Extras**: emits `affected_samples` map with nested control metadata (`affected_samples_error` and `affected_samples_repeat`).
- **Curves**: `get_well_data_with_targets` returns dict with `main_target`; control curves fetched via `get_control_curves_limited(run_id, target)` without backup mix logic.
- **Comments**: same SQL as other scripts, but always executed (no exception guard).

### `extract_classification_discrepancies_with_curves.py`
- **Domains**: patient wells with machine vs DXAI classification mismatches; supports either extraction-date or upload-date filtering via `--date-field`.
- **Queries**: single CTE for eligible discrepancy wells with optional date clause, classification-code heuristics, SKIP/BLA filter.
- **Categorisation**: maps BLA resolution vs final/machine agreement to `acted_upon`, `ignored`, `samples_repeated`; additional override to treat `EXCLUDE` LIMS as repeated.
- **Extras**: advanced control fallback using `backup-controls.csv`, target alias sets, and per-run caching; `--max-controls`, `--since-date`, `--date-field` CLI.
- **Curves**: stores target-level `machine_cls`, `dxai_cls`, `final_cls`, plus `control_curves` with `'control_type': 'PC'/'NC'`.

## Reusable Sources to Generalise
- **Comment batching** (`get_wells_comments_batch` in sample/discrepancy; identical SQL in control).
- **Target extraction**: sample & discrepancy both build list of targets with JSON-decoded readings; control variant could be normalised to same list structure (include `main_target` flag as metadata).
- **Control curve selection**: classification script’s `get_control_curves_for_run` already handles mix/backup logic and could be parameterised (e.g., disable backup for sample/control when unnecessary).
- **Connection + CLI scaffolding**: argument parser shareable with report-type-specific extensions (e.g., optional `--include-label-errors`, `--since-date`).
- **Summary reducers**: each type uses small functions returning dicts. These can be captured as per-type callables without duplicating main routine.

## Proposed Unified Extractor Plan (Pending Approval)
1. **Introduce shared helpers**
   - Create a lightweight module (e.g., `utils/report_extractors.py`) housing reusable pieces: `connect_db`, `fetch_comments_batch`, generic `decode_readings`, and the enhanced `get_control_curves` with knobs for backup usage & max controls.
   - Keep helpers minimal—only functions used by ≥2 report types to respect YAGNI.

2. **Define per-report configurations**
   - For each report type (`sample`, `control`, `discrepancy`), encapsulate:
     - SQL/selection logic (callable returning primary records + any auxiliary datasets like affected samples).
     - Record post-processing (category mapping, extra fields, curve enrichment toggles).
     - Summary reducer and any extra payload (e.g., `affected_samples`).
   - Store these in a dict keyed by report type to keep the main flow generic.

3. **Build unified CLI entry point**
   - New script (e.g., `extract_report_with_curves.py`) accepts `--report-type {sample,control,discrepancy}` plus shared options (`--db`, `--output`, `--limit`, `--max-controls`, `--since-date`, `--date-field`, `--include-label-errors` where applicable).
   - Parse report-specific flags conditionally (e.g., only expose `--include-label-errors` when `report_type=sample`).

4. **Shared processing pipeline**
   - Within `main`, after parsing args:
     1. Connect to DB once.
     2. Invoke the selected report config to fetch records.
     3. Enrich wells with targets, control curves, comments using shared helpers (config can specify extra metadata needed).
     4. Compute summary via config reducer.
     5. Assemble JSON envelope including `report_type` and any type-specific extras.

5. **Validation + parity safeguards**
   - Provide optional `--legacy-output` flag to write side-by-side JSON for quick diff while sunsetting old scripts.
   - Until removal, keep legacy scripts as thin wrappers calling the unified module to ensure consistent behaviour.

6. **Cleanup (post-verification)**
   - Once parity is confirmed, retire legacy scripts or refactor them to import the unified extractor to avoid duplication.

Please review & approve before implementation.

## Unified CLI Usage

- Sample report: `python3 extract_report_with_curves.py sample --db <db> --output <json> [--include-label-errors]`
- Control report: `python3 extract_report_with_curves.py control --db <db> --output <json> [--no-curves]`
- Discrepancy report: `python3 extract_report_with_curves.py discrepancy --db <db> --output <json> [--since-date YYYY-MM-DD] [--date-field upload|extraction]`
- Common flags: `--limit`, `--max-controls`, `--test` (shortcut limits + test output), `--generated-at` (for reproducible diffs), `--legacy-output` (secondary copy), `--compat-mode` (reserved legacy toggle).
