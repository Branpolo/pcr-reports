 Discrepancy Report Fix - Status Summary

  Problem Statement

  The discrepancy report categorization logic in the multi-database unified report generator is broken. Getting incorrect counts due to flawed categorization logic.
  Also there are concerns that it needs to remain usable across all three databases, not have unique ids etc that apply to only one db.

  File Being Fixed

  Primary file: /home/azureuser/code/wssvc-flow-codex/reports/extract_report_with_curves.py

  Specifically the discrepancy_enrich() function starting at line ~1118.

  Reference SQL Queries (Correct Behavior)

  -- Section 1: acted_upon (~221 records)
  SELECT count(distinct w.id) FROM wells w, observations o
  WHERE o.machine_cls != o.dxai_cls
    AND o.final_cls != o.machine_cls
    AND o.well_id = w.id
    AND w.created_at > '2024-05-31' AND w.created_at < '2025-06-01'
    AND w.role_alias = 'Patient'
    AND w.resolution_codes LIKE '%bla%'
    AND w.lims_status LIKE '%detected%';

  -- Section 2: samples_repeated (~2,589 records)
  SELECT count(distinct w.id) FROM wells w, observations o
  WHERE o.machine_cls != o.dxai_cls
    AND o.well_id = w.id
    AND w.created_at > '2024-05-31' AND w.created_at < '2025-06-01'
    AND w.role_alias = 'Patient'
    AND (w.lims_status NOT LIKE '%detected%'
         OR (w.error_code_id IS NOT NULL
             AND w.error_code_id NOT IN ('937829a3-aa88-44cf-bbd5-deade616cff5',
                                         '995a530f-2239-4007-80f9-4102b5826ee5',
                                         '937829a3-a630-4a86-939d-c2b1ec229c9d',
                                         '995a530f-1da9-457d-9217-5afdac6ca59f',
                                         '98b5395c-97be-4dbd-b185-9a57a25a31ca')));
  **should be updated to include all inherited error codes, not be using ids (so portable for other DBs) & to exclude IC targets**

  -- Section 3: ignored --
   select w.lims_status , w.error_code_id, w.resolution_codes, o.target_id from wells w,  observations o
      where o.machine_cls != o.dxai_cls and o.final_cls = o.machine_cls -- note final and machine match
      and o.well_id =w.id
      and w.created_at > '2024-05-31'
      and w.created_at < '2025-06-01'
      and w.role_alias is not null
      and w.role_alias = 'Patient' and w.resolution_codes like '%bla%' -- have bla
      and w.lims_status like '%detected%' -- have valid lims output non error
  count: 3729

  What We've Fixed (Lines 1174-1210)

  1. ✅ Added resolution_codes extraction (line 1174)

  resolution_codes = (record.get('resolution_codes') or '').upper()

  2. ✅ Fixed Section 1 to check ALL observations (lines 1197-1210)

  Changed from checking only primary_target to checking if ANY non-IC observation has final_cls != machine_cls:
  has_changed_result = any(
      t.get('machine_cls') is not None
      and t.get('final_cls') is not None
      and t.get('machine_cls') != t.get('final_cls')
      for t in non_ic_targets
  )

  if (has_changed_result and has_bla_resolution and has_detected_lims):
      record['clinical_category'] = 'acted_upon'
  **has detected should include %detected% so not detected, hsv detected, etc are all included**

  Result: Section 1 now gets 219/221 records (99% accurate)

  3. ✅ Added control error UUID exclusion set (lines 1131-1139) - **but not exhaustive and also using ids and not the error codes (ie from error_codes table and CSV - CONTROL_AFFECTED_SAMPLE category)**

  control_extraction_error_ids = {
      '937829a3-aa88-44cf-bbd5-deade616cff5',
      '995a530f-2239-4007-80f9-4102b5826ee5',
      '937829a3-a630-4a86-939d-c2b1ec229c9d',
      '995a530f-1da9-457d-9217-5afdac6ca59f',
      '98b5395c-97be-4dbd-b185-9a57a25a31ca'
  }

  4. ✅ Added error_code_id to SQL SELECT (line 1096)

  w.error_code_id,

  What Still Needs Fixing - CRITICAL ISSUE

  ❌ Section 2 Inocrrect Results

  Current vs Expected Counts

  | Category         | Current | Expected | Status           |
  |------------------|---------|----------|------------------|
  | Total            | 9,710   | ~9,757   | ❌ Bad          |
  | acted_upon       | 219     | 221      | ✅ 99% accurate   |
  | samples_repeated | 4,510   | ~2,690    | ❌ 1,970 too many |
  | ignored          | 4,981   | ~3729   | ❌ 1,200 too many  |

  Root cause: The 1,921 extra in samples_repeated are NULL LIMS + control error records being incorrectly categorized.

  Test Commands

  # Regenerate Quest report
  python3 -m reports.extract_report_with_curves combined \
    --db-type qst \
    --db /home/azureuser/code/wssvc-flow/input_data/quest_prod_aug2025.db \
    --output output_data/final/qst_full_csv.json \
    --sample-since-date 2024-06-01 --sample-until-date 2025-05-31 \
    --control-since-date 2024-06-01 --control-until-date 2025-05-31 \
    --discrepancy-since-date 2024-06-01 --discrepancy-until-date 2025-05-31

  # Check category breakdown
  python3 check_disc_cats.py

  # Expected output after fix:
  # acted_upon: ~221
  # samples_repeated: ~2,690
  # ignored: ~3,729
  # Total: ~6,640

  