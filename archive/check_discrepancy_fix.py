#!/usr/bin/env python3
import json

with open('output_data/combined_qst_csv_fixed.json') as f:
    data = json.load(f)

print('Discrepancy summary:', json.dumps(data['reports']['discrepancy']['summary'], indent=2))

print('\nFirst 5 discrepancy errors:')
for i, err in enumerate(data['reports']['discrepancy']['errors'][:5], 1):
    print(f"{i}. well={err['well_id'][:8]}...")
    print(f"   resolution={err.get('resolution_codes')}")
    print(f"   lims={err.get('lims_status')}")
    print(f"   machine_cls={err.get('machine_cls')}, final_cls={err.get('final_cls')}")
    print(f"   category={err.get('clinical_category')}, detail={err.get('category_detail')}")
    print()
