#!/usr/bin/env python3
import json

with open('output_data/test_combined_qst_csv.json') as f:
    data = json.load(f)

print("=== Sample Errors (first 3) ===")
for i, err in enumerate(data['reports']['sample']['errors'][:3], 1):
    print(f"{i}. well_id={err['well_id']}")
    print(f"   error_code={err.get('error_code', 'N/A')}")
    print(f"   resolution={err.get('resolution_codes', 'N/A')}")
    print(f"   lims={err.get('lims_status', 'N/A')}")
    print(f"   clinical_category={err.get('clinical_category', 'N/A')}")
    print()

print("\n=== Control Errors (first 3) ===")
for i, err in enumerate(data['reports']['control']['errors'][:3], 1):
    print(f"{i}. well_id={err['well_id']}")
    print(f"   error_code={err.get('error_code', 'N/A')}")
    print(f"   resolution={err.get('resolution_codes', 'N/A')}")
    print(f"   lims={err.get('lims_status', 'N/A')}")
    print(f"   clinical_category={err.get('clinical_category', 'N/A')}")
    print()
