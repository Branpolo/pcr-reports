#!/usr/bin/env python3
import json

databases = [
    ('QST', 'output_data/test_combined_qst_csv.json'),
    ('Notts', 'output_data/test_combined_notts_csv.json'),
    ('Vira', 'output_data/test_combined_vira_csv.json'),
]

for db_name, filepath in databases:
    with open(filepath) as f:
        data = json.load(f)

    print(f"\n{'='*60}")
    print(f"{db_name} Database Test Results")
    print(f"{'='*60}")

    print(f"\nSample Report Summary:")
    sample_summary = data['reports']['sample']['summary']
    for key, val in sample_summary.items():
        print(f"  {key}: {val}")

    print(f"\nControl Report Summary:")
    control_summary = data['reports']['control']['summary']
    for key, val in control_summary.items():
        print(f"  {key}: {val}")

    print(f"\nDiscrepancy Report Summary:")
    discrep_summary = data['reports']['discrepancy']['summary']
    for key, val in discrep_summary.items():
        print(f"  {key}: {val}")

    print(f"\nValid Results:")
    print(f"  Total mixes: {len(data['valid_results'])}")
    total_samples = sum(v['samples_detected'] + v['samples_not_detected']
                       for v in data['valid_results'].values())
    total_controls = sum(v['controls_total'] for v in data['valid_results'].values())
    print(f"  Total valid samples: {total_samples}")
    print(f"  Total controls: {total_controls}")
