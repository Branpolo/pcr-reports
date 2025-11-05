import json

d = json.load(open('output_data/final/qst_full_csv.json'))
disc = d.get('reports',{}).get('discrepancy',{})
none_cats = [e for e in disc.get('errors',[]) if e.get('clinical_category') is None]

print(f'Total None category: {len(none_cats)}')
print('\nFirst 3 samples:')
for i, e in enumerate(none_cats[:3]):
    print(f'\n{i+1}. Sample: {e.get("sample_name")}')
    print(f'   Machine cls: {e.get("machine_cls")}')
    print(f'   Final cls: {e.get("final_cls")}')
    print(f'   LIMS status: {e.get("lims_status")}')
    print(f'   Error code: {e.get("error_code")}')
    print(f'   Mix: {e.get("mix_name")}')
