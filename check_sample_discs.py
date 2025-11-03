import json

d = json.load(open('output_data/final/qst_full_csv.json'))
disc = d.get('reports',{}).get('discrepancy',{})
errs = disc.get('errors',[])

print(f'Total: {len(errs)}\n')
print('First 5 samples:\n')
for i, e in enumerate(errs[:5]):
    print(f'{i+1}. Sample: {e.get("sample_name")}')
    print(f'   Machine cls: {e.get("machine_cls")}')
    print(f'   Final cls: {e.get("final_cls")}')
    print(f'   LIMS status: {e.get("lims_status")}')
    print(f'   Error code: {e.get("error_code")}')
    print(f'   Category: {e.get("clinical_category")}')
    print()
