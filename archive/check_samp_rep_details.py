import json
from collections import Counter

d = json.load(open('output_data/final/qst_full_csv.json'))
disc = d.get('reports',{}).get('discrepancy',{})
samp_rep = [e for e in disc.get('errors',[]) if e.get('clinical_category') == 'samples_repeated']

print(f'Total samples_repeated: {len(samp_rep)}\n')

# Check LIMS status distribution
lims_counter = Counter(e.get('lims_status') for e in samp_rep)
print('LIMS status breakdown:')
for lims, count in lims_counter.most_common():
    print(f'  {lims}: {count}')

print(f'\nRecords with DETECTED in LIMS: {sum(1 for e in samp_rep if e.get("lims_status") and "DETECTED" in e.get("lims_status"))}')
print(f'Records with error_code: {sum(1 for e in samp_rep if e.get("error_code"))}')
print(f'Records WITHOUT DETECTED and WITH error: {sum(1 for e in samp_rep if (not e.get("lims_status") or "DETECTED" not in e.get("lims_status")) and e.get("error_code"))}')
print(f'Records WITHOUT DETECTED and NO error: {sum(1 for e in samp_rep if (not e.get("lims_status") or "DETECTED" not in e.get("lims_status")) and not e.get("error_code"))}')
