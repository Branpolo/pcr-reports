import json
from collections import Counter

d = json.load(open('output_data/final/qst_full_csv.json'))
disc = d.get('reports',{}).get('discrepancy',{})
cats = [e.get('clinical_category') for e in disc.get('errors',[])]
c = Counter(cats)

print('Category breakdown:')
for cat in ['acted_upon', 'samples_repeated', 'ignored', None]:
    if cat in c:
        print(f'  {cat}: {c[cat]}')
print(f'\nTotal: {len(cats)}')
