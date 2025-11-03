import json

data = json.load(open('output_data/vira_full_csv.json'))
vr = data.get('valid_results', {})

print('Total samples per mix:')
total_samples = 0
for mix in sorted(vr.keys()):
    ts = vr[mix].get('total_samples', 0)
    total_samples += ts
    print(f'  {mix}: {ts}')
print(f'Grand total samples: {total_samples}\n')

print('Total controls per mix:')
total_controls = 0
for mix in sorted(vr.keys()):
    ct = vr[mix].get('controls_total', 0)
    total_controls += ct
    print(f'  {mix}: {ct}')
print(f'Grand total controls: {total_controls}')
