#!/usr/bin/env python3
"""
Generate a single PCRAI file for testing.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the function from the main script
from extract_non_inverted_sigmoid_proper import get_run_structure
import sqlite3
import json

def main():
    # Connect to database
    quest_conn = sqlite3.connect('input_data/quest_prod_aug2025.db')
    
    # Use a specific run that has both Parvo and HHV6 for a good test
    run_id = '9f256014-d780-4e87-97db-05d79c7988a2'  # RT 27 061225_020679.sds
    
    print(f"Generating PCRAI for run {run_id}...")
    
    # Get run structure
    result = get_run_structure(quest_conn, run_id)
    
    if result is None:
        print("Error: No valid samples found")
        return
    
    pcrai_data, target_type = result
    
    # Create output directory
    os.makedirs('output_data/pcrai_test', exist_ok=True)
    
    # Create filename
    run_name = pcrai_data['name'].replace('.sds', '').replace('_hhv6-parvo', '')
    safe_run_name = run_name.replace('/', '_').replace('\\', '_')
    safe_run_id = ''.join(c for c in run_id if c.isalnum())
    filename = f"{safe_run_name}_{target_type}_{safe_run_id}.pcrai"
    filepath = os.path.join('output_data/pcrai_test', filename)
    
    # Write PCRAI file
    with open(filepath, 'w') as f:
        json.dump(pcrai_data, f, indent=2)
    
    print(f"Created: {filepath}")
    print(f"  - Sample count: {pcrai_data['sample_count']}")
    print(f"  - Mixes: {len(pcrai_data['mixes'])}")
    print(f"  - Target type: {target_type}")
    print(f"  - Parser: {pcrai_data['parser']}")
    print(f"  - Cycle count: {pcrai_data['cycle_count']}")
    
    quest_conn.close()

if __name__ == "__main__":
    main()