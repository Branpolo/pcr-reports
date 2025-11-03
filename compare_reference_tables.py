#!/usr/bin/env python3
"""
Compare error codes, LIMS statuses, and resolution codes across databases
"""
import sqlite3
import json
from collections import defaultdict


def extract_reference_data(db_path, db_name):
    """Extract error codes, LIMS statuses, and resolution codes from a database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    data = {
        'db_name': db_name,
        'db_path': db_path,
        'error_codes': [],
        'lims_statuses': [],
        'resolution_codes': []
    }

    # Extract error codes
    try:
        cursor = conn.execute("""
            SELECT id, error_code, error_message, error_level, error_type, lims_status
            FROM error_codes
            WHERE deleted_at IS NULL
            ORDER BY error_code
        """)
        data['error_codes'] = [dict(row) for row in cursor.fetchall()]
        print(f"  {db_name}: Found {len(data['error_codes'])} error codes")
    except sqlite3.OperationalError as e:
        print(f"  {db_name}: Error reading error_codes: {e}")

    # Extract LIMS statuses
    try:
        cursor = conn.execute("""
            SELECT id, code, message, type, result
            FROM lims_statuses
            ORDER BY code
        """)
        data['lims_statuses'] = [dict(row) for row in cursor.fetchall()]
        print(f"  {db_name}: Found {len(data['lims_statuses'])} LIMS statuses")
    except sqlite3.OperationalError as e:
        print(f"  {db_name}: Error reading lims_statuses: {e}")

    # Extract resolution codes
    try:
        cursor = conn.execute("""
            SELECT id, error_code_id, resolution_code, lims_status,
                   lims_message, resolution_message_id, resolution_dropdown_id,
                   affected_lims_statuses, is_default, level
            FROM resolution_codes
            WHERE deleted_at IS NULL
            ORDER BY resolution_code
        """)
        data['resolution_codes'] = [dict(row) for row in cursor.fetchall()]
        print(f"  {db_name}: Found {len(data['resolution_codes'])} resolution codes")
    except sqlite3.OperationalError as e:
        print(f"  {db_name}: Error reading resolution_codes: {e}")

    conn.close()
    return data


def compare_tables(prod_data, client1_data, client2_data):
    """Compare reference tables across databases"""

    comparison = {
        'error_codes': {},
        'lims_statuses': {},
        'resolution_codes': {}
    }

    for table_name in ['error_codes', 'lims_statuses', 'resolution_codes']:
        print(f"\n=== Comparing {table_name} ===")

        # Create sets based on table type
        if table_name == 'error_codes':
            prod_set = {(row.get('error_code'), row.get('error_message'))
                       for row in prod_data[table_name]}
            client1_set = {(row.get('error_code'), row.get('error_message'))
                          for row in client1_data[table_name]}
            client2_set = {(row.get('error_code'), row.get('error_message'))
                          for row in client2_data[table_name]}
        elif table_name == 'lims_statuses':
            prod_set = {(row.get('code'), row.get('message'))
                       for row in prod_data[table_name]}
            client1_set = {(row.get('code'), row.get('message'))
                          for row in client1_data[table_name]}
            client2_set = {(row.get('code'), row.get('message'))
                          for row in client2_data[table_name]}
        else:  # resolution_codes
            prod_set = {(row.get('resolution_code'), row.get('lims_status'), row.get('lims_message'))
                       for row in prod_data[table_name]}
            client1_set = {(row.get('resolution_code'), row.get('lims_status'), row.get('lims_message'))
                          for row in client1_data[table_name]}
            client2_set = {(row.get('resolution_code'), row.get('lims_status'), row.get('lims_message'))
                          for row in client2_data[table_name]}

        # Find differences
        only_in_prod = prod_set - client1_set - client2_set
        only_in_client1 = client1_set - prod_set
        only_in_client2 = client2_set - prod_set
        common_to_all = prod_set & client1_set & client2_set

        comparison[table_name] = {
            'only_in_prod': list(only_in_prod),
            'only_in_client1': list(only_in_client1),
            'only_in_client2': list(only_in_client2),
            'common_to_all': list(common_to_all),
            'prod_count': len(prod_data[table_name]),
            'client1_count': len(client1_data[table_name]),
            'client2_count': len(client2_data[table_name]),
        }

        print(f"  Production DB: {len(prod_data[table_name])} entries")
        print(f"  Notts DB:      {len(client1_data[table_name])} entries")
        print(f"  Vira DB:       {len(client2_data[table_name])} entries")
        print(f"  Common to all: {len(common_to_all)}")
        print(f"  Only in Prod:  {len(only_in_prod)}")
        print(f"  Only in Notts: {len(only_in_client1)}")
        print(f"  Only in Vira:  {len(only_in_client2)}")

        if only_in_prod:
            print(f"\n  Entries only in Production:")
            for entry in sorted(only_in_prod, key=lambda x: str(x))[:10]:  # Show first 10
                print(f"    {entry}")
            if len(only_in_prod) > 10:
                print(f"    ... and {len(only_in_prod) - 10} more")

        if only_in_client1:
            print(f"\n  Entries only in Notts:")
            for entry in sorted(only_in_client1, key=lambda x: str(x))[:10]:
                print(f"    {entry}")
            if len(only_in_client1) > 10:
                print(f"    ... and {len(only_in_client1) - 10} more")

        if only_in_client2:
            print(f"\n  Entries only in Vira:")
            for entry in sorted(only_in_client2, key=lambda x: str(x))[:10]:
                print(f"    {entry}")
            if len(only_in_client2) > 10:
                print(f"    ... and {len(only_in_client2) - 10} more")

    return comparison


def main():
    # Database paths
    prod_db = '/home/azureuser/code/wssvc-flow/input_data/quest_prod_aug2025.db'
    notts_db = './input/notts.db'
    vira_db = './input/vira.db'

    print("Extracting reference data from databases...\n")

    # Extract data
    prod_data = extract_reference_data(prod_db, 'Production')
    notts_data = extract_reference_data(notts_db, 'Notts')
    vira_data = extract_reference_data(vira_db, 'Vira')

    # Compare
    comparison = compare_tables(prod_data, notts_data, vira_data)

    # Save detailed comparison to JSON
    output = {
        'prod_data': prod_data,
        'notts_data': notts_data,
        'vira_data': vira_data,
        'comparison': comparison
    }

    output_file = 'output_data/reference_tables_comparison.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"\n\nDetailed comparison saved to: {output_file}")


if __name__ == '__main__':
    main()
