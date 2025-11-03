#!/usr/bin/env python3
"""
Import QST discrepancies data from CSV to SQLite database
Following DRY and YAGNI principles - only what's needed, reusing where possible
"""

import sqlite3
import pandas as pd
import json
import argparse
from tqdm import tqdm
import os

def create_database_schema(conn):
    """Create the QST readings table with 50 reading columns"""
    cursor = conn.cursor()
    
    # Build the readings columns part of the schema
    readings_columns = ", ".join([f"readings{i} REAL" for i in range(50)])
    
    schema = f"""
    CREATE TABLE IF NOT EXISTS qst_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sample_label TEXT,
        well_number TEXT,
        lims_status TEXT,
        error_code TEXT,
        error_message TEXT,
        resolution_codes TEXT,
        exclude INTEGER,
        extraction_date TEXT,
        machine_cls INTEGER,
        dxai_cls INTEGER,
        final_cls INTEGER,
        manual_cls INTEGER,
        machine_ct REAL,
        dxai_ct REAL,
        {readings_columns},
        target_name TEXT,
        mix_name TEXT,
        run_id TEXT,
        run_name TEXT,
        in_use INTEGER DEFAULT 1
    )
    """
    
    cursor.execute(schema)
    conn.commit()
    print("Database schema created successfully")

def import_csv_to_db(csv_path, db_path):
    """Import QST CSV data into SQLite database"""
    
    # Read CSV
    print(f"Reading CSV from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} records")
    
    # Handle duplicate manual_cls column - keep only first
    if 'manual_cls.1' in df.columns:
        df = df.drop('manual_cls.1', axis=1)
        print("Dropped duplicate manual_cls column")
    
    # Create/connect to database
    conn = sqlite3.connect(db_path)
    create_database_schema(conn)
    cursor = conn.cursor()
    
    # Process each row
    print("Importing data...")
    successful = 0
    failed = 0
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Importing records"):
        try:
            # Parse readings from JSON string
            readings_json = row['readings']
            readings_list = json.loads(readings_json)
            
            # Ensure we have exactly 50 readings (pad with None if needed)
            while len(readings_list) < 50:
                readings_list.append(None)
            readings_list = readings_list[:50]  # Truncate if more than 50
            
            # Prepare values for insertion
            values = [
                row['sample_label'],
                row['well_number'],
                row['lims_status'] if pd.notna(row['lims_status']) else None,
                row['error_code'] if pd.notna(row['error_code']) else None,
                row['error_message'] if pd.notna(row['error_message']) else None,
                row['resolution_codes'],
                int(row['exclude']),
                row['extraction_date'],
                int(row['machine_cls']),
                int(row['dxai_cls']),
                int(row['final_cls']),
                int(row['manual_cls']) if pd.notna(row['manual_cls']) else None,
                float(row['machine_ct']) if pd.notna(row['machine_ct']) else None,
                float(row['dxai_ct']) if pd.notna(row['dxai_ct']) else None,
            ]
            
            # Add the 50 readings
            values.extend(readings_list)
            
            # Add remaining fields
            values.extend([
                row['target_name'],
                row['mix_name'],
                row['run_id'],
                row['run_name'],
                1  # in_use = 1
            ])
            
            # Build insert query
            placeholders = ','.join(['?' for _ in range(69)])  # 14 + 50 + 5 fields
            insert_query = f"""
                INSERT INTO qst_readings (
                    sample_label, well_number, lims_status, error_code, error_message,
                    resolution_codes, exclude, extraction_date, machine_cls, dxai_cls,
                    final_cls, manual_cls, machine_ct, dxai_ct,
                    {', '.join([f'readings{i}' for i in range(50)])},
                    target_name, mix_name, run_id, run_name, in_use
                ) VALUES ({placeholders})
            """
            
            cursor.execute(insert_query, values)
            successful += 1
            
        except Exception as e:
            print(f"\nError on row {idx}: {e}")
            print(f"Sample: {row['sample_label']}")
            failed += 1
            continue
    
    # Commit all changes
    conn.commit()
    
    # Create indices for common queries
    print("\nCreating indices...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sample_label ON qst_readings(sample_label)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_well_number ON qst_readings(well_number)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_final_cls ON qst_readings(final_cls)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_in_use ON qst_readings(in_use)")
    conn.commit()
    
    # Print summary
    print(f"\nImport complete!")
    print(f"Successfully imported: {successful} records")
    print(f"Failed: {failed} records")
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM qst_readings")
    total_in_db = cursor.fetchone()[0]
    print(f"Total records in database: {total_in_db}")
    
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='Import QST discrepancies data to SQLite')
    parser.add_argument('--csv', type=str, 
                       default='input_data/qst_prod-discreps-newcolumns.csv',
                       help='Path to CSV file')
    parser.add_argument('--db', type=str,
                       default='qst_discreps.db',
                       help='Path to output database')
    parser.add_argument('--reset', action='store_true',
                       help='Delete existing database before import')
    
    args = parser.parse_args()
    
    # Check if CSV exists
    if not os.path.exists(args.csv):
        print(f"Error: CSV file not found: {args.csv}")
        return
    
    # Handle reset
    if args.reset and os.path.exists(args.db):
        os.remove(args.db)
        print(f"Deleted existing database: {args.db}")
    
    # Import data
    import_csv_to_db(args.csv, args.db)

if __name__ == "__main__":
    main()