#!/usr/bin/env python3
"""
Create additional tables for QST controls and other observations
Optimized version with batched inserts and IPC exclusion
"""

import sqlite3
import json
from tqdm import tqdm
import os

def create_tables(conn):
    """Create the qst_controls and qst_other_observations tables"""
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS qst_controls")
    cursor.execute("DROP TABLE IF EXISTS qst_other_observations")
    
    # Create qst_controls table
    print("Creating qst_controls table...")
    readings_columns = ", ".join([f"readings{i} REAL" for i in range(50)])
    
    cursor.execute(f"""
    CREATE TABLE qst_controls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        target_name TEXT,
        role_alias TEXT,
        control_label TEXT,
        well_number TEXT,
        machine_cls INTEGER,
        dxai_cls INTEGER,
        final_cls INTEGER,
        machine_ct REAL,
        dxai_ct REAL,
        {readings_columns},
        mix_name TEXT,
        observation_id TEXT,
        well_id TEXT
    )
    """)
    
    # Create indexes for controls
    cursor.execute("CREATE INDEX idx_controls_run_target ON qst_controls(run_id, target_name)")
    cursor.execute("CREATE INDEX idx_controls_role ON qst_controls(role_alias)")
    
    # Create qst_other_observations table
    print("Creating qst_other_observations table...")
    cursor.execute(f"""
    CREATE TABLE qst_other_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        well_id TEXT,
        sample_label TEXT,
        target_name TEXT,
        machine_cls INTEGER,
        dxai_cls INTEGER,
        final_cls INTEGER,
        machine_ct REAL,
        dxai_ct REAL,
        {readings_columns},
        observation_id TEXT,
        discrepancy_obs_id INTEGER,
        mix_name TEXT
    )
    """)
    
    # Create indexes for other observations
    cursor.execute("CREATE INDEX idx_other_obs_well ON qst_other_observations(well_id)")
    cursor.execute("CREATE INDEX idx_other_obs_discrepancy ON qst_other_observations(discrepancy_obs_id)")
    
    conn.commit()
    print("Tables created successfully")

def populate_controls_batch(discrep_conn, quest_conn):
    """Extract and populate controls data with batched inserts"""
    print("\nPopulating qst_controls table...")
    
    discrep_cursor = discrep_conn.cursor()
    quest_cursor = quest_conn.cursor()
    
    # Get unique run-target combinations, excluding IPC targets
    discrep_cursor.execute("""
    SELECT DISTINCT run_id, target_name, mix_name
    FROM qst_readings
    WHERE in_use = 1
    AND run_id IS NOT NULL
    AND target_name IS NOT NULL
    AND UPPER(target_name) NOT LIKE '%IPC%'
    AND UPPER(mix_name) NOT LIKE '%IPC%'
    """)
    
    combinations = discrep_cursor.fetchall()
    print(f"Found {len(combinations)} unique run-target combinations (excluding IPC)")
    
    batch_size = 100
    batch_data = []
    total_controls = 0
    
    for run_id, target_name, mix_name in tqdm(combinations, desc="Processing run-target combinations"):
        # Query for controls in this run-target combination
        # Exclude IPC roles and patient roles
        quest_cursor.execute("""
        SELECT 
            w.id as well_id,
            w.run_id,
            t.target_name,
            w.role_alias,
            w.sample_label as control_label,
            w.well_number,
            o.machine_cls,
            o.dxai_cls,
            o.final_cls,
            o.machine_ct,
            o.dxai_ct,
            o.readings,
            o.id as observation_id
        FROM wells w
        JOIN observations o ON w.id = o.well_id
        JOIN targets t ON o.target_id = t.id
        WHERE w.run_id = ?
        AND t.target_name = ?
        AND w.role_alias NOT IN ('Patient', '')
        AND w.role_alias IS NOT NULL
        AND w.role_alias NOT LIKE '%ERROR%'
        AND UPPER(w.role_alias) NOT LIKE '%IPC%'
        AND UPPER(t.target_name) NOT LIKE '%IPC%'
        """, (run_id, target_name))
        
        controls = quest_cursor.fetchall()
        
        for control in controls:
            try:
                # Parse readings JSON
                readings_json = control[11]
                readings_list = json.loads(readings_json) if readings_json else []
                
                # Ensure we have exactly 50 readings
                while len(readings_list) < 50:
                    readings_list.append(None)
                readings_list = readings_list[:50]
                
                # Prepare insert values
                values = [
                    control[1],  # run_id
                    control[2],  # target_name
                    control[3],  # role_alias
                    control[4],  # control_label
                    control[5],  # well_number
                    control[6],  # machine_cls
                    control[7],  # dxai_cls
                    control[8],  # final_cls
                    control[9],  # machine_ct
                    control[10], # dxai_ct
                ] + readings_list + [
                    mix_name,    # mix_name
                    control[12], # observation_id
                    control[0]   # well_id
                ]
                
                batch_data.append(values)
                total_controls += 1
                
                # Insert batch when it reaches batch_size
                if len(batch_data) >= batch_size:
                    placeholders = ','.join(['?' for _ in range(63)])  # 10 + 50 + 3
                    insert_query = f"""
                    INSERT INTO qst_controls (
                        run_id, target_name, role_alias, control_label, well_number,
                        machine_cls, dxai_cls, final_cls, machine_ct, dxai_ct,
                        {', '.join([f'readings{i}' for i in range(50)])},
                        mix_name, observation_id, well_id
                    ) VALUES ({placeholders})
                    """
                    
                    discrep_cursor.executemany(insert_query, batch_data)
                    discrep_conn.commit()
                    batch_data = []
                    
            except Exception as e:
                print(f"\nError processing control: {e}")
                continue
    
    # Insert remaining batch
    if batch_data:
        placeholders = ','.join(['?' for _ in range(63)])
        insert_query = f"""
        INSERT INTO qst_controls (
            run_id, target_name, role_alias, control_label, well_number,
            machine_cls, dxai_cls, final_cls, machine_ct, dxai_ct,
            {', '.join([f'readings{i}' for i in range(50)])},
            mix_name, observation_id, well_id
        ) VALUES ({placeholders})
        """
        
        discrep_cursor.executemany(insert_query, batch_data)
        discrep_conn.commit()
    
    print(f"Populated {total_controls} control records")

def populate_other_observations_batch(discrep_conn, quest_conn):
    """Extract and populate other observations with batched inserts"""
    print("\nPopulating qst_other_observations table...")
    
    discrep_cursor = discrep_conn.cursor()
    quest_cursor = quest_conn.cursor()
    
    # Get all discrepancy records, excluding IPC
    discrep_cursor.execute("""
    SELECT id, sample_label, well_number, run_id, mix_name
    FROM qst_readings
    WHERE in_use = 1
    AND sample_label IS NOT NULL
    AND UPPER(target_name) NOT LIKE '%IPC%'
    AND UPPER(mix_name) NOT LIKE '%IPC%'
    """)
    
    discrepancy_records = discrep_cursor.fetchall()
    print(f"Processing {len(discrepancy_records)} discrepancy records (excluding IPC)")
    
    batch_size = 100
    batch_data = []
    total_other_obs = 0
    processed = 0
    
    for disc_id, sample_label, well_number, run_id, mix_name in tqdm(discrepancy_records, desc="Processing wells"):
        processed += 1
        
        # Get well_id first
        quest_cursor.execute("""
        SELECT w.id as well_id
        FROM wells w
        WHERE w.sample_label = ?
        AND w.well_number = ?
        LIMIT 1
        """, (sample_label, well_number))
        
        well_result = quest_cursor.fetchone()
        if not well_result:
            continue
            
        well_id = well_result[0]
        
        # Get all observations in this well (including the main one to find it)
        quest_cursor.execute("""
        SELECT 
            o.id as observation_id,
            t.target_name,
            o.machine_cls,
            o.dxai_cls,
            o.final_cls,
            o.machine_ct,
            o.dxai_ct,
            o.readings
        FROM observations o
        LEFT JOIN targets t ON o.target_id = t.id
        WHERE o.well_id = ?
        """, (well_id,))
        
        all_observations = quest_cursor.fetchall()
        
        # Find the main observation (should match our discrepancy target)
        # We'll exclude it and REFERENCE targets
        for other_obs in all_observations:
            target = other_obs[1]
            
            # Skip REFERENCE and IPC targets
            if target and ('REFERENCE' in target.upper() or 'IPC' in target.upper()):
                continue
                
            try:
                # Parse readings JSON
                readings_json = other_obs[7]
                readings_list = json.loads(readings_json) if readings_json else []
                
                # Ensure we have exactly 50 readings
                while len(readings_list) < 50:
                    readings_list.append(None)
                readings_list = readings_list[:50]
                
                # Prepare insert values
                values = [
                    well_id,
                    sample_label,
                    other_obs[1],  # target_name
                    other_obs[2],  # machine_cls
                    other_obs[3],  # dxai_cls
                    other_obs[4],  # final_cls
                    other_obs[5],  # machine_ct
                    other_obs[6],  # dxai_ct
                ] + readings_list + [
                    other_obs[0],  # observation_id
                    disc_id,       # discrepancy_obs_id
                    mix_name
                ]
                
                batch_data.append(values)
                total_other_obs += 1
                
                # Insert batch when it reaches batch_size
                if len(batch_data) >= batch_size:
                    placeholders = ','.join(['?' for _ in range(61)])  # 8 + 50 + 3
                    insert_query = f"""
                    INSERT INTO qst_other_observations (
                        well_id, sample_label, target_name,
                        machine_cls, dxai_cls, final_cls, machine_ct, dxai_ct,
                        {', '.join([f'readings{i}' for i in range(50)])},
                        observation_id, discrepancy_obs_id, mix_name
                    ) VALUES ({placeholders})
                    """
                    
                    discrep_cursor.executemany(insert_query, batch_data)
                    discrep_conn.commit()
                    batch_data = []
                    
            except Exception as e:
                print(f"\nError processing observation: {e}")
                continue
        
        # Commit periodically
        if processed % 500 == 0:
            discrep_conn.commit()
    
    # Insert remaining batch
    if batch_data:
        placeholders = ','.join(['?' for _ in range(61)])
        insert_query = f"""
        INSERT INTO qst_other_observations (
            well_id, sample_label, target_name,
            machine_cls, dxai_cls, final_cls, machine_ct, dxai_ct,
            {', '.join([f'readings{i}' for i in range(50)])},
            observation_id, discrepancy_obs_id, mix_name
        ) VALUES ({placeholders})
        """
        
        discrep_cursor.executemany(insert_query, batch_data)
        discrep_conn.commit()
    
    print(f"Populated {total_other_obs} other observation records")

def verify_data(conn):
    """Verify the populated data"""
    cursor = conn.cursor()
    
    print("\n=== Data Verification ===")
    
    # Check controls
    cursor.execute("SELECT COUNT(*) FROM qst_controls")
    control_count = cursor.fetchone()[0]
    print(f"Total controls: {control_count}")
    
    cursor.execute("SELECT role_alias, COUNT(*) FROM qst_controls GROUP BY role_alias ORDER BY COUNT(*) DESC LIMIT 10")
    print("\nTop control types:")
    for role, count in cursor.fetchall():
        print(f"  {role}: {count}")
    
    # Check other observations
    cursor.execute("SELECT COUNT(*) FROM qst_other_observations")
    other_count = cursor.fetchone()[0]
    print(f"\nTotal other observations: {other_count}")
    
    cursor.execute("SELECT target_name, COUNT(*) FROM qst_other_observations WHERE target_name IS NOT NULL GROUP BY target_name ORDER BY COUNT(*) DESC LIMIT 10")
    print("\nTop other observation targets:")
    for target, count in cursor.fetchall():
        print(f"  {target}: {count}")
    
    # Check coverage
    cursor.execute("""
    SELECT COUNT(DISTINCT run_id || '-' || target_name) 
    FROM qst_controls
    """)
    covered_combinations = cursor.fetchone()[0]
    print(f"\nRun-target combinations with controls: {covered_combinations}")
    
    cursor.execute("""
    SELECT COUNT(DISTINCT discrepancy_obs_id)
    FROM qst_other_observations
    """)
    covered_discrepancies = cursor.fetchone()[0]
    print(f"Discrepancy records with other observations: {covered_discrepancies}")

def main():
    # Database paths
    discrep_db_path = '/home/azureuser/code/wssvc-flow/qst_discreps.db'
    quest_db_path = '/home/azureuser/code/wssvc-flow/input_data/quest_prod_aug2025.db'
    
    # Connect to databases
    print("Connecting to databases...")
    discrep_conn = sqlite3.connect(discrep_db_path)
    quest_conn = sqlite3.connect(quest_db_path)
    
    try:
        # Create tables
        create_tables(discrep_conn)
        
        # Populate controls with batching
        populate_controls_batch(discrep_conn, quest_conn)
        
        # Populate other observations with batching
        populate_other_observations_batch(discrep_conn, quest_conn)
        
        # Verify data
        verify_data(discrep_conn)
        
    finally:
        discrep_conn.close()
        quest_conn.close()
    
    print("\n=== Processing complete ===")

if __name__ == "__main__":
    main()