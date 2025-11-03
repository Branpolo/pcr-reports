#!/usr/bin/env python3

import sqlite3
import numpy as np
import csv
import argparse
import os
from tqdm import tqdm

def compute_negative_cusum(y_vals, k=0.0):
    """Compute negative CUSUM with adjustable k parameter - matches original algorithm"""
    cusum = [0]
    for i in range(1, len(y_vals)):
        diff = y_vals[i] - y_vals[i - 1]
        s = min(0, cusum[-1] + (diff - k))
        cusum.append(s)
    return cusum

def smooth_curve(y_vals, window_size=5):
    """Apply smoothing to curve data"""
    if len(y_vals) < window_size:
        return y_vals
    
    smoothed = []
    half_window = window_size // 2
    
    for i in range(len(y_vals)):
        start = max(0, i - half_window)
        end = min(len(y_vals), i + half_window + 1)
        smoothed.append(sum(y_vals[start:end]) / (end - start))
    
    return smoothed

def apply_corrected_cusum_algorithm(readings, k=0.0):
    """Apply the corrected CUSUM algorithm with adjustable k parameter"""
    margin = 50
    plot_width = 800 - 2 * margin
    plot_height = 400 - 2 * margin
    
    min_reading = min(readings)
    max_reading = max(readings)
    reading_range = max_reading - min_reading if max_reading != min_reading else 1
    
    # Convert to SVG coordinates (same as original algorithm)
    svg_y_vals = []
    for reading in readings:
        svg_y = margin + plot_height - (plot_height * (reading - min_reading) / reading_range)
        svg_y_vals.append(svg_y)
    
    svg_y_vals = np.array(svg_y_vals)
    
    # Apply simple inversion
    y_inv = np.max(svg_y_vals) - svg_y_vals
    
    # Smooth the inverted data
    y_smooth = smooth_curve(y_inv)
    
    # Apply CUSUM with custom k parameter
    cusum = compute_negative_cusum(y_smooth, k=k)
    
    return cusum, min(cusum)

def get_readings_for_id(conn, target_id):
    """Get readings for a specific ID"""
    cursor = conn.cursor()
    readings_columns = [f"readings{i}" for i in range(44)]
    readings_select = ", ".join(readings_columns)
    cursor.execute(f"SELECT {readings_select} FROM readings WHERE id = ?", (target_id,))
    row = cursor.fetchone()
    if not row:
        return []
    readings = [r for r in row if r is not None]
    return readings

def create_flattened_readings(original_readings, cusum_values, cusum_min, threshold=-80):
    """Create flattened readings for curves with significant downward trends"""
    
    # Only flatten if CUSUM min <= threshold
    if cusum_min > threshold:
        return None
    
    # Find the index where CUSUM reaches minimum (end of downward slope)
    min_val = min(cusum_values)
    min_index = cusum_values.index(min_val)
    
    # Skip if minimum occurs too early (nothing to flatten)
    if min_index <= 1:
        return None
    
    # Get the target reading value (at the minimum CUSUM point)
    target_reading = original_readings[min_index]
    
    # Determine appropriate noise scale based on data
    reading_std = np.std(original_readings)
    noise_scale = reading_std * 0.001  # Very small noise, 0.1% of standard deviation
    
    # Create flattened readings
    flattened = original_readings.copy()
    
    # Flatten all readings before the minimum point
    for i in range(min_index):
        # Add small random noise to prevent identical values
        noise = np.random.uniform(-noise_scale, noise_scale)
        flattened[i] = target_reading + noise
    
    return flattened

def get_example_ids(conn):
    """Get example IDs from database"""
    cursor = conn.cursor()
    
    # First check if example_ids table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='example_ids'")
    if not cursor.fetchone():
        print("Creating example_ids table...")
        cursor.execute("CREATE TABLE example_ids (id INTEGER PRIMARY KEY)")
        
        # Insert the example IDs from feedback plots + 2112 (key test case)
        example_ids = [3, 8, 9, 10, 20, 30, 33, 38, 49, 59, 60, 199, 203, 206, 367, 386, 
                      427, 434, 479, 486, 600, 601, 820, 1256, 1264, 1276, 1339, 1340, 
                      1782, 1825, 1862, 1877, 2112, 2300, 2304]
        
        for example_id in example_ids:
            cursor.execute("INSERT INTO example_ids (id) VALUES (?)", (example_id,))
        
        conn.commit()
        print(f"Populated example_ids table with {len(example_ids)} IDs")
    
    # Get example IDs that exist in the readings table
    cursor.execute("""
    SELECT e.id, r.cusum_min_correct
    FROM example_ids e
    JOIN readings r ON e.id = r.id
    WHERE r.in_use = 1 AND r.cusum_min_correct IS NOT NULL
    """)
    
    return cursor.fetchall()

def get_all_records(conn):
    """Get all records for export"""
    cursor = conn.cursor()
    cursor.execute("""
    SELECT id, cusum_min_correct
    FROM readings 
    WHERE in_use = 1 AND cusum_min_correct IS NOT NULL
    """)
    return cursor.fetchall()

def get_custom_records(conn, id_list):
    """Get custom records for export"""
    cursor = conn.cursor()
    placeholders = ','.join(['?'] * len(id_list))
    cursor.execute(f"""
    SELECT id, cusum_min_correct
    FROM readings 
    WHERE id IN ({placeholders}) AND in_use = 1 AND cusum_min_correct IS NOT NULL
    """, id_list)
    return cursor.fetchall()

def apply_sorting(records, sort_by, sort_order):
    """Apply sorting to records"""
    reverse_order = (sort_order == 'down')
    
    if sort_by == 'id':
        return sorted(records, key=lambda x: x[0], reverse=reverse_order)
    else:  # db-cusum
        # Handle corrupted CUSUM values by treating them as None (which sorts to end)
        def safe_cusum_key(record):
            try:
                return float(record[1]) if record[1] is not None else float('inf')
            except (ValueError, TypeError):
                return float('inf')  # Put corrupted values at the end
        return sorted(records, key=safe_cusum_key, reverse=reverse_order)

def get_metadata_columns(cursor):
    """Get metadata column names from readings table"""
    cursor.execute("PRAGMA table_info(readings)")
    columns_info = cursor.fetchall()
    
    # Extract column names and exclude readings/cusum data columns
    metadata_columns = []
    for col_info in columns_info:
        col_name = col_info[1]  # Column name is at index 1
        if not (col_name.startswith('readings') or col_name.startswith('cusum')):
            metadata_columns.append(col_name)
    
    return metadata_columns

def export_readings_and_cusum_csv(conn, records, output_file, k_param=0.0, export_columns=None, 
                                export_flattened=False, threshold=-80):
    """Export readings and CUSUM data to CSV format"""
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        # We'll determine columns dynamically based on first record
        writer = None
        headers = None
        
        for i, (record_id, db_cusum_min) in enumerate(tqdm(records, desc="Exporting to CSV")):
            try:
                # Get readings
                readings = get_readings_for_id(conn, record_id)
                if len(readings) < 10:
                    continue
                
                # Get metadata if requested
                cursor = conn.cursor()
                metadata = {}
                if export_columns is None or 'metadata' in export_columns:
                    metadata_cols = get_metadata_columns(cursor)
                    metadata_select = ", ".join(metadata_cols)
                    cursor.execute(f"SELECT {metadata_select} FROM readings WHERE id = ?", (record_id,))
                    metadata_row = cursor.fetchone()
                    metadata = dict(zip(metadata_cols, metadata_row))
                
                # Calculate CUSUM values
                if k_param == 0.0:
                    # Use database CUSUM values
                    cusum_columns = [f"cusum{j}" for j in range(len(readings))]
                    cusum_select = ", ".join(cusum_columns)
                    cursor.execute(f"SELECT {cusum_select} FROM readings WHERE id = ?", (record_id,))
                    cusum_row = cursor.fetchone()
                    # Handle corrupted CUSUM data by filtering out non-numeric values
                    cusum_values = []
                    for val in cusum_row:
                        if val is None:
                            break
                        try:
                            cusum_values.append(float(val))
                        except (ValueError, TypeError):
                            # Skip corrupted values and recalculate CUSUM instead
                            print(f"Warning: Corrupted CUSUM data for ID {record_id}, recalculating with k=0.0")
                            cusum_values, cusum_min = apply_corrected_cusum_algorithm(readings, k=0.0)
                            break
                    else:
                        # Only use database values if we processed all successfully
                        cusum_values = cusum_values[:len(readings)]
                        cusum_min = db_cusum_min
                else:
                    # Calculate CUSUM with custom k parameter
                    cusum_values, cusum_min = apply_corrected_cusum_algorithm(readings, k=k_param)
                
                # Create flattened readings if requested
                flattened_readings = None
                if export_flattened:
                    flattened_readings = create_flattened_readings(readings, cusum_values, cusum_min, threshold)
                
                # Prepare row data
                row_data = {}
                
                # Add metadata
                if export_columns is None or 'metadata' in export_columns:
                    for key, value in metadata.items():
                        row_data[key] = value
                
                # Add summary statistics
                if export_columns is None or 'summary' in export_columns:
                    row_data['cusum_min'] = float(cusum_min) if cusum_min is not None else None
                    row_data['cusum_k_parameter'] = k_param
                    row_data['readings_count'] = len(readings)
                    row_data['readings_min'] = min(readings)
                    row_data['readings_max'] = max(readings)
                    row_data['readings_mean'] = np.mean(readings)
                    row_data['readings_std'] = np.std(readings)
                    if export_flattened and flattened_readings is not None:
                        row_data['is_flattened'] = True
                        row_data['flatten_threshold'] = threshold
                    else:
                        row_data['is_flattened'] = False
                        row_data['flatten_threshold'] = threshold
                
                # Add readings data
                if export_columns is None or 'readings' in export_columns:
                    for j, reading in enumerate(readings):
                        row_data[f'readings{j}'] = reading
                
                # Add CUSUM data
                if export_columns is None or 'cusum' in export_columns:
                    for j, cusum_val in enumerate(cusum_values):
                        row_data[f'cusum{j}'] = float(cusum_val) if cusum_val is not None else None
                
                # Add flattened readings data
                if export_flattened and flattened_readings is not None:
                    if export_columns is None or 'flattened' in export_columns:
                        for j, flat_reading in enumerate(flattened_readings):
                            row_data[f'flattened{j}'] = flat_reading
                
                # Initialize CSV writer with headers from first record
                if writer is None:
                    # Natural sort to ensure readings0, readings1, ..., readings9, readings10, ... order
                    import re
                    def natural_sort_key(key):
                        """Sort key that handles numeric suffixes properly"""
                        parts = re.split(r'(\d+)', key)
                        return [int(part) if part.isdigit() else part for part in parts]
                    
                    headers = sorted(row_data.keys(), key=natural_sort_key)
                    writer = csv.DictWriter(csvfile, fieldnames=headers)
                    writer.writeheader()
                
                # Write the row
                writer.writerow(row_data)
                
            except Exception as e:
                print(f"Error processing ID {record_id}: {e}")
                continue

def main():
    parser = argparse.ArgumentParser(description='Export database data to CSV format')
    
    # Standard flags from flags_table.md - applicable ones
    parser.add_argument('--db', type=str, default="/home/azureuser/code/wssvc-flow/readings.db",
                       help='Path to database file (default: /home/azureuser/code/wssvc-flow/readings.db)')
    parser.add_argument('--output', type=str, default="database_export.csv",
                       help='Output CSV file path (default: database_export.csv)')
    parser.add_argument('--ids', type=str, 
                       help='Comma-separated list of specific IDs to export')
    parser.add_argument('--example-dataset', action='store_true',
                       help='Export example dataset from feedback plots')
    parser.add_argument('--all', action='store_true',
                       help='Export all records (alternative to --ids/--example-dataset)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of records to export')
    parser.add_argument('--sort-by', choices=['cusum', 'db-cusum', 'id'], default='id',
                       help='Sort by: "cusum" = calculated CUSUM values, "db-cusum" = database CUSUM (fast), "id" = record ID (default: id)')
    parser.add_argument('--sort-order', choices=['up', 'down'], default='up',
                       help='Sort order: "down" = high to low, "up" = low to high (default: up)')
    parser.add_argument('--k', type=float, default=0.0,
                       help='CUSUM tolerance parameter k for calculating new CUSUM values (default: 0.0 uses database values)')
    
    # CSV-specific flags
    parser.add_argument('--columns', type=str, 
                       help='Comma-separated list of column groups to export: metadata,summary,readings,cusum,flattened (default: all)')
    parser.add_argument('--export-flattened', action='store_true',
                       help='Include flattened readings data in export')
    parser.add_argument('--threshold', type=float, default=-80,
                       help='CUSUM threshold for flattening (default: -80)')
    
    # Non-applicable flags from flags_table.md (with reasons):
    # --files: CSV export is not file-based, works with database records
    # --default-k: CSV export is single k tool, not comparison tool  
    # --test-k: CSV export is single k tool, not comparison tool
    # --cusum-limit: Use --threshold instead for consistency with flattening logic
    
    args = parser.parse_args()
    
    # Validate arguments
    selection_count = sum([bool(args.ids), args.example_dataset, args.all])
    if selection_count == 0:
        parser.error("Must specify one of: --ids, --example-dataset, or --all")
    elif selection_count > 1:
        parser.error("Can only specify one of: --ids, --example-dataset, or --all")
    
    # Parse column groups
    export_columns = None
    if args.columns:
        export_columns = [col.strip() for col in args.columns.split(',')]
        valid_columns = {'metadata', 'summary', 'readings', 'cusum', 'flattened'}
        invalid_columns = set(export_columns) - valid_columns
        if invalid_columns:
            parser.error(f"Invalid column groups: {invalid_columns}. Valid options: {valid_columns}")
    
    print(f"Database: {args.db}")
    print(f"Output file: {args.output}")
    print(f"CUSUM k parameter: {args.k}")
    print(f"Sort by: {args.sort_by}, order: {args.sort_order}")
    if export_columns:
        print(f"Exporting columns: {export_columns}")
    else:
        print("Exporting all columns")
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    conn = sqlite3.connect(args.db)
    
    # Determine which records to process
    if args.example_dataset:
        print("Using example dataset from feedback plots...")
        records = get_example_ids(conn)
        file_type = "Example Dataset"
    elif args.ids:
        print(f"Processing specific IDs: {args.ids}")
        id_list = [int(x.strip()) for x in args.ids.split(',')]
        records = get_custom_records(conn, id_list)
        file_type = "Custom IDs"
    else:  # --all
        print("Processing all records...")
        records = get_all_records(conn)
        file_type = "All Records"
    
    # Apply sorting
    if args.sort_by in ['db-cusum', 'id']:
        # Fast database sorting or ID sorting
        records = apply_sorting(records, args.sort_by, args.sort_order)
    elif args.sort_by == 'cusum' and args.k != 0.0:
        # Need to recalculate CUSUM values for sorting
        print(f"Recalculating CUSUM values for sorting with k={args.k}...")
        records_with_new_cusum = []
        
        for record_id, db_cusum_min in tqdm(records, desc="Calculating CUSUM for sorting"):
            try:
                readings = get_readings_for_id(conn, record_id)
                if len(readings) >= 10:
                    _, new_cusum_min = apply_corrected_cusum_algorithm(readings, k=args.k)
                    records_with_new_cusum.append((record_id, new_cusum_min))
            except Exception as e:
                print(f"Error processing ID {record_id} for sorting: {e}")
                continue
        
        records = apply_sorting(records_with_new_cusum, 'db-cusum', args.sort_order)
    else:
        # Use database CUSUM for sorting
        records = apply_sorting(records, 'db-cusum', args.sort_order)
    
    # Apply limit if specified
    if args.limit:
        records = records[:args.limit]
        print(f"Limited to {args.limit} records")
    
    print(f"Exporting {len(records)} records...")
    
    # Export to CSV
    export_readings_and_cusum_csv(
        conn, records, args.output, 
        k_param=args.k,
        export_columns=export_columns,
        export_flattened=args.export_flattened,
        threshold=args.threshold
    )
    
    conn.close()
    
    print(f"CSV export completed: {args.output}")
    print(f"Exported {len(records)} {file_type.lower()} records")

if __name__ == "__main__":
    main()