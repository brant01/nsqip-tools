#!/usr/bin/env python3
"""Quick fix for Active column in existing data dictionary."""

import sys
import json
import polars as pl
from pathlib import Path

def fix_active_column(parquet_dir: Path):
    """Fix the Active column in existing data dictionary."""
    
    csv_path = parquet_dir / "adult_data_dictionary.csv"
    json_path = parquet_dir / "adult_data_dictionary.json"
    
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        return False
    
    print("Loading existing data dictionary...")
    
    # Load parquet data
    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        print("No parquet files found")
        return False
    
    lazy_frames = [pl.scan_parquet(pf) for pf in parquet_files]
    if len(lazy_frames) == 1:
        full_data = lazy_frames[0]
    else:
        full_data = pl.concat(lazy_frames, how="vertical_relaxed")
    
    # Get schema and years
    schema = pl.scan_parquet(parquet_files[0]).collect_schema()
    columns = list(schema.names())
    
    years_df = full_data.select(pl.col("OPERYR").unique().sort()).collect()
    years = sorted([str(y) for y in years_df["OPERYR"].to_list() if y is not None])
    most_recent_year = max(years) if years else "2022"
    
    print(f"Most recent year: {most_recent_year}")
    print("Calculating active status...")
    
    # Calculate active status for most recent year
    active_status = {}
    
    # Process in batches to avoid large queries
    batch_size = 50
    for i in range(0, len(columns), batch_size):
        batch_cols = columns[i:i+batch_size]
        batch_cols = [col for col in batch_cols if col != "OPERYR"]
        
        if not batch_cols:
            continue
            
        print(f"Processing batch {i//batch_size + 1}: {len(batch_cols)} columns")
        
        active_exprs = []
        for col in batch_cols:
            active_exprs.append(
                pl.col(col).is_not_null().sum().alias(f"{col}__active")
            )
        
        try:
            active_stats = (full_data
                           .filter(pl.col("OPERYR").cast(pl.Utf8) == most_recent_year)
                           .select(active_exprs)
                           .collect())
            
            for col in batch_cols:
                active_count = active_stats[f"{col}__active"][0] if f"{col}__active" in active_stats.columns else 0
                active_status[col] = "Yes" if active_count > 0 else "No"
        except Exception as e:
            print(f"Error processing batch: {e}")
            for col in batch_cols:
                active_status[col] = "Unknown"
    
    # Special case for OPERYR
    active_status["OPERYR"] = "N/A"
    
    # Load and update CSV
    print("Loading CSV data...")
    df = pl.read_csv(csv_path)
    
    # Update Active column
    print("Updating Active column...")
    updated_rows = []
    for row in df.iter_rows(named=True):
        col_name = row["Column Name"]
        row["Active"] = active_status.get(col_name, "Unknown")
        updated_rows.append(row)
    
    # Save updated CSV
    updated_df = pl.DataFrame(updated_rows)
    updated_df.write_csv(csv_path)
    print(f"✅ Updated CSV saved: {csv_path}")
    
    # Update JSON if it exists
    if json_path.exists():
        print("Updating JSON data...")
        with open(json_path, 'r') as f:
            json_data = json.load(f)
        
        # Update columns in JSON
        for col_data in json_data.get("columns", []):
            col_name = col_data.get("Column Name")
            if col_name and col_name in active_status:
                col_data["Active"] = active_status[col_name]
        
        # Update metadata
        json_data["metadata"]["last_active_update"] = most_recent_year
        
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=2)
        print(f"✅ Updated JSON saved: {json_path}")
    
    # Show sample results
    print("\nSample Active column results:")
    active_yes = [col for col, status in active_status.items() if status == "Yes"]
    active_no = [col for col, status in active_status.items() if status == "No"]
    print(f"Active=Yes: {len(active_yes)} columns")
    print(f"Active=No: {len(active_no)} columns")
    print(f"Sample Yes: {active_yes[:5]}")
    print(f"Sample No: {active_no[:5]}")
    
    return True

def main():
    if len(sys.argv) != 2:
        print("Usage: python quick_active_fix.py <parquet_directory>")
        sys.exit(1)
    
    parquet_dir = Path(sys.argv[1])
    if not parquet_dir.exists():
        print(f"Error: Directory not found: {parquet_dir}")
        sys.exit(1)
    
    fix_active_column(parquet_dir)

if __name__ == "__main__":
    main()