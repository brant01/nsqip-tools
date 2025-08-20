#!/usr/bin/env python3
"""Build a test parquet dataset from the most recent NSQIP file."""

import nsqip_tools
from pathlib import Path
import shutil

def main():
    # Source data directory
    # Get data directory from environment variable or use default
    import os
    source_dir = Path(os.getenv('NSQIP_DATA_DIR', Path.home() / "nsqip_data"))
    
    if not source_dir.exists():
        print(f"Error: NSQIP data directory not found: {source_dir}")
        print("Please set NSQIP_DATA_DIR environment variable or create .env file")
        return
    
    # Create test directory with just 2022 data
    test_dir = Path.home() / "projects" / "nsqip_tools" / "test_data"
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy the 2022 file for testing
    source_file = source_dir / "acs_nsqip_puf22.txt"
    test_file = test_dir / "acs_nsqip_puf22.txt"
    
    print(f"Copying {source_file.name} to test directory...")
    if not test_file.exists():
        shutil.copy2(source_file, test_file)
        print(f"Copied {test_file.stat().st_size / (1024*1024):.1f} MB")
    else:
        print("Test file already exists")
    
    # Build parquet dataset
    output_dir = Path.home() / "projects" / "nsqip_tools" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\nBuilding parquet dataset...")
    print(f"Source: {test_dir}")
    print(f"Output: {output_dir}")
    
    try:
        result = nsqip_tools.build_parquet_dataset(
            data_dir=test_dir,
            output_dir=output_dir,
            dataset_type="adult",
            memory_limit="4GB",
            verify_case_counts=False,  # Skip for test
            apply_transforms=True
        )
        
        print(f"\nSuccess!")
        print(f"Parquet dataset: {result['parquet_dir']}")
        print(f"Data dictionary: {result.get('dictionary', 'Not generated')}")
        print(f"Log file: {result['log']}")
        
        # Quick test of the dataset
        print(f"\nTesting dataset...")
        query = nsqip_tools.load_data(result['parquet_dir'])
        info = query.describe()
        print(f"Total rows: {info['total_rows']:,}")
        print(f"Columns: {info['columns']}")
        
        # Test tonsillectomy filter
        tonsil_query = query.filter_by_cpt(["42821", "42826"])
        tonsil_info = tonsil_query.describe()
        print(f"Tonsillectomy cases: {tonsil_info['total_rows']:,}")
        
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()