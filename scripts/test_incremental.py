#!/usr/bin/env python3
"""Test NSQIP Tools with incremental approach - test 3 files first, then full dataset."""

import sys
from pathlib import Path
import shutil
import traceback
from nsqip_tools import build_parquet_dataset, load_data
from nsqip_tools._internal.memory_utils import get_memory_info


def test_with_subset(data_dir: Path, num_files: int = 3):
    """Test with a subset of files first."""
    print(f"\n{'='*50}")
    print(f"TESTING WITH {num_files} FILES FIRST")
    print(f"{'='*50}")
    
    # Create temporary directory with subset of files
    temp_dir = Path("/tmp/nsqip_subset_test")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    
    # Copy first N text files
    txt_files = sorted(data_dir.glob("*.txt"))[:num_files]
    print(f"\nCopying {len(txt_files)} files for testing:")
    for f in txt_files:
        print(f"  - {f.name}")
        shutil.copy2(f, temp_dir)
    
    # Run build test
    try:
        print("\n🔄 Building parquet dataset from subset...")
        result = build_parquet_dataset(
            data_dir=temp_dir,
            dataset_type=None,  # Auto-detect
            memory_limit="2GB",
            verify_case_counts=False,  # Skip verification for subset
        )
        print("✅ Subset build successful!")
        
        # Test querying
        print("\n🔄 Testing query functionality...")
        query = load_data(result["parquet_dir"])
        df = query.lazy_frame.limit(5).collect()
        print(f"✅ Query successful! Loaded {len(df)} rows")
        
        # Clean up
        shutil.rmtree(temp_dir)
        if result["parquet_dir"].exists():
            shutil.rmtree(result["parquet_dir"])
        
        return True
        
    except Exception as e:
        print(f"\n❌ Subset test failed: {e}")
        traceback.print_exc()
        # Clean up
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        return False


def test_full_dataset(data_dir: Path):
    """Run full test on complete dataset."""
    print(f"\n{'='*50}")
    print("RUNNING FULL DATASET TEST")
    print(f"{'='*50}")
    
    # Memory info
    mem_info = get_memory_info()
    print(f"\nMemory Info:")
    print(f"  Total: {mem_info['total']}")
    print(f"  Available: {mem_info['available']}")
    print(f"  Recommended limit: {mem_info['recommended_limit']}")
    
    # Count files
    txt_files = list(data_dir.glob("*.txt"))
    print(f"\nFound {len(txt_files)} text files")
    
    try:
        # Build dataset
        print("\n🔄 Building full parquet dataset...")
        result = build_parquet_dataset(
            data_dir=data_dir,
            dataset_type=None,  # Auto-detect
            memory_limit=mem_info["recommended_limit"],
            verify_case_counts=True,
        )
        print("✅ Full dataset build successful!")
        
        # Test querying
        print("\n🔄 Testing query functionality...")
        query = load_data(result["parquet_dir"])
        
        # Test 1: Basic load
        df = query.lazy_frame.limit(10).collect()
        print(f"✅ Basic query: Loaded {len(df)} rows")
        
        # Test 2: Filter by year
        df_2019 = query.filter_by_year(2019).lazy_frame.limit(10).collect()
        print(f"✅ Year filter: Found {len(df_2019)} rows for 2019")
        
        # Test 3: Performance test
        print("\n⏱️  Performance test...")
        import time
        start = time.time()
        count = query.filter_by_year([2019, 2020, 2021]).count()
        elapsed = time.time() - start
        print(f"✅ Counted {count:,} cases from 3 years in {elapsed:.2f} seconds")
        
        print(f"\n✅ ALL TESTS PASSED!")
        print(f"Dataset location: {result['parquet_dir']}")
        if "dictionary" in result:
            print(f"Data dictionary: {result['dictionary']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Full dataset test failed: {e}")
        traceback.print_exc()
        return False


def main():
    if len(sys.argv) != 2:
        print("Usage: python test_incremental.py <data_directory>")
        sys.exit(1)
    
    data_dir = Path(sys.argv[1])
    if not data_dir.exists():
        print(f"Error: Data directory does not exist: {data_dir}")
        sys.exit(1)
    
    print("🧪 NSQIP Tools Incremental Test")
    print("================================")
    
    # Test with subset first
    if test_with_subset(data_dir, num_files=3):
        print("\n✅ Subset test passed! Proceeding with full dataset...")
        
        # Test with full dataset
        if test_full_dataset(data_dir):
            print("\n🎉 All tests completed successfully!")
        else:
            print("\n❌ Full dataset test failed")
            sys.exit(1)
    else:
        print("\n❌ Subset test failed - not proceeding with full dataset")
        sys.exit(1)


if __name__ == "__main__":
    main()