#!/usr/bin/env python3
"""
Test script for NSQIP Tools with real data.

This script is for testing the package with actual NSQIP data before publication.
It will be deleted before PyPI release.
"""
import nsqip_tools
from pathlib import Path
from typing import Optional
import sys


def test_memory_detection():
    """Test memory detection functionality."""
    print("=== Memory Detection Test ===")
    mem_info = nsqip_tools.get_memory_info()
    print(f"Total memory: {mem_info['total']}")
    print(f"Available memory: {mem_info['available']}")
    print(f"Recommended limit: {mem_info['recommended_limit']}")
    print(f"Memory usage: {mem_info['percent']}%")
    print()


def test_dataset_build(data_dir: str, dataset_type: Optional[str] = None):
    """Test building parquet dataset from real NSQIP data."""
    print(f"=== Dataset Build Test ===")
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Data directory not found: {data_path}")
        return None
    
    if dataset_type:
        print(f"Using specified dataset type: {dataset_type}")
        print(f"Will create parquet dataset in: {data_path}/{dataset_type}_nsqip_parquet/")
    else:
        print("Will auto-detect dataset type from filenames...")
    
    # List files in directory
    txt_files = list(data_path.glob("*.txt"))
    print(f"Found {len(txt_files)} .txt files:")
    for f in txt_files[:5]:  # Show first 5
        print(f"  - {f.name}")
    if len(txt_files) > 5:
        print(f"  ... and {len(txt_files) - 5} more")
    
    if not txt_files:
        print("❌ No .txt files found")
        return None
    
    try:
        # Build parquet dataset with conservative memory settings
        print("\n🔄 Building parquet dataset...")
        result = nsqip_tools.build_parquet_dataset(
            data_dir=data_path,
            dataset_type=dataset_type,  # type: ignore
            memory_limit="2GB",  # Start conservative
            verify_case_counts=True
        )
        
        print(f"✅ Dataset created: {result['parquet_dir']}")
        print(f"✅ Dictionary created: {result['dictionary']}")
        print(f"✅ Log file: {result['log']}")
        
        # Check dataset size
        total_size = 0
        parquet_files = list(result['parquet_dir'].glob("*.parquet"))
        for pf in parquet_files:
            total_size += pf.stat().st_size
        print(f"Dataset size: {total_size / (1024**3):.2f} GB ({len(parquet_files)} parquet files)")
        
        return result
        
    except Exception as e:
        print(f"❌ Dataset build failed: {e}")
        return None


def test_querying(parquet_dir: Path):
    """Test querying functionality."""
    print("=== Query Test ===")
    
    try:
        # Load data
        print("🔄 Loading data...")
        query = nsqip_tools.load_data(parquet_dir)
        
        # Test dataset info
        print("🔄 Getting dataset info...")
        info = query.describe()
        print(f"Total rows: {info['total_rows']:,}")
        print(f"Columns: {info['columns']}")
        print(f"Parquet files: {info['parquet_files']}")
        
        # Test filtering by year
        print("\n🔄 Testing year filter...")
        recent_query = query.filter_by_year([2021, 2022])
        recent_count = recent_query.count()
        print(f"Recent years (2021-2022): {recent_count:,} rows")
        
        # Test small collection
        if recent_count > 0 and recent_count < 100000:  # Reasonable size
            print("🔄 Testing collection of recent data...")
            df = recent_query.collect()
            print(f"✅ Collected {len(df)} rows successfully")
            print(f"Columns: {len(df.columns)}")
            
            # Show some basic info
            if 'OPERYR' in df.columns:
                year_counts = df['OPERYR'].value_counts().sort('OPERYR')
                print("Year distribution:")
                for row in year_counts.iter_rows():
                    print(f"  {row[0]}: {row[1]:,} cases")
        else:
            print("🔄 Dataset large, testing sampling instead...")
            sample_df = query.sample(n=1000)
            print(f"✅ Sampled {len(sample_df)} rows successfully")
        
        # Test CPT filtering on a small subset
        print("\n🔄 Testing CPT filter...")
        cpt_query = query.filter_by_year([2022]).filter_by_cpt(["44970"])  # Lap appendectomy
        cpt_count = cpt_query.count()
        print(f"CPT 44970 in 2022: {cpt_count:,} rows")
        
        if cpt_count > 0:
            df_cpt = cpt_query.collect()
            print(f"✅ Successfully filtered to {len(df_cpt)} cases")
        
        # Test demographics selection
        print("\n🔄 Testing demographics selection...")
        demo_query = query.filter_by_year([2022]).select_demographics()
        demo_info = demo_query.describe()
        print(f"Demographics subset: {demo_info['columns']} columns")
        
        return True
        
    except Exception as e:
        print(f"❌ Query test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_dictionary(dict_path: Path):
    """Test data dictionary functionality."""
    print("=== Data Dictionary Test ===")
    
    try:
        import polars as pl
        
        # Read CSV format
        df = pl.read_csv(dict_path)
        print(f"✅ Data dictionary loaded: {len(df)} columns")
        
        # Show some examples
        print("\nSample entries:")
        columns_to_show = []
        for col in ['Column Name', 'Data Type', 'Active', 'Non-Null Count']:
            if col in df.columns:
                columns_to_show.append(col)
        
        if columns_to_show:
            print(df.select(columns_to_show).head(10))
        
        # Check for active variables if column exists
        if 'Active' in df.columns:
            active_vars = df.filter(pl.col('Active') == 'Yes')
            inactive_vars = df.filter(pl.col('Active') == 'No')
            print(f"\nActive variables: {len(active_vars)}")
            print(f"Inactive variables: {len(inactive_vars)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data dictionary test failed: {e}")
        return False


def main():
    """Main test function."""
    print("🧪 NSQIP Tools Real Data Test")
    print("=" * 50)
    
    # Get data directory from user
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    else:
        data_dir = input("Enter path to NSQIP data directory: ").strip()
    
    if len(sys.argv) > 2:
        dataset_type = sys.argv[2]
    else:
        dataset_type = input("Enter dataset type (adult/pediatric) [auto-detect]: ").strip() or None
    
    # Test 1: Memory detection
    test_memory_detection()
    
    # Test 2: Dataset build
    result = test_dataset_build(data_dir, dataset_type)
    if not result:
        print("❌ Cannot continue without successful dataset build")
        return
    
    print()
    
    # Test 3: Querying
    success = test_querying(result['parquet_dir'])
    if not success:
        print("⚠️  Query test failed")
    
    print()
    
    # Test 4: Data dictionary
    test_data_dictionary(result['dictionary'])
    
    print("\n" + "=" * 50)
    print("🎉 Testing complete!")
    print(f"Dataset available at: {result['parquet_dir']}")


if __name__ == "__main__":
    main()