#!/usr/bin/env python3
"""
Comprehensive validation script for Adult NSQIP parquet dataset.

This script validates:
1. Dataset loading and basic statistics
2. Year coverage and case counts
3. Key transformations (AGE_AS_INT, ALL_CPT_CODES, etc.)
4. Query functionality (filter_by_cpt, filter_by_diagnosis, filter_by_year)
5. Data dictionary presence and accuracy
6. Memory efficiency for large queries
"""

import nsqip_tools
import polars as pl
from pathlib import Path
import json
import sys
from datetime import datetime


def print_section(title):
    """Print a formatted section header."""
    print(f"\n{'=' * 60}")
    print(f"{title}")
    print(f"{'=' * 60}")


def validate_dataset_loading(data_path):
    """Test basic dataset loading."""
    print_section("1. Dataset Loading Test")
    
    try:
        # Test loading
        query = nsqip_tools.load_data(str(data_path))
        print("✓ Dataset loaded successfully")
        
        # Get basic info
        info = query.describe()
        print(f"✓ Total rows: {info['total_rows']:,}")
        print(f"✓ Total columns: {info['columns']}")
        
        # Test count method
        count = query.count()
        print(f"✓ Count method works: {count:,} rows")
        
        return True, info
    except Exception as e:
        print(f"✗ Error loading dataset: {type(e).__name__}: {e}")
        return False, None


def validate_year_coverage(data_path):
    """Validate year coverage and case counts."""
    print_section("2. Year Coverage Validation")
    
    try:
        query = nsqip_tools.load_data(str(data_path))
        
        # Get year distribution
        year_counts = (
            query.lazy_frame
            .group_by("OPERYR")
            .agg(pl.count().alias("cases"))
            .sort("OPERYR")
            .collect()
        )
        
        print("Year distribution:")
        total_cases = 0
        for row in year_counts.iter_rows():
            year, cases = row
            total_cases += cases
            print(f"  - {year}: {cases:,} cases")
        
        print(f"\nTotal cases across all years: {total_cases:,}")
        
        # Check expected years (2006-2022 for adult)
        years = sorted(year_counts["OPERYR"].to_list())
        expected_years = list(range(2006, 2023))
        
        if years == expected_years:
            print(f"✓ All expected years present ({expected_years[0]}-{expected_years[-1]})")
        else:
            missing = set(expected_years) - set(years)
            extra = set(years) - set(expected_years)
            if missing:
                print(f"✗ Missing years: {sorted(missing)}")
            if extra:
                print(f"! Extra years: {sorted(extra)}")
        
        return True, year_counts
    except Exception as e:
        print(f"✗ Error validating years: {type(e).__name__}: {e}")
        return False, None


def validate_transformations(data_path):
    """Validate key transformations applied during dataset creation."""
    print_section("3. Transformation Validation")
    
    try:
        query = nsqip_tools.load_data(str(data_path))
        
        # Sample a small subset for validation
        sample = query.sample(1000)
        
        # Check AGE transformations
        print("\nAge transformations:")
        if all(col in sample.columns for col in ["AGE", "AGE_AS_INT", "AGE_IS_90_PLUS"]):
            # Check 90+ handling
            age_90_plus = sample.filter(pl.col("AGE") == "90+")
            if len(age_90_plus) > 0:
                correct_int = (age_90_plus["AGE_AS_INT"] == 90).all()
                correct_flag = age_90_plus["AGE_IS_90_PLUS"].all()
                if correct_int and correct_flag:
                    print("✓ AGE_AS_INT correctly converts '90+' to 90")
                    print("✓ AGE_IS_90_PLUS correctly flags '90+' cases")
                else:
                    print("✗ Issues with 90+ age handling")
            else:
                print("  - No 90+ cases in sample to validate")
            
            # Check numeric ages
            numeric_ages = sample.filter(pl.col("AGE") != "90+")
            if len(numeric_ages) > 0:
                print(f"✓ AGE_AS_INT range: {numeric_ages['AGE_AS_INT'].min()} - {numeric_ages['AGE_AS_INT'].max()}")
        else:
            print("✗ Missing age transformation columns")
        
        # Check CPT array
        print("\nCPT array transformation:")
        if "ALL_CPT_CODES" in sample.columns:
            # Check that it's a list column
            cpt_lengths = sample.select(pl.col("ALL_CPT_CODES").list.len()).to_series()
            print(f"✓ ALL_CPT_CODES present (list lengths: min={cpt_lengths.min()}, max={cpt_lengths.max()})")
            
            # Verify it contains CPT column
            sample_cpts = sample.filter(pl.col("CPT").is_not_null())
            if len(sample_cpts) > 0:
                first_row = sample_cpts.row(0, named=True)
                if first_row["CPT"] in first_row["ALL_CPT_CODES"]:
                    print("✓ Primary CPT correctly included in ALL_CPT_CODES")
        else:
            print("✗ ALL_CPT_CODES column missing")
        
        # Check diagnosis array
        print("\nDiagnosis array transformation:")
        if "ALL_DIAGNOSIS_CODES" in sample.columns:
            diag_lengths = sample.select(pl.col("ALL_DIAGNOSIS_CODES").list.len()).to_series()
            print(f"✓ ALL_DIAGNOSIS_CODES present (list lengths: min={diag_lengths.min()}, max={diag_lengths.max()})")
        else:
            print("✗ ALL_DIAGNOSIS_CODES column missing")
        
        # Check RACE_COMBINED
        print("\nRace combination:")
        if "RACE_COMBINED" in sample.columns:
            print("✓ RACE_COMBINED column present")
            unique_races = sample["RACE_COMBINED"].unique().drop_nulls().to_list()
            print(f"  Sample unique values: {unique_races[:5]}...")
        else:
            print("✗ RACE_COMBINED column missing")
        
        # Check WORK_RVU_TOTAL (adult only)
        print("\nWork RVU total:")
        if "WORK_RVU_TOTAL" in sample.columns:
            rvu_stats = sample.select(
                pl.col("WORK_RVU_TOTAL").min().alias("min"),
                pl.col("WORK_RVU_TOTAL").max().alias("max"),
                pl.col("WORK_RVU_TOTAL").mean().alias("mean")
            ).row(0, named=True)
            print(f"✓ WORK_RVU_TOTAL present (min={rvu_stats['min']:.2f}, max={rvu_stats['max']:.2f}, mean={rvu_stats['mean']:.2f})")
        else:
            print("✗ WORK_RVU_TOTAL column missing")
        
        return True
    except Exception as e:
        print(f"✗ Error validating transformations: {type(e).__name__}: {e}")
        return False


def validate_query_functionality(data_path):
    """Test various query methods."""
    print_section("4. Query Functionality Tests")
    
    try:
        query = nsqip_tools.load_data(str(data_path))
        
        # Test filter_by_cpt
        print("\nTesting filter_by_cpt:")
        common_cpts = ["44970", "44979", "49650", "49651"]  # Common laparoscopic procedures
        cpt_query = query.filter_by_cpt(common_cpts)
        cpt_count = cpt_query.count()
        print(f"✓ Filtered by CPT codes {common_cpts}: {cpt_count:,} cases")
        
        # Test filter_by_diagnosis
        print("\nTesting filter_by_diagnosis:")
        common_diagnoses = ["K80.20", "K80.21", "K35.8"]  # Gallstones, appendicitis
        diag_query = query.filter_by_diagnosis(common_diagnoses)
        diag_count = diag_query.count()
        print(f"✓ Filtered by diagnosis codes {common_diagnoses}: {diag_count:,} cases")
        
        # Test filter_by_year
        print("\nTesting filter_by_year:")
        recent_years = [2020, 2021, 2022]
        year_query = query.filter_by_year(recent_years)
        year_count = year_query.count()
        print(f"✓ Filtered by years {recent_years}: {year_count:,} cases")
        
        # Test chained filters
        print("\nTesting chained filters:")
        chained_query = (query
                        .filter_by_year([2022])
                        .filter_by_cpt(["44970"]))
        chained_count = chained_query.count()
        print(f"✓ Chained filters (2022 + CPT 44970): {chained_count:,} cases")
        
        # Test select_demographics
        print("\nTesting select_demographics:")
        demo_query = query.filter_by_year([2022]).select_demographics()
        demo_df = demo_query.sample(10)
        print(f"✓ Demographics selection works, columns: {demo_df.columns}")
        
        # Test select_outcomes
        print("\nTesting select_outcomes:")
        outcome_query = query.filter_by_year([2022]).select_outcomes()
        outcome_df = outcome_query.sample(10)
        print(f"✓ Outcomes selection works, columns: {outcome_df.columns}")
        
        # Test filter_active_variables
        print("\nTesting filter_active_variables:")
        active_query = query.filter_active_variables()
        active_info = active_query.describe()
        print(f"✓ Active variables filter works, columns reduced to: {active_info['columns']}")
        
        return True
    except Exception as e:
        print(f"✗ Error testing query functionality: {type(e).__name__}: {e}")
        return False


def validate_data_dictionary(data_path):
    """Check for data dictionary files."""
    print_section("5. Data Dictionary Validation")
    
    try:
        parent_dir = data_path.parent
        dict_formats = [
            ("CSV", parent_dir / "adult_data_dictionary.csv"),
            ("HTML", parent_dir / "adult_data_dictionary.html"),
            ("JSON", parent_dir / "adult_data_dictionary.json")
        ]
        
        found_any = False
        for format_name, dict_path in dict_formats:
            if dict_path.exists():
                print(f"✓ {format_name} data dictionary found: {dict_path.name}")
                found_any = True
                
                # For JSON, check structure
                if format_name == "JSON":
                    with open(dict_path) as f:
                        dict_data = json.load(f)
                    print(f"  - Contains {len(dict_data)} variables")
            else:
                print(f"✗ {format_name} data dictionary not found")
        
        return found_any
    except Exception as e:
        print(f"✗ Error checking data dictionary: {type(e).__name__}: {e}")
        return False


def validate_memory_efficiency(data_path):
    """Test memory-efficient operations."""
    print_section("6. Memory Efficiency Tests")
    
    try:
        query = nsqip_tools.load_data(str(data_path))
        
        # Test lazy evaluation
        print("\nTesting lazy evaluation:")
        lazy_query = (query
                     .filter_by_year([2020, 2021, 2022])
                     .lazy_frame
                     .filter(pl.col("AGE_AS_INT") > 65))
        print("✓ Lazy query created without materializing data")
        
        # Test streaming collection
        print("\nTesting streaming collection:")
        result = lazy_query.select([
            pl.count().alias("total_cases"),
            pl.col("AGE_AS_INT").mean().alias("mean_age")
        ]).collect(streaming=True)
        
        stats = result.row(0, named=True)
        print(f"✓ Streaming collection successful: {stats['total_cases']:,} cases, mean age {stats['mean_age']:.1f}")
        
        # Test memory info
        print("\nMemory information:")
        mem_info = nsqip_tools.get_memory_info()
        print(f"✓ Total RAM: {mem_info['total']}")
        print(f"✓ Available: {mem_info['available']}")
        print(f"✓ Recommended limit: {mem_info['recommended_limit']}")
        
        return True
    except Exception as e:
        print(f"✗ Error testing memory efficiency: {type(e).__name__}: {e}")
        return False


def validate_metadata(data_path):
    """Check metadata.json file."""
    print_section("7. Metadata Validation")
    
    try:
        metadata_path = data_path / "metadata.json"
        
        if metadata_path.exists():
            with open(metadata_path) as f:
                metadata = json.load(f)
            
            print("✓ metadata.json found")
            print(f"  - Dataset type: {metadata.get('dataset_type', 'Not specified')}")
            print(f"  - Created: {metadata.get('created_at', 'Not specified')}")
            print(f"  - Transform version: {metadata.get('transform_version', 'Not specified')}")
            
            if "years_included" in metadata:
                print(f"  - Years included: {metadata['years_included']}")
            
            if "total_cases" in metadata:
                print(f"  - Total cases: {metadata['total_cases']:,}")
            
            return True
        else:
            print("✗ metadata.json not found")
            return False
    except Exception as e:
        print(f"✗ Error reading metadata: {type(e).__name__}: {e}")
        return False


def main():
    """Run all validation tests."""
    print(f"\nAdult NSQIP Dataset Validation")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Path to adult NSQIP parquet dataset
    data_path = Path("data/adult_nsqip_parquet")
    
    if not data_path.exists():
        print(f"\n✗ ERROR: Dataset not found at {data_path}")
        return 1
    
    print(f"\nDataset path: {data_path.absolute()}")
    
    # Run all validation tests
    all_passed = True
    
    # 1. Basic loading
    passed, info = validate_dataset_loading(data_path)
    all_passed &= passed
    
    if not passed:
        print("\n✗ Basic loading failed, skipping remaining tests")
        return 1
    
    # 2. Year coverage
    passed, _ = validate_year_coverage(data_path)
    all_passed &= passed
    
    # 3. Transformations
    passed = validate_transformations(data_path)
    all_passed &= passed
    
    # 4. Query functionality
    passed = validate_query_functionality(data_path)
    all_passed &= passed
    
    # 5. Data dictionary
    passed = validate_data_dictionary(data_path)
    all_passed &= passed
    
    # 6. Memory efficiency
    passed = validate_memory_efficiency(data_path)
    all_passed &= passed
    
    # 7. Metadata
    passed = validate_metadata(data_path)
    all_passed &= passed
    
    # Summary
    print_section("Validation Summary")
    if all_passed:
        print("✓ All validation tests PASSED")
        return 0
    else:
        print("✗ Some validation tests FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())