"""
Comprehensive test suite for validating NSQIP and NCDB parquet datasets.

These tests validate the actual datasets in the data/ directory to ensure
they work correctly with the nsqip_tools package.
"""

import pytest
from pathlib import Path
import json
import polars as pl
import nsqip_tools
from nsqip_tools.query import NSQIPQuery


# Dataset paths - these are relative to the project root
ADULT_NSQIP_PATH = Path("data/adult_nsqip_parquet")
PEDIATRIC_NSQIP_PATH = Path("data/pediatric_nsqip_parquet")
NCDB_PATH = Path("data/ncdb_parquet_20250603")


def dataset_exists(path: Path) -> bool:
    """Check if a dataset path exists."""
    return path.exists() and path.is_dir()


@pytest.fixture(scope="module")
def adult_nsqip_query():
    """Fixture to load adult NSQIP dataset once for all tests."""
    if not dataset_exists(ADULT_NSQIP_PATH):
        pytest.skip(f"Adult NSQIP dataset not found at {ADULT_NSQIP_PATH}")
    return nsqip_tools.load_data(str(ADULT_NSQIP_PATH))


@pytest.fixture(scope="module")
def pediatric_nsqip_query():
    """Fixture to load pediatric NSQIP dataset once for all tests."""
    if not dataset_exists(PEDIATRIC_NSQIP_PATH):
        pytest.skip(f"Pediatric NSQIP dataset not found at {PEDIATRIC_NSQIP_PATH}")
    return nsqip_tools.load_data(str(PEDIATRIC_NSQIP_PATH))


@pytest.fixture(scope="module")
def ncdb_query():
    """Fixture to load NCDB dataset once for all tests."""
    if not dataset_exists(NCDB_PATH):
        pytest.skip(f"NCDB dataset not found at {NCDB_PATH}")
    return nsqip_tools.load_data(str(NCDB_PATH))


@pytest.mark.integration
@pytest.mark.requires_data
class TestAdultNSQIP:
    """Test suite for Adult NSQIP dataset."""
    
    def test_dataset_loading(self, adult_nsqip_query):
        """Test that the dataset loads successfully."""
        assert isinstance(adult_nsqip_query, NSQIPQuery)
        info = adult_nsqip_query.describe()
        assert info['total_rows'] > 0
        assert info['columns'] > 0
    
    def test_year_coverage(self, adult_nsqip_query):
        """Test that all expected years are present (2006-2022)."""
        df = (adult_nsqip_query.lazy_frame
              .group_by("OPERYR")
              .agg(pl.count())
              .sort("OPERYR")
              .collect())
        
        years = sorted(df["OPERYR"].to_list())
        expected_years = [str(year) for year in range(2006, 2023)]
        
        assert years == expected_years, f"Missing years: {set(expected_years) - set(years)}"
    
    def test_age_transformations(self, adult_nsqip_query):
        """Test AGE_AS_INT and AGE_IS_90_PLUS transformations."""
        # Sample data to check transformations
        sample = adult_nsqip_query.sample(1000)
        
        # Check required columns exist
        required_cols = ["AGE", "AGE_AS_INT", "AGE_IS_90_PLUS"]
        for col in required_cols:
            assert col in sample.columns, f"Missing column: {col}"
        
        # Check 90+ handling
        age_90_plus = sample.filter(pl.col("AGE") == "90+")
        if len(age_90_plus) > 0:
            assert (age_90_plus["AGE_AS_INT"] == 90).all()
            assert age_90_plus["AGE_IS_90_PLUS"].all()
        
        # Check numeric ages
        numeric_ages = sample.filter(pl.col("AGE") != "90+")
        if len(numeric_ages) > 0:
            assert numeric_ages["AGE_AS_INT"].min() >= 0
            assert numeric_ages["AGE_AS_INT"].max() < 90
            assert (~numeric_ages["AGE_IS_90_PLUS"]).all()
    
    def test_cpt_array(self, adult_nsqip_query):
        """Test ALL_CPT_CODES array transformation."""
        sample = adult_nsqip_query.sample(100)
        
        assert "ALL_CPT_CODES" in sample.columns
        
        # Check it's a list column
        assert sample.schema["ALL_CPT_CODES"] == pl.List(pl.Utf8)
        
        # Check primary CPT is included
        for row in sample.filter(pl.col("CPT").is_not_null()).iter_rows(named=True):
            assert row["CPT"] in row["ALL_CPT_CODES"]
    
    def test_diagnosis_array(self, adult_nsqip_query):
        """Test ALL_DIAGNOSIS_CODES array transformation."""
        sample = adult_nsqip_query.sample(100)
        
        assert "ALL_DIAGNOSIS_CODES" in sample.columns
        assert sample.schema["ALL_DIAGNOSIS_CODES"] == pl.List(pl.Utf8)
    
    def test_race_combined(self, adult_nsqip_query):
        """Test RACE_COMBINED transformation."""
        sample = adult_nsqip_query.sample(100)
        assert "RACE_COMBINED" in sample.columns
    
    def test_work_rvu_total(self, adult_nsqip_query):
        """Test WORK_RVU_TOTAL calculation."""
        sample = adult_nsqip_query.sample(100)
        
        assert "WORK_RVU_TOTAL" in sample.columns
        
        # Check values are reasonable
        rvu_stats = sample.select(
            pl.col("WORK_RVU_TOTAL").drop_nulls().describe()
        ).to_dicts()[0]
        
        assert rvu_stats["min"] >= 0
        assert rvu_stats["max"] < 1000  # Reasonable upper bound
    
    def test_filter_by_cpt(self, adult_nsqip_query):
        """Test CPT code filtering."""
        # Common laparoscopic procedures
        cpt_codes = ["44970", "44979", "49650", "49651"]
        
        filtered = adult_nsqip_query.filter_by_cpt(cpt_codes)
        count = filtered.count()
        
        assert count > 0
        
        # Verify filter worked correctly
        sample = filtered.sample(min(100, count))
        primary_cpts = sample["CPT"].to_list()
        all_cpts = [cpt for cpt_list in sample["ALL_CPT_CODES"].to_list() 
                    for cpt in cpt_list]
        
        # At least one CPT should match
        for i, row_cpts in enumerate(sample["ALL_CPT_CODES"].to_list()):
            assert any(cpt in cpt_codes for cpt in row_cpts), \
                f"Row {i} has no matching CPT codes"
    
    def test_filter_by_diagnosis(self, adult_nsqip_query):
        """Test diagnosis code filtering."""
        # Common diagnoses
        diag_codes = ["K80.20", "K80.21", "K35.8"]
        
        filtered = adult_nsqip_query.filter_by_diagnosis(diag_codes)
        count = filtered.count()
        
        if count > 0:  # Some years might not have these codes
            sample = filtered.sample(min(100, count))
            
            # Check that at least one diagnosis matches
            for i, row_diags in enumerate(sample["ALL_DIAGNOSIS_CODES"].to_list()):
                assert any(diag in diag_codes for diag in row_diags), \
                    f"Row {i} has no matching diagnosis codes"
    
    def test_filter_by_year(self, adult_nsqip_query):
        """Test year filtering."""
        recent_years = [2020, 2021, 2022]
        
        filtered = adult_nsqip_query.filter_by_year(recent_years)
        sample = filtered.sample(100)
        
        years_in_sample = sample["OPERYR"].unique().to_list()
        assert all(year in ["2020", "2021", "2022"] for year in years_in_sample)
    
    def test_chained_filters(self, adult_nsqip_query):
        """Test chaining multiple filters."""
        result = (adult_nsqip_query
                 .filter_by_year([2022])
                 .filter_by_cpt(["44970"])
                 .count())
        
        assert result >= 0
    
    def test_select_demographics(self, adult_nsqip_query):
        """Test demographic variable selection."""
        demo = adult_nsqip_query.select_demographics()
        sample = demo.sample(10)
        
        # Check for common demographic variables
        expected_cols = ["AGE", "SEX", "RACE"]
        for col in expected_cols:
            assert any(col in c for c in sample.columns), \
                f"No column containing '{col}' found"
    
    def test_select_outcomes(self, adult_nsqip_query):
        """Test outcome variable selection."""
        outcomes = adult_nsqip_query.select_outcomes()
        sample = outcomes.sample(10)
        
        # Check for common outcome variables
        expected_patterns = ["DEATH", "COMPLICATION", "READMISSION"]
        for pattern in expected_patterns:
            assert any(pattern in c for c in sample.columns), \
                f"No column containing '{pattern}' found"
    
    def test_metadata_exists(self):
        """Test that metadata.json exists."""
        metadata_path = ADULT_NSQIP_PATH / "metadata.json"
        assert metadata_path.exists()
        
        with open(metadata_path) as f:
            metadata = json.load(f)
        
        assert "dataset_type" in metadata
        assert metadata["dataset_type"] == "adult"
    
    def test_data_dictionary_exists(self):
        """Test that data dictionary files exist."""
        parent_dir = ADULT_NSQIP_PATH.parent
        
        # At least one format should exist
        dict_files = [
            parent_dir / "adult_data_dictionary.csv",
            parent_dir / "adult_data_dictionary.html",
            parent_dir / "adult_data_dictionary.json"
        ]
        
        assert any(f.exists() for f in dict_files), \
            "No data dictionary files found"


@pytest.mark.integration
@pytest.mark.requires_data
class TestPediatricNSQIP:
    """Test suite for Pediatric NSQIP dataset."""
    
    def test_dataset_loading(self, pediatric_nsqip_query):
        """Test that the dataset loads successfully."""
        assert isinstance(pediatric_nsqip_query, NSQIPQuery)
        info = pediatric_nsqip_query.describe()
        assert info['total_rows'] > 0
        assert info['columns'] > 0
    
    def test_year_coverage(self, pediatric_nsqip_query):
        """Test that expected years are present (2012-2023)."""
        df = (pediatric_nsqip_query.lazy_frame
              .group_by("OPERYR")
              .agg(pl.count())
              .sort("OPERYR")
              .collect())
        
        years = sorted(df["OPERYR"].to_list())
        
        # Pediatric started in 2012
        assert min(years) >= "2012"
        assert max(years) >= "2022"  # Should have at least through 2022
    
    def test_age_handling(self, pediatric_nsqip_query):
        """Test age handling for pediatric data."""
        sample = pediatric_nsqip_query.sample(1000)
        
        # Pediatric data might have different age columns
        age_cols = [col for col in sample.columns if "AGE" in col.upper()]
        assert len(age_cols) > 0, "No age-related columns found"
    
    def test_cpt_filtering(self, pediatric_nsqip_query):
        """Test CPT filtering for pediatric procedures."""
        # Common pediatric procedures
        cpt_codes = ["49500", "49501", "49505", "49507"]  # Hernia repairs
        
        filtered = pediatric_nsqip_query.filter_by_cpt(cpt_codes)
        count = filtered.count()
        
        if count > 0:
            sample = filtered.sample(min(100, count))
            # Verify at least one matching CPT per row
            for row_cpts in sample["ALL_CPT_CODES"].to_list():
                assert any(cpt in cpt_codes for cpt in row_cpts)
    
    def test_metadata_exists(self):
        """Test that metadata.json exists."""
        metadata_path = PEDIATRIC_NSQIP_PATH / "metadata.json"
        assert metadata_path.exists()
        
        with open(metadata_path) as f:
            metadata = json.load(f)
        
        assert "dataset_type" in metadata
        assert metadata["dataset_type"] == "pediatric"


@pytest.mark.integration
@pytest.mark.requires_data
class TestNCDB:
    """Test suite for NCDB dataset."""
    
    def test_dataset_loading(self, ncdb_query):
        """Test that the dataset loads successfully."""
        assert isinstance(ncdb_query, NSQIPQuery)
        info = ncdb_query.describe()
        assert info['total_rows'] > 0
        assert info['columns'] > 0
    
    def test_file_structure(self):
        """Test NCDB file structure (multiple cancer type files)."""
        if not dataset_exists(NCDB_PATH):
            pytest.skip("NCDB dataset not found")
        
        parquet_files = list(NCDB_PATH.glob("*.parquet"))
        assert len(parquet_files) > 0, "No parquet files found"
        
        # NCDB has separate files for different cancer types
        cancer_types = [f.stem for f in parquet_files]
        
        # Should have multiple cancer types
        assert len(cancer_types) > 5
    
    def test_data_dictionary_exists(self):
        """Test that NCDB data dictionary exists."""
        dict_files = [
            NCDB_PATH / "data_dictionary.csv",
            NCDB_PATH / "data_dictionary.html",
            NCDB_PATH / "data_dictionary.json"
        ]
        
        assert any(f.exists() for f in dict_files), \
            "No data dictionary files found"


@pytest.mark.integration
@pytest.mark.requires_data
class TestMemoryEfficiency:
    """Test memory-efficient operations across datasets."""
    
    @pytest.mark.parametrize("dataset_path", [
        ADULT_NSQIP_PATH,
        PEDIATRIC_NSQIP_PATH
    ])
    def test_lazy_evaluation(self, dataset_path):
        """Test that lazy evaluation works without loading all data."""
        if not dataset_exists(dataset_path):
            pytest.skip(f"Dataset not found at {dataset_path}")
        
        query = nsqip_tools.load_data(str(dataset_path))
        
        # Create complex lazy query
        lazy_query = (query.lazy_frame
                     .filter(pl.col("OPERYR").is_in(["2020", "2021", "2022"]))
                     .group_by("OPERYR")
                     .agg(pl.count()))
        
        # This should not load the full dataset
        result = lazy_query.collect()
        assert len(result) <= 3
    
    def test_streaming_collection(self, adult_nsqip_query):
        """Test streaming collection for large queries."""
        # Get aggregate stats without loading full data
        result = (adult_nsqip_query.lazy_frame
                 .select([
                     pl.count().alias("total_cases"),
                     pl.col("OPERYR").n_unique().alias("unique_years")
                 ])
                 .collect(streaming=True))
        
        stats = result.row(0, named=True)
        assert stats["total_cases"] > 0
        assert stats["unique_years"] > 0


@pytest.mark.integration
@pytest.mark.requires_data
class TestRealWorldQueries:
    """Test real-world query patterns."""
    
    def test_tonsillectomy_analysis(self, adult_nsqip_query):
        """Test the tonsillectomy analysis pattern from the notebook."""
        target_cpts = ["42821", "42826"]
        
        # Filter for tonsillectomy cases
        tonsil_cases = adult_nsqip_query.filter_by_cpt(target_cpts)
        
        # Further filter for cases with ONLY these CPT codes
        df = (tonsil_cases.lazy_frame
              .filter(
                  (pl.col("ALL_CPT_CODES").list.len() == 1) &
                  (pl.col("ALL_CPT_CODES").list.first().is_in(target_cpts))
              )
              .collect())
        
        assert len(df) > 0
        
        # All cases should have exactly one CPT code
        assert (df.select(pl.col("ALL_CPT_CODES").list.len())
                .to_series().unique().to_list() == [1])
    
    def test_complex_filtering(self, adult_nsqip_query):
        """Test complex multi-condition filtering."""
        # Elderly patients with gallbladder procedures in recent years
        result = (adult_nsqip_query
                 .filter_by_year([2020, 2021, 2022])
                 .filter_by_cpt(["47562", "47563", "47564"])  # Lap chole
                 .lazy_frame
                 .filter(pl.col("AGE_AS_INT") >= 65)
                 .select([
                     pl.count().alias("total_cases"),
                     pl.col("AGE_AS_INT").mean().alias("mean_age")
                 ])
                 .collect())
        
        if len(result) > 0:
            stats = result.row(0, named=True)
            assert stats["mean_age"] >= 65