"""Tests for the query module."""
import pytest
from pathlib import Path
import polars as pl
import nsqip_tools
from nsqip_tools.query import NSQIPQuery


def create_test_parquet_dataset(dataset_dir: Path) -> None:
    """Create a test parquet dataset with sample data."""
    dataset_dir.mkdir(exist_ok=True)
    
    # Create sample data
    df = pl.DataFrame({
        "CASEID": ["1", "2", "3", "4"],
        "OPERYR": ["2020", "2020", "2021", "2021"],
        "AGE": ["45", "60", "90+", "55"],
        "CPT": ["44970", "47562", "44970", "47563"],
        "PODIAG": ["K80.20", "K80.21", "K81", "K80.20"],
        "ALL_CPT_CODES": [["44970"], ["47562"], ["44970", "12345"], ["47563"]],
        "AGE_AS_INT": [45, 60, 90, 55]
    })
    
    # Split by year and save as separate parquet files
    for year in ["2020", "2021"]:
        year_df = df.filter(pl.col("OPERYR") == year)
        parquet_path = dataset_dir / f"adult_{year}.parquet"
        year_df.write_parquet(parquet_path)


def test_load_data(tmp_path):
    """Test loading data from parquet dataset."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    query = nsqip_tools.load_data(dataset_dir)
    assert isinstance(query, NSQIPQuery)
    
    # Test collecting all data
    df = query.collect()
    assert len(df) == 4
    assert "CASEID" in df.columns


def test_filter_by_cpt(tmp_path):
    """Test filtering by CPT codes."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    # Single CPT
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_cpt(["44970"])
          .collect())
    assert len(df) == 2
    assert all(df["CPT"] == "44970")
    
    # Multiple CPTs
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_cpt(["47562", "47563"])
          .collect())
    assert len(df) == 2


def test_filter_by_year(tmp_path):
    """Test filtering by operation year."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_year([2020])
          .collect())
    assert len(df) == 2
    assert all(df["OPERYR"] == "2020")


def test_filter_by_diagnosis(tmp_path):
    """Test filtering by diagnosis codes."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_diagnosis(["K80.20"])
          .collect())
    assert len(df) == 2


def test_chaining_filters(tmp_path):
    """Test chaining multiple filters."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_year([2021])
          .filter_by_cpt(["44970"])
          .collect())
    assert len(df) == 1
    assert df["CASEID"][0] == "3"


def test_integration_with_polars(tmp_path):
    """Test integration with Polars operations."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_year([2020])
          .lazy_frame
          .select(["CASEID", "AGE_AS_INT"])
          .filter(pl.col("AGE_AS_INT") > 50)
          .collect())
    
    assert len(df) == 1
    assert df["AGE_AS_INT"][0] == 60


def test_nonexistent_dataset():
    """Test error handling for non-existent dataset."""
    with pytest.raises(FileNotFoundError):
        nsqip_tools.load_data("does_not_exist")


def test_count_method(tmp_path):
    """Test the count method."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    query = nsqip_tools.load_data(dataset_dir)
    total_count = query.count()
    assert total_count == 4
    
    # Test count with filters
    filtered_count = query.filter_by_year([2020]).count()
    assert filtered_count == 2


def test_sample_method(tmp_path):
    """Test the sample method."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    query = nsqip_tools.load_data(dataset_dir)
    
    # Sample more than available - should return all
    sample_df = query.sample(n=10)
    assert len(sample_df) == 4
    
    # Sample less than available
    sample_df = query.sample(n=2, seed=42)
    assert len(sample_df) == 2


def test_describe_method(tmp_path):
    """Test the describe method."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    query = nsqip_tools.load_data(dataset_dir)
    info = query.describe()
    
    assert info["total_rows"] == 4
    assert info["columns"] == 7  # Number of columns in test data
    assert info["parquet_files"] == 2  # 2020 and 2021 files
    assert str(dataset_dir) in str(info["path"])


def test_select_demographics(tmp_path):
    """Test select_demographics method."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)
    
    query = (nsqip_tools.load_data(dataset_dir)
             .select_demographics())
    
    df = query.collect()
    # Should only have demographic columns that exist in our test data
    assert "CASEID" in df.columns
    assert "OPERYR" in df.columns
    assert "AGE" in df.columns
    # Other demo columns won't be present in our simple test data


def test_single_parquet_file(tmp_path):
    """Test loading a single parquet file."""
    # Create a single parquet file
    df = pl.DataFrame({
        "CASEID": ["1", "2"],
        "OPERYR": ["2020", "2020"],
        "CPT": ["44970", "47562"],
    })
    
    parquet_file = tmp_path / "single_file.parquet"
    df.write_parquet(parquet_file)
    
    query = nsqip_tools.load_data(parquet_file)
    result_df = query.collect()
    assert len(result_df) == 2
    assert "CASEID" in result_df.columns