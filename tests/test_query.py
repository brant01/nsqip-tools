"""Tests for the query module."""
from pathlib import Path

import polars as pl
import pytest

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
    df = query.lazy_frame.collect()
    assert len(df) == 4
    assert "CASEID" in df.columns


def test_filter_by_cpt(tmp_path):
    """Test filtering by CPT codes."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    # Single CPT
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_cpt(["44970"])
          .lazy_frame
          .collect())
    assert len(df) == 2
    assert all(df["CPT"] == "44970")

    # Multiple CPTs
    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_cpt(["47562", "47563"])
          .lazy_frame
          .collect())
    assert len(df) == 2


def test_filter_by_year(tmp_path):
    """Test filtering by operation year."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_year([2020])
          .lazy_frame
          .collect())
    assert len(df) == 2
    assert all(df["OPERYR"] == "2020")


def test_filter_by_diagnosis(tmp_path):
    """Test filtering by diagnosis codes."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_diagnosis(["K80.20"])
          .lazy_frame
          .collect())
    assert len(df) == 2


def test_chaining_filters(tmp_path):
    """Test chaining multiple filters."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    df = (nsqip_tools.load_data(dataset_dir)
          .filter_by_year([2021])
          .filter_by_cpt(["44970"])
          .lazy_frame
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
    result_df = query.lazy_frame.collect()
    assert len(result_df) == 2
    assert "CASEID" in result_df.columns


def test_columns_property(tmp_path):
    """Test the columns property returns column names."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    query = nsqip_tools.load_data(dataset_dir)
    cols = query.columns
    assert isinstance(cols, list)
    assert "CASEID" in cols
    assert "OPERYR" in cols
    assert "CPT" in cols


def test_count(tmp_path):
    """Test the count() method returns row count."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    query = nsqip_tools.load_data(dataset_dir)
    assert query.count() == 4

    # After filtering
    filtered = query.filter_by_year([2020])
    assert filtered.count() == 2


def test_sample(tmp_path):
    """Test the sample() method returns a DataFrame."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    query = nsqip_tools.load_data(dataset_dir)
    sample = query.sample(n=2, seed=42)
    assert isinstance(sample, pl.DataFrame)
    assert len(sample) == 2


def test_describe(tmp_path):
    """Test the describe() method returns summary dict."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    query = nsqip_tools.load_data(dataset_dir)
    info = query.describe()
    assert isinstance(info, dict)
    assert info["total_rows"] == 4
    assert isinstance(info["columns"], int)
    assert "parquet_path" in info


def test_repr(tmp_path):
    """Test __repr__ returns a readable string."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    query = nsqip_tools.load_data(dataset_dir)
    repr_str = repr(query)
    assert "NSQIPQuery" in repr_str
    assert "rows=" in repr_str


def test_filter_by_age(tmp_path):
    """Test filter_by_age method on NSQIPQuery."""
    dataset_dir = tmp_path / "test_dataset"
    create_test_parquet_dataset(dataset_dir)

    # Filter by min age
    query = nsqip_tools.load_data(dataset_dir)
    df = query.filter_by_age(min_age=50).collect()
    assert all(df["AGE_AS_INT"] >= 50)

    # Filter by max age
    query = nsqip_tools.load_data(dataset_dir)
    df = query.filter_by_age(max_age=55).collect()
    assert all(df["AGE_AS_INT"] <= 55)


def test_filter_elective(tmp_path):
    """Test filter_elective method on NSQIPQuery."""
    dataset_dir = tmp_path / "test_dataset"
    dataset_dir.mkdir(exist_ok=True)

    df = pl.DataFrame({
        "CASEID": ["1", "2", "3"],
        "OPERYR": ["2020", "2020", "2020"],
        "EMERGENT": ["No", "Yes", "No"],
    })
    df.write_parquet(dataset_dir / "test.parquet")

    query = nsqip_tools.load_data(dataset_dir)
    result = query.filter_elective().collect()
    assert len(result) == 2
