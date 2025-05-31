"""Tests for the query module."""
import pytest
from pathlib import Path
import duckdb
import polars as pl
import nsqip_tools
from nsqip_tools.query import NSQIPQuery


def create_test_db(db_path: Path) -> None:
    """Create a test database with sample data."""
    with duckdb.connect(str(db_path)) as con:
        # Create table
        con.execute("""
            CREATE TABLE nsqip (
                CASEID TEXT,
                OPERYR TEXT,
                AGE TEXT,
                CPT TEXT,
                PODIAG TEXT,
                ALL_CPT_CODES TEXT[],
                AGE_AS_INT DOUBLE
            )
        """)
        
        # Insert test data
        con.execute("""
            INSERT INTO nsqip VALUES
            ('1', '2020', '45', '44970', 'K80.20', ['44970'], 45),
            ('2', '2020', '60', '47562', 'K80.21', ['47562'], 60),
            ('3', '2021', '90+', '44970', 'K81', ['44970', '12345'], 90),
            ('4', '2021', '55', '47563', 'K80.20', ['47563'], 55)
        """)


def test_load_data(tmp_path):
    """Test loading data from database."""
    db_path = tmp_path / "test.duckdb"
    create_test_db(db_path)
    
    query = nsqip_tools.load_data(db_path)
    assert isinstance(query, NSQIPQuery)
    
    # Test collecting all data
    df = query.collect()
    assert len(df) == 4
    assert "CASEID" in df.columns


def test_filter_by_cpt(tmp_path):
    """Test filtering by CPT codes."""
    db_path = tmp_path / "test.duckdb"
    create_test_db(db_path)
    
    # Single CPT
    df = (nsqip_tools.load_data(db_path)
          .filter_by_cpt(["44970"])
          .collect())
    assert len(df) == 2
    assert all(df["CPT"] == "44970")
    
    # Multiple CPTs
    df = (nsqip_tools.load_data(db_path)
          .filter_by_cpt(["47562", "47563"])
          .collect())
    assert len(df) == 2


def test_filter_by_year(tmp_path):
    """Test filtering by operation year."""
    db_path = tmp_path / "test.duckdb"
    create_test_db(db_path)
    
    df = (nsqip_tools.load_data(db_path)
          .filter_by_year([2020])
          .collect())
    assert len(df) == 2
    assert all(df["OPERYR"] == "2020")


def test_filter_by_diagnosis(tmp_path):
    """Test filtering by diagnosis codes."""
    db_path = tmp_path / "test.duckdb"
    create_test_db(db_path)
    
    df = (nsqip_tools.load_data(db_path)
          .filter_by_diagnosis(["K80.20"])
          .collect())
    assert len(df) == 2


def test_chaining_filters(tmp_path):
    """Test chaining multiple filters."""
    db_path = tmp_path / "test.duckdb"
    create_test_db(db_path)
    
    df = (nsqip_tools.load_data(db_path)
          .filter_by_year([2021])
          .filter_by_cpt(["44970"])
          .collect())
    assert len(df) == 1
    assert df["CASEID"][0] == "3"


def test_integration_with_polars(tmp_path):
    """Test integration with Polars operations."""
    db_path = tmp_path / "test.duckdb"
    create_test_db(db_path)
    
    df = (nsqip_tools.load_data(db_path)
          .filter_by_year([2020])
          .lazy_frame
          .select(["CASEID", "AGE_AS_INT"])
          .filter(pl.col("AGE_AS_INT") > 50)
          .collect())
    
    assert len(df) == 1
    assert df["AGE_AS_INT"][0] == 60


def test_nonexistent_database():
    """Test error handling for non-existent database."""
    with pytest.raises(FileNotFoundError):
        nsqip_tools.load_data("does_not_exist.duckdb")