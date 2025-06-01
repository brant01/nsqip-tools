"""Tests for the builder module."""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
import nsqip_tools


def test_build_parquet_dataset_validates_inputs(tmp_path):
    """Test that build_parquet_dataset validates inputs correctly."""
    # Test invalid dataset type
    with pytest.raises(ValueError, match="Invalid dataset_type"):
        nsqip_tools.build_parquet_dataset(
            data_dir=tmp_path,
            dataset_type="invalid"  # type: ignore
        )
    
    # Test non-existent directory
    fake_path = tmp_path / "does_not_exist"
    with pytest.raises(ValueError, match="does not exist"):
        nsqip_tools.build_parquet_dataset(
            data_dir=fake_path,
            dataset_type="adult"
        )


def test_build_parquet_dataset_creates_output_dir(tmp_path):
    """Test that build_parquet_dataset creates output directory if needed."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    
    # Create a dummy text file
    (data_dir / "nsqip_test.txt").write_text("col1\tcol2\nval1\tval2")
    
    output_dir = tmp_path / "output"
    
    # Mock the actual parquet creation to avoid needing real data
    with patch('nsqip_tools.builder.create_parquet_from_text'):
        with patch('nsqip_tools.builder.apply_transformations'):
            with patch('nsqip_tools.builder._verify_case_counts'):
                result = nsqip_tools.build_parquet_dataset(
                    data_dir=data_dir,
                    output_dir=output_dir,
                    dataset_type="adult",
                    generate_dictionary=False
                )
    
    assert output_dir.exists()


def test_expected_case_counts():
    """Test that expected case counts are defined correctly."""
    from nsqip_tools.constants import EXPECTED_CASE_COUNTS
    
    # Check structure
    assert "adult" in EXPECTED_CASE_COUNTS
    assert "pediatric" in EXPECTED_CASE_COUNTS
    
    # Check some known values
    assert EXPECTED_CASE_COUNTS["adult"]["2021"] == 983851
    assert EXPECTED_CASE_COUNTS["pediatric"]["2022"] == 146331
    
    # Check 2005/2006 combined year
    assert EXPECTED_CASE_COUNTS["adult"]["2005"] == 152490
    assert EXPECTED_CASE_COUNTS["adult"]["2006"] == 152490