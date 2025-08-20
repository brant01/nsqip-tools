"""Tests for the data_dictionary module.

These tests use synthetic data that does not represent real medical data
and should not be committed to public repositories.
"""

import pytest
import polars as pl
from pathlib import Path
import json
import tempfile
from unittest.mock import patch, mock_open, MagicMock

from nsqip_tools.data_dictionary import (
    generate_data_dictionary,
    DataDictionaryGenerator,
)


@pytest.fixture
def sample_parquet_data(tmp_path):
    """Create sample parquet files for testing."""
    # Create sample data with various data types
    df1 = pl.DataFrame({
        "id": ["1", "2", "3"],
        "category": ["A", "B", "A"],
        "value": ["10.5", "20.0", "15.5"],
        "flag": ["1", "0", "1"],
        "empty_col": [None, None, None],
        "OPERYR": ["2020", "2021", "2020"]  # Use expected column name
    })
    
    df2 = pl.DataFrame({
        "id": ["4", "5", "6"],
        "category": ["B", "C", "A"],
        "value": ["25.0", "30.5", None],
        "flag": ["0", "1", "0"],
        "empty_col": [None, None, None],
        "OPERYR": ["2021", "2021", "2022"]  # Use expected column name
    })
    
    parquet_dir = tmp_path / "test_dataset"
    parquet_dir.mkdir()
    
    df1.write_parquet(parquet_dir / "data_2020.parquet")
    df2.write_parquet(parquet_dir / "data_2021.parquet")
    
    # Create metadata
    metadata = {
        "dataset_type": "test",
        "created": "2024-01-01",
        "files": ["data_2020.parquet", "data_2021.parquet"]
    }
    
    with open(parquet_dir / "metadata.json", "w") as f:
        json.dump(metadata, f)
    
    return parquet_dir


class TestDataDictionaryGenerator:
    """Test DataDictionaryGenerator class."""
    
    def test_initialization(self, sample_parquet_data):
        """Test generator initialization."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        
        assert generator.parquet_dir == Path(sample_parquet_data)
        assert generator.batch_size == 50
        assert len(generator.parquet_files) == 2
        assert hasattr(generator, 'columns')
        assert hasattr(generator, 'dtypes')
        assert hasattr(generator, 'total_rows')
    
    def test_get_all_columns(self, sample_parquet_data):
        """Test getting all unique columns across files."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        columns = generator.columns  # Use the public attribute instead
        
        expected_columns = {"id", "category", "value", "flag", "empty_col", "OPERYR"}
        assert set(columns) == expected_columns
    
    def test_analyze_column(self, sample_parquet_data):
        """Test column analysis."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        
        # Test numeric column
        stats = generator._analyze_column("value")
        
        assert stats["column_name"] == "value"
        assert stats["data_type"] in ["Utf8", "String"]  # Raw data is string
        assert stats["total_rows"] == 6
        assert stats["null_count"] == 1
        assert stats["null_percentage"] > 0
    
    def test_analyze_categorical_column(self, sample_parquet_data):
        """Test analysis of categorical column."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        
        stats = generator._analyze_column("category")
        
        assert stats["column_name"] == "category"
        assert stats["null_count"] == 0
        assert "value_counts" in stats
        
        # Should have top values
        value_counts = stats["value_counts"]
        assert "A" in value_counts
        assert "B" in value_counts
    
    def test_analyze_empty_column(self, sample_parquet_data):
        """Test analysis of completely null column."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        
        stats = generator._analyze_column("empty_col")
        
        assert stats["column_name"] == "empty_col"
        assert stats["null_count"] == 6
        assert stats["null_percentage"] == 100.0
    
    def test_generate_csv(self, sample_parquet_data, tmp_path):
        """Test CSV generation."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        csv_path = tmp_path / "test_dict.csv"
        
        generator.generate_csv(csv_path)
        
        assert csv_path.exists()
        
        # Read and verify CSV structure
        df = pl.read_csv(csv_path)
        assert "column_name" in df.columns
        assert "data_type" in df.columns
        assert "null_count" in df.columns
        assert len(df) == 6  # Should have all columns
    
    def test_generate_json(self, sample_parquet_data, tmp_path):
        """Test JSON generation."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        json_path = tmp_path / "test_dict.json"
        
        generator.generate_json(json_path)
        
        assert json_path.exists()
        
        # Read and verify JSON structure
        with open(json_path) as f:
            data = json.load(f)
        
        assert "metadata" in data
        assert "columns" in data
        assert len(data["columns"]) == 6
        
        # Check column structure
        column = data["columns"][0]
        assert "column_name" in column
        assert "data_type" in column
        assert "statistics" in column
    
    def test_generate_html(self, sample_parquet_data, tmp_path):
        """Test HTML generation."""
        generator = DataDictionaryGenerator(sample_parquet_data)
        html_path = tmp_path / "test_dict.html"
        
        generator.generate_html(html_path)
        
        assert html_path.exists()
        
        # Read and verify basic HTML structure
        with open(html_path) as f:
            html_content = f.read()
        
        assert "<html>" in html_content
        assert "<table>" in html_content
        assert "column_name" in html_content
        assert "data_type" in html_content


class TestGenerateDataDictionary:
    """Test the main generate_data_dictionary function."""
    
    def test_generate_all_formats(self, sample_parquet_data):
        """Test generation of all formats."""
        generate_data_dictionary(
            sample_parquet_data,
            output_format="all"
        )
        
        # Check that files were created
        expected_files = [
            "test_data_dictionary.csv",
            "test_data_dictionary.json", 
            "test_data_dictionary.html"
        ]
        
        for filename in expected_files:
            file_path = sample_parquet_data / filename
            assert file_path.exists(), f"Missing file: {filename}"
    
    def test_generate_csv_only(self, sample_parquet_data):
        """Test generation of CSV format only."""
        generate_data_dictionary(
            sample_parquet_data,
            output_format="csv"
        )
        
        csv_path = sample_parquet_data / "adult_data_dictionary.csv"
        assert csv_path.exists()
        
        # Should not create other formats
        json_path = sample_parquet_data / "adult_data_dictionary.json"
        html_path = sample_parquet_data / "adult_data_dictionary.html"
        assert not json_path.exists()
        assert not html_path.exists()
    
    def test_custom_output_dir(self, sample_parquet_data, tmp_path):
        """Test using custom output directory."""
        output_dir = tmp_path / "custom_output"
        
        generate_data_dictionary(
            sample_parquet_data,
            output_format="csv",
            output_dir=output_dir
        )
        
        csv_path = output_dir / "adult_data_dictionary.csv"
        assert csv_path.exists()
    
    def test_invalid_format(self, sample_parquet_data):
        """Test error handling for invalid format."""
        with pytest.raises(ValueError, match="Invalid output format"):
            generate_data_dictionary(
                sample_parquet_data,
                output_format="invalid"
            )


# Note: Helper functions like _calculate_column_stats and _format_value_counts 
# are internal implementation details and may not be exposed for testing


class TestErrorHandling:
    """Test error handling in data dictionary generation."""
    
    def test_missing_parquet_directory(self):
        """Test error when parquet directory doesn't exist."""
        with pytest.raises(FileNotFoundError):
            DataDictionaryGenerator("nonexistent/path")
    
    def test_no_parquet_files(self, tmp_path):
        """Test error when no parquet files found."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        with pytest.raises(ValueError, match="No parquet files found"):
            DataDictionaryGenerator(empty_dir)
    
    def test_corrupted_parquet_file(self, tmp_path):
        """Test handling of corrupted parquet files."""
        parquet_dir = tmp_path / "corrupted"
        parquet_dir.mkdir()
        
        # Create a fake "parquet" file
        fake_parquet = parquet_dir / "fake.parquet"
        fake_parquet.write_text("not a parquet file")
        
        # Should handle the error gracefully
        generator = DataDictionaryGenerator(parquet_dir)
        
        # Analyzing columns should handle read errors
        with pytest.raises(Exception):  # Will raise polars error
            generator._analyze_column("nonexistent")
    
    def test_memory_efficiency(self, sample_parquet_data):
        """Test that large datasets are handled efficiently."""
        generator = DataDictionaryGenerator(sample_parquet_data, batch_size=1)
        
        # Should work even with very small batch size
        stats = generator._analyze_column("category")
        assert stats["column_name"] == "category"


class TestIntegrationWithRealData:
    """Integration tests that would work with real data during development."""
    
    @pytest.mark.integration
    def test_adult_nsqip_dictionary(self):
        """Test with actual adult NSQIP data if available."""
        adult_path = Path("data/adult_nsqip_parquet")
        
        if not adult_path.exists():
            pytest.skip("Adult NSQIP data not available")
        
        # Test basic functionality without creating files
        generator = DataDictionaryGenerator(adult_path)
        columns = generator._get_all_columns()
        
        # Should have expected NSQIP columns
        expected_columns = {"CASEID", "OPERYR", "AGE", "CPT"}
        assert expected_columns.issubset(set(columns))
    
    @pytest.mark.integration  
    def test_pediatric_nsqip_dictionary(self):
        """Test with actual pediatric NSQIP data if available."""
        peds_path = Path("data/pediatric_nsqip_parquet")
        
        if not peds_path.exists():
            pytest.skip("Pediatric NSQIP data not available")
        
        generator = DataDictionaryGenerator(peds_path)
        columns = generator._get_all_columns()
        
        # Should have expected pediatric columns
        expected_columns = {"CASEID", "OPERYR", "CPT"}
        assert expected_columns.issubset(set(columns))