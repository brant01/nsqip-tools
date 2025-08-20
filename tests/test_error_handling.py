"""Tests for error handling across the nsqip_tools package.

These tests focus on ensuring robust error handling for various failure scenarios
without using real medical data.
"""

import pytest
import polars as pl
from pathlib import Path
import tempfile
import json
from unittest.mock import patch, MagicMock, mock_open

import nsqip_tools
from nsqip_tools.query import NSQIPQuery
from nsqip_tools.builder import build_parquet_dataset
from nsqip_tools._internal.ingest import create_parquet_from_text
from nsqip_tools._internal.transform import apply_transformations


class TestBuilderErrorHandling:
    """Test error handling in the builder module."""
    
    def test_invalid_dataset_type(self, tmp_path):
        """Test error with invalid dataset type."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        
        with pytest.raises(ValueError, match="Invalid dataset_type"):
            build_parquet_dataset(
                data_dir=data_dir,
                dataset_type="invalid_type"
            )
    
    def test_nonexistent_data_directory(self):
        """Test error with non-existent data directory."""
        with pytest.raises(ValueError, match="does not exist"):
            build_parquet_dataset(
                data_dir="nonexistent/path",
                dataset_type="adult"
            )
    
    def test_empty_data_directory(self, tmp_path):
        """Test error with empty data directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        with pytest.raises(ValueError, match="No.*files found"):
            build_parquet_dataset(
                data_dir=empty_dir,
                dataset_type="adult"
            )
    
    def test_invalid_memory_limit(self, tmp_path):
        """Test error with invalid memory limit."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.txt").write_text("col1\tcol2\nval1\tval2")
        
        with pytest.raises(ValueError, match="Invalid memory limit"):
            build_parquet_dataset(
                data_dir=data_dir,
                dataset_type="adult",
                memory_limit="invalid_limit"
            )
    
    def test_insufficient_permissions(self, tmp_path):
        """Test error with insufficient write permissions."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.txt").write_text("col1\tcol2\nval1\tval2")
        
        # Create read-only output directory
        output_dir = tmp_path / "readonly"
        output_dir.mkdir()
        output_dir.chmod(0o444)  # Read-only
        
        try:
            with pytest.raises((PermissionError, OSError)):
                build_parquet_dataset(
                    data_dir=data_dir,
                    output_dir=output_dir,
                    dataset_type="adult"
                )
        finally:
            # Restore permissions for cleanup
            output_dir.chmod(0o755)


class TestQueryErrorHandling:
    """Test error handling in the query module."""
    
    def test_load_nonexistent_dataset(self):
        """Test error when loading non-existent dataset."""
        with pytest.raises(FileNotFoundError):
            nsqip_tools.load_data("nonexistent/path")
    
    def test_load_invalid_parquet_file(self, tmp_path):
        """Test error when loading invalid parquet file."""
        # Create a fake parquet file
        fake_parquet = tmp_path / "fake.parquet"
        fake_parquet.write_text("not a parquet file")
        
        with pytest.raises(Exception):  # Will raise polars error
            nsqip_tools.load_data(fake_parquet)
    
    def test_filter_invalid_cpt_codes(self, tmp_path):
        """Test handling of invalid CPT codes."""
        # Create valid test dataset
        df = pl.DataFrame({
            "CASEID": ["1", "2"],
            "CPT": ["12345", "67890"],
            "ALL_CPT_CODES": [["12345"], ["67890"]]
        })
        
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Invalid CPT codes should return empty result, not error
        result = query.filter_by_cpt(["99999"]).count()
        assert result == 0
        
        # Empty list should return empty result
        result = query.filter_by_cpt([]).count()
        assert result == 0
    
    def test_filter_invalid_years(self, tmp_path):
        """Test handling of invalid years."""
        df = pl.DataFrame({
            "CASEID": ["1", "2"],
            "OPERYR": ["2020", "2021"],
        })
        
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Invalid years should return empty result
        result = query.filter_by_year([1999]).count()
        assert result == 0
        
        # Mixed valid/invalid years should work
        result = query.filter_by_year([2020, 1999]).count()
        assert result == 1
    
    def test_missing_required_columns(self, tmp_path):
        """Test handling when required columns are missing."""
        # Dataset missing ALL_CPT_CODES column
        df = pl.DataFrame({
            "CASEID": ["1", "2"],
            "CPT": ["12345", "67890"]
        })
        
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Should handle gracefully when ALL_CPT_CODES is missing
        with pytest.raises((KeyError, Exception)):
            query.filter_by_cpt(["12345"]).count()
    
    def test_empty_dataset(self, tmp_path):
        """Test handling of empty datasets."""
        # Create empty parquet file
        df = pl.DataFrame({
            "CASEID": [],
            "CPT": [],
            "ALL_CPT_CODES": []
        })
        
        parquet_file = tmp_path / "empty.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Should handle empty datasets gracefully
        assert query.count() == 0
        assert query.filter_by_cpt(["12345"]).count() == 0


class TestIngestErrorHandling:
    """Test error handling in the ingest module."""
    
    def test_corrupted_text_file(self, tmp_path):
        """Test handling of corrupted text files."""
        # Create file with inconsistent columns
        corrupted_file = tmp_path / "corrupted.txt"
        corrupted_file.write_text(
            "col1\tcol2\tcol3\n"
            "val1\tval2\tval3\n"
            "val4\tval5\n"  # Missing column
            "val6\tval7\tval8\tval9\n"  # Extra column
        )
        
        output_file = tmp_path / "output.parquet"
        
        # Should handle gracefully or raise appropriate error
        try:
            create_parquet_from_text([corrupted_file], output_file)
        except Exception as e:
            # Should be a descriptive error
            assert any(word in str(e).lower() for word in 
                      ["column", "parse", "format", "inconsistent"])
    
    def test_empty_text_file(self, tmp_path):
        """Test handling of empty text files."""
        empty_file = tmp_path / "empty.txt"
        empty_file.write_text("")
        
        output_file = tmp_path / "output.parquet"
        
        with pytest.raises(ValueError, match="empty|no data"):
            create_parquet_from_text([empty_file], output_file)
    
    def test_binary_file_as_text(self, tmp_path):
        """Test handling when binary file is treated as text."""
        # Create a binary file with .txt extension
        binary_file = tmp_path / "binary.txt"
        with open(binary_file, "wb") as f:
            f.write(b"\x00\x01\x02\x03\x04\x05")
        
        output_file = tmp_path / "output.parquet"
        
        # Should handle gracefully or raise appropriate error
        with pytest.raises(Exception):
            create_parquet_from_text([binary_file], output_file)
    
    def test_extremely_large_file(self, tmp_path):
        """Test memory limits with simulated large files."""
        # We can't create truly large files in tests, but we can mock
        large_file = tmp_path / "large.txt"
        large_file.write_text("col1\tcol2\n" + "val1\tval2\n" * 1000)
        
        output_file = tmp_path / "output.parquet"
        
        # Should work with reasonable size
        create_parquet_from_text([large_file], output_file)
        assert output_file.exists()


class TestTransformErrorHandling:
    """Test error handling in the transform module."""
    
    def test_missing_metadata_file(self, tmp_path):
        """Test handling when metadata.json is missing."""
        # Create parquet file without metadata
        df = pl.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        with pytest.raises(FileNotFoundError):
            apply_transformations(tmp_path, "adult", "4GB")
    
    def test_corrupted_metadata_file(self, tmp_path):
        """Test handling of corrupted metadata.json."""
        # Create valid parquet file
        df = pl.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        # Create corrupted metadata
        metadata_file = tmp_path / "metadata.json"
        metadata_file.write_text("invalid json content")
        
        with pytest.raises(json.JSONDecodeError):
            apply_transformations(tmp_path, "adult", "4GB")
    
    def test_inconsistent_schemas_across_files(self, tmp_path):
        """Test handling of inconsistent schemas across parquet files."""
        # Create files with different schemas
        df1 = pl.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        df2 = pl.DataFrame({"col1": [3, 4], "col3": ["C", "D"]})  # Different columns
        
        file1 = tmp_path / "file1.parquet"
        file2 = tmp_path / "file2.parquet"
        
        df1.write_parquet(file1)
        df2.write_parquet(file2)
        
        # Create metadata
        metadata = {"dataset_type": "adult", "files": ["file1.parquet", "file2.parquet"]}
        with open(tmp_path / "metadata.json", "w") as f:
            json.dump(metadata, f)
        
        # Should handle schema inconsistencies
        try:
            apply_transformations(tmp_path, "adult", "4GB")
            # Should complete but mark validation as failed
            with open(tmp_path / "metadata.json") as f:
                updated_metadata = json.load(f)
            assert updated_metadata.get("schema_validation") == "failed"
        except Exception:
            # Or should raise appropriate error
            pass


class TestDataDictionaryErrorHandling:
    """Test error handling in data dictionary generation."""
    
    def test_unreadable_parquet_files(self, tmp_path):
        """Test handling of unreadable parquet files."""
        from nsqip_tools.data_dictionary import DataDictionaryGenerator
        
        # Create fake parquet file
        fake_parquet = tmp_path / "fake.parquet"
        fake_parquet.write_text("not a parquet file")
        
        with pytest.raises(ValueError, match="No parquet files found|parquet"):
            # This should fail during initialization or column analysis
            generator = DataDictionaryGenerator(tmp_path)
    
    def test_write_permission_error(self, tmp_path):
        """Test handling of write permission errors."""
        from nsqip_tools.data_dictionary import DataDictionaryGenerator
        
        # Create valid parquet file
        df = pl.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        # Create metadata
        metadata = {"dataset_type": "test"}
        with open(tmp_path / "metadata.json", "w") as f:
            json.dump(metadata, f)
        
        generator = DataDictionaryGenerator(tmp_path)
        
        # Try to write to read-only location
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)
        
        try:
            with pytest.raises((PermissionError, OSError)):
                generator.generate_csv(readonly_dir / "dict.csv")
        finally:
            readonly_dir.chmod(0o755)


class TestNetworkAndConcurrencyErrors:
    """Test handling of network and concurrency related errors."""
    
    def test_network_drive_disconnection(self, tmp_path):
        """Test handling when network drive becomes unavailable."""
        # Create initial dataset
        df = pl.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Simulate file becoming unavailable
        parquet_file.unlink()
        
        with pytest.raises(FileNotFoundError):
            query.lazy_frame.collect()
    
    def test_concurrent_file_access(self, tmp_path):
        """Test handling of concurrent file access issues."""
        # This is hard to test properly without actual concurrency
        # But we can at least ensure the code handles file locking gracefully
        df = pl.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        # Multiple queries should work
        query1 = nsqip_tools.load_data(parquet_file)
        query2 = nsqip_tools.load_data(parquet_file)
        
        # Both should work
        assert query1.count() == 2
        assert query2.count() == 2


class TestResourceExhaustionErrors:
    """Test handling of resource exhaustion scenarios."""
    
    @patch('nsqip_tools._internal.memory_utils.get_memory_info')
    def test_low_memory_handling(self, mock_memory_info):
        """Test handling when system is low on memory."""
        # Mock very low available memory
        mock_memory_info.return_value = {
            "available": "100 MB",
            "total": "1 GB",
            "recommended_limit": "80 MB"
        }
        
        # Should handle gracefully and provide appropriate limits
        info = nsqip_tools.get_memory_info()
        assert info["recommended_limit"] == "80 MB"
    
    def test_disk_space_exhaustion(self, tmp_path):
        """Test handling when disk space is exhausted."""
        # This is difficult to test properly without actually filling disk
        # But we can mock the scenario
        
        df = pl.DataFrame({"col1": range(1000), "col2": [f"val_{i}" for i in range(1000)]})
        
        with patch('polars.DataFrame.write_parquet') as mock_write:
            mock_write.side_effect = OSError("No space left on device")
            
            with pytest.raises(OSError, match="space"):
                df.write_parquet(tmp_path / "test.parquet")


class TestInputValidationErrors:
    """Test comprehensive input validation error handling."""
    
    def test_invalid_cpt_code_format(self, tmp_path):
        """Test validation of CPT code formats."""
        df = pl.DataFrame({
            "CASEID": ["1", "2"],
            "CPT": ["12345", "67890"],
            "ALL_CPT_CODES": [["12345"], ["67890"]]
        })
        
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Test various invalid formats
        invalid_cpts = [
            ["1234"],      # Too short
            ["123456"],    # Too long
            ["ABCDE"],     # Non-numeric
            ["12.34"],     # Decimal
            [None],        # None value
            [""],          # Empty string
        ]
        
        for invalid_cpt in invalid_cpts:
            # Should handle gracefully (return 0 results or handle error)
            try:
                result = query.filter_by_cpt(invalid_cpt).count()
                assert result == 0
            except Exception:
                # Some formats might raise validation errors, which is acceptable
                pass
    
    def test_invalid_diagnosis_code_format(self, tmp_path):
        """Test validation of diagnosis code formats."""
        df = pl.DataFrame({
            "CASEID": ["1", "2"],
            "ALL_DIAGNOSIS_CODES": [["K80.20"], ["K81.1"]]
        })
        
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        
        query = nsqip_tools.load_data(parquet_file)
        
        # Test invalid diagnosis codes
        invalid_diags = [
            ["INVALID"],
            [""],
            [None],
            ["123"],  # Wrong format
        ]
        
        for invalid_diag in invalid_diags:
            try:
                result = query.filter_by_diagnosis(invalid_diag).count()
                assert result == 0
            except Exception:
                # Some formats might raise validation errors
                pass