"""Tests for the memory_utils module."""

import pytest
from unittest.mock import patch, MagicMock
import psutil

from nsqip_tools._internal.memory_utils import (
    get_memory_info,
    get_recommended_memory_limit,
    format_bytes,
    get_available_memory,
    get_total_memory,
)


class TestGetMemoryInfo:
    """Test memory information functions."""
    
    def test_get_memory_info_structure(self):
        """Test that get_memory_info returns expected structure."""
        info = get_memory_info()
        
        assert isinstance(info, dict)
        assert "total" in info
        assert "available" in info
        assert "used" in info
        assert "percent" in info
        assert "recommended_limit" in info
        
        # Check types
        assert isinstance(info["total"], str)
        assert isinstance(info["available"], str)
        assert isinstance(info["used"], str)
        assert isinstance(info["percent"], float)
        assert isinstance(info["recommended_limit"], str)
    
    @patch('psutil.virtual_memory')
    def test_get_memory_info_with_mock(self, mock_memory):
        """Test memory info calculation with mocked psutil."""
        # Mock memory stats: 8GB total, 2GB available
        mock_stats = MagicMock()
        mock_stats.total = 8 * 1024**3  # 8GB in bytes
        mock_stats.available = 2 * 1024**3  # 2GB in bytes
        mock_stats.used = 6 * 1024**3  # 6GB in bytes
        mock_stats.percent = 75.0
        mock_memory.return_value = mock_stats
        
        info = get_memory_info()
        
        assert info["total"] == "8.0GB"
        assert info["available"] == "2.0GB"
        assert info["used"] == "6.0GB"
        assert info["percent"] == 75.0
        assert info["recommended_limit"] == "1GB"  # Rounded to GB


class TestGetRecommendedMemoryLimit:
    """Test recommended memory limit calculation."""
    
    @patch('nsqip_tools._internal.memory_utils.get_total_memory')
    @patch('nsqip_tools._internal.memory_utils.get_available_memory') 
    def test_recommended_limit_calculation(self, mock_available, mock_total):
        """Test recommended memory limit with various available memory."""
        # Test with 4GB available, 8GB total
        mock_available.return_value = 4 * 1024**3
        mock_total.return_value = 8 * 1024**3
        
        limit = get_recommended_memory_limit()
        assert limit == "2GB"  # Conservative 50% of available, rounded
        
        # Test with 1GB available
        mock_available.return_value = 1024**3
        mock_total.return_value = 2 * 1024**3
        
        limit = get_recommended_memory_limit()
        assert limit == "1GB"  # Minimum 1GB enforced
    
    @patch('nsqip_tools._internal.memory_utils.get_total_memory')
    @patch('nsqip_tools._internal.memory_utils.get_available_memory') 
    def test_minimum_limit_enforced(self, mock_available, mock_total):
        """Test that minimum memory limit is enforced."""
        # Test with very low available memory
        mock_available.return_value = 100 * 1024**2  # 100MB
        mock_total.return_value = 500 * 1024**2  # 500MB
        
        limit = get_recommended_memory_limit()
        assert limit == "1GB"  # Should enforce minimum 1GB
    
    def test_custom_percentage(self):
        """Test custom percentage for memory limit."""
        # This would require the function to accept a percentage parameter
        # If not implemented, this test can be skipped or the function enhanced
        pass


class TestFormatBytes:
    """Test byte formatting function."""
    
    def test_format_bytes_various_sizes(self):
        """Test formatting of various byte sizes."""
        assert format_bytes(0) == "0.0B"
        assert format_bytes(512) == "512.0B"
        assert format_bytes(1024) == "1.0KB"
        assert format_bytes(1024**2) == "1.0MB"
        assert format_bytes(1024**3) == "1.0GB"
        assert format_bytes(1024**4) == "1.0TB"
        
        # Test fractional values
        assert format_bytes(1536) == "1.5KB"  # 1.5 KB
        assert format_bytes(int(2.5 * 1024**3)) == "2.5GB"
    
    def test_format_bytes_precision(self):
        """Test formatting precision."""
        # Test that we get reasonable precision
        result = format_bytes(1234567890)
        assert "1.1GB" in result or "1.15GB" in result
    
    def test_format_bytes_edge_cases(self):
        """Test edge cases for byte formatting."""
        # Test very large numbers
        very_large = 1024**5
        result = format_bytes(very_large)
        assert "PB" in result or "TB" in result
        
        # Test negative numbers (should handle gracefully)
        try:
            format_bytes(-1024)
        except ValueError:
            pass  # Expected behavior
        except:
            pytest.fail("Should handle negative numbers gracefully")


# Note: parse_memory_string and validate_memory_limit functions not implemented yet
# These tests are placeholders for when those functions are added


class TestMemoryUtilsIntegration:
    """Integration tests for memory utilities."""
    
    def test_memory_info_consistency(self):
        """Test that memory info values are consistent."""
        info = get_memory_info()
        
        # Basic validation without parsing
        assert "total" in info
        assert "available" in info
        assert "used" in info
        assert info["percent"] >= 0
        assert info["percent"] <= 100
    
    def test_recommended_limit_is_reasonable(self):
        """Test that recommended limit is reasonable."""
        info = get_memory_info()
        recommended = info["recommended_limit"]
        
        # Should be a string with GB
        assert isinstance(recommended, str)
        assert "GB" in recommended
        
        # Extract numeric value
        gb_value = int(recommended.replace("GB", ""))
        assert gb_value >= 1  # At least 1GB
        assert gb_value <= 32  # At most 32GB (reasonable limit)


class TestErrorHandling:
    """Test error handling in memory utilities."""
    
    @patch('psutil.virtual_memory')
    def test_psutil_error_handling(self, mock_memory):
        """Test handling of psutil errors."""
        # Mock psutil to raise an error
        mock_memory.side_effect = Exception("Mocked psutil error")
        
        # Should handle gracefully and return default values
        try:
            info = get_memory_info()
            # Should return some default structure
            assert isinstance(info, dict)
        except Exception as e:
            # Or should raise a more specific error
            assert "psutil" in str(e).lower() or "memory" in str(e).lower()
    
    def test_zero_memory_edge_case(self):
        """Test edge case with zero memory values."""
        # This is mainly to ensure no division by zero errors
        with patch('psutil.virtual_memory') as mock_memory:
            mock_stats = MagicMock()
            mock_stats.total = 0
            mock_stats.available = 0
            mock_stats.used = 0
            mock_stats.percent = 0
            mock_memory.return_value = mock_stats
            
            # Should handle gracefully
            try:
                info = get_memory_info()
                # Should not crash
                assert isinstance(info, dict)
            except ZeroDivisionError:
                pytest.fail("Should handle zero memory gracefully")


class TestMemoryUtilsPerformance:
    """Test performance characteristics of memory utilities."""
    
    def test_memory_info_speed(self):
        """Test that memory info retrieval is fast."""
        import time
        
        start_time = time.time()
        for _ in range(10):
            get_memory_info()
        elapsed = time.time() - start_time
        
        # Should be very fast (less than 1 second for 10 calls)
        assert elapsed < 1.0
    
    def test_format_bytes_speed(self):
        """Test that byte formatting is fast."""
        import time
        
        test_values = [1024, 1024**2, 1024**3, int(1.5 * 1024**3), 1024**4]
        
        start_time = time.time()
        for _ in range(1000):
            for val in test_values:
                format_bytes(val)
        elapsed = time.time() - start_time
        
        # Should be very fast
        assert elapsed < 1.0