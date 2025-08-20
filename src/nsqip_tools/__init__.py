"""NSQIP Tools: A Python package for working with NSQIP surgical data.

This package provides tools for ingesting, transforming, and querying
National Surgical Quality Improvement Program (NSQIP) data using Polars
and parquet datasets.
"""

from .query import load_data, NSQIPQuery
from .builder import build_parquet_dataset
from ._internal.memory_utils import get_memory_info, get_recommended_memory_limit
from .config import get_data_directory, get_output_directory, get_memory_limit

__all__ = [
    "build_parquet_dataset",
    "load_data",
    "NSQIPQuery",
    "get_memory_info",
    "get_recommended_memory_limit",
    "get_data_directory",
    "get_output_directory", 
    "get_memory_limit",
]

try:
    from importlib.metadata import version
    __version__ = version("nsqip-tools")
except ImportError:
    # Fallback for Python < 3.8
    from importlib_metadata import version
    __version__ = version("nsqip-tools")
except Exception:
    __version__ = "unknown"



