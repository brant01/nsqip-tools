"""NSQIP Tools: A Python package for working with NSQIP surgical data.

This package provides tools for ingesting, transforming, and querying
National Surgical Quality Improvement Program (NSQIP) data using Polars
and parquet datasets.
"""

from .query import load_data, NSQIPQuery
from .builder import build_parquet_dataset
from ._internal.memory_utils import get_memory_info, get_recommended_memory_limit

__all__ = [
    "build_parquet_dataset",
    "load_data",
    "NSQIPQuery",
    "get_memory_info",
    "get_recommended_memory_limit",
]

__version__ = "0.1.0"



