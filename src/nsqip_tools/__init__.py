"""NSQIP Tools: A Python package for working with NSQIP surgical data.

This package provides tools for ingesting, transforming, and querying
National Surgical Quality Improvement Program (NSQIP) data using Polars
and parquet datasets.
"""

from ._internal.memory_utils import get_memory_info, get_recommended_memory_limit
from .analysis import (
    calculate_bmi,
    calculate_composite_ssi,
    calculate_serious_morbidity,
    clean_asa_class,
    create_age_groups,
    create_outcome_summary,
    detect_dataset_type,
    export_for_stats,
    filter_by_age,
    filter_elective_cases,
    get_surgery_year,
    standardize_sex,
)
from .builder import build_parquet_dataset
from .config import get_data_directory, get_memory_limit, get_output_directory
from .query import NSQIPQuery, load_data

__all__ = [
    "NSQIPQuery",
    "build_parquet_dataset",
    "calculate_bmi",
    "calculate_composite_ssi",
    "calculate_serious_morbidity",
    "clean_asa_class",
    "create_age_groups",
    "create_outcome_summary",
    "detect_dataset_type",
    "export_for_stats",
    "filter_by_age",
    "filter_elective_cases",
    "get_data_directory",
    "get_memory_info",
    "get_memory_limit",
    "get_output_directory",
    "get_recommended_memory_limit",
    "get_surgery_year",
    "load_data",
    "standardize_sex",
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
