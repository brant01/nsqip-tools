"""
Shared pytest fixtures and configuration for nsqip_tools tests.
"""

from pathlib import Path

import polars as pl
import pytest

import nsqip_tools


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests"
    )
    parser.addoption(
        "--dataset-dir",
        action="store",
        default="data",
        help="Directory containing the datasets (default: data)"
    )


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests requiring real data"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "requires_data: marks tests that require actual dataset files"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers."""
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # Skip data-dependent tests if data directory doesn't exist
    dataset_dir = Path(config.getoption("--dataset-dir"))
    if not dataset_dir.exists():
        skip_data = pytest.mark.skip(reason=f"dataset directory {dataset_dir} not found")
        for item in items:
            if "requires_data" in item.keywords or "integration" in item.keywords:
                item.add_marker(skip_data)


@pytest.fixture(scope="session")
def dataset_dir(request):
    """Get the dataset directory from command line or use default."""
    return Path(request.config.getoption("--dataset-dir"))


@pytest.fixture(scope="session")
def adult_nsqip_path(dataset_dir):
    """Path to adult NSQIP dataset."""
    return dataset_dir / "adult_nsqip_parquet"


@pytest.fixture(scope="session")
def pediatric_nsqip_path(dataset_dir):
    """Path to pediatric NSQIP dataset."""
    return dataset_dir / "pediatric_nsqip_parquet"


@pytest.fixture(scope="session")
def ncdb_path(dataset_dir):
    """Path to NCDB dataset."""
    return dataset_dir / "ncdb_parquet_20250603"


@pytest.fixture
def sample_nsqip_data():
    """Create sample NSQIP data for unit tests."""
    return pl.DataFrame({
        "CASEID": ["1", "2", "3", "4", "5"],
        "OPERYR": ["2020", "2020", "2021", "2021", "2022"],
        "AGE": ["45", "60", "90+", "55", "70"],
        "AGE_AS_INT": [45, 60, 90, 55, 70],
        "AGE_IS_90_PLUS": [False, False, True, False, False],
        "SEX": ["Male", "Female", "Male", "Female", "Male"],
        "CPT": ["44970", "47562", "44970", "47563", "49650"],
        "PODIAG": ["K80.20", "K80.21", "K81", "K80.20", "K35.8"],
        "ALL_CPT_CODES": [
            ["44970"],
            ["47562"],
            ["44970", "12345"],
            ["47563"],
            ["49650", "49651"]
        ],
        "ALL_DIAGNOSIS_CODES": [
            ["K80.20"],
            ["K80.21", "K81"],
            ["K81"],
            ["K80.20"],
            ["K35.8"]
        ],
        "DEATH30": [0, 0, 0, 1, 0],
        "READMISSION": [0, 1, 0, 0, 0],
        "WORK_RVU_TOTAL": [15.5, 12.3, 15.5, 12.8, 8.7]
    })


@pytest.fixture
def temp_dataset_dir(tmp_path, sample_nsqip_data):
    """Create a temporary dataset directory with sample data."""
    dataset_dir = tmp_path / "test_dataset"
    dataset_dir.mkdir()

    # Split by year and save
    for year in ["2020", "2021", "2022"]:
        year_data = sample_nsqip_data.filter(pl.col("OPERYR") == year)
        if len(year_data) > 0:
            year_data.write_parquet(dataset_dir / f"adult_{year}.parquet")

    # Create metadata
    metadata = {
        "dataset_type": "adult",
        "created_at": "2024-01-01",
        "transform_version": "1.0.0",
        "years_included": ["2020", "2021", "2022"],
        "total_cases": len(sample_nsqip_data)
    }

    import json
    with open(dataset_dir / "metadata.json", "w") as f:
        json.dump(metadata, f)

    return dataset_dir


@pytest.fixture
def memory_info():
    """Get memory information for tests."""
    return nsqip_tools.get_memory_info()


# Performance tracking fixtures
@pytest.fixture
def benchmark_timer():
    """Simple timer for performance benchmarking."""
    import time

    class Timer:
        def __init__(self):
            self.times = {}

        def start(self, name):
            self.times[name] = time.time()

        def stop(self, name):
            if name in self.times:
                elapsed = time.time() - self.times[name]
                del self.times[name]
                return elapsed
            return None

    return Timer()


# Analysis fixtures - adult and pediatric NSQIP test DataFrames
@pytest.fixture
def adult_nsqip_df() -> pl.DataFrame:
    """Sample adult NSQIP DataFrame for unit testing analysis functions."""
    return pl.DataFrame({
        "AGE_AS_INT": [45, 67, 32, 78, 55],
        "SEX": ["Male", "Female", "male", "FEMALE", "M"],
        "ASACLAS": [
            "2-Mild Disturb",
            "3-Severe Disturb",
            "1-No Disturb",
            "4-Life Threat",
            "2-Mild Disturb",
        ],
        "HEIGHT": [70, 65, 72, 68, 71],  # inches
        "WEIGHT": [180, 150, 200, 140, 190],  # pounds
        "SUPINFEC": [
            "No Complication",
            "Superficial Incisional SSI",
            "No Complication",
            "No Complication",
            "No Complication",
        ],
        "WNDINFD": [
            "No Complication",
            "No Complication",
            "Deep Incisional SSI",
            "No Complication",
            "No Complication",
        ],
        "ORGSPCSSI": [
            "No Complication",
            "No Complication",
            "No Complication",
            "Organ/Space SSI",
            "No Complication",
        ],
        "OUPNEUMO": [
            "No Complication",
            "Pneumonia",
            "No Complication",
            "No Complication",
            "No Complication",
        ],
        "URNINFEC": [
            "No Complication",
            "No Complication",
            "No Complication",
            "Urinary Tract Infection",
            "No Complication",
        ],
        "READMISSION1": ["No", "Yes", "No", "No", "No"],
        "REOPERATION1": ["No", "No", "Yes", "No", "No"],
        "EMERGENT": ["No", "No", "No", "Yes", "No"],
        "OPERYR": [2022, 2022, 2023, 2023, 2023],
    })


@pytest.fixture
def pediatric_nsqip_df() -> pl.DataFrame:
    """Sample pediatric NSQIP DataFrame for unit testing analysis functions."""
    return pl.DataFrame({
        "AGE_DAYS": [14, 180, 730, 2555, 5475],  # neonate, infant, 2yr, 7yr, 15yr
        "SEX": ["Male", "Female", "Male", "Female", "Male"],
        "ASACLAS": [
            "2-Mild Disturb",
            "1-No Disturb",
            "2-Mild Disturb",
            "3-Severe Disturb",
            "1-No Disturb",
        ],
        "WEIGHT": [3.5, 8.0, 12.0, 25.0, 55.0],  # kg for pediatric
        "SUPINFEC": [
            "No Complication",
            "No Complication",
            "Superficial Incisional SSI",
            "No Complication",
            "No Complication",
        ],
        "WNDINFD": [
            "No Complication",
            "No Complication",
            "No Complication",
            "No Complication",
            "No Complication",
        ],
        "ORGSPCSSI": [
            "No Complication",
            "No Complication",
            "No Complication",
            "No Complication",
            "No Complication",
        ],
        "READMISSION1": ["No", "No", "No", "Yes", "No"],
        "REOPERATION": ["No", "No", "No", "No", "Yes"],
        "EMERGENT": ["Yes", "No", "No", "No", "No"],
        "OPERYR": [2022, 2022, 2023, 2023, 2023],
    })


@pytest.fixture
def adult_lazyframe(adult_nsqip_df: pl.DataFrame) -> pl.LazyFrame:
    """Sample adult NSQIP LazyFrame for unit testing analysis functions."""
    return adult_nsqip_df.lazy()


@pytest.fixture
def pediatric_lazyframe(pediatric_nsqip_df: pl.DataFrame) -> pl.LazyFrame:
    """Sample pediatric NSQIP LazyFrame for unit testing analysis functions."""
    return pediatric_nsqip_df.lazy()


# Validation helpers
@pytest.fixture
def validation_helpers():
    """Helper functions for common validations."""

    class Helpers:
        @staticmethod
        def validate_dataframe_schema(df, required_columns):
            """Validate that a dataframe has required columns."""
            missing = set(required_columns) - set(df.columns)
            assert not missing, f"Missing columns: {missing}"

        @staticmethod
        def validate_year_range(df, min_year, max_year, year_col="OPERYR"):
            """Validate year range in dataset."""
            years = df[year_col].unique().sort().to_list()
            years_int = [int(y) for y in years]
            assert min(years_int) >= min_year, f"Found year before {min_year}"
            assert max(years_int) <= max_year, f"Found year after {max_year}"

        @staticmethod
        def validate_cpt_codes(df, cpt_col="CPT"):
            """Validate CPT codes are in expected format."""
            cpts = df[cpt_col].drop_nulls().to_list()
            for cpt in cpts[:100]:  # Check first 100
                assert len(cpt) == 5, f"Invalid CPT length: {cpt}"
                assert cpt.isdigit(), f"Non-numeric CPT: {cpt}"

    return Helpers()
