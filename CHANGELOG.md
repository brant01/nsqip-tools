# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-02-23

### Added
- New `analysis` module with 12 functions promoted from clinical-db-analysis:
  - `detect_dataset_type()` - auto-detect adult vs pediatric datasets
  - `calculate_composite_ssi()` - ANY_SSI binary indicator
  - `calculate_serious_morbidity()` - composite morbidity indicator
  - `create_age_groups()` - standard age categorization
  - `clean_asa_class()` - extract numeric ASA 1-5
  - `calculate_bmi()` - BMI from HEIGHT/WEIGHT
  - `standardize_sex()` - normalize to M/F
  - `get_surgery_year()` - extract year integer
  - `filter_by_age()` - filter by age range (standalone)
  - `filter_elective_cases()` - filter non-emergency cases
  - `create_outcome_summary()` - summary table with rates
  - `export_for_stats()` - export for R/Stata/SAS
- `NSQIPQuery.filter_by_age()` - fluent age range filtering
- `NSQIPQuery.filter_elective()` - fluent elective case filtering
- `NSQIPQuery.columns` property - list column names from schema
- `NSQIPQuery.count()` - row count without full collect
- `NSQIPQuery.sample()` - random sample as DataFrame
- `NSQIPQuery.describe()` - dict with row count, column count, path
- `NSQIPQuery.__repr__()` - readable string representation

### Changed
- Rename internal `_lazy_frame` attribute to `_lf` (idiomatic Polars convention)
- GitHub repo renamed from `nsqip_tools` to `nsqip-tools`
- Migrate pytest config from `pytest.ini` to `pyproject.toml`
- Add `UP` (pyupgrade) to ruff lint rules

## [0.3.0] - 2026-01-22

### Fixed
- Replace bare `except:` clauses with `except Exception:` for proper error handling
- Remove `logging.basicConfig()` from module import to avoid overriding user config

### Added
- `py.typed` marker for PEP 561 type hint support
- Project documentation (CLAUDE.md, CONTRIBUTING.md)
- Cross-platform path configuration in `.env.example`

## [0.2.3] - 2025-01-03

### Changed
- Pre-PyPI release preparation
- Updated environment configuration and dependencies

### Added
- Comprehensive test suite with unit and integration tests
- Environment variable configuration (`NSQIP_DATA_DIR`, `NSQIP_OUTPUT_DIR`, `NSQIP_MEMORY_LIMIT`)
- `.env` file support via python-dotenv

## [0.2.2] - 2024-12-XX

### Fixed
- Version mismatch by implementing dynamic version reading from pyproject.toml

## [0.2.1] - 2024-12-XX

### Fixed
- Enforce consistent schemas during parquet creation across all files
- Schema mismatch errors when loading multiple parquet files
- `filter_by_cpt()` empty column name bug

### Added
- LazyFrame delegation to `NSQIPQuery` class for seamless Polars integration

## [0.2.0] - 2024-12-XX

### Changed
- Complete migration from DuckDB to Polars/Parquet architecture
- Simplified query interface by removing redundant convenience methods

### Added
- Auto-detection of dataset type from filenames
- Improved directory organization for network drive workflows
- Type safety improvements throughout

### Fixed
- Nested directory issues in parquet creation

## [0.1.0] - 2024-XX-XX

### Added
- Initial release
- NSQIP text file to parquet conversion
- Basic query interface with CPT and diagnosis code filtering
- Data dictionary generation (CSV, Excel, HTML)
- Memory-efficient processing with configurable limits
