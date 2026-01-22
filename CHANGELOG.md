# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Replace bare `except:` clauses with `except Exception:` for proper error handling
- Remove `logging.basicConfig()` from module import to avoid overriding user config

### Added
- `py.typed` marker for PEP 561 type hint support

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
