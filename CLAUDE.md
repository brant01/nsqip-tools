# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NSQIP Tools is a Python package for processing and analyzing National Surgical Quality Improvement Program (NSQIP) data. It provides a clean API for converting NSQIP text files into DuckDB databases, applying standard transformations, and querying the data efficiently using Polars.

**Important**: This is a public package. Users must obtain NSQIP data through official channels. Never include actual patient data in this repository.

## Development Commands

This project uses `uv` for Python package management. Always use `uv` commands instead of `python` or `pip` directly.

### Testing
```bash
uv run pytest tests/
```

### Installing the package for development
```bash
uv pip install -e .
```

### Running Python scripts
```bash
uv run python script.py
```

### Adding new dependencies
```bash
# Add to main dependencies
uv add package-name

# Add to dev dependencies
uv add --dev package-name
```

### Installing all dependencies (including dev)
```bash
uv sync --all-extras
```

## Architecture

The package has been reorganized into a clean, public API structure:

1. **Public API** (in `src/nsqip_tools/`):
   - `builder.py`: Main `build_duck_db()` function for database creation
   - `query.py`: Query API with `load_data()` and `NSQIPQuery` class
   - `data_dictionary.py`: Data dictionary generation in multiple formats
   - `constants.py`: Variable definitions, expected case counts

2. **Internal Implementation** (in `src/nsqip_tools/_internal/`):
   - `ingest.py`: Database creation from text files
   - `transform.py`: Data transformation functions
   - `inspect.py`: Data inspection utilities

### Main Functions

- **`build_duck_db()`**: Converts NSQIP text files to DuckDB with standard transformations
- **`load_data()`**: Returns an `NSQIPQuery` object for filtering data
- **Query methods**: `.filter_by_cpt()`, `.filter_by_diagnosis()`, `.filter_by_year()`, `.filter_active_variables()`

### Data Flow

1. Raw NSQIP text files → `build_duck_db()` → DuckDB database with transformations
2. DuckDB database → `load_data()` → `NSQIPQuery` → Polars LazyFrame → Results

### Standard Transformations

The `build_duck_db()` function automatically applies:

- **Age processing**: Original `AGE` kept, plus `AGE_AS_INT` and `AGE_IS_90_PLUS`
- **CPT arrays**: All CPT columns combined into `ALL_CPT_CODES`
- **Diagnosis arrays**: All diagnosis columns combined into `ALL_DIAGNOSIS_CODES`
- **Race combination**: `RACE` and `RACE_NEW` merged into `RACE_COMBINED`
- **Type conversions**: Numeric columns identified and converted
- **Case count verification**: Validates against expected counts in constants.py

### Code Conventions

- **Type hints**: Use comprehensive type hints throughout
- **Docstrings**: Include detailed docstrings with examples
- **Error handling**: Validate inputs and provide clear error messages
- **Logging**: Use INFO level logging for major operations
- **No data**: Never include actual NSQIP data or patient information
- **Memory efficiency**: Design for regular computers with limited RAM

### Testing

- Unit tests in `tests/` use mock data only
- No real NSQIP data in tests
- Test both success and error cases
- Use pytest fixtures for test databases

### Important Security Notes

- The `.gitignore` excludes all data files (*.duckdb, *.csv, *.txt)
- Notebooks are excluded to prevent accidental data exposure
- Never commit actual NSQIP data or statistics beyond the aggregate case counts