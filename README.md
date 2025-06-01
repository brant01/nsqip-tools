# NSQIP Tools

A Python package for working with National Surgical Quality Improvement Program (NSQIP) data. This package provides tools to convert NSQIP text files into optimized parquet datasets, perform standard data transformations, and query the data efficiently using Polars.

## Features

- **Data Ingestion**: Convert NSQIP tab-delimited text files to parquet format
- **Automatic Transformations**: Standard data cleaning and derived variables
- **Data Verification**: Validate case counts against expected values
- **Efficient Querying**: Filter by CPT codes, diagnosis codes, years, and more
- **Data Dictionary**: Auto-generate comprehensive data dictionaries in CSV, JSON, and HTML formats
- **Memory Efficient**: Designed to work on regular computers with limited RAM
- **Network Drive Compatible**: Works seamlessly on local or network file systems
- **Type Safe**: Comprehensive type hints throughout

## Installation

```bash
pip install nsqip-tools
```

## Quick Start

### Building a Dataset

```python
import nsqip_tools

# Build parquet dataset from NSQIP text files
result = nsqip_tools.build_parquet_dataset(
    data_dir="/path/to/nsqip/files",
    dataset_type="adult"  # or "pediatric"
)

print(f"Dataset created at: {result['parquet_dir']}")
print(f"Data dictionary at: {result['dictionary']}")
```

### Querying Data

```python
import nsqip_tools
import polars as pl

# Load and filter data
df = (nsqip_tools.load_data("/path/to/parquet/dataset")
      .filter_by_cpt(["44970", "44979"])  # Laparoscopic procedures
      .filter_by_year([2020, 2021])
      .collect())

# Chain with Polars operations
df = (nsqip_tools.load_data("/path/to/parquet/dataset")
      .filter_by_diagnosis(["K80.20"])  # Gallstones
      .lazy_frame  # Access the Polars LazyFrame
      .select(["CASEID", "AGE_AS_INT", "CPT", "OPERYR"])
      .filter(pl.col("AGE_AS_INT") > 50)
      .group_by("CPT")
      .agg(pl.count())
      .collect())
```

## API Reference

### Building Datasets

#### `build_parquet_dataset()`

Build an NSQIP parquet dataset from text files with standard transformations.

```python
result = nsqip_tools.build_parquet_dataset(
    data_dir,                    # Path to NSQIP text files
    output_dir=None,            # Output directory (defaults to data_dir)
    dataset_type="adult",       # "adult" or "pediatric"
    generate_dictionary=True,   # Generate data dictionary
    memory_limit="4GB",         # Memory limit for operations
    verify_case_counts=True,    # Verify case counts match expected
    apply_transforms=True       # Apply standard transformations
)
```

**Returns:** Dictionary with paths to:
- `parquet_dir`: Parquet dataset directory
- `dictionary`: Data dictionary CSV file (if generated)
- `log`: Build log file


### Querying Data

#### `load_data()`

Load NSQIP data from a parquet dataset for querying.

```python
query = nsqip_tools.load_data("/path/to/parquet/dataset")
```

#### Filter Methods

All filter methods return the query object for chaining:

- **`filter_by_cpt(cpt_codes)`**: Filter by CPT procedure codes
- **`filter_by_diagnosis(diagnosis_codes)`**: Filter by ICD diagnosis codes  
- **`filter_by_year(years)`**: Filter by operation years
- **`filter_active_variables()`**: Keep only variables with data in most recent year
- **`select_demographics()`**: Select common demographic variables
- **`select_outcomes()`**: Select common outcome variables

#### Accessing Results

- **`.lazy_frame`**: Get the Polars LazyFrame for custom operations
- **`.collect()`**: Execute query and return Polars DataFrame
- **`.count()`**: Get count of rows without collecting full data
- **`.sample(n)`**: Get a random sample of n rows
- **`.describe()`**: Get summary statistics about the query

## Standard Transformations

The `build_parquet_dataset()` function automatically applies these transformations:

1. **Data Type Conversion**: Identifies and converts numeric columns while preserving categorical codes
2. **Age Processing**: 
   - Keeps original `AGE` column with "90+" values
   - Creates `AGE_AS_INT` (numeric, with 90 for "90+")
   - Creates `AGE_IS_90_PLUS` boolean flag
3. **CPT Array**: Combines all CPT columns into `ALL_CPT_CODES` array
4. **Diagnosis Array**: Combines all diagnosis columns into `ALL_DIAGNOSIS_CODES` array
5. **Race Combination**: Merges `RACE` and `RACE_NEW` into `RACE_COMBINED`
6. **Work RVU**: Calculates `WORK_RVU_TOTAL` from work RVU columns (adult only)
7. **Free Flap Indicators**: Derives boolean flags based on CPT codes

## Data Dictionary

Generated data dictionaries include:

- **Column name and data type**
- **Active status** (has data in most recent year)
- **Null counts and percentages**
- **Summary statistics** (numeric: min/max/mean/median, categorical: top values)
- **Null counts by year** (useful for identifying when variables were added/removed)

Available formats:
- **CSV**: For Excel/spreadsheet users
- **Excel**: For advanced spreadsheet analysis
- **HTML**: For easy web viewing

## Memory Optimization

The package is designed for regular computers:

- **Automatic memory detection**: Recommends appropriate memory limits based on available RAM
- **Columnar storage**: Uses parquet format for efficient compression and access
- **Lazy evaluation**: Polars LazyFrames enable efficient query planning
- **Streaming support**: Can process datasets larger than available memory

```python
# Check system memory
mem_info = nsqip_tools.get_memory_info()
print(f"Total RAM: {mem_info['total']}")
print(f"Available: {mem_info['available']}")
print(f"Recommended limit: {mem_info['recommended_limit']}")

# Use automatic memory detection (default)
result = nsqip_tools.build_parquet_dataset(data_dir="/path/to/files")

# Or specify custom limit
result = nsqip_tools.build_parquet_dataset(
    data_dir="/path/to/files",
    memory_limit="8GB"
)
```

### Safe Data Collection

The package includes memory-safe collection to prevent out-of-memory errors:

```python
# Check size before collecting
query = nsqip_tools.load_data("/path/to/parquet/dataset").filter_by_year([2021])
info = query.describe()
print(f"Total rows: {info['total_rows']}")
print(f"Columns: {info['columns']}")

# Use streaming for large datasets
df = query.collect(streaming=True)

# Get a sample for exploration
sample_df = query.sample(n=10000)
```

## Network Drive Support

The package works seamlessly on network drives and file systems that don't support file locking:

```python
# Works on network drives, SMB shares, etc.
result = nsqip_tools.build_parquet_dataset(
    data_dir="/Volumes/network_drive/nsqip_data",
    output_dir="/Volumes/network_drive/processed"
)

# Query from network location
query = nsqip_tools.load_data("/Volumes/network_drive/processed/adult_nsqip_parquet")
```

## Data Requirements

- NSQIP data files must be tab-delimited text files
- Files should follow standard NSQIP naming conventions
- Expected case counts are validated based on official NSQIP documentation

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Disclaimer

This package is not affiliated with or endorsed by the American College of Surgeons National Surgical Quality Improvement Program. Users must obtain NSQIP data through official channels.