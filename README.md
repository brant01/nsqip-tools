# nsqip-tools

[![PyPI version](https://img.shields.io/pypi/v/nsqip-tools.svg)](https://pypi.org/project/nsqip-tools/)
[![Python versions](https://img.shields.io/pypi/pyversions/nsqip-tools.svg)](https://pypi.org/project/nsqip-tools/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A Python package for working with National Surgical Quality Improvement Program (NSQIP) data. Convert NSQIP text files into optimized parquet datasets, perform standard surgical quality analysis, and query data efficiently using [Polars](https://pola.rs/).

## Features

- **Data Ingestion** -- Convert NSQIP tab-delimited text files to parquet format
- **Automatic Transformations** -- Age handling, CPT/diagnosis arrays, race combination, work RVU
- **Analysis Functions** -- Composite SSI, serious morbidity, age groups, BMI, ASA cleaning, outcome summaries
- **Efficient Querying** -- Fluent API to filter by CPT codes, diagnosis codes, years, age, and elective status
- **Data Dictionary** -- Auto-generate data dictionaries in CSV, Excel, and HTML
- **Memory Efficient** -- Automatic memory detection, lazy evaluation, streaming support
- **Type Safe** -- Full type hints, supports both adult and pediatric datasets

## Installation

```bash
pip install nsqip-tools
```

## Data Access

**This package does not include NSQIP data.** You must obtain access through official channels:

- **NSQIP Participant Sites**: Contact your institution's NSQIP coordinator
- **Research Access**: Apply through the [American College of Surgeons](https://www.facs.org/quality-programs/acs-nsqip/)
- **Data Use Agreements**: Required for all NSQIP data usage

## Quick Start

### Build a Dataset

```python
import nsqip_tools

result = nsqip_tools.build_parquet_dataset(
    data_dir="/path/to/nsqip/text/files",
    dataset_type="adult",  # or "pediatric"
)
print(f"Dataset: {result['parquet_dir']}")
```

### Query Data

```python
import nsqip_tools

# Fluent filtering API
df = (nsqip_tools.load_data("/path/to/parquet/dataset")
      .filter_by_cpt(["44970", "47562"])
      .filter_by_year([2021, 2022])
      .filter_by_age(min_age=18, max_age=65)
      .filter_elective()
      .collect())

# Explore before collecting
query = nsqip_tools.load_data("/path/to/parquet/dataset")
print(query)            # NSQIPQuery(rows=150000, cols=320, path=...)
print(query.count())    # 150000
print(query.columns)    # ['CASEID', 'OPERYR', 'AGE', ...]

sample = query.sample(n=1000)
```

### Analyze Outcomes

```python
from nsqip_tools import (
    calculate_composite_ssi,
    calculate_serious_morbidity,
    create_age_groups,
    create_outcome_summary,
    export_for_stats,
)

# Build derived variables
df = calculate_composite_ssi(df)        # adds ANY_SSI (0/1)
df = calculate_serious_morbidity(df)    # adds SERIOUS_MORBIDITY (0/1)
df = create_age_groups(df)              # adds AGE_GROUP

# Summarize outcomes
summary = create_outcome_summary(df)
print(summary)  # Outcome | N | Total | Rate (%)

# Stratify by year
yearly = create_outcome_summary(df, group_var="OPERYR")

# Export for R/Stata/SAS
export_for_stats(df, "results/analysis_data.csv")
```

## Analysis Functions

All functions auto-detect adult vs pediatric datasets and preserve DataFrame/LazyFrame types.

| Function | Description | Adds Column |
|----------|-------------|-------------|
| `calculate_composite_ssi(df)` | Binary indicator from SUPINFEC, WNDINFD, ORGSPCSSI | `ANY_SSI` |
| `calculate_serious_morbidity(df)` | Composite of deep SSI, organ space SSI, pneumonia, reoperation, etc. | `SERIOUS_MORBIDITY` |
| `create_age_groups(df)` | Standard age categories (customizable bins) | `AGE_GROUP` |
| `clean_asa_class(df)` | Extract numeric 1-5 from ASA text | `ASA_SIMPLE` |
| `calculate_bmi(df)` | BMI from HEIGHT (in) and WEIGHT (lbs) | `BMI` |
| `standardize_sex(df)` | Normalize to M/F | `SEX_STANDARD` |
| `get_surgery_year(df)` | Integer year from OPERYR/ADMYR | `SURGERY_YEAR` |
| `filter_by_age(df, min_age, max_age)` | Filter by age range in years | -- |
| `filter_elective_cases(df)` | Remove emergency cases | -- |
| `detect_dataset_type(df)` | Returns `"adult"` or `"pediatric"` | -- |
| `create_outcome_summary(df)` | Summary table with counts and rates | -- |
| `export_for_stats(df, path)` | Export CSV/Parquet with binary outcomes | -- |

## Query API

### Filter Methods

All filter methods return `self` for chaining:

```python
query = (nsqip_tools.load_data(path)
         .filter_by_cpt(["44970"])
         .filter_by_diagnosis(["K80.20"])
         .filter_by_year([2021, 2022])
         .filter_by_age(min_age=18)
         .filter_elective())
```

### Utility Methods

```python
query.columns        # list of column names
query.count()        # row count without collecting
query.sample(n=100)  # random sample as DataFrame
query.describe()     # {"total_rows": ..., "columns": ..., "parquet_path": ...}
query.collect()      # execute and return DataFrame
query.lazy_frame     # access the underlying Polars LazyFrame
```

### Polars Integration

Any Polars LazyFrame method can be called directly on the query object:

```python
import polars as pl

result = (nsqip_tools.load_data(path)
          .filter_by_cpt(["44970"])
          .select(["CASEID", "AGE_AS_INT", "CPT"])
          .filter(pl.col("AGE_AS_INT") > 50)
          .group_by("CPT")
          .agg(pl.count())
          .collect())
```

## Standard Transformations

`build_parquet_dataset()` automatically applies:

1. **Data Type Conversion** -- Numeric columns detected and converted; clinical codes preserved as strings
2. **Age Processing** -- Original `AGE` column preserved; `AGE_AS_INT` (numeric, 90 for "90+") and `AGE_IS_90_PLUS` flag added
3. **CPT Array** -- All CPT columns combined into `ALL_CPT_CODES` list column
4. **Diagnosis Array** -- All diagnosis columns combined into `ALL_DIAGNOSIS_CODES` list column
5. **Race Combination** -- `RACE` and `RACE_NEW` merged into `RACE_COMBINED`
6. **Work RVU** -- `WORK_RVU_TOTAL` calculated from work RVU columns (adult only)

## Configuration

```bash
# Environment variables
export NSQIP_DATA_DIR="/path/to/nsqip/data"
export NSQIP_OUTPUT_DIR="/path/to/output"
export NSQIP_MEMORY_LIMIT="8GB"
```

Or use a `.env` file (install with `pip install nsqip-tools[config]`).

## Memory & Performance

```python
# Automatic memory detection
mem = nsqip_tools.get_memory_info()
print(f"Available: {mem['available']}")

# Stream large datasets
df = query.collect(streaming=True)

# Explore before committing to full collection
info = query.describe()
print(f"{info['total_rows']:,} rows, {info['columns']} columns")
```

## Requirements

- Python >= 3.10
- NSQIP participant user files (PUF) in tab-delimited text format

## License

MIT

## Disclaimer

This package is not affiliated with or endorsed by the American College of Surgeons or NSQIP. Users must obtain data through official channels and comply with all applicable data use agreements.
