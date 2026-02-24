# nsqip-tools

Python package for working with NSQIP (National Surgical Quality Improvement Program) data.

## Tech Stack
- **Language:** Python 3.10+
- **Data Processing:** Polars, PyArrow
- **Package Management:** uv
- **Testing:** pytest

## Architecture
```
src/nsqip_tools/
├── __init__.py          # Public API exports
├── analysis.py          # Analysis functions (SSI, morbidity, age groups, BMI, etc.)
├── builder.py           # build_parquet_dataset() - ETL orchestration
├── query.py             # NSQIPQuery class - fluent query API
├── config.py            # Environment variable management
├── constants.py         # Variable types, column groupings
├── data_dictionary.py   # DataDictionaryGenerator class
└── _internal/
    ├── ingest.py        # Text → Parquet conversion
    ├── transform.py     # Data transformations (age, CPT arrays, etc.)
    ├── inspect.py       # File inspection utilities
    └── memory_utils.py  # System memory detection
```

## Quick Commands
```bash
# Install dependencies
uv sync

# Run tests (unit only - no real data needed)
uv run pytest -m unit

# Run all tests (requires NSQIP data configured)
uv run pytest

# Build package
uv build
```

## Key Patterns

### Data Flow
1. Text files → `ingest.py` → raw parquet
2. Raw parquet → `transform.py` → transformed parquet with derived columns
3. Transformed parquet → `NSQIPQuery` → filtered results

### Important Variables
- `STRING_VARIABLES` in `constants.py` - columns that must stay as strings (codes, IDs)
- `CPT_COLUMNS`, `DIAGNOSIS_COLUMNS` - clinical code columns
- Derived: `AGE_AS_INT`, `AGE_IS_90_PLUS`, `ALL_CPT_CODES`, `ALL_DIAGNOSIS_CODES`

### Analysis Module
- Functions in `analysis.py` operate on DataFrames or LazyFrames
- Use `FrameType = TypeVar('FrameType', pl.DataFrame, pl.LazyFrame)` to preserve input type
- All functions auto-detect adult vs pediatric datasets
- `create_outcome_summary()` requires a DataFrame (not LazyFrame)
- `export_for_stats()` writes CSV and Parquet files for statistical software

### Environment Configuration
```bash
NSQIP_DATA_DIR=/path/to/nsqip/data
NSQIP_OUTPUT_DIR=/path/to/output
NSQIP_MEMORY_LIMIT=8GB
```

## Testing
- Unit tests use synthetic data (no real NSQIP files needed)
- Integration tests require real data paths in environment
- Markers: `@unit`, `@integration`, `@slow`, `@requires_data`
