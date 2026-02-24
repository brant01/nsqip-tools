# NSQIP Tools Project Conventions

## Data Processing
- Use Polars LazyFrames for all data operations (enables query optimization)
- Preserve clinical codes as strings (CPT codes, ICD codes, CASEIDs)
- Check `constants.py` for variable type definitions before processing

## Column Naming
- Derived columns use SCREAMING_SNAKE_CASE (e.g., `AGE_AS_INT`, `ALL_CPT_CODES`)
- Boolean flags end with `_IS_*` or `_HAS_*`
- Array columns start with `ALL_*`

## Age Handling (HIPAA)
- Original `AGE` column contains "90+" as string
- Use `AGE_AS_INT` for numeric comparisons (90+ becomes 90)
- Use `AGE_IS_90_PLUS` flag for accurate 90+ identification

## Query API Pattern
- `NSQIPQuery` returns `self` from filter methods (fluent interface)
- Delegate unknown attributes to underlying LazyFrame via `__getattr__`
- Internal attribute is `_lf` (idiomatic Polars convention)
- Use `collect()` only at the end of a query chain
- Utility methods: `columns`, `count()`, `sample()`, `describe()`, `__repr__()`

## Analysis Module (`analysis.py`)
- Standalone functions that operate on DataFrames or LazyFrames
- Use `FrameType = TypeVar('FrameType', pl.DataFrame, pl.LazyFrame)` to preserve input type
- All functions auto-detect adult vs pediatric via `detect_dataset_type()`
- Pediatric detection: `AGE_DAYS` column present → pediatric
- Adult detection: `AGE_AS_INT` or `AGE` column → adult
- Derived columns: `ANY_SSI`, `SERIOUS_MORBIDITY`, `AGE_GROUP`, `ASA_SIMPLE`, `BMI`, `SEX_STANDARD`, `SURGERY_YEAR`
- Use `collect_schema().names()` instead of `.columns` on LazyFrames to avoid PerformanceWarning

## Testing
- Unit tests must work without real NSQIP data
- Use synthetic data fixtures in `conftest.py`
- Mark real data tests with `@pytest.mark.requires_data`

## Memory Safety
- Check `memory_utils.py` for available memory before large operations
- Use streaming mode for datasets larger than available RAM
- Default memory limit is conservative (4GB)
