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
- Delegate unknown attributes to underlying LazyFrame
- Use `collect()` only at the end of a query chain

## Testing
- Unit tests must work without real NSQIP data
- Use synthetic data fixtures in `conftest.py`
- Mark real data tests with `@pytest.mark.requires_data`

## Memory Safety
- Check `memory_utils.py` for available memory before large operations
- Use streaming mode for datasets larger than available RAM
- Default memory limit is conservative (4GB)
