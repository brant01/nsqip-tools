"""Query and filtering functions for NSQIP data.

This module provides a fluent API for filtering NSQIP data that integrates
seamlessly with Polars LazyFrame operations.
"""
from pathlib import Path
from typing import List, Optional, Union, Self, Any
import polars as pl
import json

from .constants import (
    ALL_CPT_CODES_FIELD,
    ALL_DIAGNOSIS_CODES_FIELD,
    DIAGNOSIS_COLUMNS,
)


class NSQIPQuery:
    """A query builder for NSQIP data that behaves like a Polars LazyFrame.
    
    This class provides a fluent interface for filtering NSQIP data and 
    transparently delegates all LazyFrame methods, allowing seamless integration
    with Polars operations.
    
    Examples:
        >>> # Basic filtering with direct collect()
        >>> df = (NSQIPQuery("path/to/parquet/dir")
        ...       .filter_by_cpt(["44970", "44979"])
        ...       .filter_by_year([2020, 2021])
        ...       .collect())
        
        >>> # Use any Polars LazyFrame method directly
        >>> df = (NSQIPQuery("path/to/parquet/dir")
        ...       .filter(
        ...           (pl.col("ALL_CPT_CODES").list.len() == 1) &
        ...           (pl.col("ALL_CPT_CODES").list.first().is_in(["42821", "42826"]))
        ...       )
        ...       .select(["CASEID", "AGE", "OPERYR", "CPT"])
        ...       .collect())
        
        >>> # Mix NSQIP-specific and Polars methods
        >>> df = (NSQIPQuery("path/to/parquet/dir")
        ...       .filter_by_diagnosis(["K80.20"])
        ...       .filter(pl.col("AGE_AS_INT") > 50)
        ...       .with_columns(pl.col("AGE_AS_INT").alias("patient_age"))
        ...       .group_by("OPERYR")
        ...       .agg(pl.count())
        ...       .collect())
    """
    
    def __init__(self, parquet_path: Union[str, Path]):
        """Initialize a new NSQIP query.
        
        Args:
            parquet_path: Path to the parquet directory or a single parquet file.
            
        Raises:
            FileNotFoundError: If the path doesn't exist.
            ValueError: If no parquet files found.
        """
        self.parquet_path = Path(parquet_path)
        
        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Path not found: {self.parquet_path}")
        
        # Check if this is a directory or single file
        if self.parquet_path.is_dir():
            # Find all parquet files
            self.parquet_files = list(self.parquet_path.glob("*.parquet"))
            if not self.parquet_files:
                raise ValueError(f"No parquet files found in: {self.parquet_path}")
            
            # Load metadata if available
            metadata_path = self.parquet_path / "metadata.json"
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    self.metadata = json.load(f)
            else:
                self.metadata = {}
        else:
            # Single parquet file
            if not self.parquet_path.suffix == '.parquet':
                raise ValueError(f"File is not a parquet file: {self.parquet_path}")
            self.parquet_files = [self.parquet_path]
            self.metadata = {}
        
        # Initialize lazy frame that reads all parquet files
        self._lazy_frame = self._load_all_parquet_files()
        
    def _load_all_parquet_files(self) -> pl.LazyFrame:
        """Load all parquet files as a single LazyFrame.
        
        Returns:
            LazyFrame representing all data
        """
        # Create lazy frames for each file
        lazy_frames = []
        for parquet_file in self.parquet_files:
            lf = pl.scan_parquet(parquet_file)
            lazy_frames.append(lf)
        
        # Combine all lazy frames
        if len(lazy_frames) == 1:
            return lazy_frames[0]
        else:
            # Align schemas before concatenating to handle type mismatches
            return self._align_and_concat_schemas(lazy_frames)
    
    def _align_and_concat_schemas(self, lazy_frames: List[pl.LazyFrame]) -> pl.LazyFrame:
        """Align schemas of multiple LazyFrames and concatenate them.
        
        This method handles data type mismatches between parquet files by:
        1. Finding a common schema across all files
        2. Casting columns to compatible types
        3. Ensuring all files have the same columns
        
        Args:
            lazy_frames: List of LazyFrames to align and concatenate
            
        Returns:
            Single LazyFrame with aligned schema
        """
        if not lazy_frames:
            raise ValueError("No LazyFrames provided")
        
        if len(lazy_frames) == 1:
            return lazy_frames[0]
        
        # Get all schemas
        schemas = [lf.collect_schema() for lf in lazy_frames]
        
        # Find the union of all column names
        all_columns = set()
        for schema in schemas:
            all_columns.update(schema.names())
        
        # Determine the "best" data type for each column across all files
        column_types = {}
        for col_name in all_columns:
            types_for_column = []
            for schema in schemas:
                if col_name in schema:
                    types_for_column.append(schema[col_name])
            
            # Choose the most general type (precedence: String > Float64 > Float32 > Int64 > Int32 > Boolean)
            if pl.String in types_for_column:
                column_types[col_name] = pl.String
            elif pl.Float64 in types_for_column:
                column_types[col_name] = pl.Float64
            elif pl.Float32 in types_for_column:
                column_types[col_name] = pl.Float64  # Promote to Float64
            elif pl.Int64 in types_for_column:
                column_types[col_name] = pl.Int64
            elif pl.Int32 in types_for_column:
                column_types[col_name] = pl.Int64  # Promote to Int64
            elif pl.Boolean in types_for_column:
                column_types[col_name] = pl.Boolean
            else:
                # Use the first type found as default
                column_types[col_name] = types_for_column[0]
        
        # Align each LazyFrame to the common schema
        aligned_frames = []
        for lf in lazy_frames:
            current_schema = lf.collect_schema()
            
            # Add missing columns as nulls and cast existing columns
            select_expressions = []
            for col_name in sorted(all_columns):  # Sort for consistent column order
                target_type = column_types[col_name]
                
                if col_name in current_schema:
                    # Cast existing column to target type
                    select_expressions.append(pl.col(col_name).cast(target_type))
                else:
                    # Add missing column as null with correct type
                    select_expressions.append(pl.lit(None, dtype=target_type).alias(col_name))
            
            aligned_lf = lf.select(select_expressions)
            aligned_frames.append(aligned_lf)
        
        # Now all frames have the same schema - concatenate them
        return pl.concat(aligned_frames, how="vertical")
    
    @property
    def lazy_frame(self) -> pl.LazyFrame:
        """Get the underlying Polars LazyFrame.
        
        This allows direct access to all Polars operations.
        
        Returns:
            The current LazyFrame with all filters applied.
        """
        return self._lazy_frame
    
    def filter_by_year(self, years: Union[int, List[int]]) -> Self:
        """Filter data to specific years.
        
        Args:
            years: Single year or list of years to include.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> query.filter_by_year(2021)
            >>> query.filter_by_year([2019, 2020, 2021])
        """
        if isinstance(years, int):
            years = [years]
        
        # Handle both string and numeric OPERYR values
        year_strs = [str(y) for y in years]
        self._lazy_frame = self._lazy_frame.filter(
            pl.col("OPERYR").is_in(year_strs)
        )
        return self
    
    def filter_by_cpt(
        self, 
        cpt_codes: Union[str, List[str]], 
        use_any: bool = True
    ) -> Self:
        """Filter by CPT codes.
        
        This searches across all CPT columns in the dataset.
        
        Args:
            cpt_codes: Single CPT code or list of codes to filter by.
            use_any: If True, include rows with ANY of the specified codes.
                    If False, include rows with ALL of the specified codes.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> # Find cases with specific CPT code
            >>> query.filter_by_cpt("44970")
            
            >>> # Find cases with any of several codes
            >>> query.filter_by_cpt(["44970", "44979"])
            
            >>> # Find cases with ALL specified codes
            >>> query.filter_by_cpt(["44970", "44979"], use_any=False)
        """
        if isinstance(cpt_codes, str):
            cpt_codes = [cpt_codes]
        
        schema_names = self._lazy_frame.collect_schema().names()
        if ALL_CPT_CODES_FIELD in schema_names:
            # Use the array column if it exists
            if use_any:
                # Check if any CPT code is in the list
                expr = pl.col(ALL_CPT_CODES_FIELD).list.eval(
                    pl.element().is_in(cpt_codes)
                ).list.any()
            else:
                # Check if all CPT codes are in the list
                expr = pl.all_horizontal([
                    pl.col(ALL_CPT_CODES_FIELD).list.contains(code)
                    for code in cpt_codes
                ])
            
            self._lazy_frame = self._lazy_frame.filter(expr)
        else:
            # Fall back to checking individual CPT columns
            schema_names = self._lazy_frame.collect_schema().names()
            cpt_columns = [col for col in schema_names if col.startswith("CPT")]
            
            if not cpt_columns:
                raise ValueError("No CPT columns found in the dataset")
            
            if use_any:
                # ANY of the codes in ANY of the columns
                expr = pl.any_horizontal([
                    pl.col(col).is_in(cpt_codes) for col in cpt_columns
                ])
            else:
                # ALL codes must be present somewhere
                expressions = []
                for code in cpt_codes:
                    code_expr = pl.any_horizontal([
                        pl.col(col) == code for col in cpt_columns
                    ])
                    expressions.append(code_expr)
                expr = pl.all_horizontal(expressions)
            
            self._lazy_frame = self._lazy_frame.filter(expr)
        
        return self
    
    def filter_by_diagnosis(
        self, 
        diagnosis_codes: Union[str, List[str]], 
        use_any: bool = True
    ) -> Self:
        """Filter by diagnosis codes (ICD-9 or ICD-10).
        
        This searches across all diagnosis columns in the dataset.
        
        Args:
            diagnosis_codes: Single diagnosis code or list of codes.
            use_any: If True, include rows with ANY of the specified codes.
                    If False, include rows with ALL of the specified codes.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> # Single diagnosis
            >>> query.filter_by_diagnosis("K80.20")
            
            >>> # Multiple diagnoses (ANY)
            >>> query.filter_by_diagnosis(["K80.20", "K80.21"])
            
            >>> # Multiple diagnoses (ALL)
            >>> query.filter_by_diagnosis(["K80.20", "E11.9"], use_any=False)
        """
        if isinstance(diagnosis_codes, str):
            diagnosis_codes = [diagnosis_codes]
        
        schema_names = self._lazy_frame.collect_schema().names()
        if ALL_DIAGNOSIS_CODES_FIELD in schema_names:
            # Use the array column if it exists
            if use_any:
                expr = pl.col(ALL_DIAGNOSIS_CODES_FIELD).list.eval(
                    pl.element().is_in(diagnosis_codes)
                ).list.any()
            else:
                expr = pl.all_horizontal([
                    pl.col(ALL_DIAGNOSIS_CODES_FIELD).list.contains(code)
                    for code in diagnosis_codes
                ])
            
            self._lazy_frame = self._lazy_frame.filter(expr)
        else:
            # Fall back to checking individual columns
            diag_columns = []
            for col in DIAGNOSIS_COLUMNS:
                if col in schema_names:
                    diag_columns.append(col)
            
            if not diag_columns:
                raise ValueError("No diagnosis columns found in the dataset")
            
            if use_any:
                expr = pl.any_horizontal([
                    pl.col(col).is_in(diagnosis_codes) for col in diag_columns
                ])
            else:
                expressions = []
                for code in diagnosis_codes:
                    code_expr = pl.any_horizontal([
                        pl.col(col) == code for col in diag_columns
                    ])
                    expressions.append(code_expr)
                expr = pl.all_horizontal(expressions)
            
            self._lazy_frame = self._lazy_frame.filter(expr)
        
        return self
    
    def filter_active_variables(self, year_threshold: int = 2015) -> Self:
        """Filter to only include variables active after a certain year.
        
        This is useful for longitudinal analyses where you want consistent
        variables across years.
        
        Args:
            year_threshold: Only include columns with non-null values after this year.
            
        Returns:
            Self for method chaining.
        """
        # This would require column-level metadata about when variables were active
        # For now, we'll select columns that have non-null values after the threshold
        
        # First, get a sample of data after the threshold to check which columns have values
        sample_df = (self._lazy_frame
                    .filter(pl.col("OPERYR") >= year_threshold)
                    .select(pl.all().drop_nulls().len())
                    .collect())
        
        # Find columns with at least some non-null values
        active_columns = []
        for col in sample_df.columns:
            if sample_df[col].item() > 0:
                active_columns.append(col)
        
        # Select only active columns
        self._lazy_frame = self._lazy_frame.select(active_columns)
        
        return self
    
    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying LazyFrame.
        
        This allows NSQIPQuery to behave like a LazyFrame, supporting all
        LazyFrame methods transparently.
        
        Args:
            name: Attribute name to access.
            
        Returns:
            The attribute from the underlying LazyFrame.
        """
        # Get the attribute from the LazyFrame
        attr = getattr(self._lazy_frame, name)
        
        # If it's a method that returns a LazyFrame, wrap it to return NSQIPQuery
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, pl.LazyFrame):
                    # Create a new NSQIPQuery with the modified LazyFrame
                    new_query = NSQIPQuery.__new__(NSQIPQuery)
                    new_query.parquet_path = self.parquet_path
                    new_query.parquet_files = self.parquet_files
                    new_query.metadata = self.metadata
                    new_query._lazy_frame = result
                    return new_query
                return result
            return wrapper
        return attr


def load_data(
    data_path: Union[str, Path],
    year: Optional[Union[int, List[int]]] = None,
) -> NSQIPQuery:
    """Load NSQIP data from a parquet directory.
    
    This is the main entry point for loading NSQIP data. It returns an
    NSQIPQuery object that behaves like a LazyFrame and supports all Polars
    operations directly.
    
    Args:
        data_path: Path to the parquet directory or file.
        year: Optional year(s) to filter to immediately.
        
    Returns:
        NSQIPQuery object that supports both NSQIP-specific filtering methods
        and all standard Polars LazyFrame operations.
        
    Examples:
        >>> # Load all data and use directly like a LazyFrame
        >>> df = (load_data("path/to/parquet/dir")
        ...       .filter(pl.col("AGE_AS_INT") > 50)
        ...       .collect())
        
        >>> # Mix NSQIP-specific and Polars methods
        >>> df = (load_data("path/to/parquet/dir", year=2021)
        ...       .filter_by_cpt("44970")
        ...       .select(["CASEID", "AGE", "OPERYR"])
        ...       .collect())
        
        >>> # Complex filtering with Polars expressions
        >>> df = (load_data("path/to/parquet/dir")
        ...       .filter(
        ...           (pl.col("ALL_CPT_CODES").list.len() == 1) &
        ...           (pl.col("ALL_CPT_CODES").list.first().is_in(["42821", "42826"]))
        ...       )
        ...       .collect())
    """
    query = NSQIPQuery(data_path)
    
    if year is not None:
        query = query.filter_by_year(year)
    
    return query