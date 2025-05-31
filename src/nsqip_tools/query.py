"""Query and filtering functions for NSQIP data.

This module provides a fluent API for filtering NSQIP data that integrates
seamlessly with Polars LazyFrame operations.
"""
from pathlib import Path
from typing import List, Optional, Union, Self, Dict, Any
import polars as pl
import duckdb

from .constants import (
    TABLE_NAME,
    ALL_CPT_CODES_FIELD,
    ALL_DIAGNOSIS_CODES_FIELD,
    DIAGNOSIS_COLUMNS,
)
from ._internal.memory_utils import get_available_memory, format_bytes


class NSQIPQuery:
    """A query builder for NSQIP data that returns Polars LazyFrames.
    
    This class provides a fluent interface for filtering NSQIP data that can be
    chained with standard Polars operations.
    
    Examples:
        >>> # Basic filtering
        >>> df = (NSQIPQuery("path/to/data.duckdb")
        ...       .filter_by_cpt(["44970", "44979"])
        ...       .filter_by_year([2020, 2021])
        ...       .collect())
        
        >>> # Combine with Polars operations
        >>> df = (NSQIPQuery("path/to/data.duckdb")
        ...       .filter_by_diagnosis(["K80.20"])
        ...       .filter_active_variables()
        ...       .lazy_frame
        ...       .select(["CASEID", "AGE", "OPERYR", "CPT"])
        ...       .filter(pl.col("AGE_AS_INT") > 50)
        ...       .collect())
    """
    
    def __init__(self, db_path: Union[str, Path]):
        """Initialize a new NSQIP query.
        
        Args:
            db_path: Path to the DuckDB database file.
            
        Raises:
            FileNotFoundError: If the database file doesn't exist.
            ValueError: If the database doesn't contain the expected table.
        """
        self.db_path = Path(db_path)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database file not found: {self.db_path}")
        
        # Verify table exists
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            tables = con.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if TABLE_NAME not in table_names:
                raise ValueError(
                    f"Database does not contain expected table '{TABLE_NAME}'. "
                    f"Found tables: {table_names}"
                )
        
        # Initialize with full table scan
        self._lazy_frame: Optional[pl.LazyFrame] = None
        self._load_full_table()
    
    def _load_full_table(self) -> None:
        """Load the full table as a LazyFrame."""
        # Use DuckDB's native SQL to get a LazyFrame
        conn = duckdb.connect(str(self.db_path), read_only=True)
        arrow_table = conn.execute(f"SELECT * FROM {TABLE_NAME}").fetch_arrow_table()
        self._lazy_frame = pl.from_arrow(arrow_table).lazy()
        conn.close()
    
    @property
    def lazy_frame(self) -> pl.LazyFrame:
        """Get the current LazyFrame for further Polars operations.
        
        Returns:
            The current filtered LazyFrame.
        """
        if self._lazy_frame is None:
            raise RuntimeError("LazyFrame not initialized")
        return self._lazy_frame
    
    def filter_by_cpt(self, cpt_codes: List[str]) -> Self:
        """Filter data to cases with any of the specified CPT codes.
        
        This searches across all CPT fields (primary and additional procedures).
        
        Args:
            cpt_codes: List of CPT codes to filter by.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> query.filter_by_cpt(["44970", "44979"])  # Laparoscopic procedures
        """
        if not cpt_codes:
            return self
        
        # Filter using list operations - check if any CPT code is in the list
        filter_expr = pl.lit(False)
        for cpt in cpt_codes:
            filter_expr = filter_expr | pl.col(ALL_CPT_CODES_FIELD).list.contains(cpt)
        
        self._lazy_frame = self.lazy_frame.filter(filter_expr)
        
        return self
    
    def filter_by_diagnosis(self, diagnosis_codes: List[str]) -> Self:
        """Filter data to cases with any of the specified diagnosis codes.
        
        This searches across all diagnosis fields (ICD-9 and ICD-10).
        
        Args:
            diagnosis_codes: List of diagnosis codes to filter by.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> query.filter_by_diagnosis(["K80.20", "K80.21"])  # Gallstones
        """
        if not diagnosis_codes:
            return self
        
        # Check if we have the array column or need to check individual columns
        try:
            # Try to use the array column first
            self.lazy_frame.select(pl.col(ALL_DIAGNOSIS_CODES_FIELD)).collect_schema()
            has_array_column = True
        except:
            has_array_column = False
        
        if has_array_column:
            # Use the array column if it exists
            filter_expr = pl.lit(False)
            for diag in diagnosis_codes:
                filter_expr = filter_expr | pl.col(ALL_DIAGNOSIS_CODES_FIELD).list.contains(diag)
            
            self._lazy_frame = self.lazy_frame.filter(filter_expr)
        else:
            # Fall back to checking individual columns
            # Create a condition for any diagnosis column containing any code
            conditions = []
            schema_names = self.lazy_frame.collect_schema().names()
            for col in DIAGNOSIS_COLUMNS:
                if col in schema_names:
                    for code in diagnosis_codes:
                        conditions.append(pl.col(col) == code)
            
            if conditions:
                self._lazy_frame = self.lazy_frame.filter(
                    pl.any_horizontal(conditions)
                )
        
        return self
    
    def filter_by_year(self, years: List[int]) -> Self:
        """Filter data to specific operation years.
        
        Args:
            years: List of years to include.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> query.filter_by_year([2020, 2021, 2022])
        """
        if not years:
            return self
        
        # Convert years to strings since OPERYR is stored as string
        year_strings = [str(year) for year in years]
        self._lazy_frame = self.lazy_frame.filter(
            pl.col("OPERYR").is_in(year_strings)
        )
        
        return self
    
    def filter_active_variables(self) -> Self:
        """Filter to only include variables that have data in the most recent year.
        
        This removes columns that are all null in the most recent year of data,
        which typically indicates discontinued variables.
        
        Returns:
            Self for method chaining.
        """
        # First, get the most recent year
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            max_year = con.execute(
                f"SELECT MAX(OPERYR) FROM {TABLE_NAME}"
            ).fetchone()[0]
        
        # Get data for the most recent year to check nulls
        recent_year_df = self.lazy_frame.filter(
            pl.col("OPERYR") == max_year
        ).collect()
        
        # Find columns that have at least one non-null value
        active_columns = []
        for col in recent_year_df.columns:
            if not recent_year_df[col].is_null().all():
                active_columns.append(col)
        
        # Select only active columns
        self._lazy_frame = self.lazy_frame.select(active_columns)
        
        return self
    
    def filter(
        self,
        cpt: Optional[List[str]] = None,
        diagnosis: Optional[List[str]] = None,
        year: Optional[List[int]] = None,
        active_only: bool = False,
    ) -> Self:
        """Apply multiple filters at once.
        
        Args:
            cpt: List of CPT codes to filter by.
            diagnosis: List of diagnosis codes to filter by.
            year: List of years to include.
            active_only: Whether to filter to only active variables.
            
        Returns:
            Self for method chaining.
            
        Examples:
            >>> query.filter(
            ...     cpt=["44970"],
            ...     year=[2020, 2021],
            ...     active_only=True
            ... )
        """
        if cpt is not None:
            self.filter_by_cpt(cpt)
        if diagnosis is not None:
            self.filter_by_diagnosis(diagnosis)
        if year is not None:
            self.filter_by_year(year)
        if active_only:
            self.filter_active_variables()
        
        return self
    
    def collect(self) -> pl.DataFrame:
        """Execute the query and return results as a DataFrame.
        
        Returns:
            The filtered data as a Polars DataFrame.
        """
        return self.lazy_frame.collect()
    
    def estimate_size(self) -> Dict[str, Any]:
        """Estimate the size of the result set before collecting.
        
        This runs a quick query to estimate row count and memory usage.
        
        Returns:
            Dictionary with:
                - estimated_rows: Estimated number of rows
                - estimated_memory: Estimated memory usage (bytes)
                - estimated_memory_str: Human-readable memory size
                - available_memory: Available system memory (bytes)
                - available_memory_str: Human-readable available memory
                - safe_to_collect: Whether it's safe to collect
        """
        # Get row count estimate
        row_count = self.lazy_frame.select(pl.count()).collect().item()
        
        # Get column count
        col_count = len(self.lazy_frame.columns)
        
        # Estimate bytes per value (conservative estimate)
        # Assume average of 8 bytes per numeric, 50 bytes per string
        # This is a rough estimate - actual size varies by data type
        bytes_per_value = 30  # Conservative average
        
        # Estimate total memory
        estimated_bytes = row_count * col_count * bytes_per_value
        
        # Add overhead for Python objects and Polars internals (roughly 2x)
        estimated_bytes *= 2
        
        # Get available memory
        available_memory = get_available_memory()
        
        # Consider it safe if estimated size is less than 50% of available memory
        safe_to_collect = estimated_bytes < (available_memory * 0.5)
        
        return {
            "estimated_rows": row_count,
            "estimated_columns": col_count,
            "estimated_memory": estimated_bytes,
            "estimated_memory_str": format_bytes(estimated_bytes),
            "available_memory": available_memory,
            "available_memory_str": format_bytes(available_memory),
            "safe_to_collect": safe_to_collect,
        }
    
    def safe_collect(
        self,
        memory_limit_fraction: float = 0.5,
        force: bool = False
    ) -> pl.DataFrame:
        """Safely collect results with memory checking.
        
        This method estimates the result size and warns or prevents collection
        if it would use too much memory.
        
        Args:
            memory_limit_fraction: Maximum fraction of available memory to use (default 0.5).
            force: If True, collect even if memory usage is high (use with caution).
            
        Returns:
            The filtered data as a Polars DataFrame.
            
        Raises:
            MemoryError: If estimated size exceeds available memory and force=False.
            
        Examples:
            >>> # Safe collection with automatic memory checking
            >>> df = query.safe_collect()
            
            >>> # Force collection even if memory usage is high
            >>> df = query.safe_collect(force=True)
            
            >>> # Use only 30% of available memory
            >>> df = query.safe_collect(memory_limit_fraction=0.3)
        """
        estimate = self.estimate_size()
        
        available_memory = estimate["available_memory"]
        estimated_memory = estimate["estimated_memory"]
        memory_limit = available_memory * memory_limit_fraction
        
        if estimated_memory > memory_limit:
            error_msg = (
                f"Estimated result size ({estimate['estimated_memory_str']}) "
                f"exceeds memory limit ({format_bytes(memory_limit)} = "
                f"{memory_limit_fraction*100:.0f}% of available {estimate['available_memory_str']}). "
                f"The result would have {estimate['estimated_rows']:,} rows and "
                f"{estimate['estimated_columns']} columns."
            )
            
            if not force:
                error_msg += (
                    "\n\nSuggestions:\n"
                    "1. Add more filters to reduce the result size\n"
                    "2. Select only needed columns with .lazy_frame.select([...])\n"
                    "3. Use .safe_collect(memory_limit_fraction=0.8) to use more memory\n"
                    "4. Use .safe_collect(force=True) to override (may cause system slowdown)\n"
                    "5. Process data in chunks using .lazy_frame operations"
                )
                raise MemoryError(error_msg)
            else:
                import warnings
                warnings.warn(
                    f"WARNING: {error_msg}\n"
                    "Proceeding with collection due to force=True. "
                    "This may cause system slowdown or crashes.",
                    ResourceWarning
                )
        
        return self.collect()


def load_data(db_path: Union[str, Path]) -> NSQIPQuery:
    """Load NSQIP data from a DuckDB database.
    
    This is the main entry point for querying NSQIP data.
    
    Args:
        db_path: Path to the DuckDB database file.
        
    Returns:
        An NSQIPQuery object for filtering and analysis.
        
    Examples:
        >>> df = (load_data("nsqip_data.duckdb")
        ...       .filter_by_cpt(["44970"])
        ...       .collect())
    """
    return NSQIPQuery(db_path)