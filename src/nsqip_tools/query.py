"""Query and filtering functions for NSQIP data.

This module provides a fluent API for filtering NSQIP data that integrates
seamlessly with Polars LazyFrame operations.
"""
from pathlib import Path
from typing import List, Optional, Union, Self, Dict, Any
import polars as pl
import json

from .constants import (
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
        >>> df = (NSQIPQuery("path/to/parquet/dir")
        ...       .filter_by_cpt(["44970", "44979"])
        ...       .filter_by_year([2020, 2021])
        ...       .collect())
        
        >>> # Combine with Polars operations
        >>> df = (NSQIPQuery("path/to/parquet/dir")
        ...       .filter_by_diagnosis(["K80.20"])
        ...       .filter_active_variables()
        ...       .lazy_frame
        ...       .select(["CASEID", "AGE", "OPERYR", "CPT"])
        ...       .filter(pl.col("AGE_AS_INT") > 50)
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
        
        # Check memory
        available_memory = get_available_memory()
        self._memory_warning_threshold = 0.8  # Warn if result might use >80% of available memory
        
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
            # Union all frames - they should have the same schema
            return pl.concat(lazy_frames, how="vertical_relaxed")
    
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
            cpt_columns = [col for col in self._lazy_frame.columns if col.startswith("CPT")]
            
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
    
    def select_demographics(self) -> Self:
        """Select common demographic variables.
        
        Returns:
            Self for method chaining.
        """
        demographic_cols = [
            "CASEID", "OPERYR", "AGE", "AGE_AS_INT", "AGE_IS_90_PLUS",
            "SEX", "RACE", "RACE_NEW", "RACE_COMBINED", "ETHNICITY",
            "HEIGHT", "WEIGHT", "BMI", "DIABETES", "SMOKE", "DYSPNEA",
            "FNSTATUS1", "FNSTATUS2", "HXCOPD", "ASCITES", "HXCHF",
            "HYPERMED", "RENAFAIL", "DIALYSIS", "DISCANCR", "WNDINF",
            "STEROID", "WTLOSS", "BLEEDIS", "TRANSFUS", "PRSEPIS",
        ]
        
        # Only select columns that exist
        schema_names = self._lazy_frame.collect_schema().names()
        available_cols = [col for col in demographic_cols if col in schema_names]
        
        self._lazy_frame = self._lazy_frame.select(available_cols)
        return self
    
    def select_outcomes(self) -> Self:
        """Select common outcome variables.
        
        Returns:
            Self for method chaining.
        """
        outcome_cols = [
            "CASEID", "OPERYR", "OPTIME", "TOTHLOS", "DSUPINFEC",
            "SUPINFEC", "WNDINFD", "ORGSPCSSI", "DEHIS", "OUPNEUMO",
            "REINTUB", "PULEMBOL", "FAILWEAN", "RENAINSF", "OPRENAFL",
            "URNINFEC", "CNSCVA", "CDARREST", "CDMI", "OTHBLEED",
            "OTHDVT", "OTHSYSEP", "OTHSESHOCK", "READMISSION1",
            "REOPERATION1", "MORTALITY", "MORTPROB", "MORBPROB",
            "DEATH30YN", "READMSUSP1",
        ]
        
        schema_names = self._lazy_frame.collect_schema().names()
        available_cols = [col for col in outcome_cols if col in schema_names]
        
        self._lazy_frame = self._lazy_frame.select(available_cols)
        return self
    
    def count(self) -> int:
        """Get the count of rows matching current filters.
        
        Returns:
            Number of rows.
        """
        return self._lazy_frame.select(pl.len()).collect().item()
    
    def collect(self) -> pl.DataFrame:
        """Execute the query and return results as a DataFrame.
        
        Returns:
            Polars DataFrame with query results.
            
        Raises:
            MemoryError: If the result set is too large for available memory.
        """
        # Estimate memory usage
        row_count = self.count()
        col_count = len(self._lazy_frame.collect_schema().names())
        
        # Rough estimate: 8 bytes per cell average
        estimated_memory = row_count * col_count * 8
        available_memory = get_available_memory()
        
        if estimated_memory > available_memory * self._memory_warning_threshold:
            raise MemoryError(
                f"Query result ({format_bytes(estimated_memory)}) may exceed "
                f"available memory ({format_bytes(available_memory)}). "
                f"Consider using .lazy_frame for streaming operations."
            )
        
        return self._lazy_frame.collect()
    
    def sample(self, n: int = 1000, seed: Optional[int] = None) -> pl.DataFrame:
        """Get a random sample of rows.
        
        Args:
            n: Number of rows to sample.
            seed: Random seed for reproducibility.
            
        Returns:
            DataFrame with sampled rows.
        """
        total_rows = self.count()
        
        if n >= total_rows:
            return self.collect()
        
        # Collect first then sample (LazyFrame doesn't have sample method)
        df = self._lazy_frame.collect()
        return df.sample(n=n, seed=seed)
    
    def describe(self) -> Dict[str, Any]:
        """Get summary statistics about the current query.
        
        Returns:
            Dictionary with query information.
        """
        return {
            "total_rows": self.count(),
            "columns": len(self._lazy_frame.collect_schema().names()),
            "parquet_files": len(self.parquet_files),
            "path": str(self.parquet_path),
        }


def load_data(
    data_path: Union[str, Path],
    year: Optional[Union[int, List[int]]] = None,
) -> NSQIPQuery:
    """Load NSQIP data from a parquet directory.
    
    This is the main entry point for loading NSQIP data. It returns an
    NSQIPQuery object that can be used to filter and analyze the data.
    
    Args:
        data_path: Path to the parquet directory or file.
        year: Optional year(s) to filter to immediately.
        
    Returns:
        NSQIPQuery object for further filtering and analysis.
        
    Examples:
        >>> # Load all data
        >>> query = load_data("path/to/parquet/dir")
        
        >>> # Load specific year
        >>> query = load_data("path/to/parquet/dir", year=2021)
        
        >>> # Load multiple years
        >>> query = load_data("path/to/parquet/dir", year=[2019, 2020, 2021])
    """
    query = NSQIPQuery(data_path)
    
    if year is not None:
        query = query.filter_by_year(year)
    
    return query