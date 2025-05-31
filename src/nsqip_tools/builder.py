"""Main API for building NSQIP DuckDB databases.

This module provides the high-level interface for converting NSQIP text files
into optimized DuckDB databases with standard transformations.
"""
import logging
from pathlib import Path
from typing import Union, Optional, Literal, Dict, Any
from datetime import datetime
import duckdb

from .constants import (
    DATASET_TYPES,
    DB_NAME_TEMPLATE,
    TABLE_NAME,
    NEVER_NUMERIC,
    AGE_FIELD,
    AGE_AS_INT_FIELD,
    AGE_IS_90_PLUS_FIELD,
    AGE_NINETY_PLUS,
    CPT_COLUMNS,
    ALL_CPT_CODES_FIELD,
    DIAGNOSIS_COLUMNS,
    ALL_DIAGNOSIS_CODES_FIELD,
    COMMA_SEPARATED_COLUMNS,
    RACE_FIELD,
    RACE_NEW_FIELD,
    RACE_COMBINED_FIELD,
    EXPECTED_CASE_COUNTS,
)
from ._internal.ingest import create_duckdb_from_text
from ._internal.transform import (
    cast_column_in_place,
    fix_age_column as handle_age_column,
    add_combined_cpt_column as create_cpt_array_column,
    split_comma_separated_columns,
    add_total_rvu as calculate_work_rvu,
    add_free_flap_flags as derive_free_flap_columns,
)
from .data_dictionary import DataDictionaryGenerator
from ._internal.memory_utils import get_recommended_memory_limit, get_memory_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_duck_db(
    data_dir: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    dataset_type: Literal["adult", "pediatric"] = "adult",
    generate_dictionary: bool = True,
    memory_limit: Optional[str] = None,
    verify_case_counts: bool = True,
) -> Dict[str, Path]:
    """Build an NSQIP DuckDB database from text files with standard transformations.
    
    This function performs the complete pipeline of ingesting NSQIP text files,
    applying standard transformations, and optionally generating a data dictionary.
    All original data is preserved - transformations only add new columns.
    
    Args:
        data_dir: Directory containing NSQIP text files (tab-delimited).
        output_dir: Directory for output files. Defaults to data_dir.
        dataset_type: Type of NSQIP data ("adult" or "pediatric").
        generate_dictionary: Whether to generate a data dictionary.
        memory_limit: DuckDB memory limit (e.g., "4GB", "8GB"). If None, automatically
                     determined based on available system memory.
        verify_case_counts: Whether to verify case counts match expected values.
        
    Returns:
        Dictionary with paths to generated files:
            - "database": Path to the DuckDB database
            - "dictionary": Path to data dictionary (if generated)
            - "log": Path to the log file
            
    Raises:
        ValueError: If dataset_type is not supported or data_dir doesn't exist.
        RuntimeError: If database creation or transformation fails.
        Warning: If case counts don't match expected values (only if verify_case_counts=True).
        
    Examples:
        >>> # Basic usage
        >>> result = build_duck_db(
        ...     data_dir="/path/to/nsqip/files",
        ...     dataset_type="adult"
        ... )
        
        >>> # With custom output and memory settings
        >>> result = build_duck_db(
        ...     data_dir="/path/to/nsqip/files",
        ...     output_dir="/path/to/output",
        ...     dataset_type="pediatric",
        ...     memory_limit="8GB"
        ... )
    """
    # Validate inputs
    data_dir = Path(data_dir)
    if not data_dir.exists():
        raise ValueError(f"Data directory does not exist: {data_dir}")
    
    if dataset_type not in DATASET_TYPES:
        raise ValueError(
            f"Invalid dataset_type '{dataset_type}'. "
            f"Must be one of: {', '.join(DATASET_TYPES)}"
        )
    
    # Set output directory
    if output_dir is None:
        output_dir = data_dir
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define output files
    db_filename = DB_NAME_TEMPLATE.format(dataset_type=dataset_type)
    db_path = output_dir / db_filename
    log_path = output_dir / f"build_{dataset_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Set up file logging
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(file_handler)
    
    # Determine memory limit if not specified
    if memory_limit is None:
        memory_limit = get_recommended_memory_limit(conservative=True)
        mem_info = get_memory_info()
        logger.info(f"System memory: {mem_info['total']} total, {mem_info['available']} available")
        logger.info(f"Auto-detected memory limit: {memory_limit}")
    
    logger.info(f"Starting NSQIP database build for {dataset_type} data")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Memory limit: {memory_limit}")
    
    try:
        # Step 1: Create database from text files
        logger.info("Step 1: Creating database from text files")
        create_duckdb_from_text(
            text_file_dir=data_dir,
            db_name=db_filename,
            table_name=TABLE_NAME,
            dataset_type=dataset_type,
        )
        
        # Step 2: Verify case counts
        if verify_case_counts:
            logger.info("Step 2: Verifying case counts")
            _verify_case_counts(db_path, dataset_type)
        
        # Step 3: Apply standard transformations
        logger.info("Step 3: Applying standard transformations")
        _apply_transformations(db_path, dataset_type, memory_limit)
        
        # Step 4: Generate data dictionary
        result = {"database": db_path, "log": log_path}
        
        if generate_dictionary:
            logger.info("Step 4: Generating data dictionary")
            dict_path = _generate_data_dictionary(db_path, output_dir, dataset_type)
            result["dictionary"] = dict_path
        
        logger.info(f"Build complete! Database saved to: {db_path}")
        
    except Exception as e:
        logger.error(f"Build failed: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to build NSQIP database: {str(e)}") from e
    
    finally:
        # Remove file handler
        logger.removeHandler(file_handler)
        file_handler.close()
    
    return result


def _verify_case_counts(db_path: Path, dataset_type: str) -> None:
    """Verify that case counts match expected values per year.
    
    Logs warnings for any mismatches but does not fail the build.
    """
    expected_counts = EXPECTED_CASE_COUNTS.get(dataset_type, {})
    
    with duckdb.connect(str(db_path), read_only=True) as con:
        # Get actual counts by year
        actual_counts = con.execute(f"""
            SELECT OPERYR, COUNT(*) as case_count
            FROM {TABLE_NAME}
            GROUP BY OPERYR
            ORDER BY OPERYR
        """).df()
        
        # Convert to dictionary for easier comparison
        actual_dict = dict(zip(
            actual_counts['OPERYR'].astype(str),
            actual_counts['case_count']
        ))
        
        # Check each year
        all_match = True
        for year, expected in expected_counts.items():
            actual = actual_dict.get(year, 0)
            
            if actual != expected:
                all_match = False
                diff = actual - expected
                pct_diff = (diff / expected) * 100 if expected > 0 else 0
                
                logger.warning(
                    f"Case count mismatch for year {year}: "
                    f"expected {expected:,}, found {actual:,} "
                    f"(difference: {diff:+,}, {pct_diff:+.1f}%)"
                )
            else:
                logger.info(f"Year {year}: {actual:,} cases ✓")
        
        # Check for unexpected years
        for year in actual_dict:
            if year not in expected_counts:
                logger.warning(
                    f"Found data for unexpected year {year}: "
                    f"{actual_dict[year]:,} cases"
                )
        
        if all_match:
            logger.info("All case counts match expected values!")
        else:
            logger.warning(
                "Case count verification completed with mismatches. "
                "Please review the warnings above."
            )


def _apply_transformations(
    db_path: Path,
    dataset_type: str,
    memory_limit: str,
) -> None:
    """Apply all standard transformations to the database.
    
    This includes:
    - Type conversions for numeric columns
    - Age field processing (90+ handling)
    - CPT array creation
    - Diagnosis array creation  
    - Race field combination
    - RVU calculations
    - Free flap indicators
    """
    logger.info("Connecting to database for transformations")
    
    with duckdb.connect(str(db_path)) as con:
        # Set memory limit
        con.execute(f"SET memory_limit='{memory_limit}'")
        
        # Get all columns
        columns_df = con.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{TABLE_NAME}'"
        ).df()
        all_columns = set(columns_df['column_name'].tolist())
        
        # Step 1: Convert numeric columns (excluding those that should stay string)
        logger.info("Converting numeric columns")
        numeric_columns = []
        for col in all_columns:
            if col not in NEVER_NUMERIC:
                # Check if column contains numeric data
                try:
                    sample = con.execute(
                        f"SELECT {col} FROM {TABLE_NAME} "
                        f"WHERE {col} IS NOT NULL LIMIT 100"
                    ).fetchall()
                    
                    if sample and _is_likely_numeric(sample):
                        numeric_columns.append(col)
                except:
                    # Skip columns that cause errors
                    pass
        
        if numeric_columns:
            logger.info(f"Converting {len(numeric_columns)} numeric columns")
            for col in numeric_columns:
                try:
                    cast_column_in_place(db_path, TABLE_NAME, [col], "DOUBLE")
                except Exception as e:
                    logger.warning(f"Could not convert {col} to numeric: {e}")
        
        # Step 2: Handle age column
        if AGE_FIELD in all_columns:
            logger.info("Processing age column")
            handle_age_column(db_path, TABLE_NAME)
        
        # Step 3: Create CPT array column
        cpt_cols_present = [col for col in CPT_COLUMNS if col in all_columns]
        if cpt_cols_present:
            logger.info("Creating CPT array column")
            create_cpt_array_column(db_path, TABLE_NAME, cpt_cols_present)
        
        # Step 4: Create diagnosis array column
        diag_cols_present = [col for col in DIAGNOSIS_COLUMNS if col in all_columns]
        if diag_cols_present:
            logger.info("Creating diagnosis array column")
            _create_diagnosis_array_column(con, diag_cols_present)
        
        # Step 5: Handle comma-separated columns
        comma_cols_present = [col for col in COMMA_SEPARATED_COLUMNS if col in all_columns]
        if comma_cols_present:
            logger.info(f"Converting comma-separated columns: {comma_cols_present}")
            split_comma_separated_columns(db_path, TABLE_NAME, comma_cols_present)
        
        # Step 6: Combine race columns
        if RACE_FIELD in all_columns and RACE_NEW_FIELD in all_columns:
            logger.info("Combining race columns")
            _combine_race_columns(con)
        
        # Step 7: Calculate RVU (if applicable)
        if dataset_type == "adult":
            rvu_cols = [col for col in all_columns if col.startswith("WORKRVU")]
            if rvu_cols:
                logger.info("Calculating total work RVU")
                calculate_work_rvu(db_path, TABLE_NAME, rvu_cols)
        
        # Step 8: Derive free flap indicators
        if ALL_CPT_CODES_FIELD in all_columns or cpt_cols_present:
            logger.info("Deriving free flap indicators")
            derive_free_flap_columns(db_path, TABLE_NAME)


def _is_likely_numeric(samples: list) -> bool:
    """Check if samples are likely numeric values."""
    for value in samples:
        if value[0] is None:
            continue
        try:
            float(str(value[0]))
        except (ValueError, TypeError):
            return False
    return True


def _create_diagnosis_array_column(con: duckdb.DuckDBPyConnection, diag_cols: list) -> None:
    """Create array column combining all diagnosis codes."""
    # Build SQL to create array of non-null diagnosis codes
    array_elements = []
    for col in diag_cols:
        array_elements.append(f"CASE WHEN {col} IS NOT NULL THEN {col} END")
    
    array_sql = f"[{', '.join(array_elements)}]"
    
    # Remove nulls from array
    clean_array_sql = f"list_filter({array_sql}, x -> x IS NOT NULL)"
    
    con.execute(f"""
        ALTER TABLE {TABLE_NAME} 
        ADD COLUMN {ALL_DIAGNOSIS_CODES_FIELD} VARCHAR[]
    """)
    
    con.execute(f"""
        UPDATE {TABLE_NAME}
        SET {ALL_DIAGNOSIS_CODES_FIELD} = {clean_array_sql}
    """)


def _combine_race_columns(con: duckdb.DuckDBPyConnection) -> None:
    """Combine RACE and RACE_NEW columns."""
    con.execute(f"""
        ALTER TABLE {TABLE_NAME}
        ADD COLUMN {RACE_COMBINED_FIELD} VARCHAR
    """)
    
    con.execute(f"""
        UPDATE {TABLE_NAME}
        SET {RACE_COMBINED_FIELD} = COALESCE({RACE_NEW_FIELD}, {RACE_FIELD})
    """)


def _generate_data_dictionary(
    db_path: Path,
    output_dir: Path,
    dataset_type: str,
) -> Path:
    """Generate data dictionary using the DataDictionaryGenerator."""
    generator = DataDictionaryGenerator(db_path)
    
    # Generate all formats
    dict_base = output_dir / f"{dataset_type}_data_dictionary"
    
    # CSV format (primary for non-technical users)
    csv_path = dict_base.with_suffix(".csv")
    generator.save_to_csv(csv_path)
    
    # JSON format (for programmatic access)
    json_path = dict_base.with_suffix(".json")
    generator.save_to_json(json_path)
    
    # HTML format (for easy viewing)
    html_path = dict_base.with_suffix(".html")
    generator.save_to_html(html_path)
    
    logger.info(f"Data dictionary saved to: {csv_path}")
    
    return csv_path