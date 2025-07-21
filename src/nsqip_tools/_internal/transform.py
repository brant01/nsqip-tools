"""Transformation functions for NSQIP data using Polars."""
from pathlib import Path
import logging
import polars as pl
import json
from typing import List, Optional, Dict

from ..constants import (
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
)

logger = logging.getLogger(__name__)


def apply_transformations(parquet_dir: Path, dataset_type: str, memory_limit: str) -> None:
    """Apply all standard transformations to parquet files.
    
    Args:
        parquet_dir: Directory containing parquet files
        dataset_type: Type of dataset ("adult" or "pediatric")
        memory_limit: Memory limit for operations (not used in Polars version)
    """
    logger.info(f"Applying transformations to parquet files in {parquet_dir}")
    
    # First, determine global schema by examining all files
    parquet_files = [f for f in parquet_dir.glob("*.parquet") if f.name != "metadata.json"]
    logger.info("Determining global schema for consistent data types...")
    global_schema = determine_global_schema(parquet_files)
    logger.info(f"Global schema determined for {len(global_schema)} numeric columns")
    
    # Process each parquet file with consistent schema
    for parquet_file in parquet_files:
        logger.info(f"Transforming {parquet_file.name}")
        
        # Read the parquet file
        df = pl.read_parquet(parquet_file)
        
        # Apply transformations
        df = convert_numeric_columns(df, global_schema)
        df = process_age_columns(df)
        df = create_cpt_array(df)
        df = create_diagnosis_array(df)
        df = split_comma_separated_columns(df)
        df = combine_race_columns(df)
        
        # Dataset-specific transformations
        if dataset_type == "adult":
            df = add_work_rvu_columns(df)
            df = add_free_flap_indicators(df)
        
        # Write back to parquet
        df.write_parquet(parquet_file)
        logger.info(f"Completed transformations for {parquet_file.name}")
    
    # Update metadata
    metadata_path = parquet_dir / "metadata.json"
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    metadata["transformations_applied"] = True
    metadata["transformation_version"] = "2.0"
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Validate schema consistency after transformations
    logger.info("Validating schema consistency across all files...")
    schema_valid = validate_schema_consistency(parquet_files)
    
    if schema_valid:
        logger.info("✅ All transformations complete - schema validation passed")
        metadata["schema_validation"] = "passed"
    else:
        logger.warning("⚠️  Transformations complete but schema validation failed")
        metadata["schema_validation"] = "failed"
    
    # Update metadata with validation result
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)


def determine_global_schema(parquet_files: List[Path]) -> Dict[str, pl.DataType]:
    """Determine optimal data types for each column by examining all parquet files.
    
    Args:
        parquet_files: List of parquet files to analyze
        
    Returns:
        Dictionary mapping column names to optimal data types
    """
    logger = logging.getLogger(__name__)
    column_samples = {}
    
    # Collect samples from all files
    for parquet_file in parquet_files:
        try:
            df = pl.scan_parquet(parquet_file)
            # Get a larger sample for better type inference
            sample_df = df.head(5000).collect()
            
            for col in sample_df.columns:
                if col in NEVER_NUMERIC:
                    continue
                    
                if col not in column_samples:
                    column_samples[col] = []
                
                # Get non-null values from this file
                non_null = sample_df[col].drop_nulls()
                if len(non_null) > 0:
                    column_samples[col].extend(non_null.to_list())
        except Exception as e:
            logger.warning(f"Could not sample from {parquet_file}: {e}")
    
    # Determine best type for each column based on all samples
    global_schema = {}
    for col, samples in column_samples.items():
        if not samples:
            continue
            
        # Create a series with all samples to test type conversion
        test_series = pl.Series(samples)
        
        # Clean whitespace-only strings
        test_series = test_series.map_elements(
            lambda x: None if isinstance(x, str) and x.strip() == "" else x,
            return_dtype=pl.Utf8
        ).drop_nulls()
        
        if len(test_series) == 0:
            continue
        
        # Try integer first, then float
        try:
            test_series.cast(pl.Int64, strict=True)
            global_schema[col] = pl.Int64
            logger.debug(f"Global schema: {col} -> Int64")
        except:
            try:
                test_series.cast(pl.Float64, strict=True)
                global_schema[col] = pl.Float64
                logger.debug(f"Global schema: {col} -> Float64")
            except:
                # Keep as string
                pass
    
    return global_schema


def validate_schema_consistency(parquet_files: List[Path]) -> bool:
    """Validate that all parquet files have consistent schemas after transformation.
    
    Args:
        parquet_files: List of parquet files to validate
        
    Returns:
        True if schemas are consistent, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    if not parquet_files:
        return True
    
    # Get reference schema from first file
    try:
        reference_schema = pl.scan_parquet(parquet_files[0]).collect_schema()
        logger.info(f"Using {parquet_files[0].name} as reference schema")
    except Exception as e:
        logger.error(f"Could not read reference file {parquet_files[0]}: {e}")
        return False
    
    # Check all other files against reference
    inconsistencies = []
    for parquet_file in parquet_files[1:]:
        try:
            schema = pl.scan_parquet(parquet_file).collect_schema()
            
            # Check for column differences
            ref_cols = set(reference_schema.names())
            file_cols = set(schema.names())
            
            if ref_cols != file_cols:
                missing_in_file = ref_cols - file_cols
                extra_in_file = file_cols - ref_cols
                inconsistencies.append(f"{parquet_file.name}: missing columns {missing_in_file}, extra columns {extra_in_file}")
                continue
            
            # Check for type differences
            for col in ref_cols:
                ref_type = reference_schema[col]
                file_type = schema[col]
                if ref_type != file_type:
                    inconsistencies.append(f"{parquet_file.name}: column {col} has type {file_type}, expected {ref_type}")
        
        except Exception as e:
            inconsistencies.append(f"{parquet_file.name}: could not read schema - {e}")
    
    if inconsistencies:
        logger.error("Schema inconsistencies found:")
        for inconsistency in inconsistencies:
            logger.error(f"  - {inconsistency}")
        return False
    
    logger.info(f"Schema validation passed for {len(parquet_files)} files")
    return True


def convert_numeric_columns(df: pl.DataFrame, global_schema: Optional[Dict[str, pl.DataType]] = None) -> pl.DataFrame:
    """Convert columns that contain numeric data to appropriate numeric types.
    
    Args:
        df: Input DataFrame
        global_schema: Optional dictionary of column names to target data types
        
    Returns:
        DataFrame with numeric columns converted
    """
    if global_schema:
        # Use predefined global schema for consistency
        for col in df.columns:
            if col in global_schema and col not in NEVER_NUMERIC:
                try:
                    target_type = global_schema[col]
                    df = df.with_columns(pl.col(col).cast(target_type))
                    logger.debug(f"Converted {col} to {target_type} using global schema")
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to {target_type}: {e}")
        return df
    
    # Fallback to original sample-based approach
    for col in df.columns:
        if col in NEVER_NUMERIC:
            continue
            
        # Try to convert to numeric
        try:
            # Check if column contains numeric data by trying to cast a sample
            sample = df[col].drop_nulls().head(1000)
            if len(sample) > 0:
                # First, clean any remaining whitespace-only values that weren't caught as nulls
                df = df.with_columns(
                    pl.col(col).map_elements(
                        lambda x: None if isinstance(x, str) and x.strip() == "" else x,
                        return_dtype=pl.Utf8
                    )
                )
                
                # Try integer first
                try:
                    _ = sample.cast(pl.Int64, strict=True)
                    df = df.with_columns(pl.col(col).cast(pl.Int64))
                    logger.debug(f"Converted {col} to Int64")
                except:
                    # Try float
                    try:
                        _ = sample.cast(pl.Float64, strict=True)
                        df = df.with_columns(pl.col(col).cast(pl.Float64))
                        logger.debug(f"Converted {col} to Float64")
                    except:
                        # Keep as string
                        pass
        except Exception as e:
            logger.debug(f"Could not convert {col}: {e}")
    
    return df


def process_age_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Process age columns to add integer age and 90+ indicator.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with age columns added
    """
    if AGE_FIELD not in df.columns:
        return df
    
    # Add integer age column
    df = df.with_columns(
        pl.when(pl.col(AGE_FIELD) == AGE_NINETY_PLUS)
        .then(90)
        .otherwise(pl.col(AGE_FIELD).cast(pl.Int64, strict=False))
        .alias(AGE_AS_INT_FIELD)
    )
    
    # Add 90+ indicator
    df = df.with_columns(
        (pl.col(AGE_FIELD) == AGE_NINETY_PLUS).alias(AGE_IS_90_PLUS_FIELD)
    )
    
    logger.info(f"Added {AGE_AS_INT_FIELD} and {AGE_IS_90_PLUS_FIELD} columns")
    return df


def create_cpt_array(df: pl.DataFrame) -> pl.DataFrame:
    """Create array of all CPT codes from individual CPT columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with CPT array column added
    """
    # Find CPT columns that exist in this dataset
    cpt_cols = [col for col in CPT_COLUMNS if col in df.columns]
    
    if not cpt_cols:
        logger.warning("No CPT columns found")
        return df
    
    # Create array of non-null CPT codes
    df = df.with_columns(
        pl.concat_list([pl.col(col) for col in cpt_cols])
        .list.drop_nulls()
        .alias(ALL_CPT_CODES_FIELD)
    )
    
    logger.info(f"Created {ALL_CPT_CODES_FIELD} from {len(cpt_cols)} CPT columns")
    return df


def create_diagnosis_array(df: pl.DataFrame) -> pl.DataFrame:
    """Create array of all diagnosis codes from individual diagnosis columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with diagnosis array column added
    """
    # Find diagnosis columns that exist
    diag_cols = [col for col in DIAGNOSIS_COLUMNS if col in df.columns]
    
    if not diag_cols:
        logger.warning("No diagnosis columns found")
        return df
    
    # Create array of non-null diagnosis codes
    df = df.with_columns(
        pl.concat_list([pl.col(col) for col in diag_cols])
        .list.drop_nulls()
        .alias(ALL_DIAGNOSIS_CODES_FIELD)
    )
    
    logger.info(f"Created {ALL_DIAGNOSIS_CODES_FIELD} from {len(diag_cols)} diagnosis columns")
    return df


def split_comma_separated_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Split comma-separated columns into arrays.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with comma-separated columns converted to arrays
    """
    for col in COMMA_SEPARATED_COLUMNS:
        if col in df.columns:
            new_col = f"{col}_ARRAY"
            df = df.with_columns(
                pl.col(col)
                .str.split(",")
                .list.eval(pl.element().str.strip_chars())
                .alias(new_col)
            )
            logger.info(f"Split {col} into {new_col}")
    
    return df


def combine_race_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Combine RACE and RACE_NEW columns into RACE_COMBINED.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with combined race column
    """
    if RACE_FIELD not in df.columns and RACE_NEW_FIELD not in df.columns:
        return df
    
    if RACE_FIELD in df.columns and RACE_NEW_FIELD in df.columns:
        # Use RACE_NEW if available, otherwise RACE
        df = df.with_columns(
            pl.coalesce([pl.col(RACE_NEW_FIELD), pl.col(RACE_FIELD)])
            .alias(RACE_COMBINED_FIELD)
        )
        logger.info(f"Created {RACE_COMBINED_FIELD} from {RACE_NEW_FIELD} and {RACE_FIELD}")
    elif RACE_NEW_FIELD in df.columns:
        df = df.with_columns(pl.col(RACE_NEW_FIELD).alias(RACE_COMBINED_FIELD))
        logger.info(f"Created {RACE_COMBINED_FIELD} from {RACE_NEW_FIELD}")
    else:
        df = df.with_columns(pl.col(RACE_FIELD).alias(RACE_COMBINED_FIELD))
        logger.info(f"Created {RACE_COMBINED_FIELD} from {RACE_FIELD}")
    
    return df


def add_work_rvu_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add work RVU total column (placeholder for adult dataset).
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with work RVU columns
    """
    # This is a placeholder - actual RVU calculation would need proper mapping
    if "CPT" in df.columns:
        df = df.with_columns(
            pl.lit(0.0).alias("WORK_RVU_TOTAL")
        )
        logger.info("Added WORK_RVU_TOTAL column (placeholder)")
    
    return df


def add_free_flap_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Add free flap indicator columns (placeholder for adult dataset).
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with free flap indicators
    """
    # This is a placeholder - actual free flap detection would need CPT code lists
    if ALL_CPT_CODES_FIELD in df.columns:
        df = df.with_columns([
            pl.lit(False).alias("HAS_FREE_FLAP"),
            pl.lit(False).alias("HAS_ANY_FLAP"),
        ])
        logger.info("Added free flap indicator columns (placeholder)")
    
    return df