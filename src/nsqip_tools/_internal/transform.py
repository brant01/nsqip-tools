"""Transformation functions for NSQIP data using Polars."""
from pathlib import Path
import logging
import polars as pl
import json
from typing import List, Optional

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
    
    # Process each parquet file
    for parquet_file in parquet_dir.glob("*.parquet"):
        if parquet_file.name == "metadata.json":
            continue
            
        logger.info(f"Transforming {parquet_file.name}")
        
        # Read the parquet file
        df = pl.read_parquet(parquet_file)
        
        # Apply transformations
        df = convert_numeric_columns(df)
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
    
    logger.info("All transformations complete")


def convert_numeric_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Convert columns that contain numeric data to appropriate numeric types.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with numeric columns converted
    """
    for col in df.columns:
        if col in NEVER_NUMERIC:
            continue
            
        # Try to convert to numeric
        try:
            # Check if column contains numeric data by trying to cast a sample
            sample = df[col].drop_nulls().head(1000)
            if len(sample) > 0:
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