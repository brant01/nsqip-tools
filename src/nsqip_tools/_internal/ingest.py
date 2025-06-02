from pathlib import Path
from typing import Optional, List
import logging
import polars as pl
from datetime import datetime
import json


def create_parquet_from_text(
    text_file_dir: Path,
    output_dir: Optional[Path] = None,
    dataset_type: str = "adult",
) -> Path:
    """
    Create parquet files from a directory of TXT files with tab-separated data.
    
    Args:
        text_file_dir: Directory containing NSQIP text files
        output_dir: Directory for output files. Defaults to text_file_dir
        dataset_type: Type of dataset ("adult" or "pediatric")
        
    Returns:
        Path to the parquet directory
    """
    logging.info("Starting parquet creation process.")
    
    if not text_file_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {text_file_dir}")
    
    txt_files = sorted(text_file_dir.glob("*.txt"))
    if not txt_files:
        logging.warning(f"No .txt files found in {text_file_dir}")
        raise ValueError(f"No .txt files found in {text_file_dir}")
    
    # Use provided output directory or default to text file directory
    if output_dir is None:
        # Create parquet subdirectory in text file directory
        parquet_dir = text_file_dir / f"{dataset_type}_nsqip_parquet"
    else:
        # Use the provided output directory directly (already named appropriately)
        parquet_dir = output_dir
    
    parquet_dir.mkdir(exist_ok=True)
    
    logging.info(f"Dataset type: {dataset_type}")
    logging.info(f"Found {len(txt_files)} files to process in {text_file_dir}")
    logging.info(f"Output parquet directory: {parquet_dir}")
    
    # Get all columns from all files first
    all_columns = get_all_columns(txt_files)
    logging.info(f"Unified column set has {len(all_columns)} columns.")
    
    # Process each file
    successful_files = []
    for i, file_path in enumerate(txt_files):
        logging.info(f"[{i+1}/{len(txt_files)}] Processing {file_path.name}")
        
        try:
            df = read_clean_csv(file_path)
            df = align_df_to_schema(df, all_columns)
            
            # Write to parquet with year suffix if OPERYR exists
            if "OPERYR" in df.columns:
                year = df["OPERYR"].unique()[0]
                parquet_path = parquet_dir / f"{dataset_type}_{year}.parquet"
            else:
                parquet_path = parquet_dir / f"{file_path.stem}.parquet"
            
            df.write_parquet(parquet_path)
            successful_files.append(parquet_path.name)
            
            logging.info(f"Wrote {df.shape[0]} rows to {parquet_path.name}")
            
        except Exception as e:
            logging.error(f"Error processing file {file_path.name}: {e}")
            continue
    
    # Create metadata file
    metadata = {
        "format": "parquet",
        "dataset_type": dataset_type,
        "created": str(datetime.now()),
        "columns": all_columns,
        "files": successful_files,
        "total_files": len(successful_files),
        "source_directory": str(text_file_dir),
    }
    
    metadata_path = parquet_dir / "metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logging.info(f"Created metadata file: {metadata_path}")
    logging.info(f"Parquet dataset complete. Processed {len(successful_files)} files.")
    
    return parquet_dir


def read_clean_csv(file_path: Path) -> pl.DataFrame:
    """
    Reads a TXT file and standardizes column names to uppercase. 
    Forces all columns to be read as strings to avoid type inference errors.

    Args:
        file_path: Path to .txt file

    Returns:
        pl.DataFrame: Cleaned Polars DataFrame
    """
    try:
        df = pl.read_csv(
            file_path,
            separator="\t",
            encoding="utf8-lossy",
            null_values=["", "NULL", "NA", "-99"],
            infer_schema_length=0,  # Don't infer, treat all as string
        )
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise

    # Uppercase column names
    df = df.rename({col: col.upper() for col in df.columns})
    return df


def get_all_columns(file_paths: List[Path]) -> List[str]:
    """
    Scans all files and returns the union of all column names (uppercased).

    Args:
        file_paths: List of paths to scan

    Returns:
        List of unique column names in sorted order
    """
    all_cols = set()

    for file in file_paths:
        # Use scan_csv for memory efficiency
        df = pl.scan_csv(
            file,
            separator="\t",
            encoding="utf8-lossy",
            null_values=["", "NULL", "NA", "-99"],
            infer_schema_length=0,  # Don't infer types
        )
        schema = df.collect_schema()
        all_cols.update(col.upper() for col in schema.names())

    return sorted(all_cols)


def align_df_to_schema(
    df: pl.DataFrame,
    all_columns: List[str],
) -> pl.DataFrame:
    """
    Aligns a DataFrame to match the master schema by adding missing columns.

    Args:
        df: DataFrame to align
        all_columns: Master list of all columns

    Returns:
        DataFrame with all columns, missing ones filled with null
    """
    # Get current columns (already uppercase)
    current_cols = set(df.columns)
    
    # Find missing columns
    missing_cols = [col for col in all_columns if col not in current_cols]
    
    # Add missing columns as null strings (not null dtype)
    if missing_cols:
        df = df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias(col) for col in missing_cols
        ])
    
    # Reorder to match schema
    df = df.select(all_columns)
    
    return df