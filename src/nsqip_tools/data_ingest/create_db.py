import duckdb
from pathlib import Path
from typing import Optional
import logging
import polars as pl

from nsqip_tools.utils.logging_utils import setup_logging


def create_db(
    data_dir: Path = Path("data"),
    db_path: Optional[Path] = None,
    table_name: str = "all_data_table",
) -> None:
    """
    Create a DuckDB database from a directory of TXT files with tab-separated data.
    All columns are stored as TEXT for maximum compatibility and to avoid lossy conversion.


    Args:
        data_dir (Path): Directory containing raw .txt files.
        db_path (Optional[Path]): Path for the output DuckDB file.
                                  Defaults to <data_dir>/<dataset>_data.duckdb
        table_name (str): Name of the table to create in the DB.
    """
    
    setup_logging("create_db")
    
    logging.info("Starting database creation process.")

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    txt_files = sorted(data_dir.glob("*.txt"))
    if not txt_files:
        logging.warning(f"No .txt files found in {data_dir}")
        return

    # --- Detect dataset type from file names ---
    prefixes = {
        f.name.lower().split("_")[1] for f in txt_files if "_" in f.name.lower()
    }

    if len(prefixes) != 1:
        raise ValueError(f"Inconsistent dataset prefixes found: {prefixes}")

    prefix = prefixes.pop()
    if prefix not in {"nsqip", "peds"}:
        raise ValueError(f"Unknown dataset prefix: {prefix}")

    dataset_type = prefix  # Will be 'nsqip' or 'peds'

    if db_path is None:
        db_path = data_dir / f"{dataset_type}_data.duckdb"

    logging.info(f"Detected dataset type: {dataset_type}")
    logging.info(f"Found {len(txt_files)} files to process in {data_dir}")
    logging.info(f"Output database path: {db_path}")

    logging.info("Setup complete. Proceeding to file parsing and database construction.")
    
    # --- Collect master schema from all files --- 
    all_columns = get_all_columns(txt_files)
    logging.info(f"Unified column set has {len(all_columns)} columns.")
    
    # --- Connect and create table ---
    with duckdb.connect(db_path) as con:
        logging.info(f"Connected to DuckDB at {db_path}")
        
        # --- Create table with all columns as TEXT ---
        col_defs = ",\n".join(f'"{col}" TEXT' for col in all_columns)
        con.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        logging.info(f"Dropping existing table {table_name} if it exists.")
        create_stmt = f'CREATE TABLE "{table_name}" (\n{col_defs}\n);'
        con.execute(create_stmt)
        logging.info(f"Created table {table_name} with {len(all_columns)} columns.")
        
        for i, file_path in enumerate(txt_files):
            logging.info(f"[{i+1}/{len(txt_files)}] Processing {file_path.name}")
            
            try:
                df = read_clean_csv(file_path)
                df = align_df_to_schema(df, all_columns)
                
                # insert into DuckDB
                con.execute(f"INSERT INTO {table_name} SELECT * FROM df", {"df": df})
                logging.info(f"Inserted {df.shape[0]} rows from {file_path.name}")
                
            except Exception as e:
                logging.error(f"Error processing file {file_path.name}: {e}")
                continue
    logging.info(f"Finished building DuckDB database at {db_path}")

def read_clean_csv(file_path: Path) -> pl.DataFrame:
    """
    Reads a RSV file and standardizes column names to uppercase. 
    Handles encoding issues with 'utf8-lossy'

    Args:
        file_path (Path): _description_

    Returns:
        pl.DataFrame: _description_
    """
    
    try:
        df = (
            pl.read_csv(
                file_path,
                separator="\t",
                encoding="utf8-lossy",
                null_values=["", "NULL", "NA", "-99"],
                infer_schema_length= 10_000,
            )
        )
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise
    
    df.columns = [col.upper() for col in df.columns]
    return df

def get_all_columns(file_paths: list[Path]) -> set[str]:
    """
    Scans all files and returns the union of all column names (uppercased)

    Args:
        file_paths (list[Path]): list of Paths to files

    Returns:
        set[str]: Union of all column names from all files
    """
    all_cols: set[str] = set()
    
    for file in file_paths:
        df = pl.scan_csv(
            file,
            separator="\t",
            encoding="utf8-lossy",
            null_values=["", "NULL", "NA", "-99"],
            infer_schema_length= 10_000,
        )
        schema = df.collect_schema()
        all_cols.update(col.upper() for col in schema.keys())
        
    return sorted(all_cols)

def align_df_to_schema(
    df: pl.DataFrame,
    all_columns: set[str],
) -> pl.DataFrame:
    """
    Inserts nulls for missing columns and enforces column order.

    Args:
        df (pl.DataFrame): DataFrame to align
        all_cols (set[str]): all columns from all filse

    Returns:
        pl.DataFrame: Updated DataFrame with aligned columns
    """
    
    existing = set(df.columns)
    for col in all_columns:
        if col not in existing:
            df = df.with_columns(pl.lit(None).alias(col))
            
    return df.select(sorted(all_columns))