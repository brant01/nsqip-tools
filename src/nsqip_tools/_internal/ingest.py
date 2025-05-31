import duckdb
from pathlib import Path
from typing import Optional
import logging
import polars as pl

def create_duckdb_from_text(
    text_file_dir: Path,
    db_name: str,
    table_name: str = "nsqip",
    dataset_type: str = "adult",
) -> None:
    """
    Create a DuckDB database from a directory of TXT files with tab-separated data.
    All columns are stored as TEXT for maximum compatibility.
    """
    logging.info("Starting database creation process.")

    if not text_file_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {text_file_dir}")

    txt_files = sorted(text_file_dir.glob("*.txt"))
    if not txt_files:
        logging.warning(f"No .txt files found in {text_file_dir}")
        return

    # Create database path
    db_path = text_file_dir / db_name

    logging.info(f"Dataset type: {dataset_type}")
    logging.info(f"Found {len(txt_files)} files to process in {text_file_dir}")
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
                # Register the arrow table as a DuckDB view
                con.register("temp_df", df.to_arrow())

                # Insert into final table from the temp view
                con.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_df')

                # Optionally drop the view to avoid reuse issues
                con.unregister("temp_df")

                logging.info(f"Inserted {df.shape[0]} rows from {file_path.name}")
                
            except Exception as e:
                logging.error(f"Error processing file {file_path.name}: {e}")
                continue

        # --- Summary logging ---
        try:
            total_cols = len(all_columns)
            total_rows = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

            logging.info(f"Final table: {table_name}")
            logging.info(f"Total columns: {total_cols}")
            logging.info(f"Total rows: {total_rows}")

            # Log row count by OPERYR (if it exists)
            if "OPERYR" in all_columns:
                result = con.execute(f"""
                    SELECT OPERYR, COUNT(*) AS n 
                    FROM "{table_name}" 
                    GROUP BY OPERYR 
                    ORDER BY OPERYR
                """).fetchall()
                logging.info("Row counts by OPERYR:")
                for year, count in result:
                    logging.info(f"   {year}: {count}")
            else:
                logging.info("Column 'OPERYR' not found in table. Skipping yearly breakdown.")
        except Exception as e:
            logging.warning(f"Error during final summary logging: {e}")


def read_clean_csv(file_path: Path) -> pl.DataFrame:
    """
    Reads a TXT file and standardizes column names to uppercase. 
    Forces all columns to be read as strings to avoid type inference errors.

    Args:
        file_path (Path): Path to .txt file

    Returns:
        pl.DataFrame: Cleaned Polars DataFrame
    """
    try:
        df = pl.read_csv(
            file_path,
            separator="\t",
            encoding="utf8-lossy",
            null_values=["", "NULL", "NA", "-99"],
            infer_schema_length=None,  # full inference if needed
            dtypes={"*": pl.Utf8},      # <-- force all columns to string
        )
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise

    df.columns = [col.upper() for col in df.columns]
    return df


def get_all_columns(file_paths: list[Path]) -> list[str]:
    """
    Scans all files and returns the union of all column names (uppercased).

    Args:
        file_paths (list[Path]): List of Paths to .txt files

    Returns:
        list[str]: Sorted union of all column names
    """
    all_cols: set[str] = set()

    for file in file_paths:
        df = pl.scan_csv(
            file,
            separator="\t",
            encoding="utf8-lossy",
            null_values=["", "NULL", "NA", "-99"],
            dtypes={"*": pl.Utf8},  # force all string
        )
        schema = df.collect_schema()
        all_cols.update(col.upper() for col in schema.keys())

    return sorted(all_cols)


def align_df_to_schema(
    df: pl.DataFrame,
    all_columns: list[str],
) -> pl.DataFrame:
    """
    Inserts nulls for missing columns and enforces column order.

    Args:
        df (pl.DataFrame): DataFrame to align
        all_columns (list[str]): master column set

    Returns:
        pl.DataFrame: Updated DataFrame with aligned columns
    """
    existing = set(df.columns)
    for col in all_columns:
        if col not in existing:
            df = df.with_columns(pl.lit(None).alias(col))
            
    return df.select(all_columns)
