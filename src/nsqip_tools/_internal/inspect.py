from datetime import datetime
from pathlib import Path
from typing import Union, Optional
import sys
import logging

import duckdb
import polars as pl

def _setup_inspection_logging(log_dir: Path = Path("logs")) -> Path:
    """
    Set up logging for the data inspection process.

    Args:
        log_dir (Path): Directory where log files will be saved.

    Returns:
        Path: Path to the log file created.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"inspect_columns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler],
    )
    return log_file

def _get_table_name(db_file: Path) -> str:
    """
    Retrieve the first table name from the DuckDB database.

    Args:
        db_file (Path): Path to the DuckDB file.

    Returns:
        str: Table name.
    """
    with duckdb.connect(str(db_file)) as con:
        tables = con.execute("SHOW TABLES").fetchall()
        if not tables:
            raise ValueError("No tables found in the database.")
        return tables[0][0]

def _get_column_from_db(db_file: Path, column_name: str, table_name: str) -> pl.DataFrame:
    """
    Retrieve a specific column and OPERYR from the DuckDB table as a Polars DataFrame.

    Args:
        db_file (Path): Path to the DuckDB file.
        column_name (str): Column to extract.
        table_name (str): Table name in the database.

    Returns:
        pl.DataFrame: Resulting Polars DataFrame.
    """
    with duckdb.connect(str(db_file)) as con:
        query = f"SELECT \"{column_name}\", OPERYR FROM \"{table_name}\""
        arrow_table = con.execute(query).fetch_arrow_table()
    return pl.from_arrow(arrow_table)

def _generate_column_list(db_file: Path, table_name: Optional[str] = None) -> list[str]:
    """
    Generate a list of all columns except OPERYR in the DuckDB table.

    Args:
        db_file (Path): Path to the DuckDB file.
        table_name (Optional[str]): Table name if known.

    Returns:
        list[str]: List of column names.
    """
    with duckdb.connect(str(db_file)) as con:
        if not table_name:
            table_name = _get_table_name(db_file)
        columns = con.execute(f"DESCRIBE \"{table_name}\"").fetchall()
    return [col[0] for col in columns if col[0] != "OPERYR"]

def _column_summary(db_file: Path, column_name: str, table_name: str) -> None:
    """
    Log summary statistics for a given column.

    Args:
        db_file (Path): Path to the DuckDB file.
        column_name (str): Column to summarize.
        table_name (str): Table name in the database.
    """
    df_column = _get_column_from_db(db_file, column_name, table_name)
    logging.info("\n" + "=" * 60)
    logging.info(f"Summary for column '{column_name}':")

    dtype = df_column[column_name].dtype
    logging.info(f"Dtype: {dtype}")

    null_count = df_column[column_name].null_count()
    logging.info(f"Nulls: {null_count}")

    # Check for numeric types explicitly
    numeric_types = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64}
    if dtype in numeric_types:
        stats = df_column[column_name].describe()
        logging.info("\n" + stats.__str__())
    else:
        vc = df_column[column_name].value_counts().sort("count", descending=True)
        logging.info("Top values:")
        logging.info(vc.head(10))

    # Cross-tab with OPERYR
    logging.info("Non-null counts by OPERYR:")
    by_year = (
        df_column.filter(pl.col(column_name).is_not_null())
        .group_by("OPERYR")
        .count()
        .sort("OPERYR")
    )
    logging.info(by_year.__str__())
    logging.info("=" * 60 + "\n")

def summarize_all_columns(
    db_file: Union[str, Path], 
    table_name: Optional[str] = None,
    log_dir: Path = Path("logs")
) -> None:
    """
    Run a summary over all columns in the DuckDB table.

    Args:
        db_file (Union[str, Path]): Path to the DuckDB database.
        table_name (Optional[str]): Optional table name. Defaults to first table found.
        log_dir (Path): Directory where logs should be saved.
    """
    db_file = Path(db_file)
    log_file = _setup_inspection_logging(log_dir)

    if not db_file.exists():
        logging.error(f"Database file not found: {db_file}")
        return

    table_name = table_name or _get_table_name(db_file)
    columns = _generate_column_list(db_file, table_name)

    logging.info(f"Inspecting table '{table_name}' with {len(columns)} columns in: {db_file}")

    for col in columns:
        try:
            _column_summary(db_file, col, table_name)
        except Exception as e:
            logging.error(f"Error processing column {col}: {e}")
            continue

    logging.info(f"Finished. Log saved to: {log_file}")
