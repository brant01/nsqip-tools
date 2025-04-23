from datetime import datetime
import duckdb
import logging
from pathlib import Path
import polars as pl
from typing import Union
import sys


def column_summary(db_file: Union[Path, str], column_name: str) -> None:
    db_file = Path(db_file)
    df_column = get_column_from_db(db_file, column_name)

    logging.info("\n" + "="*50 + "\n")

    # Basic info
    logging.info(f"Summary for column '{column_name}':")
    logging.info(f"Column dtype: {df_column[column_name].dtype}")
    null_count = df_column[column_name].null_count()
    logging.info(f"Null values: {null_count}")
    
    # Numeric summary
    if df_column[column_name].dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
        stats = df_column[column_name].describe()
        logging.info(stats)
    else:
        # Categorical summary
        value_counts = df_column[column_name].value_counts().sort("count", descending=True)
        logging.info("Value counts:")
        logging.info(value_counts)
    
    # Cross-tab with OPERYR using only Polars
    logging.info("\nCounts by OPERYR:")

    by_year = (
        df_column
        .filter(pl.col(column_name).is_not_null())
        .group_by(["OPERYR"])
        .count()
        .sort(["OPERYR"])
    )

    pl.Config.set_tbl_rows(100)
    logging.info(by_year)
    
    logging.info("\n" + "="*50 + "\n")
    
    
def generate_column_list(db_file: Union[Path, str]) -> list[str]:
    """
    Generate a list of columns from a DuckDB database file.
    
    Args:
        db_file (Union[Path, str]): Path to the DuckDB database file.
        
    Returns:
        list[str]: List of column names.
    """
    
    db_file = Path(db_file)
    
    # Connect to the DuckDB database
    with duckdb.connect(db_file) as con:
        # Get the first (and only) table in the database
        tables = con.execute("SHOW TABLES").fetchall()
        if not tables:
            raise ValueError("No tables found in the database.")
        table_name = tables[0][0]
        
        # Get the column names from the table
        columns = con.execute(f"DESCRIBE {table_name}").fetchall()
        
    return [col[0] for col in columns]

def get_column_from_db(db_file: Union[Path, str], column_name: str) -> pl.DataFrame:
    db_file = Path(db_file)

    # Get the table name from the database
    with duckdb.connect(str(db_file)) as con:
        tables = con.execute("SHOW TABLES").fetchall()
        if not tables:
            raise ValueError("No tables found in the database.")
        table_name = tables[0][0]

        # Run the query and convert to Polars
        query = f"SELECT {column_name}, OPERYR FROM {table_name}"
        df = con.execute(query).fetch_arrow_table()

    return pl.DataFrame(pl.from_arrow(df))



if __name__ == "__main__":
    
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler],
    )
    
    #db_file = Path(__file__).resolve().parent.parent / "data" / "NSQIP_A_Clean.duckdb"
    db_file = Path(__file__).resolve().parent.parent / "data" / "NSQIP_P_Clean.duckdb"


    logging.info(f"Using DuckDB database: {db_file}")

    if db_file.exists():
        size_mb = db_file.stat().st_size / (1024 * 1024 * 1024)
        logging.info(f"File exists. Size: {size_mb:.2f} GB")
    else:
        logging.info("File does not exist.")

    cols = generate_column_list(db_file)
    # remove OPERYR from the list of columns
    cols = [col for col in cols if col != "OPERYR"]
    
    #cols = cols[-5:]
    
    for col in cols:
        try:
            column_summary(db_file, col)
        except Exception as e:
            logging.error(f"Error processing column {col}: {e}")
            continue
    