from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

def setup_logging(log_dir: Path) -> None:
    """
    Sets up logging to a file in the given directory.
    Also logs to stdout.

    Args:
        log_dir (Path): Directory to store log files.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"create_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.getLogger().addHandler(logging.StreamHandler())

def create_db(
    data_dir: Path = Path("data"),
    db_path: Optional[Path] = None,
    table_name: str = "all_data_table",
    convert_types: bool = True,
) -> None:
    """
    Create a DuckDB database from a directory of TXT files with tab-separated data.

    Args:
        data_dir (Path): Directory containing raw .txt files.
        db_path (Optional[Path]): Path for the output DuckDB file.
                                  Defaults to <data_dir>/<dataset>_data.duckdb
        table_name (str): Name of the table to create in the DB.
        convert_types (bool): Whether to run auto type conversion after insert.
    """
    setup_logging(Path("logs"))
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

    # --- Placeholder for next step ---
    logging.info("Setup complete. Proceeding to file parsing and database construction.")
