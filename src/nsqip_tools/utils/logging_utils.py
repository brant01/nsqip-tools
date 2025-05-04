
from pathlib import Path
import logging
from datetime import datetime

def setup_logging(name: str = "log", log_dir: Path = Path("logs")) -> None:
    """
    Set up logging to file and console. Creates a timestamped log file.

    Args:
        name (str): Base name of the log file.
        log_dir (Path): Directory where log files are stored.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )

    logging.info(f"Logging initialized. Log file: {log_file}")
