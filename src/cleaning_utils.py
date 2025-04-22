import duckdb
from pathlib import Path
import polars as pl
from typing import Union

def column_summary(data_file: Union[Path, str],
                   column_name: str,
                   ) -> None:
    
    data_file = Path(data_file)
    
    print("Worked")
    
    #with duckdb.connect(data_file) as con: