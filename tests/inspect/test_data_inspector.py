import duckdb
import polars as pl
import pytest 
from pathlib import Path

from nsqip_tools.inspect.data_inspector import summarize_all_columns

@pytest.fixture
def dummy_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    
    data = pl.DataFrame({
        "OPERYR": [2020, 2021, 2020, 2022],
        "AGE": [25, 30, 35, None],
        "SEX": ["M", "F", "F", "M"],
        "SCORE": ["1", "2", "1", "2"],  # stays as string
    })
    
    con.register("df", data.to_arrow())
    con.execute("CREATE TABLE test_table AS SELECT * FROM df")
    con.close()
    return db_path

def test_summarize_all_columns(
        tmp_path: Path,
        dummy_db: Path) -> None:
    
    logs_dir = tmp_path / "logs"
    summarize_all_columns(
        db_file=dummy_db,
        log_dir=logs_dir,
        table_name="test_table",
    )
    
    # Check if the log file was created
    log_files = list(logs_dir.glob("inspect_columns_*.log"))
    assert len(log_files) == 1
    
    # Confirm expected terms appear in the log
    contents = log_files[0].read_text(encoding="utf-8")
    assert "Column: OPERYR" in contents
    assert "Column: AGE" in contents
    assert "Summary for column 'AGE'" in contents
    assert "Non-null counts by OPERYR" in contents
    assert "Summary for column 'SCORE'" in contents