from pathlib import Path
import duckdb
import polars as pl
from typing import Union, Optional

def get_data(
    db_path: Union[Path, str],
    cpt_codes: Optional[list[str]] = None,
    columns: Optional[list[str]] = None,
) -> pl.LazyFrame:

    db_path = Path(db_path)
    print(f"Using DuckDB database: {db_path}")

    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found at {db_path}")
    
    with duckdb.connect(str(db_path), read_only=True) as con:
        tables = con.execute("SHOW TABLES").fetchall()
        table_name = tables[0][0] if tables else None
        print(f"Table name: {table_name}")

        # Validate columns
        table_columns = [row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()]
        if columns is not None:
            invalid_columns = [col for col in columns if col not in table_columns]
            if invalid_columns:
                print(f"Warning: The following columns are not in the table and will be ignored: {invalid_columns}")
            columns = [col for col in columns if col in table_columns]

        # SELECT clause
        select_clause = "*" if not columns else ", ".join(columns)

        # WHERE clause for CPT codes
        where_clauses = []
        if cpt_codes:
            cpt_list_str = "LIST_VALUE(" + ", ".join(f"'{code}'" for code in cpt_codes) + ")"
            where_clauses.append(f"array_has_any(ALL_CPT_CODES, {cpt_list_str})")

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        query = f"""
            SELECT {select_clause}
            FROM {table_name}
            {where_sql}
        """
        print("Executing query:\n", query)

        arrow_table = con.execute(query).fetch_arrow_table()
        return pl.DataFrame(pl.from_arrow(arrow_table)).lazy()


