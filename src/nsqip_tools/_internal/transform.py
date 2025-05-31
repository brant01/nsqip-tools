import duckdb
from pathlib import Path
from typing import Union

VALID_TYPES = {
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "REAL",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "INTERVAL",
    "VARCHAR",  # duckdb uses VARCHAR as the formal type for strings
    "STRING",   # alias for VARCHAR
    "BLOB",
    "CATEGORICAL",
    "UUID"
}

def cast_column_in_place(
    db_path: Union[Path, str],
    table_name: str,
    columns_to_cast: list[str],
    new_type: str = "STRING",
) -> None:
    
    if new_type not in VALID_TYPES:
        raise ValueError(f"Invalid type '{new_type}'.")
    
    db_path = Path(db_path)
        
    with duckdb.connect(str(db_path)) as con:
            for col in columns_to_cast:
                try:
                    tmp_col = f"__tmp_{col}"
                    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {tmp_col} {new_type};")
                    con.execute(f"UPDATE {table_name} SET {tmp_col} = CAST({col} AS {new_type});")
                    con.execute(f"ALTER TABLE {table_name} DROP COLUMN {col};")
                    con.execute(f"ALTER TABLE {table_name} RENAME COLUMN {tmp_col} TO {col};")
                    print(f"Column '{col}' casted to {new_type} in table '{table_name}'.")
                except Exception as e:
                    print(f"Error casting column '{col}' in table '{table_name}': {e}")
                    continue
                
    print(f"All columns casted to {new_type} in table '{table_name}'.")
    
def fix_age_column(
    db_path: Union[Path, str],
    table_name: str,
) -> None:
    
    db_path = Path(db_path)
    
    with duckdb.connect(str(db_path)) as con:
        
        con.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS AGE_AS_INT INTEGER;
                    """
        )
        
        # Populate AGE_AS_INT by removing '+' and casting to int
        con.execute(f"""
                    UPDATE {table_name}
                    SET AGE_AS_INT = CAST(REPLACE(AGE, '+', '') AS INTEGER)
                    WHERE AGE IS NOT NULL;
                    """
        )
        
        # Add AGE_IS_90_PLUS if it doesn't exist
        con.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS AGE_IS_90_PLUS BOOLEAN;
                    """
        )
        # Populate AGE_IS_90_PLUS if AGE ends with '+'
        con.execute(f"""
                    UPDATE {table_name}
                    SET AGE_IS_90_PLUS = (AGE LIKE '%+')
                    WHERE AGE IS NOT NULL;
                    """
        )

def add_combined_cpt_column(
    db_path: Union[Path, str],
    table_name: str,
    cpt_columns: list[str] ,
) -> None:
    
    db_path = Path(db_path)
    
    with duckdb.connect(str(db_path)) as con:
        
        # build SQL to combine CPT columns into an array
        array_expr = f"[{', '.join(cpt_columns)}]"
        
        # add the new column to the table if it doesn't exist
        con.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS ALL_CPT_CODES TEXT[];
                    """
        )
        
        # Update the new column with array of CPTs
        con.execute(f"""
                    UPDATE {table_name}
                    SET ALL_CPT_CODES = {array_expr}
                    WHERE { ' OR '.join(f"{col} IS NOT NULL" for col in cpt_columns) };
                    """
        )
        
def add_total_rvu(
    db_path: Union[Path, str],
    table_name: str,
    rvu_columns: list[str] ,
) -> None:
    
    db_path = Path(db_path)
    
    with duckdb.connect(str(db_path)) as con:
        
        # Add TOTAL_RVU column if it doesn't exist
        con.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN IF NOT EXISTS TOTAL_RVU DOUBLE;
        """)

        # Ensure each RVU column is cast to DOUBLE before summing
        sum_expr = " + ".join([f"COALESCE(CAST({col} AS DOUBLE), 0)" for col in rvu_columns])

        # Update the TOTAL_RVU column
        con.execute(f"""
            UPDATE {table_name}
            SET TOTAL_RVU = {sum_expr}
            WHERE { ' OR '.join(f"{col} IS NOT NULL" for col in rvu_columns) };
        """)
        
def cast_string_column_to_numeric(
    db_path: Union[Path, str],
    table_name: str,
    column_name: str,
    new_type: str = "DOUBLE"
) -> None:
    db_path = Path(db_path)

    with duckdb.connect(db_path) as con:
        # Clean up blank or invalid values by setting them to NULL
        con.execute(f"""
            UPDATE {table_name}
            SET {column_name} = NULL
            WHERE TRIM({column_name}) = '' OR {column_name} IS NULL;
        """)

        # Now cast safely
        con.execute(f"""
            ALTER TABLE {table_name}
            ALTER COLUMN {column_name} SET DATA TYPE {new_type};
        """)

    print(f"Successfully casted column '{column_name}' to {new_type}")
        
def combine_race_cols(
    db_path: Union[Path, str],
    table_name: str,
) -> None:
    
    db_path = Path(db_path)
    
    with duckdb.connect(str(db_path)) as con:
        
        # Create a temporary column to store combined values
        con.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN RACE_COMBINED STRING;
        """)
        
        # Populate it using COALESCE
        con.execute(f"""
            UPDATE {table_name}
            SET RACE_COMBINED = COALESCE(RACE_NEW, RACE);
        """)
        
        # Drop the old RACE and rename the combined column
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN RACE;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN RACE_NEW;")
        con.execute(f"ALTER TABLE {table_name} RENAME COLUMN RACE_COMBINED TO RACE;")
        
def combine_anes_cols(
    db_path: Union[Path, str],
    table_name: str,
) -> None:
    
    db_path = Path(db_path)
    
    with duckdb.connect(str(db_path)) as con:
        
        # Create a temporary column to store combined values
        con.execute(f"""
            ALTER TABLE {table_name}
            ADD COLUMN ANESTH_COMBINED STRING;
        """)
        
        # Populate it using COALESCE
        con.execute(f"""
            UPDATE {table_name}
            SET ANESTH_COMBINED = COALESCE(ANESTECH, ANESTHES);
        """)
        
        # Drop the old ANESTHES and rename the combined column
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN ANESTHES;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN ANESTECH;")
        con.execute(f"ALTER TABLE {table_name} RENAME COLUMN ANESTH_COMBINED TO ANESTECH;")
        

        
def split_comma_separated_columns(
    db_path: Union[Path, str],
    table_name: str,
    column_names: list[str],
) -> None:
    db_path = str(db_path)

    with duckdb.connect(db_path) as con:
        for column_name in column_names:
            new_column = f"{column_name}_list"

            # Drop column if it already exists
            try:
                con.execute(f"ALTER TABLE {table_name} DROP COLUMN {new_column}")
                print(f"Dropped existing column '{new_column}'.")
            except duckdb.CatalogException:
                pass  # Column didn't exist, no problem

            # Create and populate the new list column
            con.execute(f"""
                ALTER TABLE {table_name}
                ADD COLUMN {new_column} TEXT[];
            """)
            con.execute(f"""
                UPDATE {table_name}
                SET {new_column} = list_transform(str_split({column_name}, ','), x -> trim(x));
            """)
            print(f"Created list column '{new_column}' from '{column_name}'.")


def add_free_flap_flags(
    db_path: Union[str, Path],
    table_name: str,
    cpt_column: str = "ALL_CPT_CODES",
) -> None:
    db_path = str(db_path)

    # Define CPT code groups
    free_flap_cpt = [
        '15756', '15757', '15758', '20969', '43496', '20955',
        '20962', '15842', '20956', '20957', '20970'
    ]
    soft_tissue_flap = ['15756', '15757', '15758', '43496', '15842']
    bone_flap = ['20970', '20957', '20956', '20955', '20962', '20969']

    column_defs = {
        "HAS_FREE_FLAP": free_flap_cpt,
        "HAS_SOFT_FLAP": soft_tissue_flap,
        "HAS_BONE_FLAP": bone_flap,
    }

    with duckdb.connect(db_path) as con:
        for column_name, cpt_list in column_defs.items():
            # Drop if already exists
            try:
                con.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
            except duckdb.Error:  # catch all DuckDB-related exceptions
                pass  # Column didn't exist, no problem

            # Create new boolean column
            con.execute(f"""
                ALTER TABLE {table_name}
                ADD COLUMN {column_name} BOOLEAN;
            """)

            # Set value based on whether any CPT codes match
            con.execute(f"""
                UPDATE {table_name}
                SET {column_name} = list_has_any({cpt_column}, {cpt_list});
            """)

            print(f"Created column '{column_name}' for CPT group.")