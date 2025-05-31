"""Data dictionary generation for NSQIP databases.

This module provides functions to generate comprehensive data dictionaries
in multiple formats (CSV, JSON, HTML) for NSQIP databases.
"""
import json
from pathlib import Path
from typing import Union, Dict, List, Any, Optional
import duckdb
import polars as pl
from datetime import datetime

from .constants import TABLE_NAME


class DataDictionaryGenerator:
    """Generate data dictionaries for NSQIP databases.
    
    This class analyzes an NSQIP database and generates comprehensive
    data dictionaries in multiple formats.
    """
    
    def __init__(self, db_path: Union[str, Path]):
        """Initialize the generator with a database path.
        
        Args:
            db_path: Path to the DuckDB database.
            
        Raises:
            FileNotFoundError: If database doesn't exist.
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate a comprehensive summary of all columns.
        
        Returns:
            Dictionary with column summaries including:
                - name: Column name
                - data_type: SQL data type
                - python_type: Equivalent Python type
                - non_null_count: Number of non-null values
                - null_count: Number of null values
                - null_percentage: Percentage of null values
                - unique_count: Number of unique values
                - is_active: Whether column has data in most recent year
                - summary_stats: Type-specific statistics
                - null_by_year: Null counts by operation year
        """
        summaries = []
        
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            # Get column information
            columns = con.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{TABLE_NAME}'
                ORDER BY ordinal_position
            """).fetchall()
            
            # Get total row count
            total_rows = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
            
            # Get most recent year
            max_year = con.execute(f"SELECT MAX(OPERYR) FROM {TABLE_NAME}").fetchone()[0]
            
            for col_name, data_type in columns:
                summary = {
                    "name": col_name,
                    "data_type": data_type,
                    "python_type": self._sql_to_python_type(data_type),
                }
                
                # Basic statistics
                stats = con.execute(f"""
                    SELECT 
                        COUNT(*) - COUNT({col_name}) as null_count,
                        COUNT({col_name}) as non_null_count,
                        COUNT(DISTINCT {col_name}) as unique_count
                    FROM {TABLE_NAME}
                """).fetchone()
                
                summary["null_count"] = stats[0]
                summary["non_null_count"] = stats[1]
                summary["null_percentage"] = round((stats[0] / total_rows) * 100, 2)
                summary["unique_count"] = stats[2]
                
                # Check if active (has data in most recent year)
                recent_non_null = con.execute(f"""
                    SELECT COUNT({col_name})
                    FROM {TABLE_NAME}
                    WHERE OPERYR = '{max_year}'
                """).fetchone()[0]
                summary["is_active"] = recent_non_null > 0
                
                # Get null counts by year
                null_by_year = con.execute(f"""
                    SELECT 
                        OPERYR,
                        COUNT(*) as total,
                        COUNT(*) - COUNT({col_name}) as null_count
                    FROM {TABLE_NAME}
                    GROUP BY OPERYR
                    ORDER BY OPERYR
                """).df()
                
                summary["null_by_year"] = {
                    str(row['OPERYR']): {
                        'total': int(row['total']),
                        'null_count': int(row['null_count']),
                        'null_percentage': round((row['null_count'] / row['total']) * 100, 2)
                    }
                    for _, row in null_by_year.iterrows()
                }
                
                # Type-specific statistics
                if summary["non_null_count"] > 0:
                    summary["summary_stats"] = self._get_column_stats(
                        con, col_name, data_type, summary["unique_count"]
                    )
                else:
                    summary["summary_stats"] = {"all_null": True}
                
                summaries.append(summary)
        
        return {
            "database_path": str(self.db_path),
            "total_rows": total_rows,
            "total_columns": len(summaries),
            "generated_at": datetime.now().isoformat(),
            "columns": summaries
        }
    
    def _sql_to_python_type(self, sql_type: str) -> str:
        """Convert SQL type to Python type name."""
        sql_type = sql_type.upper()
        
        if "INT" in sql_type:
            return "int"
        elif sql_type in ["FLOAT", "DOUBLE", "DECIMAL", "REAL"]:
            return "float"
        elif sql_type in ["VARCHAR", "TEXT", "STRING"]:
            return "str"
        elif sql_type == "BOOLEAN":
            return "bool"
        elif "DATE" in sql_type or "TIME" in sql_type:
            return "datetime"
        elif "[]" in sql_type:
            return "list"
        else:
            return "object"
    
    def _get_column_stats(
        self,
        con: duckdb.DuckDBPyConnection,
        col_name: str,
        data_type: str,
        unique_count: int
    ) -> Dict[str, Any]:
        """Get type-specific statistics for a column."""
        stats = {}
        
        # Numeric types
        if any(t in data_type.upper() for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "REAL"]):
            numeric_stats = con.execute(f"""
                SELECT 
                    MIN({col_name}) as min_val,
                    MAX({col_name}) as max_val,
                    AVG({col_name}) as mean_val,
                    MEDIAN({col_name}) as median_val,
                    STDDEV({col_name}) as std_val
                FROM {TABLE_NAME}
            """).fetchone()
            
            stats.update({
                "min": float(numeric_stats[0]) if numeric_stats[0] is not None else None,
                "max": float(numeric_stats[1]) if numeric_stats[1] is not None else None,
                "mean": float(numeric_stats[2]) if numeric_stats[2] is not None else None,
                "median": float(numeric_stats[3]) if numeric_stats[3] is not None else None,
                "std": float(numeric_stats[4]) if numeric_stats[4] is not None else None,
            })
        
        # String/categorical types
        elif "VARCHAR" in data_type.upper() or "TEXT" in data_type.upper():
            # Get top values if reasonable number of categories
            if unique_count <= 100:
                top_values = con.execute(f"""
                    SELECT {col_name}, COUNT(*) as count
                    FROM {TABLE_NAME}
                    WHERE {col_name} IS NOT NULL
                    GROUP BY {col_name}
                    ORDER BY count DESC
                    LIMIT 15
                """).fetchall()
                
                stats["top_values"] = [
                    {"value": str(val), "count": count}
                    for val, count in top_values
                ]
            else:
                stats["unique_values"] = unique_count
                stats["note"] = "Too many unique values to list"
        
        # Boolean types
        elif "BOOLEAN" in data_type.upper():
            bool_counts = con.execute(f"""
                SELECT {col_name}, COUNT(*) as count
                FROM {TABLE_NAME}
                WHERE {col_name} IS NOT NULL
                GROUP BY {col_name}
            """).fetchall()
            
            stats["value_counts"] = {
                str(val): count for val, count in bool_counts
            }
        
        # Array types
        elif "[]" in data_type:
            stats["is_array"] = True
            # Get array length statistics
            array_stats = con.execute(f"""
                SELECT 
                    MIN(array_length({col_name})) as min_length,
                    MAX(array_length({col_name})) as max_length,
                    AVG(array_length({col_name})) as avg_length
                FROM {TABLE_NAME}
                WHERE {col_name} IS NOT NULL
            """).fetchone()
            
            stats.update({
                "min_length": int(array_stats[0]) if array_stats[0] is not None else None,
                "max_length": int(array_stats[1]) if array_stats[1] is not None else None,
                "avg_length": float(array_stats[2]) if array_stats[2] is not None else None,
            })
        
        return stats
    
    def save_to_csv(self, output_path: Union[str, Path]) -> None:
        """Save data dictionary as CSV file.
        
        Args:
            output_path: Path for output CSV file.
        """
        summary = self.generate_summary()
        
        # Flatten the data for CSV
        rows = []
        for col in summary["columns"]:
            row = {
                "Column Name": col["name"],
                "Data Type": col["data_type"],
                "Python Type": col["python_type"],
                "Active": "Yes" if col["is_active"] else "No",
                "Non-Null Count": col["non_null_count"],
                "Null Count": col["null_count"],
                "Null %": col["null_percentage"],
                "Unique Values": col["unique_count"],
            }
            
            # Add summary statistics based on type
            stats = col.get("summary_stats", {})
            if "mean" in stats:
                row.update({
                    "Min": stats.get("min"),
                    "Max": stats.get("max"),
                    "Mean": round(stats.get("mean", 0), 2) if stats.get("mean") else None,
                    "Median": round(stats.get("median", 0), 2) if stats.get("median") else None,
                    "Std Dev": round(stats.get("std", 0), 2) if stats.get("std") else None,
                })
            elif "top_values" in stats:
                top_3 = stats["top_values"][:3]
                row["Top Values"] = "; ".join([
                    f"{v['value']} ({v['count']})" for v in top_3
                ])
            
            rows.append(row)
        
        # Convert to DataFrame and save
        df = pl.DataFrame(rows)
        df.write_csv(output_path)
    
    def save_to_json(self, output_path: Union[str, Path]) -> None:
        """Save data dictionary as JSON file.
        
        Args:
            output_path: Path for output JSON file.
        """
        summary = self.generate_summary()
        
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
    
    def save_to_html(self, output_path: Union[str, Path]) -> None:
        """Save data dictionary as HTML file.
        
        Args:
            output_path: Path for output HTML file.
        """
        summary = self.generate_summary()
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>NSQIP Data Dictionary</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1, h2 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .active-yes {{ color: green; font-weight: bold; }}
        .active-no {{ color: red; }}
        .metadata {{ background-color: #e8f4f8; padding: 10px; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <h1>NSQIP Data Dictionary</h1>
    <div class="metadata">
        <p><strong>Database:</strong> {summary['database_path']}</p>
        <p><strong>Total Rows:</strong> {summary['total_rows']:,}</p>
        <p><strong>Total Columns:</strong> {summary['total_columns']}</p>
        <p><strong>Generated:</strong> {summary['generated_at']}</p>
    </div>
    
    <h2>Column Summary</h2>
    <table>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Active</th>
            <th>Non-Null</th>
            <th>Null %</th>
            <th>Unique</th>
            <th>Summary</th>
        </tr>
"""
        
        for col in summary["columns"]:
            active_class = "active-yes" if col["is_active"] else "active-no"
            
            # Format summary based on type
            stats = col.get("summary_stats", {})
            if "mean" in stats:
                summary_str = f"Range: [{stats.get('min')}, {stats.get('max')}]"
            elif "top_values" in stats:
                top_3 = stats["top_values"][:3]
                summary_str = "Top: " + ", ".join([v['value'] for v in top_3])
            else:
                summary_str = "-"
            
            html += f"""
        <tr>
            <td><strong>{col['name']}</strong></td>
            <td>{col['data_type']}</td>
            <td class="{active_class}">{'Yes' if col['is_active'] else 'No'}</td>
            <td>{col['non_null_count']:,}</td>
            <td>{col['null_percentage']}%</td>
            <td>{col['unique_count']:,}</td>
            <td>{summary_str}</td>
        </tr>
"""
        
        html += """
    </table>
</body>
</html>
"""
        
        with open(output_path, 'w') as f:
            f.write(html)