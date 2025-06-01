"""Data dictionary generation for NSQIP parquet datasets.

This module provides functions to generate comprehensive data dictionaries
in multiple formats (CSV, Excel, HTML) for NSQIP parquet datasets.
"""
import json
from pathlib import Path
from typing import Union, Dict, List, Any, Optional
import polars as pl
from datetime import datetime


class DataDictionaryGenerator:
    """Generate data dictionaries for NSQIP parquet datasets.
    
    This class analyzes an NSQIP parquet dataset and generates comprehensive
    data dictionaries in multiple formats.
    """
    
    def __init__(self, parquet_dir: Union[str, Path]):
        """Initialize the generator with a parquet dataset path.
        
        Args:
            parquet_dir: Path to the parquet dataset directory.
            
        Raises:
            FileNotFoundError: If parquet directory doesn't exist.
            ValueError: If no parquet files found.
        """
        self.parquet_dir = Path(parquet_dir)
        
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Parquet directory not found: {self.parquet_dir}")
        
        # Find parquet files
        self.parquet_files = list(self.parquet_dir.glob("*.parquet"))
        if not self.parquet_files:
            raise ValueError(f"No parquet files found in: {self.parquet_dir}")
        
        # Load metadata if available
        metadata_path = self.parquet_dir / "metadata.json"
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}
        
        # Get schema from first parquet file
        first_file = self.parquet_files[0]
        schema = pl.scan_parquet(first_file).collect_schema()
        self.columns = list(schema.names())
        self.dtypes = {name: dtype for name, dtype in schema.items()}
    
    def generate_csv(self, output_path: Union[str, Path]) -> None:
        """Generate data dictionary in CSV format.
        
        Args:
            output_path: Path for output CSV file.
        """
        output_path = Path(output_path)
        
        # Generate summary data
        summary_data = self._generate_column_summaries()
        
        # Convert to DataFrame
        df = pl.DataFrame(summary_data)
        
        # Write to CSV
        df.write_csv(output_path)
    
    def generate_excel(self, output_path: Union[str, Path]) -> None:
        """Generate data dictionary in Excel format.
        
        Args:
            output_path: Path for output Excel file.
        """
        output_path = Path(output_path)
        
        # Generate summary data
        summary_data = self._generate_column_summaries()
        
        # Convert to DataFrame
        df = pl.DataFrame(summary_data)
        
        # Write to Excel (requires xlsxwriter or openpyxl)
        try:
            df.to_pandas().to_excel(output_path, index=False)
        except ImportError:
            raise ImportError("Excel export requires pandas and xlsxwriter/openpyxl")
    
    def generate_html(self, output_path: Union[str, Path]) -> None:
        """Generate data dictionary in HTML format.
        
        Args:
            output_path: Path for output HTML file.
        """
        output_path = Path(output_path)
        
        # Generate summary data
        summary_data = self._generate_column_summaries()
        
        # Convert to DataFrame
        df = pl.DataFrame(summary_data)
        
        # Generate HTML
        html_content = self._generate_html_content(df)
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_column_summaries(self) -> List[Dict[str, Any]]:
        """Generate summary statistics for all columns.
        
        Returns:
            List of dictionaries with column information.
        """
        summaries = []
        
        # Read all parquet files as one dataset
        lazy_frames = []
        for pf in self.parquet_files:
            lazy_frames.append(pl.scan_parquet(pf))
        
        if len(lazy_frames) == 1:
            full_data = lazy_frames[0]
        else:
            full_data = pl.concat(lazy_frames, how="vertical_relaxed")
        
        # Get basic stats
        total_rows = full_data.select(pl.count()).collect().item()
        
        for col in self.columns:
            col_data = full_data.select(col)
            
            # Basic info
            summary = {
                "Column Name": col,
                "Data Type": str(self.dtypes[col]),
                "Total Rows": total_rows,
            }
            
            # Null counts
            try:
                null_count = col_data.select(pl.col(col).is_null().sum()).collect().item()
                summary["Non-Null Count"] = total_rows - null_count
                summary["Null Count"] = null_count
                summary["Null Percentage"] = f"{(null_count / total_rows * 100):.1f}%"
            except Exception:
                summary["Non-Null Count"] = "Unknown"
                summary["Null Count"] = "Unknown"
                summary["Null Percentage"] = "Unknown"
            
            # Type-specific statistics
            if self.dtypes[col] in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                try:
                    stats = col_data.select([
                        pl.col(col).min().alias("min"),
                        pl.col(col).max().alias("max"), 
                        pl.col(col).mean().alias("mean"),
                        pl.col(col).median().alias("median"),
                    ]).collect()
                    
                    summary["Min"] = stats["min"].item()
                    summary["Max"] = stats["max"].item()
                    summary["Mean"] = f"{stats['mean'].item():.2f}" if stats["mean"].item() is not None else "N/A"
                    summary["Median"] = f"{stats['median'].item():.2f}" if stats["median"].item() is not None else "N/A"
                except Exception:
                    summary["Min"] = "N/A"
                    summary["Max"] = "N/A"
                    summary["Mean"] = "N/A"
                    summary["Median"] = "N/A"
            else:
                # For string/categorical columns, get unique count
                try:
                    unique_count = col_data.select(pl.col(col).n_unique()).collect().item()
                    summary["Unique Values"] = unique_count
                    
                    # Get top 5 most common values
                    top_values = (col_data
                                 .select(col)
                                 .filter(pl.col(col).is_not_null())
                                 .group_by(col)
                                 .agg(pl.count().alias("count"))
                                 .sort("count", descending=True)
                                 .limit(5)
                                 .collect())
                    
                    if len(top_values) > 0:
                        top_list = []
                        for row in top_values.iter_rows():
                            top_list.append(f"{row[0]} ({row[1]})")
                        summary["Top Values"] = "; ".join(top_list)
                    else:
                        summary["Top Values"] = "N/A"
                        
                except Exception:
                    summary["Unique Values"] = "Unknown"
                    summary["Top Values"] = "Unknown"
            
            # Check if variable is "active" (has recent data)
            try:
                if "OPERYR" in self.columns:
                    recent_data = (full_data
                                  .filter(pl.col("OPERYR") >= 2020)  # Recent years
                                  .select(pl.col(col).drop_nulls().len())
                                  .collect().item())
                    summary["Active"] = "Yes" if recent_data > 0 else "No"
                else:
                    summary["Active"] = "Unknown"
            except Exception:
                summary["Active"] = "Unknown"
            
            summaries.append(summary)
        
        return summaries
    
    def _generate_html_content(self, df: pl.DataFrame) -> str:
        """Generate HTML content for the data dictionary.
        
        Args:
            df: DataFrame with column summaries.
            
        Returns:
            HTML string.
        """
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>NSQIP Data Dictionary</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #2c3e50; }
                table { border-collapse: collapse; width: 100%; margin-top: 20px; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; font-weight: bold; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .metadata { background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <h1>NSQIP Data Dictionary</h1>
        """
        
        # Add metadata section
        html += '<div class="metadata">'
        html += f'<h3>Dataset Information</h3>'
        html += f'<p><strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>'
        html += f'<p><strong>Source:</strong> {self.parquet_dir}</p>'
        html += f'<p><strong>Parquet Files:</strong> {len(self.parquet_files)}</p>'
        html += f'<p><strong>Total Columns:</strong> {len(self.columns)}</p>'
        
        if 'dataset_type' in self.metadata:
            html += f'<p><strong>Dataset Type:</strong> {self.metadata["dataset_type"]}</p>'
        
        html += '</div>'
        
        # Add table
        html += '<table>'
        
        # Headers
        html += '<tr>'
        for col in df.columns:
            html += f'<th>{col}</th>'
        html += '</tr>'
        
        # Data rows
        for row in df.iter_rows():
            html += '<tr>'
            for cell in row:
                html += f'<td>{cell}</td>'
            html += '</tr>'
        
        html += """
            </table>
        </body>
        </html>
        """
        
        return html