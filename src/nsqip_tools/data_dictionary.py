"""Data dictionary generation for NSQIP parquet datasets.

This module provides functions to generate comprehensive data dictionaries
in multiple formats (CSV, JSON, HTML) for NSQIP parquet datasets.
"""
import json
from pathlib import Path
from typing import Union, Dict, List, Any, Optional
import polars as pl
from datetime import datetime
from tqdm import tqdm


def generate_data_dictionary(
    parquet_dir: Union[str, Path],
    output_format: str = "all",
    output_dir: Optional[Union[str, Path]] = None,
    batch_size: int = 50
) -> None:
    """Generate data dictionary for NSQIP parquet dataset.
    
    Args:
        parquet_dir: Path to directory containing parquet files
        output_format: Format to generate ('csv', 'json', 'html', or 'all')
        output_dir: Directory to save output files (defaults to parquet_dir)
        batch_size: Number of columns to process at once (for memory management)
        
    Examples:
        >>> # Generate all formats
        >>> generate_data_dictionary("path/to/parquet/dataset")
        
        >>> # Generate only CSV
        >>> generate_data_dictionary("path/to/parquet/dataset", output_format="csv")
    """
    generator = DataDictionaryGenerator(parquet_dir, batch_size)
    
    if output_dir is None:
        output_dir = Path(parquet_dir)
    else:
        output_dir = Path(output_dir)
    
    if output_format == "all":
        generator.generate_all_formats(output_dir)
    elif output_format == "csv":
        generator.generate_csv(output_dir / "adult_data_dictionary.csv")
    elif output_format == "json":
        generator.generate_json(output_dir / "adult_data_dictionary.json")
    elif output_format == "html":
        generator.generate_html(output_dir / "adult_data_dictionary.html")
    else:
        raise ValueError(f"Unknown format: {output_format}. Use 'csv', 'json', 'html', or 'all'")


class DataDictionaryGenerator:
    """Generate data dictionaries for NSQIP parquet datasets.
    
    This class analyzes an NSQIP parquet dataset and generates comprehensive
    data dictionaries in multiple formats with enhanced statistics and year analysis.
    """
    
    def __init__(self, parquet_dir: Union[str, Path], batch_size: int = 50):
        """Initialize the generator with a parquet dataset path."""
        self.parquet_dir = Path(parquet_dir)
        self.batch_size = batch_size
        
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
        
        # Create lazy frame for full dataset
        lazy_frames = []
        for pf in self.parquet_files:
            lazy_frames.append(pl.scan_parquet(pf))
        
        if len(lazy_frames) == 1:
            self.full_data = lazy_frames[0]
        else:
            self.full_data = pl.concat(lazy_frames, how="vertical_relaxed")
            
        # Get total rows and years
        stats = self.full_data.select([
            pl.len().alias("total_rows"),
            pl.col("OPERYR").unique().sort()
        ]).collect()
        
        self.total_rows = stats["total_rows"][0]
        self.years = sorted([str(y) for y in stats["OPERYR"].to_list() if y is not None])
        self.most_recent_year = max(self.years) if self.years else None
        
        # Cache for reusing calculated data
        self._cached_summaries = None
        self._cached_year_analysis = None
    
    def generate_all_formats(self, output_dir: Path) -> None:
        """Generate all formats efficiently by reusing calculated data."""
        print("\nCalculating column summaries...")
        summaries = self._generate_column_summaries_batch()
        self._cached_summaries = summaries
        
        print("\nAnalyzing year availability...")
        year_analysis = self._generate_year_null_analysis()
        self._cached_year_analysis = year_analysis
        
        # Generate all formats from the same data
        print("\nGenerating CSV format...")
        self._write_csv_from_cache(output_dir / "adult_data_dictionary.csv")
        
        print("Generating JSON format...")
        self._write_json_from_cache(output_dir / "adult_data_dictionary.json")
        
        print("Generating HTML format...")
        self._write_html_from_cache(output_dir / "adult_data_dictionary.html")
    
    def generate_csv(self, output_path: Union[str, Path]) -> None:
        """Generate data dictionary in CSV format."""
        output_path = Path(output_path)
        
        if self._cached_summaries is None:
            summary_data = self._generate_column_summaries_batch()
        else:
            summary_data = self._cached_summaries
        
        # Remove Total Rows since it's redundant in CSV
        csv_data = []
        for summary in summary_data:
            csv_summary = summary.copy()
            csv_summary.pop("Total Rows", None)
            csv_data.append(csv_summary)
            
        df = pl.DataFrame(csv_data)
        df.write_csv(output_path)
        print(f"CSV saved to: {output_path}")
    
    def generate_json(self, output_path: Union[str, Path]) -> None:
        """Generate data dictionary in JSON format."""
        output_path = Path(output_path)
        
        # Use cached data if available
        if self._cached_summaries is None:
            summary_data = self._generate_column_summaries_batch()
        else:
            summary_data = self._cached_summaries
            
        if self._cached_year_analysis is None:
            year_null_data = self._generate_year_null_analysis()
        else:
            year_null_data = self._cached_year_analysis
        
        # Add metadata
        output_data = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": str(self.parquet_dir),
                "parquet_files": len(self.parquet_files),
                "total_columns": len(self.columns),
                "total_rows": self.total_rows,
                "years": self.years,
                "most_recent_year": self.most_recent_year,
                **self.metadata
            },
            "columns": summary_data,
            "all_null_years": year_null_data
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        print(f"JSON saved to: {output_path}")
    
    def generate_html(self, output_path: Union[str, Path]) -> None:
        """Generate enhanced HTML data dictionary."""
        output_path = Path(output_path)
        
        # Use cached data if available
        if self._cached_summaries is None:
            summary_data = self._generate_column_summaries_batch()
        else:
            summary_data = self._cached_summaries
            
        if self._cached_year_analysis is None:
            year_null_data = self._generate_year_null_analysis()
        else:
            year_null_data = self._cached_year_analysis
        
        # Remove Total Rows from summary for display
        display_data = []
        for summary in summary_data:
            display_summary = summary.copy()
            display_summary.pop("Total Rows", None)
            display_data.append(display_summary)
        
        html_content = self._generate_enhanced_html_content(
            pl.DataFrame(display_data), 
            year_null_data
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML saved to: {output_path}")
    
    def _write_csv_from_cache(self, output_path: Path) -> None:
        """Write CSV from cached data."""
        if self._cached_summaries is None:
            raise ValueError("No cached summaries available")
            
        csv_data = []
        for summary in self._cached_summaries:
            csv_summary = summary.copy()
            csv_summary.pop("Total Rows", None)
            csv_data.append(csv_summary)
        
        df = pl.DataFrame(csv_data)
        df.write_csv(output_path)
    
    def _write_json_from_cache(self, output_path: Path) -> None:
        """Write JSON from cached data."""
        if self._cached_summaries is None or self._cached_year_analysis is None:
            raise ValueError("No cached data available")
            
        output_data = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": str(self.parquet_dir),
                "parquet_files": len(self.parquet_files),
                "total_columns": len(self.columns),
                "total_rows": self.total_rows,
                "years": self.years,
                "most_recent_year": self.most_recent_year,
                **self.metadata
            },
            "columns": self._cached_summaries,
            "all_null_years": self._cached_year_analysis
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
    
    def _write_html_from_cache(self, output_path: Path) -> None:
        """Write HTML from cached data."""
        if self._cached_summaries is None or self._cached_year_analysis is None:
            raise ValueError("No cached data available")
            
        display_data = []
        for summary in self._cached_summaries:
            display_summary = summary.copy()
            display_summary.pop("Total Rows", None)
            display_data.append(display_summary)
        
        df = pl.DataFrame(display_data)
        html_content = self._generate_enhanced_html_content(df, self._cached_year_analysis)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_column_summaries_batch(self) -> List[Dict[str, Any]]:
        """Generate summaries with progress bar and better memory management."""
        print(f"Processing {len(self.columns)} columns in batches of {self.batch_size}...")
        
        summaries = []
        
        # Get active status for most recent year first
        active_status = self._get_active_status()
        
        # Get year analysis
        year_analysis = self._generate_year_null_analysis()
        
        # Process columns in batches with progress bar
        with tqdm(total=len(self.columns), desc="Processing columns") as pbar:
            for i in range(0, len(self.columns), self.batch_size):
                batch_cols = self.columns[i:i+self.batch_size]
                batch_summaries = self._process_column_batch(batch_cols, active_status, year_analysis)
                summaries.extend(batch_summaries)
                pbar.update(len(batch_cols))
        
        return summaries
    
    def _get_active_status(self) -> Dict[str, str]:
        """Get active status for all columns in the most recent year."""
        active_status = {}
        
        if self.most_recent_year and "OPERYR" in self.columns:
            print(f"Calculating active status for {self.most_recent_year}...")
            
            # Process in batches to avoid memory issues
            for i in range(0, len(self.columns), self.batch_size * 2):
                batch_cols = self.columns[i:i+self.batch_size * 2]
                active_exprs = []
                
                for col in batch_cols:
                    if col != "OPERYR":
                        active_exprs.append(
                            pl.col(col).is_not_null().sum().alias(f"{col}__active")
                        )
                
                if active_exprs:
                    active_stats = (self.full_data
                                   .filter(pl.col("OPERYR").cast(pl.Utf8) == self.most_recent_year)
                                   .select(active_exprs)
                                   .collect())
                    
                    for col in batch_cols:
                        if col == "OPERYR":
                            active_status[col] = "N/A"
                        else:
                            active_count = active_stats[f"{col}__active"][0] if f"{col}__active" in active_stats.columns else 0
                            active_status[col] = "Yes" if active_count > 0 else "No"
        else:
            active_status = {col: "N/A" for col in self.columns}
        
        return active_status
    
    def _process_column_batch(self, batch_cols: List[str], active_status: Dict[str, str], 
                            year_analysis: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        """Process a batch of columns efficiently."""
        # First, get basic count statistics for all columns
        basic_exprs = []
        for col in batch_cols:
            basic_exprs.extend([
                pl.col(col).is_null().sum().alias(f"{col}__null_count"),
                pl.col(col).is_not_null().sum().alias(f"{col}__non_null_count")
            ])
        
        # Execute basic stats query
        try:
            basic_stats = self.full_data.select(basic_exprs).collect()
        except Exception as e:
            print(f"\nError in batch, processing individually: {e}")
            return self._process_columns_individually(batch_cols, active_status, year_analysis)
        
        # Now process each column individually for type-specific stats
        summaries = []
        for col in batch_cols:
            try:
                null_count = basic_stats[f"{col}__null_count"][0]
                non_null_count = basic_stats[f"{col}__non_null_count"][0]
                null_pct = (null_count / self.total_rows * 100)
                
                summary = {
                    "Column Name": col,
                    "Data Type": str(self.dtypes[col]),
                    "Non-Null Count": non_null_count,
                    "Null Count": null_count,
                    "Null Percentage": f"{null_pct:.2f}%" if null_pct > 99.9 else f"{null_pct:.1f}%",
                    "Total Rows": self.total_rows  # Keep for JSON
                }
                
                # Try to get type-specific stats separately to avoid errors
                col_dtype = self.dtypes[col]
                if col_dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64] and non_null_count > 0:
                    try:
                        # For very large datasets, just sample if necessary
                        numeric_stats = self.full_data.select([
                            pl.col(col).min().alias("min_val"),
                            pl.col(col).max().alias("max_val"),
                            pl.col(col).mean().alias("mean_val"),
                            pl.col(col).std().alias("std_val"),
                            pl.col(col).median().alias("median_val"),
                            pl.col(col).quantile(0.25).alias("q1_val"),
                            pl.col(col).quantile(0.75).alias("q3_val")
                        ]).collect()
                        
                        summary["Min"] = numeric_stats["min_val"][0]
                        summary["Max"] = numeric_stats["max_val"][0]
                        mean_val = numeric_stats["mean_val"][0]
                        std_val = numeric_stats["std_val"][0]
                        median_val = numeric_stats["median_val"][0]
                        q1_val = numeric_stats["q1_val"][0]
                        q3_val = numeric_stats["q3_val"][0]
                        summary["Mean"] = f"{mean_val:.2f}" if mean_val is not None else "N/A"
                        summary["Std"] = f"{std_val:.2f}" if std_val is not None else "N/A"
                        summary["Q1"] = f"{q1_val:.2f}" if q1_val is not None else "N/A"
                        summary["Median"] = f"{median_val:.2f}" if median_val is not None else "N/A"
                        summary["Q3"] = f"{q3_val:.2f}" if q3_val is not None else "N/A"
                    except Exception:
                        # Column might have mixed types or other issues
                        summary["Min"] = "Error"
                        summary["Max"] = "Error"
                        summary["Mean"] = "Error"
                        summary["Std"] = "Error"
                        summary["Q1"] = "Error"
                        summary["Median"] = "Error"
                        summary["Q3"] = "Error"
                elif col_dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                    summary["Min"] = "All null"
                    summary["Max"] = "All null"
                    summary["Mean"] = "All null"
                    summary["Std"] = "All null"
                    summary["Q1"] = "All null"
                    summary["Median"] = "All null"
                    summary["Q3"] = "All null"
                else:
                    # String/categorical column
                    if non_null_count > 0:
                        try:
                            unique_count = self.full_data.select(pl.col(col).n_unique()).collect().item()
                            summary["Unique Values"] = unique_count
                            summary["Top Values"] = self._get_top_values(col)
                        except Exception:
                            summary["Unique Values"] = "Error"
                            summary["Top Values"] = "Error"
                    else:
                        summary["Unique Values"] = 0
                        summary["Top Values"] = "All null"
                
                summary["Active"] = active_status.get(col, "Unknown")
                missing_years = year_analysis.get(col, [])
                present_years = [y for y in self.years if y not in missing_years]
                summary["Years Present"] = ", ".join(present_years) if present_years else ""
                summary["Missing Years"] = ", ".join(missing_years) if missing_years else ""
                
                summaries.append(summary)
            except Exception as e:
                print(f"Error processing {col}: {e}")
                summaries.append({
                    "Column Name": col,
                    "Data Type": str(self.dtypes[col]),
                    "Non-Null Count": "Error",
                    "Null Count": "Error", 
                    "Null Percentage": "Error",
                    "Total Rows": self.total_rows,
                    "Active": active_status.get(col, "Unknown"),
                    "Years Present": "",
                    "Missing Years": ""
                })
        
        return summaries
    
    def _get_top_values(self, col: str) -> str:
        """Get top 5 values for a column."""
        try:
            top_values = (self.full_data
                         .select(col)
                         .filter(pl.col(col).is_not_null())
                         .group_by(col)
                         .agg(pl.len().alias("count"))
                         .sort("count", descending=True)
                         .limit(5)
                         .collect())
            
            if len(top_values) > 0:
                top_list = [f"{row[0]} ({row[1]:,})" for row in top_values.iter_rows()]
                return "; ".join(top_list)
            else:
                return "All null"
        except Exception:
            return "Error getting top values"
    
    def _process_columns_individually(self, cols: List[str], active_status: Dict[str, str], 
                                    year_analysis: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        """Fallback for processing columns one by one."""
        summaries = []
        for col in cols:
            try:
                # Process individual column
                null_count = self.full_data.select(pl.col(col).is_null().sum()).collect().item()
                non_null_count = self.total_rows - null_count
                null_pct = (null_count / self.total_rows * 100)
                
                missing_years = year_analysis.get(col, [])
                present_years = [y for y in self.years if y not in missing_years]
                
                summary = {
                    "Column Name": col,
                    "Data Type": str(self.dtypes[col]),
                    "Non-Null Count": non_null_count,
                    "Null Count": null_count,
                    "Null Percentage": f"{null_pct:.2f}%" if null_pct > 99.9 else f"{null_pct:.1f}%",
                    "Total Rows": self.total_rows,
                    "Active": active_status.get(col, "Unknown"),
                    "Years Present": ", ".join(present_years) if present_years else "",
                    "Missing Years": ", ".join(missing_years) if missing_years else ""
                }
                
                # Add type-specific fields based on dtype
                if self.dtypes[col] in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                    if non_null_count > 0:
                        try:
                            stats = (self.full_data
                                   .select([
                                       pl.col(col).min().alias("min_val"),
                                       pl.col(col).max().alias("max_val"),
                                       pl.col(col).mean().alias("mean_val"),
                                       pl.col(col).std().alias("std_val"),
                                       pl.col(col).quantile(0.25).alias("q1_val"),
                                       pl.col(col).median().alias("median_val"),
                                       pl.col(col).quantile(0.75).alias("q3_val")
                                   ])
                                   .collect())
                            
                            summary["Min"] = stats["min_val"][0]
                            summary["Max"] = stats["max_val"][0]
                            mean_val = stats["mean_val"][0]
                            std_val = stats["std_val"][0]
                            q1_val = stats["q1_val"][0]
                            median_val = stats["median_val"][0]
                            q3_val = stats["q3_val"][0]
                            summary["Mean"] = f"{mean_val:.2f}" if mean_val is not None else "N/A"
                            summary["Std"] = f"{std_val:.2f}" if std_val is not None else "N/A"
                            summary["Q1"] = f"{q1_val:.2f}" if q1_val is not None else "N/A"
                            summary["Median"] = f"{median_val:.2f}" if median_val is not None else "N/A"
                            summary["Q3"] = f"{q3_val:.2f}" if q3_val is not None else "N/A"
                        except Exception:
                            summary["Min"] = "Error"
                            summary["Max"] = "Error"
                            summary["Mean"] = "Error"
                            summary["Std"] = "Error"
                            summary["Q1"] = "Error"
                            summary["Median"] = "Error"
                            summary["Q3"] = "Error"
                    else:
                        summary["Min"] = "All null"
                        summary["Max"] = "All null"
                        summary["Mean"] = "All null"
                        summary["Std"] = "All null"
                        summary["Q1"] = "All null"
                        summary["Median"] = "All null"
                        summary["Q3"] = "All null"
                else:
                    if non_null_count > 0:
                        try:
                            unique_count = self.full_data.select(pl.col(col).n_unique()).collect().item()
                            summary["Unique Values"] = unique_count
                            summary["Top Values"] = self._get_top_values(col)
                        except Exception:
                            summary["Unique Values"] = "Error"
                            summary["Top Values"] = "Error"
                    else:
                        summary["Unique Values"] = 0
                        summary["Top Values"] = "All null"
                
                summaries.append(summary)
            except Exception as e:
                print(f"Error processing {col}: {e}")
                summaries.append({
                    "Column Name": col,
                    "Data Type": str(self.dtypes[col]),
                    "Non-Null Count": "Error",
                    "Null Count": "Error",
                    "Null Percentage": "Error",
                    "Total Rows": self.total_rows,
                    "Active": active_status.get(col, "Unknown"),
                    "Missing Years": ""
                })
        
        return summaries
    
    def _generate_year_null_analysis(self) -> Dict[str, List[str]]:
        """Generate analysis of which years have all nulls for each variable.
        
        Returns:
            Dict mapping column names to lists of years where the column is all null.
        """
        year_analysis = {}
        
        # Process in batches for efficiency
        for i in range(0, len(self.columns), self.batch_size):
            batch_cols = [col for col in self.columns[i:i+self.batch_size] if col != "OPERYR"]
            
            if not batch_cols:
                continue
                
            # Build expressions to check if columns have any non-null values by year
            exprs = []
            for col in batch_cols:
                exprs.append(
                    pl.col(col).is_not_null().any().alias(f"{col}_has_data")
                )
            
            # Group by year and check each column
            year_stats = (self.full_data
                         .group_by("OPERYR")
                         .agg(exprs)
                         .collect()
                         .sort("OPERYR"))
            
            # Process results
            for col in batch_cols:
                all_null_years = []
                
                for row in year_stats.iter_rows(named=True):
                    year = str(row["OPERYR"])
                    has_data = row.get(f"{col}_has_data", False)
                    
                    if not has_data:  # Column is all null for this year
                        all_null_years.append(year)
                
                year_analysis[col] = all_null_years
        
        return year_analysis
    
    def _generate_enhanced_html_content(self, df: pl.DataFrame, year_null_data: Dict) -> str:
        """Generate enhanced HTML content with year analysis."""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>NSQIP Data Dictionary</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2 {{ color: #2c3e50; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .metadata {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                .year-table {{ margin-top: 40px; }}
                .null-high {{ background-color: #ffcccc; }}
                .null-medium {{ background-color: #fff3cd; }}
                .null-low {{ background-color: #d4edda; }}
                .active-yes {{ color: green; font-weight: bold; }}
                .active-no {{ color: red; }}
                .tab-container {{ margin-top: 20px; }}
                .tab-button {{ padding: 10px 20px; cursor: pointer; background: #f1f1f1; border: none; }}
                .tab-button.active {{ background: #2c3e50; color: white; }}
                .tab-content {{ display: none; padding: 20px; border: 1px solid #ddd; }}
                .tab-content.active {{ display: block; }}
            </style>
            <script>
                function showTab(tabName) {{
                    var tabs = document.getElementsByClassName('tab-content');
                    var buttons = document.getElementsByClassName('tab-button');
                    
                    for (var i = 0; i < tabs.length; i++) {{
                        tabs[i].classList.remove('active');
                        buttons[i].classList.remove('active');
                    }}
                    
                    document.getElementById(tabName).classList.add('active');
                    document.getElementById(tabName + '-btn').classList.add('active');
                }}
            </script>
        </head>
        <body>
            <h1>NSQIP Data Dictionary</h1>
        """
        
        # Add metadata section with total rows
        html += '<div class="metadata">'
        html += f'<h3>Dataset Information</h3>'
        html += f'<p><strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>'
        html += f'<p><strong>Source:</strong> {self.parquet_dir}</p>'
        html += f'<p><strong>Parquet Files:</strong> {len(self.parquet_files)}</p>'
        html += f'<p><strong>Total Rows:</strong> {self.total_rows:,}</p>'
        html += f'<p><strong>Total Columns:</strong> {len(self.columns)}</p>'
        html += f'<p><strong>Years:</strong> {", ".join(self.years)}</p>'
        html += f'<p><strong>Most Recent Year:</strong> {self.most_recent_year}</p>'
        
        if 'dataset_type' in self.metadata:
            html += f'<p><strong>Dataset Type:</strong> {self.metadata["dataset_type"]}</p>'
        
        html += '</div>'
        
        # Add tabs
        html += '<div class="tab-container">'
        html += '<button class="tab-button active" id="summary-btn" onclick="showTab(\'summary\')">Column Summary</button>'
        html += '<button class="tab-button" id="year-analysis-btn" onclick="showTab(\'year-analysis\')">Missing by Year</button>'
        html += '</div>'
        
        # Column summary tab
        html += '<div id="summary" class="tab-content active">'
        html += '<h2>Column Summary</h2>'
        html += '<table>'
        
        # Headers
        html += '<tr>'
        for col in df.columns:
            html += f'<th>{col}</th>'
        html += '</tr>'
        
        # Data rows
        for row in df.iter_rows():
            html += '<tr>'
            for i, cell in enumerate(row):
                col_name = df.columns[i]
                if col_name == "Active":
                    if cell == "Yes":
                        html += f'<td class="active-yes">{cell}</td>'
                    elif cell == "No":
                        html += f'<td class="active-no">{cell}</td>'
                    else:
                        html += f'<td>{cell}</td>'
                else:
                    html += f'<td>{cell}</td>'
            html += '</tr>'
        
        html += '</table>'
        html += '</div>'
        
        # Year analysis tab
        html += '<div id="year-analysis" class="tab-content">'
        html += '<h2>Variable Availability by Year</h2>'
        html += '<p>Shows which variables have data (Yes) or are completely missing (No) for each year.</p>'
        
        html += '<table class="year-table">'
        html += '<tr><th>Variable</th>'
        for year in self.years:
            html += f'<th>{year}</th>'
        html += '<th>Missing Years</th>'
        html += '</tr>'
        
        # Add rows for each variable
        for col in sorted(self.columns):
            if col == "OPERYR":
                continue
                
            all_null_years = year_null_data.get(col, [])
            
            html += f'<tr><td><strong>{col}</strong></td>'
            
            # Show check or X for each year
            for year in self.years:
                if year in all_null_years:
                    html += '<td class="null-high" style="text-align: center;">No</td>'
                else:
                    html += '<td class="null-low" style="text-align: center;">Yes</td>'
            
            # Add summary column
            if all_null_years:
                html += f'<td>{", ".join(all_null_years)}</td>'
            else:
                html += '<td>-</td>'
            
            html += '</tr>'
        
        html += '</table>'
        html += '</div>'
        
        html += """
        </body>
        </html>
        """
        
        return html