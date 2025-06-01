"""Basic usage examples for NSQIP Tools.

Note: Install the package first with: uv pip install -e .
"""
import nsqip_tools
import polars as pl
from pathlib import Path


def build_dataset_example():
    """Example of building a parquet dataset from NSQIP text files."""
    # Path to your NSQIP text files
    data_dir = Path("/path/to/nsqip/files")
    
    # Build the parquet dataset
    result = nsqip_tools.build_parquet_dataset(
        data_dir=data_dir,
        dataset_type="adult",  # or "pediatric"
        memory_limit="4GB",    # Adjust based on your system
    )
    
    print(f"Dataset created: {result['parquet_dir']}")
    print(f"Data dictionary: {result['dictionary']}")
    print(f"Build log: {result['log']}")


def query_examples():
    """Examples of querying NSQIP data."""
    # Load the parquet dataset
    dataset_path = "/path/to/adult_nsqip_parquet"
    
    # Example 1: Simple CPT filter
    print("Example 1: Laparoscopic cholecystectomy cases")
    query = (nsqip_tools.load_data(dataset_path)
             .filter_by_cpt(["47562", "47563", "47564"]))
    
    # Check dataset info
    info = query.describe()
    print(f"Total rows: {info['total_rows']:,}")
    print(f"Columns: {info['columns']}")
    
    # Collect results
    lap_chole = query.collect()
    print(f"Found {len(lap_chole)} cases")
    
    # Example 2: Multiple filters
    print("\nExample 2: Recent gallbladder cases")
    recent_gb = (nsqip_tools.load_data(dataset_path)
                 .filter_by_diagnosis(["K80.20", "K80.21"])  # Gallstones
                 .filter_by_year([2020, 2021])
                 .collect())
    print(f"Found {len(recent_gb)} cases")
    
    # Example 3: Complex query with Polars
    print("\nExample 3: Age analysis by CPT")
    age_analysis = (nsqip_tools.load_data(dataset_path)
                    .filter_by_cpt(["47562", "47563"])
                    .lazy_frame
                    .select(["CPT", "AGE_AS_INT", "OPERYR"])
                    .filter(pl.col("AGE_AS_INT").is_not_null())
                    .group_by(["CPT", "OPERYR"])
                    .agg([
                        pl.count().alias("n_cases"),
                        pl.col("AGE_AS_INT").mean().alias("mean_age"),
                        pl.col("AGE_AS_INT").median().alias("median_age"),
                    ])
                    .sort(["OPERYR", "CPT"])
                    .collect())
    print(age_analysis)
    
    # Example 4: Active variables only
    print("\nExample 4: Filter to active variables")
    active_query = (nsqip_tools.load_data(dataset_path)
                   .filter_by_year([2021])
                   .filter_active_variables())
    
    active_info = active_query.describe()
    print(f"Columns after filtering: {active_info['columns']}")
    
    # Example 5: Demographic subset
    print("\nExample 5: Demographics only")
    demographics = (nsqip_tools.load_data(dataset_path)
                   .filter_by_year([2021])
                   .select_demographics()
                   .sample(n=1000))
    print(f"Demographic sample: {demographics.shape}")
    
    # Example 6: Large dataset collection
    print("\nExample 6: Large dataset collection")
    large_query = nsqip_tools.load_data(dataset_path).filter_by_year([2019, 2020, 2021])
    
    # Collect results directly
    df_large = large_query.collect()
    print(f"Collected {len(df_large)} rows")


def data_dictionary_example():
    """Example of working with data dictionaries."""
    import nsqip_tools.data_dictionary as dd
    
    # Generate data dictionary for existing dataset
    dataset_path = "/path/to/adult_nsqip_parquet"
    generator = dd.DataDictionaryGenerator(dataset_path)
    
    # Generate in different formats
    generator.generate_csv("data_dictionary.csv")
    generator.generate_excel("data_dictionary.xlsx")
    generator.generate_html("data_dictionary.html")
    
    print("Data dictionaries generated in CSV, Excel, and HTML formats")


def network_drive_example():
    """Example of working with network drives."""
    # Build dataset on network drive
    network_data_dir = Path("/Volumes/network_drive/nsqip_data")
    network_output_dir = Path("/Volumes/network_drive/processed")
    
    # This works seamlessly on network drives
    result = nsqip_tools.build_parquet_dataset(
        data_dir=network_data_dir,
        output_dir=network_output_dir,
        dataset_type="adult"
    )
    
    print(f"Network dataset created: {result['parquet_dir']}")
    
    # Query from network location
    query = nsqip_tools.load_data(result['parquet_dir'])
    sample_data = query.sample(n=100)
    print(f"Network query sample: {sample_data.shape}")


if __name__ == "__main__":
    # Uncomment to run examples
    # build_dataset_example()
    # query_examples()
    # data_dictionary_example()
    # network_drive_example()
    pass