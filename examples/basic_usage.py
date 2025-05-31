
"""Basic usage examples for NSQIP Tools."""
import nsqip_tools
import polars as pl
from pathlib import Path


def build_database_example():
    """Example of building a database from NSQIP text files."""
    # Path to your NSQIP text files
    data_dir = Path("/path/to/nsqip/files")
    
    # Build the database
    result = nsqip_tools.build_duck_db(
        data_dir=data_dir,
        dataset_type="adult",  # or "pediatric"
        memory_limit="4GB",    # Adjust based on your system
    )
    
    print(f"Database created: {result['database']}")
    print(f"Data dictionary: {result['dictionary']}")
    print(f"Build log: {result['log']}")


def query_examples():
    """Examples of querying NSQIP data."""
    # Load the database
    db_path = "adult_data.duckdb"
    
    # Example 1: Simple CPT filter with safe collection
    print("Example 1: Laparoscopic cholecystectomy cases")
    query = (nsqip_tools.load_data(db_path)
             .filter_by_cpt(["47562", "47563", "47564"]))
    
    # Check size before collecting
    size_info = query.estimate_size()
    print(f"Estimated size: {size_info['estimated_memory_str']}")
    
    # Safe collection
    lap_chole = query.safe_collect()
    print(f"Found {len(lap_chole)} cases")
    
    # Example 2: Multiple filters
    print("\nExample 2: Recent gallbladder cases")
    recent_gb = (nsqip_tools.load_data(db_path)
                 .filter_by_diagnosis(["K80.20", "K80.21"])  # Gallstones
                 .filter_by_year([2020, 2021])
                 .collect())
    print(f"Found {len(recent_gb)} cases")
    
    # Example 3: Complex query with Polars
    print("\nExample 3: Age analysis by CPT")
    age_analysis = (nsqip_tools.load_data(db_path)
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
    active_only = (nsqip_tools.load_data(db_path)
                   .filter_by_year([2021])
                   .filter_active_variables()
                   .lazy_frame
                   .select(pl.count())
                   .collect())
    print(f"Columns after filtering: {len(active_only.columns)}")


def data_dictionary_example():
    """Example of working with data dictionaries."""
    import nsqip_tools.data_dictionary as dd
    
    # Generate data dictionary for existing database
    db_path = "adult_data.duckdb"
    generator = dd.DataDictionaryGenerator(db_path)
    
    # Get summary as dictionary
    summary = generator.generate_summary()
    
    # Find inactive variables
    inactive_vars = [
        col["name"] 
        for col in summary["columns"] 
        if not col["is_active"]
    ]
    print(f"Found {len(inactive_vars)} inactive variables")
    
    # Save in different formats
    generator.save_to_csv("data_dictionary.csv")
    generator.save_to_json("data_dictionary.json")
    generator.save_to_html("data_dictionary.html")


if __name__ == "__main__":
    # Uncomment to run examples
    # build_database_example()
    # query_examples()
    # data_dictionary_example()
    pass