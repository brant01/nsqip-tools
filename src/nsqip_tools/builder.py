"""Main API for building NSQIP parquet datasets.
This module provides the high-level interface for converting NSQIP text files
into optimized parquet datasets with standard transformations.
"""
import logging
from pathlib import Path
from typing import Union, Optional, Literal, Dict
from datetime import datetime

from .constants import (
    DATASET_TYPES,
    EXPECTED_CASE_COUNTS,
    DATASET_NAME_TEMPLATE,
)
from ._internal.ingest import create_parquet_from_text
from ._internal.transform import apply_transformations
from .data_dictionary import DataDictionaryGenerator
from ._internal.memory_utils import get_recommended_memory_limit, get_memory_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_parquet_dataset(
    data_dir: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    dataset_type: Optional[Literal["adult", "pediatric"]] = None,
    generate_dictionary: bool = True,
    memory_limit: Optional[str] = None,
    verify_case_counts: bool = True,
    apply_transforms: bool = True,
) -> Dict[str, Path]:
    """Build an NSQIP parquet dataset from text files with standard transformations.
    
    This function performs the complete pipeline of ingesting NSQIP text files,
    applying standard transformations, and optionally generating a data dictionary.
    All original data is preserved - transformations only add new columns.
    
    Args:
        data_dir: Directory containing NSQIP text files (tab-delimited).
        output_dir: Directory for output files. If None, creates a parquet subdirectory 
                   within data_dir (e.g., data_dir/adult_nsqip_parquet/).
        dataset_type: Type of NSQIP data ("adult" or "pediatric"). If None, 
                     auto-detects based on file names.
        generate_dictionary: Whether to generate a data dictionary.
        memory_limit: Memory limit for operations (e.g., "4GB", "8GB"). If None,
                     automatically determined based on available system memory.
        verify_case_counts: Whether to verify case counts match expected values.
        apply_transforms: Whether to apply standard transformations.
        
    Returns:
        Dictionary with paths to generated files:
            - "parquet_dir": Path to the parquet directory
            - "dictionary": Path to data dictionary (if generated)
            - "log": Path to the log file
            
    Raises:
        ValueError: If dataset_type is not supported or no text files found.
        RuntimeError: If building the dataset fails.
        
    Example:
        >>> # Auto-detects dataset type from filenames and creates subdirectory
        >>> result = build_parquet_dataset(
        ...     data_dir="/path/to/nsqip/data",
        ...     generate_dictionary=True
        ... )
        >>> print(f"Dataset created at: {result['parquet_dir']}")
        
        >>> # Or specify explicitly
        >>> result = build_parquet_dataset(
        ...     data_dir="/path/to/nsqip/data",
        ...     dataset_type="adult"
        ... )
    """
    # Convert paths
    data_dir = Path(data_dir)
    
    # Auto-detect dataset type if not specified
    if dataset_type is None:
        detected_type = _detect_dataset_type(data_dir)
        dataset_type = detected_type  # type: ignore
        logger.info(f"Auto-detected dataset type: {dataset_type}")
    
    # Validate inputs
    if dataset_type not in DATASET_TYPES:
        raise ValueError(
            f"Invalid dataset_type '{dataset_type}'. Must be one of: {DATASET_TYPES}"
        )
    if not data_dir.exists():
        raise ValueError(f"Data directory does not exist: {data_dir}")
    
    # Create clean directory structure
    if output_dir is None:
        # Create parquet subdirectory within data directory
        dataset_name = DATASET_NAME_TEMPLATE.format(dataset_type=dataset_type)
        parquet_output_dir = data_dir / dataset_name
    else:
        parquet_output_dir = Path(output_dir)
    
    parquet_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up logging - log file goes with the parquet files
    log_path = parquet_output_dir / f"build_{dataset_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Set up file logging
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(file_handler)
    
    # Determine memory limit if not specified
    if memory_limit is None:
        memory_limit = get_recommended_memory_limit(conservative=True)
        mem_info = get_memory_info()
        logger.info(f"System memory: {mem_info['total']} total, {mem_info['available']} available")
        logger.info(f"Auto-detected memory limit: {memory_limit}")
    
    logger.info(f"Starting NSQIP parquet dataset build for {dataset_type} data")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Output directory: {parquet_output_dir}")
    logger.info(f"Memory limit: {memory_limit}")
    
    try:
        # Step 1: Create parquet files from text files
        logger.info("Step 1: Creating parquet files from text files")
        parquet_dir = create_parquet_from_text(
            text_file_dir=data_dir,
            output_dir=parquet_output_dir,
            dataset_type=dataset_type,
        )
        
        # Step 2: Verify case counts
        if verify_case_counts:
            logger.info("Step 2: Verifying case counts")
            _verify_case_counts(parquet_dir, dataset_type)
        
        # Step 3: Apply standard transformations
        if apply_transforms:
            logger.info("Step 3: Applying standard transformations")
            apply_transformations(parquet_dir, dataset_type, memory_limit)
        
        # Step 4: Generate data dictionary
        result = {"parquet_dir": parquet_dir, "log": log_path}
        
        if generate_dictionary:
            logger.info("Step 4: Generating data dictionary")
            dict_path = _generate_data_dictionary(parquet_dir, parquet_output_dir, dataset_type)
            result["dictionary"] = dict_path
        
        logger.info(f"Build complete! Dataset saved to: {parquet_dir}")
        return result
        
    except Exception as e:
        logger.error(f"Build failed: {e}")
        raise RuntimeError(f"Failed to build NSQIP dataset: {str(e)}") from e
    finally:
        # Remove the file handler to avoid duplicate logs
        logger.removeHandler(file_handler)
        file_handler.close()


def _verify_case_counts(parquet_dir: Path, dataset_type: str) -> None:
    """Verify case counts match expected values from constants.
    
    Args:
        parquet_dir: Path to parquet directory
        dataset_type: Type of dataset being verified
    """
    import polars as pl
    import json
    
    # Read metadata
    metadata_path = parquet_dir / "metadata.json"
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    # Get expected counts
    expected_counts = EXPECTED_CASE_COUNTS.get(dataset_type, {})
    if not expected_counts:
        logger.warning(f"No expected case counts defined for {dataset_type} dataset")
        return
    
    # Read all parquet files and count by year
    actual_counts = {}
    for parquet_file in parquet_dir.glob("*.parquet"):
        if parquet_file.name == "metadata.json":
            continue
            
        df = pl.scan_parquet(parquet_file)
        
        # Try to get year from OPERYR column
        if "OPERYR" in df.columns:
            year_counts = df.group_by("OPERYR").agg(pl.count()).collect()
            for row in year_counts.iter_rows():
                year = str(row[0])
                count = row[1]
                actual_counts[year] = actual_counts.get(year, 0) + count
        else:
            # If no OPERYR, count total rows
            total = df.select(pl.count()).collect().item()
            actual_counts["total"] = actual_counts.get("total", 0) + total
    
    # Compare counts
    has_mismatch = False
    for year, expected in expected_counts.items():
        actual = actual_counts.get(year, 0)
        if actual != expected:
            logger.warning(
                f"Case count mismatch for year {year}: "
                f"expected {expected:,}, found {actual:,} "
                f"(difference: {actual - expected:+,})"
            )
            has_mismatch = True
        else:
            logger.info(f"Year {year}: {actual:,} cases (verified)")
    
    # Report any unexpected years
    for year, count in actual_counts.items():
        if year not in expected_counts:
            logger.warning(f"Unexpected year {year} with {count:,} cases")
    
    if has_mismatch:
        logger.warning("Case count verification completed with mismatches")
    else:
        logger.info("Case count verification passed")


def _detect_dataset_type(data_dir: Path) -> str:
    """Auto-detect dataset type based on file names.
    
    Args:
        data_dir: Directory containing NSQIP text files
        
    Returns:
        Detected dataset type ("adult" or "pediatric")
        
    Raises:
        ValueError: If dataset type cannot be determined
    """
    txt_files = list(data_dir.glob("*.txt"))
    if not txt_files:
        raise ValueError(f"No .txt files found in {data_dir}")
    
    # Check file names for specific patterns
    for file in txt_files:
        filename_lower = file.name.lower()
        
        # Pediatric files start with 'acs_peds'
        if filename_lower.startswith('acs_peds'):
            return "pediatric"
        
        # Adult files start with 'acs_nsqip'
        if filename_lower.startswith('acs_nsqip'):
            return "adult"
    
    # If no clear pattern found, raise error
    filenames = [f.name for f in txt_files]
    raise ValueError(
        f"Could not auto-detect dataset type from filenames: {filenames}. "
        f"Expected filenames to start with 'acs_peds' (pediatric) or 'acs_nsqip' (adult). "
        f"Please specify dataset_type explicitly."
    )


def _generate_data_dictionary(parquet_dir: Path, output_dir: Path, dataset_type: str) -> Path:
    """Generate data dictionary from parquet files.
    
    Args:
        parquet_dir: Path to parquet directory
        output_dir: Directory for output files
        dataset_type: Type of dataset
        
    Returns:
        Path to generated data dictionary
    """
    generator = DataDictionaryGenerator(parquet_dir)
    
    # Generate in multiple formats
    dict_filename = f"{dataset_type}_data_dictionary"
    
    # CSV format (primary)
    dict_path = output_dir / f"{dict_filename}.csv"
    generator.generate_csv(dict_path)
    logger.info(f"Generated data dictionary: {dict_path}")
    
    # JSON format
    json_path = output_dir / f"{dict_filename}.json"
    generator.generate_json(json_path)
    logger.info(f"Generated JSON data dictionary: {json_path}")
    
    # HTML format
    html_path = output_dir / f"{dict_filename}.html"
    generator.generate_html(html_path)
    logger.info(f"Generated HTML data dictionary: {html_path}")
    
    return dict_path


# Keep backward compatibility
