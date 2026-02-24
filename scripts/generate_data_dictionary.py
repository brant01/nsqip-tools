#!/usr/bin/env python3
"""Generate data dictionary from existing NSQIP parquet dataset.

This script generates data dictionaries in CSV, JSON, and HTML formats
from an existing NSQIP parquet dataset.
"""

import argparse
import sys
from pathlib import Path

from nsqip_tools.data_dictionary import generate_data_dictionary


def main():
    parser = argparse.ArgumentParser(
        description="Generate data dictionary from NSQIP parquet dataset"
    )
    parser.add_argument(
        "parquet_dir",
        type=Path,
        help="Path to parquet dataset directory"
    )
    parser.add_argument(
        "--format",
        default="all",
        choices=["csv", "json", "html", "all"],
        help="Output format (default: all)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory to save output files (defaults to parquet directory)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of columns to process at once (default: 10, use 1 to disable batching)"
    )

    args = parser.parse_args()

    if not args.parquet_dir.exists():
        print(f"Error: Parquet directory does not exist: {args.parquet_dir}")
        sys.exit(1)

    print(f"Generating data dictionary for: {args.parquet_dir}")
    print(f"Output format: {args.format}")
    print(f"Batch size: {args.batch_size}")
    print("-" * 50)

    try:
        generate_data_dictionary(
            parquet_dir=args.parquet_dir,
            output_format=args.format,
            output_dir=args.output_dir,
            batch_size=args.batch_size
        )
        print("\nData dictionary generation complete!")

    except Exception as e:
        print(f"\nGeneration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
