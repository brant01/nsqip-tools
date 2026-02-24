#!/usr/bin/env python3
"""Build the full NSQIP dataset."""

import sys
from pathlib import Path

from nsqip_tools import build_parquet_dataset


def main():
    if len(sys.argv) != 2:
        print("Usage: python build_full_dataset.py <data_directory>")
        print("Example: python build_full_dataset.py /path/to/nsqip/data")
        sys.exit(1)

    data_dir = Path(sys.argv[1])
    if not data_dir.exists():
        print(f"Error: Data directory does not exist: {data_dir}")
        sys.exit(1)

    print("Starting NSQIP dataset build...")
    print(f"Data directory: {data_dir}")
    print("This will take approximately 45-60 minutes to complete.")
    print("-" * 50)

    try:
        result = build_parquet_dataset(
            data_dir=data_dir,
            dataset_type=None,  # Auto-detect
            memory_limit="3GB",  # Slightly higher limit for overnight run
            verify_case_counts=True,
            generate_dictionary=True,
        )

        print("\n" + "=" * 50)
        print("BUILD COMPLETE!")
        print(f"Dataset location: {result['parquet_dir']}")
        print(f"Data dictionary: {result.get('dictionary', 'Not generated')}")
        print(f"Log file: {result['log']}")

    except Exception as e:
        print(f"\nBuild failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
