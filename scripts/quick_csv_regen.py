#!/usr/bin/env python3
"""Quick CSV regeneration to test fixes."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import time

from nsqip_tools.data_dictionary import DataDictionaryGenerator


def main():
    if len(sys.argv) != 2:
        print("Usage: python quick_csv_regen.py <parquet_directory>")
        sys.exit(1)

    parquet_dir = Path(sys.argv[1])

    print("Regenerating CSV data dictionary with fixes...")
    print(f"Parquet directory: {parquet_dir}")
    print("-" * 50)

    start_time = time.time()

    try:
        # Generate just CSV for speed
        print("Initializing generator...")
        generator = DataDictionaryGenerator(parquet_dir)

        print(f"Found {len(generator.columns)} columns")
        print(f"Years: {', '.join(generator.years)}")
        print(f"Total rows: {generator.total_rows:,}")

        # Limit to first 20 columns for quick test
        print("Testing with first 20 columns...")
        generator.columns = generator.columns[:20]

        output_path = parquet_dir / "adult_data_dictionary_fixed.csv"
        generator.generate_csv(output_path)

        elapsed = time.time() - start_time
        print(f"✅ CSV generated successfully in {elapsed:.1f} seconds")
        print(f"Output: {output_path}")

        # Check the header
        with open(output_path) as f:
            header = f.readline().strip()
        print(f"Header: {header}")

    except Exception as e:
        print(f"❌ Generation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
