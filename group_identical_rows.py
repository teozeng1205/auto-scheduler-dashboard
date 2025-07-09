import pandas as pd  # type: ignore
from collections import Counter
import argparse
import os


def group_identical_rows(input_csv: str, output_csv: str, chunksize: int = 100000):
    """Group identical rows in `input_csv` and write counts to `output_csv`."""

    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    print(f"📥 Reading: {input_csv}")
    print(f"⚙️  Processing in chunks of {chunksize:,} rows …")

    # Counter to track identical rows
    row_counter: Counter = Counter()

    # Ensure we know all column names (using first chunk)
    header = pd.read_csv(input_csv, nrows=0).columns.tolist()

    for chunk in pd.read_csv(input_csv, chunksize=chunksize, dtype=str, keep_default_na=False):
        # Convert each row (as tuple) to a hashable key and count
        for row in chunk.itertuples(index=False, name=None):
            row_counter[row] += 1

    print(f"✅ Finished processing. Unique rows found: {len(row_counter):,}")
    print(f"💾 Writing grouped data to: {output_csv}")

    # Prepare output DataFrame
    rows = [list(key) + [count] for key, count in row_counter.items()]
    output_columns = header + ["row_count"]
    df_out = pd.DataFrame(rows, columns=output_columns)
    df_out.to_csv(output_csv, index=False)

    print("🎉 Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Group identical rows in a CSV and output counts.")
    parser.add_argument("input_csv", nargs="?", default="combined_all_data.csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", nargs="?", default="combined_all_data_grouped.csv", help="Path to output CSV file")
    parser.add_argument("--chunksize", type=int, default=100000, help="Number of rows per chunk while reading (default: 100000)")

    args = parser.parse_args()
    group_identical_rows(args.input_csv, args.output_csv, args.chunksize) 