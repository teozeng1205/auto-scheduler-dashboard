#!/usr/bin/env python3
"""
Group identical rows in the combined parquet dataset and write counts to output CSV.
This is similar to group_identical_rows.py but optimized for parquet data.
"""

import pandas as pd
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

    # Process in chunks to handle large files
    chunk_count = 0
    for chunk in pd.read_csv(input_csv, chunksize=chunksize, dtype=str, keep_default_na=False):
        chunk_count += 1
        print(f"  Processing chunk {chunk_count}: {len(chunk):,} rows")
        
        # Convert each row (as tuple) to a hashable key and count
        for row in chunk.itertuples(index=False, name=None):
            row_counter[row] += 1

    print(f"✅ Finished processing {chunk_count} chunks. Unique rows found: {len(row_counter):,}")
    print(f"💾 Writing grouped data to: {output_csv}")

    # Prepare output DataFrame
    rows = [list(key) + [count] for key, count in row_counter.items()]
    
    # Ensure we don't duplicate the row_count column
    if "row_count" in header:
        output_columns = header + ["grouped_row_count"]
    else:
        output_columns = header + ["row_count"]
    
    df_out = pd.DataFrame(rows, columns=output_columns)
    
    # Sort by the count column descending to see most frequent combinations first
    count_col = "grouped_row_count" if "row_count" in header else "row_count"
    df_out = df_out.sort_values(count_col, ascending=False)
    
    df_out.to_csv(output_csv, index=False)

    print("🎉 Done!")
    
    # Print some statistics
    count_col = "grouped_row_count" if "row_count" in header else "row_count"
    total_original_rows = df_out[count_col].sum()
    unique_rows = len(df_out)
    compression_ratio = total_original_rows / unique_rows if unique_rows > 0 else 0
    
    print(f"\n📊 GROUPING STATISTICS")
    print(f"Original total rows: {total_original_rows:,}")
    print(f"Unique row patterns: {unique_rows:,}")
    print(f"Compression ratio: {compression_ratio:.1f}x")
    
    # Show top 10 most frequent patterns
    print(f"\n🔝 TOP 10 MOST FREQUENT PATTERNS:")
    top_patterns = df_out.head(10)
    for idx, row in top_patterns.iterrows():
        count = row[count_col]
        # Show some identifying information
        if 'collection_frequency' in row:
            freq = row['collection_frequency']
            provider = row.get('provider', 'N/A')
            site = row.get('site', 'N/A')
            print(f"  {count:>8,}x: {freq} - {provider}|{site}")
        else:
            print(f"  {count:>8,}x: Row pattern {idx}")


def analyze_grouped_data(grouped_csv: str):
    """Analyze the grouped data to provide insights"""
    if not os.path.exists(grouped_csv):
        print(f"Grouped file not found: {grouped_csv}")
        return
    
    print(f"\n🔍 ANALYZING GROUPED DATA: {grouped_csv}")
    print("=" * 50)
    
    df = pd.read_csv(grouped_csv)
    
    # Basic statistics
    total_patterns = len(df)
    total_original_records = df['row_count'].sum()
    
    print(f"Unique patterns: {total_patterns:,}")
    print(f"Total original records: {total_original_records:,}")
    print(f"Compression achieved: {total_original_records/total_patterns:.1f}x")
    
    # Distribution analysis
    if 'collection_frequency' in df.columns:
        print(f"\n📈 Distribution by collection frequency:")
        freq_stats = df.groupby('collection_frequency').agg({
            'row_count': ['sum', 'count', 'mean', 'max']
        }).round(1)
        freq_stats.columns = ['Total_Records', 'Unique_Patterns', 'Avg_Count', 'Max_Count']
        print(freq_stats)
    
    # Provider/Site analysis
    if 'provider' in df.columns and 'site' in df.columns:
        print(f"\n🏢 Provider|Site combinations:")
        provider_site_counts = df.groupby(['provider', 'site'])['row_count'].sum().sort_values(ascending=False)
        print(f"Top 10 provider|site combinations by total records:")
        for (provider, site), count in provider_site_counts.head(10).items():
            print(f"  {provider}|{site}: {count:,} records")
    
    # Row count distribution
    print(f"\n📊 Row count distribution:")
    count_stats = df['row_count'].describe()
    print(f"  Min: {count_stats['min']:,.0f}")
    print(f"  25%: {count_stats['25%']:,.0f}")
    print(f"  50%: {count_stats['50%']:,.0f}")
    print(f"  75%: {count_stats['75%']:,.0f}")
    print(f"  Max: {count_stats['max']:,.0f}")
    print(f"  Mean: {count_stats['mean']:,.1f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Group identical rows in a CSV and output counts.")
    parser.add_argument("input_csv", nargs="?", default="combined_all_parquet_data.csv", 
                        help="Path to the input CSV file")
    parser.add_argument("output_csv", nargs="?", default="combined_all_parquet_data_grouped.csv", 
                        help="Path to output CSV file")
    parser.add_argument("--chunksize", type=int, default=100000, 
                        help="Number of rows per chunk while reading (default: 100000)")
    parser.add_argument("--analyze", action="store_true", 
                        help="Also analyze the grouped data after processing")

    args = parser.parse_args()
    
    # Group identical rows
    group_identical_rows(args.input_csv, args.output_csv, args.chunksize)
    
    # Analyze if requested
    if args.analyze:
        analyze_grouped_data(args.output_csv) 