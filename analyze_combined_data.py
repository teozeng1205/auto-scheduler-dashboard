#!/usr/bin/env python3
"""
Analyze the combined dataset and provide comprehensive insights.
"""

import pandas as pd
import numpy as np
from collections import Counter


def analyze_combined_dataset(csv_file="combined_all_data.csv"):
    """Analyze the combined dataset and provide comprehensive insights"""
    
    print(f"📊 Loading dataset: {csv_file}")
    
    # Load the dataset (read in chunks if too large)
    try:
        # Try to read the full dataset
        df = pd.read_csv(csv_file)
        print(f"✅ Successfully loaded {len(df):,} records with {len(df.columns)} columns")
    except MemoryError:
        print("⚠️  Dataset too large for memory, analyzing in chunks...")
        # If too large, analyze in chunks
        chunk_size = 100000
        chunks = []
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            chunks.append(chunk)
            if len(chunks) >= 10:  # Limit to first 1M records for analysis
                break
        df = pd.concat(chunks, ignore_index=True)
        print(f"✅ Loaded sample of {len(df):,} records for analysis")
    
    print(f"\n🎯 DATASET OVERVIEW")
    print(f"=" * 50)
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # 1. Collection Frequency Analysis
    print(f"\n📈 COLLECTION FREQUENCY DISTRIBUTION")
    print(f"=" * 50)
    if 'collection_frequency' in df.columns:
        freq_counts = df['collection_frequency'].value_counts()
        for freq, count in freq_counts.items():
            percentage = (count / len(df)) * 100
            print(f"  {freq:>8}: {count:>10,} records ({percentage:5.1f}%)")
    
    # 2. Plan ID Analysis
    print(f"\n🔢 HOURLY COLLECTION PLAN ID DISTRIBUTION")
    print(f"=" * 50)
    if 'hourly_collection_plan_id' in df.columns:
        plan_counts = df['hourly_collection_plan_id'].value_counts().head(10)
        print("  Top 10 Plan IDs:")
        for plan_id, count in plan_counts.items():
            freq = df[df['hourly_collection_plan_id'] == plan_id]['collection_frequency'].iloc[0]
            print(f"    {plan_id:>3} ({freq:>5}): {count:>8,} records")
    
    # 3. Provider and Site Analysis
    print(f"\n🏢 PROVIDER AND SITE ANALYSIS")
    print(f"=" * 50)
    if 'provider' in df.columns:
        providers = df['provider'].value_counts()
        print(f"  Provider codes: {providers.to_dict()}")
    
    if 'site' in df.columns:
        sites = df['site'].value_counts().head(10)
        print(f"  Top site codes:")
        for site, count in sites.items():
            print(f"    {site}: {count:,} records")
    
    # 4. Customer Analysis
    print(f"\n👥 CUSTOMER ANALYSIS")
    print(f"=" * 50)
    customer_cols = [col for col in df.columns if 'customer' in col.lower()]
    for col in customer_cols[:5]:  # Show first 5 customer-related columns
        if df[col].dtype == 'object':
            unique_count = df[col].nunique()
            print(f"  {col}: {unique_count:,} unique values")
            if unique_count <= 20:
                value_counts = df[col].value_counts().head(5)
                for val, count in value_counts.items():
                    print(f"    {val}: {count:,}")
    
    # 5. Time Analysis
    print(f"\n⏰ TIME ANALYSIS")
    print(f"=" * 50)
    time_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
    for col in time_cols[:10]:  # Show first 10 time-related columns
        if col in df.columns:
            non_null = df[col].notna().sum()
            print(f"  {col}: {non_null:,}/{len(df):,} non-null values")
            if non_null > 0 and df[col].dtype in ['int64', 'float64']:
                sample_values = df[col].dropna().head(3).tolist()
                print(f"    Sample values: {sample_values}")
    
    # 6. Data Quality Analysis
    print(f"\n🔍 DATA QUALITY ANALYSIS")
    print(f"=" * 50)
    
    # Missing data analysis
    missing_data = df.isnull().sum()
    high_missing = missing_data[missing_data > len(df) * 0.5]  # More than 50% missing
    if len(high_missing) > 0:
        print(f"  Columns with >50% missing data ({len(high_missing)} columns):")
        for col, missing_count in high_missing.head(10).items():
            missing_pct = (missing_count / len(df)) * 100
            print(f"    {col}: {missing_pct:.1f}% missing")
    
    # Completely filled columns
    complete_cols = missing_data[missing_data == 0]
    print(f"  Completely filled columns: {len(complete_cols)}")
    
    # 7. File Distribution Analysis
    print(f"\n📁 FILE CONTRIBUTION ANALYSIS")
    print(f"=" * 50)
    if 'collection_frequency' in df.columns and 'hourly_collection_plan_id' in df.columns:
        file_summary = df.groupby(['collection_frequency', 'hourly_collection_plan_id']).size().reset_index(name='record_count')
        
        for freq in file_summary['collection_frequency'].unique():
            freq_data = file_summary[file_summary['collection_frequency'] == freq]
            total_records = freq_data['record_count'].sum()
            file_count = len(freq_data)
            avg_records = total_records / file_count if file_count > 0 else 0
            print(f"  {freq.upper()}:")
            print(f"    Files: {file_count}")
            print(f"    Total records: {total_records:,}")
            print(f"    Avg records per file: {avg_records:,.0f}")
            
            # Show top contributing files for this frequency
            top_files = freq_data.nlargest(3, 'record_count')
            print(f"    Top contributing files:")
            for _, row in top_files.iterrows():
                print(f"      {freq}-{row['hourly_collection_plan_id']}: {row['record_count']:,} records")
    
    # 8. Column Type Summary
    print(f"\n📋 COLUMN TYPE SUMMARY")
    print(f"=" * 50)
    dtype_counts = Counter(str(dtype) for dtype in df.dtypes)
    for dtype, count in dtype_counts.items():
        print(f"  {dtype}: {count} columns")
    
    # 9. Sample Data Preview
    print(f"\n👀 SAMPLE DATA PREVIEW")
    print(f"=" * 50)
    print("First 3 records (first 10 columns):")
    preview_cols = df.columns[:10]
    print(df[preview_cols].head(3).to_string(index=False))
    
    return df


def generate_summary_report(df):
    """Generate a summary report file"""
    
    summary = []
    summary.append("# Combined Dataset Analysis Report")
    summary.append(f"Generated from {len(df):,} records")
    summary.append("")
    
    summary.append("## Dataset Overview")
    summary.append(f"- **Total Records**: {len(df):,}")
    summary.append(f"- **Total Columns**: {len(df.columns)}")
    summary.append(f"- **Memory Usage**: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    summary.append("")
    
    # Collection frequency breakdown
    if 'collection_frequency' in df.columns:
        summary.append("## Collection Frequency Distribution")
        freq_counts = df['collection_frequency'].value_counts()
        for freq, count in freq_counts.items():
            percentage = (count / len(df)) * 100
            summary.append(f"- **{freq}**: {count:,} records ({percentage:.1f}%)")
        summary.append("")
    
    # Data quality
    summary.append("## Data Quality")
    missing_data = df.isnull().sum()
    complete_cols = len(missing_data[missing_data == 0])
    partial_cols = len(missing_data[(missing_data > 0) & (missing_data < len(df))])
    empty_cols = len(missing_data[missing_data == len(df)])
    
    summary.append(f"- **Complete columns** (no missing data): {complete_cols}")
    summary.append(f"- **Partial columns** (some missing data): {partial_cols}")
    summary.append(f"- **Empty columns** (all missing data): {empty_cols}")
    summary.append("")
    
    # Save summary
    with open('combined_dataset_summary.md', 'w') as f:
        f.write('\n'.join(summary))
    
    print(f"📄 Summary report saved to: combined_dataset_summary.md")


if __name__ == "__main__":
    df = analyze_combined_dataset()
    if df is not None:
        generate_summary_report(df)
        print(f"\n🎉 Analysis complete!") 