#!/usr/bin/env python3
"""
Combine all parquet files into one comprehensive dataset.
This script processes all parquet files and creates a unified CSV.
"""

import pandas as pd
import os
import glob
from pathlib import Path


def get_parquet_files(parquet_dir="./s3_parquet_repo"):
    """
    Find all parquet files in the directory and its subdirectories.
    
    Parameters
    ----------
    parquet_dir : str
        Directory containing parquet files
        
    Returns
    -------
    dict
        Dictionary mapping collection frequencies to file lists
    """
    parquet_files = {}
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                
                # Extract collection frequency from folder name
                folder_name = os.path.basename(root)
                if folder_name not in parquet_files:
                    parquet_files[folder_name] = []
                parquet_files[folder_name].append(file_path)
    
    return parquet_files


def extract_metadata_from_filename(filename):
    """Extract collection_frequency and hourly_collection_plan_id from filename"""
    # Pattern: CollectionFrequency-id.parquet (e.g., Daily-17219.parquet, Adhoc-17217.parquet)
    basename = os.path.basename(filename)
    if basename.endswith('.parquet'):
        name_part = basename[:-8]  # Remove .parquet
        parts = name_part.split('-')
        if len(parts) == 2:
            collection_frequency = parts[0]
            try:
                hourly_collection_plan_id = int(parts[1])
                return collection_frequency, hourly_collection_plan_id
            except ValueError:
                return None, None
    return None, None


def process_parquet_file(parquet_file_path):
    """Process a single parquet file and return records with metadata"""
    filename = os.path.basename(parquet_file_path)
    collection_frequency, hourly_collection_plan_id = extract_metadata_from_filename(filename)
    
    if collection_frequency is None:
        print(f"⚠️  Warning: Could not extract metadata from {filename}")
    
    try:
        df = pd.read_parquet(parquet_file_path)
        
        # The parquet files should already have all necessary columns
        # But let's verify and add any missing metadata
        if 'file_collection_frequency' not in df.columns:
            df['file_collection_frequency'] = collection_frequency
        if 'file_hourly_collection_plan_id' not in df.columns:
            df['file_hourly_collection_plan_id'] = hourly_collection_plan_id
        
        original_record_count = len(df)
        
        print(f"✓ Processed {filename}: {original_record_count:,} records")
        return df, original_record_count
        
    except Exception as e:
        print(f"✗ Error processing {filename}: {e}")
        return None, 0


def combine_all_parquet_data(parquet_dir="./s3_parquet_repo", output_file="combined_all_parquet_data.csv"):
    """Combine all parquet files into one comprehensive dataset"""
    
    # Find all parquet files
    parquet_files_by_freq = get_parquet_files(parquet_dir)
    
    if not parquet_files_by_freq:
        print(f"No parquet files found in {parquet_dir}")
        return
    
    total_files = sum(len(files) for files in parquet_files_by_freq.values())
    print(f"Found {total_files} parquet files to process")
    
    all_dataframes = []
    file_stats = {}
    
    # Process each collection frequency
    for freq, file_list in parquet_files_by_freq.items():
        print(f"\n📊 Processing {freq} files ({len(file_list)} files)...")
        
        freq_dataframes = []
        freq_record_count = 0
        
        for parquet_file in sorted(file_list):
            df, record_count = process_parquet_file(parquet_file)
            if df is not None:
                freq_dataframes.append(df)
                freq_record_count += record_count
        
        if freq_dataframes:
            # Combine all files for this frequency
            freq_combined = pd.concat(freq_dataframes, ignore_index=True)
            all_dataframes.append(freq_combined)
            
            file_stats[freq] = {
                'files': len(freq_dataframes), 
                'records': freq_record_count
            }
            
            print(f"  ✓ Combined {len(freq_dataframes)} {freq} files: {freq_record_count:,} total records")
    
    if not all_dataframes:
        print("No records to process")
        return
    
    # Combine all frequencies
    print(f"\n📊 Combining all collection frequencies...")
    final_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Save to CSV
    print(f"💾 Saving to CSV: {output_file}")
    final_df.to_csv(output_file, index=False)
    
    print(f"\n✅ Results saved to: {output_file}")
    print(f"DataFrame shape: {final_df.shape}")
    print(f"Columns: {len(final_df.columns)}")
    
    # Print summary statistics
    print(f"\n📈 Summary by collection frequency:")
    for freq in sorted(file_stats.keys()):
        stats = file_stats[freq]
        print(f"  {freq}: {stats['files']} files, {stats['records']:,} records")
    
    # Show data distribution by frequency
    if 'collection_frequency' in final_df.columns:
        print(f"\n📊 Record distribution by collection frequency:")
        freq_counts = final_df['collection_frequency'].value_counts()
        for freq, count in freq_counts.items():
            print(f"  {freq}: {count:,} rows")
    
    # Show owner sequence distribution
    if 'ownerSequence' in final_df.columns:
        print(f"\n👥 Owner sequence distribution:")
        owner_counts = final_df['ownerSequence'].value_counts().sort_index()
        for seq, count in owner_counts.items():
            print(f"  Owner {seq}: {count:,} rows")
    
    # Show sample of columns
    print(f"\n📝 Sample columns:")
    for i, col in enumerate(sorted(final_df.columns)[:25]):  # Show first 25 columns
        non_null = final_df[col].notna().sum()
        print(f"  {col}: {non_null:,}/{len(final_df):,} non-null")
    
    if len(final_df.columns) > 25:
        print(f"  ... and {len(final_df.columns) - 25} more columns")
    
    # Show memory usage
    memory_usage = final_df.memory_usage(deep=True).sum() / 1024**2
    print(f"\n💾 Memory usage: {memory_usage:.1f} MB")
    
    return final_df


def compare_schemas(parquet_dir="./s3_parquet_repo"):
    """Compare schemas across different parquet files to ensure consistency"""
    print(f"\n🔍 SCHEMA COMPARISON")
    print("=" * 50)
    
    parquet_files_by_freq = get_parquet_files(parquet_dir)
    schemas = {}
    
    for freq, file_list in parquet_files_by_freq.items():
        if file_list:
            # Check first file of each frequency
            sample_file = file_list[0]
            try:
                df = pd.read_parquet(sample_file)
                schemas[freq] = {
                    'columns': list(df.columns),
                    'dtypes': df.dtypes.to_dict(),
                    'sample_file': os.path.basename(sample_file)
                }
                print(f"  {freq}: {len(df.columns)} columns (sample: {os.path.basename(sample_file)})")
            except Exception as e:
                print(f"  {freq}: Error reading {sample_file} - {e}")
    
    # Check if all schemas are the same
    if len(schemas) > 1:
        first_schema = list(schemas.values())[0]
        all_same = True
        
        for freq, schema in schemas.items():
            if schema['columns'] != first_schema['columns']:
                print(f"⚠️  Schema mismatch in {freq}!")
                all_same = False
        
        if all_same:
            print("✅ All collection frequencies have the same schema")
        else:
            print("❌ Schema differences detected")
            
            # Show detailed comparison
            print(f"\nDetailed schema comparison:")
            for freq, schema in schemas.items():
                print(f"  {freq}: {schema['columns'][:5]}... ({len(schema['columns'])} total)")
    
    return schemas


if __name__ == "__main__":
    print("🎯 PARQUET DATA COMBINATION PIPELINE")
    print("=" * 50)
    
    # First compare schemas
    schemas = compare_schemas()
    
    # Combine all data
    print(f"\n📥 COMBINING ALL PARQUET DATA")
    print("=" * 50)
    
    df = combine_all_parquet_data()
    
    if df is not None:
        print(f"\n🎉 Successfully combined all parquet data!")
        print(f"Final dataset: {df.shape[0]:,} records × {df.shape[1]} columns")
        
        # Additional analysis
        print(f"\n📊 QUICK DATA ANALYSIS")
        print("=" * 30)
        
        # Unique providers and sites
        if 'provider' in df.columns and 'site' in df.columns:
            unique_providers = df['provider'].nunique()
            unique_sites = df['site'].nunique()
            unique_provider_site_combos = df[['provider', 'site']].drop_duplicates().shape[0]
            print(f"Unique providers: {unique_providers}")
            print(f"Unique sites: {unique_sites}")
            print(f"Unique provider|site combinations: {unique_provider_site_combos}")
        
        # Date range
        if 'timeBox_start_date' in df.columns:
            start_dates = pd.to_datetime(df['timeBox_start_date'], format='%Y%m%d', errors='coerce')
            date_range = f"{start_dates.min().strftime('%Y-%m-%d')} to {start_dates.max().strftime('%Y-%m-%d')}"
            print(f"Date range: {date_range}")
        
        # Row count distribution
        if 'row_count' in df.columns:
            total_original_records = df['row_count'].sum()
            print(f"Total original records represented: {total_original_records:,}")
            print(f"Data compression ratio: {total_original_records / len(df):.1f}x") 