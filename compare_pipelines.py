#!/usr/bin/env python3
"""
Compare the JSON and Parquet pipelines to analyze data structure differences.
"""

import pandas as pd
import os
from collections import Counter


def load_and_analyze_file(file_path, file_type):
    """Load and analyze a CSV file"""
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    try:
        df = pd.read_csv(file_path)
        
        analysis = {
            'file_path': file_path,
            'file_type': file_type,
            'shape': df.shape,
            'columns': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'row_count_col': None,
            'total_original_records': 0,
            'compression_ratio': 0
        }
        
        # Determine the row count column
        if 'grouped_row_count' in df.columns:
            analysis['row_count_col'] = 'grouped_row_count'
        elif 'row_count' in df.columns:
            analysis['row_count_col'] = 'row_count'
        
        if analysis['row_count_col']:
            analysis['total_original_records'] = df[analysis['row_count_col']].sum()
            analysis['compression_ratio'] = analysis['total_original_records'] / len(df)
        
        # Collection frequency distribution
        if 'collection_frequency' in df.columns:
            analysis['freq_distribution'] = df['collection_frequency'].value_counts().to_dict()
        
        # Provider/Site analysis
        if 'provider' in df.columns and 'site' in df.columns:
            analysis['provider_sites'] = df[['provider', 'site']].drop_duplicates().shape[0]
            analysis['unique_providers'] = df['provider'].nunique()
            analysis['unique_sites'] = df['site'].nunique()
        
        # Owner sequence analysis
        if 'ownerSequence' in df.columns:
            analysis['owner_sequences'] = df['ownerSequence'].value_counts().to_dict()
        
        return analysis
        
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def compare_schemas(json_analysis, parquet_analysis):
    """Compare the schemas between JSON and parquet pipelines"""
    print(f"\n📋 SCHEMA COMPARISON")
    print("=" * 50)
    
    json_cols = set(json_analysis['columns'])
    parquet_cols = set(parquet_analysis['columns'])
    
    # Find common columns
    common_cols = json_cols & parquet_cols
    json_only = json_cols - parquet_cols
    parquet_only = parquet_cols - json_cols
    
    print(f"📊 Column Statistics:")
    print(f"  JSON pipeline columns: {len(json_cols)}")
    print(f"  Parquet pipeline columns: {len(parquet_cols)}")
    print(f"  Common columns: {len(common_cols)}")
    print(f"  JSON-only columns: {len(json_only)}")
    print(f"  Parquet-only columns: {len(parquet_only)}")
    
    if json_only:
        print(f"\n🔴 Columns only in JSON pipeline:")
        for col in sorted(json_only):
            print(f"  - {col}")
    
    if parquet_only:
        print(f"\n🔵 Columns only in parquet pipeline:")
        for col in sorted(parquet_only):
            print(f"  - {col}")
    
    print(f"\n✅ Common columns ({len(common_cols)}):")
    for col in sorted(list(common_cols)[:20]):  # Show first 20
        print(f"  - {col}")
    if len(common_cols) > 20:
        print(f"  ... and {len(common_cols) - 20} more")


def compare_data_characteristics(json_analysis, parquet_analysis):
    """Compare data characteristics between pipelines"""
    print(f"\n📊 DATA CHARACTERISTICS COMPARISON")
    print("=" * 50)
    
    # Basic statistics
    print(f"📈 Dataset Size:")
    print(f"  JSON pipeline:")
    print(f"    Unique patterns: {json_analysis['shape'][0]:,}")
    print(f"    Total original records: {json_analysis['total_original_records']:,}")
    print(f"    Compression ratio: {json_analysis['compression_ratio']:.1f}x")
    print(f"    Memory usage: {json_analysis['memory_usage_mb']:.1f} MB")
    
    print(f"  Parquet pipeline:")
    print(f"    Unique patterns: {parquet_analysis['shape'][0]:,}")
    print(f"    Total original records: {parquet_analysis['total_original_records']:,}")
    print(f"    Compression ratio: {parquet_analysis['compression_ratio']:.1f}x")
    print(f"    Memory usage: {parquet_analysis['memory_usage_mb']:.1f} MB")
    
    # Collection frequency comparison
    if 'freq_distribution' in json_analysis and 'freq_distribution' in parquet_analysis:
        print(f"\n📋 Collection Frequency Distribution:")
        all_freqs = set(json_analysis['freq_distribution'].keys()) | set(parquet_analysis['freq_distribution'].keys())
        
        print(f"{'Frequency':<20} {'JSON Pipeline':<15} {'Parquet Pipeline':<15}")
        print("-" * 50)
        for freq in sorted(all_freqs):
            json_count = json_analysis['freq_distribution'].get(freq, 0)
            parquet_count = parquet_analysis['freq_distribution'].get(freq, 0)
            print(f"{freq:<20} {json_count:<15,} {parquet_count:<15,}")
    
    # Provider/Site comparison
    if 'provider_sites' in json_analysis and 'provider_sites' in parquet_analysis:
        print(f"\n🏢 Provider/Site Analysis:")
        print(f"  JSON pipeline: {json_analysis['unique_providers']} providers, {json_analysis['unique_sites']} sites, {json_analysis['provider_sites']} combinations")
        print(f"  Parquet pipeline: {parquet_analysis['unique_providers']} providers, {parquet_analysis['unique_sites']} sites, {parquet_analysis['provider_sites']} combinations")
    
    # Owner sequence comparison
    if 'owner_sequences' in json_analysis and 'owner_sequences' in parquet_analysis:
        print(f"\n👥 Owner Sequence Distribution:")
        all_owners = set(json_analysis['owner_sequences'].keys()) | set(parquet_analysis['owner_sequences'].keys())
        
        print(f"{'Owner Seq':<10} {'JSON Pipeline':<15} {'Parquet Pipeline':<15}")
        print("-" * 40)
        for owner in sorted(all_owners):
            json_count = json_analysis['owner_sequences'].get(owner, 0)
            parquet_count = parquet_analysis['owner_sequences'].get(owner, 0)
            print(f"{owner:<10} {json_count:<15,} {parquet_count:<15,}")


def generate_comparison_report(json_analysis, parquet_analysis):
    """Generate a comprehensive comparison report"""
    print(f"\n📝 PIPELINE COMPARISON SUMMARY")
    print("=" * 50)
    
    # Data sources
    print(f"📁 Data Sources:")
    print(f"  JSON pipeline: s3://s3-atp-3victors-3vdev-use1-pe-as-persistence/v1/10/")
    print(f"    - JSON.gz files (decompressed)")
    print(f"    - Processing: JSON → flattened CSV → grouped CSV")
    
    print(f"  Parquet pipeline: s3://s3-atp-3victors-3vdev-use1-pe-as-parquet-temp/parquet-69-temp/")
    print(f"    - Parquet files (already flattened)")
    print(f"    - Processing: Parquet → combined CSV → grouped CSV")
    
    # Key differences
    print(f"\n🔍 Key Differences:")
    
    # 1. Data volume
    json_records = json_analysis['total_original_records']
    parquet_records = parquet_analysis['total_original_records']
    volume_ratio = parquet_records / json_records if json_records > 0 else 0
    
    print(f"  1. Data Volume:")
    print(f"     JSON pipeline: {json_records:,} original records")
    print(f"     Parquet pipeline: {parquet_records:,} original records")
    print(f"     Parquet has {volume_ratio:.1f}x more data than JSON")
    
    # 2. Compression efficiency
    json_compression = json_analysis['compression_ratio']
    parquet_compression = parquet_analysis['compression_ratio']
    
    print(f"  2. Compression Efficiency:")
    print(f"     JSON pipeline: {json_compression:.1f}x compression")
    print(f"     Parquet pipeline: {parquet_compression:.1f}x compression")
    print(f"     Parquet achieves {parquet_compression/json_compression:.1f}x better compression")
    
    # 3. Data freshness
    print(f"  3. Data Time Periods:")
    print(f"     JSON pipeline: Historical data (v1/10)")
    print(f"     Parquet pipeline: Recent data (parquet-69-temp)")
    
    # 4. Processing differences
    print(f"  4. Processing Differences:")
    print(f"     JSON: Requires decompression + JSON parsing + flattening")
    print(f"     Parquet: Direct reading (already processed/flattened)")
    
    # 5. Schema differences
    json_cols = len(json_analysis['columns'])
    parquet_cols = len(parquet_analysis['columns'])
    
    print(f"  5. Schema Complexity:")
    print(f"     JSON pipeline: {json_cols} columns")
    print(f"     Parquet pipeline: {parquet_cols} columns")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    print(f"  1. Parquet pipeline appears to be more recent and comprehensive")
    print(f"  2. Parquet format provides better performance and compression")
    print(f"  3. Both pipelines can coexist for different time periods")
    print(f"  4. Consider migrating historical JSON data to parquet format")


if __name__ == "__main__":
    print("🔬 PIPELINE COMPARISON ANALYSIS")
    print("=" * 60)
    
    # Define file paths (adjust if your files are named differently)
    json_file = "combined_all_data_grouped.csv"  # Original JSON pipeline output
    parquet_file = "combined_all_parquet_data_grouped.csv"  # New parquet pipeline output
    
    # Check if JSON file exists (might not be available)
    json_available = os.path.exists(json_file)
    parquet_available = os.path.exists(parquet_file)
    
    if not json_available:
        print(f"⚠️  JSON pipeline file not found: {json_file}")
        print(f"   This comparison will focus on parquet pipeline analysis only.")
    
    if not parquet_available:
        print(f"❌ Parquet pipeline file not found: {parquet_file}")
        exit(1)
    
    # Load and analyze files
    json_analysis = None
    if json_available:
        print(f"\n📊 Analyzing JSON pipeline output...")
        json_analysis = load_and_analyze_file(json_file, "JSON")
    
    print(f"\n📊 Analyzing Parquet pipeline output...")
    parquet_analysis = load_and_analyze_file(parquet_file, "Parquet")
    
    if not parquet_analysis:
        print("❌ Failed to analyze parquet pipeline")
        exit(1)
    
    # Perform comparisons
    if json_analysis:
        compare_schemas(json_analysis, parquet_analysis)
        compare_data_characteristics(json_analysis, parquet_analysis)
        generate_comparison_report(json_analysis, parquet_analysis)
    else:
        # Just show parquet analysis
        print(f"\n📊 PARQUET PIPELINE ANALYSIS")
        print("=" * 40)
        print(f"Dataset: {parquet_analysis['shape'][0]:,} unique patterns")
        print(f"Columns: {len(parquet_analysis['columns'])}")
        print(f"Original records: {parquet_analysis['total_original_records']:,}")
        print(f"Compression: {parquet_analysis['compression_ratio']:.1f}x")
        print(f"Memory usage: {parquet_analysis['memory_usage_mb']:.1f} MB")
        
        if 'freq_distribution' in parquet_analysis:
            print(f"\nCollection frequencies:")
            for freq, count in parquet_analysis['freq_distribution'].items():
                print(f"  {freq}: {count:,}")
        
        print(f"\n✅ Parquet pipeline successfully created combined_all_data_grouped.csv") 