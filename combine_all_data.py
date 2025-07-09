#!/usr/bin/env python3
"""
Combine all JSON files into one comprehensive dataset.
This script processes all decompressed JSON files and creates a unified CSV.
"""

import json
import pandas as pd
import os
import re
import glob
from pathlib import Path


def extract_metadata_from_filename(filename):
    """Extract collection_frequency and hourly_collection_plan_id from filename"""
    # Pattern: collection_frequency-id.json (e.g., adhoc-438.json, daily-429.json)
    match = re.match(r'([a-zA-Z]+)-(\d+)\.json$', filename)
    
    if match:
        collection_frequency = match.group(1)
        hourly_collection_plan_id = int(match.group(2))
        return collection_frequency, hourly_collection_plan_id
    else:
        return None, None


def flatten_record(record, collection_frequency=None, hourly_collection_plan_id=None):
    """Convert each JSON record to flattened CSV rows - one row per owner"""
    base_flattened = {}
    
    # File metadata: collection_frequency, hourly_collection_plan_id
    base_flattened['collection_frequency'] = collection_frequency
    base_flattened['hourly_collection_plan_id'] = hourly_collection_plan_id
    
    # providerSiteCode: All columns (renamed for clarity)
    if 'providerSiteCode' in record:
        for key, value in record['providerSiteCode'].items():
            # Rename columns for better readability
            if key == 'x':
                base_flattened['provider'] = value
            elif key == 'y':
                base_flattened['site'] = value
            else:
                base_flattened[f'providerSiteCode_{key}'] = value
    
    # siteHierarchy: customer, customer site code, priority
    if 'siteHierarchy' in record:
        site_hier = record['siteHierarchy']
        base_flattened['siteHierarchy_customer'] = site_hier.get('customer')
        base_flattened['siteHierarchy_customerSiteCode'] = site_hier.get('customerSiteCode')  
        base_flattened['siteHierarchy_priority'] = site_hier.get('priority')
    
    # requests: no columns, just count (always 1 per record)
    base_flattened['requests_count'] = 1 if 'request' in record else 0
    
    # enrichment_request: no columns, just count (not found in data)
    base_flattened['enrichment_request_count'] = 0
    
    # timeBox: all columns (top-level)
    if 'timeBox' in record:
        tb = record['timeBox']
        for key, value in tb.items():
            if isinstance(value, dict):
                # Handle nested time objects  
                for subkey, subvalue in value.items():
                    base_flattened[f'timeBox_{key}_{subkey}'] = subvalue
            else:
                base_flattened[f'timeBox_{key}'] = value
    
    # Process requestOwners - create one row per owner
    result_rows = []
    
    if 'requestOwners' in record and record['requestOwners']:
        for owner_idx, owner in enumerate(record['requestOwners']):
            # Start with base data
            flattened = base_flattened.copy()
            
            # Add owner sequence (1-based)
            flattened['ownerSequence'] = owner_idx + 1
            
            # customerCollection: all columns (without suffix)
            if 'customerCollection' in owner:
                cc = owner['customerCollection']
                for key, value in cc.items():
                    flattened[f'customerCollection_{key}'] = value
            
            # input: only filename and reference (without suffix)
            if 'input' in owner:
                inp = owner['input']
                flattened['input_filename'] = inp.get('name')  # assuming name is filename
                flattened['input_reference'] = inp.get('reference', inp.get('id'))  # fallback to id if no reference
            
            # inputRequest: no columns (just track existence)
            flattened['inputRequest_exists'] = 1 if 'inputRequest' in owner else 0
            
            # timebox: all columns (without suffix)
            if 'timeBox' in owner:
                tb = owner['timeBox']
                for key, value in tb.items():
                    if isinstance(value, dict):
                        # Handle nested time objects
                        for subkey, subvalue in value.items():
                            flattened[f'timebox_{key}_{subkey}'] = subvalue
                    else:
                        flattened[f'timebox_{key}'] = value
            
            result_rows.append(flattened)
    else:
        # No requestOwners - create single row with ownerSequence = 1 and null owner data
        flattened = base_flattened.copy()
        flattened['ownerSequence'] = 1
        
        # Add null placeholders for owner-specific columns to maintain consistency
        flattened['customerCollection_id'] = None
        flattened['customerCollection_customer'] = None
        flattened['customerCollection_name'] = None
        flattened['customerCollection_frequency'] = None
        flattened['customerCollection_earliestStartTime'] = None
        flattened['customerCollection_expectedDeliveryTime'] = None
        flattened['customerCollection_hints'] = None
        flattened['customerCollection_status'] = None
        flattened['customerCollection_customerPackagingId'] = None
        flattened['input_filename'] = None
        flattened['input_reference'] = None
        flattened['inputRequest_exists'] = 0
        
        result_rows.append(flattened)
    
    return result_rows


def process_json_file(json_file_path):
    """Process a single JSON file and return flattened records"""
    filename = os.path.basename(json_file_path)
    collection_frequency, hourly_collection_plan_id = extract_metadata_from_filename(filename)
    
    if collection_frequency is None:
        print(f"⚠️  Warning: Could not extract metadata from {filename}")
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Process all records in this file
        csv_records = []
        original_record_count = len(data)
        
        for record in data:
            flattened_rows = flatten_record(record, collection_frequency, hourly_collection_plan_id)
            csv_records.extend(flattened_rows)
        
        print(f"✓ Processed {filename}: {original_record_count} original records -> {len(csv_records)} flattened rows")
        return csv_records, original_record_count
        
    except Exception as e:
        print(f"✗ Error processing {filename}: {e}")
        return [], 0


def combine_all_data(json_dir="./s3_repo", output_file="combined_all_data.csv"):
    """Combine all JSON files into one comprehensive dataset"""
    
    # Find all JSON files (not .gz files)
    json_files = glob.glob(os.path.join(json_dir, "*.json"))
    
    if not json_files:
        print(f"No JSON files found in {json_dir}")
        return
    
    print(f"Found {len(json_files)} JSON files to process")
    
    all_records = []
    file_stats = {}
    
    # Process each file
    for json_file in sorted(json_files):
        records, original_record_count = process_json_file(json_file)
        if records:
            all_records.extend(records)
            filename = os.path.basename(json_file)
            collection_frequency, plan_id = extract_metadata_from_filename(filename)
            
            if collection_frequency not in file_stats:
                file_stats[collection_frequency] = {'files': 0, 'original_records': 0, 'flattened_rows': 0}
            file_stats[collection_frequency]['files'] += 1
            file_stats[collection_frequency]['original_records'] += original_record_count
            file_stats[collection_frequency]['flattened_rows'] += len(records)
    
    if not all_records:
        print("No records to process")
        return
    
    # Convert to DataFrame
    print(f"\n📊 Creating DataFrame from {len(all_records)} total flattened rows...")
    df = pd.DataFrame(all_records)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    
    print(f"\n✅ Results saved to: {output_file}")
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {len(df.columns)}")
    
    # Print summary statistics
    print(f"\n📈 Summary by collection frequency:")
    for freq in sorted(file_stats.keys()):
        stats = file_stats[freq]
        expansion_ratio = stats['flattened_rows'] / stats['original_records'] if stats['original_records'] > 0 else 0
        print(f"  {freq}: {stats['files']} files")
        print(f"    Original records: {stats['original_records']:,}")
        print(f"    Flattened rows: {stats['flattened_rows']:,}")
        print(f"    Expansion ratio: {expansion_ratio:.2f}x")
    
    # Show data distribution by frequency
    if 'collection_frequency' in df.columns:
        print(f"\n📊 Flattened row distribution:")
        freq_counts = df['collection_frequency'].value_counts()
        for freq, count in freq_counts.items():
            print(f"  {freq}: {count:,} rows")
    
    # Show owner sequence distribution
    if 'ownerSequence' in df.columns:
        print(f"\n👥 Owner sequence distribution:")
        owner_counts = df['ownerSequence'].value_counts().sort_index()
        for seq, count in owner_counts.items():
            print(f"  Owner {seq}: {count:,} rows")
    
    # Show sample of columns
    print(f"\n📝 Sample columns:")
    for i, col in enumerate(sorted(df.columns)[:25]):  # Show first 25 columns
        non_null = df[col].notna().sum()
        print(f"  {col}: {non_null:,}/{len(df):,} non-null")
    
    if len(df.columns) > 25:
        print(f"  ... and {len(df.columns) - 25} more columns")
    
    return df


if __name__ == "__main__":
    # Combine all data
    df = combine_all_data()
    
    if df is not None:
        print(f"\n🎉 Successfully combined all data!")
        print(f"Final dataset: {df.shape[0]:,} flattened rows × {df.shape[1]} columns") 