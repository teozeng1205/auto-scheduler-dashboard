#!/usr/bin/env python3
# ---------------------------------------------------------------------------
# Bulk-download parquet files from S3 and analyze their structure
# ---------------------------------------------------------------------------

import os
import boto3
import pandas as pd
import re
from pathlib import Path


def get_s3_client():
    """Initialize S3 client with credentials from environment"""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    )


def explore_s3_structure(prefix: str, bucket: str) -> dict:
    """
    Explore the S3 structure under the given prefix to understand folder organization.
    
    Parameters
    ----------
    prefix : str
        The S3 key prefix to explore
    bucket : str
        Name of the S3 bucket
        
    Returns
    -------
    dict
        Dictionary containing structure information
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    
    folders = set()
    files = []
    
    print(f"🔍 Exploring S3 structure under s3://{bucket}/{prefix}")
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            
            # Remove the prefix to get relative path
            rel_path = key[len(prefix):] if key.startswith(prefix) else key
            
            # Check if it's a file or folder
            if rel_path.endswith('/'):
                folders.add(rel_path)
            elif rel_path:  # Not empty (root folder)
                files.append({
                    'key': key,
                    'rel_path': rel_path,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
                
                # Extract folder from file path
                folder_path = '/'.join(rel_path.split('/')[:-1])
                if folder_path:
                    folders.add(folder_path + '/')
    
    structure = {
        'folders': sorted(list(folders)),
        'files': files,
        'total_files': len(files),
        'total_size': sum(f['size'] for f in files)
    }
    
    return structure


def download_repository(prefix: str,
                        bucket: str = "s3-atp-3victors-3vdev-use1-pe-as-parquet-temp",
                        local_root: str = "./s3_parquet_repo") -> list[str]:
    """
    Download every parquet file that lives under `bucket/prefix/*` to `local_root`.

    Parameters
    ----------
    prefix : str
        The S3 key prefix representing the "repository" you want to clone locally
        (e.g. 'parquet-69-temp/').
    bucket : str
        Name of the S3 bucket.
    local_root : str
        Local directory where the tree will be recreated.

    Returns
    -------
    list[str]
        Absolute paths of all files successfully downloaded.
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    downloaded: list[str] = []

    # Make sure the target directory exists
    os.makedirs(local_root, exist_ok=True)

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            
            # Skip if it's a folder (ends with /)
            if key.endswith('/'):
                continue

            # Strip the prefix so we can re-build the path locally
            rel_path = key[len(prefix):] if key.startswith(prefix) else key
            local_path = os.path.join(local_root, rel_path)

            # Create parent dirs as necessary
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            try:
                s3.download_file(bucket, key, local_path)
                print(f"✓ {key}  →  {local_path}")
                downloaded.append(os.path.abspath(local_path))
            except Exception as e:
                # Handle/collect errors as needed
                print(f"✗ Failed to download {key}: {e}")

    if not downloaded:
        print("⚠️  No objects matched the given prefix.")

    return downloaded


def analyze_parquet_structure(parquet_files: list[str]) -> dict:
    """
    Analyze parquet files and extract their structure and metadata.
    
    Parameters
    ----------
    parquet_files : list[str]
        List of paths to parquet files
        
    Returns
    -------
    dict
        Dictionary containing analysis results
    """
    analysis = {
        'files': [],
        'total_files': len(parquet_files),
        'schemas': {},
        'row_counts': {},
        'folder_structure': {}
    }
    
    for parquet_file in parquet_files:
        print(f"📊 Analyzing {os.path.basename(parquet_file)}...")
        
        try:
            # Read parquet file metadata
            df = pd.read_parquet(parquet_file)
            
            # Extract folder structure
            rel_path = os.path.relpath(parquet_file, start="./s3_parquet_repo")
            folder = os.path.dirname(rel_path)
            
            if folder not in analysis['folder_structure']:
                analysis['folder_structure'][folder] = []
            analysis['folder_structure'][folder].append(os.path.basename(parquet_file))
            
            file_info = {
                'file_path': parquet_file,
                'relative_path': rel_path,
                'folder': folder,
                'filename': os.path.basename(parquet_file),
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': list(df.columns),
                'dtypes': df.dtypes.to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'sample_data': df.head(3).to_dict() if len(df) > 0 else {}
            }
            
            analysis['files'].append(file_info)
            
            # Track schemas
            schema_key = str(sorted(df.columns))
            if schema_key not in analysis['schemas']:
                analysis['schemas'][schema_key] = {
                    'columns': list(df.columns),
                    'dtypes': df.dtypes.to_dict(),
                    'files': []
                }
            analysis['schemas'][schema_key]['files'].append(rel_path)
            
            # Track row counts by folder
            if folder not in analysis['row_counts']:
                analysis['row_counts'][folder] = 0
            analysis['row_counts'][folder] += len(df)
            
            print(f"  ✓ {len(df):,} rows × {len(df.columns)} columns")
            
        except Exception as e:
            print(f"  ✗ Failed to analyze {parquet_file}: {e}")
    
    return analysis


def print_analysis_report(analysis: dict):
    """Print a comprehensive analysis report"""
    print(f"\n🎯 PARQUET FILES ANALYSIS REPORT")
    print(f"=" * 50)
    
    print(f"\n📊 OVERVIEW")
    print(f"Total files analyzed: {analysis['total_files']}")
    print(f"Total folders: {len(analysis['folder_structure'])}")
    print(f"Unique schemas: {len(analysis['schemas'])}")
    
    print(f"\n📁 FOLDER STRUCTURE")
    for folder, files in analysis['folder_structure'].items():
        folder_name = folder if folder else "root"
        total_rows = analysis['row_counts'].get(folder, 0)
        print(f"  {folder_name}: {len(files)} files, {total_rows:,} total rows")
        for file in sorted(files)[:5]:  # Show first 5 files
            print(f"    - {file}")
        if len(files) > 5:
            print(f"    ... and {len(files) - 5} more files")
    
    print(f"\n🗂️  SCHEMA ANALYSIS")
    for i, (schema_key, schema_info) in enumerate(analysis['schemas'].items(), 1):
        print(f"  Schema {i}: {len(schema_info['files'])} files")
        print(f"    Columns ({len(schema_info['columns'])}): {', '.join(schema_info['columns'][:10])}")
        if len(schema_info['columns']) > 10:
            print(f"    ... and {len(schema_info['columns']) - 10} more columns")
        print(f"    Files: {', '.join(schema_info['files'][:3])}")
        if len(schema_info['files']) > 3:
            print(f"    ... and {len(schema_info['files']) - 3} more files")
    
    print(f"\n📈 ROW COUNT DISTRIBUTION")
    for folder, count in sorted(analysis['row_counts'].items()):
        folder_name = folder if folder else "root"
        print(f"  {folder_name}: {count:,} rows")
    
    # Show sample data from first file
    if analysis['files']:
        first_file = analysis['files'][0]
        print(f"\n📋 SAMPLE DATA from {first_file['filename']}")
        if first_file['sample_data']:
            df_sample = pd.DataFrame(first_file['sample_data'])
            print(df_sample.to_string(max_cols=10))


# ---------------------------------------------------------------------------
# Usage example
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # First explore the structure
    print("🔍 EXPLORING S3 PARQUET STRUCTURE")
    print("=" * 50)
    
    bucket = "s3-atp-3victors-3vdev-use1-pe-as-parquet-temp"
    prefix = "parquet-69-temp/"
    
    structure = explore_s3_structure(prefix, bucket)
    
    print(f"\n📁 Found {len(structure['folders'])} folders:")
    for folder in structure['folders'][:10]:  # Show first 10
        print(f"  {folder}")
    if len(structure['folders']) > 10:
        print(f"  ... and {len(structure['folders']) - 10} more folders")
    
    print(f"\n📄 Found {structure['total_files']} files")
    print(f"Total size: {structure['total_size'] / 1024**2:.1f} MB")
    
    # Show sample files
    print(f"\n📋 Sample files:")
    for file_info in structure['files'][:10]:  # Show first 10
        size_mb = file_info['size'] / 1024**2
        print(f"  {file_info['rel_path']} ({size_mb:.1f} MB)")
    
    # Download all parquet files
    print(f"\n📥 DOWNLOADING PARQUET FILES")
    print("=" * 50)
    
    downloaded_files = download_repository(
        prefix=prefix,
        bucket=bucket,
        local_root="./s3_parquet_repo"
    )
    
    if downloaded_files:
        print(f"\n📊 ANALYZING PARQUET STRUCTURE")
        print("=" * 50)
        
        # Analyze parquet files
        analysis = analyze_parquet_structure(downloaded_files)
        
        # Print comprehensive report
        print_analysis_report(analysis)
        
        # Save analysis to file
        import json
        with open('parquet_analysis.json', 'w') as f:
            # Convert datetime objects to strings for JSON serialization
            analysis_copy = analysis.copy()
            for file_info in analysis_copy['files']:
                if 'sample_data' in file_info:
                    # Convert any datetime objects in sample data
                    sample_data = file_info['sample_data']
                    for col, values in sample_data.items():
                        if isinstance(values, dict):
                            for idx, val in values.items():
                                if hasattr(val, 'isoformat'):  # datetime object
                                    sample_data[col][idx] = val.isoformat()
                # Convert dtypes to string
                if 'dtypes' in file_info:
                    file_info['dtypes'] = {k: str(v) for k, v in file_info['dtypes'].items()}
            
            for schema_info in analysis_copy['schemas'].values():
                if 'dtypes' in schema_info:
                    schema_info['dtypes'] = {k: str(v) for k, v in schema_info['dtypes'].items()}
            
            json.dump(analysis_copy, f, indent=2, default=str)
        
        print(f"\n💾 Analysis saved to: parquet_analysis.json")
        print(f"\n🎉 Structure analysis complete!") 