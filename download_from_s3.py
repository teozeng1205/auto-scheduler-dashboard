# ---------------------------------------------------------------------------
# Bulk-download everything under an S3 "repository" (prefix) to a local folder
# ---------------------------------------------------------------------------

import os
import boto3
import gzip
import json
import re


def get_s3_client():
    """Initialize S3 client with credentials from environment"""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN')
    )

def download_repository(prefix: str,
                        bucket: str = "s3-atp-3victors-3vdev-use1-pe-as-persistence",
                        local_root: str = "./s3_repo") -> list[str]:
    """
    Download every object that lives under `bucket/prefix/*` to `local_root`.

    Parameters
    ----------
    prefix : str
        The S3 key prefix representing the "repository" you want to clone locally
        (e.g. 'v1/10/').
    bucket : str
        Name of the S3 bucket.  Defaults to the bucket in the question.
    local_root : str
        Local directory where the tree will be recreated.

    Returns
    -------
    list[str]
        Absolute paths of all files successfully downloaded.
    """
    s3 = get_s3_client()        # reuse helper from lambda_main.py
    paginator = s3.get_paginator("list_objects_v2")

    downloaded: list[str] = []

    # Make sure the target directory exists
    os.makedirs(local_root, exist_ok=True)

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

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

def decompress_and_extract_metadata(gzipped_files: list[str]) -> list[dict]:
    """
    Decompress .json.gz files and extract metadata from filenames.
    
    Parameters
    ----------
    gzipped_files : list[str]
        List of paths to .json.gz files
        
    Returns
    -------
    list[dict]
        List of dictionaries containing file info and metadata
    """
    decompressed_files = []
    
    for gz_file in gzipped_files:
        if not gz_file.endswith('.json.gz'):
            continue
            
        # Extract metadata from filename
        filename = os.path.basename(gz_file)
        # Pattern: collection_frequency-id.json.gz (e.g., adhoc-438.json.gz, daily-429.json.gz)
        match = re.match(r'([a-zA-Z]+)-(\d+)\.json\.gz$', filename)
        
        if not match:
            print(f"⚠️  Skipping {filename} - doesn't match expected pattern")
            continue
            
        collection_frequency = match.group(1)
        hourly_collection_plan_id = int(match.group(2))
        
        # Decompress file
        json_file = gz_file.replace('.gz', '')
        
        try:
            with gzip.open(gz_file, 'rt', encoding='utf-8') as f_in:
                with open(json_file, 'w', encoding='utf-8') as f_out:
                    f_out.write(f_in.read())
            
            print(f"✓ Decompressed {filename} → {os.path.basename(json_file)}")
            
            decompressed_files.append({
                'gz_file': gz_file,
                'json_file': json_file,
                'collection_frequency': collection_frequency,
                'hourly_collection_plan_id': hourly_collection_plan_id,
                'filename': filename
            })
            
        except Exception as e:
            print(f"✗ Failed to decompress {filename}: {e}")
    
    return decompressed_files


# ---------------------------------------------------------------------------
# Usage example
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Pull everything in s3://<bucket>/v1/10/ down to ./s3_repo
    downloaded_files = download_repository(prefix="v1/10/",
                        bucket="s3-atp-3victors-3vdev-use1-pe-as-persistence",
                        local_root="./s3_repo")
    
    # Decompress all .json.gz files
    print(f"\n📦 Decompressing downloaded files...")
    decompressed_info = decompress_and_extract_metadata(downloaded_files)
    
    print(f"\n📊 Summary:")
    print(f"  Downloaded: {len(downloaded_files)} files")
    print(f"  Decompressed: {len(decompressed_info)} JSON files")
    
    # Group by collection frequency
    frequency_groups = {}
    for info in decompressed_info:
        freq = info['collection_frequency']
        if freq not in frequency_groups:
            frequency_groups[freq] = []
        frequency_groups[freq].append(info)
    
    for freq, files in frequency_groups.items():
        print(f"  {freq}: {len(files)} files")