#!/usr/bin/env python3
"""
Decompress existing .json.gz files in the s3_repo directory.
"""

import os
import gzip
import re
import glob


def decompress_file(gz_file_path):
    """Decompress a single .json.gz file"""
    json_file_path = gz_file_path.replace('.gz', '')
    
    try:
        with gzip.open(gz_file_path, 'rt', encoding='utf-8') as f_in:
            with open(json_file_path, 'w', encoding='utf-8') as f_out:
                f_out.write(f_in.read())
        
        filename = os.path.basename(gz_file_path)
        json_filename = os.path.basename(json_file_path)
        print(f"✓ Decompressed {filename} → {json_filename}")
        return json_file_path
        
    except Exception as e:
        print(f"✗ Failed to decompress {os.path.basename(gz_file_path)}: {e}")
        return None


def decompress_all_files(source_dir="./s3_repo"):
    """Decompress all .json.gz files in the specified directory"""
    
    # Find all .json.gz files
    gz_files = glob.glob(os.path.join(source_dir, "*.json.gz"))
    
    if not gz_files:
        print(f"No .json.gz files found in {source_dir}")
        return []
    
    print(f"Found {len(gz_files)} .json.gz files to decompress")
    
    decompressed_files = []
    file_stats = {}
    
    # Decompress each file
    for gz_file in sorted(gz_files):
        json_file = decompress_file(gz_file)
        if json_file:
            decompressed_files.append(json_file)
            
            # Extract metadata for stats
            filename = os.path.basename(gz_file)
            match = re.match(r'([a-zA-Z]+)-(\d+)\.json\.gz$', filename)
            if match:
                collection_frequency = match.group(1)
                if collection_frequency not in file_stats:
                    file_stats[collection_frequency] = 0
                file_stats[collection_frequency] += 1
    
    print(f"\n📊 Summary:")
    print(f"  Successfully decompressed: {len(decompressed_files)} files")
    
    if file_stats:
        print(f"  By collection frequency:")
        for freq in sorted(file_stats.keys()):
            print(f"    {freq}: {file_stats[freq]} files")
    
    return decompressed_files


if __name__ == "__main__":
    decompressed_files = decompress_all_files()
    print(f"\n🎉 Decompression complete! Ready to combine data.") 