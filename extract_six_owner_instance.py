#!/usr/bin/env python3
"""
Script to extract and print one instance of the 6-owner scheduling records.
"""

import json
import pprint

def extract_six_owner_instance():
    """Extract and print one instance of the 6-owner record."""
    
    # We know from our analysis that daily-486.json has a 6-owner record at index 76356
    filename = "s3_repo/daily-486.json"
    target_index = 76356  # 0-based index for the record we want
    
    print(f"Extracting 6-owner instance from {filename}")
    print("=" * 60)
    
    try:
        with open(filename, 'r') as f:
            # Parse the entire JSON array
            data = json.load(f)
            
        print(f"Total records in file: {len(data)}")
        
        if target_index >= len(data):
            print(f"Error: Index {target_index} is out of range (file has {len(data)} records)")
            return None
            
        record = data[target_index]
        
        print(f"Record at index {target_index}:")
        print("-" * 40)
        
        # Pretty print the record
        pprint.pprint(record, indent=2, width=100)
        
        print("-" * 40)
        print("Key Information:")
        print(f"  Number of requestOwners: {len(record.get('requestOwners', []))}")
        print(f"  Timebox end time: {record.get('timeBox', {}).get('endTime', {}).get('time', 'N/A')}")
        print(f"  Input request ID: {record.get('request', {}).get('input_request_id', 'N/A')}")
        
        if 'requestOwners' in record:
            print("  Request Owners:")
            for i, owner in enumerate(record['requestOwners']):
                customer = owner.get('customerCollection', {}).get('customer', 'Unknown')
                input_id = owner.get('input', {}).get('id', 'Unknown')
                print(f"    [{i}]: Customer={customer}, Input ID={input_id}")
                
        return record
        
    except FileNotFoundError:
        print(f"Error: {filename} not found. Make sure you've run the decompression script first.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def extract_from_alternative_file():
    """Extract the same record from daily-496.json for comparison."""
    
    filename = "s3_repo/daily-496.json"
    target_index = 76355  # 0-based index for the record we want
    
    print(f"\nAlternative extraction from {filename}")
    print("=" * 60)
    
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            
        if target_index >= len(data):
            print(f"Error: Index {target_index} is out of range (file has {len(data)} records)")
            return None
            
        record = data[target_index]
        
        print(f"Record at index {target_index}:")
        print(f"  Number of requestOwners: {len(record.get('requestOwners', []))}")
        print(f"  Timebox end time: {record.get('timeBox', {}).get('endTime', {}).get('time', 'N/A')}")
        print(f"  Input request ID: {record.get('request', {}).get('input_request_id', 'N/A')}")
        
        return record
        
    except Exception as e:
        print(f"Error with alternative file: {e}")
        return None

def find_six_owner_records():
    """Search for records with 6 owners to verify our indices."""
    
    filename = "s3_repo/daily-486.json"
    
    print(f"\nSearching for 6-owner records in {filename}")
    print("=" * 60)
    
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            
        six_owner_records = []
        for i, record in enumerate(data):
            if len(record.get('requestOwners', [])) == 6:
                six_owner_records.append((i, record))
                
        print(f"Found {len(six_owner_records)} records with 6 owners")
        
        for i, (index, record) in enumerate(six_owner_records):
            end_time = record.get('timeBox', {}).get('endTime', {}).get('time', 'N/A')
            input_request_id = record.get('request', {}).get('input_request_id', 'N/A')
            print(f"  Record {i+1}: Index {index}, End time {end_time}, Input request ID {input_request_id}")
            
        return six_owner_records
        
    except Exception as e:
        print(f"Error searching for 6-owner records: {e}")
        return []

if __name__ == "__main__":
    # First, search for 6-owner records to verify our indices
    six_owner_records = find_six_owner_records()
    
    if six_owner_records:
        # Extract the first 6-owner record found
        first_record_index = six_owner_records[0][0]
        print(f"\nExtracting first 6-owner record found at index {first_record_index}")
        
        # Update the extraction function to use the correct index
        filename = "s3_repo/daily-486.json"
        with open(filename, 'r') as f:
            data = json.load(f)
        record = data[first_record_index]
        
        print("=" * 60)
        print(f"Record at index {first_record_index}:")
        print("-" * 40)
        pprint.pprint(record, indent=2, width=100)
        
        print("-" * 40)
        print("Key Information:")
        print(f"  Number of requestOwners: {len(record.get('requestOwners', []))}")
        print(f"  Timebox end time: {record.get('timeBox', {}).get('endTime', {}).get('time', 'N/A')}")
        print(f"  Input request ID: {record.get('request', {}).get('input_request_id', 'N/A')}")
        
        if 'requestOwners' in record:
            print("  Request Owners:")
            for i, owner in enumerate(record['requestOwners']):
                customer = owner.get('customerCollection', {}).get('customer', 'Unknown')
                input_id = owner.get('input', {}).get('id', 'Unknown')
                print(f"    [{i}]: Customer={customer}, Input ID={input_id}")
    else:
        print("No 6-owner records found in the file!") 