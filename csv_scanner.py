"""
CSV Scanner Module
Efficiently scans CSV files and extracts unique values from specified columns.
Uses Python's built-in csv module for memory-efficient processing.

Configuration:
    The default CSV folder path is configured in config.py:
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
"""

import csv
import os
from pathlib import Path
from typing import Set, Generator, Optional, Dict, Tuple
from collections import defaultdict

# Import configuration from centralized config module
try:
    from config import (
        FOLDER_PATH,
        OUTPUT_REPORT_FOLDER
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"


def find_csv_files(folder_path: str) -> Generator[str, None, None]:
    """
    Recursively find all CSV files in the given folder.
    Files are yielded in sorted order for consistent, reproducible results.
    
    Args:
        folder_path: Path to the folder to scan
        
    Yields:
        Absolute paths to CSV files in sorted order
    """
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        # Sort directories for consistent traversal order
        dirs.sort()
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    # Yield files in sorted order
    for csv_file in sorted(csv_files):
        yield csv_file


def extract_unique_values_from_column(
    file_path: str, 
    column_index: int = 1,
    encoding: str = 'utf-8'
) -> Set[str]:
    """
    Extract all unique values from a specific column in a CSV file.
    Memory-efficient: processes file line by line without loading entire file.
    
    Args:
        file_path: Path to the CSV file
        column_index: Zero-based index of the column (default: 1 for second column)
        encoding: File encoding (default: utf-8)
        
    Returns:
        Set of unique string values from the specified column
    """
    unique_values = set()
    
    with open(file_path, 'r', encoding=encoding, newline='') as csvfile:
        reader = csv.reader(csvfile)
        
        # Skip header row
        try:
            next(reader)
        except StopIteration:
            # Empty file
            return unique_values
        
        # Process each row
        for row in reader:
            if len(row) > column_index:
                value = row[column_index].strip()
                if value:  # Only add non-empty values
                    unique_values.add(value)
    
    return unique_values


def scan_csv_files_for_unique_values(
    folder_path: str,
    column_name: str = "Type of mobile",
    column_index: int = 1
) -> Set[str]:
    """
    Scan all CSV files in a folder and collect unique values from a specific column.
    
    Args:
        folder_path: Path to the folder containing CSV files
        column_name: Name of the column to extract (for display purposes)
        column_index: Zero-based index of the column (default: 1 for second column)
        
    Returns:
        Set of all unique values found across all CSV files
    """
    all_unique_values = set()
    files_processed = 0
    
    print(f"Scanning folder: {folder_path}")
    print(f"Looking for column: '{column_name}' (index: {column_index})")
    print("-" * 60)
    
    for csv_file in find_csv_files(folder_path):
        try:
            print(f"Processing: {csv_file}")
            values = extract_unique_values_from_column(csv_file, column_index)
            all_unique_values.update(values)
            files_processed += 1
            print(f"  Found {len(values)} unique values in this file")
        except Exception as e:
            print(f"  Error processing file: {e}")
    
    print("-" * 60)
    print(f"Files processed: {files_processed}")
    
    return all_unique_values


def scan_csv_files_with_counts(
    folder_path: str,
    column_name: str = "Type of mobile",
    column_index: int = 1,
    mmsi_column_index: int = 2,
    imo_column_index: int = 10,
    encoding: str = 'utf-8'
) -> Tuple[Dict[str, Dict[str, any]], int]:
    """
    Scan all CSV files in a folder and count occurrences of each unique value.
    Also tracks unique MMSI and IMO counts for each mobile type.
    Memory-efficient: processes files line by line.
    
    Args:
        folder_path: Path to the folder containing CSV files
        column_name: Name of the column to extract (for display purposes)
        column_index: Zero-based index of the column (default: 1 for second column)
        mmsi_column_index: Zero-based index of MMSI column (default: 2)
        imo_column_index: Zero-based index of IMO column (default: 10)
        encoding: File encoding (default: utf-8)
        
    Returns:
        Tuple of (value_counts_dict, files_processed_count)
        value_counts_dict structure: {
            'mobile_type': {
                'count': int,           # Total row count
                'unique_mmsi': set,     # Set of unique MMSI values
                'unique_imo': set       # Set of unique IMO values
            }
        }
    """
    # Structure: {mobile_type: {'count': int, 'unique_mmsi': set, 'unique_imo': set}}
    value_data: Dict[str, Dict[str, any]] = defaultdict(lambda: {
        'count': 0,
        'unique_mmsi': set(),
        'unique_imo': set()
    })
    files_processed = 0
    
    print(f"Scanning folder: {folder_path}")
    print(f"Looking for column: '{column_name}' (index: {column_index})")
    print(f"Tracking unique MMSI (index: {mmsi_column_index}) and IMO (index: {imo_column_index})")
    print("-" * 60)
    
    for csv_file in find_csv_files(folder_path):
        try:
            print(f"Processing: {csv_file}")
            file_row_count = 0
            
            with open(csv_file, 'r', encoding=encoding, newline='') as f:
                reader = csv.reader(f)
                
                # Skip header row
                try:
                    next(reader)
                except StopIteration:
                    continue
                
                # Process each row
                for row in reader:
                    if len(row) > column_index:
                        value = row[column_index].strip()
                        if value:  # Only count non-empty values
                            value_data[value]['count'] += 1
                            file_row_count += 1
                            
                            # Extract MMSI if column exists
                            if len(row) > mmsi_column_index:
                                mmsi = row[mmsi_column_index].strip()
                                if mmsi and mmsi.isdigit():
                                    value_data[value]['unique_mmsi'].add(mmsi)
                            
                            # Extract IMO if column exists
                            if len(row) > imo_column_index:
                                imo = row[imo_column_index].strip()
                                if imo and imo.isdigit():
                                    value_data[value]['unique_imo'].add(imo)
            
            files_processed += 1
            print(f"  Processed {file_row_count} rows from this file")
            
        except Exception as e:
            print(f"  Error processing file: {e}")
    
    print("-" * 60)
    print(f"Files processed: {files_processed}")
    
    return dict(value_data), files_processed


def save_mobile_type_summary(
    value_data: Dict[str, Dict[str, any]],
    output_folder: str,
    filename: str = "master_list___Mobile_by_Type_summary.csv"
) -> str:
    """
    Save mobile type summary with counts and unique MMSI/IMO to a CSV file.
    
    Args:
        value_data: Dictionary with structure:
            {
                'mobile_type': {
                    'count': int,
                    'unique_mmsi': set,
                    'unique_imo': set
                }
            }
        output_folder: Path to the output folder (e.g., Reports Folder)
        filename: Name of the output file
        
    Returns:
        Absolute path to the created file
    """
    # Ensure output folder exists
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    file_path = output_path / filename
    
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow(['Mobile Type', 'Count', 'Unique MMSI', 'Unique IMO'])
        
        # Sort by count (descending) and write data
        sorted_items = sorted(
            value_data.items(), 
            key=lambda x: x[1]['count'], 
            reverse=True
        )
        for mobile_type, data in sorted_items:
            writer.writerow([
                mobile_type,
                data['count'],
                len(data['unique_mmsi']),
                len(data['unique_imo'])
            ])
    
    return str(file_path.absolute())


def main():
    """Main function to demonstrate CSV scanning functionality."""
    data_dir = FOLDER_PATH
    
    if not os.path.exists(data_dir):
        print(f"Error: Data directory not found: {data_dir}")
        print("Please verify the PRIMARY_FOLDER and FOLDER_PATH configuration.")
        return
    
    # Scan for unique values in "Type of mobile" column (second column, index 1)
    unique_values = scan_csv_files_for_unique_values(
        folder_path=data_dir,
        column_name="Type of mobile",
        column_index=1
    )
    
    # Print results
    print("\n" + "=" * 60)
    print(f"UNIQUE VALUES FROM 'Type of mobile' COLUMN")
    print("=" * 60)
    print(f"Total unique values found: {len(unique_values)}\n")
    
    if unique_values:
        for i, value in enumerate(sorted(unique_values), 1):
            print(f"{i}. {value}")
    else:
        print("No values found. Please ensure CSV files exist in the data directory.")
    
    print("=" * 60)


if __name__ == "__main__":
    main()
