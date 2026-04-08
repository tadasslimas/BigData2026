"""
Master Indexes Module
Analyzes CSV files to create master indexes of IMO and MMSI identifiers,
and detects anomalies in vessel identification data.

Uses multiprocessing for memory-efficient processing of large datasets.
"""

import csv
import concurrent.futures
import os
import psutil
from pathlib import Path
from typing import Dict, Set, Tuple, Generator, Optional

# Import centralized configuration
from config import (
    FOLDER_PATH as DEFAULT_FOLDER_PATH,
    CLEAN_DB_FOLDER_PATH,
    OUTPUT_REPORT_FOLDER as DEFAULT_OUTPUT_FOLDER,
    FILE_PATTERN,
    CHUNK_SIZE,
    MAX_WORKERS,
    BLACK_MMSI_LIST,
    BLACK_IMO_LIST,
    MMSI_COLUMN_INDEX,
    IMO_COLUMN_INDEX,
    FILE_ENCODING,
    MMSI_LENGTH,
    IMO_LENGTH
)


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def print_memory_usage():
    """Print detailed memory usage statistics for master and worker processes."""
    parent = psutil.Process(os.getpid())
    
    # Master process memory
    parent_mem = parent.memory_info().rss / 1024 / 1024
    
    # Worker processes memory
    children_mem = 0
    child_count = 0
    for child in parent.children(recursive=True):
        try:
            children_mem += child.memory_info().rss
            child_count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
    children_mem_mb = children_mem / 1024 / 1024
    total_mem_mb = parent_mem + children_mem_mb
    
    # System memory status
    sys_percent = psutil.virtual_memory().percent

    print("-" * 45)
    print(f"Master process memory usage: {parent_mem:>8.2f} MB, workers ({child_count} vnt.): {children_mem_mb:>8.2f} MB")
    print(f"Total memory in use: {total_mem_mb:>8.2f} MB, System RAM load: {sys_percent:>7.1f} %")


def sorted_file_generator(folder_path: Path, file_pattern: str, chunk_size: int) -> Generator[list, None, None]:
    """
    Stream sorted CSV files in chunks.
    
    If folder contains Clean_AIS_DB.csv, processes only that file.
    Otherwise processes all CSV files matching the pattern.
    
    Args:
        folder_path: Path to folder containing CSV files
        file_pattern: Glob pattern for matching files
        chunk_size: Number of rows per chunk
        
    Yields:
        Lists of CSV rows (chunks)
    """
    # Check if Clean_AIS_DB.csv exists in this folder
    clean_db_file = folder_path / "Clean_AIS_DB.csv"
    
    if clean_db_file.exists():
        # Process only the clean database file
        files = [clean_db_file]
        if file_pattern != "Clean_AIS_DB.csv":
            print(f"Using Clean_AIS_DB.csv as data source")
    else:
        # Process all CSV files matching pattern
        files = sorted(list(folder_path.glob(file_pattern)))
        if not files:
            print(f"No files found in {folder_path}")
            return

    for file_path in files:
        print(f"Reading file: {file_path.name}")
        with file_path.open('r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                next(reader)  # Skip header
            except StopIteration:
                continue
                
            chunk = []
            for i, row in enumerate(reader):
                chunk.append(row)
                if (i + 1) % chunk_size == 0:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk


# ==========================================
# DATA ANALYSIS LOGIC
# ==========================================

def analyze_data_chunk(data_chunk: list) -> list[Tuple[str, str]]:
    """
    Extract IMO-MMSI relationships from a data chunk.
    
    Args:
        data_chunk: List of CSV rows
        
    Returns:
        List of valid (imo, mmsi) pairs
    """
    valid_pairs = []
    for row in data_chunk:
        if len(row) > IMO_COLUMN_INDEX:
            mmsi = row[MMSI_COLUMN_INDEX].strip()
            imo = row[IMO_COLUMN_INDEX].strip()
            
            # Validation: length, numeric only, not in blacklists
            is_valid_mmsi = len(mmsi) == MMSI_LENGTH and mmsi.isdigit() and mmsi not in BLACK_MMSI_LIST
            is_valid_imo = len(imo) == IMO_LENGTH and imo.isdigit() and imo not in BLACK_IMO_LIST
            
            if is_valid_mmsi and is_valid_imo:
                valid_pairs.append((imo, mmsi))
                
    return valid_pairs


def create_master_indexes(
    folder_path: Optional[Path] = None,
    anomaly_folder_path: Optional[Path] = None,
    output_folder: Optional[Path] = None,
    max_workers: int = MAX_WORKERS,
    chunk_size: int = CHUNK_SIZE,
    verbose: bool = True
) -> Dict[str, any]:
    """
    Analyze CSV files and create master indexes of IMO and MMSI identifiers.
    
    Master indexes are created from Clean_AIS_DB (if available).
    Anomaly detection uses original data from Duomenys_CSV_Formate.
    
    Args:
        folder_path: Path to folder for master indexes (default: Clean_AIS_DB)
        anomaly_folder_path: Path to folder for anomaly detection (default: Duomenys_CSV_Formate)
        output_folder: Path to output folder for reports
        max_workers: Maximum number of worker processes
        chunk_size: Size of data chunks for processing
        verbose: Print progress and memory usage
        
    Returns:
        Dictionary containing:
            - 'imo_to_mmsis': Dict mapping IMO to set of MMSIs
            - 'mmsi_to_imos': Dict mapping MMSI to set of IMOs
            - 'output_files': List of generated output file paths
    """
    # Use Clean_AIS_DB.csv as the data source for master indexes
    if folder_path is None:
        clean_db_file = CLEAN_DB_FOLDER_PATH / "Clean_AIS_DB.csv"
        if clean_db_file.exists():
            folder_path = CLEAN_DB_FOLDER_PATH
        else:
            folder_path = DEFAULT_FOLDER_PATH
            if verbose:
                print(f"⚠️  Clean_AIS_DB.csv not found, using default folder: {folder_path}")
    
    # Use Duomenys_CSV_Formate for anomaly detection (original data)
    if anomaly_folder_path is None:
        anomaly_folder_path = DEFAULT_FOLDER_PATH
    
    if output_folder is None:
        output_folder = DEFAULT_OUTPUT_FOLDER
    
    # Create output directory if it doesn't exist
    output_folder.mkdir(parents=True, exist_ok=True)
    
    # Data structures for relationships
    imo_to_mmsis: Dict[str, Set[str]] = {}
    mmsi_to_imos: Dict[str, Set[str]] = {}
    
    if verbose:
        print(f"Creating master indexes from: {folder_path}")
        print(f"Detecting anomalies from: {anomaly_folder_path}")
        print_memory_usage()

    # PHASE 1: Create master indexes from Clean_AIS_DB
    if verbose:
        print("\n--- PHASE 1: Building Master Indexes ---")
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for i, chunk in enumerate(sorted_file_generator(folder_path, FILE_PATTERN, chunk_size)):
            future = executor.submit(analyze_data_chunk, chunk)
            futures.append(future)
            
            # Memory management: limit active tasks
            if len(futures) >= max_workers * 2:
                done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for d in done:
                    pairs = d.result()
                    for imo, mmsi in pairs:
                        if imo not in imo_to_mmsis:
                            imo_to_mmsis[imo] = set()
                        imo_to_mmsis[imo].add(mmsi)
                        
                        if mmsi not in mmsi_to_imos:
                            mmsi_to_imos[mmsi] = set()
                        mmsi_to_imos[mmsi].add(imo)
                    futures.remove(d)
                
                if i % 10 == 0 and verbose:
                    print_memory_usage()

        # Collect remaining results
        for d in concurrent.futures.as_completed(futures):
            pairs = d.result()
            for imo, mmsi in pairs:
                if imo not in imo_to_mmsis:
                    imo_to_mmsis[imo] = set()
                imo_to_mmsis[imo].add(mmsi)
                if mmsi not in mmsi_to_imos:
                    mmsi_to_imos[mmsi] = set()
                mmsi_to_imos[mmsi].add(imo)

    if verbose:
        print("\nMaster indexes complete. Detecting anomalies from original data...")

    # PHASE 2: Detect anomalies from original Duomenys_CSV_Formate data
    if verbose:
        print("\n--- PHASE 2: Anomaly Detection (Original Data) ---")
    
    # Temporary structures for anomaly detection from original data
    imo_to_mmsis_anomaly: Dict[str, Set[str]] = {}
    mmsi_to_imos_anomaly: Dict[str, Set[str]] = {}
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for i, chunk in enumerate(sorted_file_generator(anomaly_folder_path, FILE_PATTERN, chunk_size)):
            future = executor.submit(analyze_data_chunk, chunk)
            futures.append(future)
            
            if len(futures) >= max_workers * 2:
                done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for d in done:
                    pairs = d.result()
                    for imo, mmsi in pairs:
                        if imo not in imo_to_mmsis_anomaly:
                            imo_to_mmsis_anomaly[imo] = set()
                        imo_to_mmsis_anomaly[imo].add(mmsi)
                        
                        if mmsi not in mmsi_to_imos_anomaly:
                            mmsi_to_imos_anomaly[mmsi] = set()
                        mmsi_to_imos_anomaly[mmsi].add(imo)
                    futures.remove(d)

        # Collect remaining results for anomaly detection
        for d in concurrent.futures.as_completed(futures):
            pairs = d.result()
            for imo, mmsi in pairs:
                if imo not in imo_to_mmsis_anomaly:
                    imo_to_mmsis_anomaly[imo] = set()
                imo_to_mmsis_anomaly[imo].add(mmsi)
                if mmsi not in mmsi_to_imos_anomaly:
                    mmsi_to_imos_anomaly[mmsi] = set()
                mmsi_to_imos_anomaly[mmsi].add(imo)

    if verbose:
        print("\nCalculations complete. Generating reports...")

    # Generate output files
    output_files = []
    
    # 1. Master unique ID lists (from Clean_AIS_DB)
    # Extract all unique MMSI values
    all_unique_mmsi = set(mmsi_to_imos.keys())
    # Extract all unique IMO values
    all_unique_imo = set(imo_to_mmsis.keys())
    
    output_files.append(write_list_file(output_folder / 'master_MMSI_data.csv', 'MMSI', sorted(all_unique_mmsi)))
    output_files.append(write_list_file(output_folder / 'master_IMO_data.csv', 'IMO', sorted(all_unique_imo)))

    # 2. Anomaly: 1 IMO -> Multiple MMSIs (from original data)
    imo_anomalies = {k: v for k, v in imo_to_mmsis_anomaly.items() if len(v) > 1}
    output_files.append(write_anomaly_file(
        output_folder / 'IMO_with_multiple_MMSI.csv', 
        'IMO', 
        'MMSI_List', 
        imo_anomalies
    ))

    # 3. Anomaly: 1 MMSI -> Multiple IMOs (from original data)
    mmsi_anomalies = {k: v for k, v in mmsi_to_imos_anomaly.items() if len(v) > 1}
    output_files.append(write_anomaly_file(
        output_folder / 'MMSI_with_multiple_IMO.csv', 
        'MMSI', 
        'IMO_List', 
        mmsi_anomalies
    ))

    if verbose:
        print(f"Complete! Results saved to: {output_folder}")
        print_memory_usage()

    return {
        'imo_to_mmsis': imo_to_mmsis,
        'mmsi_to_imos': mmsi_to_imos,
        'output_files': output_files
    }


def write_list_file(file_path: Path, header: str, data: iter) -> str:
    """
    Write a list of unique values to a CSV file.
    
    Args:
        file_path: Output file path
        header: CSV column header
        data: Iterable of values to write
        
    Returns:
        Absolute path to the created file
    """
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([header])
        for value in sorted(data):
            writer.writerow([value])
    
    return str(file_path.absolute())


def write_anomaly_file(file_path: Path, key_name: str, list_name: str, data: dict) -> str:
    """
    Write anomaly data to a CSV file.
    
    Args:
        file_path: Output file path
        key_name: Name of the key column
        list_name: Name of the list column
        data: Dictionary mapping keys to sets of values
        
    Returns:
        Absolute path to the created file
    """
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([key_name, 'Count', list_name])
        # Sort by number of duplicates (descending)
        for key in sorted(data, key=lambda x: len(data[x]), reverse=True):
            items = sorted(list(data[key]))
            writer.writerow([key, len(items), "; ".join(items)])
    
    return str(file_path.absolute())
