"""
Clean AIS Database Creation Module

Creates a cleaned and filtered AIS database from raw CSV files.
Uses multiprocessing for high-performance processing and sliding window 
deduplication for minimal memory usage.

This module performs two-pass processing:
1. Statistical analysis to identify valid vessels (multiprocessing)
2. Data cleaning and deduplication with sliding window

Configuration:
    The default paths are configured in config.py or use fallback values:
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
"""

import csv
import multiprocessing
import os
from pathlib import Path
from typing import Dict, Set, Any

# Import configuration from centralized config module
try:
    from config import (
        PRIMARY_FOLDER,
        CLEAN_DB_FOLDER_PATH,
        FOLDER_PATH
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"

# Output configuration
CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
CLEAN_DB_FOLDER_PATH.mkdir(exist_ok=True, parents=True)
OUTPUT_CLEAN_DB_FILE = CLEAN_DB_FOLDER_PATH / "Clean_AIS_DB.csv"

# Filter thresholds
MIN_POS_PER_DAY = 25      # Minimum position reports per day
MIN_KM_MOVE = 5.0         # Minimum vessel movement in km
MIN_SPEED_KNOTS = 2.0     # Minimum speed threshold in knots


def analyze_single_file(file_path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Worker function: Collects statistics from a single file (Phase 1).
    
    Processes one CSV file and extracts statistical data for each MMSI:
    - Position count
    - Min/max latitude and longitude
    - Maximum speed (SOG)
    
    Args:
        file_path: Path to the CSV file to analyze
        
    Returns:
        Dictionary with MMSI as key and statistics as value
    """
    local_stats = {}
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            # Strip whitespace from field names
            reader.fieldnames = [field.strip() for field in reader.fieldnames]
            
            for row in reader:
                # Only Class A vessels
                if row.get('Type of mobile') != 'Class A':
                    continue
                
                mmsi = row.get('MMSI', '').strip()
                if len(mmsi) != 9 or not mmsi.isdigit():
                    continue
                
                try:
                    lat = float(row.get('Latitude', 91))
                    lon = float(row.get('Longitude', 181))
                    sog = float(row.get('SOG', 0))
                    
                    # Ignore rows with invalid coordinates
                    if abs(lat) > 89.9 or lat == 91.0 or abs(lon) > 179.9:
                        continue
                    
                    if mmsi not in local_stats:
                        local_stats[mmsi] = {
                            'cnt': 1,
                            'min_lat': lat,
                            'max_lat': lat,
                            'min_lon': lon,
                            'max_lon': lon,
                            'max_sog': sog
                        }
                    else:
                        s = local_stats[mmsi]
                        s['cnt'] += 1
                        if lat < s['min_lat']:
                            s['min_lat'] = lat
                        if lat > s['max_lat']:
                            s['max_lat'] = lat
                        if lon < s['min_lon']:
                            s['min_lon'] = lon
                        if lon > s['max_lon']:
                            s['max_lon'] = lon
                        if sog > s['max_sog']:
                            s['max_sog'] = sog
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        print(f"Error analyzing {file_path.name}: {e}")
        
    return local_stats


def merge_stats(global_stats: Dict, local_stats: Dict) -> None:
    """
    Merge worker results into the main dictionary.
    
    Combines statistics from multiple worker processes by:
    - Summing position counts
    - Taking min/max of coordinate ranges
    - Taking maximum speed
    
    Args:
        global_stats: Main statistics dictionary (modified in place)
        local_stats: Worker statistics dictionary to merge
    """
    for mmsi, s_local in local_stats.items():
        if mmsi not in global_stats:
            global_stats[mmsi] = s_local
        else:
            s_global = global_stats[mmsi]
            s_global['cnt'] += s_local['cnt']
            s_global['min_lat'] = min(s_global['min_lat'], s_local['min_lat'])
            s_global['max_lat'] = max(s_global['max_lat'], s_local['max_lat'])
            s_global['min_lon'] = min(s_global['min_lon'], s_local['min_lon'])
            s_global['max_lon'] = max(s_global['max_lon'], s_local['max_lon'])
            s_global['max_sog'] = max(s_global['max_sog'], s_local['max_sog'])


def create_clean_ais_database(
    input_folder: Path = None,
    output_file: Path = None,
    verbose: bool = True
) -> int:
    """
    Main function to create clean AIS database.
    
    Performs two-pass processing:
    1. Statistical analysis with multiprocessing to identify valid vessels
    2. Data cleaning and deduplication with sliding window
    
    Args:
        input_folder: Path to folder with CSV files (default: FOLDER_PATH from config)
        output_file: Path for output file (default: OUTPUT_CLEAN_DB_FILE from config)
        verbose: Whether to print progress information
        
    Returns:
        Number of valid MMSIs identified
    """
    # Use local variables to avoid modifying globals
    data_folder = Path(input_folder) if input_folder is not None else FOLDER_PATH
    output_clean_db_file = Path(output_file) if output_file is not None else OUTPUT_CLEAN_DB_FILE
    
    # Ensure output directory exists
    output_clean_db_file.parent.mkdir(exist_ok=True, parents=True)
    
    input_files = sorted(list(data_folder.glob("*.csv")))
    if not input_files:
        print(f"Error: No CSV files found in {data_folder}")
        return 0
    
    days = len(input_files)
    if verbose:
        print("=" * 60)
        print("CLEAN AIS DATABASE CREATION")
        print("=" * 60)
        print(f"\nInput folder: {data_folder}")
        print(f"Output file: {output_clean_db_file}")
        print(f"Input files: {days} daily CSV files")
        print(f"\nFilter thresholds:")
        print(f"  - Min positions per day: {MIN_POS_PER_DAY}")
        print(f"  - Minimum movement: {MIN_KM_MOVE} km")
        print(f"  - Minimum speed: {MIN_SPEED_KNOTS} knots")
        print(f"  - CPU cores: {multiprocessing.cpu_count()}")
        print("=" * 60)
    
    # PHASE 1: Statistical analysis with multiprocessing
    if verbose:
        print(f"\n--- PHASE 1: Statistical Analysis ({days} daily files) ---")
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.map(analyze_single_file, input_files)
    
    global_stats = {}
    for local_dict in results:
        merge_stats(global_stats, local_dict)
    del results  # Free RAM
    
    # Create whitelist of valid MMSIs
    min_degree_diff = MIN_KM_MOVE / 111.0
    valid_mmsis: Set[str] = set()
    
    for mmsi, s in global_stats.items():
        # Check if vessel physically moved from port area
        is_moving = (s['max_lat'] - s['min_lat'] > min_degree_diff) or \
                    (s['max_lon'] - s['min_lon'] > min_degree_diff)
        
        # Check if vessel reached normal speed at least once
        has_speed = s['max_sog'] >= MIN_SPEED_KNOTS
        
        # Apply configured thresholds (filter out port noise)
        if s['cnt'] >= (days * MIN_POS_PER_DAY) and is_moving and has_speed:
            valid_mmsis.add(mmsi)
    
    if verbose:
        print(f"Valid vessels identified: {len(valid_mmsis)}")
    
    del global_stats  # Free RAM
    
    # PHASE 2: Writing with sliding window deduplication
    if verbose:
        print(f"\n--- PHASE 2: Final Cleaning (Minimal RAM Usage) ---")
    
    header_written = False
    rows_written = 0
    
    with open(output_clean_db_file, 'w', encoding='utf-8', newline='') as fout:
        writer = None
        
        for file_path in input_files:
            if verbose:
                print(f"Processing: {file_path.name}")
            
            # Keep hash set only for current timestamp (RAM optimization)
            current_timestamp_keys = set()
            last_timestamp = None
            
            with open(file_path, 'r', encoding='utf-8-sig') as fin:
                reader = csv.DictReader(fin)
                reader.fieldnames = [f.strip() for f in reader.fieldnames]
                
                if not header_written:
                    writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    header_written = True
                
                for row in reader:
                    mmsi = row.get('MMSI', '').strip()
                    if mmsi in valid_mmsis:
                        ts = row.get('# Timestamp')
                        try:
                            lat_raw = row.get('Latitude', '91')
                            lat = float(lat_raw)
                            sog = float(row.get('SOG', 0))
                            
                            # Filter out invalid coordinates and stationary moments (SOG < 0.1)
                            if abs(lat) > 89.9 or lat == 91.0 or sog < 0.1:
                                continue
                            
                            # Clear duplicate list when timestamp changes (RAM optimization)
                            if ts != last_timestamp:
                                current_timestamp_keys.clear()
                                last_timestamp = ts
                            
                            # Use hash() for memory-efficient deduplication
                            row_hash = hash((mmsi, lat_raw, sog))
                            
                            if row_hash not in current_timestamp_keys:
                                writer.writerow(row)
                                current_timestamp_keys.add(row_hash)
                                rows_written += 1
                        except Exception:
                            continue
            
            current_timestamp_keys.clear()
    
    if verbose:
        print(f"\n✅ Process completed!")
        print(f"   Output file: {output_clean_db_file}")
        print(f"   Valid vessels: {len(valid_mmsis)}")
        print(f"   Rows written: {rows_written}")
        print("=" * 60)
    
    return len(valid_mmsis)


def main():
    """Main entry point for clean AIS database creation."""
    try:
        create_clean_ais_database(verbose=True)
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
    except Exception as e:
        print(f"\nError during clean database creation: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
