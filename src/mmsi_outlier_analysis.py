"""
MMSI Outlier Analysis Module

Detects outlier positions in vessel tracking data using sharding and distance-based filtering.
Identifies vessels with suspicious position jumps that exceed normal movement patterns.

Original source of file before integration into primary project: mmsi_outlier_analysis.v008.py
"""

import os
import csv
import hashlib
import shutil
from math import radians, sin, cos, sqrt, atan2
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Import configuration from centralized config module
try:
    from config import (
        FOLDER_PATH,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        GLOBAL_TMP_FOLDER,
        PRIMARY_FOLDER,
        MAX_WORKERS
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
    OUTPUT_REPORT_FOLDER.mkdir(exist_ok=True, parents=True)
    GLOBAL_TMP_FOLDER = Path("/tmp")
    MAX_WORKERS = os.cpu_count() or 4

# Module-specific configuration
# Use Clean_AIS_DB as data source if available
clean_db_file = CLEAN_DB_FOLDER_PATH / "Clean_AIS_DB.csv"
if clean_db_file.exists():
    DATA_FOLDER = CLEAN_DB_FOLDER_PATH
else:
    DATA_FOLDER = FOLDER_PATH

VESSELS_PROXIMITY_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"
TEMP_SHARD_DIR = GLOBAL_TMP_FOLDER / "temp_shards"
OUTPUT_SUMMARY_FILE = OUTPUT_REPORT_FOLDER / "mmsi_outlier_summary.csv"
OUTPUT_DETAILS_FILE = OUTPUT_REPORT_FOLDER / "mmsi_outlier_details.csv"

# Analysis parameters
NUM_SHARDS = 20  # Number of shards for parallel processing
MAX_DISTANCE_KM = 50.0  # Maximum allowed distance between consecutive positions

# Ensure directories exist
OUTPUT_REPORT_FOLDER.mkdir(parents=True, exist_ok=True)
TEMP_SHARD_DIR.mkdir(parents=True, exist_ok=True)


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points on Earth (in km).
    
    Args:
        lat1, lon1: Latitude and longitude of point 1 (in degrees)
        lat2, lon2: Latitude and longitude of point 2 (in degrees)
    
    Returns:
        Distance in kilometers
    """
    if lat1 == lat2 and lon1 == lon2:
        return 0.0
    
    R = 6371.0  # Earth's radius in kilometers
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    
    a = sin(dphi/2)**2 + cos(phi1) * cos(phi2) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def get_shard_id(mmsi):
    """Generate shard ID for an MMSI using MD5 hash."""
    return int(hashlib.md5(str(mmsi).encode()).hexdigest(), 16) % NUM_SHARDS


def load_proximity_mmsis(file_path):
    """
    Read the proximity file and return a set of unique MMSIs to filter by.
    
    Args:
        file_path: Path to the vessel proximity meetings CSV file
    
    Returns:
        Set of MMSIs or None if file not found (process all MMSIs)
    """
    allowed_mmsis = set()
    print(f"Loading filter list from {file_path}...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'MMSI_1' in row:
                    allowed_mmsis.add(row['MMSI_1'].strip())
                if 'MMSI_2' in row:
                    allowed_mmsis.add(row['MMSI_2'].strip())
        
        print(f"--- Found {len(allowed_mmsis)} unique vessels of interest ---\n")
        return allowed_mmsis
    except FileNotFoundError:
        print(f"Warning: Proximity file not found. Processing all MMSIs instead.")
        return None


def shard_data(allowed_mmsis):
    """
    Pass 1: Read chronologically and shard ONLY MMSIs in our filter list.
    
    Args:
        allowed_mmsis: Set of MMSIs to include (or None for all)
    """
    csv_files = sorted([f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")])
    print(f"Starting Pass 1: Sharding filtered data into {NUM_SHARDS} buckets...")
    
    handles = [open(TEMP_SHARD_DIR / f"shard_{i}.csv", 'w', newline='') for i in range(NUM_SHARDS)]
    writers = [csv.writer(h) for h in handles]
    
    total_processed = 0
    written_count = 0

    try:
        for filename in csv_files:
            file_path = DATA_FOLDER / filename
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    total_processed += 1
                    mmsi = row.get('MMSI')
                    
                    # Only shard if the MMSI is in our proximity list
                    if allowed_mmsis is not None and mmsi not in allowed_mmsis:
                        continue
                    
                    if not mmsi:
                        continue
                    
                    shard_id = get_shard_id(mmsi)
                    writers[shard_id].writerow([
                        mmsi,
                        row.get('# Timestamp', ''),
                        row.get('Latitude', 0),
                        row.get('Longitude', 0)
                    ])
                    written_count += 1
            
            print(f"  Finished {filename}...")
    
    finally:
        for h in handles:
            h.close()
    
    print(f"\nPass 1 Complete: Filtered {total_processed} rows down to {written_count} relevant rows.")


def process_shards():
    """Pass 2: Analyze shards and detect outlier positions."""
    print("Starting Pass 2: Analyzing shards...")
    
    summary_results = []
    
    with open(OUTPUT_DETAILS_FILE, 'w', newline='') as f_det:
        det_writer = csv.writer(f_det)
        det_writer.writerow([
            'MMSI', 'Timestamp', 'From_Lat', 'From_Lon', 
            'To_Lat', 'To_Lon', 'Distance_KM'
        ])

        for i in range(NUM_SHARDS):
            shard_path = TEMP_SHARD_DIR / f"shard_{i}.csv"
            if not shard_path.exists():
                continue
            
            ship_data = defaultdict(list)
            with open(shard_path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    mmsi, ts, lat, lon = row
                    ship_data[mmsi].append((ts, float(lat), float(lon)))
            
            print(f"  Processing shard {i+1}/{NUM_SHARDS}...")
            
            for mmsi, points in ship_data.items():
                if len(points) < 2:
                    continue
                
                # Sort by timestamp
                points.sort(key=lambda x: x[0])
                
                filtered = [points[0]]
                removed_count = 0
                total_dist = 0.0
                
                for j in range(1, len(points)):
                    curr = points[j]
                    prev = filtered[-1]
                    dist = haversine_distance(prev[1], prev[2], curr[1], curr[2])
                    
                    if dist <= MAX_DISTANCE_KM:
                        filtered.append(curr)
                    else:
                        removed_count += 1
                        total_dist += dist
                        # Detailed log: MMSI, TS, Prev Lat/Lon, Curr Lat/Lon, Dist
                        det_writer.writerow([
                            mmsi, curr[0], 
                            prev[1], prev[2], 
                            curr[1], curr[2], 
                            round(dist, 2)
                        ])
                
                if removed_count > 0:
                    summary_results.append({
                        'MMSI': mmsi,
                        'Total_Points': len(points),
                        'Removed_Points': removed_count,
                        'Total_Filtered_Dist_KM': round(total_dist, 2)
                    })
            
            ship_data.clear()

    # Sort summary by removed_count (descending)
    print("Sorting summary results...")
    summary_results.sort(key=lambda x: x['Removed_Points'], reverse=True)

    with open(OUTPUT_SUMMARY_FILE, 'w', newline='') as f_sum:
        writer = csv.DictWriter(
            f_sum, 
            fieldnames=['MMSI', 'Total_Points', 'Removed_Points', 'Total_Filtered_Dist_KM']
        )
        writer.writeheader()
        writer.writerows(summary_results)
    
    print(f"Summary saved to: {OUTPUT_SUMMARY_FILE}")
    print(f"Details saved to: {OUTPUT_DETAILS_FILE}")


def run_mmsi_outlier_analysis():
    """
    Main function to run MMSI outlier analysis.
    
    This function:
    1. Loads vessel list from proximity analysis (optional filter)
    2. Shards data by MMSI for parallel processing
    3. Detects position jumps exceeding MAX_DISTANCE_KM
    4. Outputs summary and detailed reports
    """
    start_time = datetime.now()
    
    print("=" * 60)
    print("MMSI OUTLIER ANALYSIS: Position Jump Detection")
    print("=" * 60)
    print(f"\nData source: {DATA_FOLDER}")
    print(f"Output folder: {OUTPUT_REPORT_FOLDER}")
    print(f"Number of shards: {NUM_SHARDS}")
    print(f"Max distance threshold: {MAX_DISTANCE_KM} km")
    print(f"Filter file: {VESSELS_PROXIMITY_FILE}")
    print("\n" + "-" * 60)
    
    # Load the MMSIs we actually care about
    allowed_mmsis = load_proximity_mmsis(VESSELS_PROXIMITY_FILE)
    
    try:
        shard_data(allowed_mmsis)
        process_shards()
        
        elapsed = datetime.now() - start_time
        print("\n" + "=" * 60)
        print("MMSI OUTLIER ANALYSIS COMPLETE")
        print("=" * 60)
        print(f"\n✅ Analysis finished in: {elapsed}")
        print(f"   Summary: {OUTPUT_SUMMARY_FILE}")
        print(f"   Details: {OUTPUT_DETAILS_FILE}")
        print("=" * 60)
    
    finally:
        # Cleanup temporary shards
        if TEMP_SHARD_DIR.exists():
            print("\nCleaning up temporary shards...")
            shutil.rmtree(TEMP_SHARD_DIR)


if __name__ == "__main__":
    try:
        run_mmsi_outlier_analysis()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\nError during MMSI outlier analysis: {str(e)}")
        import traceback
        traceback.print_exc()
