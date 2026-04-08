"""
Vessel Proximity Analysis Module (Plaukianciu salia laivu analize)
Version 0.0.10c - Enhanced with parallel chunk processing

Analyzes AIS data to detect vessels sailing close to each other for extended periods.
Uses spatial and temporal indexing to efficiently find potential vessel meetings.

Key improvements in v0.0.10c:
- Parallel chunk-based verification processing
- Improved macOS support with gsort
- Better memory management with disk-based intermediate results
- Automatic CPU core detection for optimal parallelization
"""

import csv
import concurrent.futures
from collections import defaultdict
from math import radians, cos, sin, asin, sqrt
import os
import time
import shutil
import platform
from datetime import datetime
from pathlib import Path

# Import configuration from centralized config module
try:
    from config import (
        FOLDER_PATH,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        PROXIMITY_GRID_SIZE,
        PROXIMITY_TIME_STEP,
        PROXIMITY_REQUIRED_WINDOWS,
        PROXIMITY_MAX_DIST_KM,
        PROXIMITY_SOG_DIFF_LIMIT,
        PROXIMITY_MIN_SOG_RAW_FILTER,
        PROXIMITY_MIN_AVG_MEETING_SOG,
        PROXIMITY_MIN_MAX_SPEED_GLOBAL,
        PROXIMITY_TMP_FOLDER,
        PROXIMITY_OUTPUT_FILE,
        PROXIMITY_MAX_WORKERS,
        MAX_MEMORY_TO_USE,
        GLOBAL_TMP_FOLDER,
        PROXIMITY_MAX_MEMORY_TO_USE,
        SOG_DRUGHT_OUTPUT_FILE
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
    OUTPUT_REPORT_FOLDER.mkdir(exist_ok=True, parents=True)
    
    PROXIMITY_GRID_SIZE = 0.01
    PROXIMITY_TIME_STEP = 600
    PROXIMITY_REQUIRED_WINDOWS = 12
    PROXIMITY_MAX_DIST_KM = 1.0
    PROXIMITY_SOG_DIFF_LIMIT = 2.0
    PROXIMITY_MIN_SOG_RAW_FILTER = 0.1
    PROXIMITY_MIN_AVG_MEETING_SOG = 0.5
    PROXIMITY_MIN_MAX_SPEED_GLOBAL = 1.0
    PROXIMITY_MAX_WORKERS = None  # Auto-detect CPU count
    PROXIMITY_MAX_MEMORY_TO_USE = 2  # GB
    PROXIMITY_TMP_FOLDER = Path("/tmp/ais_processing")
   
    PROXIMITY_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"
    SOG_DRUGHT_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "consolidated_speed_report.Class_A.csv"

# Module-level aliases for backward compatibility
GRID_SIZE = PROXIMITY_GRID_SIZE
TIME_STEP = PROXIMITY_TIME_STEP
REQUIRED_WINDOWS = PROXIMITY_REQUIRED_WINDOWS
MAX_DIST_KM = PROXIMITY_MAX_DIST_KM
SOG_DIFF_LIMIT = PROXIMITY_SOG_DIFF_LIMIT
MIN_SOG_RAW_FILTER = PROXIMITY_MIN_SOG_RAW_FILTER
MIN_AVG_MEETING_SOG = PROXIMITY_MIN_AVG_MEETING_SOG
MIN_MAX_SPEED_GLOBAL = PROXIMITY_MIN_MAX_SPEED_GLOBAL
DATA_FOLDER = CLEAN_DB_FOLDER_PATH
TMP_FOLDER = PROXIMITY_TMP_FOLDER
OUTPUT_FILE = PROXIMITY_OUTPUT_FILE
SPEED_REPORT_FILE = SOG_DRUGHT_OUTPUT_FILE
MAX_WORKERS = PROXIMITY_MAX_WORKERS
MAX_MEMORY_TO_USE = PROXIMITY_MAX_MEMORY_TO_USE
MASTER_MMSI_FILE = OUTPUT_REPORT_FOLDER / "master_MMSI_data.csv"
CANDIDATES_FILE = TMP_FOLDER / "candidates_raw.csv"
SORTED_FILE = TMP_FOLDER / "candidates_sorted.csv"
MATCHES_FOLDER = TMP_FOLDER / "matches"
MIN_AVG_MEETING_SOG = PROXIMITY_MIN_AVG_MEETING_SOG
MIN_MAX_SPEED_GLOBAL = PROXIMITY_MIN_MAX_SPEED_GLOBAL
DATA_FOLDER = CLEAN_DB_FOLDER_PATH
TMP_FOLDER = PROXIMITY_TMP_FOLDER
OUTPUT_FILE = PROXIMITY_OUTPUT_FILE
SPEED_REPORT_FILE = SOG_DRUGHT_OUTPUT_FILE
MAX_WORKERS = PROXIMITY_MAX_WORKERS
MAX_MEMORY_TO_USE = PROXIMITY_MAX_MEMORY_TO_USE
MASTER_MMSI_FILE = OUTPUT_REPORT_FOLDER / "master_MMSI_data.csv"
CANDIDATES_FILE = TMP_FOLDER / "candidates_raw.csv"
SORTED_FILE = TMP_FOLDER / "candidates_sorted.csv" 

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * asin(sqrt(a)) * 6371

def get_active_ships(master_file, speed_file):
    active_mmsi = set()
    try:
        with open(speed_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = row.get('MMSI', '').strip()
                max_sog = row.get('max_SOG_knots', '').strip()
                if mmsi and max_sog and float(max_sog) >= MIN_MAX_SPEED_GLOBAL:
                    active_mmsi.add(mmsi)
    except Exception as e:
        print(f"Perspėjimas: Globalus filtras nepritaikytas ({e})")
    
    final_set = set()
    with open(master_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row:
                mmsi = row[0].strip()
                if not active_mmsi or mmsi in active_mmsi:
                    final_set.add(mmsi)
    return final_set

def split_to_hours(file_path, master_set):
    files, writers = {}, {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                mmsi = row[2].strip()
                if mmsi in master_set:
                    date_id = row[0][6:10] + row[0][3:5] + row[0][0:2]
                    hour_id = row[0][11:13]
                    f_id = f"{date_id}_{hour_id}"
                    if f_id not in writers:
                        hf = open(TMP_FOLDER / f"{f_id}.csv", "a")
                        files[f_id], writers[f_id] = hf, csv.writer(hf)
                    writers[f_id].writerow(row)
    finally:
        for f in files.values(): f.close()

def analyze_hour_to_disk(hour_file):
    """2 FAZA: Erdvinė analizė (rezultatai į diską).
    
    Writes matches to separate files in MATCHES_FOLDER for better memory management.
    """
    if not hour_file.exists(): 
        return
    match_output = MATCHES_FOLDER / f"match_{hour_file.name}"
    hour_index = defaultdict(list)
    
    with open(hour_file, "r") as f:
        for row in csv.reader(f):
            try:
                dt = datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S")
                ts, lat, lon, sog = int(dt.timestamp()), float(row[3]), float(row[4]), float(row[7])
                t_bucket = ts // TIME_STEP
                gx, gy = int(lat / GRID_SIZE), int(lon / GRID_SIZE)
                hour_index[(t_bucket, gx, gy)].append((row[2], lat, lon, sog, t_bucket))
            except: 
                continue

    with open(match_output, "w") as f_out:
        for (t, gx, gy), ships in hour_index.items():
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    target = (t, gx + dx, gy + dy)
                    if target in hour_index:
                        for s1 in ships:
                            for s2 in hour_index[target]:
                                if s1[0] >= s2[0]: 
                                    continue
                                # Filter: at least one vessel must be moving
                                if s1[3] < MIN_SOG_RAW_FILTER and s2[3] < MIN_SOG_RAW_FILTER: 
                                    continue
                                if abs(s1[3] - s2[3]) <= SOG_DIFF_LIMIT:
                                    if haversine(s1[2], s1[1], s2[2], s2[1]) <= MAX_DIST_KM:
                                        f_out.write(f"{s1[0]},{s2[0]},{s1[4]},{s1[3]},{s2[3]}\n")

def check_streak_final(streak, pair):
    """4 FAZA: Streak patikra (optimizuota).
    
    Returns meeting data if streak meets criteria, None otherwise.
    Optimized for parallel chunk processing.
    """
    if not streak or len(streak) < REQUIRED_WINDOWS:
        return None
    
    total_sog = sum(s[1] + s[2] for s in streak)
    avg = total_sog / (len(streak) * 2)
    
    if avg >= MIN_AVG_MEETING_SOG:
        start_dt = datetime.fromtimestamp(streak[0][0] * TIME_STEP)
        end_dt = datetime.fromtimestamp(streak[-1][0] * TIME_STEP)
        return [pair[0], pair[1], len(streak)*10, start_dt, end_dt, round(avg, 2)]
    return None


def process_sorted_chunk(file_path, start_offset, end_offset):
    """4 FAZA: Lygiagretus surūšiuoto failo apdorojimas.
    
    Processes a chunk of the sorted file in parallel.
    Args:
        file_path: Path to sorted candidates file
        start_offset: Byte offset to start reading
        end_offset: Byte offset to stop reading
    Returns:
        List of valid meeting records
    """
    results = []
    with open(file_path, 'r') as f:
        if start_offset != 0:
            f.seek(start_offset)
            f.readline()  # Skip potential incomplete line
            
        curr_pair, streak = None, []
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            
            # If we've reached end offset but still processing same pair - continue to pair end
            if pos >= end_offset and curr_pair is None:
                break
            
            row = line.strip().split(',')
            if len(row) < 5:
                continue
            
            m1, m2, t, s1, s2 = row[0], row[1], int(row[2]), float(row[3]), float(row[4])
            pair = (m1, m2)
            
            if pos >= end_offset and pair != curr_pair:
                res = check_streak_final(streak, curr_pair)
                if res:
                    results.append(res)
                break

            if pair != curr_pair:
                res = check_streak_final(streak, curr_pair)
                if res:
                    results.append(res)
                curr_pair, streak = pair, [(t, s1, s2)]
            else:
                if t <= streak[-1][0] + 2:  # Allow 1 window gap
                    if t != streak[-1][0]:
                        streak.append((t, s1, s2))
                else:
                    res = check_streak_final(streak, curr_pair)
                    if res:
                        results.append(res)
                    streak = [(t, s1, s2)]
                    
        res = check_streak_final(streak, curr_pair)
        if res:
            results.append(res)
    return results

def run_vessel_proximity_analysis(data_folder=None, master_mmsi_file=None, 
                                   speed_report_file=None, output_file=None, verbose=True):
    """
    Run vessel proximity analysis to detect vessels sailing close to each other.
    
    This is the main entry point for integration with the CSV Data Analysis project.
    Enhanced in v0.0.10c with parallel chunk-based verification processing.
    
    Args:
        data_folder: Path to folder containing CSV files (default: uses config.FOLDER_PATH)
        master_mmsi_file: Path to master MMSI data file (default: auto-generated)
        speed_report_file: Path to speed report file (default: uses config.SOG_DRUGHT_OUTPUT_FILE)
        output_file: Path for output CSV report (default: uses config.PROXIMITY_OUTPUT_FILE)
        verbose: Whether to print detailed progress information
        
    Returns:
        int: Number of proximity incidents found
    """
    global DATA_FOLDER, MASTER_MMSI_FILE, SPEED_REPORT_FILE, OUTPUT_FILE, TMP_FOLDER, MATCHES_FOLDER
    
    # Override defaults if provided
    if data_folder is not None:
        DATA_FOLDER = Path(data_folder)
    if master_mmsi_file is not None:
        MASTER_MMSI_FILE = Path(master_mmsi_file)
    if speed_report_file is not None:
        SPEED_REPORT_FILE = Path(speed_report_file)
    if output_file is not None:
        OUTPUT_FILE = Path(output_file)
        # Ensure output directory exists
        OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)
    
    # Auto-detect CPU count if MAX_WORKERS not set
    num_cpus = os.cpu_count() or 4
    max_workers = MAX_WORKERS if MAX_WORKERS else num_cpus
    
    if verbose:
        print("\n" + "=" * 60)
        print("VESSEL PROXIMITY ANALYSIS (v0.0.10c)")
        print("=" * 60)
        print(f"\nData folder: {DATA_FOLDER}")
        print(f"Master MMSI file: {MASTER_MMSI_FILE}")
        print(f"Speed report file: {SPEED_REPORT_FILE}")
        print(f"Output file: {OUTPUT_FILE}")
        print(f"Using {max_workers} CPU cores for parallel processing")
        print(f"\nConfiguration:")
        print(f"  - Grid size: {PROXIMITY_GRID_SIZE} degrees (~1.1 km)")
        print(f"  - Time step: {PROXIMITY_TIME_STEP} seconds ({PROXIMITY_TIME_STEP//60} minutes)")
        print(f"  - Required windows: {PROXIMITY_REQUIRED_WINDOWS} (~{PROXIMITY_REQUIRED_WINDOWS * PROXIMITY_TIME_STEP // 3600} hours)")
        print(f"  - Max distance: {PROXIMITY_MAX_DIST_KM} km")
        print(f"  - SOG diff limit: {PROXIMITY_SOG_DIFF_LIMIT} knots")
        print(f"  - Min SOG filter: {PROXIMITY_MIN_SOG_RAW_FILTER} knots")
        print(f"  - Min avg meeting SOG: {PROXIMITY_MIN_AVG_MEETING_SOG} knots")
        print("=" * 60 + "\n")
    
    start_total = time.time()
    
    # Check if data folder exists
    if not DATA_FOLDER.exists():
        print(f"❌ Error: Data folder not found: {DATA_FOLDER}")
        return 0
    
    # Check if speed report exists (required for filtering)
    if not SPEED_REPORT_FILE.exists():
        print(f"⚠️  Warning: Speed report not found: {SPEED_REPORT_FILE}")
        print("   Run SOG/Draught analysis first to generate this file.")
        print("   Proceeding without speed-based filtering...")
    
    # Clean up and create temp folders
    if TMP_FOLDER.exists():
        shutil.rmtree(TMP_FOLDER)
    TMP_FOLDER.mkdir(parents=True, exist_ok=True)
    MATCHES_FOLDER.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. PREPARATION - Get active ships
        if verbose:
            print("--- Phase 1: Preparing active ship list ---")
        m_set = get_active_ships(MASTER_MMSI_FILE, SPEED_REPORT_FILE)
        input_files = sorted(list(DATA_FOLDER.glob("*.csv")))
        
        if not input_files:
            print("❌ No CSV files found in data folder.")
            return 0
        
        if verbose:
            print(f"Found {len(input_files)} CSV files to process.")
            print(f"Active ships in master list: {len(m_set)}")
        
        # 2. SPLIT TO HOURS
        if verbose:
            print("\n--- Phase 2: Splitting data by time windows ---")
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as ex:
            list(ex.map(split_to_hours, input_files, [m_set]*len(input_files)))
        
        # 3. SPATIAL ANALYSIS (writes to MATCHES_FOLDER)
        if verbose:
            print("\n--- Phase 3: Spatial analysis (disk-based matching) ---")
        tmp_files = sorted(list(TMP_FOLDER.glob("20*.csv")))
        
        if not tmp_files:
            print("⚠️  No temporal data files generated. Check input data.")
            return 0
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as ex:
            list(ex.map(analyze_hour_to_disk, tmp_files))
        
        # 4. SORTING - Combine all match files
        if verbose:
            print("\n--- Phase 4: Sorting candidates ---")
        
        os_system = platform.system()
        sort_tool = "gsort" if os_system == "Darwin" else "sort"
        # Use percentage-based buffer for better memory management
        buffer_size = "30%" if os_system != "Windows" else "2G"
        
        sort_cmd = (f"{sort_tool} -t, -k1,1 -k2,2 -k3,3n "
                    f"-S {buffer_size} --parallel={max_workers} "
                    f"-T {TMP_FOLDER} {MATCHES_FOLDER}/match_*.csv -o {SORTED_FILE}")
        
        if verbose:
            print(f"Executing: {sort_cmd}")
        
        import subprocess
        if subprocess.run(sort_cmd, shell=True).returncode != 0:
            print("⚠️  Warning: Parallel sort failed, trying standard sort...")
            fallback_cmd = f"sort -t, -k1,1 -k2,2 -k3,3n {MATCHES_FOLDER}/match_*.csv -o {SORTED_FILE}"
            if subprocess.run(fallback_cmd, shell=True).returncode != 0:
                print("❌ Error: Sort command failed.")
                return 0
        
        if verbose:
            print("Sort completed successfully")
        
        # 5. VERIFICATION - Parallel chunk processing (NEW in v0.0.10c)
        if verbose:
            print("\n--- Phase 5: Parallel verification (chunk-based) ---")
        found_count = 0
        
        if SORTED_FILE.exists():
            file_size = SORTED_FILE.stat().st_size
            chunk_size = file_size // max_workers
            offsets = [i * chunk_size for i in range(max_workers)] + [file_size]
            
            with open(OUTPUT_FILE, "w", newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(['MMSI_1', 'MMSI_2', 'Duration_Min', 'Start_Time', 'End_Time', 'Avg_SOG_Combined'])
                
                with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as ex:
                    futures = [ex.submit(process_sorted_chunk, SORTED_FILE, offsets[i], offsets[i+1]) 
                              for i in range(max_workers)]
                    for future in concurrent.futures.as_completed(futures):
                        chunk_results = future.result()
                        if chunk_results:
                            writer.writerows(chunk_results)
                            found_count += len(chunk_results)
        
        # Delete temporary sorted file
        if SORTED_FILE.exists():
            try:
                SORTED_FILE.unlink()
                if verbose:
                    print(f"Cleaned up temporary file: {SORTED_FILE}")
            except Exception:
                pass
        
        # Force garbage collection after large file processing
        import gc
        gc.collect()
        
        elapsed = time.time() - start_total
        
        if verbose:
            print(f"\n✅ Analysis complete!")
            print(f"   Found {found_count} vessel proximity incidents.")
            print(f"   Report saved to: {OUTPUT_FILE}")
            print(f"   Total elapsed time: {elapsed:.2f} seconds")
        
        return found_count
        
    except Exception as e:
        print(f"\n❌ Error during vessel proximity analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0
        
    finally:
        # Cleanup temp folder
        if TMP_FOLDER.exists():
            shutil.rmtree(TMP_FOLDER)
        
        # Force garbage collection to free memory
        import gc
        gc.collect()
        
        if verbose:
            print("\nCleanup completed.")


if __name__ == '__main__':
    try:
        run_vessel_proximity_analysis(verbose=True)
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
        # Cleanup
        if TMP_FOLDER.exists():
            shutil.rmtree(TMP_FOLDER)

