"""
Anomaly Detection Module - Gap and Draught Change Analysis

Detects AIS transmission gaps and draught changes for specific MMSI vessels.
Uses external sort for memory-efficient processing of large datasets.

Original source of file before integration into primary project: AnomalyC.v004.py
"""

import csv
import subprocess
import os
from datetime import datetime
from pathlib import Path
import platform

# Import configuration from centralized config module
try:
    from config import (
        FOLDER_PATH,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        GLOBAL_TMP_FOLDER,
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
    
DATA_FOLDER = CLEAN_DB_FOLDER_PATH

MASTER_MMSI_FILE = OUTPUT_REPORT_FOLDER / "master_MMSI_data.csv"
TEMP_DIR = GLOBAL_TMP_FOLDER / "temp_AnomalyC"
OUTPUT_GAP_FILE = OUTPUT_REPORT_FOLDER / "laivai_dingimai.csv"
OUTPUT_DRAUGHT_FILE = OUTPUT_REPORT_FOLDER / "mmsi_draught_change.csv"
MEMORY_LIMIT = "2G"
GAP_THRESHOLD_HOURS = 2  # Time gap threshold in hours

# Ensure directories exist
OUTPUT_REPORT_FOLDER.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)


def get_timestamp(date_str):
    """Convert timestamp string to Unix timestamp."""
    try:
        return int(datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S').timestamp())
    except Exception:
        return 0


def run_anomaly_detection(verbose=True):
    """
    Main function to run anomaly detection for gaps and draught changes.
    
    This function:
    1. Loads target MMSI list from master file
    2. Filters CSV data for target MMSIs
    3. Uses external sort for memory-efficient sorting
    4. Detects AIS gaps (>2 hours) and draught changes (>5%)
    5. Outputs results to CSV files
    
    Args:
        verbose: Whether to print detailed progress information (default: True)
    """
    # Get all CSV files in the data folder (uses Clean_AIS_DB if available)
    csv_files = sorted(DATA_FOLDER.glob("*.csv"))
    
    if not csv_files:
        print(f"Error: No CSV files found in {DATA_FOLDER}")
        return
    
    # Use all CSV files for analysis
    input_files = csv_files
    
    temp_unsorted = TEMP_DIR / "temp_filtered_unsorted.csv"
    temp_sorted = TEMP_DIR / "temp_filtered_sorted.csv"
    
    if verbose:
        print("=" * 60)
        print("ANOMALY DETECTION: Gap and Draught Change Analysis")
        print("=" * 60)
        print(f"\nData source: {DATA_FOLDER}")
        print(f"Input files: {[Path(f).name for f in input_files]}")
        print(f"Master MMSI file: {MASTER_MMSI_FILE}")
        print(f"Gap threshold: {GAP_THRESHOLD_HOURS} hours")
        print(f"Memory limit: {MEMORY_LIMIT}")
        print(f"Workers: {MAX_WORKERS}")
        print("\n" + "-" * 60)
    
    # Step 1: Filter data
    if verbose:
        print("\n1. Filtering data for target MMSIs...")
    if not MASTER_MMSI_FILE.exists():
        print(f"Error: Master MMSI file not found: {MASTER_MMSI_FILE}")
        print("Please run Master Index Analysis first to generate the MMSI list.")
        return
    
    target_mmsis = set()
    with open(MASTER_MMSI_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            target_mmsis.add(row['MMSI'].strip())
    
    print(f"   Loaded {len(target_mmsis)} target MMSIs from master file")
    
    # Filter and extract relevant data
    with open(temp_unsorted, 'w', encoding='utf-8') as out_f:
        for file_path in input_files:
            if not os.path.exists(file_path):
                print(f"   Skipping non-existent file: {file_path}")
                continue
            
            with open(file_path, 'r', encoding='utf-8') as in_f:
                reader = csv.reader(in_f)
                header = next(reader)
                header = [h.strip().replace('# ', '') for h in header]
                
                try:
                    mmsi_idx = header.index('MMSI')
                    ts_idx = header.index('Timestamp')
                    dr_idx = header.index('Draught')
                except ValueError as e:
                    print(f"   Error: Required column not found in {file_path}: {e}")
                    continue
                
                count = 0
                for row in reader:
                    if row[mmsi_idx] in target_mmsis:
                        ts = get_timestamp(row[ts_idx])
                        out_f.write(f"{row[mmsi_idx]}|{ts}|{row[dr_idx]}|{','.join(row)}\n")
                        count += 1
                
                if verbose:
                    print(f"   Processed {Path(file_path).name}: {count} matching rows")
    
    # Step 2: External sort
    if verbose:
        print(f"\n2. Executing external sort (using {MAX_WORKERS} workers)...")
    
    # Detect OS and use appropriate sort command
    os_system = platform.system()
    sort_tool = "gsort" if os_system == "Darwin" else "sort"
    
    # Build sort command with memory and parallel options
    sort_cmd = [
        sort_tool,
        '-t', '|',
        '-k1,1',
        '-k2,2n',
        '-S', MEMORY_LIMIT,
        f'--parallel={MAX_WORKERS}',
        '-o', str(temp_sorted),
        str(temp_unsorted)
    ]
    
    if verbose:
        print(f"   Using sort tool: {sort_tool} (OS: {os_system})")
        print(f"   Command: {' '.join(sort_cmd)}")

    try:
        subprocess.run(sort_cmd, check=True)
        print("   Sort completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"   Error during sort: {e}")
        if os_system == "Darwin":
            print("   Tip: On macOS, consider installing gsort via 'brew install gsort' for better performance")
        return
    except FileNotFoundError:
        print(f"   Error: '{sort_tool}' command not found.")
        if os_system == "Darwin":
            print("   On macOS, install gsort via Homebrew: brew install gsort")
        else:
            print("   This tool is required for external sorting.")
        return
    
    # Step 3: Analyze gaps and draught changes
    if verbose:
        print("\n3. Analyzing gaps and draught changes...")
    prev_mmsi, prev_ts, prev_draught, prev_raw = None, 0, 0.0, None
    gap_threshold_sec = GAP_THRESHOLD_HOURS * 3600
    gaps_count = 0
    draught_changes_count = 0
    
    with open(temp_sorted, 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_GAP_FILE, 'w', newline='', encoding='utf-8') as f_gap, \
         open(OUTPUT_DRAUGHT_FILE, 'w', newline='', encoding='utf-8') as f_dr:
        
        gap_writer = csv.writer(f_gap)
        dr_writer = csv.writer(f_dr)
        
        # Write headers
        dr_writer.writerow(['MMSI', 'Draught_Before', 'Draught_After', 'Change_Percent'])
        
        for line in f_in:
            parts = line.strip().split('|')
            if len(parts) < 4:
                continue
            
            curr_mmsi = parts[0]
            curr_ts = int(parts[1])
            try:
                curr_draught = float(parts[2]) if parts[2] and parts[2] != 'Unknown' else 0.0
            except ValueError:
                curr_draught = 0.0
            curr_raw = parts[3].split(',')
            
            if prev_mmsi == curr_mmsi:
                time_diff = curr_ts - prev_ts
                if time_diff > gap_threshold_sec:
                    # Record gap (both rows before and after gap)
                    gap_writer.writerow(prev_raw)
                    gap_writer.writerow(curr_raw)
                    gaps_count += 2
                    
                    # Check draught change
                    if prev_draught > 0:
                        diff = abs(curr_draught - prev_draught)
                        change_pct = diff / prev_draught
                        
                        if change_pct > 0.05:  # 5% change threshold
                            dr_writer.writerow([
                                curr_mmsi, 
                                prev_draught, 
                                curr_draught, 
                                f"{change_pct:.2%}"
                            ])
                            draught_changes_count += 1
            
            prev_mmsi, prev_ts, prev_draught, prev_raw = curr_mmsi, curr_ts, curr_draught, curr_raw
    
    # Cleanup temporary files
    try:
        os.remove(temp_unsorted)
        os.remove(temp_sorted)
    except OSError:
        pass
    
    if verbose:
        print("\n" + "=" * 60)
        print("ANOMALY DETECTION COMPLETE")
        print("=" * 60)
        print(f"\n✅ Results saved to:")
        print(f"   Gap report: {OUTPUT_GAP_FILE}")
        print(f"   Draught changes: {OUTPUT_DRAUGHT_FILE}")
        print(f"\n📊 Statistics:")
        print(f"   Total gap records: {gaps_count}")
        print(f"   Draught changes (>5%): {draught_changes_count}")
        print("=" * 60)


if __name__ == "__main__":
    try:
        run_anomaly_detection(verbose=True)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\nError during anomaly detection: {str(e)}")
        import traceback
        traceback.print_exc()
