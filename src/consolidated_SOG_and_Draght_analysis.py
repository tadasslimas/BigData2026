"""
SOG and Draught Analysis Module

Analyzes vessel speed (SOG) and draught data from CSV files.
Provides consolidated reports with statistics and alerts for variations.
"""


# Import configuration from centralized config module
try:
    from config import (
        FOLDER_PATH,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        SOG_DRUGHT_MOBILE_TYPES,
        SOG_DRUGHT_MIN_SAMPLES,
        SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS,
        SOG_DRUGHT_VARIATION_THRESHOLD_PCT,
        SOG_DRUGHT_MAX_WORKERS,
        SOG_DRUGHT_OUTPUT_FILE
    )
except ImportError:
    # Fallback configuration if config module is not available
    from pathlib import Path
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
    OUTPUT_REPORT_FOLDER.mkdir(exist_ok=True, parents=True)
    
    SOG_DRUGHT_MOBILE_TYPES = {"Class A"}
    SOG_DRUGHT_MIN_SAMPLES = 5
    SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS = 80
    SOG_DRUGHT_VARIATION_THRESHOLD_PCT = 20.0
    SOG_DRUGHT_MAX_WORKERS = 4
    SOG_DRUGHT_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "consolidated_speed_report.Class_A.csv"


# Module-level aliases for backward compatibility
# Use Clean_AIS_DB if available, otherwise fall back to FOLDER_PATH
import os
from pathlib import Path
clean_db_file = CLEAN_DB_FOLDER_PATH / "Clean_AIS_DB.csv" if 'CLEAN_DB_FOLDER_PATH' in locals() else None
DATA_FOLDER = CLEAN_DB_FOLDER_PATH if (clean_db_file and clean_db_file.exists()) else FOLDER_PATH
MOBILE_TYPES = SOG_DRUGHT_MOBILE_TYPES
MIN_SAMPLES = SOG_DRUGHT_MIN_SAMPLES
SUSPICIOUS_AVG_KNOTS = SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS
MAX_WORKERS = SOG_DRUGHT_MAX_WORKERS
DRAUGHT_VARIATION_THRESHOLD_PCT = SOG_DRUGHT_VARIATION_THRESHOLD_PCT
OUTPUT_FILE = SOG_DRUGHT_OUTPUT_FILE

# --- FUNCTION ---
import csv
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import psutil
import os

# --- CONFIGURATION ---


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


def process_file_worker(filepath):
    local_vessels = {}
    try:
        with open(filepath, mode='r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [name.strip() for name in reader.fieldnames]
            
            for row in reader:
                if row.get("Type of mobile", "").strip() not in MOBILE_TYPES:
                    continue

                mmsi = row.get("MMSI", "").strip()
                if not mmsi:
                    continue

                # Initialize vessel entry if new
                if mmsi not in local_vessels:
                    local_vessels[mmsi] = {
                        's_sum': 0.0, 's_cnt': 0, 's_max': -1.0, 's_min': 999.0,
                        'd_sum': 0.0, 'd_cnt': 0, 'd_max': -1.0, 'd_min': 999.0
                    }
                
                v = local_vessels[mmsi]

                # Process SOG
                s_val = row.get("SOG")
                if s_val and s_val.strip():
                    try:
                        sog = float(s_val)
                        v['s_sum'] += sog
                        v['s_cnt'] += 1
                        if sog > v['s_max']: v['s_max'] = sog
                        if sog < v['s_min']: v['s_min'] = sog
                    except ValueError:
                        pass

                # Process Draught
                d_val = row.get("Draught")
                if d_val and d_val.strip():
                    try:
                        draught = float(d_val)
                        # AIS often uses 0 or 25.5 as "unknown". 
                        # Only process if draught is realistically provided.
                        if draught > 0: 
                            v['d_sum'] += draught
                            v['d_cnt'] += 1
                            if draught > v['d_max']: v['d_max'] = draught
                            if draught < v['d_min']: v['d_min'] = draught
                    except ValueError:
                        pass
        return local_vessels
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return {}

def run_parallel_sog_analysis():
    file_list = sorted(list(DATA_FOLDER.glob("*.csv")))
    if not file_list: return

    master_vessels = {}
    print(f"Processing {len(file_list)} files with {MAX_WORKERS} workers...")
    print_memory_usage()
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(process_file_worker, file_list)
        print_memory_usage()
        for file_dict in results:
            for mmsi, stats in file_dict.items():
                if mmsi not in master_vessels:
                    master_vessels[mmsi] = stats
                else:
                    m = master_vessels[mmsi]
                    # Aggregate SOG
                    m['s_sum'] += stats['s_sum']
                    m['s_cnt'] += stats['s_cnt']
                    if stats['s_max'] > m['s_max']: m['s_max'] = stats['s_max']
                    if stats['s_min'] < m['s_min']: m['s_min'] = stats['s_min']
                    # Aggregate Draught
                    m['d_sum'] += stats['d_sum']
                    m['d_cnt'] += stats['d_cnt']
                    if stats['d_max'] > m['d_max']: m['d_max'] = stats['d_max']
                    if stats['d_min'] < m['d_min']: m['d_min'] = stats['d_min']

    print("Writing finalized report...")
    print_memory_usage()
    headers = [
        "MMSI", "avg_SOG_knots", "max_SOG_knots", "min_SOG_knots", "sog_samples",
        "avg_Draught", "max_Draught", "min_Draught", "draught_samples", 
        "draught_variation_alert", "suspicious"
    ]

    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        sorted_mmsis = sorted(
            master_vessels.items(),
            key=lambda x: (x[1]['s_sum'] / x[1]['s_cnt']) if x[1]['s_cnt'] > 0 else 0,
            reverse=True
        )

        for mmsi, v in sorted_mmsis:
            # SOG Calculations
            avg_sog = round(v['s_sum'] / v['s_cnt'], 2) if v['s_cnt'] > 0 else 0
            s_max = v['s_max'] if v['s_cnt'] > 0 else 0
            s_min = v['s_min'] if v['s_cnt'] > 0 else 0

            # Draught Calculations
            avg_draught = round(v['d_sum'] / v['d_cnt'], 2) if v['d_cnt'] > 0 else 0
            d_max = v['d_max'] if v['d_cnt'] > 0 else 0
            d_min = v['d_min'] if v['d_cnt'] > 0 else 0
            
            # Alerts logic
            d_diff = d_max - d_min
            var_pct = (d_diff / avg_draught * 100) if avg_draught > 0.1 else 0
            is_alert = (var_pct >= DRAUGHT_VARIATION_THRESHOLD_PCT) or (d_diff > 2.0)
            is_suspicious = (avg_sog >= SUSPICIOUS_AVG_KNOTS) and (v['s_cnt'] < MIN_SAMPLES)

            writer.writerow({
                "MMSI": mmsi,
                "avg_SOG_knots": avg_sog,
                "max_SOG_knots": s_max,
                "min_SOG_knots": s_min,
                "sog_samples": v['s_cnt'],
                "avg_Draught": avg_draught,
                "max_Draught": d_max,
                "min_Draught": d_min,
                "draught_samples": v['d_cnt'],
                "draught_variation_alert": is_alert,
                "suspicious": is_suspicious
            })
    print(f"✅ Report saved to {OUTPUT_FILE}")
    print_memory_usage()

if __name__ == "__main__":
    run_parallel_sog_analysis()



