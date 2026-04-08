import csv
import math
import os
import tempfile
import threading
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Import from config module (relative import)
try:
    from config import (
        FOLDER_PATH,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        GAP_ANALYSIS_KEY_MODE,
        GAP_ANALYSIS_MAX_WORKERS,
        AIS_GAP_HOURS_THRESHOLD,
        MIN_MOVEMENT_KM
    )
except ImportError:
    from .config import (
        FOLDER_PATH,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        GAP_ANALYSIS_KEY_MODE,
        GAP_ANALYSIS_MAX_WORKERS,
        AIS_GAP_HOURS_THRESHOLD,
        MIN_MOVEMENT_KM
    )

# ============================================================
# CONFIGURATION (from config.py)
# ============================================================

# Ensure output folder exists
OUTPUT_REPORT_FOLDER.mkdir(parents=True, exist_ok=True)

# Identity key mode
KEY_MODE = GAP_ANALYSIS_KEY_MODE

# MMSI whitelist file (master list)
CLASS_A_MMSI_LIST = OUTPUT_REPORT_FOLDER / "master_MMSI_data.csv"

# Output report file with timestamp
OUTPUT_REPORT_FILE = OUTPUT_REPORT_FOLDER / f"gap_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{KEY_MODE}.csv"
OUTPUT_REPORT_FILE = OUTPUT_REPORT_FOLDER / f"gap_analysis_report_.csv"


_BAD = {"", "none", "unknown", "undefined", "-", "+"}


# ============================================================
# FAST PARSING (timestamp fixed: "DD/MM/YYYY HH:MM:SS")
# ============================================================

@lru_cache(maxsize=4096)
def _date_to_ymd(date_part: str):
    """date_part = 'DD/MM/YYYY' -> (YYYY, MM, DD)"""
    d = int(date_part[0:2])
    m = int(date_part[3:5])
    y = int(date_part[6:10])
    return y, m, d

def parse_ts_epoch_fixed(s: str):
    """Return epoch seconds (int) or None. Expected: 'DD/MM/YYYY HH:MM:SS'."""
    if not s:
        return None
    s = s.strip()
    if len(s) != 19 or s[2] != "/" or s[5] != "/" or s[10] != " ":
        return None
    try:
        y, m, d = _date_to_ymd(s[0:10])
        hh = int(s[11:13])
        mm = int(s[14:16])
        ss = int(s[17:19])
        return int(datetime(y, m, d, hh, mm, ss).timestamp())
    except ValueError:
        return None

def parse_float(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in _BAD:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def is_valid_position(lat, lon):
    return (
        lat is not None and lon is not None
        and -90.0 <= lat <= 90.0
        and -180.0 <= lon <= 180.0
    )

def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (math.sin(dlat / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * (math.sin(dlon / 2) ** 2))
    return 2 * R * math.asin(math.sqrt(a))


# ============================================================
# WHITELIST LOADER
# ============================================================

def load_mmsi_list(file_path: Path):
    """Loads MMSI whitelist CSV (first column MMSI)."""
    valid = set()
    if not file_path.exists():
        print(f"Warning: whitelist not found: {file_path}")
        return valid

    with open(file_path, "r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        for row in r:
            if not row:
                continue
            v = (row[0] or "").strip().replace('"', "")
            if not v:
                continue
            if v.startswith("#") or v.lower() in ("mmsi", "name"):
                continue
            valid.add(v)

    print(f"Loaded {len(valid)} MMSIs from: {file_path}")
    return valid


# ============================================================
# IDENTITY KEY with KEY_MODE switch
# ship_key is either ("IMO", imo_value) or ("MMSI", mmsi_value)
# return None means "skip record" (used in IMO_ONLY when no valid IMO)
# ============================================================

def make_ship_key(mmsi: str, imo_raw: str):
    imo = (imo_raw or "").strip()
    imo_ok = bool(imo) and (imo.lower() not in _BAD)

    if KEY_MODE == "MMSI_ONLY":
        return ("MMSI", mmsi)

    if KEY_MODE == "IMO_ONLY":
        return ("IMO", imo) if imo_ok else None

    # default: IMO primary, MMSI fallback
    return ("IMO", imo) if imo_ok else ("MMSI", mmsi)


# ============================================================
# WORKER: scan file in parallel; commit in file order to shared map
# Shared global map stores: ship_key -> (ts, lat, lon, last_mmsi)
# ============================================================

def worker_process_and_commit(
    file_path: Path,
    file_idx: int,
    class_a_set: set,
    tmp_dir: Path,
    commit_cond: threading.Condition,
    next_commit_idx_ref: list,      # mutable int holder: [0]
    global_last_seen: dict,         # ship_key -> (ts, lat, lon, last_mmsi)
    out_fh                          # opened output file handle
):
    # Local per-file state
    local_last_seen = {}  # ship_key -> (ts, lat, lon, last_mmsi)
    start_state = {}      # ship_key -> (ts, lat, lon, first_mmsi)

    tmp_gaps = tmp_dir / f"gaps_{file_idx:05d}_{file_path.stem}.tmp.csv"

    # Localize for speed
    _parse_ts = parse_ts_epoch_fixed
    _pf = parse_float
    _valid = is_valid_position
    _dist = haversine_km
    _thr = AIS_GAP_HOURS_THRESHOLD
    _min_move = MIN_MOVEMENT_KM
    _dt = datetime.fromtimestamp

    processed = kept = within_gaps = 0

    # ---------- Parallel scan ----------
    with open(tmp_gaps, "w", newline="", encoding="utf-8") as tg:
        w = csv.writer(tg)

        with open(file_path, "r", encoding="utf-8", newline="", buffering=1024 * 1024) as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                if row[0].startswith("#"):
                    continue
                if len(row) < 11:  # need up to IMO index 10
                    continue

                processed += 1

                ts = _parse_ts(row[0])
                if ts is None:
                    continue

                type_mobile = (row[1] or "").strip()
                if type_mobile not in ("Class A", "Class B"):
                    continue

                mmsi = (row[2] or "").strip().replace('"', "")
                if not mmsi:
                    continue

                # whitelist filter based on MMSI (Class A only)
                if mmsi not in class_a_set:
                    continue

                lat = _pf(row[3])
                lon = _pf(row[4])
                if not _valid(lat, lon):
                    continue

                ship_key = make_ship_key(mmsi, row[10])
                if ship_key is None:
                    # IMO_ONLY mode with missing/Unknown IMO
                    continue

                kept += 1

                if ship_key not in start_state:
                    start_state[ship_key] = (ts, lat, lon, mmsi)

                prev = local_last_seen.get(ship_key)
                if prev is not None:
                    prev_ts, prev_lat, prev_lon, prev_mmsi = prev
                    gap_hours = (ts - prev_ts) / 3600.0
                    if gap_hours > _thr:
                        d = _dist(prev_lat, prev_lon, lat, lon)
                        if d >= _min_move:
                            kind, idv = ship_key
                            out_imo = idv if kind == "IMO" else ""
                            # show MMSI observed at previous point (useful for IMO-key ships)
                            out_mmsi = prev_mmsi if prev_mmsi else mmsi

                            # MMSI_changed meaningful only when identity is IMO-based and mode isn't MMSI_ONLY
                            mmsi_changed = 1 if (kind == "IMO" and KEY_MODE != "MMSI_ONLY"
                                                 and prev_mmsi and prev_mmsi != mmsi) else 0

                            w.writerow([
                                out_mmsi,
                                out_imo,
                                _dt(prev_ts).strftime("%Y-%m-%d %H:%M:%S"),
                                _dt(ts).strftime("%Y-%m-%d %H:%M:%S"),
                                f"{gap_hours:.2f}",
                                f"{d:.2f}",
                                mmsi_changed
                            ])
                            within_gaps += 1

                local_last_seen[ship_key] = (ts, lat, lon, mmsi)

    end_state = local_last_seen

    # ---------- Ordered commit (shared map correctness) ----------
    boundary_gaps = 0
    with commit_cond:
        while file_idx != next_commit_idx_ref[0]:
            commit_cond.wait()

        # 1) boundary gaps: shared global -> this file's first record
        for ship_key, (ts, lat, lon, first_mmsi) in start_state.items():
            prev = global_last_seen.get(ship_key)
            if prev is None:
                continue

            prev_ts, prev_lat, prev_lon, prev_mmsi = prev
            gap_hours = (ts - prev_ts) / 3600.0
            if gap_hours > AIS_GAP_HOURS_THRESHOLD:
                d = haversine_km(prev_lat, prev_lon, lat, lon)
                if d >= MIN_MOVEMENT_KM:
                    kind, idv = ship_key
                    out_imo = idv if kind == "IMO" else ""
                    out_mmsi = prev_mmsi if prev_mmsi else first_mmsi

                    mmsi_changed = 1 if (kind == "IMO" and KEY_MODE != "MMSI_ONLY"
                                         and prev_mmsi and prev_mmsi != first_mmsi) else 0

                    out_fh.write(
                        f"{out_mmsi},{out_imo},"
                        f"{datetime.fromtimestamp(prev_ts).strftime('%Y-%m-%d %H:%M:%S')},"
                        f"{datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')},"
                        f"{gap_hours:.2f},{d:.2f},{mmsi_changed}\n"
                    )
                    boundary_gaps += 1

        # 2) append within-file gaps from temp file
        if tmp_gaps.exists():
            with open(tmp_gaps, "r", encoding="utf-8", newline="") as g:
                for line in g:
                    if line.strip():
                        out_fh.write(line)

        # 3) update shared global map
        global_last_seen.update(end_state)

        # advance commit pointer and wake next file thread
        next_commit_idx_ref[0] += 1
        commit_cond.notify_all()

    return file_idx, file_path.name, processed, kept, within_gaps, boundary_gaps, len(end_state)


# ============================================================
# MAIN
# ============================================================

def run_gap_analysis(
    folder_path: Path = None,
    output_folder: Path = None,
    class_a_whitelist: Path = None,
    key_mode: str = None,
    max_workers: int = None,
    gap_hours_threshold: float = None,
    min_movement_km: float = None
) -> Path:
    """
    Run gap analysis on CSV files to detect AIS transmission gaps.
    
    Args:
        folder_path: Path to folder containing CSV files (default: from config)
        output_folder: Path to output folder for reports (default: from config)
        class_a_whitelist: Path to MMSI whitelist CSV (default: master_MMSI_data.csv)
        key_mode: Identity key mode - "", "MMSI_ONLY", or "IMO_ONLY" (default: from config)
        max_workers: Number of parallel workers (default: from config)
        gap_hours_threshold: Minimum gap in hours to flag (default: from config)
        min_movement_km: Minimum movement in km to consider significant (default: from config)
        
    Returns:
        Path to the generated report file
    """
    global AIS_GAP_HOURS_THRESHOLD, MIN_MOVEMENT_KM, KEY_MODE, OUTPUT_REPORT_FILE
    
    # Use provided parameters or fall back to config
    # Default to Clean_AIS_DB folder if available, otherwise use FOLDER_PATH
    if folder_path is None:
        clean_db_file = CLEAN_DB_FOLDER_PATH / "Clean_AIS_DB.csv"
        if clean_db_file.exists():
            folder_path = CLEAN_DB_FOLDER_PATH
        else:
            folder_path = FOLDER_PATH
    
    output_folder = output_folder or OUTPUT_REPORT_FOLDER
    class_a_whitelist = class_a_whitelist or CLASS_A_MMSI_LIST
    key_mode = key_mode if key_mode is not None else GAP_ANALYSIS_KEY_MODE
    max_workers = max_workers if max_workers is not None else GAP_ANALYSIS_MAX_WORKERS
    gap_hours_threshold = gap_hours_threshold if gap_hours_threshold is not None else AIS_GAP_HOURS_THRESHOLD
    min_movement_km = min_movement_km if min_movement_km is not None else MIN_MOVEMENT_KM
    
    # Update global variables for worker functions
    AIS_GAP_HOURS_THRESHOLD = gap_hours_threshold
    MIN_MOVEMENT_KM = min_movement_km
    KEY_MODE = key_mode
#    OUTPUT_REPORT_FILE = output_folder / f"gap_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{KEY_MODE}.csv"
    OUTPUT_REPORT_FILE = output_folder / f"gap_analysis_report_.csv"

    print("Loading MMSI whitelist...")
    class_a_set = load_mmsi_list(class_a_whitelist)
    if not class_a_set:
        print("Error: MMSI whitelist is empty/unavailable.")
        return None

    csv_files = sorted([p for p in Path(folder_path).iterdir()
                        if p.is_file() and p.suffix.lower() == ".csv"])
    if not csv_files:
        print("No CSV files found.")
        return None

    print(f"KEY_MODE = {KEY_MODE!r}")
    print(f"Loaded {len(class_a_set)} MMSIs from whitelist")

    # Shared in-memory ship state map:
    # ship_key -> (ts, lat, lon, last_mmsi)
    global_last_seen = {}

    # Ordered commit controller
    commit_cond = threading.Condition()
    next_commit_idx_ref = [0]

    # NVMe: start at 8–16. Too many concurrent readers can reduce throughput.
    actual_workers = min(len(csv_files), max_workers, (os.cpu_count() or 16))
    print(f"Found {len(csv_files)} files. Using ThreadPoolExecutor with {actual_workers} workers.")

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)

        with open(OUTPUT_REPORT_FILE, "w", encoding="utf-8", newline="") as out_fh:
            out_fh.write("MMSI,IMO,Start_Time,End_Time,Gap_Hours,Distance_km,MMSI_changed\n")

            futures = []
            with ThreadPoolExecutor(max_workers=actual_workers) as ex:
                for idx, fp in enumerate(csv_files):
                    futures.append(ex.submit(
                        worker_process_and_commit,
                        fp, idx,
                        class_a_set,
                        tmp_dir,
                        commit_cond, next_commit_idx_ref,
                        global_last_seen,
                        out_fh
                    ))

                # join + stats
                for fut in futures:
                    idx, name, processed, kept, within_gaps, boundary_gaps, ships = fut.result()
                    print(f"[file#{idx:03d}] {name}: processed={processed:,} kept={kept:,} "
                          f"within_gaps={within_gaps:,} boundary_gaps={boundary_gaps:,} ships={ships:,}")

    print(f"\nReport saved to: {OUTPUT_REPORT_FILE}")
    print(f"Shared ship-state map size: {len(global_last_seen):,} ship keys")
    
    return OUTPUT_REPORT_FILE


def main():
    """Main function to run gap analysis with default configuration."""
    run_gap_analysis()


if __name__ == "__main__":
    main()


