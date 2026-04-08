# Configuration Guide

## Overview

All project configuration is centralized in `src/config.py`. This single file controls:
- Input/output folder paths
- Processing parameters
- Column indices
- Validation rules
- Blacklists
- Analysis-specific thresholds

**Last Updated**: April 5, 2026

---

## 📁 Primary Folder Configuration

```python
PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
```

**Purpose**: Base directory for all maritime data

**How to change**: Uncomment and modify the alternative path:
```python
# PRIMARY_FOLDER = Path("/opt/A/NFS_Folder/Maritime_Shadow_Fleet_Detection")
```

---

## 📥 Input Data Configuration

```python
FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
FILE_PATTERN = "*.csv"
```

**Purpose**: Location and pattern for CSV files to analyze

**How to change**: 
- Modify `FOLDER_PATH` to point to your CSV data location
- Change `FILE_PATTERN` to match different file patterns (e.g., `"*.CSV"` for uppercase)

---

## 📤 Output Configuration

```python
OUTPUT_DATA_PATH = PRIMARY_FOLDER / "Data_analysis_and_outputs"
OUTPUT_REPORT_FOLDER = OUTPUT_DATA_PATH / "output3"
```

**Purpose**: Where analysis reports will be saved

**How to change**: 
- Modify paths to your preferred output location
- Reports are automatically created if they don't exist

---

## ⚙️ Processing Configuration

```python
MAX_WORKERS = 8
CHUNK_SIZE = 40000
```

**Purpose**: Control multiprocessing and memory usage

**Parameters**:
- `MAX_WORKERS`: Number of parallel processes (default: 8)
  - Increase for faster processing (uses more RAM)
  - Decrease for lower memory usage
  - Recommended: Match your CPU core count
  
- `CHUNK_SIZE`: Rows processed per chunk (default: 40,000)
  - Larger chunks = better performance, higher memory usage
  - Smaller chunks = lower memory, slower processing
  - Adjust based on your available RAM

**Performance Tips**:
- For 8GB RAM: Use `MAX_WORKERS=2`, `CHUNK_SIZE=20000`
- For 16GB RAM: Use `MAX_WORKERS=4`, `CHUNK_SIZE=40000`
- For 32GB+ RAM: Use `MAX_WORKERS=8`, `CHUNK_SIZE=80000`

---

## 🔍 Column Analysis Configuration

```python
DEFAULT_COLUMN_NAME = "Type of mobile"
DEFAULT_COLUMN_INDEX = 1  # Zero-based index (1 = second column)
```

**Purpose**: Default column for basic CSV scanning

**How to change**: 
- Update `DEFAULT_COLUMN_INDEX` to match your CSV structure
- Remember: Index is zero-based (0 = first column, 1 = second column, etc.)

---

## 🏷️ Master Index Analysis Configuration

```python
MMSI_COLUMN_INDEX = 2   # MMSI is in the 3rd column (zero-based)
IMO_COLUMN_INDEX = 10   # IMO is in the 11th column (zero-based)

MMSI_LENGTH = 9         # Valid MMSI must be exactly 9 digits
IMO_LENGTH = 7          # Valid IMO must be exactly 7 digits

BLACK_MMSI_LIST = {
    '000000000',
    '123456789',
    '987654321',
    '0',
    '111111111'
}

BLACK_IMO_LIST = {
    '0000000',
    '1234567',
    '7654321',
    '0'
}
```

**Purpose**: Column locations and validation rules for IMO/MMSI analysis

**How to change**: 
- Update column indices to match your CSV structure
- Add/remove values from blacklists as needed

---

## 🕐 Gap Analysis Configuration

```python
AIS_GAP_HOURS_THRESHOLD = 4       # Minimum gap in hours to flag
MIN_MOVEMENT_KM = 0.5             # Minimum movement in km to consider significant

GAP_ANALYSIS_KEY_MODE = "MMSI_ONLY"
# Options: "" (IMO primary, MMSI fallback), "MMSI_ONLY", "IMO_ONLY"

GAP_ANALYSIS_OUTPUT_FOLDER = OUTPUT_DATA_PATH / "gap_analysis_reports"
GAP_ANALYSIS_MAX_WORKERS = 8      # Number of parallel workers
```

**Purpose**: AIS gap detection thresholds and processing

**How to change**: 
- Adjust gap threshold (default: 4 hours)
- Modify minimum movement filter (default: 0.5 km)
- Change identity tracking mode based on your data quality

---

## 🚢 SOG and Draught Analysis Configuration

```python
SOG_DRUGHT_MOBILE_TYPES = {"Class A"}
SOG_DRUGHT_MIN_SAMPLES = 5
SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS = 80
SOG_DRUGHT_VARIATION_THRESHOLD_PCT = 20.0
SOG_DRUGHT_MAX_WORKERS = 8
SOG_DRUGHT_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "consolidated_speed_report.Class_A.csv"
```

**Purpose**: Speed and draught analysis parameters

**Parameters**:
- `SOG_DRUGHT_MOBILE_TYPES`: Which vessel types to analyze
- `SOG_DRUGHT_MIN_SAMPLES`: Minimum data points for reliable stats
- `SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS`: Speed threshold for suspicious vessels
- `SOG_DRUGHT_VARIATION_THRESHOLD_PCT`: Draught change alert threshold (%)
- `SOG_DRUGHT_MAX_WORKERS`: Parallel workers for processing

**How to change**: 
- Add "Class B" to mobile types to include Class B vessels
- Adjust thresholds based on your analysis needs

---

## 🎯 Vessel Proximity Analysis Configuration

```python
PROXIMITY_GRID_SIZE = 0.01              # Spatial grid size (degrees, ~1.1 km)
PROXIMITY_TIME_STEP = 600               # Time bucket (seconds, 600 = 10 min)
PROXIMITY_REQUIRED_WINDOWS = 12         # Consecutive windows (~2 hours)
PROXIMITY_MAX_DIST_KM = 0.5             # Max distance to be "close" (km)
PROXIMITY_SOG_DIFF_LIMIT = 1.0          # Max speed difference (knots)
PROXIMITY_MIN_SOG_RAW_FILTER = 0.1      # Minimum SOG to consider moving
PROXIMITY_MIN_AVG_MEETING_SOG = 0.5     # Min avg meeting speed for report
PROXIMITY_MIN_MAX_SPEED_GLOBAL = 1.0    # Min max speed for vessel filter
PROXIMITY_TMP_FOLDER = Path("/tmp/ais_processing")
PROXIMITY_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"
PROXIMITY_MAX_WORKERS = None            # Auto-detect CPU count (recommended)
PROXIMITY_MAX_MEMORY_TO_USE = 2         # Memory limit for sort (GB)
```

**Purpose**: Spatial and temporal parameters for detecting vessels sailing close together

**Parameters**:
- `PROXIMITY_GRID_SIZE`: Spatial bucket size (0.01° ≈ 1.1 km)
- `PROXIMITY_TIME_STEP`: Time window size (600 seconds = 10 minutes)
- `PROXIMITY_REQUIRED_WINDOWS`: Minimum consecutive windows to qualify as meeting
- `PROXIMITY_MAX_DIST_KM`: Maximum distance between vessels to be considered "close"
- `PROXIMITY_SOG_DIFF_LIMIT`: Maximum speed difference between vessels
- `PROXIMITY_MIN_SOG_RAW_FILTER`: Filter out stationary vessels (both < threshold)
- `PROXIMITY_MIN_AVG_MEETING_SOG`: Minimum average combined speed for reporting
- `PROXIMITY_MIN_MAX_SPEED_GLOBAL`: Global filter for active vessels
- `PROXIMITY_MAX_WORKERS`: Parallel workers (None = auto-detect CPU cores)
- `PROXIMITY_MAX_MEMORY_TO_USE`: Memory buffer for sort operation (GB)

**System Requirements**:
- **macOS**: Requires `gsort` from GNU coreutils (`brew install gsort`)
- **Linux**: Uses system `sort` (pre-installed)
- **Windows**: Use WSL or Git Bash for `sort` command

**Performance Tips**:
- Install `gsort` on macOS for 3-5x faster sorting
- Set `PROXIMITY_MAX_WORKERS = None` for automatic CPU core detection
- Adjust `PROXIMITY_MAX_MEMORY_TO_USE` based on available RAM
- For 8GB RAM: Use `PROXIMITY_MAX_WORKERS=2`, `PROXIMITY_MAX_MEMORY_TO_USE=1`
- For 16GB RAM: Use `PROXIMITY_MAX_WORKERS=4`, `PROXIMITY_MAX_MEMORY_TO_USE=2`
- For 32GB+ RAM: Use `PROXIMITY_MAX_WORKERS=None`, `PROXIMITY_MAX_MEMORY_TO_USE=4`

**How to change**: 
- Adjust thresholds based on your analysis requirements
- Tune worker count and memory based on system resources
- See `VESSEL_PROXIMITY_V0.0.10C_UPDATE.md` for detailed performance guide

**Purpose**: Detect vessels sailing close together

**Parameters**:
- `PROXIMITY_GRID_SIZE`: Spatial indexing resolution
- `PROXIMITY_TIME_STEP`: Temporal bucketing interval
- `PROXIMITY_REQUIRED_WINDOWS`: Minimum consecutive windows for meeting
- `PROXIMITY_MAX_DIST_KM`: Maximum distance between vessels
- `PROXIMITY_SOG_DIFF_LIMIT`: Maximum speed difference
- `PROXIMITY_MAX_WORKERS`: Workers for spatial analysis phase

**How to change**: 
- Decrease `PROXIMITY_MAX_DIST_KM` for stricter proximity detection
- Increase `PROXIMITY_REQUIRED_WINDOWS` for longer meeting detection
- Adjust `PROXIMITY_MAX_MEMORY_TO_USE` based on available RAM

---

## 🔎 Anomaly Detection Configuration

Uses shared configuration:
- `MAX_WORKERS` - Number of parallel workers
- Gap threshold: 2 hours (hardcoded)
- Draught change threshold: 5% (hardcoded)
- Memory limit: 2G (hardcoded)

**Output Files**:
- `laivai_dingimai.csv` - AIS gap records
- `mmsi_draught_change.csv` - Draught change records

---

## 📊 MMSI Outlier Analysis Configuration

```python
NUM_SHARDS = 20           # Number of shards for parallel processing
MAX_DISTANCE_KM = 50.0    # Maximum allowed distance between positions
```

**Purpose**: Position jump detection parameters

**Parameters**:
- `NUM_SHARDS`: Sharding buckets for parallel processing
- `MAX_DISTANCE_KM`: Threshold for suspicious position jumps

**How to change**: 
- Increase `NUM_SHARDS` for better parallelization (more memory)
- Decrease `MAX_DISTANCE_KM` for stricter outlier detection

---

## 📈 Final Report & DFSI Configuration

DFSI Formula (hardcoded in `final_report.py`):
```
DFSI = (Max_Gap / 2) + (Total_Jump / 10) + (Draught_Changes * 15)
```

**Weight Factors**:
- Gap hours: divided by 2
- Jump distance (km): divided by 10
- Each draught change: multiplied by 15

**Output Files**:
- `dfsi_full_list.csv` - All vessels with DFSI scores
- `dfsi_high_risk_sorted.csv` - High-risk vessels (DFSI > 0), sorted
- `Final_Comprehensive_Report.txt` - Complete report

**How to change**: 
- Modify formula weights in `src/final_report.py`
- Increase draught change weight for stricter detection

---

## 🖥️ System Configuration

```python
FILE_ENCODING = 'utf-8'
MEMORY_WARNING_THRESHOLD = 90.0  # Warning if RAM usage exceeds this %
```

**Purpose**: System-level settings

---

## Quick Reference: Performance Tuning

### For Limited RAM (< 8GB)
```python
MAX_WORKERS = 2
CHUNK_SIZE = 20000
PROXIMITY_MAX_WORKERS = 2
PROXIMITY_MAX_MEMORY_TO_USE = 1
```

### For Moderate RAM (16GB)
```python
MAX_WORKERS = 4
CHUNK_SIZE = 40000
PROXIMITY_MAX_WORKERS = 4
PROXIMITY_MAX_MEMORY_TO_USE = 2
```

### For High-End Systems (32GB+)
```python
MAX_WORKERS = 8
CHUNK_SIZE = 80000
PROXIMITY_MAX_WORKERS = 8
PROXIMITY_MAX_MEMORY_TO_USE = 4
```

---

## Troubleshooting

### Out of Memory Errors
1. Reduce `MAX_WORKERS`
2. Reduce `CHUNK_SIZE`
3. Reduce `PROXIMITY_MAX_MEMORY_TO_USE`
4. Close other applications

### Slow Processing
1. Increase `MAX_WORKERS` (if CPU has free cores)
2. Increase `CHUNK_SIZE` (if RAM available)
3. Use SSD storage for faster I/O

### Missing Data in Reports
1. Check column indices match your CSV structure
2. Verify input data contains expected columns
3. Check blacklist isn't filtering valid data
4. Adjust analysis thresholds

---

## Configuration Validation

On startup, the application validates configuration and displays:
- ✅ All paths and settings
- ⚠️ Warnings for non-critical issues
- ❌ Errors that must be fixed

Always review the configuration summary on startup!

---

**For more details**: See [`README.md`](README.md) for usage examples.

### 🚢 Master Index Analysis Configuration

```python
MMSI_COLUMN_INDEX = 2   # MMSI is in the 3rd column
IMO_COLUMN_INDEX = 10   # IMO is in the 11th column

MMSI_LENGTH = 9         # Valid MMSI must be exactly 9 digits
IMO_LENGTH = 7          # Valid IMO must be exactly 7 digits
```

**Purpose**: Column positions and validation rules for vessel identifiers

**How to change**: 
- Adjust column indices if your CSV has different structure
- Validation lengths should match international standards

---

### 🚫 Blacklist Configuration

```python
BLACK_MMSI_LIST = {
    '000000000',
    '123456789',
    '987654321',
    '0',
    '111111111'
}

BLACK_IMO_LIST = {
    '0000000',
    '1234567',
    '7654321',
    '0'
}
```

**Purpose**: Filter out invalid/test vessel identifiers

**How to change**: 
- Add more values to the sets
- Values are automatically excluded from analysis
- Useful for filtering test data or known invalid entries

---

### 🖥️ System Configuration

```python
FILE_ENCODING = 'utf-8'
MEMORY_WARNING_THRESHOLD = 90.0
```

**Purpose**: System-level settings

**Parameters**:
- `FILE_ENCODING`: Character encoding for CSV files
- `MEMORY_WARNING_THRESHOLD`: RAM usage percentage for warnings

---

## Configuration Validation

The application automatically validates configuration on startup:

```python
from config import validate_configuration

warnings, errors = validate_configuration()
```

**Checks performed**:
- ✅ Input folder exists
- ✅ Output folder can be created
- ✅ Worker count is reasonable (1-32)
- ✅ Chunk size is appropriate (1000-1,000,000)

**Error handling**:
- **Errors**: Application will not start until fixed
- **Warnings**: Shown but application continues

---

## Configuration Display

On application startup, you'll see a complete configuration summary:

```
============================================================
CONFIGURATION SUMMARY
============================================================

📁 INPUT DATA:
  Primary Folder:    /Users/valdas/Maritime_Shadow_Fleet_Detection
  CSV Files Folder:  /Users/valdas/Maritime_Shadow_Fleet_Detection/Duomenys_CSV_Formate.2026
  File Pattern:      *.csv

📤 OUTPUT DATA:
  Output Base Path:  /Users/valdas/Maritime_Shadow_Fleet_Detection/Data_analysis_and_outputs
  Reports Folder:    /Users/valdas/Maritime_Shadow_Fleet_Detection/Data_analysis_and_outputs/output3

⚙️  PROCESSING SETTINGS:
  Worker Processes:  4
  Chunk Size:        40,000 rows

🔍 COLUMN ANALYSIS:
  Default Column:    'Type of mobile'
  Column Index:      1 (0-based)

🚢 MASTER INDEX ANALYSIS:
  MMSI Column:       Index 2 (column 3)
  IMO Column:        Index 10 (column 11)
  MMSI Validation:   9 digits
  IMO Validation:    7 digits
  Blacklisted MMSI:  5 values
  Blacklisted IMO:   4 values

============================================================
```

---

## Best Practices

### 1. **Single Source of Truth**
Always modify `src/config.py` - never hardcode paths or settings in other files.

### 2. **Version Control**
Keep `config.py` under version control but consider:
- Using environment-specific configs
- Adding sensitive paths to `.gitignore`
- Creating `config.local.py` for personal overrides

### 3. **Testing Changes**
After modifying configuration:
```bash
python -c "from src.config import validate_configuration; print(validate_configuration())"
```

### 4. **Performance Tuning**
Monitor memory usage during initial runs:
- If system becomes unresponsive: Reduce `MAX_WORKERS` or `CHUNK_SIZE`
- If processing is too slow: Increase both values (if RAM allows)

### 5. **Documentation**
Update this guide when adding new configuration options.

---

## Troubleshooting

### "Input folder does not exist"
- Check `PRIMARY_FOLDER` and `FOLDER_PATH` in `config.py`
- Verify the folder exists on your system
- Check permissions

### "Memory error" or system slowdown
- Reduce `MAX_WORKERS` to 2 or 1
- Reduce `CHUNK_SIZE` to 10000 or 20000
- Close other applications

### "No data found" in reports
- Verify CSV files exist in `FOLDER_PATH`
- Check column indices match your CSV structure
- Ensure data passes validation (correct length, not blacklisted)

### Import errors
- Make sure you're running from project root
- Verify `src/config.py` exists
- Check Python path includes `src/` directory

---

## Example Configurations

### Minimal Memory Setup (4GB RAM)
```python
MAX_WORKERS = 2
CHUNK_SIZE = 10000
```

### High Performance Setup (32GB RAM)
```python
MAX_WORKERS = 8
CHUNK_SIZE = 100000
```

### Alternative Data Location
```python
PRIMARY_FOLDER = Path("/mnt/data/maritime")
FOLDER_PATH = PRIMARY_FOLDER / "ais_data"
OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "analysis_results"
```

### Custom CSV Structure
```python
# If MMSI is in column 1 and IMO in column 5
MMSI_COLUMN_INDEX = 0
IMO_COLUMN_INDEX = 4
```

---

## API Reference

### `print_configuration_summary()`
Displays complete configuration summary to console.

### `validate_configuration() -> tuple`
Returns `(warnings_list, errors_list)` for configuration validation.

### Configuration Constants
All configuration values are module-level constants that can be imported:
```python
from config import (
    PRIMARY_FOLDER,
    FOLDER_PATH,
    OUTPUT_REPORT_FOLDER,
    MAX_WORKERS,
    CHUNK_SIZE,
    # ... and all other settings
)
```
