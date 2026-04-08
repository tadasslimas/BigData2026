# Gap Analysis Integration Summary

## Overview

Successfully integrated the gap analysis functionality from `31.Gap_analysis_and_report_preparation.py` into the CSV_DATA_ANALYSIS project.

## Changes Made

### 1. File Renaming
- **Before**: `src/31.Gap_analysis_and_report_preparation.py`
- **After**: `src/gap_analysis.py`
- **Reason**: Follow Python naming conventions (lowercase with underscores)

### 2. Configuration Integration (`src/config.py`)

Added comprehensive gap analysis configuration:

```python
# Gap analysis thresholds
AIS_GAP_HOURS_THRESHOLD = 4       # Minimum gap in hours to flag
MIN_MOVEMENT_KM = 0.5             # Minimum movement in km

# Identity key mode
GAP_ANALYSIS_KEY_MODE = "MMSI_ONLY"  # Options: "", "MMSI_ONLY", "IMO_ONLY"

# Output settings
GAP_ANALYSIS_OUTPUT_FOLDER = OUTPUT_DATA_PATH / "gap_analysis_reports"

# Whitelist files
GAP_ANALYSIS_CLASS_A_WHITELIST = OUTPUT_DATA_PATH / "output/by_type/mmsi_Class_A.csv"
GAP_ANALYSIS_CLASS_B_WHITELIST = OUTPUT_DATA_PATH / "output/by_type/mmsi_Class_B.csv"

# Performance settings
GAP_ANALYSIS_MAX_WORKERS = 16     # Number of parallel workers
```

### 3. Module Refactoring (`src/gap_analysis.py`)

**Key Changes**:
- Imported configuration from `config.py` instead of hardcoded values
- Created `run_gap_analysis()` function with configurable parameters
- Maintained backward compatibility with `main()` function
- Preserved all original functionality:
  - Multi-threaded file processing
  - Ordered commit for cross-file gap detection
  - MMSI whitelist filtering
  - IMO/MMSI identity tracking
  - Haversine distance calculation
  - Temporary file management

**New Function Signature**:
```python
def run_gap_analysis(
    folder_path: Path = None,
    output_folder: Path = None,
    class_a_whitelist: Path = None,
    class_b_whitelist: Path = None,
    key_mode: str = None,
    max_workers: int = None,
    gap_hours_threshold: float = None,
    min_movement_km: float = None
) -> Path
```

### 4. Main Entry Point (`main.py`)

**Added**:
- Import for `run_gap_analysis` function
- Import for gap analysis configuration constants
- New `run_gap_analysis_menu()` function with detailed output
- Updated interactive menu with gap analysis option

**Menu Changes**:
```
BEFORE:
  1. Column Analysis
  2. Master Index Analysis
  3. Run Both Analyses
  4. Exit

AFTER:
  1. Column Analysis
  2. Master Index Analysis
  3. Gap Analysis - Detect AIS transmission gaps
  4. Run All Analyses
  5. Exit
```

### 5. Documentation

**Created**: `GAP_ANALYSIS_GUIDE.md`
- Comprehensive feature documentation
- Configuration guide
- Usage examples (interactive, direct, programmatic)
- Output format explanation
- Performance optimization tips
- Troubleshooting section
- Integration workflow examples

**Updated**: `README.md`
- Added gap analysis to features list
- Updated project structure
- Added reference to GAP_ANALYSIS_GUIDE.md

## Features Preserved

All original gap analysis features remain intact:

✅ Multi-threaded processing with ThreadPoolExecutor  
✅ Ordered commit for correct cross-file gap detection  
✅ IMO/MMSI identity tracking with KEY_MODE support  
✅ MMSI whitelist filtering (Class A and Class B)  
✅ Haversine distance calculation  
✅ Configurable gap threshold and minimum movement  
✅ Temporary file management  
✅ Comprehensive statistics output  
✅ Timestamp parsing with LRU cache optimization  

## Usage

### Option 1: Interactive Menu
```bash
python main.py
# Select option 3 for Gap Analysis
```

### Option 2: Direct Execution
```bash
python src/gap_analysis.py
```

### Option 3: Programmatic
```python
from src.gap_analysis import run_gap_analysis

# With default configuration
report_path = run_gap_analysis()

# With custom parameters
report_path = run_gap_analysis(
    gap_hours_threshold=6,
    min_movement_km=1.0,
    key_mode="IMO_ONLY"
)
```

## Prerequisites

Before running gap analysis:

1. **Generate MMSI Whitelists**: Run master index analysis first to create:
   - `Data_analysis_and_outputs/output/by_type/mmsi_Class_A.csv`
   - `Data_analysis_and_outputs/output/by_type/mmsi_Class_B.csv`

2. **CSV Data Format**: Ensure input CSV files have:
   - Column 0: Timestamp (DD/MM/YYYY HH:MM:SS)
   - Column 1: Type of mobile (Class A / Class B)
   - Column 2: MMSI (9 digits)
   - Column 3: Latitude
   - Column 4: Longitude
   - Column 10: IMO number (7 digits)

## Output

Gap analysis reports are saved to:
```
Data_analysis_and_outputs/gap_analysis_reports/gap_analysis_report_YYYYMMDD_HHMMSS.{KEY_MODE}.csv
```

**Report Columns**:
- MMSI: Vessel's MMSI number
- IMO: Vessel's IMO number (if available)
- Start_Time: Last position before gap
- End_Time: First position after gap
- Gap_Hours: Duration of transmission gap
- Distance_km: Distance traveled during gap
- MMSI_changed: Flag for MMSI identity changes

## Configuration Customization

All settings can be adjusted in `src/config.py`:

```python
# More sensitive gap detection
AIS_GAP_HOURS_THRESHOLD = 2       # Detect gaps > 2 hours
MIN_MOVEMENT_KM = 0.2             # Detect movement > 0.2 km

# Different identity tracking
GAP_ANALYSIS_KEY_MODE = ""        # IMO primary, MMSI fallback

# Performance tuning
GAP_ANALYSIS_MAX_WORKERS = 8      # Reduce for slower storage
```

## Testing

No syntax errors detected. All files validated successfully.

## Next Steps

1. ✅ Integration complete
2. ⏳ Run master index analysis to generate MMSI whitelists
3. ⏳ Run gap analysis on your dataset
4. ⏳ Review gap analysis reports for suspicious vessel behavior
5. ⏳ (Optional) Create unit tests for gap analysis module

## Benefits

- **Centralized Configuration**: All settings in one place (`config.py`)
- **Modular Design**: Gap analysis can be run standalone or as part of main menu
- **Consistent Interface**: Follows same patterns as other analysis modules
- **Comprehensive Documentation**: Easy to understand and use
- **Maintainable**: Clean separation of concerns
- **Extensible**: Easy to add new features or modify existing ones

## Support

For detailed usage instructions, see:
- [`GAP_ANALYSIS_GUIDE.md`](GAP_ANALYSIS_GUIDE.md) - Complete gap analysis documentation
- [`README.md`](README.md) - Project overview and quick start
