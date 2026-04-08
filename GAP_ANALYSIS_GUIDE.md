# Gap Analysis Guide

## Overview

The Gap Analysis module detects AIS (Automatic Identification System) transmission gaps in maritime vessel tracking data. These gaps may indicate:
- AIS transponder manipulation
- Potential spoofing activities
- Equipment malfunctions
- Intentional signal blackouts

## How It Works

The gap analysis module:

1. **Processes CSV files** containing AIS position reports
2. **Tracks vessels** by IMO number (primary) or MMSI (fallback)
3. **Detects gaps** where transmission intervals exceed the threshold
4. **Calculates distance** traveled between last known and next known positions
5. **Flags suspicious gaps** where vessels moved significantly during the blackout

## Configuration

All settings are in `src/config.py`:

```python
# Gap analysis thresholds
AIS_GAP_HOURS_THRESHOLD = 4       # Minimum gap in hours to flag
MIN_MOVEMENT_KM = 0.5             # Minimum movement in km to consider significant

# Identity key mode
GAP_ANALYSIS_KEY_MODE = "MMSI_ONLY"  # Options: "", "MMSI_ONLY", "IMO_ONLY"

# Output settings
GAP_ANALYSIS_OUTPUT_FOLDER = OUTPUT_DATA_PATH / "gap_analysis_reports"

# Performance settings
GAP_ANALYSIS_MAX_WORKERS = 16     # Number of parallel workers
```

### Identity Key Modes

- **`""` (Empty string)**: IMO primary, MMSI fallback (recommended)
- **`"MMSI_ONLY"`**: Track vessels by MMSI only (ignores IMO)
- **`"IMO_ONLY"`**: Track vessels by IMO only (skips records without valid IMO)

## Prerequisites

Before running gap analysis, ensure you have:

1. **MMSI Whitelist Files**:
   - `Data_analysis_and_outputs/output/by_type/mmsi_Class_A.csv`
   - `Data_analysis_and_outputs/output/by_type/mmsi_Class_B.csv`

2. **CSV Data Files** with the following columns:
   - Column 0: Timestamp (DD/MM/YYYY HH:MM:SS format)
   - Column 1: Type of mobile (Class A / Class B)
   - Column 2: MMSI (9-digit number)
   - Column 3: Latitude
   - Column 4: Longitude
   - Column 10: IMO number (7-digit number)

## Running Gap Analysis

### Option 1: Interactive Menu

```bash
python main.py
```

Select option `3` for Gap Analysis.

### Option 2: Direct Module Execution

```bash
python src/gap_analysis.py
```

### Option 3: Programmatic Usage

```python
from src.gap_analysis import run_gap_analysis
from pathlib import Path

# Run with default configuration
report_path = run_gap_analysis()

# Run with custom parameters
report_path = run_gap_analysis(
    folder_path=Path("/path/to/csv/files"),
    output_folder=Path("/path/to/output"),
    key_mode="MMSI_ONLY",
    gap_hours_threshold=6,
    min_movement_km=1.0,
    max_workers=8
)
```

## Output Format

The gap analysis report is a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| MMSI | MMSI number observed at the gap |
| IMO | IMO number (if available) |
| Start_Time | Timestamp of last position before gap |
| End_Time | Timestamp of first position after gap |
| Gap_Hours | Duration of transmission gap in hours |
| Distance_km | Distance traveled during gap in kilometers |
| MMSI_changed | Flag indicating if MMSI changed (1=changed, 0=unchanged) |

### Example Output

```csv
MMSI,IMO,Start_Time,End_Time,Gap_Hours,Distance_km,MMSI_changed
211234567,9876543,2025-03-15 08:30:00,2025-03-15 14:45:00,6.25,127.43,0
311987654,,2025-03-15 10:00:00,2025-03-15 16:30:00,6.50,89.21,0
```

## Performance Optimization

The gap analysis uses parallel processing with ThreadPoolExecutor:

- **Default workers**: 16 (suitable for NVMe SSDs)
- **Recommended range**: 8-16 workers
- **Too many workers**: Can reduce throughput due to I/O contention
- **Too few workers**: Underutilizes available resources

Adjust `GAP_ANALYSIS_MAX_WORKERS` in `config.py` based on your storage speed.

## Interpreting Results

### High-Risk Indicators

1. **Long gaps + Large distances**: Gaps > 4 hours with movement > 50 km
2. **Repeated gaps**: Same vessel with multiple gaps
3. **MMSI changes**: Vessels changing MMSI during gaps (potential identity spoofing)
4. **Strategic locations**: Gaps near ports, borders, or restricted areas

### False Positive Reduction

The module filters out:
- Gaps with minimal movement (< 0.5 km by default)
- Records with invalid coordinates
- Vessels not in the MMSI whitelist
- Records with missing or malformed timestamps

## Troubleshooting

### Common Issues

**"Both whitelists are empty/unavailable"**
- Run master index analysis first to generate MMSI whitelist files
- Verify whitelist files exist in `Data_analysis_and_outputs/output/by_type/`

**"No CSV files found"**
- Check `FOLDER_PATH` configuration in `config.py`
- Ensure CSV files are in the specified directory

**Slow processing**
- Increase `GAP_ANALYSIS_MAX_WORKERS` if you have fast storage
- Ensure sufficient RAM (processing is memory-efficient but benefits from available memory)

**Missing gaps**
- Lower `AIS_GAP_HOURS_THRESHOLD` to detect shorter gaps
- Lower `MIN_MOVEMENT_KM` to catch smaller movements

## Integration with Other Analyses

For comprehensive maritime data analysis:

1. **Column Analysis** (`main.py` option 1): Extract unique mobile types
2. **Master Index Analysis** (`main.py` option 2): Create IMO/MMSI relationship maps
3. **Gap Analysis** (`main.py` option 3): Detect transmission gaps

Run all analyses in sequence for complete vessel behavior profiling.

## Example Workflow

```bash
# 1. Run column analysis to understand data composition
python main.py
# Select option 1

# 2. Run master index to create MMSI whitelists
python main.py
# Select option 2

# 3. Run gap analysis to detect suspicious behavior
python main.py
# Select option 3

# 4. Review gap analysis report in:
# Data_analysis_and_outputs/gap_analysis_reports/gap_analysis_report_YYYYMMDD_HHMMSS.MMSI_ONLY.csv
```

## Technical Details

### Memory Efficiency

- Processes files line-by-line (does not load entire files into memory)
- Uses temporary files for intermediate gap detection
- Shared in-memory state map for cross-file vessel tracking

### Parallel Processing

- Files processed in parallel using ThreadPoolExecutor
- Ordered commit ensures correct cross-file gap detection
- Thread-safe state management with condition variables

### Timestamp Parsing

- Expected format: `DD/MM/YYYY HH:MM:SS`
- Uses LRU cache for fast date parsing
- Invalid timestamps are skipped

## References

- Maritime Shadow Fleet Detection Project
- AIS Data Analysis Best Practices
- IMO Number Verification: 7-digit with checksum
- MMSI Number Format: 9-digit maritime mobile service identity
