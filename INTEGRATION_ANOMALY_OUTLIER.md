# Integration Summary: Anomaly Detection and MMSI Outlier Analysis

## Overview
Successfully integrated two new analysis modules into the CSV_DATA_ANALYSIS project:
- **Anomaly Detection** (`anomaly_detection.py`) - Gap and draught change analysis
- **MMSI Outlier Analysis** (`mmsi_outlier_analysis.py`) - Position jump detection

## Files Created

### 1. `src/anomaly_detection.py`
**Purpose**: Detect AIS transmission gaps and draught changes for specific vessels

**Features**:
- Filters CSV data by target MMSI list from master index
- Uses external `sort` command for memory-efficient sorting
- **Enhanced in v0.0.10c**: Uses `gsort` on macOS for better performance
- Detects AIS gaps > 2 hours
- Identifies draught changes > 5%
- Outputs two reports:
  - `laivai_dingimai.csv` - Gap records (vessels disappearing from AIS)
  - `mmsi_draught_change.csv` - Draught change records

**Configuration**:
- Gap threshold: 2 hours
- Draught change threshold: 5%
- Memory limit: 2G
- Workers: Configurable (default: CPU count)

**Prerequisites**: 
- Master MMSI file (`master_MMSI_data.csv`) from Master Index Analysis
- **macOS**: Requires `gsort` (`brew install gsort`)

**Main Function**: `run_anomaly_detection(verbose=True)`

---

### 2. `src/mmsi_outlier_analysis.py`
**Purpose**: Detect suspicious position jumps in vessel tracking data

**Features**:
- Two-pass processing with sharding for memory efficiency
- Filters by vessels from proximity analysis (optional)
- Uses MD5 hashing for consistent MMSI sharding
- Detects position jumps > 50 km using Haversine distance
- Outputs two reports:
  - `mmsi_outlier_summary.csv` - Summary by MMSI (ranked by outlier count)
  - `mmsi_outlier_details.csv` - Individual position jump records

**Configuration**:
- Number of shards: 20
- Max distance threshold: 50.0 km
- Filter file: `vessel_proximity_meetings.csv` (optional)

**Prerequisites**: 
- Vessel proximity file (recommended but not required)

**Main Function**: `run_mmsi_outlier_analysis()`

---

## Integration Changes

### `main.py` Updates
1. **Added imports**:
   ```python
   from anomaly_detection import run_anomaly_detection
   from mmsi_outlier_analysis import run_mmsi_outlier_analysis
   ```

2. **Added menu functions**:
   - `run_anomaly_detection_menu()` - Interactive wrapper for anomaly detection
   - `run_mmsi_outlier_analysis_menu()` - Interactive wrapper for outlier analysis

3. **Updated main menu**:
   - Option 6: Anomaly Detection
   - Option 7: MMSI Outlier Analysis
   - Option 8: Run All Analyses (includes both new modules)
   - Option 9: Exit

### `README.md` Updates
- Added documentation for both new analysis modules
- Updated project structure to include new files
- Expanded features list to 7 analysis types

### Cleanup
- Removed old version files:
  - `AnomalyC.v004.py`
  - `mmsi_outlier_analysis.v008.py`
  - `consolidated_SOG_and_Draght_analysis.py.old`

---

## Usage

### Running from Main Menu
```bash
python main.py
```
Select option 6 for Anomaly Detection or option 7 for MMSI Outlier Analysis.

### Running Directly
```bash
# Anomaly Detection
python src/anomaly_detection.py

# MMSI Outlier Analysis
python src/mmsi_outlier_analysis.py
```

---

## Recommended Analysis Workflow

1. **Column Analysis** - Extract mobile types and counts
2. **Master Index Analysis** - Create MMSI/IMO indexes
3. **SOG/Draught Analysis** - Filter by vessel activity
4. **Vessel Proximity Analysis** - Identify vessels of interest
5. **Anomaly Detection** - Detect gaps and draught changes (uses master index)
6. **MMSI Outlier Analysis** - Detect position jumps (uses proximity list)
7. **Gap Analysis** - Detect AIS transmission gaps

---

## Technical Notes

### Memory Efficiency
Both modules use memory-efficient techniques:
- **Anomaly Detection**: External sort via Unix `sort` command
- **MMSI Outlier**: Sharding into 20 buckets for parallel processing

### Dependencies
- No additional dependencies required
- Uses Python standard library + psutil
- Anomaly Detection requires Unix `sort` command (available on macOS/Linux)

### Configuration
Both modules use centralized configuration from `src/config.py`:
- `FOLDER_PATH` - Input CSV files location
- `OUTPUT_REPORT_FOLDER` - Output reports location
- `MAX_WORKERS` - Number of parallel workers

### Error Handling
- Graceful handling of missing prerequisite files
- User prompts to continue or skip if prerequisites missing
- Cleanup of temporary files on completion or error

---

## Testing Recommendations

1. Test with sample data files
2. Verify output file formats
3. Check memory usage with large datasets
4. Validate gap detection thresholds
5. Confirm position jump calculations

---

## Future Enhancements

Potential improvements:
- Configurable thresholds via config.py
- Progress bars for long-running operations
- Additional filtering options
- Visualization of results
- Unit tests for both modules
- Performance benchmarks
