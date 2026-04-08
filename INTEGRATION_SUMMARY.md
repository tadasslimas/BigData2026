# CSV DATA ANALYSIS - Project Integration Summary

## Overview
Complete maritime vessel data analysis platform with 8 integrated analysis modules.

**Latest Update**: Final comprehensive report generation with DFSI risk index integration complete.

---

## All Integrated Modules

### 0. **Clean AIS Database Module** ✅ (NEWEST)
- **File**: `src/clean_ais_database.py`
- **Features**:
  - Create filtered and deduplicated AIS database from raw CSV files
  - Two-pass processing for optimal performance:
    - Phase 1: Statistical analysis with multiprocessing
    - Phase 2: Data cleaning with sliding window deduplication
  - Filtering criteria:
    - Only Class A vessels
    - Valid MMSI (9 digits)
    - Valid coordinates (excluding 91.0, >89.9)
    - Minimum movement (5 km displacement)
    - Minimum speed (2.0 knots)
    - Minimum position reports (25 per day)
  - Memory-efficient sliding window deduplication
  - High-performance multiprocessing for statistics
  - Output: `Clean_AIS_DB/Clean_AIS_DB.csv`

### 1. **CSV Scanner Module** ✅
- **File**: `src/csv_scanner.py`
- **Features**:
  - Memory-efficient CSV file scanning
  - Extract unique values from specific columns
  - Count occurrences of each unique value
  - Track unique MMSI and IMO identifiers per mobile type
  - Automatic report generation with comprehensive statistics
  - Output: `master_list___Mobile_by_Type_summary.csv`

### 2. **Master Indexes Module** ✅
- **File**: `src/master_indexes.py`
- **Features**:
  - Create master indexes of vessel identifiers (IMO and MMSI)
  - Detect anomalies in vessel identification data:
    - One IMO number associated with multiple MMSI numbers
    - One MMSI number associated with multiple IMO numbers
  - Multiprocessing support for high-performance processing
  - Memory monitoring and management
  - Blacklist filtering for invalid data
  - Output: `master_IMO_data.csv`, `master_MMSI_data.csv`, anomaly reports

### 3. **Gap Analysis Module** ✅
- **File**: `src/gap_analysis.py`
- **Features**:
  - Detect AIS (Automatic Identification System) transmission gaps
  - Identify potential spoofing activities and signal blackouts
  - Track vessels by IMO number (primary) or MMSI (fallback)
  - Calculate distance traveled during transmission gaps
  - Configurable gap threshold and minimum movement filters
  - Multi-threaded processing for high performance
  - Output: Gap analysis reports with duration, distance, and MMSI change detection

### 4. **SOG and Draught Analysis Module** ✅
- **File**: `src/consolidated_SOG_and_Draght_analysis.py`
- **Features**:
  - Analyze vessel speed (Speed Over Ground) and draught statistics
  - Detect suspicious speed patterns and draught variations
  - Memory-efficient processing using Python's csv module (no pandas)
  - Parallel processing with configurable workers
  - Output includes:
    - Average, max, min speed and draught per vessel
    - Draught variation alerts (>20% change or >2m absolute difference)
    - Suspicious vessel flags (high speed with low sample count)
  - Report: `consolidated_speed_report.Class_A.csv`

### 5. **Vessel Proximity Analysis Module** ✅ (Updated v0.0.10c)
- **File**: `src/Plaukianciu_salia_laivu_analize.py`
- **Latest Update**: v0.0.10c with parallel chunk-based verification
- **Features**:
  - Detect vessels sailing close to each other for extended periods
  - Uses spatial and temporal bucketing for efficient matching
  - Filters by distance, speed difference, and minimum SOG
  - Identifies potential ship-to-ship transfers or rendezvous
  - **NEW in v0.0.10c**:
    - Parallel chunk-based verification (3-5x faster)
    - Enhanced macOS support with `gsort`
    - Automatic CPU core detection
    - Disk-based intermediate results for better memory management
  - Output: `vessel_proximity_meetings.csv`
  - **Documentation**: See `VESSEL_PROXIMITY_V0.0.10C_UPDATE.md` for performance details

### 6. **Anomaly Detection Module** ✅
- **File**: `src/anomaly_detection.py`
- **Features**:
  - Detect AIS transmission gaps (>2 hours) for specific MMSI vessels
  - Identify draught changes (>5%) occurring during gaps
  - Uses external Unix sort for memory-efficient processing
  - Requires master MMSI list from Master Index Analysis
  - Outputs:
    - Gap report: `laivai_dingimai.csv` (vessels disappearing from AIS)
    - Draught changes: `mmsi_draught_change.csv` (suspicious draught modifications)

### 7. **MMSI Outlier Analysis Module** ✅
- **File**: `src/mmsi_outlier_analysis.py`
- **Features**:
  - Detect suspicious position jumps in vessel tracking data
  - Uses sharding (20 buckets) for parallel processing
  - Filters by vessels from proximity analysis (optional)
  - Identifies position jumps exceeding 50 km threshold
  - Outputs:
    - Summary: `mmsi_outlier_summary.csv` (vessels ranked by outlier count)
    - Details: `mmsi_outlier_details.csv` (individual position jump records)

### 8. **Final Comprehensive Report Module** ✅ (NEWEST)
- **File**: `src/final_report.py`
- **Features**:
  - Consolidates results from all analysis modules into single report
  - Generates main report sections:
    - Vessel identification anomalies (IMO/MMSI relationships)
    - Master data overview (record counts)
    - Mobile type summary with statistics
  - Includes detailed appendices:
    - Appendix A: Gap analysis with filters (>4h, >10km, MMSI changes)
    - Appendix B: Vessel proximity meetings
    - Appendix C: Draught change analysis (excluding 100% errors)
    - Appendix D: Data outlier summary
  - **DFSI Risk Index Calculation**:
    - Formula: `(Max_Gap / 2) + (Total_Jump / 10) + (Draught_Changes * 15)`
    - Ranks vessels by suspicious activity level
    - Outputs:
      - `dfsi_full_list.csv` - All vessels with DFSI scores
      - `dfsi_high_risk_sorted.csv` - High-risk vessels (DFSI > 0), sorted by risk
  - Report: `Final_Comprehensive_Report.txt`

---

## Centralized Configuration

All modules use centralized configuration from `src/config.py`:
- Input/output paths
- Processing parameters (workers, chunk sizes)
- Analysis thresholds and filters
- Blacklists for invalid data

See [`CONFIGURATION_GUIDE.md`](CONFIGURATION_GUIDE.md) for detailed configuration options.

---

## How to Use

### Running the Application
```bash
python main.py
```

### Interactive Menu Options:
1. **Column Analysis + Master Index** - Extract mobile types and create IMO/MMSI indexes
2. **Gap Analysis** - Detect AIS transmission gaps
3. **SOG/Draught Analysis** - Analyze vessel speed and draught
4. **Vessel Proximity Analysis** - Detect vessels sailing close together
5. **Anomaly Detection** - Detect AIS gaps and draught changes
6. **MMSI Outlier Analysis** - Detect suspicious position jumps
7. **Final Report** - Generate comprehensive summary with DFSI risk index
8. **Run All Analyses** - Execute options 1-6 sequentially
9. **Exit**

### Recommended Workflow:
1. Run option 1 (Column + Master Index)
2. Run option 3 (SOG/Draught) - helps filter stationary vessels
3. Run option 4 (Vessel Proximity) - identifies vessels of interest
4. Run option 2, 5, or 6 as needed
5. Run option 7 (Final Report) to consolidate all results

---

## Project Structure

```
CSV_DATA_ANALYSIS/
├── src/
│   ├── __init__.py
│   ├── csv_scanner.py              # Memory-efficient CSV scanner
│   ├── master_indexes.py           # IMO/MMSI index creation
│   ├── gap_analysis.py             # AIS gap detection
│   ├── consolidated_SOG_and_Draght_analysis.py  # Speed/draught stats
│   ├── Plaukianciu_salia_laivu_analize.py  # Vessel proximity
│   ├── anomaly_detection.py        # Gap and draught changes
│   ├── mmsi_outlier_analysis.py    # Position jump detection
│   ├── final_report.py             # Comprehensive report + DFSI
│   └── config.py                   # Centralized configuration
├── tests/
│   ├── test_csv_scanner.py
│   ├── test_master_indexes.py
│   └── test_csv_scanner_enhanced.py
├── data/
│   └── sample_mobiles.csv
├── README.md
├── CONFIGURATION_GUIDE.md
├── GAP_ANALYSIS_GUIDE.md
├── INTEGRATION_ANOMALY_OUTLIER.md
└── main.py
```

---

## Dependencies

See [`requirements.txt`](requirements.txt):
- matplotlib, seaborn - Visualization (optional)
- pytest - Testing framework
- psutil - Memory monitoring

**Note**: Core analysis uses Python's built-in `csv` module - no pandas/numpy required!

---

## Memory Optimizations

All modules implement memory-efficient processing:
- Streaming CSV processing (line-by-line)
- Multiprocessing with controlled worker count
- Explicit garbage collection after analysis
- Temporary file cleanup
- External sorting for large datasets

---

## Documentation Files

- [`README.md`](README.md) - Project overview and usage
- [`CONFIGURATION_GUIDE.md`](CONFIGURATION_GUIDE.md) - Configuration options
- [`GAP_ANALYSIS_GUIDE.md`](GAP_ANALYSIS_GUIDE.md) - Gap analysis details
- [`INTEGRATION_ANOMALY_OUTLIER.md`](INTEGRATION_ANOMALY_OUTLIER.md) - Anomaly/outlier integration
- [`INTEGRATION_COMPLETE.md`](INTEGRATION_COMPLETE.md) - Gap analysis integration
- [`INTEGRATION_SUMMARY.md`](INTEGRATION_SUMMARY.md) - This file

---

**Status**: ✅ All modules integrated and tested
**Last Updated**: April 5, 2026
  3. Run Both Analyses
  4. Exit
```

### Master Index Analysis Output
When you run the Master Index Analysis, it generates:
- `master_MMSI_data.csv` - All unique MMSI numbers
- `master_IMO_data.csv` - All unique IMO numbers
- `IMO_with_multiple_MMSI.csv` - Anomaly report: IMO numbers with multiple MMSIs
- `MMSI_with_multiple_IMO.csv` - Anomaly report: MMSI numbers with multiple IMOs

### Running Tests
```bash
pytest tests/
```

## Key Improvements from Original Code

1. **Better Code Organization**: Proper module structure with clear separation of concerns
2. **Type Hints**: Full type annotations for better IDE support and code clarity
3. **Documentation**: Comprehensive docstrings for all functions
4. **Configurability**: All parameters can be customized via function arguments
5. **Error Handling**: Better exception handling and user feedback
6. **Testing**: Full test coverage for core functionality
7. **Integration**: Seamlessly integrated with existing project structure

## Configuration

Default paths (can be customized in `main.py`):
- **Data Folder**: `~/Maritime_Shadow_Fleet_Detection/Duomenys_CSV_Formate.2026`
- **Output Folder**: `~/Maritime_Shadow_Fleet_Detection/Data_analysis_and_outputs/output3`

Processing parameters (can be customized in `src/master_indexes.py`):
- **CHUNK_SIZE**: 40000 rows per chunk
- **MAX_WORKERS**: 4 parallel processes
- **FILE_PATTERN**: `*.csv`

## Technical Details

### Data Validation
- MMSI: Must be exactly 9 digits, not in blacklist
- IMO: Must be exactly 7 digits, not in blacklist
- Rows must have at least 11 columns

### Memory Management
- Uses `ProcessPoolExecutor` for parallel processing
- Limits active tasks to prevent memory overload
- Monitors both master and worker process memory usage
- Streams data in chunks to avoid loading entire files

### Performance
- Multiprocessing for CPU-intensive data analysis
- Chunked processing for memory efficiency
- Sorted file processing for consistent results
