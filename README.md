# CSV_DATA_ANALYSIS

A Python project for CSV data analysis and processing using memory-efficient streaming techniques.

## Overview

This project provides tools for analyzing maritime vessel data from CSV files, including:
- Mobile type classification and counting
- MMSI/IMO identifier tracking and anomaly detection
- AIS transmission gap analysis
- SOG (Speed Over Ground) and draught statistics
- Vessel proximity analysis
- Clean AIS database creation with filtering and deduplication

**Key Features:**
- ✅ Memory-efficient processing using Python's built-in `csv` module
- ✅ Parallel processing with multiprocessing
- ✅ No dependency on pandas for core analysis functions
- ✅ Suitable for processing large CSV files without loading entire datasets into memory
- ✅ Multiprocessing for high-performance statistical analysis
- ✅ Sliding window deduplication for minimal memory usage

## Project Structure

```
CSV_DATA_ANALYSIS/
├── .github/
│   └── copilot-instructions.md
├── src/
│   └── __init__.py
├── tests/
│   └── __init__.py
├── data/
│   └── .gitkeep
├── .gitignore
├── requirements.txt
├── README.md
└── main.py
```

## Getting Started

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- **macOS users only**: GNU coreutils (for `gsort` command)
  ```bash
  brew install coreutils
  ```

### Installation

1. Navigate to the project directory:
   ```bash
   cd CSV_DATA_ANALYSIS
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. **Visualization dependencies** (for Options 20-21):
   - The visualization scripts require PySpark and Folium
   - Ensure Spark is properly configured if using distributed processing
   - For local processing, PySpark will run in standalone mode

5. **macOS users**: Verify gsort installation:
   ```bash
   gsort --version
   # Should display: sort (GNU coreutils) X.XX
   ```

### Usage

Run the main script:
```bash
python main.py
```

The application will display a welcome screen with:
- Complete configuration summary
- Input/output folder paths
- Processing settings (workers, chunk size)
- Column analysis settings
- Master index analysis settings
- Interactive menu to choose analysis mode

### Running Tests

```bash
pytest tests/
```

## Features

### 0. Clean AIS Database Creation (NEW)
- Create filtered and deduplicated AIS database from raw CSV files
- **Two-pass processing**:
  - Phase 1: Statistical analysis with multiprocessing
  - Phase 2: Data cleaning with sliding window deduplication
- **Filtering criteria**:
  - Only Class A vessels
  - Valid MMSI (9 digits)
  - Valid coordinates (excluding 91.0, >89.9)
  - Minimum movement (5 km displacement)
  - Minimum speed (2.0 knots)
  - Minimum position reports (25 per day)
- **Memory-efficient**: Sliding window deduplication
- **High-performance**: Multiprocessing for statistical analysis
- Output saved to `Clean_AIS_DB/Clean_AIS_DB.csv`

### 1. Column Analysis
- Memory-efficient CSV file scanning
- Extract unique values from specific columns
- **Count occurrences** of each unique value
- **Track unique MMSI identifiers** per mobile type
- **Track unique IMO identifiers** per mobile type
- **Automatic report generation** with comprehensive statistics
- Process large files without loading entire dataset into memory
- Uses Python's built-in `csv` module for streaming
- Output saved to `master_list___Mobile_by_Type_summary.csv`
- **Enhanced CSV format**: `Mobile Type, Count, Unique MMSI, Unique IMO`

### 2. Master Index Analysis (IMO/MMSI)
- Create master indexes of vessel identifiers (IMO and MMSI)
- Detect anomalies in vessel identification data:
  - One IMO number associated with multiple MMSI numbers
- One MMSI number associated with multiple IMO numbers
- Multiprocessing support for high-performance processing
- Memory monitoring and management
- Blacklist filtering for invalid data
- Automatic report generation

### 3. Gap Analysis
- Detect AIS (Automatic Identification System) transmission gaps
- Identify potential spoofing activities and signal blackouts
- Track vessels by IMO number (primary) or MMSI (fallback)
- Calculate distance traveled during transmission gaps
- Configurable gap threshold and minimum movement filters
- Multi-threaded processing for high performance
- Output reports with gap duration, distance, and MMSI change detection
- See [`GAP_ANALYSIS_GUIDE.md`](GAP_ANALYSIS_GUIDE.md) for detailed documentation

### 4. SOG and Draught Analysis
- Analyze vessel speed (Speed Over Ground) and draught statistics
- Detect suspicious speed patterns and draught variations
- Memory-efficient processing using Python's csv module
- Parallel processing with configurable workers
- Output includes:
  - Average, max, min speed and draught per vessel
  - Draught variation alerts (>20% change or >2m absolute difference)
  - Suspicious vessel flags (high speed with low sample count)
- Report saved to `consolidated_speed_report.Class_A.csv`

### 5. Vessel Proximity Analysis (Updated v0.0.10c)
- Detect vessels sailing close to each other for extended periods
- **NEW in v0.0.10c**:
  - Parallel chunk-based verification processing (3-5x faster)
  - Enhanced macOS support with `gsort` for better resource management
  - Automatic CPU core detection for optimal performance
  - Disk-based intermediate results for reduced memory usage
- Uses spatial and temporal bucketing for efficient matching
- Filters by distance, speed difference, and minimum SOG
- Identifies potential ship-to-ship transfers or rendezvous
- Output includes meeting duration, distance, and vessel details
- Report saved to `vessel_proximity_meetings.csv`
- **See [`VESSEL_PROXIMITY_V0.0.10C_UPDATE.md`](VESSEL_PROXIMITY_V0.0.10C_UPDATE.md) for detailed performance guide**

### 6. Anomaly Detection (NEW)
- Detect AIS transmission gaps (>2 hours) for specific MMSI vessels
- Identify draught changes (>5%) occurring during gaps
- Uses external sort for memory-efficient processing
- **Enhanced**: Uses `gsort` on macOS for better performance
- Requires master MMSI list from Master Index Analysis
- Outputs:
  - Gap report: `laivai_dingimai.csv` (vessels disappearing from AIS)
  - Draught changes: `mmsi_draught_change.csv` (suspicious draught modifications)

### 7. MMSI Outlier Analysis (NEW)
- Detect suspicious position jumps in vessel tracking data
- Uses sharding for parallel processing of large datasets
- Filters by vessels from proximity analysis (optional)
- Identifies position jumps exceeding 50 km threshold
- Outputs:
  - Summary: `mmsi_outlier_summary.csv` (vessels ranked by outlier count)
  - Details: `mmsi_outlier_details.csv` (individual position jump records)

### 8. Final Comprehensive Report (NEW)
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
- Report saved to: `Final_Comprehensive_Report.txt`

### 9. Vessel Proximity Visualization - Short Version (NEW)
- Generate interactive HTML maps for vessel proximity incidents
- **Features**:
  - Cleaned trajectory visualization (removes position jumps >60 knots)
  - Interactive Folium maps with popup information
  - Shows both vessels' tracks with color coding (blue/red)
  - Displays incident details (duration, average SOG, MMSI numbers)
- **Configuration**:
  - Uses PySpark for parallel processing
  - Max physical speed filter: 60 knots
  - Output saved to: `Geo_Maps_Clean/` folder
- **Prerequisites**: Run Vessel Proximity Analysis (Option 4) first

### 10. Vessel Proximity Visualization - Full Version (NEW)
- Comprehensive interactive HTML maps with detailed analysis
- **Features**:
  - Detailed trajectory visualization with full data
  - Enhanced filtering and outlier removal
  - Interactive Folium maps with extensive popup information
  - Complete incident details and statistics
  - DFSI risk index integration
- **Configuration**:
  - Uses PySpark for parallel processing
  - Full detailed output mode
  - Output saved to: `Geo_Maps_Clean.FULL/` folder
- **Prerequisites**: Run Vessel Proximity Analysis (Option 4) first

## Project Structure

```
CSV_DATA_ANALYSIS/
├── src/
│   ├── __init__.py
│   ├── csv_scanner.py              # Memory-efficient CSV scanner
│   ├── clean_ais_database.py       # Clean AIS DB creation with filtering
│   ├── master_indexes.py           # IMO/MMSI index creation and anomaly detection
│   ├── gap_analysis.py             # AIS transmission gap detection
│   ├── consolidated_SOG_and_Draght_analysis.py  # Speed and draught statistics
│   ├── Plaukianciu_salia_laivu_analize.py  # Vessel proximity analysis
│   ├── anomaly_detection.py        # Gap and draught change detection
│   ├── mmsi_outlier_analysis.py    # Position jump detection
│   ├── final_report.py             # Comprehensive report generation with DFSI
│   ├── Laivu_Vizualizacija__SHORT.py   # Short version proximity visualization
│   ├── Laivu_Vizualizacija__FULL.py    # Full version proximity visualization
│   └── config.py                   # Centralized configuration management
├── tests/
│   ├── __init__.py
│   ├── test_csv_scanner.py
│   ├── test_master_indexes.py
│   └── test_csv_scanner_enhanced.py
├── data/
│   └── sample_mobiles.csv
├── requirements.txt
├── README.md
├── CONFIGURATION_GUIDE.md          # Complete configuration reference
├── GAP_ANALYSIS_GUIDE.md           # Detailed gap analysis documentation
├── INTEGRATION_ANOMALY_OUTLIER.md  # Anomaly and outlier integration guide
├── VESSEL_PROXIMITY_V0.0.10C_UPDATE.md  # Vessel proximity performance update
└── main.py                         # Main entry point with interactive menu
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Add tests if applicable
4. Submit a pull request

## License

This project is open source and available under the MIT License.
