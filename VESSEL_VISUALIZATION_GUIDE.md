# Vessel Proximity Visualization Guide

## Overview

The CSV_DATA_ANALYSIS project now includes two visualization tools for vessel proximity incidents:

1. **Short Version** (Option 20) - Quick visualization with basic cleaning
2. **Full Version** (Option 21) - Comprehensive visualization with detailed analysis

Both tools generate interactive HTML maps showing vessels that sailed close to each other for extended periods.

## Features

### Short Version (Laivu_Vizualizacija__SHORT.py)

- **Purpose**: Quick generation of cleaned trajectory maps
- **Processing Time**: Faster, suitable for rapid analysis
- **Features**:
  - Cleaned trajectory visualization (removes position jumps >60 knots)
  - Interactive Folium maps with popup information
  - Shows both vessels' tracks with color coding (blue/red)
  - Displays incident details:
    - Incident number
    - MMSI numbers of both vessels
    - Meeting duration (minutes)
    - Average speed during meeting (knots)
    - Number of removed position jumps per vessel

### Full Version (Laivu_Vizualizacija__FULL.py)

- **Purpose**: Comprehensive analysis with detailed filtering
- **Processing Time**: Longer, more thorough analysis
- **Features**:
  - Detailed trajectory visualization with full data
  - Enhanced filtering and outlier removal (50 km threshold)
  - Interactive Folium maps with extensive popup information
  - Complete incident details and statistics
  - DFSI risk index integration
  - Outlier filtering log generation

## Configuration

Both visualization tools use centralized configuration from `src/config.py`:

```python
PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
PROXIMITY_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"
```

### Output Folders

- **Short Version**: `Geo_Maps_Clean/`
- **Full Version**: `Geo_Maps_Clean.FULL/`

### Data Sources

- **Proximity Incidents**: `vessel_proximity_meetings.csv` (from Option 4)
- **AIS Data**: `Clean_AIS_DB/*.csv` folder

## Usage

### From Main Menu

1. Run the main application:
   ```bash
   python main.py
   ```

2. Select visualization option:
   - **Option 20**: Short version visualization
   - **Option 21**: Full version visualization

3. Confirm when prompted:
   ```
   Generate proximity visualizations? (y/n): y
   ```

### Standalone Execution

You can also run the visualization scripts directly:

```bash
# Short version
python src/Laivu_Vizualizacija__SHORT.py

# Full version
python src/Laivu_Vizualizacija__FULL.py
```

## Prerequisites

### Required Analysis

Before running visualizations, you must first run:
- **Option 4**: Vessel Proximity Analysis (generates `vessel_proximity_meetings.csv`)
- **Option 0**: Clean AIS Database Creation (recommended for clean data)

### Python Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

Key dependencies:
- `folium>=0.14.0` - Interactive map generation
- `pyspark>=3.4.0` - Parallel processing
- `pandas>=2.0.0` - Data manipulation

### Spark Configuration

The visualization scripts support both:
- **Local mode**: PySpark runs in standalone mode (default)
- **Distributed mode**: Connects to Spark cluster (if configured)

To use distributed processing, modify:
```python
USING_DISTRIBUTED_PYSPARK_CALCULATION = True
```

## Output Format

### HTML Map Files

Each proximity incident generates an HTML file:

**Short Version**:
```
Laivu_plaukimas_Salia.{MMSI1}_{MMSI2}_{IncidentID}.html
```

**Full Version**:
```
Incident_{IncidentID}_{MMSI1}_{MMSI2}_CLEAN.html
```

### Map Features

- **Blue Line**: First vessel's trajectory
- **Red Line**: Second vessel's trajectory
- **Green Marker**: Starting point with popup info
- **Interactive Controls**: Zoom, pan, click for details

## Technical Details

### Data Processing Pipeline

1. **Load Proximity Data**: Read `vessel_proximity_meetings.csv`
2. **Load AIS Data**: Read CSV files from `Clean_AIS_DB/`
3. **Join Data**: Match AIS positions to proximity incidents
4. **Clean Trajectories**: Remove position jumps exceeding speed threshold
5. **Generate Maps**: Create interactive Folium maps
6. **Save Output**: Store HTML files in output folder

### Cleaning Algorithms

**Short Version**:
- Max physical speed: 60 knots
- Removes points causing impossible speeds
- Checks both forward and backward speed calculations

**Full Version**:
- Max distance threshold: 50 km between consecutive points
- Min distance threshold: 0.001 km (avoids noise)
- Comprehensive outlier detection and logging

## Troubleshooting

### Common Issues

**1. "No module named 'Laivu_Vizualizacija__SHORT'"**
- Solution: Run as subprocess (already handled by main.py)
- Or ensure src/ is in Python path

**2. Spark connection errors**
- Solution: Check Spark cluster is running
- Or set `USING_DISTRIBUTED_PYSPARK_CALCULATION = False` for local mode

**3. No maps generated**
- Check if `vessel_proximity_meetings.csv` exists
- Verify `Clean_AIS_DB/` folder contains CSV files
- Ensure proximity analysis was run first

**4. Empty maps or missing trajectories**
- Check time format in AIS data (dd/MM/yyyy HH:mm:ss)
- Verify MMSI numbers match between proximity and AIS data
- Check if vessels have sufficient position reports

### Performance Tips

- **Reduce data size**: Filter to specific date ranges
- **Use local mode**: Avoid network overhead if processing locally
- **Adjust thresholds**: Higher speed/distance thresholds = less filtering
- **Monitor Spark UI**: Check for stragglers or resource issues

## Examples

### Sample Output

After running visualization, you'll see:
```
1. Kraunami incidentų duomenys...
2. Kraunami AIS duomenys iš klasterio...
3. Jungiami duomenys (Join)...
4. Paleidžiamas lygiagretus apdorojimas (Pantrykite, tai gali užtrukti)...

--- APDOROJIMO SUVESTINĖ ---
Incidentas #0: Sėkmė: Laivu_plaukimas_Salia.123456789_987654321_0.html
Incidentas #1: Sėkmė: Laivu_plaukimas_Salia.111222333_444555666_1.html
```

### Viewing Results

Open generated HTML files in any web browser:
```bash
open /Users/valdas/Maritime_Shadow_Fleet_Detection/Data_analysis_and_outputs/output3/Geo_Maps_Clean/Laivu_plaukimas_Salia.*.html
```

## Integration with Other Modules

The visualization tools integrate with:
- **Vessel Proximity Analysis** (Option 4): Provides incident data
- **Clean AIS Database** (Option 0): Provides cleaned position data
- **Final Report** (Option 8): DFSI risk scores (Full version)
- **MMSI Outlier Analysis** (Option 6): Outlier filtering (Full version)

## Future Enhancements

Planned improvements:
- Real-time visualization streaming
- 3D trajectory visualization
- Export to KML/KMZ for Google Earth
- Animated time-series playback
- Cluster analysis for multiple vessel encounters

## Support

For issues or questions:
1. Check this documentation
2. Review error messages in terminal output
3. Verify all prerequisites are met
4. Check Spark logs if using distributed mode
