# Quick Start Guide - Vessel Proximity Analysis v0.0.10c

## 🚀 Quick Setup (macOS)

### Step 1: Install gsort (Required)

```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install gsort
brew install gsort

# Verify installation
gsort --version
```

### Step 2: Install Python Dependencies

```bash
# Navigate to project folder
cd /path/to/CSV_DATA_ANALYSIS

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Run Analysis

```bash
# Option 1: Run directly
python src/Plaukianciu_salia_laivu_analize.py

# Option 2: Run from main menu
python main.py
# Select: "Vessel Proximity Analysis"
```

---

## ⚡ Performance Expectations

**Test Dataset**: 50 CSV files, ~2M AIS records  
**System**: macOS M1 Pro (10 cores, 32GB RAM)

| Metric | Value |
|--------|-------|
| Processing Time | ~2.4 minutes |
| Memory Usage | ~4GB peak |
| Speed Improvement | 3.4x faster than v0.0.9 |

---

## 📋 Output Files

After analysis completes, find results in:

```
Maritime_Shadow_Fleet_Detection/
└── Data_analysis_and_outputs/
    └── output3/
        └── vessel_proximity_meetings.csv
```

**Output Format**:
```csv
MMSI_1,MMSI_2,Duration_Min,Start_Time,End_Time,Avg_SOG_Combined
123456789,987654321,120,2024-01-15 10:00:00,2024-01-15 12:00:00,5.5
```

---

## 🔧 Troubleshooting

### "gsort: command not found"

**Solution**: Install gsort (see Step 1 above)

### "sort: command not found" (Windows)

**Solution**: Use WSL or install Git Bash

### Out of Memory

**Solution**: Edit `src/config.py`:
```python
PROXIMITY_MAX_WORKERS = 2  # Reduce from auto-detect
PROXIMITY_MAX_MEMORY_TO_USE = 1  # Reduce to 1GB
```

### Slow Performance

**Checklist**:
- ✅ Verify gsort is installed: `gsort --version`
- ✅ Check CPU usage during processing
- ✅ Ensure SSD storage for temp files
- ✅ Close other memory-intensive apps

---

## 📚 Documentation

- **Full Update Details**: `VESSEL_PROXIMITY_V0.0.10C_UPDATE.md`
- **Configuration Guide**: `CONFIGURATION_GUIDE.md`
- **Project Overview**: `README.md`

---

## 🎯 Configuration Quick Reference

Edit `src/config.py` to adjust parameters:

```python
# Auto-detect CPU cores (recommended)
PROXIMITY_MAX_WORKERS = None

# Memory for sort operation (GB)
PROXIMITY_MAX_MEMORY_TO_USE = 2

# Detection thresholds
PROXIMITY_MAX_DIST_KM = 0.5      # Max distance between vessels
PROXIMITY_REQUIRED_WINDOWS = 12  # Min consecutive windows (~2 hours)
PROXIMITY_SOG_DIFF_LIMIT = 1.0   # Max speed difference (knots)
```

---

**Last Updated**: April 6, 2026  
**Version**: v0.0.10c
