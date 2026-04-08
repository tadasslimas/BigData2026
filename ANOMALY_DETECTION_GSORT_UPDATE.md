# Anomaly Detection Module - gsort Integration Update

## Overview

The `anomaly_detection.py` module has been updated to use `gsort` on macOS, matching the implementation in `Plaukianciu_salia_laivu_analize.py` (v0.0.10c).

**Date**: April 6, 2026  
**Module**: `src/anomaly_detection.py`

---

## 🚀 Changes Made

### 1. macOS Detection and gsort Usage

The module now automatically detects the operating system and uses the appropriate sort command:

```python
os_system = platform.system()
sort_tool = "gsort" if os_system == "Darwin" else "sort"
```

### 2. Enhanced Error Messages

Improved error handling with platform-specific guidance:

**Before**:
```python
print("   Error: 'sort' command not found. This tool is required for external sorting.")
print("   On macOS, it should be available by default.")
```

**After**:
```python
if os_system == "Darwin":
    print("   On macOS, install gsort via Homebrew: brew install gsort")
else:
    print("   This tool is required for external sorting.")
```

### 3. Verbose Mode Support

Added `verbose` parameter to `run_anomaly_detection()` function:

```python
def run_anomaly_detection(verbose=True):
    """
    Main function to run anomaly detection for gaps and draught changes.
    
    Args:
        verbose: Whether to print detailed progress information (default: True)
    """
```

All print statements now respect the `verbose` flag for cleaner integration with other modules.

### 4. Sort Command Details

The sort command now shows detailed information when verbose mode is enabled:

```python
if verbose:
    print(f"   Using sort tool: {sort_tool} (OS: {os_system})")
    print(f"   Command: {' '.join(sort_cmd)}")
```

---

## 🔧 Technical Details

### Sort Command Configuration

```python
sort_cmd = [
    sort_tool,              # 'gsort' on macOS, 'sort' on Linux
    '-t', '|',              # Field separator
    '-k1,1',                # Sort by MMSI (field 1)
    '-k2,2n',               # Then by timestamp (field 2, numeric)
    '-S', MEMORY_LIMIT,     # Memory buffer (e.g., '2G')
    f'--parallel={MAX_WORKERS}',  # Parallel workers
    '-o', str(temp_sorted), # Output file
    str(temp_unsorted)      # Input file
]
```

### Platform Behavior

| Platform | Sort Tool | Installation |
|----------|-----------|--------------|
| macOS | `gsort` | `brew install gsort` |
| Linux | `sort` | Pre-installed (GNU coreutils) |
| Windows (WSL) | `sort` | Available in WSL |
| Windows (Git Bash) | `sort` | Included with Git |

---

## 📋 Benefits

### Performance Improvements

**macOS users** will experience:
- ⚡ **Faster sorting** with GNU sort optimizations
- 💾 **Better memory management** with configurable buffer size
- 🔄 **Parallel processing** with `--parallel` flag
- 📊 **Consistent behavior** across all analysis modules

### Consistency Across Modules

Both modules now use the same approach:
1. ✅ `Plaukianciu_salia_laivu_analize.py` (v0.0.10c)
2. ✅ `anomaly_detection.py` (updated)

This ensures:
- Consistent performance across all analyses
- Single dependency to manage (`gsort`)
- Unified error handling and messaging
- Easier maintenance and troubleshooting

---

## 🎯 Usage

### Basic Usage

```python
from anomaly_detection import run_anomaly_detection

# Run with default verbose output
run_anomaly_detection()

# Run silently (for integration with other scripts)
run_anomaly_detection(verbose=False)
```

### Command Line

```bash
# Run directly
python src/anomaly_detection.py

# Run from main menu
python main.py
# Select: "Anomaly Detection"
```

---

## 📦 Dependencies

### Python Packages

No additional Python packages required. Uses standard library:
- `csv` - Data processing
- `subprocess` - External command execution
- `platform` - OS detection
- `pathlib` - Path manipulation

### System Dependencies

**macOS**:
```bash
brew install gsort
```

**Linux**: No additional dependencies (GNU sort pre-installed)

**Windows**: Use WSL or Git Bash

---

## 🔍 Verification

To verify `gsort` is working correctly:

```bash
# Check if gsort is installed (macOS)
gsort --version

# Should display: sort (GNU coreutils) X.XX
```

When running anomaly detection on macOS, you should see:
```
2. Executing external sort (using 8 workers)...
   Using sort tool: gsort (OS: Darwin)
   Command: gsort -t | -k1,1 -k2,2n -S 2G --parallel=8 -o /tmp/... /tmp/...
   Sort completed successfully
```

---

## 🐛 Troubleshooting

### Issue: "gsort: command not found"

**Solution**:
```bash
# Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install gsort
brew install gsort
```

### Issue: Sort fails with memory error

**Solution**: Reduce memory limit in `anomaly_detection.py`:

```python
MEMORY_LIMIT = "1G"  # Reduce from "2G"
```

### Issue: Slow performance

**Checklist**:
- ✅ Verify gsort is installed: `gsort --version`
- ✅ Check CPU usage during sort operation
- ✅ Ensure SSD storage for temporary files
- ✅ Increase `MAX_WORKERS` if CPU usage is low

---

## 📚 Related Documentation

- **Vessel Proximity Update**: `VESSEL_PROXIMITY_V0.0.10C_UPDATE.md`
- **Anomaly Integration**: `INTEGRATION_ANOMALY_OUTLIER.md`
- **Configuration Guide**: `CONFIGURATION_GUIDE.md`
- **Quick Start**: `QUICKSTART_VESSEL_PROXIMITY.md`

---

## ✅ Testing Recommendations

1. **Test on macOS**:
   ```bash
   brew install gsort
   python src/anomaly_detection.py
   ```

2. **Verify output files**:
   - `laivai_dingimai.csv` - Gap records
   - `mmsi_draught_change.csv` - Draught changes

3. **Check performance**:
   - Monitor CPU usage during sort phase
   - Compare execution time before/after gsort installation

---

**Last Updated**: April 6, 2026  
**Module Version**: anomaly_detection.py (gsort-enabled)
