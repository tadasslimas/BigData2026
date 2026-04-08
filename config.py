"""
Configuration Module
Centralized configuration management for the CSV Data Analysis project.

All global settings, paths, and parameters are defined here for easy access
and modification.
"""
import os
from pathlib import Path
import psutil
import sys
import platform


### Obtaining UID for specific temporary folder configuration and for potential user-specific settings
USER_ID_FOR_SPECIFIC_FOLDERS = os.getlogin()

# ==========================================
# PRIMARY FOLDER CONFIGURATION
# ==========================================
# Base folder for all maritime data
# Uncomment and modify the line below to use a custom path:
# PRIMARY_FOLDER = Path("/opt/A/NFS_Folder/Maritime_Shadow_Fleet_Detection")
PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"

#### Global Temporary Folder for intermediate files (can be set to system temp or a specific path)
GLOBAL_TMP_FOLDER = Path("/tmp")

if USER_ID_FOR_SPECIFIC_FOLDERS ==  "valdas":
    GLOBAL_TMP_FOLDER = Path.home() / "tmp"

if USER_ID_FOR_SPECIFIC_FOLDERS ==  "tadasslimas":
    GLOBAL_TMP_FOLDER = Path("/Volumes/VolumeA/Temp/Tado__darbas")
    PRIMARY_FOLDER = Path("/Volumes/VolumeA/Tado___darbas/Maritime_Shadow_Fleet_Detection")


# ==========================================
# INPUT DATA CONFIGURATION
# ==========================================

# Folder containing CSV files to analyze
FOLDER_PATH = PRIMARY_FOLDER / "Duomenys_CSV_Formate"
CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"  # Folder for cleaned AIS data


# File pattern for matching CSV files
FILE_PATTERN = "*.csv"

# ==========================================
# OUTPUT CONFIGURATION
# ==========================================

# Base folder for all analysis outputs
OUTPUT_DATA_PATH = PRIMARY_FOLDER / "Data_analysis_and_outputs"

# Specific folder for master index reports
OUTPUT_REPORT_FOLDER = OUTPUT_DATA_PATH / "output3"

# ==========================================
# PROCESSING CONFIGURATION
# ==========================================

# Number of parallel worker processes for multiprocessing
# Increase for faster processing (uses more RAM)
# Decrease for lower memory usage
MAX_WORKERS = 4

# Number of rows to process in each chunk
# Larger chunks = better performance but higher memory usage
# Smaller chunks = lower memory usage but slower processing
CHUNK_SIZE = 10000

# Maximum allowed RAM to use for processing (percentage of total system RAM)
## size in GB 
MAX_MEMORY_TO_USE = 1

# ==========================================
# COLUMN ANALYSIS CONFIGURATION
# ==========================================

# Default column settings for basic CSV scanning
DEFAULT_COLUMN_NAME = "Type of mobile"
DEFAULT_COLUMN_INDEX = 1  # Zero-based index (1 = second column)

# ==========================================
# MASTER INDEX ANALYSIS CONFIGURATION
# ==========================================

# Column indices for IMO/MMSI analysis (zero-based)
MMSI_COLUMN_INDEX = 2   # MMSI is in the 3rd column
IMO_COLUMN_INDEX = 10   # IMO is in the 11th column

# Validation rules
MMSI_LENGTH = 9         # Valid MMSI must be exactly 9 digits
IMO_LENGTH = 7          # Valid IMO must be exactly 7 digits

# Blacklists - invalid values to filter out
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

# ==========================================
# GAP ANALYSIS CONFIGURATION
# ==========================================

# Gap analysis thresholds
AIS_GAP_HOURS_THRESHOLD = 4       # Minimum gap in hours to flag
MIN_MOVEMENT_KM = 0.5             # Minimum movement in km to consider significant

# Gap analysis identity key mode
# ""           -> IMO primary, MMSI fallback (recommended default)
# "MMSI_ONLY"  -> only MMSI identity (ignore IMO)
# "IMO_ONLY"   -> only IMO identity (skip rows with missing/Unknown IMO)
GAP_ANALYSIS_KEY_MODE = "MMSI_ONLY"

# Gap analysis output settings
GAP_ANALYSIS_OUTPUT_FOLDER = OUTPUT_DATA_PATH / "gap_analysis_reports"

# MMSI whitelist files for gap analysis
GAP_ANALYSIS_CLASS_A_WHITELIST = OUTPUT_DATA_PATH / "output/by_type/mmsi_Class_A.csv"
GAP_ANALYSIS_CLASS_B_WHITELIST = OUTPUT_DATA_PATH / "output/by_type/mmsi_Class_B.csv"

# ThreadPoolExecutor configuration for gap analysis
GAP_ANALYSIS_MAX_WORKERS = MAX_WORKERS      # Number of parallel workers (8-16 recommended for NVMe)

# ==========================================
# SOG AND DRAUGHT ANALYSIS CONFIGURATION
# ==========================================

# Mobile types to include in SOG/Draught analysis
SOG_DRUGHT_MOBILE_TYPES = {"Class A"}

# Minimum number of samples required for reliable statistics
SOG_DRUGHT_MIN_SAMPLES = 5

# Suspicious average speed threshold (knots)
# Vessels with avg SOG above this with low samples are flagged
SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS = 80

# Draught variation alert threshold (percentage)
# Vessels with draught variation > this percentage are flagged
SOG_DRUGHT_VARIATION_THRESHOLD_PCT = 20.0

# Number of parallel workers for SOG/Draught analysis
SOG_DRUGHT_MAX_WORKERS = MAX_WORKERS

# Output file for SOG/Draught analysis
SOG_DRUGHT_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "consolidated_speed_report.Class_A.csv"

# ==========================================
# VESSEL PROXIMITY ANALYSIS CONFIGURATION
# ==========================================

# Grid size for spatial indexing (degrees, ~1.1 km at equator)
PROXIMITY_GRID_SIZE = 0.01

# Time step for temporal bucketing (seconds, 600 = 10 minutes)
PROXIMITY_TIME_STEP = 600

# Required consecutive time windows to flag as meeting (~2 hours = 12 windows)
PROXIMITY_REQUIRED_WINDOWS = 12

# Maximum distance between vessels to be considered "close" (km)
PROXIMITY_MAX_DIST_KM = 0.5

# Maximum speed difference between vessels (knots)
PROXIMITY_SOG_DIFF_LIMIT = 1.0

# Minimum SOG filter - prevents writing if both vessels are stationary
PROXIMITY_MIN_SOG_RAW_FILTER = 0.1

# Minimum average meeting speed for final report
PROXIMITY_MIN_AVG_MEETING_SOG = 0.5

# Minimum max speed for global filter (vessels must be generally active)
PROXIMITY_MIN_MAX_SPEED_GLOBAL = 1.0

# Temporary folder for processing (uses system temp by default)
PROXIMITY_TMP_FOLDER = Path(f"{GLOBAL_TMP_FOLDER}/ais_processing")

# Output file for vessel proximity analysis
PROXIMITY_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"

# Proximity max workers (parallel processes for proximity analysis)
PROXIMITY_MAX_WORKERS = MAX_WORKERS     # 8-16 recommended for NVMe, adjust based on system capabilities    

PROXIMITY_MAX_MEMORY_TO_USE = MAX_MEMORY_TO_USE  # GB - maximum RAM to use for proximity analysis (adjust based on system capabilities)


# ==========================================
# SYSTEM CONFIGURATION
# ==========================================

# File encoding for reading CSV files
FILE_ENCODING = 'utf-8'

# Memory monitoring threshold (percentage of system RAM)
# Warning will be shown if system RAM usage exceeds this value
MEMORY_WARNING_THRESHOLD = 90.0

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def print_configuration_summary():
    """
    Print a summary of the current configuration settings.
    Useful for displaying on application startup.
    """
    print("=" * 60)
    print("CONFIGURATION SUMMARY")
    print("=" * 60)
    
    print("\nINPUT DATA:")
    print(f"  Primary Folder:    {PRIMARY_FOLDER}")
    print(f"  CSV Files Folder:  {FOLDER_PATH}")
    print(f"  File Pattern:      {FILE_PATTERN}")
    
    print("\nOUTPUT DATA:")
    print(f"  Output Base Path:  {OUTPUT_DATA_PATH}")
    print(f"  Reports Folder:    {OUTPUT_REPORT_FOLDER}")
    print(f"  Temporary Folder:  {GLOBAL_TMP_FOLDER}")
    
    
    print("\nPROCESSING SETTINGS:")
    print(f"  Worker Processes:  {MAX_WORKERS}")
    print(f"  Chunk Size:        {CHUNK_SIZE:,} rows")
    
    print("\nCOLUMN ANALYSIS:")
    print(f"  Default Column:    '{DEFAULT_COLUMN_NAME}'")
    print(f"  Column Index:      {DEFAULT_COLUMN_INDEX} (0-based)")
    
    print("\nMASTER INDEX ANALYSIS:")
    print(f"  MMSI Column:       Index {MMSI_COLUMN_INDEX} (column {MMSI_COLUMN_INDEX + 1})")
    print(f"  IMO Column:        Index {IMO_COLUMN_INDEX} (column {IMO_COLUMN_INDEX + 1})")
    print(f"  MMSI Validation:   {MMSI_LENGTH} digits")
    print(f"  IMO Validation:    {IMO_LENGTH} digits")
    print(f"  Blacklisted MMSI:  {len(BLACK_MMSI_LIST)} values")
    print(f"  Blacklisted IMO:   {len(BLACK_IMO_LIST)} values")

    print("\nHARDWARE INFORMATION:")

    print(f" System:             {platform.system()} {platform.release()}, Architecture: {platform.machine()}")
    print(f" CPU Type:           {platform.processor()}, Number of CPU cores: {psutil.cpu_count(logical=False)}, Logical CPUs: {psutil.cpu_count(logical=True)}, Multithreading supported: {'Yes' if psutil.cpu_count(logical=True) > psutil.cpu_count(logical=False) else 'No'}")

    # RAM Information
    ram_info = psutil.virtual_memory()
    print(f" Total RAM:          {ram_info.total / (1024**3):.2f} GB, Available RAM: {ram_info.available / (1024**3):.2f} GB, RAM Usage: {ram_info.percent}%")

    # Python Version
    print(f" Python Version:     {sys.version}")
    try:
        gil_status = "Disabled" if not sys._is_gil_enabled() else "Enabled"
    except AttributeError:
        gil_status = "Enabled (Legacy Python)"
    print(f" GIL Status:         {gil_status}")


    print("\n" + "=" * 60)


def validate_configuration():
    """
    Validate the current configuration and return warnings/errors.
    
    Returns:
        tuple: (warnings_list, errors_list)
    """
    warnings = []
    errors = []
    
    # Check if input folder exists
    if not FOLDER_PATH.exists():
        errors.append(f"Input folder does not exist: {FOLDER_PATH}")
    
    # Check if output folder can be created
    try:
        OUTPUT_REPORT_FOLDER.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        errors.append(f"Cannot create output folder: {e}")
    
    # Check worker count
    if MAX_WORKERS < 1:
        errors.append("MAX_WORKERS must be at least 1")
    elif MAX_WORKERS > 32:
        warnings.append(f"High worker count ({MAX_WORKERS}) may cause memory issues")
    
    # Check chunk size
    if CHUNK_SIZE < 1000:
        warnings.append(f"Small chunk size ({CHUNK_SIZE}) may reduce performance")
    elif CHUNK_SIZE > 1000000:
        warnings.append(f"Large chunk size ({CHUNK_SIZE}) may increase memory usage")
    
    return warnings, errors
