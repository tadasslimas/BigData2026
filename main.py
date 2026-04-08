"""
CSV Data Analysis Project
Main entry point for the application.
"""

import os
import sys
import multiprocessing
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from csv_scanner import (
    scan_csv_files_for_unique_values,
    scan_csv_files_with_counts,
    save_mobile_type_summary
)
from master_indexes import create_master_indexes
from gap_analysis import run_gap_analysis
from consolidated_SOG_and_Draght_analysis import run_parallel_sog_analysis
from Plaukianciu_salia_laivu_analize import run_vessel_proximity_analysis
from anomaly_detection import run_anomaly_detection
from mmsi_outlier_analysis import run_mmsi_outlier_analysis
from final_report import run_final_report_generation
from clean_ais_database import create_clean_ais_database
from config import (
    PRIMARY_FOLDER,
    FOLDER_PATH,
    OUTPUT_REPORT_FOLDER,
    GAP_ANALYSIS_OUTPUT_FOLDER,
    MAX_WORKERS,
    CHUNK_SIZE,
    DEFAULT_COLUMN_NAME,
    DEFAULT_COLUMN_INDEX,
    MMSI_COLUMN_INDEX,
    IMO_COLUMN_INDEX,
    AIS_GAP_HOURS_THRESHOLD,
    MIN_MOVEMENT_KM,
    GAP_ANALYSIS_KEY_MODE,
    GAP_ANALYSIS_MAX_WORKERS,
    SOG_DRUGHT_OUTPUT_FILE,
    SOG_DRUGHT_MOBILE_TYPES,
    SOG_DRUGHT_MIN_SAMPLES,
    SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS,
    SOG_DRUGHT_VARIATION_THRESHOLD_PCT,
    SOG_DRUGHT_MAX_WORKERS,
    PROXIMITY_OUTPUT_FILE,
    PROXIMITY_GRID_SIZE,
    PROXIMITY_TIME_STEP,
    PROXIMITY_REQUIRED_WINDOWS,
    PROXIMITY_MAX_DIST_KM,
    PROXIMITY_SOG_DIFF_LIMIT,
    CLEAN_DB_FOLDER_PATH,
    print_configuration_summary,
    validate_configuration
)


def run_column_analysis():
    """Run analysis to extract unique values from a specific column."""
    print("\n" + "=" * 60)
    print("COLUMN ANALYSIS: Unique Values Extraction")
    print("=" * 60)
    
    data_dir = FOLDER_PATH
    
    if not os.path.exists(data_dir):
        print(f"\nError: Data directory not found: {data_dir}")
        print("Please verify the PRIMARY_FOLDER and FOLDER_PATH configuration.")
        return
    
    # Check if there are any CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    if not csv_files:
        print("\nNo CSV files found in the data directory.")
        print(f"Searched in: {data_dir}")
        print("Please ensure CSV files with a 'Type of mobile' column (second column) exist.")
        return
    
    print(f"\nFound {len(csv_files)} CSV file(s) in data directory:")
    for file in csv_files:
        print(f"  - {file}")
    
    print("\n" + "=" * 60)
    print("Starting CSV Analysis...")
    print("=" * 60 + "\n")
    
    # Scan all CSV files for unique values with counts and MMSI/IMO tracking
    value_data, files_processed = scan_csv_files_with_counts(
        folder_path=data_dir,
        column_name=DEFAULT_COLUMN_NAME,
        column_index=DEFAULT_COLUMN_INDEX,
        mmsi_column_index=MMSI_COLUMN_INDEX,
        imo_column_index=IMO_COLUMN_INDEX
    )
    
    # Save results to CSV file
    output_file = save_mobile_type_summary(
        value_data=value_data,
        output_folder=str(OUTPUT_REPORT_FOLDER),
        filename="master_list___Mobile_by_Type_summary.csv"
    )
    
    # Calculate totals
    total_types = len(value_data)
    total_rows = sum(data['count'] for data in value_data.values())
    total_unique_mmsi = len(set().union(*[data['unique_mmsi'] for data in value_data.values()]) if value_data else set())
    total_unique_imo = len(set().union(*[data['unique_imo'] for data in value_data.values()]) if value_data else set())
    
    # Print results
    print("\n" + "=" * 60)
    print(f"RESULTS: '{DEFAULT_COLUMN_NAME}' ANALYSIS")
    print("=" * 60)
    print(f"Total unique mobile types: {total_types}")
    print(f"Total rows processed:      {total_rows:,}")
    print(f"Total unique MMSI:         {total_unique_mmsi:,}")
    print(f"Total unique IMO:          {total_unique_imo:,}")
    print(f"\nResults saved to: {output_file}")
    
    if value_data:
        print("\nMobile types by count (descending):")
        sorted_data = sorted(value_data.items(), key=lambda x: x[1]['count'], reverse=True)
        for i, (mobile_type, data) in enumerate(sorted_data, 1):
            print(f"  {i:3d}. {mobile_type:30s} - Rows: {data['count']:6d}, "
                  f"MMSI: {len(data['unique_mmsi']):5d}, IMO: {len(data['unique_imo']):4d}")
    else:
        print("\nNo values found. Please check your CSV files.")
    
    print("=" * 60)


def run_master_index_analysis():
    """Run master index creation and anomaly detection."""
    print("\n" + "=" * 60)
    print("MASTER INDEX ANALYSIS: IMO/MMSI Relationship Mapping")
    print("=" * 60)
    
    # Use Clean_AIS_DB folder for master indexes
    master_index_folder = Path(CLEAN_DB_FOLDER_PATH)
    
    if not master_index_folder.exists():
        print(f"\nError: Clean AIS DB directory not found: {master_index_folder}")
        print("Please run 'Clean AIS Database Creation' (Option 0) first.")
        print("Falling back to default folder...")
        master_index_folder = Path(FOLDER_PATH)
    
    # Use Duomenys_CSV_Formate for anomaly detection (original data)
    anomaly_folder = Path(FOLDER_PATH)
    
    print(f"\nCreating master indexes from: {master_index_folder}")
    print(f"Detecting anomalies from: {anomaly_folder}")
    print(f"Output reports will be saved to: {OUTPUT_REPORT_FOLDER}")
    print(f"Processing with {MAX_WORKERS} workers, chunk size: {CHUNK_SIZE:,} rows")
    print("Processing may take several minutes for large datasets.\n")
    
    try:
        results = create_master_indexes(
            folder_path=master_index_folder,
            anomaly_folder_path=anomaly_folder,
            verbose=True
        )
        
        print("\n" + "=" * 60)
        print("ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Total unique IMO numbers: {len(results['imo_to_mmsis'])}")
        print(f"Total unique MMSI numbers: {len(results['mmsi_to_imos'])}")
        print(f"\nOutput files generated:")
        for file_path in results['output_files']:
            print(f"  - {file_path}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("Please check that CSV files contain the expected column structure.")
        print(f"Expected: MMSI in column {MMSI_COLUMN_INDEX + 1} (index {MMSI_COLUMN_INDEX}), IMO in column {IMO_COLUMN_INDEX + 1} (index {IMO_COLUMN_INDEX})")
    finally:
        # Force garbage collection to free memory after analysis
        import gc
        gc.collect()


def run_gap_analysis_menu():
    """Run AIS gap analysis to detect transmission gaps and potential spoofing."""
    print("\n" + "=" * 60)
    print("GAP ANALYSIS: AIS Transmission Gap Detection")
    print("=" * 60)
    
    # Use Clean_AIS_DB as the data source
    folder_path = Path(CLEAN_DB_FOLDER_PATH)
    
    if not folder_path.exists():
        print(f"\nError: Clean AIS DB directory not found: {folder_path}")
        print("Please run 'Clean AIS Database Creation' (Option 0) first.")
        print("Falling back to default folder...")
        folder_path = Path(FOLDER_PATH)
    
    print(f"\nAnalyzing CSV files in: {folder_path}")
    print("This will detect AIS transmission gaps indicating potential spoofing.")
    print(f"Output reports will be saved to: {GAP_ANALYSIS_OUTPUT_FOLDER}")
    print(f"\nConfiguration:")
    print(f"  - Gap threshold: {AIS_GAP_HOURS_THRESHOLD} hours")
    print(f"  - Minimum movement: {MIN_MOVEMENT_KM} km")
    print(f"  - Identity mode: {GAP_ANALYSIS_KEY_MODE}")
    print(f"  - Max workers: {GAP_ANALYSIS_MAX_WORKERS}")
    print("\nProcessing may take several minutes for large datasets.\n")
    
    try:
        report_path = run_gap_analysis()
        
        if report_path:
            print("\n" + "=" * 60)
            print("GAP ANALYSIS COMPLETE")
            print("=" * 60)
            print(f"\n✅ Report saved to: {report_path}")
            print("=" * 60)
        else:
            print("\n⚠️  Gap analysis failed. Check error messages above.")
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("Please check that CSV files contain the expected column structure.")
        print("Ensure MMSI whitelist files exist in the output/by_type folder.")
    finally:
        # Force garbage collection to free memory after analysis
        import gc
        gc.collect()


def run_sog_draught_analysis_menu():
    """Run SOG and Draught analysis to detect speed and draught anomalies."""
    print("\n" + "=" * 60)
    print("SOG AND DRAUGHT ANALYSIS: Speed and Draught Statistics")
    print("=" * 60)
    
    # Use Clean_AIS_DB as the data source
    folder_path = Path(CLEAN_DB_FOLDER_PATH)
    
    if not folder_path.exists():
        print(f"\nError: Clean AIS DB directory not found: {folder_path}")
        print("Please run 'Clean AIS Database Creation' (Option 0) first.")
        print("Falling back to default folder...")
        folder_path = Path(FOLDER_PATH)
    
    print(f"\nAnalyzing CSV files in: {folder_path}")
    print("This will analyze vessel speed (SOG) and draught data.")
    print(f"Output report will be saved to: {SOG_DRUGHT_OUTPUT_FILE}")
    print(f"\nConfiguration:")
    print(f"  - Mobile types: {SOG_DRUGHT_MOBILE_TYPES}")
    print(f"  - Min samples: {SOG_DRUGHT_MIN_SAMPLES}")
    print(f"  - Suspicious speed threshold: {SOG_DRUGHT_SUSPICIOUS_AVG_KNOTS} knots")
    print(f"  - Draught variation threshold: {SOG_DRUGHT_VARIATION_THRESHOLD_PCT}%")
    print(f"  - Max workers: {SOG_DRUGHT_MAX_WORKERS}")
    print("\nProcessing may take several minutes for large datasets.\n")
    
    try:
        # Run the parallel SOG analysis
        run_parallel_sog_analysis()
        
        print("\n" + "=" * 60)
        print("SOG AND DRAUGHT ANALYSIS COMPLETE")
        print("=" * 60)
        print(f"\n✅ Report saved to: {SOG_DRUGHT_OUTPUT_FILE}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("Please check that CSV files contain the expected column structure.")
        print("Required columns: 'Type of mobile', 'MMSI', 'SOG', 'Draught'")
    finally:
        # Force garbage collection to free memory after analysis
        import gc
        gc.collect()


def run_vessel_proximity_analysis_menu():
    """Run vessel proximity analysis to detect vessels sailing close to each other."""
    print("\n" + "=" * 60)
    print("VESSEL PROXIMITY ANALYSIS: Detect Close Vessel Encounters")
    print("=" * 60)
    
    # Use Clean_AIS_DB as the data source
    folder_path = Path(CLEAN_DB_FOLDER_PATH)
    
    if not folder_path.exists():
        print(f"\nError: Clean AIS DB directory not found: {folder_path}")
        print("Please run 'Clean AIS Database Creation' (Option 0) first.")
        print("Falling back to default folder...")
        folder_path = Path(FOLDER_PATH)
    
    # Check if speed report exists (recommended prerequisite)
    if not SOG_DRUGHT_OUTPUT_FILE.exists():
        print(f"\n⚠️  Warning: Speed report not found: {SOG_DRUGHT_OUTPUT_FILE}")
        print("   It's recommended to run SOG/Draught Analysis first.")
        print("   This helps filter out stationary vessels.")
        response = input("\nContinue anyway? (y/n): ").strip().lower()
        if response != 'y':
            print("Returning to main menu...")
            return
    
    print(f"\nAnalyzing CSV files in: {folder_path}")
    print("This will detect vessels sailing close to each other for extended periods.")
    print(f"Output report will be saved to: {PROXIMITY_OUTPUT_FILE}")
    print(f"\nConfiguration:")
    print(f"  - Grid size: {PROXIMITY_GRID_SIZE} degrees (~1.1 km)")
    print(f"  - Time step: {PROXIMITY_TIME_STEP} seconds ({PROXIMITY_TIME_STEP//60} minutes)")
    print(f"  - Required windows: {PROXIMITY_REQUIRED_WINDOWS} (~{PROXIMITY_REQUIRED_WINDOWS * PROXIMITY_TIME_STEP // 3600} hours)")
    print(f"  - Max distance: {PROXIMITY_MAX_DIST_KM} km")
    print(f"  - SOG diff limit: {PROXIMITY_SOG_DIFF_LIMIT} knots")
    print("\nProcessing may take several minutes for large datasets.\n")
    
    try:
        found_count = run_vessel_proximity_analysis(
            data_folder=folder_path,
            verbose=True
        )
        
        if found_count > 0:
            print("\n" + "=" * 60)
            print("VESSEL PROXIMITY ANALYSIS COMPLETE")
            print("=" * 60)
            print(f"\n✅ Found {found_count} vessel proximity incidents.")
            print(f"   Report saved to: {PROXIMITY_OUTPUT_FILE}")
            print("=" * 60)
        elif found_count == 0:
            print("\n" + "=" * 60)
            print("VESSEL PROXIMITY ANALYSIS COMPLETE")
            print("=" * 60)
            print("\nℹ️  No vessel proximity incidents found.")
            print("   This could mean:")
            print(f"   - No vessels were within {PROXIMITY_MAX_DIST_KM} km of each other")
            print(f"   - Vessels didn't stay close for {PROXIMITY_REQUIRED_WINDOWS * PROXIMITY_TIME_STEP // 3600} hours")
            print("=" * 60)
        else:
            print("\n⚠️  Analysis failed. Check error messages above.")
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        print("Please check that CSV files contain the expected column structure.")
        print("Required columns: Timestamp, Latitude, Longitude, MMSI, SOG")
    finally:
        # Force garbage collection to free memory after analysis
        import gc
        gc.collect()
        print("\n🧹 Memory cleanup completed.")


def run_anomaly_detection_menu():
    """Run anomaly detection for AIS gaps and draught changes."""
    print("\n" + "=" * 60)
    print("ANOMALY DETECTION: Gap and Draught Change Analysis")
    print("=" * 60)
    
    # Use Clean_AIS_DB as the data source
    folder_path = Path(CLEAN_DB_FOLDER_PATH)
    
    if not folder_path.exists():
        print(f"\nError: Clean AIS DB directory not found: {folder_path}")
        print("Please run 'Clean AIS Database Creation' (Option 0) first.")
        print("Falling back to default folder...")
        folder_path = Path(FOLDER_PATH)
    
    # Check if master MMSI file exists
    master_mmsi_file = OUTPUT_REPORT_FOLDER / "master_MMSI_data.csv"
    if not master_mmsi_file.exists():
        print(f"\n⚠️  Warning: Master MMSI file not found: {master_mmsi_file}")
        print("   It's recommended to run Master Index Analysis first.")
        response = input("\nContinue anyway? (y/n): ").strip().lower()
        if response != 'y':
            print("Returning to main menu...")
            return
    
    print(f"\nAnalyzing CSV files in: {folder_path}")
    print("This will detect AIS transmission gaps (>2 hours) and draught changes (>5%).")
    print(f"Output reports will be saved to:")
    print(f"  - Gap report: {OUTPUT_REPORT_FOLDER / 'laivai_dingimai.csv'}")
    print(f"  - Draught changes: {OUTPUT_REPORT_FOLDER / 'mmsi_draught_change.csv'}")
    print(f"\nConfiguration:")
    print(f"  - Gap threshold: 2 hours")
    print(f"  - Draught change threshold: 5%")
    print(f"  - Memory limit: 2G")
    print(f"  - Workers: {MAX_WORKERS}")
    print("\nProcessing may take several minutes for large datasets.\n")
    
    try:
        run_anomaly_detection()
    except Exception as e:
        print(f"\nError during anomaly detection: {str(e)}")
    finally:
        # Force garbage collection to free memory after analysis
        import gc
        gc.collect()


def run_mmsi_outlier_analysis_menu():
    """Run MMSI outlier analysis to detect position jumps."""
    print("\n" + "=" * 60)
    print("MMSI OUTLIER ANALYSIS: Position Jump Detection")
    print("=" * 60)
    
    # Use Clean_AIS_DB as the data source
    folder_path = Path(CLEAN_DB_FOLDER_PATH)
    
    if not folder_path.exists():
        print(f"\nError: Clean AIS DB directory not found: {folder_path}")
        print("Please run 'Clean AIS Database Creation' (Option 0) first.")
        print("Falling back to default folder...")
        folder_path = Path(FOLDER_PATH)
    
    # Check if proximity file exists (recommended prerequisite)
    proximity_file = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"
    if not proximity_file.exists():
        print(f"\n⚠️  Warning: Proximity file not found: {proximity_file}")
        print("   It's recommended to run Vessel Proximity Analysis first.")
        print("   This provides a filter list of vessels of interest.")
        response = input("\nContinue anyway? (y/n): ").strip().lower()
        if response != 'y':
            print("Returning to main menu...")
            return
    
    print(f"\nAnalyzing CSV files in: {folder_path}")
    print("This will detect vessels with suspicious position jumps exceeding 50 km.")
    print(f"Output reports will be saved to:")
    print(f"  - Summary: {OUTPUT_REPORT_FOLDER / 'mmsi_outlier_summary.csv'}")
    print(f"  - Details: {OUTPUT_REPORT_FOLDER / 'mmsi_outlier_details.csv'}")
    print(f"\nConfiguration:")
    print(f"  - Number of shards: 20")
    print(f"  - Max distance threshold: 50.0 km")
    print(f"  - Filter file: {proximity_file}")
    print("\nProcessing may take several minutes for large datasets.\n")
    
    try:
        run_mmsi_outlier_analysis()
    except Exception as e:
        print(f"\nError during MMSI outlier analysis: {str(e)}")
    finally:
        # Force garbage collection to free memory after analysis
        import gc
        gc.collect()


def run_final_report_menu():
    """Generate comprehensive final report consolidating all analysis results."""
    print("\n" + "=" * 60)
    print("FINAL REPORT GENERATION: Comprehensive Analysis Summary")
    print("=" * 60)
    
    print("\nThis will generate a comprehensive report including:")
    print("  • Vessel identification anomalies (IMO/MMSI)")
    print("  • Master data overview")
    print("  • Mobile type summary")
    print("  • Gap analysis with filters (>4h, >10km, MMSI changes)")
    print("  • Vessel proximity meetings")
    print("  • Draught change analysis")
    print("  • Data outlier summary")
    print("  • DFSI risk index calculation and ranking")
    print("\n⚠️  Note: Run all other analyses first for complete results.")
    
    response = input("\nGenerate final report? (y/n): ").strip().lower()
    if response != 'y':
        print("Returning to main menu...")
        return
    
    try:
        report_path = run_final_report_generation()
        
        if report_path:
            print("\n" + "=" * 60)
            print("FINAL REPORT COMPLETE")
            print("=" * 60)
            print(f"\n✅ Report saved to: {report_path}")
            print(f"   DFSI full list: {OUTPUT_REPORT_FOLDER / 'dfsi_full_list.csv'}")
            print(f"   High-risk vessels: {OUTPUT_REPORT_FOLDER / 'dfsi_high_risk_sorted.csv'}")
            print("=" * 60)
        else:
            print("\n⚠️  Report generation failed. Check error messages above.")
        
    except Exception as e:
        print(f"\nError during report generation: {str(e)}")
    finally:
        # Force garbage collection to free memory
        import gc
        gc.collect()


def run_vessel_proximity_visualization_short_menu():
    """Run short version of vessel proximity visualization for quick access."""
    print("\n" + "=" * 60)
    print("VESSEL PROXIMITY VISUALIZATION (Short Version)")
    print("=" * 60)
    
    print("\nThis will generate interactive HTML maps for vessel proximity incidents.")
    print("Features:")
    print("  • Cleaned trajectory visualization (removes position jumps)")
    print("  • Interactive Folium maps with popup information")
    print("  • Shows both vessels' tracks with color coding")
    print("  • Displays incident details (duration, average SOG)")
    print(f"\nOutput will be saved to:")
    print(f"  - {OUTPUT_REPORT_FOLDER / 'Geo_Maps_Clean.SHORT'}")
    print(f"\nData sources:")
    print(f"  - Proximity incidents: {PROXIMITY_OUTPUT_FILE}")
    print(f"  - AIS data: {CLEAN_DB_FOLDER_PATH}")
    print(f"\nConfiguration:")
    print(f"  - Max physical speed filter: 60 knots")
    print(f"  - Uses PySpark for parallel processing")
    print("\n⚠️  Note: Requires vessel proximity analysis to be run first.")
    print("   Processing may take several minutes depending on data size.")
    
    response = input("\nGenerate proximity visualizations? (y/n): ").strip().lower()
    if response != 'y':
        print("Returning to main menu...")
        return
    
    try:
        # Run the visualization script directly using subprocess
        # This avoids module import issues with Spark workers
        import subprocess
        script_path = os.path.join(os.path.dirname(__file__), 'src', 'Laivu_Vizualizacija__SHORT.py')
        
        print(f"\n🚀 Running visualization script: {script_path}")
        print("=" * 60 + "\n")
        
        # Run the script directly
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=os.path.dirname(__file__)
        )
        
        if result.returncode == 0:
            print("\n" + "=" * 60)
            print("VISUALIZATION COMPLETE")
            print("=" * 60)
            print(f"\n✅ Maps saved to: {OUTPUT_REPORT_FOLDER / 'Geo_Maps_Clean'}")
            print("=" * 60)
        else:
            print(f"\n❌ Visualization failed with exit code {result.returncode}")
        
    except Exception as e:
        print(f"\nError during visualization: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Force garbage collection to free memory
        import gc
        gc.collect()


def run_vessel_proximity_visualization_full_menu():
    """Run full version of vessel proximity visualization with detailed output."""
    print("\n" + "=" * 60)
    print("VESSEL PROXIMITY VISUALIZATION (Full Version)")
    print("=" * 60)
    
    print("\nThis will generate comprehensive interactive HTML maps for vessel proximity incidents.")
    print("Features:")
    print("  • Detailed trajectory visualization with full data")
    print("  • Interactive Folium maps with extensive popup information")
    print("  • Shows both vessels' tracks with color coding")
    print("  • Displays complete incident details and statistics")
    print("  • Enhanced analysis and reporting")
    print(f"\nOutput will be saved to:")
    print(f"  - {OUTPUT_REPORT_FOLDER / 'Geo_Maps_Clean.FULL'}")
    print(f"\nData sources:")
    print(f"  - Proximity incidents: {PROXIMITY_OUTPUT_FILE}")
    print(f"  - AIS data: {CLEAN_DB_FOLDER_PATH}")
    print(f"\nConfiguration:")
    print(f"  - Uses PySpark for parallel processing")
    print(f"  - Full detailed output mode")
    print("\n⚠️  Note: Requires vessel proximity analysis to be run first.")
    print("   Processing may take several minutes depending on data size.")
    
    response = input("\nGenerate full proximity visualizations? (y/n): ").strip().lower()
    if response != 'y':
        print("Returning to main menu...")
        return
    
    try:
        # Run the visualization script directly using subprocess
        # This avoids module import issues with Spark workers
        import subprocess
        script_path = os.path.join(os.path.dirname(__file__), 'src', 'Laivu_Vizualizacija__FULL.py')
        
        print(f"\n🚀 Running visualization script: {script_path}")
        print("=" * 60 + "\n")
        
        # Run the script directly
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=os.path.dirname(__file__)
        )
        
        if result.returncode == 0:
            print("\n" + "=" * 60)
            print("VISUALIZATION COMPLETE")
            print("=" * 60)
            print(f"\n✅ Maps saved to: {OUTPUT_REPORT_FOLDER / 'Geo_Maps_Clean'}")
            print("=" * 60)
        else:
            print(f"\n❌ Visualization failed with exit code {result.returncode}")
        
    except Exception as e:
        print(f"\nError during visualization: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Force garbage collection to free memory
        import gc
        gc.collect()


def run_clean_ais_database_menu():
    """Run clean AIS database creation to filter and deduplicate vessel data."""
    print("\n" + "=" * 60)
    print("CLEAN AIS DATABASE CREATION")
    print("=" * 60)
    
    folder_path = Path(FOLDER_PATH)
    
    if not folder_path.exists():
        print(f"\nError: Data directory not found: {folder_path}")
        print("Please verify the PRIMARY_FOLDER and FOLDER_PATH configuration.")
        return
    
    print(f"\nProcessing CSV files in: {folder_path}")
    print("This will create a cleaned and filtered AIS database with:")
    print("  • Only Class A vessels")
    print("  • Valid MMSI identifiers (9 digits)")
    print("  • Valid coordinates (excluding 91.0, >89.9)")
    print("  • Movement filtering (minimum 5 km displacement)")
    print("  • Speed filtering (minimum 2.0 knots)")
    print("  • Minimum position reports (25 per day)")
    print("  • Sliding window deduplication")
    print(f"\nOutput will be saved to:")
    print(f"  - {PRIMARY_FOLDER / 'Clean_AIS_DB' / 'Clean_AIS_DB.csv'}")
    print(f"\nConfiguration:")
    print(f"  - Min positions per day: 25")
    print(f"  - Minimum movement: 5.0 km")
    print(f"  - Minimum speed: 2.0 knots")
    print(f"  - CPU cores: {multiprocessing.cpu_count()}")
    print("\n⚠️  Note: This is a two-pass process using multiprocessing.")
    print("   Processing may take several minutes for large datasets.")
    
    response = input("\nCreate clean AIS database? (y/n): ").strip().lower()
    if response != 'y':
        print("Returning to main menu...")
        return
    
    try:
        from clean_ais_database import create_clean_ais_database
        valid_count = create_clean_ais_database(verbose=True)
        
        if valid_count > 0:
            print("\n" + "=" * 60)
            print("CLEAN AIS DATABASE CREATION COMPLETE")
            print("=" * 60)
            print(f"\n✅ Valid vessels processed: {valid_count}")
            print("=" * 60)
        else:
            print("\n⚠️  No valid vessels found. Check filter thresholds.")
        
    except Exception as e:
        print(f"\nError during clean database creation: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Force garbage collection to free memory
        import gc
        gc.collect()


def main():
    """Main function to run the CSV data analysis."""
    print("\n" + "=" * 60)
    print("WELCOME TO CSV DATA ANALYSIS!")
    print("=" * 60)
    print("\nThis is your starting point for maritime data analysis.")
    
    # Display configuration summary
    print_configuration_summary()
    
    # Validate configuration and show any warnings
    warnings, errors = validate_configuration()
    if errors:
        print("\n⚠️  CONFIGURATION ERRORS:")
        for error in errors:
            print(f"  ❌ {error}")
        print("\nPlease fix the errors above before running analysis.")
        return
    
    if warnings:
        print("\n⚠️  CONFIGURATION WARNINGS:")
        for warning in warnings:
            print(f"  ⚡ {warning}")
    
    print("\n📊 AVAILABLE ANALYSIS MODES:")
    print("  0. Clean AIS Database - Create filtered and deduplicated AIS database")
    print("  1. Column Analysis - Extract unique values from specific column")
    print("     Master Index Analysis - Create IMO/MMSI indexes and detect anomalies")
    print("  2. Gap Analysis - Detect AIS transmission gaps and potential spoofing")
    print("  3. SOG/Draught Analysis - Analyze vessel speed and draught statistics")
    print("  4. Vessel Proximity Analysis - Detect vessels sailing close to each other")
    print("  5. Anomaly Detection - Detect AIS gaps and draught changes")
    print("  6. MMSI Outlier Analysis - Detect suspicious position jumps")
    print("  7. Run All Analyses (excluding final report)")
    print("\n" + "=" * 60)
    print("  8. Final Report - Generate comprehensive analysis summary with DFSI")
    print("  9. Exit")
    print("\n" + "=" * 60)
    print("  20. Vessel Proximity Visualization (Short) - Generate HTML maps for proximity incidents")
    print("  21. Vessel Proximity Visualization (Long) - Detailed visualization for proximity incidents")


    while True:
        print("\n" + "=" * 60)
        choice = input("\nSelect analysis mode (0-9): ").strip()
        
        if choice == '0':
            run_clean_ais_database_menu()
        elif choice == '1':
            run_column_analysis()
            run_master_index_analysis()
        elif choice == '2':
            run_gap_analysis_menu()
        elif choice == '3':
            run_sog_draught_analysis_menu()
        elif choice == '4':
            run_vessel_proximity_analysis_menu()
        elif choice == '5':
            run_anomaly_detection_menu()
        elif choice == '6':
            run_mmsi_outlier_analysis_menu()
        elif choice == '8':
            run_final_report_menu()
        elif choice == '7':
            print("\n" + "=" * 60)
            print("Running All Analyses (using Clean_AIS_DB as data source)")
            print("=" * 60)
            run_column_analysis()
            run_master_index_analysis()
            run_gap_analysis_menu()
            run_sog_draught_analysis_menu()
            run_vessel_proximity_analysis_menu()
            run_anomaly_detection_menu()
            run_mmsi_outlier_analysis_menu()
        elif choice == '9':
            print("\n✅ Exiting. Goodbye!")
            break
        elif choice == '20':
            run_vessel_proximity_visualization_short_menu()
        elif choice == '21':
            run_vessel_proximity_visualization_full_menu()
        else:
            print("❌ Invalid choice. Please enter a number between 0-9, 20, or 21.")


if __name__ == "__main__":
    main()
