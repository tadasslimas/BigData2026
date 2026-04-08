import pandas as pd
import folium
import numpy as np
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import subprocess
import os
import sys

# Ensure src directory and parent directory are in Python path for Spark workers
src_path = os.path.dirname(__file__)
parent_path = os.path.dirname(src_path)
if src_path not in sys.path:
    sys.path.insert(0, src_path)
if parent_path not in sys.path:
    sys.path.insert(0, parent_path)

# Import configuration from centralized config module
try:
    from config import (
        PRIMARY_FOLDER,
        CLEAN_DB_FOLDER_PATH,
        OUTPUT_REPORT_FOLDER,
        PROXIMITY_OUTPUT_FILE,
        PROXIMITY_TMP_FOLDER
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
    PROXIMITY_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"
    PROXIMITY_TMP_FOLDER = Path("/tmp/ais_processing")

####
## Laivu susitikimu vizualizacija - trumpa versija
## Source code - Laivu_Vizualinis_Tikrinimas.v007.pspark_versija.py
####

NFS_PATH = "/opt/A/NFS_Folder"
USING_DISTRIBUTED_PYSPARK_CALCULATION=True
USING_DISTRIBUTED_PYSPARK_CALCULATION=False

# --- KONFIGŪRACIJA (using centralized config) ---
# Nurodome tik aplanką (be *.csv), Spark nuskaitys viską viduje automatiškai
DATA_FOLDER = str(CLEAN_DB_FOLDER_PATH) 
RESULTS_FILE = str(PROXIMITY_OUTPUT_FILE)
GEO_MAPS_FOLDER = OUTPUT_REPORT_FOLDER / "Geo_Maps_Clean.SHORT"
GEO_MAPS_FOLDER.mkdir(parents=True, exist_ok=True)

MAX_PHYSICAL_SPEED_KNOTS = 60.0

# Spark session will be created lazily in run_visualization()
spark = None


def get_spark_session():
    """Create or return existing Spark session."""
    global spark
    if spark is not None:
        return spark
    
    if USING_DISTRIBUTED_PYSPARK_CALCULATION == True:
        # --- SPARK SESIJA ---
        spark = SparkSession.builder \
            .appName("Laivu_plaukianciu_short_salia_analize") \
            .master("spark://192.168.0.115:7077") \
            .config("spark.executor.memory", "10g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "112") \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .appName("Maritime_Parallel_Mapping") \
            .config("spark.executor.memory", "10g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "112") \
            .getOrCreate()
    
    return spark


def is_nfs_mounted(target_path):
    try:
        # We run 'mount' and capture output. 
        # timeout=5 prevents the script from hanging if the NFS server is dead.
        result = subprocess.run(['mount'], capture_output=True, text=True, timeout=5)
        
        for line in result.stdout.splitlines():
            # Check if the target path is in the line and 'nfs' is mentioned
            # (Works for 'nfs', 'nfs4', etc.)
            if target_path in line and 'nfs' in line.lower():
                return True
        return False
    except subprocess.TimeoutExpired:
        print(f"Error: Connection to {target_path} timed out (Server might be down).")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def haversine_np(lats, lons):
    """Greitas atstumo skaičiavimas (NM)."""
    lats, lons = np.radians(lats), np.radians(lons)
    d_lats, d_lons = lats.diff(), lons.diff()
    a = np.sin(d_lats/2)**2 + np.cos(lats.shift()) * np.cos(lats) * np.sin(d_lons/2)**2
    return 2 * np.arcsin(np.sqrt(a)) * 6371 * 0.539957

def clean_and_map_batch(pdf_group):
    """Vykdoma lygiagrečiai: Valymas + Žemėlapis su Info langu."""
    if pdf_group.empty:
        return pd.DataFrame(columns=['incident_idx', 'status'])

    # Identifikacija ir papildomi duomenys iš JOIN'o
    idx = pdf_group['incident_idx'].iloc[0]
    mmsis = pdf_group['MMSI'].unique().tolist()
    
    # Išsitraukiame papildomą info (paimame iš pirmos eilutės, nes visai grupei ji vienoda)
    trukme = pdf_group['Duration_Min'].iloc[0]
    vid_sog = pdf_group['Avg_SOG_Combined'].iloc[0]
    
    if len(mmsis) < 2:
        return pd.DataFrame({'incident_idx': [idx], 'status': ['Klaida: Nepakanka laivų']})

    m1, m2 = str(mmsis[0]), str(mmsis[1])
    tracks = []
    removed_counts = [] # Čia saugosime, kiek taškų išvalėme

    # Trajektorijų valymas
    for m in [m1, m2]:
        raw_df = pdf_group[pdf_group['MMSI'].astype(str) == m].sort_values('dt')
        
        if len(raw_df) < 3:
            tracks.append(raw_df)
            removed_counts.append(0)
            continue
            
        dist_nm = haversine_np(raw_df['Latitude'], raw_df['Longitude'])
        time_diff_h = raw_df['dt'].diff().dt.total_seconds() / 3600.0
        sog_calc = dist_nm / time_diff_h.replace(0, np.nan)
        
        mask = (sog_calc < MAX_PHYSICAL_SPEED_KNOTS) | sog_calc.isna()
        future_sog = sog_calc.shift(-1)
        mask = mask & ((future_sog < MAX_PHYSICAL_SPEED_KNOTS) | future_sog.isna())
        
        cleaned_df = raw_df[mask].copy()
        tracks.append(cleaned_df)
        removed_counts.append(len(raw_df) - len(cleaned_df))

    track1, track2 = tracks[0], tracks[1]
    
    if track1.empty or track2.empty:
        return pd.DataFrame({'incident_idx': [idx], 'status': ['Klaida: Tuščia po valymo']})

    # Žemėlapio braižymas
    try:
        m = folium.Map(location=[track1['Latitude'].mean(), track1['Longitude'].mean()], zoom_start=11)
        
        # Braižome linijas
        folium.PolyLine(track1[['Latitude', 'Longitude']].values.tolist(), color="blue", weight=4, opacity=0.7).add_to(m)
        folium.PolyLine(track2[['Latitude', 'Longitude']].values.tolist(), color="red", weight=4, opacity=0.7).add_to(m)
        
        # Sugrąžintas INFO HTML
        info_html = f"""
        <div style="font-family: Arial; font-size: 12px; width: 180px;">
            <b>Incidentas #{idx} (Išvalytas)</b><br>
            MMSI 1: {m1}<br>
            MMSI 2: {m2}<br>
            Trukme: {trukme} min.<br>
            Vid. SOG: {vid_sog} kn.<br>
            <span style="color:gray; font-size:10px;">Pašalinta 'šuolių': 
            {removed_counts[0]} (L1), {removed_counts[1]} (L2)</span>
        </div>
        """
        
        # Pridedame Markerį pradžios taške
        folium.Marker(
            [track1.iloc[0]['Latitude'], track1.iloc[0]['Longitude']],
            popup=folium.Popup(info_html, max_width=200),
            icon=folium.Icon(color='green', icon='play')
        ).add_to(m)

        # Saugome
#        file_name = f"Incidentas_{idx}_{m1}_{m2}_CLEAN.html"
        file_name = f"Laivu_plaukimas_Salia.{m1}_{m2}_{idx}.html"
        m.save(str(GEO_MAPS_FOLDER / file_name))
        
        return pd.DataFrame({'incident_idx': [idx], 'status': [f'Sėkmė: {file_name}']})
    
    except Exception as e:
        return pd.DataFrame({'incident_idx': [idx], 'status': [f'Klaida: {str(e)}']})
    

def run_visualization():
    """
    Run short version of vessel proximity visualization.
    Generates interactive HTML maps for vessel proximity incidents.
    """
    import sys
    import importlib
    
    # Ensure src directory and parent directory are in Python path for Spark workers
    src_path = os.path.dirname(__file__)
    parent_path = os.path.dirname(src_path)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    if parent_path not in sys.path:
        sys.path.insert(0, parent_path)
    
    # Import this module explicitly to ensure it's available for Spark workers
    module_name = __name__
    if module_name not in sys.modules:
        importlib.import_module('Laivu_Vizualizacija__SHORT')
    
    # Initialize Spark session
    spark = get_spark_session()
    
    # Check NFS mount if using distributed calculation
    if USING_DISTRIBUTED_PYSPARK_CALCULATION == True:
        if is_nfs_mounted(NFS_PATH):
            print(f"✅ NFS folder '{NFS_PATH}' is mounted and responsive.")
        else:
            print(f"❌ NFS folder '{NFS_PATH}' is NOT mounted.")
    
    # --- PAGRINDINIS PROCESAS ---
    try:
        print("1. Kraunami incidentų duomenys...")
        results_df = pd.read_csv(RESULTS_FILE)
        # Pridedame unikalų ID kiekvienai eilutei, jei jo nėra
        results_df['incident_idx'] = range(len(results_df))
        results_broadcast = spark.createDataFrame(results_df)

        print("2. Kraunami AIS duomenys iš klasterio...")
        # Svarbu: nurodome tik aplanką. Spark pats suras CSV failus.
        ais_sdf = spark.read.csv(DATA_FOLDER, header=True, inferSchema=True) \
            .withColumnRenamed("# Timestamp", "timestamp") \
            .withColumn("dt", F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss")) \
            .select("dt", "MMSI", "Latitude", "Longitude")

        print("3. Jungiami duomenys (Join)...")
        # Filtruojame AIS taškus, kurie patenka į incidentų laiko ir laivų rėmus
        joined_sdf = ais_sdf.join(
            F.broadcast(results_broadcast),
            (
                ((ais_sdf.MMSI == results_broadcast.MMSI_1) | (ais_sdf.MMSI == results_broadcast.MMSI_2)) &
                (ais_sdf.dt >= F.to_timestamp(results_broadcast.Start_Time)) &
                (ais_sdf.dt <= F.to_timestamp(results_broadcast.End_Time))
            )
        )

        print("4. Paleidžiamas lygiagretus apdorojimas (Pantrykite, tai gali užtrukti)...")
        # Sugrupuojame pagal incidentą ir siunčiame į Pandas UDF
        result_status = joined_sdf.groupBy("incident_idx").applyInPandas(
            clean_and_map_batch, 
            schema="incident_idx long, status string"
        )

        # Tikroji vykdymo pradžia (Action)
        final_results = result_status.collect()

        print("\n--- APDOROJIMO SUVESTINĖ ---")
        if not final_results:
            print("DĖMESIO: Nerasta jokių atitinkančių AIS duomenų. Patikrinkite laiko formatus!")
        else:
            for row in final_results:
                print(f"Incidentas #{row['incident_idx']}: {row['status']}")

    except Exception as e:
        print(f"\nKritinė klaida programos vykdymo metu:\n{str(e)}")
        raise
    finally:
        # spark.stop() # Galite atkomentuoti, jei norite uždaryti sesiją iškart po darbo
        print("\nProcesas baigtas.")


# Standalone execution
if __name__ == "__main__":
    run_visualization()
