from pyspark.sql import SparkSession
import json
import os  
import csv
import folium
from math import radians, sin, cos, sqrt, atan2
from pathlib import Path
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
        PROXIMITY_OUTPUT_FILE
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    CLEAN_DB_FOLDER_PATH = PRIMARY_FOLDER / "Clean_AIS_DB"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
    PROXIMITY_OUTPUT_FILE = OUTPUT_REPORT_FOLDER / "vessel_proximity_meetings.csv"

DISTRIBUTED_SPARK = True
USING_DISTRIBUTED_PYSPARK_CALCULATION=False
# DISTRIBUTED_SPARK = False
##### Code version
CODE_VERSION = "v001_FULL"

# Distance threshold for filtering outlier coordinates (in kilometers)
# Ships typically travel at 15-25 knots (28-46 km/h). Max reasonable distance between AIS transmissions:
# - For 1-hour interval: ~50 km (very fast ship)
# - For 10-minute interval: ~8 km
# Setting threshold to catch GPS errors while keeping legitimate fast movements
MAX_DISTANCE_KM = 50.0  # Maximum allowed distance between consecutive points
MIN_DISTANCE_KM = 0.001  # Minimum distance to consider as movement (avoid noise)


# --- KONFIGŪRACIJA (using centralized config) ---
NFS_PATH = "/opt/A/NFS_Folder"
PRIMARY_WORKING_FOLDER = PRIMARY_FOLDER
if USING_DISTRIBUTED_PYSPARK_CALCULATION == True:
    PRIMARY_WORKING_FOLDER = Path(NFS_PATH) / "Maritime_Shadow_Fleet_Detection"
# Specify the folder containing CSV files
PRIMARY_CSV_DATA_FOLDER = str(CLEAN_DB_FOLDER_PATH)


VESSELS_PROXIMITY_FILE = str(PROXIMITY_OUTPUT_FILE)

DFSI_PROXIMITY_FILE = str(OUTPUT_REPORT_FOLDER / "dfsi_proximity_meetings.csv")
DFSI_OUTLIER_LOG_FILE = str(OUTPUT_REPORT_FOLDER / "dfsi_outlier_filtering_log.csv")

# Initialize the outlier log CSV file (write header once)
with open(DFSI_OUTLIER_LOG_FILE, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['MMSI', 'Total_Filtered_Distance_KM', 'Number_of_Removed_Points', 'Max_Distance_Threshold_KM']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()



GEO_MAPS_FOLDER = OUTPUT_REPORT_FOLDER / "Geo_Maps_Clean.FULL"
GEO_MAPS_FOLDER.mkdir(parents=True, exist_ok=True)



def extract_country_code(mmsi):
  """
  Extracts the country code from an MMSI number.

  Args:
    mmsi: The MMSI number as a string.

  Returns:
    The country code as a string, or None if the MMSI number is invalid.
  """
  try:
    country_code = mmsi[:3]  # Extract the first 3 digits
    return country_code
  except (TypeError, IndexError):
    return None # Handle cases where the input is not a string or too short

def get_country_name(country_code):
  """
  Retrieves the country name from a CSV file based on the short code.

  Args:
    country_code: The country code (e.g., "AL").

  Returns:
    The country name as a string, or None if the code is not found.
  """
  try:
    # Use project data folder instead of external folder
    # MMSI_Codes.csv is now located in the project's data/ folder
    project_root = Path(__file__).parent.parent
    MMSI_Country_Code_csv_file = project_root / "data" / "MMSI_Codes.csv"

    with open(MMSI_Country_Code_csv_file, 'r', newline='') as file:
      reader = csv.DictReader(file)  # Use DictReader for easier access by column name
      for row in reader:
        if row['MID'] == country_code:  # Case-sensitive comparison
          return row['Country Name']
    return None # Code not found in the CSV file

  except FileNotFoundError:
    print(f"Error: CSV file '{MMSI_Country_Code_csv_file}' not found.")
    print("Please ensure MMSI_Codes.csv is located in the project's data/ folder.")
    return None
  except Exception as e:
      print(f"An error occurred: {e}") # Catch other potential errors during CSV processing
      return None
  
if USING_DISTRIBUTED_PYSPARK_CALCULATION == True:
# --- SPARK SESIJA ---
    spark = SparkSession.builder \
        .appName(f"Greta_Plaukianciu_Laivu_Diagrama.MP.{CODE_VERSION}") \
        .master("spark://192.168.0.115:7077") \
        .config("spark.executor.memory", "10g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "112") \
        .getOrCreate()

if USING_DISTRIBUTED_PYSPARK_CALCULATION != True:
    spark = SparkSession.builder \
        .appName(f"Greta_Plaukianciu_Laivu_Diagrama.{CODE_VERSION}") \
        .config("spark.executor.memory", "10g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "112") \
        .getOrCreate()

# --- 1. LOAD ALL CSV DATA ---
# Gather all CSV file paths
csv_files = [f for f in os.listdir(PRIMARY_CSV_DATA_FOLDER) if f.endswith(".csv")]
if not csv_files:
    print("No CSV files found in the directory.")
    spark.stop()
    exit()

csv_files.sort()

# Construct file paths and read them all at once
# Note: spark.read.csv handles reading a list of files. It infers schema from the first file.
# Ensure all files have the same header structure for this to work seamlessly.
csv_paths = [os.path.join(PRIMARY_CSV_DATA_FOLDER, f) for f in sorted(csv_files)]

print(f"Loading {len(csv_paths)} CSV files into Spark DataFrame...")
data = spark.read.csv(csv_paths, header=True, inferSchema=True, sep=",")


# --- 3. PROCESSING & MAP GENERATION ---
### Geo_Maps_Main_Folder = f"{PRIMARY_WORKING_FOLDER}/Geo_Maps"
Geo_Maps_Main_Folder = GEO_MAPS_FOLDER
# Create base folder if not exists
if not os.path.exists(Geo_Maps_Main_Folder):
    os.makedirs(Geo_Maps_Main_Folder)

def get_country_info(mmsi_num):
    # Placeholder for your extract_country_code logic
    # Ensure this is outside the inner loop or cached if it involves an API call
    try:
        cc = extract_country_code(str(mmsi_num))
        cn = get_country_name(cc)
        return cc, cn
    except:
        return "XX", "Unknown"


def get_color(x):
    if 0 <= x < 0.2:
        return 'green'
    elif 0.2 < x <= 5:
        return 'blue'
    elif 5 < x <= 10:
        return 'orange'
    else:
        return 'red'

def get_type_color(type_name):
    if type_name == 'Tanker':
        return '<span style="color: red;">{}</span>'.format(type_name)
    else:
        return '{}'.format(type_name)

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on Earth using the Haversine formula.
    
    Args:
        lat1, lon1: Latitude and longitude of point 1 (in degrees)
        lat2, lon2: Latitude and longitude of point 2 (in degrees)
    
    Returns:
        Distance in kilometers
    """
    R = 6371.0  # Earth's radius in kilometers
    
    # Convert degrees to radians
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    
    # Haversine formula
    a = sin(delta_lat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return R * c

def filter_outlier_coordinates(points_list, mmsi, max_distance_km=MAX_DISTANCE_KM, output_csv_file=None):
    """
    Filter out erroneous GPS coordinates that are unrealistically far from previous points.
    This removes "flying ship" artifacts caused by GPS errors or data corruption.
    
    Args:
        points_list: List of [lat, lon] coordinates in chronological order
        mmsi: MMSI identifier for logging purposes
        max_distance_km: Maximum allowed distance between consecutive points (default: 50 km)
        output_csv_file: Optional path to CSV file for logging filtering results
    
    Returns:
        Filtered list of coordinates with outliers removed
    """
    if len(points_list) < 2:
        return points_list
    
    filtered_points = [points_list[0]]  # Always keep the first point
    total_filtered_distance = 0.0
    
    for i in range(1, len(points_list)):
        current_point = points_list[i]
        previous_point = filtered_points[-1]  # Compare with last valid point
        
        lat1, lon1 = previous_point[0], previous_point[1]
        lat2, lon2 = current_point[0], current_point[1]
        
        distance = haversine_distance(lat1, lon1, lat2, lon2)
        
        # Keep point if it's within reasonable distance
        if distance <= max_distance_km:
            filtered_points.append(current_point)
        else:
            print(f"  ⚠️  Filtered outlier: {distance:.1f} km jump at point {i} "
                  f"({lat1:.4f}, {lon1:.4f}) -> ({lat2:.4f}, {lon2:.4f})")
            total_filtered_distance += distance
    
    removed_count = len(points_list) - len(filtered_points)
    if removed_count > 0:
        print(f"  ✓ Removed {removed_count} outlier coordinate(s) from trajectory")
        
        # Log to CSV if output file is specified
        if output_csv_file:
            file_exists = os.path.exists(output_csv_file)
            with open(output_csv_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['MMSI', 'Total_Filtered_Distance_KM', 'Number_of_Removed_Points', 'Max_Distance_Threshold_KM']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow({
                    'MMSI': mmsi,
                    'Total_Filtered_Distance_KM': round(total_filtered_distance, 2),
                    'Number_of_Removed_Points': removed_count,
                    'Max_Distance_Threshold_KM': max_distance_km
                })
    
    return filtered_points



unique_pairs = []
seen_pairs = set()

try:
    with open(VESSELS_PROXIMITY_FILE, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            # Get the raw MMSI values
            m1 = row['MMSI_1']
            m2 = row['MMSI_2']
            
            # 2. Normalize the pair: Sort them so (A, B) is treated same as (B, A)
            # This prevents duplicates if the ships switch roles or appear in reverse order
            mmsi_sort = tuple(sorted([m1, m2]))
            
            # 3. Only add if we haven't seen this connection before
            if mmsi_sort not in seen_pairs:
                seen_pairs.add(mmsi_sort)
                
                # Add the unique pair to our list with metadata (keep first occurrence data)
                unique_pairs.append({
                    'mmsi_a': m1,
                    'mmsi_b': m2,
                    'duration_min': row['Duration_Min'],
                    # Note: Start/End times are usually stored as well for Folium animations
                    # 'start_time': row['Start_Time'], 
                    # 'end_time': row['End_Time'], 
                    'normalized_pair': mmsi_sort 
                })
            
    # Convert set count to integer for summary
    total_unique = len(unique_pairs)
    
    print(f"Total unique connections found: {total_unique}\n")
    
    # 4. Display the list for verification
    print("List ready for iteration:\n")
    print("-" * 40)
    for i, p in enumerate(unique_pairs, 1):
        print(f"Pair {i}: {p['mmsi_a']} <-> {p['mmsi_b']}")
        print(f"         Duration: {p['duration_min']} mins\n")

    print("=" * 40)
    print(f"Final List Length (len(unique_pairs)): {len(unique_pairs)}")
    print("You can now run: for pair in unique_pairs: ...")

except FileNotFoundError:
    print(f"Error: File '{VESSELS_PROXIMITY_FILE}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")



for pair in unique_pairs:
    SHIP1 = pair['mmsi_a']
    SHIP2 = pair['mmsi_b']  

    ToMD = 'Class A'
    print(f"Processing pair of ships using following MMSIs: {SHIP1}, {SHIP2}, Type: {ToMD}")
    
    # Filter Data for Current MMSI
    filtered_df_ship1 = data.filter((data["MMSI"] == SHIP1))
    
    # Collect only this specific filtered subset to the driver
    rows_list_ship1 = filtered_df_ship1.collect()
    
    if not rows_list_ship1:
        print(f"No rows found for MMSI: {SHIP1}, ToMD: {ToMD}")
        continue
    
    # --- 5.1. Prepare Map ---

    # Get country info
    try:
        Country_Code_Ship1, Country_Name_Ship1 = get_country_info(SHIP1)
    except:
        Country_Code_Ship1, Country_Name_Ship1 = "00", "Unknown"


    # Filter Data for Current MMSI
    filtered_df_ship2 = data.filter((data["MMSI"] == SHIP2))
    
    # Collect only this specific filtered subset to the driver
    rows_list_ship2 = filtered_df_ship2.collect()
    
    if not rows_list_ship2:
        print(f"No rows found for MMSI: {SHIP2}, ToMD: {ToMD}")
        continue
    
    # --- 5.1. Prepare Map ---

    # Get country info
    try:
        Country_Code_Ship2, Country_Name_Ship2 = get_country_info(SHIP2)
    except:
        Country_Code_Ship2, Country_Name_Ship2 = "00", "Unknown"


    # --- 3.2. Prepare Map ---
    # Initialize map
    m = folium.Map(location=[56, 10], zoom_start=8) 

    # Helper functions to extract metadata


    circle_list_ship1 = []
    points_ship1 = []
    # Build GeoJSON Features
    for row in rows_list_ship1:
        circle_list_ship1.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["Longitude"]), float(row["Latitude"])]
            },
            "properties": {
                "MMSI": str(row["MMSI"]),
                "Type": row["Type of mobile"],
                "Timestamp": row["# Timestamp"],
                "Navigational_Status": row["Navigational status"],
                "Name": row["Name"],
                "ROT": row["ROT"],
                "SOG": row["SOG"],
                "COG": row["COG"],
                "Heading": row["Heading"],
                "IMO": row["IMO"],
                "Callsign": row["Callsign"],
                "Ship_type": row["Ship type"],
                "Cargo_type": row["Cargo type"],
                "Width": row["Width"],
                "Length": row["Length"],
                "Draught": row["Draught"],
                "Destination": row["Destination"],
                "ETA": row["ETA"],
                "Data_source_type": row["Data source type"],
                "A": row["A"],
                "B": row["B"],
                "C": row["C"],
                "D": row["D"],
                "Type_of_positioning_device": row["Type of position fixing device"]
            }
        })

    circle_list_ship2 = []
    points_ship2 = []
    # Build GeoJSON Features
    for row in rows_list_ship2:
        circle_list_ship2.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["Longitude"]), float(row["Latitude"])]
            },
            "properties": {
                "MMSI": str(row["MMSI"]),
                "Type": row["Type of mobile"],
                "Timestamp": row["# Timestamp"],
                "Navigational_Status": row["Navigational status"],
                "Name": row["Name"],
                "ROT": row["ROT"],
                "SOG": row["SOG"],
                "COG": row["COG"],
                "Heading": row["Heading"],
                "IMO": row["IMO"],
                "Callsign": row["Callsign"],
                "Ship_type": row["Ship type"],
                "Cargo_type": row["Cargo type"],
                "Width": row["Width"],
                "Length": row["Length"],
                "Draught": row["Draught"],
                "Destination": row["Destination"],
                "ETA": row["ETA"],
                "Data_source_type": row["Data source type"],
                "A": row["A"],
                "B": row["B"],
                "C": row["C"],
                "D": row["D"],
                "Type_of_positioning_device": row["Type of position fixing device"]
            }
        })


    # --- 3.3. Create Circles & Popups ---
    if not circle_list_ship1:
        print(f"No location points to plot for {SHIP1}")
        continue

    if not circle_list_ship2:
        print(f"No location points to plot for {SHIP2}")
        continue

  # Create GeoJSON string once
    if circle_list_ship1:
        geojson_string = json.dumps({"type": "FeatureCollection", "features": circle_list_ship1}, indent=2)
        features_data = json.loads(geojson_string)["features"]
    else:
        features_data = []
    
    for feature in features_data:
        lat = feature['geometry']['coordinates'][1]
        lon = feature['geometry']['coordinates'][0]
        
        props = feature['properties']
        mmsi_name = str(props['MMSI'])
        Type_name = props['Type']
        Name_name = props['Name']
        Time_Stamp = props['Timestamp']
        Navigation_Status = props['Navigational_Status']
        R_O_T = props['ROT']
        SpeedOverGround = props['SOG']
        CourceOverGround = props['COG']
        Data_Source = props['Data_source_type']
        Type_of_positioning_fix_device = props['Type_of_positioning_device']
        Ship_Type = props['Ship_type']
        Heading = feature['properties']['Heading']
        Cargo_Type = feature['properties']['Cargo_type']
        Call_sign = feature['properties']['Callsign']
        
        I_M_O = props['IMO']
        ETA = props['ETA']
        Draught = props['Draught']
        
        Destination = props['Destination']
        Width = props['Width']
        Length = props['Length']

        A_value = props['A']
        B_value = props['B']
        C_value = props['C']
        D_value = props['D']
        
        Draw_Color = 'red' 



        PopUp_Label = f"<b>Navigation status:</b> {Navigation_Status}<br>" \
                       f"<b>Time:</b> {Time_Stamp}<br>" \
                       f"<b>MMSI:</b> {mmsi_name}<br>" \
                       f"<b>Type:</b> {Type_name}<br>" \
                       f"<b>Name:</b> {Name_name}<br>" \
                       f"<b>Rate of Turn:</b> {R_O_T}"

        # Logic to change popup content based on ToMD
        if ToMD == 'AtoN':
            PopUp_Label = f"<b>Name:</b> {Name_name}<br><b>Type:</b> {Type_name}<br><b>MMSI:</b> {mmsi_name}<br>" \
                           f"<b>Timestamp:</b> {Time_Stamp}<br><b>Data Source Type:</b> {Data_Source}<br>" \
                           f"<b>Type of positional fixing device:</b> {Type_of_positioning_fix_device}<br><br>" \
                           f"<b>Length from GPS to the bow:</b> {A_value}<br>" \
                           f"<b>Length from GPS to the stern:</b> {B_value}<br>" \
                           f"<b>Length from GPS to starboard side:</b> {C_value}<br>" \
                           f"<b>Length from GPS to port side:</b> {D_value}<br><div style=\"width: 300px;\">"
        elif ToMD == 'Base Station':
            PopUp_Label = f"<b>Type:</b> {Type_name}<br><b>MMSI:</b> {mmsi_name}<br>" \
                           f"<b>Timestamp:</b> {Time_Stamp}<br><b>Data Source Type:</b> {Data_Source}<br>" \
                           f"<b>Type of positional fixing device:</b> {Type_of_positioning_fix_device}<br><div style=\"width: 300px;\">" 
        elif ToMD == 'Class A':
                    PopUp_Label =f"<b>Name:</b> {Name_name}  <b>Type:</b> {Type_name}  <b>MMSI:</b> {mmsi_name}  <b>Callsign:</b> {Call_sign} <br>" \
                        f"<b>Country registration:</b> {Country_Name_Ship1}, <b>MID</b>: {Country_Code_Ship1} </br>" \
                        f"<b>Ship type:</b> " + get_type_color(Ship_Type) + "  </b>" \
                        f"<b>Type of cargo:</b> {Cargo_Type} <br>" \
                        f"<b>Navigation:</b> {Navigation_Status} <br>" \
                        f"<b>Timestamp:</b> {Time_Stamp}<br>" \
                        f"<b>Data Source Type:</b> {Data_Source} " \
                        f"<b>alternative source:</b> {Type_of_positioning_fix_device}<br><br>"\
                        f"<b>Rate on turn:</b> {R_O_T} "\
                        f"<b>Speed Over Ground:</b> {SpeedOverGround}  "\
                        f"<b>Course Over Ground:</b> {CourceOverGround}<br>"\
                        f"<b>Destination:</b> {Destination}  <b>IMO:</b> {I_M_O}  <b>ETA:</b> {ETA}<br>"\
                        f"<b>Width of the vessel:</b> {Width} <b>Lenght of the vessel:</b> {Length} "\
                        f"<b>Draught:</b> {Draught} <br> "\
                        f"<b>Length from GPS to the bow:</b> {A_value} "\
                        f"<b>to stern:</b> {B_value} "\
                        f"<b>to starboard:</b> {C_value} "\
                        f"<b>to port:</b> {D_value} <div style=\"width: 400px;\">"
                    
#                    SpeedOverGround_Int = SpeedOverGround
#                    print(f"Greitis: -{SpeedOverGround}-")
#                    if SpeedOverGround_Int == 0:
#                        Draw_Color = 'green'
#                        print('Color: green')
                    if SpeedOverGround is not None:
                        Draw_Color = get_color(SpeedOverGround)
#                       print(f"Colour: {get_color(SpeedOverGround)}")
    
        else:
            # Default generic popup logic if other ToMD
            PopUp_Label = f"<b>MMSI:</b> {mmsi_name}<br><b>Type:</b> {Type_name}<br><b>Name:</b> {Name_name}<div style=\"width: 300px;\">"

             

        circle_ship1 = folium.Circle(location=[lat, lon], radius=5, 
                               color=Draw_Color, fill=True, fill_color=Draw_Color, 
                               popup=PopUp_Label, max_width=400)
        circle_ship1.add_to(m)
        points_ship1.append([lat, lon])

    # Filter outlier coordinates for Ship 1 to remove "flying" artifacts
    print(f"Filtering outlier coordinates for Ship 1 ({SHIP1})...")
    points_ship1_filtered = filter_outlier_coordinates(points_ship1, mmsi=SHIP1, max_distance_km=MAX_DISTANCE_KM, output_csv_file=DFSI_OUTLIER_LOG_FILE)
    
    # Update the line with filtered points
    line_ship1 = folium.PolyLine(locations=points_ship1_filtered, color='red', weight=2, opacity=0.7)
    line_ship1.add_to(m)
    if circle_list_ship2:
        geojson_string2 = json.dumps({"type": "FeatureCollection", "features": circle_list_ship2}, indent=2)
        features_data2 = json.loads(geojson_string2)["features"]
    else:
        features_data2 = []
    
    for feature in features_data2:
        lat = feature['geometry']['coordinates'][1]
        lon = feature['geometry']['coordinates'][0]
        
        props = feature['properties']
        mmsi_name = str(props['MMSI'])
        Type_name = props['Type']
        Name_name = props['Name']
        Time_Stamp = props['Timestamp']
        Navigation_Status = props['Navigational_Status']
        R_O_T = props['ROT']
        SpeedOverGround = props['SOG']
        CourceOverGround = props['COG']
        Data_Source = props['Data_source_type']
        Type_of_positioning_fix_device = props['Type_of_positioning_device']
        Ship_Type = props['Ship_type']
        Heading = feature['properties']['Heading']
        Cargo_Type = feature['properties']['Cargo_type']
        Call_sign = feature['properties']['Callsign']
        
        I_M_O = props['IMO']
        ETA = props['ETA']
        Draught = props['Draught']
        
        Destination = props['Destination']
        Width = props['Width']
        Length = props['Length']

        A_value = props['A']
        B_value = props['B']
        C_value = props['C']
        D_value = props['D']
        
        Draw_Color = 'red' 



        PopUp_Label = f"<b>Navigation status:</b> {Navigation_Status}<br>" \
                       f"<b>Time:</b> {Time_Stamp}<br>" \
                       f"<b>MMSI:</b> {mmsi_name}<br>" \
                       f"<b>Type:</b> {Type_name}<br>" \
                       f"<b>Name:</b> {Name_name}<br>" \
                       f"<b>Rate of Turn:</b> {R_O_T}"

        # Logic to change popup content based on ToMD
        if ToMD == 'AtoN':
            PopUp_Label = f"<b>Name:</b> {Name_name}<br><b>Type:</b> {Type_name}<br><b>MMSI:</b> {mmsi_name}<br>" \
                           f"<b>Timestamp:</b> {Time_Stamp}<br><b>Data Source Type:</b> {Data_Source}<br>" \
                           f"<b>Type of positional fixing device:</b> {Type_of_positioning_fix_device}<br><br>" \
                           f"<b>Length from GPS to the bow:</b> {A_value}<br>" \
                           f"<b>Length from GPS to the stern:</b> {B_value}<br>" \
                           f"<b>Length from GPS to starboard side:</b> {C_value}<br>" \
                           f"<b>Length from GPS to port side:</b> {D_value}<br><div style=\"width: 300px;\">"
        elif ToMD == 'Class A':
                    PopUp_Label =f"<b>Name:</b> {Name_name}  <b>Type:</b> {Type_name}  <b>MMSI:</b> {mmsi_name}  <b>Callsign:</b> {Call_sign} <br>" \
                        f"<b>Country registration:</b> {Country_Name_Ship2}, <b>MID</b>: {Country_Code_Ship2} </br>" \
                        f"<b>Ship type:</b> " + get_type_color(Ship_Type) + "  </b>" \
                        f"<b>Type of cargo:</b> {Cargo_Type} <br>" \
                        f"<b>Navigation:</b> {Navigation_Status} <br>" \
                        f"<b>Timestamp:</b> {Time_Stamp}<br>" \
                        f"<b>Data Source Type:</b> {Data_Source} " \
                        f"<b>alternative source:</b> {Type_of_positioning_fix_device}<br><br>"\
                        f"<b>Rate on turn:</b> {R_O_T} "\
                        f"<b>Speed Over Ground:</b> {SpeedOverGround}  "\
                        f"<b>Course Over Ground:</b> {CourceOverGround}<br>"\
                        f"<b>Destination:</b> {Destination}  <b>IMO:</b> {I_M_O}  <b>ETA:</b> {ETA}<br>"\
                        f"<b>Width of the vessel:</b> {Width} <b>Lenght of the vessel:</b> {Length} "\
                        f"<b>Draught:</b> {Draught} <br> "\
                        f"<b>Length from GPS to the bow:</b> {A_value} "\
                        f"<b>to stern:</b> {B_value} "\
                        f"<b>to starboard:</b> {C_value} "\
                        f"<b>to port:</b> {D_value} <div style=\"width: 400px;\">"
                    
#                    SpeedOverGround_Int = SpeedOverGround
#                    print(f"Greitis: -{SpeedOverGround}-")
#                    if SpeedOverGround_Int == 0:
#                        Draw_Color = 'green'
#                        print('Color: green')
                    if SpeedOverGround is not None:
                        Draw_Color = get_color(SpeedOverGround)
#                       print(f"Colour: {get_color(SpeedOverGround)}")
    
        else:
            # Default generic popup logic if other ToMD
            PopUp_Label = f"<b>MMSI:</b> {mmsi_name}<br><b>Type:</b> {Type_name}<br><b>Name:</b> {Name_name}<div style=\"width: 300px;\">"

             

        circle_ship2 = folium.Circle(location=[lat, lon], radius=5, 
                               color=Draw_Color, fill=True, fill_color=Draw_Color, 
                               popup=PopUp_Label, max_width=400)
        circle_ship2.add_to(m)
        points_ship2.append([lat, lon])

    # Filter outlier coordinates for Ship 2 to remove "flying" artifacts
    print(f"Filtering outlier coordinates for Ship 2 ({SHIP2})...")
    points_ship2_filtered = filter_outlier_coordinates(points_ship2, mmsi=SHIP2, max_distance_km=MAX_DISTANCE_KM, output_csv_file=DFSI_OUTLIER_LOG_FILE)
    
    # Update the line with filtered points
    line_ship1 = folium.PolyLine(locations=points_ship1_filtered, color='green', weight=2, opacity=0.7)
    line_ship1.add_to(m)
    line_ship2 = folium.PolyLine(locations=points_ship2_filtered, color='brown', weight=2, opacity=0.7)
    line_ship2.add_to(m)

    # --- 3.4. Save Map ---
    # Create specific folder for this ToMD and MMSI pair
    # Save Map
    map_filename = f"{GEO_MAPS_FOLDER}/GPLD_{SHIP1}_and_{SHIP2}._{CODE_VERSION}_.html"
    m.save(map_filename)
     
    print(f"Saved map: {map_filename}")

# Stop Spark Session
spark.stop()
