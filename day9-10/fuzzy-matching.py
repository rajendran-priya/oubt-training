import pandas as pd
from thefuzz import process
import os

# Get the project root directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load official NYC Taxi Zone Lookup (CSV)
zone_path = os.path.join(project_root, "yellow-taxi-data-set ", "taxi_zone_lookup.csv")
df_zones = pd.read_csv(zone_path)

# Load a sample of Yellow Taxi Trip Data (Parquet)
# Using August 2025 data as a sample
trip_path = os.path.join(project_root, "yellow-taxi-data-set ", "yellow_tripdata_2025-08.parquet")
df_trips = pd.read_parquet(trip_path).head(1000)  # taking first 1000 rows for speed

print(f"Loaded {len(df_zones)} official zones.")
print(f"Loaded {len(df_trips)} trip records.")
print(f"Zone data columns: {df_zones.columns.tolist()}")
print(f"Trip data columns: {df_trips.columns.tolist()}")
print()



# Imagine we have 'messy' neighborhood names from a different source
external_data = pd.DataFrame({
    'external_name': ['Upper East Side N.', 'JFK Arport', 'Btry Park City'],
    'temperature': [42, 38, 40]
})

# Get the list of 'correct' names from the actual NYC data
official_names = df_zones['Zone'].tolist()

def match_nyc_zones(messy_name, choices):
    # Returns (best_match, score)
    match, score = process.extractOne(messy_name, choices)
    return match if score > 80 else None

# Clean the external data by matching it to NYC official zones
external_data['Official_Zone'] = external_data['external_name'].apply(
    lambda x: match_nyc_zones(x, official_names)
)

print(external_data)

# 1. Join External Data to the Zone Lookup (to get the LocationID)
clean_mapping = pd.merge(
    external_data, 
    df_zones[['LocationID', 'Zone']], 
    left_on='Official_Zone', 
    right_on='Zone'
)

# 2. Join that to the actual Trip Data
# PulsLocationID = Pickup Location ID
final_dataset = pd.merge(
    df_trips, 
    clean_mapping[['LocationID', 'temperature']], 
    left_on='PULocationID', 
    right_on='LocationID'
)

print(f"Final dataset has {len(final_dataset)} matches.")
print(final_dataset.head())