import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from thefuzz import fuzz



# --- 2. DATA EXTRACTION ---
csv_path = '/Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/yellow-taxi-data-set /taxi_zone_lookup.csv'
df_raw_zones = pd.read_csv(csv_path)

# --- 3. DATA QUALITY: FUZZY DUPLICATE DETECTION ---
print("🔍 Checking for duplicate Taxi Zones using Fuzzy Matching...")
zones = df_raw_zones['Zone'].unique()
duplicatesFound = False

for i, zone_a in enumerate(zones):
    for zone_b in zones[i+1:]:
        score = fuzz.ratio(zone_a, zone_b)
        # Score > 90 indicates potential typo/duplicate
        if score > 90 and score < 100:
            print(f"⚠️ Potential Duplicate Found: '{zone_a}' and '{zone_b}' (Score: {score})")
            duplicatesFound = True

if not duplicatesFound:
    print("✅ No fuzzy duplicates detected in Taxi Zones.")

