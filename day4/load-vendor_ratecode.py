import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from thefuzz import fuzz

# --- 1. CONFIGURATION ---
user_raw = "admin"
password_raw = "Check123*"
host = "yellotaxi-master-database.cgvogeskmafq.us-east-1.rds.amazonaws.com"
target_db = "nyc_taxi_master_hub" # Professional database name

# URL-encode credentials
user = urllib.parse.quote_plus(user_raw)
password = urllib.parse.quote_plus(password_raw)

# Connect to the default 'mysql' database first to create the new hub
engine_setup = create_engine(f"mysql+pymysql://{user}:{password}@{host}/mysql")

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

# --- 4. PREPARE VENDOR & RATECODE DATA ---
df_vendors = pd.DataFrame([
    (1, 'Creative Mobile Technologies, LLC', 'CMT'),
    (2, 'Curb Mobility, LLC', 'Verifone'),
    (6, 'Myle Technologies Inc', 'Myle'),
    (7, 'Helix', 'Helix')
], columns=['VendorID', 'VendorName', 'Abbreviation'])

df_rate_codes = pd.DataFrame([
    (1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'), 
    (4, 'Nassau/Westchester'), (5, 'Negotiated fare'), 
    (6, 'Group ride'), (99, 'Unknown')
], columns=['RatecodeID', 'Description'])

# --- 5. NORMALIZE TAXI ZONES (Lookup Tables) ---
# Create Borough Lookup
boroughs = pd.DataFrame(df_raw_zones['Borough'].unique(), columns=['BoroughName']).reset_index(names='BoroughID')

# Create Service Zone Lookup
service_zones = pd.DataFrame(df_raw_zones['service_zone'].unique(), columns=['ServiceName']).reset_index(names='ServiceID')

# Link IDs back to the main Zone table
df_zones_final = df_raw_zones.merge(boroughs, left_on='Borough', right_on='BoroughName')
df_zones_final = df_zones_final.merge(service_zones, left_on='service_zone', right_on='ServiceName')
df_zones_final = df_zones_final[['LocationID', 'Zone', 'BoroughID', 'ServiceID']]

# --- 6. EXECUTE LOAD TO RDS ---
try:
    with engine_setup.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {target_db};"))
        print(f"✅ Created Hub Database: {target_db}")

    # Re-connect to the new specific Hub
    engine_hub = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{target_db}")

    with engine_hub.begin() as conn:
        print(f"🚀 Loading Golden Records into {target_db}...")
        
        # Load all 5 Master Tables
        boroughs.to_sql('dim_borough', conn, if_exists='replace', index=False)
        service_zones.to_sql('dim_service_zone', conn, if_exists='replace', index=False)
        df_zones_final.to_sql('dim_taxi_zones', conn, if_exists='replace', index=False)
        df_vendors.to_sql('dim_vendor', conn, if_exists='replace', index=False)
        df_rate_codes.to_sql('dim_rate_code', conn, if_exists='replace', index=False)

        # Apply Constraints (Governance)
        conn.execute(text("ALTER TABLE dim_borough ADD PRIMARY KEY (BoroughID);"))
        conn.execute(text("ALTER TABLE dim_service_zone ADD PRIMARY KEY (ServiceID);"))
        conn.execute(text("ALTER TABLE dim_taxi_zones ADD PRIMARY KEY (LocationID);"))
        conn.execute(text("ALTER TABLE dim_vendor ADD PRIMARY KEY (VendorID);"))
        conn.execute(text("ALTER TABLE dim_rate_code ADD PRIMARY KEY (RatecodeID);"))
        
        # Add Foreign Keys
        conn.execute(text("""
            ALTER TABLE dim_taxi_zones 
            ADD CONSTRAINT fk_borough FOREIGN KEY (BoroughID) REFERENCES dim_borough(BoroughID),
            ADD CONSTRAINT fk_service FOREIGN KEY (ServiceID) REFERENCES dim_service_zone(ServiceID);
        """))

    print(f"🎉 SUCCESS! Your NYC Taxi Master Hub is fully built in database: {target_db}")

except Exception as e:
    print(f"❌ FAILED: {e}")