import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime

# --- 1. SETTINGS ---
user_raw, password_raw = "admin", "Achieve123*"
host = "yellotaxi-master-database.cgvogeskmafq.us-east-1.rds.amazonaws.com"
target_db = "nyc_taxi_master_hub"

user = urllib.parse.quote_plus(user_raw)
password = urllib.parse.quote_plus(password_raw)

# Connect to 'mysql' system db first
engine_setup = create_engine(f"mysql+pymysql://{user}:{password}@{host}/mysql")

# --- 2. DATA PREPARATION ---
csv_path = '/Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/yellow-taxi-data-set /taxi_zone_lookup.csv'
df_raw = pd.read_csv(csv_path)

def add_governance(df):
    df_out = df.copy() 
    df_out['created_by'] = 'Priya_ETL_Admin'
    df_out['updated_by'] = 'Priya_ETL_Admin'
    df_out['approved_by'] = 'Data_Steward_Primary'
    df_out['version'] = 1.0
    df_out['created_at'] = datetime.now()
    return df_out

# --- PREPARE ALL 5 DATASETS ---
# 1 & 2: Vendor and Rate Code
df_vendors = add_governance(pd.DataFrame([
    (1, 'Creative Mobile Technologies, LLC', 'CMT'),
    (2, 'Curb Mobility, LLC', 'Verifone'),
    (6, 'Myle Technologies Inc', 'Myle'),
    (7, 'Helix', 'Helix')
], columns=['VendorID', 'VendorName', 'Abbreviation']))

df_rate_codes = add_governance(pd.DataFrame([
    (1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'), 
    (4, 'Nassau/Westchester'), (5, 'Negotiated fare'), 
    (6, 'Group ride'), (99, 'Unknown')
], columns=['RatecodeID', 'Description']))

# 3, 4 & 5: Zones, Boroughs, and Service Zones
boroughs = add_governance(pd.DataFrame(df_raw['Borough'].unique(), columns=['BoroughName']).reset_index(names='BoroughID'))
service_zones = add_governance(pd.DataFrame(df_raw['service_zone'].unique(), columns=['ServiceName']).reset_index(names='ServiceID'))

df_zones_final = df_raw.merge(boroughs[['BoroughID', 'BoroughName']], left_on='Borough', right_on='BoroughName')
df_zones_final = df_zones_final.merge(service_zones[['ServiceID', 'ServiceName']], left_on='service_zone', right_on='ServiceName')
df_zones_final = add_governance(df_zones_final[['LocationID', 'Zone', 'BoroughID', 'ServiceID']])

# --- 3. EXECUTION ---
try:
    with engine_setup.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {target_db};"))

    engine_hub = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{target_db}")

    with engine_hub.connect() as conn:
        print("🧹 Cleaning up old schema and constraints...")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        
        # Drop all 5 potential tables to ensure a clean start
        tables_to_drop = ['dim_taxi_zones', 'dim_borough', 'dim_service_zone', 'dim_vendor', 'dim_rate_code']
        for table in tables_to_drop:
            conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
        
        print("📤 Loading all 5 Governed Tables...")
        df_vendors.to_sql('dim_vendor', conn, if_exists='replace', index=False)
        df_rate_codes.to_sql('dim_rate_code', conn, if_exists='replace', index=False)
        boroughs.to_sql('dim_borough', conn, if_exists='replace', index=False)
        service_zones.to_sql('dim_service_zone', conn, if_exists='replace', index=False)
        df_zones_final.to_sql('dim_taxi_zones', conn, if_exists='replace', index=False)

        print("🔑 Creating Primary Key Indexes...")
        conn.execute(text("ALTER TABLE dim_vendor ADD PRIMARY KEY (VendorID);"))
        conn.execute(text("ALTER TABLE dim_rate_code ADD PRIMARY KEY (RatecodeID);"))
        conn.execute(text("ALTER TABLE dim_borough ADD PRIMARY KEY (BoroughID);"))
        conn.execute(text("ALTER TABLE dim_service_zone ADD PRIMARY KEY (ServiceID);"))
        conn.execute(text("ALTER TABLE dim_taxi_zones ADD PRIMARY KEY (LocationID);"))

        print("🔗 Establishing Referential Integrity...")
        conn.execute(text("ALTER TABLE dim_taxi_zones ADD CONSTRAINT fk_borough FOREIGN KEY (BoroughID) REFERENCES dim_borough(BoroughID);"))
        conn.execute(text("ALTER TABLE dim_taxi_zones ADD CONSTRAINT fk_service FOREIGN KEY (ServiceID) REFERENCES dim_service_zone(ServiceID);"))

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        print(f"🎉 SUCCESS! All 5 tables in '{target_db}' are now fully governed and row-populated.")

except Exception as e:
    print(f"❌ FAILED: {e}")