import pandas as pd
import pyarrow.parquet as pq

# Load datasets
zones = pd.read_csv('data/taxi_zone_lookup.csv')
trips = pq.read_table('/Users/priyarajendran/Desktop/git/rajendran-priya/oubt-training/yellow-taxi-data-set /taxi_zone_lookup.csv').to_pandas()

print("=" * 50)
print("LAB 1: IDENTIFY MASTER DATA ENTITIES")
print("=" * 50)

# Analyze Zone Master Data
print(f"\n1. ZONE MASTER: {len(zones)} records")
print(f"   Columns: {list(zones.columns)}")

# Identify Reference Data
print(f"\n2. REFERENCE DATA:")
print(f"   - VendorIDs: {sorted(trips['VendorID'].unique())}")
print(f"   - RateCodes: {sorted(trips['RatecodeID'].dropna().unique())}")
print(f"   - PaymentTypes: {sorted(trips['payment_type'].unique())}")

# Transactional Data
print(f"\n3. TRANSACTIONAL DATA:")
print(f"   - Trip Records: {len(trips):,}")