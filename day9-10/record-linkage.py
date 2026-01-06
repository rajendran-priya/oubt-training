import pandas as pd
import recordlinkage
import os

# 1. Load the official NYC Zone data (The 'True' data)
zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
df_zones = pd.read_csv(zone_url).set_index('LocationID')

# 2. Create 'Messy' data (What you are trying to match)
messy_data = pd.DataFrame({
    'id': [101, 102, 103],
    'Borough': ['Manhattan', 'Manhattan', 'Queens'],
    'Zone_Name': ['Alphbet City', 'Btry Park', 'Astoria NYC']
}).set_index('id')

# 3. Step 1: Indexing (Candidate Generation)
# We 'Block' by Borough so we only compare Manhattan to Manhattan (saves time)
indexer = recordlinkage.Index()
indexer.block('Borough')
candidate_links = indexer.index(messy_data, df_zones)

# 4. Step 2: Comparison Logic
compare = recordlinkage.Compare()

# Compare the 'Zone_Name' from messy_data to 'Zone' in df_zones
# Using 'jarowinkler' algorithm (great for typos in names)
compare.string('Zone_Name', 'Zone', method='jarowinkler', threshold=0.85, label='score')

# 5. Step 3: Compute & Classify
features = compare.compute(candidate_links, messy_data, df_zones)

# Filter for matches where the score is 1.0 (met our threshold)
matches = features[features['score'] == 1.0]

print("Matches Found:")
print(matches)

# 1. Convert the 'matches' MultiIndex into a standard DataFrame
df_matches = matches.reset_index()

# 2. Merge with 'messy_data' to get the original messy names
df_comparison = pd.merge(
    df_matches, 
    messy_data[['Zone_Name']], 
    left_on='id', 
    right_index=True
)

# 3. Merge with 'df_zones' to get the official NYC names
df_comparison = pd.merge(
    df_comparison, 
    df_zones[['Zone']], 
    left_on='LocationID', 
    right_index=True
)

# Rename for clarity
df_comparison = df_comparison.rename(columns={
    'Zone_Name': 'Input (Messy)',
    'Zone': 'Match (Official)'
})

print("--- Final Comparison Table ---")
print(df_comparison[['id', 'LocationID', 'Input (Messy)', 'Match (Official)', 'score']])