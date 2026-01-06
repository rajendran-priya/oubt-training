import pandas as pd
import recordlinkage

# 1. Setup messy vendor data
vendors = pd.DataFrame({
    'vendor_name': ['Creative Mobile Technologies', 'VeriFone Inc', 'Verifone', 'CMT', 'Vrifone']
})

# 2. Comparison Logic (Self-linkage to find duplicates within one list)
indexer = recordlinkage.Index()
indexer.full() # Small dataset, so we compare everything
pairs = indexer.index(vendors)

compare = recordlinkage.Compare()
compare.string('vendor_name', 'vendor_name', method='jarowinkler', label='conf_score')

# 3. Generate Confidence Scores
features = compare.compute(pairs, vendors)

# 4. Building the "Steward Review Queue"
# Auto-match high scores, flag mid-range scores for review
auto_match_threshold = 0.95
steward_review_threshold = 0.80

features['action'] = 'reject'
features.loc[features['conf_score'] >= steward_review_threshold, 'action'] = 'steward_review'
features.loc[features['conf_score'] >= auto_match_threshold, 'action'] = 'auto_merge'

# Display what goes to the human Steward
review_queue = features[features['action'] == 'steward_review']
print("--- Steward Review Queue ---")
print(review_queue)