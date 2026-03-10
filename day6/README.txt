•	Day 6: Data Lake Concepts & Modern Storage
o	Data lake vs warehouse vs data mart
o	Architecture patterns: raw/processed/curated zones
o	AWS Lake Formation basics
o	Metadata management and cataloging
o	Master data layer in data lakes
o	Parquet, ORC, Avro comparison
o	Delta Lake: ACID transactions, time travel, schema evolution
o	Apache Iceberg & Hudi overview
o	Lakehouse architecture
o	Master data zone in data lake (separate from raw/processed/curated)
o	Governance zones:
•	Landing/Raw: No governance (original source state)
•	Validated: Basic quality checks passed
•	Curated: Business rules applied, enriched with master data
•	Master: Golden records only, strictly governed
•	Archive: Compliance retention, immutable
o	Access patterns by zone: Different governance for different zones
o	Data lineage as governance requirement: Where did this come from? How was it transformed?
o	Hands-on Lab: 
•	Design data lake architecture with Draw.io showing governance boundaries
•	Implement data lake zones in S3 with appropriate tagging and access controls
•	Convert data to Delta Lake format
•	Use Delta Lake time travel features for audit trail compliance
