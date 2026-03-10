•	Day 14-15: Slowly Changing Dimensions & Master Data Versioning 
o	Technical Content
•	SCD Types 0-6 detailed
•	Temporal tables and bitemporal modeling
•	Surrogate keys vs natural keys
•	Implementation patterns
•	Version control for master data
•	Audit trails and change tracking
•	Point-in-time queries
•	Rollback strategies
o	Why SCD matters for governance:
•	Regulatory compliance: "Show me customer address on date of transaction" (GDPR, audit)
•	Dispute resolution: "What was product price when order placed?" (legal)
•	Impact analysis: "How many customers affected by this vendor change?" (risk)
•	Rollback capability: "Undo this batch of incorrect updates" (data quality)
o	SCD Type Selection as Governance Decision:
•	Type 1 (overwrite): For reference data where history not needed (e.g., typo fixes)
•	Type 2 (history tracking): For regulated data where history mandatory (e.g., customer segments)
•	Type 3 (limited history): For business-driven "current vs prior" (e.g., territory assignments)
o	Temporal queries for governance use cases:
•	Point-in-time reporting for compliance
•	"As of" joins for accurate historical analysis
•	Change frequency analytics for stewardship
o	Master Data Versioning 
•	Version metadata for golden records:
•	version_number, effective_from, effective_to
•	created_by, approved_by, approval_date
•	change_reason, change_source
•	is_current_flag
•	Versioning policies:
•	How long is history retained? (Regulatory requirement vs operational need)
•	When are versions purged? (Never? After 7 years?)
•	How is "truth" determined when conflicts exist?
o	Hands-on:
•	Implement SCD Type 2 in PostgreSQL RDS with metadata columns
•	Write stored procedures for SCD operations
•	Create PySpark script for SCD with Delta Lake leveraging time travel for rollback
•	Implement in SQL with version control
•	Create version management procedures: 
•	approve_version(record_id, approver, reason)
•	rollback_version(record_id, target_version, reason)
•	audit_version_history(record_id, date_range)
