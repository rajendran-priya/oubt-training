•	Day 9-10: Advanced MDM - Matching, Deduplication & Lifecycle Governance
o	Data profiling techniques
o	Fuzzy matching algorithms: Levenshtein, Jaro-Winkler
o	Probabilistic matching
o	ML-based matching
o	Survivorship rules
o	Master Data Lifecycle States:
•	Proposed: New record submitted, awaiting review
•	Active: Approved golden record, in use
•	Deprecated: Marked for retirement, still quarriable
•	Retired: No longer valid, archived for compliance
o	Change Management Process:
•	Who can propose new master data?
•	What approval workflow applies? (Auto-approve? Steward review? Owner sign-off?)
•	How are conflicting updates resolved? (Last write wins? Steward arbitration? Automated rules?)
•	How long is change history retained?
o	Match Confidence & Governance:
•	High confidence (>95%): Auto-merge with audit log
•	Medium confidence (80-95%): Flag for steward review
•	Low confidence (<80%): Require manual resolution
o	Document match logic as versioned rules (governance artifact)
o	Change Data Capture (CDC) for master data with audit trail
•	Hands-on:
o	Implement fuzzy matching with Python (fuzzywuzzy, recordlinkage)
o	Build deduplication pipeline for vendors confidence scores and steward review queue
o	Create data quality scorecard with business impact mapping

Week 2 Deliverable
•	Document "Week 2 Learnings"
•	Build data lake with Delta Lake format and governance zones
•	Implement Glue ETL with data quality checks tied to business rules
•	Deploy MDM with: 
o	Matching/deduplication engine with confidence thresholds
o	Lifecycle state management
o	Role-based access control
o	Audit trail and change history
•	Governance Dashboard: 
o	Quality metrics by domain
o	Master data approval queue
o	Orphan transactions report
o	Steward activity log
