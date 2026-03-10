•	Day 4: Master Data Management - Foundation & Strategy
o	MDM Concepts & Business Alignment
•	What is MDM and why it matters to the business (not just IT)
•	Master data domains: Customer, Product, Location, Vendor 
•	For each domain: Who cares? What decisions depend on it? What breaks if it's wrong?
•	MDM implementation styles with real-world scenarios: 
•	Registry: We need a single view but systems must stay independent 
•	Consolidation: We need read-only golden records for analytics
•	Coexistence: We need bidirectional sync with operational systems
•	Centralized: Master data is created and managed in one system
o	MDM Governance Model
•	MDM Roles & Responsibilities: 
•	Data Owner: Accountable for domain (usually business VP)
•	Data Steward: Day-to-day quality & change management (usually business analyst)
•	Data Custodian: Technical implementation (data engineer - that's you!)
•	Data Consumer: Uses data (analysts, apps, ML models)
•	MDM Operating Model: 
•	How are golden records created? (Application form? Automated match?)
•	Who approves changes? (Steward? Owner? Automated rules?)
•	What's the escalation path for conflicts?
•	How are exceptions handled?
o	Golden records and data consolidation
o	Match and merge strategies with governance implications
o	Data quality dimensions: accuracy, completeness, consistency, timeliness, validity, uniqueness
o	MDM components with governance lens:
•	Repository (storage)
•	Integration layer (publish/subscribe)
•	Quality engine (rules as governance artifacts)
•	Workflow engine (approval workflows)
•	Audit & lineage (regulatory compliance)
o	Hands-on:
•	Identify master data domains in NYC taxi dataset (zones, vendors, rate codes)
•	For each domain, document: 
•	Business owner (simulated)
•	Business justification (why is this master data?)
•	Update frequency and change triggers
•	Quality requirements and thresholds
•	Approval workflow (who can create/update/retire?)
•	Create simple matching algorithm for duplicate detection
•	Design golden record strategy for taxi zones with survivorship rules documented
•	Build master data tables in RDS with audit columns (created_by, updated_by, approved_by, version)
